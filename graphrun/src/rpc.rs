use crate::catalog::Catalog;
use crate::cluster::ClusterNetwork;
use crate::compiler::compile_yaml;
use crate::domain::{Command, CommandBody, DomainEvent, assignments_from, worker_has_ready};
use crate::error::ErrorKind;
use crate::generated::client_server::{Client, ClientServer};
use crate::generated::raft_server::{Raft as RaftSvc, RaftServer};
use crate::generated::worker_server::{Worker as WorkerSvc, WorkerServer};
use crate::generated::{
    Ack, Blob, CancelRequest, ClaimRequest, ClaimResponse, ClockAcknowledgeRequest,
    ClockHealthRequest, ClockHealthResponse, CommandResultRequest, CommandResultResponse,
    HistoryRequest, HistoryResponse, InspectRequest, InspectResponse, ListRequest, ListResponse,
    PublishCatalogRequest, PublishDefinitionRequest, ReconcileRequest, RegisterRequest,
    RegisterResponse, RenewRequest, RenewResponse, RenewSessionRequest, RenewSessionResponse,
    ReplayRequest, ReplayResponse, ReportRequest, SignalRequest, StartRequest, StartResponse,
    WatchReadyRequest, WatchReadyResponse,
};
use crate::ids::{
    ActivationId, CommandId, EventId, LeaseRevision, OwnerGeneration, RunId, WorkerSessionId,
};
use crate::storage::{StorageHandle, TypeConfig};
use crate::tls::TlsMaterial;
use crate::value::Value;
use crate::worker_contract::{WorkerCapability, role_name};
use crate::write::{
    admit_unapplied, inspect_view, linearizable_read, now, write_raft, write_raft_response,
};
use openraft::raft::AppendEntriesRequest;
use openraft::{Raft, ServerState};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct GraphServices {
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
    publication_ca: Arc<String>,
    publication_cluster: crate::tls::ClusterId,
    genesis_members: BTreeMap<u64, SocketAddr>,
    network: ClusterNetwork,
    node_id: u64,
}

impl GraphServices {
    pub fn new(
        raft: Raft<TypeConfig>,
        storage: StorageHandle,
        notify: Arc<Notify>,
        ca_pem: String,
        node_id: u64,
        genesis_members: BTreeMap<u64, SocketAddr>,
        network: ClusterNetwork,
    ) -> Self {
        let cluster = crate::tls::cluster_id_from_ca(&ca_pem).expect("hex cluster id");
        Self {
            raft,
            storage,
            notify,
            publication_ca: Arc::new(ca_pem),
            publication_cluster: cluster,
            genesis_members,
            network,
            node_id,
        }
    }

    fn verified_peer<T>(
        &self,
        request: &Request<T>,
    ) -> Result<crate::tls::VerifiedPeerIdentity, Status> {
        let certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("mTLS peer certificate required"))?;
        let chain: Vec<rustls::pki_types::CertificateDer<'static>> = certs
            .iter()
            .map(|cert| rustls::pki_types::CertificateDer::from(cert.as_ref().to_vec()))
            .collect();
        crate::tls::verify_peer_identity(&self.publication_ca, &self.publication_cluster, &chain)
            .map_err(status_error)
    }

    fn authenticated_context<T>(
        &self,
        request: &Request<T>,
        role: crate::tls::PeerRole,
    ) -> Result<crate::publication::AuthContext, Status> {
        let peer = self.verified_peer(request)?;
        peer.require_role(role).map_err(status_error)?;
        Ok(crate::publication::AuthContext::verified_peer(&peer))
    }

    fn query_context<T>(
        &self,
        request: &Request<T>,
    ) -> Result<crate::publication::AuthContext, Status> {
        self.authenticated_context(request, crate::tls::PeerRole::Client)
    }

    async fn member_context<T>(&self, request: &Request<T>, sender_id: u64) -> Result<(), Status> {
        let peer = self.verified_peer(request)?;
        let member_id =
            crate::tls::PrincipalId::parse(sender_id.to_string()).map_err(status_error)?;
        peer.require_member_identity(&self.publication_cluster, &member_id)
            .map_err(status_error)?;
        let roster = self.storage.applied_members().await.map_err(status_error)?;
        let expected = if roster.is_empty() && self.storage.last_applied_index().await == 0 {
            self.genesis_members.get(&sender_id).copied()
        } else {
            roster
                .get(&sender_id)
                .and_then(|endpoint| endpoint.parse::<SocketAddr>().ok())
        }
        .ok_or_else(|| Status::permission_denied("member is not in the committed roster"))?;
        let source = request
            .remote_addr()
            .ok_or_else(|| Status::permission_denied("member endpoint unavailable"))?;
        if source.ip() != expected.ip() {
            return Err(Status::permission_denied(
                "member endpoint does not match roster",
            ));
        }
        Ok(())
    }

    async fn read_barrier(&self) -> Result<(), Status> {
        self.require_leader()?;
        linearizable_read(&self.raft).await.map_err(status_error)
    }

    fn require_leader(&self) -> Result<(), Status> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state == ServerState::Leader {
            return Ok(());
        }
        let mut status = Status::unavailable("not leader; retry with the elected leader");
        if let Some(leader) = metrics.current_leader {
            if let (Some(node), Some((configured_addr, server_name))) = (
                metrics.membership_config.membership().get_node(&leader),
                self.network.peer(leader),
            ) {
                if let Ok(endpoint) = node.addr.parse::<SocketAddr>() {
                    if configured_addr == endpoint {
                        status.metadata_mut().insert(
                            "graphrun-leader-endpoint",
                            format!("https://{endpoint}")
                                .parse()
                                .expect("socket address is ASCII"),
                        );
                        status.metadata_mut().insert(
                            "graphrun-leader-server-name",
                            server_name.parse().expect("configured DNS name is ASCII"),
                        );
                    }
                }
            }
        }
        Err(status)
    }

    async fn publication_write(
        &self,
        auth: &crate::publication::AuthContext,
        id: CommandId,
        operation: crate::publication::PublicationOperation,
    ) -> Result<crate::publication::CommandResult, Status> {
        let command =
            crate::publication::command(auth, id, now(), operation).map_err(status_error)?;
        let reply = write_raft_response(&self.raft, &self.storage, command)
            .await
            .map_err(status_error)?;
        let receipt = reply.command_result.ok_or_else(|| {
            Status::unavailable(format!(
                "unknown outcome for command {id}; query or retry the same identity"
            ))
        })?;
        receipt.ensure_applied().map_err(status_error)?;
        Ok(receipt)
    }

    async fn worker_session<T>(
        &self,
        request: &Request<T>,
        session: WorkerSessionId,
    ) -> Result<(), Status> {
        let peer = self.verified_peer(request)?;
        peer.require_role(crate::tls::PeerRole::Worker)
            .map_err(status_error)?;
        self.require_leader()?;
        let metrics = self.raft.metrics().borrow().clone();
        let state = self.storage.query_state().await;
        let current = state.sessions.get(&session).ok_or_else(|| {
            if metrics.last_log_index.unwrap_or(0)
                > metrics.last_applied.map(|id| id.index).unwrap_or(0)
            {
                Status::unavailable("worker session apply is pending")
            } else {
                Status::unauthenticated("unknown worker session")
            }
        })?;
        if current.principal_id.is_empty() || current.principal_id != peer.principal_id().as_str() {
            return Err(Status::permission_denied(
                "worker session belongs to another principal",
            ));
        }
        if now().as_millis() >= current.expires_ms {
            return Err(Status::failed_precondition("worker session expired"));
        }
        Ok(())
    }
}

fn status_error(err: crate::error::Error) -> Status {
    use crate::error::ErrorKind;
    match err.kind {
        ErrorKind::InvalidArgument => Status::invalid_argument(err.to_string()),
        ErrorKind::AlreadyExists => Status::already_exists(err.to_string()),
        ErrorKind::FailedPrecondition => Status::failed_precondition(err.to_string()),
        ErrorKind::Unauthenticated => Status::unauthenticated(err.to_string()),
        ErrorKind::PermissionDenied => Status::permission_denied(err.to_string()),
        ErrorKind::NotFound => Status::not_found(err.to_string()),
        ErrorKind::Unavailable => Status::unavailable(err.to_string()),
        ErrorKind::ResourceExhausted => Status::resource_exhausted(err.to_string()),
        ErrorKind::DeadlineExceeded => Status::deadline_exceeded(err.to_string()),
    }
}

fn receipt_response(
    receipt: &crate::publication::CommandResult,
) -> Result<CommandResultResponse, Status> {
    Ok(CommandResultResponse {
        result_json: serde_json::to_vec(receipt)
            .map_err(|err| Status::internal(err.to_string()))?,
    })
}

fn worker_duplicate(
    state: &crate::domain::State,
    id: CommandId,
    body: &CommandBody,
) -> Result<bool, Status> {
    if !state.commands.contains_key(&id) {
        return Ok(false);
    }
    let actual = crate::domain::worker_request_digest(body).map_err(status_error)?;
    if state.worker_command_digests.get(&id).map(String::as_str) != actual.as_deref() {
        return Err(Status::already_exists(
            "worker command ID reused with different request",
        ));
    }
    Ok(true)
}

#[tonic::async_trait]
impl RaftSvc for GraphServices {
    async fn append_entries(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        self.member_context(&request, request.get_ref().sender_id)
            .await?;
        let rpc: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(&request.get_ref().json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if rpc.vote.leader_id.voted_for() != Some(request.get_ref().sender_id) {
            return Err(Status::permission_denied(
                "Raft leader differs from signed member",
            ));
        }
        if !rpc.entries.is_empty() {
            let metrics = self.raft.metrics().borrow().clone();
            let last_log = metrics.last_log_index.unwrap_or(0);
            let applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);
            let incoming = rpc.entries.len() as u64;
            let incoming_bytes: u64 = rpc
                .entries
                .iter()
                .map(|entry| {
                    serde_json::to_vec(entry)
                        .map(|bytes| bytes.len() as u64)
                        .unwrap_or(0)
                })
                .sum();
            let pending = last_log.saturating_sub(applied);
            admit_unapplied(
                pending.saturating_add(incoming),
                pending
                    .saturating_add(1)
                    .saturating_mul(incoming_bytes.max(1)),
            )
            .map_err(|err| Status::resource_exhausted(err.to_string()))?;
        }
        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(Blob {
            json: serde_json::to_vec(&resp).map_err(|err| Status::internal(err.to_string()))?,
            sender_id: self.node_id,
        }))
    }

    async fn vote(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        self.member_context(&request, request.get_ref().sender_id)
            .await?;
        let rpc: openraft::raft::VoteRequest<u64> = serde_json::from_slice(&request.get_ref().json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if rpc.vote.leader_id.voted_for() != Some(request.get_ref().sender_id) {
            return Err(Status::permission_denied(
                "Raft candidate differs from signed member",
            ));
        }
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(Blob {
            json: serde_json::to_vec(&resp).map_err(|err| Status::internal(err.to_string()))?,
            sender_id: self.node_id,
        }))
    }

    async fn install_snapshot(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        self.member_context(&request, request.get_ref().sender_id)
            .await?;
        let rpc: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&request.get_ref().json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if rpc.vote.leader_id.voted_for() != Some(request.get_ref().sender_id) {
            return Err(Status::permission_denied(
                "snapshot leader differs from signed member",
            ));
        }
        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(Blob {
            json: serde_json::to_vec(&resp).map_err(|err| Status::internal(err.to_string()))?,
            sender_id: self.node_id,
        }))
    }

    async fn clock_health(
        &self,
        request: Request<ClockHealthRequest>,
    ) -> Result<Response<ClockHealthResponse>, Status> {
        self.member_context(&request, request.get_ref().sender_id)
            .await?;
        self.storage
            .clock()
            .sample(&self.storage)
            .await
            .map_err(status_error)?;
        Ok(Response::new(ClockHealthResponse {
            member_id: self.node_id,
            wall_ms: crate::write::now().as_millis(),
        }))
    }
}

#[tonic::async_trait]
impl Client for GraphServices {
    async fn publish_catalog(
        &self,
        request: Request<PublishCatalogRequest>,
    ) -> Result<Response<CommandResultResponse>, Status> {
        let auth = self.authenticated_context(&request, crate::tls::PeerRole::Admin)?;
        self.require_leader()?;
        let req = request.into_inner();
        let catalog: Catalog = serde_json::from_slice(&req.catalog_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let receipt = self
            .publication_write(
                &auth,
                parse_publication_command_id(&req.command_id)?,
                crate::publication::PublicationOperation::Catalog {
                    version: req.version,
                    catalog,
                },
            )
            .await?;
        Ok(Response::new(receipt_response(&receipt)?))
    }

    async fn publish_definition(
        &self,
        request: Request<PublishDefinitionRequest>,
    ) -> Result<Response<CommandResultResponse>, Status> {
        let auth = self.authenticated_context(&request, crate::tls::PeerRole::Admin)?;
        self.require_leader()?;
        self.read_barrier().await?;
        let req = request.into_inner();
        let state = self.storage.query_state().await;
        let catalog = state
            .published_catalogs
            .get(&req.catalog_version)
            .ok_or_else(|| Status::not_found("published catalog not found"))?;
        catalog.verify().map_err(status_error)?;
        let definition = compile_yaml(&req.yaml, &catalog.catalog)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let receipt = self
            .publication_write(
                &auth,
                parse_publication_command_id(&req.command_id)?,
                crate::publication::PublicationOperation::Definition {
                    definition: Box::new(definition),
                    catalog_version: req.catalog_version,
                },
            )
            .await?;
        Ok(Response::new(receipt_response(&receipt)?))
    }

    async fn get_command_result(
        &self,
        request: Request<CommandResultRequest>,
    ) -> Result<Response<CommandResultResponse>, Status> {
        let auth = self.query_context(&request)?;
        let id = parse_publication_command_id(&request.into_inner().command_id)?;
        self.read_barrier().await?;
        let state = self.storage.query_state().await;
        let receipt = state
            .command_results
            .get(&auth.key(id).storage_key())
            .ok_or_else(|| Status::not_found("command result not found"))?;
        receipt.ensure_format().map_err(status_error)?;
        Ok(Response::new(receipt_response(receipt)?))
    }

    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let auth = self.authenticated_context(&request, crate::tls::PeerRole::Client)?;
        self.require_leader()?;
        let req = request.into_inner();
        if !req.workflow.is_empty() {
            if !req.yaml.is_empty() || !req.catalog_json.is_empty() {
                return Err(Status::invalid_argument(
                    "published start cannot include inline source/catalog",
                ));
            }
            let input: Value = serde_json::from_slice(&req.input_json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            let receipt = self
                .publication_write(
                    &auth,
                    parse_publication_command_id(&req.command_id)?,
                    crate::publication::PublicationOperation::Start {
                        workflow: req.workflow,
                        version: (req.version != 0).then_some(req.version),
                        start_key: req.start_key,
                        input,
                    },
                )
                .await?;
            self.notify.notify_one();
            return Ok(Response::new(StartResponse {
                run_id: receipt.applied_run().map_err(status_error)?.to_hex(),
                error: String::new(),
                command_result_json: serde_json::to_vec(&receipt)
                    .map_err(|err| Status::internal(err.to_string()))?,
            }));
        }
        let catalog: Catalog = serde_json::from_slice(&req.catalog_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let input: Value = serde_json::from_slice(&req.input_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let definition = compile_yaml(&req.yaml, &catalog)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let run = RunId::generate();
        let command_id = parse_command_id(&req.command_id)?;
        let result = write_raft_response(
            &self.raft,
            &self.storage,
            Command {
                id: command_id,
                time: now(),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition),
                    input,
                    catalog: Box::new(catalog),
                },
            },
        )
        .await;
        self.notify.notify_one();
        match result {
            Ok(reply) if reply.error.is_none() => Ok(Response::new(StartResponse {
                run_id: reply
                    .run_id
                    .ok_or_else(|| Status::internal("missing committed run identity"))?
                    .to_hex(),
                error: String::new(),
                command_result_json: Vec::new(),
            })),
            Ok(reply) => {
                let error = reply.error.unwrap_or_default();
                if error.starts_with("AlreadyExists") {
                    Err(Status::already_exists(error))
                } else {
                    Err(Status::failed_precondition(error))
                }
            }
            Err(err) => Err(status_error(err)),
        }
    }

    async fn signal(&self, request: Request<SignalRequest>) -> Result<Response<Ack>, Status> {
        self.query_context(&request)?;
        self.require_leader()?;
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        let event_id = EventId::from_hex(&req.event_id).map_err(Status::invalid_argument)?;
        let payload: Value = serde_json::from_slice(&req.payload_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        ack(write_raft(
            &self.raft,
            &self.storage,
            Command {
                id: parse_command_id(&req.command_id)?,
                time: now(),
                body: CommandBody::Signal {
                    run,
                    event_id,
                    signal: req.name,
                    key: req.key,
                    payload,
                },
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }

    async fn cancel(&self, request: Request<CancelRequest>) -> Result<Response<Ack>, Status> {
        self.query_context(&request)?;
        self.require_leader()?;
        let req = request.into_inner();
        ack(write_raft(
            &self.raft,
            &self.storage,
            Command {
                id: parse_command_id(&req.command_id)?,
                time: now(),
                body: CommandBody::Cancel {
                    run: parse_run(&req.run_id)?,
                    reason: req.reason,
                },
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }

    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectResponse>, Status> {
        self.query_context(&request)?;
        self.read_barrier().await?;
        let run = parse_run(&request.into_inner().run_id)?;
        let state = self.storage.query_state().await;
        if let Some(summary) = state.terminal_summaries.get(&run) {
            return Ok(Response::new(InspectResponse {
                view_json: serde_json::to_vec(&crate::history::summary_view(summary))
                    .map_err(|err| Status::internal(err.to_string()))?,
                error: String::new(),
            }));
        }
        let Some(run_state) = state.runs.get(&run) else {
            return Err(Status::not_found("unknown run"));
        };
        if let Some(pinned) = &run_state.published {
            pinned
                .verify(&run_state.definition, &run_state.catalog)
                .map_err(status_error)?;
        }
        let view = inspect_view(&state, run);
        Ok(Response::new(InspectResponse {
            view_json: serde_json::to_vec(&view)
                .map_err(|err| Status::internal(err.to_string()))?,
            error: String::new(),
        }))
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        self.query_context(&request)?;
        self.read_barrier().await?;
        let state = self.storage.query_state().await;
        let runs: Vec<_> = state
            .runs
            .values()
            .map(|run| {
                serde_json::json!({
                    "run": run.id.to_hex(),
                    "definition": run.definition.id,
                })
            })
            .collect();
        let mut runs = runs;
        runs.extend(
            state
                .terminal_summaries
                .values()
                .map(crate::history::summary_view),
        );
        Ok(Response::new(ListResponse {
            runs_json: serde_json::to_vec(&runs)
                .map_err(|err| Status::internal(err.to_string()))?,
        }))
    }

    async fn history(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<HistoryResponse>, Status> {
        self.query_context(&request)?;
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        self.read_barrier().await?;
        let state = self.storage.query_state().await;
        let page = crate::history::page(&state, run, req.after_sequence, req.page_size, now())
            .map_err(status_error)?;
        Ok(Response::new(HistoryResponse {
            events_json: serde_json::to_vec(&page)
                .map_err(|err| Status::internal(err.to_string()))?,
            error: String::new(),
        }))
    }

    async fn replay(
        &self,
        request: Request<ReplayRequest>,
    ) -> Result<Response<ReplayResponse>, Status> {
        self.query_context(&request)?;
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        self.read_barrier().await?;
        let state = self.storage.query_state().await;
        let projection = crate::history::reconstruct_at(&state, run, req.through_sequence, now())
            .map_err(status_error)?;
        Ok(Response::new(ReplayResponse {
            view_json: serde_json::to_vec(&inspect_view(&projection, run))
                .map_err(|err| Status::internal(err.to_string()))?,
            error: String::new(),
        }))
    }

    async fn acknowledge_clock(
        &self,
        request: Request<ClockAcknowledgeRequest>,
    ) -> Result<Response<Ack>, Status> {
        self.authenticated_context(&request, crate::tls::PeerRole::Admin)?;
        self.storage
            .clock()
            .acknowledge(&self.storage, &request.into_inner().reason)
            .await
            .map_err(status_error)?;
        Ok(Response::new(Ack {
            error: String::new(),
        }))
    }
}

#[tonic::async_trait]
impl WorkerSvc for GraphServices {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let peer = self.verified_peer(&request)?;
        peer.require_role(crate::tls::PeerRole::Worker)
            .map_err(status_error)?;
        self.require_leader()?;
        let req = request.into_inner();
        if req.principal_id != peer.principal_id().as_str() {
            return Err(Status::permission_denied(
                "worker principal does not match signed certificate",
            ));
        }
        let capabilities: Vec<WorkerCapability> = req
            .capabilities
            .into_iter()
            .map(WorkerCapability::from_wire)
            .collect::<crate::error::Result<_>>()
            .map_err(status_error)?;
        let session =
            WorkerSessionId::from_hex(&req.session_id).map_err(Status::invalid_argument)?;
        let id = parse_publication_command_id(&req.command_id)?;
        write_raft(
            &self.raft,
            &self.storage,
            Command {
                id,
                time: now(),
                body: CommandBody::RegisterWorker {
                    session,
                    principal_id: peer.principal_id().as_str().to_owned(),
                    capabilities,
                    capacity: req.capacity,
                    protocol_min: req.protocol_min,
                    protocol_max: req.protocol_max,
                },
            },
        )
        .await
        .map_err(status_error)?;
        let state = self.storage.query_state().await;
        let events = state
            .commands
            .get(&id)
            .ok_or_else(|| Status::internal("registration receipt unavailable"))?;
        let expiry = events
            .iter()
            .find_map(|event| {
                if let DomainEvent::WorkerRegistered {
                    session: registered,
                    expires_ms,
                    ..
                } = event
                {
                    (*registered == session).then_some(*expires_ms)
                } else {
                    None
                }
            })
            .ok_or_else(|| Status::failed_precondition("registration not applied"))?;
        Ok(Response::new(RegisterResponse {
            error: String::new(),
            revision: 1,
            lease_expiry_ms: expiry,
        }))
    }

    async fn renew_session(
        &self,
        request: Request<RenewSessionRequest>,
    ) -> Result<Response<RenewSessionResponse>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let id = parse_publication_command_id(&req.command_id)?;
        write_raft(
            &self.raft,
            &self.storage,
            Command {
                id,
                time: now(),
                body: CommandBody::RenewWorkerSession {
                    session,
                    revision: LeaseRevision::new(req.revision),
                },
            },
        )
        .await
        .map_err(status_error)?;
        let state = self.storage.query_state().await;
        let events = state
            .commands
            .get(&id)
            .ok_or_else(|| Status::internal("session renewal receipt unavailable"))?;
        let (revision, lease_expiry_ms) = events
            .iter()
            .find_map(|event| {
                if let DomainEvent::WorkerSessionRenewed {
                    revision,
                    expires_ms,
                    ..
                } = event
                {
                    Some((revision.get(), *expires_ms))
                } else {
                    None
                }
            })
            .ok_or_else(|| Status::failed_precondition("session renewal not applied"))?;
        let worker = state
            .sessions
            .get(&session)
            .ok_or_else(|| Status::failed_precondition("session retired"))?;
        if worker.revision != revision
            || worker.expires_ms != lease_expiry_ms
            || now().as_millis() >= lease_expiry_ms
        {
            return Err(Status::failed_precondition(
                "cached session renewal is no longer current",
            ));
        }
        Ok(Response::new(RenewSessionResponse {
            revision,
            lease_expiry_ms,
        }))
    }

    async fn watch_ready(
        &self,
        request: Request<WatchReadyRequest>,
    ) -> Result<Response<WatchReadyResponse>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let mut metrics = self.raft.metrics();
        let previous = (req.generation, req.cursor);
        loop {
            let observed = metrics.borrow().clone();
            let generation = observed.current_term;
            let cursor = observed.last_applied.map(|id| id.index).unwrap_or(0);
            let state = self.storage.query_state().await;
            let ready = worker_has_ready(&state, session, now()).map_err(status_error)?;
            let resync =
                req.generation != 0 && (generation != req.generation || cursor != req.cursor);
            if ready || resync || (generation, cursor) != previous {
                return Ok(Response::new(WatchReadyResponse {
                    generation,
                    cursor,
                    ready,
                    resync,
                }));
            }
            if tokio::time::timeout(std::time::Duration::from_secs(15), metrics.changed())
                .await
                .is_err()
            {
                return Ok(Response::new(WatchReadyResponse {
                    generation,
                    cursor,
                    ready: false,
                    resync: false,
                }));
            }
        }
    }

    async fn claim(
        &self,
        request: Request<ClaimRequest>,
    ) -> Result<Response<ClaimResponse>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let command = Command {
            id: parse_command_id(&req.command_id)?,
            time: now(),
            body: CommandBody::Claim {
                session,
                capacity: req.capacity,
            },
        };
        write_raft(&self.raft, &self.storage, command.clone())
            .await
            .map_err(status_error)?;
        self.notify.notify_one();
        let state = self.storage.query_state().await;
        let events = state
            .commands
            .get(&command.id)
            .ok_or_else(|| Status::internal("claim receipt unavailable"))?;
        let assignments = assignments_from(&state, events)
            .map_err(status_error)?
            .into_iter()
            .map(|item| -> Result<crate::generated::Assignment, Status> {
                Ok(crate::generated::Assignment {
                    run_id: item.run.to_hex(),
                    activation_id: item.activation.to_hex(),
                    activity_name: item.activity_name,
                    activity_version: item.activity_version,
                    input_json: serde_json::to_vec(&item.input)
                        .map_err(|err| Status::internal(err.to_string()))?,
                    role: role_name(item.role).to_owned(),
                    effect_key: item.effect_key.to_hex(),
                    generation: item.generation.get(),
                    revision: item.revision.get(),
                    lease_expiry_ms: item.lease_expiry_ms,
                    attempt_deadline_ms: item.attempt_deadline_ms,
                    session_id: item.session.to_hex(),
                    codec_version: item.capability.codec_version,
                    input_schema_digest: item.capability.input_schema_digest,
                    output_schema_digest: item.capability.output_schema_digest,
                    contract_digest: item.capability.contract_digest,
                    attempt: item.attempt,
                    scope_id: item.scope.to_hex(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ClaimResponse {
            assignments,
            error: String::new(),
        }))
    }

    async fn renew(
        &self,
        request: Request<RenewRequest>,
    ) -> Result<Response<RenewResponse>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let activation =
            ActivationId::from_hex(&req.activation_id).map_err(Status::invalid_argument)?;
        let command = Command {
            id: parse_command_id(&req.command_id)?,
            time: now(),
            body: CommandBody::Renew {
                session,
                activation,
                generation: OwnerGeneration::new(req.generation),
                revision: LeaseRevision::new(req.revision),
            },
        };
        match write_raft(&self.raft, &self.storage, command.clone()).await {
            Ok(()) => {
                let state = self.storage.query_state().await;
                let events = state
                    .commands
                    .get(&command.id)
                    .ok_or_else(|| Status::internal("claim renewal receipt unavailable"))?;
                let claim = events
                    .iter()
                    .find_map(|event| {
                        if let DomainEvent::ClaimRenewed {
                            revision,
                            lease_expiry_ms,
                            ..
                        } = event
                        {
                            Some((revision.get(), *lease_expiry_ms))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| Status::failed_precondition("claim renewal not applied"))?;
                let current = state
                    .activations
                    .get(&activation)
                    .and_then(|act| act.claim.as_ref())
                    .ok_or_else(|| Status::failed_precondition("claim is no longer current"))?;
                if current.session != session
                    || current.generation != OwnerGeneration::new(req.generation)
                    || current.revision.get() != claim.0
                    || current.lease_expiry_ms != claim.1
                    || now().as_millis() >= current.lease_expiry_ms
                    || now().as_millis() >= current.attempt_deadline_ms
                {
                    return Err(Status::failed_precondition(
                        "cached claim renewal is no longer current",
                    ));
                }
                Ok(Response::new(RenewResponse {
                    revision: claim.0,
                    lease_expiry_ms: claim.1,
                    error: String::new(),
                }))
            }
            Err(err)
                if matches!(
                    err.kind,
                    ErrorKind::Unavailable | ErrorKind::DeadlineExceeded
                ) =>
            {
                Err(status_error(err))
            }
            Err(err) => Ok(Response::new(RenewResponse {
                revision: req.revision,
                lease_expiry_ms: 0,
                error: err.to_string(),
            })),
        }
    }

    async fn report(&self, request: Request<ReportRequest>) -> Result<Response<Ack>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        let activation =
            ActivationId::from_hex(&req.activation_id).map_err(Status::invalid_argument)?;
        if req.error_code.is_empty() && req.output_json.is_empty() {
            return Err(Status::invalid_argument(
                "report requires output or a typed error",
            ));
        }
        if !req.error_code.is_empty()
            && (!req.output_json.is_empty() || req.error_message.is_empty())
        {
            return Err(Status::invalid_argument(
                "error report requires a message and no output",
            ));
        }
        let result = if req.error_code.is_empty() {
            crate::domain::WorkerResult::Success {
                output: serde_json::from_slice(&req.output_json)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            }
        } else {
            crate::domain::WorkerResult::Error {
                code: req.error_code,
                message: req.error_message,
            }
        };
        let body = CommandBody::ReportWorker {
            run,
            activation,
            session,
            generation: OwnerGeneration::new(req.generation),
            revision: LeaseRevision::new(req.revision),
            schema_digest: req.output_schema_digest.clone(),
            result,
        };
        let id = parse_publication_command_id(&req.command_id)?;
        let state = self.storage.query_state().await;
        if worker_duplicate(&state, id, &body)? {
            return ack(Ok(()));
        }
        let act = state
            .activations
            .get(&activation)
            .ok_or_else(|| Status::not_found("unknown activation"))?;
        if act.run != run {
            return Err(Status::failed_precondition(
                "activation belongs to another run",
            ));
        }
        let (name, version, _) = crate::domain::activity_key(&state, activation)
            .ok_or_else(|| Status::failed_precondition("claimed activity unavailable"))?;
        let role = act
            .claim
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("no claim"))?
            .role;
        let capability = crate::worker_contract::capability_for(
            &state
                .runs
                .get(&run)
                .ok_or_else(|| Status::not_found("unknown run"))?
                .catalog,
            &crate::ids::ActivityKey::new(name, version),
            role,
        )
        .map_err(status_error)?;
        if req.output_schema_digest != capability.output_schema_digest {
            return Err(Status::failed_precondition("output schema digest mismatch"));
        }
        ack(write_raft(
            &self.raft,
            &self.storage,
            Command {
                id,
                time: now(),
                body,
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }

    async fn reconcile(&self, request: Request<ReconcileRequest>) -> Result<Response<Ack>, Status> {
        let session = WorkerSessionId::from_hex(&request.get_ref().session_id)
            .map_err(Status::invalid_argument)?;
        self.worker_session(&request, session).await?;
        let req = request.into_inner();
        let outcome = match req.outcome.as_str() {
            "applied" => crate::domain::ReconcileOutcome::Applied,
            "not_applied" => crate::domain::ReconcileOutcome::NotApplied,
            "unknown" => crate::domain::ReconcileOutcome::Unknown,
            other => return Err(Status::invalid_argument(format!("unknown outcome {other}"))),
        };
        let output = if req.output_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice(&req.output_json)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            )
        };
        if !req.error_code.is_empty()
            && (outcome != crate::domain::ReconcileOutcome::Unknown
                || !req.output_json.is_empty()
                || req.error_message.is_empty())
        {
            return Err(Status::invalid_argument(
                "reconciliation failure requires unknown outcome, code and message",
            ));
        }
        if req.error_code.is_empty() && !req.error_message.is_empty() {
            return Err(Status::invalid_argument(
                "reconciliation message without code",
            ));
        }
        let run = parse_run(&req.run_id)?;
        let activation =
            ActivationId::from_hex(&req.activation_id).map_err(Status::invalid_argument)?;
        let result = if req.error_code.is_empty() {
            crate::domain::WorkerProbe::Observed { outcome, output }
        } else {
            crate::domain::WorkerProbe::Error {
                code: req.error_code,
                message: req.error_message,
            }
        };
        let body = CommandBody::ReconcileWorker {
            run,
            activation,
            session,
            generation: OwnerGeneration::new(req.generation),
            revision: LeaseRevision::new(req.revision),
            schema_digest: req.output_schema_digest.clone(),
            result,
        };
        let id = parse_publication_command_id(&req.command_id)?;
        let state = self.storage.query_state().await;
        if worker_duplicate(&state, id, &body)? {
            return ack(Ok(()));
        }
        let (name, version, _) = crate::domain::activity_key(&state, activation)
            .ok_or_else(|| Status::failed_precondition("reconciliation contract unavailable"))?;
        let capability = crate::worker_contract::capability_for(
            &state
                .runs
                .get(&run)
                .ok_or_else(|| Status::not_found("unknown run"))?
                .catalog,
            &crate::ids::ActivityKey::new(name, version),
            crate::ids::ExecutionRole::Reconciliation,
        )
        .map_err(status_error)?;
        if req.output_schema_digest != capability.output_schema_digest {
            return Err(Status::failed_precondition("output schema digest mismatch"));
        }
        ack(write_raft(
            &self.raft,
            &self.storage,
            Command {
                id,
                time: now(),
                body,
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }
}

pub async fn serve_grpc(
    listener: tokio::net::TcpListener,
    tls: TlsMaterial,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
    node_id: u64,
    genesis_members: BTreeMap<u64, SocketAddr>,
    network: ClusterNetwork,
) -> crate::error::Result<()> {
    crate::tls::install_provider();
    let svc = GraphServices::new(
        raft,
        storage,
        notify,
        tls.ca_pem.clone(),
        node_id,
        genesis_members,
        network,
    );
    let identity = Identity::from_pem(tls.cert_pem, tls.key_pem);
    let ca = Certificate::from_pem(tls.ca_pem);
    let tls_config = ServerTlsConfig::new().identity(identity).client_ca_root(ca);
    Server::builder()
        .tls_config(tls_config)
        .map_err(|err| crate::error::Error::invalid(err.to_string()))?
        .add_service(RaftServer::new(svc.clone()))
        .add_service(ClientServer::new(svc.clone()))
        .add_service(WorkerServer::new(svc))
        .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener))
        .await
        .map_err(|err| crate::error::Error::invalid(err.to_string()))
}

pub fn client_tls(tls: &TlsMaterial) -> crate::error::Result<tonic::transport::ClientTlsConfig> {
    crate::tls::install_provider();
    Ok(tonic::transport::ClientTlsConfig::new()
        .identity(Identity::from_pem(&tls.cert_pem, &tls.key_pem))
        .ca_certificate(Certificate::from_pem(&tls.ca_pem))
        .domain_name(tls.server_name.clone()))
}

fn parse_run(text: &str) -> Result<RunId, Status> {
    RunId::from_hex(text).map_err(Status::invalid_argument)
}

fn parse_command_id(text: &str) -> Result<CommandId, Status> {
    if text.is_empty() {
        return Ok(CommandId::generate());
    }
    CommandId::from_hex(text).map_err(Status::invalid_argument)
}

fn parse_publication_command_id(text: &str) -> Result<CommandId, Status> {
    let id = CommandId::from_hex(text).map_err(Status::invalid_argument)?;
    if id.as_bytes() == &[0; 16] {
        return Err(Status::invalid_argument("command ID must be nonempty"));
    }
    Ok(id)
}

fn ack(result: Result<(), crate::error::Error>) -> Result<Response<Ack>, Status> {
    match result {
        Ok(()) => Ok(Response::new(Ack {
            error: String::new(),
        })),
        Err(err)
            if matches!(
                err.kind,
                ErrorKind::Unavailable | ErrorKind::DeadlineExceeded
            ) =>
        {
            Err(status_error(err))
        }
        Err(err) => Ok(Response::new(Ack {
            error: err.to_string(),
        })),
    }
}
