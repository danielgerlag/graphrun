use crate::catalog::Catalog;
use crate::compiler::compile_yaml;
use crate::domain::{Command, CommandBody, assignments_from};
use crate::error::ErrorKind;
use crate::generated::client_server::{Client, ClientServer};
use crate::generated::raft_server::{Raft as RaftSvc, RaftServer};
use crate::generated::worker_server::{Worker as WorkerSvc, WorkerServer};
use crate::generated::{
    Ack, Blob, CancelRequest, ClaimRequest, ClaimResponse, CommandResultRequest,
    CommandResultResponse, HistoryRequest, HistoryResponse, InspectRequest, InspectResponse,
    ListRequest, ListResponse, PublishCatalogRequest, PublishDefinitionRequest, ReconcileRequest,
    RegisterRequest, RenewRequest, RenewResponse, ReplayRequest, ReplayResponse, ReportRequest,
    SignalRequest, StartRequest, StartResponse,
};
use crate::ids::{
    ActivationId, CommandId, EventId, LeaseRevision, OwnerGeneration, RunId, WorkerSessionId,
};
use crate::storage::{StorageHandle, TypeConfig};
use crate::tls::TlsMaterial;
use crate::value::Value;
use crate::write::{admit_unapplied, inspect_view, now, write_raft, write_raft_response};
use openraft::Raft;
use openraft::raft::AppendEntriesRequest;
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
}

impl GraphServices {
    pub fn new(
        raft: Raft<TypeConfig>,
        storage: StorageHandle,
        notify: Arc<Notify>,
        ca_pem: String,
    ) -> Self {
        use sha2::Digest;
        let cluster = hex::encode(&sha2::Sha256::digest(ca_pem.as_bytes())[..16]);
        Self {
            raft,
            storage,
            notify,
            publication_ca: Arc::new(ca_pem),
            publication_cluster: crate::tls::ClusterId::parse(cluster).expect("hex cluster id"),
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
        let peer = self.verified_peer(request)?;
        if !peer.roles().any(|role| role == crate::tls::PeerRole::Admin) {
            peer.require_role(crate::tls::PeerRole::Client)
                .map_err(status_error)?;
        }
        Ok(crate::publication::AuthContext::verified_peer(&peer))
    }

    async fn publication_write(
        &self,
        auth: &crate::publication::AuthContext,
        id: CommandId,
        operation: crate::publication::PublicationOperation,
    ) -> Result<crate::publication::CommandResult, Status> {
        let command =
            crate::publication::command(auth, id, now(), operation).map_err(status_error)?;
        let reply = write_raft_response(&self.raft, command)
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

#[tonic::async_trait]
impl RaftSvc for GraphServices {
    async fn append_entries(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        let rpc: AppendEntriesRequest<TypeConfig> =
            serde_json::from_slice(&request.into_inner().json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
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
        }))
    }

    async fn vote(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        let rpc = serde_json::from_slice(&request.into_inner().json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(Blob {
            json: serde_json::to_vec(&resp).map_err(|err| Status::internal(err.to_string()))?,
        }))
    }

    async fn install_snapshot(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        let rpc = serde_json::from_slice(&request.into_inner().json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(Blob {
            json: serde_json::to_vec(&resp).map_err(|err| Status::internal(err.to_string()))?,
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
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
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
        let auth = if !request.get_ref().workflow.is_empty() {
            Some(self.authenticated_context(&request, crate::tls::PeerRole::Client)?)
        } else {
            None
        };
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
                    auth.as_ref().expect("checked"),
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
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        let event_id = EventId::from_hex(&req.event_id).map_err(Status::invalid_argument)?;
        let payload: Value = serde_json::from_slice(&req.payload_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        ack(write_raft(
            &self.raft,
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
        let req = request.into_inner();
        ack(write_raft(
            &self.raft,
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
            return Ok(Response::new(InspectResponse {
                view_json: Vec::new(),
                error: "unknown run".to_owned(),
            }));
        };
        if let Some(pinned) = &run_state.published {
            pinned
                .verify(&run_state.definition, &run_state.catalog)
                .map_err(status_error)?;
        }
        let view = inspect_view(&state, run);
        Ok(Response::new(InspectResponse {
            view_json: serde_json::to_vec(&view).unwrap_or_default(),
            error: String::new(),
        }))
    }

    async fn list(&self, _request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
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
            runs_json: serde_json::to_vec(&runs).unwrap_or_default(),
        }))
    }

    async fn history(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<HistoryResponse>, Status> {
        self.query_context(&request)?;
        let req = request.into_inner();
        let run = parse_run(&req.run_id)?;
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
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
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        let state = self.storage.query_state().await;
        let projection = crate::history::reconstruct_at(&state, run, req.through_sequence, now())
            .map_err(status_error)?;
        Ok(Response::new(ReplayResponse {
            view_json: serde_json::to_vec(&inspect_view(&projection, run))
                .map_err(|err| Status::internal(err.to_string()))?,
            error: String::new(),
        }))
    }
}

#[tonic::async_trait]
impl WorkerSvc for GraphServices {
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let session =
            WorkerSessionId::from_hex(&req.session_id).map_err(Status::invalid_argument)?;
        ack(write_raft(
            &self.raft,
            Command {
                id: CommandId::generate(),
                time: now(),
                body: CommandBody::RegisterSession {
                    session,
                    activities: req.activities,
                    capacity: req.capacity,
                },
            },
        )
        .await)
    }

    async fn claim(
        &self,
        request: Request<ClaimRequest>,
    ) -> Result<Response<ClaimResponse>, Status> {
        let req = request.into_inner();
        let session =
            WorkerSessionId::from_hex(&req.session_id).map_err(Status::invalid_argument)?;
        let command = Command {
            id: parse_command_id(&req.command_id)?,
            time: now(),
            body: CommandBody::Claim {
                session,
                capacity: req.capacity,
            },
        };
        if let Err(err) = write_raft(&self.raft, command.clone()).await {
            return Ok(Response::new(ClaimResponse {
                assignments: Vec::new(),
                error: err.to_string(),
            }));
        }
        self.notify.notify_one();
        let state = self.storage.query_state().await;
        let events = state.commands.get(&command.id).cloned().unwrap_or_default();
        let assignments = assignments_from(&events)
            .into_iter()
            .map(|item| crate::generated::Assignment {
                run_id: item.run.to_hex(),
                activation_id: item.activation.to_hex(),
                activity_name: item.activity_name,
                activity_version: item.activity_version,
                input_json: serde_json::to_vec(&item.input).unwrap_or_default(),
                role: format!("{:?}", item.role).to_lowercase(),
                effect_key: item.effect_key.to_hex(),
                generation: item.generation.get(),
                revision: item.revision.get(),
                lease_expiry_ms: item.lease_expiry_ms,
                attempt_deadline_ms: item.attempt_deadline_ms,
                session_id: item.session.to_hex(),
            })
            .collect();
        Ok(Response::new(ClaimResponse {
            assignments,
            error: String::new(),
        }))
    }

    async fn renew(
        &self,
        request: Request<RenewRequest>,
    ) -> Result<Response<RenewResponse>, Status> {
        let req = request.into_inner();
        let session =
            WorkerSessionId::from_hex(&req.session_id).map_err(Status::invalid_argument)?;
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
        match write_raft(&self.raft, command.clone()).await {
            Ok(()) => {
                let state = self.storage.query_state().await;
                let claim = state
                    .activations
                    .get(&activation)
                    .and_then(|act| act.claim.clone());
                Ok(Response::new(RenewResponse {
                    revision: claim
                        .as_ref()
                        .map(|c| c.revision.get())
                        .unwrap_or(req.revision),
                    lease_expiry_ms: claim.as_ref().map(|c| c.lease_expiry_ms).unwrap_or(0),
                    error: String::new(),
                }))
            }
            Err(err) => Ok(Response::new(RenewResponse {
                revision: req.revision,
                lease_expiry_ms: 0,
                error: err.to_string(),
            })),
        }
    }

    async fn report(&self, request: Request<ReportRequest>) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let output: Value = serde_json::from_slice(&req.output_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        ack(write_raft(
            &self.raft,
            Command {
                id: parse_command_id(&req.command_id)?,
                time: now(),
                body: CommandBody::ReportAssigned {
                    run: parse_run(&req.run_id)?,
                    activation: ActivationId::from_hex(&req.activation_id)
                        .map_err(Status::invalid_argument)?,
                    output,
                    session: WorkerSessionId::from_hex(&req.session_id)
                        .map_err(Status::invalid_argument)?,
                    generation: OwnerGeneration::new(req.generation),
                    revision: LeaseRevision::new(req.revision),
                },
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }

    async fn reconcile(&self, request: Request<ReconcileRequest>) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let outcome = match req.outcome.as_str() {
            "applied" => crate::domain::ReconcileOutcome::Applied,
            "not_applied" => crate::domain::ReconcileOutcome::NotApplied,
            "unknown" => crate::domain::ReconcileOutcome::Unknown,
            other => {
                return Ok(Response::new(Ack {
                    error: format!("unknown outcome {other}"),
                }));
            }
        };
        let output = if req.output_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice(&req.output_json)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            )
        };
        ack(write_raft(
            &self.raft,
            Command {
                id: parse_command_id(&req.command_id)?,
                time: now(),
                body: CommandBody::Reconcile {
                    run: parse_run(&req.run_id)?,
                    activation: ActivationId::from_hex(&req.activation_id)
                        .map_err(Status::invalid_argument)?,
                    session: WorkerSessionId::from_hex(&req.session_id)
                        .map_err(Status::invalid_argument)?,
                    generation: OwnerGeneration::new(req.generation),
                    revision: LeaseRevision::new(req.revision),
                    outcome,
                    output,
                },
            },
        )
        .await
        .map(|_| self.notify.notify_one()))
    }
}

pub async fn serve_grpc(
    bind: SocketAddr,
    tls: TlsMaterial,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
) -> crate::error::Result<()> {
    crate::tls::install_provider();
    let svc = GraphServices::new(raft, storage, notify, tls.ca_pem.clone());
    let identity = Identity::from_pem(tls.cert_pem, tls.key_pem);
    let ca = Certificate::from_pem(tls.ca_pem);
    let tls_config = ServerTlsConfig::new().identity(identity).client_ca_root(ca);
    Server::builder()
        .tls_config(tls_config)
        .map_err(|err| crate::error::Error::invalid(err.to_string()))?
        .add_service(RaftServer::new(svc.clone()))
        .add_service(ClientServer::new(svc.clone()))
        .add_service(WorkerServer::new(svc))
        .serve(bind)
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
        Err(err) if err.kind == ErrorKind::FailedPrecondition => Ok(Response::new(Ack {
            error: err.to_string(),
        })),
        Err(err) => Ok(Response::new(Ack {
            error: err.to_string(),
        })),
    }
}
