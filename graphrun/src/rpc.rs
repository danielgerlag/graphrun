use crate::catalog::Catalog;
use crate::compiler::compile_yaml;
use crate::domain::{Command, CommandBody, assignments_from};
use crate::error::ErrorKind;
use crate::generated::client_server::{Client, ClientServer};
use crate::generated::raft_server::{Raft as RaftSvc, RaftServer};
use crate::generated::worker_server::{Worker as WorkerSvc, WorkerServer};
use crate::generated::{
    Ack, Blob, CancelRequest, ClaimRequest, ClaimResponse, HistoryRequest, HistoryResponse,
    InspectRequest, InspectResponse, ListRequest, ListResponse, ReconcileRequest, RegisterRequest,
    RenewRequest, RenewResponse, ReportRequest, SignalRequest, StartRequest, StartResponse,
};
use crate::ids::{
    ActivationId, CommandId, EventId, LeaseRevision, OwnerGeneration, RunId, WorkerSessionId,
};
use crate::storage::{StorageHandle, TypeConfig};
use crate::tls::TlsMaterial;
use crate::value::Value;
use crate::write::{inspect_view, now, write_raft};
use openraft::Raft;
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
}

impl GraphServices {
    pub fn new(raft: Raft<TypeConfig>, storage: StorageHandle, notify: Arc<Notify>) -> Self {
        Self {
            raft,
            storage,
            notify,
        }
    }
}

#[tonic::async_trait]
impl RaftSvc for GraphServices {
    async fn append_entries(&self, request: Request<Blob>) -> Result<Response<Blob>, Status> {
        let rpc = serde_json::from_slice(&request.into_inner().json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
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
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let req = request.into_inner();
        let catalog: Catalog = serde_json::from_slice(&req.catalog_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let input: Value = serde_json::from_slice(&req.input_json)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let definition = compile_yaml(&req.yaml, &catalog)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let run = RunId::generate();
        let command_id = parse_command_id(&req.command_id)?;
        let result = write_raft(
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
            Ok(()) => Ok(Response::new(StartResponse {
                run_id: run.to_hex(),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(StartResponse {
                run_id: run.to_hex(),
                error: err.to_string(),
            })),
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
        if !state.runs.contains_key(&run) {
            return Ok(Response::new(InspectResponse {
                view_json: Vec::new(),
                error: "unknown run".to_owned(),
            }));
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
        Ok(Response::new(ListResponse {
            runs_json: serde_json::to_vec(&runs).unwrap_or_default(),
        }))
    }

    async fn history(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<HistoryResponse>, Status> {
        let run = parse_run(&request.into_inner().run_id)?;
        let state = self.storage.query_state().await;
        let events = crate::domain::run_events(&state, run);
        Ok(Response::new(HistoryResponse {
            events_json: serde_json::to_vec(events).unwrap_or_default(),
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
        let assignments = assignments_from(&state, &events)
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

    async fn reconcile(
        &self,
        _request: Request<ReconcileRequest>,
    ) -> Result<Response<Ack>, Status> {
        Ok(Response::new(Ack {
            error: "reconciliation probes are not implemented".to_owned(),
        }))
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
    let identity = Identity::from_pem(tls.cert_pem, tls.key_pem);
    let ca = Certificate::from_pem(tls.ca_pem);
    let tls_config = ServerTlsConfig::new().identity(identity).client_ca_root(ca);
    let svc = GraphServices::new(raft, storage, notify);
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
