use crate::catalog::{Catalog, ExecutionKind};
use crate::cluster::{ClusterNetwork, MemberConfig, serve_raft};
use crate::compiler::compile_yaml;
use crate::domain::{
    Command, CommandBody, ObligationStatus, RunStatus, State, active_runs, activity_key,
    ready_activations, reconstruct, run_events, run_output,
};
use crate::error::{Error, ErrorKind, Result};
use crate::ids::{CommandId, EventId, RunId};
use crate::ir::Definition;
use crate::storage::{LocalNetwork, RaftRequest, StorageHandle, TypeConfig, load_domain_readonly};
use crate::time::EngineTime;
use crate::value::Value;
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

pub struct Engine {
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    storage_thread: Mutex<Option<JoinHandle<()>>>,
    data_dir: PathBuf,
    notify: Arc<Notify>,
    worker: Option<tokio::task::JoinHandle<()>>,
    control: tokio::task::JoinHandle<()>,
    raft_server: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    Start {
        yaml: String,
        catalog: Catalog,
        input: Value,
        wait_ms: Option<u64>,
    },
    Signal {
        run: String,
        name: String,
        key: String,
        event_id: String,
        payload: Value,
    },
    Cancel {
        run: String,
        reason: String,
    },
    Inspect {
        run: String,
    },
    List,
    History {
        run: String,
    },
    Snapshot,
    Health,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub body: serde_json::Value,
}

impl Engine {
    pub async fn local(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(|err| Error::invalid(err.to_string()))?;
        write_identity(&data_dir)?;
        let db_path = data_dir.join("member.redb");
        let (storage, storage_thread) = StorageHandle::open(&db_path)?;
        let log_store = storage.log_store();
        let state_machine = storage.state_machine();
        let config = Config {
            cluster_name: "graphrun-local".to_owned(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            max_payload_entries: 4,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 1024,
            ..Config::default()
        }
        .validate()
        .map_err(|err| Error::invalid(err.to_string()))?;
        let raft =
            Raft::<TypeConfig>::new(1, Arc::new(config), LocalNetwork, log_store, state_machine)
                .await
                .map_err(|err| Error::invalid(err.to_string()))?;
        let members = BTreeMap::from([(1, BasicNode::new(""))]);
        match raft.initialize(members).await {
            Ok(()) => {}
            Err(err) => {
                let msg = err.to_string();
                if !msg.contains("NotAllowed") && !msg.contains("not allowed") {
                    return Err(Error::invalid(msg));
                }
            }
        }
        raft.wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "local member is leader")
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        let notify = Arc::new(Notify::new());
        let worker = tokio::spawn(worker_loop(raft.clone(), storage.clone(), notify.clone()));
        let sock = data_dir.join("control.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).map_err(|err| Error::invalid(err.to_string()))?;
        let control = tokio::spawn(control_loop(
            listener,
            raft.clone(),
            storage.clone(),
            notify.clone(),
        ));
        Ok(Self {
            raft,
            storage,
            storage_thread: Mutex::new(Some(storage_thread)),
            data_dir,
            notify,
            worker: Some(worker),
            control,
            raft_server: None,
        })
    }

    pub async fn member(config: MemberConfig) -> Result<Self> {
        crate::tls::install_provider();
        let data_dir = config.data_dir.clone();
        std::fs::create_dir_all(&data_dir).map_err(|err| Error::invalid(err.to_string()))?;
        write_identity(&data_dir)?;
        let db_path = data_dir.join("member.redb");
        let (storage, storage_thread) = StorageHandle::open(&db_path)?;
        let log_store = storage.log_store();
        let state_machine = storage.state_machine();
        let raft_config = Config {
            cluster_name: "graphrun".to_owned(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            max_payload_entries: 4,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 1024,
            ..Config::default()
        }
        .validate()
        .map_err(|err| Error::invalid(err.to_string()))?;
        let network = ClusterNetwork::new(config.peers.clone());
        let raft = Raft::<TypeConfig>::new(
            config.node_id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
        let raft_server = {
            let raft = raft.clone();
            let bind = config.bind;
            let tls = config.tls.clone();
            tokio::spawn(async move {
                let _ = serve_raft(bind, tls, raft).await;
            })
        };
        if config.initialize {
            let mut members =
                BTreeMap::from([(config.node_id, BasicNode::new(config.bind.to_string()))]);
            for (id, (addr, _)) in &config.peers {
                members.insert(*id, BasicNode::new(addr.to_string()));
            }
            match raft.initialize(members).await {
                Ok(()) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("NotAllowed") && !msg.contains("not allowed") {
                        return Err(Error::invalid(msg));
                    }
                }
            }
            raft.wait(Some(Duration::from_secs(10)))
                .state(ServerState::Leader, "member is leader")
                .await
                .map_err(|err| Error::invalid(err.to_string()))?;
        }
        let notify = Arc::new(Notify::new());
        let worker = if config.host_activities {
            Some(tokio::spawn(worker_loop(
                raft.clone(),
                storage.clone(),
                notify.clone(),
            )))
        } else {
            None
        };
        let sock = data_dir.join("control.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).map_err(|err| Error::invalid(err.to_string()))?;
        let control = tokio::spawn(control_loop(
            listener,
            raft.clone(),
            storage.clone(),
            notify.clone(),
        ));
        Ok(Self {
            raft,
            storage,
            storage_thread: Mutex::new(Some(storage_thread)),
            data_dir,
            notify,
            worker,
            control,
            raft_server: Some(raft_server),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn control_sock(&self) -> PathBuf {
        self.data_dir.join("control.sock")
    }

    pub async fn start(
        &self,
        definition: Definition,
        catalog: Catalog,
        input: Value,
    ) -> Result<RunId> {
        let run = RunId::generate();
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Start {
                run,
                definition: Box::new(definition),
                input,
                catalog: Box::new(catalog),
            },
        };
        self.write(command).await?;
        self.notify.notify_one();
        Ok(run)
    }

    pub async fn start_yaml(&self, yaml: &str, catalog: &Catalog, input: Value) -> Result<RunId> {
        let definition = compile_yaml(yaml, catalog)?;
        self.start(definition, catalog.clone(), input).await
    }

    pub async fn signal(
        &self,
        run: RunId,
        event_id: EventId,
        name: &str,
        key: &str,
        payload: Value,
    ) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Signal {
                run,
                event_id,
                signal: name.to_owned(),
                key: key.to_owned(),
                payload,
            },
        };
        self.write(command).await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn cancel(&self, run: RunId, reason: &str) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Cancel {
                run,
                reason: reason.to_owned(),
            },
        };
        self.write(command).await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn inspect(&self, run: RunId) -> Result<State> {
        let state = self.storage.query_state().await;
        if !state.runs.contains_key(&run) {
            return Err(Error::new(ErrorKind::NotFound, "unknown run"));
        }
        Ok(state)
    }

    pub async fn inspect_json(&self, run: RunId) -> Result<serde_json::Value> {
        let state = self.inspect(run).await?;
        Ok(inspect_view(&state, run))
    }

    pub async fn list(&self) -> serde_json::Value {
        let state = self.storage.query_state().await;
        let runs: Vec<_> = state
            .runs
            .values()
            .map(|run| {
                serde_json::json!({
                    "run": run.id.to_hex(),
                    "definition": run.definition.id,
                    "status": match run.status {
                        RunStatus::Active => "active",
                        RunStatus::Succeeded { .. } => "succeeded",
                        RunStatus::Failed { .. } => "failed",
                    },
                })
            })
            .collect();
        serde_json::json!({ "runs": runs })
    }

    pub async fn history(&self, run: RunId) -> Result<Vec<crate::domain::DomainEvent>> {
        let state = self.inspect(run).await?;
        Ok(run_events(&state, run).to_vec())
    }

    pub async fn snapshot(&self) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn wait_terminal(&self, run: RunId, timeout: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let state = self.storage.query_state().await;
            if let Some(output) = run_output(&state, run) {
                return Ok(output);
            }
            if let Some(run_state) = state.runs.get(&run) {
                if let RunStatus::Failed { error } = &run_state.status {
                    return Err(Error::invalid(format!(
                        "run failed: {} {}",
                        error.code, error.message
                    )));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::DeadlineExceeded,
                    "timed out waiting for run",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.progress(run).await?;
        }
    }

    pub async fn progress(&self, run: RunId) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Progress { run },
        };
        self.write(command).await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        self.control.abort();
        if let Some(server) = &self.raft_server {
            server.abort();
        }
        let _ = self.raft.shutdown().await;
        self.storage.shutdown();
        if let Some(thread) = self.storage_thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(self.control_sock());
        Ok(())
    }

    async fn write(&self, command: Command) -> Result<()> {
        write_raft(&self.raft, command).await
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        self.control.abort();
        if let Some(server) = &self.raft_server {
            server.abort();
        }
        self.storage.shutdown();
        if let Some(thread) = self.storage_thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(self.data_dir.join("control.sock"));
    }
}

pub fn replay(data_dir: impl AsRef<Path>) -> Result<State> {
    let db_path = data_dir.as_ref().join("member.redb");
    let state = load_domain_readonly(&db_path)?;
    for (run, events) in &state.history {
        let Some(run_state) = state.runs.get(run) else {
            continue;
        };
        let rebuilt = reconstruct(
            run_state.definition.clone(),
            run_state.catalog.clone(),
            events,
        )?;
        let live = run_output(&state, *run);
        let replayed = run_output(&rebuilt, *run);
        if live != replayed {
            return Err(Error::invalid(format!(
                "replay mismatch for run {}",
                run.to_hex()
            )));
        }
    }
    Ok(state)
}

pub async fn connect_control(
    sock: impl AsRef<Path>,
    req: ControlRequest,
) -> Result<ControlResponse> {
    let mut stream = UnixStream::connect(sock.as_ref())
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    let mut line = serde_json::to_string(&req).map_err(|err| Error::invalid(err.to_string()))?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    serde_json::from_str(&response).map_err(|err| Error::invalid(err.to_string()))
}

fn write_identity(dir: &Path) -> Result<()> {
    let path = dir.join("identity.json");
    if path.exists() {
        return Ok(());
    }
    let body = serde_json::json!({
        "mode": "local",
        "node_id": 1u64,
        "cluster_name": "graphrun-local",
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&body).unwrap())
        .map_err(|err| Error::invalid(err.to_string()))
}

fn now() -> EngineTime {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    EngineTime::from_millis(ms)
}

async fn write_raft(raft: &Raft<TypeConfig>, command: Command) -> Result<()> {
    let resp = raft
        .client_write(RaftRequest { command })
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    if let Some(err) = resp.data.error {
        return Err(Error::invalid(err));
    }
    Ok(())
}

async fn control_loop(
    listener: UnixListener,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let raft = raft.clone();
        let storage = storage.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            let _ = handle_control(stream, raft, storage, notify).await;
        });
    }
}

async fn handle_control(
    stream: UnixStream,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    let req: ControlRequest =
        serde_json::from_str(&line).map_err(|err| Error::invalid(err.to_string()))?;
    let resp = dispatch_control(req, &raft, &storage, &notify).await;
    let mut out = serde_json::to_string(&resp).unwrap();
    out.push('\n');
    let mut stream = reader.into_inner();
    stream
        .write_all(out.as_bytes())
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(())
}

async fn dispatch_control(
    req: ControlRequest,
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    notify: &Notify,
) -> ControlResponse {
    let result = match req {
        ControlRequest::Health => Ok(serde_json::json!({"status":"ok","mode":"local"})),
        ControlRequest::List => {
            let state = storage.query_state().await;
            let runs: Vec<_> = state
                .runs
                .values()
                .map(|run| {
                    serde_json::json!({
                        "run": run.id.to_hex(),
                        "definition": run.definition.id,
                        "status": match run.status {
                            RunStatus::Active => "active",
                            RunStatus::Succeeded { .. } => "succeeded",
                            RunStatus::Failed { .. } => "failed",
                        },
                    })
                })
                .collect();
            Ok(serde_json::json!({"runs": runs}))
        }
        ControlRequest::Start {
            yaml,
            catalog,
            input,
            wait_ms,
        } => match start_via_raft(raft, notify, &yaml, catalog, input).await {
            Ok(run) => {
                if let Some(ms) = wait_ms {
                    match wait_via_raft(raft, storage, notify, run, Duration::from_millis(ms)).await
                    {
                        Ok(output) => Ok(serde_json::json!({
                            "run": run.to_hex(),
                            "status": "succeeded",
                            "output": output,
                        })),
                        Err(err) => Ok(serde_json::json!({
                            "run": run.to_hex(),
                            "status": "pending",
                            "error": err.to_string(),
                        })),
                    }
                } else {
                    Ok(serde_json::json!({"run": run.to_hex(), "status": "started"}))
                }
            }
            Err(err) => Err(err),
        },
        ControlRequest::Signal {
            run,
            name,
            key,
            event_id,
            payload,
        } => {
            let parsed = parse_run_event(&run, &event_id);
            match parsed {
                Ok((run, event_id)) => write_raft(
                    raft,
                    Command {
                        id: CommandId::generate(),
                        time: now(),
                        body: CommandBody::Signal {
                            run,
                            event_id,
                            signal: name,
                            key,
                            payload,
                        },
                    },
                )
                .await
                .map(|_| {
                    notify.notify_one();
                    serde_json::json!({"status":"ok"})
                }),
                Err(err) => Err(err),
            }
        }
        ControlRequest::Cancel { run, reason } => match RunId::from_hex(&run) {
            Ok(run) => write_raft(
                raft,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::Cancel { run, reason },
                },
            )
            .await
            .map(|_| {
                notify.notify_one();
                serde_json::json!({"status":"ok"})
            }),
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Inspect { run } => match RunId::from_hex(&run) {
            Ok(run) => {
                let state = storage.query_state().await;
                if state.runs.contains_key(&run) {
                    Ok(inspect_view(&state, run))
                } else {
                    Err(Error::new(ErrorKind::NotFound, "unknown run"))
                }
            }
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::History { run } => match RunId::from_hex(&run) {
            Ok(run) => {
                let state = storage.query_state().await;
                match serde_json::to_value(run_events(&state, run)) {
                    Ok(body) => Ok(body),
                    Err(err) => Err(Error::invalid(err.to_string())),
                }
            }
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Snapshot => raft
            .trigger()
            .snapshot()
            .await
            .map(|_| serde_json::json!({"status":"ok"}))
            .map_err(|err| Error::invalid(err.to_string())),
    };
    match result {
        Ok(body) => ControlResponse {
            ok: true,
            error: None,
            body,
        },
        Err(err) => ControlResponse {
            ok: false,
            error: Some(err.to_string()),
            body: serde_json::Value::Null,
        },
    }
}

fn parse_run_event(run: &str, event_id: &str) -> Result<(RunId, EventId)> {
    let run = RunId::from_hex(run).map_err(Error::invalid)?;
    let event_id = EventId::from_hex(event_id).map_err(Error::invalid)?;
    Ok((run, event_id))
}

async fn start_via_raft(
    raft: &Raft<TypeConfig>,
    notify: &Notify,
    yaml: &str,
    catalog: Catalog,
    input: Value,
) -> Result<RunId> {
    let definition = compile_yaml(yaml, &catalog)?;
    let run = RunId::generate();
    write_raft(
        raft,
        Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Start {
                run,
                definition: Box::new(definition),
                input,
                catalog: Box::new(catalog),
            },
        },
    )
    .await?;
    notify.notify_one();
    Ok(run)
}

async fn wait_via_raft(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    notify: &Notify,
    run: RunId,
    timeout: Duration,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let state = storage.query_state().await;
        if let Some(output) = run_output(&state, run) {
            return Ok(output);
        }
        if let Some(run_state) = state.runs.get(&run) {
            if let RunStatus::Failed { error } = &run_state.status {
                return Err(Error::invalid(format!(
                    "run failed: {} {}",
                    error.code, error.message
                )));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::new(
                ErrorKind::DeadlineExceeded,
                "timed out waiting for run",
            ));
        }
        let _ = write_raft(
            raft,
            Command {
                id: CommandId::generate(),
                time: now(),
                body: CommandBody::Progress { run },
            },
        )
        .await;
        notify.notify_one();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn inspect_view(state: &State, run: RunId) -> serde_json::Value {
    let Some(run_state) = state.runs.get(&run) else {
        return serde_json::json!({"error": "unknown run"});
    };
    let waits: Vec<_> = state
        .waits
        .values()
        .filter(|wait| wait.run == run && wait.pending)
        .map(|wait| {
            serde_json::json!({
                "id": wait.id.to_hex(),
                "signal": wait.signal,
                "key": wait.key,
                "deadline_ms": wait.deadline_ms,
            })
        })
        .collect();
    let obligations: Vec<_> = state
        .obligations
        .iter()
        .filter(|item| item.run == run)
        .map(|item| {
            serde_json::json!({
                "forward": item.forward.to_hex(),
                "handler": item.handler,
                "status": match item.status {
                    ObligationStatus::Open => "open",
                    ObligationStatus::Compensating { .. } => "compensating",
                    ObligationStatus::Compensated => "compensated",
                    ObligationStatus::Released => "released",
                },
            })
        })
        .collect();
    serde_json::json!({
        "run": run.to_hex(),
        "definition": run_state.definition.id,
        "version": run_state.definition.version,
        "status": match &run_state.status {
            RunStatus::Active => "active",
            RunStatus::Succeeded { .. } => "succeeded",
            RunStatus::Failed { .. } => "failed",
        },
        "output": match &run_state.status {
            RunStatus::Succeeded { output } => Some(output.clone()),
            _ => None,
        },
        "error": match &run_state.status {
            RunStatus::Failed { error } => Some(serde_json::json!({
                "code": error.code,
                "message": error.message
            })),
            _ => None,
        },
        "pending_waits": waits,
        "obligations": obligations,
        "event_count": run_events(state, run).len(),
    })
}

async fn worker_loop(raft: Raft<TypeConfig>, storage: StorageHandle, notify: Arc<Notify>) {
    loop {
        notify.notified().await;
        let state = storage.query_state().await;
        for run in active_runs(&state) {
            let _ = write_raft(
                &raft,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::Progress { run },
                },
            )
            .await;
            let state = storage.query_state().await;
            for activation in ready_activations(&state, run) {
                let Some((name, version, input)) = activity_key(&state, activation) else {
                    continue;
                };
                let blocking = state
                    .runs
                    .get(&run)
                    .and_then(|run_state| {
                        run_state
                            .catalog
                            .activities
                            .get(&crate::ids::ActivityKey::new(&name, version))
                    })
                    .is_some_and(|contract| contract.execution == ExecutionKind::Blocking);
                let output = if blocking {
                    let name = name.clone();
                    let input = input.clone();
                    match tokio::task::spawn_blocking(move || builtin_handler(&name, &input)).await
                    {
                        Ok(Ok(output)) => output,
                        _ => continue,
                    }
                } else {
                    match builtin_handler(&name, &input) {
                        Ok(output) => output,
                        Err(_) => continue,
                    }
                };
                let _ = write_raft(
                    &raft,
                    Command {
                        id: CommandId::generate(),
                        time: now(),
                        body: CommandBody::ReportLeaf {
                            run,
                            activation,
                            output,
                        },
                    },
                )
                .await;
            }
        }
    }
}

pub fn builtin_handler(name: &str, input: &Value) -> Result<Value> {
    match name {
        "counter.increment" => {
            let Value::Object(fields) = input else {
                return Err(Error::invalid("counter input"));
            };
            let value = fields
                .get("value")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::invalid("counter.value"))?
                + 1;
            Ok(Value::Object(BTreeMap::from([(
                "value".to_owned(),
                Value::Int(value),
            )])))
        }
        "inventory.reserve" => {
            let Value::Object(fields) = input else {
                return Err(Error::invalid("order input"));
            };
            let mut out = fields.clone();
            out.insert(
                "reservation_id".to_owned(),
                Value::String("res-1".to_owned()),
            );
            Ok(Value::Object(out))
        }
        "payment.charge" => {
            let Value::Object(fields) = input else {
                return Err(Error::invalid("reserved input"));
            };
            Ok(Value::Object(BTreeMap::from([
                (
                    "order_id".to_owned(),
                    fields
                        .get("order_id")
                        .cloned()
                        .unwrap_or(Value::String(String::new())),
                ),
                (
                    "amount".to_owned(),
                    fields.get("amount").cloned().unwrap_or(Value::Int(0)),
                ),
                ("payment_id".to_owned(), Value::String("pay-1".to_owned())),
            ])))
        }
        "tax.quote" => {
            let Value::Object(fields) = input else {
                return Err(Error::invalid("order input"));
            };
            let amount = fields.get("amount").and_then(Value::as_i64).unwrap_or(0);
            Ok(Value::Object(BTreeMap::from([(
                "cents".to_owned(),
                Value::Int(amount / 10),
            )])))
        }
        "shipping.quote" => Ok(Value::Object(BTreeMap::from([(
            "cents".to_owned(),
            Value::Int(500),
        )]))),
        "remote.echo" | "test.gate" => Ok(input.clone()),
        "inventory.release" | "payment.refund" => Ok(Value::Null),
        _ => Ok(input.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Catalog {
        Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap()
    }

    fn order() -> Value {
        Value::Object(BTreeMap::from([
            ("order_id".to_owned(), Value::String("o1".to_owned())),
            ("amount".to_owned(), Value::Int(1000)),
        ]))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_sequence_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let run = {
            let engine = Engine::local(dir.path()).await.unwrap();
            let run = engine.start_yaml(yaml, &catalog(), order()).await.unwrap();
            let output = engine
                .wait_terminal(run, Duration::from_secs(10))
                .await
                .unwrap();
            let Value::Object(fields) = output else {
                panic!("receipt");
            };
            assert_eq!(fields.get("payment_id").unwrap().as_str(), Some("pay-1"));
            engine.shutdown().await.unwrap();
            run
        };
        let engine = Engine::local(dir.path()).await.unwrap();
        let state = engine.inspect(run).await.unwrap();
        let output = run_output(&state, run).expect("persisted run");
        let Value::Object(fields) = output else {
            panic!("receipt");
        };
        assert_eq!(fields.get("payment_id").unwrap().as_str(), Some("pay-1"));
        engine.shutdown().await.unwrap();
        let replayed = replay(dir.path()).unwrap();
        assert!(run_output(&replayed, run).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_local_owner_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let err = match Engine::local(dir.path()).await {
            Ok(_) => panic!("second owner must be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorKind::FailedPrecondition);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_wait_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = include_str!("../../docs/specs/v1/examples/events.yaml");
        let input = Value::Object(BTreeMap::from([(
            "key".to_owned(),
            Value::String("k1".to_owned()),
        )]));
        let run = {
            let engine = Engine::local(dir.path()).await.unwrap();
            let run = engine.start_yaml(yaml, &catalog(), input).await.unwrap();
            for _ in 0..50 {
                engine.progress(run).await.unwrap();
                let state = engine.inspect(run).await.unwrap();
                if state.waits.values().any(|wait| wait.pending) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            engine.shutdown().await.unwrap();
            run
        };
        let engine = Engine::local(dir.path()).await.unwrap();
        engine
            .signal(
                run,
                EventId::generate(),
                "approval",
                "k1",
                Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))])),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(output.pointer("/approved").unwrap(), &Value::Bool(true));
        engine.shutdown().await.unwrap();
    }
}
