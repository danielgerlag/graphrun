use crate::catalog::{Catalog, ExecutionKind};
use crate::cluster::{ClusterNetwork, MemberConfig};
use crate::compiler::compile_yaml;
use crate::domain::{Command, CommandBody, RunStatus, State, run_output};
use crate::error::{Error, ErrorKind, Result};
use crate::handlers::Handlers;
use crate::ids::{CommandId, EventId, RunId};
use crate::ir::Definition;
use crate::limits;
use crate::rpc::serve_grpc;
use crate::snapshot_framing::{SnapshotManifest, copy_payload, verify_snapshot, write_snapshot};
use crate::storage::{
    LocalNetwork, ScheduleWake, StorageHandle, TypeConfig, load_domain_readonly,
    validate_history_store,
};
use crate::value::Value;
use crate::write::{
    health_view, inspect_view, linearizable_read, now, write_raft, write_raft_response,
};
use openraft::{BasicNode, ChangeMembers, Config, Raft, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, Semaphore};

fn wake(notify: &Notify) {
    notify.notify_waiters();
    notify.notify_one();
}

pub static BLOCKING_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
pub static SLOW_RELEASE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub struct LedgerEntry {
    pub physical: u32,
    pub output: Value,
}

static EFFECT_LEDGER: LazyLock<Mutex<HashMap<String, LedgerEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn ledger_apply(key: &str, compute: impl FnOnce() -> Value) -> Value {
    let mut guard = EFFECT_LEDGER.lock().unwrap();
    if let Some(row) = guard.get_mut(key) {
        row.physical = row.physical.saturating_add(1);
        return row.output.clone();
    }
    let output = compute();
    guard.insert(
        key.to_owned(),
        LedgerEntry {
            physical: 1,
            output: output.clone(),
        },
    );
    output
}

pub fn ledger_get(key: &str) -> Option<LedgerEntry> {
    EFFECT_LEDGER.lock().unwrap().get(key).cloned()
}

pub fn ledger_clear() {
    EFFECT_LEDGER.lock().unwrap().clear();
}

fn keyed_output(key: Option<&str>, kind: &str, compute: impl FnOnce() -> Value) -> Result<Value> {
    match key {
        Some(key) if !key.is_empty() => apply_keyed(key, kind, compute()),
        _ => Ok(compute()),
    }
}

fn apply_keyed(key: &str, kind: &str, output: Value) -> Result<Value> {
    if let Some(url) = crate::provider::provider_url() {
        let resp = crate::provider::apply_effect(&url, key, kind, &output)?;
        match resp.status {
            crate::provider::EffectStatus::Applied => Ok(resp.output.unwrap_or(output)),
            crate::provider::EffectStatus::Pending => Err(Error::new(
                crate::error::ErrorKind::Unavailable,
                "provider effect pending",
            )),
            crate::provider::EffectStatus::NotApplied => Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "provider effect not applied",
            )),
            crate::provider::EffectStatus::Unknown => Err(Error::new(
                crate::error::ErrorKind::Unavailable,
                "provider effect unknown",
            )),
        }
    } else {
        Ok(ledger_apply(key, || output))
    }
}

/// In-process workflow runtime.
///
/// Open with [`Engine::local`] on a data directory in your Tokio process.
/// There is no in-memory shortcut: commands still go through one-member Raft
/// onto a redb file in that directory.
pub struct Engine {
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    storage_thread: Mutex<Option<JoinHandle<()>>>,
    data_dir: PathBuf,
    notify: Arc<Notify>,
    worker: Option<tokio::task::JoinHandle<()>>,
    scheduler: Option<tokio::task::JoinHandle<()>>,
    control: tokio::task::JoinHandle<()>,
    raft_server: Option<tokio::task::JoinHandle<()>>,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
    clock_watchdog: tokio::task::JoinHandle<()>,
    cluster_net: Option<ClusterNetwork>,
    handlers: Handlers,
    publication_auth: crate::publication::AuthContext,
}

#[derive(Serialize, Deserialize)]
struct LogicalBackupManifest {
    format: String,
    kind: String,
    snapshot: String,
    sha256: String,
}

struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.0.display(), %error, "temporary backup file cleanup failed");
        }
    }
}

fn private_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::File::options();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Build a local engine and register activity handlers before it opens.
pub struct LocalBuilder {
    data_dir: PathBuf,
    handlers: Handlers,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    PublishCatalog {
        version: u32,
        catalog: Catalog,
        command_id: String,
    },
    PublishDefinition {
        yaml: String,
        catalog_version: u32,
        command_id: String,
    },
    StartPublished {
        workflow: String,
        version: Option<u32>,
        start_key: String,
        input: Value,
        command_id: String,
        wait_ms: Option<u64>,
    },
    CommandResult {
        command_id: String,
    },
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
        #[serde(default)]
        after_sequence: u64,
        #[serde(default)]
        page_size: u32,
    },
    Replay {
        run: String,
        through_sequence: u64,
    },
    Snapshot,
    Health,
    Acknowledge {
        reason: String,
    },
    AcknowledgeClock {
        reason: String,
    },
    ResolveBlocked {
        run: String,
        forward: String,
        input: Value,
    },
    Abandon {
        run: String,
        reason: String,
    },
    Join {
        node_id: u64,
        addr: String,
        ca_pem: String,
        cert_pem: String,
        key_pem: String,
        server_name: String,
    },
    Promote {
        node_id: u64,
    },
    Remove {
        node_id: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub body: serde_json::Value,
}

impl Engine {
    /// Build a local engine. Register handlers with [`LocalBuilder::activity`]
    /// before [`LocalBuilder::open`]. Call [`LocalBuilder::fixtures`] to enable
    /// the sample catalog names (`counter.increment`, `inventory.reserve`, …).
    pub fn builder(data_dir: impl AsRef<Path>) -> LocalBuilder {
        LocalBuilder {
            data_dir: data_dir.as_ref().to_path_buf(),
            handlers: Handlers::empty(),
        }
    }

    /// Open a local engine with built-in fixture handlers.
    ///
    /// For application handlers, use [`Engine::builder`] instead. Unregistered
    /// activity names fail; they do not echo input.
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let dir = tempfile::tempdir().unwrap();
    /// let engine = graphrun::Engine::local(dir.path()).await.unwrap();
    /// engine.shutdown().await.unwrap();
    /// # });
    /// ```
    pub async fn local(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::builder(data_dir).fixtures().open().await
    }
}

impl LocalBuilder {
    /// Enable built-in fixture handlers used by samples and tests.
    pub fn fixtures(self) -> Self {
        self.handlers.enable_fixtures();
        self
    }

    /// Register an async activity handler at version 1.
    pub fn activity<I, O, F, Fut>(self, name: &str, handler: F) -> Result<Self>
    where
        I: crate::schema::DurablePayload,
        O: crate::schema::DurablePayload,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<O>> + Send + 'static,
    {
        self.handlers.activity(name, handler)?;
        Ok(self)
    }

    /// Register a blocking activity handler at version 1.
    pub fn blocking<I, O, F>(self, name: &str, handler: F) -> Result<Self>
    where
        I: crate::schema::DurablePayload,
        O: crate::schema::DurablePayload,
        F: Fn(I) -> Result<O> + Send + Sync + 'static,
    {
        self.handlers.blocking(name, handler)?;
        Ok(self)
    }

    pub async fn open(self) -> Result<Engine> {
        let data_dir = self.data_dir;
        let handlers = self.handlers;
        std::fs::create_dir_all(&data_dir).map_err(|err| Error::invalid(err.to_string()))?;
        let db_path = data_dir.join("member.redb");
        let existing_identity = read_identity(&data_dir, None)?;
        let restored_path = data_dir.join("restored-domain.json");
        let restored_domain = if restored_path.exists() {
            if !existing_identity
                .as_ref()
                .is_some_and(|(_, restored)| *restored)
            {
                return Err(Error::new(
                    ErrorKind::FailedPrecondition,
                    "restored domain has no matching restored identity (directory left untouched)",
                ));
            }
            let bytes =
                std::fs::read(&restored_path).map_err(|err| Error::invalid(err.to_string()))?;
            let domain: State = serde_json::from_slice(&bytes)
                .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
            Some(domain)
        } else {
            None
        };
        let (storage, storage_thread) = if restored_domain.is_some() {
            StorageHandle::open_restored(&db_path)?
        } else {
            StorageHandle::open(&db_path)?
        };
        let publication_auth = match existing_identity {
            Some((auth, _)) => auth,
            None => write_identity(&data_dir, None)?,
        };
        if let Some(domain) = restored_domain {
            storage.install_domain(domain).await?;
            std::fs::remove_file(&restored_path).map_err(|err| Error::invalid(err.to_string()))?;
        }
        let log_store = storage.log_store();
        let state_machine = storage.state_machine();
        let config = Config {
            cluster_name: "graphrun-local".to_owned(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            max_payload_entries: 4,
            snapshot_max_chunk_size: 1024 * 1024,
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
        let scheduler = Some(tokio::spawn(scheduler_loop(
            raft.clone(),
            storage.clone(),
            notify.clone(),
        )));
        let worker = tokio::spawn(worker_loop(
            raft.clone(),
            storage.clone(),
            notify.clone(),
            handlers.clone(),
        ));
        let sock = data_dir.join("control.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).map_err(|err| Error::invalid(err.to_string()))?;
        restrict_control_socket(&sock)?;
        let control = tokio::spawn(control_loop(
            listener,
            raft.clone(),
            storage.clone(),
            notify.clone(),
            None,
            publication_auth.clone(),
        ));
        let snapshot_task = Some(tokio::spawn(snapshot_controller(
            raft.clone(),
            storage.clone(),
        )));
        let clock_watchdog = tokio::spawn(clock_watchdog_loop(storage.clone()));
        Ok(Engine {
            raft,
            storage,
            storage_thread: Mutex::new(Some(storage_thread)),
            data_dir,
            notify,
            worker: Some(worker),
            scheduler,
            control,
            raft_server: None,
            snapshot_task,
            clock_watchdog,
            cluster_net: None,
            handlers,
            publication_auth,
        })
    }
}

impl Engine {
    pub async fn member(config: MemberConfig) -> Result<Self> {
        crate::tls::install_provider();
        let expected_cluster = crate::tls::cluster_id_from_ca(&config.tls.ca_pem)?;
        crate::tls::verify_peer_identity(
            &config.tls.ca_pem,
            &expected_cluster,
            &crate::tls::load_certs(&config.tls.cert_pem)?,
        )?
        .require_member_identity(
            &expected_cluster,
            &crate::tls::PrincipalId::parse(config.node_id.to_string())?,
        )?;
        crate::tls::server_config(&config.tls)?;
        let listener = tokio::net::TcpListener::bind(config.bind)
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::FailedPrecondition,
                    format!("member listener {} unavailable: {err}", config.bind),
                )
            })?;
        let data_dir = config.data_dir.clone();
        std::fs::create_dir_all(&data_dir).map_err(|err| Error::invalid(err.to_string()))?;
        use sha2::Digest;
        let ca_digest = sha2::Sha256::digest(config.tls.ca_pem.as_bytes());
        let cluster_id = hex::encode(&ca_digest[..16]);
        let db_path = data_dir.join("member.redb");
        let existing_identity = read_identity(&data_dir, Some(&cluster_id))?;
        let (storage, storage_thread) = StorageHandle::open_member(&db_path)?;
        let publication_auth = match existing_identity {
            Some((auth, _)) => auth,
            None => write_identity(&data_dir, Some(&cluster_id))?,
        };
        let log_store = storage.log_store();
        let state_machine = storage.state_machine();
        let raft_config = Config {
            cluster_name: "graphrun".to_owned(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            max_payload_entries: 4,
            snapshot_max_chunk_size: 1024 * 1024,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 1024,
            ..Config::default()
        }
        .validate()
        .map_err(|err| Error::invalid(err.to_string()))?;
        let network = ClusterNetwork::new(config.node_id, config.tls.clone(), config.peers.clone());
        storage.clock().configure_network(network.clone());
        let raft = Raft::<TypeConfig>::new(
            config.node_id,
            Arc::new(raft_config),
            network.clone(),
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
        let notify = Arc::new(Notify::new());
        let scheduler = Some(tokio::spawn(scheduler_loop(
            raft.clone(),
            storage.clone(),
            notify.clone(),
        )));
        let raft_server = {
            let raft = raft.clone();
            let tls = config.tls.clone();
            let storage = storage.clone();
            let notify = notify.clone();
            let node_id = config.node_id;
            let mut genesis_members = BTreeMap::from([(node_id, config.bind)]);
            genesis_members.extend(config.peers.iter().map(|(id, (addr, _))| (*id, *addr)));
            let network = network.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_grpc(
                    listener,
                    tls,
                    raft,
                    storage,
                    notify,
                    node_id,
                    genesis_members,
                    network,
                )
                .await
                {
                    tracing::error!(%error, "member gRPC listener stopped");
                }
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
        let handlers = if config.host_activities {
            Handlers::fixtures()
        } else {
            Handlers::empty()
        };
        let worker = if config.host_activities {
            Some(tokio::spawn(worker_loop(
                raft.clone(),
                storage.clone(),
                notify.clone(),
                handlers.clone(),
            )))
        } else {
            None
        };
        let sock = data_dir.join("control.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).map_err(|err| Error::invalid(err.to_string()))?;
        restrict_control_socket(&sock)?;
        let control = tokio::spawn(control_loop(
            listener,
            raft.clone(),
            storage.clone(),
            notify.clone(),
            Some(network.clone()),
            publication_auth.clone(),
        ));
        let snapshot_task = Some(tokio::spawn(snapshot_controller(
            raft.clone(),
            storage.clone(),
        )));
        let clock_watchdog = tokio::spawn(clock_watchdog_loop(storage.clone()));
        Ok(Self {
            raft,
            storage,
            storage_thread: Mutex::new(Some(storage_thread)),
            data_dir,
            notify,
            worker,
            scheduler,
            control,
            raft_server: Some(raft_server),
            snapshot_task,
            clock_watchdog,
            cluster_net: Some(network),
            handlers,
            publication_auth,
        })
    }

    /// Commit an immutable catalog of versioned activity and schema contracts.
    pub async fn publish_catalog(
        &self,
        version: u32,
        catalog: Catalog,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_catalog_with_command(version, catalog, CommandId::generate())
            .await
    }

    pub async fn publish_catalog_with_command(
        &self,
        version: u32,
        catalog: Catalog,
        command_id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_with_command(
            command_id,
            crate::publication::PublicationOperation::Catalog { version, catalog },
        )
        .await
    }

    /// Publish a compiled definition against an already published catalog.
    pub async fn publish_definition(
        &self,
        definition: Definition,
        catalog_version: u32,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_definition_with_command(definition, catalog_version, CommandId::generate())
            .await
    }

    pub async fn publish_definition_with_command(
        &self,
        definition: Definition,
        catalog_version: u32,
        command_id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_with_command(
            command_id,
            crate::publication::PublicationOperation::Definition {
                definition: Box::new(definition),
                catalog_version,
            },
        )
        .await
    }

    pub async fn publish_definition_yaml(
        &self,
        yaml: &str,
        catalog_version: u32,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_definition_yaml_with_command(yaml, catalog_version, CommandId::generate())
            .await
    }

    pub async fn publish_definition_yaml_with_command(
        &self,
        yaml: &str,
        catalog_version: u32,
        command_id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        let state = self.storage.query_state().await;
        let catalog = state
            .published_catalogs
            .get(&catalog_version)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "published catalog not found"))?;
        catalog.verify()?;
        self.publish_definition_with_command(
            compile_yaml(yaml, &catalog.catalog)?,
            catalog_version,
            command_id,
        )
        .await
    }

    pub async fn start_published(
        &self,
        workflow: &str,
        version: Option<u32>,
        start_key: &str,
        input: Value,
    ) -> Result<RunId> {
        self.start_published_with_command(
            workflow,
            version,
            start_key,
            input,
            CommandId::generate(),
        )
        .await
    }

    /// Reuse `command_id` for all retries, including across leader changes.
    pub async fn start_published_with_command(
        &self,
        workflow: &str,
        version: Option<u32>,
        start_key: &str,
        input: Value,
        command_id: CommandId,
    ) -> Result<RunId> {
        let receipt = self
            .publish_with_command(
                command_id,
                crate::publication::PublicationOperation::Start {
                    workflow: workflow.to_owned(),
                    version,
                    start_key: start_key.to_owned(),
                    input,
                },
            )
            .await?;
        let run = receipt.applied_run()?;
        wake(&self.notify);
        Ok(run)
    }

    pub async fn command_result(
        &self,
        command_id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        linearizable_read(&self.raft).await?;
        self.storage
            .query_state()
            .await
            .command_results
            .get(&self.publication_auth.key(command_id).storage_key())
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "command result not found"))
            .and_then(|receipt| {
                receipt.ensure_format()?;
                Ok(receipt)
            })
    }

    async fn publish_with_command(
        &self,
        command_id: CommandId,
        operation: crate::publication::PublicationOperation,
    ) -> Result<crate::publication::CommandResult> {
        publication_via_raft(
            &self.raft,
            &self.storage,
            &self.publication_auth,
            command_id,
            operation,
        )
        .await
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn control_sock(&self) -> PathBuf {
        self.data_dir.join("control.sock")
    }

    /// Start a run from a compiled definition and return its id immediately.
    pub async fn start(
        &self,
        definition: Definition,
        catalog: Catalog,
        input: Value,
    ) -> Result<RunId> {
        if self.worker.is_some() {
            self.handlers.require_all(&definition.activity_keys())?;
        }
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
        wake(&self.notify);
        Ok(run)
    }

    /// Compile `yaml` against `catalog` and [`start`](Self::start) it.
    pub async fn start_yaml(&self, yaml: &str, catalog: &Catalog, input: Value) -> Result<RunId> {
        let definition = compile_yaml(yaml, catalog)?;
        self.start(definition, catalog.clone(), input).await
    }

    /// Deliver an event to a `wait_signal` on `run`.
    ///
    /// `name` and `key` must match the wait. `event_id` is 32 hex characters;
    /// a duplicate id is idempotent.
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
        wake(&self.notify);
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
        wake(&self.notify);
        Ok(())
    }

    pub async fn inspect(&self, run: RunId) -> Result<State> {
        linearizable_read(&self.raft).await?;
        let state = self.storage.query_state().await;
        let run_state = state
            .runs
            .get(&run)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "unknown run"))?;
        if let Some(pinned) = &run_state.published {
            pinned.verify(&run_state.definition, &run_state.catalog)?;
        }
        Ok(state)
    }

    pub async fn inspect_json(&self, run: RunId) -> Result<serde_json::Value> {
        let state = self.storage.query_state().await;
        if let Some(summary) = state.terminal_summaries.get(&run) {
            return Ok(crate::history::summary_view(summary));
        }
        let run_state = state
            .runs
            .get(&run)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "unknown run"))?;
        if let Some(pinned) = &run_state.published {
            pinned.verify(&run_state.definition, &run_state.catalog)?;
        }
        Ok(inspect_view(&state, run))
    }

    pub async fn health(&self) -> serde_json::Value {
        let metrics = self.raft.metrics().borrow().clone();
        let state = self.storage.query_state().await;
        let clock_safe = self.storage.clock().sample(&self.storage).await.is_ok();
        let quorum_safe =
            tokio::time::timeout(Duration::from_secs(2), self.raft.ensure_linearizable())
                .await
                .is_ok_and(|result| result.is_ok());
        health_view(
            format!("{:?}", metrics.state),
            metrics.last_applied.map(|id| id.index),
            metrics.last_log_index,
            metrics.membership_config.voter_ids().collect(),
            &state,
            clock_safe,
            self.storage.clock().fault_reason().await,
            quorum_safe,
            self.storage.scheduler_stats(),
        )
    }

    pub async fn list(&self) -> Result<serde_json::Value> {
        linearizable_read(&self.raft).await?;
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
        let mut runs = runs;
        runs.extend(
            state
                .terminal_summaries
                .values()
                .map(crate::history::summary_view),
        );
        Ok(serde_json::json!({ "runs": runs }))
    }

    pub async fn history(&self, run: RunId) -> Result<Vec<crate::domain::DomainEvent>> {
        let state = history_state(&self.raft, &self.storage).await?;
        let mut after = 0;
        let mut events = Vec::new();
        loop {
            let page =
                crate::history::page(&state, run, after, crate::history::MAX_PAGE_LIMIT, now())?;
            if page.unavailable {
                return Err(Error::new(
                    ErrorKind::Unavailable,
                    "history range is unavailable",
                ));
            }
            events.extend(page.events.into_iter().map(|entry| entry.event));
            match page.next_cursor {
                Some(cursor) => after = cursor,
                None => return Ok(events),
            }
        }
    }

    pub async fn history_page(
        &self,
        run: RunId,
        after: u64,
        limit: u32,
    ) -> Result<crate::history::HistoryPage> {
        crate::history::page(
            &history_state(&self.raft, &self.storage).await?,
            run,
            after,
            limit,
            now(),
        )
    }

    pub async fn reconstruct_at(&self, run: RunId, through: u64) -> Result<State> {
        crate::history::reconstruct_at(
            &history_state(&self.raft, &self.storage).await?,
            run,
            through,
            now(),
        )
    }

    #[cfg(test)]
    pub async fn run_worker(endpoint: String, tls: crate::tls::TlsMaterial) -> Result<()> {
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))?;
        crate::worker::Worker::builder(endpoint, tls, catalog)
            .fixture_handlers()
            .open()
            .await?
            .run()
            .await
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == ServerState::Leader
    }

    pub fn voter_ids(&self) -> Vec<u64> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect()
    }

    pub fn insert_peer(&self, id: u64, addr: SocketAddr, tls: crate::tls::TlsMaterial) {
        if let Some(net) = &self.cluster_net {
            net.insert_peer(id, addr, tls);
        }
    }

    pub async fn add_learner(&self, id: u64, addr: SocketAddr) -> Result<()> {
        self.raft
            .add_learner(id, BasicNode::new(addr.to_string()), true)
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        Ok(())
    }

    pub async fn add_voter(&self, id: u64) -> Result<()> {
        self.raft
            .change_membership(
                ChangeMembers::AddVoterIds(std::iter::once(id).collect()),
                true,
            )
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        Ok(())
    }

    pub async fn remove_voter(&self, id: u64) -> Result<()> {
        self.raft
            .change_membership(
                ChangeMembers::RemoveVoters(std::iter::once(id).collect()),
                false,
            )
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        self.raft
            .change_membership(
                ChangeMembers::RemoveNodes(std::iter::once(id).collect()),
                false,
            )
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        Ok(())
    }

    pub async fn abandon_compensation(&self, run: RunId, reason: &str) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::AbandonCompensation {
                run,
                reason: reason.to_owned(),
            },
        };
        self.write(command).await?;
        wake(&self.notify);
        Ok(())
    }

    pub async fn resolve_blocked(
        &self,
        run: RunId,
        forward: crate::ids::ActivationId,
        input: Value,
    ) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::ResolveBlocked {
                run,
                forward,
                input,
            },
        };
        self.write(command).await?;
        wake(&self.notify);
        Ok(())
    }

    pub async fn acknowledge_recovery(&self, reason: &str) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::AcknowledgeRecovery {
                reason: reason.to_owned(),
            },
        };
        self.write(command).await?;
        wake(&self.notify);
        Ok(())
    }

    pub async fn acknowledge_clock(&self, reason: &str) -> Result<()> {
        self.storage
            .clock()
            .acknowledge(&self.storage, reason)
            .await
    }

    pub fn backup(data_dir: impl AsRef<Path>, out: impl AsRef<Path>) -> Result<()> {
        let state = load_domain_readonly(data_dir.as_ref().join("member.redb"))?;
        let out = out.as_ref();
        std::fs::create_dir_all(out).map_err(|err| Error::invalid(err.to_string()))?;
        let id = CommandId::generate().to_hex();
        let raw_path = out.join(format!("domain-{id}.json.tmp"));
        let mut raw = private_file(&raw_path).map_err(|err| Error::invalid(err.to_string()))?;
        let _raw_cleanup = RemoveOnDrop(raw_path.clone());
        serde_json::to_writer(&mut raw, &state).map_err(|err| Error::invalid(err.to_string()))?;
        raw.sync_all()
            .map_err(|err| Error::invalid(err.to_string()))?;
        let body_len = raw
            .metadata()
            .map_err(|err| Error::invalid(err.to_string()))?
            .len();
        drop(raw);
        let snapshot_manifest = SnapshotManifest {
            framing_version: 1,
            generation: 0,
            applied_json: b"null".to_vec(),
            membership_json: b"null".to_vec(),
            payload_bytes: body_len,
            record_formats: vec![
                "graphrun.domain/v1".to_owned(),
                crate::history::EVENT_FORMAT.to_owned(),
                crate::history::CHECKPOINT_FORMAT.to_owned(),
                crate::publication::RESULT_FORMAT.to_owned(),
                crate::record_store::FORMAT.to_owned(),
                crate::record_store::FRAGMENT_FORMAT.to_owned(),
            ],
            record_count: 1,
        };
        let stage_path = out.join(format!("application-{id}.snap.tmp"));
        let digest = write_snapshot(
            &mut std::fs::File::open(&raw_path).map_err(|err| Error::invalid(err.to_string()))?,
            &stage_path,
            &snapshot_manifest,
        )
        .map_err(|err| Error::invalid(err.to_string()))?;
        let _stage_cleanup = RemoveOnDrop(stage_path.clone());
        let snapshot_name = format!("application-{}.snap", hex::encode(digest));
        let snapshot_path = out.join(&snapshot_name);
        if snapshot_path.exists() {
            let (_, previous) =
                verify_snapshot(&snapshot_path).map_err(|err| Error::invalid(err.to_string()))?;
            if previous != digest {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "backup artifact identity collides with different bytes",
                ));
            }
        } else {
            std::fs::rename(&stage_path, &snapshot_path)
                .map_err(|err| Error::invalid(err.to_string()))?;
        }
        std::fs::File::open(out)
            .and_then(|file| file.sync_all())
            .map_err(|err| Error::invalid(err.to_string()))?;
        let manifest = LogicalBackupManifest {
            format: "graphrun.backup/v2".to_owned(),
            kind: "logical-application".to_owned(),
            snapshot: snapshot_name,
            sha256: hex::encode(digest),
        };
        let manifest_stage = out.join(format!("manifest-{id}.json.tmp"));
        let mut file =
            private_file(&manifest_stage).map_err(|err| Error::invalid(err.to_string()))?;
        let _manifest_cleanup = RemoveOnDrop(manifest_stage.clone());
        serde_json::to_writer_pretty(&mut file, &manifest)
            .map_err(|err| Error::invalid(err.to_string()))?;
        file.sync_all()
            .map_err(|err| Error::invalid(err.to_string()))?;
        std::fs::rename(&manifest_stage, out.join("manifest.json"))
            .map_err(|err| Error::invalid(err.to_string()))?;
        std::fs::File::open(out)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| Error::invalid(err.to_string()))?;
        Ok(())
    }

    pub fn restore(from: impl AsRef<Path>, dest: impl AsRef<Path>, reason: &str) -> Result<()> {
        if reason.is_empty() {
            return Err(Error::invalid("restore requires a reason"));
        }
        let from = from.as_ref();
        let dest = dest.as_ref();
        let manifest: LogicalBackupManifest = serde_json::from_slice(
            &std::fs::read(from.join("manifest.json"))
                .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?,
        )
        .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        if manifest.format != "graphrun.backup/v2"
            || manifest.kind != "logical-application"
            || !manifest.snapshot.starts_with("application-")
            || !manifest.snapshot.ends_with(".snap")
            || Path::new(&manifest.snapshot).components().count() != 1
            || manifest.snapshot.contains('/')
            || manifest.snapshot.contains('\\')
            || manifest.snapshot.contains(':')
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "unsupported backup manifest; source left untouched",
            ));
        }
        let snapshot = from.join(&manifest.snapshot);
        let (framing, digest) = verify_snapshot(&snapshot)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        let required = [
            "graphrun.domain/v1",
            crate::history::EVENT_FORMAT,
            crate::history::CHECKPOINT_FORMAT,
            crate::publication::RESULT_FORMAT,
            crate::record_store::FORMAT,
            crate::record_store::FRAGMENT_FORMAT,
        ];
        if manifest.sha256 != hex::encode(digest)
            || framing.generation != 0
            || framing.applied_json != b"null"
            || framing.membership_json != b"null"
            || framing.record_formats.len() != required.len()
            || required
                .iter()
                .any(|format| !framing.record_formats.iter().any(|stored| stored == format))
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "backup digest, authority, or retained reader versions are unavailable",
            ));
        }
        let raw_path = std::env::temp_dir().join(format!(
            "graphrun-restore-{}.json.tmp",
            CommandId::generate().to_hex()
        ));
        let mut raw = private_file(&raw_path).map_err(|err| Error::invalid(err.to_string()))?;
        let _raw_cleanup = RemoveOnDrop(raw_path.clone());
        copy_payload(&snapshot, &mut raw)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        raw.sync_all()
            .map_err(|err| Error::invalid(err.to_string()))?;
        drop(raw);
        let mut domain: State = serde_json::from_reader(
            std::fs::File::open(&raw_path).map_err(|err| Error::invalid(err.to_string()))?,
        )
        .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        validate_history_store(&domain)?;
        if dest.exists()
            && std::fs::read_dir(dest)
                .map_err(|err| Error::invalid(err.to_string()))?
                .next()
                .is_some()
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "restore destination is not empty; directory left untouched",
            ));
        }
        domain.recovery = Some(crate::domain::RecoveryHold {
            reason: reason.to_owned(),
            authorized: false,
        });
        for act in domain.activations.values_mut() {
            if act.status == crate::domain::ActivationStatus::Ready {
                domain
                    .interventions
                    .insert(act.id, "restored active scope".to_owned());
            }
            act.claim = None;
        }
        std::fs::create_dir_all(dest).map_err(|err| Error::invalid(err.to_string()))?;
        let cluster = format!(
            "graphrun-restored-{}",
            crate::ids::CommandId::generate().to_hex()
        );
        let identity = serde_json::json!({
            "mode": "local",
            "node_id": 1u64,
            "cluster_name": cluster,
            "format": "graphrun.local-identity/v1",
            "cluster_id": crate::ids::ClusterId::generate().to_hex(),
            "restored": true,
        });
        let mut identity_file = private_file(&dest.join("identity.json"))
            .map_err(|err| Error::invalid(err.to_string()))?;
        serde_json::to_writer_pretty(&mut identity_file, &identity)
            .map_err(|err| Error::invalid(err.to_string()))?;
        identity_file
            .sync_all()
            .map_err(|err| Error::invalid(err.to_string()))?;
        let mut domain_file = private_file(&dest.join("restored-domain.json"))
            .map_err(|err| Error::invalid(err.to_string()))?;
        serde_json::to_writer(&mut domain_file, &domain)
            .map_err(|err| Error::invalid(err.to_string()))?;
        domain_file
            .sync_all()
            .map_err(|err| Error::invalid(err.to_string()))?;
        std::fs::File::open(dest)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::invalid(err.to_string()))?;
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn last_applied_index(&self) -> u64 {
        self.storage.last_applied_index().await
    }

    #[cfg(test)]
    pub(crate) fn inject_clock_watermark(&self, watermark: u64) {
        self.storage.clock().inject_watermark(watermark);
    }

    #[cfg(test)]
    pub(crate) fn clear_clock_watermark(&self) {
        self.storage.clock().clear_injected_watermark();
    }

    pub fn raft_applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0)
    }

    /// Block until `run` succeeds or fails, or `timeout` elapses.
    ///
    /// On success, returns the run output. On domain failure, returns an error
    /// with the failure code and message.
    pub async fn wait_terminal(&self, run: RunId, timeout: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let state = self.storage.query_state().await;
            if let Some(output) = run_output(&state, run) {
                linearizable_read(&self.raft).await?;
                return Ok(output);
            }
            if let Some(run_state) = state.runs.get(&run) {
                if let RunStatus::Failed { error } = &run_state.status {
                    linearizable_read(&self.raft).await?;
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
        self.commit_progress(run).await?;
        wake(&self.notify);
        Ok(())
    }

    pub async fn commit_progress(&self, run: RunId) -> Result<()> {
        let command = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Progress { run },
        };
        self.write(command).await
    }

    /// Stop workers, Raft, and the control socket. Does not delete `data_dir`.
    pub async fn shutdown(&self) -> Result<()> {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        if let Some(scheduler) = &self.scheduler {
            scheduler.abort();
        }
        self.control.abort();
        if let Some(server) = &self.raft_server {
            server.abort();
        }
        if let Some(task) = &self.snapshot_task {
            task.abort();
        }
        self.clock_watchdog.abort();
        let _ = self.raft.shutdown().await;
        self.storage.shutdown();
        if let Some(thread) = self.storage_thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(self.control_sock());
        Ok(())
    }

    async fn write(&self, command: Command) -> Result<()> {
        write_raft(&self.raft, &self.storage, command).await
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        if let Some(scheduler) = &self.scheduler {
            scheduler.abort();
        }
        self.clock_watchdog.abort();
        self.control.abort();
        if let Some(server) = &self.raft_server {
            server.abort();
        }
        if let Some(task) = &self.snapshot_task {
            task.abort();
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
    for run in state.runs.keys() {
        if !state.history.contains_key(run) {
            return Err(Error::new(
                ErrorKind::Unavailable,
                format!("missing retained history for run {}", run.to_hex()),
            ));
        }
    }
    for (run, events) in &state.history {
        let Some(run_state) = state.runs.get(run) else {
            return Err(Error::new(
                ErrorKind::Unavailable,
                format!("history has no retained run {}", run.to_hex()),
            ));
        };
        if let Some(pinned) = &run_state.published {
            pinned.verify(&run_state.definition, &run_state.catalog)?;
        }
        let rebuilt = crate::history::reconstruct_at(&state, *run, events.len() as u64, now())?;
        let live = run_output(&state, *run);
        let replayed = run_output(&rebuilt, *run);
        if live != replayed || run_state.status != rebuilt.runs[run].status {
            return Err(Error::invalid(format!(
                "replay mismatch for run {}",
                run.to_hex()
            )));
        }
    }
    Ok(state)
}

async fn history_state(raft: &Raft<TypeConfig>, storage: &StorageHandle) -> Result<State> {
    raft.ensure_linearizable()
        .await
        .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))?;
    Ok(storage.query_state().await)
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

fn read_identity(
    dir: &Path,
    expected_cluster: Option<&str>,
) -> Result<Option<(crate::publication::AuthContext, bool)>> {
    let path = dir.join("identity.json");
    if path.exists() {
        let bytes = std::fs::read(&path).map_err(|err| Error::invalid(err.to_string()))?;
        let identity: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        if identity["format"] != "graphrun.local-identity/v1" {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "incompatible pre-publication identity (directory left untouched)",
            ));
        }
        let cluster = identity["cluster_id"]
            .as_str()
            .ok_or_else(|| Error::new(ErrorKind::FailedPrecondition, "missing cluster identity"))?;
        crate::ids::ClusterId::from_hex(cluster)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err))?;
        if expected_cluster.is_some_and(|expected| expected != cluster) {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "cluster identity does not match configured CA",
            ));
        }
        return Ok(Some((
            crate::publication::AuthContext::local_owner(cluster.to_owned()),
            identity["restored"] == true,
        )));
    }
    if dir.join("member.redb").exists() || dir.join("restored-domain.json").exists() {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "member store or restored domain has no compatible identity (directory left untouched)",
        ));
    }
    Ok(None)
}

fn write_identity(
    dir: &Path,
    expected_cluster: Option<&str>,
) -> Result<crate::publication::AuthContext> {
    let path = dir.join("identity.json");
    let cluster_id = expected_cluster
        .map(str::to_owned)
        .unwrap_or_else(|| crate::ids::ClusterId::generate().to_hex());
    let body = serde_json::json!({
        "format": "graphrun.local-identity/v1",
        "cluster_id": cluster_id,
        "mode": "local",
        "node_id": 1u64,
        "cluster_name": "graphrun-local",
    });
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| Error::invalid(err.to_string()))?;
    file.write_all(&serde_json::to_vec_pretty(&body).unwrap())
        .map_err(|err| Error::invalid(err.to_string()))?;
    Ok(crate::publication::AuthContext::local_owner(cluster_id))
}

fn restrict_control_socket(sock: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))
        .map_err(|err| Error::invalid(err.to_string()))
}

async fn control_loop(
    listener: UnixListener,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
    cluster_net: Option<ClusterNetwork>,
    auth: crate::publication::AuthContext,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let raft = raft.clone();
        let storage = storage.clone();
        let notify = notify.clone();
        let cluster_net = cluster_net.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let _ = handle_control(stream, raft, storage, notify, cluster_net, auth).await;
        });
    }
}

async fn handle_control(
    stream: UnixStream,
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
    cluster_net: Option<ClusterNetwork>,
    auth: crate::publication::AuthContext,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let owner = std::fs::metadata(
        stream
            .local_addr()
            .map_err(|err| Error::invalid(err.to_string()))?
            .as_pathname()
            .ok_or_else(|| Error::invalid("control socket has no pathname"))?,
    )
    .map_err(|err| Error::invalid(err.to_string()))?
    .uid();
    if stream
        .peer_cred()
        .map_err(|err| Error::invalid(err.to_string()))?
        .uid()
        != owner
    {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "control socket owner only",
        ));
    }
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    let req: ControlRequest =
        serde_json::from_str(&line).map_err(|err| Error::invalid(err.to_string()))?;
    let resp = dispatch_control(req, &raft, &storage, &notify, cluster_net.as_ref(), &auth).await;
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
    cluster_net: Option<&ClusterNetwork>,
    auth: &crate::publication::AuthContext,
) -> ControlResponse {
    let result = match req {
        ControlRequest::PublishCatalog {
            version,
            catalog,
            command_id,
        } => match CommandId::from_hex(&command_id) {
            Ok(id) => publication_via_raft(
                raft,
                storage,
                auth,
                id,
                crate::publication::PublicationOperation::Catalog { version, catalog },
            )
            .await
            .and_then(|receipt| {
                serde_json::to_value(receipt).map_err(|err| Error::invalid(err.to_string()))
            }),
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::PublishDefinition {
            yaml,
            catalog_version,
            command_id,
        } => match CommandId::from_hex(&command_id) {
            Ok(id) => {
                let state = storage.query_state().await;
                let definition = state
                    .published_catalogs
                    .get(&catalog_version)
                    .ok_or_else(|| Error::new(ErrorKind::NotFound, "published catalog not found"))
                    .and_then(|published| {
                        published.verify()?;
                        compile_yaml(&yaml, &published.catalog)
                    });
                match definition {
                    Ok(definition) => publication_via_raft(
                        raft,
                        storage,
                        auth,
                        id,
                        crate::publication::PublicationOperation::Definition {
                            definition: Box::new(definition),
                            catalog_version,
                        },
                    )
                    .await
                    .and_then(|receipt| {
                        serde_json::to_value(receipt).map_err(|err| Error::invalid(err.to_string()))
                    }),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::StartPublished {
            workflow,
            version,
            start_key,
            input,
            command_id,
            wait_ms,
        } => match CommandId::from_hex(&command_id) {
            Ok(id) => match publication_via_raft(
                raft,
                storage,
                auth,
                id,
                crate::publication::PublicationOperation::Start {
                    workflow,
                    version,
                    start_key,
                    input,
                },
            )
            .await
            .and_then(|receipt| receipt.applied_run())
            {
                Ok(run) => {
                    wake(notify);
                    if let Some(ms) = wait_ms {
                        wait_via_raft(raft, storage, notify, run, Duration::from_millis(ms)).await
                                .map(|output| serde_json::json!({"run":run.to_hex(),"status":"succeeded","output":output}))
                    } else {
                        Ok(serde_json::json!({"run":run.to_hex(),"status":"started"}))
                    }
                }
                Err(err) => Err(err),
            },
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::CommandResult { command_id } => match CommandId::from_hex(&command_id) {
            Ok(id) => match linearizable_read(raft).await {
                Ok(_) => storage
                    .query_state()
                    .await
                    .command_results
                    .get(&auth.key(id).storage_key())
                    .ok_or_else(|| Error::new(ErrorKind::NotFound, "command result not found"))
                    .and_then(|receipt| {
                        receipt.ensure_format()?;
                        serde_json::to_value(receipt).map_err(|err| Error::invalid(err.to_string()))
                    }),
                Err(err) => Err(err),
            },
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Health => {
            let metrics = raft.metrics().borrow().clone();
            let state = storage.query_state().await;
            let clock_safe = storage.clock().sample(storage).await.is_ok();
            let quorum_safe =
                tokio::time::timeout(Duration::from_secs(2), raft.ensure_linearizable())
                    .await
                    .is_ok_and(|result| result.is_ok());
            Ok(health_view(
                format!("{:?}", metrics.state),
                metrics.last_applied.map(|id| id.index),
                metrics.last_log_index,
                metrics.membership_config.voter_ids().collect(),
                &state,
                clock_safe,
                storage.clock().fault_reason().await,
                quorum_safe,
                storage.scheduler_stats(),
            ))
        }
        ControlRequest::List => {
            let checked = linearizable_read(raft).await;
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
            checked.map(|_| serde_json::json!({"runs": runs}))
        }
        ControlRequest::Start {
            yaml,
            catalog,
            input,
            wait_ms,
        } => match start_via_raft(raft, storage, notify, &yaml, catalog, input).await {
            Ok(run) => {
                if let Some(ms) = wait_ms {
                    match wait_via_raft(raft, storage, notify, run, Duration::from_millis(ms)).await
                    {
                        Ok(output) => Ok(serde_json::json!({
                            "run": run.to_hex(),
                            "status": "succeeded",
                            "output": output,
                        })),
                        Err(err) => Err(err),
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
                    storage,
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
                    wake(notify);
                    serde_json::json!({"status":"ok"})
                }),
                Err(err) => Err(err),
            }
        }
        ControlRequest::Cancel { run, reason } => match RunId::from_hex(&run) {
            Ok(run) => write_raft(
                raft,
                storage,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::Cancel { run, reason },
                },
            )
            .await
            .map(|_| {
                wake(notify);
                serde_json::json!({"status":"ok"})
            }),
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Inspect { run } => match RunId::from_hex(&run) {
            Ok(run) => match linearizable_read(raft).await {
                Ok(()) => {
                    let state = storage.query_state().await;
                    if let Some(summary) = state.terminal_summaries.get(&run) {
                        Ok(crate::history::summary_view(summary))
                    } else {
                        state
                            .runs
                            .get(&run)
                            .ok_or_else(|| Error::new(ErrorKind::NotFound, "unknown run"))
                            .and_then(|run_state| {
                                if let Some(pinned) = &run_state.published {
                                    pinned.verify(&run_state.definition, &run_state.catalog)?;
                                }
                                Ok(inspect_view(&state, run))
                            })
                    }
                }
                Err(err) => Err(err),
            },
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::History {
            run,
            after_sequence,
            page_size,
        } => match RunId::from_hex(&run) {
            Ok(run) => match history_state(raft, storage).await {
                Ok(state) => crate::history::page(&state, run, after_sequence, page_size, now())
                    .and_then(|page| {
                        serde_json::to_value(page).map_err(|err| Error::invalid(err.to_string()))
                    }),
                Err(err) => Err(err),
            },
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Replay {
            run,
            through_sequence,
        } => match RunId::from_hex(&run) {
            Ok(run) => history_state(raft, storage)
                .await
                .and_then(|state| {
                    crate::history::reconstruct_at(&state, run, through_sequence, now())
                })
                .map(|projection| inspect_view(&projection, run)),
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Snapshot => raft
            .trigger()
            .snapshot()
            .await
            .map(|_| serde_json::json!({"status":"ok"}))
            .map_err(|err| Error::invalid(err.to_string())),
        ControlRequest::Acknowledge { reason } => write_raft(
            raft,
            storage,
            Command {
                id: CommandId::generate(),
                time: now(),
                body: CommandBody::AcknowledgeRecovery { reason },
            },
        )
        .await
        .map(|_| {
            wake(notify);
            serde_json::json!({"status":"ok"})
        }),
        ControlRequest::AcknowledgeClock { reason } => storage
            .clock()
            .acknowledge(storage, &reason)
            .await
            .map(|_| serde_json::json!({"status":"ok"})),
        ControlRequest::ResolveBlocked {
            run,
            forward,
            input,
        } => match (
            RunId::from_hex(&run),
            crate::ids::ActivationId::from_hex(&forward),
        ) {
            (Ok(run), Ok(forward)) => write_raft(
                raft,
                storage,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::ResolveBlocked {
                        run,
                        forward,
                        input,
                    },
                },
            )
            .await
            .map(|_| {
                wake(notify);
                serde_json::json!({"status":"ok"})
            }),
            (Err(err), _) | (_, Err(err)) => Err(Error::invalid(err)),
        },
        ControlRequest::Abandon { run, reason } => match RunId::from_hex(&run) {
            Ok(run) => write_raft(
                raft,
                storage,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::AbandonCompensation { run, reason },
                },
            )
            .await
            .map(|_| {
                wake(notify);
                serde_json::json!({"status":"ok"})
            }),
            Err(err) => Err(Error::invalid(err)),
        },
        ControlRequest::Join {
            node_id,
            addr,
            ca_pem,
            cert_pem,
            key_pem,
            server_name,
        } => match (cluster_net, addr.parse::<std::net::SocketAddr>()) {
            (None, _) => Err(Error::invalid("join requires a clustered member")),
            (_, Err(err)) => Err(Error::invalid(err.to_string())),
            (Some(net), Ok(parsed)) => {
                net.insert_peer(
                    node_id,
                    parsed,
                    crate::tls::TlsMaterial {
                        ca_pem,
                        cert_pem,
                        key_pem,
                        server_name,
                    },
                );
                raft.add_learner(node_id, BasicNode::new(parsed.to_string()), true)
                    .await
                    .map(|_| serde_json::json!({"status":"ok","node_id": node_id}))
                    .map_err(|err| Error::invalid(err.to_string()))
            }
        },
        ControlRequest::Promote { node_id } => raft
            .change_membership(
                ChangeMembers::AddVoterIds(std::iter::once(node_id).collect()),
                true,
            )
            .await
            .map(|_| serde_json::json!({"status":"ok","node_id": node_id}))
            .map_err(|err| Error::invalid(err.to_string())),
        ControlRequest::Remove { node_id } => {
            match raft
                .change_membership(
                    ChangeMembers::RemoveVoters(std::iter::once(node_id).collect()),
                    false,
                )
                .await
            {
                Ok(_) => raft
                    .change_membership(
                        ChangeMembers::RemoveNodes(std::iter::once(node_id).collect()),
                        false,
                    )
                    .await
                    .map(|_| serde_json::json!({"status":"ok","node_id": node_id}))
                    .map_err(|err| Error::invalid(err.to_string())),
                Err(err) => Err(Error::invalid(err.to_string())),
            }
        }
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
    storage: &StorageHandle,
    notify: &Notify,
    yaml: &str,
    catalog: Catalog,
    input: Value,
) -> Result<RunId> {
    let definition = compile_yaml(yaml, &catalog)?;
    let run = RunId::generate();
    write_raft(
        raft,
        storage,
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
    wake(notify);
    Ok(run)
}

async fn publication_via_raft(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    auth: &crate::publication::AuthContext,
    command_id: CommandId,
    operation: crate::publication::PublicationOperation,
) -> Result<crate::publication::CommandResult> {
    let command = crate::publication::command(auth, command_id, now(), operation)?;
    let reply = write_raft_response(raft, storage, command).await?;
    let receipt = reply.command_result.ok_or_else(|| {
        Error::new(
            ErrorKind::Unavailable,
            format!("unknown outcome for command {command_id}; query or retry the same ID"),
        )
    })?;
    receipt.ensure_applied()?;
    Ok(receipt)
}

async fn wait_via_raft(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    _notify: &Notify,
    run: RunId,
    timeout: Duration,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut changes = storage.subscribe_schedule();
    loop {
        let state = storage.query_state().await;
        if let Some(output) = run_output(&state, run) {
            linearizable_read(raft).await?;
            return Ok(output);
        }
        if let Some(run_state) = state.runs.get(&run) {
            if let RunStatus::Failed { error } = &run_state.status {
                linearizable_read(raft).await?;
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
        tokio::select! {
            result = changes.changed() => result.map_err(|_| {
                Error::new(ErrorKind::Unavailable, "storage schedule notifications stopped")
            })?,
            _ = tokio::time::sleep_until(deadline) => {
                return Err(Error::new(ErrorKind::DeadlineExceeded, "timed out waiting for run"));
            }
        }
    }
}

pub const SNAPSHOT_ENTRY_THRESHOLD: u64 = 20_000;
pub const SNAPSHOT_BYTE_THRESHOLD: u64 = 512 * 1024 * 1024;
pub const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30 * 60);

pub fn should_snapshot(
    applied: u64,
    last_snapshot: u64,
    applied_bytes_since_snapshot: u64,
    since_snapshot: Duration,
) -> bool {
    applied.saturating_sub(last_snapshot) >= SNAPSHOT_ENTRY_THRESHOLD
        || applied_bytes_since_snapshot >= SNAPSHOT_BYTE_THRESHOLD
        || (since_snapshot >= SNAPSHOT_INTERVAL && applied > last_snapshot)
}

async fn snapshot_controller(raft: Raft<TypeConfig>, storage: StorageHandle) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let progress = match storage.snapshot_progress().await {
            Ok(progress) => progress,
            Err(error) => {
                tracing::error!(%error, "snapshot controller cannot read durable progress");
                return;
            }
        };
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            continue;
        }
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        let now_ms = match crate::time::wall_millis() {
            Ok(now_ms) => now_ms,
            Err(error) => {
                tracing::error!(%error, "snapshot controller wall clock unavailable");
                return;
            }
        };
        let since = Duration::from_millis(now_ms.saturating_sub(progress.last_snapshot_ms));
        if should_snapshot(
            applied,
            progress.last_snapshot_applied,
            progress
                .applied_bytes
                .saturating_sub(progress.last_snapshot_bytes),
            since,
        ) && let Err(error) = raft.trigger().snapshot().await
        {
            tracing::warn!(%error, "snapshot threshold reached but build failed");
        }
    }
}

async fn clock_watchdog_loop(storage: StorageHandle) {
    loop {
        let _ = storage.clock().sample(&storage).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn scheduler_loop(raft: Raft<TypeConfig>, storage: StorageHandle, _notify: Arc<Notify>) {
    let mut changes = storage.subscribe_schedule();
    let mut metrics = raft.metrics();
    let mut leader_term = None;
    loop {
        let observed = metrics.borrow().clone();
        if observed.state != ServerState::Leader {
            leader_term = None;
            if metrics.changed().await.is_err() {
                return;
            }
            continue;
        }
        if leader_term != Some(observed.current_term) {
            storage.record_schedule_wake(ScheduleWake::Leadership);
            leader_term = Some(observed.current_term);
        }
        let view = match storage.schedule_view().await {
            Ok(view) => view,
            Err(error) => {
                tracing::error!(%error, "scheduler index unavailable");
                return;
            }
        };
        storage.observe_scheduler(view.revision);
        let now_ms = match crate::time::wall_millis() {
            Ok(now_ms) => now_ms,
            Err(error) => {
                tracing::error!(%error, "scheduler wall clock unavailable");
                return;
            }
        };
        let mut next_due = view.retention_ms;
        if view.retention_ms.is_some_and(|at| at <= now_ms) {
            storage.record_schedule_wake(ScheduleWake::Retention);
            if let Err(error) = write_raft(
                &raft,
                &storage,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::PruneHistory { limit: 512 },
                },
            )
            .await
            {
                tracing::warn!(%error, "retention proposal deferred");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            continue;
        }
        let mut progressed = false;
        for (run, row) in &view.runs {
            if row.progress || row.deadline_ms.is_some_and(|at| at <= now_ms) {
                if let Err(error) = write_raft(
                    &raft,
                    &storage,
                    Command {
                        id: CommandId::generate(),
                        time: now(),
                        body: CommandBody::Progress { run: *run },
                    },
                )
                .await
                {
                    tracing::warn!(%run, %error, "run progression deferred");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                progressed = true;
                break;
            }
            if let Some(at) = row.deadline_ms {
                next_due = Some(next_due.map_or(at, |previous| previous.min(at)));
            }
        }
        if progressed {
            continue;
        }
        let mut deadline = next_due.map(|at| {
            Box::pin(tokio::time::sleep(Duration::from_millis(
                at.saturating_sub(now_ms),
            )))
        });
        loop {
            if *changes.borrow() != view.revision {
                break;
            }
            tokio::select! {
                change = changes.changed() => {
                    if change.is_err() {
                        return;
                    }
                    storage.record_schedule_wake(ScheduleWake::Applied);
                    break;
                }
                change = metrics.changed() => {
                    if change.is_err() {
                        return;
                    }
                    let latest = metrics.borrow();
                    if latest.state != ServerState::Leader
                        || latest.current_term != observed.current_term
                    {
                        storage.record_schedule_wake(ScheduleWake::Leadership);
                        break;
                    }
                }
                _ = async {
                    if let Some(timer) = deadline.as_mut() {
                        timer.as_mut().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    storage.record_schedule_wake(ScheduleWake::Deadline);
                    break;
                },
            }
        }
    }
}

async fn check_dispatch(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    session_expiry_ms: u64,
    lease_expiry_ms: u64,
    attempt_deadline_ms: u64,
) -> Result<()> {
    linearizable_read(raft).await?;
    storage.clock().authorize(storage, raft).await?;
    let wall = crate::time::wall_millis()?;
    if wall >= attempt_deadline_ms
        || wall.saturating_add(5_000) >= session_expiry_ms
        || wall.saturating_add(5_000) >= lease_expiry_ms
    {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "assignment is at or beyond its execution or lease stop margin",
        ));
    }
    Ok(())
}

async fn worker_loop(
    raft: Raft<TypeConfig>,
    storage: StorageHandle,
    notify: Arc<Notify>,
    handlers: Handlers,
) {
    let session = crate::ids::WorkerSessionId::generate();
    let blocking_slots = Arc::new(Semaphore::new(limits::BLOCKING_POOL_DEFAULT as usize));
    let mut changes = storage.subscribe_schedule();
    let mut leadership = raft.metrics();
    let mut renewal = tokio::time::interval(Duration::from_secs(5));
    renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = write_raft(
        &raft,
        &storage,
        Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: crate::limits::CLAIM_BATCH,
            },
        },
    )
    .await;
    loop {
        let observed = leadership.borrow().clone();
        if observed.state != ServerState::Leader {
            if leadership.changed().await.is_err() {
                return;
            }
            continue;
        }
        let view = match storage.schedule_view().await {
            Ok(view) => view,
            Err(error) => {
                tracing::error!(%error, "local worker schedule index unavailable");
                return;
            }
        };
        storage.observe_local_worker(view.revision);
        let ready = if view.runs.iter().any(|(_, row)| row.ready) {
            let state = storage.query_state().await;
            if state
                .sessions
                .get(&session)
                .is_none_or(|worker| now().as_millis() >= worker.expires_ms)
            {
                if let Err(error) = write_raft(
                    &raft,
                    &storage,
                    Command {
                        id: CommandId::generate(),
                        time: now(),
                        body: CommandBody::RegisterSession {
                            session,
                            activities: vec!["*".to_owned()],
                            capacity: crate::limits::CLAIM_BATCH,
                        },
                    },
                )
                .await
                {
                    tracing::warn!(%error, "local worker registration deferred");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                continue;
            } else {
                match crate::domain::worker_has_ready(&state, session, now()) {
                    Ok(ready) => ready,
                    Err(error) => {
                        tracing::error!(%error, "local worker readiness failed");
                        return;
                    }
                }
            }
        } else {
            false
        };
        if !ready {
            loop {
                if *changes.borrow() != view.revision {
                    break;
                }
                tokio::select! {
                    changed = changes.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        storage.record_schedule_wake(ScheduleWake::Worker);
                        break;
                    }
                    changed = leadership.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        let latest = leadership.borrow();
                        if latest.state != ServerState::Leader
                            || latest.current_term != observed.current_term
                        {
                            storage.record_schedule_wake(ScheduleWake::Leadership);
                            break;
                        }
                    }
                    _ = renewal.tick() => {
                        if let Err(error) = write_raft(
                            &raft,
                            &storage,
                            Command {
                                id: CommandId::generate(),
                                time: now(),
                                body: CommandBody::RenewLocalSession { session },
                            },
                        ).await {
                            tracing::warn!(%error, "local worker renewal failed");
                            match write_raft(
                                &raft,
                                &storage,
                                Command {
                                    id: CommandId::generate(),
                                    time: now(),
                                    body: CommandBody::RegisterSession {
                                        session,
                                        activities: vec!["*".to_owned()],
                                        capacity: crate::limits::CLAIM_BATCH,
                                    },
                                },
                            ).await {
                                Ok(()) => break,
                                Err(error) => tracing::warn!(%error, "local worker re-registration failed"),
                            }
                        }
                    }
                }
            }
            continue;
        }
        let claim = Command {
            id: CommandId::generate(),
            time: now(),
            body: CommandBody::Claim {
                session,
                capacity: crate::limits::CLAIM_BATCH,
            },
        };
        if write_raft(&raft, &storage, claim.clone()).await.is_err() {
            let _ = write_raft(
                &raft,
                &storage,
                Command {
                    id: CommandId::generate(),
                    time: now(),
                    body: CommandBody::RegisterSession {
                        session,
                        activities: vec!["*".to_owned()],
                        capacity: crate::limits::CLAIM_BATCH,
                    },
                },
            )
            .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let mut did_work = false;
        let state = storage.query_state().await;
        let events = state.commands.get(&claim.id).cloned().unwrap_or_default();
        let Ok(assignments) = crate::domain::assignments_from(&state, &events) else {
            tracing::error!("committed local claim has an unavailable assignment");
            continue;
        };
        for assignment in assignments {
            let Some(session) = state.sessions.get(&assignment.session) else {
                tracing::error!(session = %assignment.session, "committed assignment has no worker session");
                break;
            };
            if let Err(error) = check_dispatch(
                &raft,
                &storage,
                session.expires_ms,
                assignment.lease_expiry_ms,
                assignment.attempt_deadline_ms,
            )
            .await
            {
                tracing::warn!(%error, "worker dispatch stopped");
                break;
            }
            let blocking = state
                .runs
                .get(&assignment.run)
                .and_then(|run_state| {
                    run_state
                        .catalog
                        .activities
                        .get(&crate::ids::ActivityKey::new(
                            &assignment.activity_name,
                            assignment.activity_version,
                        ))
                })
                .is_some_and(|contract| contract.execution == ExecutionKind::Blocking);
            if assignment.role == crate::ids::ExecutionRole::Reconciliation {
                let outcome = handlers.reconcile(
                    &assignment.activity_name,
                    &assignment.input,
                    Some(assignment.effect_key.to_hex().as_str()),
                );
                let _ = write_raft(
                    &raft,
                    &storage,
                    Command {
                        id: CommandId::generate(),
                        time: now(),
                        body: CommandBody::Reconcile {
                            run: assignment.run,
                            activation: assignment.activation,
                            session: assignment.session,
                            generation: assignment.generation,
                            revision: assignment.revision,
                            outcome: outcome.0,
                            output: outcome.1,
                        },
                    },
                )
                .await;
            } else {
                let effect_key = assignment.effect_key.to_hex();
                let ran = if blocking {
                    let handlers = handlers.clone();
                    let name = assignment.activity_name.clone();
                    let version = assignment.activity_version;
                    let input = assignment.input.clone();
                    let effect_key = effect_key.clone();
                    let Ok(permit) = blocking_slots.clone().acquire_owned().await else {
                        continue;
                    };
                    if let Err(error) = check_dispatch(
                        &raft,
                        &storage,
                        session.expires_ms,
                        assignment.lease_expiry_ms,
                        assignment.attempt_deadline_ms,
                    )
                    .await
                    {
                        tracing::warn!(%error, "blocking activity dispatch stopped");
                        continue;
                    }
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        handlers.run_blocking(&name, version, input, Some(effect_key.as_str()))
                    })
                    .await
                    .unwrap_or_else(|err| Err(Error::invalid(err.to_string())))
                } else {
                    handlers
                        .run(
                            &assignment.activity_name,
                            assignment.activity_version,
                            assignment.input.clone(),
                            Some(effect_key.as_str()),
                            false,
                        )
                        .await
                };
                match ran {
                    Ok(output) => {
                        let _ = write_raft(
                            &raft,
                            &storage,
                            Command {
                                id: CommandId::generate(),
                                time: now(),
                                body: CommandBody::ReportAssigned {
                                    run: assignment.run,
                                    activation: assignment.activation,
                                    output,
                                    session: assignment.session,
                                    generation: assignment.generation,
                                    revision: assignment.revision,
                                },
                            },
                        )
                        .await;
                    }
                    Err(err) => {
                        let (code, message) = if err.message.contains("activity.unregistered") {
                            ("activity.unregistered".to_owned(), err.message.clone())
                        } else {
                            ("activity.failed".to_owned(), err.message.clone())
                        };
                        let _ = write_raft(
                            &raft,
                            &storage,
                            Command {
                                id: CommandId::generate(),
                                time: now(),
                                body: CommandBody::ReportError {
                                    run: assignment.run,
                                    activation: assignment.activation,
                                    code,
                                    message,
                                },
                            },
                        )
                        .await;
                    }
                }
            }
            did_work = true;
        }
        if did_work {
            wake(&notify);
        }
    }
}

pub fn builtin_handler(name: &str, input: &Value) -> Result<Value> {
    dispatch_handler(name, input, None)
}

pub(crate) fn dispatch_handler(
    name: &str,
    input: &Value,
    effect_key: Option<&str>,
) -> Result<Value> {
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
            let order_id = fields
                .get("order_id")
                .cloned()
                .unwrap_or(Value::String(String::new()));
            let amount = fields.get("amount").cloned().unwrap_or(Value::Int(0));
            keyed_output(effect_key, "forward", || {
                Value::Object(BTreeMap::from([
                    ("order_id".to_owned(), order_id.clone()),
                    ("amount".to_owned(), amount.clone()),
                    (
                        "reservation_id".to_owned(),
                        Value::String("res-1".to_owned()),
                    ),
                ]))
            })
        }
        "payment.charge" => {
            let Value::Object(fields) = input else {
                return Err(Error::invalid("reserved input"));
            };
            let order_id = fields
                .get("order_id")
                .cloned()
                .unwrap_or(Value::String(String::new()));
            let amount = fields.get("amount").cloned().unwrap_or(Value::Int(0));
            keyed_output(effect_key, "forward", || {
                Value::Object(BTreeMap::from([
                    ("order_id".to_owned(), order_id.clone()),
                    ("amount".to_owned(), amount.clone()),
                    ("payment_id".to_owned(), Value::String("pay-1".to_owned())),
                ]))
            })
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
        "test.block" => {
            BLOCKING_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(400));
            BLOCKING_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
            Ok(input.clone())
        }
        "remote.echo" | "test.gate" => Ok(input.clone()),
        "inventory.release" | "payment.refund" => {
            if name == "inventory.release" && SLOW_RELEASE.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(800));
            }
            if let Some(key) = effect_key {
                let undo = format!("{key}:undo");
                apply_keyed(&undo, "compensate", Value::Null)?;
            }
            Ok(Value::Null)
        }
        "test.manual" => Ok(input.clone()),
        _ => Err(Error::invalid(format!("activity.unregistered: {name}"))),
    }
}

pub fn builtin_reconcile(
    name: &str,
    input: &Value,
    effect_key: Option<&str>,
) -> (crate::domain::ReconcileOutcome, Option<Value>) {
    if let (Some(url), Some(key)) = (crate::provider::provider_url(), effect_key) {
        match crate::provider::probe_effect(&url, key) {
            Ok(resp) => {
                return match resp.status {
                    crate::provider::EffectStatus::Applied => (
                        crate::domain::ReconcileOutcome::Applied,
                        resp.output.or_else(|| Some(input.clone())),
                    ),
                    crate::provider::EffectStatus::NotApplied => {
                        (crate::domain::ReconcileOutcome::NotApplied, None)
                    }
                    crate::provider::EffectStatus::Pending
                    | crate::provider::EffectStatus::Unknown => {
                        (crate::domain::ReconcileOutcome::Unknown, None)
                    }
                };
            }
            Err(_) => return (crate::domain::ReconcileOutcome::Unknown, None),
        }
    }
    match name {
        "test.lookup" | "inventory.lookup" | "payment.lookup" => (
            crate::domain::ReconcileOutcome::Applied,
            Some(input.clone()),
        ),
        _ => (crate::domain::ReconcileOutcome::Unknown, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static COMPENSATION_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct HandlerCounter {
        value: i64,
    }
    crate::payload!(HandlerCounter, "counter");

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
    async fn fresh_directory_initializes_store_before_identity() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        assert!(dir.path().join("identity.json").exists());
        assert!(dir.path().join("member.redb").exists());
        engine.shutdown().await.unwrap();

        let engine = Engine::local(dir.path()).await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existing_identity_with_missing_member_store_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let id = CommandId::generate();
        let engine = Engine::local(dir.path()).await.unwrap();
        engine
            .publish_catalog_with_command(1, Catalog::default(), id)
            .await
            .unwrap();
        engine.shutdown().await.unwrap();

        let identity = dir.path().join("identity.json");
        let bytes = std::fs::read(&identity).unwrap();
        let db_path = dir.path().join("member.redb");
        std::fs::remove_file(&db_path).unwrap();

        let err = Engine::local(dir.path()).await.err().unwrap();
        assert_eq!(err.kind, ErrorKind::FailedPrecondition);
        assert!(err.message.contains("member store missing"));
        assert!(!db_path.exists());
        assert_eq!(std::fs::read(identity).unwrap(), bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_catalog_receipt_survives_restart_and_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let id = CommandId::generate();
        let bad_catalog = |multiple_of| {
            let mut catalog = Catalog::default();
            catalog.schemas.insert(
                crate::schema::SchemaKey::parse("fraction/v1").unwrap(),
                serde_json::json!({"type":"number","multipleOf":multiple_of}),
            );
            catalog
        };
        let engine = Engine::local(dir.path()).await.unwrap();
        let err = engine
            .publish_catalog_with_command(1, bad_catalog(0.5), id)
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        let receipt = engine.command_result(id).await.unwrap();
        assert_eq!(receipt.ensure_applied().unwrap_err().kind, err.kind);

        let retry = engine
            .publish_catalog_with_command(1, bad_catalog(0.5), id)
            .await
            .unwrap_err();
        assert_eq!(retry, err);
        assert_eq!(
            serde_json::to_value(engine.command_result(id).await.unwrap()).unwrap(),
            serde_json::to_value(&receipt).unwrap()
        );
        let conflict = engine
            .publish_catalog_with_command(1, bad_catalog(0.25), id)
            .await
            .unwrap_err();
        assert_eq!(conflict.kind, ErrorKind::AlreadyExists);
        engine.publish_catalog(1, Catalog::default()).await.unwrap();
        engine.shutdown().await.unwrap();

        let engine = Engine::local(dir.path()).await.unwrap();
        assert_eq!(
            serde_json::to_value(engine.command_result(id).await.unwrap()).unwrap(),
            serde_json::to_value(&receipt).unwrap()
        );
        assert_eq!(
            engine
                .publish_catalog_with_command(1, bad_catalog(0.5), id)
                .await
                .unwrap_err(),
            err
        );
        assert_eq!(
            engine
                .publish_catalog_with_command(1, bad_catalog(0.25), id)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::AlreadyExists
        );
        engine.shutdown().await.unwrap();
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
        let before = std::fs::metadata(dir.path().join("member.redb"))
            .unwrap()
            .modified()
            .unwrap();
        let _ = replay(dir.path()).unwrap();
        let after = std::fs::metadata(dir.path().join("member.redb"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(before, after, "readonly replay must not write the store");
    }

    fn bump_yaml() -> &'static str {
        r#"
dsl: graphrun/v1
id: bump
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: inc
nodes:
  inc:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: workflow.input}
    next: done
  done:
    kind: complete
    output: {from: nodes.inc.output}
"#
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unregistered_activity_fails_at_start() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::builder(dir.path()).open().await.unwrap();
        let err = engine
            .start_yaml(
                bump_yaml(),
                &catalog(),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("activity.unregistered"), "{err}");
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_handler_runs() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::builder(dir.path())
            .activity("counter.increment", |input: HandlerCounter| async move {
                Ok(HandlerCounter {
                    value: input.value + 10,
                })
            })
            .unwrap()
            .open()
            .await
            .unwrap();
        let run = engine
            .start_yaml(
                bump_yaml(),
                &catalog(),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            output,
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(11))]))
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_engine_survives_past_session_lease() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let yaml = r#"
dsl: graphrun/v1
id: bump
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: inc
nodes:
  inc:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: workflow.input}
    next: done
  done:
    kind: complete
    output: {from: nodes.inc.output}
"#;
        let input = Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))]));
        for i in 0..80 {
            if i == 10 {
                tokio::time::sleep(crate::policy::SESSION_LEASE + Duration::from_millis(200)).await;
            }
            let run = engine
                .start_yaml(yaml, &catalog(), input.clone())
                .await
                .unwrap();
            let output = engine
                .wait_terminal(run, Duration::from_secs(5))
                .await
                .unwrap_or_else(|err| panic!("run {i}: {err}"));
            assert_eq!(
                output,
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))]))
            );
        }
        engine.shutdown().await.unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_wait_does_not_rescan_ready_index_on_local_renewal() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let run = engine
            .start_yaml(
                r#"
dsl: graphrun/v1
id: waiting_without_deadline
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  continue: {schema: unit/v1}
start: pause
nodes:
  pause:
    kind: wait_signal
    signal: continue
    key: {literal: k1}
    timeout: null
    next: finish
  finish:
    kind: complete
    output: {literal: null}
"#,
                &catalog(),
                Value::Null,
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let state = engine.storage.query_state().await;
            let view = engine.storage.schedule_view().await.unwrap();
            if state.waits.values().any(|wait| wait.pending)
                && view.runs.iter().any(|(id, row)| {
                    *id == run && !row.progress && !row.ready && row.deadline_ms.is_none()
                })
                && engine.storage.scheduler_observed_revision() >= view.revision
                && engine.storage.local_worker_observed_revision() >= view.revision
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "wait did not settle"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let before = engine.storage.schedule_discovery_reads();
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(matches!(
            engine.storage.query_state().await.runs[&run].status,
            RunStatus::Active
        ));
        assert_eq!(engine.storage.schedule_discovery_reads(), before);
        engine
            .signal(run, EventId::generate(), "continue", "k1", Value::Null)
            .await
            .unwrap();
        assert_eq!(
            engine
                .wait_terminal(run, Duration::from_secs(10))
                .await
                .unwrap(),
            Value::Null
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog(),
                order(),
            )
            .await
            .unwrap();
        let _ = engine.wait_terminal(run, Duration::from_secs(10)).await;
        engine.snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        engine.shutdown().await.unwrap();
        let snaps = dir.path().join("snapshots");
        let found = snaps
            .read_dir()
            .map(|entries| entries.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        assert!(found > 0, "expected snapshot file in {}", snaps.display());
        let replayed = replay(dir.path()).unwrap();
        assert!(run_output(&replayed, run).is_some());
    }

    #[test]
    fn snapshot_controller_threshold() {
        assert!(!should_snapshot(10, 0, 0, Duration::from_secs(1)));
        assert!(should_snapshot(
            SNAPSHOT_ENTRY_THRESHOLD,
            0,
            0,
            Duration::from_secs(1)
        ));
        assert!(should_snapshot(
            5,
            0,
            SNAPSHOT_BYTE_THRESHOLD,
            Duration::from_secs(1)
        ));
        assert!(!should_snapshot(
            5,
            0,
            SNAPSHOT_BYTE_THRESHOLD - 1,
            Duration::from_secs(1)
        ));
        assert!(should_snapshot(5, 0, 0, SNAPSHOT_INTERVAL));
        assert!(!should_snapshot(0, 0, 0, SNAPSHOT_INTERVAL));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "applies 20_000 raft entries (~4 min)"]
    async fn snapshot_controller_fires_at_20000_entries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/events.yaml"),
                &catalog(),
                Value::Object(BTreeMap::from([(
                    "key".to_owned(),
                    Value::String("k1".to_owned()),
                )])),
            )
            .await
            .unwrap();
        let mut writes = 0u64;
        while engine.raft_applied_index() < SNAPSHOT_ENTRY_THRESHOLD {
            engine.commit_progress(run).await.unwrap();
            writes += 1;
            if writes == 32 {
                assert!(
                    engine.raft_applied_index() > 0,
                    "raft applied index stayed 0 after {writes} writes"
                );
            }
            if writes.is_multiple_of(2_000) {
                eprintln!(
                    "snapshot fill writes={writes} applied={}",
                    engine.raft_applied_index()
                );
            }
            if writes > SNAPSHOT_ENTRY_THRESHOLD + 256 {
                break;
            }
        }
        let applied = engine.raft_applied_index();
        assert!(
            applied >= SNAPSHOT_ENTRY_THRESHOLD,
            "applied {applied} below threshold"
        );
        let snaps = dir.path().join("snapshots");
        let mut found = 0;
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            found = snaps
                .read_dir()
                .map(|entries| {
                    entries
                        .flatten()
                        .filter(|entry| {
                            entry.path().extension().and_then(|ext| ext.to_str()) == Some("snap")
                        })
                        .count()
                })
                .unwrap_or(0);
            if found > 0 {
                break;
            }
        }
        engine.shutdown().await.unwrap();
        assert!(
            found > 0,
            "controller did not write a snapshot after {applied} applied entries in {}",
            snaps.display()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_saga_failure_compensates() {
        let _guard = COMPENSATION_TEST.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/saga.yaml"),
                &catalog(),
                Value::Object(BTreeMap::from([
                    ("order_id".to_owned(), Value::String("o1".to_owned())),
                    ("amount".to_owned(), Value::Int(1000)),
                    ("fail_after_payment".to_owned(), Value::Bool(true)),
                ])),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let state = engine.inspect(run).await.unwrap();
            let failed = matches!(
                state.runs.get(&run).unwrap().status,
                RunStatus::Failed { .. }
            );
            let compensated =
                state.obligations.iter().all(|item| {
                    matches!(item.status, crate::domain::ObligationStatus::Compensated)
                }) && !state.obligations.is_empty();
            if failed && compensated {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "saga did not compensate; failed={failed} obligations={}",
                    state.obligations.len()
                );
            }
            let _ = engine.progress(run).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let events = engine.history(run).await.unwrap();
        let handlers: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                crate::domain::DomainEvent::CompensationStarted { handler, .. } => {
                    Some(handler.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(handlers, ["payment.refund", "inventory.release"]);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_nested_saga_transfers() {
        let _guard = COMPENSATION_TEST.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/nested-saga.yaml"),
                &catalog(),
                order(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let state = engine.inspect(run).await.unwrap();
            let failed = matches!(
                state.runs.get(&run).unwrap().status,
                RunStatus::Failed { .. }
            );
            let compensated = state.obligations.iter().all(|item| {
                matches!(
                    item.status,
                    crate::domain::ObligationStatus::Compensated
                        | crate::domain::ObligationStatus::Released
                )
            }) && !state.obligations.is_empty();
            if failed && compensated {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("nested saga did not settle");
            }
            let _ = engine.progress(run).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let events = engine.history(run).await.unwrap();
        let handlers: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                crate::domain::DomainEvent::CompensationStarted { handler, .. } => {
                    Some(handler.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(handlers, ["payment.refund", "inventory.release"]);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_cancel_does_not_prove_termination() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let yaml = r#"
dsl: graphrun/v1
id: block_once
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: block
nodes:
  block:
    kind: activity
    activity: {name: test.block, version: 1}
    input: {from: workflow.input}
    next: done
  done:
    kind: complete
    output: {from: nodes.block.output}
"#;
        let run = engine
            .start_yaml(
                yaml,
                &catalog(),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
            )
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        loop {
            if BLOCKING_IN_FLIGHT.load(Ordering::SeqCst) == 1 {
                break;
            }
            if started.elapsed() > Duration::from_secs(5) {
                panic!("blocking handler never entered");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        engine.cancel(run, "stop").await.unwrap();
        assert_eq!(
            BLOCKING_IN_FLIGHT.load(Ordering::SeqCst),
            1,
            "cancel must not terminate the blocking thread"
        );
        let _ = engine.inspect(run).await.unwrap();
        let _ = engine.progress(run).await;
        let inflight_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while BLOCKING_IN_FLIGHT.load(Ordering::SeqCst) != 0 {
            if tokio::time::Instant::now() >= inflight_deadline {
                panic!("blocking handler never returned");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let state = engine.inspect(run).await.unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_unfinished_parallel_branches() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r#"
dsl: graphrun/v1
id: unfinished_parallel
version: 1
input_schema: unit/v1
output_schema: {tuple: [approval/v1, approval/v1]}
signals:
  approval: {schema: approval/v1}
start: both
nodes:
  both:
    kind: parallel
    branches:
      - name: left
        input: {literal: null}
        body:
          input_schema: unit/v1
          output_schema: approval/v1
          start: wait
          nodes:
            wait:
              kind: wait_signal
              signal: approval
              key: {literal: "k1"}
              consume_from: buffered
              timeout: null
              next: done
            done:
              kind: complete
              output: {from: nodes.wait.output}
      - name: right
        input: {literal: null}
        body:
          input_schema: unit/v1
          output_schema: approval/v1
          start: wait
          nodes:
            wait:
              kind: wait_signal
              signal: approval
              key: {literal: "k2"}
              consume_from: buffered
              timeout: null
              next: done
            done:
              kind: complete
              output: {from: nodes.wait.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.both.output}
"#;
        let run = {
            let engine = Engine::local(dir.path()).await.unwrap();
            let run = engine
                .start_yaml(yaml, &catalog(), Value::Null)
                .await
                .unwrap();
            let mut opened = false;
            for _ in 0..50 {
                engine.progress(run).await.unwrap();
                let state = engine.inspect(run).await.unwrap();
                let pending = state.waits.values().filter(|wait| wait.pending).count();
                let branches: Vec<_> = state
                    .scopes
                    .values()
                    .filter_map(|scope| match &scope.role {
                        crate::domain::ScopeRole::ParallelBranch { name, .. } => Some(name.clone()),
                        _ => None,
                    })
                    .collect();
                if pending == 2 && branches.len() == 2 {
                    assert!(branches.contains(&"left".to_owned()));
                    assert!(branches.contains(&"right".to_owned()));
                    engine.snapshot().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    opened = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(opened, "parallel waits did not open");
            engine.shutdown().await.unwrap();
            run
        };
        let engine = Engine::local(dir.path()).await.unwrap();
        let state = engine.inspect(run).await.unwrap();
        let branches: Vec<_> = state
            .scopes
            .values()
            .filter_map(|scope| match &scope.role {
                crate::domain::ScopeRole::ParallelBranch { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(branches.len(), 2);
        assert!(state.waits.values().filter(|wait| wait.pending).count() >= 1);
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
        engine
            .signal(
                run,
                EventId::generate(),
                "approval",
                "k2",
                Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))])),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(10))
            .await
            .unwrap();
        let Value::Array(items) = output else {
            panic!("tuple");
        };
        assert_eq!(items.len(), 2);
        let state = engine.inspect(run).await.unwrap();
        let branch_count = state
            .scopes
            .values()
            .filter(|scope| matches!(scope.role, crate::domain::ScopeRole::ParallelBranch { .. }))
            .count();
        assert_eq!(
            branch_count, 2,
            "snapshot restore must not duplicate branches"
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_during_saga_compensates() {
        let _guard = COMPENSATION_TEST.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::local(dir.path()).await.unwrap();
        let mut fail_order = order();
        if let Value::Object(fields) = &mut fail_order {
            fields.insert("fail_after_payment".to_owned(), Value::Bool(true));
        }
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/saga.yaml"),
                &catalog(),
                fail_order,
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let state = engine.inspect(run).await.unwrap();
            if state
                .obligations
                .iter()
                .any(|item| item.handler == "inventory.release")
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("reserve obligation never registered");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if let Err(err) = engine.cancel(run, "stop").await {
            let state = engine.inspect(run).await.unwrap();
            let inactive = !matches!(state.runs.get(&run).unwrap().status, RunStatus::Active);
            assert!(
                inactive || err.to_string().contains("not active"),
                "cancel failed: {err}"
            );
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let state = engine.inspect(run).await.unwrap();
            let failed = matches!(
                &state.runs.get(&run).unwrap().status,
                RunStatus::Failed { .. }
            );
            let compensated = !state.obligations.is_empty()
                && state.obligations.iter().all(|item| {
                    matches!(item.status, crate::domain::ObligationStatus::Compensated)
                });
            if failed && compensated {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("cancel did not compensate");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compensation_resumes_after_restart() {
        let _guard = COMPENSATION_TEST.lock().await;
        SLOW_RELEASE.store(true, Ordering::SeqCst);
        let dir = tempfile::tempdir().unwrap();
        let run = {
            let engine = Engine::local(dir.path()).await.unwrap();
            let run = engine
                .start_yaml(
                    include_str!("../../docs/specs/v1/examples/saga.yaml"),
                    &catalog(),
                    Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                        ("fail_after_payment".to_owned(), Value::Bool(true)),
                    ])),
                )
                .await
                .unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                let state = engine.inspect(run).await.unwrap();
                let refunded = state.obligations.iter().any(|item| {
                    item.handler == "payment.refund"
                        && matches!(item.status, crate::domain::ObligationStatus::Compensated)
                });
                let release_open = state.obligations.iter().any(|item| {
                    item.handler == "inventory.release"
                        && !matches!(item.status, crate::domain::ObligationStatus::Compensated)
                });
                if refunded && release_open {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    SLOW_RELEASE.store(false, Ordering::SeqCst);
                    panic!("did not catch mid-compensation");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            engine.shutdown().await.unwrap();
            run
        };
        SLOW_RELEASE.store(false, Ordering::SeqCst);
        let engine = Engine::local(dir.path()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let state = engine.inspect(run).await.unwrap();
            let done = !state.obligations.is_empty()
                && state.obligations.iter().all(|item| {
                    matches!(item.status, crate::domain::ObligationStatus::Compensated)
                });
            if done {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("compensation did not resume");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let events = engine.history(run).await.unwrap();
        let releases = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    crate::domain::DomainEvent::CompensationStarted { handler, .. } if handler == "inventory.release"
                )
            })
            .count();
        assert_eq!(releases, 1);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn logical_restore_suspends_until_authorized() {
        let src = tempfile::tempdir().unwrap();
        let backup = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let run = {
            let engine = Engine::local(src.path()).await.unwrap();
            let run = engine
                .start_yaml(
                    include_str!("../../docs/specs/v1/examples/events.yaml"),
                    &catalog(),
                    Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                )
                .await
                .unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let state = engine.inspect(run).await.unwrap();
                if state.waits.values().any(|wait| wait.pending) {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("wait never opened");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            engine.shutdown().await.unwrap();
            Engine::backup(src.path(), backup.path()).unwrap();
            run
        };
        Engine::restore(backup.path(), dest.path(), "disaster").unwrap();
        let identity = std::fs::read_to_string(dest.path().join("identity.json")).unwrap();
        assert!(identity.contains("graphrun-restored-"));
        assert!(!dest.path().join("member.redb").exists());
        let engine = Engine::local(dest.path()).await.unwrap();
        let state = engine.inspect(run).await.unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Active
        ));
        assert!(state.recovery.as_ref().is_some_and(|hold| !hold.authorized));
        let err = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog(),
                order(),
            )
            .await
            .expect_err("start must stay suspended");
        assert!(err.to_string().contains("suspended"));
        engine
            .acknowledge_recovery("operator authorized restore")
            .await
            .unwrap();
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

        assert!(!dest.path().join("restored-domain.json").exists());
        let identity_path = dest.path().join("identity.json");
        let identity = std::fs::read(&identity_path).unwrap();
        let db_path = dest.path().join("member.redb");
        std::fs::remove_file(&db_path).unwrap();
        let err = Engine::local(dest.path()).await.err().unwrap();
        assert_eq!(err.kind, ErrorKind::FailedPrecondition);
        assert!(!db_path.exists());
        assert_eq!(std::fs::read(identity_path).unwrap(), identity);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_rejects_corrupt_and_legacy_backups_without_writing_destination() {
        let source = tempfile::tempdir().unwrap();
        let backup = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let engine = Engine::local(source.path()).await.unwrap();
        engine.shutdown().await.unwrap();
        Engine::backup(source.path(), backup.path()).unwrap();
        let manifest: LogicalBackupManifest =
            serde_json::from_slice(&std::fs::read(backup.path().join("manifest.json")).unwrap())
                .unwrap();
        let snapshot_path = backup.path().join(&manifest.snapshot);
        let mut corrupt = std::fs::read(&snapshot_path).unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&snapshot_path, &corrupt).unwrap();
        let error = Engine::restore(backup.path(), destination.path(), "recovery")
            .expect_err("corrupt snapshot must be rejected");
        assert_eq!(error.kind, ErrorKind::FailedPrecondition);
        assert_eq!(std::fs::read(&snapshot_path).unwrap(), corrupt);
        assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);

        std::fs::write(
            backup.path().join("manifest.json"),
            br#"{"format":"graphrun.backup/v1","kind":"logical-domain"}"#,
        )
        .unwrap();
        let error = Engine::restore(backup.path(), destination.path(), "recovery")
            .expect_err("old backup format must be rejected");
        assert_eq!(error.kind, ErrorKind::FailedPrecondition);
        assert_eq!(std::fs::read_dir(destination.path()).unwrap().count(), 0);
    }
}
