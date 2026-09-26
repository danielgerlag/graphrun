#![allow(clippy::result_large_err)]

use crate::domain::{self, Command, State};
use crate::error::{Error, Result};
use crate::record_store;
use crate::schedule::{RunSchedule, retention_deadline};
use crate::snapshot_framing::{
    SnapshotManifest, SnapshotStream, copy_payload, verify_snapshot, write_snapshot,
};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, Membership,
    RaftLogId, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::io::{self, Read};
use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(test, feature = "fault-injection"))]
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = SnapshotStream,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftRequest {
    pub command: Command,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RaftResponse {
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<crate::error::ErrorKind>,
    #[serde(default)]
    pub command_result: Option<crate::publication::CommandResult>,
    #[serde(default)]
    pub run_id: Option<crate::ids::RunId>,
}

const META: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("meta");
const LOG: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("log");
const SCHEDULE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("schedule");
const APP_ROWS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("app_records_v2");

#[derive(Clone, Serialize, Deserialize)]
struct StoreManifest {
    format: String,
    reader_floor: u16,
    writer_format: u16,
    active_generation: u64,
    domain_format: String,
    state_record_format: String,
    fragment_format: String,
    event_format: String,
    checkpoint_format: String,
    result_format: String,
}

impl StoreManifest {
    fn new() -> Self {
        Self {
            format: "graphrun.member-store/v2".to_owned(),
            reader_floor: 2,
            writer_format: 2,
            active_generation: 1,
            domain_format: "graphrun.domain/v1".to_owned(),
            state_record_format: record_store::FORMAT.to_owned(),
            fragment_format: record_store::FRAGMENT_FORMAT.to_owned(),
            event_format: crate::history::EVENT_FORMAT.to_owned(),
            checkpoint_format: crate::history::CHECKPOINT_FORMAT.to_owned(),
            result_format: crate::publication::RESULT_FORMAT.to_owned(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.format != "graphrun.member-store/v2"
            || self.reader_floor > 2
            || self.writer_format != 2
            || self.active_generation == 0
            || self.domain_format != "graphrun.domain/v1"
            || self.state_record_format != record_store::FORMAT
            || self.fragment_format != record_store::FRAGMENT_FORMAT
            || self.event_format != crate::history::EVENT_FORMAT
            || self.checkpoint_format != crate::history::CHECKPOINT_FORMAT
            || self.result_format != crate::publication::RESULT_FORMAT
        {
            return Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "unsupported member store or retained record version; migrate explicitly",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SnapshotRegistry {
    format: u16,
    meta: SnapMeta,
    file_name: String,
    sha256: String,
    generation: u64,
    source_generation: u64,
    applied_bytes: u64,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
struct PendingCredits {
    entries: u32,
    bytes: u64,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub struct SnapshotProgress {
    pub applied_bytes: u64,
    pub last_snapshot_bytes: u64,
    pub last_snapshot_applied: u64,
    pub last_snapshot_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ScheduleView {
    pub revision: u64,
    pub runs: Vec<(crate::ids::RunId, RunSchedule)>,
    pub retention_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct SchedulerStats {
    pub ready_index_discovery_reads: u64,
    pub revision: u64,
    pub scheduler_observed_revision: u64,
    pub local_worker_observed_revision: u64,
    pub applied_wakes: u64,
    pub leadership_wakes: u64,
    pub deadline_wakes: u64,
    pub retention_wakes: u64,
    pub worker_wakes: u64,
}

pub(crate) enum ScheduleWake {
    Applied,
    Leadership,
    Deadline,
    Retention,
    Worker,
}

type LogIdT = LogId<u64>;
type VoteT = Vote<u64>;
type EntryT = Entry<TypeConfig>;
type MembershipT = StoredMembership<u64, BasicNode>;
type SnapMeta = SnapshotMeta<u64, BasicNode>;
type StoErr = StorageError<u64>;

fn sto_err(verb: ErrorVerb, err: impl std::fmt::Display) -> StoErr {
    StorageIOError::new(ErrorSubject::Store, verb, AnyError::error(err.to_string())).into()
}

#[cfg(any(test, feature = "fault-injection"))]
struct Cut {
    point: &'static str,
}

#[cfg(any(test, feature = "fault-injection"))]
static CUT: Mutex<Option<Cut>> = Mutex::new(None);

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static LOCAL_CUT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
thread_local! {
    static COMMIT_UNCERTAIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn commit_immediate(txn: redb::WriteTransaction) -> std::result::Result<(), StoErr> {
    txn.commit().map_err(|err| {
        COMMIT_UNCERTAIN.with(|uncertain| uncertain.set(true));
        sto_err(ErrorVerb::Write, err)
    })
}

fn begin_immediate(db: &Database) -> std::result::Result<redb::WriteTransaction, StoErr> {
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_quick_repair(true);
    Ok(txn)
}

#[cfg(any(test, feature = "fault-injection"))]
pub fn inject_cut(point: &'static str) {
    LOCAL_CUT.with(|cell| cell.set(Some(point)));
}

#[cfg(any(test, feature = "fault-injection"))]
pub fn inject_cut_any_thread(point: &'static str) {
    *CUT.lock().unwrap() = Some(Cut { point });
}

#[cfg(any(test, feature = "fault-injection"))]
pub fn clear_cut() {
    LOCAL_CUT.with(|cell| cell.set(None));
    *CUT.lock().unwrap() = None;
}

#[cfg(any(test, feature = "fault-injection"))]
fn after_persist(point: &'static str) -> std::result::Result<(), StoErr> {
    let cut = {
        let local = LOCAL_CUT.with(|cell| cell.get() == Some(point));
        if local {
            true
        } else {
            let guard = CUT.lock().unwrap();
            matches!(guard.as_ref(), Some(cut) if cut.point == point)
        }
    };
    let env_hit = {
        #[cfg(feature = "fault-injection")]
        {
            std::env::var("GRAPHUN_FAULT").ok().as_deref() == Some(point)
        }
        #[cfg(not(feature = "fault-injection"))]
        {
            false
        }
    };
    if cut || env_hit {
        LOCAL_CUT.with(|cell| cell.set(None));
        *CUT.lock().unwrap() = None;
        COMMIT_UNCERTAIN.with(|uncertain| uncertain.set(true));
        return Err(sto_err(ErrorVerb::Write, format!("fault cut {point}")));
    }
    Ok(())
}

#[cfg(not(any(test, feature = "fault-injection")))]
fn after_persist(_point: &'static str) -> std::result::Result<(), StoErr> {
    Ok(())
}

enum Req {
    SaveVote(VoteT, oneshot::Sender<std::result::Result<(), StoErr>>),
    ReadVote(oneshot::Sender<std::result::Result<Option<VoteT>, StoErr>>),
    Append(
        Vec<EntryT>,
        oneshot::Sender<std::result::Result<(), StoErr>>,
    ),
    Truncate(LogIdT, oneshot::Sender<std::result::Result<(), StoErr>>),
    Purge(LogIdT, oneshot::Sender<std::result::Result<(), StoErr>>),
    LogState(oneshot::Sender<std::result::Result<LogState<TypeConfig>, StoErr>>),
    GetLogs(
        u64,
        u64,
        oneshot::Sender<std::result::Result<Vec<EntryT>, StoErr>>,
    ),
    AppliedState(oneshot::Sender<std::result::Result<(Option<LogIdT>, MembershipT), StoErr>>),
    UnappliedUsage(oneshot::Sender<std::result::Result<(u32, u64), StoErr>>),
    SnapshotProgress(oneshot::Sender<std::result::Result<SnapshotProgress, StoErr>>),
    PreflightAppend(
        Vec<(u64, u64)>,
        oneshot::Sender<std::result::Result<(u32, u64), StoErr>>,
    ),
    Apply(
        Vec<EntryT>,
        oneshot::Sender<std::result::Result<Vec<RaftResponse>, StoErr>>,
    ),
    BuildSnapshot(oneshot::Sender<std::result::Result<Snapshot<TypeConfig>, StoErr>>),
    SnapshotBuilt(
        std::result::Result<(Snapshot<TypeConfig>, SnapshotRegistry), StoErr>,
        oneshot::Sender<std::result::Result<Snapshot<TypeConfig>, StoErr>>,
    ),
    InstallSnapshot(
        SnapMeta,
        PathBuf,
        oneshot::Sender<std::result::Result<(), StoErr>>,
    ),
    CurrentSnapshot(oneshot::Sender<std::result::Result<Option<Snapshot<TypeConfig>>, StoErr>>),
    QueryState(oneshot::Sender<State>),
    ScheduleView(oneshot::Sender<std::result::Result<ScheduleView, StoErr>>),
    QueryWatermark(oneshot::Sender<u64>),
    SetClockFault(bool, oneshot::Sender<std::result::Result<(), StoErr>>),
    InstallDomain(Box<State>, oneshot::Sender<std::result::Result<(), StoErr>>),
    Shutdown,
}

#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<Req>,
    clock: Arc<crate::clock::ClockAuthority>,
    watermark: Arc<AtomicU64>,
    schedule_watch: watch::Sender<u64>,
    schedule_reads: Arc<AtomicU64>,
    scheduler_observed: Arc<AtomicU64>,
    local_worker_observed: Arc<AtomicU64>,
    schedule_wakes: Arc<[AtomicU64; 5]>,
    snapshot_dir: Arc<PathBuf>,
    inbound_snapshot: Arc<Semaphore>,
}

impl StorageHandle {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        Self::open_inner(path, false, false)
    }

    pub(crate) fn open_member(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        Self::open_inner(path, false, true)
    }

    pub(crate) fn open_restored(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        Self::open_inner(path, true, false)
    }

    fn open_inner(
        path: impl AsRef<Path>,
        restoring: bool,
        clustered: bool,
    ) -> Result<(Self, JoinHandle<()>)> {
        let path = path.as_ref().to_path_buf();
        let mut clock_faulted = false;
        let mut initial_watermark = 0;
        if path.exists() {
            let db = ReadOnlyDatabase::open(&path).map_err(|err| {
                Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string())
            })?;
            let format: Option<String> = load_json(&db, "publication_start_format");
            if format.as_deref() != Some("graphrun.publication-store/v1") {
                return Err(Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "incompatible pre-publication store; migrate explicitly (directory left untouched)",
                ));
            }
            let state = load_json::<_, State>(&db, "domain").ok_or_else(|| {
                Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "domain missing or unreadable (directory left untouched)",
                )
            })?;
            validate_history_store(&state)?;
            let store_manifest = verified_store_manifest(&db)?;
            verify_app_mirror(&db, &state, store_manifest.active_generation)?;
            if let Some(registry) = optional_json::<_, SnapshotRegistry>(&db, "snapshot_registry")?
            {
                if registry.generation != store_manifest.active_generation {
                    return Err(Error::new(
                        crate::error::ErrorKind::FailedPrecondition,
                        "snapshot registry is not in the active generation",
                    ));
                }
                let snapshots = path
                    .parent()
                    .map(|parent| parent.join("snapshots"))
                    .unwrap_or_else(|| PathBuf::from("snapshots"));
                verify_registry(&snapshots, &registry).map_err(|err| {
                    Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string())
                })?;
            } else if load_bytes(&db, "snapshot_meta").is_some()
                || load_bytes(&db, "snapshot_data").is_some()
            {
                return Err(Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "pre-file snapshot store is incompatible; directory left untouched",
                ));
            }
            initial_watermark = state.engine_time_watermark_ms;
            if let Some(bytes) = load_bytes(&db, "clock_fault") {
                clock_faulted = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::new(
                        crate::error::ErrorKind::FailedPrecondition,
                        "clock fault record is corrupt",
                    )
                })?;
            }
        } else if !restoring
            && path
                .parent()
                .is_some_and(|parent| parent.join("identity.json").exists())
        {
            return Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "member store missing for existing identity (directory left untouched)",
            ));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| Error::invalid(err.to_string()))?;
        }
        let (tx, rx) = mpsc::channel(crate::limits::MAX_QUEUED_COMMANDS as usize);
        let weak_tx = tx.downgrade();
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let snapshot_dir = Arc::new(
            path.parent()
                .map(|parent| parent.join("snapshots"))
                .unwrap_or_else(|| PathBuf::from("snapshots")),
        );
        let (schedule_watch, _) = watch::channel(0);
        let schedule_reads = Arc::new(AtomicU64::new(0));
        let scheduler_observed = Arc::new(AtomicU64::new(0));
        let local_worker_observed = Arc::new(AtomicU64::new(0));
        let schedule_wakes = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let watermark = Arc::new(AtomicU64::new(initial_watermark));
        let stored_watermark = watermark.clone();
        let stored_schedule_watch = schedule_watch.clone();
        let stored_schedule_reads = schedule_reads.clone();
        let handle = std::thread::Builder::new()
            .name("graphrun-storage".into())
            .spawn(move || {
                storage_thread(
                    path,
                    rx,
                    weak_tx,
                    ready_tx,
                    stored_watermark,
                    stored_schedule_watch,
                    stored_schedule_reads,
                    clustered,
                )
            })
            .map_err(|err| Error::invalid(err.to_string()))?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok((
                Self {
                    tx,
                    clock: Arc::new(crate::clock::ClockAuthority::new(clock_faulted)),
                    watermark,
                    schedule_watch,
                    schedule_reads,
                    scheduler_observed,
                    local_worker_observed,
                    schedule_wakes,
                    snapshot_dir,
                    inbound_snapshot: Arc::new(Semaphore::new(1)),
                },
                handle,
            )),
            Ok(Err(err)) => Err(Error::new(crate::error::ErrorKind::FailedPrecondition, err)),
            Err(_) => Err(Error::invalid("storage thread exited before opening")),
        }
    }

    pub fn log_store(&self) -> LogStore {
        LogStore {
            handle: self.clone(),
        }
    }

    pub fn state_machine(&self) -> StateMachineStore {
        StateMachineStore {
            handle: self.clone(),
        }
    }

    pub fn clock(&self) -> &crate::clock::ClockAuthority {
        &self.clock
    }

    pub(crate) fn subscribe_schedule(&self) -> watch::Receiver<u64> {
        self.schedule_watch.subscribe()
    }

    pub(crate) async fn schedule_view(&self) -> Result<ScheduleView> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::ScheduleView(tx))
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage stopped"))?;
        rx.await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage dropped"))?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))
    }

    pub fn schedule_discovery_reads(&self) -> u64 {
        self.schedule_reads.load(Ordering::Relaxed)
    }

    pub fn scheduler_stats(&self) -> SchedulerStats {
        SchedulerStats {
            ready_index_discovery_reads: self.schedule_discovery_reads(),
            revision: self.schedule_revision(),
            scheduler_observed_revision: self.scheduler_observed_revision(),
            local_worker_observed_revision: self.local_worker_observed_revision(),
            applied_wakes: self.schedule_wakes[0].load(Ordering::Relaxed),
            leadership_wakes: self.schedule_wakes[1].load(Ordering::Relaxed),
            deadline_wakes: self.schedule_wakes[2].load(Ordering::Relaxed),
            retention_wakes: self.schedule_wakes[3].load(Ordering::Relaxed),
            worker_wakes: self.schedule_wakes[4].load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_schedule_wake(&self, cause: ScheduleWake) {
        let index = match cause {
            ScheduleWake::Applied => 0,
            ScheduleWake::Leadership => 1,
            ScheduleWake::Deadline => 2,
            ScheduleWake::Retention => 3,
            ScheduleWake::Worker => 4,
        };
        self.schedule_wakes[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn schedule_revision(&self) -> u64 {
        *self.schedule_watch.borrow()
    }

    pub fn scheduler_observed_revision(&self) -> u64 {
        self.scheduler_observed.load(Ordering::Acquire)
    }

    pub fn local_worker_observed_revision(&self) -> u64 {
        self.local_worker_observed.load(Ordering::Acquire)
    }

    pub(crate) fn observe_scheduler(&self, revision: u64) {
        self.scheduler_observed
            .fetch_max(revision, Ordering::Release);
    }

    pub(crate) fn observe_local_worker(&self, revision: u64) {
        self.local_worker_observed
            .fetch_max(revision, Ordering::Release);
    }

    pub fn engine_watermark_cached(&self) -> u64 {
        self.watermark.load(Ordering::SeqCst)
    }

    pub async fn engine_watermark(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Req::QueryWatermark(tx)).await.map_err(|_| {
            Error::new(
                crate::error::ErrorKind::Unavailable,
                "storage thread stopped",
            )
        })?;
        rx.await.map_err(|_| {
            Error::new(
                crate::error::ErrorKind::Unavailable,
                "storage thread dropped",
            )
        })
    }

    pub async fn persist_clock_fault(&self, faulted: bool) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::SetClockFault(faulted, tx))
            .await
            .map_err(|_| {
                Error::new(
                    crate::error::ErrorKind::Unavailable,
                    "storage thread stopped",
                )
            })?;
        rx.await
            .map_err(|_| {
                Error::new(
                    crate::error::ErrorKind::Unavailable,
                    "storage thread dropped",
                )
            })?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))
    }

    pub async fn query_state(&self) -> State {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::QueryState(tx))
            .await
            .expect("storage thread stopped during state query");
        rx.await.expect("storage thread dropped state query")
    }

    pub async fn install_domain(&self, state: State) -> Result<()> {
        validate_history_store(&state)?;
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::InstallDomain(Box::new(state), tx))
            .await
            .map_err(|_| Error::invalid("storage thread stopped"))?;
        rx.await
            .map_err(|_| Error::invalid("storage thread dropped"))?
            .map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn last_applied_index(&self) -> u64 {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::AppliedState(tx))
            .await
            .expect("storage thread stopped during applied-position query");
        match rx.await {
            Ok(Ok((Some(id), _))) => id.index,
            Ok(Ok((None, _))) => 0,
            Ok(Err(err)) => panic!("applied-position query failed: {err}"),
            Err(err) => panic!("storage thread dropped applied-position query: {err}"),
        }
    }

    pub async fn applied_membership(&self) -> Result<StoredMembership<u64, BasicNode>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Req::AppliedState(tx)).await.map_err(|_| {
            Error::new(
                crate::error::ErrorKind::Unavailable,
                "storage thread stopped",
            )
        })?;
        let (_, membership) = rx
            .await
            .map_err(|_| {
                Error::new(
                    crate::error::ErrorKind::Unavailable,
                    "storage thread dropped",
                )
            })?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))?;
        Ok(membership)
    }

    pub async fn unapplied_usage(&self) -> Result<(u32, u64)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::UnappliedUsage(tx))
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage stopped"))?;
        rx.await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage dropped"))?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))
    }

    pub async fn snapshot_progress(&self) -> Result<SnapshotProgress> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::SnapshotProgress(tx))
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage stopped"))?;
        rx.await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage dropped"))?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))
    }

    pub async fn projected_append_usage(&self, entries: Vec<(u64, u64)>) -> Result<(u32, u64)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::PreflightAppend(entries, tx))
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage stopped"))?;
        rx.await
            .map_err(|_| Error::new(crate::error::ErrorKind::Unavailable, "storage dropped"))?
            .map_err(|err| Error::new(crate::error::ErrorKind::Unavailable, err.to_string()))
    }

    pub async fn applied_members(&self) -> Result<BTreeMap<u64, String>> {
        Ok(self
            .applied_membership()
            .await?
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect())
    }

    pub fn shutdown(&self) {
        match self.tx.try_send(Req::Shutdown) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(request)) => {
                let sender = self.tx.clone();
                std::thread::spawn(move || {
                    if let Err(error) = sender.blocking_send(request) {
                        tracing::error!(%error, "storage shutdown queue stopped");
                    }
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("storage shutdown requested after thread stopped");
            }
        }
    }
}

pub fn load_domain_readonly(path: impl AsRef<Path>) -> Result<State> {
    let db = ReadOnlyDatabase::open(path.as_ref())
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    let format: Option<String> = load_json(&db, "publication_start_format");
    if format.as_deref() != Some("graphrun.publication-store/v1") {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "incompatible store format",
        ));
    }
    let state = load_json(&db, "domain").ok_or_else(|| {
        Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "domain missing or unreadable",
        )
    })?;
    validate_history_store(&state)?;
    let store_manifest = verified_store_manifest(&db)?;
    verify_app_mirror(&db, &state, store_manifest.active_generation)?;
    if let Some(registry) = optional_json::<_, SnapshotRegistry>(&db, "snapshot_registry")? {
        if registry.generation != store_manifest.active_generation {
            return Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "snapshot registry is not in the active generation",
            ));
        }
        let snapshots = path
            .as_ref()
            .parent()
            .map(|parent| parent.join("snapshots"))
            .unwrap_or_else(|| PathBuf::from("snapshots"));
        verify_registry(&snapshots, &registry).map_err(|err| {
            Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string())
        })?;
    } else if load_bytes(&db, "snapshot_meta").is_some()
        || load_bytes(&db, "snapshot_data").is_some()
    {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "pre-file snapshot store is incompatible; directory left untouched",
        ));
    }
    Ok(state)
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CompactOutcome {
    pub passes: u32,
    pub complete: bool,
}

pub fn compact_offline(path: impl AsRef<Path>) -> Result<CompactOutcome> {
    let mut db = Database::open(path.as_ref()).map_err(|err| {
        Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            format!("offline compaction requires an existing, unowned member store: {err}"),
        )
    })?;
    let manifest = verified_store_manifest(&db)?;
    let state: State = required_json(&db, "domain")?;
    validate_history_store(&state)?;
    verify_app_mirror(&db, &state, manifest.active_generation)?;
    let mut passes = 0;
    while passes < 16 {
        let compacted = db.compact().map_err(|err| {
            Error::new(
                crate::error::ErrorKind::Unavailable,
                format!("redb compaction outcome uncertain; reopen before retry: {err}"),
            )
        })?;
        if !compacted {
            return Ok(CompactOutcome {
                passes,
                complete: true,
            });
        }
        passes += 1;
    }
    Ok(CompactOutcome {
        passes,
        complete: false,
    })
}

pub(crate) fn validate_history_store(state: &State) -> Result<()> {
    for run in state.runs.keys() {
        let incompatible = || {
            Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                format!(
                    "run {} has incompatible pre-history records; migrate explicitly (directory left untouched)",
                    run.to_hex()
                ),
            )
        };
        let (history, records) = match (
            state.history.get(run),
            state.history_records.get(run),
            state.history_dependencies.get(run),
        ) {
            (Some(history), Some(records), Some(_))
                if !history.is_empty() && records.len() == history.len() =>
            {
                (history, records)
            }
            _ => return Err(incompatible()),
        };
        let history_len = history.len() as u64;
        if records.iter().enumerate().any(|(i, record)| {
            record.format != crate::history::EVENT_FORMAT
                || record.run != *run
                || record.sequence != i as u64 + 1
        }) || state.checkpoints.get(run).is_some_and(|checkpoint| {
            checkpoint.format != crate::history::CHECKPOINT_FORMAT
                || checkpoint.through_run_sequence == 0
                || checkpoint.through_run_sequence > history_len
        }) {
            return Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                format!(
                    "run {} has unsupported history or checkpoint format; migrate explicitly (directory left untouched)",
                    run.to_hex()
                ),
            ));
        }
    }
    Ok(())
}

fn service_import_io(
    req: Req,
    db: &Database,
    applied: Option<LogIdT>,
    membership: &MembershipT,
    purged: &mut Option<LogIdT>,
    watermark: u64,
) -> Option<Req> {
    match req {
        Req::SaveVote(vote, tx) => {
            let _ = tx.send(put_json(db, "vote", &vote));
        }
        Req::ReadVote(tx) => {
            let _ = tx.send(optional_json(db, "vote").map_err(|err| sto_err(ErrorVerb::Read, err)));
        }
        Req::Append(entries, tx) => {
            let _ = tx.send(admit_and_append(db, applied, &entries));
        }
        Req::Truncate(id, tx) => {
            let _ = tx.send(truncate_logs(db, id.index));
        }
        Req::Purge(id, tx) => {
            let result = purge_logs(db, id.index, &id);
            if result.is_ok() {
                *purged = Some(id);
            }
            let _ = tx.send(result);
        }
        Req::LogState(tx) => {
            let _ = tx.send(log_state(db, *purged));
        }
        Req::GetLogs(start, end, tx) => {
            let _ = tx.send(get_logs(db, start, end));
        }
        Req::AppliedState(tx) => {
            let _ = tx.send(Ok((applied, membership.clone())));
        }
        Req::UnappliedUsage(tx) => {
            let _ = tx.send(persisted_credits(db).map(|credits| (credits.entries, credits.bytes)));
        }
        Req::PreflightAppend(entries, tx) => {
            let _ = tx.send(
                projected_append_usage(db, applied, &entries)
                    .map(|credits| (credits.entries, credits.bytes)),
            );
        }
        Req::SnapshotProgress(tx) => {
            let _ = tx.send(read_snapshot_progress(db));
        }
        Req::QueryWatermark(tx) => {
            let _ = tx.send(watermark);
        }
        Req::SetClockFault(faulted, tx) => {
            let _ = tx.send(put_json(db, "clock_fault", &faulted));
        }
        other => return Some(other),
    }
    None
}

fn storage_thread(
    path: PathBuf,
    mut rx: mpsc::Receiver<Req>,
    requests: mpsc::WeakSender<Req>,
    ready: std_mpsc::Sender<std::result::Result<(), String>>,
    watermark: Arc<AtomicU64>,
    schedule_watch: watch::Sender<u64>,
    schedule_reads: Arc<AtomicU64>,
    clustered: bool,
) {
    let db = match Database::create(&path) {
        Ok(db) => Arc::new(db),
        Err(err) => {
            let _ = ready.send(Err(format!("redb open failed: {err}")));
            return;
        }
    };
    if load_json::<_, String>(db.as_ref(), "publication_start_format").is_none() {
        if let Err(err) = initialize_publication_store(&db) {
            let _ = ready.send(Err(err.to_string()));
            return;
        }
    }
    if let Err(err) = verified_store_manifest(db.as_ref()) {
        let _ = ready.send(Err(err.to_string()));
        return;
    }
    let loaded = (|| -> Result<_> {
        let last_purged = optional_json::<_, Option<LogIdT>>(db.as_ref(), "last_purged")?.flatten();
        let last_applied =
            optional_json::<_, Option<LogIdT>>(db.as_ref(), "last_applied")?.flatten();
        let membership = optional_json::<_, MembershipT>(db.as_ref(), "membership")?
            .unwrap_or_else(|| StoredMembership::new(None, Membership::new(vec![], None)));
        let domain: State = required_json(db.as_ref(), "domain")?;
        validate_history_store(&domain)?;
        let manifest = verified_store_manifest(db.as_ref())?;
        verify_app_mirror(db.as_ref(), &domain, manifest.active_generation)?;
        optional_json::<_, VoteT>(db.as_ref(), "vote")?;
        Ok((last_purged, last_applied, membership, domain))
    })();
    let (mut last_purged, mut last_applied, mut last_membership, mut domain) = match loaded {
        Ok(loaded) => loaded,
        Err(err) => {
            let _ = ready.send(Err(err.to_string()));
            return;
        }
    };
    let Some(mut schedule_revision): Option<u64> = load_json(db.as_ref(), "schedule_revision")
    else {
        let _ = ready.send(Err("schedule index is missing or corrupt".to_owned()));
        return;
    };
    let snapshot_dir = path
        .parent()
        .map(|parent| parent.join("snapshots"))
        .unwrap_or_else(|| PathBuf::from("snapshots"));
    let mut snapshot: Option<SnapshotRegistry> = match load_bytes(db.as_ref(), "snapshot_registry")
    {
        Some(bytes) => {
            let result = serde_json::from_slice::<SnapshotRegistry>(&bytes)
                .map_err(|err| sto_err(ErrorVerb::Read, err))
                .and_then(|registry| {
                    verify_registry(&snapshot_dir, &registry)?;
                    Ok(registry)
                });
            match result {
                Ok(registry) => Some(registry),
                Err(err) => {
                    let _ = ready.send(Err(format!("active snapshot unavailable: {err}")));
                    return;
                }
            }
        }
        None => {
            if load_bytes(db.as_ref(), "snapshot_meta").is_some()
                || load_bytes(db.as_ref(), "snapshot_data").is_some()
            {
                let _ = ready.send(Err(
                    "pre-file snapshot registry is incompatible; directory left untouched"
                        .to_owned(),
                ));
                return;
            }
            None
        }
    };
    let mut cleanup_pending = match cleanup_staged_snapshots(&snapshot_dir, snapshot.as_ref(), 16) {
        Ok(pending) => pending,
        Err(err) => {
            let _ = ready.send(Err(format!("snapshot cleanup failed: {err}")));
            return;
        }
    };
    let mut gc_pending = true;
    let mut app_gc_pending = true;
    let mut deferred = VecDeque::new();
    let mut snapshot_building = false;
    schedule_watch.send_replace(schedule_revision);
    let _ = ready.send(Ok(()));
    while let Some(req) = deferred.pop_front().or_else(|| rx.blocking_recv()) {
        match req {
            Req::Shutdown => break,
            Req::SaveVote(vote, tx) => {
                let _ = tx.send(put_json(&db, "vote", &vote));
            }
            Req::ReadVote(tx) => {
                let _ = tx.send(
                    optional_json(db.as_ref(), "vote").map_err(|err| sto_err(ErrorVerb::Read, err)),
                );
            }
            Req::Append(entries, tx) => {
                let _ = tx.send(admit_and_append(&db, last_applied, &entries));
            }
            Req::Truncate(log_id, tx) => {
                let result = truncate_logs(&db, log_id.index);
                if result.is_ok() {
                    gc_pending = true;
                }
                let _ = tx.send(result);
            }
            Req::Purge(log_id, tx) => {
                let res = purge_logs(&db, log_id.index, &log_id);
                if res.is_ok() {
                    last_purged = Some(log_id);
                    gc_pending = true;
                }
                let _ = tx.send(res);
            }
            Req::LogState(tx) => {
                let _ = tx.send(log_state(&db, last_purged));
            }
            Req::GetLogs(start, end, tx) => {
                let _ = tx.send(get_logs(&db, start, end));
            }
            Req::AppliedState(tx) => {
                let _ = tx.send(Ok((last_applied, last_membership.clone())));
            }
            Req::UnappliedUsage(tx) => {
                let _ =
                    tx.send(persisted_credits(&db).map(|credits| (credits.entries, credits.bytes)));
            }
            Req::SnapshotProgress(tx) => {
                let _ = tx.send(read_snapshot_progress(&db));
            }
            Req::PreflightAppend(entries, tx) => {
                let _ = tx.send(
                    projected_append_usage(&db, last_applied, &entries)
                        .map(|credits| (credits.entries, credits.bytes)),
                );
            }
            Req::Apply(entries, tx) => {
                let previous_revision = schedule_revision;
                let res = apply_entries(
                    &db,
                    &mut last_applied,
                    &mut last_membership,
                    &mut domain,
                    &mut schedule_revision,
                    entries,
                );
                if res.is_ok() {
                    watermark.store(domain.engine_time_watermark_ms, Ordering::SeqCst);
                    if schedule_revision != previous_revision {
                        schedule_watch.send_replace(schedule_revision);
                    }
                }
                let _ = tx.send(res);
            }
            Req::BuildSnapshot(tx) => {
                if snapshot_building {
                    let _ = tx.send(Err(sto_err(
                        ErrorVerb::Write,
                        "snapshot build is already in progress",
                    )));
                    continue;
                }
                let Some(sender) = requests.upgrade() else {
                    let _ = tx.send(Err(sto_err(ErrorVerb::Write, "storage owner stopped")));
                    continue;
                };
                snapshot_building = true;
                let db = db.clone();
                let path = path.clone();
                let dir = snapshot_dir.clone();
                std::thread::spawn(move || {
                    let result = build_snapshot(&db, &path, &dir, clustered);
                    if sender
                        .blocking_send(Req::SnapshotBuilt(result, tx))
                        .is_err()
                    {
                        tracing::error!("storage owner stopped before snapshot publication");
                    }
                });
            }
            Req::SnapshotBuilt(result, tx) => {
                snapshot_building = false;
                let res = result.and_then(|(snap, registry)| {
                    let active = verified_store_manifest(db.as_ref())
                        .map_err(|err| sto_err(ErrorVerb::Read, err))?
                        .active_generation;
                    if registry.generation != active
                        || snapshot.as_ref().is_some_and(|current| {
                            current.meta.last_log_id > registry.meta.last_log_id
                        })
                    {
                        return Err(sto_err(
                            ErrorVerb::Write,
                            "snapshot built from a superseded generation or applied position",
                        ));
                    }
                    after_persist("after-snapshot-file")?;
                    publish_snapshot_registry(&db, &registry)?;
                    snapshot = Some(registry);
                    after_persist("after-snapshot-activate")?;
                    Ok(snap)
                });
                let _ = tx.send(res);
                cleanup_pending = true;
            }
            Req::InstallSnapshot(meta, data, tx) => {
                let previous_applied = last_applied;
                let previous_membership = last_membership.clone();
                let previous_watermark = domain.engine_time_watermark_ms;
                let res = install_snapshot(
                    &db,
                    &path,
                    &snapshot_dir,
                    &mut last_applied,
                    &mut last_membership,
                    &mut domain,
                    &mut snapshot,
                    &mut schedule_revision,
                    meta,
                    data,
                    clustered,
                    || {
                        for _ in 0..32 {
                            if deferred.len() >= crate::limits::MAX_QUEUED_COMMANDS as usize {
                                break;
                            }
                            let Ok(request) = rx.try_recv() else {
                                break;
                            };
                            if let Some(waiting) = service_import_io(
                                request,
                                &db,
                                previous_applied,
                                &previous_membership,
                                &mut last_purged,
                                previous_watermark,
                            ) {
                                deferred.push_back(waiting);
                            }
                            if COMMIT_UNCERTAIN.with(|uncertain| uncertain.get()) {
                                return Err(sto_err(
                                    ErrorVerb::Write,
                                    "storage commit outcome uncertain during import",
                                ));
                            }
                        }
                        verify_snapshot_headroom(&snapshot_dir, &path, clustered)?;
                        Ok(())
                    },
                );
                if res.is_ok() {
                    watermark.store(domain.engine_time_watermark_ms, Ordering::SeqCst);
                    schedule_watch.send_replace(schedule_revision);
                    gc_pending = true;
                    app_gc_pending = true;
                }
                let _ = tx.send(res);
                cleanup_pending = true;
            }
            Req::CurrentSnapshot(tx) => {
                let out = snapshot
                    .as_ref()
                    .map(|registry| current_snapshot(&snapshot_dir, registry))
                    .transpose();
                let _ = tx.send(out);
            }
            Req::QueryState(tx) => {
                let _ = tx.send(domain.clone());
            }
            Req::ScheduleView(tx) => {
                schedule_reads.fetch_add(1, Ordering::Relaxed);
                let _ = tx.send(load_schedule_view(&db));
            }
            Req::QueryWatermark(tx) => {
                let _ = tx.send(domain.engine_time_watermark_ms);
            }
            Req::SetClockFault(faulted, tx) => {
                let _ = tx.send(put_json(&db, "clock_fault", &faulted));
            }
            Req::InstallDomain(next, tx) => {
                let mut next = *next;
                next.engine_time_watermark_ms = next
                    .engine_time_watermark_ms
                    .max(domain.engine_time_watermark_ms);
                let res = validate_history_store(&next)
                    .map_err(|err| sto_err(ErrorVerb::Write, err))
                    .and_then(|()| {
                        let generation = verified_store_manifest(db.as_ref())
                            .map_err(|err| sto_err(ErrorVerb::Read, err))?
                            .active_generation;
                        let txn = begin_immediate(&db)?;
                        {
                            let mut meta = txn
                                .open_table(META)
                                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                            let bytes = serde_json::to_vec(&next)
                                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                            meta.insert("domain", bytes.as_slice())
                                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                        }
                        write_app_delta(
                            &txn,
                            &domain,
                            &next,
                            generation,
                            last_applied.map_or(0, |id| id.index).saturating_add(1),
                        )?;
                        let revision = replace_schedule(
                            &txn,
                            &next,
                            schedule_revision,
                            generation,
                            last_applied.map_or(0, |id| id.index),
                        )?;
                        commit_immediate(txn)?;
                        schedule_revision = revision;
                        Ok(())
                    });
                if res.is_ok() {
                    domain = next;
                    watermark.store(domain.engine_time_watermark_ms, Ordering::SeqCst);
                    schedule_watch.send_replace(schedule_revision);
                }
                let _ = tx.send(res);
            }
        }
        if COMMIT_UNCERTAIN.with(|uncertain| uncertain.get()) {
            tracing::error!("storage commit outcome uncertain; member must reopen before writing");
            break;
        }
        if cleanup_pending {
            match cleanup_staged_snapshots(&snapshot_dir, snapshot.as_ref(), 16) {
                Ok(pending) => cleanup_pending = pending,
                Err(err) => {
                    tracing::error!(%err, "snapshot cleanup failed; member must reopen");
                    break;
                }
            }
        }
        if gc_pending {
            match gc_invisible_logs(&db) {
                Ok(more) => gc_pending = more,
                Err(err) => {
                    tracing::error!(%err, "invisible log cleanup failed; member must reopen");
                    break;
                }
            }
        }
        if app_gc_pending {
            match gc_inactive_app_rows(&db) {
                Ok(more) => app_gc_pending = more,
                Err(err) => {
                    tracing::error!(%err, "inactive application generation cleanup failed");
                    break;
                }
            }
        }
    }
}

fn initialize_publication_store(db: &Database) -> std::result::Result<(), StoErr> {
    let txn = begin_immediate(db)?;
    {
        let mut meta = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let format = serde_json::to_vec("graphrun.publication-store/v1")
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let domain =
            serde_json::to_vec(&State::default()).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("publication_start_format", format.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("domain", domain.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let generation = serde_json::to_vec(&1u64).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("active_generation", generation.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let visible_log = serde_json::to_vec(&Option::<LogIdT>::None)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("visible_log_end", visible_log.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let store_manifest = serde_json::to_vec(&StoreManifest::new())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("store_manifest", store_manifest.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert(
            "schedule_revision",
            serde_json::to_vec(&0u64).unwrap().as_slice(),
        )
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert(
            "schedule_retention",
            serde_json::to_vec(&Option::<u64>::None).unwrap().as_slice(),
        )
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let credits = serde_json::to_vec(&PendingCredits::default())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("unapplied_credits", credits.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let progress = SnapshotProgress {
            last_snapshot_ms: crate::time::wall_millis()
                .map_err(|err| sto_err(ErrorVerb::Write, err))?,
            ..SnapshotProgress::default()
        };
        let progress_bytes =
            serde_json::to_vec(&progress).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("snapshot_progress", progress_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    txn.open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.open_table(SCHEDULE)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut app = txn
            .open_table(APP_ROWS)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let rows = record_store::encode(&State::default(), 1, 1)
            .and_then(record_store::frame_records)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        for (key, value) in rows {
            app.insert(key.as_str(), value.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    commit_immediate(txn)
}

fn load_json<D, T>(db: &D, key: &str) -> Option<T>
where
    D: ReadableDatabase,
    T: for<'de> Deserialize<'de>,
{
    let txn = db.begin_read().ok()?;
    let table = txn.open_table(META).ok()?;
    let value = table.get(key).ok()??;
    serde_json::from_slice(value.value()).ok()
}

fn optional_json<D, T>(db: &D, key: &str) -> Result<Option<T>>
where
    D: ReadableDatabase,
    T: for<'de> Deserialize<'de>,
{
    let txn = db
        .begin_read()
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    let table = txn
        .open_table(META)
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    let value = table
        .get(key)
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    value
        .map(|value| {
            serde_json::from_slice(value.value()).map_err(|err| {
                Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    format!("{key} is corrupt or unsupported: {err}"),
                )
            })
        })
        .transpose()
}

fn required_json<D, T>(db: &D, key: &str) -> Result<T>
where
    D: ReadableDatabase,
    T: for<'de> Deserialize<'de>,
{
    optional_json(db, key)?.ok_or_else(|| {
        Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            format!("required {key} missing; directory left untouched"),
        )
    })
}

fn verified_store_manifest<D: ReadableDatabase>(db: &D) -> Result<StoreManifest> {
    let manifest: StoreManifest = required_json(db, "store_manifest")?;
    manifest.validate()?;
    let active: u64 = required_json(db, "active_generation")?;
    if active != manifest.active_generation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "active generation disagrees with store manifest",
        ));
    }
    let progress: SnapshotProgress = required_json(db, "snapshot_progress")?;
    let visible_end: Option<LogIdT> = required_json(db, "visible_log_end")?;
    let applied = optional_json::<_, Option<LogIdT>>(db, "last_applied")?
        .flatten()
        .map_or(0, |id| id.index);
    if progress.last_snapshot_ms == 0
        || progress.last_snapshot_bytes > progress.applied_bytes
        || progress.last_snapshot_applied > applied
    {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "snapshot progress record contradicts applied metadata",
        ));
    }
    let purged = optional_json::<_, Option<LogIdT>>(db, "last_purged")?.flatten();
    if purged.is_some_and(|purged| visible_end.is_some_and(|end| end.index < purged.index)) {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "visible log boundary precedes durable purge",
        ));
    }
    Ok(manifest)
}

fn load_bytes<D: ReadableDatabase>(db: &D, key: &str) -> Option<Vec<u8>> {
    let txn = db.begin_read().ok()?;
    let table = txn.open_table(META).ok()?;
    let value = table.get(key).ok()??;
    Some(value.value().to_vec())
}

fn put_json<T: Serialize>(db: &Database, key: &str, value: &T) -> std::result::Result<(), StoErr> {
    let bytes = serde_json::to_vec(value).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    put_bytes(db, key, &bytes)
}

fn put_bytes(db: &Database, key: &str, bytes: &[u8]) -> std::result::Result<(), StoErr> {
    let txn = begin_immediate(db)?;
    {
        let mut table = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert(key, bytes)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    commit_immediate(txn)?;
    Ok(())
}

fn load_schedule_view(db: &Database) -> std::result::Result<ScheduleView, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(SCHEDULE)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut runs = Vec::new();
    for record in table.iter().map_err(|err| sto_err(ErrorVerb::Read, err))? {
        let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let run = crate::ids::RunId::from_hex(key.value())
            .map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let schedule: RunSchedule =
            serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))?;
        if schedule.format != 1 {
            return Err(sto_err(
                ErrorVerb::Read,
                "unsupported persisted schedule index format",
            ));
        }
        runs.push((run, schedule));
    }
    let meta = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let read = |key| -> std::result::Result<Vec<u8>, StoErr> {
        let value = meta
            .get(key)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
            .ok_or_else(|| sto_err(ErrorVerb::Read, format!("missing {key}")))?;
        Ok(value.value().to_vec())
    };
    let revision = serde_json::from_slice(&read("schedule_revision")?)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let retention_ms = serde_json::from_slice(&read("schedule_retention")?)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let generation: u64 = serde_json::from_slice(&read("active_generation")?)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    if runs.iter().any(|(_, row)| row.generation != generation) {
        return Err(sto_err(
            ErrorVerb::Read,
            "schedule row belongs to an inactive generation",
        ));
    }
    Ok(ScheduleView {
        revision,
        runs,
        retention_ms,
    })
}

fn load_app_generation<D: ReadableDatabase>(
    db: &D,
    generation: u64,
) -> std::result::Result<State, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(APP_ROWS)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let prefix = format!("{generation:016x}/");
    let mut rows = Vec::new();
    for record in table
        .range(prefix.as_str()..)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
    {
        let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        if !key.value().starts_with(&prefix) {
            break;
        }
        rows.push((key.value().to_owned(), value.value().to_vec()));
    }
    let logical =
        record_store::restore_records(rows).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    record_store::decode(logical, generation).map_err(|err| sto_err(ErrorVerb::Read, err))
}

#[cfg(test)]
fn load_app_run<D: ReadableDatabase>(
    db: &D,
    generation: u64,
    run: crate::ids::RunId,
) -> std::result::Result<State, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(APP_ROWS)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let prefixes = [
        format!("{generation:016x}/global/"),
        format!("{generation:016x}/run-{}/", run.to_hex()),
    ];
    let mut rows = Vec::new();
    for prefix in &prefixes {
        for record in table
            .range(prefix.as_str()..)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            if !key.value().starts_with(prefix) {
                break;
            }
            rows.push((key.value().to_owned(), value.value().to_vec()));
        }
    }
    let logical =
        record_store::restore_records(rows).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    record_store::decode(logical, generation).map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn verify_app_mirror<D: ReadableDatabase>(db: &D, state: &State, generation: u64) -> Result<()> {
    let records = load_app_generation(db, generation)
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    let expected = serde_json::to_value(state).map_err(|err| Error::invalid(err.to_string()))?;
    let actual = serde_json::to_value(records).map_err(|err| Error::invalid(err.to_string()))?;
    if actual != expected {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "generation records disagree with the materialized state",
        ));
    }
    Ok(())
}

struct StoredAppRecord {
    value: Vec<u8>,
    keys: Vec<String>,
}

fn stored_app_record(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> std::result::Result<Option<StoredAppRecord>, StoErr> {
    let Some(header) = table
        .get(key)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
    else {
        return Ok(None);
    };
    let mut rows = vec![(key.to_owned(), header.value().to_vec())];
    let mut physical_keys = vec![key.to_owned()];
    let prefix = format!("{key}/fragment/");
    for record in table
        .range(prefix.as_str()..)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
    {
        let (fragment_key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        if !fragment_key.value().starts_with(&prefix) {
            break;
        }
        physical_keys.push(fragment_key.value().to_owned());
        rows.push((fragment_key.value().to_owned(), value.value().to_vec()));
    }
    let mut logical =
        record_store::restore_records(rows).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let value = logical
        .remove(key)
        .ok_or_else(|| sto_err(ErrorVerb::Read, "state record identity missing"))?;
    Ok(Some(StoredAppRecord {
        value,
        keys: physical_keys,
    }))
}

fn write_app_delta(
    txn: &redb::WriteTransaction,
    before: &State,
    after: &State,
    generation: u64,
    revision: u64,
) -> std::result::Result<(), StoErr> {
    let previous = record_store::encode(before, generation, 1)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let next = record_store::encode(after, generation, revision.max(1))
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let mut table = txn
        .open_table(APP_ROWS)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    for (key, old) in &previous {
        if !next.contains_key(key) {
            let stored = stored_app_record(&table, key)?
                .ok_or_else(|| sto_err(ErrorVerb::Read, "required application record missing"))?;
            if !record_store::same_value(&stored.value, old)
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
            {
                return Err(sto_err(
                    ErrorVerb::Read,
                    "application record changed unexpectedly",
                ));
            }
            for physical_key in stored.keys {
                table
                    .remove(physical_key.as_str())
                    .map_err(|err| sto_err(ErrorVerb::Write, err))?;
            }
        }
    }
    for (key, value) in &next {
        if let Some(old) = previous.get(key) {
            if record_store::same_value(old, value).map_err(|err| sto_err(ErrorVerb::Read, err))? {
                continue;
            }
        }
        match (stored_app_record(&table, key)?, previous.get(key)) {
            (Some(stored), Some(old))
                if record_store::same_value(&stored.value, old)
                    .map_err(|err| sto_err(ErrorVerb::Read, err))? =>
            {
                for physical_key in stored.keys {
                    table
                        .remove(physical_key.as_str())
                        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                }
            }
            (None, None) => {}
            _ => {
                return Err(sto_err(
                    ErrorVerb::Read,
                    "application record changed or disappeared",
                ));
            }
        }
        let framed = record_store::frame_records(std::iter::once((key.clone(), value.clone())))
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        for (physical_key, bytes) in framed {
            table
                .insert(physical_key.as_str(), bytes.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct ImportStats {
    transactions: u32,
    records: u64,
    largest_batch_bytes: usize,
    largest_batch_records: usize,
}

fn clear_inactive_generation(
    db: &Database,
    generation: u64,
    after_batch: &mut impl FnMut() -> std::result::Result<(), StoErr>,
) -> std::result::Result<(), StoErr> {
    let prefix = format!("{generation:016x}/");
    loop {
        let txn = begin_immediate(db)?;
        let mut table = txn
            .open_table(APP_ROWS)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let mut keys = Vec::new();
        let mut bytes = 0usize;
        for record in table
            .range(prefix.as_str()..)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let size = key.value().len() + value.value().len();
            if keys.len() == 4_096
                || (!keys.is_empty() && bytes.saturating_add(size) > 4 * 1024 * 1024)
            {
                break;
            }
            keys.push(key.value().to_owned());
            bytes += size;
        }
        for key in &keys {
            table
                .remove(key.as_str())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
        drop(table);
        if keys.is_empty() {
            return Ok(());
        }
        commit_immediate(txn)?;
        after_persist("after-generation-cleanup")?;
        after_batch()?;
    }
}

fn persist_import_batch(
    db: &Database,
    batch: &[(String, Vec<u8>)],
) -> std::result::Result<(), StoErr> {
    let txn = begin_immediate(db)?;
    let mut table = txn
        .open_table(APP_ROWS)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    for (key, value) in batch {
        table
            .insert(key.as_str(), value.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    drop(table);
    commit_immediate(txn)?;
    after_persist("after-generation-batch")
}

fn stage_app_generation(
    db: &Database,
    state: &State,
    generation: u64,
    revision: u64,
    mut between_batches: impl FnMut() -> std::result::Result<(), StoErr>,
) -> std::result::Result<ImportStats, StoErr> {
    clear_inactive_generation(db, generation, &mut between_batches)?;
    let mut stats = ImportStats::default();
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let rows = record_store::encode(state, generation, revision)
        .and_then(record_store::frame_records)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    for (key, value) in rows {
        let size = key.len() + value.len();
        if size > 4 * 1024 * 1024 {
            return Err(sto_err(
                ErrorVerb::Write,
                "single state record exceeds bounded snapshot import batch",
            ));
        }
        if batch.len() == 4_096 || batch_bytes.saturating_add(size) > 4 * 1024 * 1024 {
            persist_import_batch(db, &batch)?;
            between_batches()?;
            stats.transactions += 1;
            stats.largest_batch_bytes = stats.largest_batch_bytes.max(batch_bytes);
            stats.largest_batch_records = stats.largest_batch_records.max(batch.len());
            batch.clear();
            batch_bytes = 0;
        }
        batch_bytes += size;
        batch.push((key, value));
        stats.records += 1;
    }
    if !batch.is_empty() {
        persist_import_batch(db, &batch)?;
        between_batches()?;
        stats.transactions += 1;
        stats.largest_batch_bytes = stats.largest_batch_bytes.max(batch_bytes);
        stats.largest_batch_records = stats.largest_batch_records.max(batch.len());
    }
    verify_app_mirror(db, state, generation).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    Ok(stats)
}

fn gc_inactive_app_rows(db: &Database) -> std::result::Result<bool, StoErr> {
    let active = verified_store_manifest(db)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .active_generation;
    let active_prefix = format!("{active:016x}/");
    let txn = begin_immediate(db)?;
    let mut table = txn
        .open_table(APP_ROWS)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let mut keys = Vec::new();
    let mut bytes = 0usize;
    let mut more = false;
    for record in table
        .range(..active_prefix.as_str())
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
    {
        let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let size = key.value().len() + value.value().len();
        if !select_gc_key(&mut keys, &mut bytes, key.value().to_owned(), size) {
            more = true;
            break;
        }
    }
    if !more && let Some(next) = active.checked_add(1) {
        let next_prefix = format!("{next:016x}/");
        for record in table
            .range(next_prefix.as_str()..)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            let size = key.value().len() + value.value().len();
            if !select_gc_key(&mut keys, &mut bytes, key.value().to_owned(), size) {
                more = true;
                break;
            }
        }
    }
    for key in &keys {
        table
            .remove(key.as_str())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    drop(table);
    if !keys.is_empty() {
        commit_immediate(txn)?;
        after_persist("after-generation-cleanup")?;
    }
    Ok(more)
}

fn update_schedule(
    txn: &redb::WriteTransaction,
    state: &State,
    affected: &BTreeMap<crate::ids::RunId, Option<bool>>,
    current_revision: u64,
    generation: u64,
    applied_index: u64,
    now_ms: u64,
) -> std::result::Result<Option<u64>, StoErr> {
    let mut changed = false;
    {
        let mut table = txn
            .open_table(SCHEDULE)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        for (run, progress) in affected {
            let key = run.to_hex();
            let previous = table
                .get(key.as_str())
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
                .map(|value| serde_json::from_slice::<RunSchedule>(value.value()))
                .transpose()
                .map_err(|err| sto_err(ErrorVerb::Read, err))?;
            if previous.as_ref().is_some_and(|row| row.format != 1) {
                return Err(sto_err(
                    ErrorVerb::Read,
                    "unsupported persisted schedule index format",
                ));
            }
            let next = RunSchedule::from_state(
                state,
                *run,
                progress.unwrap_or(previous.as_ref().is_some_and(|row| row.progress)),
                now_ms,
                generation,
                applied_index,
            );
            if match (&previous, &next) {
                (Some(old), Some(new)) => new.changed_from(old),
                (None, None) => false,
                _ => true,
            } {
                changed = true;
                if let Some(next) = next {
                    let bytes =
                        serde_json::to_vec(&next).map_err(|err| sto_err(ErrorVerb::Write, err))?;
                    table
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                } else {
                    table
                        .remove(key.as_str())
                        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
                }
            }
        }
    }
    let mut meta = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let previous_retention: Option<u64> = {
        let value = meta
            .get("schedule_retention")
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
            .ok_or_else(|| sto_err(ErrorVerb::Read, "missing schedule retention"))?;
        serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))?
    };
    let retention = retention_deadline(state);
    if retention != previous_retention {
        changed = true;
        let bytes = serde_json::to_vec(&retention).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("schedule_retention", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    if changed {
        let revision = current_revision
            .checked_add(1)
            .ok_or_else(|| sto_err(ErrorVerb::Write, "schedule revision exhausted"))?;
        let bytes = serde_json::to_vec(&revision).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("schedule_revision", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        Ok(Some(revision))
    } else {
        Ok(None)
    }
}

fn replace_schedule(
    txn: &redb::WriteTransaction,
    state: &State,
    current_revision: u64,
    generation: u64,
    applied_index: u64,
) -> std::result::Result<u64, StoErr> {
    {
        let mut table = txn
            .open_table(SCHEDULE)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let keys: Vec<String> = table
            .iter()
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
            .map(|record| {
                record
                    .map(|(key, _)| key.value().to_owned())
                    .map_err(|err| sto_err(ErrorVerb::Read, err))
            })
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            table
                .remove(key.as_str())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    let affected = domain::active_runs(state)
        .into_iter()
        .map(|run| (run, Some(true)))
        .collect();
    let revision = update_schedule(
        txn,
        state,
        &affected,
        current_revision,
        generation,
        applied_index,
        state.engine_time_watermark_ms,
    )?;
    if revision.is_none() {
        let revision = current_revision
            .checked_add(1)
            .ok_or_else(|| sto_err(ErrorVerb::Write, "schedule revision exhausted"))?;
        let bytes = serde_json::to_vec(&revision).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        txn.open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?
            .insert("schedule_revision", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        return Ok(revision);
    }
    Ok(revision.expect("schedule revision present"))
}

fn read_credits(txn: &redb::WriteTransaction) -> std::result::Result<PendingCredits, StoErr> {
    let meta = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let value = meta
        .get("unapplied_credits")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "missing unapplied credit ledger"))?;
    serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn persisted_credits(db: &Database) -> std::result::Result<PendingCredits, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let meta = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let value = meta
        .get("unapplied_credits")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "missing unapplied credit ledger"))?;
    serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn read_snapshot_progress(db: &Database) -> std::result::Result<SnapshotProgress, StoErr> {
    required_json(db, "snapshot_progress").map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn visible_log_end(db: &Database) -> std::result::Result<Option<LogIdT>, StoErr> {
    required_json(db, "visible_log_end").map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn persisted_applied(db: &Database) -> std::result::Result<Option<LogIdT>, StoErr> {
    optional_json::<_, Option<LogIdT>>(db, "last_applied")
        .map(|value| value.flatten())
        .map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn persisted_purged(db: &Database) -> std::result::Result<Option<LogIdT>, StoErr> {
    optional_json::<_, Option<LogIdT>>(db, "last_purged")
        .map(|value| value.flatten())
        .map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn projected_append_usage(
    db: &Database,
    applied: Option<LogIdT>,
    entries: &[(u64, u64)],
) -> std::result::Result<PendingCredits, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut projected = persisted_credits(db)?;
    let visible = visible_log_end(db)?;
    let purged = persisted_purged(db)?;
    let log = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut overwritten = BTreeMap::new();
    for &(index, length) in entries {
        if applied.is_some_and(|id| index <= id.index) || purged.is_some_and(|id| index <= id.index)
        {
            continue;
        }
        let previous = match overwritten.insert(index, length) {
            Some(previous) => Some(previous),
            None if visible.is_some_and(|end| index <= end.index) => log
                .get(&index)
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
                .map(|value| value.value().len() as u64),
            None => None,
        };
        if let Some(previous) = previous {
            projected.bytes = projected
                .bytes
                .checked_sub(previous)
                .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied ledger mismatch"))?;
        } else {
            projected.entries = projected
                .entries
                .checked_add(1)
                .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied entry overflow"))?;
        }
        projected.bytes = projected
            .bytes
            .checked_add(length)
            .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied byte overflow"))?;
    }
    Ok(projected)
}

fn write_credits(
    txn: &redb::WriteTransaction,
    credits: PendingCredits,
) -> std::result::Result<(), StoErr> {
    let bytes = serde_json::to_vec(&credits).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?
        .insert("unapplied_credits", bytes.as_slice())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    Ok(())
}

fn remove_pending_credit(
    credits: &mut PendingCredits,
    bytes: usize,
) -> std::result::Result<(), StoErr> {
    credits.entries = credits
        .entries
        .checked_sub(1)
        .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied entry ledger mismatch"))?;
    credits.bytes = credits
        .bytes
        .checked_sub(bytes as u64)
        .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied byte ledger mismatch"))?;
    Ok(())
}

fn visible_in_txn(txn: &redb::WriteTransaction) -> std::result::Result<Option<LogIdT>, StoErr> {
    let meta = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let value = meta
        .get("visible_log_end")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "visible log boundary missing"))?;
    serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))
}

fn release_applied_credits(
    txn: &redb::WriteTransaction,
    from: Option<LogIdT>,
    through: Option<LogIdT>,
) -> std::result::Result<(), StoErr> {
    let Some(through) = through else {
        return Ok(());
    };
    let start = from.map_or(0, |id| id.index.saturating_add(1));
    let Some(visible) = visible_in_txn(txn)? else {
        return Ok(());
    };
    let end = through.index.min(visible.index);
    if start > end {
        return Ok(());
    }
    let mut credits = read_credits(txn)?;
    let log = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    for record in log
        .range(start..=end)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
    {
        let (_, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        remove_pending_credit(&mut credits, value.value().len())?;
    }
    drop(log);
    write_credits(txn, credits)
}

fn recount_unapplied_credits(
    txn: &redb::WriteTransaction,
    applied: Option<LogIdT>,
) -> std::result::Result<(), StoErr> {
    let start = applied.map_or(0, |id| id.index.saturating_add(1));
    let visible = visible_in_txn(txn)?;
    let log = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut credits = PendingCredits::default();
    if let Some(end) = visible.filter(|end| start <= end.index) {
        for record in log
            .range(start..=end.index)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (_, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            credits.entries = credits
                .entries
                .checked_add(1)
                .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied count overflow"))?;
            credits.bytes = credits
                .bytes
                .checked_add(value.value().len() as u64)
                .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied byte overflow"))?;
            if credits.entries > crate::limits::UNAPPLIED_ENTRIES
                || credits.bytes > crate::limits::UNAPPLIED_BYTES
            {
                return Err(sto_err(
                    ErrorVerb::Read,
                    "snapshot leaves an over-limit unapplied log",
                ));
            }
        }
    }
    drop(log);
    write_credits(txn, credits)
}

fn admit_and_append(
    db: &Database,
    last_applied: Option<LogIdT>,
    entries: &[EntryT],
) -> std::result::Result<(), StoErr> {
    let previous_visible = visible_log_end(db)?;
    let purged = persisted_purged(db)?;
    let mut visible = previous_visible;
    let mut appended = BTreeMap::new();
    let txn = begin_immediate(db)?;
    let mut credits = read_credits(&txn)?;
    {
        let mut table = txn
            .open_table(LOG)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        for entry in entries {
            let bytes = serde_json::to_vec(entry).map_err(|err| sto_err(ErrorVerb::Write, err))?;
            let index = entry.get_log_id().index;
            if let Some(last) = visible.or(purged)
                && index > last.index.saturating_add(1)
            {
                return Err(sto_err(ErrorVerb::Write, "append would expose a log gap"));
            }
            if visible.is_none_or(|last| index >= last.index) {
                visible = Some(*entry.get_log_id());
            }
            if last_applied.is_none_or(|id| index > id.index)
                && purged.is_none_or(|id| index > id.index)
            {
                let previous = match appended.insert(index, bytes.len() as u64) {
                    Some(previous) => Some(previous),
                    None if previous_visible.is_some_and(|last| index <= last.index) => table
                        .get(&index)
                        .map_err(|err| sto_err(ErrorVerb::Read, err))?
                        .map(|value| value.value().len() as u64),
                    None => None,
                };
                if let Some(previous) = previous {
                    credits.bytes = credits
                        .bytes
                        .checked_sub(previous)
                        .ok_or_else(|| sto_err(ErrorVerb::Read, "unapplied ledger mismatch"))?;
                } else {
                    credits.entries = credits
                        .entries
                        .checked_add(1)
                        .ok_or_else(|| sto_err(ErrorVerb::Write, "unapplied entry overflow"))?;
                }
                credits.bytes = credits
                    .bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| sto_err(ErrorVerb::Write, "unapplied byte overflow"))?;
                if credits.entries > crate::limits::UNAPPLIED_ENTRIES {
                    return Err(sto_err(
                        ErrorVerb::Write,
                        "unapplied entry credit exhausted",
                    ));
                }
                if credits.bytes > crate::limits::UNAPPLIED_BYTES {
                    return Err(sto_err(ErrorVerb::Write, "unapplied byte credit exhausted"));
                }
            }
            table
                .insert(&index, bytes.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    write_credits(&txn, credits)?;
    let bytes = serde_json::to_vec(&visible).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?
        .insert("visible_log_end", bytes.as_slice())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    commit_immediate(txn)?;
    after_persist("after-log")?;
    Ok(())
}

#[cfg(test)]
fn append_logs(db: &Database, entries: &[EntryT]) -> std::result::Result<(), StoErr> {
    admit_and_append(db, load_json(db, "last_applied"), entries)
}

fn verify_registry(dir: &Path, registry: &SnapshotRegistry) -> std::result::Result<(), StoErr> {
    if registry.format != 1
        || registry.file_name.is_empty()
        || Path::new(&registry.file_name).components().count() != 1
        || registry.file_name.contains('/')
        || registry.file_name.contains('\\')
        || registry.file_name.contains(':')
    {
        return Err(sto_err(ErrorVerb::Read, "unsupported snapshot registry"));
    }
    let (manifest, digest) = verify_snapshot(&dir.join(&registry.file_name))
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let applied = serde_json::to_vec(&registry.meta.last_log_id)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let membership = serde_json::to_vec(&registry.meta.last_membership)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    if manifest.generation != registry.source_generation
        || manifest.applied_json != applied
        || manifest.membership_json != membership
        || !manifest
            .record_formats
            .iter()
            .any(|format| format == "graphrun.domain/v1")
        || !manifest
            .record_formats
            .iter()
            .any(|format| format == record_store::FORMAT)
        || !manifest
            .record_formats
            .iter()
            .any(|format| format == record_store::FRAGMENT_FORMAT)
        || manifest.record_formats.iter().any(|format| {
            !matches!(
                format.as_str(),
                "graphrun.domain/v1"
                    | crate::history::EVENT_FORMAT
                    | crate::history::CHECKPOINT_FORMAT
                    | crate::publication::RESULT_FORMAT
                    | record_store::FORMAT
                    | record_store::FRAGMENT_FORMAT
            )
        })
        || hex::encode(digest) != registry.sha256
    {
        return Err(sto_err(
            ErrorVerb::Read,
            "snapshot registry or required record version does not match file",
        ));
    }
    Ok(())
}

fn current_snapshot(
    dir: &Path,
    registry: &SnapshotRegistry,
) -> std::result::Result<Snapshot<TypeConfig>, StoErr> {
    verify_registry(dir, registry)?;
    let stream = SnapshotStream::open(dir.join(&registry.file_name), false)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    Ok(Snapshot {
        meta: registry.meta.clone(),
        snapshot: Box::new(stream),
    })
}

fn snapshot_headroom(db_path: &Path, clustered: bool) -> std::result::Result<u64, StoErr> {
    if !clustered {
        return Ok(512 * 1024 * 1024);
    }
    let db_size = std::fs::metadata(db_path)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .len();
    Ok((8 * 1024 * 1024 * 1024).max(db_size.saturating_mul(20) / 100))
}

fn free_snapshot_space(dir: &Path) -> std::result::Result<u64, StoErr> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(dir.as_os_str().as_bytes())
            .map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(name.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(sto_err(ErrorVerb::Read, io::Error::last_os_error()));
        }
        let stat = unsafe { stat.assume_init() };
        u128::from(stat.f_bavail)
            .checked_mul(u128::from(stat.f_frsize))
            .and_then(|available| u64::try_from(available).ok())
            .ok_or_else(|| sto_err(ErrorVerb::Read, "snapshot free-space count overflow"))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Err(sto_err(
            ErrorVerb::Read,
            "snapshot admission requires supported filesystem space reporting",
        ))
    }
}

fn admit_snapshot_space(
    dir: &Path,
    db_path: &Path,
    encoded: u64,
    clustered: bool,
) -> std::result::Result<(), StoErr> {
    if encoded > 128 * 1024 * 1024 * 1024 + 16 * 1024 * 1024 {
        return Err(sto_err(
            ErrorVerb::Write,
            "snapshot container exceeds the 128 GiB payload limit",
        ));
    }
    let headroom = snapshot_headroom(db_path, clustered)?;
    let required = encoded
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(headroom))
        .ok_or_else(|| sto_err(ErrorVerb::Write, "snapshot disk reservation overflows"))?;
    let free = free_snapshot_space(dir)?;
    if free < required {
        return Err(sto_err(
            ErrorVerb::Write,
            format!("snapshot requires {required} free bytes but only {free} are available"),
        ));
    }
    Ok(())
}

fn verify_snapshot_headroom(
    dir: &Path,
    db_path: &Path,
    clustered: bool,
) -> std::result::Result<(), StoErr> {
    let free = free_snapshot_space(dir)?;
    let headroom = snapshot_headroom(db_path, clustered)?;
    if free < headroom {
        return Err(sto_err(
            ErrorVerb::Write,
            format!("snapshot import needs {headroom} bytes headroom; only {free} remain"),
        ));
    }
    Ok(())
}

fn publish_snapshot_registry(
    db: &Database,
    registry: &SnapshotRegistry,
) -> std::result::Result<(), StoErr> {
    let mut progress = read_snapshot_progress(db)?;
    progress.last_snapshot_applied = registry.meta.last_log_id.map_or(0, |id| id.index);
    if registry.applied_bytes > progress.applied_bytes {
        return Err(sto_err(
            ErrorVerb::Read,
            "snapshot byte baseline exceeds current applied bytes",
        ));
    }
    progress.last_snapshot_bytes = registry.applied_bytes;
    progress.last_snapshot_ms =
        crate::time::wall_millis().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let txn = begin_immediate(db)?;
    let mut table = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let registry_bytes =
        serde_json::to_vec(registry).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    table
        .insert("snapshot_registry", registry_bytes.as_slice())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let progress_bytes =
        serde_json::to_vec(&progress).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    table
        .insert("snapshot_progress", progress_bytes.as_slice())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    drop(table);
    commit_immediate(txn)
}

fn cleanup_staged_snapshots(
    dir: &Path,
    active: Option<&SnapshotRegistry>,
    limit: usize,
) -> std::result::Result<bool, StoErr> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(sto_err(ErrorVerb::Read, err)),
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if active.is_some_and(|registry| registry.file_name == name) {
            continue;
        }
        let generated = (name.starts_with("snap-") || name.starts_with("received-"))
            && name.ends_with(".snap")
            || (name.starts_with("receiving-")
                || name.starts_with("import-")
                || name.starts_with("snap-"))
                && name.ends_with(".tmp");
        if !generated
            || !entry
                .file_type()
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
                .is_file()
        {
            continue;
        }
        if removed == limit {
            return Ok(true);
        }
        std::fs::remove_file(entry.path()).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        after_persist("after-snapshot-cleanup")?;
        removed += 1;
    }
    Ok(false)
}

fn truncate_logs(db: &Database, from: u64) -> std::result::Result<(), StoErr> {
    let Some(old_visible) = visible_log_end(db)? else {
        return Ok(());
    };
    if from > old_visible.index {
        return Ok(());
    }
    let purged = persisted_purged(db)?;
    if purged.is_some_and(|id| from <= id.index) {
        return Err(sto_err(ErrorVerb::Write, "truncate crosses durable purge"));
    }
    let applied = persisted_applied(db)?;
    let txn = begin_immediate(db)?;
    let mut credits = read_credits(&txn)?;
    let table = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let predecessor = if let Some(index) = from.checked_sub(1) {
        table
            .get(&index)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
            .map(|record| {
                serde_json::from_slice::<EntryT>(record.value())
                    .map(|entry| *entry.get_log_id())
                    .map_err(|err| sto_err(ErrorVerb::Read, err))
            })
            .transpose()?
            .or(purged.filter(|id| id.index == index))
    } else {
        purged
    };
    if from != 0 && predecessor.is_none() {
        return Err(sto_err(
            ErrorVerb::Read,
            "visible log predecessor missing during truncate",
        ));
    }
    let start = applied.map_or(from, |id| from.max(id.index.saturating_add(1)));
    if start <= old_visible.index {
        for record in table
            .range(start..=old_visible.index)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (_, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            remove_pending_credit(&mut credits, value.value().len())?;
        }
    }
    drop(table);
    write_credits(&txn, credits)?;
    let bytes = serde_json::to_vec(&predecessor).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?
        .insert("visible_log_end", bytes.as_slice())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    commit_immediate(txn)?;
    after_persist("after-log-boundary")?;
    Ok(())
}

fn purge_logs(
    db: &Database,
    through: u64,
    last_purged: &LogIdT,
) -> std::result::Result<(), StoErr> {
    let old_visible = visible_log_end(db)?;
    let previous_purged = persisted_purged(db)?;
    if let Some(old) = previous_purged {
        if through < old.index {
            return Ok(());
        }
        if through == old.index {
            if *last_purged != old {
                return Err(sto_err(ErrorVerb::Read, "purged log identity changed"));
            }
            return Ok(());
        }
    }
    let applied = persisted_applied(db)?;
    let txn = begin_immediate(db)?;
    let mut credits = read_credits(&txn)?;
    {
        let logs = txn
            .open_table(LOG)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let first = previous_purged.map_or(0, |id| id.index.saturating_add(1));
        let first = applied.map_or(first, |id| first.max(id.index.saturating_add(1)));
        if let Some(last) = old_visible
            .map(|visible| through.min(visible.index))
            .filter(|last| first <= *last)
        {
            for record in logs
                .range(first..=last)
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
            {
                let (_, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
                remove_pending_credit(&mut credits, value.value().len())?;
            }
        }
    }
    {
        let mut meta = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let bytes =
            serde_json::to_vec(last_purged).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("last_purged", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let visible = old_visible
            .filter(|visible| visible.index > through)
            .or(Some(*last_purged));
        let bytes = serde_json::to_vec(&visible).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("visible_log_end", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    write_credits(&txn, credits)?;
    commit_immediate(txn)?;
    after_persist("after-log-boundary")?;
    Ok(())
}

fn log_state(
    db: &Database,
    last_purged: Option<LogIdT>,
) -> std::result::Result<LogState<TypeConfig>, StoErr> {
    let visible = visible_log_end(db)?;
    if last_purged.is_some_and(|purged| visible.is_some_and(|end| end.index < purged.index)) {
        return Err(sto_err(
            ErrorVerb::Read,
            "visible log precedes durable purge",
        ));
    }
    Ok(LogState {
        last_purged_log_id: last_purged,
        last_log_id: visible.or(last_purged),
    })
}

fn get_logs(db: &Database, start: u64, end: u64) -> std::result::Result<Vec<EntryT>, StoErr> {
    let Some(visible) = visible_log_end(db)? else {
        return Ok(Vec::new());
    };
    if start >= end {
        return Ok(Vec::new());
    }
    let purged = persisted_purged(db)?;
    let first = purged.map_or(start, |id| start.max(id.index.saturating_add(1)));
    let last = visible.index.min(end - 1);
    if first > last || purged.is_some_and(|id| id.index == u64::MAX) {
        return Ok(Vec::new());
    }
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut out = Vec::new();
    let iter = table
        .range(first..=last)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    for row in iter {
        let (_, value) = row.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let entry: EntryT =
            serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))?;
        out.push(entry);
    }
    Ok(out)
}

fn select_gc_key<K>(keys: &mut Vec<K>, bytes: &mut usize, index: K, size: usize) -> bool {
    if keys.len() == 4_096 || (!keys.is_empty() && bytes.saturating_add(size) > 4 * 1024 * 1024) {
        return false;
    }
    keys.push(index);
    *bytes = bytes.saturating_add(size);
    true
}

fn gc_invisible_logs(db: &Database) -> std::result::Result<bool, StoErr> {
    let visible = visible_log_end(db)?;
    let purged = persisted_purged(db)?;
    let txn = begin_immediate(db)?;
    let mut table = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let mut keys = Vec::new();
    let mut bytes = 0usize;
    let mut more = false;
    if let Some(purged) = purged {
        for record in table
            .range(..=purged.index)
            .map_err(|err| sto_err(ErrorVerb::Read, err))?
        {
            let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
            if !select_gc_key(&mut keys, &mut bytes, key.value(), value.value().len()) {
                more = true;
                break;
            }
        }
    }
    if !more {
        if let Some(visible) = visible {
            if let Some(first) = visible.index.checked_add(1) {
                for record in table
                    .range(first..)
                    .map_err(|err| sto_err(ErrorVerb::Read, err))?
                {
                    let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
                    if !select_gc_key(&mut keys, &mut bytes, key.value(), value.value().len()) {
                        more = true;
                        break;
                    }
                }
            }
        } else {
            for record in table.iter().map_err(|err| sto_err(ErrorVerb::Read, err))? {
                let (key, value) = record.map_err(|err| sto_err(ErrorVerb::Read, err))?;
                if !select_gc_key(&mut keys, &mut bytes, key.value(), value.value().len()) {
                    more = true;
                    break;
                }
            }
        }
    }
    for key in &keys {
        table
            .remove(key)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    drop(table);
    if !keys.is_empty() {
        commit_immediate(txn)?;
        after_persist("after-log-gc")?;
    }
    Ok(more)
}

fn apply_entries(
    db: &Database,
    last_applied: &mut Option<LogIdT>,
    last_membership: &mut MembershipT,
    domain: &mut State,
    schedule_revision: &mut u64,
    entries: Vec<EntryT>,
) -> std::result::Result<Vec<RaftResponse>, StoErr> {
    let mut replies = Vec::new();
    let previous_applied = *last_applied;
    let mut next_applied = previous_applied;
    let mut next_membership = last_membership.clone();
    let mut next_domain = domain.clone();
    let mut domain_changed = false;
    let mut membership_changed = false;
    let mut affected = BTreeMap::<crate::ids::RunId, Option<bool>>::new();
    let mut applied_bytes = 0u64;
    for entry in entries {
        if entry.get_log_id().index > previous_applied.map_or(0, |id| id.index) {
            let encoded =
                serde_json::to_vec(&entry).map_err(|err| sto_err(ErrorVerb::Write, err))?;
            applied_bytes = applied_bytes
                .checked_add(encoded.len() as u64)
                .ok_or_else(|| sto_err(ErrorVerb::Write, "applied byte counter overflow"))?;
        }
        next_applied = Some(*entry.get_log_id());
        if let Some(membership) = openraft::entry::RaftPayload::get_membership(&entry.payload) {
            next_membership = StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
            membership_changed = true;
        }
        let mut reply = RaftResponse::default();
        if let EntryPayload::Normal(req) = &entry.payload {
            domain_changed = true;
            if let domain::CommandBody::Publication { key, operation } = &req.command.body {
                if key.command_id != req.command.id
                    || key.cluster_id.is_empty()
                    || key.principal_id.is_empty()
                {
                    reply.error = Some("invalid authenticated command identity".to_owned());
                    reply.error_kind = Some(crate::error::ErrorKind::InvalidArgument);
                } else {
                    let mut command = req.command.clone();
                    if !next_domain.command_results.contains_key(&key.storage_key()) {
                        command.time = crate::time::EngineTime::from_millis(
                            next_domain
                                .engine_time_watermark_ms
                                .max(command.time.as_millis()),
                        );
                        next_domain.engine_time_watermark_ms = command.time.as_millis();
                    }
                    let receipt =
                        crate::publication::apply(&mut next_domain, &command, key, operation);
                    if let Ok(run) = receipt.applied_run() {
                        affected.insert(run, Some(true));
                    }
                    if let Err(err) = receipt.ensure_applied() {
                        reply.error_kind = Some(err.kind);
                        reply.error = Some(err.to_string());
                    }
                    reply.command_result = Some(receipt);
                }
            } else {
                let mut candidate = next_domain.clone();
                match domain::commit_command(&mut candidate, req.command.clone()) {
                    Ok(events) => {
                        if let domain::CommandBody::Progress { run } = &req.command.body {
                            affected.insert(*run, Some(!events.is_empty()));
                        }
                        for event in &events {
                            if let Some(run) = domain::event_owner(&candidate, event)
                                .map_err(|err| sto_err(ErrorVerb::Read, err))?
                            {
                                let progress = !matches!(
                                    event,
                                    domain::DomainEvent::ClaimGranted { .. }
                                        | domain::DomainEvent::ClaimRenewed { .. }
                                );
                                affected
                                    .entry(run)
                                    .and_modify(|existing| {
                                        if progress {
                                            *existing = Some(true);
                                        }
                                    })
                                    .or_insert(progress.then_some(true));
                            }
                        }
                        if matches!(
                            &req.command.body,
                            domain::CommandBody::AcknowledgeRecovery { .. }
                        ) {
                            for run in domain::active_runs(&candidate) {
                                affected.insert(run, Some(true));
                            }
                        }
                        next_domain = candidate;
                        domain_changed = true;
                        reply.run_id = events.iter().find_map(|event| match event {
                            domain::DomainEvent::RunAdmitted { run, .. } => Some(*run),
                            _ => None,
                        });
                    }
                    Err(err) => {
                        reply.error_kind = Some(err.kind);
                        reply.error = Some(err.to_string());
                    }
                }
            }
        }
        replies.push(reply);
    }
    let mut snapshot_progress = read_snapshot_progress(db)?;
    snapshot_progress.applied_bytes = snapshot_progress
        .applied_bytes
        .checked_add(applied_bytes)
        .ok_or_else(|| sto_err(ErrorVerb::Write, "applied byte counter overflow"))?;
    let generation = verified_store_manifest(db)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .active_generation;
    let txn = begin_immediate(db)?;
    {
        let mut meta = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let applied =
            serde_json::to_vec(&next_applied).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("last_applied", applied.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let progress_bytes =
            serde_json::to_vec(&snapshot_progress).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("snapshot_progress", progress_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        if membership_changed {
            let membership = serde_json::to_vec(&next_membership)
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
            meta.insert("membership", membership.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
        if domain_changed {
            let domain_bytes =
                serde_json::to_vec(&next_domain).map_err(|err| sto_err(ErrorVerb::Write, err))?;
            meta.insert("domain", domain_bytes.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    after_persist("after-applied-metadata")?;
    let record_revision = next_applied
        .map_or(0, |id| id.index)
        .checked_add(1)
        .ok_or_else(|| sto_err(ErrorVerb::Write, "record revision exhausted"))?;
    write_app_delta(&txn, domain, &next_domain, generation, record_revision)?;
    after_persist("after-event-append")?;
    release_applied_credits(&txn, previous_applied, next_applied)?;
    let next_revision = update_schedule(
        &txn,
        &next_domain,
        &affected,
        *schedule_revision,
        generation,
        next_applied.map_or(0, |id| id.index),
        next_domain.engine_time_watermark_ms,
    )?;
    after_persist("after-domain-index")?;
    commit_immediate(txn)?;
    *last_applied = next_applied;
    *last_membership = next_membership;
    *domain = next_domain;
    if let Some(revision) = next_revision {
        *schedule_revision = revision;
    }
    after_persist("after-apply")?;
    Ok(replies)
}

fn build_snapshot(
    db: &Database,
    db_path: &Path,
    dir: &Path,
    clustered: bool,
) -> std::result::Result<(Snapshot<TypeConfig>, SnapshotRegistry), StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(META)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let raw_manifest = table
        .get("store_manifest")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "snapshot store manifest is missing"))?;
    let store_manifest: StoreManifest = serde_json::from_slice(raw_manifest.value())
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    store_manifest
        .validate()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let raw_generation = table
        .get("active_generation")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "active snapshot generation is missing"))?;
    let generation: u64 = serde_json::from_slice(raw_generation.value())
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    if generation != store_manifest.active_generation {
        return Err(sto_err(
            ErrorVerb::Read,
            "snapshot generation differs from store manifest",
        ));
    }
    let last_applied: Option<LogIdT> = table
        .get("last_applied")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .map(|record| serde_json::from_slice(record.value()))
        .transpose()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .flatten();
    let last_membership: MembershipT = table
        .get("membership")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .map(|record| serde_json::from_slice(record.value()))
        .transpose()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .unwrap_or_else(|| StoredMembership::new(None, Membership::new(vec![], None)));
    let meta = SnapshotMeta {
        last_log_id: last_applied,
        last_membership,
        snapshot_id: format!(
            "snap-{}-{}",
            last_applied.map_or(0, |id| id.index),
            crate::ids::CommandId::generate().to_hex()
        ),
    };
    let applied =
        serde_json::to_vec(&meta.last_log_id).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let membership =
        serde_json::to_vec(&meta.last_membership).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let domain = table
        .get("domain")
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .ok_or_else(|| sto_err(ErrorVerb::Read, "snapshot domain is missing"))?;
    let mut prefix = Vec::with_capacity(applied.len() + membership.len() + 3);
    prefix.push(b'[');
    prefix.extend_from_slice(&applied);
    prefix.push(b',');
    prefix.extend_from_slice(&membership);
    prefix.push(b',');
    let manifest = SnapshotManifest {
        framing_version: 1,
        generation,
        applied_json: applied,
        membership_json: membership,
        payload_bytes: (prefix.len() as u64)
            .checked_add(domain.value().len() as u64)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| sto_err(ErrorVerb::Write, "snapshot length overflow"))?,
        record_formats: vec![
            "graphrun.domain/v1".to_owned(),
            crate::history::EVENT_FORMAT.to_owned(),
            crate::history::CHECKPOINT_FORMAT.to_owned(),
            crate::publication::RESULT_FORMAT.to_owned(),
            record_store::FORMAT.to_owned(),
            record_store::FRAGMENT_FORMAT.to_owned(),
        ],
        record_count: 1,
    };
    std::fs::create_dir_all(dir).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    admit_snapshot_space(dir, db_path, manifest.payload_bytes, clustered)?;
    let name = format!("{}.snap", meta.snapshot_id);
    let stage = dir.join(format!("{name}.tmp"));
    let mut source = io::Cursor::new(prefix)
        .chain(io::Cursor::new(domain.value()))
        .chain(io::Cursor::new(b"]".as_slice()));
    let digest = write_snapshot(&mut source, &stage, &manifest)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let final_path = dir.join(&name);
    std::fs::rename(&stage, &final_path).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let registry = SnapshotRegistry {
        format: 1,
        meta,
        file_name: name,
        sha256: hex::encode(digest),
        generation,
        source_generation: generation,
        applied_bytes: serde_json::from_slice::<SnapshotProgress>(
            table
                .get("snapshot_progress")
                .map_err(|err| sto_err(ErrorVerb::Read, err))?
                .ok_or_else(|| sto_err(ErrorVerb::Read, "snapshot progress is missing"))?
                .value(),
        )
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .applied_bytes,
    };
    let snapshot = current_snapshot(dir, &registry)?;
    Ok((snapshot, registry))
}

fn install_snapshot(
    db: &Database,
    db_path: &Path,
    dir: &Path,
    last_applied: &mut Option<LogIdT>,
    last_membership: &mut MembershipT,
    domain: &mut State,
    snapshot: &mut Option<SnapshotRegistry>,
    schedule_revision: &mut u64,
    meta: SnapMeta,
    received: PathBuf,
    clustered: bool,
    mut between_batches: impl FnMut() -> std::result::Result<(), StoErr>,
) -> std::result::Result<(), StoErr> {
    let started = std::time::Instant::now();
    std::fs::create_dir_all(dir).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let encoded = std::fs::metadata(&received)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?
        .len();
    admit_snapshot_space(dir, db_path, encoded, clustered)?;
    let staged = dir.join(format!(
        "import-{}.snap.tmp",
        crate::ids::CommandId::generate().to_hex()
    ));
    let mut source = std::fs::File::open(&received).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut destination = std::fs::File::options()
        .write(true)
        .create_new(true)
        .open(&staged)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    io::copy(&mut source, &mut destination).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    destination
        .sync_all()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let (manifest, digest) =
        verify_snapshot(&staged).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let expected_applied =
        serde_json::to_vec(&meta.last_log_id).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let expected_membership =
        serde_json::to_vec(&meta.last_membership).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    if manifest.applied_json != expected_applied
        || manifest.membership_json != expected_membership
        || !manifest
            .record_formats
            .iter()
            .any(|format| format == "graphrun.domain/v1")
    {
        return Err(sto_err(
            ErrorVerb::Read,
            "snapshot manifest does not match Raft metadata or required readers",
        ));
    }
    let raw = dir.join(format!(
        "import-{}.json.tmp",
        crate::ids::CommandId::generate().to_hex()
    ));
    let decoded = (|| -> std::result::Result<_, StoErr> {
        let mut file = std::fs::File::options()
            .write(true)
            .create_new(true)
            .open(&raw)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        copy_payload(&staged, &mut file).map_err(|err| sto_err(ErrorVerb::Read, err))?;
        file.sync_all()
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let file = std::fs::File::open(&raw).map_err(|err| sto_err(ErrorVerb::Read, err))?;
        serde_json::from_reader::<_, (Option<LogIdT>, MembershipT, State)>(file)
            .map_err(|err| sto_err(ErrorVerb::Read, err))
    })();
    if let Err(err) = std::fs::remove_file(&raw) {
        if decoded.is_ok() {
            return Err(sto_err(ErrorVerb::Write, err));
        }
        if err.kind() != io::ErrorKind::NotFound {
            tracing::warn!(path = %raw.display(), %err, "failed to remove rejected snapshot payload");
        }
    }
    let (applied, membership, mut restored) = decoded?;
    if applied != meta.last_log_id || membership != meta.last_membership {
        return Err(sto_err(
            ErrorVerb::Read,
            "snapshot payload does not match its Raft metadata",
        ));
    }
    restored.engine_time_watermark_ms = restored
        .engine_time_watermark_ms
        .max(domain.engine_time_watermark_ms);
    validate_history_store(&restored).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let mut store_manifest =
        verified_store_manifest(db).map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let generation = store_manifest
        .active_generation
        .checked_add(1)
        .ok_or_else(|| sto_err(ErrorVerb::Write, "snapshot generation exhausted"))?;
    store_manifest.active_generation = generation;
    let record_revision = applied
        .map_or(0, |id| id.index)
        .checked_add(1)
        .ok_or_else(|| sto_err(ErrorVerb::Write, "record revision exhausted"))?;
    let imported = stage_app_generation(db, &restored, generation, record_revision, || {
        if started.elapsed() >= std::time::Duration::from_secs(2 * 60 * 60) {
            return Err(sto_err(
                ErrorVerb::Write,
                "snapshot install exceeded two hours",
            ));
        }
        between_batches()
    })?;
    if started.elapsed() >= std::time::Duration::from_secs(2 * 60 * 60) {
        return Err(sto_err(
            ErrorVerb::Write,
            "snapshot install exceeded two hours",
        ));
    }
    tracing::info!(
        generation,
        records = imported.records,
        transactions = imported.transactions,
        max_batch_bytes = imported.largest_batch_bytes,
        max_batch_records = imported.largest_batch_records,
        "snapshot generation staged"
    );
    let file_name = format!(
        "received-{}.snap",
        crate::ids::CommandId::generate().to_hex()
    );
    std::fs::rename(&staged, dir.join(&file_name)).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    after_persist("after-snapshot-file")?;
    let mut progress = read_snapshot_progress(db)?;
    let registry = SnapshotRegistry {
        format: 1,
        meta: meta.clone(),
        file_name,
        sha256: hex::encode(digest),
        generation,
        source_generation: manifest.generation,
        applied_bytes: progress.applied_bytes,
    };
    verify_registry(dir, &registry)?;
    progress.last_snapshot_applied = applied.map_or(0, |id| id.index);
    progress.last_snapshot_bytes = progress.applied_bytes;
    progress.last_snapshot_ms =
        crate::time::wall_millis().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let old_visible = visible_log_end(db)?;
    let visible = match (old_visible, applied) {
        (Some(old), Some(new)) if new.index > old.index => Some(new),
        (None, Some(new)) => Some(new),
        (old, _) => old,
    };
    let txn = begin_immediate(db)?;
    {
        let mut table = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let applied_bytes =
            serde_json::to_vec(&applied).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("last_applied", applied_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let membership_bytes =
            serde_json::to_vec(&membership).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("membership", membership_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let domain_bytes =
            serde_json::to_vec(&restored).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("domain", domain_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let meta_bytes =
            serde_json::to_vec(&registry).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("snapshot_registry", meta_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let progress_bytes =
            serde_json::to_vec(&progress).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("snapshot_progress", progress_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let generation_bytes =
            serde_json::to_vec(&generation).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("active_generation", generation_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let visible_bytes =
            serde_json::to_vec(&visible).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("visible_log_end", visible_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let store_bytes =
            serde_json::to_vec(&store_manifest).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("store_manifest", store_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    let new_revision = replace_schedule(
        &txn,
        &restored,
        *schedule_revision,
        generation,
        applied.map_or(0, |id| id.index),
    )?;
    recount_unapplied_credits(&txn, applied)?;
    commit_immediate(txn)?;
    *last_applied = applied;
    *last_membership = membership;
    *domain = restored;
    *snapshot = Some(registry);
    *schedule_revision = new_revision;
    after_persist("after-snapshot-activate")?;
    after_persist("after-snapshot")?;
    Ok(())
}

#[derive(Clone)]
pub struct LogStore {
    handle: StorageHandle,
}

#[derive(Clone)]
pub struct StateMachineStore {
    handle: StorageHandle,
}

impl LogStore {
    async fn send<T>(
        &self,
        req: impl FnOnce(oneshot::Sender<std::result::Result<T, StoErr>>) -> Req,
    ) -> std::result::Result<T, StoErr> {
        let (tx, rx) = oneshot::channel();
        self.handle
            .tx
            .send(req(tx))
            .await
            .map_err(|_| sto_err(ErrorVerb::Write, "storage thread stopped"))?;
        rx.await
            .map_err(|_| sto_err(ErrorVerb::Read, "storage thread dropped"))?
    }
}

impl StateMachineStore {
    async fn send<T>(
        &self,
        req: impl FnOnce(oneshot::Sender<std::result::Result<T, StoErr>>) -> Req,
    ) -> std::result::Result<T, StoErr> {
        let (tx, rx) = oneshot::channel();
        self.handle
            .tx
            .send(req(tx))
            .await
            .map_err(|_| sto_err(ErrorVerb::Write, "storage thread stopped"))?;
        rx.await
            .map_err(|_| sto_err(ErrorVerb::Read, "storage thread dropped"))?
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<EntryT>, StoErr> {
        let start = match range.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(v) => v.saturating_add(1),
            Bound::Excluded(v) => *v,
            Bound::Unbounded => u64::MAX,
        };
        self.send(|tx| Req::GetLogs(start, end, tx)).await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<TypeConfig>, StoErr> {
        self.send(Req::LogState).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteT) -> std::result::Result<(), StoErr> {
        let vote = *vote;
        self.send(|tx| Req::SaveVote(vote, tx)).await
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<VoteT>, StoErr> {
        self.send(Req::ReadVote).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StoErr>
    where
        I: IntoIterator<Item = EntryT> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        match self.send(|tx| Req::Append(entries, tx)).await {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                callback.log_io_completed(Err(io::Error::other(err.to_string())));
                Err(err)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogIdT) -> std::result::Result<(), StoErr> {
        self.send(|tx| Req::Truncate(log_id, tx)).await
    }

    async fn purge(&mut self, log_id: LogIdT) -> std::result::Result<(), StoErr> {
        self.send(|tx| Req::Purge(log_id, tx)).await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> std::result::Result<Snapshot<TypeConfig>, StoErr> {
        self.send(Req::BuildSnapshot).await
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = StateMachineStore;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<(Option<LogIdT>, MembershipT), StoErr> {
        self.send(Req::AppliedState).await
    }

    async fn apply<I>(&mut self, entries: I) -> std::result::Result<Vec<RaftResponse>, StoErr>
    where
        I: IntoIterator<Item = EntryT> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.send(|tx| Req::Apply(entries, tx)).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<SnapshotStream>, StoErr> {
        let permit = self
            .handle
            .inbound_snapshot
            .clone()
            .try_acquire_owned()
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        std::fs::create_dir_all(&*self.handle.snapshot_dir)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let path = self.handle.snapshot_dir.join(format!(
            "receiving-{}.snap.tmp",
            crate::ids::CommandId::generate().to_hex()
        ));
        std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let stream =
            SnapshotStream::open(path, true).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        Ok(Box::new(stream.with_permit(permit)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapMeta,
        snapshot: Box<SnapshotStream>,
    ) -> std::result::Result<(), StoErr> {
        snapshot
            .sync_all()
            .await
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let path = snapshot.path.clone();
        let meta = meta.clone();
        self.send(|tx| Req::InstallSnapshot(meta, path, tx)).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StoErr> {
        self.send(Req::CurrentSnapshot).await
    }
}

pub struct LocalNetwork;

impl RaftNetworkFactory<TypeConfig> for LocalNetwork {
    type Network = IsolatedPeer;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        IsolatedPeer
    }
}

pub struct IsolatedPeer;

impl RaftNetwork<TypeConfig> for IsolatedPeer {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
            "single-member network has no peers",
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
            "single-member network has no peers",
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
            "single-member network has no peers",
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::StoreBuilder;
    use redb::ReadableTableMetadata;

    pub struct RedbStoreBuilder;

    impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, tempfile::TempDir> for RedbStoreBuilder {
        async fn build(
            &self,
        ) -> std::result::Result<(tempfile::TempDir, LogStore, StateMachineStore), StoErr> {
            let dir = tempfile::TempDir::new().map_err(|err| sto_err(ErrorVerb::Write, err))?;
            let (handle, _join) = StorageHandle::open(dir.path().join("member.redb"))
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
            Ok((dir, handle.log_store(), handle.state_machine()))
        }
    }

    #[test]
    fn openraft_storage_suite() {
        openraft::testing::Suite::<
            TypeConfig,
            LogStore,
            StateMachineStore,
            RedbStoreBuilder,
            tempfile::TempDir,
        >::test_all(RedbStoreBuilder)
        .unwrap();
    }

    #[test]
    fn missing_member_store_with_identity_does_not_bootstrap() {
        let dir = tempfile::tempdir().unwrap();
        let identity = dir.path().join("identity.json");
        let bytes = b"existing identity";
        std::fs::write(&identity, bytes).unwrap();
        let db_path = dir.path().join("member.redb");

        let err = StorageHandle::open(&db_path).err().unwrap();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(err.message.contains("member store missing"));
        assert!(!db_path.exists());
        assert_eq!(std::fs::read(&identity).unwrap(), bytes);
    }

    #[test]
    fn log_cut_after_persist_keeps_entries() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Blank,
        };
        inject_cut("after-log");
        let err = append_logs(&db, &[entry]).unwrap_err();
        clear_cut();
        assert!(err.to_string().contains("fault cut"));
        let loaded = get_logs(&db, 1, 2).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    #[tokio::test]
    async fn open_rejects_pre_history_active_run_before_starting_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let catalog = crate::catalog::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = crate::compiler::compile_yaml(
            "\
dsl: graphrun/v1
id: old_active_store
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {literal: null}
",
            &catalog,
        )
        .unwrap();
        let run = crate::ids::RunId::from_bytes([8; 16]);
        let mut domain = State::default();
        domain::commit_command(
            &mut domain,
            Command {
                id: crate::ids::CommandId::from_bytes([9; 16]),
                time: crate::time::EngineTime::from_millis(1),
                body: domain::CommandBody::Start {
                    run,
                    definition: Box::new(definition),
                    input: crate::value::Value::Null,
                    catalog: Box::new(catalog),
                },
            },
        )
        .unwrap();
        assert!(matches!(
            domain.runs[&run].status,
            domain::RunStatus::Active
        ));
        let compatible = domain.clone();
        domain.history_records.clear();
        domain.history_dependencies.clear();
        put_json(&db, "domain", &domain).unwrap();
        drop(db);

        let error = StorageHandle::open(&path)
            .err()
            .expect("pre-history active store must be rejected");
        assert_eq!(error.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(
            error.message.contains("history") && error.message.contains("migrate"),
            "error must explain the incompatible store: {error}"
        );
        let db = ReadOnlyDatabase::open(&path).unwrap();
        let untouched: State = load_json(&db, "domain").unwrap();
        assert!(untouched.runs.contains_key(&run));
        assert!(untouched.history_records.is_empty());

        let fresh = dir.path().join("fresh.redb");
        let (handle, thread) = StorageHandle::open(&fresh).unwrap();
        let restore_error = handle.install_domain(domain).await.unwrap_err();
        assert_eq!(
            restore_error.kind,
            crate::error::ErrorKind::FailedPrecondition
        );
        assert!(handle.query_state().await.runs.is_empty());
        handle.install_domain(compatible).await.unwrap();
        assert!(handle.query_state().await.runs.contains_key(&run));
        handle.shutdown();
        thread.join().unwrap();
        let (handle, thread) = StorageHandle::open(&fresh).unwrap();
        assert!(handle.query_state().await.runs.contains_key(&run));
        handle.shutdown();
        thread.join().unwrap();
    }

    #[test]
    fn apply_cut_does_not_mix_transactions() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let mut last_applied = None;
        let mut last_membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut domain = State::default();
        let mut schedule_revision = 0;
        inject_cut("after-apply");
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Blank,
        };
        let err = apply_entries(
            &db,
            &mut last_applied,
            &mut last_membership,
            &mut domain,
            &mut schedule_revision,
            vec![entry],
        )
        .unwrap_err();
        clear_cut();
        assert!(err.to_string().contains("fault cut"));
        assert!(last_applied.is_some());
        let loaded: Option<LogIdT> = load_json(&db, "last_applied");
        assert_eq!(loaded, last_applied);
    }

    #[test]
    fn event_index_and_applied_metadata_cuts_leave_the_old_generation() {
        let catalog = crate::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = crate::compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        for (index, point) in [
            "after-applied-metadata",
            "after-event-append",
            "after-domain-index",
        ]
        .into_iter()
        .enumerate()
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("member.redb");
            let db = Database::create(&path).unwrap();
            initialize_publication_store(&db).unwrap();
            let run = crate::ids::RunId::from_bytes([index as u8 + 1; 16]);
            let entry = Entry::<TypeConfig> {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(RaftRequest {
                    command: crate::domain::Command {
                        id: crate::ids::CommandId::from_bytes([9; 16]),
                        time: crate::time::EngineTime::from_millis(1),
                        body: crate::domain::CommandBody::Start {
                            run,
                            definition: Box::new(definition.clone()),
                            input: crate::Value::Object(
                                [
                                    ("order_id".to_owned(), crate::Value::String("r1".to_owned())),
                                    ("amount".to_owned(), crate::Value::Int(1)),
                                ]
                                .into_iter()
                                .collect(),
                            ),
                            catalog: Box::new(catalog.clone()),
                        },
                    },
                }),
            };
            let mut applied = None;
            let mut membership = StoredMembership::new(None, Membership::new(vec![], None));
            let mut domain = State::default();
            let mut revision = 0;
            inject_cut(point);
            let error = apply_entries(
                &db,
                &mut applied,
                &mut membership,
                &mut domain,
                &mut revision,
                vec![entry],
            )
            .unwrap_err();
            clear_cut();
            assert!(error.to_string().contains(point));
            let disk: State = required_json(&db, "domain").unwrap();
            assert!(disk.runs.is_empty());
            assert!(load_app_generation(&db, 1).unwrap().runs.is_empty());
            assert!(load_json::<_, Option<LogIdT>>(&db, "last_applied").is_none());
            drop(db);
            let (handle, thread) = StorageHandle::open(&path).unwrap();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            assert!(runtime.block_on(handle.query_state()).runs.is_empty());
            handle.shutdown();
            thread.join().unwrap();
        }
    }

    #[test]
    fn snapshot_install_cut_keeps_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let old_applied = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 1));
        let old_membership = StoredMembership::new(None, Membership::new(vec![], None));
        let old_domain = State::default();
        put_json(&db, "last_applied", &old_applied).unwrap();
        put_json(&db, "membership", &old_membership).unwrap();
        put_json(&db, "domain", &old_domain).unwrap();
        let new_applied = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 9));
        let new_membership = old_membership.clone();
        let new_domain = State::default();
        let data = serde_json::to_vec(&(new_applied, new_membership.clone(), &new_domain)).unwrap();
        let snapshot_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        let staged = snapshot_dir.join("receiving.snap.tmp");
        write_snapshot(
            &mut data.as_slice(),
            &staged,
            &SnapshotManifest {
                framing_version: 1,
                generation: 1,
                applied_json: serde_json::to_vec(&new_applied).unwrap(),
                membership_json: serde_json::to_vec(&new_membership).unwrap(),
                payload_bytes: data.len() as u64,
                record_formats: vec![
                    "graphrun.domain/v1".to_owned(),
                    record_store::FORMAT.to_owned(),
                    record_store::FRAGMENT_FORMAT.to_owned(),
                ],
                record_count: 1,
            },
        )
        .unwrap();
        let meta = SnapshotMeta {
            last_log_id: new_applied,
            last_membership: new_membership.clone(),
            snapshot_id: "snap-9".to_owned(),
        };
        let mut last_applied = old_applied;
        let mut last_membership = old_membership.clone();
        let mut domain = old_domain.clone();
        let mut snapshot = None;
        let mut schedule_revision = 0;
        inject_cut("after-snapshot");
        let err = install_snapshot(
            &db,
            &path,
            &snapshot_dir,
            &mut last_applied,
            &mut last_membership,
            &mut domain,
            &mut snapshot,
            &mut schedule_revision,
            meta,
            staged,
            false,
            || Ok(()),
        )
        .unwrap_err();
        clear_cut();
        assert!(err.to_string().contains("fault cut"));
        let disk_applied: Option<LogIdT> = load_json(&db, "last_applied");
        let disk_domain: State = load_json(&db, "domain").unwrap();
        let registry: SnapshotRegistry = load_json(&db, "snapshot_registry").unwrap();
        let active_generation: u64 = load_json(&db, "active_generation").unwrap();
        assert_eq!(disk_applied, new_applied);
        assert_eq!(last_applied, new_applied);
        assert_eq!(disk_applied, last_applied);
        assert_eq!(active_generation, 2);
        assert_eq!(registry.generation, 2);
        assert!(!registry.file_name.contains("receiving"));
        verify_registry(&snapshot_dir, &registry).unwrap();
        assert!(snapshot_dir.join("receiving.snap.tmp").exists());
        assert!(load_bytes(&db, "snapshot_data").is_none());
        let _ = (old_domain, disk_domain);
    }

    #[test]
    fn corrupted_active_snapshot_rejects_reopen_without_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let snapshots = dir.path().join("snapshots");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let (_, registry) = build_snapshot(&db, &path, &snapshots, false).unwrap();
        put_json(&db, "snapshot_registry", &registry).unwrap();
        let snapshot_path = snapshots.join(&registry.file_name);
        let mut bytes = std::fs::read(&snapshot_path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&snapshot_path, &bytes).unwrap();
        drop(db);
        let original_store = std::fs::read(&path).unwrap();
        let err = StorageHandle::open(&path).err().unwrap();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(err.message.contains("snapshot"));
        assert_eq!(std::fs::read(&path).unwrap(), original_store);
        assert_eq!(std::fs::read(&snapshot_path).unwrap(), bytes);
    }

    #[test]
    fn unsupported_store_reader_rejects_without_reinitialization() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let mut manifest = StoreManifest::new();
        manifest.reader_floor = 3;
        put_json(&db, "store_manifest", &manifest).unwrap();
        drop(db);
        let original = std::fs::read(&path).unwrap();
        let err = StorageHandle::open(&path).err().unwrap();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(err.message.contains("version"));
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }

    #[test]
    fn snapshot_cleanup_is_bounded_and_preserves_active_file() {
        let dir = tempfile::tempdir().unwrap();
        let names = [
            "snap-old.snap",
            "receiving-abcd.snap.tmp",
            "import-abcd.json.tmp",
            "snap-active.snap",
            "backup-owned-by-user",
        ];
        for name in names {
            std::fs::write(dir.path().join(name), b"file").unwrap();
        }
        let registry = SnapshotRegistry {
            format: 1,
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::new(None, Membership::new(vec![], None)),
                snapshot_id: "active".to_owned(),
            },
            file_name: "snap-active.snap".to_owned(),
            sha256: String::new(),
            generation: 1,
            source_generation: 1,
            applied_bytes: 0,
        };
        let mut removed = 0;
        while cleanup_staged_snapshots(dir.path(), Some(&registry), 1).unwrap() {
            removed += 1;
            assert!(removed <= 3);
        }
        assert_eq!(removed, 2);
        assert!(dir.path().join("snap-active.snap").exists());
        assert!(dir.path().join("backup-owned-by-user").exists());
        for name in &names[..3] {
            assert!(!dir.path().join(name).exists());
        }
    }

    #[test]
    fn offline_compaction_rejects_an_open_member() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        assert_eq!(
            compact_offline(&path).unwrap_err().kind,
            crate::error::ErrorKind::FailedPrecondition
        );
        drop(db);
        let outcome = compact_offline(&path).unwrap();
        assert!(outcome.complete, "rerun any partial offline compaction");
        let (handle, thread) = StorageHandle::open(&path).unwrap();
        handle.shutdown();
        thread.join().unwrap();
    }

    #[test]
    fn snapshot_byte_progress_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let entries: Vec<EntryT> = (1..=2)
            .map(|index| Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
                payload: EntryPayload::Blank,
            })
            .collect();
        append_logs(&db, &entries).unwrap();
        let mut applied = None;
        let mut membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut domain = State::default();
        let mut revision = 0;
        apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut domain,
            &mut revision,
            entries,
        )
        .unwrap();
        let before = read_snapshot_progress(&db).unwrap();
        assert!(before.applied_bytes > 0);
        assert_eq!(before.last_snapshot_bytes, 0);
        let (_, registry) =
            build_snapshot(&db, &path, &dir.path().join("snapshots"), false).unwrap();
        publish_snapshot_registry(&db, &registry).unwrap();
        drop(db);
        let db = ReadOnlyDatabase::open(&path).unwrap();
        let after: SnapshotProgress = required_json(&db, "snapshot_progress").unwrap();
        assert_eq!(after.applied_bytes, before.applied_bytes);
        assert_eq!(after.last_snapshot_bytes, after.applied_bytes);
        assert_eq!(after.last_snapshot_applied, 2);
        assert!(after.last_snapshot_ms >= before.last_snapshot_ms);
    }

    #[test]
    fn snapshot_import_batches_are_bounded_before_generation_activation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        let mut state = State::default();
        for n in 0u64..5_000 {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&n.to_be_bytes());
            state
                .command_times
                .insert(crate::ids::CommandId::from_bytes(bytes), n);
        }
        let mut serviced = 0;
        let stats = stage_app_generation(&db, &state, 2, 7, || {
            serviced += 1;
            if serviced == 1 {
                append_logs(
                    &db,
                    &[Entry {
                        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                        payload: EntryPayload::Blank,
                    }],
                )?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(
            stats.records,
            record_store::encode(&State::default(), 2, 7).unwrap().len() as u64 + 5_000
        );
        assert!(stats.transactions >= 2);
        assert!(serviced >= 2);
        assert_eq!(get_logs(&db, 1, 2).unwrap().len(), 1);
        assert!(stats.largest_batch_records <= 4_096);
        assert!(stats.largest_batch_bytes <= 4 * 1024 * 1024);
        assert_eq!(
            serde_json::to_value(load_app_generation(&db, 2).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        let manifest = verified_store_manifest(&db).unwrap();
        assert_eq!(manifest.active_generation, 1);
        assert!(
            load_app_generation(&db, 1)
                .unwrap()
                .command_times
                .is_empty()
        );
        assert!(gc_inactive_app_rows(&db).unwrap());
        assert!(!gc_inactive_app_rows(&db).unwrap());
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(APP_ROWS).unwrap();
        let first = table.range("0000000000000002/"..).unwrap().next();
        assert!(
            first
                .transpose()
                .unwrap()
                .is_none_or(|(key, _)| !key.value().starts_with("0000000000000002/"))
        );
    }

    #[test]
    fn snapshot_admission_enforces_disk_reservation_policy() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("member.redb");
        std::fs::write(&db_path, b"db").unwrap();
        assert_eq!(
            snapshot_headroom(&db_path, false).unwrap(),
            512 * 1024 * 1024
        );
        assert_eq!(
            snapshot_headroom(&db_path, true).unwrap(),
            8 * 1024 * 1024 * 1024
        );
        let oversize = 128 * 1024 * 1024 * 1024 + 16 * 1024 * 1024 + 1;
        assert!(admit_snapshot_space(dir.path(), &db_path, oversize, false).is_err());
    }

    #[test]
    fn import_batch_services_queued_raft_log_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        let mut state = State::default();
        for n in 0u64..5_000 {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&n.to_be_bytes());
            state
                .command_times
                .insert(crate::ids::CommandId::from_bytes(id), n);
        }
        let (queue, mut pending) = mpsc::channel(8);
        let (reply, received) = oneshot::channel();
        assert!(
            queue
                .try_send(Req::Append(
                    vec![Entry {
                        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                        payload: EntryPayload::Blank,
                    }],
                    reply,
                ))
                .is_ok()
        );
        let membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut purged = None;
        let mut serviced = 0;
        let stats = stage_app_generation(&db, &state, 2, 1, || {
            if let Ok(request) = pending.try_recv() {
                assert!(
                    service_import_io(request, &db, None, &membership, &mut purged, 0).is_none()
                );
                serviced += 1;
            }
            Ok(())
        })
        .unwrap();
        assert!(stats.transactions >= 2);
        assert_eq!(serviced, 1);
        received.blocking_recv().unwrap().unwrap();
        assert_eq!(get_logs(&db, 1, 2).unwrap().len(), 1);
        assert_eq!(verified_store_manifest(&db).unwrap().active_generation, 1);
    }

    #[test]
    fn large_state_record_imports_in_one_megabyte_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        let state = State {
            recovery: Some(crate::domain::RecoveryHold {
                reason: "x".repeat(5 * 1024 * 1024),
                authorized: false,
            }),
            ..State::default()
        };
        let stats = stage_app_generation(&db, &state, 2, 1, || Ok(())).unwrap();
        assert!(stats.transactions >= 2);
        assert!(stats.largest_batch_bytes <= 4 * 1024 * 1024);
        assert!(stats.largest_batch_records <= 4_096);
        let restored = load_app_generation(&db, 2).unwrap();
        assert_eq!(restored.recovery.unwrap().reason.len(), 5 * 1024 * 1024);
    }

    #[test]
    fn scoped_run_read_does_not_load_other_runs_history() {
        use crate::domain::{Command, CommandBody};

        let catalog = crate::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = crate::compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let runs = [
            crate::ids::RunId::from_bytes([1; 16]),
            crate::ids::RunId::from_bytes([2; 16]),
        ];
        for (index, run) in runs.iter().copied().enumerate() {
            crate::domain::start_run(
                &mut state,
                Command {
                    id: crate::ids::CommandId::from_bytes([index as u8 + 1; 16]),
                    time: crate::time::EngineTime::from_millis(1),
                    body: CommandBody::Start {
                        run,
                        definition: Box::new(definition.clone()),
                        catalog: Box::new(catalog.clone()),
                        input: crate::Value::Object(
                            [
                                ("order_id".to_owned(), crate::Value::String("r1".to_owned())),
                                ("amount".to_owned(), crate::Value::Int(1)),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    },
                },
                definition.clone(),
                catalog.clone(),
            )
            .unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        stage_app_generation(&db, &state, 2, 1, || Ok(())).unwrap();
        let scoped = load_app_run(&db, 2, runs[0]).unwrap();
        assert_eq!(scoped.runs.len(), 1);
        assert!(scoped.runs.contains_key(&runs[0]));
        assert!(scoped.history.contains_key(&runs[0]));
        assert!(!scoped.history.contains_key(&runs[1]));
    }

    #[test]
    fn purge_does_not_drop_domain_history() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let entries: Vec<EntryT> = (1..=3)
            .map(|index| Entry::<TypeConfig> {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
                payload: EntryPayload::Blank,
            })
            .collect();
        append_logs(&db, &entries).unwrap();
        let mut history = std::collections::HashMap::new();
        history.insert(
            crate::ids::RunId::from_bytes([1; 16]),
            vec![crate::domain::DomainEvent::RunAdmitted {
                run: crate::ids::RunId::from_bytes([1; 16]),
                definition_id: "seq".to_owned(),
                definition_version: 1,
                input: crate::value::Value::Null,
                root: crate::ids::ScopeId::from_bytes([2; 16]),
                policy: crate::policy::CapturedRunPolicy::defaults(None),
                admitted_ms: 0,
            }],
        );
        let domain = State {
            history,
            ..State::default()
        };
        put_json(&db, "domain", &domain).unwrap();
        let through = LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
        purge_logs(&db, 2, &through).unwrap();
        let remaining = get_logs(&db, 0, u64::MAX).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].get_log_id().index, 3);
        let loaded: State = load_json(&db, "domain").unwrap();
        assert_eq!(loaded.history, domain.history);
        truncate_logs(&db, 3).unwrap();
        let after_truncate = get_logs(&db, 0, u64::MAX).unwrap();
        assert!(after_truncate.is_empty());
        let loaded_again: State = load_json(&db, "domain").unwrap();
        assert_eq!(loaded_again.history, domain.history);
        let resurrected = get_logs(&db, 1, 3).unwrap();
        assert!(resurrected.is_empty());
    }

    #[test]
    fn truncated_suffix_stays_invisible_before_and_after_bounded_gc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let entry = |index, term| Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        };
        append_logs(
            &db,
            &(1..=4).map(|index| entry(index, 1)).collect::<Vec<_>>(),
        )
        .unwrap();
        inject_cut("after-log-boundary");
        assert!(truncate_logs(&db, 2).is_err());
        clear_cut();
        assert_eq!(log_state(&db, None).unwrap().last_log_id.unwrap().index, 1);
        assert!(get_logs(&db, 2, u64::MAX).unwrap().is_empty());
        let txn = db.begin_read().unwrap();
        assert!(txn.open_table(LOG).unwrap().get(&3).unwrap().is_some());
        drop(txn);
        append_logs(&db, &[entry(2, 2)]).unwrap();
        let visible = get_logs(&db, 1, u64::MAX).unwrap();
        assert_eq!(
            visible
                .iter()
                .map(|item| item.get_log_id().index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(visible[1].get_log_id(), entry(2, 2).get_log_id());
        assert!(!gc_invisible_logs(&db).unwrap());
        let txn = db.begin_read().unwrap();
        assert!(txn.open_table(LOG).unwrap().get(&3).unwrap().is_none());
        drop(txn);
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            get_logs(&db, 1, u64::MAX)
                .unwrap()
                .iter()
                .map(|item| item.get_log_id().index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn physical_log_gc_limits_each_transaction_to_4096_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        let entry = |index| Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Blank,
        };
        let first: Vec<EntryT> = (1..=4_096).map(entry).collect();
        append_logs(&db, &first).unwrap();
        let mut applied = None;
        let mut membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut state = State::default();
        let mut revision = 0;
        apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut state,
            &mut revision,
            first,
        )
        .unwrap();
        let tail: Vec<EntryT> = (4_097..=4_100).map(entry).collect();
        append_logs(&db, &tail).unwrap();
        apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut state,
            &mut revision,
            tail,
        )
        .unwrap();
        let purged = *entry(4_100).get_log_id();
        purge_logs(&db, 4_100, &purged).unwrap();
        assert!(get_logs(&db, 1, u64::MAX).unwrap().is_empty());
        assert!(gc_invisible_logs(&db).unwrap());
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(LOG).unwrap();
        assert_eq!(table.len().unwrap(), 4);
        drop(table);
        drop(txn);
        assert!(!gc_invisible_logs(&db).unwrap());
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(LOG)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn committed_history_cleanup_survives_restart_after_log_purge() {
        use crate::domain::{CommandBody, RunStatus};
        use crate::ids::{CommandId, RunId};
        use crate::time::EngineTime;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.redb");
        let db = Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let catalog = crate::catalog::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = crate::compiler::compile_yaml(
            "\
dsl: graphrun/v1
id: persisted_history
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {literal: null}
",
            &catalog,
        )
        .unwrap();
        let run = RunId::from_bytes([7; 16]);
        let entry = |index, time_ms, body| Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(RaftRequest {
                command: Command {
                    id: CommandId::from_bytes([index as u8; 16]),
                    time: EngineTime::from_millis(time_ms),
                    body,
                },
            }),
        };
        let start = entry(
            1,
            1,
            CommandBody::Start {
                run,
                definition: Box::new(definition),
                catalog: Box::new(catalog),
                input: crate::value::Value::Null,
            },
        );
        let progress = entry(2, 2, CommandBody::Progress { run });
        append_logs(&db, &[start.clone(), progress.clone()]).unwrap();
        let mut applied = None;
        let mut membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut state = State::default();
        let mut schedule_revision = 0;
        let responses = apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut state,
            &mut schedule_revision,
            vec![start, progress],
        )
        .unwrap();
        assert!(responses.iter().all(|reply| reply.error.is_none()));
        assert!(matches!(
            state.runs[&run].status,
            RunStatus::Succeeded { .. }
        ));
        let through = state.history[&run].len() as u64;
        assert_eq!(state.checkpoints[&run].through_run_sequence, through);
        purge_logs(
            &db,
            2,
            &LogId::new(openraft::CommittedLeaderId::new(1, 1), 2),
        )
        .unwrap();
        assert!(get_logs(&db, 1, 3).unwrap().is_empty());
        assert_eq!(
            crate::history::page(&state, run, 0, 100, EngineTime::from_millis(3))
                .unwrap()
                .retained_through,
            through
        );

        let expiry = entry(
            3,
            2 + 30 * 24 * 60 * 60 * 1000,
            CommandBody::PruneHistory { limit: 128 },
        );
        let response = apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut state,
            &mut schedule_revision,
            vec![expiry],
        )
        .unwrap();
        assert!(response[0].error.is_none());
        assert!(!state.runs.contains_key(&run));
        drop(db);

        let (handle, thread) = StorageHandle::open(&path).unwrap();
        let restored = handle.query_state().await;
        let page = crate::history::page(
            &restored,
            run,
            0,
            100,
            EngineTime::from_millis(2 + 30 * 24 * 60 * 60 * 1000),
        )
        .unwrap();
        assert!(page.unavailable);
        assert_eq!(page.unavailable_range.unwrap().last, through);
        handle.shutdown();
        thread.join().unwrap();
    }

    #[test]
    fn unapplied_credits_bound_append_without_truncating_reads() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        initialize_publication_store(&db).unwrap();
        let batch: Vec<EntryT> = (1..=crate::limits::UNAPPLIED_ENTRIES)
            .map(|index| Entry::<TypeConfig> {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), u64::from(index)),
                payload: EntryPayload::Blank,
            })
            .collect();
        append_logs(&db, &batch).unwrap();
        let extra = [Entry::<TypeConfig> {
            log_id: LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                u64::from(crate::limits::UNAPPLIED_ENTRIES) + 1,
            ),
            payload: EntryPayload::Blank,
        }];
        let err = admit_and_append(&db, None, &extra).unwrap_err();
        assert!(err.to_string().contains("unapplied entry credit"));
        let loaded = get_logs(&db, 1, u64::MAX).unwrap();
        assert_eq!(loaded.len(), crate::limits::UNAPPLIED_ENTRIES as usize);
        assert_eq!(
            loaded.last().unwrap().get_log_id().index,
            u64::from(crate::limits::UNAPPLIED_ENTRIES)
        );
    }

    #[test]
    fn unapplied_credits_follow_overwrite_apply_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("member.redb")).unwrap();
        initialize_publication_store(&db).unwrap();
        let entry = |index, term| Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        };
        let first = entry(1, 1);
        let second = entry(2, 1);
        admit_and_append(&db, None, &[first.clone(), second]).unwrap();
        let bytes = persisted_credits(&db).unwrap().bytes;
        assert_eq!(persisted_credits(&db).unwrap().entries, 2);
        let replacement = entry(2, 2);
        let size = serde_json::to_vec(&replacement).unwrap().len() as u64;
        let projected = projected_append_usage(&db, None, &[(2, size)]).unwrap();
        assert_eq!(projected.entries, 2);
        admit_and_append(&db, None, std::slice::from_ref(&replacement)).unwrap();
        assert_eq!(persisted_credits(&db).unwrap().bytes, projected.bytes);
        assert_eq!(persisted_credits(&db).unwrap().entries, 2);
        assert!(bytes > 0);

        let mut applied = None;
        let mut membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut domain = State::default();
        let mut schedule_revision = 0;
        apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut domain,
            &mut schedule_revision,
            vec![first],
        )
        .unwrap();
        let remaining = persisted_credits(&db).unwrap();
        assert_eq!(remaining.entries, 1);
        assert_eq!(remaining.bytes, size);
        truncate_logs(&db, 2).unwrap();
        assert_eq!(persisted_credits(&db).unwrap().entries, 0);
        assert_eq!(persisted_credits(&db).unwrap().bytes, 0);
    }
}
