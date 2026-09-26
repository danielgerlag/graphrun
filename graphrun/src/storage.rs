#![allow(clippy::result_large_err)]

use crate::domain::{self, Command, State};
use crate::error::{Error, Result};
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
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor, Seek, SeekFrom};
use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread::JoinHandle;
use tokio::sync::oneshot;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftRequest {
    pub command: Command,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RaftResponse {
    pub error: Option<String>,
    #[serde(default)]
    pub command_result: Option<crate::publication::CommandResult>,
    #[serde(default)]
    pub run_id: Option<crate::ids::RunId>,
}

const META: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("meta");
const LOG: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("log");

type LogIdT = LogId<u64>;
type VoteT = Vote<u64>;
type EntryT = Entry<TypeConfig>;
type MembershipT = StoredMembership<u64, BasicNode>;
type SnapMeta = SnapshotMeta<u64, BasicNode>;
type StoErr = StorageError<u64>;

fn sto_err(verb: ErrorVerb, err: impl std::fmt::Display) -> StoErr {
    StorageIOError::new(ErrorSubject::Store, verb, AnyError::error(err.to_string())).into()
}

struct Cut {
    point: &'static str,
}

static CUT: Mutex<Option<Cut>> = Mutex::new(None);

thread_local! {
    static LOCAL_CUT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
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
        return Err(sto_err(ErrorVerb::Write, format!("fault cut {point}")));
    }
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
    Apply(
        Vec<EntryT>,
        oneshot::Sender<std::result::Result<Vec<RaftResponse>, StoErr>>,
    ),
    BuildSnapshot(oneshot::Sender<std::result::Result<Snapshot<TypeConfig>, StoErr>>),
    InstallSnapshot(
        SnapMeta,
        Vec<u8>,
        oneshot::Sender<std::result::Result<(), StoErr>>,
    ),
    CurrentSnapshot(oneshot::Sender<std::result::Result<Option<Snapshot<TypeConfig>>, StoErr>>),
    QueryState(oneshot::Sender<State>),
    InstallDomain(Box<State>, oneshot::Sender<std::result::Result<(), StoErr>>),
    Shutdown,
}

#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<Req>,
}

impl StorageHandle {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        Self::open_inner(path, false)
    }

    pub(crate) fn open_restored(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        Self::open_inner(path, true)
    }

    fn open_inner(path: impl AsRef<Path>, restoring: bool) -> Result<(Self, JoinHandle<()>)> {
        let path = path.as_ref().to_path_buf();
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
        let (tx, rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("graphrun-storage".into())
            .spawn(move || storage_thread(path, rx, ready_tx))
            .map_err(|err| Error::invalid(err.to_string()))?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok((Self { tx }, handle)),
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

    pub async fn query_state(&self) -> State {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Req::QueryState(tx));
        rx.await.unwrap_or_default()
    }

    pub async fn install_domain(&self, state: State) -> Result<()> {
        validate_history_store(&state)?;
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Req::InstallDomain(Box::new(state), tx))
            .map_err(|_| Error::invalid("storage thread stopped"))?;
        rx.await
            .map_err(|_| Error::invalid("storage thread dropped"))?
            .map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn last_applied_index(&self) -> u64 {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Req::AppliedState(tx));
        match rx.await {
            Ok(Ok((Some(id), _))) => id.index,
            _ => 0,
        }
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(Req::Shutdown);
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
    Ok(state)
}

fn validate_history_store(state: &State) -> Result<()> {
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

fn storage_thread(
    path: PathBuf,
    rx: mpsc::Receiver<Req>,
    ready: mpsc::Sender<std::result::Result<(), String>>,
) {
    let db = match Database::create(&path) {
        Ok(db) => db,
        Err(err) => {
            let _ = ready.send(Err(format!("redb open failed: {err}")));
            return;
        }
    };
    if load_json::<_, String>(&db, "publication_start_format").is_none() {
        if let Err(err) = initialize_publication_store(&db) {
            let _ = ready.send(Err(err.to_string()));
            return;
        }
    }
    let _ = ready.send(Ok(()));
    let mut last_purged: Option<LogIdT> = load_json(&db, "last_purged");
    let mut last_applied: Option<LogIdT> = load_json(&db, "last_applied");
    let mut last_membership: MembershipT = load_json(&db, "membership")
        .unwrap_or_else(|| StoredMembership::new(None, Membership::new(vec![], None)));
    let mut domain: State = load_json(&db, "domain").unwrap_or_default();
    let snapshot_dir = path
        .parent()
        .map(|parent| parent.join("snapshots"))
        .unwrap_or_else(|| PathBuf::from("snapshots"));
    let mut snapshot: Option<(SnapMeta, Vec<u8>)> = match (
        load_json(&db, "snapshot_meta"),
        load_bytes(&db, "snapshot_data"),
    ) {
        (Some(meta), Some(data)) => Some((meta, data)),
        _ => None,
    };
    while let Ok(req) = rx.recv() {
        match req {
            Req::Shutdown => break,
            Req::SaveVote(vote, tx) => {
                let _ = tx.send(put_json(&db, "vote", &vote));
            }
            Req::ReadVote(tx) => {
                let _ = tx.send(Ok(load_json(&db, "vote")));
            }
            Req::Append(entries, tx) => {
                let _ = tx.send(admit_and_append(&db, last_applied, &entries));
            }
            Req::Truncate(log_id, tx) => {
                let _ = tx.send(truncate_logs(&db, log_id.index));
            }
            Req::Purge(log_id, tx) => {
                last_purged = Some(log_id);
                let res = purge_logs(&db, log_id.index, &log_id);
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
            Req::Apply(entries, tx) => {
                let res = apply_entries(
                    &db,
                    &mut last_applied,
                    &mut last_membership,
                    &mut domain,
                    entries,
                );
                let _ = tx.send(res);
            }
            Req::BuildSnapshot(tx) => {
                let res = build_snapshot(last_applied, last_membership.clone(), &domain);
                let res = match res {
                    Ok(snap) => {
                        if let Err(err) =
                            write_snapshot_file(&snapshot_dir, &snap.meta, snap.snapshot.get_ref())
                        {
                            Err(sto_err(ErrorVerb::Write, err))
                        } else if let Err(err) = after_persist("after-snapshot-file") {
                            Err(err)
                        } else {
                            snapshot = Some((snap.meta.clone(), snap.snapshot.get_ref().clone()));
                            if let Err(err) = put_json(&db, "snapshot_meta", &snap.meta) {
                                Err(err)
                            } else if let Err(err) =
                                put_bytes(&db, "snapshot_data", snap.snapshot.get_ref())
                            {
                                Err(err)
                            } else if let Err(err) = after_persist("after-snapshot-activate") {
                                Err(err)
                            } else {
                                Ok(snap)
                            }
                        }
                    }
                    Err(err) => Err(err),
                };
                let _ = tx.send(res);
            }
            Req::InstallSnapshot(meta, data, tx) => {
                let res = install_snapshot(
                    &db,
                    &mut last_applied,
                    &mut last_membership,
                    &mut domain,
                    &mut snapshot,
                    meta,
                    data,
                );
                let _ = tx.send(res);
            }
            Req::CurrentSnapshot(tx) => {
                let out = snapshot.as_ref().map(|(meta, data)| Snapshot {
                    meta: meta.clone(),
                    snapshot: Box::new(Cursor::new(data.clone())),
                });
                let _ = tx.send(Ok(out));
            }
            Req::QueryState(tx) => {
                let _ = tx.send(domain.clone());
            }
            Req::InstallDomain(next, tx) => {
                let res = validate_history_store(&next)
                    .map_err(|err| sto_err(ErrorVerb::Write, err))
                    .and_then(|()| put_json(&db, "domain", &next));
                if res.is_ok() {
                    domain = *next;
                }
                let _ = tx.send(res);
            }
        }
    }
}

fn initialize_publication_store(db: &Database) -> std::result::Result<(), StoErr> {
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
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
    }
    txn.open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))
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
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut table = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert(key, bytes)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    Ok(())
}

fn encoded_entry_len(entry: &EntryT) -> u64 {
    serde_json::to_vec(entry)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn admit_and_append(
    db: &Database,
    last_applied: Option<LogIdT>,
    entries: &[EntryT],
) -> std::result::Result<(), StoErr> {
    let applied = last_applied.map(|id| id.index).unwrap_or(0);
    let pending = get_logs(db, applied.saturating_add(1), u64::MAX)?;
    let mut sizes = BTreeMap::new();
    for entry in pending.iter().chain(entries.iter()) {
        sizes.insert(entry.get_log_id().index, encoded_entry_len(entry));
    }
    if sizes.len() > crate::limits::UNAPPLIED_ENTRIES as usize {
        return Err(sto_err(
            ErrorVerb::Write,
            "unapplied entry credit exhausted",
        ));
    }
    let bytes: u64 = sizes.values().copied().sum();
    if bytes > crate::limits::UNAPPLIED_BYTES {
        return Err(sto_err(ErrorVerb::Write, "unapplied byte credit exhausted"));
    }
    append_logs(db, entries)
}

fn append_logs(db: &Database, entries: &[EntryT]) -> std::result::Result<(), StoErr> {
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut table = txn
            .open_table(LOG)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        for entry in entries {
            let bytes = serde_json::to_vec(entry).map_err(|err| sto_err(ErrorVerb::Write, err))?;
            table
                .insert(&entry.get_log_id().index, bytes.as_slice())
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    after_persist("after-log")?;
    Ok(())
}

fn write_snapshot_file(dir: &Path, meta: &SnapMeta, data: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let digest = hex::encode(Sha256::digest(data));
    let take = digest.len().min(16);
    let name = format!("{}-{}.snap", meta.snapshot_id, &digest[..take]);
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, data)?;
    std::fs::rename(tmp, dir.join(name))?;
    Ok(())
}

fn truncate_logs(db: &Database, from: u64) -> std::result::Result<(), StoErr> {
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut table = txn
            .open_table(LOG)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let keys: Vec<u64> = table
            .range(from..)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?
            .filter_map(|row| row.ok().map(|(k, _)| k.value()))
            .collect();
        for key in keys {
            table
                .remove(&key)
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    Ok(())
}

fn purge_logs(
    db: &Database,
    through: u64,
    last_purged: &LogIdT,
) -> std::result::Result<(), StoErr> {
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut logs = txn
            .open_table(LOG)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let keys: Vec<u64> = logs
            .range(..=through)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?
            .filter_map(|row| row.ok().map(|(k, _)| k.value()))
            .collect();
        for key in keys {
            logs.remove(&key)
                .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        }
        let mut meta = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let bytes =
            serde_json::to_vec(last_purged).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("last_purged", bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    Ok(())
}

fn log_state(
    db: &Database,
    last_purged: Option<LogIdT>,
) -> std::result::Result<LogState<TypeConfig>, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let last_log_id = match table.last().map_err(|err| sto_err(ErrorVerb::Read, err))? {
        Some((_, value)) => {
            let entry: EntryT = serde_json::from_slice(value.value())
                .map_err(|err| sto_err(ErrorVerb::Read, err))?;
            Some(*entry.get_log_id())
        }
        None => last_purged,
    };
    Ok(LogState {
        last_purged_log_id: last_purged,
        last_log_id,
    })
}

fn get_logs(db: &Database, start: u64, end: u64) -> std::result::Result<Vec<EntryT>, StoErr> {
    let txn = db
        .begin_read()
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let table = txn
        .open_table(LOG)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    let mut out = Vec::new();
    let iter = table
        .range(start..end)
        .map_err(|err| sto_err(ErrorVerb::Read, err))?;
    for row in iter {
        let (_, value) = row.map_err(|err| sto_err(ErrorVerb::Read, err))?;
        let entry: EntryT =
            serde_json::from_slice(value.value()).map_err(|err| sto_err(ErrorVerb::Read, err))?;
        out.push(entry);
    }
    Ok(out)
}

fn apply_entries(
    db: &Database,
    last_applied: &mut Option<LogIdT>,
    last_membership: &mut MembershipT,
    domain: &mut State,
    entries: Vec<EntryT>,
) -> std::result::Result<Vec<RaftResponse>, StoErr> {
    let mut replies = Vec::new();
    let mut next_applied = *last_applied;
    let mut next_membership = last_membership.clone();
    let mut next_domain = domain.clone();
    let mut domain_changed = false;
    let mut membership_changed = false;
    for entry in entries {
        next_applied = Some(*entry.get_log_id());
        if let Some(membership) = openraft::entry::RaftPayload::get_membership(&entry.payload) {
            next_membership = StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
            membership_changed = true;
        }
        let mut reply = RaftResponse::default();
        if let EntryPayload::Normal(req) = &entry.payload {
            if let domain::CommandBody::Publication { key, operation } = &req.command.body {
                if key.command_id != req.command.id
                    || key.cluster_id.is_empty()
                    || key.principal_id.is_empty()
                {
                    reply.error = Some("invalid authenticated command identity".to_owned());
                } else {
                    let receipt =
                        crate::publication::apply(&mut next_domain, &req.command, key, operation);
                    domain_changed = true;
                    if let Err(err) = receipt.ensure_applied() {
                        reply.error = Some(err.to_string());
                    }
                    reply.command_result = Some(receipt);
                }
            } else {
                let mut candidate = next_domain.clone();
                match domain::commit_command(&mut candidate, req.command.clone()) {
                    Ok(events) => {
                        next_domain = candidate;
                        domain_changed = true;
                        reply.run_id = events.iter().find_map(|event| match event {
                            domain::DomainEvent::RunAdmitted { run, .. } => Some(*run),
                            _ => None,
                        });
                    }
                    Err(err) => reply.error = Some(err.to_string()),
                }
            }
        }
        replies.push(reply);
    }
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    {
        let mut meta = txn
            .open_table(META)
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let applied =
            serde_json::to_vec(&next_applied).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("last_applied", applied.as_slice())
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
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    *last_applied = next_applied;
    *last_membership = next_membership;
    *domain = next_domain;
    after_persist("after-apply")?;
    Ok(replies)
}

fn build_snapshot(
    last_applied: Option<LogIdT>,
    last_membership: MembershipT,
    domain: &State,
) -> std::result::Result<Snapshot<TypeConfig>, StoErr> {
    let payload = serde_json::to_vec(&(last_applied, last_membership.clone(), domain))
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let meta = SnapshotMeta {
        last_log_id: last_applied,
        last_membership,
        snapshot_id: format!("snap-{}", last_applied.map(|id| id.index).unwrap_or(0)),
    };
    Ok(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(payload)),
    })
}

fn install_snapshot(
    db: &Database,
    last_applied: &mut Option<LogIdT>,
    last_membership: &mut MembershipT,
    domain: &mut State,
    snapshot: &mut Option<(SnapMeta, Vec<u8>)>,
    meta: SnapMeta,
    data: Vec<u8>,
) -> std::result::Result<(), StoErr> {
    let (applied, membership, restored): (Option<LogIdT>, MembershipT, State) =
        serde_json::from_slice(&data).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    validate_history_store(&restored).map_err(|err| sto_err(ErrorVerb::Write, err))?;
    let mut txn = db
        .begin_write()
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|err| sto_err(ErrorVerb::Write, err))?;
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
        let meta_bytes = serde_json::to_vec(&meta).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("snapshot_meta", meta_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("snapshot_data", data.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
    *last_applied = applied;
    *last_membership = membership;
    *domain = restored;
    *snapshot = Some((meta, data));
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
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StoErr> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapMeta,
        mut snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StoErr> {
        snapshot
            .seek(SeekFrom::Start(0))
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let data = snapshot.get_ref().clone();
        let meta = meta.clone();
        self.send(|tx| Req::InstallSnapshot(meta, data, tx)).await
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
        {
            let txn = db.begin_write().unwrap();
            let _ = txn.open_table(LOG);
            let _ = txn.open_table(META);
            txn.commit().unwrap();
        }
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
        {
            let txn = db.begin_write().unwrap();
            let _ = txn.open_table(LOG);
            let _ = txn.open_table(META);
            txn.commit().unwrap();
        }
        let mut last_applied = None;
        let mut last_membership = StoredMembership::new(None, Membership::new(vec![], None));
        let mut domain = State::default();
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
    fn snapshot_install_cut_keeps_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        {
            let txn = db.begin_write().unwrap();
            let _ = txn.open_table(LOG);
            let _ = txn.open_table(META);
            txn.commit().unwrap();
        }
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
        let meta = SnapshotMeta {
            last_log_id: new_applied,
            last_membership: new_membership.clone(),
            snapshot_id: "snap-9".to_owned(),
        };
        let mut last_applied = old_applied;
        let mut last_membership = old_membership.clone();
        let mut domain = old_domain.clone();
        let mut snapshot = None;
        inject_cut("after-snapshot");
        let err = install_snapshot(
            &db,
            &mut last_applied,
            &mut last_membership,
            &mut domain,
            &mut snapshot,
            meta,
            data,
        )
        .unwrap_err();
        clear_cut();
        assert!(err.to_string().contains("fault cut"));
        let disk_applied: Option<LogIdT> = load_json(&db, "last_applied");
        let disk_domain: State = load_json(&db, "domain").unwrap();
        let disk_meta: Option<SnapMeta> = load_json(&db, "snapshot_meta");
        let disk_data: Option<Vec<u8>> = load_bytes(&db, "snapshot_data");
        assert_eq!(disk_applied, new_applied);
        assert_eq!(last_applied, new_applied);
        assert_eq!(disk_applied, last_applied);
        assert!(disk_meta.is_some());
        assert!(disk_data.is_some());
        let _ = (old_domain, disk_domain);
    }

    #[test]
    fn purge_does_not_drop_domain_history() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("member.redb");
        let db = redb::Database::create(&path).unwrap();
        {
            let txn = db.begin_write().unwrap();
            let _ = txn.open_table(LOG);
            let _ = txn.open_table(META);
            txn.commit().unwrap();
        }
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
        let responses = apply_entries(
            &db,
            &mut applied,
            &mut membership,
            &mut state,
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
        let response =
            apply_entries(&db, &mut applied, &mut membership, &mut state, vec![expiry]).unwrap();
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
        {
            let txn = db.begin_write().unwrap();
            let _ = txn.open_table(LOG);
            let _ = txn.open_table(META);
            txn.commit().unwrap();
        }
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
}
