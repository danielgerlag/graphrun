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

static CUT: Mutex<Option<&'static str>> = Mutex::new(None);

#[cfg(any(test, feature = "fault-injection"))]
pub fn inject_cut(point: &'static str) {
    *CUT.lock().unwrap() = Some(point);
}

fn after_persist(point: &'static str) -> std::result::Result<(), StoErr> {
    if CUT.lock().unwrap().as_deref() == Some(point) {
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
    Shutdown,
}

#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<Req>,
}

impl StorageHandle {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, JoinHandle<()>)> {
        let path = path.as_ref().to_path_buf();
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

    pub fn shutdown(&self) {
        let _ = self.tx.send(Req::Shutdown);
    }
}

pub fn load_domain_readonly(path: impl AsRef<Path>) -> Result<State> {
    let db = ReadOnlyDatabase::open(path.as_ref())
        .map_err(|err| Error::new(crate::error::ErrorKind::FailedPrecondition, err.to_string()))?;
    Ok(load_json(&db, "domain").unwrap_or_default())
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
    let _ = ready.send(Ok(()));
    if let Ok(txn) = db.begin_write() {
        let _ = txn.open_table(META);
        let _ = txn.open_table(LOG);
        let _ = txn.commit();
    }
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
                let _ = tx.send(append_logs(&db, &entries));
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
                if let Ok(ref snap) = res {
                    let _ = write_snapshot_file(&snapshot_dir, &snap.meta, snap.snapshot.get_ref());
                    snapshot = Some((snap.meta.clone(), snap.snapshot.get_ref().clone()));
                    let _ = put_json(&db, "snapshot_meta", &snap.meta);
                    let _ = put_bytes(&db, "snapshot_data", snap.snapshot.get_ref());
                }
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
        }
    }
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
    for entry in entries {
        *last_applied = Some(*entry.get_log_id());
        if let Some(membership) = openraft::entry::RaftPayload::get_membership(&entry.payload) {
            *last_membership = StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
        }
        let mut reply = RaftResponse::default();
        if let EntryPayload::Normal(req) = &entry.payload {
            match domain::commit_command(domain, req.command.clone()) {
                Ok(_) => {}
                Err(err) => reply.error = Some(err.to_string()),
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
            serde_json::to_vec(last_applied).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("last_applied", applied.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let membership =
            serde_json::to_vec(last_membership).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("membership", membership.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let domain_bytes =
            serde_json::to_vec(domain).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        meta.insert("domain", domain_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
    }
    txn.commit().map_err(|err| sto_err(ErrorVerb::Write, err))?;
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
    *last_applied = applied;
    *last_membership = membership;
    *domain = restored;
    *snapshot = Some((meta.clone(), data.clone()));
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
            serde_json::to_vec(last_applied).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("last_applied", applied_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let membership_bytes =
            serde_json::to_vec(last_membership).map_err(|err| sto_err(ErrorVerb::Write, err))?;
        table
            .insert("membership", membership_bytes.as_slice())
            .map_err(|err| sto_err(ErrorVerb::Write, err))?;
        let domain_bytes =
            serde_json::to_vec(domain).map_err(|err| sto_err(ErrorVerb::Write, err))?;
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
    fn log_cut_after_persist_keeps_entries() {
        inject_cut("after-log");
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
        let err = append_logs(&db, &[entry]).unwrap_err();
        assert!(err.to_string().contains("fault cut"));
        let loaded = get_logs(&db, 1, 2).unwrap();
        assert_eq!(loaded.len(), 1);
    }
}
