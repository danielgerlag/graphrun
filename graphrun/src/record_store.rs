use crate::domain::{DomainEvent, State};
use crate::error::{Error, ErrorKind, Result};
use crate::ids::{ActivationId, CommandId, RunId, ScopeId, WaitId, WorkerSessionId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const FORMAT: &str = "graphrun.state-record/v2";
pub(crate) const FRAGMENT_FORMAT: &str = "graphrun.record-fragments/v1";
const NESTED_FIELDS: &[&str] = &["published_definitions", "start_keys"];
const HISTORY_FIELDS: &[&str] = &["history", "history_records"];
const LIST_FIELDS: &[&str] = &["inbox", "obligations"];
const RUN_MAP_FIELDS: &[&str] = &[
    "runs",
    "history_dependencies",
    "checkpoints",
    "terminal_summaries",
];
const RUN_OWNED_FIELDS: &[&str] = &[
    "scopes",
    "activations",
    "waits",
    "inbox",
    "obligations",
    "signal_tombstones",
];
const RUN_ACTIVATION_FIELDS: &[&str] = &[
    "loop_carry",
    "foreach_items",
    "foreach_done",
    "parallel_done",
    "saga_errors",
    "interventions",
];
const OMITTED_EMPTY_MAP_FIELDS: &[&str] = &[
    "command_actors",
    "command_external_ids",
    "authenticated_request_digests",
];
const FRAGMENT_BYTES: usize = 1024 * 1024;
const MAX_RECORD_BYTES: usize = 65 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct VersionedRecord {
    format: String,
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    order: Option<u64>,
    value: Value,
}

#[derive(Serialize, Deserialize)]
struct FragmentHeader {
    format: String,
    bytes: u64,
    chunks: u32,
    sha256: String,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::FailedPrecondition, message.into())
}

pub(crate) fn run_record_fields() -> impl Iterator<Item = &'static str> {
    RUN_MAP_FIELDS
        .iter()
        .chain(RUN_OWNED_FIELDS)
        .chain(RUN_ACTIVATION_FIELDS)
        .copied()
}

fn owner(field: &str, key: Option<&str>, value: &Value, state: &State) -> Result<Option<String>> {
    if RUN_MAP_FIELDS.contains(&field) {
        return Ok(key.map(str::to_owned));
    }
    if RUN_OWNED_FIELDS.contains(&field) {
        return value
            .get("run")
            .and_then(Value::as_str)
            .map(|run| Some(run.to_owned()))
            .ok_or_else(|| invalid(format!("{field} record has no run owner")));
    }
    if RUN_ACTIVATION_FIELDS.contains(&field) {
        let id = key.ok_or_else(|| invalid(format!("{field} has no activation identity")))?;
        let activation = crate::ids::ActivationId::from_hex(id)
            .map_err(|err| invalid(format!("invalid {field} activation: {err}")))?;
        return state
            .activations
            .get(&activation)
            .map(|act| Some(act.run.to_hex()))
            .ok_or_else(|| invalid(format!("{field} activation owner missing")));
    }
    Ok(None)
}

fn record_key(
    generation: u64,
    run: Option<&str>,
    field: &str,
    tag: &str,
    key: &str,
    nested: Option<&str>,
) -> String {
    let run = run.map_or_else(|| "global".to_owned(), |run| format!("run-{run}"));
    let primary = hex::encode(key.as_bytes());
    let nested = nested
        .map(|value| hex::encode(value.as_bytes()))
        .unwrap_or_default();
    format!("{generation:016x}/{run}/{field}/{tag}/{primary}/{nested}")
}

pub(crate) fn map_key(generation: u64, field: &str, key: &str) -> String {
    record_key(generation, None, field, "map", key, None)
}

pub(crate) fn run_map_key(generation: u64, run: RunId, field: &str) -> String {
    record_key(
        generation,
        Some(&run.to_hex()),
        field,
        "map",
        &run.to_hex(),
        None,
    )
}

pub(crate) fn run_list_key(generation: u64, run: RunId, field: &str, id: &str) -> String {
    record_key(generation, Some(&run.to_hex()), field, "list", id, None)
}

pub(crate) fn nested_key(
    generation: u64,
    run: Option<RunId>,
    field: &str,
    group: &str,
    child: &str,
) -> String {
    let run = run.map(|run| run.to_hex());
    record_key(
        generation,
        run.as_deref(),
        field,
        "nested",
        group,
        Some(child),
    )
}

pub(crate) fn encode_nested_value(
    generation: u64,
    revision: u64,
    run: Option<RunId>,
    field: &str,
    group: &str,
    child: &str,
    value: Value,
) -> Result<(String, Vec<u8>)> {
    let key = nested_key(generation, run, field, group, child);
    let mut rows = BTreeMap::new();
    insert(&mut rows, key.clone(), value, revision)?;
    let bytes = rows
        .remove(&key)
        .ok_or_else(|| invalid("nested record encoding failed"))?;
    Ok((key, bytes))
}

pub(crate) fn encode_value(
    generation: u64,
    revision: u64,
    run: Option<RunId>,
    field: &str,
    tag: &str,
    id: &str,
    value: Value,
) -> Result<(String, Vec<u8>)> {
    let run = run.map(|run| run.to_hex());
    let key = record_key(generation, run.as_deref(), field, tag, id, None);
    let mut rows = BTreeMap::new();
    insert(&mut rows, key.clone(), value, revision)?;
    let bytes = rows
        .remove(&key)
        .ok_or_else(|| invalid("record encoding failed"))?;
    Ok((key, bytes))
}

pub(crate) fn retired_key(generation: u64, run: RunId) -> String {
    record_key(generation, None, "retired_runs", "map", &run.to_hex(), None)
}

pub(crate) fn retired_marker(
    generation: u64,
    revision: u64,
    run: RunId,
) -> Result<(String, Vec<u8>)> {
    let key = retired_key(generation, run);
    let mut rows = BTreeMap::new();
    insert(&mut rows, key.clone(), Value::Bool(true), revision)?;
    let bytes = rows
        .remove(&key)
        .ok_or_else(|| invalid("retired run marker missing"))?;
    Ok((key, bytes))
}

pub(crate) fn is_retired_marker(key: &str, bytes: &[u8], generation: u64) -> Result<Option<RunId>> {
    let pieces: Vec<_> = key.split('/').collect();
    if pieces.len() != 6 || pieces[0] != format!("{generation:016x}") {
        return Err(invalid("invalid retired run marker generation"));
    }
    if pieces[2] != "retired_runs" {
        return Ok(None);
    }
    if pieces[1] != "global" || pieces[3] != "map" || !pieces[5].is_empty() {
        return Err(invalid("malformed retired run marker"));
    }
    let run = RunId::from_hex(&decode_key(pieces[4])?).map_err(invalid)?;
    let record: VersionedRecord = serde_json::from_slice(bytes)
        .map_err(|err| invalid(format!("corrupt retired run marker: {err}")))?;
    if record.format != FORMAT
        || record.revision == 0
        || record.order.is_some()
        || record.value != Value::Bool(true)
    {
        return Err(invalid("unsupported retired run marker"));
    }
    Ok(Some(run))
}

pub(crate) fn hidden_retired_physical(key: &str, retired: &BTreeMap<RunId, bool>) -> bool {
    let pieces: Vec<_> = key.split('/').collect();
    pieces.len() >= 6
        && pieces[1]
            .strip_prefix("run-")
            .and_then(|run| RunId::from_hex(run).ok())
            .and_then(|run| retired.get(&run))
            .is_some_and(|has_summary| {
                pieces[2] != "terminal_summaries"
                    && (!matches!(pieces[2], "start_keys" | "signal_tombstones") || !has_summary)
            })
}

pub(crate) fn history_sequence(key: &str) -> Result<u64> {
    let parts: Vec<_> = key.split('/').collect();
    if parts.len() < 6 || !HISTORY_FIELDS.contains(&parts[2]) || parts[3] != "sequence" {
        return Err(invalid("invalid history record key"));
    }
    decode_key(parts[4])?
        .parse()
        .map_err(|err| invalid(format!("invalid history sequence: {err}")))
}

fn insert(
    rows: &mut BTreeMap<String, Vec<u8>>,
    key: String,
    value: Value,
    revision: u64,
) -> Result<()> {
    let bytes = serde_json::to_vec(&VersionedRecord {
        format: FORMAT.to_owned(),
        revision,
        order: None,
        value,
    })
    .map_err(|err| Error::invalid(err.to_string()))?;
    if rows.insert(key, bytes).is_some() {
        return Err(invalid("duplicate state record identity"));
    }
    Ok(())
}

fn insert_ordered(
    rows: &mut BTreeMap<String, Vec<u8>>,
    key: String,
    value: Value,
    revision: u64,
    order: u64,
) -> Result<()> {
    let bytes = serde_json::to_vec(&VersionedRecord {
        format: FORMAT.to_owned(),
        revision,
        order: Some(order),
        value,
    })
    .map_err(|err| Error::invalid(err.to_string()))?;
    if rows.insert(key, bytes).is_some() {
        return Err(invalid("duplicate state list identity"));
    }
    Ok(())
}

fn list_identity(field: &str, value: &Value) -> Result<(String, String)> {
    let run = value
        .get("run")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{field} entry has no run identity")))?;
    let identity = match field {
        "inbox" => "event_id",
        "obligations" => "forward",
        _ => return Err(invalid("unsupported state list")),
    };
    let id = value
        .get(identity)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{field} entry has no {identity}")))?;
    Ok((run.to_owned(), id.to_owned()))
}

fn order_key(generation: u64, field: &str) -> String {
    record_key(
        generation,
        None,
        &format!("{field}_order"),
        "scalar",
        "",
        None,
    )
}

pub(crate) fn encode_scalar(
    generation: u64,
    revision: u64,
    field: &str,
    value: Value,
) -> Result<(String, Vec<u8>)> {
    if field != "engine_time_watermark_ms" {
        return Err(invalid("unsupported scalar update"));
    }
    let key = record_key(generation, None, field, "scalar", "", None);
    let mut rows = BTreeMap::new();
    insert(&mut rows, key.clone(), value, revision)?;
    let bytes = rows
        .remove(&key)
        .ok_or_else(|| invalid("scalar encoding failed"))?;
    Ok((key, bytes))
}

pub(crate) fn scalar_key(generation: u64, field: &str) -> String {
    record_key(generation, None, field, "scalar", "", None)
}

pub(crate) fn decode_scalar(field: &str, bytes: &[u8]) -> Result<Value> {
    if !matches!(field, "artifact_origins" | "current_cluster_id") {
        return Err(invalid("unsupported scalar read"));
    }
    let record: VersionedRecord = serde_json::from_slice(bytes)
        .map_err(|err| invalid(format!("corrupt {field} scalar: {err}")))?;
    if record.format != FORMAT || record.revision == 0 {
        return Err(invalid(format!("unsupported {field} scalar version")));
    }
    Ok(record.value)
}

pub(crate) fn same_value(left: &[u8], right: &[u8]) -> Result<bool> {
    let left: VersionedRecord = serde_json::from_slice(left)
        .map_err(|err| invalid(format!("stored state record is corrupt: {err}")))?;
    let right: VersionedRecord = serde_json::from_slice(right)
        .map_err(|err| invalid(format!("new state record is corrupt: {err}")))?;
    if left.format != FORMAT || right.format != FORMAT || left.revision == 0 || right.revision == 0
    {
        return Err(invalid("unsupported state record version"));
    }
    Ok(left.value == right.value && left.order == right.order)
}

pub(crate) fn frame_records(
    logical: impl IntoIterator<Item = (String, Vec<u8>)>,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut physical = BTreeMap::new();
    for (key, bytes) in logical {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("state record exceeds the 65 MiB per-record limit"));
        }
        if bytes.len() <= FRAGMENT_BYTES {
            if physical.insert(key, bytes).is_some() {
                return Err(invalid("duplicate physical state record"));
            }
            continue;
        }
        let chunks = bytes.len().div_ceil(FRAGMENT_BYTES);
        let header = FragmentHeader {
            format: FRAGMENT_FORMAT.to_owned(),
            bytes: bytes.len() as u64,
            chunks: u32::try_from(chunks).map_err(|_| invalid("too many record fragments"))?,
            sha256: hex::encode(Sha256::digest(&bytes)),
        };
        let head = serde_json::to_vec(&header).map_err(|err| Error::invalid(err.to_string()))?;
        if physical.insert(key.clone(), head).is_some() {
            return Err(invalid("duplicate physical state record"));
        }
        for (index, fragment) in bytes.chunks(FRAGMENT_BYTES).enumerate() {
            let fragment_key = format!("{key}/fragment/{index:08}");
            if physical.insert(fragment_key, fragment.to_vec()).is_some() {
                return Err(invalid("duplicate state record fragment"));
            }
        }
    }
    Ok(physical)
}

pub(crate) fn restore_records(
    physical: impl IntoIterator<Item = (String, Vec<u8>)>,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut heads = BTreeMap::new();
    let mut fragments: BTreeMap<String, BTreeMap<u32, Vec<u8>>> = BTreeMap::new();
    for (key, value) in physical {
        let segments: Vec<_> = key.split('/').collect();
        if segments.len() == 6 {
            if heads.insert(key, value).is_some() {
                return Err(invalid("duplicate state record"));
            }
        } else if segments.len() == 8 && segments[6] == "fragment" {
            let index = segments[7]
                .parse::<u32>()
                .map_err(|err| invalid(format!("invalid fragment index: {err}")))?;
            let base = segments[..6].join("/");
            if fragments
                .entry(base)
                .or_default()
                .insert(index, value)
                .is_some()
            {
                return Err(invalid("duplicate state record fragment"));
            }
        } else {
            return Err(invalid("unsupported physical state record key"));
        }
    }
    for (key, chunks) in fragments {
        let header = heads
            .get_mut(&key)
            .ok_or_else(|| invalid("state record fragments have no header"))?;
        let manifest: FragmentHeader = serde_json::from_slice(header)
            .map_err(|err| invalid(format!("invalid state record fragment header: {err}")))?;
        let total = usize::try_from(manifest.bytes)
            .map_err(|_| invalid("state record fragment length overflows usize"))?;
        if manifest.format != FRAGMENT_FORMAT
            || total > MAX_RECORD_BYTES
            || manifest.chunks as usize != chunks.len()
            || manifest.chunks == 0
        {
            return Err(invalid("unsupported or incomplete state record fragments"));
        }
        let mut assembled = Vec::with_capacity(total);
        for index in 0..manifest.chunks {
            let fragment = chunks
                .get(&index)
                .ok_or_else(|| invalid("state record fragment sequence has a gap"))?;
            if fragment.is_empty() || fragment.len() > FRAGMENT_BYTES {
                return Err(invalid("state record fragment has invalid length"));
            }
            assembled.extend_from_slice(fragment);
        }
        if assembled.len() != total || hex::encode(Sha256::digest(&assembled)) != manifest.sha256 {
            return Err(invalid("state record fragment checksum mismatch"));
        }
        *header = assembled;
    }
    for value in heads.values() {
        if let Ok(header) = serde_json::from_slice::<FragmentHeader>(value)
            && header.format == FRAGMENT_FORMAT
        {
            return Err(invalid("state record fragments are missing"));
        }
    }
    Ok(heads)
}

pub(crate) fn encode(
    state: &State,
    generation: u64,
    revision: u64,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let Value::Object(defaults) =
        serde_json::to_value(State::default()).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("default state must be an object"));
    };
    let Value::Object(fields) =
        serde_json::to_value(state).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("state must be an object"));
    };
    let mut rows = BTreeMap::new();
    for (field, value) in fields {
        if HISTORY_FIELDS.contains(&field.as_str()) {
            let Value::Object(runs) = value else {
                return Err(invalid(format!("{field} must be indexed by run")));
            };
            for (run, entries) in runs {
                let Value::Array(entries) = entries else {
                    return Err(invalid(format!("{field} must contain ordered events")));
                };
                for (index, entry) in entries.into_iter().enumerate() {
                    let sequence = index as u64 + 1;
                    let key = record_key(
                        generation,
                        Some(&run),
                        &field,
                        "sequence",
                        &format!("{sequence:020}"),
                        None,
                    );
                    insert(&mut rows, key, entry, revision)?;
                }
            }
        } else if NESTED_FIELDS.contains(&field.as_str()) {
            let Value::Object(groups) = value else {
                return Err(invalid(format!("{field} must be an object")));
            };
            for (group, children) in groups {
                let Value::Object(children) = children else {
                    return Err(invalid(format!("{field} group must be an object")));
                };
                for (child, value) in children {
                    let run = if field == "start_keys" {
                        Some(
                            value
                                .get("run")
                                .and_then(Value::as_str)
                                .ok_or_else(|| invalid("start key has no run identity"))?,
                        )
                    } else {
                        None
                    };
                    let key = record_key(generation, run, &field, "nested", &group, Some(&child));
                    insert(&mut rows, key, value, revision)?;
                }
            }
        } else if LIST_FIELDS.contains(&field.as_str()) {
            let Value::Array(entries) = value else {
                return Err(invalid(format!("{field} must be an array")));
            };
            let count = entries.len();
            for (index, value) in entries.into_iter().enumerate() {
                let (run, identity) = list_identity(&field, &value)?;
                let key = record_key(generation, Some(&run), &field, "list", &identity, None);
                insert_ordered(&mut rows, key, value, revision, index as u64)?;
            }
            insert(
                &mut rows,
                order_key(generation, &field),
                Value::from(count as u64),
                revision,
            )?;
        } else if defaults.get(&field).is_some_and(Value::is_object)
            || OMITTED_EMPTY_MAP_FIELDS.contains(&field.as_str())
        {
            let Value::Object(entries) = value else {
                return Err(invalid(format!("{field} must be an object")));
            };
            for (entry, value) in entries {
                let run = owner(&field, Some(&entry), &value, state)?;
                let key = record_key(generation, run.as_deref(), &field, "map", &entry, None);
                insert(&mut rows, key, value, revision)?;
            }
        } else {
            let key = record_key(generation, None, &field, "scalar", "", None);
            insert(&mut rows, key, value, revision)?;
        }
    }
    Ok(rows)
}

pub(crate) fn encode_applied(
    state: &State,
    generation: u64,
    revision: u64,
    runs: &BTreeSet<RunId>,
    events: &[DomainEvent],
    command_id: Option<CommandId>,
    sessions: &BTreeSet<WorkerSessionId>,
    previous: &BTreeMap<String, Vec<u8>>,
    history_from: &BTreeMap<RunId, u64>,
) -> Result<(BTreeMap<String, Vec<u8>>, State)> {
    let mut selected = State {
        current_cluster_id: state.current_cluster_id.clone(),
        artifact_origins: state.artifact_origins.clone(),
        engine_time_watermark_ms: state.engine_time_watermark_ms,
        next_generation: state.next_generation,
        fair_cursor: state.fair_cursor,
        recovery: state.recovery.clone(),
        ..State::default()
    };
    if let Some(id) = command_id {
        if let Some(value) = state.commands.get(&id) {
            selected.commands.insert(id, value.clone());
        }
        if let Some(value) = state.command_external_ids.get(&id) {
            selected.command_external_ids.insert(id, *value);
        }
        if let Some(value) = state.command_actors.get(&id) {
            selected.command_actors.insert(id, value.clone());
        }
        if let Some(value) = state.authenticated_request_digests.get(&id) {
            selected
                .authenticated_request_digests
                .insert(id, value.clone());
        }
        if let Some(value) = state.legacy_start_digests.get(&id) {
            selected.legacy_start_digests.insert(id, value.clone());
        }
        if let Some(value) = state.worker_command_digests.get(&id) {
            selected.worker_command_digests.insert(id, value.clone());
        }
        if let Some(value) = state.command_times.get(&id) {
            selected.command_times.insert(id, *value);
        }
    }
    for id in sessions {
        if let Some(value) = state.sessions.get(id) {
            selected.sessions.insert(*id, value.clone());
        }
    }
    for event in events {
        if let DomainEvent::EventAccepted { run, event_id, .. } = event {
            let key = format!("{}:{}", run.to_hex(), event_id.to_hex());
            if let Some(value) = state.signal_tombstones.get(&key) {
                selected.signal_tombstones.insert(key, value.clone());
            }
        }
    }

    for run in runs {
        let name = run.to_hex();
        let prefix = format!("{generation:016x}/run-{name}/");
        let mut scopes = BTreeSet::new();
        let mut activations = BTreeSet::new();
        let mut waits = BTreeSet::new();
        for key in previous.keys().filter(|key| key.starts_with(&prefix)) {
            let parts: Vec<_> = key.split('/').collect();
            let primary = decode_key(parts[4])?;
            match parts[2] {
                "scopes" => {
                    scopes.insert(ScopeId::from_hex(&primary).map_err(invalid)?);
                }
                "activations" => {
                    activations.insert(ActivationId::from_hex(&primary).map_err(invalid)?);
                }
                "waits" => {
                    waits.insert(WaitId::from_hex(&primary).map_err(invalid)?);
                }
                "signal_tombstones" => {
                    if let Some(value) = state.signal_tombstones.get(&primary) {
                        selected.signal_tombstones.insert(primary, value.clone());
                    }
                }
                _ => {}
            }
        }
        for event in events {
            if let Some(owner) = crate::domain::event_owner(state, event)?
                && owner == *run
            {
                match event {
                    DomainEvent::ScopeOpened { scope, .. } => {
                        scopes.insert(*scope);
                    }
                    DomainEvent::ActivationOpened { activation, .. }
                    | DomainEvent::CompensationStarted { activation, .. } => {
                        activations.insert(*activation);
                    }
                    DomainEvent::WaitOpened { wait, .. } => {
                        waits.insert(*wait);
                    }
                    _ => {}
                }
            }
        }
        if let Some(value) = state.runs.get(run) {
            selected.runs.insert(*run, value.clone());
        }
        if let Some(value) = state.history_dependencies.get(run) {
            selected.history_dependencies.insert(*run, value.clone());
        }
        if let Some(value) = state.checkpoints.get(run) {
            selected.checkpoints.insert(*run, value.clone());
        }
        if let Some(value) = state.terminal_summaries.get(run) {
            selected.terminal_summaries.insert(*run, value.clone());
        }
        for id in &scopes {
            if let Some(value) = state.scopes.get(id) {
                selected.scopes.insert(*id, value.clone());
            }
        }
        for id in &activations {
            if let Some(value) = state.activations.get(id) {
                selected.activations.insert(*id, value.clone());
            }
            if let Some(value) = state.loop_carry.get(id) {
                selected.loop_carry.insert(*id, value.clone());
            }
            if let Some(value) = state.foreach_items.get(id) {
                selected.foreach_items.insert(*id, value.clone());
            }
            if let Some(value) = state.foreach_done.get(id) {
                selected.foreach_done.insert(*id, value.clone());
            }
            if let Some(value) = state.parallel_done.get(id) {
                selected.parallel_done.insert(*id, value.clone());
            }
            if let Some(value) = state.saga_errors.get(id) {
                selected.saga_errors.insert(*id, value.clone());
            }
            if let Some(value) = state.interventions.get(id) {
                selected.interventions.insert(*id, value.clone());
            }
        }
        for id in &waits {
            if let Some(value) = state.waits.get(id) {
                selected.waits.insert(*id, value.clone());
            }
        }
    }
    let encoded = encode(&selected, generation, revision)?;
    let id = command_id.map(|id| id.to_hex());
    let session_keys: BTreeSet<_> = sessions.iter().map(|id| id.to_hex()).collect();
    let mut result = BTreeMap::new();
    for (key, value) in encoded {
        let parts: Vec<_> = key.split('/').collect();
        let keep = if parts[1] != "global" {
            runs.iter()
                .any(|run| parts[1] == format!("run-{}", run.to_hex()))
        } else {
            match parts[2] {
                "current_cluster_id"
                | "artifact_origins"
                | "engine_time_watermark_ms"
                | "next_generation"
                | "fair_cursor"
                | "recovery" => true,
                "sessions" => session_keys.contains(&decode_key(parts[4])?),
                "commands"
                | "command_actors"
                | "command_external_ids"
                | "authenticated_request_digests"
                | "legacy_start_digests"
                | "worker_command_digests"
                | "command_times" => id.as_deref() == Some(decode_key(parts[4])?.as_str()),
                _ => false,
            }
        };
        if keep {
            result.insert(key, value);
        }
    }
    for run in runs {
        let first = *history_from.get(run).unwrap_or(&0);
        let events = state.history.get(run).map(Vec::as_slice).unwrap_or(&[]);
        let records = state
            .history_records
            .get(run)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if events.len() != records.len() || events.len() < first as usize {
            return Err(invalid("retained history sequence cannot be appended"));
        }
        for (field, entries) in [
            (
                "history",
                events
                    .iter()
                    .skip(first as usize)
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>(),
            ),
            (
                "history_records",
                records
                    .iter()
                    .skip(first as usize)
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>(),
            ),
        ] {
            let entries =
                entries.map_err(|err| invalid(format!("cannot encode history record: {err}")))?;
            for (offset, value) in entries.into_iter().enumerate() {
                let key = record_key(
                    generation,
                    Some(&run.to_hex()),
                    field,
                    "sequence",
                    &format!("{:020}", first + offset as u64 + 1),
                    None,
                );
                insert(&mut result, key, value, revision)?;
            }
        }
    }
    for (field, entries) in [
        (
            "inbox",
            state
                .inbox
                .iter()
                .filter(|item| item.run.is_some_and(|run| runs.contains(&run)))
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>(),
        ),
        (
            "obligations",
            state
                .obligations
                .iter()
                .filter(|item| runs.contains(&item.run))
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>(),
        ),
    ] {
        let entries = entries.map_err(|err| invalid(format!("cannot encode {field}: {err}")))?;
        let counter_key = order_key(generation, field);
        let counter = previous
            .get(&counter_key)
            .ok_or_else(|| invalid(format!("{field} order counter is missing")))?;
        let counter: VersionedRecord = serde_json::from_slice(counter)
            .map_err(|err| invalid(format!("invalid {field} order counter: {err}")))?;
        if counter.format != FORMAT || counter.order.is_some() {
            return Err(invalid(format!("unsupported {field} order counter")));
        }
        let mut next_order = counter
            .value
            .as_u64()
            .ok_or_else(|| invalid(format!("{field} order counter is not an integer")))?;
        for value in entries {
            let (run, identity) = list_identity(field, &value)?;
            if !runs.iter().any(|id| id.to_hex() == run) {
                continue;
            }
            let key = record_key(generation, Some(&run), field, "list", &identity, None);
            let order = if let Some(old) = previous.get(&key) {
                let old: VersionedRecord = serde_json::from_slice(old)
                    .map_err(|err| invalid(format!("corrupt {field} entry: {err}")))?;
                if old.format != FORMAT {
                    return Err(invalid(format!("unsupported {field} entry")));
                }
                old.order
                    .ok_or_else(|| invalid(format!("{field} entry has no order")))?
            } else {
                let assigned = next_order;
                next_order = next_order
                    .checked_add(1)
                    .ok_or_else(|| invalid(format!("{field} order exhausted")))?;
                assigned
            };
            insert_ordered(&mut result, key, value, revision, order)?;
        }
        insert(&mut result, counter_key, Value::from(next_order), revision)?;
    }
    Ok((result, selected))
}

fn decode_key(key: &str) -> Result<String> {
    let bytes = hex::decode(key).map_err(|err| invalid(format!("bad record key: {err}")))?;
    String::from_utf8(bytes).map_err(|err| invalid(format!("record key is not UTF-8: {err}")))
}

fn add_to_array(
    arrays: &mut BTreeMap<(String, String), BTreeMap<u64, Value>>,
    field: &str,
    group: &str,
    sequence: &str,
    value: Value,
) -> Result<()> {
    let sequence = sequence
        .parse::<u64>()
        .map_err(|err| invalid(format!("invalid {field} sequence: {err}")))?;
    if arrays
        .entry((field.to_owned(), group.to_owned()))
        .or_default()
        .insert(sequence, value)
        .is_some()
    {
        return Err(invalid("duplicate state array sequence"));
    }
    Ok(())
}

fn hidden_retired_row(key: &str, retired: &BTreeSet<RunId>, summarized: &BTreeSet<RunId>) -> bool {
    let pieces: Vec<_> = key.split('/').collect();
    pieces.len() == 6
        && pieces[1]
            .strip_prefix("run-")
            .and_then(|run| RunId::from_hex(run).ok())
            .is_some_and(|run| {
                retired.contains(&run)
                    && pieces[2] != "terminal_summaries"
                    && (!matches!(pieces[2], "start_keys" | "signal_tombstones")
                        || !summarized.contains(&run))
            })
}

pub(crate) fn decode(
    rows: impl IntoIterator<Item = (String, Vec<u8>)>,
    generation: u64,
) -> Result<State> {
    let Value::Object(mut root) =
        serde_json::to_value(State::default()).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("default state must be an object"));
    };
    let mut arrays: BTreeMap<(String, String), BTreeMap<u64, Value>> = BTreeMap::new();
    let mut list_counters = BTreeMap::new();
    let mut seen_keys = std::collections::BTreeSet::new();
    let rows: Vec<_> = rows.into_iter().collect();
    let retired = rows
        .iter()
        .try_fold(BTreeSet::new(), |mut retired, (key, bytes)| {
            if let Some(run) = is_retired_marker(key, bytes, generation)? {
                if !retired.insert(run) {
                    return Err(invalid("duplicate retired run marker"));
                }
            }
            Ok(retired)
        })?;
    let summarized: BTreeSet<_> = rows
        .iter()
        .filter_map(|(key, _)| {
            let pieces: Vec<_> = key.split('/').collect();
            (pieces.len() == 6 && pieces[2] == "terminal_summaries")
                .then(|| pieces[1].strip_prefix("run-"))
                .flatten()
                .and_then(|run| RunId::from_hex(run).ok())
        })
        .collect();
    for (key, bytes) in rows {
        if !seen_keys.insert(key.clone()) {
            return Err(invalid("duplicate state record key"));
        }
        let pieces: Vec<_> = key.split('/').collect();
        if pieces.len() == 6 && pieces[2] == "retired_runs" {
            continue;
        }
        if hidden_retired_row(&key, &retired, &summarized) {
            continue;
        }
        if pieces.len() != 6
            || pieces[0] != format!("{generation:016x}")
            || (!root.contains_key(pieces[2])
                && !OMITTED_EMPTY_MAP_FIELDS.contains(&pieces[2])
                && !matches!(pieces[2], "inbox_order" | "obligations_order"))
        {
            return Err(invalid("unknown or cross-generation state record key"));
        }
        let value: VersionedRecord = serde_json::from_slice(&bytes)
            .map_err(|err| invalid(format!("corrupt state record: {err}")))?;
        if value.format != FORMAT || value.revision == 0 {
            return Err(invalid("unsupported state record format or revision"));
        }
        let field = pieces[2];
        let primary = decode_key(pieces[4])?;
        let run = pieces[1].strip_prefix("run-");
        if matches!(field, "inbox_order" | "obligations_order") {
            if pieces[1] != "global"
                || pieces[3] != "scalar"
                || !primary.is_empty()
                || !pieces[5].is_empty()
                || value.order.is_some()
            {
                return Err(invalid("malformed list order counter"));
            }
            let counter = value
                .value
                .as_u64()
                .ok_or_else(|| invalid("invalid list order counter"))?;
            list_counters.insert(field.to_owned(), counter);
            continue;
        }
        match pieces[3] {
            "scalar" if pieces[1] == "global" && primary.is_empty() && value.order.is_none() => {
                root.insert(field.to_owned(), value.value);
            }
            "map" if pieces[5].is_empty() && value.order.is_none() => {
                let map = root
                    .entry(field.to_owned())
                    .or_insert_with(|| Value::Object(Map::new()))
                    .as_object_mut()
                    .ok_or_else(|| invalid(format!("{field} is not a map")))?;
                if map.insert(primary, value.value).is_some() {
                    return Err(invalid("duplicate state map record"));
                }
            }
            "nested"
                if (pieces[1] == "global" || (field == "start_keys" && run.is_some()))
                    && value.order.is_none() =>
            {
                if field == "start_keys" && value.value.get("run").and_then(Value::as_str) != run {
                    return Err(invalid("start key run owner differs from value"));
                }
                let nested = decode_key(pieces[5])?;
                let group = root
                    .get_mut(field)
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| invalid(format!("{field} is not a nested map")))?;
                let members = group
                    .entry(primary)
                    .or_insert_with(|| Value::Object(Map::new()))
                    .as_object_mut()
                    .ok_or_else(|| invalid("nested state record group has wrong type"))?;
                if members.insert(nested, value.value).is_some() {
                    return Err(invalid("duplicate nested state record"));
                }
            }
            "sequence"
                if HISTORY_FIELDS.contains(&field) && run.is_some() && value.order.is_none() =>
            {
                add_to_array(
                    &mut arrays,
                    field,
                    run.expect("checked"),
                    &primary,
                    value.value,
                )?;
            }
            "list" if LIST_FIELDS.contains(&field) && run.is_some() => {
                let order = value
                    .order
                    .ok_or_else(|| invalid(format!("{field} entry has no ordering value")))?;
                let (owner, identity) = list_identity(field, &value.value)?;
                if run != Some(owner.as_str()) || identity != primary {
                    return Err(invalid(format!(
                        "{field} record identity differs from value"
                    )));
                }
                add_to_array(&mut arrays, field, "", &order.to_string(), value.value)?;
            }
            _ => return Err(invalid("malformed state record key")),
        }
    }
    for ((field, group), values) in arrays {
        let historical = HISTORY_FIELDS.contains(&field.as_str());
        let first = u64::from(historical);
        let mut ordered = Vec::with_capacity(values.len());
        for (sequence, value) in values {
            if historical && sequence != first + ordered.len() as u64 {
                return Err(invalid(format!("{field} has a missing sequence")));
            }
            if !historical
                && sequence
                    >= *list_counters
                        .get(&format!("{field}_order"))
                        .ok_or_else(|| invalid(format!("{field} order counter missing")))?
            {
                return Err(invalid(format!("{field} entry exceeds order counter")));
            }
            ordered.push(value);
        }
        if group.is_empty() {
            root.insert(field, Value::Array(ordered));
        } else {
            root.get_mut(&field)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| invalid("history field is not a map"))?
                .insert(group, Value::Array(ordered));
        }
    }
    for field in LIST_FIELDS {
        if !list_counters.contains_key(&format!("{field}_order")) {
            return Err(invalid(format!("{field} order counter missing")));
        }
    }
    let state: State = serde_json::from_value(Value::Object(root))
        .map_err(|err| invalid(format!("state records cannot be assembled: {err}")))?;
    let expected = encode(&state, generation, 1)?;
    if !expected.keys().all(|key| seen_keys.contains(key))
        || expected.len()
            != seen_keys
                .iter()
                .filter(|key| {
                    let pieces: Vec<_> = key.split('/').collect();
                    pieces[2] != "retired_runs" && !hidden_retired_row(key, &retired, &summarized)
                })
                .count()
    {
        return Err(invalid(
            "state record owner or identity does not match its value",
        ));
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{InboxEntry, Obligation, ObligationStatus};
    use crate::ids::{ActivationId, CommandId, EventId, RunId, ScopeId};
    use crate::time::EngineTime;

    #[test]
    fn round_trips_versioned_events_and_command_maps() {
        let run = RunId::from_bytes([7; 16]);
        let command = CommandId::from_bytes([9; 16]);
        let mut state = State {
            engine_time_watermark_ms: 42,
            ..State::default()
        };
        state.command_times.insert(command, 42);
        state.history.insert(
            run,
            vec![crate::domain::DomainEvent::RunAdmitted {
                run,
                definition_id: "test".to_owned(),
                definition_version: 1,
                input: crate::Value::Null,
                root: ScopeId::from_bytes([2; 16]),
                policy: crate::policy::CapturedRunPolicy::defaults(None),
                admitted_ms: EngineTime::from_millis(42).as_millis(),
            }],
        );
        let rows = encode(&state, 3, 11).unwrap();
        assert!(rows.keys().any(|key| key.contains("/history/sequence/")));
        let restored = decode(rows, 3).unwrap();
        assert_eq!(
            serde_json::to_value(restored).unwrap(),
            serde_json::to_value(state).unwrap()
        );
    }

    #[test]
    fn selected_records_do_not_include_unrelated_runs_or_receipts() {
        let run = RunId::from_bytes([1; 16]);
        let command = CommandId::from_bytes([2; 16]);
        let mut state = State::default();
        state.command_times.insert(command, 7);
        state.commands.insert(command, Vec::new());
        let event = DomainEvent::RunAdmitted {
            run,
            definition_id: "selected".to_owned(),
            definition_version: 1,
            input: crate::Value::Null,
            root: ScopeId::from_bytes([3; 16]),
            policy: crate::policy::CapturedRunPolicy::defaults(None),
            admitted_ms: 7,
        };
        state.history.insert(run, vec![event.clone()]);
        state.history_records.insert(
            run,
            vec![crate::history::HistoryEntry {
                format: crate::history::EVENT_FORMAT.to_owned(),
                run,
                sequence: 1,
                command_id: Some(command),
                principal_id: None,
                recorded_ms: 7,
                payload: crate::history::ArtifactRef::capture_in(
                    "",
                    "graphrun.domain-event/v1",
                    &event,
                )
                .unwrap(),
                input_ref: None,
                output_ref: None,
            }],
        );
        let runs = BTreeSet::from([run]);
        let initial = encode(&State::default(), 1, 1).unwrap();
        let selected = encode_applied(
            &state,
            1,
            7,
            &runs,
            &[],
            Some(command),
            &BTreeSet::new(),
            &initial,
            &BTreeMap::new(),
        )
        .unwrap()
        .0;
        for number in 4..=100 {
            let other_run = RunId::from_bytes([number; 16]);
            let other_command = CommandId::from_bytes([number; 16]);
            state.history.insert(
                other_run,
                vec![DomainEvent::RunAdmitted {
                    run: other_run,
                    definition_id: "unrelated".to_owned(),
                    definition_version: 1,
                    input: crate::Value::Null,
                    root: ScopeId::from_bytes([number; 16]),
                    policy: crate::policy::CapturedRunPolicy::defaults(None),
                    admitted_ms: 7,
                }],
            );
            state.command_times.insert(other_command, 7);
            state.commands.insert(other_command, Vec::new());
        }
        let with_unrelated = encode_applied(
            &state,
            1,
            7,
            &runs,
            &[],
            Some(command),
            &BTreeSet::new(),
            &initial,
            &BTreeMap::new(),
        )
        .unwrap()
        .0;
        assert_eq!(with_unrelated, selected);
        assert_eq!(with_unrelated.len(), 12);
    }

    #[test]
    fn retiring_one_run_does_not_rekey_other_run_lists() {
        let retired = RunId::from_bytes([1; 16]);
        let retained = RunId::from_bytes([2; 16]);
        let inbox = |run, byte, sequence| InboxEntry {
            run: Some(run),
            event_id: EventId::from_bytes([byte; 16]),
            signal: "approved".to_owned(),
            key: "k".to_owned(),
            payload: crate::Value::Null,
            sequence,
            reserved_wait: None,
            consumed: false,
            accepted_ms: sequence,
            expires_ms: 100,
        };
        let obligation = |run, byte| Obligation {
            run,
            forward: ActivationId::from_bytes([byte; 16]),
            owner: ActivationId::from_bytes([byte; 16]),
            handler: "undo".to_owned(),
            handler_version: 1,
            input: crate::Value::Null,
            status: ObligationStatus::Open,
        };
        let mut state = State {
            inbox: vec![inbox(retired, 3, 1), inbox(retained, 4, 1)],
            obligations: vec![obligation(retired, 5), obligation(retained, 6)],
            ..State::default()
        };
        let old = encode(&state, 1, 1).unwrap();
        state.inbox.remove(0);
        state.obligations.remove(0);
        let next = encode(&state, 1, 2).unwrap();
        for field in ["inbox", "obligations"] {
            let old_key = old
                .keys()
                .find(|key| key.contains(&format!("/run-{}/{}", retained.to_hex(), field)))
                .unwrap();
            assert!(
                next.contains_key(old_key),
                "{field} changed its retained identity"
            );
        }
    }

    #[test]
    fn retired_marker_hides_history_but_keeps_summary_and_signal_tombstone() {
        let retired = RunId::from_bytes([11; 16]);
        let active = RunId::from_bytes([12; 16]);
        let event = |run| DomainEvent::RunAdmitted {
            run,
            definition_id: "test".to_owned(),
            definition_version: 1,
            input: crate::Value::Null,
            root: ScopeId::from_bytes([13; 16]),
            policy: crate::policy::CapturedRunPolicy::defaults(None),
            admitted_ms: 10,
        };
        let mut state = State::default();
        state.history.insert(retired, vec![event(retired)]);
        state.history.insert(active, vec![event(active)]);
        state.terminal_summaries.insert(
            retired,
            crate::history::TerminalSummary {
                run: retired,
                workflow: "test".to_owned(),
                version: 1,
                status: "succeeded".to_owned(),
                terminal_ms: 10,
                expires_ms: 20,
                history_through: 1,
            },
        );
        let event_id = EventId::from_bytes([14; 16]);
        let tombstone_key = format!("{}:{}", retired.to_hex(), event_id.to_hex());
        state.signal_tombstones.insert(
            tombstone_key.clone(),
            crate::history::SignalTombstone {
                run: retired,
                event_id,
                signal: "approved".to_owned(),
                key: "order".to_owned(),
                payload: crate::history::ArtifactRef::capture_in(
                    "",
                    "graphrun.signal-payload/v1",
                    &crate::Value::Null,
                )
                .unwrap(),
            },
        );
        let mut rows = encode(&state, 1, 1).unwrap();
        let (key, marker) = retired_marker(1, 2, retired).unwrap();
        rows.insert(key, marker);
        let visible = decode(rows, 1).unwrap();
        assert!(!visible.history.contains_key(&retired));
        assert_eq!(visible.history[&active].len(), 1);
        assert_eq!(visible.terminal_summaries[&retired].history_through, 1);
        assert!(visible.signal_tombstones.contains_key(&tombstone_key));
    }

    #[test]
    fn rejects_unknown_versions_and_missing_event_sequence() {
        let mut rows = encode(&State::default(), 1, 1).unwrap();
        let key = rows.keys().next().unwrap().clone();
        let mut record: VersionedRecord = serde_json::from_slice(&rows[&key]).unwrap();
        record.format = "graphrun.state-record/v99".to_owned();
        rows.insert(key, serde_json::to_vec(&record).unwrap());
        assert_eq!(
            decode(rows, 1).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn run_records_keep_scope_owners_and_reject_a_foreign_owner() {
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
        let run = RunId::from_bytes([3; 16]);
        let mut state = State::default();
        crate::domain::start_run(
            &mut state,
            Command {
                id: CommandId::from_bytes([4; 16]),
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
                time: EngineTime::from_millis(1),
            },
            definition,
            catalog,
        )
        .unwrap();
        let rows = encode(&state, 1, 1).unwrap();
        assert!(
            rows.keys()
                .any(|key| key.contains(&format!("/run-{}/scopes/map/", run.to_hex())))
        );
        assert_eq!(
            serde_json::to_value(decode(rows.clone(), 1).unwrap()).unwrap(),
            serde_json::to_value(state).unwrap()
        );
        let (key, value) = rows
            .iter()
            .find(|(key, _)| key.contains("/scopes/map/"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .unwrap();
        let mut corrupted = rows;
        corrupted.remove(&key);
        corrupted.insert(key.replace(&run.to_hex(), &"f".repeat(32)), value);
        assert_eq!(
            decode(corrupted, 1).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn fragments_large_rows_and_rejects_missing_or_corrupt_chunks() {
        let key = record_key(1, None, "recovery", "scalar", "", None);
        let value = serde_json::to_vec(&VersionedRecord {
            format: FORMAT.to_owned(),
            revision: 1,
            order: None,
            value: Value::String("x".repeat(5 * FRAGMENT_BYTES)),
        })
        .unwrap();
        let rows = frame_records(std::iter::once((key.clone(), value.clone()))).unwrap();
        assert!(rows.len() > 5);
        assert!(
            rows.values()
                .all(|fragment| fragment.len() <= FRAGMENT_BYTES)
        );
        assert_eq!(restore_records(rows.clone()).unwrap()[&key], value);
        let fragment = rows
            .keys()
            .find(|key| key.contains("/fragment/"))
            .unwrap()
            .clone();
        let mut missing = rows.clone();
        missing.remove(&fragment);
        assert_eq!(
            restore_records(missing).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
        let mut corrupt = rows;
        corrupt.get_mut(&fragment).unwrap()[0] ^= 1;
        assert_eq!(
            restore_records(corrupt).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn scoped_actor_receipt_keys_remain_individual_records() {
        let external = CommandId::from_bytes([1; 16]);
        let internal = crate::domain::authenticated_command_id("local-owner", external).unwrap();
        let mut state = State::default();
        state
            .command_actors
            .insert(internal, "local-owner".to_owned());
        state.command_external_ids.insert(internal, external);
        state
            .authenticated_request_digests
            .insert(internal, "body-digest".to_owned());
        let rows = encode(&state, 1, 1).unwrap();
        for field in [
            "command_actors",
            "command_external_ids",
            "authenticated_request_digests",
        ] {
            assert!(
                rows.keys()
                    .any(|key| key.contains(&format!("/{field}/map/")))
            );
            assert!(
                !rows
                    .keys()
                    .any(|key| key.contains(&format!("/{field}/scalar/")))
            );
        }
        let restored = decode(rows, 1).unwrap();
        assert_eq!(restored.command_actors[&internal], "local-owner");
        assert_eq!(restored.command_external_ids[&internal], external);
        assert_eq!(
            restored.authenticated_request_digests[&internal],
            "body-digest"
        );
    }
}
