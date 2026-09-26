//! Recorded workflow facts and read-only historical projections.
use crate::domain::{self, DomainEvent, RunState, RunStatus, State};
use crate::error::{Error, ErrorKind, Result};
use crate::ids::{CommandId, EventId, RunId};
use crate::time::EngineTime;
use crate::value::canonical_json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BinaryHeap};

pub const EVENT_FORMAT: &str = "graphrun.run-event/v1";
pub const CHECKPOINT_FORMAT: &str = "graphrun.run-checkpoint/v2";
pub const ARTIFACT_FORMAT: &str = "graphrun-artifact/v1";
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
pub const PAGE_LIMIT: u32 = 100;
pub const MAX_PAGE_LIMIT: u32 = 1000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub format: String,
    pub schema: String,
    pub byte_len: u64,
    pub sha256: String,
}

impl ArtifactRef {
    pub fn capture<T: Serialize>(schema: &str, value: &T) -> Result<Self> {
        let json = serde_json::to_value(value)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.to_string()))?;
        let bytes = canonical_json(&json)
            .map_err(|err| Error::new(ErrorKind::FailedPrecondition, err.message))?;
        let mut digest = Sha256::new();
        digest.update(b"graphrun-artifact/v1\0");
        digest.update(schema.as_bytes());
        digest.update([0]);
        digest.update(&bytes);
        Ok(Self {
            format: ARTIFACT_FORMAT.to_owned(),
            schema: schema.to_owned(),
            byte_len: bytes.len() as u64,
            sha256: hex::encode(digest.finalize()),
        })
    }

    pub fn verify<T: Serialize>(&self, schema: &str, value: &T) -> Result<()> {
        if self != &Self::capture(schema, value).map_err(|err| unavailable(err.message))? {
            return Err(unavailable(
                "missing, corrupt or unsupported retained artifact",
            ));
        }
        Ok(())
    }
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unavailable, message)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredArtifacts {
    pub definition: ArtifactRef,
    pub catalog: ArtifactRef,
    pub policy: ArtifactRef,
    pub schemas: BTreeMap<String, ArtifactRef>,
    pub contracts: BTreeMap<String, ArtifactRef>,
}

impl RequiredArtifacts {
    fn capture(run: &RunState) -> Result<Self> {
        let mut schemas = BTreeMap::new();
        for (key, schema) in &run.catalog.schemas {
            let key = key.as_stable_name();
            schemas.insert(
                key.clone(),
                ArtifactRef::capture(&format!("graphrun.schema/{key}"), schema)?,
            );
        }
        let mut contracts = BTreeMap::new();
        for (key, contract) in &run.catalog.activities {
            let key = format!("activity/{}", key.as_stable_name());
            contracts.insert(
                key.clone(),
                ArtifactRef::capture(&format!("graphrun.contract/{key}"), contract)?,
            );
        }
        for (key, contract) in &run.catalog.reconcilers {
            let key = format!("reconciler/{}", key.as_stable_name());
            contracts.insert(
                key.clone(),
                ArtifactRef::capture(&format!("graphrun.contract/{key}"), contract)?,
            );
        }
        Ok(Self {
            definition: ArtifactRef::capture("graphrun.definition/v1", &run.definition)?,
            catalog: ArtifactRef::capture("graphrun.catalog/v1", &run.catalog)?,
            policy: ArtifactRef::capture("graphrun.run-policy/v1", &run.policy)?,
            schemas,
            contracts,
        })
    }

    fn verify(&self, run: &RunState) -> Result<()> {
        if self != &Self::capture(run).map_err(|err| unavailable(err.message))? {
            return Err(unavailable(
                "missing, corrupt or unsupported retained definition, schema, contract or policy",
            ));
        }
        if let Some(published) = &run.published {
            published
                .verify(&run.definition, &run.catalog)
                .map_err(|err| unavailable(err.message))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub format: String,
    pub run: RunId,
    pub sequence: u64,
    pub command_id: Option<CommandId>,
    pub principal_id: Option<String>,
    pub recorded_ms: u64,
    pub payload: ArtifactRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordedEvent {
    #[serde(flatten)]
    pub record: HistoryEntry,
    pub event: DomainEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryPage {
    pub run: RunId,
    pub retained_from: u64,
    pub retained_through: u64,
    pub next_cursor: Option<u64>,
    pub unavailable: bool,
    pub unavailable_range: Option<UnavailableRange>,
    pub events: Vec<RecordedEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnavailableRange {
    pub first: u64,
    pub last: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunCheckpoint {
    pub format: String,
    pub through_run_sequence: u64,
    pub required: RequiredArtifacts,
    pub projection_ref: ArtifactRef,
    pub projection: Box<State>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalSummary {
    pub run: RunId,
    pub workflow: String,
    pub version: u32,
    pub status: String,
    pub terminal_ms: u64,
    pub expires_ms: u64,
    pub history_through: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalTombstone {
    pub run: RunId,
    pub event_id: EventId,
    pub signal: String,
    pub key: String,
    pub payload: ArtifactRef,
}

pub(crate) fn append(
    state: &mut State,
    run: RunId,
    event: &DomainEvent,
    command_id: Option<CommandId>,
    principal_id: Option<&str>,
    recorded_ms: u64,
) -> Result<()> {
    let sequence = state
        .history
        .get(&run)
        .map_or(1, |events| events.len() as u64 + 1);
    let required = if matches!(event, DomainEvent::RunAdmitted { .. }) {
        Some(RequiredArtifacts::capture(&state.runs[&run])?)
    } else {
        None
    };
    let payload_ref = ArtifactRef::capture("graphrun.domain-event/v1", event)?;
    let signal_ref = match event {
        DomainEvent::EventAccepted { payload, .. } => {
            Some(ArtifactRef::capture("graphrun.signal-payload/v1", payload)?)
        }
        _ => None,
    };
    if let Some(required) = required {
        state.history_dependencies.insert(run, required);
    }
    state.history.entry(run).or_default().push(event.clone());
    state
        .history_records
        .entry(run)
        .or_default()
        .push(HistoryEntry {
            format: EVENT_FORMAT.to_owned(),
            run,
            sequence,
            command_id,
            principal_id: principal_id.map(str::to_owned),
            recorded_ms,
            payload: payload_ref,
        });
    if let DomainEvent::EventAccepted {
        event_id,
        signal,
        key,
        ..
    } = event
    {
        state.signal_tombstones.insert(
            format!("{}:{}", run.to_hex(), event_id.to_hex()),
            SignalTombstone {
                run,
                event_id: *event_id,
                signal: signal.clone(),
                key: key.clone(),
                payload: signal_ref.expect("accepted event has a payload reference"),
            },
        );
    }
    Ok(())
}

fn retained(state: &State, run: RunId, now: EngineTime) -> Result<(&RunState, u64)> {
    let Some(run_state) = state.runs.get(&run) else {
        return Err(if state.terminal_summaries.contains_key(&run) {
            unavailable("history range is unavailable; terminal summary retained")
        } else {
            Error::new(ErrorKind::NotFound, "unknown run")
        });
    };
    let through = state
        .history
        .get(&run)
        .map_or(0, |events| events.len() as u64);
    if !matches!(run_state.status, RunStatus::Active)
        && run_state.terminal_ms != 0
        && now.as_millis()
            >= run_state.terminal_ms.saturating_add(
                run_state
                    .policy
                    .terminal_history_days
                    .saturating_mul(DAY_MS),
            )
    {
        return Err(unavailable(format!(
            "history range 1..={through} is unavailable"
        )));
    }
    state
        .history_dependencies
        .get(&run)
        .ok_or_else(|| unavailable("missing retained history dependencies"))?
        .verify(run_state)?;
    Ok((run_state, through))
}

fn validated_entries(
    state: &State,
    run: RunId,
    start: u64,
    through: u64,
) -> Result<Vec<RecordedEvent>> {
    let events = state
        .history
        .get(&run)
        .ok_or_else(|| unavailable("missing retained run events"))?;
    let records = state
        .history_records
        .get(&run)
        .ok_or_else(|| unavailable("missing retained event records"))?;
    if records.len() != events.len() || events.is_empty() {
        return Err(unavailable("incomplete retained event range"));
    }
    records
        .iter()
        .zip(events)
        .skip(start.saturating_sub(1) as usize)
        .take(through.saturating_sub(start).saturating_add(1) as usize)
        .enumerate()
        .map(|(index, (record, event))| {
            if record.format != EVENT_FORMAT
                || record.run != run
                || record.sequence != start + index as u64
            {
                return Err(unavailable(
                    "unsupported or noncontiguous run event sequence",
                ));
            }
            record.payload.verify("graphrun.domain-event/v1", event)?;
            Ok(RecordedEvent {
                record: record.clone(),
                event: event.clone(),
            })
        })
        .collect()
}

pub fn page(
    state: &State,
    run: RunId,
    after: u64,
    limit: u32,
    now: EngineTime,
) -> Result<HistoryPage> {
    let limit = if limit == 0 { PAGE_LIMIT } else { limit };
    if limit > MAX_PAGE_LIMIT {
        return Err(Error::invalid(format!(
            "history page size must be 0 (default) or 1..={MAX_PAGE_LIMIT}"
        )));
    }
    if let Some(summary) = state.terminal_summaries.get(&run) {
        if after > summary.history_through {
            return Err(Error::invalid("history cursor exceeds run sequence"));
        }
        return Ok(HistoryPage {
            run,
            retained_from: summary.history_through.saturating_add(1),
            retained_through: summary.history_through,
            next_cursor: None,
            unavailable: true,
            unavailable_range: Some(UnavailableRange {
                first: 1,
                last: summary.history_through,
            }),
            events: Vec::new(),
        });
    }
    let (_, through) = retained(state, run, now)?;
    if after > through {
        return Err(Error::invalid("history cursor exceeds retained sequence"));
    }
    let end = through.min(after.saturating_add(u64::from(limit)));
    let page = if after < end {
        validated_entries(state, run, after + 1, end)?
    } else {
        Vec::new()
    };
    let last = page.last().map_or(after, |entry| entry.record.sequence);
    Ok(HistoryPage {
        run,
        retained_from: 1,
        retained_through: through,
        next_cursor: (last < through).then_some(last),
        unavailable: false,
        unavailable_range: None,
        events: page,
    })
}

pub fn reconstruct_at(state: &State, run: RunId, through: u64, now: EngineTime) -> Result<State> {
    let (source, available) = retained(state, run, now)?;
    if through == 0 || through > available {
        return Err(Error::invalid(format!(
            "requested sequence {through} is outside retained range 1..={available}"
        )));
    }
    let entries = validated_entries(state, run, 1, through)?;
    let checkpoint = state.checkpoints.get(&run);
    if let Some(checkpoint) = checkpoint {
        if checkpoint.format != CHECKPOINT_FORMAT
            || checkpoint.through_run_sequence == 0
            || checkpoint.through_run_sequence > available
        {
            return Err(unavailable(
                "unsupported or out-of-range retained checkpoint",
            ));
        }
        checkpoint.required.verify(source)?;
        checkpoint.projection_ref.verify(
            "graphrun.run-projection/v2",
            &(checkpoint.through_run_sequence, &checkpoint.projection),
        )?;
        if !checkpoint.projection.runs.contains_key(&run) {
            return Err(unavailable("retained checkpoint has no projected run"));
        }
    }
    let checkpoint = checkpoint.filter(|checkpoint| checkpoint.through_run_sequence <= through);
    let (mut projection, first) = match checkpoint {
        Some(checkpoint) => (
            (*checkpoint.projection).clone(),
            checkpoint.through_run_sequence,
        ),
        None => {
            let first = &entries[0].event;
            let DomainEvent::RunAdmitted { .. } = first else {
                return Err(unavailable("missing run admission event"));
            };
            (
                domain::reconstruct(
                    source.definition.clone(),
                    source.catalog.clone(),
                    std::slice::from_ref(first),
                )
                .map_err(|err| unavailable(err.message))?,
                1,
            )
        }
    };
    for entry in entries.iter().skip(first as usize) {
        domain::evolve(&mut projection, &entry.event);
        if matches!(
            entry.event,
            DomainEvent::RunSucceeded { .. } | DomainEvent::RunFailed { .. }
        ) {
            projection
                .runs
                .get_mut(&run)
                .expect("admitted run")
                .terminal_ms = entry.record.recorded_ms;
        }
    }
    projection
        .runs
        .get_mut(&run)
        .expect("admitted run")
        .published = source.published.clone();
    projection.history.insert(
        run,
        entries.iter().map(|entry| entry.event.clone()).collect(),
    );
    projection.history_records.insert(
        run,
        entries.iter().map(|entry| entry.record.clone()).collect(),
    );
    Ok(projection)
}

#[doc(hidden)]
pub fn checkpoint_after_command(state: &mut State, run: RunId, time: EngineTime) -> Result<()> {
    let Some(run_state) = state.runs.get(&run) else {
        return Ok(());
    };
    let through = state
        .history
        .get(&run)
        .map_or(0, |events| events.len() as u64);
    if through == 0 {
        return Ok(());
    }
    let previous = state
        .checkpoints
        .get(&run)
        .map_or(0, |cp| cp.through_run_sequence);
    let terminal = !matches!(run_state.status, RunStatus::Active);
    if through.saturating_sub(previous) < run_state.policy.checkpoint_event_cadence && !terminal {
        return Ok(());
    }
    if previous == through {
        return Ok(());
    }
    let required = state
        .history_dependencies
        .get(&run)
        .ok_or_else(|| unavailable("missing retained history dependencies"))?
        .clone();
    let mut projection = reconstruct_at(state, run, through, time)?;
    projection.history.clear();
    projection.history_records.clear();
    projection.history_dependencies.clear();
    projection.checkpoints.clear();
    projection.terminal_summaries.clear();
    projection.signal_tombstones.clear();
    projection.commands.clear();
    projection.command_results.clear();
    projection.legacy_start_digests.clear();
    projection.worker_command_digests.clear();
    projection.command_times.clear();
    projection.published_catalogs.clear();
    projection.published_definitions.clear();
    projection.start_keys.clear();
    projection.sessions.clear();
    let projection_ref =
        ArtifactRef::capture("graphrun.run-projection/v2", &(through, &projection))?;
    state.checkpoints.insert(
        run,
        RunCheckpoint {
            format: CHECKPOINT_FORMAT.to_owned(),
            through_run_sequence: through,
            required,
            projection_ref,
            projection: Box::new(projection),
        },
    );
    Ok(())
}

pub(crate) fn prune(
    state: &mut State,
    command_id: CommandId,
    time: EngineTime,
    limit: usize,
) -> Result<Vec<DomainEvent>> {
    let now = time.as_millis();
    let mut remaining = limit;
    let mut expired_events = Vec::new();
    let old_results = earliest_due(
        state.command_results.iter().filter_map(|(key, receipt)| {
            let due = receipt.recorded_ms.saturating_add(24 * 60 * 60 * 1000);
            (now >= due).then(|| (due, key.clone()))
        }),
        remaining,
    );
    for key in old_results {
        state.command_results.remove(&key);
        remaining -= 1;
    }
    let old_commands = earliest_due(
        state.command_times.iter().filter_map(|(id, recorded)| {
            let due = recorded.saturating_add(24 * 60 * 60 * 1000);
            (now >= due).then_some((due, *id))
        }),
        remaining,
    );
    for id in old_commands {
        state.command_times.remove(&id);
        state.commands.remove(&id);
        state.legacy_start_digests.remove(&id);
        state.worker_command_digests.remove(&id);
        remaining -= 1;
    }
    let expired_inbox = earliest_due(
        state.inbox.iter().filter_map(|entry| {
            (entry.expires_ms != 0
                && now >= entry.expires_ms
                && (entry.consumed || entry.reserved_wait.is_none()))
            .then_some((entry.expires_ms, entry.event_id))
        }),
        remaining,
    );
    for id in expired_inbox {
        let entry = state
            .inbox
            .iter()
            .find(|entry| entry.event_id == id)
            .expect("selected event");
        let run = entry.run;
        let consumed = entry.consumed;
        if !consumed && run.is_none() {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "expiring input event has no run identity",
            ));
        }
        if let Some(run) = run.filter(|run| {
            !consumed
                && state
                    .runs
                    .get(run)
                    .is_some_and(|run_state| matches!(run_state.status, RunStatus::Active))
        }) {
            let event = DomainEvent::EventExpired { run, event_id: id };
            domain::apply_events_with_cause(
                state,
                std::slice::from_ref(&event),
                Some(command_id),
                None,
                time,
            )?;
            checkpoint_after_command(state, run, time)?;
            expired_events.push(event);
        } else {
            state.inbox.retain(|entry| entry.event_id != id);
        }
        remaining -= 1;
    }
    let expired_runs = earliest_due(
        state.runs.values().filter_map(|run| {
            let due = run
                .terminal_ms
                .saturating_add(run.policy.terminal_history_days.saturating_mul(DAY_MS));
            (!matches!(run.status, RunStatus::Active) && run.terminal_ms != 0 && now >= due)
                .then_some((due, run.id))
        }),
        remaining,
    );
    for id in expired_runs {
        let run = state.runs.remove(&id).expect("selected run");
        state.terminal_summaries.insert(
            id,
            TerminalSummary {
                run: id,
                workflow: run.definition.id,
                version: run.definition.version,
                status: match run.status {
                    RunStatus::Succeeded { .. } => "succeeded",
                    RunStatus::Failed { .. } => "failed",
                    RunStatus::Active => unreachable!(),
                }
                .to_owned(),
                terminal_ms: run.terminal_ms,
                expires_ms: run
                    .terminal_ms
                    .saturating_add(run.policy.terminal_summary_days.saturating_mul(DAY_MS)),
                history_through: state
                    .history
                    .get(&id)
                    .map_or(0, |events| events.len() as u64),
            },
        );
        state.history.remove(&id);
        state.history_records.remove(&id);
        state.history_dependencies.remove(&id);
        state.checkpoints.remove(&id);
        state.scopes.retain(|_, scope| scope.run != id);
        let removed: Vec<_> = state
            .activations
            .values()
            .filter(|act| act.run == id)
            .map(|act| act.id)
            .collect();
        state.activations.retain(|_, act| act.run != id);
        for activation in removed {
            state.loop_carry.remove(&activation);
            state.foreach_items.remove(&activation);
            state.foreach_done.remove(&activation);
            state.parallel_done.remove(&activation);
            state.saga_errors.remove(&activation);
            state.interventions.remove(&activation);
        }
        state.waits.retain(|_, wait| wait.run != id);
        state.inbox.retain(|entry| entry.run != Some(id));
        state.obligations.retain(|item| item.run != id);
        remaining -= 1;
    }
    let old_summaries = earliest_due(
        state
            .terminal_summaries
            .values()
            .filter(|summary| now >= summary.expires_ms)
            .map(|summary| (summary.expires_ms, summary.run)),
        remaining,
    );
    for id in old_summaries {
        state.terminal_summaries.remove(&id);
        state.signal_tombstones.retain(|_, signal| signal.run != id);
        for keys in state.start_keys.values_mut() {
            keys.retain(|_, key| key.run != id);
        }
    }
    Ok(expired_events)
}

fn earliest_due<K: Ord>(candidates: impl Iterator<Item = (u64, K)>, limit: usize) -> Vec<K> {
    if limit == 0 {
        return Vec::new();
    }
    let mut selected = BinaryHeap::new();
    for candidate in candidates {
        if selected.len() < limit {
            selected.push(candidate);
        } else if selected.peek().is_some_and(|latest| candidate < *latest) {
            selected.pop();
            selected.push(candidate);
        }
    }
    selected
        .into_sorted_vec()
        .into_iter()
        .map(|(_, key)| key)
        .collect()
}

pub fn summary_view(summary: &TerminalSummary) -> serde_json::Value {
    serde_json::json!({
        "run": summary.run.to_hex(),
        "definition": summary.workflow,
        "version": summary.version,
        "status": summary.status,
        "history_unavailable": true,
        "history_through": summary.history_through,
    })
}

pub fn replay_view(
    state: &State,
    run: RunId,
    through: u64,
    now: EngineTime,
) -> Result<serde_json::Value> {
    let projection = reconstruct_at(state, run, through, now)?;
    Ok(projection_view(&projection, run))
}

pub fn projection_view(projection: &State, run: RunId) -> serde_json::Value {
    crate::write::inspect_view(projection, run)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Command, CommandBody};
    use crate::publication::{
        CommandKey, CommandResult, Disposition, RESULT_FORMAT, StartKeyRecord,
    };

    fn id(index: u128) -> CommandId {
        CommandId::from_bytes(index.to_be_bytes())
    }

    fn state_with_worker_commands(reverse: bool, now: u64) -> State {
        let mut state = State::default();
        let mut indices: Vec<_> = (1..=11).collect();
        if reverse {
            indices.reverse();
        }
        for index in indices {
            let command_id = id(index);
            let recorded_ms = match index {
                10 => now - DAY_MS,
                11 => now - DAY_MS + 1,
                _ => (index as u64 - 1) / 2,
            };
            state.command_times.insert(command_id, recorded_ms);
            state.commands.insert(command_id, Vec::new());
            state
                .worker_command_digests
                .insert(command_id, format!("digest-{index}"));
        }
        state.legacy_start_digests.insert(id(1), "legacy".into());

        let key = CommandKey {
            cluster_id: "cluster".into(),
            principal_id: "owner".into(),
            command_id: id(1),
        };
        state.command_results.insert(
            key.storage_key(),
            CommandResult {
                format: RESULT_FORMAT.into(),
                key,
                request_digest: "receipt".into(),
                operation: "start".into(),
                target: "workflow".into(),
                outcome: Disposition::Applied {
                    run: None,
                    digest: None,
                    version: None,
                },
                event_range: None,
                recorded_ms: now - 1,
            },
        );
        state
            .start_keys
            .entry("cluster".into())
            .or_default()
            .insert(
                "start".into(),
                StartKeyRecord {
                    run: RunId::from_bytes([7; 16]),
                    version: 1,
                    input_digest: "start-digest".into(),
                    admitted_ms: 0,
                },
            );
        state
    }

    #[test]
    fn worker_digests_expire_with_commands_in_deterministic_batches() {
        let now = DAY_MS + 1_000;
        let mut forward = state_with_worker_commands(false, now);
        let mut reverse = state_with_worker_commands(true, now);

        for batch in 0..4u128 {
            let prune_command = Command {
                id: id(100 + batch),
                time: EngineTime::from_millis(now),
                body: CommandBody::PruneHistory { limit: 3 },
            };
            assert!(
                domain::commit_command(&mut forward, prune_command.clone())
                    .unwrap()
                    .is_empty()
            );
            assert!(
                domain::commit_command(&mut reverse, prune_command)
                    .unwrap()
                    .is_empty()
            );

            for state in [&forward, &reverse] {
                for index in 1..=10 {
                    let retained = index > ((batch + 1) * 3).min(10);
                    assert_eq!(state.command_times.contains_key(&id(index)), retained);
                    assert_eq!(state.commands.contains_key(&id(index)), retained);
                    assert_eq!(
                        state.worker_command_digests.contains_key(&id(index)),
                        retained
                    );
                }
                assert!(!state.legacy_start_digests.contains_key(&id(1)));
                assert!(state.command_times.contains_key(&id(11)));
                assert!(state.commands.contains_key(&id(11)));
                assert_eq!(
                    state.worker_command_digests.get(&id(11)).unwrap(),
                    "digest-11"
                );
                assert_eq!(state.command_results.len(), 1);
                assert_eq!(
                    state.command_results.values().next().unwrap().recorded_ms,
                    now - 1
                );
                assert_eq!(
                    state.start_keys["cluster"]["start"].input_digest,
                    "start-digest"
                );
            }
        }
    }
}
