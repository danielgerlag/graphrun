use crate::domain::{Command, ObligationStatus, State, run_events};
use crate::error::{Error, ErrorKind, Result};
use crate::ids::RunId;
use crate::storage::StorageHandle;
use crate::storage::{RaftRequest, RaftResponse, TypeConfig};
use crate::time::EngineTime;
use openraft::{Raft, ServerState};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn now() -> EngineTime {
    EngineTime::from_millis(wall_ms())
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) async fn write_raft(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    command: Command,
) -> Result<()> {
    let resp = write_raft_response(raft, storage, command).await?;
    if let Some(err) = resp.error {
        return Err(Error::invalid(err));
    }
    Ok(())
}

pub(crate) async fn write_raft_response(
    raft: &Raft<TypeConfig>,
    storage: &StorageHandle,
    command: Command,
) -> Result<RaftResponse> {
    let metrics = raft.metrics().borrow().clone();
    if metrics.state != ServerState::Leader {
        return Err(Error::new(
            ErrorKind::Unavailable,
            format!(
                "not leader (leader={:?}); outcome unknown for command {}",
                metrics.current_leader, command.id
            ),
        ));
    }
    storage.clock().authorize(storage, raft).await?;
    let metrics = raft.metrics().borrow().clone();
    let last_log = metrics.last_log_index.unwrap_or(0);
    let applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);
    let pending = last_log.saturating_sub(applied);
    let encoded = serde_json::to_vec(&command)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0);
    admit_unapplied(pending, pending.saturating_add(1).saturating_mul(encoded))?;
    let identity = match &command.body {
        crate::domain::CommandBody::Publication { key, .. } => format!(
            "cluster={} principal={} command={}",
            key.cluster_id, key.principal_id, key.command_id
        ),
        _ => format!("command={}", command.id),
    };
    let resp = raft
        .client_write(RaftRequest { command })
        .await
        .map_err(|err| {
            Error::new(
                ErrorKind::Unavailable,
                format!("unknown outcome ({identity}); retry or query the same command ID: {err}"),
            )
        })?;
    Ok(resp.data)
}

pub(crate) async fn linearizable_read(raft: &Raft<TypeConfig>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), raft.ensure_linearizable())
        .await
        .map_err(|_| {
            Error::new(
                ErrorKind::DeadlineExceeded,
                "read barrier deadline exceeded",
            )
        })?
        .map_err(|err| {
            Error::new(
                ErrorKind::Unavailable,
                format!("leader/quorum unavailable: {err}"),
            )
        })?;
    Ok(())
}

pub(crate) fn inspect_view(state: &State, run: RunId) -> serde_json::Value {
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
                    ObligationStatus::Blocked { .. } => "blocked",
                    ObligationStatus::Irreversible { .. } => "irreversible",
                    ObligationStatus::Abandoned => "abandoned",
                },
            })
        })
        .collect();
    serde_json::json!({
        "run": run.to_hex(),
        "definition": run_state.definition.id,
        "version": run_state.definition.version,
        "published": run_state.published,
        "status": match &run_state.status {
            crate::domain::RunStatus::Active => "active",
            crate::domain::RunStatus::Succeeded { .. } => "succeeded",
            crate::domain::RunStatus::Failed { .. } => "failed",
        },
        "output": match &run_state.status {
            crate::domain::RunStatus::Succeeded { output } => Some(output.clone()),
            _ => None,
        },
        "error": match &run_state.status {
            crate::domain::RunStatus::Failed { error } => Some(serde_json::json!({
                "code": error.code,
                "message": error.message
            })),
            _ => None,
        },
        "pending_waits": waits,
        "obligations": obligations,
        "event_count": run_events(state, run).len(),
        "recovery": state.recovery.as_ref().map(|hold| {
            serde_json::json!({
                "reason": hold.reason,
                "authorized": hold.authorized,
                "suspended": !hold.authorized,
            })
        }),
        "blocked_reason": stall_reason(state, run),
        "ready_leaves": state
            .activations
            .values()
            .filter(|act| act.run == run && act.status == crate::domain::ActivationStatus::Ready)
            .count(),
        "open_scopes": state
            .scopes
            .values()
            .filter(|scope| {
                scope.run == run && matches!(scope.status, crate::domain::ScopeStatus::Open)
            })
            .count(),
        "inbox_depth": state
            .inbox
            .iter()
            .filter(|entry| entry.run == Some(run) && !entry.consumed)
            .count(),
        "interventions": state
            .interventions
            .iter()
            .filter(|(id, _)| state.activations.get(id).is_some_and(|act| act.run == run))
            .map(|(id, reason)| {
                serde_json::json!({"activation": id.to_hex(), "reason": reason})
            })
            .collect::<Vec<_>>(),
    })
}

fn stall_reason(state: &State, run: RunId) -> Option<String> {
    let run_state = state.runs.get(&run)?;
    if !matches!(run_state.status, crate::domain::RunStatus::Active) {
        return None;
    }
    if state.recovery.as_ref().is_some_and(|hold| !hold.authorized) {
        return Some("execution suspended until recovery is acknowledged".to_owned());
    }
    if let Some((_, reason)) = state
        .interventions
        .iter()
        .find(|(id, _)| state.activations.get(id).is_some_and(|act| act.run == run))
    {
        return Some(format!("intervention required: {reason}"));
    }
    if let Some(wait) = state
        .waits
        .values()
        .find(|wait| wait.run == run && wait.pending)
    {
        return Some(format!(
            "waiting for signal {} key {}",
            wait.signal, wait.key
        ));
    }
    if let Some(item) = state.obligations.iter().find(|item| item.run == run) {
        match &item.status {
            ObligationStatus::Blocked { reason } => {
                return Some(format!("compensation input blocked: {reason}"));
            }
            ObligationStatus::Irreversible { reason } => {
                return Some(format!("irreversible effect: {reason}"));
            }
            ObligationStatus::Compensating { .. } => {
                return Some(format!("compensation in flight: {}", item.handler));
            }
            _ => {}
        }
    }
    if state.activations.values().any(|act| {
        act.run == run
            && act.status == crate::domain::ActivationStatus::Ready
            && act.claim.is_some()
    }) {
        return Some("activity claimed; waiting for worker result".to_owned());
    }
    if state
        .activations
        .values()
        .any(|act| act.run == run && act.status == crate::domain::ActivationStatus::Ready)
    {
        return Some("ready work waiting for a worker claim".to_owned());
    }
    Some("active with no pending wait, obligation, or ready leaf".to_owned())
}

pub(crate) fn health_view(
    raft_state: String,
    last_applied: Option<u64>,
    last_log: Option<u64>,
    voters: Vec<u64>,
    state: &State,
    clock_safe: bool,
    clock_fault: Option<String>,
    quorum_safe: bool,
) -> serde_json::Value {
    let applied = last_applied.unwrap_or(0);
    let log = last_log.unwrap_or(0);
    serde_json::json!({
        "status": if clock_safe && quorum_safe { "ok" } else { "unavailable" },
        "clock_safe": clock_safe,
        "clock_fault": clock_fault,
        "quorum_safe": quorum_safe,
        "engine_time_watermark_ms": state.engine_time_watermark_ms,
        "state": raft_state,
        "last_applied": last_applied,
        "last_log": last_log,
        "apply_lag": log.saturating_sub(applied),
        "unapplied_entries": log.saturating_sub(applied),
        "voters": voters,
        "active_runs": state.runs.values().filter(|run| matches!(run.status, crate::domain::RunStatus::Active)).count(),
        "ready_leaves": state.activations.values().filter(|act| act.status == crate::domain::ActivationStatus::Ready).count(),
        "inbox_depth": state.inbox.iter().filter(|entry| !entry.consumed).count(),
        "open_scopes": state.scopes.values().filter(|scope| matches!(scope.status, crate::domain::ScopeStatus::Open)).count(),
        "recovery": state.recovery.as_ref().map(|hold| {
            serde_json::json!({
                "reason": hold.reason,
                "authorized": hold.authorized,
                "suspended": !hold.authorized,
            })
        }),
    })
}

pub(crate) fn admit_unapplied(unapplied_entries: u64, unapplied_bytes: u64) -> Result<()> {
    if unapplied_entries >= crate::limits::UNAPPLIED_ENTRIES as u64 {
        return Err(Error::new(
            ErrorKind::ResourceExhausted,
            "unapplied entry credit exhausted",
        ));
    }
    if unapplied_bytes >= crate::limits::UNAPPLIED_BYTES {
        return Err(Error::new(
            ErrorKind::ResourceExhausted,
            "unapplied byte credit exhausted",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unapplied_credits_reject_at_limit() {
        assert!(admit_unapplied(0, 0).is_ok());
        assert!(
            admit_unapplied(
                u64::from(crate::limits::UNAPPLIED_ENTRIES) - 1,
                crate::limits::UNAPPLIED_BYTES - 1
            )
            .is_ok()
        );
        let entries = admit_unapplied(u64::from(crate::limits::UNAPPLIED_ENTRIES), 0).unwrap_err();
        assert_eq!(entries.kind, ErrorKind::ResourceExhausted);
        assert!(entries.to_string().contains("unapplied entry credit"));
        let bytes = admit_unapplied(0, crate::limits::UNAPPLIED_BYTES).unwrap_err();
        assert_eq!(bytes.kind, ErrorKind::ResourceExhausted);
        assert!(bytes.to_string().contains("unapplied byte credit"));
    }
}
