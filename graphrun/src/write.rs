use crate::domain::{Command, ObligationStatus, State, run_events};
use crate::error::{Error, Result};
use crate::ids::RunId;
use crate::storage::{RaftRequest, TypeConfig};
use crate::time::EngineTime;
use openraft::Raft;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now() -> EngineTime {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    EngineTime::from_millis(ms)
}

pub(crate) async fn write_raft(raft: &Raft<TypeConfig>, command: Command) -> Result<()> {
    let resp = raft
        .client_write(RaftRequest { command })
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    if let Some(err) = resp.data.error {
        return Err(Error::invalid(err));
    }
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
                },
            })
        })
        .collect();
    serde_json::json!({
        "run": run.to_hex(),
        "definition": run_state.definition.id,
        "version": run_state.definition.version,
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
    })
}
