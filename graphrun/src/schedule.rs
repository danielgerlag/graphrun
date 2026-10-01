use crate::domain::{ActivationStatus, RunStatus, State};
use crate::ids::RunId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunSchedule {
    pub format: u16,
    pub generation: u64,
    pub progress: bool,
    pub ready: bool,
    pub deadline_ms: Option<u64>,
    pub source_revision: u64,
}

impl RunSchedule {
    pub fn from_state(
        state: &State,
        run: RunId,
        progress: bool,
        now_ms: u64,
        generation: u64,
        source_revision: u64,
    ) -> Option<Self> {
        let current = state.runs.get(&run)?;
        if !matches!(current.status, RunStatus::Active) {
            return None;
        }
        let mut deadline_ms = state
            .waits
            .values()
            .filter(|wait| wait.run == run && wait.pending)
            .filter_map(|wait| wait.deadline_ms)
            .min();
        let mut ready = false;
        for activation in state.activations.values().filter(|act| act.run == run) {
            if activation.status != ActivationStatus::Ready
                || state.interventions.contains_key(&activation.id)
                || state
                    .waits
                    .values()
                    .any(|wait| wait.activation == activation.id && wait.pending)
            {
                continue;
            }
            match &activation.claim {
                Some(claim) if claim.lease_expiry_ms > now_ms => {
                    let due = claim.lease_expiry_ms;
                    deadline_ms = Some(deadline_ms.map_or(due, |old| old.min(due)));
                }
                _ => ready = true,
            }
        }
        Some(Self {
            format: 1,
            generation,
            progress,
            ready,
            deadline_ms,
            source_revision,
        })
    }

    pub fn changed_from(&self, previous: &Self) -> bool {
        self.generation != previous.generation
            || self.progress != previous.progress
            || self.ready != previous.ready
            || self.deadline_ms != previous.deadline_ms
    }
}

pub(crate) fn retention_deadline(state: &State) -> Option<u64> {
    const DAY_MS: u64 = 24 * 60 * 60 * 1_000;
    let receipts = state
        .command_results
        .values()
        .map(|result| result.recorded_ms.saturating_add(DAY_MS));
    let commands = state
        .command_times
        .values()
        .map(|recorded| recorded.saturating_add(DAY_MS));
    let inbox = state
        .inbox
        .iter()
        .filter(|entry| entry.expires_ms != 0 && (entry.consumed || entry.reserved_wait.is_none()))
        .map(|entry| entry.expires_ms);
    let runs = state
        .runs
        .values()
        .filter(|run| !matches!(run.status, RunStatus::Active) && run.terminal_ms != 0)
        .map(|run| {
            run.terminal_ms
                .saturating_add(run.policy.terminal_history_days.saturating_mul(DAY_MS))
        });
    let summaries = state
        .terminal_summaries
        .values()
        .map(|summary| summary.expires_ms);
    receipts
        .chain(commands)
        .chain(inbox)
        .chain(runs)
        .chain(summaries)
        .min()
}
