use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const FORWARD_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const COMPENSATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const RECONCILIATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
pub const WORKER_CANCEL_GRACE: Duration = Duration::from_secs(5);
pub const WORKER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub const FORWARD_ATTEMPT_BUDGET: u32 = 5;
pub const COMPENSATION_ATTEMPT_BUDGET: u32 = 10;
pub const MAX_ATTEMPT_BUDGET: u32 = 10;
pub const OPERATOR_EXTENSION: u32 = 10;
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
pub const FORWARD_BACKOFF_CAP: Duration = Duration::from_secs(60);
pub const COMPENSATION_BACKOFF_CAP: Duration = Duration::from_secs(5 * 60);
pub const BACKOFF_MULTIPLIER_MILLIS: u32 = 2_000;
pub const RECONCILIATION_PROBES: u32 = 3;
pub const RECONCILIATION_GAP: Duration = Duration::from_secs(30);
pub const TERMINAL_HISTORY_DAYS: u64 = 30;
pub const TERMINAL_SUMMARY_DAYS: u64 = 90;
pub const COMMAND_RESULT_HOURS: u64 = 24;
pub const UNRESERVED_EVENT_DAYS: u64 = 7;
pub const CHECKPOINT_EVENT_CADENCE: u64 = 256;
pub const SESSION_LEASE: Duration = Duration::from_secs(30);
pub const RENEWAL_INTERVAL: Duration = Duration::from_secs(5);
pub const STOP_DISPATCH_MARGIN: Duration = Duration::from_secs(5);
pub const RENEWAL_RPC_TIMEOUT: Duration = Duration::from_secs(2);
pub const RPC_DEADLINE: Duration = Duration::from_secs(5);
pub const MIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
pub const MAX_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const MAX_RUN_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Backoff {
    pub initial_ms: u64,
    pub multiplier_millis: u32,
    pub max_ms: u64,
}

impl Backoff {
    pub fn forward_default() -> Self {
        Self {
            initial_ms: INITIAL_BACKOFF.as_millis() as u64,
            multiplier_millis: BACKOFF_MULTIPLIER_MILLIS,
            max_ms: FORWARD_BACKOFF_CAP.as_millis() as u64,
        }
    }

    pub fn compensation_default() -> Self {
        Self {
            initial_ms: INITIAL_BACKOFF.as_millis() as u64,
            multiplier_millis: BACKOFF_MULTIPLIER_MILLIS,
            max_ms: COMPENSATION_BACKOFF_CAP.as_millis() as u64,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.initial_ms == 0 || self.max_ms == 0 {
            return Err(Error::invalid("backoff durations must be positive"));
        }
        if self.initial_ms > self.max_ms {
            return Err(Error::invalid("backoff initial exceeds max"));
        }
        if self.max_ms > Duration::from_secs(60 * 60).as_millis() as u64 {
            return Err(Error::invalid("backoff max exceeds one hour"));
        }
        if !(1_000..=10_000).contains(&self.multiplier_millis) {
            return Err(Error::invalid(
                "backoff multiplier must be between 1 and 10",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub errors: Vec<String>,
    pub max_attempts: u32,
    pub backoff: Backoff,
}

impl RetryPolicy {
    pub fn forward_default() -> Self {
        Self {
            errors: Vec::new(),
            max_attempts: FORWARD_ATTEMPT_BUDGET,
            backoff: Backoff::forward_default(),
        }
    }

    pub fn compensation_default(retryable_codes: Vec<String>) -> Self {
        Self {
            errors: retryable_codes,
            max_attempts: COMPENSATION_ATTEMPT_BUDGET,
            backoff: Backoff::compensation_default(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_attempts == 0 || self.max_attempts > MAX_ATTEMPT_BUDGET {
            return Err(Error::invalid("max_attempts is out of range"));
        }
        self.backoff.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedRunPolicy {
    pub forward_attempt_timeout_ms: u64,
    pub compensation_attempt_timeout_ms: u64,
    pub reconciliation_attempt_timeout_ms: u64,
    pub run_timeout_ms: Option<u64>,
    pub terminal_history_days: u64,
    pub terminal_summary_days: u64,
    pub command_result_hours: u64,
    pub unreserved_event_days: u64,
    pub checkpoint_event_cadence: u64,
}

impl CapturedRunPolicy {
    pub fn defaults(run_timeout_ms: Option<u64>) -> Self {
        Self {
            forward_attempt_timeout_ms: FORWARD_ATTEMPT_TIMEOUT.as_millis() as u64,
            compensation_attempt_timeout_ms: COMPENSATION_ATTEMPT_TIMEOUT.as_millis() as u64,
            reconciliation_attempt_timeout_ms: RECONCILIATION_ATTEMPT_TIMEOUT.as_millis() as u64,
            run_timeout_ms,
            terminal_history_days: TERMINAL_HISTORY_DAYS,
            terminal_summary_days: TERMINAL_SUMMARY_DAYS,
            command_result_hours: COMMAND_RESULT_HOURS,
            unreserved_event_days: UNRESERVED_EVENT_DAYS,
            checkpoint_event_cadence: CHECKPOINT_EVENT_CADENCE,
        }
    }
}

pub fn validate_attempt_timeout(timeout: Duration) -> Result<()> {
    if timeout < MIN_ATTEMPT_TIMEOUT || timeout > MAX_ATTEMPT_TIMEOUT {
        return Err(Error::invalid(
            "attempt timeout must be between 1 second and 24 hours",
        ));
    }
    Ok(())
}
