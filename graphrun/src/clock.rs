use crate::cluster::ClusterNetwork;
use crate::error::{Error, ErrorKind, Result};
use crate::storage::{StorageHandle, TypeConfig};
use crate::time::{ClockDeltaFault, boot_millis, clock_delta_fault, wall_millis};
use openraft::{LogId, Raft};
use std::collections::BTreeSet;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex as AsyncMutex, watch};

const QUORUM_REFRESH_MS: u64 = 5_000;
const QUORUM_EXPIRY_MS: u64 = 10_000;

#[derive(Default)]
struct Monitor {
    previous: Option<(u64, u64)>,
    faulted: bool,
    fault_recorded: bool,
    healthy_since: Option<u64>,
    fault_reason: Option<String>,
}

impl Monitor {
    fn observe(&mut self, wall: u64, boot: u64, watermark: u64) -> bool {
        let mut fault = (wall.saturating_add(2_000) < watermark).then(|| {
            format!(
                "persisted watermark {watermark} ms is ahead of wall {wall} ms by more than 2 s"
            )
        });
        if let Some((previous_wall, previous_boot)) = self.previous {
            if let Some(delta) = clock_delta_fault(previous_wall, previous_boot, wall, boot) {
                fault = Some(match delta {
                    ClockDeltaFault::BootReversed => "boot clock reversed".to_owned(),
                    ClockDeltaFault::WatchdogGap(elapsed) => {
                        format!("watchdog gap {elapsed} ms exceeds 2000 ms")
                    }
                    ClockDeltaFault::WallBootSkew(discrepancy, elapsed) => {
                        format!("wall/boot delta differs by {discrepancy} ms over {elapsed} ms")
                    }
                });
            }
        }
        self.previous = Some((wall, boot));
        if let Some(reason) = fault.as_ref() {
            self.faulted = true;
            self.healthy_since = None;
            self.fault_reason = Some(reason.clone());
        } else if self.healthy_since.is_none() {
            self.healthy_since = Some(boot);
        }
        fault.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ClockFaultState {
    Healthy,
    Latched { reason: String },
}

#[derive(Clone)]
pub(crate) struct FreshVoterQuorum {
    pub membership_log_id: Option<LogId<u64>>,
    pub sampled_voters: BTreeSet<u64>,
    pub checked_boot_ms: u64,
    voter_configs: Vec<BTreeSet<u64>>,
}

pub struct ClockAuthority {
    monitor: AsyncMutex<Monitor>,
    network: Mutex<Option<ClusterNetwork>>,
    last_quorum: AsyncMutex<Option<FreshVoterQuorum>>,
    fault_signal: watch::Sender<ClockFaultState>,
    #[cfg(test)]
    test_watermark: AtomicU64,
}

impl ClockAuthority {
    pub fn new(faulted: bool) -> Self {
        let reason = "clock fault persisted from previous process".to_owned();
        let (fault_signal, _) = watch::channel(if faulted {
            ClockFaultState::Latched {
                reason: reason.clone(),
            }
        } else {
            ClockFaultState::Healthy
        });
        Self {
            monitor: AsyncMutex::new(Monitor {
                faulted,
                fault_recorded: faulted,
                fault_reason: faulted.then_some(reason),
                ..Monitor::default()
            }),
            network: Mutex::new(None),
            last_quorum: AsyncMutex::new(None),
            fault_signal,
            #[cfg(test)]
            test_watermark: AtomicU64::new(0),
        }
    }

    pub fn configure_network(&self, network: ClusterNetwork) {
        *self.network.lock().unwrap() = Some(network);
    }

    pub(crate) fn subscribe_fault(&self) -> watch::Receiver<ClockFaultState> {
        self.fault_signal.subscribe()
    }

    pub async fn sample(&self, storage: &StorageHandle) -> Result<(u64, u64)> {
        let mut monitor = self.monitor.lock().await;
        let (wall, boot) = match (wall_millis(), boot_millis()) {
            (Ok(wall), Ok(boot)) => (wall, boot),
            (Err(error), _) | (_, Err(error)) => {
                let newly_faulted = !monitor.faulted;
                monitor.faulted = true;
                monitor.healthy_since = None;
                monitor.fault_reason = Some(error.to_string());
                if newly_faulted {
                    self.fault_signal.send_replace(ClockFaultState::Latched {
                        reason: error.to_string(),
                    });
                    tracing::error!(%error, "clock source failed; scheduling and effects disabled");
                }
                if !monitor.fault_recorded {
                    storage.persist_clock_fault(true).await.inspect_err(|persist| {
                        tracing::error!(%persist, "clock fault could not be persisted; member remains fenced");
                    })?;
                    monitor.fault_recorded = true;
                }
                return Err(error);
            }
        };
        let watermark = storage.engine_watermark_cached();
        #[cfg(test)]
        let watermark = watermark.max(self.test_watermark.load(Ordering::SeqCst));
        let was_faulted = monitor.faulted;
        monitor.observe(wall, boot, watermark);
        if monitor.faulted && !was_faulted {
            self.fault_signal.send_replace(ClockFaultState::Latched {
                reason: monitor.fault_reason.clone().expect("fault has a reason"),
            });
            tracing::error!(
                reason = monitor.fault_reason.as_deref().unwrap_or("unknown"),
                "clock fault latched; scheduling and effects disabled"
            );
        }
        if monitor.faulted && !monitor.fault_recorded {
            storage.persist_clock_fault(true).await.inspect_err(|error| {
                tracing::error!(%error, "clock fault could not be persisted; member remains fenced");
            })?;
            monitor.fault_recorded = true;
        }
        if monitor.faulted {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                format!(
                    "clock unsafe ({}); explicit acknowledgement after ten healthy seconds required",
                    monitor.fault_reason.as_deref().unwrap_or("unknown")
                ),
            ));
        }
        Ok((wall, boot))
    }

    pub async fn acknowledge(&self, storage: &StorageHandle, reason: &str) -> Result<()> {
        if reason.trim().is_empty() {
            return Err(Error::invalid("clock acknowledgement requires a reason"));
        }
        let mut monitor = self.monitor.lock().await;
        if !monitor.faulted {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "no clock fault to acknowledge",
            ));
        }
        if !monitor.fault_recorded {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "clock fault must be persisted before acknowledgement",
            ));
        }
        let wall = wall_millis()?;
        let boot = boot_millis()?;
        let watermark = storage.engine_watermark().await?;
        if monitor.observe(wall, boot, watermark)
            || boot.saturating_sub(monitor.healthy_since.unwrap_or(boot)) < 10_000
            || wall.saturating_sub(1_000) < watermark
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "clock must be healthy for ten seconds and catch up to the committed watermark",
            ));
        }
        storage.persist_clock_fault(false).await?;
        monitor.faulted = false;
        monitor.fault_recorded = false;
        monitor.fault_reason = None;
        *self.last_quorum.lock().await = None;
        self.fault_signal.send_replace(ClockFaultState::Healthy);
        Ok(())
    }

    pub async fn authorize(&self, storage: &StorageHandle, raft: &Raft<TypeConfig>) -> Result<()> {
        self.sample_quorum(storage, raft, true).await.map(|_| ())
    }

    pub(crate) async fn fresh_voter_quorum(
        &self,
        storage: &StorageHandle,
        raft: &Raft<TypeConfig>,
    ) -> Result<FreshVoterQuorum> {
        self.sample_quorum(storage, raft, false).await
    }

    async fn sample_quorum(
        &self,
        storage: &StorageHandle,
        raft: &Raft<TypeConfig>,
        allow_cached: bool,
    ) -> Result<FreshVoterQuorum> {
        let (_, before_boot) = self.sample(storage).await?;
        let metrics = raft.metrics().borrow().clone();
        let voter_configs = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .clone();
        let voters: BTreeSet<u64> = metrics.membership_config.voter_ids().collect();
        let membership_log_id = *metrics.membership_config.log_id();
        if voter_configs.is_empty() || voters.is_empty() {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "committed voting membership unavailable",
            ));
        }
        if voters.len() == 1 {
            let sampled_voters = BTreeSet::from([metrics.id]);
            if !joint_quorum(&voter_configs, &sampled_voters) {
                return Err(Error::new(
                    ErrorKind::Unavailable,
                    "local member is not the voting quorum",
                ));
            }
            return Ok(FreshVoterQuorum {
                checked_boot_ms: before_boot,
                membership_log_id,
                sampled_voters,
                voter_configs,
            });
        }
        let mut last = self.last_quorum.lock().await;
        if allow_cached
            && last.as_ref().is_some_and(|sample| {
                sample.membership_log_id == membership_log_id
                    && sample.voter_configs == voter_configs
                    && before_boot.saturating_sub(sample.checked_boot_ms) < QUORUM_REFRESH_MS
            })
        {
            return Ok(last.as_ref().expect("checked cached quorum").clone());
        }
        let mut roster = storage.applied_membership().await?;
        if roster.membership().get_joint_config().is_empty() {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while roster.membership().get_joint_config().is_empty()
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                roster = storage.applied_membership().await?;
            }
        }
        if roster != *metrics.membership_config {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "applied voting membership differs from Raft membership",
            ));
        }
        let local_id = metrics.id;
        if !voters.contains(&local_id) {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "local member is not a committed voter",
            ));
        }
        let network = if voters.len() > 1 {
            Some(self.network.lock().unwrap().clone().ok_or_else(|| {
                Error::new(ErrorKind::Unavailable, "cluster clock network unavailable")
            })?)
        } else {
            None
        };
        let mut healthy = BTreeSet::from([local_id]);
        let mut failures = Vec::new();
        for id in voters.iter().copied().filter(|id| *id != local_id) {
            let Some(endpoint) = roster.membership().get_node(&id) else {
                failures.push(format!("member {id}: not in committed roster"));
                continue;
            };
            match network
                .as_ref()
                .expect("multi-voter clock network")
                .probe_clock(id, &endpoint.addr)
                .await
            {
                Ok(()) => {
                    healthy.insert(id);
                    if joint_quorum(&voter_configs, &healthy) {
                        break;
                    }
                }
                Err(err) => failures.push(format!("member {id}: {err}")),
            }
        }
        let (_, after_boot) = self.sample(storage).await?;
        if raft.metrics().borrow().membership_config.as_ref() != &roster {
            return Err(Error::new(
                ErrorKind::Unavailable,
                "voting membership changed during clock probes",
            ));
        }
        if joint_quorum(&voter_configs, &healthy) {
            let sample = FreshVoterQuorum {
                checked_boot_ms: after_boot,
                membership_log_id,
                sampled_voters: healthy,
                voter_configs,
            };
            *last = Some(sample.clone());
            return Ok(sample);
        }
        if allow_cached
            && last.as_ref().is_some_and(|sample| {
                sample.membership_log_id == membership_log_id
                    && sample.voter_configs == voter_configs
                    && after_boot.saturating_sub(sample.checked_boot_ms) < QUORUM_EXPIRY_MS
            })
        {
            return Ok(last.as_ref().expect("checked cached quorum").clone());
        }
        Err(Error::new(
            ErrorKind::Unavailable,
            format!(
                "clock-health voting quorum unavailable ({})",
                failures.join("; ")
            ),
        ))
    }

    pub async fn fault_reason(&self) -> Option<String> {
        self.monitor.lock().await.fault_reason.clone()
    }

    #[cfg(test)]
    pub fn inject_watermark(&self, watermark: u64) {
        self.test_watermark.store(watermark, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn clear_injected_watermark(&self) {
        self.test_watermark.store(0, Ordering::SeqCst);
    }
}

fn joint_quorum(configs: &[BTreeSet<u64>], healthy: &BTreeSet<u64>) -> bool {
    !configs.is_empty()
        && configs.iter().all(|voters| {
            !voters.is_empty() && voters.intersection(healthy).count() > voters.len() / 2
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_and_suspend_latch_fault_without_lowering_watermark() {
        let mut clock = Monitor::default();
        assert!(!clock.observe(100_000, 10_000, 100_000));
        assert!(!clock.observe(100_250, 10_250, 100_000));
        assert!(clock.observe(99_999, 10_500, 100_250));
        assert!(clock.faulted);
        assert!(!clock.observe(100_249, 10_750, 100_250));
        assert!(clock.faulted);
        assert!(clock.observe(103_000, 13_000, 100_250));
    }

    #[test]
    fn joint_quorum_requires_majority_in_both_configs() {
        let configs = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([3, 4, 5])];
        assert!(!joint_quorum(&configs, &BTreeSet::from([1, 2, 4])));
        assert!(joint_quorum(&configs, &BTreeSet::from([1, 2, 3, 4])));
        assert!(!joint_quorum(&[], &BTreeSet::from([1])));
    }

    #[test]
    fn watchdog_gap_is_strictly_greater_than_two_seconds() {
        let mut at_limit = Monitor::default();
        assert!(!at_limit.observe(100_000, 100_000, 0));
        assert!(!at_limit.observe(102_000, 102_000, 0));
        let mut beyond_limit = Monitor::default();
        assert!(!beyond_limit.observe(100_000, 100_000, 0));
        assert!(beyond_limit.observe(102_001, 102_001, 0));
        assert!(beyond_limit.faulted);
    }

    #[tokio::test]
    async fn persisted_fault_is_visible_to_late_subscribers() {
        let authority = ClockAuthority::new(true);
        let fault = authority.subscribe_fault();
        assert!(matches!(&*fault.borrow(), ClockFaultState::Latched { .. }));
    }
}
