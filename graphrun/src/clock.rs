use crate::cluster::ClusterNetwork;
use crate::error::{Error, ErrorKind, Result};
use crate::storage::{StorageHandle, TypeConfig};
use crate::time::{boot_millis, wall_millis};
use openraft::Raft;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as AsyncMutex;

const MAX_DISCREPANCY_MS: u64 = 250;
const MAX_WATCHDOG_GAP_MS: u64 = 2_000;
const QUORUM_REFRESH_MS: u64 = 5_000;
const QUORUM_EXPIRY_MS: u64 = 10_000;

#[derive(Default)]
struct Monitor {
    previous: Option<(u64, u64)>,
    faulted: bool,
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
            let elapsed = boot.saturating_sub(previous_boot);
            let wall_elapsed = wall as i128 - previous_wall as i128;
            let discrepancy = (wall_elapsed - elapsed as i128).unsigned_abs();
            if boot < previous_boot {
                fault = Some("boot clock reversed".to_owned());
            } else if discrepancy > u128::from(MAX_DISCREPANCY_MS + elapsed / 1_000) {
                fault = Some(format!(
                    "wall/boot delta differs by {discrepancy} ms over {elapsed} ms"
                ));
            } else if elapsed > MAX_WATCHDOG_GAP_MS {
                fault = Some(format!("watchdog gap {elapsed} ms exceeds 2000 ms"));
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

struct QuorumSample {
    checked_boot_ms: u64,
    membership_index: Option<u64>,
    voters: Vec<u64>,
}

pub struct ClockAuthority {
    monitor: AsyncMutex<Monitor>,
    network: Mutex<Option<ClusterNetwork>>,
    last_quorum: AsyncMutex<Option<QuorumSample>>,
    #[cfg(test)]
    test_watermark: AtomicU64,
}

impl ClockAuthority {
    pub fn new(faulted: bool) -> Self {
        Self {
            monitor: AsyncMutex::new(Monitor {
                faulted,
                fault_reason: faulted
                    .then(|| "clock fault persisted from previous process".to_owned()),
                ..Monitor::default()
            }),
            network: Mutex::new(None),
            last_quorum: AsyncMutex::new(None),
            #[cfg(test)]
            test_watermark: AtomicU64::new(0),
        }
    }

    pub fn configure_network(&self, network: ClusterNetwork) {
        *self.network.lock().unwrap() = Some(network);
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
                    storage.persist_clock_fault(true).await?;
                    tracing::error!(%error, "clock source failed; scheduling and effects disabled");
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
            storage.persist_clock_fault(true).await?;
            tracing::error!(
                reason = monitor.fault_reason.as_deref().unwrap_or("unknown"),
                "clock fault latched; scheduling and effects disabled"
            );
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
        monitor.fault_reason = None;
        *self.last_quorum.lock().await = None;
        Ok(())
    }

    pub async fn authorize(&self, storage: &StorageHandle, raft: &Raft<TypeConfig>) -> Result<()> {
        let (_, before_boot) = self.sample(storage).await?;
        let metrics = raft.metrics().borrow().clone();
        let voters: Vec<u64> = metrics.membership_config.voter_ids().collect();
        let membership_index = metrics.membership_config.log_id().map(|id| id.index);
        if voters.len() <= 1 {
            return Ok(());
        }
        let mut last = self.last_quorum.lock().await;
        if last.as_ref().is_some_and(|sample| {
            sample.membership_index == membership_index
                && sample.voters == voters
                && before_boot.saturating_sub(sample.checked_boot_ms) < QUORUM_REFRESH_MS
        }) {
            return Ok(());
        }
        let network = self.network.lock().unwrap().clone().ok_or_else(|| {
            Error::new(ErrorKind::Unavailable, "cluster clock network unavailable")
        })?;
        let mut roster = storage.applied_members().await?;
        if roster.is_empty() {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while roster.is_empty() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                roster = storage.applied_members().await?;
            }
        }
        let mut healthy = usize::from(voters.contains(&network.local_id()));
        let mut failures = Vec::new();
        for id in voters
            .iter()
            .copied()
            .filter(|id| *id != network.local_id())
        {
            let Some(endpoint) = roster.get(&id) else {
                failures.push(format!("member {id}: not in committed roster"));
                continue;
            };
            match network.probe_clock(id, endpoint).await {
                Ok(()) => healthy += 1,
                Err(err) => failures.push(format!("member {id}: {err}")),
            }
        }
        let (_, after_boot) = self.sample(storage).await?;
        if healthy > voters.len() / 2 {
            *last = Some(QuorumSample {
                checked_boot_ms: after_boot,
                membership_index,
                voters,
            });
            return Ok(());
        }
        if last.as_ref().is_some_and(|sample| {
            sample.membership_index == membership_index
                && sample.voters == voters
                && after_boot.saturating_sub(sample.checked_boot_ms) < QUORUM_EXPIRY_MS
        }) {
            return Ok(());
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
}
