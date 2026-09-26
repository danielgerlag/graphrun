use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn wall_millis() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "wall clock precedes Unix epoch",
            )
        })?
        .as_millis()
        .try_into()
        .map_err(|_| {
            Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "wall clock out of range",
            )
        })
}

#[cfg(target_os = "linux")]
pub fn boot_millis() -> Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) } != 0 {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            std::io::Error::last_os_error().to_string(),
        ));
    }
    if ts.tv_sec < 0 || !(0..1_000_000_000).contains(&ts.tv_nsec) {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "boot clock returned an invalid timespec",
        ));
    }
    Ok((ts.tv_sec as u64)
        .saturating_mul(1_000)
        .saturating_add(ts.tv_nsec as u64 / 1_000_000))
}

#[cfg(target_os = "macos")]
pub fn boot_millis() -> Result<u64> {
    unsafe extern "C" {
        fn mach_continuous_time() -> u64;
        fn mach_timebase_info(info: *mut libc::mach_timebase_info_data_t) -> libc::kern_return_t;
    }
    let mut info = libc::mach_timebase_info_data_t { numer: 0, denom: 0 };
    if unsafe { mach_timebase_info(&mut info) } != 0 || info.denom == 0 {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "mach continuous time unavailable",
        ));
    }
    ((u128::from(unsafe { mach_continuous_time() }) * u128::from(info.numer))
        / u128::from(info.denom)
        / 1_000_000)
        .try_into()
        .map_err(|_| {
            Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "boot clock out of range",
            )
        })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn boot_millis() -> Result<u64> {
    Err(Error::new(
        crate::error::ErrorKind::FailedPrecondition,
        "suspend-aware boot clock is required on Linux or macOS",
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClockDeltaFault {
    BootReversed,
    WatchdogGap(u64),
    WallBootSkew(u128, u64),
}

pub(crate) fn clock_delta_fault(
    previous_wall: u64,
    previous_boot: u64,
    wall: u64,
    boot: u64,
) -> Option<ClockDeltaFault> {
    let Some(elapsed) = boot.checked_sub(previous_boot) else {
        return Some(ClockDeltaFault::BootReversed);
    };
    if elapsed > 2_000 {
        return Some(ClockDeltaFault::WatchdogGap(elapsed));
    }
    let difference =
        (i128::from(wall) - i128::from(previous_wall) - i128::from(elapsed)).unsigned_abs();
    if difference > u128::from(250 + elapsed / 1_000) {
        return Some(ClockDeltaFault::WallBootSkew(difference, elapsed));
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EngineTime(u64);

impl EngineTime {
    pub const UNIX_EPOCH: Self = Self(0);

    pub const fn from_millis(ms: u64) -> Self {
        Self(ms)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    pub fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration.as_millis() as u64))
    }
}

pub fn parse_duration(text: &str) -> Result<Duration> {
    let bytes = text.as_bytes();
    let split = bytes
        .iter()
        .position(|b| !b.is_ascii_digit())
        .ok_or_else(|| Error::invalid(format!("duration {text} is missing a unit")))?;
    if split == 0 {
        return Err(Error::invalid(format!("duration {text} has no integer")));
    }
    let n: u64 = text[..split]
        .parse()
        .map_err(|_| Error::invalid(format!("duration {text} is not a positive integer")))?;
    if n == 0 {
        return Err(Error::invalid(format!("duration {text} must be positive")));
    }
    match &text[split..] {
        "ms" => Ok(Duration::from_millis(n)),
        "s" => Ok(Duration::from_secs(n)),
        "m" => Ok(Duration::from_secs(n.saturating_mul(60))),
        "h" => Ok(Duration::from_secs(n.saturating_mul(60 * 60))),
        "d" => Ok(Duration::from_secs(n.saturating_mul(60 * 60 * 24))),
        unit => Err(Error::invalid(format!("unknown duration unit {unit}"))),
    }
}

pub fn duration_to_millis(duration: Duration) -> Result<u64> {
    let ms = duration.as_millis();
    if ms == 0 || ms > u128::from(u64::MAX) {
        return Err(Error::invalid("duration is out of range"));
    }
    Ok(ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(parse_duration("10ms").unwrap(), Duration::from_millis(10));
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("5").is_err());
    }
}
