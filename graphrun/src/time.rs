use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
