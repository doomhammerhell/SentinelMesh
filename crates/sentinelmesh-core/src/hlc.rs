use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Hybrid Logical Clock (HLC) for causal ordering in distributed systems.
/// Implements the HLC algorithm as described in "Logical Physical Clocks and Consistent
/// Snapshots in Distributed Systems".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hlc {
    /// Wall clock time (l in the paper).
    pub wall_time: i64,
    /// Logical counter (c in the paper).
    pub logical: u32,
}

impl Hlc {
    pub fn new(wall_time: i64, logical: u32) -> Self {
        Self { wall_time, logical }
    }

    /// Generate a new HLC from a given wall time and the previous HLC.
    pub fn from_now(previous: &Self, now_ms: i64) -> Self {
        let l_next = std::cmp::max(previous.wall_time, now_ms);
        let c_next = if l_next == previous.wall_time {
            previous.logical + 1
        } else {
            0
        };

        pub_hlc(l_next, c_next)
    }

    /// Update the local HLC given a remote HLC and a given wall time.
    pub fn update_with_time(&mut self, remote: &Self, now_ms: i64) {
        let l_next = std::cmp::max(self.wall_time, std::cmp::max(remote.wall_time, now_ms));

        let c_next = if l_next == self.wall_time && l_next == remote.wall_time {
            std::cmp::max(self.logical, remote.logical) + 1
        } else if l_next == self.wall_time {
            self.logical + 1
        } else if l_next == remote.wall_time {
            remote.logical + 1
        } else {
            0
        };

        self.wall_time = l_next;
        self.logical = c_next;
    }

    /// Generate a new HLC from the current system time and the previous HLC.
    pub fn now(previous: &Self) -> Self {
        Self::from_now(previous, Utc::now().timestamp_millis())
    }

    /// Update the local HLC given a remote HLC and the current system time.
    pub fn update(&mut self, remote: &Self) {
        self.update_with_time(remote, Utc::now().timestamp_millis())
    }

    pub fn to_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.wall_time).unwrap_or_else(Utc::now)
    }
}

fn pub_hlc(wall_time: i64, logical: u32) -> Hlc {
    Hlc { wall_time, logical }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_time.cmp(&other.wall_time) {
            Ordering::Equal => self.logical.cmp(&other.logical),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_deterministic_monotonicity() {
        let h1 = Hlc::new(1000, 0);

        // Wall time advances
        let h2 = Hlc::from_now(&h1, 1100);
        assert!(h2 > h1);
        assert_eq!(h2.wall_time, 1100);
        assert_eq!(h2.logical, 0);

        // Wall time stays same, counter increments
        let h3 = Hlc::from_now(&h2, 1100);
        assert!(h3 > h2);
        assert_eq!(h3.wall_time, 1100);
        assert_eq!(h3.logical, 1);

        // Update with remote
        let mut local = Hlc::new(1200, 0);
        let remote = Hlc::new(1300, 10);
        local.update_with_time(&remote, 1250);
        assert!(local > remote);
        assert_eq!(local.wall_time, 1300);
        assert_eq!(local.logical, 11);
    }
}
