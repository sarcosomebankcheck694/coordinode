//! Monotonic timestamp allocation for MVCC versioning.
//!
//! CE uses a local atomic counter — no central oracle needed for
//! single-shard deployments. EE will replace this with HLC.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A monotonic timestamp used for MVCC version ordering.
///
/// Timestamps are u64 values where larger = newer. The zero value
/// is reserved as "no timestamp" / "before all versions".
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The zero timestamp (before all versions).
    pub const ZERO: Self = Self(0);

    /// The maximum timestamp.
    pub const MAX: Self = Self(u64::MAX);

    /// Create a timestamp from a raw u64 value.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw u64 value.
    pub fn as_raw(self) -> u64 {
        self.0
    }

    /// Check if this is the zero timestamp.
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ts:{}", self.0)
    }
}

/// Local monotonic timestamp oracle for CE single-shard mode.
///
/// Allocates timestamps as a simple atomic counter. Thread-safe
/// and lock-free. Guaranteed monotonically increasing.
///
/// Initial value is seeded from wall clock (microseconds since epoch)
/// to ensure timestamps survive process restarts without collision.
#[derive(Debug)]
pub struct TimestampOracle {
    counter: AtomicU64,
}

impl TimestampOracle {
    /// Create a new oracle seeded from the current wall clock.
    pub fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros() as u64;

        Self {
            counter: AtomicU64::new(seed),
        }
    }

    /// Create an oracle starting from a specific timestamp.
    /// Used for recovery (resume from last known commit_ts).
    pub fn resume_from(last_ts: Timestamp) -> Self {
        Self {
            counter: AtomicU64::new(last_ts.as_raw()),
        }
    }

    /// Allocate the next timestamp (for start_ts or commit_ts).
    pub fn next(&self) -> Timestamp {
        let ts = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Timestamp(ts)
    }

    /// Get the current high-water mark without advancing.
    pub fn current(&self) -> Timestamp {
        Timestamp(self.counter.load(Ordering::SeqCst))
    }

    /// Advance the oracle to at least the given timestamp.
    /// Used during recovery when replaying committed transactions.
    pub fn advance_to(&self, ts: Timestamp) {
        self.counter.fetch_max(ts.as_raw(), Ordering::SeqCst);
    }
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_ordering() {
        let a = Timestamp::from_raw(1);
        let b = Timestamp::from_raw(2);
        assert!(a < b);
        assert!(Timestamp::ZERO < a);
        assert!(b < Timestamp::MAX);
    }

    #[test]
    fn timestamp_zero() {
        assert!(Timestamp::ZERO.is_zero());
        assert!(!Timestamp::from_raw(1).is_zero());
    }

    #[test]
    fn oracle_allocates_monotonically() {
        let oracle = TimestampOracle::resume_from(Timestamp::from_raw(100));
        let t1 = oracle.next();
        let t2 = oracle.next();
        let t3 = oracle.next();

        assert_eq!(t1.as_raw(), 101);
        assert_eq!(t2.as_raw(), 102);
        assert_eq!(t3.as_raw(), 103);
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn oracle_current_does_not_advance() {
        let oracle = TimestampOracle::resume_from(Timestamp::from_raw(50));
        assert_eq!(oracle.current().as_raw(), 50);
        assert_eq!(oracle.current().as_raw(), 50);
    }

    #[test]
    fn oracle_advance_to() {
        let oracle = TimestampOracle::resume_from(Timestamp::from_raw(10));
        oracle.advance_to(Timestamp::from_raw(100));
        let next = oracle.next();
        assert_eq!(next.as_raw(), 101);
    }

    #[test]
    fn oracle_advance_to_does_not_go_backwards() {
        let oracle = TimestampOracle::resume_from(Timestamp::from_raw(100));
        oracle.advance_to(Timestamp::from_raw(50));
        let next = oracle.next();
        assert_eq!(next.as_raw(), 101);
    }

    #[test]
    fn oracle_concurrent_allocation() {
        use std::collections::BTreeSet;
        use std::sync::Arc;

        let oracle = Arc::new(TimestampOracle::resume_from(Timestamp::from_raw(0)));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let oracle = Arc::clone(&oracle);
            handles.push(std::thread::spawn(move || {
                (0..1000)
                    .map(|_| oracle.next().as_raw())
                    .collect::<Vec<_>>()
            }));
        }

        let mut all_ts: BTreeSet<u64> = BTreeSet::new();
        for h in handles {
            let ts_vec = h.join().expect("thread panicked");
            for ts in ts_vec {
                assert!(all_ts.insert(ts), "duplicate timestamp: {ts}");
            }
        }

        // 8 threads × 1000 = 8000 unique timestamps
        assert_eq!(all_ts.len(), 8000);
    }

    #[test]
    fn oracle_new_seeds_from_clock() {
        let oracle = TimestampOracle::new();
        let ts = oracle.next();
        // Should be roughly current time in microseconds (> year 2020)
        assert!(ts.as_raw() > 1_600_000_000_000_000);
    }
}
