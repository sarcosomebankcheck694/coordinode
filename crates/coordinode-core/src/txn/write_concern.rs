//! Write concern levels for controlling write durability guarantees.
//!
//! Write concerns determine when a write is acknowledged to the client
//! relative to the cluster's replication state.
//!
//! ## Levels
//!
//! - **W0**: Fire-and-forget. No acknowledgement. Data may be lost.
//! - **Memory**: Data in RAM only (~1µs ACK). Lost on process crash.
//!   Background drain thread batches into Raft proposals. Visible
//!   immediately to local readers.
//! - **Cache**: Data in RAM + NVMe cache (~100µs ACK). Survives process
//!   crash, lost on power failure. Background drain to Raft.
//! - **W1**: Leader WAL fsync. Acknowledged after leader persists to disk.
//!   Rollback possible if leader fails before replication.
//! - **Majority**: Raft majority quorum ACK. Cannot be rolled back.
//!   Production default.
//!
//! ## Journal gate (j:true)
//!
//! When `journal = true`, forces WAL fsync before acknowledgement,
//! independent of the write concern level. `j:true` with `w:0`
//! silently upgrades to `w:1` (cannot journal without local write).
//! `j:true` with `w:memory` silently upgrades to `w:1`.
//!
//! ## Volatile write drain
//!
//! `w:memory` and `w:cache` writes are applied locally for immediate
//! read visibility, then buffered in a [`DrainBuffer`](super::drain::DrainBuffer)
//! for background Raft replication. Un-drained writes are lost on
//! crash (memory) or power failure (cache). Graceful shutdown flushes
//! all drain buffers before exit.

/// Write concern level controlling durability guarantees.
///
/// Maps to proto `WriteConcernLevel` in `consistency.proto`.
///
/// Ordered by durability: W0 < Memory < Cache < W1 < Majority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WriteConcernLevel {
    /// Fire-and-forget. No acknowledgement to client.
    /// Data may be lost immediately. Use only for metrics/telemetry.
    W0,

    /// Data in RAM only (~1µs ACK). Lost on process crash.
    /// Visible immediately to local readers. Background drain thread
    /// batches into Raft proposals every ~100ms. Un-drained writes
    /// lost on crash. Use for hot counters, session state, real-time signals.
    Memory,

    /// Data in RAM + NVMe cache (~100µs ACK). Survives process crash,
    /// lost on power failure. Background drain to Raft.
    /// Use for analytics events, non-critical user data.
    Cache,

    /// Leader WAL fsync. Acknowledged after leader persists locally.
    /// Rollback possible if leader crashes before replication.
    W1,

    /// Raft majority quorum acknowledgement. Cannot be rolled back.
    /// Production default. Required for causal consistency sessions.
    #[default]
    Majority,
}

impl WriteConcernLevel {
    /// Whether this level requires Raft majority acknowledgement.
    pub fn requires_majority(&self) -> bool {
        matches!(self, Self::Majority)
    }

    /// Whether data at this level can be rolled back on leader failure.
    pub fn can_rollback(&self) -> bool {
        !matches!(self, Self::Majority)
    }

    /// Whether this level is safe for causal consistency sessions.
    /// Causal sessions require majority to ensure operationTime is durable.
    /// Memory/Cache writes are NOT replicated until drained — if the leader
    /// crashes before drain, operationTime becomes a dangling causal dependency.
    pub fn is_causal_safe(&self) -> bool {
        matches!(self, Self::Majority)
    }

    /// Whether this level uses the volatile drain buffer instead of
    /// synchronous Raft proposals.
    pub fn is_volatile(&self) -> bool {
        matches!(self, Self::Memory | Self::Cache)
    }
}

/// Write concern configuration for a mutation or transaction.
#[derive(Debug, Clone)]
pub struct WriteConcern {
    /// Durability level.
    pub level: WriteConcernLevel,

    /// Journal gate: force WAL fsync before acknowledgement.
    /// When true with `W0`, silently upgrades to `W1`.
    pub journal: bool,

    /// Timeout in milliseconds for write concern satisfaction.
    /// `0` = no timeout (wait indefinitely).
    /// On timeout, data is NOT rolled back — client must verify.
    pub timeout_ms: u32,
}

impl Default for WriteConcern {
    fn default() -> Self {
        Self {
            level: WriteConcernLevel::Majority,
            journal: false,
            timeout_ms: 0,
        }
    }
}

impl WriteConcern {
    /// W0: fire-and-forget, no acknowledgement.
    pub fn w0() -> Self {
        Self {
            level: WriteConcernLevel::W0,
            journal: false,
            timeout_ms: 0,
        }
    }

    /// W1: leader WAL fsync only.
    pub fn w1() -> Self {
        Self {
            level: WriteConcernLevel::W1,
            journal: false,
            timeout_ms: 0,
        }
    }

    /// Majority: Raft quorum acknowledgement (production default).
    pub fn majority() -> Self {
        Self::default()
    }

    /// w:memory — data in RAM, ~1µs ACK, drain to Raft in background.
    pub fn memory() -> Self {
        Self {
            level: WriteConcernLevel::Memory,
            journal: false,
            timeout_ms: 0,
        }
    }

    /// w:cache — data in RAM + NVMe, ~100µs ACK, drain to Raft in background.
    pub fn cache() -> Self {
        Self {
            level: WriteConcernLevel::Cache,
            journal: false,
            timeout_ms: 0,
        }
    }

    /// Majority with journal gate and timeout.
    pub fn majority_journaled(timeout_ms: u32) -> Self {
        Self {
            level: WriteConcernLevel::Majority,
            journal: true,
            timeout_ms,
        }
    }

    /// Resolve effective level after applying journal gate overrides.
    ///
    /// - `j:true` with `W0` → upgrades to `W1` (cannot journal without local write)
    /// - `j:true` with `Memory` → upgrades to `W1` (contradictory: can't be
    ///   in-memory AND journaled)
    /// - All other combinations unchanged.
    pub fn effective_level(&self) -> WriteConcernLevel {
        if self.journal {
            match self.level {
                WriteConcernLevel::W0 | WriteConcernLevel::Memory => WriteConcernLevel::W1,
                other => other,
            }
        } else {
            self.level
        }
    }

    /// Validate that the write concern configuration is consistent.
    pub fn validate(&self) -> Result<(), &'static str> {
        // j:true + memory is contradictory but handled silently by effective_level
        // (upgrades to W1). No hard validation errors.
        Ok(())
    }

    /// Check if this write concern is safe for causal consistency sessions.
    /// Returns error message if not.
    ///
    /// Causal sessions HARD REJECT anything below majority. Memory/Cache writes
    /// are not replicated until drained — if the leader crashes before drain,
    /// operationTime becomes a dangling causal dependency.
    pub fn validate_for_causal_session(&self) -> Result<(), &'static str> {
        if !self.effective_level().is_causal_safe() {
            return Err("causal sessions require writeConcern >= 'majority'; \
                 use non-causal session for volatile writes (w:memory, w:cache, w:0, w:1)");
        }
        Ok(())
    }
}

impl std::fmt::Display for WriteConcernLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::W0 => write!(f, "w:0"),
            Self::Memory => write!(f, "w:memory"),
            Self::Cache => write!(f, "w:cache"),
            Self::W1 => write!(f, "w:1"),
            Self::Majority => write!(f, "w:majority"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_majority() {
        let wc = WriteConcern::default();
        assert_eq!(wc.level, WriteConcernLevel::Majority);
        assert!(!wc.journal);
        assert_eq!(wc.timeout_ms, 0);
    }

    #[test]
    fn constructors() {
        assert_eq!(WriteConcern::w0().level, WriteConcernLevel::W0);
        assert_eq!(WriteConcern::memory().level, WriteConcernLevel::Memory);
        assert_eq!(WriteConcern::cache().level, WriteConcernLevel::Cache);
        assert_eq!(WriteConcern::w1().level, WriteConcernLevel::W1);
        assert_eq!(WriteConcern::majority().level, WriteConcernLevel::Majority);

        let mj = WriteConcern::majority_journaled(5000);
        assert_eq!(mj.level, WriteConcernLevel::Majority);
        assert!(mj.journal);
        assert_eq!(mj.timeout_ms, 5000);
    }

    #[test]
    fn effective_level_journal_upgrade() {
        // j:true + W0 → W1
        let wc = WriteConcern {
            level: WriteConcernLevel::W0,
            journal: true,
            timeout_ms: 0,
        };
        assert_eq!(wc.effective_level(), WriteConcernLevel::W1);

        // j:true + Memory → W1 (contradictory: can't be in-memory AND journaled)
        let wc_mem = WriteConcern {
            level: WriteConcernLevel::Memory,
            journal: true,
            timeout_ms: 0,
        };
        assert_eq!(wc_mem.effective_level(), WriteConcernLevel::W1);

        // j:true + Cache → Cache (NVMe survives process crash, j:true is redundant)
        let wc_cache = WriteConcern {
            level: WriteConcernLevel::Cache,
            journal: true,
            timeout_ms: 0,
        };
        assert_eq!(wc_cache.effective_level(), WriteConcernLevel::Cache);

        // j:true + W1 → W1 (no change)
        let wc2 = WriteConcern {
            level: WriteConcernLevel::W1,
            journal: true,
            timeout_ms: 0,
        };
        assert_eq!(wc2.effective_level(), WriteConcernLevel::W1);

        // j:false + W0 → W0 (no upgrade)
        assert_eq!(WriteConcern::w0().effective_level(), WriteConcernLevel::W0);
    }

    #[test]
    fn requires_majority() {
        assert!(!WriteConcernLevel::W0.requires_majority());
        assert!(!WriteConcernLevel::Memory.requires_majority());
        assert!(!WriteConcernLevel::Cache.requires_majority());
        assert!(!WriteConcernLevel::W1.requires_majority());
        assert!(WriteConcernLevel::Majority.requires_majority());
    }

    #[test]
    fn can_rollback() {
        assert!(WriteConcernLevel::W0.can_rollback());
        assert!(WriteConcernLevel::Memory.can_rollback());
        assert!(WriteConcernLevel::Cache.can_rollback());
        assert!(WriteConcernLevel::W1.can_rollback());
        assert!(!WriteConcernLevel::Majority.can_rollback());
    }

    #[test]
    fn causal_safety() {
        assert!(!WriteConcernLevel::W0.is_causal_safe());
        assert!(!WriteConcernLevel::Memory.is_causal_safe());
        assert!(!WriteConcernLevel::Cache.is_causal_safe());
        assert!(!WriteConcernLevel::W1.is_causal_safe());
        assert!(WriteConcernLevel::Majority.is_causal_safe());
    }

    #[test]
    fn is_volatile() {
        assert!(!WriteConcernLevel::W0.is_volatile());
        assert!(WriteConcernLevel::Memory.is_volatile());
        assert!(WriteConcernLevel::Cache.is_volatile());
        assert!(!WriteConcernLevel::W1.is_volatile());
        assert!(!WriteConcernLevel::Majority.is_volatile());
    }

    #[test]
    fn validate_for_causal_session() {
        assert!(WriteConcern::majority()
            .validate_for_causal_session()
            .is_ok());
        assert!(WriteConcern::w0().validate_for_causal_session().is_err());
        assert!(WriteConcern::w1().validate_for_causal_session().is_err());
        assert!(WriteConcern::memory()
            .validate_for_causal_session()
            .is_err());
        assert!(WriteConcern::cache().validate_for_causal_session().is_err());

        // j:true + W0 upgrades to W1 → still not causal-safe
        let wc = WriteConcern {
            level: WriteConcernLevel::W0,
            journal: true,
            timeout_ms: 0,
        };
        assert!(wc.validate_for_causal_session().is_err());

        // j:true + Memory upgrades to W1 → still not causal-safe
        let wc_mem = WriteConcern {
            level: WriteConcernLevel::Memory,
            journal: true,
            timeout_ms: 0,
        };
        assert!(wc_mem.validate_for_causal_session().is_err());
    }

    #[test]
    fn display() {
        assert_eq!(WriteConcernLevel::W0.to_string(), "w:0");
        assert_eq!(WriteConcernLevel::Memory.to_string(), "w:memory");
        assert_eq!(WriteConcernLevel::Cache.to_string(), "w:cache");
        assert_eq!(WriteConcernLevel::W1.to_string(), "w:1");
        assert_eq!(WriteConcernLevel::Majority.to_string(), "w:majority");
    }

    #[test]
    fn validate_ok() {
        assert!(WriteConcern::w0().validate().is_ok());
        assert!(WriteConcern::memory().validate().is_ok());
        assert!(WriteConcern::cache().validate().is_ok());
        assert!(WriteConcern::w1().validate().is_ok());
        assert!(WriteConcern::majority().validate().is_ok());
        assert!(WriteConcern::majority_journaled(5000).validate().is_ok());
    }
}
