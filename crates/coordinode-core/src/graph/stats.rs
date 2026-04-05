//! Storage statistics for query cost estimation.
//!
//! The `StorageStats` trait provides an interface for the query planner
//! to access real storage statistics instead of hardcoded defaults.
//! Implementations should cache results and refresh periodically,
//! as computing exact statistics requires storage scans.

/// Storage statistics interface for query cost estimation.
///
/// Enables the query planner to use real data distribution information
/// (node counts, fan-out) instead of hardcoded defaults. Implementations
/// should be lightweight to call (cached internally).
pub trait StorageStats {
    /// Total number of nodes in storage.
    fn total_node_count(&self) -> u64;

    /// Number of nodes with a specific label.
    /// Returns None if per-label statistics are not available.
    fn node_count_for_label(&self, label: &str) -> Option<u64>;

    /// Average outgoing fan-out for a specific edge type.
    /// Returns None if per-edge-type statistics are not available.
    fn avg_fan_out_for_type(&self, edge_type: &str) -> Option<f64>;

    /// Overall average fan-out across all edge types.
    fn avg_fan_out(&self) -> f64;

    /// Number of distinct labels in the database.
    fn label_count(&self) -> u64;
}
