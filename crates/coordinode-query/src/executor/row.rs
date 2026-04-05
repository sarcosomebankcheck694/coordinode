//! Row type: a single result tuple from query execution.

use std::collections::BTreeMap;

use coordinode_core::graph::types::Value;

/// A result row: ordered map of column name → value.
///
/// Uses BTreeMap for deterministic column ordering in output.
pub type Row = BTreeMap<String, Value>;
