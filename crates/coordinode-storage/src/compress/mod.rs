//! Compression: per-level codec selection, field interning (future).
//!
//! Re-exports compression configuration types from the storage config module
//! for use by other crates that need to configure storage compression.

pub use crate::engine::config::{CompressionCodec, CompressionConfig};
