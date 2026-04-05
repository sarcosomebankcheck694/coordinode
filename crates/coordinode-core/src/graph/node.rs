//! Node storage: key encoding, ID allocation, and record serialization.
//!
//! Nodes are stored as single KV entries in the `node:` partition:
//!
//! ```text
//! Key:   node:<shard_id u16 BE>:<node_id u64 BE>
//! Value: MessagePack { label: String, props: HashMap<u32, Value> }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// A unique 64-bit node identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Create a NodeId from a raw u64.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw u64 value.
    pub fn as_raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}", self.0)
    }
}

/// Monotonic node ID allocator.
///
/// Thread-safe, lock-free. Each call to `next()` returns a unique,
/// monotonically increasing ID. The allocator can be seeded from
/// a starting value (e.g., after recovery).
pub struct NodeIdAllocator {
    counter: AtomicU64,
}

impl NodeIdAllocator {
    /// Create a new allocator starting from 0.
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    /// Create an allocator resuming from the last allocated ID.
    pub fn resume_from(last_id: NodeId) -> Self {
        Self {
            counter: AtomicU64::new(last_id.as_raw()),
        }
    }

    /// Allocate the next node ID.
    pub fn next(&self) -> NodeId {
        let id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        NodeId(id)
    }

    /// Get the current high-water mark without advancing.
    pub fn current(&self) -> NodeId {
        NodeId(self.counter.load(Ordering::SeqCst))
    }

    /// Advance to at least the given ID (for recovery).
    pub fn advance_to(&self, id: NodeId) {
        self.counter.fetch_max(id.as_raw(), Ordering::SeqCst);
    }
}

impl Default for NodeIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// -- Key encoding --

/// Key prefix for node records.
const NODE_KEY_PREFIX: &[u8] = b"node:";

/// Encode a node key: `node:<shard_id u16 BE>:<node_id u64 BE>`.
///
/// Big-endian ensures lexicographic ordering matches numeric ordering,
/// enabling efficient range scans within a shard.
pub fn encode_node_key(shard_id: u16, node_id: NodeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_KEY_PREFIX.len() + 2 + 1 + 8);
    key.extend_from_slice(NODE_KEY_PREFIX);
    key.extend_from_slice(&shard_id.to_be_bytes());
    key.push(b':');
    key.extend_from_slice(&node_id.as_raw().to_be_bytes());
    key
}

/// Decode a node key back into (shard_id, node_id).
///
/// Returns `None` if the key doesn't match the expected format.
pub fn decode_node_key(key: &[u8]) -> Option<(u16, NodeId)> {
    let prefix_len = NODE_KEY_PREFIX.len();
    // node: (5) + shard (2) + : (1) + id (8) = 16
    if key.len() != prefix_len + 2 + 1 + 8 {
        return None;
    }
    if &key[..prefix_len] != NODE_KEY_PREFIX {
        return None;
    }
    if key[prefix_len + 2] != b':' {
        return None;
    }
    let shard_id = u16::from_be_bytes(key[prefix_len..prefix_len + 2].try_into().ok()?);
    let node_id = u64::from_be_bytes(key[prefix_len + 3..prefix_len + 11].try_into().ok()?);
    Some((shard_id, NodeId(node_id)))
}

// -- Node record --

/// A node record stored in the `node:` partition.
///
/// Properties are stored with interned field IDs (u32 keys) rather than
/// string field names, achieving ~80% reduction in key storage.
///
/// In VALIDATED schema mode, undeclared properties are stored in `extra`
/// with string keys (no interning). Declared properties remain in `props`.
///
/// Nodes support multiple labels per OpenCypher spec: `(n:User:Admin)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeRecord {
    /// The node's labels (e.g., ["User", "Admin"]).
    /// First label is the primary label used for schema lookups.
    pub labels: Vec<String>,

    /// Properties keyed by interned field ID.
    /// Values are MessagePack-compatible via `PropertyValue`.
    pub props: HashMap<u32, PropertyValue>,

    /// Overflow map for undeclared properties in VALIDATED schema mode.
    /// Uses string keys (no interning) to avoid polluting the field interner
    /// with ad-hoc property names. Empty/None in STRICT and FLEXIBLE modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, PropertyValue>>,
}

/// A property value — alias for the full type system `Value` enum.
///
/// See `graph::types::Value` for all 12 supported types.
pub type PropertyValue = super::types::Value;

impl NodeRecord {
    /// Create a new node record with a single label and no properties.
    pub fn new(label: impl Into<String>) -> Self {
        let label = label.into();
        Self {
            labels: if label.is_empty() {
                Vec::new()
            } else {
                vec![label]
            },
            props: HashMap::new(),
            extra: None,
        }
    }

    /// Create a node record with multiple labels.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self {
            labels,
            props: HashMap::new(),
            extra: None,
        }
    }

    /// The primary label (first in the list), or empty string if no labels.
    pub fn primary_label(&self) -> &str {
        self.labels.first().map(|s| s.as_str()).unwrap_or("")
    }

    /// Check if the node has a specific label.
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }

    /// Add a label if not already present.
    pub fn add_label(&mut self, label: String) {
        if !self.has_label(&label) {
            self.labels.push(label);
        }
    }

    /// Remove a label. Returns true if the label was present.
    pub fn remove_label(&mut self, label: &str) -> bool {
        let len_before = self.labels.len();
        self.labels.retain(|l| l != label);
        self.labels.len() < len_before
    }

    /// Set a property by interned field ID.
    pub fn set(&mut self, field_id: u32, value: PropertyValue) {
        self.props.insert(field_id, value);
    }

    /// Get a property by interned field ID.
    pub fn get(&self, field_id: u32) -> Option<&PropertyValue> {
        self.props.get(&field_id)
    }

    /// Remove a property by interned field ID.
    pub fn remove(&mut self, field_id: u32) -> Option<PropertyValue> {
        self.props.remove(&field_id)
    }

    /// Set an undeclared property in the extra overflow map (VALIDATED mode).
    pub fn set_extra(&mut self, name: impl Into<String>, value: PropertyValue) {
        self.extra
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
    }

    /// Get an undeclared property from the extra overflow map.
    pub fn get_extra(&self, name: &str) -> Option<&PropertyValue> {
        self.extra.as_ref()?.get(name)
    }

    /// Serialize to MessagePack bytes.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack bytes.
    ///
    /// Handles both raw msgpack (legacy) and prefix-encoded format from the
    /// DocumentMerge operator (0x00 prefix = full NodeRecord, see ADR-015).
    pub fn from_msgpack(data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        if !data.is_empty() && data[0] == crate::graph::doc_delta::PREFIX_NODE_RECORD {
            // Prefix-encoded format (after DocumentMerge): strip 0x00 prefix.
            rmp_serde::from_slice(&data[1..])
        } else if !data.is_empty() && data[0] == crate::graph::doc_delta::PREFIX_DOC_DELTA {
            // This is a raw merge operand, not a full record — cannot decode.
            Err(rmp_serde::decode::Error::Syntax(
                "cannot decode DocDelta merge operand as NodeRecord".to_string(),
            ))
        } else {
            // Legacy: raw msgpack without prefix.
            rmp_serde::from_slice(data)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // -- NodeId tests --

    #[test]
    fn node_id_roundtrip() {
        let id = NodeId::from_raw(42);
        assert_eq!(id.as_raw(), 42);
    }

    #[test]
    fn node_id_ordering() {
        let a = NodeId::from_raw(1);
        let b = NodeId::from_raw(2);
        assert!(a < b);
    }

    #[test]
    fn node_id_display() {
        let id = NodeId::from_raw(99);
        assert_eq!(format!("{id}"), "node:99");
    }

    // -- NodeIdAllocator tests --

    #[test]
    fn allocator_starts_from_one() {
        let alloc = NodeIdAllocator::new();
        assert_eq!(alloc.next().as_raw(), 1);
        assert_eq!(alloc.next().as_raw(), 2);
    }

    #[test]
    fn allocator_resume() {
        let alloc = NodeIdAllocator::resume_from(NodeId::from_raw(100));
        assert_eq!(alloc.next().as_raw(), 101);
    }

    #[test]
    fn allocator_current_does_not_advance() {
        let alloc = NodeIdAllocator::resume_from(NodeId::from_raw(50));
        assert_eq!(alloc.current().as_raw(), 50);
        assert_eq!(alloc.current().as_raw(), 50);
    }

    #[test]
    fn allocator_advance_to() {
        let alloc = NodeIdAllocator::resume_from(NodeId::from_raw(10));
        alloc.advance_to(NodeId::from_raw(100));
        assert_eq!(alloc.next().as_raw(), 101);
    }

    #[test]
    fn allocator_concurrent() {
        use std::collections::BTreeSet;
        use std::sync::Arc;

        let alloc = Arc::new(NodeIdAllocator::new());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let alloc = Arc::clone(&alloc);
            handles.push(std::thread::spawn(move || {
                (0..500).map(|_| alloc.next().as_raw()).collect::<Vec<_>>()
            }));
        }

        let mut all: BTreeSet<u64> = BTreeSet::new();
        for h in handles {
            for id in h.join().expect("thread panicked") {
                assert!(all.insert(id), "duplicate ID: {id}");
            }
        }
        assert_eq!(all.len(), 4000);
    }

    // -- Key encoding tests --

    #[test]
    fn encode_decode_key_roundtrip() {
        let shard = 42u16;
        let id = NodeId::from_raw(12345);
        let key = encode_node_key(shard, id);
        let (dec_shard, dec_id) = decode_node_key(&key).expect("decode failed");
        assert_eq!(dec_shard, shard);
        assert_eq!(dec_id, id);
    }

    #[test]
    fn key_ordering_within_shard() {
        let k1 = encode_node_key(1, NodeId::from_raw(100));
        let k2 = encode_node_key(1, NodeId::from_raw(200));
        assert!(k1 < k2, "keys should sort by node_id within shard");
    }

    #[test]
    fn key_ordering_across_shards() {
        let k1 = encode_node_key(1, NodeId::from_raw(999));
        let k2 = encode_node_key(2, NodeId::from_raw(1));
        assert!(k1 < k2, "shard 1 keys should sort before shard 2");
    }

    #[test]
    fn decode_invalid_key() {
        assert!(decode_node_key(b"").is_none());
        assert!(decode_node_key(b"short").is_none());
        assert!(decode_node_key(b"wrong:prefix12345678").is_none());
    }

    #[test]
    fn key_starts_with_prefix() {
        let key = encode_node_key(0, NodeId::from_raw(1));
        assert!(key.starts_with(b"node:"));
    }

    // -- NodeRecord tests --

    #[test]
    fn record_new() {
        let rec = NodeRecord::new("User");
        assert_eq!(rec.primary_label(), "User");
        assert_eq!(rec.labels, vec!["User"]);
        assert!(rec.props.is_empty());
    }

    #[test]
    fn record_with_labels() {
        let rec = NodeRecord::with_labels(vec!["User".into(), "Admin".into()]);
        assert_eq!(rec.primary_label(), "User");
        assert!(rec.has_label("User"));
        assert!(rec.has_label("Admin"));
        assert!(!rec.has_label("Guest"));
    }

    #[test]
    fn record_add_remove_label() {
        let mut rec = NodeRecord::new("User");
        rec.add_label("Admin".into());
        assert!(rec.has_label("Admin"));
        assert_eq!(rec.labels.len(), 2);

        // Duplicate add is no-op
        rec.add_label("Admin".into());
        assert_eq!(rec.labels.len(), 2);

        // Remove label
        assert!(rec.remove_label("Admin"));
        assert!(!rec.has_label("Admin"));
        assert_eq!(rec.labels.len(), 1);

        // Remove non-existent label
        assert!(!rec.remove_label("Guest"));
    }

    #[test]
    fn record_empty_label() {
        let rec = NodeRecord::new("");
        assert!(rec.labels.is_empty());
        assert_eq!(rec.primary_label(), "");
    }

    #[test]
    fn record_set_get_remove() {
        let mut rec = NodeRecord::new("User");
        rec.set(1, PropertyValue::String("Alice".into()));
        rec.set(2, PropertyValue::Int(30));

        assert_eq!(rec.get(1), Some(&PropertyValue::String("Alice".into())));
        assert_eq!(rec.get(2), Some(&PropertyValue::Int(30)));
        assert_eq!(rec.get(99), None);

        let removed = rec.remove(1);
        assert_eq!(removed, Some(PropertyValue::String("Alice".into())));
        assert_eq!(rec.get(1), None);
    }

    #[test]
    fn record_msgpack_roundtrip() {
        let mut rec = NodeRecord::new("Movie");
        rec.set(1, PropertyValue::String("Inception".into()));
        rec.set(2, PropertyValue::Int(2010));
        rec.set(3, PropertyValue::Float(8.8));
        rec.set(4, PropertyValue::Bool(true));
        rec.set(5, PropertyValue::Null);
        rec.set(6, PropertyValue::Binary(vec![0xDE, 0xAD]));
        rec.set(
            7,
            PropertyValue::Array(vec![
                PropertyValue::Int(1),
                PropertyValue::String("two".into()),
            ]),
        );

        let bytes = rec.to_msgpack().expect("serialize failed");
        let restored = NodeRecord::from_msgpack(&bytes).expect("deserialize failed");
        assert_eq!(rec, restored);
    }

    #[test]
    fn record_empty_props_roundtrip() {
        let rec = NodeRecord::new("Empty");
        let bytes = rec.to_msgpack().expect("serialize");
        let restored = NodeRecord::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(rec, restored);
    }

    #[test]
    fn record_msgpack_is_compact() {
        let mut rec = NodeRecord::new("User");
        // With interned IDs (u32 keys), the MessagePack output should be
        // significantly smaller than using string field names
        rec.set(1, PropertyValue::String("Alice".into()));
        rec.set(2, PropertyValue::Int(30));

        let bytes = rec.to_msgpack().expect("serialize");
        // MessagePack with integer keys should be quite compact
        assert!(
            bytes.len() < 50,
            "encoded size {} should be < 50",
            bytes.len()
        );
    }

    // -- G028: extra overflow map --

    #[test]
    fn extra_set_and_get() {
        let mut rec = NodeRecord::new("User");
        rec.set_extra("ad_hoc", PropertyValue::String("value".into()));
        assert_eq!(
            rec.get_extra("ad_hoc"),
            Some(&PropertyValue::String("value".into()))
        );
        assert!(rec.get_extra("missing").is_none());
    }

    #[test]
    fn extra_none_by_default() {
        let rec = NodeRecord::new("User");
        assert!(rec.extra.is_none());
        assert!(rec.get_extra("anything").is_none());
    }

    #[test]
    fn extra_roundtrip_msgpack() {
        let mut rec = NodeRecord::new("Config");
        rec.set(1, PropertyValue::String("declared".into()));
        rec.set_extra("dynamic_key", PropertyValue::Int(42));
        rec.set_extra("another", PropertyValue::Bool(true));

        let bytes = rec.to_msgpack().expect("serialize");
        let decoded = NodeRecord::from_msgpack(&bytes).expect("deserialize");

        assert_eq!(
            decoded.props.get(&1),
            Some(&PropertyValue::String("declared".into()))
        );
        assert_eq!(
            decoded.get_extra("dynamic_key"),
            Some(&PropertyValue::Int(42))
        );
        assert_eq!(
            decoded.get_extra("another"),
            Some(&PropertyValue::Bool(true))
        );
    }

    #[test]
    fn extra_none_skipped_in_serialization() {
        // NodeRecord without extra should be backward compatible
        let mut rec = NodeRecord::new("User");
        rec.set(1, PropertyValue::String("Alice".into()));
        let bytes = rec.to_msgpack().expect("serialize");

        // Should deserialize even without extra field (serde default)
        let decoded = NodeRecord::from_msgpack(&bytes).expect("deserialize");
        assert!(decoded.extra.is_none());
    }

    #[test]
    fn backward_compat_roundtrip_without_extra() {
        // NodeRecord without extra should roundtrip cleanly.
        // This verifies backward compat with old format (no extra field).
        let rec = NodeRecord::new("Test");
        let bytes = rec.to_msgpack().expect("serialize");
        let decoded = NodeRecord::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(rec, decoded);
    }
}
