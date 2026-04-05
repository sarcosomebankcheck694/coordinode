//! BlobStore: content-addressed chunk storage for large values.
//!
//! Large values (embeddings, documents, images) are split into 256KB chunks,
//! each identified by its SHA-256 hash. Content-addressing enables automatic
//! deduplication — identical chunks are stored once regardless of references.
//!
//! ## Storage layout
//!
//! ```text
//! blob:<sha256_hex>           → chunk data (up to 256KB)
//! blobref:<node_id>:<prop_id> → ordered list of chunk hashes
//! ```
//!
//! ## Inline threshold
//!
//! Values smaller than 4KB are stored directly as node properties.
//! Values >= 4KB are chunked and stored via BlobStore.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::node::NodeId;

/// Default chunk size: 256KB.
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Maximum chunk size: 16MB.
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Inline threshold: values below this are stored as node properties.
pub const INLINE_THRESHOLD: usize = 4 * 1024;

/// A SHA-256 content hash identifying a blob chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId([u8; 32]);

impl ChunkId {
    /// Compute the SHA-256 hash of the given data.
    pub fn from_data(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        let mut id = [0u8; 32];
        id.copy_from_slice(&hash);
        Self(id)
    }

    /// Create from raw 32-byte hash.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the raw 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Format as lowercase hex string (64 chars).
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parse from hex string.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(chunk).ok()?;
            bytes[i] = u8::from_str_radix(s, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// A reference from a node property to a sequence of blob chunks.
///
/// The chunks, when concatenated in order, reconstruct the original value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRef {
    /// Ordered list of chunk IDs.
    pub chunks: Vec<ChunkId>,
    /// Total size of the original value in bytes.
    pub total_size: u64,
}

impl BlobRef {
    /// Create a new blob reference.
    pub fn new(chunks: Vec<ChunkId>, total_size: u64) -> Self {
        Self { chunks, total_size }
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Serialize to MessagePack.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack.
    pub fn from_msgpack(data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(data)
    }
}

// -- Key encoding --

/// Encode a blob chunk key: `blob:<sha256_hex>`.
pub fn encode_blob_key(chunk_id: &ChunkId) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + 64);
    key.extend_from_slice(b"blob:");
    key.extend_from_slice(chunk_id.to_hex().as_bytes());
    key
}

/// Encode a blob reference key: `blobref:<node_id BE>:<prop_id u32 BE>`.
pub fn encode_blobref_key(node_id: NodeId, prop_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 8 + 1 + 4);
    key.extend_from_slice(b"blobref:");
    key.extend_from_slice(&node_id.as_raw().to_be_bytes());
    key.push(b':');
    key.extend_from_slice(&prop_id.to_be_bytes());
    key
}

// -- Chunking --

/// Split data into content-addressed chunks.
///
/// Returns a list of `(ChunkId, chunk_data)` pairs. Identical data
/// produces identical ChunkIds, enabling deduplication at the storage layer.
pub fn chunk_data(data: &[u8], chunk_size: usize) -> Vec<(ChunkId, Vec<u8>)> {
    assert!(chunk_size > 0 && chunk_size <= MAX_CHUNK_SIZE);

    if data.is_empty() {
        return Vec::new();
    }

    data.chunks(chunk_size)
        .map(|chunk| {
            let id = ChunkId::from_data(chunk);
            (id, chunk.to_vec())
        })
        .collect()
}

/// Check if a value should be stored inline (< threshold) or via BlobStore.
pub fn should_inline(data: &[u8]) -> bool {
    data.len() < INLINE_THRESHOLD
}

/// Create a `BlobRef` from data by chunking and hashing.
///
/// Returns the `BlobRef` and the individual chunks for storage.
pub fn create_blob(data: &[u8]) -> (BlobRef, Vec<(ChunkId, Vec<u8>)>) {
    create_blob_with_chunk_size(data, DEFAULT_CHUNK_SIZE)
}

/// Create a `BlobRef` with custom chunk size.
pub fn create_blob_with_chunk_size(
    data: &[u8],
    chunk_size: usize,
) -> (BlobRef, Vec<(ChunkId, Vec<u8>)>) {
    let chunks = chunk_data(data, chunk_size);
    let chunk_ids: Vec<ChunkId> = chunks.iter().map(|(id, _)| *id).collect();
    let blob_ref = BlobRef::new(chunk_ids, data.len() as u64);
    (blob_ref, chunks)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_from_data() {
        let id = ChunkId::from_data(b"hello world");
        assert_eq!(id.as_bytes().len(), 32);
    }

    #[test]
    fn chunk_id_deterministic() {
        let id1 = ChunkId::from_data(b"same data");
        let id2 = ChunkId::from_data(b"same data");
        assert_eq!(id1, id2);
    }

    #[test]
    fn chunk_id_different_data() {
        let id1 = ChunkId::from_data(b"data a");
        let id2 = ChunkId::from_data(b"data b");
        assert_ne!(id1, id2);
    }

    #[test]
    fn chunk_id_hex_roundtrip() {
        let id = ChunkId::from_data(b"test");
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64);
        let restored = ChunkId::from_hex(&hex).expect("parse hex");
        assert_eq!(id, restored);
    }

    #[test]
    fn chunk_id_from_hex_invalid() {
        assert!(ChunkId::from_hex("short").is_none());
        assert!(ChunkId::from_hex(&"zz".repeat(32)).is_none());
    }

    #[test]
    fn chunk_id_display() {
        let id = ChunkId::from_data(b"test");
        let display = format!("{id}");
        assert_eq!(display.len(), 64);
        assert_eq!(display, id.to_hex());
    }

    #[test]
    fn chunk_data_small() {
        let data = b"small data";
        let chunks = chunk_data(data, DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1, data);
    }

    #[test]
    fn chunk_data_exact_boundary() {
        let data = vec![0xABu8; DEFAULT_CHUNK_SIZE];
        let chunks = chunk_data(&data, DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn chunk_data_multiple_chunks() {
        let data = vec![0xABu8; DEFAULT_CHUNK_SIZE * 3 + 100];
        let chunks = chunk_data(&data, DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].1.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks[3].1.len(), 100);
    }

    #[test]
    fn chunk_data_empty() {
        let chunks = chunk_data(b"", DEFAULT_CHUNK_SIZE);
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_data_dedup() {
        // Two identical chunks should have same ChunkId
        let data = vec![0xFFu8; DEFAULT_CHUNK_SIZE * 2];
        let chunks = chunk_data(&data, DEFAULT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].0, chunks[1].0); // same content = same hash
    }

    #[test]
    fn should_inline_small() {
        assert!(should_inline(&[0u8; 100]));
        assert!(should_inline(&[0u8; INLINE_THRESHOLD - 1]));
    }

    #[test]
    fn should_inline_large() {
        assert!(!should_inline(&[0u8; INLINE_THRESHOLD]));
        assert!(!should_inline(&[0u8; INLINE_THRESHOLD + 1]));
    }

    #[test]
    fn create_blob_roundtrip() {
        let data = vec![0xABu8; DEFAULT_CHUNK_SIZE * 2 + 500];
        let (blob_ref, chunks) = create_blob(&data);

        assert_eq!(blob_ref.total_size, data.len() as u64);
        assert_eq!(blob_ref.chunk_count(), 3);
        assert_eq!(chunks.len(), 3);

        // Reassemble
        let mut reassembled = Vec::new();
        for (_, chunk_data) in &chunks {
            reassembled.extend_from_slice(chunk_data);
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn blob_ref_msgpack_roundtrip() {
        let (blob_ref, _) = create_blob(b"test data for blob");
        let bytes = blob_ref.to_msgpack().expect("serialize");
        let restored = BlobRef::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(blob_ref, restored);
    }

    #[test]
    fn blob_key_encoding() {
        let id = ChunkId::from_data(b"test");
        let key = encode_blob_key(&id);
        assert!(key.starts_with(b"blob:"));
        // 5 + 64 = 69 bytes
        assert_eq!(key.len(), 69);
    }

    #[test]
    fn blobref_key_encoding() {
        let key = encode_blobref_key(NodeId::from_raw(42), 3);
        assert!(key.starts_with(b"blobref:"));
    }

    #[test]
    fn blobref_keys_sort_by_node_id() {
        let k1 = encode_blobref_key(NodeId::from_raw(1), 0);
        let k2 = encode_blobref_key(NodeId::from_raw(2), 0);
        assert!(k1 < k2);
    }

    #[test]
    fn custom_chunk_size() {
        let data = vec![0u8; 1000];
        let (blob_ref, chunks) = create_blob_with_chunk_size(&data, 300);
        assert_eq!(chunks.len(), 4); // 300+300+300+100
        assert_eq!(blob_ref.chunk_count(), 4);
        assert_eq!(blob_ref.total_size, 1000);
    }
}
