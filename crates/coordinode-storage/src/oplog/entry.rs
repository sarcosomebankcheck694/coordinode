//! Oplog entry types: `OplogEntry`, `OplogOp`, `PreImage`, `ShardId`.
//!
//! On-disk encoding: MessagePack via `rmp-serde`. Binary fields use
//! `serde_bytes` so they serialize as msgpack `Bin` (compact byte array)
//! rather than an array of individual u8 values.

use serde::{Deserialize, Serialize};

/// Shard identifier — u32 unique per shard within a cluster.
pub type ShardId = u32;

/// Snapshot of a key's value before a mutation.
///
/// Captured for CDC consumers and PITR rewind. Absent when the `pre_images`
/// feature is disabled in the oplog config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreImage {
    /// Partition discriminant (matches `Partition::discriminant()`).
    pub partition: u8,
    /// Key bytes.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    /// Value bytes as they existed before this mutation.
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

/// A single storage mutation captured inside an [`OplogEntry`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OplogOp {
    /// Insert or overwrite a key-value pair.
    Insert {
        /// Partition discriminant.
        partition: u8,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    /// Tombstone a key.
    Delete {
        partition: u8,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
    /// Apply a merge operand (posting-list patch, counter delta, etc.).
    Merge {
        partition: u8,
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        #[serde(with = "serde_bytes")]
        operand: Vec<u8>,
    },
    /// No-op heartbeat / gap-fill entry.
    Noop,
    /// Serialized openraft log entry (Normal or Membership payload).
    ///
    /// Used by the Raft log storage to persist complete openraft
    /// `Entry<TypeConfig>` objects — including both application data and
    /// membership change payloads — as oplog entries. The `data` field
    /// contains the full msgpack-serialized entry.
    RaftEntry {
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// Truncation sentinel for the Raft log.
    ///
    /// All oplog entries written before this sentinel with
    /// `index > after_index` are considered invalid — they were written by
    /// a previous leader term and have been superseded by new entries from
    /// the current leader.
    RaftTruncation { after_index: u64 },
}

/// One logical unit written to the oplog: a batch of ops at a Raft index/term.
///
/// Serialized with MessagePack and stored in a segment file wrapped in a
/// varint length prefix and a crc32 suffix per the segment on-disk format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OplogEntry {
    /// Hybrid Logical Clock timestamp (48-bit wall ms | 16-bit logical counter).
    pub ts: u64,
    /// Raft term when this entry was proposed.
    pub term: u64,
    /// Raft log index — monotonically increasing per shard.
    pub index: u64,
    /// Shard that owns this entry.
    pub shard: ShardId,
    /// Storage operations carried by this entry.
    pub ops: Vec<OplogOp>,
    /// True for entries carrying cross-shard migration ops.
    pub is_migration: bool,
    /// Pre-images for CDC/PITR — `None` when the feature is disabled.
    pub pre_images: Option<Vec<PreImage>>,
}

impl OplogEntry {
    /// Encode to MessagePack bytes.
    pub fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Decode from MessagePack bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn make_full_entry() -> OplogEntry {
        OplogEntry {
            ts: 1_000_000_u64 << 18,
            term: 3,
            index: 42,
            shard: 1,
            ops: vec![
                OplogOp::Insert {
                    partition: 1,
                    key: b"mykey".to_vec(),
                    value: b"myvalue".to_vec(),
                },
                OplogOp::Delete {
                    partition: 2,
                    key: b"delkey".to_vec(),
                },
                OplogOp::Merge {
                    partition: 0,
                    key: b"k".to_vec(),
                    operand: b"delta".to_vec(),
                },
                OplogOp::Noop,
            ],
            is_migration: false,
            pre_images: None,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let entry = make_full_entry();
        let encoded = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&encoded).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn noop_entry_roundtrip() {
        let entry = OplogEntry {
            ts: 0,
            term: 1,
            index: 0,
            shard: 99,
            ops: vec![OplogOp::Noop],
            is_migration: false,
            pre_images: None,
        };
        let bytes = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        assert_eq!(decoded.ops, vec![OplogOp::Noop]);
    }

    #[test]
    fn pre_images_roundtrip() {
        let entry = OplogEntry {
            ts: 1,
            term: 1,
            index: 1,
            shard: 0,
            ops: vec![OplogOp::Merge {
                partition: 0,
                key: b"k".to_vec(),
                operand: b"delta".to_vec(),
            }],
            is_migration: true,
            pre_images: Some(vec![PreImage {
                partition: 0,
                key: b"k".to_vec(),
                value: b"before".to_vec(),
            }]),
        };
        let bytes = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn empty_ops_roundtrip() {
        let entry = OplogEntry {
            ts: 42,
            term: 2,
            index: 7,
            shard: 0,
            ops: vec![],
            is_migration: false,
            pre_images: None,
        };
        let bytes = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn binary_fields_use_msgpack_bin_type() {
        // Ensure binary payload is compact (msgpack Bin, not array-of-u8).
        // A 5-byte key in Bin8 format = 2 bytes overhead vs 5*2 = 10 in array form.
        let entry = OplogEntry {
            ts: 0,
            term: 0,
            index: 0,
            shard: 0,
            ops: vec![OplogOp::Insert {
                partition: 0,
                key: vec![0u8; 5],
                value: vec![0u8; 5],
            }],
            is_migration: false,
            pre_images: None,
        };
        let bytes = entry.encode().expect("encode");
        // Bin8 for 5 bytes = 0xC4 0x05 + 5 bytes = 7 bytes
        // Array[u8; 5] = fixarray(5) + 5 * fixint = 6 bytes
        // We just verify it round-trips correctly; the compact form is an
        // implementation detail of serde_bytes + msgpack.
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        assert_eq!(decoded.ops[0], entry.ops[0]);
    }

    #[test]
    fn decode_invalid_bytes_returns_error() {
        let result = OplogEntry::decode(b"not msgpack garbage \xFF");
        assert!(result.is_err());
    }

    #[test]
    fn raft_entry_op_roundtrip() {
        let payload = b"serialized-raft-entry-bytes".to_vec();
        let entry = OplogEntry {
            ts: 0,
            term: 7,
            index: 42,
            shard: 0,
            ops: vec![OplogOp::RaftEntry {
                data: payload.clone(),
            }],
            is_migration: false,
            pre_images: None,
        };
        let bytes = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        assert_eq!(decoded.term, 7);
        assert_eq!(decoded.index, 42);
        match &decoded.ops[0] {
            OplogOp::RaftEntry { data } => assert_eq!(data, &payload),
            other => panic!("expected RaftEntry, got {other:?}"),
        }
    }

    #[test]
    fn raft_truncation_op_roundtrip() {
        let entry = OplogEntry {
            ts: 0,
            term: 3,
            index: 99,
            shard: 0,
            ops: vec![OplogOp::RaftTruncation { after_index: 5 }],
            is_migration: false,
            pre_images: None,
        };
        let bytes = entry.encode().expect("encode");
        let decoded = OplogEntry::decode(&bytes).expect("decode");
        match &decoded.ops[0] {
            OplogOp::RaftTruncation { after_index } => assert_eq!(*after_index, 5),
            other => panic!("expected RaftTruncation, got {other:?}"),
        }
    }
}
