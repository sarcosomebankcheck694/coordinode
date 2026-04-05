//! Field name interning: bidirectional mapping between property names and u32 IDs.
//!
//! Property graph storage repeats field names across millions of nodes.
//! Interning replaces string keys with varint-encoded integer IDs, achieving
//! ~80% reduction in property key storage.
//!
//! ## Varint encoding
//!
//! IDs 0-127 → 1 byte, IDs 128-16383 → 2 bytes, IDs 16384+ → up to 5 bytes.
//! Uses standard unsigned LEB128 (same as protobuf varint).

use std::collections::HashMap;

/// Bidirectional field name ↔ u32 ID mapping.
///
/// Thread-safe access requires external synchronization (e.g., `RwLock`).
/// IDs are assigned monotonically starting from 1 (0 is reserved).
#[derive(Debug, Clone)]
pub struct FieldInterner {
    name_to_id: HashMap<String, u32>,
    id_to_name: Vec<String>,
    next_id: u32,
}

impl FieldInterner {
    /// Reserved ID representing "no field" / uninitialized.
    pub const RESERVED_ID: u32 = 0;

    /// Create a new empty interner.
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            id_to_name: vec![String::new()], // index 0 = reserved
            next_id: 1,
        }
    }

    /// Intern a field name, returning its ID.
    ///
    /// If the name was already interned, returns the existing ID.
    /// Otherwise assigns a new monotonically increasing ID.
    pub fn intern(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.name_to_id.insert(name.to_string(), id);
        self.id_to_name.push(name.to_string());
        id
    }

    /// Look up an ID without interning. Returns `None` if not interned.
    pub fn lookup(&self, name: &str) -> Option<u32> {
        self.name_to_id.get(name).copied()
    }

    /// Resolve an ID back to its field name.
    ///
    /// Returns `None` if the ID is out of range or is the reserved ID (0).
    pub fn resolve(&self, id: u32) -> Option<&str> {
        if id == Self::RESERVED_ID {
            return None;
        }
        self.id_to_name.get(id as usize).map(|s| s.as_str())
    }

    /// Number of interned field names (excluding reserved ID 0).
    pub fn len(&self) -> usize {
        self.name_to_id.len()
    }

    /// Whether the interner has no entries.
    pub fn is_empty(&self) -> bool {
        self.name_to_id.is_empty()
    }

    /// Iterator over all (name, id) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, u32)> {
        self.name_to_id
            .iter()
            .map(|(name, &id)| (name.as_str(), id))
    }

    /// Serialize the interner to bytes (for persistence in schema: partition).
    ///
    /// Format: count (u32 LE) followed by (id: u32 LE, name_len: u16 LE, name: bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let count = self.name_to_id.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());

        for (name, &id) in &self.name_to_id {
            buf.extend_from_slice(&id.to_le_bytes());
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            buf.extend_from_slice(&name_len.to_le_bytes());
            buf.extend_from_slice(name_bytes);
        }
        buf
    }

    /// Deserialize an interner from bytes.
    ///
    /// Returns `None` if the data is malformed.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes(data[..4].try_into().ok()?) as usize;
        let mut offset = 4;
        let mut interner = Self::new();

        for _ in 0..count {
            if offset + 6 > data.len() {
                return None;
            }
            let id = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?);
            offset += 4;
            let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().ok()?) as usize;
            offset += 2;
            if offset + name_len > data.len() {
                return None;
            }
            let name = std::str::from_utf8(&data[offset..offset + name_len]).ok()?;
            offset += name_len;

            interner.name_to_id.insert(name.to_string(), id);
            // Ensure id_to_name is large enough
            while interner.id_to_name.len() <= id as usize {
                interner.id_to_name.push(String::new());
            }
            interner.id_to_name[id as usize] = name.to_string();
            if id >= interner.next_id {
                interner.next_id = id + 1;
            }
        }

        Some(interner)
    }
}

impl Default for FieldInterner {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a u32 as unsigned LEB128 varint.
///
/// Returns number of bytes written (1-5).
pub fn encode_varint(value: u32, buf: &mut [u8; 5]) -> usize {
    let mut v = value;
    let mut i = 0;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf[i] = byte;
            return i + 1;
        }
        buf[i] = byte | 0x80;
        i += 1;
    }
}

/// Decode an unsigned LEB128 varint from bytes.
///
/// Returns `(value, bytes_consumed)` or `None` if malformed.
pub fn decode_varint(data: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 35 {
            return None; // overflow
        }
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None // incomplete
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn intern_new_field() {
        let mut interner = FieldInterner::new();
        let id = interner.intern("name");
        assert_eq!(id, 1);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn intern_returns_same_id() {
        let mut interner = FieldInterner::new();
        let id1 = interner.intern("name");
        let id2 = interner.intern("name");
        assert_eq!(id1, id2);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn intern_multiple_fields() {
        let mut interner = FieldInterner::new();
        let id_name = interner.intern("name");
        let id_age = interner.intern("age");
        let id_email = interner.intern("email");

        assert_eq!(id_name, 1);
        assert_eq!(id_age, 2);
        assert_eq!(id_email, 3);
        assert_eq!(interner.len(), 3);
    }

    #[test]
    fn resolve_existing_id() {
        let mut interner = FieldInterner::new();
        let id = interner.intern("name");
        assert_eq!(interner.resolve(id), Some("name"));
    }

    #[test]
    fn resolve_nonexistent_id() {
        let interner = FieldInterner::new();
        assert_eq!(interner.resolve(999), None);
    }

    #[test]
    fn resolve_reserved_id() {
        let interner = FieldInterner::new();
        assert_eq!(interner.resolve(FieldInterner::RESERVED_ID), None);
    }

    #[test]
    fn lookup_existing() {
        let mut interner = FieldInterner::new();
        interner.intern("name");
        assert_eq!(interner.lookup("name"), Some(1));
    }

    #[test]
    fn lookup_missing() {
        let interner = FieldInterner::new();
        assert_eq!(interner.lookup("name"), None);
    }

    #[test]
    fn empty_interner() {
        let interner = FieldInterner::new();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let mut interner = FieldInterner::new();
        interner.intern("name");
        interner.intern("age");
        interner.intern("email");

        let bytes = interner.to_bytes();
        let restored = FieldInterner::from_bytes(&bytes).expect("deserialize failed");

        assert_eq!(restored.len(), 3);
        assert_eq!(restored.lookup("name"), Some(1));
        assert_eq!(restored.lookup("age"), Some(2));
        assert_eq!(restored.lookup("email"), Some(3));
        assert_eq!(restored.resolve(1), Some("name"));
        assert_eq!(restored.resolve(2), Some("age"));
        assert_eq!(restored.resolve(3), Some("email"));
    }

    #[test]
    fn serialize_empty() {
        let interner = FieldInterner::new();
        let bytes = interner.to_bytes();
        let restored = FieldInterner::from_bytes(&bytes).expect("deserialize failed");
        assert!(restored.is_empty());
    }

    #[test]
    fn deserialize_new_ids_after_restore() {
        let mut interner = FieldInterner::new();
        interner.intern("a");
        interner.intern("b");

        let bytes = interner.to_bytes();
        let mut restored = FieldInterner::from_bytes(&bytes).expect("deserialize failed");

        // New field after restore should get next ID
        let id_c = restored.intern("c");
        assert_eq!(id_c, 3);
    }

    #[test]
    fn deserialize_malformed_returns_none() {
        assert!(FieldInterner::from_bytes(&[]).is_none());
        assert!(FieldInterner::from_bytes(&[0xFF]).is_none());
        // Count says 1 entry but no data follows
        assert!(FieldInterner::from_bytes(&[1, 0, 0, 0]).is_none());
    }

    // Varint tests

    #[test]
    fn varint_single_byte() {
        let mut buf = [0u8; 5];
        let len = encode_varint(0, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);

        let len = encode_varint(127, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 127);
    }

    #[test]
    fn varint_two_bytes() {
        let mut buf = [0u8; 5];
        let len = encode_varint(128, &mut buf);
        assert_eq!(len, 2);

        let (val, consumed) = decode_varint(&buf[..len]).expect("decode failed");
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn varint_roundtrip() {
        let test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 65535, u32::MAX];
        for &v in &test_values {
            let mut buf = [0u8; 5];
            let len = encode_varint(v, &mut buf);
            let (decoded, consumed) = decode_varint(&buf[..len]).expect("decode failed");
            assert_eq!(decoded, v, "roundtrip failed for {v}");
            assert_eq!(consumed, len);
        }
    }

    #[test]
    fn varint_size_boundaries() {
        let mut buf = [0u8; 5];
        // 1 byte: 0-127
        assert_eq!(encode_varint(127, &mut buf), 1);
        // 2 bytes: 128-16383
        assert_eq!(encode_varint(128, &mut buf), 2);
        assert_eq!(encode_varint(16383, &mut buf), 2);
        // 3 bytes: 16384-2097151
        assert_eq!(encode_varint(16384, &mut buf), 3);
        // 5 bytes: u32::MAX
        assert_eq!(encode_varint(u32::MAX, &mut buf), 5);
    }

    #[test]
    fn varint_decode_empty() {
        assert!(decode_varint(&[]).is_none());
    }

    #[test]
    fn varint_decode_incomplete() {
        // High bit set but no continuation byte
        assert!(decode_varint(&[0x80]).is_none());
    }

    #[test]
    fn iter_fields() {
        let mut interner = FieldInterner::new();
        interner.intern("a");
        interner.intern("b");
        interner.intern("c");

        let mut pairs: Vec<_> = interner.iter().collect();
        pairs.sort_by_key(|&(_, id)| id);
        assert_eq!(pairs, vec![("a", 1), ("b", 2), ("c", 3)]);
    }
}
