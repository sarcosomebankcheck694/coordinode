//! In-memory SSE token index for equality search.
//!
//! Stores `SearchToken → Vec<node_id>` mappings. On the server side,
//! this index is populated during writes and queried during encrypted
//! equality searches.
//!
//! In production, tokens are persisted in CoordiNode storage (sse: column family).
//! This module provides the in-memory index logic; storage integration
//! happens at the storage layer.
//!
//! Song-Wagner-Perrig 2000: simple lookup table, no complex data structures.

use std::collections::HashMap;

use super::field::SseError;
use super::token::SearchToken;

/// In-memory SSE token index.
///
/// Maps search tokens to sets of node IDs. Thread-safe access is the
/// caller's responsibility (use behind `RwLock` if shared).
///
/// In clustered mode, this index is replicated via Raft — all writes
/// go through the Raft proposal pipeline, and the index is rebuilt
/// from the Raft log on follower nodes.
pub struct EncryptedFieldIndex {
    /// Token → node IDs mapping.
    /// Multiple nodes can have the same encrypted value (same token).
    entries: HashMap<SearchToken, Vec<u64>>,
}

impl EncryptedFieldIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Insert a token → node_id mapping.
    ///
    /// Idempotent: if the (token, node_id) pair already exists, this is a no-op.
    pub fn insert(&mut self, token: SearchToken, node_id: u64) {
        let ids = self.entries.entry(token).or_default();
        if !ids.contains(&node_id) {
            ids.push(node_id);
        }
    }

    /// Remove a node_id from a token's mapping.
    ///
    /// Used when updating or deleting an encrypted field.
    /// If the token has no more node_ids, the entry is removed.
    pub fn remove(&mut self, token: &SearchToken, node_id: u64) {
        if let Some(ids) = self.entries.get_mut(token) {
            ids.retain(|&id| id != node_id);
            if ids.is_empty() {
                self.entries.remove(token);
            }
        }
    }

    /// Remove all entries for a node_id across all tokens.
    ///
    /// Used when deleting a node entirely.
    pub fn remove_node(&mut self, node_id: u64) {
        self.entries.retain(|_, ids| {
            ids.retain(|&id| id != node_id);
            !ids.is_empty()
        });
    }

    /// Search: find all node IDs that have a matching token.
    ///
    /// This is the server-side equality comparison:
    /// client sends `query_token = HMAC(search_key, query_value)`,
    /// server looks up `query_token` in the index.
    pub fn search(&self, query_token: &SearchToken) -> Result<Vec<u64>, SseError> {
        match self.entries.get(query_token) {
            Some(ids) => Ok(ids.clone()),
            None => Ok(Vec::new()),
        }
    }

    /// Number of distinct tokens in the index.
    pub fn num_tokens(&self) -> usize {
        self.entries.len()
    }

    /// Total number of (token, node_id) entries.
    pub fn num_entries(&self) -> usize {
        self.entries.values().map(|ids| ids.len()).sum()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all (token, node_ids) entries.
    ///
    /// Useful for persistence: serialize the index to sse: keyspace.
    pub fn iter(&self) -> impl Iterator<Item = (&SearchToken, &[u64])> {
        self.entries.iter().map(|(k, v)| (k, v.as_slice()))
    }
}

impl Default for EncryptedFieldIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::encrypted::keys::SearchKey;
    use crate::encrypted::token::generate_search_token;

    fn make_token(value: &[u8]) -> SearchToken {
        let key = SearchKey::from_bytes(&[1u8; 32]).unwrap();
        generate_search_token(value, &key)
    }

    #[test]
    fn insert_and_search() {
        let mut idx = EncryptedFieldIndex::new();
        let token = make_token(b"alice@example.com");

        idx.insert(token.clone(), 1);

        let results = idx.search(&token).unwrap();
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn search_not_found_returns_empty() {
        let idx = EncryptedFieldIndex::new();
        let token = make_token(b"nonexistent");

        let results = idx.search(&token).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_nodes_same_token() {
        let mut idx = EncryptedFieldIndex::new();
        let token = make_token(b"shared_value");

        idx.insert(token.clone(), 1);
        idx.insert(token.clone(), 2);
        idx.insert(token.clone(), 3);

        let results = idx.search(&token).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(results.contains(&3));
    }

    #[test]
    fn insert_idempotent() {
        let mut idx = EncryptedFieldIndex::new();
        let token = make_token(b"value");

        idx.insert(token.clone(), 1);
        idx.insert(token.clone(), 1); // duplicate

        let results = idx.search(&token).unwrap();
        assert_eq!(results, vec![1], "duplicate insert should be idempotent");
    }

    #[test]
    fn remove_specific_node() {
        let mut idx = EncryptedFieldIndex::new();
        let token = make_token(b"value");

        idx.insert(token.clone(), 1);
        idx.insert(token.clone(), 2);
        idx.remove(&token, 1);

        let results = idx.search(&token).unwrap();
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn remove_last_node_removes_token() {
        let mut idx = EncryptedFieldIndex::new();
        let token = make_token(b"value");

        idx.insert(token.clone(), 1);
        idx.remove(&token, 1);

        assert!(idx.is_empty());
        assert_eq!(idx.num_tokens(), 0);
    }

    #[test]
    fn remove_node_across_all_tokens() {
        let mut idx = EncryptedFieldIndex::new();
        let t1 = make_token(b"email");
        let t2 = make_token(b"name");

        idx.insert(t1.clone(), 1);
        idx.insert(t2.clone(), 1);
        idx.insert(t1.clone(), 2);

        idx.remove_node(1);

        assert!(idx.search(&t1).unwrap().contains(&2));
        assert!(!idx.search(&t1).unwrap().contains(&1));
        assert!(idx.search(&t2).unwrap().is_empty());
    }

    #[test]
    fn num_tokens_and_entries() {
        let mut idx = EncryptedFieldIndex::new();
        let t1 = make_token(b"a");
        let t2 = make_token(b"b");

        idx.insert(t1, 1);
        idx.insert(t2.clone(), 2);
        idx.insert(t2, 3);

        assert_eq!(idx.num_tokens(), 2);
        assert_eq!(idx.num_entries(), 3);
    }

    #[test]
    fn different_tokens_isolate_results() {
        let key1 = SearchKey::generate();
        let key2 = SearchKey::generate();

        let mut idx = EncryptedFieldIndex::new();

        // Same value but different keys → different tokens → isolated results
        let t1 = generate_search_token(b"secret", &key1);
        let t2 = generate_search_token(b"secret", &key2);

        idx.insert(t1.clone(), 1);
        idx.insert(t2.clone(), 2);

        assert_eq!(idx.search(&t1).unwrap(), vec![1]);
        assert_eq!(idx.search(&t2).unwrap(), vec![2]);
    }

    #[test]
    fn end_to_end_sse_flow() {
        // Full SSE flow: encrypt + tokenize + index + search + decrypt
        use crate::encrypted::field::{decrypt_field, encrypt_field};
        use crate::encrypted::keys::KeyPair;

        let pair = KeyPair::generate();
        let mut idx = EncryptedFieldIndex::new();

        // WRITE: client encrypts + generates token
        let plaintext = b"alice@example.com";
        let encrypted = encrypt_field(plaintext, &pair.field_key).unwrap();
        let token = generate_search_token(plaintext, &pair.search_key);
        idx.insert(token, 1);

        // Simulate storing encrypted value (in real system: CoordiNode storage)
        let stored_ciphertext = encrypted.as_bytes().to_vec();

        // SEARCH: client generates query token
        let query_token = generate_search_token(b"alice@example.com", &pair.search_key);
        let matching_ids = idx.search(&query_token).unwrap();
        assert_eq!(matching_ids, vec![1]);

        // Client decrypts the result
        let restored = crate::encrypted::field::EncryptedField::from_bytes(stored_ciphertext);
        let decrypted = decrypt_field(&restored, &pair.field_key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn end_to_end_no_match() {
        use crate::encrypted::keys::KeyPair;

        let pair = KeyPair::generate();
        let mut idx = EncryptedFieldIndex::new();

        // Store alice
        let token = generate_search_token(b"alice@example.com", &pair.search_key);
        idx.insert(token, 1);

        // Search for bob → no match
        let query = generate_search_token(b"bob@example.com", &pair.search_key);
        let results = idx.search(&query).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn iter_entries() {
        let mut idx = EncryptedFieldIndex::new();
        let t1 = make_token(b"a");
        let t2 = make_token(b"b");

        idx.insert(t1, 1);
        idx.insert(t2, 2);

        let entries: Vec<_> = idx.iter().collect();
        assert_eq!(entries.len(), 2);
    }
}
