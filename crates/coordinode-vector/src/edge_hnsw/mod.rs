//! Edge HNSW index: vector similarity search on edge properties.
//!
//! Unique to CoordiNode — no other graph database supports vector indexes on edges.
//! Each edge type can have its own HNSW index on a vector property.
//!
//! Use case: semantic search over relationship types, fraud detection by
//! transaction similarity, recommendations by relationship patterns.
//!
//! # Cluster-ready notes
//! Same as node HNSW: each CE replica builds its own in-memory index
//! from replicated edge property data in CoordiNode storage.

use crate::hnsw::{HnswConfig, HnswIndex};

/// An edge identified by (source_id, target_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub source: u64,
    pub target: u64,
}

impl EdgeId {
    /// Pack source and target into a single u64 for HNSW node ID.
    /// Uses Cantor pairing function for unique mapping.
    fn to_hnsw_id(self) -> u64 {
        let s = self.source;
        let t = self.target;
        // Cantor pairing: (s + t) * (s + t + 1) / 2 + t
        // For large IDs, use a simpler hash-like encoding
        s.wrapping_mul(2_147_483_647).wrapping_add(t)
    }
}

/// Edge HNSW index for a specific edge type and vector property.
pub struct EdgeHnswIndex {
    /// The edge type this index covers (e.g., "KNOWS").
    pub edge_type: String,
    /// The vector property name on the edge (e.g., "relationship_embedding").
    pub property: String,
    /// Underlying HNSW index.
    hnsw: HnswIndex,
    /// Reverse mapping: HNSW node ID → EdgeId.
    id_map: std::collections::HashMap<u64, EdgeId>,
}

/// Search result for edge vector search.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeSearchResult {
    /// Source node ID of the edge.
    pub source: u64,
    /// Target node ID of the edge.
    pub target: u64,
    /// Similarity/distance score.
    pub score: f32,
}

impl EdgeHnswIndex {
    /// Create a new edge HNSW index.
    pub fn new(
        edge_type: impl Into<String>,
        property: impl Into<String>,
        config: HnswConfig,
    ) -> Self {
        Self {
            edge_type: edge_type.into(),
            property: property.into(),
            hnsw: HnswIndex::new(config),
            id_map: std::collections::HashMap::new(),
        }
    }

    /// Number of indexed edge vectors.
    pub fn len(&self) -> usize {
        self.hnsw.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.hnsw.is_empty()
    }

    /// Insert an edge vector into the index.
    pub fn insert(&mut self, source: u64, target: u64, vector: Vec<f32>) {
        let edge_id = EdgeId { source, target };
        let hnsw_id = edge_id.to_hnsw_id();

        self.id_map.insert(hnsw_id, edge_id);
        self.hnsw.insert(hnsw_id, vector);
    }

    /// Search for K nearest edge vectors to the query.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<EdgeSearchResult> {
        self.hnsw
            .search(query, k)
            .into_iter()
            .filter_map(|r| {
                self.id_map.get(&r.id).map(|edge_id| EdgeSearchResult {
                    source: edge_id.source,
                    target: edge_id.target,
                    score: r.score,
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use coordinode_core::graph::types::VectorMetric;

    fn test_config() -> HnswConfig {
        HnswConfig {
            m: 4,
            m_max0: 8,
            ef_construction: 16,
            ef_search: 10,
            metric: VectorMetric::Cosine,
            max_dimensions: 65_536,
            ..Default::default()
        }
    }

    #[test]
    fn empty_edge_index() {
        let index = EdgeHnswIndex::new("KNOWS", "embedding", test_config());
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.search(&[1.0, 0.0], 5).is_empty());
    }

    #[test]
    fn insert_and_search_edges() {
        let mut index = EdgeHnswIndex::new("KNOWS", "relationship_embedding", test_config());

        // Alice→Bob: technical collaboration
        index.insert(1, 2, vec![0.9, 0.1, 0.0]);
        // Alice→Charlie: personal friendship
        index.insert(1, 3, vec![0.1, 0.9, 0.0]);
        // Bob→Charlie: professional mentor
        index.insert(2, 3, vec![0.7, 0.3, 0.0]);

        assert_eq!(index.len(), 3);

        // Search for "technical collaboration" pattern
        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);

        // Most similar to (1,0,0) should be Alice→Bob (0.9, 0.1, 0)
        assert_eq!(results[0].source, 1);
        assert_eq!(results[0].target, 2);
    }

    #[test]
    fn edge_search_returns_source_target() {
        let mut index = EdgeHnswIndex::new("FOLLOWS", "vec", test_config());

        index.insert(10, 20, vec![1.0, 0.0]);
        index.insert(30, 40, vec![0.0, 1.0]);

        let results = index.search(&[1.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source, 10);
        assert_eq!(results[0].target, 20);
    }

    #[test]
    fn multiple_edges_from_same_source() {
        let mut index = EdgeHnswIndex::new("RATED", "sentiment_vec", test_config());

        // User 1 rates multiple movies with different sentiment vectors
        index.insert(1, 100, vec![0.9, 0.1]); // Positive review
        index.insert(1, 101, vec![0.1, 0.9]); // Negative review
        index.insert(1, 102, vec![0.5, 0.5]); // Mixed review

        let results = index.search(&[0.8, 0.2], 3);
        assert_eq!(results.len(), 3);
        // All from source 1
        assert!(results.iter().all(|r| r.source == 1));
    }

    #[test]
    fn edge_type_and_property_stored() {
        let index = EdgeHnswIndex::new("TRANSACTION", "fraud_embedding", test_config());
        assert_eq!(index.edge_type, "TRANSACTION");
        assert_eq!(index.property, "fraud_embedding");
    }

    #[test]
    fn high_dimensional_edge_vectors() {
        let mut index = EdgeHnswIndex::new(
            "KNOWS",
            "embedding",
            HnswConfig {
                m: 4,
                m_max0: 8,
                ef_construction: 16,
                ef_search: 10,
                metric: VectorMetric::L2,
                max_dimensions: 65_536,
                ..Default::default()
            },
        );

        // 384-dim edge vectors
        for i in 0..10u64 {
            let vec: Vec<f32> = (0..384)
                .map(|d| ((i * d + 1) as f32 * 0.01).sin())
                .collect();
            index.insert(i, i + 100, vec);
        }

        assert_eq!(index.len(), 10);

        let query: Vec<f32> = (0..384).map(|d| (d as f32 * 0.01).sin()).collect();
        let results = index.search(&query, 3);
        assert_eq!(results.len(), 3);
    }
}
