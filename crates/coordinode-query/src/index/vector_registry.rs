//! Vector index registry: tracks active HNSW indexes for vector search acceleration.
//!
//! Holds in-memory HNSW index instances keyed by (label, property).
//! Indexes are populated from stored vectors on Database::open() and
//! maintained incrementally on node create/update/delete.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use coordinode_core::graph::node::NodeId;
use coordinode_vector::hnsw::{HnswConfig, HnswIndex, SearchResult};
use coordinode_vector::VectorLoader;

use super::definition::IndexDefinition;

/// Key for vector index lookup: (label, property).
type VectorIndexKey = (String, String);

/// Thread-safe handle to an HNSW index. Readers can search concurrently;
/// writers (insert/delete) acquire exclusive access.
pub type HnswHandle = Arc<RwLock<HnswIndex>>;

/// Registry of active HNSW vector indexes.
///
/// Holds live HNSW graph structures in memory. Each index corresponds
/// to an `IndexDefinition` with `index_type == Hnsw` persisted in the
/// schema partition. The HNSW graph itself is rebuilt from stored vectors
/// on startup and maintained incrementally during writes.
pub struct VectorIndexRegistry {
    /// Active HNSW indexes: (label, property) → live HNSW graph.
    indexes: HashMap<VectorIndexKey, HnswHandle>,
    /// Index definitions keyed by (label, property) for metadata lookup.
    definitions: HashMap<VectorIndexKey, IndexDefinition>,
}

impl VectorIndexRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            definitions: HashMap::new(),
        }
    }

    /// Register a new vector index with an empty HNSW graph.
    ///
    /// Creates the HNSW structure from the index definition's config.
    /// The caller is responsible for populating the index with existing
    /// vectors (see `populate_from_storage`).
    pub fn register(&mut self, def: IndexDefinition) {
        let Some(config) = def.vector_config.as_ref() else {
            tracing::error!(
                "register called with non-vector IndexDefinition: {}",
                def.name
            );
            return;
        };

        let hnsw_config = HnswConfig {
            m: config.m,
            m_max0: config.m * 2,
            ef_construction: config.ef_construction,
            ef_search: 200,
            metric: config.metric,
            max_dimensions: config.dimensions,
            quantization: config.quantization,
            rerank_candidates: 100,
            calibration_threshold: 1000,
            offload_vectors: config.offload_vectors,
            property_name: def.property().to_string(),
        };

        let hnsw = HnswIndex::new(hnsw_config);
        let key = (def.label.clone(), def.property().to_string());
        self.indexes
            .insert(key.clone(), Arc::new(RwLock::new(hnsw)));
        self.definitions.insert(key, def);
    }

    /// Register an index with a pre-built HNSW graph (e.g. after rebuild).
    pub fn register_with_index(&mut self, def: IndexDefinition, hnsw: HnswIndex) {
        let key = (def.label.clone(), def.property().to_string());
        self.indexes
            .insert(key.clone(), Arc::new(RwLock::new(hnsw)));
        self.definitions.insert(key, def);
    }

    /// Unregister a vector index.
    pub fn unregister(&mut self, label: &str, property: &str) {
        let key = (label.to_string(), property.to_string());
        self.indexes.remove(&key);
        self.definitions.remove(&key);
    }

    /// Get a handle to the HNSW index for a (label, property) pair.
    pub fn get(&self, label: &str, property: &str) -> Option<&HnswHandle> {
        self.indexes.get(&(label.to_string(), property.to_string()))
    }

    /// Get the index definition for a (label, property) pair.
    pub fn get_definition(&self, label: &str, property: &str) -> Option<&IndexDefinition> {
        self.definitions
            .get(&(label.to_string(), property.to_string()))
    }

    /// List all registered vector indexes for a given label.
    pub fn indexes_for_label(&self, label: &str) -> Vec<(&str, &HnswHandle)> {
        self.indexes
            .iter()
            .filter(|((l, _), _)| l == label)
            .map(|((_, p), h)| (p.as_str(), h))
            .collect()
    }

    /// Check if a vector index exists for a (label, property).
    pub fn has_index(&self, label: &str, property: &str) -> bool {
        self.indexes
            .contains_key(&(label.to_string(), property.to_string()))
    }

    /// Number of registered vector indexes.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Iterate over all registered index definitions.
    ///
    /// Used during startup to rebuild HNSW graphs from stored vectors.
    pub fn definitions(&self) -> impl Iterator<Item = &IndexDefinition> {
        self.definitions.values()
    }

    /// Insert a vector into all applicable HNSW indexes for a node.
    ///
    /// Called on node creation or vector property update.
    pub fn on_vector_written(&self, label: &str, node_id: NodeId, property: &str, vector: &[f32]) {
        if let Some(handle) = self.get(label, property) {
            if let Ok(mut hnsw) = handle.write() {
                hnsw.insert(node_id.as_raw(), vector.to_vec());
            }
        }
    }

    /// Remove a vector from all applicable HNSW indexes for a node.
    ///
    /// Called on node deletion or vector property removal.
    /// HNSW does not support true deletion — we rely on the MVCC
    /// visibility filter to exclude deleted nodes from search results.
    /// The vector remains in the graph to avoid fragmentation.
    /// Periodic rebuild (>50% tombstones) is tracked separately.
    pub fn on_vector_deleted(&self, _label: &str, _node_id: NodeId, _property: &str) {
        // HNSW graph deletion is handled via MVCC post-filter visibility.
        // Physical removal would fragment the graph. Tracked as future
        // optimization: rebuild when tombstone ratio exceeds threshold.
    }

    /// Search the HNSW index for a (label, property) pair.
    ///
    /// Returns `None` if no index exists, or the search results if found.
    pub fn search(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
    ) -> Option<Vec<SearchResult>> {
        let handle = self.get(label, property)?;
        let hnsw = handle.read().ok()?;
        Some(hnsw.search(query, k))
    }

    /// Search with optional VectorLoader for disk-backed f32 reranking.
    ///
    /// When the HNSW index has offloaded f32 vectors, the loader provides
    /// them on-demand for exact reranking. Falls back to in-memory search
    /// when no loader is provided or vectors are not offloaded.
    pub fn search_with_loader(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
        loader: Option<&dyn VectorLoader>,
    ) -> Option<Vec<SearchResult>> {
        let handle = self.get(label, property)?;
        let hnsw = handle.read().ok()?;
        if let Some(loader) = loader {
            Some(hnsw.search_with_loader(query, k, loader))
        } else {
            Some(hnsw.search(query, k))
        }
    }

    /// Bulk-insert multiple vectors into a specific HNSW index.
    ///
    /// Used during index rebuild (on Database::open or CREATE INDEX).
    /// The caller is responsible for scanning stored nodes and extracting
    /// vectors — this method just does the HNSW insertions.
    pub fn bulk_insert(
        &self,
        label: &str,
        property: &str,
        vectors: impl Iterator<Item = (u64, Vec<f32>)>,
    ) -> usize {
        let handle = match self.get(label, property) {
            Some(h) => h.clone(),
            None => return 0,
        };

        let mut count = 0;
        if let Ok(mut hnsw) = handle.write() {
            for (id, vec) in vectors {
                hnsw.insert(id, vec);
                count += 1;
            }
        }
        count
    }
}

impl Default for VectorIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::index::VectorIndexConfig;
    use coordinode_core::graph::types::VectorMetric;

    fn test_config() -> VectorIndexConfig {
        VectorIndexConfig {
            dimensions: 3,
            metric: VectorMetric::Cosine,
            m: 16,
            ef_construction: 200,
            quantization: false,
            offload_vectors: false,
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut reg = VectorIndexRegistry::new();
        let def = IndexDefinition::hnsw("movie_embedding", "Movie", "embedding", test_config());

        reg.register(def);

        assert!(reg.has_index("Movie", "embedding"));
        assert!(!reg.has_index("Movie", "title"));
        assert!(!reg.has_index("User", "embedding"));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn search_empty_index_returns_empty() {
        let mut reg = VectorIndexRegistry::new();
        let def = IndexDefinition::hnsw("movie_embedding", "Movie", "embedding", test_config());
        reg.register(def);

        let results = reg.search("Movie", "embedding", &[1.0, 0.0, 0.0], 10);
        assert!(results.is_some());
        assert!(results.unwrap().is_empty());
    }

    #[test]
    fn insert_and_search() {
        let mut reg = VectorIndexRegistry::new();
        let def = IndexDefinition::hnsw("movie_embedding", "Movie", "embedding", test_config());
        reg.register(def);

        // Insert vectors
        reg.on_vector_written("Movie", NodeId::from_raw(1), "embedding", &[1.0, 0.0, 0.0]);
        reg.on_vector_written("Movie", NodeId::from_raw(2), "embedding", &[0.0, 1.0, 0.0]);
        reg.on_vector_written("Movie", NodeId::from_raw(3), "embedding", &[0.9, 0.1, 0.0]);

        // Search for vector closest to [1.0, 0.0, 0.0]
        let results = reg
            .search("Movie", "embedding", &[1.0, 0.0, 0.0], 2)
            .expect("search should return results");

        assert_eq!(results.len(), 2);
        // Node 1 should be closest (exact match), then node 3
        assert_eq!(results[0].id, 1);
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn indexes_for_label() {
        let mut reg = VectorIndexRegistry::new();
        reg.register(IndexDefinition::hnsw(
            "movie_embed",
            "Movie",
            "embedding",
            test_config(),
        ));
        reg.register(IndexDefinition::hnsw(
            "movie_thumb",
            "Movie",
            "thumbnail_vec",
            test_config(),
        ));
        reg.register(IndexDefinition::hnsw(
            "user_embed",
            "User",
            "embedding",
            test_config(),
        ));

        let movie_idxs = reg.indexes_for_label("Movie");
        assert_eq!(movie_idxs.len(), 2);

        let user_idxs = reg.indexes_for_label("User");
        assert_eq!(user_idxs.len(), 1);
    }

    #[test]
    fn unregister() {
        let mut reg = VectorIndexRegistry::new();
        reg.register(IndexDefinition::hnsw(
            "movie_embed",
            "Movie",
            "embedding",
            test_config(),
        ));
        assert!(reg.has_index("Movie", "embedding"));

        reg.unregister("Movie", "embedding");
        assert!(!reg.has_index("Movie", "embedding"));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn no_index_search_returns_none() {
        let reg = VectorIndexRegistry::new();
        let results = reg.search("Movie", "embedding", &[1.0, 0.0, 0.0], 10);
        assert!(results.is_none());
    }
}
