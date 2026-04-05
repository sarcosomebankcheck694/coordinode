//! Flat (brute-force) vector index: exact nearest neighbor search.
//!
//! For small datasets (<100K vectors). Computes distance to every vector
//! and returns exact top-K results. No approximation, 100% recall.
//!
//! Automatically selected by the query planner when the dataset is small
//! enough that brute-force is faster than HNSW index overhead.

use coordinode_core::graph::types::VectorMetric;

use crate::metrics;

/// Flat vector index: stores all vectors and computes exact distances.
pub struct FlatIndex {
    /// Distance metric.
    metric: VectorMetric,
    /// Stored vectors: (id, vector).
    vectors: Vec<(u64, Vec<f32>)>,
}

/// Search result with node ID and distance/similarity score.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatSearchResult {
    pub id: u64,
    pub score: f32,
}

impl FlatIndex {
    /// Create a new empty flat index.
    pub fn new(metric: VectorMetric) -> Self {
        Self {
            metric,
            vectors: Vec::new(),
        }
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Insert a vector. Duplicates (same ID) are ignored.
    pub fn insert(&mut self, id: u64, vector: Vec<f32>) {
        if self
            .vectors
            .iter()
            .any(|(existing_id, _)| *existing_id == id)
        {
            return;
        }
        self.vectors.push((id, vector));
    }

    /// Remove a vector by ID.
    pub fn remove(&mut self, id: u64) -> bool {
        let before = self.vectors.len();
        self.vectors.retain(|(vid, _)| *vid != id);
        self.vectors.len() < before
    }

    /// Search for K exact nearest neighbors.
    ///
    /// Returns results sorted by distance (nearest first for L2/L1,
    /// highest similarity first for Cosine/DotProduct).
    pub fn search(&self, query: &[f32], k: usize) -> Vec<FlatSearchResult> {
        if self.is_empty() || k == 0 {
            return Vec::new();
        }

        let mut scored: Vec<FlatSearchResult> = self
            .vectors
            .iter()
            .map(|(id, vec)| {
                let score = self.compute_distance(query, vec);
                FlatSearchResult { id: *id, score }
            })
            .collect();

        // Sort by distance (ascending for L2/L1, "ascending" for transformed cosine/dot)
        scored.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        scored.truncate(k);
        scored
    }

    /// Compute distance using the configured metric.
    /// Returns a value where lower = more similar (consistent with HNSW).
    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            VectorMetric::Cosine => 1.0 - metrics::cosine_similarity(a, b),
            VectorMetric::L2 => metrics::euclidean_distance_squared(a, b),
            VectorMetric::DotProduct => -metrics::dot_product(a, b),
            VectorMetric::L1 => metrics::manhattan_distance(a, b),
        }
    }

    /// Check if brute-force is recommended for this dataset size.
    /// Returns true for <100K vectors (flat is faster than HNSW overhead).
    pub fn is_recommended_size(&self) -> bool {
        self.vectors.len() < 100_000
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_index() {
        let index = FlatIndex::new(VectorMetric::L2);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.search(&[1.0, 0.0], 5).is_empty());
    }

    #[test]
    fn insert_and_search_l2() {
        let mut index = FlatIndex::new(VectorMetric::L2);

        index.insert(1, vec![0.0, 0.0]);
        index.insert(2, vec![1.0, 0.0]);
        index.insert(3, vec![0.0, 1.0]);
        index.insert(4, vec![10.0, 10.0]);

        let results = index.search(&[0.1, 0.1], 3);
        assert_eq!(results.len(), 3);
        // Nearest to (0.1, 0.1) is (0, 0)
        assert_eq!(results[0].id, 1);
        // Next are (1,0) and (0,1) — equidistant
        let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn exact_nn_100_percent_recall() {
        let mut index = FlatIndex::new(VectorMetric::L2);

        // Insert 50 points
        for i in 0..50u64 {
            index.insert(i, vec![i as f32, (i * 2) as f32]);
        }

        // Query at (10.5, 21.0) — nearest should be point 10 (10, 20) and 11 (11, 22)
        let results = index.search(&[10.5, 21.0], 2);
        assert_eq!(results.len(), 2);
        // Exact NN — 100% recall guaranteed
        assert!(results.iter().any(|r| r.id == 10));
        assert!(results.iter().any(|r| r.id == 11));
    }

    #[test]
    fn cosine_metric() {
        let mut index = FlatIndex::new(VectorMetric::Cosine);

        index.insert(1, vec![1.0, 0.0]);
        index.insert(2, vec![0.0, 1.0]);
        index.insert(3, vec![-1.0, 0.0]);

        let results = index.search(&[1.0, 0.0], 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn dot_product_metric() {
        let mut index = FlatIndex::new(VectorMetric::DotProduct);

        index.insert(1, vec![1.0, 0.0]);
        index.insert(2, vec![0.5, 0.5]);
        index.insert(3, vec![0.0, 1.0]);

        // Highest dot product with (1,0) is (1,0) itself
        let results = index.search(&[1.0, 0.0], 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn manhattan_metric() {
        let mut index = FlatIndex::new(VectorMetric::L1);

        index.insert(1, vec![0.0, 0.0]);
        index.insert(2, vec![5.0, 5.0]);

        let results = index.search(&[0.1, 0.1], 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn k_greater_than_size() {
        let mut index = FlatIndex::new(VectorMetric::L2);
        index.insert(1, vec![0.0]);
        index.insert(2, vec![1.0]);

        let results = index.search(&[0.0], 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn k_zero_returns_empty() {
        let mut index = FlatIndex::new(VectorMetric::L2);
        index.insert(1, vec![0.0]);

        assert!(index.search(&[0.0], 0).is_empty());
    }

    #[test]
    fn duplicate_insert_ignored() {
        let mut index = FlatIndex::new(VectorMetric::L2);
        index.insert(1, vec![0.0]);
        index.insert(1, vec![99.0]); // Same ID — ignored
        assert_eq!(index.len(), 1);

        let results = index.search(&[0.0], 1);
        assert_eq!(results[0].score, 0.0); // Original vector kept
    }

    #[test]
    fn remove_vector() {
        let mut index = FlatIndex::new(VectorMetric::L2);
        index.insert(1, vec![0.0]);
        index.insert(2, vec![1.0]);

        assert!(index.remove(1));
        assert_eq!(index.len(), 1);
        assert!(!index.remove(1)); // Already removed

        let results = index.search(&[0.0], 1);
        assert_eq!(results[0].id, 2);
    }

    #[test]
    fn recommended_size_threshold() {
        let index = FlatIndex::new(VectorMetric::L2);
        assert!(index.is_recommended_size());
    }

    #[test]
    fn high_dimensional() {
        let mut index = FlatIndex::new(VectorMetric::Cosine);

        for i in 0..20u64 {
            let vec: Vec<f32> = (0..384)
                .map(|d| ((i * d + 1) as f32 * 0.01).sin())
                .collect();
            index.insert(i, vec);
        }

        let query: Vec<f32> = (0..384).map(|d| (d as f32 * 0.01).sin()).collect();
        let results = index.search(&query, 5);
        assert_eq!(results.len(), 5);
    }
}
