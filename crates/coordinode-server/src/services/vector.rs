use tonic::{Request, Response, Status};

use crate::proto::query;

#[derive(Default)]
pub struct VectorServiceImpl;

#[tonic::async_trait]
impl query::vector_service_server::VectorService for VectorServiceImpl {
    async fn vector_search(
        &self,
        request: Request<query::VectorSearchRequest>,
    ) -> Result<Response<query::VectorSearchResponse>, Status> {
        let req = request.into_inner();

        let query_vector = req
            .query_vector
            .ok_or_else(|| Status::invalid_argument("query_vector is required"))?;

        if query_vector.values.is_empty() {
            return Err(Status::invalid_argument("query_vector must not be empty"));
        }

        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be > 0"));
        }

        // Stub: return empty results.
        // Full implementation requires HNSW index integration with storage,
        // which depends on how indexes are loaded (startup/on-demand).
        Ok(Response::new(query::VectorSearchResponse {
            results: vec![],
        }))
    }

    async fn hybrid_search(
        &self,
        request: Request<query::HybridSearchRequest>,
    ) -> Result<Response<query::HybridSearchResponse>, Status> {
        let req = request.into_inner();

        let query_vector = req
            .query_vector
            .ok_or_else(|| Status::invalid_argument("query_vector is required"))?;

        if query_vector.values.is_empty() {
            return Err(Status::invalid_argument("query_vector must not be empty"));
        }

        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be > 0"));
        }

        // Stub: return empty results.
        // Full hybrid requires graph traversal + vector filtering integration.
        Ok(Response::new(query::HybridSearchResponse {
            results: vec![],
        }))
    }
}
