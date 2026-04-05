use tonic::{Request, Response, Status};

use crate::proto::graph;

#[derive(Default)]
pub struct SchemaServiceImpl;

#[tonic::async_trait]
impl graph::schema_service_server::SchemaService for SchemaServiceImpl {
    async fn create_label(
        &self,
        request: Request<graph::CreateLabelRequest>,
    ) -> Result<Response<graph::Label>, Status> {
        let req = request.into_inner();
        Ok(Response::new(graph::Label {
            name: req.name,
            properties: req.properties,
            version: 1,
        }))
    }

    async fn create_edge_type(
        &self,
        request: Request<graph::CreateEdgeTypeRequest>,
    ) -> Result<Response<graph::EdgeType>, Status> {
        let req = request.into_inner();
        Ok(Response::new(graph::EdgeType {
            name: req.name,
            properties: req.properties,
            version: 1,
        }))
    }

    async fn list_labels(
        &self,
        _request: Request<graph::ListLabelsRequest>,
    ) -> Result<Response<graph::ListLabelsResponse>, Status> {
        Ok(Response::new(graph::ListLabelsResponse { labels: vec![] }))
    }

    async fn list_edge_types(
        &self,
        _request: Request<graph::ListEdgeTypesRequest>,
    ) -> Result<Response<graph::ListEdgeTypesResponse>, Status> {
        Ok(Response::new(graph::ListEdgeTypesResponse {
            edge_types: vec![],
        }))
    }
}
