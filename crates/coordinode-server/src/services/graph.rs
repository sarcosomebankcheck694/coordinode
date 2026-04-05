use tonic::{Request, Response, Status};

use crate::proto::graph;

#[derive(Default)]
pub struct GraphServiceImpl;

#[tonic::async_trait]
impl graph::graph_service_server::GraphService for GraphServiceImpl {
    async fn create_node(
        &self,
        request: Request<graph::CreateNodeRequest>,
    ) -> Result<Response<graph::Node>, Status> {
        let req = request.into_inner();
        Ok(Response::new(graph::Node {
            node_id: 0,
            labels: req.labels,
            properties: req.properties,
        }))
    }

    async fn get_node(
        &self,
        request: Request<graph::GetNodeRequest>,
    ) -> Result<Response<graph::Node>, Status> {
        let req = request.into_inner();
        Err(Status::not_found(format!("node {} not found", req.node_id)))
    }

    async fn create_edge(
        &self,
        request: Request<graph::CreateEdgeRequest>,
    ) -> Result<Response<graph::Edge>, Status> {
        let req = request.into_inner();
        Ok(Response::new(graph::Edge {
            edge_id: 0,
            edge_type: req.edge_type,
            source_node_id: req.source_node_id,
            target_node_id: req.target_node_id,
            properties: req.properties,
        }))
    }

    async fn traverse(
        &self,
        _request: Request<graph::TraverseRequest>,
    ) -> Result<Response<graph::TraverseResponse>, Status> {
        Ok(Response::new(graph::TraverseResponse {
            nodes: vec![],
            edges: vec![],
            pagination: None,
        }))
    }
}
