use tonic::{Request, Response, Status};

use crate::proto::health;

#[derive(Default)]
pub struct HealthServiceImpl;

#[tonic::async_trait]
impl health::health_service_server::HealthService for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<health::HealthCheckRequest>,
    ) -> Result<Response<health::HealthCheckResponse>, Status> {
        Ok(Response::new(health::HealthCheckResponse {
            status: health::ServingStatus::Serving as i32,
        }))
    }

    async fn ready(
        &self,
        _request: Request<health::ReadyRequest>,
    ) -> Result<Response<health::ReadyResponse>, Status> {
        Ok(Response::new(health::ReadyResponse {
            ready: true,
            reason: String::new(),
        }))
    }
}
