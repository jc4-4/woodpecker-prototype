use tonic::{transport::Server, Request, Response, Status};

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

use protobuf::agent_service_server::{AgentService, AgentServiceServer};
use protobuf::{
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};

#[derive(Debug, Default)]
pub struct WoodpeckerAgentService {}

#[tonic::async_trait]
impl AgentService for WoodpeckerAgentService {
    async fn get_agent_config(
        &self,
        _request: Request<GetAgentConfigRequest>,
    ) -> Result<Response<GetAgentConfigResponse>, Status> {
        Ok(Response::new(GetAgentConfigResponse {}))
    }

    async fn get_destinations(
        &self,
        _request: Request<GetDestinationsRequest>,
    ) -> Result<Response<GetDestinationsResponse>, Status> {
        Ok(Response::new(GetDestinationsResponse {
            destinations: vec![]
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = WoodpeckerAgentService::default();
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(AgentServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}
