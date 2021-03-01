mod agent_service;

use futures::stream::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

use protobuf::agent_service_server::{AgentService, AgentServiceServer};
use protobuf::{
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};
use std::pin::Pin;

#[derive(Debug, Default)]
pub struct WoodpeckerAgentService {}

#[tonic::async_trait]
impl AgentService for WoodpeckerAgentService {
    type GetDestinationsStream = Pin<
        Box<dyn Stream<Item = Result<GetDestinationsResponse, Status>> + Send + Sync + 'static>,
    >;

    async fn get_agent_config(
        &self,
        _request: Request<GetAgentConfigRequest>,
    ) -> Result<Response<GetAgentConfigResponse>, Status> {
        Ok(Response::new(GetAgentConfigResponse {}))
    }

    async fn get_destinations(
        &self,
        request: Request<Streaming<GetDestinationsRequest>>,
    ) -> Result<Response<Self::GetDestinationsStream>, Status> {
        let mut i: i32 = 0;
        let mut stream = request.into_inner();
        let output = async_stream::try_stream! {
            while let Some(_) = stream.message().await? {
                i += 1;
                yield GetDestinationsResponse {
                    destinations: vec![format!("{}", i),]
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
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
