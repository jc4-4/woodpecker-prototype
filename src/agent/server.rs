use std::pin::Pin;

use futures::stream::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use prototype::agent::protobuf::{
    agent_service_server::{AgentService, AgentServiceServer},
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};

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

// Refactor this out of main to avoid nested tokio runtime when running test.
async fn serve() -> Result<(), tonic::transport::Error> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = WoodpeckerAgentService::default();
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(AgentServiceServer::new(service))
        .serve(addr)
        .await
}

#[tokio::main]
pub async fn main() -> Result<(), tonic::transport::Error> {
    serve().await
}

#[cfg(test)]
mod tests {
    use super::WoodpeckerAgentService;
    use prototype::agent::protobuf::{
        agent_service_client::AgentServiceClient, agent_service_server::AgentServiceServer,
        GetDestinationsRequest,
    };
    use tokio::task;

    // Use multi_thread to have server on a separate thread.
    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_roundtrip() {
        let t = task::spawn_blocking(|| async { super::serve().await })
            .await
            .unwrap();
        let _s = task::spawn(t);

        let mut client = AgentServiceClient::connect("http://[::1]:50051")
            .await
            .unwrap();
        let outbound1 = async_stream::stream! {
            yield GetDestinationsRequest {};
        };

        let mut inbound1 = client
            .get_destinations(outbound1)
            .await
            .unwrap()
            .into_inner();
        let mut i = 0;
        while let Some(r) = inbound1.message().await.unwrap() {
            i += 1;
            println!("Batch: {:?}", r);
            assert_eq!(vec!["1"], r.destinations);
        }
        assert_eq!(1, i);
    }
}
