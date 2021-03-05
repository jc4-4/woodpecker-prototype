use std::pin::Pin;

use futures::stream::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use prototype::agent::protobuf::{
    agent_service_server::{AgentService, AgentServiceServer},
    CreateKeysRequest, CreateKeysResponse, DeleteKeysRequest, DeleteKeysResponse,
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};

#[derive(Clone, Default)]
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

    async fn create_keys(
        &self,
        _request: Request<CreateKeysRequest>,
    ) -> Result<Response<CreateKeysResponse>, Status> {
        Ok(Response::new(CreateKeysResponse { keys: vec![] }))
    }

    async fn delete_keys(
        &self,
        _request: Request<DeleteKeysRequest>,
    ) -> Result<Response<DeleteKeysResponse>, Status> {
        Ok(Response::new(DeleteKeysResponse {}))
    }
}

// Refactor this out of main to avoid nested tokio runtime when running test.
async fn run_server() -> Result<(), tonic::transport::Error> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = WoodpeckerAgentService::default();
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(AgentServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), tonic::transport::Error> {
    run_server().await
}

#[cfg(test)]
mod functional_test {
    use prototype::agent::protobuf::{
        agent_service_client::AgentServiceClient, CreateKeysRequest, CreateKeysResponse,
        DeleteKeysRequest, DeleteKeysResponse, GetDestinationsRequest,
    };
    use tokio::task;
    use tokio::time::{sleep, Duration};
    use tonic::transport::Channel;
    use tonic::Response;

    async fn client() -> AgentServiceClient<Channel> {
        AgentServiceClient::connect("http://[::1]:50051")
            .await
            .expect("Client fails to connect: ")
    }

    // Use multi_thread to have server on a separate thread.
    #[tokio::test]
    async fn get_destinations_roundtrip() {
        let task = task::spawn_blocking(|| async { super::run_server().await })
            .await
            .unwrap();
        let _server = task::spawn(task);
        // Wait for server to start
        sleep(Duration::from_millis(1000)).await;

        let mut client = client().await;
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

    // Use multi_thread to have server on a separate thread.
    #[tokio::test]
    async fn create_delete_keys_roundtrip() {
        let task = task::spawn_blocking(|| async { super::run_server().await })
            .await
            .unwrap();
        let _server = task::spawn(task);
        // Wait for server to start
        sleep(Duration::from_millis(1000)).await;

        let mut client = client().await;

        let res: Response<CreateKeysResponse> =
            client.create_keys(CreateKeysRequest {}).await.unwrap();
        let keys = res.into_inner().keys;
        assert!(keys.is_empty());

        let _res: Response<DeleteKeysResponse> = client
            .delete_keys(DeleteKeysRequest { keys: vec![] })
            .await
            .unwrap();
        // Struct is empty. Nothing to assert on.
    }
}
