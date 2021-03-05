


use tonic::{transport::Server, Request, Response, Status};

use prototype::agent::key_repository::KeyRepository;
use prototype::agent::protobuf::{
    agent_service_server::{AgentService, AgentServiceServer},
    CreateKeysRequest, CreateKeysResponse, DeleteKeysRequest, DeleteKeysResponse,
    GetAgentConfigRequest, GetAgentConfigResponse,
};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct WoodpeckerAgentService {
    repository: Arc<KeyRepository>,
}

#[tonic::async_trait]
impl AgentService for WoodpeckerAgentService {
    async fn get_agent_config(
        &self,
        _request: Request<GetAgentConfigRequest>,
    ) -> Result<Response<GetAgentConfigResponse>, Status> {
        Ok(Response::new(GetAgentConfigResponse {}))
    }

    async fn create_keys(
        &self,
        _request: Request<CreateKeysRequest>,
    ) -> Result<Response<CreateKeysResponse>, Status> {
        let keys = self.repository.produce(5).await;
        Ok(Response::new(CreateKeysResponse { keys }))
    }

    async fn delete_keys(
        &self,
        request: Request<DeleteKeysRequest>,
    ) -> Result<Response<DeleteKeysResponse>, Status> {
        let keys = request.into_inner().keys;
        for key in keys {
            self.repository.consume(key).await;
        }
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
        DeleteKeysRequest, DeleteKeysResponse,
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
        println!("Received keys: {:?}", keys);
        assert_eq!(5, keys.len());
        for key in keys.clone() {
            assert!(key.starts_with("http://localhost:4566/default_bucket/"));
        }

        let _res: Response<DeleteKeysResponse> = client
            .delete_keys(DeleteKeysRequest { keys })
            .await
            .unwrap();
        // Struct is empty. Nothing to assert on.
    }
}
