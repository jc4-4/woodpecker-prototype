use crate::agent::key_repository::{KeyRepository, SignedKey};
use crate::agent::protobuf::{
    agent_service_server::{AgentService, AgentServiceServer},
    CreateKeysRequest, CreateKeysResponse, DeleteKeysRequest, DeleteKeysResponse,
    GetAgentConfigRequest, GetAgentConfigResponse,
};
use crate::error::Result;
use log::debug;
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone, Default)]
pub struct WoodpeckerAgentService {
    repository: Arc<KeyRepository>,
}

#[tonic::async_trait]
impl AgentService for WoodpeckerAgentService {
    async fn get_agent_config(
        &self,
        _request: Request<GetAgentConfigRequest>,
    ) -> std::result::Result<Response<GetAgentConfigResponse>, Status> {
        Ok(Response::new(GetAgentConfigResponse {}))
    }

    async fn create_keys(
        &self,
        _request: Request<CreateKeysRequest>,
    ) -> std::result::Result<Response<CreateKeysResponse>, Status> {
        let keys = self.repository.produce(5).await;
        let values = keys.iter().map(SignedKey::to_string).collect();
        debug!("Created keys: {:?}", values);
        Ok(Response::new(CreateKeysResponse { keys: values }))
    }

    async fn delete_keys(
        &self,
        request: Request<DeleteKeysRequest>,
    ) -> std::result::Result<Response<DeleteKeysResponse>, Status> {
        for key in request.into_inner().keys {
            debug!("Deleting key: {}", key);
            self.repository
                .consume(SignedKey { value: key })
                .await
                .unwrap();
        }
        Ok(Response::new(DeleteKeysResponse {}))
    }
}

// Refactor this out of main to avoid nested tokio runtime when running test.
async fn run_server() -> Result<()> {
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
pub async fn main() -> Result<()> {
    env_logger::init();
    run_server().await
}

#[cfg(test)]
mod tests {
    use crate::agent::protobuf::{
        agent_service_client::AgentServiceClient, CreateKeysRequest, CreateKeysResponse,
        DeleteKeysRequest, DeleteKeysResponse,
    };
    use crate::error::Result;
    use log::debug;
    use rusoto_core::Region;
    use rusoto_sqs::{CreateQueueRequest, CreateQueueResult, DeleteQueueRequest, Sqs, SqsClient};
    use serial_test::serial;
    use tokio::task;
    use tokio::time::{sleep, Duration};
    use tonic::transport::Channel;
    use tonic::Response;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    async fn client() -> AgentServiceClient<Channel> {
        AgentServiceClient::connect("http://[::1]:50051")
            .await
            .expect("Client fails to connect: ")
    }

    async fn create_queue() -> Result<CreateQueueResult> {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        let sqs_client = SqsClient::new(region);
        let queue_name = "default_queue_name".to_string();
        debug!("Creating queue with name: {}", queue_name);
        let result = sqs_client
            .create_queue(CreateQueueRequest {
                queue_name,
                ..Default::default()
            })
            .await?;
        Ok(result)
    }

    async fn delete_queue() -> Result<()> {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        let sqs_client = SqsClient::new(region);
        let queue_url = "http://localhost:4566/000000000000/default_queue_name".to_string();
        debug!("Deleting queue with name: {}", queue_url);

        sqs_client
            .delete_queue(DeleteQueueRequest {
                queue_url,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn create_delete_keys_roundtrip() {
        init();
        create_queue().await.unwrap();
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
        assert_eq!(5, keys.len());
        for key in keys.clone() {
            assert!(key.starts_with("http://localhost:4566/default_bucket/"));
        }

        let _res: Response<DeleteKeysResponse> = client
            .delete_keys(DeleteKeysRequest { keys })
            .await
            .unwrap();
        // Struct is empty. Nothing to assert on.
        delete_queue().await.unwrap();
    }
}
