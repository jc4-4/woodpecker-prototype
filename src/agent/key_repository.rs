use rusoto_core::{Region, RusotoError};
use rusoto_credential::AwsCredentials;
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::PutObjectRequest;
use rusoto_sqs::{SendMessageError, SendMessageRequest, Sqs, SqsClient};
use uuid::Uuid;

type Key = String;

#[derive(Clone)]
pub struct KeyRepository {
    bucket: String,
    queue_url: String,
    region: Region,
    credentials: AwsCredentials,
    sqs_client: SqsClient,
}

fn new_key() -> String {
    Uuid::new_v4().to_string()
}

/// Default to use localstack at port 4566.
impl Default for KeyRepository {
    fn default() -> Self {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        KeyRepository {
            bucket: "default_bucket".to_string(),
            queue_url: "http://localhost:4566/000000000000/default_queue_name".to_string(),
            region: region.clone(),
            credentials: Default::default(),
            sqs_client: SqsClient::new(region),
        }
    }
}

impl KeyRepository {
    fn new(
        bucket: String,
        queue_url: String,
        region: Region,
        credentials: AwsCredentials,
    ) -> KeyRepository {
        KeyRepository {
            bucket,
            queue_url,
            region: region.clone(),
            credentials,
            sqs_client: SqsClient::new(region.clone()),
        }
    }

    async fn produce(&self, n: usize) -> Vec<Key> {
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            let req = PutObjectRequest {
                bucket: self.bucket.clone(),
                key: new_key(),
                ..Default::default()
            };
            keys.push(req.get_presigned_url(
                &self.region,
                &self.credentials,
                &PreSignedRequestOption::default(),
            ));
        }
        keys
    }

    // TODO: create custom errors.
    async fn consume(&self, key: Key) -> Result<(), RusotoError<SendMessageError>> {
        let req = SendMessageRequest {
            queue_url: self.queue_url.clone(),
            message_body: key,
            ..Default::default()
        };
        self.sqs_client.send_message(req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rusoto_core::{Region, RusotoError};
    use rusoto_sqs::{
        CreateQueueError, CreateQueueRequest, CreateQueueResult, DeleteQueueRequest,
        ReceiveMessageRequest, Sqs, SqsClient,
    };

    use super::KeyRepository;

    async fn create_queue(
        repository: &KeyRepository,
    ) -> Result<CreateQueueResult, RusotoError<CreateQueueError>> {
        let parts: Vec<&str> = repository.queue_url.split("/").collect();
        let queue_name = parts
            .last()
            .expect("queue_name in repository.queue_url")
            .to_string();
        println!("Creating queue with name: {}", queue_name);

        repository
            .sqs_client
            .create_queue(CreateQueueRequest {
                // Matching the default of KeyRepository
                queue_name,
                ..Default::default()
            })
            .await
    }

    async fn receive_message(repository: &KeyRepository) -> Vec<String> {
        let res = repository
            .sqs_client
            .receive_message(ReceiveMessageRequest {
                queue_url: repository.queue_url.clone(),
                ..Default::default()
            })
            .await;
        res.unwrap()
            .messages
            .unwrap()
            .iter()
            .map(|m| m.body.as_ref().unwrap().clone())
            .collect()
    }

    async fn delete_queue(repository: &KeyRepository) -> () {
        repository
            .sqs_client
            .delete_queue(DeleteQueueRequest {
                queue_url: repository.queue_url.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_zero() {
        let repository = KeyRepository::default();
        let keys = repository.produce(0).await;
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_one() {
        let repository = KeyRepository::default();
        let keys = repository.produce(1).await;
        assert_eq!(1, keys.len());
        assert!(keys[0].starts_with("http://localhost:4566/default_bucket/"));
    }

    #[tokio::test]
    async fn roundtrip() {
        let repository = KeyRepository::default();
        create_queue(&repository)
            .await
            .expect("Failed to create queue: ");

        let keys = repository.produce(1).await;
        repository.consume(keys[0].clone()).await.unwrap();

        let messages = receive_message(&repository).await;
        assert_eq!(keys, messages);
        delete_queue(&repository).await;
    }
}
