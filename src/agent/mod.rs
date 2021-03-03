use rusoto_core::{Region, RusotoError};
use rusoto_credential::AwsCredentials;
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::PutObjectRequest;
use rusoto_sqs::{SendMessageError, SendMessageRequest, Sqs, SqsClient};
use uuid::Uuid;

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}
// mod functional_test;

type Key = String;

pub struct Service {
    bucket: String,
    queue_url: String,
    region: Region,
    credentials: AwsCredentials,
    sqs_client: SqsClient,
}

fn new_key() -> String {
    Uuid::new_v4().to_string()
}

impl Service {
    fn new(
        bucket: String,
        queue_url: String,
        region: Region,
        credentials: AwsCredentials,
    ) -> Service {
        Service {
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
    use rusoto_core::Region;
    use rusoto_sqs::{
        CreateQueueRequest, DeleteQueueRequest, ReceiveMessageRequest, Sqs, SqsClient,
    };

    use super::Service;

    fn service(queue_url: String) -> Service {
        Service::new(
            "bucket".to_string(),
            queue_url,
            Region::Custom {
                name: "local".to_string(),
                endpoint: "http://localhost:4566".to_string(),
            },
            Default::default(),
        )
    }

    fn read_only_service() -> Service {
        service("queue_url does not exist!".to_string())
    }

    fn sqs_client() -> SqsClient {
        SqsClient::new(Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        })
    }

    async fn create_queue(queue_name: String) -> String {
        let res = sqs_client()
            .create_queue(CreateQueueRequest {
                queue_name,
                ..Default::default()
            })
            .await;
        res.unwrap().queue_url.unwrap().clone()
    }

    async fn receive_message(queue_url: String) -> Vec<String> {
        let res = sqs_client()
            .receive_message(ReceiveMessageRequest {
                queue_url,
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

    async fn delete_queue(queue_url: String) -> () {
        sqs_client()
            .delete_queue(DeleteQueueRequest {
                queue_url,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_zero() {
        let keys = read_only_service().produce(0).await;
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_one() {
        let keys = read_only_service().produce(1).await;
        assert_eq!(1, keys.len());
        assert!(keys[0].starts_with("http://localhost:4566/bucket/"));
    }

    #[tokio::test]
    async fn roundtrip() {
        let queue_url = create_queue("queue_name".to_string().clone()).await;
        let service = service(queue_url.clone());

        let keys = service.produce(1).await;
        service.consume(keys[0].clone()).await.unwrap();

        let messages = receive_message(queue_url.clone()).await;
        assert_eq!(keys, messages);
        delete_queue(queue_url.clone()).await;
    }
}
