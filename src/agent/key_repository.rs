use crate::data::pub_sub::{PubSub, SqsPubSub};
use crate::error::Result;
use rusoto_core::Region;
use rusoto_credential::AwsCredentials;
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::PutObjectRequest;

use std::fmt;
use uuid::Uuid;

/// This struct represents a signed key in a string.
/// The underlying key uniquely identifies content in the space.
#[derive(Clone)]
pub struct SignedKey {
    pub value: String,
}

impl fmt::Display for SignedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Clone)]
pub struct KeyRepository {
    bucket: String,
    queue_url: String,
    region: Region,
    credentials: AwsCredentials,
    pub_sub: SqsPubSub,
}

// TODO: create partitions by agent id, account id, etc.
// Example: /bucket/account_id/agent_id/uuid
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
        KeyRepository::new(
            "default_bucket".to_string(),
            "http://localhost:4566/000000000000/default_queue_name".to_string(),
            region.clone(),
            AwsCredentials::default(),
        )
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
            pub_sub: SqsPubSub::new(region),
        }
    }

    pub async fn produce(&self, n: usize) -> Vec<SignedKey> {
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            let req = PutObjectRequest {
                bucket: self.bucket.clone(),
                key: new_key(),
                ..Default::default()
            };
            keys.push(SignedKey {
                value: req.get_presigned_url(
                    &self.region,
                    &self.credentials,
                    &PreSignedRequestOption::default(),
                ),
            });
        }
        keys
    }

    pub async fn consume(&self, keys: Vec<SignedKey>) -> Result<()> {
        let messages = keys.iter().map(|sk| sk.to_string()).collect();
        self.pub_sub
            .send_messages(self.queue_url.clone(), messages)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::KeyRepository;
    use crate::data::pub_sub::PubSub;
    use crate::error::Result;
    use rusoto_sqs::Sqs;
    use serial_test::serial;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
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
        assert!(keys[0]
            .to_string()
            .starts_with("http://localhost:4566/default_bucket/"));
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        let repository = KeyRepository::default();
        let queue_id = repository
            .pub_sub
            .create_queue("default_queue_name".to_string())
            .await?;

        let keys = repository.produce(1).await;
        repository.consume(keys.clone()).await.unwrap();

        let messages = repository
            .pub_sub
            .receive_messages(queue_id.clone())
            .await?;
        assert_eq!(1, messages.len());
        assert_eq!(keys[0].to_string(), messages[0].1);

        repository.pub_sub.delete_queue(queue_id.clone()).await?;
        Ok(())
    }
}
