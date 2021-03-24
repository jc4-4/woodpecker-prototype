use crate::data::pub_sub::{PubSub, SqsPubSub};
use crate::error::Result;
use rusoto_core::Region;
use rusoto_credential::AwsCredentials;
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::PutObjectRequest;

use std::fmt;
use uuid::Uuid;

/// A PresignedUrl is a place holder for uploads.
#[derive(Clone)]
pub struct PresignedUrl {
    pub value: String,
}

impl PresignedUrl  {
    fn new(value: &str) -> PresignedUrl {
        PresignedUrl {
            value: value.to_string(),
        }
    }
}

impl fmt::Display for PresignedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Clone)]
pub struct PresignedUrlRepository {
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

// https://s3.amazonaws.com/bucket/key.xyz?X-Amz-...
// => key.xyz
pub fn get_key(bucket: &str, url: &PresignedUrl) -> String {
    let url = url.to_string();
    let i = url.find(bucket).expect("presigned url has bucket name");
    let j = url.find('?').expect("presigned url has ?");
    // https://s3.amazonaws.com/bucket/key.xyz?X-Amz-...
    //                         ^              ^
    //                         i              j
    url[i + bucket.len() + 1..j].to_string()
}

/// Default to use localstack at port 4566.
impl Default for PresignedUrlRepository {
    fn default() -> Self {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        PresignedUrlRepository::new(
            "default-bucket".to_string(),
            "http://localhost:4566/000000000000/default_queue_name".to_string(),
            region,
            AwsCredentials::default(),
        )
    }
}

impl PresignedUrlRepository {
    fn new(
        bucket: String,
        queue_url: String,
        region: Region,
        credentials: AwsCredentials,
    ) -> PresignedUrlRepository {
        PresignedUrlRepository {
            bucket,
            queue_url,
            region: region.clone(),
            credentials,
            pub_sub: SqsPubSub::new(region),
        }
    }

    /// Produce PresignedUrls for client use.
    pub async fn produce(&self, n: usize) -> Vec<PresignedUrl> {
        let mut urls = Vec::with_capacity(n);
        for _ in 0..n {
            let req = PutObjectRequest {
                bucket: self.bucket.clone(),
                key: new_key(),
                ..Default::default()
            };
            let url = req.get_presigned_url(&self.region,
                                            &self.credentials,
                                            &PreSignedRequestOption::default());
            urls.push(PresignedUrl::new(&url));
        }
        urls
    }

    /// Client is done with the PresignedUrls. Convert them to tasks.
    pub async fn consume(&self, keys: Vec<PresignedUrl>) -> Result<()> {
        let messages = keys.iter().map(|sk| get_key(&self.bucket, sk)).collect();
        self.pub_sub
            .send_messages(self.queue_url.clone(), messages)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::pub_sub::PubSub;
    use crate::error::Result;
    use log::debug;
    use serial_test::serial;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_zero() {
        let repository = PresignedUrlRepository::default();
        let urls = repository.produce(0).await;
        assert!(urls.is_empty());
    }

    #[tokio::test]
    async fn test_one() {
        init();
        let repository = PresignedUrlRepository::default();
        let urls = repository.produce(1).await;
        assert_eq!(1, urls.len());
        debug!("{}", urls[0]);
        assert!(urls[0]
            .to_string()
            .starts_with("http://localhost:4566/default-bucket/"));
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        let repository = PresignedUrlRepository::default();
        let queue_id = repository
            .pub_sub
            .create_queue("default_queue_name".to_string())
            .await?;

        let urls = repository.produce(1).await;
        repository.consume(urls.clone()).await.unwrap();

        let messages = repository
            .pub_sub
            .receive_messages(queue_id.clone())
            .await?;
        assert_eq!(1, messages.len());
        assert_eq!(get_key(&repository.bucket, &urls[0]), messages[0].1);

        repository.pub_sub.delete_queue(queue_id.clone()).await?;
        Ok(())
    }

    #[test]
    fn key() {
        let url = PresignedUrl::new(
            "http://localhost:4566/default-bucket/d4683880-f813-40f6-a923-675739a0902e?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%2F20210322%2Flocal%2Fs3%2Faws4_request&X-Amz-Date=20210322T050314Z&X-Amz-Expires=3600&X-Amz-Signature=3d21b0d16ce021b3244c0f73bd84b092228e96488c6fdb1c62c6d67b6dd10733&X-Amz-SignedHeaders=host");

        assert_eq!(
            "d4683880-f813-40f6-a923-675739a0902e",
            get_key("default-bucket", &url)
        );
    }
}