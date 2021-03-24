use crate::error::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use log::debug;
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    PutObjectRequest, S3Client, StreamingBody, S3,
};

/// A BlobStore where you can store and retrieve binary large objects (blobs).
#[async_trait]
pub trait BlobStore {
    /// Create a bucket with the name, which must be unique.
    async fn create_bucket(&self, name: &str) -> Result<()>;
    /// Delete a bucket by name.
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    /// Store an object under a bucket with key and body.
    async fn put_object(&self, bucket: &str, key: &str, body: StreamingBody) -> Result<()>;
    /// Get an object from a bucket with key.
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes>;
    /// Delete an object from a bucket with key.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;
}

/// A BlobStore based on AWS S3.
#[derive(Clone)]
pub struct S3BlobStore {
    s3_client: S3Client,
}

impl S3BlobStore {
    pub fn new(region: Region) -> S3BlobStore {
        S3BlobStore {
            s3_client: S3Client::new(region),
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn create_bucket(&self, name: &str) -> Result<()> {
        let req = CreateBucketRequest {
            bucket: name.to_string(),
            ..Default::default()
        };
        self.s3_client.create_bucket(req).await?;
        debug!("Created bucket: {}", name);
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let req = DeleteBucketRequest {
            bucket: name.to_string(),
            ..Default::default()
        };
        self.s3_client.delete_bucket(req).await?;
        debug!("Deleted bucket: {}", name);
        Ok(())
    }

    async fn put_object(&self, bucket: &str, key: &str, body: StreamingBody) -> Result<()> {
        let req = PutObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: Some(body),
            ..Default::default()
        };
        self.s3_client.put_object(req).await?;
        debug!("Put object under bucket {} with key {}", bucket, key);
        Ok(())
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let req = GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };
        debug!("Get object under bucket {} with key {}", bucket, key);
        let res = self.s3_client.get_object(req).await?;
        let stream = res.body.unwrap();
        let y: BytesMut = stream
            .map_ok(|b| BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();
        Ok(y.freeze())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let req = DeleteObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };
        self.s3_client.delete_object(req).await?;
        debug!("Delete object under bucket {} with key {}", bucket, key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_core::Region;
    use serial_test::serial;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        let blob_store = S3BlobStore::new(Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        });

        let bucket_name = "bucket-name".to_string();
        blob_store.create_bucket(&bucket_name).await?;

        let object_name = "object-name".to_string();
        let object_body = b"object-body\n";
        blob_store
            .put_object(
                &bucket_name,
                &object_name,
                StreamingBody::from(object_body.to_vec()),
            )
            .await?;

        let body = blob_store.get_object(&bucket_name, &object_name).await?;
        assert_eq!(&body[..], object_body);

        blob_store.delete_object(&bucket_name, &object_name).await?;
        blob_store.delete_bucket(&bucket_name).await?;
        Ok(())
    }
}
