use crate::error::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
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
    async fn create_bucket(&self, name: String) -> Result<()>;
    /// Delete a bucket by name.
    async fn delete_bucket(&self, name: String) -> Result<()>;
    /// Store an object under a bucket with key and body.
    async fn put_object(&self, bucket: String, key: String, body: StreamingBody) -> Result<()>;
    /// Get an object from a bucket with key.
    async fn get_object(&self, bucket: String, key: String) -> Result<Bytes>;
    /// Delete an object from a bucket with key.
    async fn delete_object(&self, bucket: String, key: String) -> Result<()>;
}

/// A BlobStore based on AWS S3.
#[derive(Clone)]
pub struct S3BlobStore {
    s3_client: S3Client,
}

impl S3BlobStore {
    fn new(region: Region) -> S3BlobStore {
        S3BlobStore {
            s3_client: S3Client::new(region),
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn create_bucket(&self, name: String) -> Result<()> {
        let req = CreateBucketRequest {
            bucket: name.clone(),
            ..Default::default()
        };
        self.s3_client.create_bucket(req).await?;
        debug!("Created bucket: {}", name);
        Ok(())
    }

    async fn delete_bucket(&self, name: String) -> Result<()> {
        let req = DeleteBucketRequest {
            bucket: name.clone(),
            ..Default::default()
        };
        self.s3_client.delete_bucket(req).await?;
        debug!("Deleted bucket: {}", name);
        Ok(())
    }

    async fn put_object(&self, bucket: String, key: String, body: StreamingBody) -> Result<()> {
        let req = PutObjectRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            body: Some(body),
            ..Default::default()
        };
        self.s3_client.put_object(req).await?;
        debug!("Put object under bucket {} with key {}", bucket, key);
        Ok(())
    }

    async fn get_object(&self, bucket: String, key: String) -> Result<Bytes> {
        let req = GetObjectRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            ..Default::default()
        };
        let res = self.s3_client.get_object(req).await?;
        let stream = res.body.unwrap();
        let y: BytesMut = stream
            .map_ok(|b| BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();
        debug!("Get object under bucket {} with key {}", bucket, key);
        Ok(y.freeze())
    }

    async fn delete_object(&self, bucket: String, key: String) -> Result<()> {
        let req = DeleteObjectRequest {
            bucket: bucket.clone(),
            key: key.clone(),
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
        let blob_store = S3BlobStore::new(Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        });

        let bucket_name = "bucket-name".to_string();
        blob_store.create_bucket(bucket_name.clone()).await?;

        let object_name = "object-name".to_string();
        let object_body = b"object-body\n";
        blob_store
            .put_object(
                bucket_name.clone(),
                object_name.clone(),
                StreamingBody::from(object_body.to_vec()),
            )
            .await?;

        let body = blob_store
            .get_object(bucket_name.clone(), object_name.clone())
            .await?;
        assert_eq!(&body[..], object_body);

        blob_store
            .delete_object(bucket_name.clone(), object_name.clone())
            .await?;
        blob_store.delete_bucket(bucket_name.clone()).await?;
        Ok(())
    }
}
