#[cfg(test)]
pub(crate) mod tests {
    use crate::data::blob_store::{BlobStore, S3BlobStore};
    use crate::data::pub_sub::{PubSub, SqsPubSub};
    use crate::error::Result;
    use rusoto_core::Region;
    use rusoto_s3::{ListObjectsV2Request, S3Client, S3};

    pub async fn create_default_bucket() {
        let blob_store = S3BlobStore::new(local_region());
        blob_store.create_bucket("default-bucket").await.unwrap();
    }

    pub async fn list_default_bucket() -> Result<Vec<String>> {
        let s3_client = S3Client::new(local_region());
        let res = s3_client
            .list_objects_v2(ListObjectsV2Request {
                bucket: "default-bucket".to_string(),
                ..Default::default()
            })
            .await?;

        let mut objects = vec![];
        if let Some(contents) = res.contents {
            for object in contents {
                objects.push(object.key.unwrap())
            }
        }

        Ok(objects)
    }

    pub async fn delete_default_bucket() {
        let blob_store = S3BlobStore::new(local_region());
        for key in list_default_bucket().await.unwrap() {
            blob_store
                .delete_object("default-bucket", &key)
                .await
                .unwrap();
        }
        blob_store.delete_bucket("default-bucket").await.unwrap();
    }

    pub async fn create_default_queue() {
        let pub_sub = SqsPubSub::new(local_region());
        pub_sub.create_queue("default_queue_name").await.unwrap();
    }

    pub async fn delete_default_queue() {
        let pub_sub = SqsPubSub::new(local_region());
        pub_sub
            .delete_queue("http://localhost:4566/000000000000/default_queue_name")
            .await
            .unwrap();
    }

    fn local_region() -> Region {
        Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        }
    }
}
