#[cfg(test)]
pub(crate) mod tests {
    use crate::data::blob_store::{BlobStore, S3BlobStore};
    use crate::data::pub_sub::{PubSub, SqsPubSub};
    use crate::error::Result;
    use crate::ingress::schema::{Schema, SchemaRepository};
    use log::debug;
    use rusoto_core::Region;
    use rusoto_dynamodb::{
        AttributeDefinition, CreateTableInput, DeleteTableInput, DynamoDb, DynamoDbClient,
        KeySchemaElement,
    };
    use rusoto_s3::{ListObjectsV2Request, S3Client, S3};
    use std::sync::Arc;

    type ArrowDataType = arrow::datatypes::DataType;
    type ArrowField = arrow::datatypes::Field;
    type ArrowSchema = arrow::datatypes::Schema;

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

    pub async fn create_default_table() {
        let client = DynamoDbClient::new(local_region());
        let req = CreateTableInput {
            table_name: "default-table".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "key".to_string(),
                key_type: "HASH".to_string(),
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "key".to_string(),
                attribute_type: "S".to_string(),
            }],
            billing_mode: Some("PAY_PER_REQUEST".to_string()),
            ..Default::default()
        };
        debug!("{:#?}", req);
        client.create_table(req).await.unwrap();
    }

    pub async fn delete_default_table() {
        let client = DynamoDbClient::new(local_region());
        let req = DeleteTableInput {
            table_name: "default-table".to_string(),
            ..Default::default()
        };
        debug!("{:#?}", req);
        client.delete_table(req).await.unwrap();
    }

    pub async fn populate_test_schemas() {
        let repository = SchemaRepository::new("default-table", local_region());
        repository
            .put_schema(
                "RUST_SINGLE_LINE",
                Schema::new(
                    "f=(?P<f>\\w+)",
                    Arc::new(ArrowSchema::new(vec![ArrowField::new(
                        "f",
                        ArrowDataType::Utf8,
                        false,
                    )])),
                ),
            )
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
