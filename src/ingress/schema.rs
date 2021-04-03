use crate::error::{woodpecker_error, Result};
use log::debug;
use rusoto_core::Region;
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, GetItemInput, PutItemInput};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;

type ArrowSchemaRef = arrow::datatypes::SchemaRef;

/// A schema consist of a regex and an arrow schema.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// A regex tells us how to parse the input.
    pub regex: String,
    /// An arrow_schema tells us how to read/write the output.
    pub arrow_schema: ArrowSchemaRef,
}

impl Schema {
    pub fn new(regex: &str, arrow_schema: ArrowSchemaRef) -> Schema {
        Schema {
            regex: regex.to_string(),
            arrow_schema,
        }
    }
}

static KEY: &str = "key";

pub struct SchemaRepository {
    table_name: String,
    client: DynamoDbClient,
}

impl Default for SchemaRepository {
    fn default() -> Self {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        SchemaRepository::new("default-table", region)
    }
}

impl SchemaRepository {
    pub fn new(table_name: &str, region: Region) -> SchemaRepository {
        SchemaRepository {
            table_name: table_name.to_string(),
            client: DynamoDbClient::new(region),
        }
    }

    pub async fn put_schema(&self, key: &str, schema: Schema) -> Result<()> {
        // TODO: conditional put to avoid accidental overwrites.
        let mut item = serde_dynamodb::to_hashmap(&schema)?;
        item.insert(
            KEY.to_string(),
            AttributeValue {
                s: Some(key.to_string()),
                ..Default::default()
            },
        );
        let req = PutItemInput {
            table_name: self.table_name.clone(),
            item,
            ..Default::default()
        };
        debug!("PutItemInput: {:#?}", req);
        self.client.put_item(req).await.expect("Put item failure: ");
        Ok(())
    }

    pub async fn get_schema(&self, key: &str) -> Result<Schema> {
        let mut item = HashMap::new();
        item.insert(
            KEY.to_string(),
            AttributeValue {
                s: Some(key.to_string()),
                ..Default::default()
            },
        );
        let req = GetItemInput {
            table_name: self.table_name.clone(),
            key: item,
            ..Default::default()
        };

        let res = self.client.get_item(req).await.expect("Get item failure: ");
        match res.item {
            Some(mut item) => {
                // Insert metadata if not present. Otherwise, deserialize might fail.
                if let Some(arrow_schema) = item.get_mut("arrow_schema") {
                    let map = arrow_schema.m.as_mut().unwrap();
                    if let Vacant(metadata) = map.entry("metadata".to_string()) {
                        metadata.insert(AttributeValue {
                            m: Some(HashMap::new()),
                            ..Default::default()
                        });
                    }
                }
                let schema = serde_dynamodb::from_hashmap(item)?;
                Ok(schema)
            }
            None => Err(woodpecker_error(
                format!("Schema does not exist with key: {}", key).as_str(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_util::tests::{create_default_table, delete_default_table};
    use serial_test::serial;
    use std::sync::Arc;

    type ArrowSchema = arrow::datatypes::Schema;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        create_default_table().await;

        let schema = Schema::new("regex", Arc::new(ArrowSchema::empty()));
        let repository = SchemaRepository::default();

        let key = "id";
        repository.put_schema(key, schema.clone()).await?;
        assert_eq!(schema, repository.get_schema(key).await?);
        delete_default_table().await;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn does_not_exist() -> Result<()> {
        init();
        create_default_table().await;

        let repository = SchemaRepository::default();
        let res = repository.get_schema("does not exist").await;
        assert!(res
            .err()
            .unwrap()
            .to_string()
            .starts_with("General error: Schema does not exist"));
        delete_default_table().await;
        Ok(())
    }
}
