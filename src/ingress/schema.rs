use crate::error::{woodpecker_error, Result};
use arrow::datatypes;
use std::collections::HashMap;

type ArrowSchema = datatypes::Schema;
type ArrowSchemaRef = datatypes::SchemaRef;

/// A schema consist of a regex and an arrow schema.
#[derive(Debug, Clone, PartialEq, Eq)]
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

pub struct SchemaRepository {
    // TODO: use a dynamo db client.
    repository: HashMap<String, Schema>,
}

impl SchemaRepository {
    pub fn new() -> SchemaRepository {
        SchemaRepository {
            repository: HashMap::new(),
        }
    }

    pub async fn put_schema(&mut self, key: &str, schema: Schema) -> Result<()> {
        match self.repository.insert(key.to_string(), schema) {
            None => Ok(()),
            Some(_) => Err(woodpecker_error(
                format!("Schema already exist under key {}", key).as_str(),
            )),
        }
    }

    pub async fn get_schema(&self, key: &str) -> Result<&Schema> {
        match self.repository.get(key) {
            Some(schema) => Ok(schema),
            None => Err(woodpecker_error(
                format!("Schema does not exist with key: {}", key).as_str(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn roundtrip() -> Result<()> {
        init();

        let schema = Schema::new("regex", Arc::new(ArrowSchema::new(vec![])));
        let mut repository = SchemaRepository::new();

        let key = "id";
        repository.put_schema(key, schema.clone()).await?;
        assert_eq!(schema, *repository.get_schema(key).await?);
        Ok(())
    }

    #[tokio::test]
    async fn does_not_exist() -> Result<()> {
        init();
        let repository = SchemaRepository::new();
        let res = repository.get_schema("does not exist").await;
        assert!(res
            .err()
            .unwrap()
            .to_string()
            .starts_with("General error: Schema does not exist"));
        Ok(())
    }
}
