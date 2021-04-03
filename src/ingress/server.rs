use crate::data::blob_store::{BlobStore, S3BlobStore};
use crate::data::pub_sub::{PubSub, SqsPubSub};
use crate::error::Result;
use crate::ingress::parser::Parser;
use crate::ingress::schema::SchemaRepository;
use crate::ingress::writer::Writer;
use log::{debug, info};
use rusoto_core::Region;

use crate::serde::ingress_task::IngressTask;
use rusoto_s3::StreamingBody;
use tokio::time::{sleep, Duration};

/// Receive message from a queue for files to parse.
/// Then write the parsed files to the bucket.
pub struct IngressService {
    bucket: String,
    queue_url: String,
    schema_repository: SchemaRepository,
    blob_store: S3BlobStore,
    pub_sub: SqsPubSub,
}

/// Default to use localstack at port 4566.
// TODO: use config-rs to manage regions, buckets, and queues.
impl Default for IngressService {
    fn default() -> Self {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        IngressService::new(
            "default-bucket".to_string(),
            "http://localhost:4566/000000000000/default_queue_name".to_string(),
            region,
        )
    }
}

impl IngressService {
    pub fn new(bucket: String, queue_url: String, region: Region) -> IngressService {
        IngressService {
            bucket,
            queue_url,
            // TODO: fix this
            schema_repository: SchemaRepository::default(),
            blob_store: S3BlobStore::new(region.clone()),
            pub_sub: SqsPubSub::new(region),
        }
    }

    /// Process tasks from queue and delete them afterwards.
    pub async fn process_tasks(&self) -> Result<Vec<String>> {
        let messages = self.pub_sub.receive_messages(&self.queue_url).await?;
        if messages.is_empty() {
            return Ok(vec![]);
        }

        let mut ids = Vec::with_capacity(messages.len());
        let mut files = Vec::with_capacity(messages.len());
        for (id, message) in messages {
            ids.push(id);
            let task: IngressTask = serde_json::from_str(&message)?;
            let file = self.work(task).await?;
            files.push(file);
        }

        self.pub_sub.delete_messages(&self.queue_url, ids).await?;
        Ok(files)
    }

    /// Work on a single task - download, parser, write, and upload.
    async fn work(&self, task: IngressTask) -> Result<String> {
        debug!("Working on task: {:?}", &task);
        let blob = self.blob_store.get_object(&task.bucket, &task.key).await?;
        // TODO: extract schema from message instead of hardcode
        let schema = self
            .schema_repository
            .get_schema("RUST_SINGLE_LINE")
            .await?;
        let parser = Parser::new(schema.regex.as_str(), schema.arrow_schema.clone());
        let batch = parser.parse(blob);
        let writer = Writer::new(schema.arrow_schema.clone());
        let file = writer.write(batch);
        self.blob_store
            .put_object(&self.bucket, &file.name, StreamingBody::from(file.content))
            .await?;
        self.blob_store
            .delete_object(&task.bucket, &task.key)
            .await?;
        Ok(file.name)
    }
}

// Refactor this out of main to avoid nested tokio runtime when running test.
pub async fn run_server() -> Result<()> {
    let service = IngressService::default();
    loop {
        let tasks = service.process_tasks().await?;
        info!("Ingested files: {:?}", tasks);
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    run_server().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::client::uploader::Uploader;
    use crate::agent::server::presigned_url::{PresignedUrl, PresignedUrlRepository};
    use crate::resource_util::tests::{
        create_default_bucket, create_default_queue, create_default_table, delete_default_bucket,
        delete_default_queue, delete_default_table, populate_test_schemas,
    };
    use crate::serde::ingress_task::IngressTask;
    use log::debug;
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};
    use serial_test::serial;
    use std::sync::Arc;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        create_default_queue().await;
        create_default_bucket().await;
        create_default_table().await;
        populate_test_schemas().await;

        let service = IngressService::default();
        let key_repository = PresignedUrlRepository::default();
        let keys = key_repository.produce(1).await;
        let bytes = b"f=oo";
        let url = keys[0].to_string();
        let uploader = Uploader::default();
        uploader.upload(&url, bytes).await?;

        let url = PresignedUrl::new(&url);
        let task: IngressTask = url.into();
        let blob = service
            .blob_store
            .get_object(&task.bucket, &task.key)
            .await?;
        debug!("Blob: {:?}", blob.to_vec());
        key_repository.consume(keys).await?;

        let files = service.process_tasks().await?;
        assert_eq!(1, files.len());

        let bytes = service
            .blob_store
            .get_object(&service.bucket, &files[0])
            .await?;
        let cursor = SliceableCursor::new(bytes.to_vec());
        let reader = SerializedFileReader::new(cursor).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut reader = arrow_reader.get_record_reader(1024).unwrap();
        let actual_batch = reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");
        assert_eq!(1, actual_batch.num_columns());
        assert_eq!(1, actual_batch.num_rows());
        debug!("Actual_batch: {:#?}", actual_batch);

        delete_default_table().await;
        delete_default_queue().await;
        delete_default_bucket().await;
        Ok(())
    }
}
