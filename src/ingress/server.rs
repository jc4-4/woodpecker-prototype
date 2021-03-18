use crate::ingress::parser::Parser;
use crate::ingress::writer::Writer;
use arrow::datatypes::{DataType, Field, Schema};
use rusoto_core::Region;
use rusoto_s3::S3Client;
use rusoto_sqs::SqsClient;
use std::sync::Arc;

/// Receive message from a queue for files to parse.
/// Then write the parsed files to the bucket.
pub struct IngressService {
    // TODO: create a schema service that creates parser and writer.
    parser: Parser,
    writer: Writer,
    bucket: String,
    queue_url: String,
    s3_client: S3Client,
    sqs_client: SqsClient,
}

/// Default to use localstack at port 4566.
// TODO: use config-rs to manage regions, buckets, and queues.
impl Default for IngressService {
    fn default() -> Self {
        let region = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let parser = Parser::new("f=(?P<f>\\w+),b=(?P<b>\\w+)?", schema.clone());
        let writer = Writer::new(schema);
        IngressService::new(
            parser,
            writer,
            "default_bucket".to_string(),
            "http://localhost:4566/000000000000/default_queue_name".to_string(),
            region,
        )
    }
}

impl IngressService {
    fn new(
        parser: Parser,
        writer: Writer,
        bucket: String,
        queue_url: String,
        region: Region,
    ) -> IngressService {
        IngressService {
            parser,
            writer,
            bucket,
            queue_url,
            s3_client: S3Client::new(region.clone()),
            sqs_client: SqsClient::new(region.clone()),
        }
    }

    fn receive_message() {
        todo!()
    }

    fn upload_file() {
        todo!()
    }

    fn delete_message() {
        todo!()
    }
}
