
#[cfg(test)]
mod tests {
    use rusoto_core::Region;
    use rusoto_s3::{S3Client, S3};

    #[tokio::test]
    async fn s3_list_buckets() {
        let localstack = Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        };
        let client = S3Client::new(localstack);
        let output = client.list_buckets().await.unwrap();
        assert_eq!(Some(vec![]), output.buckets);
    }
}
