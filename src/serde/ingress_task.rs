use crate::agent::presigned_url_repository::PresignedUrl;
use log::debug;
use serde::{Deserialize, Serialize};

/// A task represents a file for ingress to process.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct IngressTask {
    // TODO: add table id to support different schemas.
    pub bucket: String,
    pub key: String,
}

impl From<PresignedUrl> for IngressTask {
    fn from(url: PresignedUrl) -> Self {
        let url = url.to_string();
        let parts: Vec<&str> = url.split(&['/', '?'][..]).collect();
        debug!("Parts of a PresignedUrl: {:?}", parts);
        IngressTask {
            bucket: parts[3].to_string(),
            key: parts[4].to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        let url = PresignedUrl::new("https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<your-access-key-id>/20130721/us-east-1/s3/aws4_request
&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>");
        let task: IngressTask = url.into();
        assert_eq!("examplebucket", task.bucket);
        assert_eq!("test.txt", task.key);
    }

    #[test]
    fn test_local() {
        // example from localstack
        let url = PresignedUrl::new("http://localhost:4566/default-bucket/630faa67-02eb-49b9-b7d0-30b21e595044?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%2F20210324%2Flocal%2Fs3%2Faws4_request&X-Amz-Date=20210324T040114Z&X-Amz-Expires=3600&X-Amz-Signature=7b44209560c1a43b67b277f456084635c513ead1a3f7926b59e67a52ca3484d4&X-Amz-SignedHeaders=host");
        let task: IngressTask = url.into();
        assert_eq!("default-bucket", task.bucket);
        assert_eq!("630faa67-02eb-49b9-b7d0-30b21e595044", task.key);
    }
}
