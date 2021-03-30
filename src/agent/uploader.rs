use crate::error::Result;
use log::debug;
use reqwest::Client;

/// Uploader for presigned url
#[derive(Debug, Default, Clone)]
pub struct Uploader {
    client: Client,
}

impl Uploader {
    pub async fn upload(&self, presigned_url: &str, bytes: &[u8]) -> Result<()> {
        let client = reqwest::Client::new();
        // TODO: retry strategy
        let _res = client
            .put(presigned_url)
            .body(bytes.to_vec())
            .send()
            .await
            .expect("Put object with presigned url failed");
        debug!("Object uploaded to {}", presigned_url);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use warp::Filter;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn roundtrip() -> Result<()> {
        init();

        // Set up a server at localhost:50051.
        tokio::spawn(async move {
            let routes = warp::any().map(|| warp::reply());
            warp::serve(routes).run(([127, 0, 0, 1], 50051)).await;
        });

        // Instead of `assert!` in server, rely on absent of upload failure.
        let uploader = Uploader::default();
        uploader
            .upload("http://127.0.0.1:50051/", b"content")
            .await?;

        Ok(())
    }
}
