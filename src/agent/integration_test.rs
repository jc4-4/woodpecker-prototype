#[cfg(test)]
mod tests {
    use crate::agent::server::server;
    use crate::error::Result;
    use crate::resource_util::tests::{
        create_default_bucket, create_default_queue, delete_default_bucket, delete_default_queue,
        list_default_bucket,
    };
    use log::debug;
    use serial_test::serial;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio::task;
    use tokio::time::{sleep, Duration};
    use crate::agent::client::agent::{AgentConfig, Agent};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        create_default_bucket().await;
        create_default_queue().await;
        let task = task::spawn_blocking(|| async { server::run_server().await }).await?;
        let _server = task::spawn(task);
        // Wait for server to start
        sleep(Duration::from_millis(1000)).await;

        let content = b"Mary had a little lamb\nLittle lamb, little lamb";
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write(content)?;

        let path_str = temp_file.path().to_str().unwrap();
        let config = AgentConfig {
            file: path_str.to_string(),
            buffer_size: 1024,
        };
        let mut agent = Agent::try_new(config).await?;
        agent.work().await?;
        let keys = list_default_bucket().await?;
        debug!("Keys under default bucket: {:?}", keys);
        assert_eq!(1, keys.len());

        delete_default_bucket().await;
        delete_default_queue().await;
        Ok(())
    }
}
