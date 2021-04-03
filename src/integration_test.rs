#[cfg(test)]
mod tests {
    use crate::agent;
    use crate::agent::client::agent::{Agent, AgentConfig};
    use crate::error::Result;
    use crate::ingress;
    use crate::resource_util::tests::{
        create_default_bucket, create_default_queue, create_default_table, delete_default_bucket,
        delete_default_queue, delete_default_table, list_default_bucket, populate_test_schemas,
    };
    use log::debug;
    use serial_test::serial;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio::task;
    use tokio::time::{sleep, Duration};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    async fn start_agent_server() -> Result<()> {
        let task =
            task::spawn_blocking(|| async { agent::server::server::run_server().await }).await?;
        let _server = task::spawn(task);
        // Wait for server to start
        sleep(Duration::from_millis(1000)).await;
        Ok(())
    }

    async fn start_ingress_server() -> Result<()> {
        let task = task::spawn_blocking(|| async { ingress::server::run_server().await }).await?;
        let _server = task::spawn(task);
        // Wait for server to start
        sleep(Duration::from_millis(1000)).await;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        create_default_bucket().await;
        create_default_queue().await;
        create_default_table().await;
        populate_test_schemas().await;

        start_agent_server().await?;
        start_ingress_server().await?;

        let content = b"f=oo";
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write(content)?;

        let path_str = temp_file.path().to_str().unwrap();
        let config = AgentConfig {
            file: path_str.to_string(),
            buffer_size: 1024,
        };
        let mut agent = Agent::try_new(config).await?;
        agent.work().await?;

        // Wait for ingress
        sleep(Duration::from_millis(1000)).await;
        let keys = list_default_bucket().await?;
        debug!("Keys under default bucket: {:?}", keys);
        assert_eq!(1, keys.len());
        assert!(keys[0].to_string().starts_with("parquet-"));

        delete_default_table().await;
        delete_default_bucket().await;
        delete_default_queue().await;
        Ok(())
    }
}
