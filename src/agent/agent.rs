use crate::agent::protobuf::{
    agent_service_client::AgentServiceClient, CreateKeysRequest, CreateKeysResponse,
    DeleteKeysRequest, DeleteKeysResponse,
};
use crate::agent::tailer::Tailer;
use crate::agent::uploader::Uploader;
use crate::error::Result;
use log::debug;
use tonic::transport::Channel;

// TODO: dynamic config from agent service
/// Configure the behaviour of the agent.
#[derive(Clone)]
pub struct AgentConfig {
    /// Where are the log files
    // TODO: support multiple files
    file: String,
    /// How much buffer per file
    buffer_size: usize,
}

/// The agent tails log files and upload them.
pub struct Agent {
    tailer: Tailer,
    client: AgentServiceClient<Channel>,
    uploader: Uploader,
}

impl Agent {
    pub async fn try_new(config: AgentConfig) -> Result<Agent> {
        // TODO: do it in a way that does not use static scope and leak memory.
        let tailer = Tailer::try_new(Box::leak(config.file.into_boxed_str()), config.buffer_size)?;

        // TODO: service discovery
        let client = AgentServiceClient::connect("http://[::1]:50051").await?;

        Ok(Agent {
            tailer,
            client,
            uploader: Uploader::default(),
        })
    }

    // Diff new config with existing one.
    // For any added file, create a new tailer.
    // For any removed file, only remove once current tailer reads fully.
    fn reload(&self, _config: AgentConfig) {
        todo!()
    }

    pub async fn work(&mut self) -> Result<()> {
        match self.tailer.read()? {
            Some(buffer) => {
                debug!("Buffer received: {:?}", buffer);
                // Note: if I refactor the following snippet to another function,
                // Rust compiler complains that there are two mutable references,
                // one from read() and one from the other function.
                // TODO: request specific number of keys, don't waste them.
                // TODO: can we reuse keys across tailers?
                let request = CreateKeysRequest {};
                let response: CreateKeysResponse =
                    self.client.create_keys(request).await?.into_inner();

                let keys: Vec<String> = response.keys;
                self.uploader.upload(&keys[0], buffer).await?;

                let request = DeleteKeysRequest {
                    keys: vec![keys[0].clone()],
                };
                let _response: DeleteKeysResponse =
                    self.client.delete_keys(request).await?.into_inner();

                Ok(())
            }
            None => {
                debug!("Reached end of file");
                if self.tailer.is_rotated()? {
                    debug!("Rotate to new file");
                    self.tailer.rotate()?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::server;
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
