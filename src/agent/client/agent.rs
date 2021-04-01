use crate::agent::client::tailer::Tailer;
use crate::agent::client::uploader::Uploader;
use crate::agent::protobuf::{
    agent_service_client::AgentServiceClient, CreateKeysRequest, CreateKeysResponse,
    DeleteKeysRequest, DeleteKeysResponse,
};
use crate::error::Result;
use async_trait::async_trait;
use log::debug;
use tonic::transport::Channel;

// TODO: dynamic config from agent service
/// Configure the behaviour of the agent.
#[derive(Clone)]
pub struct AgentConfig {
    /// Where are the log files
    // TODO: support multiple files
    pub file: String,
    /// How much buffer per file
    pub buffer_size: usize,
}

#[async_trait]
trait BufferEater {
    async fn consume(&mut self, buffer: &[u8]) -> Result<()>;
}

/// The agent tails log files and upload them.
// TODO: use a Consumer<Buffer> for better testability.
// TODO: add test case around rotation.
pub struct Agent {
    tailer: Tailer,
    consumer: Box<dyn BufferEater>,
}

struct BufferUploader {
    client: AgentServiceClient<Channel>,
    uploader: Uploader,
}

#[async_trait]
impl BufferEater for BufferUploader {
    async fn consume(&mut self, buffer: &[u8]) -> Result<()> {
        debug!("Buffer received: {:?}", buffer);
        // Note: if I refactor the following snippet to another function,
        // Rust compiler complains that there are two mutable references,
        // one from read() and one from the other function.
        // TODO: request specific number of keys, don't waste them.
        // TODO: can we reuse keys across tailers?
        let request = CreateKeysRequest {};
        let response: CreateKeysResponse = self.client.create_keys(request).await?.into_inner();

        let keys: Vec<String> = response.keys;
        self.uploader.upload(&keys[0], buffer).await?;

        let request = DeleteKeysRequest {
            keys: vec![keys[0].clone()],
        };
        let _response: DeleteKeysResponse = self.client.delete_keys(request).await?.into_inner();
        Ok(())
    }
}

impl Agent {
    pub async fn try_new(config: AgentConfig) -> Result<Agent> {
        let tailer = Tailer::try_new(&config.file, config.buffer_size)?;

        // TODO: service discovery
        let client = AgentServiceClient::connect("http://[::1]:50051").await?;

        Ok(Agent {
            tailer,
            consumer: Box::new(BufferUploader {
                client,
                uploader: Uploader::default(),
            }),
        })
    }

    // Diff new config with existing one.
    // For any added file, create a new tailer.
    // For any removed file, only remove once current tailer reads fully.
    fn reload(&self, _config: AgentConfig) {
        todo!()
    }

    // TODO: return something to indicate end of file or not.
    pub async fn work(&mut self) -> Result<()> {
        match self.tailer.read()? {
            Some(buffer) => {
                self.consumer.consume(buffer).await?;
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
    use crate::agent::client::agent::{Agent, BufferEater};
    use crate::agent::client::tailer::Tailer;
    use crate::error::Result;
    use async_trait::async_trait;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;

    /// Collect all buffer consumed for comparison later.
    struct BufferCollector {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    #[async_trait]
    impl BufferEater for BufferCollector {
        async fn consume(&mut self, buffer: &[u8]) -> Result<()> {
            let mut buf = self.buffer.lock().unwrap();
            buf.extend_from_slice(buffer);
            Ok(())
        }
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn read_all() -> Result<()> {
        init();

        let content = b"Mary had a little lamb\nLittle lamb, little lamb";
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write(content)?;

        let path_str = temp_file.path().to_str().unwrap();
        let tailer = Tailer::try_new(path_str, 10)?;
        let buf = Arc::new(Mutex::new(vec![]));
        let mut agent = Agent {
            tailer,
            consumer: Box::new(BufferCollector {
                buffer: buf.clone(),
            }),
        };
        for _ in 0..10 {
            agent.work().await?;
        }
        assert_eq!(content, buf.lock().unwrap().as_slice());
        Ok(())
    }
}
