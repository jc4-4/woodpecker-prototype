use crate::agent::client::tailer::Tailer;
use crate::agent::client::uploader::Uploader;
use crate::agent::protobuf::{
    agent_service_client::AgentServiceClient, CreateKeysRequest, CreateKeysResponse,
    DeleteKeysRequest, DeleteKeysResponse,
};
use crate::error::Result;
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

/// The agent tails log files and upload them.
// TODO: use a Consumer<Buffer> for better testability.
// TODO: add test case around rotation.
pub struct Agent {
    tailer: Tailer,
    client: AgentServiceClient<Channel>,
    uploader: Uploader,
}

impl Agent {
    pub async fn try_new(config: AgentConfig) -> Result<Agent> {
        let tailer = Tailer::try_new(&config.file, config.buffer_size)?;

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
