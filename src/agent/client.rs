use crate::agent::protobuf::{
    agent_service_client::AgentServiceClient, GetAgentConfigRequest, GetAgentConfigResponse,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut client = AgentServiceClient::connect("http://[::1]:50051").await?;
    {
        let request = tonic::Request::new(GetAgentConfigRequest {});
        let response: GetAgentConfigResponse = client.get_agent_config(request).await?.into_inner();
        println!("get_agent_config response={:?}", response);
    }

    Ok(())
}
