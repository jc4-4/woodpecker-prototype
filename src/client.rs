pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}
use protobuf::agent_service_client::AgentServiceClient;
use protobuf::{
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = tonic::transport::Channel::from_static("http://[::1]:50051")
        .connect()
        .await?;
    let mut client = AgentServiceClient::new(channel);
    let request = tonic::Request::new(
        GetAgentConfigRequest {},
    );

    let response = client.get_agent_config(request).await?.into_inner();
    println!("RESPONSE={:?}", response);
    Ok(())
}