pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

use tonic::Streaming;

use protobuf::agent_service_client::AgentServiceClient;
use protobuf::{
    GetAgentConfigRequest, GetAgentConfigResponse, GetDestinationsRequest, GetDestinationsResponse,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = AgentServiceClient::connect("http://[::1]:50051").await?;
    {
        let request = tonic::Request::new(GetAgentConfigRequest {});
        let response: GetAgentConfigResponse = client.get_agent_config(request).await?.into_inner();
        println!("get_agent_config response={:?}", response);
    }

    let outbound1 = async_stream::stream! {
        yield GetDestinationsRequest {};
    };

    let outbound2 = async_stream::stream! {
        yield GetDestinationsRequest {};
        yield GetDestinationsRequest {};
    };

    let mut i: i32 = 0;
    let mut j: i32 = 0;
    let mut inbound1 = client.get_destinations(outbound1).await?.into_inner();
    while let Some(r) = inbound1.message().await? {
        println!("Batch {} {}: {:?}", i, j, r);
        j += 1;
    }

    i += 1;
    let mut inbound2: Streaming<GetDestinationsResponse> =
        client.get_destinations(outbound2).await?.into_inner();
    while let Some(r) = inbound2.message().await? {
        println!("Batch {} {}: {:?}", i, j, r);
        j += 1;
    }

    Ok(())
}
