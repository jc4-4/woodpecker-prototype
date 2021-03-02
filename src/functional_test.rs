#[cfg(test)]
mod tests {
    use crate::protobuf::{
        agent_service_client::AgentServiceClient, agent_service_server::AgentServiceServer,
        GetDestinationsRequest,
    };
    use crate::WoodpeckerAgentService;
    use tokio::task;

    // Use multi_thread to have server on a separate thread.
    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_roundtrip() {
        let task = task::spawn_blocking(|| async {
            let addr = "[::1]:50051".parse().unwrap();
            let service = WoodpeckerAgentService::default();
            println!("Server listening on {}", addr);
            tonic::transport::Server::builder()
                .add_service(AgentServiceServer::new(service))
                .serve(addr)
                .await?;
            Ok::<(), tonic::transport::Error>(())
        })
        .await
        .unwrap();
        let _server = task::spawn(task);

        let mut client = AgentServiceClient::connect("http://[::1]:50051")
            .await
            .unwrap();
        let outbound1 = async_stream::stream! {
            yield GetDestinationsRequest {};
        };

        let mut inbound1 = client
            .get_destinations(outbound1)
            .await
            .unwrap()
            .into_inner();
        let mut i = 0;
        while let Some(r) = inbound1.message().await.unwrap() {
            i += 1;
            println!("Batch: {:?}", r);
            assert_eq!(vec!["1"], r.destinations);
        }
        assert_eq!(1, i);
    }
}
