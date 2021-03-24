use crate::error::Result;
use async_trait::async_trait;
use log::debug;
use rusoto_core::Region;
use rusoto_sqs::{
    CreateQueueRequest, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry,
    DeleteMessageRequest, DeleteQueueRequest, ReceiveMessageRequest, SendMessageBatchRequest,
    SendMessageBatchRequestEntry, SendMessageRequest, Sqs, SqsClient,
};

/// A PubSub where you can send and receive messages in a queue.
#[async_trait]
pub trait PubSub {
    /// Create a queue with name. Returns a queue id.
    async fn create_queue(&self, name: &str) -> Result<String>;
    /// Delete a queue by id.
    async fn delete_queue(&self, queue_id: &str) -> Result<()>;
    /// Send messages to a queue.
    async fn send_messages(&self, queue_id: &str, messages: Vec<String>) -> Result<()>;
    /// Receive messages from a queue. Does not delete the messages.
    /// Returns both the message id and its content.
    async fn receive_messages(&self, queue_id: &str) -> Result<Vec<(String, String)>>;
    /// Delete messages previously received.
    async fn delete_messages(&self, queue_id: &str, message_ids: Vec<String>) -> Result<()>;
}

/// A PubSub based on AWS SQS.
#[derive(Clone)]
pub struct SqsPubSub {
    sqs_client: SqsClient,
}

impl SqsPubSub {
    pub fn new(region: Region) -> SqsPubSub {
        SqsPubSub {
            sqs_client: SqsClient::new(region),
        }
    }
}

#[async_trait]
impl PubSub for SqsPubSub {
    async fn create_queue(&self, name: &str) -> Result<String> {
        let req = CreateQueueRequest {
            queue_name: name.to_string(),
            ..Default::default()
        };
        let res = self.sqs_client.create_queue(req).await?;
        let queue_id = res.queue_url.unwrap();
        debug!("Created queue name: {} id: {}", name, queue_id);
        Ok(queue_id)
    }

    async fn delete_queue(&self, queue_id: &str) -> Result<()> {
        let req = DeleteQueueRequest {
            queue_url: queue_id.to_string(),
        };
        debug!("Delete queue with id: {}", queue_id);
        Ok(self.sqs_client.delete_queue(req).await?)
    }

    async fn send_messages(&self, queue_id: &str, messages: Vec<String>) -> Result<()> {
        debug!("Sending to queue {} messages: {:?}", queue_id, messages);
        if messages.is_empty() {
            Ok(())
        } else if messages.len() == 1 {
            let req = SendMessageRequest {
                queue_url: queue_id.to_string(),
                message_body: messages[0].clone(),
                ..Default::default()
            };
            self.sqs_client.send_message(req).await?;
            Ok(())
        } else {
            let mut entries = Vec::with_capacity(messages.len());
            for (i, message) in messages.iter().enumerate() {
                entries.push(SendMessageBatchRequestEntry {
                    id: i.to_string(),
                    message_body: message.clone(),
                    ..Default::default()
                });
            }

            let req = SendMessageBatchRequest {
                queue_url: queue_id.to_string(),
                entries,
            };

            // TODO: batch by number (up to 10) or size (up to 256KB)
            // TODO: retry partial failures
            self.sqs_client.send_message_batch(req).await?;
            Ok(())
        }
    }

    async fn receive_messages(&self, queue_id: &str) -> Result<Vec<(String, String)>> {
        let req = ReceiveMessageRequest {
            queue_url: queue_id.to_string(),
            max_number_of_messages: Some(10),
            ..Default::default()
        };
        let res = self.sqs_client.receive_message(req).await?;
        let messages = res.messages.unwrap_or_default();
        let mut v = Vec::with_capacity(messages.len());
        for message in &messages {
            let message_id = message.receipt_handle.as_ref().unwrap().clone();
            let body = message.body.as_ref().unwrap().clone();
            v.push((message_id, body));
        }
        debug!("Receiving from queue {} messages: {:?}", queue_id, v);
        Ok(v)
    }

    // TODO: batch by number (up to 10) or size (up to 256KB)
    // TODO: handle retry
    async fn delete_messages(&self, queue_id: &str, message_ids: Vec<String>) -> Result<()> {
        debug!(
            "Deleting from queue {} messages: {:?}",
            queue_id, message_ids
        );
        if message_ids.is_empty() {
            Ok(())
        } else if message_ids.len() == 1 {
            let req = DeleteMessageRequest {
                queue_url: queue_id.to_string(),
                receipt_handle: message_ids[0].clone(),
            };
            Ok(self.sqs_client.delete_message(req).await?)
        } else {
            // TODO: batch by number (up to 10) or size (up to 256KB)
            // TODO: retry partial failures
            let mut entries = Vec::with_capacity(message_ids.len());
            for (i, message) in message_ids.iter().enumerate() {
                entries.push(DeleteMessageBatchRequestEntry {
                    id: i.to_string(),
                    receipt_handle: message.clone(),
                });
            }

            let req = DeleteMessageBatchRequest {
                queue_url: queue_id.to_string(),
                entries,
            };
            self.sqs_client.delete_message_batch(req).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_core::Region;
    use serial_test::serial;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    #[serial]
    async fn roundtrip() -> Result<()> {
        init();
        let pub_sub = SqsPubSub::new(Region::Custom {
            name: "local".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        });
        let queue_id = pub_sub.create_queue("my_queue").await?;
        pub_sub
            .send_messages(&queue_id, vec!["message".to_string()])
            .await?;
        let recv = pub_sub.receive_messages(&queue_id).await?;
        assert_eq!(1, recv.len());
        let (message_id, message_body) = &recv[0];
        assert_eq!("message", message_body);
        assert!(pub_sub.receive_messages(&queue_id).await?.is_empty());
        pub_sub
            .delete_messages(&queue_id, vec![message_id.clone()])
            .await?;
        pub_sub.delete_queue(&queue_id).await?;
        Ok(())
    }
}
