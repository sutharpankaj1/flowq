//! Broker - Main message broker orchestrator
//!
//! The Broker is the central component that coordinates all operations.

use std::sync::Arc;

use flowq_storage::StorageEngine;
use flowq_types::{Message, MessageId, Queue, QueueConfig, QueueStats, Result};
use tracing::info;

/// Main message broker
pub struct Broker {
    /// Storage backend
    storage: Arc<dyn StorageEngine>,
}

impl Broker {
    /// Create a new broker with the given storage backend
    pub fn new(storage: impl StorageEngine + 'static) -> Self {
        info!("Initializing FlowQ broker");
        Self {
            storage: Arc::new(storage),
        }
    }

    /// Create a new broker with an Arc storage
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        info!("Initializing FlowQ broker");
        Self { storage }
    }

    /// Get a reference to the storage engine
    pub fn storage(&self) -> &dyn StorageEngine {
        self.storage.as_ref()
    }

    // ==================== Queue Operations ====================

    /// Create a new queue with default configuration
    pub async fn create_queue(&self, name: impl Into<String>) -> Result<Queue> {
        let queue = Queue::new(name);
        self.storage.create_queue(queue).await
    }

    /// Create a new queue with custom configuration
    pub async fn create_queue_with_config(
        &self,
        name: impl Into<String>,
        config: QueueConfig,
    ) -> Result<Queue> {
        let queue = Queue::with_config(name, config);
        self.storage.create_queue(queue).await
    }

    /// Get a queue by name
    pub async fn get_queue(&self, name: &str) -> Result<Option<Queue>> {
        self.storage.get_queue(name).await
    }

    /// List all queues
    pub async fn list_queues(&self) -> Result<Vec<Queue>> {
        self.storage.list_queues().await
    }

    /// Delete a queue
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.storage.delete_queue(name).await
    }

    /// Get queue statistics
    pub async fn get_queue_stats(&self, name: &str) -> Result<QueueStats> {
        self.storage.get_queue_stats(name).await
    }

    /// Purge all messages from a queue
    pub async fn purge_queue(&self, name: &str) -> Result<u64> {
        self.storage.purge_queue(name).await
    }

    // ==================== Message Operations ====================

    /// Publish a message to a queue
    pub async fn publish(&self, queue_name: &str, message: Message) -> Result<MessageId> {
        self.storage.push_message(queue_name, message).await
    }

    /// Publish raw bytes to a queue
    pub async fn publish_bytes(
        &self,
        queue_name: &str,
        body: impl Into<bytes::Bytes>,
    ) -> Result<MessageId> {
        let message = Message::new(body);
        self.publish(queue_name, message).await
    }

    /// Receive a single message from a queue
    pub async fn receive(&self, queue_name: &str) -> Result<Option<Message>> {
        self.storage.pop_message(queue_name).await
    }

    /// Receive multiple messages from a queue
    pub async fn receive_batch(&self, queue_name: &str, max: usize) -> Result<Vec<Message>> {
        self.storage.pop_messages(queue_name, max).await
    }

    /// Peek at the next message without removing it
    pub async fn peek(&self, queue_name: &str) -> Result<Option<Message>> {
        self.storage.peek_message(queue_name).await
    }

    /// Acknowledge a message (mark as successfully processed)
    pub async fn ack(&self, queue_name: &str, message_id: &MessageId) -> Result<()> {
        self.storage.ack_message(queue_name, message_id).await
    }

    /// Negative acknowledge (return to queue for retry)
    pub async fn nack(&self, queue_name: &str, message_id: &MessageId) -> Result<()> {
        self.storage.nack_message(queue_name, message_id).await
    }

    // ==================== Maintenance ====================

    /// Start background maintenance tasks
    pub async fn start_maintenance(&self) {
        let storage = Arc::clone(&self.storage);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            loop {
                interval.tick().await;
                if let Err(e) = storage.cleanup_expired().await {
                    tracing::error!(error = %e, "Failed to cleanup expired messages");
                }
            }
        });

        info!("Background maintenance started");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowq_storage::MemoryStorage;

    fn create_test_broker() -> Broker {
        Broker::new(MemoryStorage::new())
    }

    #[tokio::test]
    async fn test_create_queue() {
        let broker = create_test_broker();

        let queue = broker.create_queue("test-queue").await.unwrap();
        assert_eq!(queue.name, "test-queue");

        let queues = broker.list_queues().await.unwrap();
        assert_eq!(queues.len(), 1);
    }

    #[tokio::test]
    async fn test_publish_and_receive() {
        let broker = create_test_broker();
        broker.create_queue("test").await.unwrap();

        // Publish
        let msg_id = broker.publish_bytes("test", "Hello!").await.unwrap();

        // Receive
        let received = broker.receive("test").await.unwrap();
        assert!(received.is_some());

        let msg = received.unwrap();
        assert_eq!(msg.id, msg_id);
        assert_eq!(msg.body_as_str(), Some("Hello!"));

        // Ack
        broker.ack("test", &msg.id).await.unwrap();

        // Queue should be empty
        let stats = broker.get_queue_stats("test").await.unwrap();
        assert_eq!(stats.message_count, 0);
    }

    #[tokio::test]
    async fn test_receive_batch() {
        let broker = create_test_broker();
        broker.create_queue("test").await.unwrap();

        // Publish multiple messages
        for i in 0..5 {
            broker
                .publish_bytes("test", format!("Message {}", i))
                .await
                .unwrap();
        }

        // Receive batch
        let messages = broker.receive_batch("test", 3).await.unwrap();
        assert_eq!(messages.len(), 3);

        // Stats should show 2 pending, 3 in-flight
        let stats = broker.get_queue_stats("test").await.unwrap();
        assert_eq!(stats.pending_count, 2);
        assert_eq!(stats.in_flight_count, 3);
    }

    #[tokio::test]
    async fn test_nack_returns_to_queue() {
        let broker = create_test_broker();
        broker.create_queue("test").await.unwrap();

        broker.publish_bytes("test", "test message").await.unwrap();

        let msg = broker.receive("test").await.unwrap().unwrap();
        broker.nack("test", &msg.id).await.unwrap();

        // Message should be back in queue
        let stats = broker.get_queue_stats("test").await.unwrap();
        assert_eq!(stats.pending_count, 1);
        assert_eq!(stats.in_flight_count, 0);
    }
}
