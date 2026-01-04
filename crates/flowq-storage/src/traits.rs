//! Storage engine trait definition
//!
//! Defines the interface that all storage backends must implement.

use async_trait::async_trait;
use flowq_types::{Message, MessageId, Queue, QueueStats, Result};

/// Storage engine trait - all backends implement this
#[async_trait]
pub trait StorageEngine: Send + Sync {
    // ==================== Queue Operations ====================

    /// Create a new queue
    async fn create_queue(&self, queue: Queue) -> Result<Queue>;

    /// Get a queue by name
    async fn get_queue(&self, name: &str) -> Result<Option<Queue>>;

    /// List all queues
    async fn list_queues(&self) -> Result<Vec<Queue>>;

    /// Delete a queue and all its messages
    async fn delete_queue(&self, name: &str) -> Result<()>;

    /// Get queue statistics
    async fn get_queue_stats(&self, name: &str) -> Result<QueueStats>;

    // ==================== Message Operations ====================

    /// Store a message in a queue
    async fn push_message(&self, queue_name: &str, message: Message) -> Result<MessageId>;

    /// Get the next available message from a queue (marks as delivered)
    async fn pop_message(&self, queue_name: &str) -> Result<Option<Message>>;

    /// Get multiple messages from a queue
    async fn pop_messages(&self, queue_name: &str, max: usize) -> Result<Vec<Message>>;

    /// Peek at a message without removing it
    async fn peek_message(&self, queue_name: &str) -> Result<Option<Message>>;

    /// Acknowledge a message (mark as processed, remove from queue)
    async fn ack_message(&self, queue_name: &str, message_id: &MessageId) -> Result<()>;

    /// Negative acknowledge (return to queue for retry)
    async fn nack_message(&self, queue_name: &str, message_id: &MessageId) -> Result<()>;

    /// Get a specific message by ID
    async fn get_message(&self, queue_name: &str, message_id: &MessageId) -> Result<Option<Message>>;

    /// Delete all messages from a queue
    async fn purge_queue(&self, queue_name: &str) -> Result<u64>;

    // ==================== Maintenance ====================

    /// Clean up expired messages
    async fn cleanup_expired(&self) -> Result<u64>;
}
