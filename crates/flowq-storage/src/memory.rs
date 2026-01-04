//! In-memory storage backend
//!
//! Fast, non-persistent storage for development and testing.
//! All data is lost when the process exits.

use std::collections::VecDeque;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use flowq_types::{Error, Message, MessageId, MessageStatus, Queue, QueueStats, Result};
use tracing::{debug, info};

use crate::traits::StorageEngine;

/// Internal queue data structure
struct QueueData {
    /// Queue metadata
    queue: Queue,
    /// Messages in the queue (pending)
    messages: VecDeque<Message>,
    /// Messages currently being processed (delivered but not acked)
    in_flight: DashMap<MessageId, Message>,
}

impl QueueData {
    fn new(queue: Queue) -> Self {
        Self {
            queue,
            messages: VecDeque::new(),
            in_flight: DashMap::new(),
        }
    }
}

/// In-memory storage implementation
pub struct MemoryStorage {
    /// Queues stored by name
    queues: DashMap<String, QueueData>,
}

impl MemoryStorage {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        info!("Initializing in-memory storage");
        Self {
            queues: DashMap::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    // ==================== Queue Operations ====================

    async fn create_queue(&self, queue: Queue) -> Result<Queue> {
        let name = queue.name.clone();

        if self.queues.contains_key(&name) {
            return Err(Error::QueueAlreadyExists(name));
        }

        let queue_clone = queue.clone();
        self.queues.insert(name.clone(), QueueData::new(queue));
        info!(queue = %name, "Queue created");

        Ok(queue_clone)
    }

    async fn get_queue(&self, name: &str) -> Result<Option<Queue>> {
        Ok(self.queues.get(name).map(|q| q.queue.clone()))
    }

    async fn list_queues(&self) -> Result<Vec<Queue>> {
        Ok(self.queues.iter().map(|q| q.queue.clone()).collect())
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        match self.queues.remove(name) {
            Some(_) => {
                info!(queue = %name, "Queue deleted");
                Ok(())
            }
            None => Err(Error::QueueNotFound(name.to_string())),
        }
    }

    async fn get_queue_stats(&self, name: &str) -> Result<QueueStats> {
        let queue_data = self
            .queues
            .get(name)
            .ok_or_else(|| Error::QueueNotFound(name.to_string()))?;

        let pending_count = queue_data.messages.len() as u64;
        let in_flight_count = queue_data.in_flight.len() as u64;
        let size_bytes: u64 = queue_data
            .messages
            .iter()
            .map(|m| m.body.len() as u64)
            .sum();

        Ok(QueueStats {
            message_count: pending_count + in_flight_count,
            pending_count,
            in_flight_count,
            size_bytes,
            consumer_count: 0, // TODO: Track consumers
            publish_rate: 0.0, // TODO: Calculate rate
            consume_rate: 0.0,
        })
    }

    // ==================== Message Operations ====================

    async fn push_message(&self, queue_name: &str, message: Message) -> Result<MessageId> {
        let mut queue_data = self
            .queues
            .get_mut(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        // Check queue limits
        if queue_data.queue.config.max_messages > 0
            && queue_data.messages.len() as u64 >= queue_data.queue.config.max_messages
        {
            return Err(Error::QueueFull(queue_name.to_string()));
        }

        let message_id = message.id.clone();
        queue_data.messages.push_back(message);

        debug!(
            queue = %queue_name,
            message_id = %message_id,
            "Message pushed"
        );

        Ok(message_id)
    }

    async fn pop_message(&self, queue_name: &str) -> Result<Option<Message>> {
        let mut queue_data = self
            .queues
            .get_mut(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        // Find first non-expired message
        while let Some(mut message) = queue_data.messages.pop_front() {
            // Skip expired messages
            if message.is_expired() {
                debug!(
                    queue = %queue_name,
                    message_id = %message.id,
                    "Skipping expired message"
                );
                continue;
            }

            // Update message status
            message.status = MessageStatus::Delivered;
            message.delivery_count += 1;

            // Move to in-flight
            let message_clone = message.clone();
            queue_data.in_flight.insert(message.id.clone(), message);

            debug!(
                queue = %queue_name,
                message_id = %message_clone.id,
                delivery_count = message_clone.delivery_count,
                "Message popped"
            );

            return Ok(Some(message_clone));
        }

        Ok(None)
    }

    async fn pop_messages(&self, queue_name: &str, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);

        for _ in 0..max {
            match self.pop_message(queue_name).await? {
                Some(msg) => messages.push(msg),
                None => break,
            }
        }

        Ok(messages)
    }

    async fn peek_message(&self, queue_name: &str) -> Result<Option<Message>> {
        let queue_data = self
            .queues
            .get(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        Ok(queue_data.messages.front().cloned())
    }

    async fn ack_message(&self, queue_name: &str, message_id: &MessageId) -> Result<()> {
        let queue_data = self
            .queues
            .get(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        match queue_data.in_flight.remove(message_id) {
            Some(_) => {
                debug!(
                    queue = %queue_name,
                    message_id = %message_id,
                    "Message acknowledged"
                );
                Ok(())
            }
            None => Err(Error::MessageNotFound(message_id.to_string())),
        }
    }

    async fn nack_message(&self, queue_name: &str, message_id: &MessageId) -> Result<()> {
        let mut queue_data = self
            .queues
            .get_mut(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        match queue_data.in_flight.remove(message_id) {
            Some((_, mut message)) => {
                // Check retry limit
                if message.delivery_count >= queue_data.queue.config.max_retries {
                    // TODO: Move to DLQ
                    message.status = MessageStatus::Failed;
                    debug!(
                        queue = %queue_name,
                        message_id = %message_id,
                        "Message exceeded max retries, marking as failed"
                    );
                } else {
                    // Return to queue
                    message.status = MessageStatus::Pending;
                    queue_data.messages.push_front(message);
                    debug!(
                        queue = %queue_name,
                        message_id = %message_id,
                        "Message returned to queue"
                    );
                }
                Ok(())
            }
            None => Err(Error::MessageNotFound(message_id.to_string())),
        }
    }

    async fn get_message(&self, queue_name: &str, message_id: &MessageId) -> Result<Option<Message>> {
        let queue_data = self
            .queues
            .get(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        // Check in-flight first
        if let Some(msg) = queue_data.in_flight.get(message_id) {
            return Ok(Some(msg.clone()));
        }

        // Check pending messages
        Ok(queue_data
            .messages
            .iter()
            .find(|m| &m.id == message_id)
            .cloned())
    }

    async fn purge_queue(&self, queue_name: &str) -> Result<u64> {
        let mut queue_data = self
            .queues
            .get_mut(queue_name)
            .ok_or_else(|| Error::QueueNotFound(queue_name.to_string()))?;

        let count = queue_data.messages.len() as u64;
        queue_data.messages.clear();
        queue_data.in_flight.clear();

        info!(queue = %queue_name, count = count, "Queue purged");
        Ok(count)
    }

    // ==================== Maintenance ====================

    async fn cleanup_expired(&self) -> Result<u64> {
        let mut total_cleaned = 0u64;
        let now = Utc::now();

        for mut queue_data in self.queues.iter_mut() {
            let before_count = queue_data.messages.len();
            queue_data.messages.retain(|m| {
                m.expires_at.map(|exp| now <= exp).unwrap_or(true)
            });
            let removed = before_count - queue_data.messages.len();
            total_cleaned += removed as u64;
        }

        if total_cleaned > 0 {
            debug!(count = total_cleaned, "Cleaned up expired messages");
        }

        Ok(total_cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_get_queue() {
        let storage = MemoryStorage::new();
        let queue = Queue::new("test-queue");

        storage.create_queue(queue.clone()).await.unwrap();

        let retrieved = storage.get_queue("test-queue").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "test-queue");
    }

    #[tokio::test]
    async fn test_push_and_pop_message() {
        let storage = MemoryStorage::new();
        storage.create_queue(Queue::new("test")).await.unwrap();

        let msg = Message::new("Hello, World!");
        let msg_id = storage.push_message("test", msg).await.unwrap();

        let received = storage.pop_message("test").await.unwrap();
        assert!(received.is_some());

        let received = received.unwrap();
        assert_eq!(received.id, msg_id);
        assert_eq!(received.body_as_str(), Some("Hello, World!"));
        assert_eq!(received.delivery_count, 1);
    }

    #[tokio::test]
    async fn test_ack_message() {
        let storage = MemoryStorage::new();
        storage.create_queue(Queue::new("test")).await.unwrap();

        let msg = Message::new("test");
        storage.push_message("test", msg).await.unwrap();

        let received = storage.pop_message("test").await.unwrap().unwrap();
        storage.ack_message("test", &received.id).await.unwrap();

        // Message should be gone
        let stats = storage.get_queue_stats("test").await.unwrap();
        assert_eq!(stats.message_count, 0);
    }

    #[tokio::test]
    async fn test_nack_message() {
        let storage = MemoryStorage::new();
        storage.create_queue(Queue::new("test")).await.unwrap();

        let msg = Message::new("test");
        storage.push_message("test", msg).await.unwrap();

        let received = storage.pop_message("test").await.unwrap().unwrap();
        storage.nack_message("test", &received.id).await.unwrap();

        // Message should be back in queue
        let stats = storage.get_queue_stats("test").await.unwrap();
        assert_eq!(stats.pending_count, 1);
    }
}
