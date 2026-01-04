//! Queue types for FlowQ
//!
//! Defines Queue configuration and metadata types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Unique identifier for a queue
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct QueueId(pub Uuid);

impl QueueId {
    /// Create a new random QueueId
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for QueueId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for QueueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Queue configuration
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueConfig {
    /// Maximum number of messages in the queue (0 = unlimited)
    #[serde(default)]
    pub max_messages: u64,

    /// Maximum size in bytes (0 = unlimited)
    #[serde(default)]
    pub max_size_bytes: u64,

    /// Default message TTL in seconds (0 = no expiry)
    #[serde(default)]
    pub message_ttl_secs: u64,

    /// Visibility timeout in seconds (how long a message is hidden after receive)
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout_secs: u64,

    /// Maximum retry attempts before sending to DLQ
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Dead letter queue name (optional)
    pub dead_letter_queue: Option<String>,

    /// Enable deduplication
    #[serde(default)]
    pub dedup_enabled: bool,

    /// Deduplication window in seconds
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
}

fn default_visibility_timeout() -> u64 {
    30 // 30 seconds
}

fn default_max_retries() -> u32 {
    5
}

fn default_dedup_window() -> u64 {
    300 // 5 minutes
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_messages: 0,
            max_size_bytes: 0,
            message_ttl_secs: 0,
            visibility_timeout_secs: default_visibility_timeout(),
            max_retries: default_max_retries(),
            dead_letter_queue: None,
            dedup_enabled: false,
            dedup_window_secs: default_dedup_window(),
        }
    }
}

/// Queue metadata and state
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Queue {
    /// Unique queue identifier
    pub id: QueueId,

    /// Queue name (unique)
    pub name: String,

    /// Queue configuration
    pub config: QueueConfig,

    /// When the queue was created
    pub created_at: DateTime<Utc>,

    /// When the queue was last updated
    pub updated_at: DateTime<Utc>,
}

impl Queue {
    /// Create a new queue with the given name and default config
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: QueueId::new(),
            name: name.into(),
            config: QueueConfig::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new queue with custom config
    pub fn with_config(name: impl Into<String>, config: QueueConfig) -> Self {
        let now = Utc::now();
        Self {
            id: QueueId::new(),
            name: name.into(),
            config,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Queue statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct QueueStats {
    /// Total number of messages in the queue
    pub message_count: u64,

    /// Number of pending messages
    pub pending_count: u64,

    /// Number of messages being processed
    pub in_flight_count: u64,

    /// Total size of all messages in bytes
    pub size_bytes: u64,

    /// Number of active consumers
    pub consumer_count: u64,

    /// Messages published per second (recent average)
    pub publish_rate: f64,

    /// Messages consumed per second (recent average)
    pub consume_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_creation() {
        let queue = Queue::new("test-queue");
        assert_eq!(queue.name, "test-queue");
        assert_eq!(queue.config.visibility_timeout_secs, 30);
        assert_eq!(queue.config.max_retries, 5);
    }

    #[test]
    fn test_queue_with_config() {
        let config = QueueConfig {
            max_messages: 1000,
            message_ttl_secs: 3600,
            ..Default::default()
        };
        let queue = Queue::with_config("my-queue", config);
        assert_eq!(queue.config.max_messages, 1000);
        assert_eq!(queue.config.message_ttl_secs, 3600);
    }
}
