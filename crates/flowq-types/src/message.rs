//! Message types for FlowQ
//!
//! Defines the core Message struct and related types.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;

/// Unique identifier for a message
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct MessageId(pub Uuid);

impl MessageId {
    /// Create a new random MessageId
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Status of a message in the queue
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MessageStatus {
    /// Message is waiting to be consumed
    Pending,
    /// Message has been delivered to a consumer (awaiting ack)
    Delivered,
    /// Message has been acknowledged
    Acked,
    /// Message processing failed
    Failed,
}

impl Default for MessageStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// A message in the queue
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Message {
    /// Unique message identifier
    pub id: MessageId,

    /// Message body (raw bytes)
    #[serde(with = "bytes_serde")]
    #[schema(value_type = String)]
    pub body: Bytes,

    /// Content type (e.g., "application/json")
    pub content_type: Option<String>,

    /// Custom attributes/headers
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Message priority (1-10, higher = more important)
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Current status
    #[serde(default)]
    pub status: MessageStatus,

    /// Number of delivery attempts
    #[serde(default)]
    pub delivery_count: u32,

    /// When the message was created
    pub created_at: DateTime<Utc>,

    /// When the message expires (optional)
    pub expires_at: Option<DateTime<Utc>>,

    /// Deduplication ID (optional)
    pub dedup_id: Option<String>,
}

fn default_priority() -> u8 {
    5
}

impl Message {
    /// Create a new message with the given body
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            id: MessageId::new(),
            body: body.into(),
            content_type: None,
            attributes: HashMap::new(),
            priority: 5,
            status: MessageStatus::Pending,
            delivery_count: 0,
            created_at: Utc::now(),
            expires_at: None,
            dedup_id: None,
        }
    }

    /// Create a new message with JSON content
    pub fn json<T: Serialize>(data: &T) -> Result<Self, serde_json::Error> {
        let body = serde_json::to_vec(data)?;
        let mut msg = Self::new(body);
        msg.content_type = Some("application/json".to_string());
        Ok(msg)
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set priority (clamped to 1-10)
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority.clamp(1, 10);
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Set expiration time
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Set deduplication ID
    pub fn with_dedup_id(mut self, dedup_id: impl Into<String>) -> Self {
        self.dedup_id = Some(dedup_id.into());
        self
    }

    /// Check if the message has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| Utc::now() > exp)
            .unwrap_or(false)
    }

    /// Get the body as a string (if valid UTF-8)
    pub fn body_as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    /// Deserialize the body as JSON
    pub fn body_as_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

/// Custom serialization for Bytes (as base64 or raw)
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // For JSON, we serialize as string if it's valid UTF-8, otherwise base64
        if let Ok(s) = std::str::from_utf8(bytes) {
            s.serialize(serializer)
        } else {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            encoded.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Bytes::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new("Hello, World!");
        assert_eq!(msg.body_as_str(), Some("Hello, World!"));
        assert_eq!(msg.priority, 5);
        assert_eq!(msg.status, MessageStatus::Pending);
    }

    #[test]
    fn test_message_builder() {
        let msg = Message::new("test")
            .with_priority(8)
            .with_content_type("text/plain")
            .with_attribute("key", "value");

        assert_eq!(msg.priority, 8);
        assert_eq!(msg.content_type, Some("text/plain".to_string()));
        assert_eq!(msg.attributes.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_json_message() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let msg = Message::json(&data).unwrap();
        assert_eq!(msg.content_type, Some("application/json".to_string()));

        let parsed: TestData = msg.body_as_json().unwrap();
        assert_eq!(parsed, data);
    }
}
