//! Error types for FlowQ
//!
//! Defines all error types used throughout the application.

use thiserror::Error;

/// Main error type for FlowQ operations
#[derive(Error, Debug)]
pub enum Error {
    /// Queue not found
    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    /// Queue already exists
    #[error("Queue already exists: {0}")]
    QueueAlreadyExists(String),

    /// Message not found
    #[error("Message not found: {0}")]
    MessageNotFound(String),

    /// Queue is full
    #[error("Queue is full: {0}")]
    QueueFull(String),

    /// Queue is empty
    #[error("Queue is empty: {0}")]
    QueueEmpty(String),

    /// Invalid message format
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for FlowQ operations
pub type Result<T> = std::result::Result<T, Error>;
