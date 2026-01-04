//! FlowQ Types - Core domain types for the message broker
//!
//! This crate contains all shared types used across FlowQ components.

pub mod error;
pub mod message;
pub mod queue;

// Re-export commonly used types
pub use error::{Error, Result};
pub use message::{Message, MessageId, MessageStatus};
pub use queue::{Queue, QueueConfig, QueueId, QueueStats};
