//! FlowQ Core - Core business logic for the message broker
//!
//! This crate contains the main broker implementation including:
//! - Broker: Main orchestrator
//! - Queue management
//! - Message handling

pub mod broker;

// Re-exports
pub use broker::Broker;
