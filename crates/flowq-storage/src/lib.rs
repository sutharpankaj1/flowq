//! FlowQ Storage - Storage backends for the message broker
//!
//! This crate provides pluggable storage implementations.
//! Currently supports:
//! - In-memory storage (default, for development/testing)
//!
//! Future:
//! - SQLite
//! - PostgreSQL

pub mod traits;

#[cfg(feature = "memory")]
pub mod memory;

// Re-exports
pub use traits::StorageEngine;

#[cfg(feature = "memory")]
pub use memory::MemoryStorage;
