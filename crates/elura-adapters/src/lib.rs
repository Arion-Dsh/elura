//! Infrastructure adapters grouped by backing service and runtime concern.

#![deny(rustdoc::broken_intra_doc_links)]

pub mod account_version;
#[cfg(feature = "redis")]
pub mod admission;
pub mod discovery;
#[cfg(feature = "redis")]
pub mod distributed;
#[cfg(feature = "kubernetes")]
pub mod kubernetes;
pub mod outbox;
#[cfg(feature = "redis")]
pub mod player_invalidation;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sql")]
mod sql_migration;
