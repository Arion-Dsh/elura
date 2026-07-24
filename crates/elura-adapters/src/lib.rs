//! Infrastructure adapters grouped by application capability.

#![deny(rustdoc::broken_intra_doc_links)]

pub mod account_version;
#[cfg(feature = "redis")]
pub mod admission;
#[cfg(feature = "discovery")]
pub mod discovery;
#[cfg(feature = "redis")]
mod distributed;
#[cfg(feature = "redis")]
pub mod invalidation;
#[cfg(feature = "kubernetes")]
pub mod kubernetes;
#[cfg(feature = "redis")]
pub mod otp;
pub mod outbox;
#[cfg(feature = "redis")]
mod player_invalidation;
#[cfg(feature = "redis")]
pub mod presence;
#[cfg(feature = "redis")]
pub mod push;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "redis")]
pub mod registration;
#[cfg(feature = "redis")]
pub mod replay_protection;
#[cfg(feature = "redis")]
pub mod session_control;
#[cfg(feature = "sql")]
mod sql_migration;
