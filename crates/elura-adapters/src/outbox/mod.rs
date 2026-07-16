#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "sql")]
mod sql;

#[cfg(feature = "redis")]
pub use crate::redis::idempotency::RedisIdempotencyStore;
#[cfg(feature = "redis")]
pub use redis::RedisOutbox;
#[cfg(feature = "sql")]
pub use sql::{OUTBOX_SCHEMA_VERSION, SqlOutbox};
#[cfg(feature = "admin")]
mod admin;
#[cfg(feature = "admin")]
pub use admin::{OutboxAdmin, OutboxAdminConfig};
