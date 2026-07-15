mod contract;
mod dispatcher;
mod memory;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "sql")]
mod sql;

pub use contract::{DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore};
pub use dispatcher::{
    Dispatcher, DispatcherConfig, DispatcherStats, EventHandler, IdempotencyStore,
    MemoryIdempotencyStore,
};
pub use memory::MemoryOutbox;
#[cfg(feature = "redis")]
pub use redis::RedisOutbox;
#[cfg(feature = "sql")]
pub use sql::{OUTBOX_SCHEMA_VERSION, SqlOutbox};
#[cfg(feature = "admin")]
mod admin;
#[cfg(feature = "admin")]
pub use admin::{OutboxAdmin, OutboxAdminConfig};
