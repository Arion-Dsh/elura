use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::{Error, Result};
use redis::aio::ConnectionManager;
use uuid::Uuid;

use crate::outbox::IdempotencyStore;

#[derive(Clone)]
pub struct RedisIdempotencyStore {
    connection: ConnectionManager,
    prefix: String,
}

impl RedisIdempotencyStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let connection = client.get_connection_manager().await.map_err(redis_error)?;
        Self::new(connection, prefix)
    }

    pub fn new(connection: ConnectionManager, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "Redis idempotency prefix is empty".into(),
            ));
        }
        Ok(Self { connection, prefix })
    }

    fn key(&self, id: Uuid) -> String {
        format!("{}{}", self.prefix, id)
    }
}

#[async_trait]
impl IdempotencyStore for RedisIdempotencyStore {
    async fn seen(&self, id: Uuid) -> Result<bool> {
        let mut connection = self.connection.clone();
        redis::cmd("EXISTS")
            .arg(self.key(id))
            .query_async::<u64>(&mut connection)
            .await
            .map(|count| count > 0)
            .map_err(redis_error)
    }

    async fn mark(&self, id: Uuid, expires_at: SystemTime) -> Result<()> {
        let now = SystemTime::now();
        let ttl = expires_at
            .duration_since(now)
            .unwrap_or_else(|_| std::time::Duration::from_millis(1))
            .as_millis()
            .max(1);
        let value = now
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_nanos();
        let mut connection = self.connection.clone();
        redis::cmd("SET")
            .arg(self.key(id))
            .arg(value)
            .arg("NX")
            .arg("PX")
            .arg(ttl)
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    super::map_redis_error("Redis idempotency", error)
}
