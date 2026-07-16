use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::{Error, Result};
use uuid::Uuid;

use elura_runtime::outbox::IdempotencyStore;

use super::{RedisConnection, standalone_connection, validate_key_prefix};

#[derive(Clone)]
pub struct RedisIdempotencyStore {
    connection: RedisConnection,
    prefix: String,
}

impl RedisIdempotencyStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, prefix)
    }

    fn from_connection(connection: RedisConnection, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_key_prefix(&prefix)?;
        Ok(Self { connection, prefix })
    }

    fn key(&self, id: Uuid) -> String {
        format!("{}:outbox:idempotency:{id}", self.prefix)
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
