//! Replay-protection adapters.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::replay_protection::ReplayProtectionStore;
use elura_core::{Error, Result};

use crate::redis::{
    RedisConnection, cluster_connection, map_redis_error, standalone_connection,
    validate_key_prefix,
};

#[derive(Clone)]
pub struct RedisReplayProtectionStore {
    connection: RedisConnection,
    prefix: String,
}

impl RedisReplayProtectionStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, prefix)
    }

    pub async fn connect_cluster<I, S>(nodes: I, prefix: impl Into<String>) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_connection(cluster_connection(nodes).await?, prefix)
    }

    fn from_connection(connection: RedisConnection, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_key_prefix(&prefix)?;
        Ok(Self { connection, prefix })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }
}

#[async_trait]
impl ReplayProtectionStore for RedisReplayProtectionStore {
    async fn reserve(&self, key: &str, expires_at: u64) -> Result<bool> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_secs();
        if expires_at <= now {
            return Ok(false);
        }
        let key = self.key(&format!("replay:{key}"));
        let ttl = expires_at - now;
        let mut connection = self.connection.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl)
            .query_async(&mut connection)
            .await
            .map_err(|error| map_redis_error("Redis replay", error))?;
        Ok(result.is_some())
    }
}
