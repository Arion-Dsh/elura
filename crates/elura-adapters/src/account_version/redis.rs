use async_trait::async_trait;
use elura_core::account_version::{
    AccountVersionKey, AccountVersionStore, MutableAccountVersionStore,
};
use elura_core::{Error, Result};

use super::{signed_version, validate_write};
use crate::redis::{RedisConnection, standalone_connection, validate_key_prefix};

#[derive(Clone)]
pub struct RedisAccountVersionStore {
    connection: RedisConnection,
    prefix: String,
}

impl RedisAccountVersionStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, prefix)
    }

    fn from_connection(connection: RedisConnection, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_key_prefix(&prefix)?;
        Ok(Self { connection, prefix })
    }

    fn key(&self, key: AccountVersionKey) -> String {
        format!(
            "{}:account-version:{}:{}:{}",
            self.prefix, key.region_id, key.realm_id, key.user_id
        )
    }
}

#[async_trait]
impl AccountVersionStore for RedisAccountVersionStore {
    async fn current(&self, key: AccountVersionKey) -> Result<Option<u64>> {
        key.validate()?;
        let mut connection = self.connection.clone();
        let value: Option<i64> = redis::cmd("GET")
            .arg(self.key(key))
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        value
            .map(|version| {
                u64::try_from(version)
                    .ok()
                    .filter(|version| *version != 0)
                    .ok_or_else(|| Error::Internal("invalid Redis account version".into()))
            })
            .transpose()
    }
}

#[async_trait]
impl MutableAccountVersionStore for RedisAccountVersionStore {
    async fn set(&self, key: AccountVersionKey, version: u64) -> Result<()> {
        validate_write(key, version)?;
        let version = signed_version(version)?;
        let mut connection = self.connection.clone();
        redis::cmd("SET")
            .arg(self.key(key))
            .arg(version)
            .query_async::<()>(&mut connection)
            .await
            .map_err(redis_error)
    }

    async fn increment(&self, key: AccountVersionKey) -> Result<u64> {
        key.validate()?;
        let mut connection = self.connection.clone();
        let version: i64 = redis::cmd("INCR")
            .arg(self.key(key))
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        u64::try_from(version)
            .ok()
            .filter(|version| *version != 0)
            .ok_or_else(|| Error::Internal("invalid Redis account version".into()))
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    crate::redis::map_redis_error("Redis account version", error)
}
