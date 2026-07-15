use async_trait::async_trait;
use elura_core::account_version::{
    AccountVersionKey, AccountVersionStore, MutableAccountVersionStore,
};
use elura_core::{Error, Result};
use redis::aio::ConnectionManager;

use super::{signed_version, validate_write};

#[derive(Clone)]
pub struct RedisAccountVersionStore {
    connection: ConnectionManager,
    prefix: String,
}

impl RedisAccountVersionStore {
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let connection = client.get_connection_manager().await.map_err(redis_error)?;
        Self::new(connection, prefix)
    }

    pub fn new(connection: ConnectionManager, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "account version Redis prefix is required".into(),
            ));
        }
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
