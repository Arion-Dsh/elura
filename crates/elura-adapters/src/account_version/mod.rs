mod memory;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "sql")]
mod sql;

pub use memory::MemoryAccountVersionStore;
#[cfg(feature = "redis")]
pub use redis::RedisAccountVersionStore;
#[cfg(feature = "sql")]
pub use sql::{ACCOUNT_VERSION_SCHEMA_VERSION, SqlAccountVersionStore};

use elura_core::account_version::AccountVersionKey;
use elura_core::{Error, Result};

fn validate_write(key: AccountVersionKey, version: u64) -> Result<()> {
    key.validate()?;
    if version == 0 {
        return Err(Error::InvalidConfig(
            "account version must be positive".into(),
        ));
    }
    Ok(())
}

#[cfg(any(feature = "redis", feature = "sql"))]
fn signed_version(version: u64) -> Result<i64> {
    i64::try_from(version)
        .map_err(|_| Error::InvalidConfig("account version exceeds storage range".into()))
}
