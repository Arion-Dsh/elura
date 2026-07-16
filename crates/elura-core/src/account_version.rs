use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::session::Identity;
use crate::{Error, Result};

mod memory;

pub use memory::MemoryAccountVersionStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountVersionKey {
    pub region_id: u32,
    pub realm_id: u32,
    pub user_id: i64,
}

impl AccountVersionKey {
    pub fn new(region_id: u32, realm_id: u32, user_id: i64) -> Result<Self> {
        let key = Self {
            region_id,
            realm_id,
            user_id,
        };
        key.validate()?;
        Ok(key)
    }

    pub fn from_identity(identity: &Identity) -> Self {
        Self {
            region_id: identity.region_id,
            realm_id: identity.realm_id,
            user_id: identity.user_id,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0 || self.realm_id == 0 || self.user_id <= 0 {
            return Err(Error::InvalidConfig(
                "account version key requires region, realm, and user".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait AccountVersionStore: Send + Sync + 'static {
    async fn current(&self, key: AccountVersionKey) -> Result<Option<u64>>;
}

#[async_trait]
pub trait MutableAccountVersionStore: AccountVersionStore {
    async fn set(&self, key: AccountVersionKey, version: u64) -> Result<()>;
    async fn increment(&self, key: AccountVersionKey) -> Result<u64>;
}
