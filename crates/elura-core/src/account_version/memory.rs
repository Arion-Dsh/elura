use std::collections::HashMap;
use std::sync::RwLock;

use crate::account_version::{AccountVersionKey, AccountVersionStore, MutableAccountVersionStore};
use crate::{Error, Result};
use async_trait::async_trait;

fn validate_write(key: AccountVersionKey, version: u64) -> Result<()> {
    key.validate()?;
    if version == 0 {
        return Err(Error::InvalidConfig(
            "account version must be positive".into(),
        ));
    }
    Ok(())
}

#[derive(Default)]
pub struct MemoryAccountVersionStore {
    versions: RwLock<HashMap<AccountVersionKey, u64>>,
}

#[async_trait]
impl AccountVersionStore for MemoryAccountVersionStore {
    async fn current(&self, key: AccountVersionKey) -> Result<Option<u64>> {
        key.validate()?;
        Ok(self
            .versions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .copied())
    }
}

#[async_trait]
impl MutableAccountVersionStore for MemoryAccountVersionStore {
    async fn set(&self, key: AccountVersionKey, version: u64) -> Result<()> {
        validate_write(key, version)?;
        self.versions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, version);
        Ok(())
    }

    async fn increment(&self, key: AccountVersionKey) -> Result<u64> {
        key.validate()?;
        let mut versions = self
            .versions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let next = versions
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Internal("account version overflow".into()))?;
        versions.insert(key, next);
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sets_and_atomically_increments_versions() {
        let store = MemoryAccountVersionStore::default();
        let key = AccountVersionKey::new(1, 2, 3).unwrap();
        assert_eq!(store.current(key).await.unwrap(), None);
        store.set(key, 4).await.unwrap();
        assert_eq!(store.increment(key).await.unwrap(), 5);
        assert_eq!(store.current(key).await.unwrap(), Some(5));
    }
}
