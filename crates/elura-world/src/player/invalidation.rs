use std::sync::Arc;

use async_trait::async_trait;
use elura_core::session::PlayerKey;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

use super::cache::{PlayerCache, PlayerSnapshot};

pub const INVALIDATION_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerInvalidation {
    pub schema_version: u32,
    pub namespace: String,
    pub player: PlayerKey,
    pub minimum_version: u64,
    pub source_id: String,
}

impl PlayerInvalidation {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != INVALIDATION_SCHEMA_VERSION
            || self.namespace.is_empty()
            || self.player.validate().is_err()
            || self.source_id.is_empty()
        {
            return Err(Error::InvalidConfig(
                "invalid player cache invalidation".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait InvalidationHandler: Send + Sync + 'static {
    async fn handle(&self, invalidation: PlayerInvalidation);
}

#[async_trait]
pub trait InvalidationBus: Send + Sync + 'static {
    async fn publish(&self, invalidation: &PlayerInvalidation) -> Result<()>;
    async fn subscribe(&self, handler: Arc<dyn InvalidationHandler>) -> Result<()>;
}

pub struct PlayerCacheSynchronizer<T> {
    cache: Arc<PlayerCache<T>>,
    bus: Arc<dyn InvalidationBus>,
    namespace: Arc<str>,
    source_id: Arc<str>,
}

impl<T> PlayerCacheSynchronizer<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(
        cache: Arc<PlayerCache<T>>,
        bus: Arc<dyn InvalidationBus>,
        namespace: impl Into<Arc<str>>,
        source_id: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let source_id = source_id.into();
        if namespace.is_empty() || source_id.is_empty() {
            return Err(Error::InvalidConfig(
                "player cache namespace and source ID are required".into(),
            ));
        }
        Ok(Self {
            cache,
            bus,
            namespace,
            source_id,
        })
    }

    pub async fn store_committed(
        &self,
        player: PlayerKey,
        snapshot: PlayerSnapshot<T>,
    ) -> Result<()> {
        self.cache.store(player, snapshot.clone()).await?;
        self.bus
            .publish(&PlayerInvalidation {
                schema_version: INVALIDATION_SCHEMA_VERSION,
                namespace: self.namespace.to_string(),
                player,
                minimum_version: snapshot.version,
                source_id: self.source_id.to_string(),
            })
            .await
    }

    pub async fn delete_committed(&self, player: PlayerKey) -> Result<()> {
        player.validate()?;
        self.cache.invalidate(player, 0).await;
        self.bus
            .publish(&PlayerInvalidation {
                schema_version: INVALIDATION_SCHEMA_VERSION,
                namespace: self.namespace.to_string(),
                player,
                minimum_version: 0,
                source_id: self.source_id.to_string(),
            })
            .await
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.bus.clone().subscribe(self).await
    }
}

#[async_trait]
impl<T> InvalidationHandler for PlayerCacheSynchronizer<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn handle(&self, invalidation: PlayerInvalidation) {
        if invalidation.validate().is_ok()
            && invalidation.namespace == self.namespace.as_ref()
            && invalidation.source_id != self.source_id.as_ref()
        {
            self.cache
                .invalidate(invalidation.player, invalidation.minimum_version)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::player::PlayerCacheConfig;

    fn player(realm_id: u32, user_id: i64) -> PlayerKey {
        PlayerKey::new(1, realm_id, user_id).unwrap()
    }

    #[derive(Default)]
    struct RecordingBus {
        published: Mutex<Vec<PlayerInvalidation>>,
    }

    #[async_trait]
    impl InvalidationBus for RecordingBus {
        async fn publish(&self, invalidation: &PlayerInvalidation) -> Result<()> {
            self.published
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(invalidation.clone());
            Ok(())
        }

        async fn subscribe(&self, _handler: Arc<dyn InvalidationHandler>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn publishes_commits_and_applies_newer_remote_versions() {
        let cache = Arc::new(PlayerCache::new(PlayerCacheConfig::default()).unwrap());
        let bus = Arc::new(RecordingBus::default());
        let synchronizer =
            PlayerCacheSynchronizer::new(cache.clone(), bus.clone(), "players", "world-a").unwrap();
        synchronizer
            .store_committed(
                player(1, 42),
                PlayerSnapshot {
                    value: String::from("state"),
                    version: 7,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            bus.published
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())[0]
                .minimum_version,
            7
        );
        synchronizer
            .handle(PlayerInvalidation {
                schema_version: INVALIDATION_SCHEMA_VERSION,
                namespace: "players".into(),
                player: player(1, 42),
                minimum_version: 7,
                source_id: "world-b".into(),
            })
            .await;
        assert_eq!(cache.len().await, 1);
        synchronizer
            .handle(PlayerInvalidation {
                schema_version: INVALIDATION_SCHEMA_VERSION,
                namespace: "players".into(),
                player: player(1, 42),
                minimum_version: 8,
                source_id: "world-b".into(),
            })
            .await;
        assert!(cache.is_empty().await);
    }
}
