use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::Result;
use elura_core::session::PlayerKey;

use super::cache::{PlayerCache, PlayerSnapshot};
use crate::{ContextKey, Next, WorldContext, WorldMiddleware};

#[async_trait]
pub trait PlayerLoader<T>: Send + Sync + 'static {
    async fn load(&self, context: &WorldContext, player: PlayerKey) -> Result<PlayerSnapshot<T>>;
}

pub struct CachedPlayerLoader<T> {
    cache: Arc<PlayerCache<T>>,
    source: Arc<dyn PlayerLoader<T>>,
}

impl<T> CachedPlayerLoader<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(cache: Arc<PlayerCache<T>>, source: Arc<dyn PlayerLoader<T>>) -> Self {
        Self { cache, source }
    }
}

#[async_trait]
impl<T> PlayerLoader<T> for CachedPlayerLoader<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn load(&self, context: &WorldContext, player: PlayerKey) -> Result<PlayerSnapshot<T>> {
        self.cache
            .load(player, |player| {
                let source = self.source.clone();
                let context = context.clone();
                async move { source.load(&context, player).await }
            })
            .await
    }
}

pub struct PlayerStateMiddleware<T> {
    key: ContextKey<PlayerSnapshot<T>>,
    loader: Arc<dyn PlayerLoader<T>>,
}

impl<T> PlayerStateMiddleware<T> {
    pub fn new(key: ContextKey<PlayerSnapshot<T>>, loader: Arc<dyn PlayerLoader<T>>) -> Self {
        Self { key, loader }
    }
}

#[async_trait]
impl<T> WorldMiddleware for PlayerStateMiddleware<T>
where
    T: Send + Sync + 'static,
{
    async fn handle(&self, context: WorldContext, payload: Bytes, next: Next<'_>) -> Result<Bytes> {
        let snapshot = self
            .loader
            .load(&context, context.identity.player_key())
            .await?;
        next.run(context.with_value(&self.key, snapshot), payload)
            .await
    }
}
