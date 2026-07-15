use std::sync::Arc;

use async_trait::async_trait;
use elura_core::{Error, Result};
use elura_world::player::{InvalidationBus, InvalidationHandler, PlayerInvalidation};
use futures_util::StreamExt;

use crate::redis::{SubscriptionCounters, SubscriptionStats, reconnect_delay};

pub struct RedisInvalidationBus {
    client: redis::Client,
    channel: String,
    stats: Arc<SubscriptionCounters>,
}

impl RedisInvalidationBus {
    pub fn new(client: redis::Client, channel: impl Into<String>) -> Result<Self> {
        let channel = channel.into();
        if channel.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "player invalidation channel is empty".into(),
            ));
        }
        Ok(Self {
            client,
            channel,
            stats: Arc::new(SubscriptionCounters::default()),
        })
    }

    pub fn stats(&self) -> SubscriptionStats {
        self.stats.snapshot()
    }
}

#[async_trait]
impl InvalidationBus for RedisInvalidationBus {
    async fn publish(&self, invalidation: &PlayerInvalidation) -> Result<()> {
        invalidation.validate()?;
        let mut connection = self
            .client
            .get_connection_manager()
            .await
            .map_err(redis_error)?;
        redis::cmd("PUBLISH")
            .arg(&self.channel)
            .arg(serde_json::to_vec(invalidation)?)
            .query_async::<usize>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    async fn subscribe(&self, handler: Arc<dyn InvalidationHandler>) -> Result<()> {
        let mut backoff = std::time::Duration::ZERO;
        loop {
            let mut subscription = match self.client.get_async_pubsub().await {
                Ok(subscription) => subscription,
                Err(_) => {
                    self.stats.reconnect();
                    backoff = reconnect_delay(backoff, false);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            if subscription.subscribe(&self.channel).await.is_err() {
                self.stats.reconnect();
                backoff = reconnect_delay(backoff, false);
                tokio::time::sleep(backoff).await;
                continue;
            }
            let mut active = false;
            let mut messages = subscription.on_message();
            while let Some(message) = messages.next().await {
                active = true;
                let Ok(payload) = message.get_payload::<Vec<u8>>() else {
                    self.stats.malformed();
                    continue;
                };
                let Ok(invalidation) = serde_json::from_slice::<PlayerInvalidation>(&payload)
                else {
                    self.stats.malformed();
                    continue;
                };
                if invalidation.validate().is_err() {
                    self.stats.malformed();
                    continue;
                }
                self.stats.message();
                handler.handle(invalidation).await;
            }
            self.stats.reconnect();
            backoff = reconnect_delay(backoff, active);
            tokio::time::sleep(backoff).await;
        }
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    crate::redis::map_redis_error("Redis player invalidation", error)
}
