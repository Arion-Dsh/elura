use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::push::{
    PushHandler, PushReceipt, PushRequest, PushTarget, PushTargetResolver, PushTransport,
};
use elura_core::{Error, Result};
use elura_gateway::presence::OnlineDirectoryTargetResolver;
use redis::AsyncCommands;
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::distributed::RedisOnlineDirectory;

use super::{RedisConnection, cluster_connection, standalone_connection};
use super::{SubscriptionCounters, SubscriptionStats, reconnect_delay};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct RedisStreamPushConfig {
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub max_len: usize,
    pub claim_idle: Duration,
    pub read_block: Duration,
    pub batch_size: usize,
}

impl RedisStreamPushConfig {
    fn normalize(mut self, gateway_id: &str) -> Result<Self> {
        if self.stream.is_empty() {
            self.stream = "gateway:push-stream".into();
        }
        if self.group.is_empty() {
            self.group = format!("elura-gateway-{gateway_id}");
        }
        if self.consumer.is_empty() {
            self.consumer = gateway_id.to_owned();
        }
        if self.max_len == 0 {
            self.max_len = 100_000;
        }
        if self.claim_idle.is_zero() {
            self.claim_idle = Duration::from_secs(30);
        }
        if self.read_block.is_zero() {
            self.read_block = Duration::from_secs(1);
        }
        if self.batch_size == 0 {
            self.batch_size = 128;
        }
        if self.stream.trim().is_empty()
            || self.group.trim().is_empty()
            || self.consumer.trim().is_empty()
            || self.batch_size > 4096
        {
            return Err(Error::InvalidConfig(
                "invalid Redis Stream Push config".into(),
            ));
        }
        Ok(self)
    }
}

impl Default for RedisStreamPushConfig {
    fn default() -> Self {
        Self {
            stream: String::new(),
            group: String::new(),
            consumer: String::new(),
            max_len: 0,
            claim_idle: Duration::ZERO,
            read_block: Duration::ZERO,
            batch_size: 0,
        }
    }
}

pub struct RedisStreamPushBus {
    connection: RedisConnection,
    key_prefix: String,
    resolver: Arc<dyn PushTargetResolver>,
    gateway_id: String,
    config: RedisStreamPushConfig,
    stats: Arc<SubscriptionCounters>,
}

impl RedisStreamPushBus {
    pub fn new(
        directory: Arc<RedisOnlineDirectory>,
        gateway_id: impl Into<String>,
        config: RedisStreamPushConfig,
    ) -> Result<Self> {
        let connection = directory.connection();
        let key_prefix = directory.prefix().to_owned();
        let resolver = Arc::new(OnlineDirectoryTargetResolver::new(directory));
        Self::build(connection, key_prefix, resolver, gateway_id, config)
    }

    pub async fn connect(
        url: &str,
        key_prefix: impl Into<String>,
        resolver: Arc<dyn PushTargetResolver>,
        gateway_id: impl Into<String>,
        config: RedisStreamPushConfig,
    ) -> Result<Self> {
        Self::from_connection(
            standalone_connection(url).await?,
            key_prefix,
            resolver,
            gateway_id,
            config,
        )
    }

    pub async fn connect_cluster<I, S>(
        nodes: I,
        key_prefix: impl Into<String>,
        resolver: Arc<dyn PushTargetResolver>,
        gateway_id: impl Into<String>,
        config: RedisStreamPushConfig,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_connection(
            cluster_connection(nodes).await?,
            key_prefix,
            resolver,
            gateway_id,
            config,
        )
    }

    fn from_connection(
        connection: RedisConnection,
        key_prefix: impl Into<String>,
        resolver: Arc<dyn PushTargetResolver>,
        gateway_id: impl Into<String>,
        config: RedisStreamPushConfig,
    ) -> Result<Self> {
        let key_prefix = connection.atomic_prefix(&key_prefix.into())?;
        Self::build(connection, key_prefix, resolver, gateway_id, config)
    }

    fn build(
        connection: RedisConnection,
        key_prefix: impl Into<String>,
        resolver: Arc<dyn PushTargetResolver>,
        gateway_id: impl Into<String>,
        config: RedisStreamPushConfig,
    ) -> Result<Self> {
        let gateway_id = gateway_id.into();
        let key_prefix = key_prefix.into();
        if gateway_id.trim().is_empty() || key_prefix.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "Redis Stream Push gateway ID or key prefix is empty".into(),
            ));
        }
        Ok(Self {
            connection,
            key_prefix,
            resolver,
            config: config.normalize(&gateway_id)?,
            gateway_id,
            stats: Arc::new(SubscriptionCounters::default()),
        })
    }

    pub fn stats(&self) -> SubscriptionStats {
        self.stats.snapshot()
    }

    fn streams(&self) -> [String; 2] {
        [
            self.key(&self.config.stream),
            self.key(&format!("{}:{}", self.config.stream, self.gateway_id)),
        ]
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.key_prefix)
    }

    async fn publish_streams(&self, request: &PushRequest) -> Result<Vec<String>> {
        let gateways = self.resolver.resolve_gateways(request).await?;
        if matches!(request.target, PushTarget::Realm) {
            Ok(vec![self.key(&self.config.stream)])
        } else {
            Ok(gateways
                .into_iter()
                .map(|gateway| self.key(&format!("{}:{gateway}", self.config.stream)))
                .collect())
        }
    }

    async fn ensure_groups(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        for stream in self.streams() {
            let result = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(&self.config.group)
                .arg("0")
                .arg("MKSTREAM")
                .query_async::<String>(&mut connection)
                .await;
            if let Err(error) = result
                && !error.to_string().contains("BUSYGROUP")
            {
                return Err(redis_error(error));
            }
        }
        Ok(())
    }

    async fn claim_pending(&self, handler: &Arc<dyn PushHandler>) -> Result<()> {
        let mut connection = self.connection.clone();
        for stream in self.streams() {
            let reply: StreamAutoClaimReply = connection
                .xautoclaim_options(
                    &stream,
                    &self.config.group,
                    &self.config.consumer,
                    self.config.claim_idle.as_millis(),
                    "0-0",
                    StreamAutoClaimOptions::default().count(self.config.batch_size),
                )
                .await
                .map_err(redis_error)?;
            self.deliver(&mut connection, &stream, reply.claimed, handler)
                .await?;
        }
        Ok(())
    }

    async fn read_new(&self, handler: &Arc<dyn PushHandler>) -> Result<()> {
        let streams = self.streams();
        let options = StreamReadOptions::default()
            .group(&self.config.group, &self.config.consumer)
            .count(self.config.batch_size)
            .block(self.config.read_block.as_millis() as usize);
        let mut connection = self.connection.clone();
        let reply: StreamReadReply = connection
            .xread_options(&streams, &[">", ">"], &options)
            .await
            .map_err(redis_error)?;
        for key in reply.keys {
            self.deliver(&mut connection, &key.key, key.ids, handler)
                .await?;
        }
        Ok(())
    }

    async fn deliver(
        &self,
        connection: &mut RedisConnection,
        stream: &str,
        messages: Vec<StreamId>,
        handler: &Arc<dyn PushHandler>,
    ) -> Result<()> {
        for message in messages {
            let Some(payload) = message.get::<Vec<u8>>("payload") else {
                self.stats.malformed();
                connection
                    .xack::<_, _, _, u64>(stream, &self.config.group, &[&message.id])
                    .await
                    .map_err(redis_error)?;
                continue;
            };
            let request = match serde_json::from_slice::<PushRequest>(&payload) {
                Ok(request) if request.validate().is_ok() => request,
                _ => {
                    self.stats.malformed();
                    connection
                        .xack::<_, _, _, u64>(stream, &self.config.group, &[&message.id])
                        .await
                        .map_err(redis_error)?;
                    continue;
                }
            };
            self.stats.message();
            if handler.deliver(request).await.is_ok() {
                connection
                    .xack::<_, _, _, u64>(stream, &self.config.group, &[&message.id])
                    .await
                    .map_err(redis_error)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PushTransport for RedisStreamPushBus {
    async fn publish(&self, request: &PushRequest) -> Result<PushReceipt> {
        request.validate()?;
        let payload = serde_json::to_vec(request)?;
        let mut connection = self.connection.clone();
        for stream in self.publish_streams(request).await? {
            redis::cmd("XADD")
                .arg(stream)
                .arg("MAXLEN")
                .arg("~")
                .arg(self.config.max_len)
                .arg("*")
                .arg("payload")
                .arg(&payload)
                .query_async::<String>(&mut connection)
                .await
                .map_err(redis_error)?;
        }
        Ok(PushReceipt::accepted(request, 0))
    }

    async fn subscribe(
        &self,
        handler: Arc<dyn PushHandler>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut backoff = Duration::ZERO;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let cycle = async {
                self.ensure_groups().await?;
                self.claim_pending(&handler).await?;
                self.read_new(&handler).await
            };
            let failed = tokio::select! {
                result = cycle => result.is_err(),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    false
                }
            };
            if failed {
                self.stats.reconnect();
                backoff = reconnect_delay(backoff, false);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
                continue;
            }
            backoff = Duration::ZERO;
        }
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    super::map_redis_error("Redis Push Stream", error)
}
