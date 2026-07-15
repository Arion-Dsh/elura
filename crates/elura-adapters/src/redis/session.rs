use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::session::{SessionControlEvent, SessionControlHandler, SessionControlTransport};
use elura_core::{Error, Result};
use redis::AsyncCommands;
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{
    RedisConnection, SubscriptionCounters, SubscriptionStats, cluster_connection, reconnect_delay,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisSessionControlConfig {
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub max_len: usize,
    pub claim_idle: Duration,
    pub read_block: Duration,
    pub batch_size: usize,
}

impl Default for RedisSessionControlConfig {
    fn default() -> Self {
        Self {
            stream: "session:control".into(),
            group: String::new(),
            consumer: String::new(),
            max_len: 100_000,
            claim_idle: Duration::from_secs(30),
            read_block: Duration::from_secs(1),
            batch_size: 128,
        }
    }
}

pub struct RedisSessionControlBus {
    source: RedisSessionConnectionSource,
    config: RedisSessionControlConfig,
    stats: Arc<SubscriptionCounters>,
}

enum RedisSessionConnectionSource {
    Standalone(redis::Client),
    Cluster(RedisConnection),
}

impl RedisSessionControlBus {
    pub fn new(
        client: redis::Client,
        gateway_id: impl Into<String>,
        config: RedisSessionControlConfig,
    ) -> Result<Self> {
        Self::with_source(
            RedisSessionConnectionSource::Standalone(client),
            gateway_id,
            config,
        )
    }

    /// Connects Session Control to the same Redis Cluster used by other transport adapters.
    pub async fn connect_cluster<I, S>(
        nodes: I,
        gateway_id: impl Into<String>,
        config: RedisSessionControlConfig,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::with_source(
            RedisSessionConnectionSource::Cluster(cluster_connection(nodes).await?),
            gateway_id,
            config,
        )
    }

    fn with_source(
        source: RedisSessionConnectionSource,
        gateway_id: impl Into<String>,
        mut config: RedisSessionControlConfig,
    ) -> Result<Self> {
        let gateway_id = gateway_id.into();
        if config.group.is_empty() {
            config.group = format!("elura-session-control-{gateway_id}");
        }
        if config.consumer.is_empty() {
            config.consumer = gateway_id;
        }
        if config.stream.trim().is_empty()
            || config.stream.len() > 256
            || !config.stream.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
            || config.group.trim().is_empty()
            || config.group.len() > 256
            || config.consumer.trim().is_empty()
            || config.consumer.len() > 256
            || config.max_len == 0
            || config.claim_idle.is_zero()
            || config.read_block.is_zero()
            || config.batch_size == 0
            || config.batch_size > 4096
        {
            return Err(Error::InvalidConfig(
                "invalid Redis Session control config".into(),
            ));
        }
        Ok(Self {
            source,
            config,
            stats: Arc::new(SubscriptionCounters::default()),
        })
    }

    async fn connection(&self) -> Result<RedisConnection> {
        match &self.source {
            RedisSessionConnectionSource::Standalone(client) => client
                .get_connection_manager()
                .await
                .map(RedisConnection::Standalone)
                .map_err(redis_error),
            RedisSessionConnectionSource::Cluster(connection) => Ok(connection.clone()),
        }
    }

    pub fn stats(&self) -> SubscriptionStats {
        self.stats.snapshot()
    }

    async fn ensure_group(&self) -> Result<()> {
        let mut connection = self.connection().await?;
        let result = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.config.stream)
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
        Ok(())
    }

    async fn consume(&self, handler: &Arc<dyn SessionControlHandler>) -> Result<()> {
        let mut connection = self.connection().await?;
        let claimed: StreamAutoClaimReply = connection
            .xautoclaim_options(
                &self.config.stream,
                &self.config.group,
                &self.config.consumer,
                self.config.claim_idle.as_millis(),
                "0-0",
                StreamAutoClaimOptions::default().count(self.config.batch_size),
            )
            .await
            .map_err(redis_error)?;
        self.deliver(&mut connection, claimed.claimed, handler)
            .await?;
        let options = StreamReadOptions::default()
            .group(&self.config.group, &self.config.consumer)
            .count(self.config.batch_size)
            .block(self.config.read_block.as_millis() as usize);
        let reply: StreamReadReply = connection
            .xread_options(&[&self.config.stream], &[">"], &options)
            .await
            .map_err(redis_error)?;
        for stream in reply.keys {
            self.deliver(&mut connection, stream.ids, handler).await?;
        }
        Ok(())
    }

    async fn deliver(
        &self,
        connection: &mut RedisConnection,
        messages: Vec<StreamId>,
        handler: &Arc<dyn SessionControlHandler>,
    ) -> Result<()> {
        for message in messages {
            let event = message
                .get::<Vec<u8>>("payload")
                .and_then(|payload| serde_json::from_slice::<SessionControlEvent>(&payload).ok());
            let Some(event) = event.filter(|event| event.validate().is_ok()) else {
                self.stats.malformed();
                connection
                    .xack::<_, _, _, u64>(&self.config.stream, &self.config.group, &[&message.id])
                    .await
                    .map_err(redis_error)?;
                continue;
            };
            self.stats.message();
            if handler.handle(event).await.is_ok() {
                connection
                    .xack::<_, _, _, u64>(&self.config.stream, &self.config.group, &[&message.id])
                    .await
                    .map_err(redis_error)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SessionControlTransport for RedisSessionControlBus {
    async fn publish(&self, event: &SessionControlEvent) -> Result<()> {
        event.validate()?;
        let mut connection = self.connection().await?;
        redis::cmd("XADD")
            .arg(&self.config.stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.config.max_len)
            .arg("*")
            .arg("payload")
            .arg(serde_json::to_vec(event)?)
            .query_async::<String>(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    async fn subscribe(
        &self,
        handler: Arc<dyn SessionControlHandler>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut backoff = Duration::ZERO;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let cycle = async {
                self.ensure_group().await?;
                self.consume(&handler).await
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
                backoff = reconnect_delay(backoff, self.stats.snapshot().messages > 0);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
            } else {
                backoff = Duration::ZERO;
            }
        }
    }
}

fn redis_error(error: redis::RedisError) -> Error {
    super::map_redis_error("Redis Session Control", error)
}
