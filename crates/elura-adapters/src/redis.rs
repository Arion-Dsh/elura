use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use elura_core::ticket::ReplayStore;
use elura_core::{Error, Result};
use redis::aio::ConnectionManager;

pub use crate::distributed::RedisOnlineDirectory;

mod health;
mod idempotency;
mod otp;
mod push_stream;
mod session;

pub use health::{RedisHealth, RedisHealthStats, SubscriptionStats};
pub(crate) use health::{SubscriptionCounters, reconnect_delay};
pub use idempotency::RedisIdempotencyStore;
pub use otp::RedisOtpStore;
pub use push_stream::{RedisStreamPushBus, RedisStreamPushConfig};
pub use session::{RedisSessionControlBus, RedisSessionControlConfig};

#[derive(Clone)]
pub struct RedisReplayStore {
    connection: RedisConnection,
    prefix: String,
}

#[derive(Clone)]
pub(crate) enum RedisConnection {
    Standalone(ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

impl redis::aio::ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        command: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Standalone(connection) => connection.req_packed_command(command),
            Self::Cluster(connection) => connection.req_packed_command(command),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Standalone(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Standalone(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

pub(crate) async fn standalone_connection(url: &str) -> Result<RedisConnection> {
    let client = redis::Client::open(url).map_err(redis_error)?;
    let connection = client.get_connection_manager().await.map_err(redis_error)?;
    Ok(RedisConnection::Standalone(connection))
}

pub(crate) async fn cluster_connection<I, S>(nodes: I) -> Result<RedisConnection>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let nodes = normalize_cluster_nodes(nodes)?;
    let client = redis::cluster::ClusterClient::new(nodes).map_err(redis_error)?;
    let connection = client.get_async_connection().await.map_err(redis_error)?;
    Ok(RedisConnection::Cluster(connection))
}

fn normalize_cluster_nodes<I, S>(nodes: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let nodes = nodes
        .into_iter()
        .map(Into::into)
        .map(|node: String| node.trim().to_owned())
        .filter(|node| !node.is_empty())
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return Err(Error::InvalidConfig(
            "Redis Cluster requires at least one seed node".into(),
        ));
    }
    Ok(nodes)
}

impl RedisReplayStore {
    /// Connects to a standalone Redis deployment.
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
        Ok(Self {
            connection: standalone_connection(url).await?,
            prefix: prefix.into(),
        })
    }

    /// Connects directly to Redis Cluster nodes and discovers the complete slot map.
    ///
    /// The node list only seeds topology discovery. Replay ticket keys contain no hash
    /// tag, so Redis distributes different tickets across all cluster slots.
    pub async fn connect_cluster<I, S>(nodes: I, prefix: impl Into<String>) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let prefix = prefix.into();
        if prefix.trim().is_empty() || prefix.contains(['{', '}']) {
            return Err(Error::InvalidConfig(
                "Redis Replay Cluster prefix must be non-empty and must not contain hash tags"
                    .into(),
            ));
        }
        Ok(Self {
            connection: cluster_connection(nodes).await?,
            prefix,
        })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }
}

#[async_trait]
impl ReplayStore for RedisReplayStore {
    async fn reserve(&self, ticket_id: &str, expires_at: u64) -> Result<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_secs();
        if expires_at <= now {
            return Ok(false);
        }
        let key = self.key(&format!("ticket:{ticket_id}"));
        let ttl = expires_at - now;
        let mut connection = self.connection.clone();
        let result = reserve_ticket(&mut connection, &key, ttl).await?;
        Ok(result)
    }
}

async fn reserve_ticket(
    connection: &mut impl redis::aio::ConnectionLike,
    key: &str,
    ttl: u64,
) -> Result<bool> {
    let result: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(ttl)
        .query_async(connection)
        .await
        .map_err(redis_error)?;
    Ok(result.is_some())
}

fn redis_error(error: redis::RedisError) -> Error {
    map_redis_error("redis", error)
}

pub(crate) fn map_redis_error(context: &str, error: redis::RedisError) -> Error {
    let detail = format!("{context}: {error}");
    if !matches!(error.retry_method(), redis::RetryMethod::NoRetry) {
        return Error::Unavailable;
    }
    match error.kind() {
        redis::ErrorKind::InvalidClientConfig | redis::ErrorKind::AuthenticationFailed => {
            Error::InvalidConfig(detail)
        }
        _ => Error::Internal(detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_transient_and_permanent_redis_errors() {
        assert!(matches!(
            map_redis_error(
                "test",
                redis::RedisError::from((redis::ErrorKind::Io, "disconnected")),
            ),
            Error::Unavailable
        ));
        assert!(matches!(
            map_redis_error(
                "test",
                redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "invalid")),
            ),
            Error::InvalidConfig(_)
        ));
        assert!(matches!(
            map_redis_error(
                "test",
                redis::RedisError::from((redis::ErrorKind::UnexpectedReturnType, "unexpected",)),
            ),
            Error::Internal(_)
        ));
    }

    #[test]
    fn replay_ticket_keys_are_not_pinned_to_one_cluster_slot() {
        use redis::cluster_routing::Slot;

        assert_ne!(
            Slot::for_key("elura:ticket:alpha"),
            Slot::for_key("elura:ticket:bravo")
        );
    }

    #[tokio::test]
    async fn cluster_connection_requires_a_seed_node() {
        let result = RedisReplayStore::connect_cluster(Vec::<String>::new(), "elura").await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn cluster_connection_rejects_a_hash_tagged_prefix() {
        let result = RedisReplayStore::connect_cluster(["redis://127.0.0.1/"], "{elura}").await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
