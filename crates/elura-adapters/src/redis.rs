use elura_core::{Error, Result};
use redis::aio::ConnectionManager;

mod health;
pub(crate) mod idempotency;
pub(crate) mod otp;
pub(crate) mod push_stream;
pub(crate) mod session;

pub use health::{RedisHealth, RedisHealthStats, SubscriptionStats};
pub(crate) use health::{SubscriptionCounters, reconnect_delay};

#[derive(Clone)]
enum RedisConnectionInner {
    Standalone(ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

#[derive(Clone)]
pub(crate) struct RedisConnection {
    inner: RedisConnectionInner,
    standalone_client: Option<redis::Client>,
}

impl RedisConnection {
    pub(crate) fn is_cluster(&self) -> bool {
        matches!(self.inner, RedisConnectionInner::Cluster(_))
    }

    pub(crate) fn pubsub_client(&self) -> Result<redis::Client> {
        self.standalone_client.clone().ok_or_else(|| {
            Error::InvalidConfig("this Redis adapter requires standalone Pub/Sub".into())
        })
    }

    pub(crate) fn atomic_prefix(&self, prefix: &str) -> Result<String> {
        validate_key_prefix(prefix)?;
        if self.is_cluster() {
            Ok(format!("{prefix}:{{transport}}"))
        } else {
            Ok(prefix.to_owned())
        }
    }
}

pub(crate) async fn standalone_connection(url: &str) -> Result<RedisConnection> {
    if url.trim().is_empty() {
        return Err(Error::InvalidConfig("Redis URL is required".into()));
    }
    let client = redis::Client::open(url).map_err(connection_error)?;
    let connection = client
        .get_connection_manager()
        .await
        .map_err(connection_error)?;
    Ok(RedisConnection {
        inner: RedisConnectionInner::Standalone(connection),
        standalone_client: Some(client),
    })
}

pub(crate) async fn cluster_connection<I, S>(nodes: I) -> Result<RedisConnection>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let nodes = normalize_cluster_nodes(nodes)?;
    let client = redis::cluster::ClusterClient::new(nodes).map_err(connection_error)?;
    let connection = client
        .get_async_connection()
        .await
        .map_err(connection_error)?;
    Ok(RedisConnection {
        inner: RedisConnectionInner::Cluster(connection),
        standalone_client: None,
    })
}

impl redis::aio::ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        command: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match &mut self.inner {
            RedisConnectionInner::Standalone(connection) => connection.req_packed_command(command),
            RedisConnectionInner::Cluster(connection) => connection.req_packed_command(command),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match &mut self.inner {
            RedisConnectionInner::Standalone(connection) => {
                connection.req_packed_commands(pipeline, offset, count)
            }
            RedisConnectionInner::Cluster(connection) => {
                connection.req_packed_commands(pipeline, offset, count)
            }
        }
    }

    fn get_db(&self) -> i64 {
        match &self.inner {
            RedisConnectionInner::Standalone(connection) => connection.get_db(),
            RedisConnectionInner::Cluster(connection) => connection.get_db(),
        }
    }
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

fn connection_error(error: redis::RedisError) -> Error {
    map_redis_error("Redis connection", error)
}

pub(crate) fn validate_key_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty()
        || prefix.len() > 128
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::InvalidConfig(
            "Redis key prefix must contain only letters, digits, '-', '_', '.' or ':'".into(),
        ));
    }
    Ok(())
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
        let result = cluster_connection(Vec::<String>::new()).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn key_prefix_rejects_hash_tags() {
        let result = validate_key_prefix("{elura}");
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
