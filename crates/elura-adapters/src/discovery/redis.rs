use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use elura_core::{Error, Result};
use elura_gateway::discovery::{WorldDiscovery, WorldRouteTarget, WorldRouteUpdater};
use elura_world::registration::{WorldRegistrar, WorldRegistration};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

use crate::redis::{
    RedisConnection, cluster_connection, standalone_connection, validate_key_prefix,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedisWorldLease {
    lease_id: Uuid,
    registration: WorldRegistration,
}

pub struct RedisWorldRegistrar {
    connection: RedisConnection,
    key: String,
    channel: String,
    payload: Vec<u8>,
    ttl_millis: u64,
    renew_interval: Duration,
}

/// Configuration owned by the Redis World registration adapter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RedisWorldRegistrationConfig {
    pub key_prefix: String,
    pub advertise_address: String,
    pub region_id: u32,
    pub realm_id: u32,
    #[serde(default)]
    pub route: u32,
    pub ttl: Duration,
    pub renew_interval: Duration,
}

impl RedisWorldRegistrationConfig {
    /// Creates registration configuration with a 30-second TTL and 10-second renewal interval.
    pub fn new(
        key_prefix: impl Into<String>,
        advertise_address: impl Into<String>,
        region_id: u32,
        realm_id: u32,
    ) -> Self {
        Self {
            key_prefix: key_prefix.into(),
            advertise_address: advertise_address.into(),
            region_id,
            realm_id,
            route: 0,
            ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
        }
    }

    fn registration(&self, world_id: String) -> Result<WorldRegistration> {
        validate_key_prefix(&self.key_prefix)?;
        if self.ttl.is_zero()
            || self.renew_interval.is_zero()
            || self.ttl < self.renew_interval.saturating_mul(2)
        {
            return Err(Error::InvalidConfig(
                "Redis World registration requires TTL >= 2 * renew interval".into(),
            ));
        }
        let registration = WorldRegistration {
            world_id,
            region_id: self.region_id,
            realm_id: self.realm_id,
            route: self.route,
            address: self.advertise_address.clone(),
        };
        registration.validate()?;
        Ok(registration)
    }
}

impl RedisWorldRegistrar {
    pub async fn connect(
        url: &str,
        world_id: impl Into<String>,
        config: RedisWorldRegistrationConfig,
    ) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, world_id, config)
    }

    pub async fn connect_cluster<I, S>(
        nodes: I,
        world_id: impl Into<String>,
        config: RedisWorldRegistrationConfig,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_connection(cluster_connection(nodes).await?, world_id, config)
    }

    fn from_connection(
        connection: RedisConnection,
        world_id: impl Into<String>,
        config: RedisWorldRegistrationConfig,
    ) -> Result<Self> {
        let registration = config.registration(world_id.into())?;
        let payload = serde_json::to_vec(&RedisWorldLease {
            lease_id: Uuid::new_v4(),
            registration: registration.clone(),
        })?;
        Ok(Self {
            connection,
            key: format!("{}:instance:{}", config.key_prefix, registration.world_id),
            channel: format!("{}:changes", config.key_prefix),
            payload,
            ttl_millis: duration_millis(config.ttl)?,
            renew_interval: config.renew_interval,
        })
    }
}

#[async_trait]
impl WorldRegistrar for RedisWorldRegistrar {
    fn renew_interval(&self) -> Duration {
        self.renew_interval
    }

    async fn register(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        let acquired: i64 = redis::Script::new(
            "if redis.call('EXISTS',KEYS[1])==1 then return 0 end redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]);redis.call('PUBLISH',ARGV[3],'changed');return 1",
        )
        .key(&self.key)
        .arg(&self.payload)
        .arg(self.ttl_millis)
        .arg(&self.channel)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        if acquired == 1 {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }

    async fn renew(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        let renewed: i64 = redis::Script::new(
            "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end return 0",
        )
        .key(&self.key)
        .arg(&self.payload)
        .arg(self.ttl_millis)
        .invoke_async(&mut connection)
        .await
        .map_err(redis_error)?;
        if renewed == 1 {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }

    async fn unregister(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::Script::new(
            "if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('DEL',KEYS[1]);redis.call('PUBLISH',ARGV[2],'changed');return 1 end return 0",
        )
        .key(&self.key)
        .arg(&self.payload)
        .arg(&self.channel)
        .invoke_async::<i64>(&mut connection)
        .await
        .map_err(redis_error)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RouteScope {
    region_id: u32,
    realm_id: u32,
    route: u32,
}

pub struct RedisWorldDiscovery {
    connection: RedisConnection,
    client: redis::Client,
    key_prefix: String,
    channel: String,
    refresh_interval: Duration,
    known_scopes: Mutex<BTreeSet<RouteScope>>,
}

/// Configuration owned by the Redis World discovery adapter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RedisWorldDiscoveryConfig {
    pub key_prefix: String,
    pub refresh_interval: Duration,
}

impl RedisWorldDiscoveryConfig {
    /// Creates discovery configuration with a five-second refresh interval.
    pub fn new(key_prefix: impl Into<String>) -> Self {
        Self {
            key_prefix: key_prefix.into(),
            refresh_interval: Duration::from_secs(5),
        }
    }
}

impl RedisWorldDiscovery {
    pub async fn connect(url: &str, config: RedisWorldDiscoveryConfig) -> Result<Self> {
        Self::from_connection(standalone_connection(url).await?, config)
    }

    fn from_connection(
        connection: RedisConnection,
        config: RedisWorldDiscoveryConfig,
    ) -> Result<Self> {
        validate_key_prefix(&config.key_prefix)?;
        if config.refresh_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "Redis World discovery refresh interval must be positive".into(),
            ));
        }
        let client = connection.pubsub_client()?;
        let key_prefix = config.key_prefix;
        Ok(Self {
            connection,
            client,
            key_prefix: key_prefix.clone(),
            channel: format!("{key_prefix}:changes"),
            refresh_interval: config.refresh_interval,
            known_scopes: Mutex::new(BTreeSet::new()),
        })
    }

    async fn synchronize(&self, updater: &Arc<dyn WorldRouteUpdater>) -> Result<()> {
        let mut connection = self.connection.clone();
        let mut cursor = 0_u64;
        let mut leases = Vec::new();
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}:instance:*", self.key_prefix))
                .arg("COUNT")
                .arg(256)
                .query_async(&mut connection)
                .await
                .map_err(redis_error)?;
            if !keys.is_empty() {
                let payloads: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                    .arg(keys)
                    .query_async(&mut connection)
                    .await
                    .map_err(redis_error)?;
                for payload in payloads.into_iter().flatten() {
                    match serde_json::from_slice::<RedisWorldLease>(&payload) {
                        Ok(lease) if lease.registration.validate().is_ok() => leases.push(lease),
                        _ => warn!("ignoring malformed Redis World registration"),
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        let mut resolutions = futures_util::stream::FuturesUnordered::new();
        for lease in leases {
            let registration = lease.registration;
            resolutions.push(async move {
                let address = tokio::net::lookup_host(&registration.address)
                    .await
                    .map_err(|error| {
                        Error::InvalidConfig(format!(
                            "resolve World {} address {}: {error}",
                            registration.world_id, registration.address
                        ))
                    })?
                    .find(|address| !address.ip().is_unspecified() && address.port() > 0)
                    .ok_or_else(|| {
                        Error::InvalidConfig(format!(
                            "World {} has no reachable address",
                            registration.world_id
                        ))
                    })?;
                Ok::<_, Error>((
                    RouteScope {
                        region_id: registration.region_id,
                        realm_id: registration.realm_id,
                        route: registration.route,
                    },
                    WorldRouteTarget {
                        world_id: registration.world_id,
                        address,
                    },
                ))
            });
        }
        let mut groups = BTreeMap::<RouteScope, Vec<WorldRouteTarget>>::new();
        while let Some(resolved) = resolutions.next().await {
            match resolved {
                Ok((scope, target)) => {
                    groups.entry(scope).or_default().push(target);
                }
                Err(error) => warn!(%error, "ignoring unresolvable Redis World registration"),
            }
        }
        for targets in groups.values_mut() {
            targets.sort_by(|left, right| {
                left.world_id
                    .cmp(&right.world_id)
                    .then(left.address.cmp(&right.address))
            });
        }
        let previous = self
            .known_scopes
            .lock()
            .map_err(|_| Error::Internal("World discovery state poisoned".into()))?
            .clone();
        let current = groups.keys().copied().collect::<BTreeSet<_>>();
        for scope in previous.union(&current) {
            updater
                .replace_targets(
                    scope.region_id,
                    scope.realm_id,
                    scope.route,
                    groups.remove(scope).unwrap_or_default(),
                )
                .await?;
        }
        *self
            .known_scopes
            .lock()
            .map_err(|_| Error::Internal("World discovery state poisoned".into()))? = current;
        Ok(())
    }

    async fn watch_once(
        &self,
        updater: &Arc<dyn WorldRouteUpdater>,
        shutdown: &mut watch::Receiver<bool>,
        ticker: &mut tokio::time::Interval,
    ) -> Result<bool> {
        let mut subscription = self.client.get_async_pubsub().await.map_err(redis_error)?;
        subscription
            .subscribe(&self.channel)
            .await
            .map_err(redis_error)?;
        let mut messages = subscription.on_message();
        loop {
            tokio::select! {
                changed = shutdown.changed() => return Ok(changed.is_err() || *shutdown.borrow()),
                _ = ticker.tick() => self.synchronize(updater).await?,
                message = messages.next() => match message {
                    Some(_) => self.synchronize(updater).await?,
                    None => return Ok(false),
                }
            }
        }
    }
}

#[async_trait]
impl WorldDiscovery for RedisWorldDiscovery {
    async fn run(
        &self,
        updater: Arc<dyn WorldRouteUpdater>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if let Err(error) = self.synchronize(&updater).await {
            warn!(%error, "initial Redis World discovery refresh failed; retrying");
        }
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.refresh_interval,
            self.refresh_interval,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match self.watch_once(&updater, &mut shutdown, &mut ticker).await {
                Ok(true) => return Ok(()),
                Ok(false) | Err(_) => {
                    warn!("Redis World discovery subscription disconnected; reconnecting");
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                    if let Err(error) = self.synchronize(&updater).await {
                        warn!(%error, "Redis World discovery refresh failed; retaining known routes");
                    }
                }
            }
        }
    }
}

fn duration_millis(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .ok()
        .filter(|millis| *millis > 0)
        .ok_or_else(|| Error::InvalidConfig("duration exceeds Redis TTL range".into()))
}

fn redis_error(error: redis::RedisError) -> Error {
    crate::redis::map_redis_error("Redis World discovery", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingUpdater {
        targets: Mutex<Vec<WorldRouteTarget>>,
    }

    #[async_trait]
    impl WorldRouteUpdater for RecordingUpdater {
        async fn replace_targets(
            &self,
            _region_id: u32,
            _realm_id: u32,
            _route: u32,
            targets: Vec<WorldRouteTarget>,
        ) -> Result<()> {
            *self
                .targets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = targets;
            Ok(())
        }
    }

    fn registration(key_prefix: String) -> RedisWorldRegistrationConfig {
        RedisWorldRegistrationConfig {
            key_prefix,
            advertise_address: "127.0.0.1:18000".into(),
            region_id: 1,
            realm_id: 1,
            route: 0,
            ttl: Duration::from_secs(15),
            renew_interval: Duration::from_secs(5),
        }
    }

    #[test]
    fn registration_config_owns_validation() {
        let mut config = registration("elura:worlds".into());
        config.ttl = Duration::from_secs(5);
        config.renew_interval = Duration::from_secs(3);
        assert!(config.registration("world-1".into()).is_err());
    }

    #[tokio::test]
    async fn registration_is_discovered_and_removed() {
        let Ok(redis_url) = std::env::var("ELURA_TEST_REDIS_URL") else {
            return;
        };
        let key_prefix = format!("elura:test:worlds:{}", Uuid::new_v4());
        let config = registration(key_prefix.clone());
        let registrar = RedisWorldRegistrar::connect(&redis_url, "world-1", config.clone())
            .await
            .unwrap();
        registrar.register().await.unwrap();
        let replacement = RedisWorldRegistrar::connect(&redis_url, "world-1", config)
            .await
            .unwrap();
        assert!(replacement.register().await.is_err());

        let discovery = RedisWorldDiscovery::connect(
            &redis_url,
            RedisWorldDiscoveryConfig {
                key_prefix,
                refresh_interval: Duration::from_secs(5),
            },
        )
        .await
        .unwrap();
        let recorder = Arc::new(RecordingUpdater::default());
        let updater: Arc<dyn WorldRouteUpdater> = recorder.clone();
        discovery.synchronize(&updater).await.unwrap();
        assert_eq!(
            recorder
                .targets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            1
        );

        registrar.unregister().await.unwrap();
        replacement.register().await.unwrap();
        replacement.unregister().await.unwrap();
        discovery.synchronize(&updater).await.unwrap();
        assert!(
            recorder
                .targets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }
}
