use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use elura_core::gateway_world::{
    WorldDiscovery, WorldRegistrar, WorldRegistration, WorldRouteTarget, WorldRouteUpdater,
};
use elura_core::{Error, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedisWorldLease {
    lease_id: Uuid,
    registration: WorldRegistration,
}

pub struct RedisWorldRegistrar {
    client: redis::Client,
    key: String,
    channel: String,
    payload: Vec<u8>,
    ttl_millis: u64,
    renew_interval: Duration,
}

/// Configuration owned by the Redis World registration adapter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisWorldRegistrationConfig {
    /// Injected by the upper application so secrets are not serialized.
    #[serde(skip)]
    pub url: String,
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
    pub fn build(self, world_id: impl Into<String>) -> Result<RedisWorldRegistrar> {
        RedisWorldRegistrar::new(self, world_id.into())
    }

    fn registration(&self, world_id: String) -> Result<WorldRegistration> {
        validate_redis(&self.url, &self.key_prefix)?;
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
    pub fn new(config: RedisWorldRegistrationConfig, world_id: String) -> Result<Self> {
        let registration = config.registration(world_id)?;
        let client = redis::Client::open(config.url.as_str()).map_err(redis_error)?;
        let payload = serde_json::to_vec(&RedisWorldLease {
            lease_id: Uuid::new_v4(),
            registration: registration.clone(),
        })?;
        Ok(Self {
            client,
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
        let mut connection = self
            .client
            .get_connection_manager()
            .await
            .map_err(redis_error)?;
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
        let mut connection = self
            .client
            .get_connection_manager()
            .await
            .map_err(redis_error)?;
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
        let mut connection = self
            .client
            .get_connection_manager()
            .await
            .map_err(redis_error)?;
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
    client: redis::Client,
    key_prefix: String,
    channel: String,
    refresh_interval: Duration,
    known_scopes: Mutex<BTreeSet<RouteScope>>,
}

/// Configuration owned by the Redis World discovery adapter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisWorldDiscoveryConfig {
    /// Injected by the upper application so secrets are not serialized.
    #[serde(skip)]
    pub url: String,
    pub key_prefix: String,
    pub refresh_interval: Duration,
}

impl RedisWorldDiscoveryConfig {
    pub fn build(self) -> Result<RedisWorldDiscovery> {
        RedisWorldDiscovery::new(&self.url, &self.key_prefix, self.refresh_interval)
    }
}

impl RedisWorldDiscovery {
    pub fn new(url: &str, key_prefix: &str, refresh_interval: Duration) -> Result<Self> {
        validate_redis(url, key_prefix)?;
        if refresh_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "Redis World discovery refresh interval must be positive".into(),
            ));
        }
        Ok(Self {
            client: redis::Client::open(url).map_err(redis_error)?,
            key_prefix: key_prefix.into(),
            channel: format!("{key_prefix}:changes"),
            refresh_interval,
            known_scopes: Mutex::new(BTreeSet::new()),
        })
    }

    async fn synchronize(&self, updater: &Arc<dyn WorldRouteUpdater>) -> Result<()> {
        let mut connection = self
            .client
            .get_connection_manager()
            .await
            .map_err(redis_error)?;
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

fn validate_redis(url: &str, prefix: &str) -> Result<()> {
    if url.trim().is_empty()
        || prefix.is_empty()
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::InvalidConfig(
            "Redis URL and safe World discovery key prefix are required".into(),
        ));
    }
    Ok(())
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

    fn registration(url: String, key_prefix: String) -> RedisWorldRegistrationConfig {
        RedisWorldRegistrationConfig {
            url,
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
    fn registration_config_owns_validation_and_redacts_redis_url() {
        let mut config = registration(
            "redis://user:secret@127.0.0.1/".into(),
            "elura:worlds".into(),
        );
        config.ttl = Duration::from_secs(5);
        config.renew_interval = Duration::from_secs(3);
        assert!(config.clone().build("world-1").is_err());
        assert!(!serde_json::to_string(&config).unwrap().contains("secret"));
    }

    #[tokio::test]
    async fn registration_is_discovered_and_removed() {
        let Ok(redis_url) = std::env::var("ELURA_TEST_REDIS_URL") else {
            return;
        };
        let key_prefix = format!("elura:test:worlds:{}", Uuid::new_v4());
        let config = registration(redis_url.clone(), key_prefix.clone());
        let registrar = config.clone().build("world-1").unwrap();
        registrar.register().await.unwrap();
        let replacement = config.build("world-1").unwrap();
        assert!(replacement.register().await.is_err());

        let discovery =
            RedisWorldDiscovery::new(&redis_url, &key_prefix, Duration::from_secs(5)).unwrap();
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
