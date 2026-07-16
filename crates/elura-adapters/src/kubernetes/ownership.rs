use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elura_core::gateway_world::{WorldRouteTarget, WorldRouteUpdater};
use elura_core::ownership::{Assignment, OwnershipResolver, OwnershipTable, preferred_world};
use elura_core::{Error, Result};
use futures_util::StreamExt;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client, ResourceExt, api::PostParams};
use tokio::sync::{Notify, watch};

use super::kube_error;

const TYPE_LABEL: &str = "elura.dev/ownership";
const TYPE_VALUE: &str = "world-shard";
const REGION_LABEL: &str = "elura.dev/region-id";
const REALM_LABEL: &str = "elura.dev/realm-id";
const SHARD_LABEL: &str = "elura.dev/shard-id";

fn scope_selector(region_id: u32, realm_id: u32) -> String {
    format!("{TYPE_LABEL}={TYPE_VALUE},{REGION_LABEL}={region_id},{REALM_LABEL}={realm_id}")
}

fn lease_prefix(region_id: u32, realm_id: u32) -> String {
    format!("elura-r{region_id}-realm{realm_id}-world-shard-")
}

fn scope_matches(lease: &Lease, region_id: u32, realm_id: u32) -> bool {
    let labels = lease.metadata.labels.as_ref();
    let region_id = region_id.to_string();
    let realm_id = realm_id.to_string();
    labels.and_then(|v| v.get(TYPE_LABEL)).map(String::as_str) == Some(TYPE_VALUE)
        && labels.and_then(|v| v.get(REGION_LABEL)).map(String::as_str) == Some(region_id.as_str())
        && labels.and_then(|v| v.get(REALM_LABEL)).map(String::as_str) == Some(realm_id.as_str())
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OwnershipObserverConfig {
    pub namespace: String,
    pub region_id: u32,
    pub realm_id: u32,
    pub shard_count: u32,
    pub additional_label_selector: Option<String>,
}

impl OwnershipObserverConfig {
    /// Creates ownership observation configuration without an extra label selector.
    pub fn new(
        namespace: impl Into<String>,
        region_id: u32,
        realm_id: u32,
        shard_count: u32,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            region_id,
            realm_id,
            shard_count,
            additional_label_selector: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty()
            || self.region_id == 0
            || self.realm_id == 0
            || self.shard_count == 0
        {
            return Err(Error::InvalidConfig(
                "Kubernetes ownership namespace, region, realm and shard count are required".into(),
            ));
        }
        Ok(())
    }
}

pub struct OwnershipObserver {
    api: Api<Lease>,
    config: OwnershipObserverConfig,
    table: OwnershipTable,
    synchronized: AtomicBool,
    active_count: AtomicUsize,
}

impl OwnershipObserver {
    pub async fn in_cluster(config: OwnershipObserverConfig) -> Result<Self> {
        let client = Client::try_default().await.map_err(kube_error)?;
        Self::new(client, config)
    }

    pub fn new(client: Client, config: OwnershipObserverConfig) -> Result<Self> {
        config.validate()?;
        let table = OwnershipTable::new(config.shard_count)?;
        Ok(Self {
            api: Api::namespaced(client, &config.namespace),
            config,
            table,
            synchronized: AtomicBool::new(false),
            active_count: AtomicUsize::new(0),
        })
    }

    pub fn ready(&self) -> bool {
        self.synchronized.load(Ordering::Acquire)
            && self.active_count.load(Ordering::Acquire) == self.config.shard_count as usize
    }

    pub fn snapshot(&self) -> Result<Vec<Assignment>> {
        self.table.snapshot()
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut labels = scope_selector(self.config.region_id, self.config.realm_id);
        if let Some(extra) = self
            .config
            .additional_label_selector
            .as_deref()
            .filter(|v| !v.is_empty())
        {
            labels.push(',');
            labels.push_str(extra);
        }
        let mut stream = std::pin::pin!(watcher::watcher(
            self.api.clone(),
            watcher::Config::default().labels(&labels),
        ));
        let mut current = BTreeMap::<String, Lease>::new();
        let mut initializing = None::<BTreeMap<String, Lease>>;
        let mut expiry_tick = tokio::time::interval(Duration::from_secs(1));
        expiry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); },
                _ = expiry_tick.tick(), if self.synchronized.load(Ordering::Relaxed) => self.publish(current.values())?,
                event = stream.next() => match event {
                    Some(Ok(Event::Init)) => initializing = Some(BTreeMap::new()),
                    Some(Ok(Event::InitApply(lease))) => if let Some(buffer) = &mut initializing { buffer.insert(lease.name_any(), lease); },
                    Some(Ok(Event::InitDone)) => {
                        if let Some(buffer) = initializing.take() { current = buffer; }
                        self.publish(current.values())?;
                        self.synchronized.store(true, Ordering::Release);
                    }
                    Some(Ok(Event::Apply(lease))) => { current.insert(lease.name_any(), lease); self.publish(current.values())?; }
                    Some(Ok(Event::Delete(lease))) => { current.remove(&lease.name_any()); self.publish(current.values())?; }
                    Some(Err(error)) => tracing::warn!(%error, "Kubernetes Lease watch error"),
                    None => return Err(Error::Unavailable),
                }
            }
        }
    }

    fn publish<'a>(&self, leases: impl Iterator<Item = &'a Lease>) -> Result<()> {
        let assignments = assignments_from_leases(
            leases,
            Utc::now(),
            self.config.shard_count,
            self.config.region_id,
            self.config.realm_id,
        );
        self.active_count
            .store(assignments.len(), Ordering::Release);
        self.table.replace(assignments)
    }
}

#[async_trait]
impl OwnershipResolver for OwnershipObserver {
    async fn resolve(&self, region_id: u32, realm_id: u32, shard: u32) -> Result<Assignment> {
        if region_id != self.config.region_id || realm_id != self.config.realm_id {
            return Err(Error::Unavailable);
        }
        self.table.resolve(region_id, realm_id, shard).await
    }
}

pub fn assignments_from_leases<'a>(
    leases: impl IntoIterator<Item = &'a Lease>,
    now: DateTime<Utc>,
    shard_count: u32,
    region_id: u32,
    realm_id: u32,
) -> Vec<Assignment> {
    let mut selected = HashMap::<u32, (Assignment, DateTime<Utc>)>::new();
    for lease in leases {
        if !scope_matches(lease, region_id, realm_id) {
            continue;
        }
        let Some(shard_id) = lease
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(SHARD_LABEL))
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value < shard_count)
        else {
            continue;
        };
        let Some(spec) = lease.spec.as_ref() else {
            continue;
        };
        let Some(world_id) = spec
            .holder_identity
            .as_ref()
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(epoch) = spec.lease_transitions.filter(|value| *value > 0) else {
            continue;
        };
        let (Some(renewed), Some(duration)) = (
            spec.renew_time.as_ref(),
            spec.lease_duration_seconds.filter(|value| *value > 0),
        ) else {
            continue;
        };
        let renewed = DateTime::<Utc>::from(SystemTime::from(renewed.0));
        if now >= renewed + chrono::Duration::seconds(i64::from(duration)) {
            continue;
        }
        let candidate = Assignment {
            region_id,
            realm_id,
            shard_id,
            world_id: world_id.clone(),
            epoch: epoch as u64,
        };
        let should_replace = selected
            .get(&shard_id)
            .is_none_or(|(previous, previous_renewed)| {
                candidate.epoch > previous.epoch
                    || (candidate.epoch == previous.epoch && renewed > *previous_renewed)
            });
        if should_replace {
            selected.insert(shard_id, (candidate, renewed));
        }
    }
    let mut result: Vec<_> = selected.into_values().map(|value| value.0).collect();
    result.sort_by_key(|value| value.shard_id);
    result
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OwnershipCoordinatorConfig {
    pub namespace: String,
    pub region_id: u32,
    pub realm_id: u32,
    pub shard_count: u32,
    pub lease_duration: Duration,
    pub renew_interval: Duration,
}

impl OwnershipCoordinatorConfig {
    pub fn new(
        namespace: impl Into<String>,
        region_id: u32,
        realm_id: u32,
        shard_count: u32,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            region_id,
            realm_id,
            shard_count,
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty()
            || self.region_id == 0
            || self.realm_id == 0
            || self.shard_count == 0
        {
            return Err(Error::InvalidConfig(
                "Kubernetes coordinator namespace, region, realm and shard count are required"
                    .into(),
            ));
        }
        if self.renew_interval.is_zero()
            || self.renew_interval >= self.lease_duration
            || self.lease_duration.as_secs() > i32::MAX as u64
        {
            return Err(Error::InvalidConfig(
                "Kubernetes coordinator requires renew interval < lease duration".into(),
            ));
        }
        Ok(())
    }
}

pub struct OwnershipCoordinator {
    api: Api<Lease>,
    config: OwnershipCoordinatorConfig,
    world_ids: Mutex<Vec<String>>,
    changed: Notify,
}

impl OwnershipCoordinator {
    pub async fn in_cluster(config: OwnershipCoordinatorConfig) -> Result<Self> {
        let client = Client::try_default().await.map_err(kube_error)?;
        Self::new(client, config)
    }
    pub fn new(client: Client, config: OwnershipCoordinatorConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            api: Api::namespaced(client, &config.namespace),
            config,
            world_ids: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }
    pub async fn update_worlds(&self, targets: &[WorldRouteTarget]) -> Result<()> {
        let ids: BTreeSet<_> = targets
            .iter()
            .map(|target| target.world_id.clone())
            .collect();
        if ids.iter().any(String::is_empty) {
            return Err(Error::InvalidConfig("empty World instance ID".into()));
        }
        *self
            .world_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ids.into_iter().collect();
        self.changed.notify_one();
        Ok(())
    }
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut timer = tokio::time::interval(self.config.renew_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); },
                _ = timer.tick() => self.reconcile().await?,
                _ = self.changed.notified() => self.reconcile().await?,
            }
        }
    }
    pub async fn reconcile(&self) -> Result<()> {
        let world_ids = self
            .world_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if world_ids.is_empty() {
            return Ok(());
        }
        for shard_id in 0..self.config.shard_count {
            let world_id = preferred_world(shard_id, &world_ids)?;
            self.reconcile_lease(shard_id, world_id).await?;
        }
        Ok(())
    }
    async fn reconcile_lease(&self, shard_id: u32, world_id: String) -> Result<()> {
        let name = format!(
            "{}{shard_id}",
            lease_prefix(self.config.region_id, self.config.realm_id)
        );
        let now = MicroTime(Timestamp::now());
        let duration = self.config.lease_duration.as_secs() as i32;
        let existing = self.api.get_opt(&name).await.map_err(kube_error)?;
        let lease = if let Some(mut lease) = existing {
            if !scope_matches(&lease, self.config.region_id, self.config.realm_id) {
                return Err(Error::InvalidConfig(format!(
                    "Lease {name} belongs to another scope"
                )));
            }
            let spec = lease.spec.get_or_insert_default();
            let transitions = spec.lease_transitions.unwrap_or(1).max(1);
            if spec.holder_identity.as_deref() != Some(&world_id) {
                spec.lease_transitions = Some(
                    transitions
                        .checked_add(1)
                        .ok_or_else(|| Error::Internal("Lease epoch exhausted".into()))?,
                );
                spec.holder_identity = Some(world_id);
                spec.acquire_time = Some(now.clone());
            }
            spec.lease_duration_seconds = Some(duration);
            spec.renew_time = Some(now);
            lease
        } else {
            Lease {
                metadata: ObjectMeta {
                    name: Some(name.clone()),
                    labels: Some(lease_labels(
                        self.config.region_id,
                        self.config.realm_id,
                        shard_id,
                    )),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(world_id),
                    lease_duration_seconds: Some(duration),
                    acquire_time: Some(now.clone()),
                    renew_time: Some(now),
                    lease_transitions: Some(1),
                    ..Default::default()
                }),
            }
        };
        if lease.metadata.resource_version.is_some() {
            self.api
                .replace(&name, &PostParams::default(), &lease)
                .await
                .map_err(kube_error)?;
        } else {
            self.api
                .create(&PostParams::default(), &lease)
                .await
                .map_err(kube_error)?;
        }
        Ok(())
    }
}

#[async_trait]
impl WorldRouteUpdater for OwnershipCoordinator {
    async fn replace_targets(
        &self,
        region_id: u32,
        realm_id: u32,
        _route: u32,
        targets: Vec<WorldRouteTarget>,
    ) -> Result<()> {
        if region_id != self.config.region_id || realm_id != self.config.realm_id {
            return Err(Error::InvalidConfig(
                "coordinator route scope mismatch".into(),
            ));
        }
        self.update_worlds(&targets).await
    }
}

fn lease_labels(region_id: u32, realm_id: u32, shard_id: u32) -> BTreeMap<String, String> {
    BTreeMap::from([
        (TYPE_LABEL.into(), TYPE_VALUE.into()),
        (REGION_LABEL.into(), region_id.to_string()),
        (REALM_LABEL.into(), realm_id.to_string()),
        (SHARD_LABEL.into(), shard_id.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(shard: u32, owner: &str, epoch: i32, renewed: DateTime<Utc>) -> Lease {
        Lease {
            metadata: ObjectMeta {
                labels: Some(lease_labels(1, 2, shard)),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(owner.into()),
                lease_duration_seconds: Some(30),
                renew_time: Some(MicroTime(
                    Timestamp::try_from(SystemTime::from(renewed)).unwrap(),
                )),
                lease_transitions: Some(epoch),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn ignores_expired_and_selects_highest_epoch() {
        let now = Utc::now();
        let old = lease(0, "old", 2, now - chrono::Duration::seconds(31));
        let low = lease(1, "low", 2, now);
        let high = lease(1, "high", 3, now);
        assert_eq!(
            assignments_from_leases([&old, &low, &high], now, 4, 1, 2),
            vec![Assignment {
                region_id: 1,
                realm_id: 2,
                shard_id: 1,
                world_id: "high".into(),
                epoch: 3
            }]
        );
    }
}
