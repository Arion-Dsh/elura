use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use elura_core::gateway_world::{WorldRouteTarget, WorldRouteUpdater};
use elura_core::{Error, Result};
use futures_util::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client, ResourceExt, api::ListParams};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::kube_error;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EndpointWatcherConfig {
    pub namespace: String,
    pub service: String,
    pub port_name: String,
    pub region_id: u32,
    pub realm_id: u32,
    pub route: u32,
}

impl EndpointWatcherConfig {
    /// Creates an endpoint watcher for a World service and route scope.
    pub fn new(
        namespace: impl Into<String>,
        service: impl Into<String>,
        port_name: impl Into<String>,
        region_id: u32,
        realm_id: u32,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            service: service.into(),
            port_name: port_name.into(),
            region_id,
            realm_id,
            route: 0,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty()
            || self.service.trim().is_empty()
            || self.port_name.trim().is_empty()
            || self.region_id == 0
            || self.realm_id == 0
        {
            return Err(Error::InvalidConfig(
                "Kubernetes endpoint namespace, service, port, region and realm are required"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EndpointWatcherStats {
    pub synchronized: bool,
    pub endpoint_count: usize,
    pub updates: u64,
    pub errors: u64,
}

pub struct EndpointWatcher {
    api: Api<EndpointSlice>,
    config: EndpointWatcherConfig,
    updater: Arc<dyn WorldRouteUpdater>,
    synchronized: AtomicBool,
    endpoint_count: AtomicUsize,
    updates: AtomicU64,
    errors: AtomicU64,
}

impl EndpointWatcher {
    pub async fn in_cluster(
        config: EndpointWatcherConfig,
        updater: Arc<dyn WorldRouteUpdater>,
    ) -> Result<Self> {
        let client = Client::try_default().await.map_err(kube_error)?;
        Self::new(client, config, updater)
    }

    pub fn new(
        client: Client,
        config: EndpointWatcherConfig,
        updater: Arc<dyn WorldRouteUpdater>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            api: Api::namespaced(client, &config.namespace),
            config,
            updater,
            synchronized: AtomicBool::new(false),
            endpoint_count: AtomicUsize::new(0),
            updates: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    pub fn ready(&self) -> bool {
        self.synchronized.load(Ordering::Acquire) && self.endpoint_count.load(Ordering::Acquire) > 0
    }

    pub fn stats(&self) -> EndpointWatcherStats {
        EndpointWatcherStats {
            synchronized: self.synchronized.load(Ordering::Relaxed),
            endpoint_count: self.endpoint_count.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let label = format!("kubernetes.io/service-name={}", self.config.service);
        let mut stream = std::pin::pin!(watcher::watcher(
            self.api.clone(),
            watcher::Config::default().labels(&label),
        ));
        let mut current = BTreeMap::<String, EndpointSlice>::new();
        let mut initializing = None::<BTreeMap<String, EndpointSlice>>;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                event = stream.next() => match event {
                    Some(Ok(Event::Init)) => initializing = Some(BTreeMap::new()),
                    Some(Ok(Event::InitApply(slice))) => {
                        if let Some(buffer) = &mut initializing {
                            buffer.insert(slice.name_any(), slice);
                        }
                    }
                    Some(Ok(Event::InitDone)) => {
                        if let Some(buffer) = initializing.take() { current = buffer; }
                        self.publish(current.values()).await?;
                        self.synchronized.store(true, Ordering::Release);
                    }
                    Some(Ok(Event::Apply(slice))) => {
                        current.insert(slice.name_any(), slice);
                        self.publish(current.values()).await?;
                    }
                    Some(Ok(Event::Delete(slice))) => {
                        current.remove(&slice.name_any());
                        self.publish(current.values()).await?;
                    }
                    Some(Err(error)) => {
                        self.errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(service = %self.config.service, %error, "Kubernetes EndpointSlice watch error");
                    }
                    None => return Err(Error::Unavailable),
                }
            }
        }
    }

    async fn publish<'a>(&self, slices: impl Iterator<Item = &'a EndpointSlice>) -> Result<()> {
        let targets = targets_from_slices(slices, &self.config.port_name);
        self.updater
            .replace_targets(
                self.config.region_id,
                self.config.realm_id,
                self.config.route,
                targets.clone(),
            )
            .await?;
        self.endpoint_count.store(targets.len(), Ordering::Release);
        self.updates.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub struct EndpointDiscovery {
    api: Api<EndpointSlice>,
    service: String,
    port_name: String,
}

impl EndpointDiscovery {
    pub async fn in_cluster(
        namespace: &str,
        service: impl Into<String>,
        port_name: impl Into<String>,
    ) -> Result<Self> {
        let client = Client::try_default().await.map_err(kube_error)?;
        Ok(Self {
            api: Api::namespaced(client, namespace),
            service: service.into(),
            port_name: port_name.into(),
        })
    }

    pub async fn resolve(&self) -> Result<Vec<WorldRouteTarget>> {
        let slices = self
            .api
            .list(
                &ListParams::default()
                    .labels(&format!("kubernetes.io/service-name={}", self.service)),
            )
            .await
            .map_err(kube_error)?;
        Ok(targets_from_slices(slices.iter(), &self.port_name))
    }
}

pub fn targets_from_slices<'a>(
    slices: impl IntoIterator<Item = &'a EndpointSlice>,
    port_name: &str,
) -> Vec<WorldRouteTarget> {
    let mut targets = BTreeMap::<SocketAddr, String>::new();
    for slice in slices {
        let port = slice.ports.as_deref().and_then(|ports| {
            ports.iter().find_map(|port| {
                (port.name.as_deref() == Some(port_name)
                    && port.protocol.as_deref().is_none_or(|value| value == "TCP"))
                .then_some(port.port)
                .flatten()
                .filter(|value| (1..=65535).contains(value))
            })
        });
        let Some(port) = port else { continue };
        for endpoint in &slice.endpoints {
            let conditions = endpoint.conditions.as_ref();
            if conditions.and_then(|value| value.terminating) == Some(true)
                || conditions.and_then(|value| value.ready) == Some(false)
                || (conditions.and_then(|value| value.ready).is_none()
                    && conditions.and_then(|value| value.serving) == Some(false))
            {
                continue;
            }
            let world_id = endpoint
                .target_ref
                .as_ref()
                .filter(|target| target.kind.as_deref().is_none_or(|kind| kind == "Pod"))
                .and_then(|target| target.name.clone())
                .unwrap_or_default();
            if world_id.is_empty() {
                continue;
            }
            for address in &endpoint.addresses {
                if let Ok(ip) = address.parse() {
                    targets
                        .entry(SocketAddr::new(ip, port as u16))
                        .and_modify(|known| {
                            if world_id < *known {
                                known.clone_from(&world_id);
                            }
                        })
                        .or_insert_with(|| world_id.clone());
                }
            }
        }
    }
    let mut seen_ids = BTreeSet::new();
    targets
        .into_iter()
        .filter_map(|(address, world_id)| {
            seen_ids
                .insert(world_id.clone())
                .then_some(WorldRouteTarget { world_id, address })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};

    #[test]
    fn extracts_ready_tcp_targets_with_pod_identity() {
        let slice = EndpointSlice {
            ports: Some(vec![EndpointPort {
                name: Some("grpc".into()),
                port: Some(7000),
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            endpoints: vec![
                Endpoint {
                    addresses: vec!["10.0.0.2".into()],
                    target_ref: Some(ObjectReference {
                        kind: Some("Pod".into()),
                        name: Some("world-b".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Endpoint {
                    addresses: vec!["10.0.0.1".into()],
                    conditions: Some(EndpointConditions {
                        ready: Some(false),
                        ..Default::default()
                    }),
                    target_ref: Some(ObjectReference {
                        name: Some("world-a".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            targets_from_slices([&slice], "grpc"),
            vec![WorldRouteTarget {
                world_id: "world-b".into(),
                address: "10.0.0.2:7000".parse().unwrap(),
            }]
        );
    }
}
