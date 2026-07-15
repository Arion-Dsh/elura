//! Deployment-specific World discovery adapters.
//!
//! Every adapter owns its typed configuration. This module never reads files,
//! environment variables or a global application configuration; the upper
//! application constructs the selected adapter and injects it into the runtime.

mod dns;
#[cfg(feature = "redis")]
mod redis;

pub use dns::{DnsWorldDiscovery, DnsWorldDiscoveryConfig};
#[cfg(feature = "redis")]
pub use redis::{
    RedisWorldDiscovery, RedisWorldDiscoveryConfig, RedisWorldRegistrar,
    RedisWorldRegistrationConfig,
};

#[cfg(feature = "kubernetes")]
use std::sync::Arc;

#[cfg(feature = "kubernetes")]
use elura_core::Result;
#[cfg(feature = "kubernetes")]
use elura_core::gateway_world::WorldDiscovery;
#[cfg(feature = "kubernetes")]
use elura_core::gateway_world::WorldRouteUpdater;
#[cfg(feature = "kubernetes")]
use tokio::sync::watch;

#[cfg(feature = "kubernetes")]
use crate::kubernetes::{EndpointWatcher, EndpointWatcherConfig};

/// Kubernetes EndpointSlice-backed World discovery.
#[cfg(feature = "kubernetes")]
pub struct KubernetesWorldDiscovery {
    config: EndpointWatcherConfig,
}

#[cfg(feature = "kubernetes")]
impl KubernetesWorldDiscovery {
    pub fn new(config: EndpointWatcherConfig) -> Self {
        Self { config }
    }
}

#[cfg(feature = "kubernetes")]
#[async_trait::async_trait]
impl WorldDiscovery for KubernetesWorldDiscovery {
    async fn run(
        &self,
        updater: Arc<dyn WorldRouteUpdater>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        EndpointWatcher::in_cluster(self.config.clone(), updater)
            .await?
            .run(shutdown)
            .await
    }
}
