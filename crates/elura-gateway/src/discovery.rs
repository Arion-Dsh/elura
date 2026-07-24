//! Gateway-owned World discovery and command-routing contracts.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
pub use elura_core::gateway_world::WorldRequest;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Gateway-side port for dispatching commands to World.
#[async_trait]
pub trait WorldClient: Send + Sync + 'static {
    async fn command(&self, request: WorldRequest) -> Result<Bytes>;

    async fn readiness(&self) -> Result<()> {
        Ok(())
    }
}

/// Limits for Gateway-to-World connection pools.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayWorldRoutingConfig {
    pub pool_size: usize,
    pub max_in_flight_per_connection: usize,
}

impl Default for GatewayWorldRoutingConfig {
    fn default() -> Self {
        Self {
            pool_size: 1,
            max_in_flight_per_connection: 64,
        }
    }
}

impl GatewayWorldRoutingConfig {
    pub fn validate(&self) -> Result<()> {
        if self.pool_size == 0
            || self.pool_size > 1024
            || self.max_in_flight_per_connection == 0
            || self.max_in_flight_per_connection > 4096
        {
            return Err(Error::InvalidConfig(
                "invalid Gateway World routing limits".into(),
            ));
        }
        Ok(())
    }
}

/// One routable World instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRouteTarget {
    pub world_id: String,
    pub address: SocketAddr,
}

impl WorldRouteTarget {
    pub fn validate(&self) -> Result<()> {
        if self.world_id.trim().is_empty() || self.address.port() == 0 {
            return Err(Error::InvalidConfig("invalid World route target".into()));
        }
        Ok(())
    }
}

/// Receives complete target sets from a discovery source.
#[async_trait]
pub trait WorldRouteUpdater: Send + Sync + 'static {
    async fn replace_targets(
        &self,
        region_id: u32,
        realm_id: u32,
        route: u32,
        targets: Vec<WorldRouteTarget>,
    ) -> Result<()>;
}

/// Runs Gateway-side World discovery until shutdown.
#[async_trait]
pub trait WorldDiscovery: Send + Sync + 'static {
    async fn run(
        &self,
        updater: Arc<dyn WorldRouteUpdater>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_config_contains_no_provider_configuration() {
        let encoded = serde_json::to_value(GatewayWorldRoutingConfig::default()).unwrap();
        assert_eq!(encoded["pool_size"], 1);
        assert!(encoded.get("provider").is_none());
        assert!(encoded.get("url").is_none());
    }
}
