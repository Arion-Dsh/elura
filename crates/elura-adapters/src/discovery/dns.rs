use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::gateway_world::{WorldDiscovery, WorldRouteTarget, WorldRouteUpdater};
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Configuration owned by the DNS discovery adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DnsWorldDiscoveryConfig {
    pub endpoint: String,
    pub region_id: u32,
    pub realm_id: u32,
    #[serde(default)]
    pub route: u32,
    pub refresh_interval: Duration,
}

impl DnsWorldDiscoveryConfig {
    /// Creates DNS discovery configuration for a World route scope.
    pub fn new(endpoint: impl Into<String>, region_id: u32, realm_id: u32) -> Self {
        Self {
            endpoint: endpoint.into(),
            region_id,
            realm_id,
            route: 0,
            refresh_interval: Duration::from_secs(5),
        }
    }
}

pub struct DnsWorldDiscovery {
    endpoint: String,
    region_id: u32,
    realm_id: u32,
    route: u32,
    refresh_interval: Duration,
}

impl DnsWorldDiscovery {
    pub fn new(config: DnsWorldDiscoveryConfig) -> Result<Self> {
        let valid_endpoint = config
            .endpoint
            .rsplit_once(':')
            .is_some_and(|(host, port)| {
                !host.trim().is_empty() && port.parse::<u16>().is_ok_and(|port| port > 0)
            });
        if !valid_endpoint
            || config.region_id == 0
            || config.realm_id == 0
            || config.refresh_interval.is_zero()
        {
            return Err(Error::InvalidConfig(
                "DNS discovery requires endpoint, region, realm and refresh interval".into(),
            ));
        }
        Ok(Self {
            endpoint: config.endpoint,
            region_id: config.region_id,
            realm_id: config.realm_id,
            route: config.route,
            refresh_interval: config.refresh_interval,
        })
    }

    async fn synchronize(&self, updater: &Arc<dyn WorldRouteUpdater>) -> Result<()> {
        let mut addresses = tokio::net::lookup_host(&self.endpoint)
            .await
            .map_err(|error| Error::Internal(format!("DNS World discovery: {error}")))?
            .filter(|address| !address.ip().is_unspecified() && address.port() > 0)
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        let targets = addresses
            .into_iter()
            .map(|address| WorldRouteTarget {
                world_id: address.to_string(),
                address,
            })
            .collect();
        updater
            .replace_targets(self.region_id, self.realm_id, self.route, targets)
            .await
    }
}

#[async_trait]
impl WorldDiscovery for DnsWorldDiscovery {
    async fn run(
        &self,
        updater: Arc<dyn WorldRouteUpdater>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ticker = tokio::time::interval(self.refresh_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                _ = ticker.tick() => {
                    if let Err(error) = self.synchronize(&updater).await {
                        tracing::warn!(%error, endpoint = %self.endpoint, "DNS World discovery refresh failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_config_builds_and_rejects_invalid_endpoints() {
        let config = DnsWorldDiscoveryConfig {
            endpoint: "world.internal:18000".into(),
            region_id: 1,
            realm_id: 1,
            route: 0,
            refresh_interval: Duration::from_secs(5),
        };
        DnsWorldDiscovery::new(config).unwrap();

        let invalid = DnsWorldDiscoveryConfig {
            endpoint: "world-without-port".into(),
            region_id: 1,
            realm_id: 1,
            route: 0,
            refresh_interval: Duration::from_secs(5),
        };
        assert!(DnsWorldDiscovery::new(invalid).is_err());
    }
}
