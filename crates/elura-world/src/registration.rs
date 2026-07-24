//! World-owned service-registration contracts.

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

/// Provider-neutral description published by a World instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRegistration {
    pub world_id: String,
    pub region_id: u32,
    pub realm_id: u32,
    pub route: u32,
    pub address: String,
}

impl WorldRegistration {
    pub fn validate(&self) -> Result<()> {
        let valid_id = !self.world_id.is_empty()
            && self
                .world_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        let unspecified_address = self
            .address
            .parse::<SocketAddr>()
            .is_ok_and(|address| address.ip().is_unspecified());
        let valid_address = self.address.rsplit_once(':').is_some_and(|(host, port)| {
            !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port > 0)
        });
        if !valid_id
            || self.region_id == 0
            || self.realm_id == 0
            || !valid_address
            || unspecified_address
        {
            return Err(Error::InvalidConfig("invalid World registration".into()));
        }
        Ok(())
    }
}

/// Registers and renews one World instance with a discovery backend.
#[async_trait]
pub trait WorldRegistrar: Send + Sync + 'static {
    fn renew_interval(&self) -> Duration;
    async fn register(&self) -> Result<()>;
    async fn renew(&self) -> Result<()>;
    async fn unregister(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_validates_provider_neutral_identity() {
        let mut registration = WorldRegistration {
            world_id: "world-1".into(),
            region_id: 1,
            realm_id: 1,
            route: 0,
            address: "127.0.0.1:18000".into(),
        };
        registration.validate().unwrap();
        registration.world_id = "world:1".into();
        assert!(registration.validate().is_err());
    }
}
