use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Transport exposed by a public Gateway endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GatewayEndpointTransport {
    /// A raw TCP ELR2 endpoint.
    Tcp,
    /// An unencrypted WebSocket endpoint.
    Ws,
    /// A TLS-protected WebSocket endpoint.
    Wss,
    /// A TLS 1.3 QUIC endpoint carrying ELR2 frames.
    Quic,
}

/// A public Gateway endpoint for one region and realm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealmGatewayEndpoint {
    pub region_id: u32,
    pub realm_id: u32,
    pub transport: GatewayEndpointTransport,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub path: String,
}

impl RealmGatewayEndpoint {
    /// Validates endpoint configuration before exposing it to clients.
    pub fn validate(&self) -> Result<()> {
        if self.region_id == 0
            || self.realm_id == 0
            || self.host.trim().is_empty()
            || self.host.chars().any(char::is_whitespace)
            || self.port == 0
        {
            return Err(Error::InvalidConfig(
                "invalid public Gateway endpoint".into(),
            ));
        }
        match self.transport {
            GatewayEndpointTransport::Tcp | GatewayEndpointTransport::Quic
                if !self.path.is_empty() =>
            {
                Err(Error::InvalidConfig(
                    "TCP and QUIC Gateway endpoints cannot have a path".into(),
                ))
            }
            GatewayEndpointTransport::Ws | GatewayEndpointTransport::Wss
                if !self.path.starts_with('/') =>
            {
                Err(Error::InvalidConfig(
                    "WebSocket Gateway endpoint path must start with /".into(),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Formats the client-facing address returned by a login service.
    pub fn address(&self) -> Result<String> {
        self.validate()?;
        match self.transport {
            GatewayEndpointTransport::Tcp => Ok(format!("{}:{}", self.host, self.port)),
            GatewayEndpointTransport::Ws => {
                Ok(format!("ws://{}:{}{}", self.host, self.port, self.path))
            }
            GatewayEndpointTransport::Wss => {
                Ok(format!("wss://{}:{}{}", self.host, self.port, self.path))
            }
            GatewayEndpointTransport::Quic => Ok(format!("quic://{}:{}", self.host, self.port)),
        }
    }
}

/// In-memory directory used by the login service to resolve a realm's public Gateway endpoint.
#[derive(Default)]
pub struct RealmGatewayDirectory {
    endpoints: RwLock<HashMap<(u32, u32), RealmGatewayEndpoint>>,
}

impl RealmGatewayDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replaces the public endpoint map.
    pub fn replace(&self, endpoints: impl IntoIterator<Item = RealmGatewayEndpoint>) -> Result<()> {
        let mut next = HashMap::new();
        for endpoint in endpoints {
            endpoint.validate()?;
            let key = (endpoint.region_id, endpoint.realm_id);
            if next.insert(key, endpoint).is_some() {
                return Err(Error::InvalidConfig(
                    "duplicate realm Gateway endpoint".into(),
                ));
            }
        }
        *self
            .endpoints
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
        Ok(())
    }

    /// Resolves the endpoint to include in a successful login response.
    pub fn resolve(&self, region_id: u32, realm_id: u32) -> Result<RealmGatewayEndpoint> {
        self.endpoints
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(region_id, realm_id))
            .cloned()
            .ok_or(Error::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(region_id: u32, realm_id: u32) -> RealmGatewayEndpoint {
        RealmGatewayEndpoint {
            region_id,
            realm_id,
            transport: GatewayEndpointTransport::Wss,
            host: "r1.game.example".into(),
            port: 443,
            path: "/elr2".into(),
        }
    }

    #[test]
    fn resolves_serialized_endpoint() {
        let endpoint: RealmGatewayEndpoint = serde_json::from_str(
            r#"{"region_id":1,"realm_id":2,"transport":"wss","host":"r1.game.example","port":443,"path":"/elr2"}"#,
        )
        .unwrap();
        assert_eq!(
            endpoint.address().unwrap(),
            "wss://r1.game.example:443/elr2"
        );

        let directory = RealmGatewayDirectory::new();
        directory.replace([endpoint]).unwrap();
        assert_eq!(directory.resolve(1, 2).unwrap().host, "r1.game.example");
    }

    #[test]
    fn rejects_duplicate_or_invalid_endpoints() {
        let directory = RealmGatewayDirectory::new();
        assert!(directory.replace([endpoint(1, 1), endpoint(1, 1)]).is_err());

        let mut invalid = endpoint(1, 2);
        invalid.path = "elr2".into();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn formats_quic_endpoint() {
        let mut endpoint = endpoint(1, 2);
        endpoint.transport = GatewayEndpointTransport::Quic;
        endpoint.path.clear();
        assert_eq!(endpoint.address().unwrap(), "quic://r1.game.example:443");
    }
}
