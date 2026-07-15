use std::net::SocketAddr;
use std::time::Duration;

use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorldConfig {
    pub listen: SocketAddr,
    pub max_payload: usize,
    pub max_connections: usize,
    pub max_in_flight_per_connection: usize,
    pub tls_handshake_timeout: Duration,
    pub handler_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for WorldConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:18000".parse().expect("static address"),
            max_payload: 1 << 20,
            max_connections: 1024,
            max_in_flight_per_connection: 64,
            tls_handshake_timeout: Duration::from_secs(5),
            handler_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl WorldConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_connections == 0
            || self.max_in_flight_per_connection == 0
            || self.max_in_flight_per_connection > 4096
            || self.max_payload == 0
            || self.tls_handshake_timeout.is_zero()
            || self.handler_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
        {
            return Err(Error::InvalidConfig("world limits must be positive".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_uses_defaults_and_rejects_unknown_fields() {
        let config: WorldConfig = serde_json::from_str(r#"{"listen":"0.0.0.0:19000"}"#).unwrap();
        assert_eq!(config.listen.port(), 19000);
        assert_eq!(config.max_payload, WorldConfig::default().max_payload);
        assert!(serde_json::from_str::<WorldConfig>(r#"{"unknown":true}"#).is_err());
        assert!(serde_json::from_str::<WorldConfig>(r#"{"request_replay_capacity":100}"#).is_err());
    }
}
