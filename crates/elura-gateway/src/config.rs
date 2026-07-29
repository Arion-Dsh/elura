use std::collections::HashMap;
use std::time::Duration;

use elura_core::protocol::{HEADER_LEN, ROUTE_AUTHENTICATE};
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::builder::{GatewayRealmAdmissionConfig, GatewayTicketConfig, GatewayWorldTlsConfig};
use crate::discovery::GatewayWorldRoutingConfig;
use crate::protection::ProtectionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRateLimit {
    pub requests_per_second: u32,
    pub burst: u32,
}

/// Transport-neutral Gateway Session and process configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayConfig {
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub max_payload: usize,
    pub request_rate: u32,
    pub request_burst: u32,
    pub inbound_byte_rate: u32,
    pub inbound_byte_burst: u32,
    pub ip_request_rate: u32,
    pub ip_request_burst: u32,
    pub max_rate_limit_violations: u32,
    pub max_protocol_violations: u32,
    pub route_rate_limits: HashMap<u32, RouteRateLimit>,
    pub inbound_queue: usize,
    pub response_queue: usize,
    pub push_queue: usize,
    pub idle_timeout: Duration,
    pub authentication_timeout: Duration,
    pub handler_timeout: Duration,
    pub write_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub shutdown_timeout: Duration,
    pub readiness_timeout: Duration,
    pub ticket: GatewayTicketConfig,
    #[serde(skip)]
    pub internal_token: Option<String>,
    pub protection: Option<ProtectionConfig>,
    pub world_tls: Option<GatewayWorldTlsConfig>,
    pub world_routing: GatewayWorldRoutingConfig,
    pub realm_admission: Option<GatewayRealmAdmissionConfig>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_connections_per_ip: 100,
            max_payload: 1 << 20,
            request_rate: 200,
            request_burst: 400,
            inbound_byte_rate: 8 << 20,
            inbound_byte_burst: 2 << 20,
            ip_request_rate: 2_000,
            ip_request_burst: 4_000,
            max_rate_limit_violations: 20,
            max_protocol_violations: 3,
            route_rate_limits: HashMap::from([(
                ROUTE_AUTHENTICATE,
                RouteRateLimit {
                    requests_per_second: 5,
                    burst: 5,
                },
            )]),
            inbound_queue: 64,
            response_queue: 64,
            push_queue: 64,
            idle_timeout: Duration::from_secs(90),
            authentication_timeout: Duration::from_secs(10),
            handler_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            readiness_timeout: Duration::from_secs(2),
            ticket: GatewayTicketConfig::default(),
            internal_token: None,
            protection: None,
            world_tls: None,
            world_routing: GatewayWorldRoutingConfig::default(),
            realm_admission: None,
        }
    }
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<()> {
        positive(self.max_connections, "max_connections")?;
        positive(self.max_connections_per_ip, "max_connections_per_ip")?;
        positive(self.max_payload, "max_payload")?;
        positive(self.request_rate, "request_rate")?;
        positive(self.request_burst, "request_burst")?;
        positive(self.inbound_byte_rate, "inbound_byte_rate")?;
        positive(self.inbound_byte_burst, "inbound_byte_burst")?;
        positive(self.max_rate_limit_violations, "max_rate_limit_violations")?;
        positive(self.max_protocol_violations, "max_protocol_violations")?;
        positive(self.inbound_queue, "inbound_queue")?;
        positive(self.response_queue, "response_queue")?;
        positive(self.push_queue, "push_queue")?;

        let minimum_burst = self
            .max_payload
            .checked_add(HEADER_LEN)
            .ok_or_else(|| invalid("max_payload is too large"))?;
        if (self.inbound_byte_burst as usize) < minimum_burst {
            return Err(invalid(
                "inbound_byte_burst must fit one maximum-sized frame",
            ));
        }

        match (self.ip_request_rate, self.ip_request_burst) {
            (0, 0) => {}
            (0, _) | (_, 0) => {
                return Err(invalid(
                    "ip_request_rate and ip_request_burst must both be zero or both be positive",
                ));
            }
            _ => {}
        }

        for (name, value) in [
            ("idle_timeout", self.idle_timeout),
            ("authentication_timeout", self.authentication_timeout),
            ("handler_timeout", self.handler_timeout),
            ("write_timeout", self.write_timeout),
            ("heartbeat_interval", self.heartbeat_interval),
            ("shutdown_timeout", self.shutdown_timeout),
            ("readiness_timeout", self.readiness_timeout),
        ] {
            if value.is_zero() {
                return Err(invalid(format!("{name} must be positive")));
            }
        }

        for (&route, limit) in &self.route_rate_limits {
            if route == 0 {
                return Err(invalid("route_rate_limits cannot contain route 0"));
            }
            if limit.requests_per_second == 0 || limit.burst == 0 {
                return Err(invalid(format!(
                    "route_rate_limits[{route}] must have a positive rate and burst"
                )));
            }
        }

        if self.ticket.login_ttl.is_zero() {
            return Err(invalid("ticket.login_ttl must be positive"));
        }
        if self.ticket.reconnect_ttl.is_zero() {
            return Err(invalid("ticket.reconnect_ttl must be positive"));
        }

        self.world_routing.validate()
    }
}

fn positive<T>(value: T, name: &str) -> Result<()>
where
    T: Default + PartialEq,
{
    if value == T::default() {
        return Err(invalid(format!("{name} must be positive")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidConfig(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_uses_defaults_and_rejects_unknown_fields() {
        let config: GatewayConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            config.max_connections,
            GatewayConfig::default().max_connections
        );
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"unknown":true}"#).is_err());
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"listen":"0.0.0.0:17000"}"#).is_err());
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"admin":null}"#).is_err());
    }

    #[test]
    fn validates_resource_limits() {
        let mut config = GatewayConfig::default();
        config.inbound_byte_burst = config.max_payload as u32;
        assert!(config.validate().is_err());

        let config = GatewayConfig {
            request_rate: 0,
            ..GatewayConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfig(message)) if message == "request_rate must be positive"
        ));

        let mut config = GatewayConfig {
            ip_request_rate: 0,
            ip_request_burst: 0,
            ..GatewayConfig::default()
        };
        assert!(config.validate().is_ok());
        config.ip_request_burst = 1;
        assert!(config.validate().is_err());
    }
}
