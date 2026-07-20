use std::{env, fs, sync::Arc};

use elura::adapters::discovery::{DnsWorldDiscovery, DnsWorldDiscoveryConfig};
use elura::prelude::*;
use serde::Deserialize;

/// Application-owned configuration. Elura never chooses an adapter or reads
/// this file/environment on the application's behalf.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfig {
    runtime: GatewayConfig,
    admin: AdminServerConfig,
    tcp: TcpConfig,
    #[serde(default)]
    udp: Option<UdpConfig>,
    #[serde(default)]
    quic: Option<QuicConfig>,
    #[serde(default)]
    webtransport: Option<WebTransportConfig>,
    discovery: DnsWorldDiscoveryConfig,
}

impl AppConfig {
    fn load() -> elura::Result<Self> {
        let path = env::var("APP_GATEWAY_CONFIG").unwrap_or_else(|_| "config/gateway.json".into());
        let mut config: Self = serde_json::from_slice(&fs::read(path)?)?;
        config.runtime.ticket.key = required_env("APP_TICKET_KEY")?;
        config.runtime.internal_token = Some(required_env("APP_INTERNAL_TOKEN")?);
        if let Some(value) = optional_env("APP_GATEWAY_ADDR") {
            config.tcp.listen = parse_address("APP_GATEWAY_ADDR", &value)?;
        }
        if let Some(value) = optional_env("APP_GATEWAY_ADMIN_ADDR") {
            config.admin.listen = parse_address("APP_GATEWAY_ADMIN_ADDR", &value)?;
        }
        config.admin.token = optional_env("APP_ADMIN_TOKEN");
        if let Some(value) = optional_env("APP_INSTANCE_ID") {
            config.admin.instance_id = value;
        }
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> elura::Result<()> {
    let app = AppConfig::load()?;
    let discovery = Arc::new(DnsWorldDiscovery::new(app.discovery)?);
    let tcp = TcpTransport::new(app.tcp)?;
    let mut gateway = Gateway::new(app.runtime).transport(tcp);
    if let Some(udp) = app.udp {
        gateway = gateway.transport(udp);
    }
    if let Some(quic) = app.quic {
        gateway = gateway.transport(quic);
    }
    if let Some(webtransport) = app.webtransport {
        gateway = gateway.transport(webtransport);
    }
    gateway.world_discovery(discovery).run(app.admin).await
}

fn required_env(name: &str) -> elura::Result<String> {
    env::var(name).map_err(|_| elura::Error::InvalidConfig(format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_address(name: &str, value: &str) -> elura::Result<std::net::SocketAddr> {
    value
        .parse()
        .map_err(|_| elura::Error::InvalidConfig(format!("invalid {name}")))
}
