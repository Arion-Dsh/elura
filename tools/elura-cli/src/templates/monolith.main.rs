use std::{env, fs};

use elura::prelude::*;
use prost::Message;
use serde::Deserialize;

struct GetPlayerProfile;

impl Route for GetPlayerProfile {
    const ID: u32 = 100;
    const NAME: &'static str = "player.get_profile";

    type Request = GetPlayerProfileRequest;
    type Response = GetPlayerProfileResponse;
}

#[derive(Clone, Deserialize)]
struct PlayerProfileConfig {
    display_name_prefix: String,
    welcome_message: String,
}

impl PlayerProfileConfig {
    fn display_name(&self, user_id: i64) -> String {
        format!("{}{user_id}", self.display_name_prefix)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfig {
    gateway: GatewayConfig,
    world: WorldConfig,
    admin: AdminServerConfig,
    tcp: TcpConfig,
    #[serde(default)]
    udp: Option<UdpConfig>,
    #[serde(default)]
    quic: Option<QuicConfig>,
    #[serde(default)]
    webtransport: Option<WebTransportConfig>,
    profile: PlayerProfileConfig,
}

impl AppConfig {
    fn load() -> elura::Result<Self> {
        let path = env::var("APP_MONOLITH_CONFIG")
            .unwrap_or_else(|_| "config/monolith.json".into());
        let mut config: Self = serde_json::from_slice(&fs::read(path)?)?;
        config.gateway.ticket.key = required_env("APP_TICKET_KEY")?;
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

#[derive(Clone, PartialEq, Message)]
struct GetPlayerProfileRequest {}

#[derive(Clone, PartialEq, Message)]
struct GetPlayerProfileResponse {
    #[prost(int64, tag = "1")]
    user_id: i64,
    #[prost(uint32, tag = "2")]
    region_id: u32,
    #[prost(uint32, tag = "3")]
    realm_id: u32,
    #[prost(string, tag = "4")]
    display_name: String,
    #[prost(string, tag = "5")]
    welcome_message: String,
}

#[tokio::main]
async fn main() -> elura::Result<()> {
    let app = AppConfig::load()?;
    let tcp = TcpTransport::new(app.tcp)?;
    let mut monolith = Monolith::new(app.gateway, app.world).transport(tcp);
    if let Some(udp) = app.udp {
        monolith = monolith.transport(udp);
    }
    if let Some(quic) = app.quic {
        monolith = monolith.transport(quic);
    }
    if let Some(webtransport) = app.webtransport {
        monolith = monolith.transport(webtransport);
    }
    register(monolith, app.profile).run(app.admin).await
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

fn register(monolith: Monolith, profile: PlayerProfileConfig) -> Monolith {
    monolith.route(
        GetPlayerProfile,
        move |context: WorldContext, _request| {
            let profile = profile.clone();
            async move {
                let identity = context.identity;
                Ok(GetPlayerProfileResponse {
                    user_id: identity.user_id,
                    region_id: identity.region_id,
                    realm_id: identity.realm_id,
                    display_name: profile.display_name(identity.user_id),
                    welcome_message: profile.welcome_message,
                })
            }
        },
    )
}
