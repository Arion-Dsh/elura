use std::{env, fs};

use elura::prelude::*;
use prost::Message;
use serde::Deserialize;

const ROUTE_GET_PLAYER_PROFILE: u32 = 100;

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

/// Application-owned configuration. Add an adapter-specific registration
/// config here when this deployment needs service registration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfig {
    runtime: WorldLaunchConfig,
    profile: PlayerProfileConfig,
}

impl AppConfig {
    fn load() -> elura::Result<Self> {
        let path = env::var("APP_WORLD_CONFIG").unwrap_or_else(|_| "config/world.json".into());
        let mut config: Self = serde_json::from_slice(&fs::read(path)?)?;
        config.runtime.internal_token = required_env("APP_INTERNAL_TOKEN")?;
        if let Some(value) = optional_env("APP_WORLD_LISTEN") {
            config.runtime.world.listen = parse_address("APP_WORLD_LISTEN", &value)?;
        }
        if let Some(value) = optional_env("APP_WORLD_ADMIN_ADDR") {
            config.runtime.admin.listen = parse_address("APP_WORLD_ADMIN_ADDR", &value)?;
        }
        config.runtime.admin.token = optional_env("APP_ADMIN_TOKEN");
        if let Some(value) = optional_env("APP_INSTANCE_ID") {
            config.runtime.admin.instance_id = value;
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
    WorldLauncher::new(app.runtime)?
        .configure(move |builder| register(builder, app.profile))?
        .run()
        .await
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

fn register(builder: &mut WorldBuilder, profile: PlayerProfileConfig) -> elura::Result<()> {
    builder.register_handler::<GetPlayerProfileRequest, GetPlayerProfileResponse, _, _>(
        ROUTE_GET_PLAYER_PROFILE,
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
    )?;
    Ok(())
}
