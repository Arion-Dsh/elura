use std::error::Error;
use std::str::FromStr;
use std::time::Duration;

use elura_runtime::lifecycle::shutdown_signal;
use elura_runtime::security::InternalToken;
use elura_world::{WorldConfig, WorldServer};
use tokio::sync::watch;

type AnyError = Box<dyn Error + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

const ROUTE_ECHO: u32 = 1000;

fn env_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T>(name: &str, default: &str) -> AnyResult<T>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    Ok(env_value(name, default).parse()?)
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let handler_delay = Duration::from_micros(env_parse("ELURA_HANDLER_DELAY_US", "0")?);
    let mut builder = WorldServer::builder(WorldConfig {
        listen: env_parse("ELURA_WORLD_LISTEN", "0.0.0.0:18000")?,
        max_connections: env_parse("ELURA_WORLD_MAX_CONNECTIONS", "1024")?,
        max_in_flight_per_connection: env_parse("ELURA_WORLD_IN_FLIGHT", "64")?,
        handler_timeout: Duration::from_secs(30),
        ..WorldConfig::default()
    })?;
    builder.register(ROUTE_ECHO, move |_context, payload| async move {
        if !handler_delay.is_zero() {
            tokio::time::sleep(handler_delay).await;
        }
        Ok(payload)
    })?;
    let world = builder
        .build()?
        .with_internal_token(InternalToken::new(env_value(
            "ELURA_INTERNAL_TOKEN",
            "elura-rs-perf-internal-token-2026",
        ))?);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });
    world.serve(shutdown_rx).await?;
    Ok(())
}
