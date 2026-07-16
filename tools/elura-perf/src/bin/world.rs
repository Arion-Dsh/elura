use std::error::Error;
use std::str::FromStr;
use std::time::Duration;

use elura_runtime::lifecycle::shutdown_signal;
use elura_runtime::observability::AdminServerConfig;
use elura_world::{World, WorldConfig};
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
    let mut config = WorldConfig::default();
    config.listen = env_parse("ELURA_WORLD_LISTEN", "0.0.0.0:18000")?;
    config.max_connections = env_parse("ELURA_WORLD_MAX_CONNECTIONS", "1024")?;
    config.max_in_flight_per_connection = env_parse("ELURA_WORLD_IN_FLIGHT", "64")?;
    config.handler_timeout = Duration::from_secs(30);
    config.internal_token = Some(env_value(
        "ELURA_INTERNAL_TOKEN",
        "elura-rs-perf-internal-token-2026",
    ));
    let world = World::new(config)
        .route_raw(ROUTE_ECHO, move |_context, payload| async move {
            if !handler_delay.is_zero() {
                tokio::time::sleep(handler_delay).await;
            }
            Ok(payload)
        })
        .build()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });
    let admin = AdminServerConfig::new(
        env_parse("ELURA_WORLD_ADMIN_LISTEN", "127.0.0.1:18001")?,
        "world",
        env_value("ELURA_WORLD_ID", "world-1"),
    );
    world.serve(admin, shutdown_rx).await?;
    Ok(())
}
