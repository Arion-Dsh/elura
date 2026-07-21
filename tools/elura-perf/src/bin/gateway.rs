use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use elura_adapters::online::RedisOnlineDirectory;
use elura_adapters::replay::RedisReplayStore;
use elura_core::online::DuplicateLoginMode;
use elura_gateway::transport::{TcpConfig, TcpTransport};
use elura_gateway::{Gateway, GatewayConfig, GatewayOnlineConfig, TcpWorldClient};
use elura_runtime::lifecycle::shutdown_signal;
use elura_runtime::observability::AdminServerConfig;
use elura_runtime::security::InternalToken;
use tokio::net::lookup_host;
use tokio::sync::watch;

type AnyError = Box<dyn Error + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

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

async fn resolve_address(value: &str) -> AnyResult<SocketAddr> {
    lookup_host(value)
        .await?
        .next()
        .ok_or_else(|| format!("cannot resolve {value}").into())
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let listen = env_parse("ELURA_GATEWAY_LISTEN", "0.0.0.0:17000")?;
    let world_address = resolve_address(&env_value("ELURA_WORLD_ADDRESS", "world:18000")).await?;
    let max_connections = env_parse("ELURA_MAX_CONNECTIONS", "20000")?;
    let max_connections_per_ip = env_parse("ELURA_MAX_CONNECTIONS_PER_IP", "20000")?;
    let ip_request_rate = env_parse("ELURA_IP_REQUEST_RATE", "0")?;
    let ip_request_burst = env_parse("ELURA_IP_REQUEST_BURST", "0")?;
    let world_pool_size = env_parse("ELURA_WORLD_POOL_SIZE", "32")?;
    let world_in_flight = env_parse("ELURA_WORLD_IN_FLIGHT", "64")?;
    let gateway_id = env_value("ELURA_GATEWAY_ID", "gateway-1");
    let redis_url = env_value("ELURA_REDIS_URL", "redis://redis:6379/");
    let internal_token = InternalToken::new(env_value(
        "ELURA_INTERNAL_TOKEN",
        "elura-rs-perf-internal-token-2026",
    ))?;
    let ticket_key = env_value(
        "ELURA_TICKET_KEY",
        "elura-rs-perf-ticket-key-at-least-32-bytes-2026",
    );

    let replay = Arc::new(RedisReplayStore::connect(&redis_url, "elura-perf").await?);
    let online = Arc::new(
        RedisOnlineDirectory::connect(&redis_url, "elura-perf:online", Duration::from_secs(60))
            .await?,
    );
    let world = Arc::new(
        TcpWorldClient::with_pool_size(world_address, 1 << 20, world_pool_size)?
            .with_max_in_flight_per_connection(world_in_flight)?
            .with_internal_token(internal_token),
    );
    let mut config = GatewayConfig::default();
    config.max_connections = max_connections;
    config.max_connections_per_ip = max_connections_per_ip;
    config.request_rate = 100_000;
    config.request_burst = 100_000;
    config.ip_request_rate = ip_request_rate;
    config.ip_request_burst = ip_request_burst;
    config.handler_timeout = Duration::from_secs(30);
    config.idle_timeout = Duration::from_secs(300);
    config.ticket.key = ticket_key;
    config.ticket.issuer = "auth".into();
    config.ticket.audience = "gateway".into();
    config.ticket.login_ttl = Duration::from_secs(300);
    let mut tcp_config = TcpConfig::default();
    tcp_config.listen = listen;
    let tcp = TcpTransport::new(tcp_config)?;
    let gateway = Gateway::new(config)
        .transport(tcp)
        .replay_store(replay)
        .world_client(world)
        .online_directory(
            online,
            GatewayOnlineConfig::new(
                gateway_id.clone(),
                Duration::from_secs(60),
                Duration::from_secs(20),
                DuplicateLoginMode::AllowMultiple,
            ),
        )
        .build()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });
    let admin = AdminServerConfig::new(
        env_parse("ELURA_GATEWAY_ADMIN_LISTEN", "127.0.0.1:17001")?,
        "gateway",
        gateway_id,
    );
    gateway.serve(admin, shutdown_rx).await?;
    Ok(())
}
