use bytes::Bytes;
use elura_gateway::GatewayConfig;
use elura_gateway::transport::{TcpConfig, TcpTransport};
use elura_monolith::{Monolith, MonolithServer};
use elura_runtime::observability::AdminServerConfig;
use elura_world::WorldConfig;

#[allow(dead_code)]
async fn run_monolith(monolith: Monolith, admin: AdminServerConfig) -> elura_core::Result<()> {
    monolith.run(admin).await
}

fn assemble() -> elura_core::Result<MonolithServer> {
    let mut gateway = GatewayConfig::default();
    gateway.ticket.key = "public-api-key-public-api-key-0001".into();

    Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(TcpConfig::default())?)
        .route_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
        .build()
}

#[test]
fn application_api_uses_only_public_types() {
    assemble().unwrap();
}
