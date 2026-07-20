use std::sync::{Arc, Mutex};

use elura::prelude::*;
use elura_spot_demo::{Arena, DEMO_TICKET_KEY, Move, SERVER_ADDRESS};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> elura::Result<()> {
    init_logging();

    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| SERVER_ADDRESS.to_owned())
        .parse()
        .map_err(|_| elura::Error::InvalidConfig("invalid server address".into()))?;

    let mut gateway = GatewayConfig::default();
    gateway.ticket.key = DEMO_TICKET_KEY.to_owned();

    let mut tcp = TcpConfig::default();
    tcp.listen = address;

    let arena = Arc::new(Mutex::new(Arena::default()));
    info!(%address, admin = "127.0.0.1:17001", "tiny arena server starting");

    Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(tcp)?)
        .route(Move, move |context: WorldContext, request| {
            let arena = arena.clone();
            async move {
                let mut arena = arena
                    .lock()
                    .map_err(|_| elura::Error::Internal("arena lock poisoned".into()))?;
                let player_id = context.identity.user_id;
                let joined = !arena.contains_player(player_id);
                let dx = request.dx;
                let dy = request.dy;
                let snapshot = arena.apply_move(player_id, request);

                if joined {
                    info!(
                        player_id,
                        players = snapshot.players.len(),
                        "player joined arena"
                    );
                }
                if let Some(player) = snapshot
                    .players
                    .iter()
                    .find(|player| player.id == player_id)
                {
                    debug!(
                        player_id,
                        dx,
                        dy,
                        x = player.x,
                        y = player.y,
                        players = snapshot.players.len(),
                        "authoritative movement applied"
                    );
                }

                Ok(snapshot)
            }
        })
        .run(AdminServerConfig::loopback(
            17001,
            "tiny-arena",
            "local-demo",
        ))
        .await
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,elura_gateway=info,elura_world=info,elura_spot_demo=info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
