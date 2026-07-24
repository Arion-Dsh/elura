use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elura::gameplay::simulation::{FixedStepClock, SimulationConfig};
use elura::prelude::*;
use elura_spot_demo::{
    Arena, DEMO_TICKET_KEY, Move, Realtime, SERVER_ADDRESS, TICK_DURATION, TICK_RATE,
};
use tracing::{debug, error, info};
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
    let tick_task = tokio::spawn(run_fixed_ticks(arena.clone()));
    let movement_arena = arena.clone();
    let realtime_arena = arena;
    info!(
        %address,
        admin = "127.0.0.1:17001",
        tick_rate = TICK_RATE,
        "tiny arena server starting"
    );

    let result = Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(tcp)?)
        .route(Move, move |context: WorldContext, request| {
            let arena = movement_arena.clone();
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
        .route(Realtime, move |context: WorldContext, request| {
            let arena = realtime_arena.clone();
            async move {
                arena
                    .lock()
                    .map_err(|_| elura::Error::Internal("arena lock poisoned".into()))?
                    .exchange(context.identity.user_id, request)
            }
        })
        .run(AdminServerConfig::loopback(
            17001,
            "tiny-arena",
            "local-demo",
        ))
        .await;
    tick_task.abort();
    result
}

async fn run_fixed_ticks(arena: Arc<Mutex<Arena>>) {
    let mut simulation = SimulationConfig::default();
    simulation.step = TICK_DURATION;
    let mut clock = FixedStepClock::new(simulation).expect("valid fixed-Tick config");
    let mut pulse = tokio::time::interval(Duration::from_millis(10));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = Instant::now();

    loop {
        pulse.tick().await;
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(previous);
        previous = now;
        let result = arena
            .lock()
            .map_err(|_| elura::Error::Internal("arena lock poisoned".into()))
            .and_then(|mut arena| {
                clock.advance(elapsed, |_| arena.advance_tick())?;
                Ok(())
            });
        if let Err(error) = result {
            error!(%error, "authoritative arena Tick failed");
        }
    }
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
