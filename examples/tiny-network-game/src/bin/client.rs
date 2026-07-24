use std::collections::HashMap;
use std::io;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use elura::prelude::{Identity, TicketService};
use elura_client::{ClientError, ClientEvent, ClientResult, ConnectionState, EluraClient};
use elura_spot_demo::{
    ARENA_HEIGHT, ARENA_WIDTH, DEMO_TICKET_KEY, MOVE_STEP, MoveRequest, PLAYER_SIZE, ROUTE_MOVE,
    SERVER_ADDRESS, Snapshot,
};
use spottedcat::{Context, DrawOption, Image, Key, Pt, Spot, Text, WindowConfig};

enum NetworkEvent {
    Status(String),
    Snapshot(Snapshot),
}

struct Game {
    player_id: i64,
    input_tx: Sender<(i32, i32)>,
    event_rx: Receiver<NetworkEvent>,
    last_input: (i32, i32),
    players: HashMap<i64, SmoothedPlayer>,
    snapshot_generation: u64,
    status: String,
    local_image: Image,
    remote_image: Image,
    font_id: u32,
}

struct SmoothedPlayer {
    x: f32,
    y: f32,
    target_x: f32,
    target_y: f32,
    seen_generation: u64,
}

impl Spot for Game {
    fn initialize(ctx: &mut Context) -> Self {
        let player_id = std::env::args()
            .nth(1)
            .and_then(|value| value.parse().ok())
            .filter(|id: &i64| *id > 0)
            .unwrap_or(1);
        let address = std::env::args()
            .nth(2)
            .unwrap_or_else(|| SERVER_ADDRESS.to_owned());
        let (input_tx, input_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        spawn_network(player_id, address, input_rx, event_tx);

        let local_image = solid_image(ctx, [70, 240, 160, 255]);
        let remote_image = solid_image(ctx, [255, 105, 145, 255]);
        let font_id = spottedcat::register_font(ctx, load_system_font());

        Self {
            player_id,
            input_tx,
            event_rx,
            last_input: (0, 0),
            players: HashMap::new(),
            snapshot_generation: 0,
            status: "connecting...".into(),
            local_image,
            remote_image,
            font_id,
        }
    }

    fn update(&mut self, ctx: &mut Context, dt: Duration) {
        let x =
            i32::from(spottedcat::key_down(ctx, Key::D) || spottedcat::key_down(ctx, Key::Right))
                - i32::from(
                    spottedcat::key_down(ctx, Key::A) || spottedcat::key_down(ctx, Key::Left),
                );
        let y =
            i32::from(spottedcat::key_down(ctx, Key::S) || spottedcat::key_down(ctx, Key::Down))
                - i32::from(
                    spottedcat::key_down(ctx, Key::W) || spottedcat::key_down(ctx, Key::Up),
                );
        if (x, y) != self.last_input {
            self.last_input = (x, y);
            let _ = self.input_tx.send((x, y));
        }

        let events = self.event_rx.try_iter().collect::<Vec<_>>();
        for event in events {
            match event {
                NetworkEvent::Status(status) => self.status = status,
                NetworkEvent::Snapshot(snapshot) => self.apply_snapshot(snapshot),
            }
        }

        self.smooth_players(dt.as_secs_f32());
    }

    fn draw(&mut self, ctx: &mut Context, screen: Image) {
        let (width, height) = spottedcat::window_size(ctx);
        let scale_x = width.as_f32() / ARENA_WIDTH;
        let scale_y = height.as_f32() / ARENA_HEIGHT;

        for (&player_id, player) in &self.players {
            let image = if player_id == self.player_id {
                &self.local_image
            } else {
                &self.remote_image
            };
            screen.draw(
                ctx,
                image,
                DrawOption::default()
                    .with_position([
                        Pt::from((player.x - PLAYER_SIZE / 2.0) * scale_x),
                        Pt::from((player.y - PLAYER_SIZE / 2.0) * scale_y),
                    ])
                    .with_scale([scale_x, scale_y]),
            );
        }

        let hud = Text::new(
            format!(
                "Player {} | WASD / arrows | {}",
                self.player_id, self.status
            ),
            self.font_id,
        )
        .with_font_size(Pt::from(20.0))
        .with_color([0.92, 0.95, 1.0, 1.0]);
        screen.draw(
            ctx,
            &hud,
            DrawOption::default().with_position([Pt::from(18.0), Pt::from(18.0)]),
        );
    }
}

impl Game {
    fn apply_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot_generation = self.snapshot_generation.wrapping_add(1);
        let generation = self.snapshot_generation;
        self.status = format!("online - {} player(s)", snapshot.players.len());

        for state in snapshot.players {
            let player = self
                .players
                .entry(state.id)
                .or_insert_with(|| SmoothedPlayer {
                    x: state.x,
                    y: state.y,
                    target_x: state.x,
                    target_y: state.y,
                    seen_generation: generation,
                });
            player.target_x = state.x;
            player.target_y = state.y;
            player.seen_generation = generation;
        }

        self.players
            .retain(|_, player| player.seen_generation == generation);
    }

    fn smooth_players(&mut self, dt: f32) {
        let prediction_speed = MOVE_STEP / 0.05;
        let half = PLAYER_SIZE / 2.0;

        for (&player_id, player) in &mut self.players {
            if player_id == self.player_id {
                let predicted_x = self.last_input.0 as f32 * prediction_speed * dt;
                let predicted_y = self.last_input.1 as f32 * prediction_speed * dt;
                player.x = (player.x + predicted_x).clamp(half, ARENA_WIDTH - half);
                player.y = (player.y + predicted_y).clamp(half, ARENA_HEIGHT - half);
                player.target_x = (player.target_x + predicted_x).clamp(half, ARENA_WIDTH - half);
                player.target_y = (player.target_y + predicted_y).clamp(half, ARENA_HEIGHT - half);

                let correction = 1.0 - (-8.0 * dt).exp();
                player.x += (player.target_x - player.x) * correction;
                player.y += (player.target_y - player.y) * correction;
            } else {
                let interpolation = 1.0 - (-20.0 * dt).exp();
                player.x += (player.target_x - player.x) * interpolation;
                player.y += (player.target_y - player.y) * interpolation;
            }
        }
    }
}

fn solid_image(ctx: &mut Context, rgba: [u8; 4]) -> Image {
    let pixels = rgba.repeat((PLAYER_SIZE * PLAYER_SIZE) as usize);
    Image::new(ctx, Pt::from(PLAYER_SIZE), Pt::from(PLAYER_SIZE), &pixels)
        .expect("create player image")
}

fn load_system_font() -> Vec<u8> {
    system_font_paths()
        .iter()
        .find_map(|path| std::fs::read(path).ok())
        .expect("the demo needs a common system font")
}

#[cfg(target_os = "windows")]
fn system_font_paths() -> &'static [&'static str] {
    &[
        "C:\\Windows\\Fonts\\arial.ttf",
        "C:\\Windows\\Fonts\\segoeui.ttf",
    ]
}

#[cfg(target_os = "macos")]
fn system_font_paths() -> &'static [&'static str] {
    &[
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttf",
    ]
}

#[cfg(target_os = "linux")]
fn system_font_paths() -> &'static [&'static str] {
    &[
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
fn system_font_paths() -> &'static [&'static str] {
    &[]
}

fn spawn_network(
    player_id: i64,
    address: String,
    input_rx: Receiver<(i32, i32)>,
    event_tx: Sender<NetworkEvent>,
) {
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create network runtime");
        runtime.block_on(network_loop(player_id, address, input_rx, event_tx));
    });
}

async fn network_loop(
    player_id: i64,
    address: String,
    input_rx: Receiver<(i32, i32)>,
    event_tx: Sender<NetworkEvent>,
) {
    let mut input = (0, 0);
    loop {
        if event_tx
            .send(NetworkEvent::Status(format!("connecting to {address}")))
            .is_err()
        {
            return;
        }

        match connect_and_authenticate(player_id, &address).await {
            Ok(connection) => {
                let _ = event_tx.send(NetworkEvent::Status("online".into()));
                let mut client_events = connection.subscribe();
                let mut tick = tokio::time::interval(Duration::from_millis(50));
                loop {
                    tokio::select! {
                        event = client_events.recv() => {
                            match event {
                                Ok(ClientEvent::Reconnected) => {
                                    let _ = event_tx.send(NetworkEvent::Status(
                                        "reconnected - resuming gameplay".into(),
                                    ));
                                }
                                Ok(ClientEvent::ReauthenticationRequired)
                                | Ok(ClientEvent::ReconnectExhausted)
                                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                Ok(ClientEvent::Reconnecting { attempt, .. }) => {
                                    let _ = event_tx.send(NetworkEvent::Status(format!(
                                        "reconnecting (attempt {attempt})",
                                    )));
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    let _ = event_tx.send(NetworkEvent::Status(format!(
                                        "network event listener skipped {skipped} events",
                                    )));
                                }
                                Ok(_) => {}
                            }
                            continue;
                        }
                        _ = tick.tick() => {}
                    }
                    loop {
                        match input_rx.try_recv() {
                            Ok(next) => input = next,
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => return,
                        }
                    }

                    match send_move(&connection, input).await {
                        Ok(snapshot) => {
                            if event_tx.send(NetworkEvent::Snapshot(snapshot)).is_err() {
                                return;
                            }
                        }
                        Err(ClientError::RequestInterrupted)
                        | Err(ClientError::NotConnected(ConnectionState::Reconnecting)) => {
                            let _ = event_tx.send(NetworkEvent::Status(
                                "connection interrupted - waiting to reconnect".into(),
                            ));
                        }
                        Err(error) => {
                            let _ = event_tx
                                .send(NetworkEvent::Status(format!("reconnecting: {error}")));
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                let _ = event_tx.send(NetworkEvent::Status(format!("retrying: {error}")));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn connect_and_authenticate(player_id: i64, address: &str) -> io::Result<EluraClient> {
    let tickets = TicketService::new(
        DEMO_TICKET_KEY,
        "game-login",
        "game-gateway",
        Duration::from_secs(60),
        Duration::from_secs(30 * 60),
    )
    .map_err(io::Error::other)?;
    let ticket = tickets
        .issue_login(Identity {
            account_id: player_id,
            user_id: player_id,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .map_err(io::Error::other)?;
    EluraClient::connect(address, ticket)
        .await
        .map_err(io::Error::other)
}

async fn send_move(connection: &EluraClient, input: (i32, i32)) -> ClientResult<Snapshot> {
    let request = MoveRequest {
        dx: input.0,
        dy: input.1,
    };
    connection.request_protobuf(ROUTE_MOVE, &request).await
}

fn main() {
    let player_id = std::env::args().nth(1).unwrap_or_else(|| "1".into());
    spottedcat::run::<Game>(WindowConfig {
        title: format!("Elura + Spottedcat | Player {player_id}"),
        width: Pt::from(800.0),
        height: Pt::from(600.0),
        ..Default::default()
    });
}
