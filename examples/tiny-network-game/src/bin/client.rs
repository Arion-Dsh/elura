use std::collections::HashMap;
use std::io;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use elura::gameplay::netcode::{
    InputSender, InputSenderConfig, InterpolationBuffer, InterpolationConfig, PredictionBuffer,
    PredictionConfig, TickSyncConfig, TickSyncRequest, TickSyncResponse, TickSynchronizer,
};
use elura::gameplay::replication::{ReplicationAck, ReplicationConfig, ReplicationReceiver};
use elura::prelude::{Identity, TicketService};
use elura_client::{ClientError, ClientEvent, ConnectionState, EluraClient};
use elura_spot_demo::{
    ARENA_HEIGHT, ARENA_WIDTH, DEMO_TICKET_KEY, MOVE_STEP, MoveInput, PLAYER_SIZE, PlayerState,
    ROUTE_REALTIME, RealtimeRequest, RealtimeResponse, SERVER_ADDRESS, TICK_DURATION, TICK_RATE,
    apply_delta, apply_input,
};
use spottedcat::{Context, DrawOption, Image, Key, Pt, Spot, Text, WindowConfig};

enum NetworkEvent {
    Status(String),
    Frame(NetworkFrame),
}

struct NetworkFrame {
    estimated_server_tick: f64,
    server_tick: u64,
    round_trip_time: Duration,
    local: Option<PlayerState>,
    players: Vec<PlayerState>,
}

struct Game {
    player_id: i64,
    input_tx: Sender<MoveInput>,
    event_rx: Receiver<NetworkEvent>,
    last_input: MoveInput,
    local: Option<SmoothedPlayer>,
    remote: HashMap<i64, RemotePlayer>,
    remote_generation: u64,
    render_clock: Instant,
    estimated_server_tick: f64,
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
}

struct RemotePlayer {
    x: f32,
    y: f32,
    seen_generation: u64,
    interpolation: InterpolationBuffer<PlayerState>,
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
            last_input: MoveInput { dx: 0, dy: 0 },
            local: None,
            remote: HashMap::new(),
            remote_generation: 0,
            render_clock: Instant::now(),
            estimated_server_tick: 0.0,
            status: "connecting...".into(),
            local_image,
            remote_image,
            font_id,
        }
    }

    fn update(&mut self, ctx: &mut Context, dt: Duration) {
        let dx =
            i32::from(spottedcat::key_down(ctx, Key::D) || spottedcat::key_down(ctx, Key::Right))
                - i32::from(
                    spottedcat::key_down(ctx, Key::A) || spottedcat::key_down(ctx, Key::Left),
                );
        let dy =
            i32::from(spottedcat::key_down(ctx, Key::S) || spottedcat::key_down(ctx, Key::Down))
                - i32::from(
                    spottedcat::key_down(ctx, Key::W) || spottedcat::key_down(ctx, Key::Up),
                );
        if (dx, dy) != (self.last_input.dx, self.last_input.dy) {
            self.last_input = MoveInput { dx, dy };
            let _ = self.input_tx.send(self.last_input.clone());
        }

        let events = self.event_rx.try_iter().collect::<Vec<_>>();
        for event in events {
            match event {
                NetworkEvent::Status(status) => self.status = status,
                NetworkEvent::Frame(frame) => self.apply_frame(frame),
            }
        }

        self.advance_render(dt.as_secs_f32());
    }

    fn draw(&mut self, ctx: &mut Context, screen: Image) {
        let (width, height) = spottedcat::window_size(ctx);
        let scale_x = width.as_f32() / ARENA_WIDTH;
        let scale_y = height.as_f32() / ARENA_HEIGHT;

        if let Some(local) = &self.local {
            draw_player(
                ctx,
                &screen,
                &self.local_image,
                local.x,
                local.y,
                scale_x,
                scale_y,
            );
        }
        for player in self.remote.values() {
            draw_player(
                ctx,
                &screen,
                &self.remote_image,
                player.x,
                player.y,
                scale_x,
                scale_y,
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
    fn apply_frame(&mut self, frame: NetworkFrame) {
        self.estimated_server_tick = frame.estimated_server_tick;
        self.status = format!(
            "online - {} player(s) | tick {} | RTT {:.1} ms",
            frame.players.len(),
            frame.server_tick,
            frame.round_trip_time.as_secs_f64() * 1_000.0
        );

        if let Some(state) = frame.local {
            let local = self.local.get_or_insert(SmoothedPlayer {
                x: state.x,
                y: state.y,
                target_x: state.x,
                target_y: state.y,
            });
            local.target_x = state.x;
            local.target_y = state.y;
        }

        self.remote_generation = self.remote_generation.wrapping_add(1);
        let generation = self.remote_generation;
        let arrived_at = self.render_clock.elapsed();
        for state in frame
            .players
            .into_iter()
            .filter(|state| state.id != self.player_id)
        {
            let initial_x = state.x;
            let initial_y = state.y;
            let player = self.remote.entry(state.id).or_insert_with(|| {
                let mut config = InterpolationConfig::default();
                config.tick_rate = TICK_RATE;
                RemotePlayer {
                    x: initial_x,
                    y: initial_y,
                    seen_generation: generation,
                    interpolation: InterpolationBuffer::new(config)
                        .expect("valid interpolation config"),
                }
            });
            player.seen_generation = generation;
            player
                .interpolation
                .insert(frame.server_tick.max(1), state, arrived_at)
                .expect("network frames arrive monotonically");
        }
        self.remote
            .retain(|_, player| player.seen_generation == generation);
    }

    fn advance_render(&mut self, dt: f32) {
        self.estimated_server_tick += f64::from(dt) * f64::from(TICK_RATE);
        if let Some(local) = &mut self.local {
            let prediction_speed = MOVE_STEP / TICK_DURATION.as_secs_f32();
            local.x = (local.x + self.last_input.dx as f32 * prediction_speed * dt)
                .clamp(PLAYER_SIZE / 2.0, ARENA_WIDTH - PLAYER_SIZE / 2.0);
            local.y = (local.y + self.last_input.dy as f32 * prediction_speed * dt)
                .clamp(PLAYER_SIZE / 2.0, ARENA_HEIGHT - PLAYER_SIZE / 2.0);
            let correction = 1.0 - (-12.0 * dt).exp();
            local.x += (local.target_x - local.x) * correction;
            local.y += (local.target_y - local.y) * correction;
        }

        for player in self.remote.values_mut() {
            if let Ok(sample) = player.interpolation.sample(self.estimated_server_tick) {
                player.x =
                    sample.previous.x + (sample.next.x - sample.previous.x) * sample.alpha as f32;
                player.y =
                    sample.previous.y + (sample.next.y - sample.previous.y) * sample.alpha as f32;
            }
        }
    }
}

struct RealtimeClient {
    player_id: i64,
    started_at: Instant,
    input_epoch: u64,
    replication_epoch: u64,
    input_sender: InputSender<MoveInput>,
    prediction: PredictionBuffer<MoveInput, PlayerState>,
    replication: ReplicationReceiver<i64, PlayerState, elura_spot_demo::PlayerDelta>,
    replication_ack: ReplicationAck,
    tick_sync: TickSynchronizer,
    predicted_state: Option<PlayerState>,
    last_target_tick: u64,
    next_sync_sequence: u64,
}

struct PendingExchange {
    sync: TickSyncRequest,
    input_epoch: u64,
}

impl RealtimeClient {
    fn new(player_id: i64) -> Result<Self, String> {
        let mut tick_config = TickSyncConfig::default();
        tick_config.tick_rate = TICK_RATE;
        Ok(Self {
            player_id,
            started_at: Instant::now(),
            input_epoch: 0,
            replication_epoch: 0,
            input_sender: InputSender::new(InputSenderConfig::default())
                .map_err(|error| error.to_string())?,
            prediction: PredictionBuffer::new(PredictionConfig::default())
                .map_err(|error| error.to_string())?,
            replication: ReplicationReceiver::new(ReplicationConfig::default())
                .map_err(|error| error.to_string())?,
            replication_ack: ReplicationAck {
                acknowledged_sequence: 0,
                applied_tick: 0,
            },
            tick_sync: TickSynchronizer::new(tick_config).map_err(|error| error.to_string())?,
            predicted_state: None,
            last_target_tick: 0,
            next_sync_sequence: 1,
        })
    }

    fn build_request(
        &mut self,
        input: MoveInput,
    ) -> Result<(RealtimeRequest, PendingExchange), String> {
        let sent_at = self.started_at.elapsed();
        let local_tick = local_tick(sent_at);
        if self.input_epoch > 0
            && let Some(predicted) = &mut self.predicted_state
        {
            let target_tick = self
                .tick_sync
                .recommended_input_tick(local_tick)
                .max(self.last_target_tick.saturating_add(1))
                .max(1);
            self.input_sender
                .record(target_tick, input.clone())
                .map_err(|error| error.to_string())?;
            apply_input(predicted, &input);
            self.prediction
                .record(target_tick, input, predicted.clone())
                .map_err(|error| error.to_string())?;
            self.last_target_tick = target_tick;
        }

        let sync = TickSyncRequest {
            sequence: self.next_sync_sequence,
            client_sent_at: sent_at,
        };
        self.next_sync_sequence = self.next_sync_sequence.saturating_add(1);
        let request = RealtimeRequest::from_input_packet(
            self.input_epoch,
            self.replication_epoch,
            self.input_sender.packet(local_tick.floor() as u64),
            self.replication_ack,
            sync.sequence,
            sync.client_sent_at,
        );
        Ok((
            request,
            PendingExchange {
                sync,
                input_epoch: self.input_epoch,
            },
        ))
    }

    fn apply_response(
        &mut self,
        pending: PendingExchange,
        response: RealtimeResponse,
    ) -> Result<NetworkFrame, String> {
        let received_at = self.started_at.elapsed();
        let sync_response = TickSyncResponse {
            sequence: response.sync_sequence,
            client_sent_at: Duration::from_nanos(response.client_sent_nanos),
            server_received_at: Duration::from_nanos(response.server_received_nanos),
            server_sent_at: Duration::from_nanos(response.server_sent_nanos),
            server_tick: response.server_tick,
        };
        let sample = sync_response
            .sample(pending.sync, received_at, local_tick(received_at))
            .map_err(|error| error.to_string())?;
        let sync_report = self
            .tick_sync
            .observe(sample)
            .map_err(|error| error.to_string())?;

        if self.input_epoch != response.input_epoch {
            self.input_epoch = response.input_epoch;
            self.input_sender = InputSender::new(InputSenderConfig::default())
                .map_err(|error| error.to_string())?;
            self.prediction = PredictionBuffer::new(PredictionConfig::default())
                .map_err(|error| error.to_string())?;
            self.predicted_state = None;
            self.last_target_tick = response.server_tick;
        } else if pending.input_epoch == response.input_epoch {
            self.input_sender
                .acknowledge(response.input_acknowledgement())
                .map_err(|error| error.to_string())?;
        }

        if self.replication_epoch != response.replication_epoch {
            self.replication_epoch = response.replication_epoch;
            self.replication
                .reset()
                .map_err(|error| error.to_string())?;
            self.replication_ack = ReplicationAck {
                acknowledged_sequence: 0,
                applied_tick: 0,
            };
        }
        let report = self
            .replication
            .receive(
                response
                    .replication_packet()
                    .map_err(|error| error.to_string())?,
                apply_delta,
            )
            .map_err(|error| error.to_string())?;
        self.replication_ack = report.acknowledgement;

        let authoritative = self
            .replication
            .entity(&self.player_id)
            .map(|state| state.state.clone());
        if let Some(authoritative) = authoritative {
            if self.predicted_state.is_none() {
                self.prediction.reset(response.server_tick);
                self.last_target_tick = self.last_target_tick.max(response.server_tick);
                self.predicted_state = Some(authoritative);
            } else {
                let reconciled = self
                    .prediction
                    .reconcile(response.server_tick, authoritative, |state, _, input| {
                        apply_input(state, input);
                    })
                    .map_err(|error| error.to_string())?;
                self.predicted_state = Some(reconciled.corrected_state);
            }
        }

        let players = self
            .replication
            .entities()
            .map(|(_, state)| state.state.clone())
            .collect();
        Ok(NetworkFrame {
            estimated_server_tick: sync_report.estimated_server_tick,
            server_tick: response.server_tick,
            round_trip_time: sync_report.network_round_trip_time,
            local: self.predicted_state.clone(),
            players,
        })
    }
}

fn draw_player(
    ctx: &mut Context,
    screen: &Image,
    image: &Image,
    x: f32,
    y: f32,
    scale_x: f32,
    scale_y: f32,
) {
    screen.draw(
        ctx,
        image,
        DrawOption::default()
            .with_position([
                Pt::from((x - PLAYER_SIZE / 2.0) * scale_x),
                Pt::from((y - PLAYER_SIZE / 2.0) * scale_y),
            ])
            .with_scale([scale_x, scale_y]),
    );
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
    input_rx: Receiver<MoveInput>,
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
    input_rx: Receiver<MoveInput>,
    event_tx: Sender<NetworkEvent>,
) {
    let mut input = MoveInput { dx: 0, dy: 0 };
    let mut realtime = match RealtimeClient::new(player_id) {
        Ok(realtime) => realtime,
        Err(error) => {
            let _ = event_tx.send(NetworkEvent::Status(format!(
                "netcode initialization failed: {error}"
            )));
            return;
        }
    };

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
                let mut tick = tokio::time::interval(TICK_DURATION);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                    if connection.state() != ConnectionState::Connected {
                        continue;
                    }

                    let (request, pending) = match realtime.build_request(input.clone()) {
                        Ok(request) => request,
                        Err(error) => {
                            let _ = event_tx
                                .send(NetworkEvent::Status(format!("netcode error: {error}")));
                            break;
                        }
                    };
                    match connection
                        .request_protobuf::<_, RealtimeResponse>(ROUTE_REALTIME, &request)
                        .await
                    {
                        Ok(response) => match realtime.apply_response(pending, response) {
                            Ok(frame) => {
                                if event_tx.send(NetworkEvent::Frame(frame)).is_err() {
                                    return;
                                }
                            }
                            Err(error) => {
                                let _ = event_tx
                                    .send(NetworkEvent::Status(format!("netcode error: {error}")));
                                break;
                            }
                        },
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

fn local_tick(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * f64::from(TICK_RATE)
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
