//! Shared wire protocol and authoritative arena state for the tiny network game.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use elura::gameplay::lag_compensation::{LagCompensationConfig, LagCompensationHistory};
use elura::gameplay::netcode::{
    InputAck, InputFrame, InputPacket, InputReceiver, InputReceiverConfig, PredictionKey,
};
use elura::gameplay::replication::{
    ReplicationAck, ReplicationBatch, ReplicationConfig, ReplicationError, ReplicationEvent,
    ReplicationPacket, ReplicationSender, VersionedState,
};
use elura::prelude::Route;
use prost::Message;

pub const ARENA_WIDTH: f32 = 800.0;
pub const ARENA_HEIGHT: f32 = 600.0;
pub const PLAYER_SIZE: f32 = 28.0;
pub const MOVE_STEP: f32 = 8.0;
pub const TICK_RATE: u32 = 20;
pub const TICK_DURATION: Duration = Duration::from_millis(1_000 / TICK_RATE as u64);
pub const ROUTE_MOVE: u32 = 100;
pub const ROUTE_REALTIME: u32 = 101;
pub const SERVER_ADDRESS: &str = "127.0.0.1:17000";
pub const DEMO_TICKET_KEY: &str = "elura-spot-tiny-game-demo-key-2026";

/// Legacy request/response movement route retained for the SDK stress tests.
pub struct Move;

impl Route for Move {
    const ID: u32 = ROUTE_MOVE;
    const NAME: &'static str = "arena.move";

    type Request = MoveRequest;
    type Response = Snapshot;
}

/// Fixed-Tick input and replication exchange used by the graphical client.
pub struct Realtime;

impl Route for Realtime {
    const ID: u32 = ROUTE_REALTIME;
    const NAME: &'static str = "arena.realtime";

    type Request = RealtimeRequest;
    type Response = RealtimeResponse;
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct MoveRequest {
    #[prost(sint32, tag = "1")]
    pub dx: i32,
    #[prost(sint32, tag = "2")]
    pub dy: i32,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct MoveInput {
    #[prost(sint32, tag = "1")]
    pub dx: i32,
    #[prost(sint32, tag = "2")]
    pub dy: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct PlayerState {
    #[prost(int64, tag = "1")]
    pub id: i64,
    #[prost(float, tag = "2")]
    pub x: f32,
    #[prost(float, tag = "3")]
    pub y: f32,
}

#[derive(Clone, PartialEq, Message)]
pub struct PlayerDelta {
    #[prost(float, tag = "1")]
    pub x: f32,
    #[prost(float, tag = "2")]
    pub y: f32,
}

#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {
    #[prost(message, repeated, tag = "1")]
    pub players: Vec<PlayerState>,
}

#[derive(Clone, PartialEq, Message)]
pub struct InputFrameMessage {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(uint64, tag = "2")]
    pub target_tick: u64,
    #[prost(message, optional, tag = "3")]
    pub input: Option<MoveInput>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RealtimeRequest {
    #[prost(uint64, tag = "1")]
    pub input_epoch: u64,
    #[prost(uint64, tag = "2")]
    pub replication_epoch: u64,
    #[prost(uint64, tag = "3")]
    pub client_tick: u64,
    #[prost(uint64, tag = "4")]
    pub acknowledged_server_tick: u64,
    #[prost(message, repeated, tag = "5")]
    pub inputs: Vec<InputFrameMessage>,
    #[prost(uint64, tag = "6")]
    pub replication_ack_sequence: u64,
    #[prost(uint64, tag = "7")]
    pub replication_ack_tick: u64,
    #[prost(uint64, tag = "8")]
    pub sync_sequence: u64,
    #[prost(uint64, tag = "9")]
    pub client_sent_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReplicationEventMessage {
    #[prost(int64, tag = "1")]
    pub entity: i64,
    #[prost(enumeration = "ReplicationEventKind", tag = "2")]
    pub kind: i32,
    #[prost(uint64, tag = "3")]
    pub base_version: u64,
    #[prost(uint64, tag = "4")]
    pub version: u64,
    #[prost(uint64, optional, tag = "5")]
    pub prediction_key: Option<u64>,
    #[prost(message, optional, tag = "6")]
    pub state: Option<PlayerState>,
    #[prost(message, optional, tag = "7")]
    pub delta: Option<PlayerDelta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ReplicationEventKind {
    Unspecified = 0,
    Spawn = 1,
    Despawn = 2,
    Update = 3,
    Keyframe = 4,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReplicationBatchMessage {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(uint64, tag = "2")]
    pub tick: u64,
    #[prost(message, repeated, tag = "3")]
    pub events: Vec<ReplicationEventMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RealtimeResponse {
    #[prost(uint64, tag = "1")]
    pub server_tick: u64,
    #[prost(uint64, tag = "2")]
    pub input_epoch: u64,
    #[prost(uint64, tag = "3")]
    pub input_ack_sequence: u64,
    #[prost(uint64, tag = "4")]
    pub replication_epoch: u64,
    #[prost(message, repeated, tag = "5")]
    pub replication_batches: Vec<ReplicationBatchMessage>,
    #[prost(uint64, tag = "6")]
    pub sync_sequence: u64,
    #[prost(uint64, tag = "7")]
    pub client_sent_nanos: u64,
    #[prost(uint64, tag = "8")]
    pub server_received_nanos: u64,
    #[prost(uint64, tag = "9")]
    pub server_sent_nanos: u64,
}

impl RealtimeRequest {
    pub fn from_input_packet(
        input_epoch: u64,
        replication_epoch: u64,
        packet: InputPacket<MoveInput>,
        replication_ack: ReplicationAck,
        sync_sequence: u64,
        client_sent_at: Duration,
    ) -> Self {
        Self {
            input_epoch,
            replication_epoch,
            client_tick: packet.client_tick,
            acknowledged_server_tick: packet.acknowledged_server_tick,
            inputs: packet
                .inputs
                .into_iter()
                .map(|frame| InputFrameMessage {
                    sequence: frame.sequence,
                    target_tick: frame.target_tick,
                    input: Some(frame.input),
                })
                .collect(),
            replication_ack_sequence: replication_ack.acknowledged_sequence,
            replication_ack_tick: replication_ack.applied_tick,
            sync_sequence,
            client_sent_nanos: duration_nanos(client_sent_at),
        }
    }

    fn input_packet(&self) -> elura::Result<InputPacket<MoveInput>> {
        let inputs = self
            .inputs
            .iter()
            .map(|frame| {
                Ok(InputFrame {
                    sequence: frame.sequence,
                    target_tick: frame.target_tick,
                    input: frame.input.clone().ok_or_else(|| {
                        elura::Error::InvalidFrame("realtime input is missing".into())
                    })?,
                })
            })
            .collect::<elura::Result<Vec<_>>>()?;
        Ok(InputPacket {
            client_tick: self.client_tick,
            acknowledged_server_tick: self.acknowledged_server_tick,
            inputs,
        })
    }
}

impl RealtimeResponse {
    pub fn input_acknowledgement(&self) -> InputAck {
        InputAck {
            server_tick: self.server_tick,
            acknowledged_sequence: self.input_ack_sequence,
        }
    }

    pub fn replication_packet(
        &self,
    ) -> elura::Result<ReplicationPacket<i64, PlayerState, PlayerDelta>> {
        let batches = self
            .replication_batches
            .iter()
            .map(decode_replication_batch)
            .collect::<elura::Result<Vec<_>>>()?;
        Ok(ReplicationPacket { batches })
    }
}

struct TrackedPlayer {
    state: PlayerState,
    version: u64,
    last_seen: Instant,
    input_epoch: u64,
    input_receiver: InputReceiver,
    pending_inputs: BTreeMap<u64, MoveInput>,
    last_input: MoveInput,
    replication_epoch: u64,
    replication: ReplicationSender<i64, PlayerState, PlayerDelta>,
}

impl TrackedPlayer {
    fn new(player_id: i64, now: Instant) -> elura::Result<Self> {
        let lane = player_id.rem_euclid(8) as f32;
        Ok(Self {
            state: PlayerState {
                id: player_id,
                x: 100.0 + lane * 70.0,
                y: ARENA_HEIGHT / 2.0,
            },
            version: 1,
            last_seen: now,
            input_epoch: 1,
            input_receiver: InputReceiver::new(InputReceiverConfig::default())
                .map_err(netcode_error)?,
            pending_inputs: BTreeMap::new(),
            last_input: MoveInput { dx: 0, dy: 0 },
            replication_epoch: 1,
            replication: ReplicationSender::new(ReplicationConfig::default())
                .map_err(replication_error)?,
        })
    }

    fn reset_input_stream(&mut self) -> elura::Result<()> {
        self.input_epoch = self.input_epoch.saturating_add(1).max(1);
        self.input_receiver =
            InputReceiver::new(InputReceiverConfig::default()).map_err(netcode_error)?;
        self.pending_inputs.clear();
        self.last_input = MoveInput { dx: 0, dy: 0 };
        Ok(())
    }

    fn reset_replication_stream(&mut self) {
        self.replication_epoch = self.replication_epoch.saturating_add(1).max(1);
        self.replication.reset();
    }
}

pub struct Arena {
    players: HashMap<i64, TrackedPlayer>,
    tick: u64,
    started_at: Instant,
    history: LagCompensationHistory<Snapshot>,
}

impl Default for Arena {
    fn default() -> Self {
        Self {
            players: HashMap::new(),
            tick: 0,
            started_at: Instant::now(),
            history: LagCompensationHistory::new(LagCompensationConfig::default())
                .expect("valid demo history config"),
        }
    }
}

impl Arena {
    pub fn tick(&self) -> u64 {
        self.tick
    }

    pub fn contains_player(&self, player_id: i64) -> bool {
        self.players.contains_key(&player_id)
    }

    /// Compatibility path used by the high-concurrency SDK transport tests.
    pub fn apply_move(&mut self, player_id: i64, request: MoveRequest) -> Snapshot {
        let now = Instant::now();
        self.remove_stale_players(now);
        let player = self
            .players
            .entry(player_id)
            .or_insert_with(|| TrackedPlayer::new(player_id, now).expect("valid demo config"));
        apply_input(
            &mut player.state,
            &MoveInput {
                dx: request.dx,
                dy: request.dy,
            },
        );
        player.version = player.version.saturating_add(1);
        player.last_seen = now;
        self.snapshot()
    }

    /// Applies one authoritative fixed simulation Tick.
    pub fn advance_tick(&mut self) -> elura::Result<()> {
        self.tick = self.tick.saturating_add(1);
        let tick = self.tick;
        let now = Instant::now();
        self.remove_stale_players(now);

        for player in self.players.values_mut() {
            if let Some(input) = player.pending_inputs.remove(&tick) {
                player.last_input = input;
            }
            player.pending_inputs.retain(|target, _| *target > tick);
            let previous = (player.state.x, player.state.y);
            apply_input(&mut player.state, &player.last_input);
            if previous != (player.state.x, player.state.y) {
                player.version = player.version.saturating_add(1);
            }
        }

        self.history
            .record(tick, self.snapshot())
            .map_err(lag_compensation_error)?;

        let visible = self
            .players
            .iter()
            .map(|(&id, player)| {
                (
                    id,
                    VersionedState {
                        version: player.version,
                        prediction_key: None,
                        state: player.state.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();

        for player in self.players.values_mut() {
            let update =
                player
                    .replication
                    .update(tick, visible.iter().cloned(), |_, _, current| {
                        Some(PlayerDelta {
                            x: current.state.x,
                            y: current.state.y,
                        })
                    });
            if matches!(update, Err(ReplicationError::HistoryFull)) {
                player.reset_replication_stream();
                player
                    .replication
                    .update(tick, visible.iter().cloned(), |_, _, _| None)
                    .map_err(replication_error)?;
            } else {
                update.map_err(replication_error)?;
            }
        }
        Ok(())
    }

    /// Validates one client packet and returns the current reliable replication window.
    pub fn exchange(
        &mut self,
        player_id: i64,
        request: RealtimeRequest,
    ) -> elura::Result<RealtimeResponse> {
        let received_at = self.started_at.elapsed();
        let now = Instant::now();
        let existed = self.players.contains_key(&player_id);
        if !existed {
            self.players
                .insert(player_id, TrackedPlayer::new(player_id, now)?);
        }
        let player = self.players.get_mut(&player_id).expect("player inserted");
        player.last_seen = now;

        if existed && request.input_epoch == 0 {
            player.reset_input_stream()?;
        }
        if existed && request.replication_epoch == 0 {
            player.reset_replication_stream();
        }

        let input_ack = if request.input_epoch == 0 || request.input_epoch == player.input_epoch {
            let report = player
                .input_receiver
                .receive(self.tick, request.input_packet()?)
                .map_err(netcode_error)?;
            for frame in report.accepted {
                player.pending_inputs.insert(frame.target_tick, frame.input);
            }
            report.acknowledgement
        } else {
            InputAck {
                server_tick: self.tick,
                acknowledged_sequence: player.input_receiver.acknowledged_sequence(),
            }
        };

        if request.replication_epoch == player.replication_epoch
            && request.replication_ack_sequence > 0
        {
            player
                .replication
                .acknowledge(ReplicationAck {
                    acknowledged_sequence: request.replication_ack_sequence,
                    applied_tick: request.replication_ack_tick,
                })
                .map_err(replication_error)?;
        }

        let packet = player.replication.packet();
        let sent_at = self.started_at.elapsed();
        Ok(RealtimeResponse {
            server_tick: self.tick,
            input_epoch: player.input_epoch,
            input_ack_sequence: input_ack.acknowledged_sequence,
            replication_epoch: player.replication_epoch,
            replication_batches: encode_replication_packet(packet),
            sync_sequence: request.sync_sequence,
            client_sent_nanos: request.client_sent_nanos,
            server_received_nanos: duration_nanos(received_at),
            server_sent_nanos: duration_nanos(sent_at),
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut players = self
            .players
            .values()
            .map(|player| player.state.clone())
            .collect::<Vec<_>>();
        players.sort_by_key(|player| player.id);
        Snapshot { players }
    }

    /// Returns one player's immutable state at an exact retained authoritative Tick.
    ///
    /// A game can run its own hit or visibility query in the same callback rather than cloning
    /// the state. The tiny example exposes this helper only to make the history behavior testable.
    pub fn historical_player(
        &mut self,
        target_tick: u64,
        player_id: i64,
    ) -> elura::Result<Option<PlayerState>> {
        self.history
            .with_rewind(target_tick, |_, snapshot| {
                snapshot
                    .players
                    .iter()
                    .find(|player| player.id == player_id)
                    .cloned()
            })
            .map_err(lag_compensation_error)
    }

    fn remove_stale_players(&mut self, now: Instant) {
        self.players
            .retain(|_, player| now.duration_since(player.last_seen) < Duration::from_secs(10));
    }
}

pub fn apply_input(state: &mut PlayerState, input: &MoveInput) {
    let half = PLAYER_SIZE / 2.0;
    state.x = (state.x + input.dx.clamp(-1, 1) as f32 * MOVE_STEP).clamp(half, ARENA_WIDTH - half);
    state.y = (state.y + input.dy.clamp(-1, 1) as f32 * MOVE_STEP).clamp(half, ARENA_HEIGHT - half);
}

pub fn apply_delta(
    _entity: &i64,
    previous: &PlayerState,
    delta: &PlayerDelta,
) -> Option<PlayerState> {
    Some(PlayerState {
        id: previous.id,
        x: delta.x,
        y: delta.y,
    })
}

fn encode_replication_packet(
    packet: ReplicationPacket<i64, PlayerState, PlayerDelta>,
) -> Vec<ReplicationBatchMessage> {
    packet
        .batches
        .into_iter()
        .map(|batch| ReplicationBatchMessage {
            sequence: batch.sequence,
            tick: batch.tick,
            events: batch
                .events
                .into_iter()
                .map(encode_replication_event)
                .collect(),
        })
        .collect()
}

fn encode_replication_event(
    event: ReplicationEvent<i64, PlayerState, PlayerDelta>,
) -> ReplicationEventMessage {
    match event {
        ReplicationEvent::Spawn {
            entity,
            version,
            prediction_key,
            state,
        } => ReplicationEventMessage {
            entity,
            kind: ReplicationEventKind::Spawn as i32,
            base_version: 0,
            version,
            prediction_key: prediction_key.map(|key| key.0),
            state: Some(state),
            delta: None,
        },
        ReplicationEvent::Despawn { entity } => ReplicationEventMessage {
            entity,
            kind: ReplicationEventKind::Despawn as i32,
            base_version: 0,
            version: 0,
            prediction_key: None,
            state: None,
            delta: None,
        },
        ReplicationEvent::Update {
            entity,
            base_version,
            version,
            delta,
        } => ReplicationEventMessage {
            entity,
            kind: ReplicationEventKind::Update as i32,
            base_version,
            version,
            prediction_key: None,
            state: None,
            delta: Some(delta),
        },
        ReplicationEvent::Keyframe {
            entity,
            version,
            prediction_key,
            state,
        } => ReplicationEventMessage {
            entity,
            kind: ReplicationEventKind::Keyframe as i32,
            base_version: 0,
            version,
            prediction_key: prediction_key.map(|key| key.0),
            state: Some(state),
            delta: None,
        },
    }
}

fn decode_replication_batch(
    batch: &ReplicationBatchMessage,
) -> elura::Result<ReplicationBatch<i64, PlayerState, PlayerDelta>> {
    Ok(ReplicationBatch {
        sequence: batch.sequence,
        tick: batch.tick,
        events: batch
            .events
            .iter()
            .map(decode_replication_event)
            .collect::<elura::Result<Vec<_>>>()?,
    })
}

fn decode_replication_event(
    event: &ReplicationEventMessage,
) -> elura::Result<ReplicationEvent<i64, PlayerState, PlayerDelta>> {
    let kind = ReplicationEventKind::try_from(event.kind)
        .map_err(|_| elura::Error::InvalidFrame("invalid replication event kind".into()))?;
    match kind {
        ReplicationEventKind::Spawn => Ok(ReplicationEvent::Spawn {
            entity: event.entity,
            version: event.version,
            prediction_key: event.prediction_key.map(PredictionKey),
            state: event
                .state
                .clone()
                .ok_or_else(|| elura::Error::InvalidFrame("spawn state is missing".into()))?,
        }),
        ReplicationEventKind::Despawn => Ok(ReplicationEvent::Despawn {
            entity: event.entity,
        }),
        ReplicationEventKind::Update => Ok(ReplicationEvent::Update {
            entity: event.entity,
            base_version: event.base_version,
            version: event.version,
            delta: event
                .delta
                .clone()
                .ok_or_else(|| elura::Error::InvalidFrame("update delta is missing".into()))?,
        }),
        ReplicationEventKind::Keyframe => Ok(ReplicationEvent::Keyframe {
            entity: event.entity,
            version: event.version,
            prediction_key: event.prediction_key.map(PredictionKey),
            state: event
                .state
                .clone()
                .ok_or_else(|| elura::Error::InvalidFrame("keyframe state is missing".into()))?,
        }),
        ReplicationEventKind::Unspecified => Err(elura::Error::InvalidFrame(
            "replication event kind is unspecified".into(),
        )),
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn netcode_error(error: impl std::fmt::Display) -> elura::Error {
    elura::Error::InvalidFrame(error.to_string())
}

fn replication_error(error: impl std::fmt::Display) -> elura::Error {
    elura::Error::InvalidFrame(error.to_string())
}

fn lag_compensation_error(error: impl std::fmt::Display) -> elura::Error {
    elura::Error::InvalidFrame(error.to_string())
}

#[cfg(test)]
mod tests {
    use elura::gameplay::net_sim::{NetSimConfig, SendOutcome, SimulatedLink};
    use elura::gameplay::netcode::{
        InputReceiver, InputReceiverConfig, InputSender, InputSenderConfig, PredictionBuffer,
        PredictionConfig,
    };
    use elura::gameplay::replication::{ReplicationConfig, ReplicationReceiver};

    use super::*;

    #[test]
    fn movement_is_authoritative_bounded_and_sorted() {
        let mut arena = Arena::default();
        arena.apply_move(2, MoveRequest { dx: 99, dy: 0 });
        let snapshot = arena.apply_move(1, MoveRequest { dx: -99, dy: -99 });

        assert_eq!(
            snapshot
                .players
                .iter()
                .map(|player| player.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(snapshot.players.iter().all(|player| {
            player.x >= PLAYER_SIZE / 2.0
                && player.x <= ARENA_WIDTH - PLAYER_SIZE / 2.0
                && player.y >= PLAYER_SIZE / 2.0
                && player.y <= ARENA_HEIGHT - PLAYER_SIZE / 2.0
        }));
    }

    #[test]
    fn realtime_exchange_recovers_redundant_input_and_replication() {
        let mut arena = Arena::default();
        let mut inputs =
            InputSender::new(InputSenderConfig::default()).expect("valid input config");
        let mut replication = ReplicationReceiver::new(ReplicationConfig::default())
            .expect("valid replication config");

        let bootstrap = RealtimeRequest::from_input_packet(
            0,
            0,
            inputs.packet(0),
            ReplicationAck {
                acknowledged_sequence: 0,
                applied_tick: 0,
            },
            1,
            Duration::ZERO,
        );
        let bootstrap = arena.exchange(1, bootstrap).expect("bootstrap exchange");
        assert_eq!(bootstrap.input_epoch, 1);
        assert_eq!(bootstrap.replication_epoch, 1);

        arena.advance_tick().expect("first Tick");
        inputs
            .record(2, MoveInput { dx: 1, dy: 0 })
            .expect("first input");
        let first_packet = inputs.packet(1);
        inputs
            .record(3, MoveInput { dx: 1, dy: 0 })
            .expect("second input");
        let redundant_packet = inputs.packet(2);
        assert_eq!(first_packet.inputs.len(), 1);
        assert_eq!(redundant_packet.inputs.len(), 2);

        let request = RealtimeRequest::from_input_packet(
            bootstrap.input_epoch,
            bootstrap.replication_epoch,
            redundant_packet,
            ReplicationAck {
                acknowledged_sequence: 0,
                applied_tick: 0,
            },
            2,
            Duration::from_millis(50),
        );
        let response = arena.exchange(1, request).expect("input exchange");
        assert_eq!(response.input_ack_sequence, 2);
        inputs
            .acknowledge(response.input_acknowledgement())
            .expect("valid input ACK");
        assert!(inputs.is_empty());

        arena.advance_tick().expect("second Tick");
        arena.advance_tick().expect("third Tick");
        let response = arena
            .exchange(
                1,
                RealtimeRequest::from_input_packet(
                    bootstrap.input_epoch,
                    bootstrap.replication_epoch,
                    inputs.packet(3),
                    ReplicationAck {
                        acknowledged_sequence: 0,
                        applied_tick: 0,
                    },
                    3,
                    Duration::from_millis(100),
                ),
            )
            .expect("replication exchange");
        let report = replication
            .receive(
                response.replication_packet().expect("wire packet"),
                apply_delta,
            )
            .expect("replication applies");
        assert!(report.applied_batches > 0);
        let player = &replication.entity(&1).expect("replicated player").state;
        assert_eq!(player.x, 100.0 + 70.0 + MOVE_STEP * 2.0);
    }

    #[test]
    fn prediction_replays_only_unconfirmed_inputs() {
        let initial = PlayerState {
            id: 1,
            x: 100.0,
            y: 100.0,
        };
        let input = MoveInput { dx: 1, dy: 0 };
        let mut predicted = initial.clone();
        let mut prediction =
            PredictionBuffer::new(PredictionConfig::default()).expect("valid prediction config");
        apply_input(&mut predicted, &input);
        prediction
            .record(2, input.clone(), predicted.clone())
            .expect("first prediction");
        apply_input(&mut predicted, &input);
        prediction
            .record(3, input, predicted)
            .expect("second prediction");

        let report = prediction
            .reconcile(2, initial, |state, _, input| apply_input(state, input))
            .expect("prediction reconciles");
        assert_eq!(report.replayed_inputs, 1);
        assert_eq!(report.corrected_state.x, 100.0 + MOVE_STEP);
    }

    #[test]
    fn redundant_input_recovers_after_simulated_packet_loss() {
        let mut sender =
            InputSender::new(InputSenderConfig::default()).expect("valid sender config");
        let mut receiver =
            InputReceiver::new(InputReceiverConfig::default()).expect("valid receiver config");
        sender
            .record(10, MoveInput { dx: 1, dy: 0 })
            .expect("first input");

        let mut loss = NetSimConfig::default();
        loss.loss_rate = 1.0;
        let mut dropped = SimulatedLink::new(loss).expect("valid loss config");
        assert_eq!(
            dropped
                .send(Duration::ZERO, 32, sender.packet(1))
                .expect("loss simulation"),
            SendOutcome::DroppedByLoss
        );

        sender
            .record(11, MoveInput { dx: 1, dy: 0 })
            .expect("second input");
        let mut delayed_config = NetSimConfig::default();
        delayed_config.latency = Duration::from_millis(100);
        let mut delayed = SimulatedLink::new(delayed_config).expect("valid delay config");
        delayed
            .send(Duration::ZERO, 64, sender.packet(2))
            .expect("delayed send");
        assert!(
            delayed
                .receive(Duration::from_millis(99))
                .expect("early receive")
                .is_empty()
        );
        let packet = delayed
            .receive(Duration::from_millis(100))
            .expect("on-time receive")
            .pop()
            .expect("redundant packet delivered")
            .payload;
        let report = receiver.receive(9, packet).expect("redundant inputs apply");
        assert_eq!(report.accepted.len(), 2);
        assert_eq!(report.acknowledgement.acknowledged_sequence, 2);
    }

    #[test]
    fn historical_query_reads_an_earlier_tick_without_mutating_live_state() {
        let mut arena = Arena::default();
        arena.apply_move(1, MoveRequest { dx: 0, dy: 0 });
        arena.advance_tick().expect("first Tick");
        let historical = arena
            .historical_player(1, 1)
            .expect("historical query")
            .expect("historical player");

        arena.apply_move(1, MoveRequest { dx: 1, dy: 0 });
        arena.advance_tick().expect("second Tick");
        let live = arena.snapshot().players.remove(0);

        assert!(live.x > historical.x);
        assert_eq!(
            arena
                .historical_player(1, 1)
                .expect("repeat historical query")
                .expect("historical player"),
            historical
        );
    }
}
