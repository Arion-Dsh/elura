//! Shared protocol and authoritative arena state for the tiny network game.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use elura::prelude::Route;
use prost::Message;

pub const ARENA_WIDTH: f32 = 800.0;
pub const ARENA_HEIGHT: f32 = 600.0;
pub const PLAYER_SIZE: f32 = 28.0;
pub const MOVE_STEP: f32 = 8.0;
pub const ROUTE_MOVE: u32 = 100;
pub const SERVER_ADDRESS: &str = "127.0.0.1:17000";
pub const DEMO_TICKET_KEY: &str = "elura-spot-tiny-game-demo-key-2026";

pub struct Move;

impl Route for Move {
    const ID: u32 = ROUTE_MOVE;
    const NAME: &'static str = "arena.move";

    type Request = MoveRequest;
    type Response = Snapshot;
}

#[derive(Clone, PartialEq, Message)]
pub struct MoveRequest {
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
pub struct Snapshot {
    #[prost(message, repeated, tag = "1")]
    pub players: Vec<PlayerState>,
}

struct TrackedPlayer {
    state: PlayerState,
    last_seen: Instant,
}

#[derive(Default)]
pub struct Arena {
    players: HashMap<i64, TrackedPlayer>,
}

impl Arena {
    pub fn contains_player(&self, player_id: i64) -> bool {
        self.players.contains_key(&player_id)
    }

    pub fn apply_move(&mut self, player_id: i64, request: MoveRequest) -> Snapshot {
        let now = Instant::now();
        self.players
            .retain(|_, player| now.duration_since(player.last_seen) < Duration::from_secs(10));

        let player = self.players.entry(player_id).or_insert_with(|| {
            let lane = player_id.rem_euclid(8) as f32;
            TrackedPlayer {
                state: PlayerState {
                    id: player_id,
                    x: 100.0 + lane * 70.0,
                    y: ARENA_HEIGHT / 2.0,
                },
                last_seen: now,
            }
        });

        let half = PLAYER_SIZE / 2.0;
        player.state.x = (player.state.x + request.dx.clamp(-1, 1) as f32 * MOVE_STEP)
            .clamp(half, ARENA_WIDTH - half);
        player.state.y = (player.state.y + request.dy.clamp(-1, 1) as f32 * MOVE_STEP)
            .clamp(half, ARENA_HEIGHT - half);
        player.last_seen = now;

        let mut players = self
            .players
            .values()
            .map(|player| player.state.clone())
            .collect::<Vec<_>>();
        players.sort_by_key(|player| player.id);
        Snapshot { players }
    }
}

#[cfg(test)]
mod tests {
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
}
