use std::io::Read;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::replay::{ReplayReader, ReplayRecord};
use crate::state_hash::StateHash;
use crate::{Error, Result};

use super::{Command, Room, Simulation};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayStats {
    pub commands_submitted: u64,
    pub ticks_advanced: u64,
    pub checkpoints_verified: u64,
}

#[async_trait]
pub trait ReplayDriver<C>: Send + Sync {
    fn room_id(&self) -> &str;
    fn current_tick(&self) -> u64;
    fn tick_rate(&self) -> u32;
    fn input_delay_ticks(&self) -> u64;
    fn rollback_window_ticks(&self) -> u64;
    async fn submit_at(&self, tick: u64, command: Command<C>) -> Result<()>;
    async fn advance(&self) -> Result<()>;
    async fn canonical_state(&self) -> Result<Vec<u8>>;
}

#[async_trait]
impl<S> ReplayDriver<S::Command> for Room<S>
where
    S: Simulation + Sync,
    S::Command: Sync,
{
    fn room_id(&self) -> &str {
        self.id()
    }
    fn current_tick(&self) -> u64 {
        self.current_tick()
    }
    fn tick_rate(&self) -> u32 {
        self.tick_rate()
    }
    fn input_delay_ticks(&self) -> u64 {
        self.input_delay_ticks()
    }
    fn rollback_window_ticks(&self) -> u64 {
        self.rollback_window_ticks()
    }
    async fn submit_at(&self, tick: u64, command: Command<S::Command>) -> Result<()> {
        self.submit_at(tick, command).await
    }
    async fn advance(&self) -> Result<()> {
        self.advance().await
    }
    async fn canonical_state(&self) -> Result<Vec<u8>> {
        self.canonical_state().await
    }
}

pub async fn play_replay<R, C, D>(reader: &mut ReplayReader<R>, driver: &D) -> Result<ReplayStats>
where
    R: Read,
    C: DeserializeOwned + Send + Clone + 'static,
    D: ReplayDriver<C>,
{
    let header = reader.header();
    if driver.room_id() != header.room_id
        || driver.current_tick() != 0
        || driver.tick_rate() != header.tick_rate
        || driver.input_delay_ticks() != header.input_delay_ticks
        || driver.rollback_window_ticks() != header.rollback_window_ticks
    {
        return Err(Error::InvalidConfig(
            "replay driver does not match header".into(),
        ));
    }
    let mut stats = ReplayStats::default();
    while let Some(record) = reader.read_next::<C>()? {
        match record {
            ReplayRecord::Command {
                tick,
                player,
                sequence,
                command,
            } => {
                driver
                    .submit_at(
                        tick,
                        Command {
                            player,
                            sequence,
                            value: command,
                        },
                    )
                    .await?;
                stats.commands_submitted += 1;
            }
            ReplayRecord::Tick { tick } => {
                if tick != driver.current_tick() + 1 {
                    return Err(Error::InvalidFrame("replay Tick is out of order".into()));
                }
                driver.advance().await?;
                if driver.current_tick() != tick {
                    return Err(Error::InvalidFrame("replay driver Tick mismatch".into()));
                }
                stats.ticks_advanced += 1;
            }
            ReplayRecord::Checkpoint {
                tick,
                state: _,
                state_hash,
            } => {
                if driver.current_tick() != tick {
                    return Err(Error::InvalidFrame(
                        "replay checkpoint Tick mismatch".into(),
                    ));
                }
                let canonical = driver.canonical_state().await?;
                if !StateHash::from_bytes(state_hash).matches(&canonical) {
                    return Err(Error::InvalidFrame(
                        "replay checkpoint state mismatch".into(),
                    ));
                }
                stats.checkpoints_verified += 1;
            }
        }
    }
    Ok(stats)
}
