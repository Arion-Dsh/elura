use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::{NetcodeError, NetcodeResult};

/// Bounds for client prediction history and authoritative replay work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct PredictionConfig {
    /// Maximum unconfirmed input frames retained locally.
    pub history_capacity: usize,
    /// Maximum pending inputs replayed by one reconciliation.
    pub max_replay_steps: usize,
}

impl Default for PredictionConfig {
    fn default() -> Self {
        Self {
            history_capacity: 256,
            max_replay_steps: 64,
        }
    }
}

impl PredictionConfig {
    /// Validates prediction memory and replay bounds.
    pub fn validate(&self) -> NetcodeResult<()> {
        if self.history_capacity == 0 {
            return Err(NetcodeError::InvalidConfig(
                "prediction history capacity must be positive",
            ));
        }
        if self.max_replay_steps == 0 || self.max_replay_steps > self.history_capacity {
            return Err(NetcodeError::InvalidConfig(
                "prediction replay limit must be within history capacity",
            ));
        }
        Ok(())
    }
}

/// One locally simulated input and the predicted state after it committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictionFrame<I, S> {
    /// Client simulation Tick assigned to this input.
    pub tick: u64,
    /// Application-owned local input.
    pub input: I,
    /// Predicted application state after this input and Tick.
    pub predicted_state: S,
}

/// Result of restoring an authoritative state and replaying unconfirmed inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationReport<S> {
    /// Server Tick represented by the authoritative state.
    pub authoritative_tick: u64,
    /// Local predicted state previously recorded at the authoritative Tick, when retained.
    pub predicted_state_at_authoritative_tick: Option<S>,
    /// Corrected present state after replaying every pending input.
    pub corrected_state: S,
    /// Number of unconfirmed inputs replayed.
    pub replayed_inputs: usize,
    /// Newest local predicted Tick after reconciliation.
    pub current_tick: u64,
}

/// Bounded client input/state history for server reconciliation.
#[derive(Debug, Clone)]
pub struct PredictionBuffer<I, S> {
    config: PredictionConfig,
    frames: VecDeque<PredictionFrame<I, S>>,
    confirmed_tick: u64,
}

impl<I, S> PredictionBuffer<I, S> {
    /// Creates an empty prediction history.
    pub fn new(config: PredictionConfig) -> NetcodeResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            frames: VecDeque::with_capacity(config.history_capacity),
            confirmed_tick: 0,
        })
    }

    /// Records one successfully simulated local input and resulting state.
    pub fn record(&mut self, tick: u64, input: I, predicted_state: S) -> NetcodeResult<()> {
        let previous = self
            .frames
            .back()
            .map_or(self.confirmed_tick, |frame| frame.tick);
        if tick == 0 || tick <= previous {
            return Err(NetcodeError::InvalidInput(
                "prediction Tick must increase and be positive",
            ));
        }
        if self.frames.len() >= self.config.history_capacity {
            return Err(NetcodeError::PredictionHistoryFull);
        }
        self.frames.push_back(PredictionFrame {
            tick,
            input,
            predicted_state,
        });
        Ok(())
    }

    /// Returns the highest authoritative Tick already reconciled.
    pub fn confirmed_tick(&self) -> u64 {
        self.confirmed_tick
    }

    /// Returns the number of inputs awaiting authoritative confirmation.
    pub fn pending_len(&self) -> usize {
        self.frames.len()
    }

    /// Returns whether no unconfirmed input remains.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Iterates retained prediction frames in Tick order.
    pub fn frames(&self) -> impl Iterator<Item = &PredictionFrame<I, S>> {
        self.frames.iter()
    }
}

impl<I, S> PredictionBuffer<I, S>
where
    S: Clone,
{
    /// Restores an authoritative state and deterministically replays later local inputs.
    ///
    /// `simulate` owns game rules and must advance `state` exactly once for the supplied Tick and
    /// input. A replay-limit or ordering error leaves the buffer unchanged.
    pub fn reconcile<Simulate>(
        &mut self,
        authoritative_tick: u64,
        authoritative_state: S,
        mut simulate: Simulate,
    ) -> NetcodeResult<ReconciliationReport<S>>
    where
        Simulate: FnMut(&mut S, u64, &I),
    {
        if authoritative_tick < self.confirmed_tick {
            return Err(NetcodeError::InvalidInput(
                "authoritative prediction Tick moved backwards",
            ));
        }
        let confirmed_frames = self
            .frames
            .iter()
            .position(|frame| frame.tick > authoritative_tick)
            .unwrap_or(self.frames.len());
        let replayed_inputs = self.frames.len() - confirmed_frames;
        if replayed_inputs > self.config.max_replay_steps {
            return Err(NetcodeError::ReplayLimitExceeded);
        }

        let predicted_state_at_authoritative_tick = confirmed_frames
            .checked_sub(1)
            .and_then(|index| self.frames.get(index))
            .filter(|frame| frame.tick == authoritative_tick)
            .map(|frame| frame.predicted_state.clone());
        let mut corrected_state = authoritative_state;
        self.frames.drain(..confirmed_frames);
        for frame in &mut self.frames {
            simulate(&mut corrected_state, frame.tick, &frame.input);
            frame.predicted_state = corrected_state.clone();
        }
        let current_tick = self
            .frames
            .back()
            .map_or(authoritative_tick, |frame| frame.tick);

        self.confirmed_tick = authoritative_tick;
        Ok(ReconciliationReport {
            authoritative_tick,
            predicted_state_at_authoritative_tick,
            corrected_state,
            replayed_inputs,
            current_tick,
        })
    }

    /// Clears all prediction history and installs a new confirmed Tick.
    pub fn reset(&mut self, confirmed_tick: u64) {
        self.frames.clear();
        self.confirmed_tick = confirmed_tick;
    }
}
