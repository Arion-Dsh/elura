//! Bounded authoritative history for server-side lag-compensated queries.
//!
//! [`LagCompensationHistory`] retains application-owned immutable collision or query snapshots by
//! simulation Tick. It validates a requested rewind window and lends the historical state to a
//! callback without mutating the live World or Scene. Hit detection and game rules remain in the
//! application.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use std::collections::VecDeque;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Result returned by lag-compensation history operations.
pub type LagCompensationResult<T> = std::result::Result<T, LagCompensationError>;

/// Configuration, Tick ordering, or rewind validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LagCompensationError {
    /// History bounds cannot cover the configured rewind window.
    InvalidConfig(&'static str),
    /// Recorded authoritative ticks are not strictly increasing.
    InvalidTickOrder,
    /// A rewind was requested before any state was recorded.
    HistoryEmpty,
    /// A client requested a Tick newer than the authoritative current Tick.
    FutureTick,
    /// A client requested a Tick outside the allowed rewind window.
    RewindLimitExceeded,
    /// The exact requested Tick is no longer present in history.
    StateUnavailable,
}

impl fmt::Display for LagCompensationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid lag compensation config: {message}")
            }
            Self::InvalidTickOrder => {
                formatter.write_str("authoritative history Tick did not increase")
            }
            Self::HistoryEmpty => formatter.write_str("lag compensation history is empty"),
            Self::FutureTick => formatter.write_str("cannot rewind to a future Tick"),
            Self::RewindLimitExceeded => {
                formatter.write_str("requested Tick exceeds the rewind limit")
            }
            Self::StateUnavailable => {
                formatter.write_str("requested historical state is unavailable")
            }
        }
    }
}

impl std::error::Error for LagCompensationError {}

/// Memory and trust bounds for authoritative rewind queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct LagCompensationConfig {
    /// Maximum authoritative snapshots retained.
    pub history_capacity: usize,
    /// Maximum number of ticks a client query may rewind from the current Tick.
    pub max_rewind_ticks: u64,
}

impl Default for LagCompensationConfig {
    fn default() -> Self {
        Self {
            history_capacity: 64,
            max_rewind_ticks: 30,
        }
    }
}

impl LagCompensationConfig {
    /// Validates that retained history can cover every allowed rewind Tick.
    pub fn validate(&self) -> LagCompensationResult<()> {
        if self.history_capacity == 0 || self.max_rewind_ticks == 0 {
            return Err(LagCompensationError::InvalidConfig(
                "history capacity and rewind window must be positive",
            ));
        }
        let required = self.max_rewind_ticks.saturating_add(1);
        if u64::try_from(self.history_capacity).map_or(true, |capacity| capacity < required) {
            return Err(LagCompensationError::InvalidConfig(
                "history capacity must include the current Tick and full rewind window",
            ));
        }
        Ok(())
    }
}

/// Validated context accompanying one historical state query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewindContext {
    /// Requested and exactly resolved historical Tick.
    pub target_tick: u64,
    /// Current authoritative Tick at query time.
    pub current_tick: u64,
    /// Distance between current and historical state.
    pub rewind_ticks: u64,
}

/// Borrowed historical state and its validated rewind context.
#[derive(Debug, Clone, Copy)]
pub struct RewindSample<'a, S> {
    /// Rewind validation metadata.
    pub context: RewindContext,
    /// Immutable application-owned collision or query state.
    pub state: &'a S,
}

/// Operational counts for authoritative history and rewind queries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LagCompensationStats {
    /// Authoritative states successfully recorded.
    pub states_recorded: u64,
    /// Historical queries successfully resolved.
    pub queries_completed: u64,
    /// Future, expired, or unavailable queries rejected.
    pub queries_rejected: u64,
}

/// Tick-indexed immutable authoritative state history.
#[derive(Debug, Clone)]
pub struct LagCompensationHistory<S> {
    config: LagCompensationConfig,
    states: VecDeque<(u64, S)>,
    stats: LagCompensationStats,
}

impl<S> LagCompensationHistory<S> {
    /// Creates an empty history ring.
    pub fn new(config: LagCompensationConfig) -> LagCompensationResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            states: VecDeque::with_capacity(config.history_capacity),
            stats: LagCompensationStats::default(),
        })
    }

    /// Records one immutable application snapshot at a strictly increasing Tick.
    pub fn record(&mut self, tick: u64, state: S) -> LagCompensationResult<()> {
        if tick == 0
            || self
                .states
                .back()
                .is_some_and(|(current, _)| tick <= *current)
        {
            return Err(LagCompensationError::InvalidTickOrder);
        }
        if self.states.len() == self.config.history_capacity {
            self.states.pop_front();
        }
        self.states.push_back((tick, state));
        self.stats.states_recorded = self.stats.states_recorded.saturating_add(1);
        Ok(())
    }

    /// Validates and borrows the exact historical state for a client-reported Tick.
    pub fn rewind(&mut self, target_tick: u64) -> LagCompensationResult<RewindSample<'_, S>> {
        let result = self.validate_rewind(target_tick);
        match result {
            Ok((context, index)) => {
                self.stats.queries_completed = self.stats.queries_completed.saturating_add(1);
                Ok(RewindSample {
                    context,
                    state: &self.states[index].1,
                })
            }
            Err(error) => {
                self.stats.queries_rejected = self.stats.queries_rejected.saturating_add(1);
                Err(error)
            }
        }
    }

    /// Runs an application hit-test or query against one immutable historical state.
    pub fn with_rewind<R>(
        &mut self,
        target_tick: u64,
        query: impl FnOnce(RewindContext, &S) -> R,
    ) -> LagCompensationResult<R> {
        let sample = self.rewind(target_tick)?;
        Ok(query(sample.context, sample.state))
    }

    fn validate_rewind(&self, target_tick: u64) -> LagCompensationResult<(RewindContext, usize)> {
        let Some((current_tick, _)) = self.states.back() else {
            return Err(LagCompensationError::HistoryEmpty);
        };
        if target_tick > *current_tick {
            return Err(LagCompensationError::FutureTick);
        }
        let rewind_ticks = *current_tick - target_tick;
        if rewind_ticks > self.config.max_rewind_ticks {
            return Err(LagCompensationError::RewindLimitExceeded);
        }
        let index = self
            .states
            .binary_search_by_key(&target_tick, |(tick, _)| *tick)
            .map_err(|_| LagCompensationError::StateUnavailable)?;
        Ok((
            RewindContext {
                target_tick,
                current_tick: *current_tick,
                rewind_ticks,
            },
            index,
        ))
    }

    /// Returns the newest authoritative Tick in history.
    pub fn current_tick(&self) -> Option<u64> {
        self.states.back().map(|(tick, _)| *tick)
    }

    /// Returns the oldest retained authoritative Tick.
    pub fn oldest_tick(&self) -> Option<u64> {
        self.states.front().map(|(tick, _)| *tick)
    }

    /// Returns the number of retained states.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Returns whether no authoritative state has been recorded.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Returns current recording and query counts.
    pub fn stats(&self) -> LagCompensationStats {
        self.stats
    }

    /// Clears history and statistics.
    pub fn reset(&mut self) {
        self.states.clear();
        self.stats = LagCompensationStats::default();
    }
}
