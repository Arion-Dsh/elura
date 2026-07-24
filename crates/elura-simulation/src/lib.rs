//! Deterministic fixed-step simulation timing primitives.
//!
//! [`FixedStepClock`] converts irregular wall-clock updates into bounded fixed-duration steps. It
//! does not create threads or timers; an application, scene, or test explicitly supplies elapsed
//! time and owns all simulation state.

#![deny(missing_docs)]

use std::fmt;
use std::time::Duration;

/// Limits used by a [`FixedStepClock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SimulationConfig {
    /// Deterministic duration represented by one simulation step.
    pub step: Duration,
    /// Maximum steps executed by one call to [`FixedStepClock::advance`].
    pub max_steps_per_update: u32,
    /// Maximum wall-clock backlog retained for later updates.
    pub max_accumulated_time: Duration,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            step: Duration::from_millis(50),
            max_steps_per_update: 4,
            max_accumulated_time: Duration::from_millis(500),
        }
    }
}

impl SimulationConfig {
    /// Validates fixed-step and overload limits.
    pub fn validate(&self) -> SimulationResult<()> {
        if self.step.is_zero() {
            return Err(SimulationError::InvalidConfig("step must be positive"));
        }
        if self.max_steps_per_update == 0 {
            return Err(SimulationError::InvalidConfig(
                "max_steps_per_update must be positive",
            ));
        }
        if self.max_accumulated_time < self.step {
            return Err(SimulationError::InvalidConfig(
                "max_accumulated_time must be at least one step",
            ));
        }
        Ok(())
    }
}

/// Context supplied to one deterministic simulation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimulationStep {
    /// One-based global step number after this step commits.
    pub tick: u128,
    /// Fixed delta configured for every step.
    pub delta: Duration,
    /// Total deterministic simulation time after this step commits.
    pub simulation_time: Duration,
}

/// Summary returned after advancing a fixed-step clock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulationReport {
    /// Number of steps committed by this update.
    pub steps: u32,
    /// Most recently committed global tick.
    pub tick: u128,
    /// Total deterministic time committed by the simulation.
    pub simulation_time: Duration,
    /// Wall-clock backlog retained after this update.
    pub backlog: Duration,
    /// Whole fixed steps still waiting in the backlog.
    pub backlog_steps: u128,
    /// Clamped render interpolation ratio in `0.0..=1.0`.
    pub interpolation: f64,
    /// Excess wall-clock time discarded by this update.
    pub dropped_time: Duration,
    /// Excess wall-clock time discarded since the last reset.
    pub total_dropped_time: Duration,
}

/// Synchronous state machine advanced by [`FixedStepClock`].
///
/// Simulation steps should avoid blocking I/O. Publish durable work after a successful step or use
/// an application-owned outbox when external side effects are required.
pub trait FixedStepSimulation {
    /// Application error returned by a failed step.
    type Error;

    /// Applies exactly one deterministic step.
    fn step(&mut self, step: SimulationStep) -> std::result::Result<(), Self::Error>;
}

/// Result returned while constructing simulation timing primitives.
pub type SimulationResult<T> = std::result::Result<T, SimulationError>;

/// Invalid fixed-step clock configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SimulationError {
    /// Configuration cannot produce a bounded fixed-step clock.
    InvalidConfig(&'static str),
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid simulation config: {message}")
            }
        }
    }
}

impl std::error::Error for SimulationError {}

/// Deterministic accumulator with bounded catch-up work.
#[derive(Debug, Clone)]
pub struct FixedStepClock {
    config: SimulationConfig,
    accumulator: Duration,
    simulation_time: Duration,
    tick: u128,
    total_dropped_time: Duration,
}

impl FixedStepClock {
    /// Creates a clock at tick zero with no backlog.
    pub fn new(config: SimulationConfig) -> SimulationResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            accumulator: Duration::ZERO,
            simulation_time: Duration::ZERO,
            tick: 0,
            total_dropped_time: Duration::ZERO,
        })
    }

    /// Returns the immutable timing configuration.
    pub fn config(&self) -> &SimulationConfig {
        &self.config
    }

    /// Returns the most recently committed tick.
    pub fn tick(&self) -> u128 {
        self.tick
    }

    /// Returns committed deterministic simulation time.
    pub fn simulation_time(&self) -> Duration {
        self.simulation_time
    }

    /// Returns retained wall-clock backlog.
    pub fn backlog(&self) -> Duration {
        self.accumulator
    }

    /// Returns total discarded wall-clock time.
    pub fn total_dropped_time(&self) -> Duration {
        self.total_dropped_time
    }

    /// Adds elapsed wall time and invokes a callback for each fixed step that may run this update.
    ///
    /// A failed callback does not consume its fixed step. Successfully committed earlier steps stay
    /// committed, and the remaining backlog can be retried by a later call.
    pub fn advance<E, F>(
        &mut self,
        elapsed: Duration,
        mut step: F,
    ) -> std::result::Result<SimulationReport, E>
    where
        F: FnMut(SimulationStep) -> std::result::Result<(), E>,
    {
        let accumulated = self.accumulator.saturating_add(elapsed);
        let dropped_time = accumulated.saturating_sub(self.config.max_accumulated_time);
        self.accumulator = accumulated.min(self.config.max_accumulated_time);
        self.total_dropped_time = self.total_dropped_time.saturating_add(dropped_time);

        let available = whole_steps(self.accumulator, self.config.step);
        let scheduled = available.min(u128::from(self.config.max_steps_per_update)) as u32;
        let mut committed = 0;
        for _ in 0..scheduled {
            let next_tick = self.tick.saturating_add(1);
            let next_time = self.simulation_time.saturating_add(self.config.step);
            step(SimulationStep {
                tick: next_tick,
                delta: self.config.step,
                simulation_time: next_time,
            })?;
            self.tick = next_tick;
            self.simulation_time = next_time;
            self.accumulator = self.accumulator.saturating_sub(self.config.step);
            committed += 1;
        }

        Ok(self.report(committed, dropped_time))
    }

    /// Advances a value implementing the [`FixedStepSimulation`] trait.
    pub fn advance_simulation<S>(
        &mut self,
        elapsed: Duration,
        simulation: &mut S,
    ) -> std::result::Result<SimulationReport, S::Error>
    where
        S: FixedStepSimulation,
    {
        self.advance(elapsed, |step| simulation.step(step))
    }

    /// Clears tick, time, backlog and overload counters while retaining configuration.
    pub fn reset(&mut self) {
        self.accumulator = Duration::ZERO;
        self.simulation_time = Duration::ZERO;
        self.tick = 0;
        self.total_dropped_time = Duration::ZERO;
    }

    fn report(&self, steps: u32, dropped_time: Duration) -> SimulationReport {
        let backlog_steps = whole_steps(self.accumulator, self.config.step);
        let interpolation = if backlog_steps > 0 {
            1.0
        } else {
            duration_ratio(self.accumulator, self.config.step)
        };
        SimulationReport {
            steps,
            tick: self.tick,
            simulation_time: self.simulation_time,
            backlog: self.accumulator,
            backlog_steps,
            interpolation,
            dropped_time,
            total_dropped_time: self.total_dropped_time,
        }
    }
}

fn whole_steps(duration: Duration, step: Duration) -> u128 {
    duration.as_nanos() / step.as_nanos()
}

fn duration_ratio(duration: Duration, step: Duration) -> f64 {
    duration.as_secs_f64() / step.as_secs_f64()
}
