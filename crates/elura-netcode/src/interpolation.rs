use std::collections::VecDeque;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{NetcodeError, NetcodeResult};

/// Remote-state buffer and adaptive interpolation-delay parameters.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct InterpolationConfig {
    /// Authoritative server simulation frequency.
    pub tick_rate: u32,
    /// Maximum remote states retained in Tick order.
    pub capacity: usize,
    /// Normal render delay before measured jitter or late arrivals are added.
    pub base_delay_ticks: f64,
    /// Minimum adaptive render delay.
    pub min_delay_ticks: f64,
    /// Maximum adaptive render delay.
    pub max_delay_ticks: f64,
    /// Exponential smoothing weight for arrival jitter and late-sample pressure.
    pub smoothing: f64,
    /// Number of additional delay ticks added per measured jitter Tick.
    pub jitter_multiplier: f64,
    /// Maximum additional delay caused by repeatedly late samples.
    pub late_sample_penalty_ticks: f64,
    /// Maximum interpolation-delay movement after one newest-state observation.
    pub max_adjustment_per_sample_ticks: f64,
}

impl Default for InterpolationConfig {
    fn default() -> Self {
        Self {
            tick_rate: 30,
            capacity: 128,
            base_delay_ticks: 2.0,
            min_delay_ticks: 1.0,
            max_delay_ticks: 8.0,
            smoothing: 0.1,
            jitter_multiplier: 2.0,
            late_sample_penalty_ticks: 2.0,
            max_adjustment_per_sample_ticks: 0.25,
        }
    }
}

impl InterpolationConfig {
    /// Validates Tick, capacity, smoothing, and delay bounds.
    pub fn validate(&self) -> NetcodeResult<()> {
        let values = [
            self.base_delay_ticks,
            self.min_delay_ticks,
            self.max_delay_ticks,
            self.smoothing,
            self.jitter_multiplier,
            self.late_sample_penalty_ticks,
            self.max_adjustment_per_sample_ticks,
        ];
        if !(1..=240).contains(&self.tick_rate) || self.capacity < 2 {
            return Err(NetcodeError::InvalidConfig(
                "interpolation Tick rate or capacity is invalid",
            ));
        }
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
            || self.smoothing <= 0.0
            || self.smoothing > 1.0
            || self.min_delay_ticks > self.base_delay_ticks
            || self.base_delay_ticks > self.max_delay_ticks
            || self.max_adjustment_per_sample_ticks == 0.0
        {
            return Err(NetcodeError::InvalidConfig(
                "interpolation delay parameters are invalid",
            ));
        }
        Ok(())
    }
}

/// Result of inserting one remote state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterpolationInsert {
    /// A state newer than every buffered Tick was inserted.
    Newest,
    /// A previously missing older Tick was inserted out of order.
    Late,
    /// A duplicate Tick replaced its buffered state.
    Replaced,
}

/// Current interpolation timing statistics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InterpolationStats {
    /// Smoothed absolute arrival variation expressed in simulation ticks.
    pub jitter_ticks: f64,
    /// Smoothed fraction of observations that arrived behind the newest buffered Tick.
    pub late_sample_pressure: f64,
    /// Current adaptive render delay.
    pub delay_ticks: f64,
    /// Number of out-of-order states inserted.
    pub late_samples: u64,
    /// Number of duplicate Tick states replaced.
    pub replaced_samples: u64,
}

/// Two buffered states and interpolation factor selected for rendering.
#[derive(Debug, Clone, Copy)]
pub struct InterpolationSample<'a, S> {
    /// Fractional server Tick selected for rendering.
    pub render_tick: f64,
    /// Tick of the earlier state.
    pub previous_tick: u64,
    /// Earlier state supplied to application interpolation.
    pub previous: &'a S,
    /// Tick of the later state.
    pub next_tick: u64,
    /// Later state supplied to application interpolation.
    pub next: &'a S,
    /// Clamped blend factor between `previous` and `next`.
    pub alpha: f64,
    /// `true` when rendering is ahead of the newest state and the newest state is held.
    pub holding_newest: bool,
}

/// Tick-ordered remote state buffer with adaptive jitter delay.
#[derive(Debug, Clone)]
pub struct InterpolationBuffer<S> {
    config: InterpolationConfig,
    states: VecDeque<(u64, S)>,
    last_arrival: Option<Duration>,
    last_newest: Option<(u64, Duration)>,
    jitter_ticks: f64,
    late_pressure: f64,
    delay_ticks: f64,
    late_samples: u64,
    replaced_samples: u64,
}

impl<S> InterpolationBuffer<S> {
    /// Creates an empty remote-state buffer.
    pub fn new(config: InterpolationConfig) -> NetcodeResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            states: VecDeque::with_capacity(config.capacity),
            last_arrival: None,
            last_newest: None,
            jitter_ticks: 0.0,
            late_pressure: 0.0,
            delay_ticks: config.base_delay_ticks,
            late_samples: 0,
            replaced_samples: 0,
        })
    }

    /// Inserts one remote state observed at a local monotonic arrival time.
    pub fn insert(
        &mut self,
        tick: u64,
        state: S,
        arrived_at: Duration,
    ) -> NetcodeResult<InterpolationInsert> {
        if tick == 0 {
            return Err(NetcodeError::InvalidInput(
                "interpolation state Tick must be positive",
            ));
        }
        if self
            .last_arrival
            .is_some_and(|last_arrival| arrived_at < last_arrival)
        {
            return Err(NetcodeError::InvalidSample(
                "interpolation arrival time moved backwards",
            ));
        }
        let newest = self.states.back().map(|(tick, _)| *tick);
        let state_index = self
            .states
            .binary_search_by_key(&tick, |(buffered_tick, _)| *buffered_tick);
        let disposition = match state_index {
            Ok(_) => {
                self.replaced_samples = self.replaced_samples.saturating_add(1);
                InterpolationInsert::Replaced
            }
            Err(_) if newest.is_some_and(|newest| tick < newest) => {
                self.late_samples = self.late_samples.saturating_add(1);
                InterpolationInsert::Late
            }
            Err(_) => InterpolationInsert::Newest,
        };

        let late_observation = matches!(disposition, InterpolationInsert::Late);
        self.late_pressure +=
            ((u8::from(late_observation) as f64) - self.late_pressure) * self.config.smoothing;
        if matches!(disposition, InterpolationInsert::Newest) {
            if let Some((previous_tick, previous_arrival)) = self.last_newest {
                let tick_gap = tick.saturating_sub(previous_tick);
                if tick_gap > 0 {
                    let actual = arrived_at.saturating_sub(previous_arrival).as_secs_f64();
                    let expected = tick_gap as f64 / f64::from(self.config.tick_rate);
                    let variation_ticks =
                        (actual - expected).abs() * f64::from(self.config.tick_rate);
                    self.jitter_ticks +=
                        (variation_ticks - self.jitter_ticks) * self.config.smoothing;
                }
            }
            self.last_newest = Some((tick, arrived_at));
        }
        self.last_arrival = Some(arrived_at);

        let target_delay = (self.config.base_delay_ticks
            + self.jitter_ticks * self.config.jitter_multiplier
            + self.late_pressure * self.config.late_sample_penalty_ticks)
            .clamp(self.config.min_delay_ticks, self.config.max_delay_ticks);
        let adjustment = (target_delay - self.delay_ticks).clamp(
            -self.config.max_adjustment_per_sample_ticks,
            self.config.max_adjustment_per_sample_ticks,
        );
        self.delay_ticks += adjustment;

        match state_index {
            Ok(index) => self.states[index].1 = state,
            Err(index) => self.states.insert(index, (tick, state)),
        }
        if self.states.len() > self.config.capacity {
            self.states.pop_front();
        }
        Ok(disposition)
    }

    /// Selects buffered states around the adaptive delayed render Tick.
    pub fn sample(&self, estimated_server_tick: f64) -> NetcodeResult<InterpolationSample<'_, S>> {
        if !estimated_server_tick.is_finite() || estimated_server_tick < 0.0 {
            return Err(NetcodeError::InvalidSample(
                "estimated server Tick must be finite and non-negative",
            ));
        }
        let Some((oldest_tick, oldest)) = self.states.front() else {
            return Err(NetcodeError::InterpolationBufferEmpty);
        };
        let (newest_tick, newest) = self.states.back().unwrap();
        let render_tick = (estimated_server_tick - self.delay_ticks).max(0.0);
        if render_tick <= *oldest_tick as f64 {
            return Ok(InterpolationSample {
                render_tick,
                previous_tick: *oldest_tick,
                previous: oldest,
                next_tick: *oldest_tick,
                next: oldest,
                alpha: 0.0,
                holding_newest: false,
            });
        }
        if render_tick >= *newest_tick as f64 {
            return Ok(InterpolationSample {
                render_tick,
                previous_tick: *newest_tick,
                previous: newest,
                next_tick: *newest_tick,
                next: newest,
                alpha: 0.0,
                holding_newest: render_tick > *newest_tick as f64,
            });
        }

        let previous_bound = render_tick.floor() as u64;
        let next_bound = render_tick.ceil() as u64;
        let previous_index = self
            .states
            .binary_search_by_key(&previous_bound, |(tick, _)| *tick)
            .unwrap_or_else(|index| index - 1);
        let next_index = self
            .states
            .binary_search_by_key(&next_bound, |(tick, _)| *tick)
            .unwrap_or_else(|index| index);
        let (previous_tick, previous) = &self.states[previous_index];
        let (next_tick, next) = &self.states[next_index];
        let span = next_tick.saturating_sub(*previous_tick);
        let alpha = if span == 0 {
            0.0
        } else {
            ((render_tick - *previous_tick as f64) / span as f64).clamp(0.0, 1.0)
        };
        Ok(InterpolationSample {
            render_tick,
            previous_tick: *previous_tick,
            previous,
            next_tick: *next_tick,
            next,
            alpha,
            holding_newest: false,
        })
    }

    /// Returns current jitter and adaptive-delay measurements.
    pub fn stats(&self) -> InterpolationStats {
        InterpolationStats {
            jitter_ticks: self.jitter_ticks,
            late_sample_pressure: self.late_pressure,
            delay_ticks: self.delay_ticks,
            late_samples: self.late_samples,
            replaced_samples: self.replaced_samples,
        }
    }

    /// Returns the number of buffered remote states.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Returns whether no remote state has arrived.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Clears buffered states and timing measurements.
    pub fn reset(&mut self) {
        self.states.clear();
        self.last_arrival = None;
        self.last_newest = None;
        self.jitter_ticks = 0.0;
        self.late_pressure = 0.0;
        self.delay_ticks = self.config.base_delay_ticks;
        self.late_samples = 0;
        self.replaced_samples = 0;
    }
}
