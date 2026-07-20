use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{NetcodeError, NetcodeResult};

/// Client-side Tick estimation limits.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct TickSyncConfig {
    /// Authoritative server simulation frequency.
    pub tick_rate: u32,
    /// Future input delay added after the estimated current server Tick.
    pub input_delay_ticks: u64,
    /// Exponential smoothing weight applied to each accepted sample in `0.0..=1.0`.
    pub smoothing: f64,
    /// Largest network round-trip duration accepted as a useful sample.
    pub max_round_trip_time: Duration,
    /// Largest Tick-offset correction accepted from one sample before smoothing.
    pub max_offset_correction_ticks: f64,
}

impl Default for TickSyncConfig {
    fn default() -> Self {
        Self {
            tick_rate: 30,
            input_delay_ticks: 2,
            smoothing: 0.2,
            max_round_trip_time: Duration::from_secs(2),
            max_offset_correction_ticks: 4.0,
        }
    }
}

impl TickSyncConfig {
    /// Validates Tick rate, duration, and smoothing bounds.
    pub fn validate(&self) -> NetcodeResult<()> {
        if !(1..=240).contains(&self.tick_rate) {
            return Err(NetcodeError::InvalidConfig(
                "Tick rate must be within 1..=240",
            ));
        }
        if !self.smoothing.is_finite() || self.smoothing <= 0.0 || self.smoothing > 1.0 {
            return Err(NetcodeError::InvalidConfig(
                "Tick smoothing must be within 0.0..=1.0",
            ));
        }
        if self.max_round_trip_time.is_zero() {
            return Err(NetcodeError::InvalidConfig(
                "maximum round-trip time must be positive",
            ));
        }
        if !self.max_offset_correction_ticks.is_finite() || self.max_offset_correction_ticks <= 0.0
        {
            return Err(NetcodeError::InvalidConfig(
                "maximum Tick correction must be finite and positive",
            ));
        }
        Ok(())
    }
}

/// Client probe echoed by a Tick synchronization response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickSyncRequest {
    /// Client-selected probe sequence used to correlate the response.
    pub sequence: u64,
    /// Client monotonic time when this probe was sent.
    pub client_sent_at: Duration,
}

/// Authoritative server response used to construct a [`TickSyncSample`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickSyncResponse {
    /// Echoed client probe sequence.
    pub sequence: u64,
    /// Echoed client monotonic send time.
    pub client_sent_at: Duration,
    /// Server monotonic time when the probe was received.
    pub server_received_at: Duration,
    /// Server monotonic time when this response was sent.
    pub server_sent_at: Duration,
    /// Authoritative simulation Tick at `server_sent_at`.
    pub server_tick: u64,
}

impl TickSyncResponse {
    /// Converts the four probe timestamps into an estimator sample.
    pub fn sample(
        self,
        request: TickSyncRequest,
        client_received_at: Duration,
        local_tick: f64,
    ) -> NetcodeResult<TickSyncSample> {
        if self.sequence != request.sequence || self.client_sent_at != request.client_sent_at {
            return Err(NetcodeError::InvalidSample(
                "Tick response does not match its request",
            ));
        }
        let server_processing_time = self
            .server_sent_at
            .checked_sub(self.server_received_at)
            .ok_or(NetcodeError::InvalidSample(
                "server send time precedes receive time",
            ))?;
        Ok(TickSyncSample {
            local_tick,
            server_tick: self.server_tick,
            client_sent_at: request.client_sent_at,
            client_received_at,
            server_processing_time,
        })
    }
}

/// One client observation of an authoritative server Tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickSyncSample {
    /// Client simulation Tick when the response was received; fractional values are allowed.
    pub local_tick: f64,
    /// Authoritative server Tick at response send time.
    pub server_tick: u64,
    /// Client monotonic time when the request was sent.
    pub client_sent_at: Duration,
    /// Client monotonic time when the response was received.
    pub client_received_at: Duration,
    /// Time spent processing the probe on the server.
    pub server_processing_time: Duration,
}

/// Updated estimates returned after accepting one Tick sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickSyncReport {
    /// Total client-observed request/response duration.
    pub round_trip_time: Duration,
    /// Round-trip duration after subtracting server processing time.
    pub network_round_trip_time: Duration,
    /// Estimated one-way network delay.
    pub one_way_delay: Duration,
    /// Unsmoothed server-minus-client Tick offset from this sample.
    pub raw_offset_ticks: f64,
    /// Current smoothed server-minus-client Tick offset.
    pub offset_ticks: f64,
    /// Estimated server Tick at client response receipt.
    pub estimated_server_tick: f64,
    /// Recommended future authoritative Tick for the next client input.
    pub recommended_input_tick: u64,
}

/// Client-side estimator for authoritative server Tick position.
#[derive(Debug, Clone)]
pub struct TickSynchronizer {
    config: TickSyncConfig,
    offset_ticks: f64,
    smoothed_network_rtt_seconds: f64,
    samples: u64,
}

impl TickSynchronizer {
    /// Creates an estimator with no accepted samples.
    pub fn new(config: TickSyncConfig) -> NetcodeResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            offset_ticks: 0.0,
            smoothed_network_rtt_seconds: 0.0,
            samples: 0,
        })
    }

    /// Observes one response and updates RTT and Tick-offset estimates.
    ///
    /// The server Tick is defined at response send time. Half the network RTT is therefore added
    /// before comparing it with the client's Tick at response receipt.
    pub fn observe(&mut self, sample: TickSyncSample) -> NetcodeResult<TickSyncReport> {
        if !sample.local_tick.is_finite() || sample.local_tick < 0.0 {
            return Err(NetcodeError::InvalidSample(
                "local Tick must be finite and non-negative",
            ));
        }
        let round_trip_time = sample
            .client_received_at
            .checked_sub(sample.client_sent_at)
            .ok_or(NetcodeError::InvalidSample(
                "client receive time precedes send time",
            ))?;
        let network_round_trip_time = round_trip_time
            .checked_sub(sample.server_processing_time)
            .ok_or(NetcodeError::InvalidSample(
                "server processing exceeds total round-trip time",
            ))?;
        if network_round_trip_time > self.config.max_round_trip_time {
            return Err(NetcodeError::InvalidSample(
                "network round-trip time exceeds the configured maximum",
            ));
        }

        let network_seconds = network_round_trip_time.as_secs_f64();
        let one_way_seconds = network_seconds / 2.0;
        let estimated_server_tick =
            sample.server_tick as f64 + one_way_seconds * f64::from(self.config.tick_rate);
        let raw_offset_ticks = estimated_server_tick - sample.local_tick;

        if self.samples == 0 {
            self.offset_ticks = raw_offset_ticks;
            self.smoothed_network_rtt_seconds = network_seconds;
        } else {
            let correction = (raw_offset_ticks - self.offset_ticks).clamp(
                -self.config.max_offset_correction_ticks,
                self.config.max_offset_correction_ticks,
            );
            self.offset_ticks += correction * self.config.smoothing;
            self.smoothed_network_rtt_seconds +=
                (network_seconds - self.smoothed_network_rtt_seconds) * self.config.smoothing;
        }
        self.samples = self.samples.saturating_add(1);

        Ok(TickSyncReport {
            round_trip_time,
            network_round_trip_time,
            one_way_delay: Duration::from_secs_f64(one_way_seconds),
            raw_offset_ticks,
            offset_ticks: self.offset_ticks,
            estimated_server_tick: self.estimate_server_tick(sample.local_tick),
            recommended_input_tick: self.recommended_input_tick(sample.local_tick),
        })
    }

    /// Estimates the server's fractional Tick at a given local fractional Tick.
    pub fn estimate_server_tick(&self, local_tick: f64) -> f64 {
        if !local_tick.is_finite() {
            return 0.0;
        }
        (local_tick + self.offset_ticks).max(0.0)
    }

    /// Chooses the next authoritative input Tick using the configured input delay.
    pub fn recommended_input_tick(&self, local_tick: f64) -> u64 {
        let estimated = self.estimate_server_tick(local_tick).floor();
        let current = if estimated >= u64::MAX as f64 {
            u64::MAX
        } else {
            estimated as u64
        };
        current
            .saturating_add(self.config.input_delay_ticks)
            .saturating_add(1)
    }

    /// Returns the smoothed server-minus-client Tick offset.
    pub fn offset_ticks(&self) -> f64 {
        self.offset_ticks
    }

    /// Returns the smoothed network RTT after subtracting server processing.
    pub fn network_round_trip_time(&self) -> Option<Duration> {
        (self.samples > 0)
            .then(|| Duration::from_secs_f64(self.smoothed_network_rtt_seconds.max(0.0)))
    }

    /// Returns the number of accepted synchronization samples.
    pub fn samples(&self) -> u64 {
        self.samples
    }
}
