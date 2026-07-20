//! Deterministic network-condition simulation for tests and local development.
//!
//! [`SimulatedLink`] queues application-owned packets under configured latency, jitter, loss,
//! duplication, reordering delay, queue capacity, and serialization bandwidth. It performs no I/O
//! and owns no clock or task; tests supply monotonic time explicitly. Create one link per direction
//! when asymmetric client/server conditions are required.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Result returned by network simulation operations.
pub type NetSimResult<T> = std::result::Result<T, NetSimError>;

/// Invalid deterministic network simulation configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetSimError {
    /// One probability, duration, bandwidth, or queue bound is invalid.
    InvalidConfig(&'static str),
    /// Caller-provided monotonic simulation time moved backwards.
    TimeMovedBackwards,
}

impl fmt::Display for NetSimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid network simulation config: {message}")
            }
            Self::TimeMovedBackwards => {
                formatter.write_str("network simulation time moved backwards")
            }
        }
    }
}

impl std::error::Error for NetSimError {}

/// Deterministic one-way simulated link parameters.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct NetSimConfig {
    /// Fixed one-way propagation delay.
    pub latency: Duration,
    /// Uniform random propagation variation in `-jitter..=jitter`.
    pub jitter: Duration,
    /// Independent packet loss probability in `0.0..=1.0`.
    pub loss_rate: f64,
    /// Probability that one accepted packet produces a second copy.
    pub duplicate_rate: f64,
    /// Probability that one copy receives additional reordering delay.
    pub reorder_rate: f64,
    /// Maximum uniformly selected additional reordering delay.
    pub max_reorder_delay: Duration,
    /// Serialization bandwidth in bytes per second; zero means unlimited.
    pub bandwidth_bytes_per_second: u64,
    /// Maximum packet copies waiting for delivery.
    pub max_queued_packets: usize,
    /// Deterministic pseudo-random seed.
    pub seed: u64,
}

impl Default for NetSimConfig {
    fn default() -> Self {
        Self {
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_rate: 0.0,
            duplicate_rate: 0.0,
            reorder_rate: 0.0,
            max_reorder_delay: Duration::from_millis(100),
            bandwidth_bytes_per_second: 0,
            max_queued_packets: 4096,
            seed: 1,
        }
    }
}

impl NetSimConfig {
    /// Validates probabilities and bounded queue/reordering parameters.
    pub fn validate(&self) -> NetSimResult<()> {
        for probability in [self.loss_rate, self.duplicate_rate, self.reorder_rate] {
            if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
                return Err(NetSimError::InvalidConfig(
                    "packet probabilities must be within 0.0..=1.0",
                ));
            }
        }
        if self.max_queued_packets == 0 {
            return Err(NetSimError::InvalidConfig(
                "maximum queued packets must be positive",
            ));
        }
        if self.reorder_rate > 0.0 && self.max_reorder_delay.is_zero() {
            return Err(NetSimError::InvalidConfig(
                "reordering delay must be positive when reordering is enabled",
            ));
        }
        Ok(())
    }
}

/// Result of attempting to send one packet through a simulated link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// Packet was probabilistically lost before entering the queue.
    DroppedByLoss,
    /// Packet and all requested copies were rejected by the queue bound.
    DroppedByQueue,
    /// One or two packet copies were queued.
    Queued {
        /// Number of scheduled copies.
        copies: usize,
        /// Earliest scheduled delivery time among those copies.
        first_delivery: Duration,
    },
}

/// One packet released when simulated time reaches its delivery deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredPacket<T> {
    /// Application-owned packet payload.
    pub payload: T,
    /// Caller-supplied encoded packet size.
    pub bytes: usize,
    /// Simulated delivery deadline.
    pub delivered_at: Duration,
}

/// Operational statistics for one simulated direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetSimStats {
    /// Original send attempts.
    pub packets_sent: u64,
    /// Original packets probabilistically lost.
    pub packets_lost: u64,
    /// Original packets rejected because all copies would exceed queue capacity.
    pub packets_queue_dropped: u64,
    /// Extra packet copies scheduled.
    pub duplicate_copies: u64,
    /// Copies assigned extra reordering delay.
    pub reordered_copies: u64,
    /// Packet copies released to the receiver.
    pub packets_delivered: u64,
    /// Bytes accepted into the simulated queue, including duplicates.
    pub bytes_queued: u64,
    /// Bytes released to the receiver.
    pub bytes_delivered: u64,
    /// Aggregate nanoseconds spent waiting for serialization bandwidth.
    pub bandwidth_delay_nanos: u128,
}

#[derive(Debug, Clone)]
struct Scheduled<T> {
    deliver_at: Duration,
    order: u64,
    bytes: usize,
    payload: T,
}

impl<T> PartialEq for Scheduled<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.order == other.order
    }
}

impl<T> Eq for Scheduled<T> {}

impl<T> PartialOrd for Scheduled<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Scheduled<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deliver_at
            .cmp(&self.deliver_at)
            .then_with(|| other.order.cmp(&self.order))
    }
}

/// Deterministic queued simulation of one network direction.
#[derive(Debug, Clone)]
pub struct SimulatedLink<T> {
    config: NetSimConfig,
    queue: BinaryHeap<Scheduled<T>>,
    random: DeterministicRandom,
    next_order: u64,
    transmit_available_at: Duration,
    last_time: Duration,
    stats: NetSimStats,
}

impl<T> SimulatedLink<T> {
    /// Creates an empty one-way link.
    pub fn new(config: NetSimConfig) -> NetSimResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            queue: BinaryHeap::new(),
            random: DeterministicRandom::new(config.seed),
            next_order: 0,
            transmit_available_at: Duration::ZERO,
            last_time: Duration::ZERO,
            stats: NetSimStats::default(),
        })
    }

    /// Releases every packet whose simulated deadline is no later than `now`.
    pub fn receive(&mut self, now: Duration) -> NetSimResult<Vec<DeliveredPacket<T>>> {
        self.validate_time(now)?;
        let mut delivered = Vec::new();
        while self
            .queue
            .peek()
            .is_some_and(|scheduled| scheduled.deliver_at <= now)
        {
            let scheduled = self.queue.pop().unwrap();
            self.stats.packets_delivered = self.stats.packets_delivered.saturating_add(1);
            self.stats.bytes_delivered = self
                .stats
                .bytes_delivered
                .saturating_add(scheduled.bytes as u64);
            delivered.push(DeliveredPacket {
                payload: scheduled.payload,
                bytes: scheduled.bytes,
                delivered_at: scheduled.deliver_at,
            });
        }
        Ok(delivered)
    }

    /// Returns the next scheduled delivery time.
    pub fn next_delivery(&self) -> Option<Duration> {
        self.queue.peek().map(|scheduled| scheduled.deliver_at)
    }

    /// Returns the number of queued packet copies.
    pub fn queued_packets(&self) -> usize {
        self.queue.len()
    }

    /// Returns current deterministic simulation counters.
    pub fn stats(&self) -> NetSimStats {
        self.stats
    }

    fn validate_time(&mut self, now: Duration) -> NetSimResult<()> {
        if now < self.last_time {
            return Err(NetSimError::TimeMovedBackwards);
        }
        self.last_time = now;
        Ok(())
    }
}

impl<T: Clone> SimulatedLink<T> {
    /// Attempts to send one packet with an explicit encoded byte size at monotonic time `now`.
    pub fn send(&mut self, now: Duration, bytes: usize, payload: T) -> NetSimResult<SendOutcome> {
        self.validate_time(now)?;
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        if self.random.chance(self.config.loss_rate) {
            self.stats.packets_lost = self.stats.packets_lost.saturating_add(1);
            return Ok(SendOutcome::DroppedByLoss);
        }

        let copies = 1 + usize::from(self.random.chance(self.config.duplicate_rate));
        if self.queue.len().saturating_add(copies) > self.config.max_queued_packets {
            self.stats.packets_queue_dropped = self.stats.packets_queue_dropped.saturating_add(1);
            return Ok(SendOutcome::DroppedByQueue);
        }

        let mut first_delivery = None;
        for copy in 0..copies {
            let transmission_start = now.max(self.transmit_available_at);
            let bandwidth_delay = transmission_start.saturating_sub(now);
            self.stats.bandwidth_delay_nanos = self
                .stats
                .bandwidth_delay_nanos
                .saturating_add(bandwidth_delay.as_nanos());
            let serialization =
                serialization_duration(bytes, self.config.bandwidth_bytes_per_second);
            self.transmit_available_at = transmission_start.saturating_add(serialization);

            let mut propagation =
                jittered_delay(self.config.latency, self.config.jitter, &mut self.random);
            if self.random.chance(self.config.reorder_rate) {
                propagation = propagation.saturating_add(scale_duration(
                    self.config.max_reorder_delay,
                    self.random.unit(),
                ));
                self.stats.reordered_copies = self.stats.reordered_copies.saturating_add(1);
            }
            let deliver_at = self
                .transmit_available_at
                .saturating_add(propagation)
                .saturating_add(Duration::from_nanos(copy as u64));
            first_delivery =
                Some(first_delivery.map_or(deliver_at, |first: Duration| first.min(deliver_at)));
            self.next_order = self.next_order.saturating_add(1);
            self.queue.push(Scheduled {
                deliver_at,
                order: self.next_order,
                bytes,
                payload: payload.clone(),
            });
        }

        if copies > 1 {
            self.stats.duplicate_copies = self.stats.duplicate_copies.saturating_add(1);
        }
        self.stats.bytes_queued = self
            .stats
            .bytes_queued
            .saturating_add((bytes as u64).saturating_mul(copies as u64));
        Ok(SendOutcome::Queued {
            copies,
            first_delivery: first_delivery.unwrap(),
        })
    }
}

fn serialization_duration(bytes: usize, bytes_per_second: u64) -> Duration {
    if bytes_per_second == 0 || bytes == 0 {
        return Duration::ZERO;
    }
    let numerator = (bytes as u128).saturating_mul(1_000_000_000);
    let denominator = u128::from(bytes_per_second);
    let nanos = numerator.saturating_add(denominator.saturating_sub(1)) / denominator;
    duration_from_nanos(nanos)
}

fn jittered_delay(
    latency: Duration,
    jitter: Duration,
    random: &mut DeterministicRandom,
) -> Duration {
    if jitter.is_zero() {
        return latency;
    }
    let signed = random.unit() * 2.0 - 1.0;
    let magnitude = scale_duration(jitter, signed.abs());
    if signed.is_sign_negative() {
        latency.saturating_sub(magnitude)
    } else {
        latency.saturating_add(magnitude)
    }
}

fn scale_duration(duration: Duration, factor: f64) -> Duration {
    duration_from_nanos((duration.as_nanos() as f64 * factor.clamp(0.0, 1.0)) as u128)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = nanos / 1_000_000_000;
    if seconds > u128::from(u64::MAX) {
        return Duration::MAX;
    }
    Duration::new(seconds as u64, (nanos % 1_000_000_000) as u32)
}

#[derive(Debug, Clone)]
struct DeterministicRandom {
    state: u64,
}

impl DeterministicRandom {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    fn next(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn unit(&mut self) -> f64 {
        self.next() as f64 / u64::MAX as f64
    }

    fn chance(&mut self, probability: f64) -> bool {
        probability >= 1.0 || (probability > 0.0 && self.unit() < probability)
    }
}
