use elura_netcode::PredictionKey;
use serde::{Deserialize, Serialize};

use crate::{ReplicationError, ReplicationResult};

/// Memory, batching, redundancy, and reorder limits for one observer stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct ReplicationConfig {
    /// Maximum batches retained by a sender until cumulatively acknowledged.
    pub history_capacity: usize,
    /// Maximum retained batches included in one packet.
    ///
    /// When history is larger, the oldest unacknowledged batch is included alongside the newest
    /// batches so a missing cumulative-ACK gap continues to make progress.
    pub redundancy: usize,
    /// Maximum entity events stored in one ordered batch.
    pub max_events_per_batch: usize,
    /// Maximum entities tracked for one observer.
    pub max_entities: usize,
    /// Maximum batch sequence distance buffered above the cumulative acknowledgement.
    pub reorder_window: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            history_capacity: 64,
            redundancy: 3,
            max_events_per_batch: 256,
            max_entities: 4096,
            reorder_window: 256,
        }
    }
}

impl ReplicationConfig {
    /// Validates all memory and protocol bounds.
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.history_capacity == 0 {
            return Err(ReplicationError::InvalidConfig(
                "batch history capacity must be positive",
            ));
        }
        if self.redundancy == 0 || self.redundancy > self.history_capacity {
            return Err(ReplicationError::InvalidConfig(
                "batch redundancy must be within history capacity",
            ));
        }
        if self.max_events_per_batch == 0 {
            return Err(ReplicationError::InvalidConfig(
                "maximum events per batch must be positive",
            ));
        }
        if self.max_entities == 0 {
            return Err(ReplicationError::InvalidConfig(
                "maximum replicated entities must be positive",
            ));
        }
        if self.reorder_window == 0 {
            return Err(ReplicationError::InvalidConfig(
                "batch reorder window must be positive",
            ));
        }
        if u64::try_from(self.history_capacity)
            .map_or(true, |history| self.reorder_window < history)
        {
            return Err(ReplicationError::InvalidConfig(
                "batch reorder window must cover sender history",
            ));
        }
        Ok(())
    }
}

/// Application state paired with its monotonically increasing entity version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedState<S> {
    /// Application-owned entity state version.
    pub version: u64,
    /// Optional originating client prediction key for authoritative spawn matching.
    pub prediction_key: Option<PredictionKey>,
    /// Full application-owned entity state.
    pub state: S,
}

/// One observer-facing entity lifecycle or state change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationEvent<I, S, D> {
    /// An entity entered visibility and must be created from full state.
    Spawn {
        /// Stable entity identifier.
        entity: I,
        /// Full state version.
        version: u64,
        /// Optional key matching a client-predicted temporary entity.
        prediction_key: Option<PredictionKey>,
        /// Full entity state.
        state: S,
    },
    /// An entity left visibility and must be removed.
    Despawn {
        /// Stable entity identifier.
        entity: I,
    },
    /// A visible entity changed relative to a known state version.
    Update {
        /// Stable entity identifier.
        entity: I,
        /// State version required before applying `delta`.
        base_version: u64,
        /// State version produced after applying `delta`.
        version: u64,
        /// Application-owned state delta.
        delta: D,
    },
    /// A visible entity must replace any local baseline with full state.
    Keyframe {
        /// Stable entity identifier.
        entity: I,
        /// Replacement state version.
        version: u64,
        /// Optional key matching a client-predicted temporary entity.
        prediction_key: Option<PredictionKey>,
        /// Replacement full entity state.
        state: S,
    },
}

impl<I, S, D> ReplicationEvent<I, S, D> {
    /// Returns the entity affected by this event.
    pub fn entity(&self) -> &I {
        match self {
            Self::Spawn { entity, .. }
            | Self::Despawn { entity }
            | Self::Update { entity, .. }
            | Self::Keyframe { entity, .. } => entity,
        }
    }
}

/// Ordered entity changes generated at one authoritative simulation Tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationBatch<I, S, D> {
    /// Monotonically increasing observer-stream batch sequence.
    pub sequence: u64,
    /// Authoritative simulation Tick represented by these changes.
    pub tick: u64,
    /// Bounded entity changes applied in order.
    pub events: Vec<ReplicationEvent<I, S, D>>,
}

/// Transport-neutral packet containing redundant ordered batches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationPacket<I, S, D> {
    /// Oldest gap candidate and recent unacknowledged batches, ordered by sequence.
    pub batches: Vec<ReplicationBatch<I, S, D>>,
}

/// Cumulative receiver acknowledgement for one observer stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationAck {
    /// Highest contiguous batch sequence successfully applied.
    pub acknowledged_sequence: u64,
    /// Tick of the acknowledged batch, or zero before any batch is applied.
    pub applied_tick: u64,
}
