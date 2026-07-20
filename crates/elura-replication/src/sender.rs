use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    ReplicationAck, ReplicationBatch, ReplicationConfig, ReplicationError, ReplicationEvent,
    ReplicationPacket, ReplicationResult, VersionedState,
};

/// Per-observer visibility reconciler and reliable batch history.
#[derive(Debug, Clone)]
pub struct ReplicationSender<I, S, D> {
    config: ReplicationConfig,
    next_sequence: u64,
    acknowledged_sequence: u64,
    last_tick: u64,
    projected: BTreeMap<I, VersionedState<S>>,
    pending: VecDeque<ReplicationBatch<I, S, D>>,
    force_keyframes: bool,
}

impl<I, S, D> ReplicationSender<I, S, D>
where
    I: Ord + Clone,
    S: Clone,
{
    /// Creates an empty observer stream whose first batch sequence is one.
    pub fn new(config: ReplicationConfig) -> ReplicationResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            next_sequence: 1,
            acknowledged_sequence: 0,
            last_tick: 0,
            projected: BTreeMap::new(),
            pending: VecDeque::with_capacity(config.history_capacity),
            force_keyframes: false,
        })
    }

    /// Reconciles the full currently visible state set and queues ordered replication batches.
    ///
    /// `delta` returns an application-owned delta from the projected client state to the current
    /// state. Returning `None` emits a full [`ReplicationEvent::Keyframe`] instead. Processing is
    /// transactional: invalid input or insufficient history capacity leaves this sender unchanged.
    pub fn update<Visible, EncodeDelta>(
        &mut self,
        tick: u64,
        visible: Visible,
        mut delta: EncodeDelta,
    ) -> ReplicationResult<usize>
    where
        Visible: IntoIterator<Item = (I, VersionedState<S>)>,
        EncodeDelta: FnMut(&I, &VersionedState<S>, &VersionedState<S>) -> Option<D>,
    {
        if tick == 0 || tick <= self.last_tick {
            return Err(ReplicationError::InvalidOrder(
                "update Tick must increase and be positive",
            ));
        }
        let mut desired = BTreeMap::new();
        for (entity, state) in visible {
            if desired.insert(entity, state).is_some() {
                return Err(ReplicationError::InvalidPacket(
                    "visible state contains a duplicate entity",
                ));
            }
        }
        if desired.len() > self.config.max_entities {
            return Err(ReplicationError::EntityLimitExceeded);
        }

        let mut events = Vec::new();
        for entity in self.projected.keys() {
            if !desired.contains_key(entity) {
                events.push(ReplicationEvent::Despawn {
                    entity: entity.clone(),
                });
            }
        }
        for (entity, current) in &desired {
            match self.projected.get(entity) {
                None => events.push(ReplicationEvent::Spawn {
                    entity: entity.clone(),
                    version: current.version,
                    prediction_key: current.prediction_key,
                    state: current.state.clone(),
                }),
                Some(previous) if current.version < previous.version => {
                    return Err(ReplicationError::VersionRegression);
                }
                Some(_previous) if self.force_keyframes => {
                    events.push(ReplicationEvent::Keyframe {
                        entity: entity.clone(),
                        version: current.version,
                        prediction_key: current.prediction_key,
                        state: current.state.clone(),
                    });
                }
                Some(previous) if current.version != previous.version => {
                    if let Some(encoded) = delta(entity, previous, current) {
                        events.push(ReplicationEvent::Update {
                            entity: entity.clone(),
                            base_version: previous.version,
                            version: current.version,
                            delta: encoded,
                        });
                    } else {
                        events.push(ReplicationEvent::Keyframe {
                            entity: entity.clone(),
                            version: current.version,
                            prediction_key: current.prediction_key,
                            state: current.state.clone(),
                        });
                    }
                }
                Some(_) => {}
            }
        }

        let batch_count = events.len().div_ceil(self.config.max_events_per_batch);
        if self.pending.len().saturating_add(batch_count) > self.config.history_capacity {
            return Err(ReplicationError::HistoryFull);
        }
        let batch_count_u64 =
            u64::try_from(batch_count).map_err(|_| ReplicationError::SequenceExhausted)?;
        let next_sequence = self
            .next_sequence
            .checked_add(batch_count_u64)
            .ok_or(ReplicationError::SequenceExhausted)?;

        let mut iterator = events.into_iter();
        for sequence in self.next_sequence..next_sequence {
            let batch_events = iterator
                .by_ref()
                .take(self.config.max_events_per_batch)
                .collect();
            self.pending.push_back(ReplicationBatch {
                sequence,
                tick,
                events: batch_events,
            });
        }
        self.next_sequence = next_sequence;
        self.last_tick = tick;
        self.projected = desired;
        self.force_keyframes = false;
        Ok(batch_count)
    }

    /// Applies a cumulative receiver acknowledgement and releases retained batches.
    pub fn acknowledge(&mut self, acknowledgement: ReplicationAck) -> ReplicationResult<usize> {
        let last_issued = self.next_sequence - 1;
        if acknowledgement.acknowledged_sequence > last_issued {
            return Err(ReplicationError::InvalidAcknowledgement);
        }
        if acknowledgement.acknowledged_sequence <= self.acknowledged_sequence {
            return Ok(0);
        }
        let expected_tick = self
            .pending
            .iter()
            .find(|batch| batch.sequence == acknowledgement.acknowledged_sequence)
            .map(|batch| batch.tick)
            .ok_or(ReplicationError::InvalidAcknowledgement)?;
        if expected_tick != acknowledgement.applied_tick {
            return Err(ReplicationError::InvalidAcknowledgement);
        }

        self.acknowledged_sequence = acknowledgement.acknowledged_sequence;
        let before = self.pending.len();
        while self
            .pending
            .front()
            .is_some_and(|batch| batch.sequence <= self.acknowledged_sequence)
        {
            self.pending.pop_front();
        }
        Ok(before - self.pending.len())
    }

    /// Forces full keyframes for all entities visible on the next update.
    pub fn force_keyframes(&mut self) {
        self.force_keyframes = true;
    }

    /// Clears stream state and restarts sequence numbering.
    ///
    /// Sender and receiver resets must be coordinated by the application, normally through a
    /// reconnect or explicit stream-reset message.
    pub fn reset(&mut self) {
        self.next_sequence = 1;
        self.acknowledged_sequence = 0;
        self.last_tick = 0;
        self.projected.clear();
        self.pending.clear();
        self.force_keyframes = false;
    }

    /// Returns the number of entities projected after all queued batches are applied.
    pub fn projected_entities(&self) -> usize {
        self.projected.len()
    }

    /// Returns the number of batches waiting for cumulative acknowledgement.
    pub fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    /// Returns the highest cumulatively acknowledged batch sequence.
    pub fn acknowledged_sequence(&self) -> u64 {
        self.acknowledged_sequence
    }
}

impl<I, S, D> ReplicationSender<I, S, D>
where
    I: Ord + Clone,
    S: Clone,
    D: Clone,
{
    /// Builds a packet that prioritizes both the oldest ACK gap and recent state.
    pub fn packet(&self) -> ReplicationPacket<I, S, D> {
        if self.pending.len() <= self.config.redundancy {
            return ReplicationPacket {
                batches: self.pending.iter().cloned().collect(),
            };
        }

        let mut selected = BTreeSet::new();
        selected.insert(0);
        let newest = self.config.redundancy.saturating_sub(1);
        for index in self.pending.len().saturating_sub(newest)..self.pending.len() {
            selected.insert(index);
        }
        ReplicationPacket {
            batches: selected
                .into_iter()
                .filter_map(|index| self.pending.get(index).cloned())
                .collect(),
        }
    }
}
