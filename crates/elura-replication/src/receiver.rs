use std::collections::{BTreeMap, BTreeSet};

use elura_netcode::{SequenceDisposition, SequenceWindow};

use crate::{
    ReplicationAck, ReplicationBatch, ReplicationConfig, ReplicationError, ReplicationEvent,
    ReplicationPacket, ReplicationResult, VersionedState,
};

/// Result after buffering and applying one replication packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationReceiveReport {
    /// Newly applied contiguous batches.
    pub applied_batches: usize,
    /// Entity events applied across those batches.
    pub applied_events: usize,
    /// Previously received redundant batches ignored from this packet.
    pub duplicate_batches: usize,
    /// Cumulative acknowledgement to return to the sender.
    pub acknowledgement: ReplicationAck,
}

/// Ordered receiver and entity baseline store for one observer stream.
#[derive(Debug, Clone)]
pub struct ReplicationReceiver<I, S, D> {
    config: ReplicationConfig,
    sequences: SequenceWindow,
    pending: BTreeMap<u64, ReplicationBatch<I, S, D>>,
    entities: BTreeMap<I, VersionedState<S>>,
    applied_tick: u64,
}

impl<I, S, D> ReplicationReceiver<I, S, D>
where
    I: Ord + Clone,
    S: Clone,
{
    /// Creates an empty receiver expecting batch sequence one.
    pub fn new(config: ReplicationConfig) -> ReplicationResult<Self> {
        config.validate()?;
        let sequences = SequenceWindow::new(config.reorder_window).map_err(|_| {
            ReplicationError::InvalidConfig("batch reorder window must be positive")
        })?;
        Ok(Self {
            config,
            sequences,
            pending: BTreeMap::new(),
            entities: BTreeMap::new(),
            applied_tick: 0,
        })
    }

    /// Buffers reordered batches and applies every newly contiguous batch transactionally.
    ///
    /// `apply_delta` must reconstruct the next full state from the current baseline and event
    /// delta. Returning `None` rejects the entire packet without changing receiver state.
    pub fn receive<ApplyDelta>(
        &mut self,
        mut packet: ReplicationPacket<I, S, D>,
        mut apply_delta: ApplyDelta,
    ) -> ReplicationResult<ReplicationReceiveReport>
    where
        ApplyDelta: FnMut(&I, &S, &D) -> Option<S>,
    {
        if packet.batches.len() > self.config.redundancy {
            return Err(ReplicationError::InvalidPacket(
                "packet contains too many redundant batches",
            ));
        }
        packet.batches.sort_by_key(|batch| batch.sequence);

        let old_acknowledged = self.sequences.acknowledged();
        let mut next_sequences = self.sequences.clone();
        let mut accepted = BTreeMap::new();
        let mut next_tick = self.applied_tick;
        let mut duplicate_batches = 0;

        for batch in packet.batches {
            if batch.tick == 0 || batch.events.len() > self.config.max_events_per_batch {
                return Err(ReplicationError::InvalidPacket(
                    "batch Tick or event count is invalid",
                ));
            }
            match next_sequences.observe(batch.sequence).map_err(|_| {
                ReplicationError::InvalidPacket("batch sequence exceeds the reorder window")
            })? {
                SequenceDisposition::Duplicate => duplicate_batches += 1,
                SequenceDisposition::Accepted => {
                    accepted.insert(batch.sequence, batch);
                }
            }
        }

        let new_acknowledged = next_sequences.acknowledged();
        let mut entity_count = self.entities.len();
        let mut entity_changes = BTreeMap::new();
        let mut applied_batches = 0;
        let mut applied_events = 0;
        for sequence in old_acknowledged.saturating_add(1)..=new_acknowledged {
            let accepted_batch = accepted.remove(&sequence);
            let batch = accepted_batch
                .as_ref()
                .or_else(|| self.pending.get(&sequence))
                .ok_or(ReplicationError::InvalidPacket(
                    "contiguous batch payload is missing",
                ))?;
            if batch.tick < next_tick {
                return Err(ReplicationError::InvalidOrder(
                    "applied batch Tick moved backwards",
                ));
            }
            applied_events += stage_batch(
                &self.entities,
                &mut entity_changes,
                &mut entity_count,
                batch,
                self.config.max_entities,
                &mut apply_delta,
            )?;
            next_tick = batch.tick;
            applied_batches += 1;
        }

        for (entity, state) in entity_changes {
            if let Some(state) = state {
                self.entities.insert(entity, state);
            } else {
                self.entities.remove(&entity);
            }
        }
        self.pending
            .retain(|sequence, _| *sequence > new_acknowledged);
        self.pending.extend(accepted);
        self.sequences = next_sequences;
        self.applied_tick = next_tick;
        Ok(ReplicationReceiveReport {
            applied_batches,
            applied_events,
            duplicate_batches,
            acknowledgement: ReplicationAck {
                acknowledged_sequence: self.sequences.acknowledged(),
                applied_tick: self.applied_tick,
            },
        })
    }

    /// Returns an entity's currently applied version and full state.
    pub fn entity(&self, entity: &I) -> Option<&VersionedState<S>> {
        self.entities.get(entity)
    }

    /// Iterates all currently spawned entities in identifier order.
    pub fn entities(&self) -> impl Iterator<Item = (&I, &VersionedState<S>)> {
        self.entities.iter()
    }

    /// Returns the number of currently spawned entities.
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    /// Returns whether no entity is currently spawned.
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    /// Returns the number of reordered batches waiting above a sequence gap.
    pub fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    /// Returns the highest contiguous applied batch sequence.
    pub fn acknowledged_sequence(&self) -> u64 {
        self.sequences.acknowledged()
    }

    /// Clears all stream and entity state.
    pub fn reset(&mut self) -> ReplicationResult<()> {
        self.sequences = SequenceWindow::new(self.config.reorder_window).map_err(|_| {
            ReplicationError::InvalidConfig("batch reorder window must be positive")
        })?;
        self.pending.clear();
        self.entities.clear();
        self.applied_tick = 0;
        Ok(())
    }
}

fn stage_batch<I, S, D, ApplyDelta>(
    entities: &BTreeMap<I, VersionedState<S>>,
    changes: &mut BTreeMap<I, Option<VersionedState<S>>>,
    entity_count: &mut usize,
    batch: &ReplicationBatch<I, S, D>,
    max_entities: usize,
    apply_delta: &mut ApplyDelta,
) -> ReplicationResult<usize>
where
    I: Ord + Clone,
    S: Clone,
    ApplyDelta: FnMut(&I, &S, &D) -> Option<S>,
{
    let mut affected = BTreeSet::new();
    for event in &batch.events {
        if !affected.insert(event.entity().clone()) {
            return Err(ReplicationError::InvalidPacket(
                "batch changes one entity more than once",
            ));
        }
        match event {
            ReplicationEvent::Spawn {
                entity,
                version,
                prediction_key,
                state,
            } => {
                if staged_entity(entities, changes, entity).is_some()
                    || *entity_count >= max_entities
                {
                    return Err(if staged_entity(entities, changes, entity).is_some() {
                        ReplicationError::InvalidPacket("spawned entity already exists")
                    } else {
                        ReplicationError::EntityLimitExceeded
                    });
                }
                changes.insert(
                    entity.clone(),
                    Some(VersionedState {
                        version: *version,
                        prediction_key: *prediction_key,
                        state: state.clone(),
                    }),
                );
                *entity_count += 1;
            }
            ReplicationEvent::Despawn { entity } => {
                if staged_entity(entities, changes, entity).is_none() {
                    return Err(ReplicationError::InvalidPacket(
                        "despawned entity does not exist",
                    ));
                }
                changes.insert(entity.clone(), None);
                *entity_count -= 1;
            }
            ReplicationEvent::Update {
                entity,
                base_version,
                version,
                delta,
            } => {
                let current = staged_entity(entities, changes, entity)
                    .ok_or(ReplicationError::BaselineMismatch)?;
                if current.version != *base_version || *version <= *base_version {
                    return Err(ReplicationError::BaselineMismatch);
                }
                let state = apply_delta(entity, &current.state, delta)
                    .ok_or(ReplicationError::DeltaRejected)?;
                let prediction_key = current.prediction_key;
                changes.insert(
                    entity.clone(),
                    Some(VersionedState {
                        version: *version,
                        prediction_key,
                        state,
                    }),
                );
            }
            ReplicationEvent::Keyframe {
                entity,
                version,
                prediction_key,
                state,
            } => {
                let existed = staged_entity(entities, changes, entity).is_some();
                if !existed && *entity_count >= max_entities {
                    return Err(ReplicationError::EntityLimitExceeded);
                }
                changes.insert(
                    entity.clone(),
                    Some(VersionedState {
                        version: *version,
                        prediction_key: *prediction_key,
                        state: state.clone(),
                    }),
                );
                if !existed {
                    *entity_count += 1;
                }
            }
        }
    }
    Ok(batch.events.len())
}

fn staged_entity<'a, I, S>(
    entities: &'a BTreeMap<I, VersionedState<S>>,
    changes: &'a BTreeMap<I, Option<VersionedState<S>>>,
    entity: &I,
) -> Option<&'a VersionedState<S>>
where
    I: Ord,
{
    changes
        .get(entity)
        .map_or_else(|| entities.get(entity), Option::as_ref)
}
