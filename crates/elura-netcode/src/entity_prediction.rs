use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{NetcodeError, NetcodeResult};

/// Client-generated stable key used to match a predicted spawn with its authoritative entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PredictionKey(pub u64);

/// Monotonic prediction-key allocator.
#[derive(Debug, Clone)]
pub struct PredictionKeyGenerator {
    next: u64,
}

impl Default for PredictionKeyGenerator {
    fn default() -> Self {
        Self { next: 1 }
    }
}

impl PredictionKeyGenerator {
    /// Creates a generator with an application-restored next key.
    pub fn with_next(next: u64) -> NetcodeResult<Self> {
        if next == 0 {
            return Err(NetcodeError::InvalidConfig(
                "next prediction key must be positive",
            ));
        }
        Ok(Self { next })
    }

    /// Allocates the next non-zero prediction key.
    pub fn generate(&mut self) -> NetcodeResult<PredictionKey> {
        let key = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or(NetcodeError::SequenceExhausted)?;
        Ok(PredictionKey(key))
    }
}

/// Bounds for unmatched locally predicted entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct PredictedEntityConfig {
    /// Maximum pending predicted entities.
    pub capacity: usize,
    /// Maximum age before an unmatched prediction expires.
    pub timeout_ticks: u64,
}

impl Default for PredictedEntityConfig {
    fn default() -> Self {
        Self {
            capacity: 128,
            timeout_ticks: 120,
        }
    }
}

impl PredictedEntityConfig {
    /// Validates matcher capacity and expiry bounds.
    pub fn validate(&self) -> NetcodeResult<()> {
        if self.capacity == 0 || self.timeout_ticks == 0 {
            return Err(NetcodeError::InvalidConfig(
                "predicted entity limits must be positive",
            ));
        }
        Ok(())
    }
}

/// One locally spawned entity waiting for authoritative identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictedEntity<I> {
    /// Client prediction key included in the authoritative spawn request/state.
    pub key: PredictionKey,
    /// Temporary application-owned client entity identifier.
    pub temporary_entity: I,
    /// Client Tick when the predicted entity was created.
    pub spawned_tick: u64,
}

/// Successful mapping from temporary entity identity to authoritative identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityMatch<I> {
    /// Matched prediction key.
    pub key: PredictionKey,
    /// Temporary client entity to replace or merge.
    pub temporary_entity: I,
    /// Authoritative server entity identity.
    pub authoritative_entity: I,
    /// Ticks elapsed before the match arrived.
    pub age_ticks: u64,
}

/// Bounded predicted-spawn registry for one client simulation.
#[derive(Debug, Clone)]
pub struct PredictedEntityMatcher<I> {
    config: PredictedEntityConfig,
    pending: BTreeMap<PredictionKey, PredictedEntity<I>>,
    temporary: BTreeMap<I, PredictionKey>,
}

impl<I> PredictedEntityMatcher<I>
where
    I: Ord + Clone,
{
    /// Creates an empty predicted-entity matcher.
    pub fn new(config: PredictedEntityConfig) -> NetcodeResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            pending: BTreeMap::new(),
            temporary: BTreeMap::new(),
        })
    }

    /// Registers one temporary entity under a unique prediction key.
    pub fn register(
        &mut self,
        key: PredictionKey,
        temporary_entity: I,
        spawned_tick: u64,
    ) -> NetcodeResult<()> {
        if key.0 == 0 || spawned_tick == 0 {
            return Err(NetcodeError::InvalidInput(
                "prediction key and spawn Tick must be positive",
            ));
        }
        if self.pending.contains_key(&key) || self.temporary.contains_key(&temporary_entity) {
            return Err(NetcodeError::InvalidInput(
                "prediction key or temporary entity is already registered",
            ));
        }
        if self.pending.len() >= self.config.capacity {
            return Err(NetcodeError::PredictedEntityLimit);
        }
        self.temporary.insert(temporary_entity.clone(), key);
        self.pending.insert(
            key,
            PredictedEntity {
                key,
                temporary_entity,
                spawned_tick,
            },
        );
        Ok(())
    }

    /// Resolves one authoritative spawn and removes its temporary mapping.
    pub fn resolve(
        &mut self,
        key: PredictionKey,
        authoritative_entity: I,
        current_tick: u64,
    ) -> NetcodeResult<Option<EntityMatch<I>>> {
        let Some(predicted) = self.pending.get(&key) else {
            return Ok(None);
        };
        if current_tick < predicted.spawned_tick {
            return Err(NetcodeError::InvalidInput(
                "authoritative match Tick precedes predicted spawn",
            ));
        }
        if current_tick - predicted.spawned_tick > self.config.timeout_ticks {
            let predicted = self.pending.remove(&key).unwrap();
            self.temporary.remove(&predicted.temporary_entity);
            return Ok(None);
        }
        let predicted = self.pending.remove(&key).unwrap();
        self.temporary.remove(&predicted.temporary_entity);
        Ok(Some(EntityMatch {
            key,
            temporary_entity: predicted.temporary_entity,
            authoritative_entity,
            age_ticks: current_tick - predicted.spawned_tick,
        }))
    }

    /// Removes and returns predictions older than the configured timeout.
    pub fn expire(&mut self, current_tick: u64) -> Vec<PredictedEntity<I>> {
        let expired_keys = self
            .pending
            .iter()
            .filter(|(_, predicted)| {
                current_tick.saturating_sub(predicted.spawned_tick) > self.config.timeout_ticks
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        expired_keys
            .into_iter()
            .filter_map(|key| {
                let predicted = self.pending.remove(&key)?;
                self.temporary.remove(&predicted.temporary_entity);
                Some(predicted)
            })
            .collect()
    }

    /// Returns the number of unmatched predicted entities.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns whether no predicted entity is waiting for a match.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Returns a pending prediction by key.
    pub fn get(&self, key: PredictionKey) -> Option<&PredictedEntity<I>> {
        self.pending.get(&key)
    }

    /// Clears all unmatched predicted entities.
    pub fn clear(&mut self) {
        self.pending.clear();
        self.temporary.clear();
    }
}
