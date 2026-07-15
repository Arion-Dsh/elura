use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use elura_core::{Error, Result};
use uuid::Uuid;

use super::contract::validate_reason;
use super::{DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore};

#[derive(Clone)]
struct Entry {
    event: OutboxEvent,
    attempt: u32,
    worker: Option<String>,
    token: Option<Uuid>,
    lease_until: Option<SystemTime>,
    last_error: String,
}

#[derive(Default)]
struct State {
    active: HashMap<Uuid, Entry>,
    dead: HashMap<Uuid, DeadLetter>,
    completed: HashMap<Uuid, OutboxEvent>,
}

#[derive(Default)]
pub struct MemoryOutbox {
    state: Mutex<State>,
}

impl MemoryOutbox {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::Internal("outbox lock poisoned".into()))
    }

    fn leased<'a>(state: &'a mut State, delivery: &OutboxDelivery) -> Result<&'a mut Entry> {
        let entry = state
            .active
            .get_mut(&delivery.event.id)
            .ok_or(Error::OutboxNotFound)?;
        if entry.worker.as_deref() != Some(&delivery.worker)
            || entry.token != Some(delivery.token)
            || entry
                .lease_until
                .is_none_or(|until| until <= SystemTime::now())
        {
            return Err(Error::OutboxLeaseLost);
        }
        Ok(entry)
    }
}

#[async_trait]
impl OutboxStore for MemoryOutbox {
    async fn append(&self, event: OutboxEvent) -> Result<()> {
        event.validate()?;
        let mut state = self.lock()?;
        let existing = state
            .active
            .get(&event.id)
            .map(|entry| &entry.event)
            .or_else(|| state.dead.get(&event.id).map(|letter| &letter.event))
            .or_else(|| state.completed.get(&event.id));
        if let Some(existing) = existing {
            return if existing.same_identity(&event) {
                Ok(())
            } else {
                Err(Error::DuplicateEvent)
            };
        }
        state.active.insert(
            event.id,
            Entry {
                event,
                attempt: 0,
                worker: None,
                token: None,
                lease_until: None,
                last_error: String::new(),
            },
        );
        Ok(())
    }

    async fn acquire(
        &self,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        if worker.trim().is_empty()
            || worker.len() > 128
            || limit == 0
            || limit > 4096
            || lease.is_zero()
        {
            return Err(Error::InvalidConfig("invalid outbox lease".into()));
        }
        let now = SystemTime::now();
        let mut state = self.lock()?;
        let mut ids: Vec<_> = state
            .active
            .iter()
            .filter_map(|(id, entry)| {
                let available = entry.event.available_at <= now;
                let unleased = entry.lease_until.is_none_or(|until| until <= now);
                (available && unleased).then_some(*id)
            })
            .collect();
        ids.sort_by_key(|id| {
            let event = &state.active[id].event;
            (event.created_at, *id)
        });
        ids.truncate(limit);
        let mut deliveries = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(entry) = state.active.get_mut(&id) else {
                continue;
            };
            entry.attempt = entry.attempt.saturating_add(1);
            let token = Uuid::new_v4();
            let lease_until = now + lease;
            entry.worker = Some(worker.to_owned());
            entry.token = Some(token);
            entry.lease_until = Some(lease_until);
            deliveries.push(OutboxDelivery {
                event: entry.event.clone(),
                attempt: entry.attempt,
                worker: worker.to_owned(),
                token,
                lease_until,
            });
        }
        Ok(deliveries)
    }

    async fn ack(&self, delivery: &OutboxDelivery) -> Result<()> {
        let mut state = self.lock()?;
        Self::leased(&mut state, delivery)?;
        let entry = state
            .active
            .remove(&delivery.event.id)
            .ok_or(Error::OutboxNotFound)?;
        state.completed.insert(entry.event.id, entry.event);
        Ok(())
    }

    async fn renew(&self, delivery: &OutboxDelivery, lease: Duration) -> Result<()> {
        if lease.is_zero() {
            return Err(Error::InvalidConfig("invalid outbox lease".into()));
        }
        let mut state = self.lock()?;
        let entry = Self::leased(&mut state, delivery)?;
        entry.lease_until = Some(SystemTime::now() + lease);
        Ok(())
    }

    async fn retry(
        &self,
        delivery: &OutboxDelivery,
        available_at: SystemTime,
        reason: &str,
    ) -> Result<()> {
        validate_reason(reason)?;
        let mut state = self.lock()?;
        let entry = Self::leased(&mut state, delivery)?;
        entry.event.available_at = available_at.max(SystemTime::now());
        entry.last_error = reason.to_owned();
        entry.worker = None;
        entry.token = None;
        entry.lease_until = None;
        Ok(())
    }

    async fn dead_letter(&self, delivery: &OutboxDelivery, reason: &str) -> Result<()> {
        validate_reason(reason)?;
        let mut state = self.lock()?;
        Self::leased(&mut state, delivery)?;
        let entry = state
            .active
            .remove(&delivery.event.id)
            .ok_or(Error::OutboxNotFound)?;
        state.dead.insert(
            entry.event.id,
            DeadLetter {
                event: entry.event,
                attempt: entry.attempt,
                reason: reason.to_owned(),
                failed_at: SystemTime::now(),
            },
        );
        Ok(())
    }

    async fn list_dead_letters(&self, limit: usize) -> Result<Vec<DeadLetter>> {
        if limit == 0 {
            return Err(Error::InvalidConfig("dead-letter limit is zero".into()));
        }
        let state = self.lock()?;
        let mut letters: Vec<_> = state.dead.values().cloned().collect();
        letters.sort_by_key(|letter| std::cmp::Reverse(letter.failed_at));
        letters.truncate(limit);
        Ok(letters)
    }

    async fn replay_dead_letter(&self, id: Uuid, available_at: SystemTime) -> Result<()> {
        let mut state = self.lock()?;
        let mut letter = state.dead.remove(&id).ok_or(Error::OutboxNotFound)?;
        letter.event.available_at = available_at.max(SystemTime::now());
        state.active.insert(
            id,
            Entry {
                event: letter.event,
                attempt: 0,
                worker: None,
                token: None,
                lease_until: None,
                last_error: String::new(),
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn preserves_events_and_fences_expired_leases() {
        let store = MemoryOutbox::new();
        let event = OutboxEvent::new("mail", b"hello".to_vec()).unwrap();
        store.append(event.clone()).await.unwrap();
        store.append(event.clone()).await.unwrap();
        let first = store
            .acquire("one", 1, Duration::from_millis(5))
            .await
            .unwrap()
            .remove(0);
        tokio::time::sleep(Duration::from_millis(8)).await;
        let second = store
            .acquire("two", 1, Duration::from_secs(1))
            .await
            .unwrap()
            .remove(0);
        assert!(matches!(
            store.ack(&first).await,
            Err(Error::OutboxLeaseLost)
        ));
        store
            .retry(&second, SystemTime::now(), "temporary")
            .await
            .unwrap();
        let retried = store
            .acquire("two", 1, Duration::from_secs(1))
            .await
            .unwrap()
            .remove(0);
        assert_eq!(retried.event.topic, "mail");
        assert_eq!(retried.event.payload, b"hello");
        assert_eq!(retried.attempt, 3);
    }

    #[tokio::test]
    async fn dead_letters_and_replays() {
        let store = MemoryOutbox::new();
        let event = OutboxEvent::new("reward", vec![7]).unwrap();
        store.append(event.clone()).await.unwrap();
        let delivery = store
            .acquire("worker", 1, Duration::from_secs(1))
            .await
            .unwrap()
            .remove(0);
        store.dead_letter(&delivery, "permanent").await.unwrap();
        assert_eq!(store.list_dead_letters(10).await.unwrap().len(), 1);
        store
            .replay_dead_letter(event.id, SystemTime::now())
            .await
            .unwrap();
        let replay = store
            .acquire("worker", 1, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(replay[0].attempt, 1);
        assert_eq!(replay[0].event.payload, vec![7]);
    }
}
