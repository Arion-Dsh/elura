use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use elura_core::Result;
use elura_core::outbox::{DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore};
use elura_runtime::outbox::{Dispatcher, DispatcherConfig, IdempotencyStore};
use uuid::Uuid;

struct ApplicationOutboxStore;

#[async_trait]
impl OutboxStore for ApplicationOutboxStore {
    async fn append(&self, _event: OutboxEvent) -> Result<()> {
        Ok(())
    }

    async fn acquire(
        &self,
        _worker: &str,
        _limit: usize,
        _lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        Ok(Vec::new())
    }

    async fn renew(&self, _delivery: &OutboxDelivery, _lease: Duration) -> Result<()> {
        Ok(())
    }

    async fn ack(&self, _delivery: &OutboxDelivery) -> Result<()> {
        Ok(())
    }

    async fn retry(
        &self,
        _delivery: &OutboxDelivery,
        _available_at: SystemTime,
        _reason: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn dead_letter(&self, _delivery: &OutboxDelivery, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn list_dead_letters(&self, _limit: usize) -> Result<Vec<DeadLetter>> {
        Ok(Vec::new())
    }

    async fn replay_dead_letter(&self, _id: Uuid, _available_at: SystemTime) -> Result<()> {
        Ok(())
    }
}

struct ApplicationIdempotencyStore;

#[async_trait]
impl IdempotencyStore for ApplicationIdempotencyStore {
    async fn seen(&self, _id: Uuid) -> Result<bool> {
        Ok(false)
    }

    async fn mark(&self, _id: Uuid, _expires_at: SystemTime) -> Result<()> {
        Ok(())
    }
}

#[test]
fn application_can_inject_its_own_outbox_adapters() {
    let mut config = DispatcherConfig::default();
    config.idempotency = Some(Arc::new(ApplicationIdempotencyStore));
    let _dispatcher = Dispatcher::new(
        Arc::new(ApplicationOutboxStore),
        Arc::new(|_event| async { Ok(()) }),
        config,
    )
    .unwrap();
}
