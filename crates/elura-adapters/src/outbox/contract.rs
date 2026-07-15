use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub topic: String,
    pub key: String,
    pub trace_id: String,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub created_at: SystemTime,
    pub available_at: SystemTime,
}

impl OutboxEvent {
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Result<Self> {
        Self::with_id(Uuid::new_v4(), topic, payload)
    }

    pub fn with_id(
        id: Uuid,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        let now = SystemTime::now();
        let event = Self {
            id,
            topic: topic.into(),
            key: String::new(),
            trace_id: String::new(),
            payload: payload.into(),
            headers: BTreeMap::new(),
            created_at: now,
            available_at: now,
        };
        event.validate()?;
        Ok(event)
    }

    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil()
            || self.topic.trim().is_empty()
            || self.created_at > self.available_at
            || self.topic.len() > 255
            || self.key.len() > 512
            || self.trace_id.len() > 128
            || self.payload.len() > 1024 * 1024
            || self.headers.len() > 64
            || self
                .headers
                .iter()
                .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 1024)
        {
            return Err(Error::InvalidConfig("invalid outbox event".into()));
        }
        Ok(())
    }

    pub fn same_identity(&self, other: &Self) -> bool {
        self.id == other.id
            && self.topic == other.topic
            && self.key == other.key
            && self.trace_id == other.trace_id
            && self.payload == other.payload
            && self.headers == other.headers
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxDelivery {
    pub event: OutboxEvent,
    pub attempt: u32,
    pub worker: String,
    pub token: Uuid,
    pub lease_until: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub event: OutboxEvent,
    pub attempt: u32,
    pub reason: String,
    pub failed_at: SystemTime,
}

#[async_trait]
/// Durable at-least-once event storage with fenced delivery leases.
///
/// Event IDs are idempotency keys. Acquiring or renewing a delivery must issue
/// and validate its token atomically; stale tokens must never acknowledge,
/// retry, or dead-letter a delivery owned by another worker.
pub trait OutboxStore: Send + Sync + 'static {
    async fn append(&self, event: OutboxEvent) -> Result<()>;
    async fn acquire(
        &self,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>>;
    async fn renew(&self, delivery: &OutboxDelivery, lease: Duration) -> Result<()>;
    async fn ack(&self, delivery: &OutboxDelivery) -> Result<()>;
    async fn retry(
        &self,
        delivery: &OutboxDelivery,
        available_at: SystemTime,
        reason: &str,
    ) -> Result<()>;
    async fn dead_letter(&self, delivery: &OutboxDelivery, reason: &str) -> Result<()>;
    async fn list_dead_letters(&self, limit: usize) -> Result<Vec<DeadLetter>>;
    async fn replay_dead_letter(&self, id: Uuid, available_at: SystemTime) -> Result<()>;
}

pub(crate) fn validate_reason(reason: &str) -> Result<()> {
    if reason.len() > 4096 {
        Err(Error::InvalidConfig(
            "outbox failure reason exceeds limit".into(),
        ))
    } else {
        Ok(())
    }
}
