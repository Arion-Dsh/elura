use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use elura_core::{Error, Result};
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::watch;
use uuid::Uuid;

use super::{OutboxDelivery, OutboxEvent, OutboxStore};

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    async fn handle(&self, event: OutboxEvent) -> Result<()>;
}

#[async_trait]
impl<F, Fut> EventHandler for F
where
    F: Fn(OutboxEvent) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    async fn handle(&self, event: OutboxEvent) -> Result<()> {
        self(event).await
    }
}

#[async_trait]
/// Suppresses delivery of events that were already completed by this
/// dispatcher.
///
/// This check is not a business exactly-once boundary: handlers must still
/// make externally visible effects idempotent with their own stable business
/// key and transactional constraint.
pub trait IdempotencyStore: Send + Sync + 'static {
    async fn seen(&self, id: Uuid) -> Result<bool>;
    async fn mark(&self, id: Uuid, expires_at: SystemTime) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryIdempotencyStore {
    seen: Mutex<HashMap<Uuid, SystemTime>>,
}

#[async_trait]
impl IdempotencyStore for MemoryIdempotencyStore {
    async fn seen(&self, id: Uuid) -> Result<bool> {
        let mut seen = self
            .seen
            .lock()
            .map_err(|_| Error::Internal("idempotency lock poisoned".into()))?;
        let now = SystemTime::now();
        seen.retain(|_, expires| *expires > now);
        Ok(seen.contains_key(&id))
    }

    async fn mark(&self, id: Uuid, expires_at: SystemTime) -> Result<()> {
        self.seen
            .lock()
            .map_err(|_| Error::Internal("idempotency lock poisoned".into()))?
            .insert(id, expires_at);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DispatcherConfig {
    pub worker_id: String,
    pub batch_size: usize,
    pub lease: Duration,
    pub poll_interval: Duration,
    pub processing_timeout: Duration,
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub idempotency_ttl: Duration,
    pub idempotency: Option<Arc<dyn IdempotencyStore>>,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            worker_id: "outbox-worker".into(),
            batch_size: 32,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(500),
            processing_timeout: Duration::from_secs(30),
            max_attempts: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            idempotency_ttl: Duration::from_secs(3600),
            idempotency: None,
        }
    }
}

impl DispatcherConfig {
    fn validate(&self) -> Result<()> {
        if self.worker_id.trim().is_empty()
            || self.worker_id.len() > 128
            || self.batch_size == 0
            || self.batch_size > 4096
            || self.lease.is_zero()
            || self.poll_interval.is_zero()
            || self.processing_timeout.is_zero()
            || self.max_attempts == 0
            || self.initial_backoff.is_zero()
            || self.max_backoff < self.initial_backoff
            || self.idempotency_ttl.is_zero()
        {
            return Err(Error::InvalidConfig("invalid outbox dispatcher".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DispatcherStats {
    pub claimed: u64,
    pub completed: u64,
    pub retried: u64,
    pub dead_lettered: u64,
    pub duplicates: u64,
    pub failures: u64,
}

#[derive(Default)]
struct AtomicStats {
    claimed: AtomicU64,
    completed: AtomicU64,
    retried: AtomicU64,
    dead_lettered: AtomicU64,
    duplicates: AtomicU64,
    failures: AtomicU64,
}

pub struct Dispatcher {
    store: Arc<dyn OutboxStore>,
    handler: Arc<dyn EventHandler>,
    config: DispatcherConfig,
    stats: AtomicStats,
}

impl Dispatcher {
    pub fn new(
        store: Arc<dyn OutboxStore>,
        handler: Arc<dyn EventHandler>,
        config: DispatcherConfig,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            store,
            handler,
            config,
            stats: AtomicStats::default(),
        })
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let count = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    continue;
                }
                result = self.run_once() => result?,
            };
            if count == 0 {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                    _ = tokio::time::sleep(self.config.poll_interval) => {}
                }
            }
        }
    }

    pub async fn run_once(&self) -> Result<usize> {
        let deliveries = self
            .store
            .acquire(
                &self.config.worker_id,
                self.config.batch_size,
                self.config.lease,
            )
            .await?;
        let count = deliveries.len();
        let mut pending = FuturesUnordered::new();
        for delivery in deliveries {
            pending.push(self.process_delivery(delivery));
        }
        let mut first_error = None;
        while let Some(result) = pending.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(count), Err)
    }

    async fn process_delivery(&self, delivery: OutboxDelivery) -> Result<()> {
        let interval = (self.config.lease / 3).max(Duration::from_millis(1));
        let mut renewal = tokio::time::interval(interval);
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        renewal.tick().await;
        let process = self.finish_delivery(&delivery);
        tokio::pin!(process);
        loop {
            tokio::select! {
                result = &mut process => return result,
                _ = renewal.tick() => self.store.renew(&delivery, self.config.lease).await?,
            }
        }
    }

    async fn finish_delivery(&self, delivery: &OutboxDelivery) -> Result<()> {
        self.stats.claimed.fetch_add(1, Ordering::Relaxed);
        if let Some(idempotency) = &self.config.idempotency
            && idempotency.seen(delivery.event.id).await?
        {
            self.store.ack(delivery).await?;
            self.stats.duplicates.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let result = tokio::time::timeout(
            self.config.processing_timeout,
            self.handler.handle(delivery.event.clone()),
        )
        .await
        .unwrap_or(Err(Error::Timeout));
        let already_processed = matches!(&result, Err(Error::AlreadyProcessed));
        if result.is_ok() || already_processed {
            if let Some(idempotency) = &self.config.idempotency {
                idempotency
                    .mark(
                        delivery.event.id,
                        SystemTime::now() + self.config.idempotency_ttl,
                    )
                    .await?;
            }
            self.store.ack(delivery).await?;
            self.stats.completed.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let Err(error) = result else {
            return Ok(());
        };
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        if delivery.attempt >= self.config.max_attempts {
            self.store.dead_letter(delivery, &error.to_string()).await?;
            self.stats.dead_lettered.fetch_add(1, Ordering::Relaxed);
        } else {
            let available_at = SystemTime::now() + self.backoff(delivery.attempt);
            self.store
                .retry(delivery, available_at, &error.to_string())
                .await?;
            self.stats.retried.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let multiplier = 1_u32
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.config
            .initial_backoff
            .saturating_mul(multiplier)
            .min(self.config.max_backoff)
    }

    pub fn stats(&self) -> DispatcherStats {
        DispatcherStats {
            claimed: self.stats.claimed.load(Ordering::Relaxed),
            completed: self.stats.completed.load(Ordering::Relaxed),
            retried: self.stats.retried.load(Ordering::Relaxed),
            dead_lettered: self.stats.dead_lettered.load(Ordering::Relaxed),
            duplicates: self.stats.duplicates.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::outbox::MemoryOutbox;

    #[tokio::test]
    async fn retries_then_dead_letters_without_losing_payload() {
        let store = Arc::new(MemoryOutbox::new());
        let event = OutboxEvent::new("mail", b"body".to_vec()).unwrap();
        store.append(event).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = Dispatcher::new(
            store.clone(),
            Arc::new({
                let calls = calls.clone();
                move |event: OutboxEvent| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        assert_eq!(event.payload, b"body");
                        Err(Error::Unavailable)
                    }
                }
            }),
            DispatcherConfig {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                ..DispatcherConfig::default()
            },
        )
        .unwrap();
        assert_eq!(dispatcher.run_once().await.unwrap(), 1);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(dispatcher.run_once().await.unwrap(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(store.list_dead_letters(10).await.unwrap().len(), 1);
        assert_eq!(dispatcher.stats().dead_lettered, 1);
    }

    #[tokio::test]
    async fn idempotency_skips_duplicate_business_effect() {
        let store = Arc::new(MemoryOutbox::new());
        let event = OutboxEvent::new("reward", vec![1]).unwrap();
        store.append(event.clone()).await.unwrap();
        let idempotency = Arc::new(MemoryIdempotencyStore::default());
        idempotency
            .mark(event.id, SystemTime::now() + Duration::from_secs(60))
            .await
            .unwrap();
        let dispatcher = Dispatcher::new(
            store,
            Arc::new(|_: OutboxEvent| async { panic!("handler must be skipped") }),
            DispatcherConfig {
                idempotency: Some(idempotency),
                ..DispatcherConfig::default()
            },
        )
        .unwrap();
        dispatcher.run_once().await.unwrap();
        assert_eq!(dispatcher.stats().duplicates, 1);
    }

    #[tokio::test]
    async fn renews_lease_while_a_slow_handler_runs() {
        let store = Arc::new(MemoryOutbox::new());
        store
            .append(OutboxEvent::new("slow", vec![1]).unwrap())
            .await
            .unwrap();
        let dispatcher = Arc::new(
            Dispatcher::new(
                store.clone(),
                Arc::new(|_: OutboxEvent| async {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(())
                }),
                DispatcherConfig {
                    lease: Duration::from_millis(30),
                    ..DispatcherConfig::default()
                },
            )
            .unwrap(),
        );
        let task = tokio::spawn({
            let dispatcher = dispatcher.clone();
            async move { dispatcher.run_once().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            store
                .acquire("other", 1, Duration::from_secs(1))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }

    #[tokio::test]
    async fn times_out_a_stuck_handler_and_schedules_retry() {
        let store = Arc::new(MemoryOutbox::new());
        store
            .append(OutboxEvent::new("stuck", vec![1]).unwrap())
            .await
            .unwrap();
        let dispatcher = Dispatcher::new(
            store,
            Arc::new(|_: OutboxEvent| async { std::future::pending::<Result<()>>().await }),
            DispatcherConfig {
                processing_timeout: Duration::from_millis(10),
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                ..DispatcherConfig::default()
            },
        )
        .unwrap();
        assert_eq!(dispatcher.run_once().await.unwrap(), 1);
        assert_eq!(dispatcher.stats().retried, 1);
    }

    #[tokio::test]
    async fn shutdown_cancels_an_in_flight_batch() {
        let store = Arc::new(MemoryOutbox::new());
        store
            .append(OutboxEvent::new("stuck", vec![1]).unwrap())
            .await
            .unwrap();
        let dispatcher = Arc::new(
            Dispatcher::new(
                store,
                Arc::new(|_: OutboxEvent| async { std::future::pending::<Result<()>>().await }),
                DispatcherConfig {
                    processing_timeout: Duration::from_secs(60),
                    ..DispatcherConfig::default()
                },
            )
            .unwrap(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn({
            let dispatcher = dispatcher.clone();
            async move { dispatcher.run(shutdown_rx).await }
        });
        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
