use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProtectionConfig {
    pub max_concurrent: usize,
    pub queue_timeout: Duration,
    pub failure_threshold: u32,
    pub open_timeout: Duration,
    pub half_open_max: usize,
}
impl Default for ProtectionConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 1024,
            queue_timeout: Duration::from_millis(10),
            failure_threshold: 5,
            open_timeout: Duration::from_secs(5),
            half_open_max: 1,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}
#[derive(Debug, Clone, Copy)]
pub struct ProtectionStats {
    pub active: i64,
    pub accepted: u64,
    pub rejected_concurrency: u64,
    pub rejected_circuit: u64,
    pub transient_failures: u64,
    pub opened: u64,
    pub circuit: CircuitState,
}
struct Circuit {
    state: CircuitState,
    fails: u32,
    until: Option<Instant>,
    half: usize,
}
pub struct BackendProtector {
    config: ProtectionConfig,
    slots: Arc<Semaphore>,
    circuit: Mutex<Circuit>,
    active: AtomicI64,
    accepted: AtomicU64,
    rejected_concurrency: AtomicU64,
    rejected_circuit: AtomicU64,
    failures: AtomicU64,
    opened: AtomicU64,
}
struct Permit<'a> {
    owner: &'a BackendProtector,
    _slot: OwnedSemaphorePermit,
    half: bool,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if self.half {
            let mut circuit = self
                .owner
                .circuit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            circuit.half = circuit.half.saturating_sub(1);
        }
        self.owner.active.fetch_sub(1, Ordering::Relaxed);
    }
}
impl BackendProtector {
    pub fn new(config: ProtectionConfig) -> Result<Self> {
        if config.max_concurrent == 0
            || config.failure_threshold == 0
            || config.open_timeout.is_zero()
            || config.half_open_max == 0
        {
            return Err(Error::InvalidConfig("backend protection".into()));
        }
        Ok(Self {
            slots: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            circuit: Mutex::new(Circuit {
                state: CircuitState::Closed,
                fails: 0,
                until: None,
                half: 0,
            }),
            active: AtomicI64::new(0),
            accepted: AtomicU64::new(0),
            rejected_concurrency: AtomicU64::new(0),
            rejected_circuit: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            opened: AtomicU64::new(0),
        })
    }
    async fn acquire(&self) -> Result<Permit<'_>> {
        let slot = if self.config.queue_timeout.is_zero() {
            self.slots.clone().try_acquire_owned().map_err(|_| {
                self.rejected_concurrency.fetch_add(1, Ordering::Relaxed);
                Error::QueueFull
            })?
        } else {
            timeout(
                self.config.queue_timeout,
                self.slots.clone().acquire_owned(),
            )
            .await
            .map_err(|_| {
                self.rejected_concurrency.fetch_add(1, Ordering::Relaxed);
                Error::QueueFull
            })?
            .map_err(|_| Error::Unavailable)?
        };
        self.active.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut c = self
            .circuit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if c.state == CircuitState::Open && c.until.is_some_and(|v| now >= v) {
            c.state = CircuitState::HalfOpen;
            c.half = 0
        }
        let half = if c.state == CircuitState::HalfOpen && c.half < self.config.half_open_max {
            c.half += 1;
            true
        } else {
            false
        };
        let allowed = c.state == CircuitState::Closed || half;
        drop(c);
        if !allowed {
            self.active.fetch_sub(1, Ordering::Relaxed);
            self.rejected_circuit.fetch_add(1, Ordering::Relaxed);
            drop(slot);
            return Err(Error::Unavailable);
        }
        self.accepted.fetch_add(1, Ordering::Relaxed);
        Ok(Permit {
            owner: self,
            _slot: slot,
            half,
        })
    }
    pub async fn execute<T, F, U, P>(&self, f: F, transient: P) -> Result<T>
    where
        F: FnOnce() -> U,
        U: Future<Output = Result<T>>,
        P: FnOnce(&Error) -> bool,
    {
        let permit = self.acquire().await?;
        let result = f().await;
        let failed = result.as_ref().err().is_some_and(transient);
        self.complete(&permit, failed);
        result
    }
    fn complete(&self, p: &Permit<'_>, failed: bool) {
        let mut c = self
            .circuit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
            c.fails += 1;
            if p.half || c.fails >= self.config.failure_threshold {
                c.state = CircuitState::Open;
                c.until = Some(Instant::now() + self.config.open_timeout);
                c.fails = 0;
                c.half = 0;
                self.opened.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            c.fails = 0;
            if c.state == CircuitState::HalfOpen {
                c.state = CircuitState::Closed;
                c.until = None;
                c.half = 0
            }
        }
    }
    pub async fn stats(&self) -> ProtectionStats {
        ProtectionStats {
            active: self.active.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected_concurrency: self.rejected_concurrency.load(Ordering::Relaxed),
            rejected_circuit: self.rejected_circuit.load(Ordering::Relaxed),
            transient_failures: self.failures.load(Ordering::Relaxed),
            opened: self.opened.load(Ordering::Relaxed),
            circuit: self
                .circuit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .state,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn opens_and_recovers() {
        let p = BackendProtector::new(ProtectionConfig {
            failure_threshold: 1,
            open_timeout: Duration::from_millis(1),
            ..Default::default()
        })
        .unwrap();
        assert!(
            p.execute(|| async { Err::<(), _>(Error::Unavailable) }, |_| true)
                .await
                .is_err()
        );
        assert_eq!(p.stats().await.circuit, CircuitState::Open);
        tokio::time::sleep(Duration::from_millis(2)).await;
        p.execute(|| async { Ok::<_, Error>(()) }, |_| true)
            .await
            .unwrap();
        assert_eq!(p.stats().await.circuit, CircuitState::Closed)
    }

    #[tokio::test]
    async fn cancellation_releases_active_capacity() {
        let protector = Arc::new(BackendProtector::new(ProtectionConfig::default()).unwrap());
        let task = tokio::spawn({
            let protector = protector.clone();
            async move {
                protector
                    .execute(std::future::pending::<Result<()>>, |_| true)
                    .await
            }
        });
        while protector.stats().await.active == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(protector.stats().await.active, 0);
    }
}
