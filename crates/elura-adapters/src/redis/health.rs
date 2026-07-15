use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use elura_core::{Error, Result};
use elura_runtime::observability::ReadinessProbe;
use serde::Serialize;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct SubscriptionStats {
    pub reconnects: u64,
    pub messages: u64,
    pub malformed_messages: u64,
}

#[derive(Default)]
pub(crate) struct SubscriptionCounters {
    reconnects: AtomicU64,
    messages: AtomicU64,
    malformed_messages: AtomicU64,
}

impl SubscriptionCounters {
    pub(crate) fn reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn message(&self) {
        self.messages.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn malformed(&self) {
        self.malformed_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> SubscriptionStats {
        SubscriptionStats {
            reconnects: self.reconnects.load(Ordering::Relaxed),
            messages: self.messages.load(Ordering::Relaxed),
            malformed_messages: self.malformed_messages.load(Ordering::Relaxed),
        }
    }
}

pub(crate) fn reconnect_delay(previous: Duration, was_active: bool) -> Duration {
    if previous.is_zero() || was_active {
        Duration::from_millis(100)
    } else {
        previous.saturating_mul(2).min(Duration::from_secs(5))
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct RedisHealthStats {
    pub ready: bool,
    pub checks: u64,
    pub failures: u64,
    pub last_success_unix: i64,
}

pub struct RedisHealth {
    client: redis::Client,
    ready: AtomicBool,
    checks: AtomicU64,
    failures: AtomicU64,
    last_success: AtomicI64,
}

impl RedisHealth {
    pub fn new(client: redis::Client) -> Arc<Self> {
        Arc::new(Self {
            client,
            ready: AtomicBool::new(false),
            checks: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            last_success: AtomicI64::new(0),
        })
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> RedisHealthStats {
        RedisHealthStats {
            ready: self.ready(),
            checks: self.checks.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            last_success_unix: self.last_success.load(Ordering::Relaxed),
        }
    }

    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
        interval: Duration,
        timeout: Duration,
    ) -> Result<()> {
        if interval.is_zero() || timeout.is_zero() {
            return Err(Error::InvalidConfig(
                "Redis health interval and timeout must be positive".into(),
            ));
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                self.ready.store(false, Ordering::Release);
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.ready.store(false, Ordering::Release);
                        return Ok(());
                    }
                }
                _ = ticker.tick() => self.probe(timeout).await,
            }
        }
    }

    async fn probe(&self, timeout: Duration) {
        self.checks.fetch_add(1, Ordering::Relaxed);
        let result = tokio::time::timeout(timeout, async {
            let mut connection = self.client.get_connection_manager().await?;
            redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await
        })
        .await;
        if matches!(result, Ok(Ok(ref pong)) if pong == "PONG") {
            self.ready.store(true, Ordering::Release);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs() as i64);
            self.last_success.store(now, Ordering::Relaxed);
        } else {
            self.ready.store(false, Ordering::Release);
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl ReadinessProbe for RedisHealth {
    async fn check(&self) -> Result<()> {
        if self.ready() {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_resets_after_activity_and_is_bounded() {
        assert_eq!(
            reconnect_delay(Duration::ZERO, false),
            Duration::from_millis(100)
        );
        assert_eq!(
            reconnect_delay(Duration::from_secs(5), false),
            Duration::from_secs(5)
        );
        assert_eq!(
            reconnect_delay(Duration::from_secs(3), true),
            Duration::from_millis(100)
        );
    }
}
