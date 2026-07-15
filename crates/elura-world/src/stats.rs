use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use elura_core::Error;
use serde::Serialize;

pub(crate) struct WorldStats {
    started_at: SystemTime,
    started: Instant,
    commands: AtomicU64,
    active: AtomicI64,
    succeeded: AtomicU64,
    business_failures: AtomicU64,
    internal_failures: AtomicU64,
    timeouts: AtomicU64,
    panics: AtomicU64,
    duration_nanos: AtomicU64,
    latency_buckets: [AtomicU64; 6],
}

pub(crate) struct CommandTimer<'a> {
    stats: &'a WorldStats,
    started: Instant,
}

impl Drop for CommandTimer<'_> {
    fn drop(&mut self) {
        self.stats.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorldStatsSnapshot {
    pub started_at: SystemTime,
    pub uptime_millis: u64,
    pub commands: u64,
    pub active_commands: i64,
    pub succeeded: u64,
    pub business_failures: u64,
    pub internal_failures: u64,
    pub timeouts: u64,
    pub panics: u64,
    pub duration_nanos: u64,
    pub latency_buckets: [u64; 6],
}

impl Default for WorldStats {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now(),
            started: Instant::now(),
            commands: AtomicU64::new(0),
            active: AtomicI64::new(0),
            succeeded: AtomicU64::new(0),
            business_failures: AtomicU64::new(0),
            internal_failures: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            duration_nanos: AtomicU64::new(0),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl WorldStats {
    pub fn begin(&self) -> CommandTimer<'_> {
        self.commands.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        CommandTimer {
            stats: self,
            started: Instant::now(),
        }
    }

    pub fn panic(&self) {
        self.panics.fetch_add(1, Ordering::Relaxed);
    }

    pub fn finish<T>(&self, timer: CommandTimer<'_>, result: &Result<T, Error>) {
        let elapsed = timer.started.elapsed();
        self.duration_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        let limits = [
            Duration::from_millis(1),
            Duration::from_millis(5),
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(100),
        ];
        let bucket = limits
            .iter()
            .position(|limit| elapsed <= *limit)
            .unwrap_or(limits.len());
        self.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        match result {
            Ok(_) => {
                self.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            Err(Error::Business { .. }) => {
                self.business_failures.fetch_add(1, Ordering::Relaxed);
            }
            Err(Error::Timeout) => {
                self.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.internal_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> WorldStatsSnapshot {
        WorldStatsSnapshot {
            started_at: self.started_at,
            uptime_millis: self.started.elapsed().as_millis() as u64,
            commands: self.commands.load(Ordering::Relaxed),
            active_commands: self.active.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            business_failures: self.business_failures.load(Ordering::Relaxed),
            internal_failures: self.internal_failures.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            duration_nanos: self.duration_nanos.load(Ordering::Relaxed),
            latency_buckets: std::array::from_fn(|index| {
                self.latency_buckets[index].load(Ordering::Relaxed)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_command_timer_releases_active_count() {
        let stats = WorldStats::default();
        let timer = stats.begin();
        assert_eq!(stats.snapshot().active_commands, 1);
        drop(timer);
        assert_eq!(stats.snapshot().active_commands, 0);
    }
}
