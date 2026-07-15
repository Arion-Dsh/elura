use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elura_core::rate_limit::TokenBucket;
use elura_core::{Error, Result};

const RATE_LIMIT_SHARDS: usize = 64;
const RATE_LIMIT_IDLE_TTL: Duration = Duration::from_secs(300);
const PRUNE_INTERVAL: u64 = 256;

pub(crate) struct ConnectionLimiter {
    limit: usize,
    counts: Mutex<HashMap<IpAddr, usize>>,
}

impl ConnectionLimiter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            counts: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn try_enter(self: &Arc<Self>, ip: IpAddr) -> Result<ConnectionPermit> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| Error::Internal("connection limit lock poisoned".into()))?;
        let count = counts.entry(ip).or_default();
        if *count >= self.limit {
            return Err(Error::Unavailable);
        }
        *count += 1;
        Ok(ConnectionPermit {
            limiter: self.clone(),
            ip,
        })
    }
}

pub(crate) struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
    ip: IpAddr,
}

struct RateEntry {
    bucket: TokenBucket,
    last_seen: Instant,
}

struct RateShard<K> {
    entries: HashMap<K, RateEntry>,
    operations: u64,
}

/// A sharded process-local limiter for IP and authenticated-account keys.
pub(crate) struct KeyedRateLimiter<K> {
    rate: u32,
    burst: u32,
    shards: Vec<Mutex<RateShard<K>>>,
}

impl<K> KeyedRateLimiter<K>
where
    K: Eq + Hash,
{
    pub(crate) fn new(rate: u32, burst: u32) -> Self {
        Self {
            rate,
            burst,
            shards: (0..RATE_LIMIT_SHARDS)
                .map(|_| {
                    Mutex::new(RateShard {
                        entries: HashMap::new(),
                        operations: 0,
                    })
                })
                .collect(),
        }
    }

    pub(crate) fn allow(&self, key: K) -> bool {
        let now = Instant::now();
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.shards.len();
        let mut shard = self.shards[index]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.operations = shard.operations.wrapping_add(1);
        if shard.operations.is_multiple_of(PRUNE_INTERVAL) {
            shard
                .entries
                .retain(|_, entry| now.duration_since(entry.last_seen) < RATE_LIMIT_IDLE_TTL);
        }
        let entry = shard.entries.entry(key).or_insert_with(|| RateEntry {
            bucket: TokenBucket::new(self.rate, self.burst),
            last_seen: now,
        });
        entry.last_seen = now;
        entry.bucket.allow()
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let mut counts = self
            .limiter
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.ip);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn releases_per_ip_capacity_on_drop() {
        let limiter = Arc::new(ConnectionLimiter::new(1));
        let ip = "127.0.0.1".parse().unwrap();
        let permit = limiter.try_enter(ip).unwrap();
        assert!(limiter.try_enter(ip).is_err());
        drop(permit);
        assert!(limiter.try_enter(ip).is_ok());
    }

    #[test]
    fn keyed_rate_limit_is_isolated_by_key() {
        let limiter = KeyedRateLimiter::new(1, 1);
        assert!(limiter.allow("one"));
        assert!(!limiter.allow("one"));
        assert!(limiter.allow("two"));
    }
}
