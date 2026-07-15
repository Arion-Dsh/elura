use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use elura_core::Result;
use elura_core::session::PlayerKey;
use tokio::sync::Mutex;

const EXECUTOR_SHARDS: usize = 64;
const LOCKS_PER_SHARD: usize = 1024;
const LOCK_IDLE_TTL: Duration = Duration::from_secs(60);
const PRUNE_INTERVAL: u64 = 256;

struct CachedLock {
    lock: Arc<Mutex<()>>,
    last_seen: Instant,
}

struct LockShard {
    entries: HashMap<PlayerKey, CachedLock>,
    operations: u64,
}

pub struct KeyedExecutor {
    shards: Box<[StdMutex<LockShard>]>,
}

impl Default for KeyedExecutor {
    fn default() -> Self {
        Self {
            shards: (0..EXECUTOR_SHARDS)
                .map(|_| {
                    StdMutex::new(LockShard {
                        entries: HashMap::new(),
                        operations: 0,
                    })
                })
                .collect(),
        }
    }
}

impl KeyedExecutor {
    pub async fn execute<F, T>(&self, key: PlayerKey, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let lock = {
            let mut shard = self.shards[self.shard_index(key)]
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let now = Instant::now();
            shard.operations = shard.operations.wrapping_add(1);
            if shard.operations.is_multiple_of(PRUNE_INTERVAL) {
                shard.entries.retain(|_, entry| {
                    Arc::strong_count(&entry.lock) > 1
                        || now.duration_since(entry.last_seen) < LOCK_IDLE_TTL
                });
            }
            if let Some(entry) = shard.entries.get_mut(&key) {
                entry.last_seen = now;
                entry.lock.clone()
            } else {
                if shard.entries.len() >= LOCKS_PER_SHARD {
                    evict_oldest_unlocked(&mut shard.entries);
                }
                let lock = Arc::new(Mutex::new(()));
                shard.entries.insert(
                    key,
                    CachedLock {
                        lock: lock.clone(),
                        last_seen: now,
                    },
                );
                lock
            }
        };
        let _guard = lock.lock().await;
        operation.await
    }

    fn shard_index(&self, key: PlayerKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }
}

fn evict_oldest_unlocked(entries: &mut HashMap<PlayerKey, CachedLock>) {
    let oldest = entries
        .iter()
        .filter(|(_, entry)| Arc::strong_count(&entry.lock) == 1)
        .min_by_key(|(_, entry)| entry.last_seen)
        .map(|(key, _)| *key);
    if let Some(key) = oldest {
        entries.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_keeps_a_reusable_player_lock() {
        let executor = KeyedExecutor::default();
        let player = PlayerKey::new(1, 1, 1).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(1),
            executor.execute(player, std::future::pending::<Result<()>>()),
        )
        .await;
        assert!(result.is_err());
        let shard = executor.shard_index(player);
        let cached = executor.shards[shard]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(&player)
            .unwrap()
            .lock
            .clone();

        executor.execute(player, async { Ok(()) }).await.unwrap();
        let reused = executor.shards[shard]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(&player)
            .unwrap()
            .lock
            .clone();
        assert!(Arc::ptr_eq(&cached, &reused));
    }
}
