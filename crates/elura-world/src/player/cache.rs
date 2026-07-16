use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use elura_core::session::PlayerKey;
use elura_core::{Error, Result};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Clone)]
pub struct PlayerSnapshot<T> {
    pub value: T,
    pub version: u64,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PlayerCacheConfig {
    pub ttl: Duration,
    pub max_entries: usize,
}

impl Default for PlayerCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            max_entries: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlayerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub evictions: u64,
}

struct Entry<T> {
    snapshot: PlayerSnapshot<T>,
    expires_at: Instant,
    access: u64,
}

pub struct PlayerCache<T> {
    config: PlayerCacheConfig,
    entries: RwLock<HashMap<PlayerKey, Entry<T>>>,
    order: Mutex<VecDeque<(PlayerKey, u64)>>,
    loads: RwLock<HashMap<PlayerKey, Weak<AsyncMutex<()>>>>,
    clock: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    load_count: AtomicU64,
    evictions: AtomicU64,
}

struct LoadGuard<'a> {
    loads: &'a RwLock<HashMap<PlayerKey, Weak<AsyncMutex<()>>>>,
    player: PlayerKey,
    lock: Arc<AsyncMutex<()>>,
}

impl Drop for LoadGuard<'_> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lock) != 1 {
            return;
        }
        let mut loads = self
            .loads
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if loads
            .get(&self.player)
            .is_some_and(|current| current.ptr_eq(&Arc::downgrade(&self.lock)))
        {
            loads.remove(&self.player);
        }
    }
}

impl<T> PlayerCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(config: PlayerCacheConfig) -> Result<Self> {
        if config.ttl.is_zero() || config.max_entries == 0 {
            return Err(Error::InvalidConfig("invalid player cache limits".into()));
        }
        Ok(Self {
            config,
            entries: RwLock::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            loads: RwLock::new(HashMap::new()),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            load_count: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    pub async fn load<F, Fut>(&self, player: PlayerKey, loader: F) -> Result<PlayerSnapshot<T>>
    where
        F: FnOnce(PlayerKey) -> Fut,
        Fut: std::future::Future<Output = Result<PlayerSnapshot<T>>>,
    {
        player.validate()?;
        if let Some(snapshot) = self.lookup(player) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(snapshot);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let lock = self.load_lock(player);
        let cleanup = LoadGuard {
            loads: &self.loads,
            player,
            lock,
        };
        let _guard = cleanup.lock.lock().await;
        if let Some(snapshot) = self.lookup(player) {
            return Ok(snapshot);
        }
        self.load_count.fetch_add(1, Ordering::Relaxed);
        let loaded = loader(player).await?;
        self.store(player, loaded.clone()).await?;
        Ok(loaded)
    }

    pub async fn store(&self, player: PlayerKey, snapshot: PlayerSnapshot<T>) -> Result<()> {
        player.validate()?;
        let now = Instant::now();
        let access = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if entries
            .get(&player)
            .is_some_and(|current| current.snapshot.version > snapshot.version)
        {
            return Err(Error::InvalidConfig("stale player snapshot".into()));
        }
        entries.insert(
            player,
            Entry {
                snapshot,
                expires_at: now + self.config.ttl,
                access,
            },
        );
        self.record_access(&entries, player, access);
        while entries.len() > self.config.max_entries {
            let candidate = self
                .order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front();
            let Some((oldest, access)) = candidate else {
                break;
            };
            if entries
                .get(&oldest)
                .is_some_and(|entry| entry.access == access)
            {
                entries.remove(&oldest);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    pub async fn invalidate(&self, player: PlayerKey, minimum_version: u64) -> bool {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if entries
            .get(&player)
            .is_some_and(|entry| minimum_version == 0 || entry.snapshot.version < minimum_version)
        {
            entries.remove(&player);
            true
        } else {
            false
        }
    }

    pub async fn len(&self) -> usize {
        let now = Instant::now();
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.retain(|_, entry| entry.expires_at > now);
        entries.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    pub fn stats(&self) -> PlayerCacheStats {
        PlayerCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            loads: self.load_count.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    fn lookup(&self, player: PlayerKey) -> Option<PlayerSnapshot<T>> {
        let now = Instant::now();
        let access = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if entries.get(&player)?.expires_at <= now {
            entries.remove(&player);
            return None;
        }
        let snapshot = {
            let entry = entries.get_mut(&player)?;
            entry.access = access;
            entry.snapshot.clone()
        };
        self.record_access(&entries, player, access);
        Some(snapshot)
    }

    fn load_lock(&self, player: PlayerKey) -> Arc<AsyncMutex<()>> {
        let mut loads = self
            .loads
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match loads.get(&player).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(AsyncMutex::new(()));
                loads.insert(player, Arc::downgrade(&lock));
                lock
            }
        }
    }

    fn record_access(
        &self,
        entries: &HashMap<PlayerKey, Entry<T>>,
        player: PlayerKey,
        access: u64,
    ) {
        let mut order = self
            .order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        order.push_back((player, access));
        if order.len() > self.config.max_entries.saturating_mul(4) {
            let mut current: Vec<_> = entries
                .iter()
                .map(|(player, entry)| (*player, entry.access))
                .collect();
            current.sort_unstable_by_key(|(_, access)| *access);
            *order = current.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::future::join_all;

    use super::*;

    fn player(realm_id: u32, user_id: i64) -> PlayerKey {
        PlayerKey::new(1, realm_id, user_id).unwrap()
    }

    #[tokio::test]
    async fn coalesces_loads_and_guards_versions() {
        let cache = Arc::new(
            PlayerCache::new(PlayerCacheConfig {
                ttl: Duration::from_secs(30),
                max_entries: 2,
            })
            .unwrap(),
        );
        let loads = Arc::new(AtomicUsize::new(0));
        let tasks = (0..16).map(|_| {
            let cache = cache.clone();
            let loads = loads.clone();
            tokio::spawn(async move {
                cache
                    .load(player(1, 7), move |_| async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(PlayerSnapshot {
                            value: String::from("player"),
                            version: 3,
                        })
                    })
                    .await
                    .unwrap()
            })
        });
        let snapshots = join_all(tasks).await;
        assert!(
            snapshots
                .into_iter()
                .all(|result| result.unwrap().version == 3)
        );
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(cache.stats().loads, 1);
        assert!(
            cache
                .store(
                    player(1, 7),
                    PlayerSnapshot {
                        value: String::from("stale"),
                        version: 2,
                    },
                )
                .await
                .is_err()
        );
        assert!(!cache.invalidate(player(1, 7), 3).await);
        assert!(cache.invalidate(player(1, 7), 4).await);
    }

    #[tokio::test]
    async fn expires_and_evicts_lru_entries() {
        let cache = PlayerCache::new(PlayerCacheConfig {
            ttl: Duration::from_millis(15),
            max_entries: 2,
        })
        .unwrap();
        for user_id in 1..=3 {
            cache
                .store(
                    player(1, user_id),
                    PlayerSnapshot {
                        value: user_id,
                        version: 1,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(cache.len().await, 2);
        assert_eq!(cache.stats().evictions, 1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn separates_equal_user_ids_from_different_realms() {
        let cache = PlayerCache::new(PlayerCacheConfig::default()).unwrap();
        cache
            .store(
                player(1, 42),
                PlayerSnapshot {
                    value: "realm-1",
                    version: 1,
                },
            )
            .await
            .unwrap();
        cache
            .store(
                player(2, 42),
                PlayerSnapshot {
                    value: "realm-2",
                    version: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(cache.lookup(player(1, 42)).unwrap().value, "realm-1");
        assert_eq!(cache.lookup(player(2, 42)).unwrap().value, "realm-2");
    }

    #[tokio::test]
    async fn cancelled_load_removes_unused_load_lock() {
        let cache = PlayerCache::<String>::new(PlayerCacheConfig::default()).unwrap();
        let player = player(1, 7);
        let result = tokio::time::timeout(
            Duration::from_millis(1),
            cache.load(player, |_| std::future::pending()),
        )
        .await;
        assert!(result.is_err());
        assert!(
            cache
                .loads
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }
}
