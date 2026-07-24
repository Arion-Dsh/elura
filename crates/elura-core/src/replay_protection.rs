//! Cross-domain single-use replay protection.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::{Error, Result};

/// Atomically reserves replay-sensitive identifiers until expiration.
///
/// Exactly one concurrent caller for the same non-expired key must receive
/// `true`; every other caller receives `false`. Backend failures must return
/// an error and must never be interpreted as a successful reservation.
#[async_trait]
pub trait ReplayProtectionStore: Send + Sync {
    async fn reserve(&self, key: &str, expires_at: u64) -> Result<bool>;
}

/// In-memory replay protection for tests and single-process deployments.
#[derive(Default)]
pub struct MemoryReplayProtectionStore {
    used: Mutex<HashMap<String, u64>>,
}

#[async_trait]
impl ReplayProtectionStore for MemoryReplayProtectionStore {
    async fn reserve(&self, key: &str, expires_at: u64) -> Result<bool> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_secs();
        if expires_at <= now {
            return Ok(false);
        }
        let mut used = self
            .used
            .lock()
            .map_err(|_| Error::Internal("replay protection lock poisoned".into()))?;
        used.retain(|_, expires_at| *expires_at > now);
        if used.contains_key(key) {
            return Ok(false);
        }
        used.insert(key.to_owned(), expires_at);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_expired_and_duplicate_reservations() {
        let store = MemoryReplayProtectionStore::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(!store.reserve("expired", now).await.unwrap());
        assert!(store.reserve("fresh", now + 60).await.unwrap());
        assert!(!store.reserve("fresh", now + 60).await.unwrap());
    }
}
