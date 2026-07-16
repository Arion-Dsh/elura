use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::Result;
use crate::otp::{OtpCreateResult, OtpRecord, OtpStore, OtpVerifyResult};
use async_trait::async_trait;
use subtle::ConstantTimeEq;

struct Entry {
    digest: Vec<u8>,
    expires_at: Instant,
    attempts: u32,
}

#[derive(Default)]
pub struct MemoryOtpStore {
    entries: Mutex<HashMap<(String, String), Entry>>,
    cooldowns: Mutex<HashMap<(String, String), Instant>>,
}

#[async_trait]
impl OtpStore for MemoryOtpStore {
    async fn create(
        &self,
        record: OtpRecord,
        ttl: Duration,
        cooldown: Duration,
    ) -> Result<OtpCreateResult> {
        record.validate()?;
        let now = Instant::now();
        let key = (record.subject_key, record.purpose);
        let mut cooldowns = self
            .cooldowns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cooldowns.retain(|_, expires_at| *expires_at > now);
        if cooldowns.contains_key(&key) {
            return Ok(OtpCreateResult::Cooldown);
        }
        cooldowns.insert(key.clone(), now + cooldown);
        drop(cooldowns);
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                key,
                Entry {
                    digest: record.code_digest,
                    expires_at: now + ttl,
                    attempts: 0,
                },
            );
        Ok(OtpCreateResult::Stored)
    }

    async fn verify_and_consume(
        &self,
        record: OtpRecord,
        max_attempts: u32,
    ) -> Result<OtpVerifyResult> {
        record.validate()?;
        let key = (record.subject_key, record.purpose);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = entries.get_mut(&key) else {
            return Ok(OtpVerifyResult::Missing);
        };
        if Instant::now() >= entry.expires_at {
            entries.remove(&key);
            return Ok(OtpVerifyResult::Missing);
        }
        if bool::from(entry.digest.ct_eq(&record.code_digest)) {
            entries.remove(&key);
            return Ok(OtpVerifyResult::Valid);
        }
        entry.attempts += 1;
        if entry.attempts >= max_attempts {
            entries.remove(&key);
        }
        Ok(OtpVerifyResult::Invalid)
    }

    async fn delete(&self, subject_key: &str, purpose: &str) -> Result<()> {
        let key = (subject_key.to_owned(), purpose.to_owned());
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key);
        self.cooldowns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key);
        Ok(())
    }
}
