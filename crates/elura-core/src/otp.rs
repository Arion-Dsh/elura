use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtpRecord {
    pub subject_key: String,
    pub purpose: String,
    pub code_digest: Vec<u8>,
}

impl OtpRecord {
    pub fn validate(&self) -> Result<()> {
        if self.subject_key.is_empty()
            || self.subject_key.len() > 256
            || self.purpose.is_empty()
            || self.purpose.len() > 64
            || self.code_digest.len() < 16
            || self.code_digest.len() > 128
        {
            return Err(Error::InvalidConfig("invalid OTP record".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtpCreateResult {
    Stored,
    Cooldown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtpVerifyResult {
    Valid,
    Invalid,
    Missing,
}

#[async_trait]
/// Atomic OTP challenge storage.
///
/// `create` must install the challenge and cooldown as one logical operation.
/// `verify_and_consume` must atomically count failures and consume a successful
/// challenge so concurrent verification can succeed at most once.
pub trait OtpStore: Send + Sync {
    async fn create(
        &self,
        record: OtpRecord,
        ttl: Duration,
        cooldown: Duration,
    ) -> Result<OtpCreateResult>;
    async fn verify_and_consume(
        &self,
        record: OtpRecord,
        max_attempts: u32,
    ) -> Result<OtpVerifyResult>;
    async fn delete(&self, subject_key: &str, purpose: &str) -> Result<()>;
}
