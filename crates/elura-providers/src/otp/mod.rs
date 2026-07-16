//! One-time-password issuance and verification.

use crate::identity::{OtpSender, OtpVerifier};
use crate::{ProviderError, ProviderResult};
use async_trait::async_trait;
use elura_core::otp::{MemoryOtpStore, OtpCreateResult, OtpRecord, OtpStore, OtpVerifyResult};
use hmac::{Hmac, KeyInit, Mac};
use rand::RngExt;
use sha2::Sha256;
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
#[derive(Clone)]
#[non_exhaustive]
pub struct OtpConfig {
    pub digits: usize,
    pub ttl: Duration,
    pub cooldown: Duration,
    pub max_attempts: u32,
}
impl Default for OtpConfig {
    fn default() -> Self {
        Self {
            digits: 6,
            ttl: Duration::from_secs(300),
            cooldown: Duration::from_secs(60),
            max_attempts: 5,
        }
    }
}
pub struct OtpService {
    config: OtpConfig,
    secret: Vec<u8>,
    sender: Arc<dyn OtpSender>,
    store: Arc<dyn OtpStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtpChallenge {
    pub id: String,
    pub expires_in: Duration,
}
impl OtpService {
    pub fn new(
        config: OtpConfig,
        secret: Vec<u8>,
        sender: Arc<dyn OtpSender>,
        store: Arc<dyn OtpStore>,
    ) -> ProviderResult<Self> {
        if !(4..=10).contains(&config.digits)
            || config.ttl.is_zero()
            || config.cooldown.is_zero()
            || config.max_attempts == 0
            || secret.len() < 32
        {
            return Err(ProviderError::Config("invalid OTP config".into()));
        }
        Ok(Self {
            config,
            secret,
            sender,
            store,
        })
    }

    pub fn with_memory(
        config: OtpConfig,
        secret: Vec<u8>,
        sender: Arc<dyn OtpSender>,
    ) -> ProviderResult<Self> {
        Self::new(config, secret, sender, Arc::new(MemoryOtpStore::default()))
    }

    fn digest(&self, phone: &str, purpose: &str, code: &str) -> ProviderResult<Vec<u8>> {
        let mut m = Hmac::<Sha256>::new_from_slice(&self.secret)
            .map_err(|_| ProviderError::Config("OTP secret".into()))?;
        m.update(phone.as_bytes());
        m.update(&[0]);
        m.update(purpose.as_bytes());
        m.update(&[0]);
        m.update(code.as_bytes());
        Ok(m.finalize().into_bytes().to_vec())
    }

    fn subject_key(&self, phone: &str, purpose: &str) -> ProviderResult<String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .map_err(|_| ProviderError::Config("OTP secret".into()))?;
        mac.update(b"subject\0");
        mac.update(phone.as_bytes());
        mac.update(&[0]);
        mac.update(purpose.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    pub async fn issue(&self, phone: &str, purpose: &str) -> ProviderResult<OtpChallenge> {
        valid(phone, purpose)?;
        let upper = 10_u64.pow(self.config.digits as u32);
        let code = format!(
            "{:0width$}",
            rand::rng().random_range(0..upper),
            width = self.config.digits
        );
        let subject_key = self.subject_key(phone, purpose)?;
        let record = OtpRecord {
            subject_key: subject_key.clone(),
            purpose: purpose.to_owned(),
            code_digest: self.digest(phone, purpose, &code)?,
        };
        match self
            .store
            .create(record, self.config.ttl, self.config.cooldown)
            .await
            .map_err(store_error)?
        {
            OtpCreateResult::Stored => {}
            OtpCreateResult::Cooldown => {
                return Err(ProviderError::Rejected("OTP resend limited".into()));
            }
            _ => {
                return Err(ProviderError::InvalidResponse(
                    "unsupported OTP store create result".into(),
                ));
            }
        }
        if let Err(e) = self.sender.send_code(phone, &code, purpose).await {
            let _ = self.store.delete(&subject_key, purpose).await;
            return Err(e);
        }
        Ok(OtpChallenge {
            id: subject_key,
            expires_in: self.config.ttl,
        })
    }
}
#[async_trait]
impl OtpVerifier for OtpService {
    async fn verify(
        &self,
        challenge_id: &str,
        phone: &str,
        code: &str,
        purpose: &str,
    ) -> ProviderResult<()> {
        valid(phone, purpose)?;
        let subject_key = self.subject_key(phone, purpose)?;
        if challenge_id
            .as_bytes()
            .ct_eq(subject_key.as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(ProviderError::InvalidCredentials);
        }
        if code.len() != self.config.digits || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ProviderError::InvalidCredentials);
        }
        let result = self
            .store
            .verify_and_consume(
                OtpRecord {
                    subject_key,
                    purpose: purpose.to_owned(),
                    code_digest: self.digest(phone, purpose, code)?,
                },
                self.config.max_attempts,
            )
            .await
            .map_err(store_error)?;
        if result != OtpVerifyResult::Valid {
            return Err(ProviderError::InvalidCredentials);
        }
        Ok(())
    }
}

fn store_error(_: elura_core::Error) -> ProviderError {
    ProviderError::Unavailable
}
fn valid(phone: &str, purpose: &str) -> ProviderResult<()> {
    if !phone.starts_with('+')
        || phone.len() < 8
        || phone.len() > 16
        || !phone[1..].bytes().all(|b| b.is_ascii_digit())
        || purpose.is_empty()
    {
        Err(ProviderError::InvalidCredentials)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct RecordingSender(Mutex<Option<String>>);

    #[async_trait]
    impl OtpSender for RecordingSender {
        async fn send_code(&self, _phone: &str, code: &str, _purpose: &str) -> ProviderResult<()> {
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(code.to_owned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn store_backed_otp_is_single_use_and_enforces_cooldown() {
        let sender = Arc::new(RecordingSender::default());
        let service = OtpService::with_memory(
            OtpConfig {
                cooldown: Duration::from_secs(60),
                ..OtpConfig::default()
            },
            vec![7; 32],
            sender.clone(),
        )
        .unwrap();
        let challenge = service.issue("+8613800138000", "login").await.unwrap();
        let code = sender
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .unwrap();
        assert!(service.issue("+8613800138000", "login").await.is_err());
        service
            .verify(&challenge.id, "+8613800138000", &code, "login")
            .await
            .unwrap();
        assert!(
            service
                .verify(&challenge.id, "+8613800138000", &code, "login")
                .await
                .is_err()
        );
    }
}
