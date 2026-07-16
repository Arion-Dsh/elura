#![cfg(feature = "otp")]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::Result;
use elura_core::otp::{OtpCreateResult, OtpRecord, OtpStore, OtpVerifyResult};
use elura_providers::ProviderResult;
use elura_providers::identity::OtpSender;
use elura_providers::otp::{OtpConfig, OtpService};

struct ApplicationOtpStore;

#[async_trait]
impl OtpStore for ApplicationOtpStore {
    async fn create(
        &self,
        _record: OtpRecord,
        _ttl: Duration,
        _cooldown: Duration,
    ) -> Result<OtpCreateResult> {
        Ok(OtpCreateResult::Stored)
    }

    async fn verify_and_consume(
        &self,
        _record: OtpRecord,
        _max_attempts: u32,
    ) -> Result<OtpVerifyResult> {
        Ok(OtpVerifyResult::Valid)
    }

    async fn delete(&self, _subject_key: &str, _purpose: &str) -> Result<()> {
        Ok(())
    }
}

struct ApplicationOtpSender;

#[async_trait]
impl OtpSender for ApplicationOtpSender {
    async fn send_code(&self, _phone: &str, _code: &str, _purpose: &str) -> ProviderResult<()> {
        Ok(())
    }
}

#[test]
fn application_can_inject_its_own_otp_store() {
    let _service = OtpService::new(
        OtpConfig::default(),
        vec![7; 32],
        Arc::new(ApplicationOtpSender),
        Arc::new(ApplicationOtpStore),
    )
    .unwrap();
}
