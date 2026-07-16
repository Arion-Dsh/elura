use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::registry::{
    IdentityProvider, IdentityProviderCapabilities, IdentityRegistrationMode, ProviderName,
    VerifiedIdentity,
};
use crate::{ProviderError, ProviderResult};

#[async_trait]
pub trait OtpVerifier: Send + Sync {
    async fn verify(
        &self,
        challenge_id: &str,
        phone: &str,
        code: &str,
        purpose: &str,
    ) -> ProviderResult<()>;
}

pub struct PhoneProvider<V> {
    verifier: V,
}

impl<V> PhoneProvider<V> {
    pub fn new(verifier: V) -> Self {
        Self { verifier }
    }
}

/// Phone number and one-time-code credential accepted by [`PhoneProvider`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhoneCredential {
    /// E.164 phone number.
    pub phone: String,
    /// Opaque challenge identifier returned by the OTP issuer.
    pub challenge_id: String,
    /// One-time verification code.
    pub code: String,
}

impl PhoneCredential {
    /// Creates a phone credential.
    pub fn new(
        phone: impl Into<String>,
        challenge_id: impl Into<String>,
        code: impl Into<String>,
    ) -> Self {
        Self {
            phone: phone.into(),
            challenge_id: challenge_id.into(),
            code: code.into(),
        }
    }
}

impl<V: OtpVerifier> PhoneProvider<V> {
    async fn verify(&self, credential: Value, purpose: &str) -> ProviderResult<VerifiedIdentity> {
        let credential: PhoneCredential =
            serde_json::from_value(credential).map_err(|_| ProviderError::InvalidCredentials)?;
        let phone = normalize_phone(&credential.phone)?;
        if credential.challenge_id.trim().is_empty()
            || credential.challenge_id.len() > 256
            || credential.code.trim().is_empty()
        {
            return Err(ProviderError::InvalidCredentials);
        }
        self.verifier
            .verify(
                credential.challenge_id.trim(),
                &phone,
                credential.code.trim(),
                purpose,
            )
            .await?;
        Ok(VerifiedIdentity {
            provider: ProviderName::parse("phone")?,
            subject: phone,
            union_id: None,
            attributes: HashMap::new(),
        })
    }
}

#[async_trait]
impl<V: OtpVerifier> IdentityProvider for PhoneProvider<V> {
    fn name(&self) -> &str {
        "phone"
    }
    fn capabilities(&self) -> IdentityProviderCapabilities {
        IdentityProviderCapabilities {
            link: true,
            registration: IdentityRegistrationMode::BindingStore,
        }
    }
    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        self.verify(credential, "login").await
    }
    async fn authenticate_link(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        self.verify(credential, "bind_phone").await
    }
}

fn normalize_phone(value: &str) -> ProviderResult<String> {
    let phone = value.trim().replace([' ', '-'], "");
    if !phone.starts_with('+')
        || !(8..=16).contains(&phone.len())
        || !phone[1..].bytes().all(|byte| byte.is_ascii_digit())
    {
        Err(ProviderError::InvalidCredentials)
    } else {
        Ok(phone)
    }
}
