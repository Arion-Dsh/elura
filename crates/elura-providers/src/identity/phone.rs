use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use super::registry::{IdentityProvider, IdentityProviderCapabilities, VerifiedIdentity};
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PhoneCredential {
    phone: String,
    challenge_id: String,
    code: String,
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
            provider: "phone".into(),
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
            registration: false,
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
