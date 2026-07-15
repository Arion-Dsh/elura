use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use super::registry::{IdentityProvider, VerifiedIdentity};
use crate::{ProviderError, ProviderResult};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Serialize, Deserialize)]
struct GuestClaims {
    subject: String,
    issued_at: u64,
    expires_at: u64,
    nonce: String,
}

pub struct GuestProvider {
    secrets: Vec<Vec<u8>>,
}

impl GuestProvider {
    pub fn new(secret: impl Into<Vec<u8>>) -> ProviderResult<Self> {
        Self::new_rotating(secret, std::iter::empty::<Vec<u8>>())
    }

    pub fn new_rotating(
        primary: impl Into<Vec<u8>>,
        previous: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> ProviderResult<Self> {
        let mut secrets = vec![primary.into()];
        secrets.extend(previous.into_iter().map(Into::into));
        if secrets.len() > 16 || secrets.iter().any(|secret| secret.len() < 32) {
            return Err(ProviderError::Config(
                "guest secrets must be >=32 bytes and at most 16 keys".into(),
            ));
        }
        Ok(Self { secrets })
    }

    pub fn issue(&self, subject: &str, ttl: Duration) -> ProviderResult<String> {
        let subject = subject.trim();
        if subject.is_empty()
            || subject.len() > 256
            || ttl.is_zero()
            || ttl > Duration::from_secs(86_400)
        {
            return Err(ProviderError::Config("invalid guest subject or ttl".into()));
        }
        let now = unix_time()?;
        let mut nonce = [0_u8; 16];
        rand::rng().fill_bytes(&mut nonce);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&GuestClaims {
                subject: subject.into(),
                issued_at: now,
                expires_at: now + ttl.as_secs(),
                nonce: URL_SAFE_NO_PAD.encode(nonce),
            })
            .map_err(|_| ProviderError::Unavailable)?,
        );
        Ok(format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(sign(&self.secrets[0], payload.as_bytes())?)
        ))
    }

    fn verify(&self, token: &str) -> ProviderResult<String> {
        if token.len() > 8192 {
            return Err(ProviderError::InvalidCredentials);
        }
        let (payload, signature) = token
            .split_once('.')
            .ok_or(ProviderError::InvalidCredentials)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| ProviderError::InvalidCredentials)?;
        let mut valid = 0_u8;
        for secret in &self.secrets {
            valid |= signature
                .ct_eq(sign(secret, payload.as_bytes())?.as_slice())
                .unwrap_u8();
        }
        if valid != 1 {
            return Err(ProviderError::InvalidCredentials);
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| ProviderError::InvalidCredentials)?;
        if decoded.len() > 4096 {
            return Err(ProviderError::InvalidCredentials);
        }
        let claims: GuestClaims =
            serde_json::from_slice(&decoded).map_err(|_| ProviderError::InvalidCredentials)?;
        let now = unix_time()?;
        if claims.subject.trim().is_empty()
            || claims.subject.len() > 256
            || claims.nonce.is_empty()
            || claims.issued_at > now
            || claims.expires_at <= now
        {
            return Err(ProviderError::InvalidCredentials);
        }
        Ok(claims.subject.trim().into())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GuestCredential {
    token: String,
}

#[async_trait]
impl IdentityProvider for GuestProvider {
    fn name(&self) -> &str {
        "guest"
    }
    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        let credential: GuestCredential =
            serde_json::from_value(credential).map_err(|_| ProviderError::InvalidCredentials)?;
        Ok(VerifiedIdentity {
            provider: self.name().into(),
            subject: self.verify(credential.token.trim())?,
            union_id: None,
            attributes: HashMap::new(),
        })
    }
}

fn sign(secret: &[u8], message: &[u8]) -> ProviderResult<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| ProviderError::Config("invalid guest secret".into()))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn unix_time() -> ProviderResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|_| ProviderError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rotation_accepts_previous_key_and_rejects_unknown_fields() {
        let old = GuestProvider::new([1_u8; 32]).unwrap();
        let rotating = GuestProvider::new_rotating([2_u8; 32], [[1_u8; 32]]).unwrap();
        let token = old.issue("device", Duration::from_secs(60)).unwrap();
        assert!(
            rotating
                .authenticate(serde_json::json!({"token": token}))
                .await
                .is_ok()
        );
        assert!(rotating.authenticate(serde_json::json!({"token": rotating.issue("device", Duration::from_secs(60)).unwrap(), "extra": true})).await.is_err());
    }
}
