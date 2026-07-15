use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::session::Identity;
use crate::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketClaims {
    pub issuer: String,
    pub audience: String,
    pub ticket_id: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub identity: Identity,
}

#[async_trait]
pub trait ReplayStore: Send + Sync {
    /// Atomically reserves a ticket until `expires_at`.
    ///
    /// Exactly one concurrent caller for the same non-expired ID must receive
    /// `true`; every other caller receives `false`. Backend failures must return
    /// an error and must never be interpreted as a successful reservation.
    async fn reserve(&self, ticket_id: &str, expires_at: u64) -> Result<bool>;
}

#[derive(Default)]
pub struct MemoryReplayStore {
    used: Mutex<HashSet<String>>,
}

#[async_trait]
impl ReplayStore for MemoryReplayStore {
    async fn reserve(&self, ticket_id: &str, _expires_at: u64) -> Result<bool> {
        let mut used = self
            .used
            .lock()
            .map_err(|_| Error::Internal("replay lock poisoned".into()))?;
        Ok(used.insert(ticket_id.to_owned()))
    }
}

pub struct TicketService {
    keys: Vec<Vec<u8>>,
    issuer: String,
    audience: String,
    ttl: Duration,
}

impl TicketService {
    pub fn new(
        key: impl Into<Vec<u8>>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        Self::new_rotating(key, std::iter::empty::<Vec<u8>>(), issuer, audience, ttl)
    }

    pub fn new_rotating(
        primary_key: impl Into<Vec<u8>>,
        previous_keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let mut keys = vec![primary_key.into()];
        keys.extend(previous_keys.into_iter().map(Into::into));
        if keys.iter().any(|key| key.len() < 32)
            || keys.len() > 16
            || ttl.is_zero()
            || ttl > Duration::from_secs(3600)
        {
            return Err(Error::InvalidConfig(
                "ticket keys must be >=32 bytes, contain at most 16 keys and ttl <=1h".into(),
            ));
        }
        Ok(Self {
            keys,
            issuer: issuer.into(),
            audience: audience.into(),
            ttl,
        })
    }

    pub fn issue(&self, identity: Identity) -> Result<String> {
        identity.validate()?;
        let now = unix_time()?;
        let mut nonce = [0_u8; 16];
        rand::rng().fill_bytes(&mut nonce);
        let claims = TicketClaims {
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            ticket_id: URL_SAFE_NO_PAD.encode(nonce),
            issued_at: now,
            expires_at: now + self.ttl.as_secs(),
            identity,
        };
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signature = sign(&self.keys[0], payload.as_bytes())?;
        Ok(format!("{payload}.{}", URL_SAFE_NO_PAD.encode(signature)))
    }

    pub async fn verify(&self, token: &str, replay: &dyn ReplayStore) -> Result<TicketClaims> {
        self.validate(token)?.consume(replay).await
    }

    pub fn validate(&self, token: &str) -> Result<VerifiedTicket> {
        let (payload, signature) = token.split_once('.').ok_or(Error::Authentication)?;
        if payload.len() > 4096 || signature.len() > 128 {
            return Err(Error::Authentication);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| Error::Authentication)?;
        let mut valid = 0_u8;
        for key in &self.keys {
            let expected = sign(key, payload.as_bytes())?;
            valid |= signature.ct_eq(expected.as_slice()).unwrap_u8();
        }
        if valid != 1 {
            return Err(Error::Authentication);
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| Error::Authentication)?;
        let claims: TicketClaims =
            serde_json::from_slice(&decoded).map_err(|_| Error::Authentication)?;
        let now = unix_time()?;
        if claims.issuer != self.issuer
            || claims.audience != self.audience
            || claims.issued_at > now + 30
            || claims.expires_at <= now
        {
            return Err(Error::TicketExpired);
        }
        claims.identity.validate()?;
        Ok(VerifiedTicket { claims })
    }
}

pub struct VerifiedTicket {
    claims: TicketClaims,
}

impl VerifiedTicket {
    pub const fn claims(&self) -> &TicketClaims {
        &self.claims
    }

    pub async fn consume(self, replay: &dyn ReplayStore) -> Result<TicketClaims> {
        if !replay
            .reserve(&self.claims.ticket_id, self.claims.expires_at)
            .await?
        {
            return Err(Error::TicketReplayed);
        }
        Ok(self.claims)
    }
}

fn sign(key: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| Error::InvalidConfig("invalid ticket key".into()))?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn unix_time() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Error::Internal("system clock is before epoch".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> Identity {
        Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        }
    }

    #[tokio::test]
    async fn ticket_is_single_use() {
        let service =
            TicketService::new([7_u8; 32], "auth", "gateway", Duration::from_secs(60)).unwrap();
        let replay = MemoryReplayStore::default();
        let token = service.issue(identity()).unwrap();
        assert_eq!(
            service.verify(&token, &replay).await.unwrap().identity,
            identity()
        );
        assert!(matches!(
            service.verify(&token, &replay).await,
            Err(Error::TicketReplayed)
        ));
    }

    #[tokio::test]
    async fn rotation_accepts_old_key_but_signs_with_primary() {
        let old =
            TicketService::new([1_u8; 32], "auth", "gateway", Duration::from_secs(60)).unwrap();
        let rotating = TicketService::new_rotating(
            [2_u8; 32],
            [[1_u8; 32]],
            "auth",
            "gateway",
            Duration::from_secs(60),
        )
        .unwrap();
        let primary_only =
            TicketService::new([2_u8; 32], "auth", "gateway", Duration::from_secs(60)).unwrap();
        assert!(rotating.validate(&old.issue(identity()).unwrap()).is_ok());
        assert!(
            primary_only
                .validate(&rotating.issue(identity()).unwrap())
                .is_ok()
        );
        assert!(old.validate(&rotating.issue(identity()).unwrap()).is_err());
    }

    #[tokio::test]
    async fn validation_failure_can_happen_before_consumption() {
        let service =
            TicketService::new([3_u8; 32], "auth", "gateway", Duration::from_secs(60)).unwrap();
        let replay = MemoryReplayStore::default();
        let token = service.issue(identity()).unwrap();
        let verified = service.validate(&token).unwrap();
        assert_eq!(verified.claims().identity, identity());
        drop(verified);
        assert!(service.verify(&token, &replay).await.is_ok());
    }
}
