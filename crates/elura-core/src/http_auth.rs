//! Stateless HTTP access tokens and rotating refresh tokens.
//!
//! Access tokens are reusable until expiry and are therefore suitable for
//! independent HTTP requests that may reach different application instances.
//! Refresh tokens are single-use and reuse [`crate::ticket::ReplayStore`] for
//! distributed rotation and replay protection.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::identity::Principal;
use crate::ticket::ReplayStore;
use crate::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

const MAX_ACCESS_TTL: Duration = Duration::from_secs(60 * 60);
const MAX_REFRESH_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);
const MAX_SCOPES: usize = 64;
const MAX_SCOPE_BYTES: usize = 64;

/// Purpose assigned to an HTTP authentication token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HttpTokenPurpose {
    /// Reusable bearer token presented on HTTP business requests.
    Access,
    /// Single-use token consumed when rotating an HTTP login.
    Refresh,
}

/// Signed claims carried by an HTTP access or refresh token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpTokenClaims {
    /// Service that issued the token.
    pub issuer: String,
    /// HTTP API expected to accept the token.
    pub audience: String,
    /// Random identifier used for refresh-token replay protection.
    pub token_id: String,
    /// Unix timestamp at which the token was issued.
    pub issued_at: u64,
    /// Unix timestamp after which the token is invalid.
    pub expires_at: u64,
    /// Whether this is an access or refresh token.
    pub purpose: HttpTokenPurpose,
    /// Authenticated application account.
    pub principal: Principal,
    /// Sorted, deduplicated permissions granted to the token.
    pub scopes: Vec<String>,
}

impl HttpTokenClaims {
    /// Returns whether this token grants `scope`.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes
            .binary_search_by(|candidate| candidate.as_str().cmp(scope))
            .is_ok()
    }
}

/// Access and refresh credentials issued from one successful login.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpTokenPair {
    /// Reusable bearer token for HTTP business requests.
    pub access_token: String,
    /// Access-token lifetime in seconds.
    pub access_expires_in_seconds: u64,
    /// Single-use rotating token for renewing the HTTP login.
    pub refresh_token: String,
    /// Refresh-token lifetime in seconds.
    pub refresh_expires_in_seconds: u64,
}

/// HMAC-backed HTTP access and refresh token issuer.
///
/// The first key signs new tokens. Remaining keys only verify tokens, allowing
/// rolling key rotation without invalidating every active login immediately.
pub struct HttpTokenService {
    keys: Vec<Vec<u8>>,
    issuer: String,
    audience: String,
    access_ttl: Duration,
    refresh_ttl: Duration,
}

impl HttpTokenService {
    /// Creates a service with one signing key.
    pub fn new(
        key: impl Into<Vec<u8>>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        access_ttl: Duration,
        refresh_ttl: Duration,
    ) -> Result<Self> {
        Self::new_rotating(
            key,
            std::iter::empty::<Vec<u8>>(),
            issuer,
            audience,
            access_ttl,
            refresh_ttl,
        )
    }

    /// Creates a service with a primary signing key and verification-only old keys.
    pub fn new_rotating(
        primary_key: impl Into<Vec<u8>>,
        previous_keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        access_ttl: Duration,
        refresh_ttl: Duration,
    ) -> Result<Self> {
        let mut keys = vec![primary_key.into()];
        keys.extend(previous_keys.into_iter().map(Into::into));
        let issuer = issuer.into();
        let audience = audience.into();
        if keys.iter().any(|key| key.len() < 32)
            || keys.len() > 16
            || issuer.trim().is_empty()
            || audience.trim().is_empty()
            || access_ttl.is_zero()
            || access_ttl > MAX_ACCESS_TTL
            || refresh_ttl.is_zero()
            || refresh_ttl > MAX_REFRESH_TTL
            || refresh_ttl <= access_ttl
        {
            return Err(Error::InvalidConfig(
                "HTTP token keys must be >=32 bytes, contain at most 16 keys, names must be non-empty, and 0 < access TTL < refresh TTL within supported limits"
                    .into(),
            ));
        }
        Ok(Self {
            keys,
            issuer,
            audience,
            access_ttl,
            refresh_ttl,
        })
    }

    /// Issues reusable access and single-use refresh tokens.
    pub fn issue(
        &self,
        principal: Principal,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<HttpTokenPair> {
        principal.validate().map_err(|_| Error::Authentication)?;
        let scopes = normalize_scopes(scopes)?;
        Ok(HttpTokenPair {
            access_token: self.issue_for(
                principal,
                scopes.clone(),
                HttpTokenPurpose::Access,
                self.access_ttl,
            )?,
            access_expires_in_seconds: self.access_ttl.as_secs(),
            refresh_token: self.issue_for(
                principal,
                scopes,
                HttpTokenPurpose::Refresh,
                self.refresh_ttl,
            )?,
            refresh_expires_in_seconds: self.refresh_ttl.as_secs(),
        })
    }

    /// Validates a reusable access token without consuming it.
    pub fn verify_access(&self, token: &str) -> Result<HttpTokenClaims> {
        let claims = self.validate(token)?;
        if claims.purpose != HttpTokenPurpose::Access {
            return Err(Error::Authentication);
        }
        Ok(claims)
    }

    /// Atomically consumes a refresh token and returns a newly rotated pair.
    pub async fn rotate_refresh(
        &self,
        token: &str,
        replay: &dyn ReplayStore,
    ) -> Result<HttpTokenPair> {
        let claims = self.validate(token)?;
        if claims.purpose != HttpTokenPurpose::Refresh {
            return Err(Error::Authentication);
        }
        if !replay.reserve(&claims.token_id, claims.expires_at).await? {
            return Err(Error::TicketReplayed);
        }
        self.issue(claims.principal, claims.scopes)
    }

    /// Returns the configured access-token lifetime.
    pub const fn access_ttl(&self) -> Duration {
        self.access_ttl
    }

    /// Returns the configured refresh-token lifetime.
    pub const fn refresh_ttl(&self) -> Duration {
        self.refresh_ttl
    }

    fn issue_for(
        &self,
        principal: Principal,
        scopes: Vec<String>,
        purpose: HttpTokenPurpose,
        ttl: Duration,
    ) -> Result<String> {
        let now = unix_time()?;
        let mut nonce = [0_u8; 16];
        rand::rng().fill_bytes(&mut nonce);
        let claims = HttpTokenClaims {
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            token_id: URL_SAFE_NO_PAD.encode(nonce),
            issued_at: now,
            expires_at: now + ttl.as_secs(),
            purpose,
            principal,
            scopes,
        };
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signature = sign(&self.keys[0], payload.as_bytes())?;
        Ok(format!("{payload}.{}", URL_SAFE_NO_PAD.encode(signature)))
    }

    fn validate(&self, token: &str) -> Result<HttpTokenClaims> {
        let (payload, signature) = token.split_once('.').ok_or(Error::Authentication)?;
        if payload.len() > 8192 || signature.len() > 128 {
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
        let claims: HttpTokenClaims =
            serde_json::from_slice(&decoded).map_err(|_| Error::Authentication)?;
        let now = unix_time()?;
        let maximum_lifetime = match claims.purpose {
            HttpTokenPurpose::Access => self.access_ttl,
            HttpTokenPurpose::Refresh => self.refresh_ttl,
        }
        .as_secs();
        if claims.issuer != self.issuer
            || claims.audience != self.audience
            || claims.issued_at > now + 30
            || claims.expires_at <= claims.issued_at
            || claims.expires_at - claims.issued_at > maximum_lifetime
            || claims.expires_at <= now
        {
            return Err(Error::TicketExpired);
        }
        claims
            .principal
            .validate()
            .map_err(|_| Error::Authentication)?;
        validate_normalized_scopes(&claims.scopes)?;
        Ok(claims)
    }
}

fn normalize_scopes(scopes: impl IntoIterator<Item = impl Into<String>>) -> Result<Vec<String>> {
    let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
    scopes.sort_unstable();
    scopes.dedup();
    validate_normalized_scopes(&scopes)?;
    Ok(scopes)
}

fn validate_normalized_scopes(scopes: &[String]) -> Result<()> {
    if scopes.len() > MAX_SCOPES
        || scopes.windows(2).any(|pair| pair[0] >= pair[1])
        || scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > MAX_SCOPE_BYTES
                || !scope.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b':' | b'_' | b'-' | b'.' | b'*')
                })
        })
    {
        return Err(Error::Authentication);
    }
    Ok(())
}

fn sign(key: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| Error::InvalidConfig("invalid HTTP token key".into()))?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn unix_time() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Error::Internal("system clock is before unix epoch".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ticket::MemoryReplayStore;

    fn service() -> HttpTokenService {
        HttpTokenService::new(
            [7_u8; 32],
            "game-login",
            "game-http-api",
            Duration::from_secs(900),
            Duration::from_secs(30 * 24 * 60 * 60),
        )
        .unwrap()
    }

    fn principal() -> Principal {
        Principal {
            account_id: 42,
            generation: 3,
        }
    }

    #[test]
    fn access_tokens_are_reusable_and_scopes_are_normalized() {
        let service = service();
        let pair = service
            .issue(
                principal(),
                ["payments:write", "profile:read", "profile:read"],
            )
            .unwrap();
        let first = service.verify_access(&pair.access_token).unwrap();
        let second = service.verify_access(&pair.access_token).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.scopes,
            vec!["payments:write".to_owned(), "profile:read".to_owned()]
        );
        assert!(first.has_scope("payments:write"));
        assert!(!first.has_scope("admin"));
        assert!(matches!(
            service.verify_access(&pair.refresh_token),
            Err(Error::Authentication)
        ));
    }

    #[tokio::test]
    async fn refresh_tokens_rotate_once() {
        let service = service();
        let replay = MemoryReplayStore::default();
        let pair = service.issue(principal(), ["profile:read"]).unwrap();
        let rotated = service
            .rotate_refresh(&pair.refresh_token, &replay)
            .await
            .unwrap();
        assert_ne!(rotated.access_token, pair.access_token);
        assert!(service.verify_access(&rotated.access_token).is_ok());
        assert!(matches!(
            service.rotate_refresh(&pair.refresh_token, &replay).await,
            Err(Error::TicketReplayed)
        ));
    }

    #[test]
    fn rejects_tampering_and_wrong_audience() {
        let service = service();
        let pair = service.issue(principal(), ["profile:read"]).unwrap();
        let mut tampered = pair.access_token.into_bytes();
        tampered[4] ^= 1;
        assert!(matches!(
            service.verify_access(std::str::from_utf8(&tampered).unwrap()),
            Err(Error::Authentication)
        ));

        let other = HttpTokenService::new(
            [7_u8; 32],
            "game-login",
            "other-api",
            Duration::from_secs(900),
            Duration::from_secs(30 * 24 * 60 * 60),
        )
        .unwrap();
        let token = service
            .issue(principal(), ["profile:read"])
            .unwrap()
            .access_token;
        assert!(matches!(
            other.verify_access(&token),
            Err(Error::TicketExpired)
        ));
    }
}
