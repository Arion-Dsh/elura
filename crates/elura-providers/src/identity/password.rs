#![deny(missing_docs)]

use std::collections::HashMap;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Argon2, Params, Version};
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::registry::{
    IdentityProvider, IdentityProviderCapabilities, IdentityRegistrationMode,
    PasswordCredentialStore, Principal, ProviderName, VerifiedIdentity,
};
use crate::{ProviderError, ProviderResult};

/// Username/password identity provider backed by application-owned storage.
pub struct PasswordProvider<R> {
    repository: R,
    dummy_hash: String,
    minimum_password: usize,
    maximum_password: usize,
}

impl<R: PasswordCredentialStore> PasswordProvider<R> {
    /// Creates a provider with the default password-length policy.
    pub fn new(repository: R) -> ProviderResult<Self> {
        Self::with_password_limits(repository, 8, 1024)
    }

    /// Creates a provider with an explicit inclusive password-length policy.
    pub fn with_password_limits(
        repository: R,
        minimum: usize,
        maximum: usize,
    ) -> ProviderResult<Self> {
        if minimum < 8 || maximum < minimum || maximum > 1024 {
            return Err(ProviderError::Config(
                "invalid password length policy".into(),
            ));
        }
        Ok(Self {
            repository,
            dummy_hash: hash_password("elura-dummy-password")?,
            minimum_password: minimum,
            maximum_password: maximum,
        })
    }

    fn decode(&self, credential: Value) -> ProviderResult<(String, String)> {
        let credential: PasswordCredential =
            serde_json::from_value(credential).map_err(|_| ProviderError::InvalidCredentials)?;
        let username = normalize_username(&credential.username)?;
        if !(self.minimum_password..=self.maximum_password).contains(&credential.password.len()) {
            return Err(ProviderError::InvalidCredentials);
        }
        Ok((username, credential.password))
    }
}

/// Username and password credential accepted by [`PasswordProvider`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordCredential {
    /// User-supplied account name.
    pub username: String,
    /// Plaintext password. It is consumed by the provider and never retained.
    pub password: String,
}

impl PasswordCredential {
    /// Creates a password credential.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

#[async_trait]
impl<R: PasswordCredentialStore> IdentityProvider for PasswordProvider<R> {
    fn name(&self) -> &str {
        "password"
    }
    fn capabilities(&self) -> IdentityProviderCapabilities {
        IdentityProviderCapabilities {
            link: false,
            registration: IdentityRegistrationMode::ProviderManaged,
        }
    }

    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        let (username, password) = self.decode(credential)?;
        let stored = self
            .repository
            .find_password_hash(&username)
            .await
            .map_err(credential_store_error)?;
        let candidate = stored.as_deref().unwrap_or(&self.dummy_hash);
        let parsed = PasswordHash::new(candidate).map_err(|_| ProviderError::InvalidCredentials)?;
        let verified = Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
        if stored.is_none() || !verified {
            return Err(ProviderError::InvalidCredentials);
        }
        Ok(VerifiedIdentity {
            provider: ProviderName::parse(self.name())?,
            subject: username,
            union_id: None,
            attributes: HashMap::new(),
        })
    }

    async fn register(&self, credential: Value) -> ProviderResult<Principal> {
        let (username, password) = self.decode(credential)?;
        let hash = hash_password(&password)?;
        let principal = self
            .repository
            .create_password_account(&username, &hash)
            .await
            .map_err(credential_store_error)?;
        principal
            .validate()
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        Ok(principal)
    }
}

fn credential_store_error(error: elura_core::Error) -> ProviderError {
    match error {
        elura_core::Error::Authentication => ProviderError::InvalidCredentials,
        elura_core::Error::RateLimited => ProviderError::RateLimited { retry_after: None },
        elura_core::Error::Unavailable | elura_core::Error::Timeout | elura_core::Error::Io(_) => {
            ProviderError::Unavailable
        }
        _ => ProviderError::Rejected("password credential store operation failed".into()),
    }
}

/// Hashes a password as an Argon2id PHC string suitable for persistent storage.
pub fn hash_password(password: &str) -> ProviderResult<String> {
    if !(8..=1024).contains(&password.len()) {
        return Err(ProviderError::Config(
            "password length must be 8..=1024 bytes".into(),
        ));
    }
    let params = Params::new(19_456, 2, 1, Some(32))
        .map_err(|error| ProviderError::Config(error.to_string()))?;
    let argon = Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params);
    let mut salt_bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|error| ProviderError::Config(error.to_string()))?;
    argon
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| ProviderError::Config(error.to_string()))
}

/// Trims and lowercases a username and validates its structural bounds.
pub fn normalize_username(value: &str) -> ProviderResult<String> {
    let normalized = value.trim().to_lowercase();
    if normalized.is_empty()
        || normalized.chars().count() > 64
        || normalized.chars().any(char::is_control)
    {
        Err(ProviderError::InvalidCredentials)
    } else {
        Ok(normalized)
    }
}
