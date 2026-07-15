use std::collections::HashMap;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Argon2, Params, Version};
use async_trait::async_trait;
use rand::RngCore;
use serde::Deserialize;
use serde_json::Value;

use super::registry::{
    IdentityProvider, IdentityProviderCapabilities, Principal, VerifiedIdentity,
};
use crate::{ProviderError, ProviderResult};

#[async_trait]
pub trait PasswordRepository: Send + Sync {
    async fn password_hash(&self, username: &str) -> ProviderResult<Option<String>>;
    async fn register_password(
        &self,
        username: &str,
        password_hash: &str,
    ) -> ProviderResult<Principal>;
}

pub struct PasswordProvider<R> {
    repository: R,
    dummy_hash: String,
    minimum_password: usize,
    maximum_password: usize,
}

impl<R: PasswordRepository> PasswordProvider<R> {
    pub fn new(repository: R) -> ProviderResult<Self> {
        Self::with_password_limits(repository, 8, 1024)
    }

    pub fn with_password_limits(
        repository: R,
        minimum: usize,
        maximum: usize,
    ) -> ProviderResult<Self> {
        if minimum == 0 || maximum < minimum || maximum > 1 << 20 {
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PasswordCredential {
    username: String,
    password: String,
}

#[async_trait]
impl<R: PasswordRepository> IdentityProvider for PasswordProvider<R> {
    fn name(&self) -> &str {
        "password"
    }
    fn capabilities(&self) -> IdentityProviderCapabilities {
        IdentityProviderCapabilities {
            link: false,
            registration: true,
        }
    }

    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        let (username, password) = self.decode(credential)?;
        let stored = self.repository.password_hash(&username).await?;
        let candidate = stored.as_deref().unwrap_or(&self.dummy_hash);
        let parsed = PasswordHash::new(candidate).map_err(|_| ProviderError::InvalidCredentials)?;
        let verified = Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
        if stored.is_none() || !verified {
            return Err(ProviderError::InvalidCredentials);
        }
        Ok(VerifiedIdentity {
            provider: self.name().into(),
            subject: username,
            union_id: None,
            attributes: HashMap::new(),
        })
    }

    async fn register(&self, credential: Value) -> ProviderResult<Principal> {
        let (username, password) = self.decode(credential)?;
        let hash = hash_password(&password)?;
        let principal = self.repository.register_password(&username, &hash).await?;
        principal.validate()?;
        Ok(principal)
    }
}

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
