//! External identity and application-account binding contracts.

#![deny(missing_docs)]

use std::{collections::HashMap, fmt, str::FromStr};

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Result;

/// Error returned when an identity-domain value violates its structural contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityValidationError(&'static str);

impl IdentityValidationError {
    fn new(message: &'static str) -> Self {
        Self(message)
    }
}

impl fmt::Display for IdentityValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for IdentityValidationError {}

/// Validated, normalized identifier for an external identity provider.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderName(String);

impl ProviderName {
    /// Parses a provider name after trimming whitespace and folding ASCII case.
    pub fn parse(value: impl AsRef<str>) -> std::result::Result<Self, IdentityValidationError> {
        let normalized = value.as_ref().trim().to_ascii_lowercase();
        if !(1..=32).contains(&normalized.len())
            || !normalized
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
            || !normalized.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
            })
        {
            return Err(IdentityValidationError::new("invalid provider name"));
        }
        Ok(Self(normalized))
    }

    /// Returns the normalized provider name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ProviderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ProviderName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ProviderName {
    type Err = IdentityValidationError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for ProviderName {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProviderName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// Identity proven by an external identity provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedIdentity {
    /// Provider that established the identity.
    pub provider: ProviderName,
    /// Stable provider-local subject identifier.
    pub subject: String,
    /// Optional provider-wide identifier shared across applications.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub union_id: Option<String>,
    /// Bounded, non-secret identity metadata.
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl VerifiedIdentity {
    /// Validates identity field lengths and metadata bounds.
    pub fn validate(&self) -> std::result::Result<(), IdentityValidationError> {
        if self.subject.trim().is_empty()
            || self.subject.len() > 512
            || self
                .union_id
                .as_ref()
                .is_some_and(|value| value.len() > 512)
            || self.attributes.len() > 64
            || self
                .attributes
                .iter()
                .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 2048)
        {
            return Err(IdentityValidationError::new("invalid verified identity"));
        }
        Ok(())
    }
}

/// Application account resolved from an external identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    /// Positive application account identifier.
    pub account_id: i64,
    /// Positive account generation used for session invalidation.
    pub generation: u64,
}

impl Principal {
    /// Validates the account identifier and generation.
    pub fn validate(&self) -> std::result::Result<(), IdentityValidationError> {
        if self.account_id <= 0 || self.generation == 0 {
            Err(IdentityValidationError::new("invalid account principal"))
        } else {
            Ok(())
        }
    }
}

/// Application-owned persistence for external-identity/account bindings.
#[async_trait]
pub trait IdentityBindingStore: Send + Sync + 'static {
    /// Finds the account currently bound to an authenticated identity.
    async fn find_account(&self, identity: &VerifiedIdentity) -> Result<Option<Principal>>;

    /// Creates an account and its initial identity binding atomically.
    async fn create_account(&self, identity: VerifiedIdentity) -> Result<Principal>;

    /// Links an additional authenticated identity to an existing account.
    async fn link(&self, principal: Principal, identity: VerifiedIdentity) -> Result<()>;
}

/// Application-owned persistence used by username/password authentication.
///
/// Implementations are responsible for enforcing username uniqueness and for
/// atomically creating the application account together with its password
/// credential. Password hashes use the PHC string format; plaintext passwords
/// never cross this boundary.
#[async_trait]
pub trait PasswordCredentialStore: Send + Sync + 'static {
    /// Finds the PHC-formatted password hash for a normalized username.
    async fn find_password_hash(&self, username: &str) -> Result<Option<String>>;

    /// Atomically creates an account and stores its initial password hash.
    async fn create_password_account(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<Principal>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_are_normalized_and_validated() {
        assert_eq!(ProviderName::parse(" WeChat ").unwrap().as_str(), "wechat");
        assert!(ProviderName::parse("9invalid").is_err());
    }
}
