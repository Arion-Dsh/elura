use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ProviderError, ProviderResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedIdentity {
    pub provider: String,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub union_id: Option<String>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl VerifiedIdentity {
    pub fn validate(&self) -> ProviderResult<()> {
        if !valid_provider_name(&self.provider)
            || self.subject.trim().is_empty()
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
            return Err(ProviderError::InvalidResponse(
                "invalid verified identity".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityProviderCapabilities {
    pub link: bool,
    pub registration: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityProviderInfo {
    pub name: String,
    pub supports_link: bool,
    pub supports_registration: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub account_id: i64,
    pub generation: u64,
}

impl Principal {
    pub fn validate(&self) -> ProviderResult<()> {
        if self.account_id <= 0 || self.generation == 0 {
            Err(ProviderError::InvalidResponse(
                "invalid account principal".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
pub trait IdentityProvider: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> IdentityProviderCapabilities {
        IdentityProviderCapabilities::default()
    }
    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity>;
    async fn authenticate_link(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        self.authenticate(credential).await
    }
    async fn register(&self, _credential: Value) -> ProviderResult<Principal> {
        Err(ProviderError::Unsupported)
    }
}

#[async_trait]
pub trait AccountStore: Send + Sync {
    async fn resolve(&self, identity: VerifiedIdentity) -> ProviderResult<Principal>;
    async fn link(&self, principal: Principal, identity: VerifiedIdentity) -> ProviderResult<()>;
}

pub type IdentityProviderFactory =
    Arc<dyn Fn() -> ProviderResult<Arc<dyn IdentityProvider>> + Send + Sync>;

#[derive(Default)]
pub struct IdentityRegistry {
    providers: RwLock<BTreeMap<String, Arc<dyn IdentityProvider>>>,
}

impl IdentityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(
        enabled: impl IntoIterator<Item = impl AsRef<str>>,
        factories: &HashMap<String, IdentityProviderFactory>,
    ) -> ProviderResult<Self> {
        let registry = Self::new();
        let mut count = 0;
        for value in enabled {
            count += 1;
            let name = normalize_provider_name(value.as_ref());
            if !valid_provider_name(&name) {
                return Err(ProviderError::Config(format!(
                    "invalid enabled provider {name}"
                )));
            }
            let factory = factories
                .get(&name)
                .ok_or_else(|| ProviderError::Config(format!("provider {name} has no factory")))?;
            registry.register_arc(factory()?)?;
        }
        if count == 0 {
            return Err(ProviderError::Config(
                "at least one identity provider is required".into(),
            ));
        }
        Ok(registry)
    }

    pub fn register(&self, provider: impl IdentityProvider + 'static) -> ProviderResult<()> {
        self.register_arc(Arc::new(provider))
    }

    pub fn register_arc(&self, provider: Arc<dyn IdentityProvider>) -> ProviderResult<()> {
        let name = provider.name().to_owned();
        if !valid_provider_name(&name) || normalize_provider_name(&name) != name {
            return Err(ProviderError::Config(format!(
                "invalid provider name {name}"
            )));
        }
        let mut providers = self
            .providers
            .write()
            .map_err(|_| ProviderError::Unavailable)?;
        if providers.contains_key(&name) {
            return Err(ProviderError::Config(format!("duplicate provider {name}")));
        }
        providers.insert(name, provider);
        Ok(())
    }

    pub fn provider(&self, name: &str) -> ProviderResult<Arc<dyn IdentityProvider>> {
        let name = normalize_provider_name(name);
        self.providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?
            .get(&name)
            .cloned()
            .ok_or(ProviderError::InvalidCredentials)
    }

    pub fn providers(&self) -> ProviderResult<Vec<IdentityProviderInfo>> {
        Ok(self
            .providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?
            .iter()
            .map(|(name, provider)| {
                let capabilities = provider.capabilities();
                IdentityProviderInfo {
                    name: name.clone(),
                    supports_link: capabilities.link,
                    supports_registration: capabilities.registration,
                }
            })
            .collect())
    }

    pub async fn authenticate(
        &self,
        name: &str,
        credential: Value,
    ) -> ProviderResult<VerifiedIdentity> {
        let provider = self.provider(name)?;
        normalize_identity(provider.name(), provider.authenticate(credential).await?)
    }
}

pub struct IdentityService {
    registry: Arc<IdentityRegistry>,
    accounts: Arc<dyn AccountStore>,
}

impl IdentityService {
    pub fn new(registry: Arc<IdentityRegistry>, accounts: Arc<dyn AccountStore>) -> Self {
        Self { registry, accounts }
    }

    pub async fn login(&self, provider_name: &str, credential: Value) -> ProviderResult<Principal> {
        let identity = self
            .registry
            .authenticate(provider_name, credential)
            .await?;
        let principal = self.accounts.resolve(identity).await?;
        principal.validate()?;
        Ok(principal)
    }

    pub async fn register(
        &self,
        provider_name: &str,
        credential: Value,
    ) -> ProviderResult<Principal> {
        let provider = self.registry.provider(provider_name)?;
        if !provider.capabilities().registration {
            return Err(ProviderError::Unsupported);
        }
        let principal = provider.register(credential).await?;
        principal.validate()?;
        Ok(principal)
    }

    pub async fn link(
        &self,
        principal: Principal,
        provider_name: &str,
        credential: Value,
    ) -> ProviderResult<()> {
        principal.validate()?;
        let provider = self.registry.provider(provider_name)?;
        let identity = normalize_identity(
            provider.name(),
            provider.authenticate_link(credential).await?,
        )?;
        self.accounts.link(principal, identity).await
    }
}

fn normalize_identity(
    provider: &str,
    mut identity: VerifiedIdentity,
) -> ProviderResult<VerifiedIdentity> {
    if identity.provider.is_empty() {
        identity.provider = provider.into();
    }
    identity.subject = identity.subject.trim().to_owned();
    identity.union_id = identity
        .union_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if identity.provider != provider {
        return Err(ProviderError::InvalidResponse(
            "provider identity mismatch".into(),
        ));
    }
    identity.validate()?;
    Ok(identity)
}

fn normalize_provider_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn valid_provider_name(value: &str) -> bool {
    (1..=32).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Provider;
    #[async_trait]
    impl IdentityProvider for Provider {
        fn name(&self) -> &str {
            "test"
        }
        fn capabilities(&self) -> IdentityProviderCapabilities {
            IdentityProviderCapabilities {
                link: true,
                registration: false,
            }
        }
        async fn authenticate(&self, _credential: Value) -> ProviderResult<VerifiedIdentity> {
            Ok(VerifiedIdentity {
                provider: "test".into(),
                subject: "subject".into(),
                union_id: None,
                attributes: HashMap::new(),
            })
        }
    }

    #[tokio::test]
    async fn registry_reports_capabilities_and_authenticates() {
        let registry = IdentityRegistry::new();
        registry.register(Provider).unwrap();
        assert!(registry.providers().unwrap()[0].supports_link);
        assert_eq!(
            registry
                .authenticate(" TEST ", Value::Null)
                .await
                .unwrap()
                .subject,
            "subject"
        );
    }
}
