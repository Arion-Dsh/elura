use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use elura_core::identity::{
    IdentityBindingStore, PasswordCredentialStore, Principal, ProviderName, VerifiedIdentity,
};

use crate::{ProviderError, ProviderResult};

/// Account-creation path supported by an identity provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IdentityRegistrationMode {
    /// The provider cannot create accounts.
    #[default]
    Unsupported,
    /// [`IdentityBindingStore`] creates the account from an authenticated identity.
    BindingStore,
    /// The provider owns an atomic provider-specific registration transaction.
    ProviderManaged,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Operations implemented by an identity provider.
pub struct IdentityProviderCapabilities {
    /// Whether the provider can authenticate a credential for account linking.
    pub link: bool,
    /// Account-creation path supported by the provider.
    pub registration: IdentityRegistrationMode,
}

/// Public provider metadata returned by [`IdentityRegistry::providers`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityProviderInfo {
    /// Stable normalized provider name.
    pub name: ProviderName,
    /// Operations implemented by the provider.
    pub capabilities: IdentityProviderCapabilities,
}

#[async_trait]
/// Object-safe boundary implemented by identity integrations.
pub trait IdentityProvider: Send + Sync {
    /// Stable lowercase provider name.
    fn name(&self) -> &str;
    /// Operations implemented by this provider.
    fn capabilities(&self) -> IdentityProviderCapabilities {
        IdentityProviderCapabilities::default()
    }
    /// Authenticates a provider-specific JSON credential.
    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity>;
    /// Authenticates a credential specifically for linking to an existing account.
    async fn authenticate_link(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        self.authenticate(credential).await
    }
    /// Registers a new account using a provider-specific credential.
    async fn register(&self, _credential: Value) -> ProviderResult<Principal> {
        Err(ProviderError::Unsupported)
    }
}

pub type IdentityProviderFactory =
    Arc<dyn Fn() -> ProviderResult<Arc<dyn IdentityProvider>> + Send + Sync>;

#[derive(Default)]
pub struct IdentityRegistry {
    providers: RwLock<BTreeMap<ProviderName, Arc<dyn IdentityProvider>>>,
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
            let name = ProviderName::parse(value.as_ref()).map_err(|error| {
                ProviderError::Config(format!("invalid enabled provider: {error}"))
            })?;
            let factory = factories
                .get(name.as_str())
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
        let raw_name = provider.name();
        let name = ProviderName::parse(raw_name)
            .map_err(|error| ProviderError::Config(format!("invalid provider name: {error}")))?;
        if name.as_str() != raw_name {
            return Err(ProviderError::Config(format!(
                "provider name must be normalized: {raw_name}"
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
        let name = ProviderName::parse(name)?;
        self.providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?
            .get(&name)
            .cloned()
            .ok_or_else(|| ProviderError::UnknownProvider(name.to_string()))
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
                    capabilities,
                }
            })
            .collect())
    }

    pub async fn authenticate<C: Serialize>(
        &self,
        name: &str,
        credential: C,
    ) -> ProviderResult<VerifiedIdentity> {
        let provider = self.provider(name)?;
        let credential = encode_credential(credential)?;
        normalize_identity(provider.name(), provider.authenticate(credential).await?)
    }
}

pub struct IdentityService {
    registry: Arc<IdentityRegistry>,
    bindings: Arc<dyn IdentityBindingStore>,
}

impl IdentityService {
    pub fn new(registry: Arc<IdentityRegistry>, bindings: Arc<dyn IdentityBindingStore>) -> Self {
        Self { registry, bindings }
    }

    pub async fn login<C: Serialize>(
        &self,
        provider_name: &str,
        credential: C,
    ) -> ProviderResult<Principal> {
        let identity = self
            .registry
            .authenticate(provider_name, credential)
            .await?;
        let principal = self
            .bindings
            .find_account(&identity)
            .await
            .map_err(binding_error)?
            .ok_or(ProviderError::InvalidCredentials)?;
        principal
            .validate()
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        Ok(principal)
    }

    pub async fn register<C: Serialize>(
        &self,
        provider_name: &str,
        credential: C,
    ) -> ProviderResult<Principal> {
        let provider = self.registry.provider(provider_name)?;
        let credential = encode_credential(credential)?;
        let principal = match provider.capabilities().registration {
            IdentityRegistrationMode::Unsupported => return Err(ProviderError::Unsupported),
            IdentityRegistrationMode::BindingStore => {
                let identity =
                    normalize_identity(provider.name(), provider.authenticate(credential).await?)?;
                self.bindings
                    .create_account(identity)
                    .await
                    .map_err(binding_error)?
            }
            IdentityRegistrationMode::ProviderManaged => provider.register(credential).await?,
        };
        principal
            .validate()
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        Ok(principal)
    }

    pub async fn link<C: Serialize>(
        &self,
        principal: Principal,
        provider_name: &str,
        credential: C,
    ) -> ProviderResult<()> {
        principal
            .validate()
            .map_err(|error| ProviderError::InvalidRequest(error.to_string()))?;
        let provider = self.registry.provider(provider_name)?;
        if !provider.capabilities().link {
            return Err(ProviderError::Unsupported);
        }
        let credential = encode_credential(credential)?;
        let identity = normalize_identity(
            provider.name(),
            provider.authenticate_link(credential).await?,
        )?;
        self.bindings
            .link(principal, identity)
            .await
            .map_err(binding_error)
    }
}

fn encode_credential<C: Serialize>(credential: C) -> ProviderResult<Value> {
    serde_json::to_value(credential)
        .map_err(|error| ProviderError::InvalidRequest(format!("invalid credential: {error}")))
}

fn normalize_identity(
    provider: &str,
    mut identity: VerifiedIdentity,
) -> ProviderResult<VerifiedIdentity> {
    identity.subject = identity.subject.trim().to_owned();
    identity.union_id = identity
        .union_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if identity.provider.as_str() != provider {
        return Err(ProviderError::InvalidResponse(
            "provider identity mismatch".into(),
        ));
    }
    identity
        .validate()
        .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
    Ok(identity)
}

fn binding_error(error: elura_core::Error) -> ProviderError {
    match error {
        elura_core::Error::Authentication => ProviderError::InvalidCredentials,
        elura_core::Error::RateLimited => ProviderError::RateLimited { retry_after: None },
        elura_core::Error::Unavailable | elura_core::Error::Timeout | elura_core::Error::Io(_) => {
            ProviderError::Unavailable
        }
        _ => ProviderError::Rejected("identity binding operation failed".into()),
    }
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
                registration: IdentityRegistrationMode::BindingStore,
            }
        }
        async fn authenticate(&self, _credential: Value) -> ProviderResult<VerifiedIdentity> {
            Ok(VerifiedIdentity {
                provider: ProviderName::parse("test")?,
                subject: "subject".into(),
                union_id: None,
                attributes: HashMap::new(),
            })
        }
    }

    struct NonLinkProvider;
    #[async_trait]
    impl IdentityProvider for NonLinkProvider {
        fn name(&self) -> &str {
            "non_link"
        }

        async fn authenticate(&self, _credential: Value) -> ProviderResult<VerifiedIdentity> {
            unreachable!("link capability must be checked before authentication")
        }
    }

    struct Accounts;
    #[async_trait]
    impl IdentityBindingStore for Accounts {
        async fn find_account(
            &self,
            _identity: &VerifiedIdentity,
        ) -> elura_core::Result<Option<Principal>> {
            unreachable!()
        }

        async fn create_account(
            &self,
            _identity: VerifiedIdentity,
        ) -> elura_core::Result<Principal> {
            Ok(Principal {
                account_id: 7,
                generation: 1,
            })
        }

        async fn link(
            &self,
            _principal: Principal,
            _identity: VerifiedIdentity,
        ) -> elura_core::Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn registry_reports_capabilities_and_authenticates() {
        let registry = IdentityRegistry::new();
        registry.register(Provider).unwrap();
        assert!(registry.providers().unwrap()[0].capabilities.link);
        assert_eq!(
            registry
                .authenticate(" TEST ", Value::Null)
                .await
                .unwrap()
                .subject,
            "subject"
        );
    }

    #[tokio::test]
    async fn service_enforces_link_capability() {
        let registry = Arc::new(IdentityRegistry::new());
        registry.register(NonLinkProvider).unwrap();
        let service = IdentityService::new(registry, Arc::new(Accounts));
        let result = service
            .link(
                Principal {
                    account_id: 1,
                    generation: 1,
                },
                "non_link",
                Value::Null,
            )
            .await;
        assert!(matches!(result, Err(ProviderError::Unsupported)));
    }

    #[tokio::test]
    async fn binding_store_registration_creates_account() {
        let registry = Arc::new(IdentityRegistry::new());
        registry.register(Provider).unwrap();
        let service = IdentityService::new(registry, Arc::new(Accounts));
        let principal = service.register("test", Value::Null).await.unwrap();
        assert_eq!(principal.account_id, 7);
    }
}
