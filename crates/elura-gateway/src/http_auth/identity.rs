//! Bridge from Elura identity providers to the HTTP authentication API.

use std::sync::Arc;

use async_trait::async_trait;
use elura_core::identity::Principal;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use elura_providers::ProviderError;
use elura_providers::identity::IdentityService;
use serde_json::Value;

use super::{GameSessionTicketRequest, HttpLoginBackend, HttpLoginGrant};

/// Application-owned authorization policy used after identity-provider login.
///
/// Provider authentication and account binding belong to
/// [`IdentityService`]. Applications retain control of permissions and the
/// mapping from an account to a selected game player.
#[async_trait]
pub trait IdentityHttpPolicy: Send + Sync + 'static {
    /// Returns the HTTP scopes granted after a successful provider login.
    ///
    /// `provider` is the successfully authenticated provider name. The
    /// credential itself is deliberately not exposed to this policy.
    async fn login_scopes(&self, principal: Principal, provider: &str) -> Result<Vec<String>>;

    /// Resolves and authorizes a player selection for one account.
    ///
    /// Implementations must verify that the selected player belongs to
    /// `principal` and may enter the requested region and realm.
    async fn game_identity(
        &self,
        principal: Principal,
        request: &GameSessionTicketRequest,
    ) -> Result<Identity>;
}

/// Framework-provided [`HttpLoginBackend`] backed by [`IdentityService`].
///
/// This adapter performs existing-account login exactly once. Registration
/// and account linking remain explicit application flows so one-time
/// third-party authorization codes are never retried through another identity
/// operation.
pub struct IdentityHttpBackend {
    identities: Arc<IdentityService>,
    policy: Arc<dyn IdentityHttpPolicy>,
}

impl IdentityHttpBackend {
    /// Creates an HTTP login backend from identity and authorization services.
    pub fn new(identities: Arc<IdentityService>, policy: Arc<dyn IdentityHttpPolicy>) -> Self {
        Self { identities, policy }
    }
}

#[async_trait]
impl HttpLoginBackend for IdentityHttpBackend {
    async fn login(&self, provider: &str, credential: Value) -> Result<HttpLoginGrant> {
        let principal = self
            .identities
            .login(provider, credential)
            .await
            .map_err(map_provider_error)?;
        let scopes = self.policy.login_scopes(principal, provider).await?;
        Ok(HttpLoginGrant::new(principal, scopes))
    }

    async fn game_identity(
        &self,
        principal: Principal,
        request: &GameSessionTicketRequest,
    ) -> Result<Identity> {
        self.policy.game_identity(principal, request).await
    }
}

fn map_provider_error(error: ProviderError) -> Error {
    match error {
        ProviderError::Unavailable => Error::Unavailable,
        ProviderError::RateLimited { .. } => Error::RateLimited,
        ProviderError::Config(_) | ProviderError::InvalidResponse(_) => {
            Error::Internal("identity provider operation failed".into())
        }
        ProviderError::InvalidRequest(_)
        | ProviderError::UnknownProvider(_)
        | ProviderError::InvalidCredentials
        | ProviderError::Rejected(_)
        | ProviderError::InvalidSignature
        | ProviderError::AlreadyProcessed
        | ProviderError::Unsupported => Error::Authentication,
        _ => Error::Internal("identity provider operation failed".into()),
    }
}

#[cfg(test)]
mod tests {
    use elura_core::identity::{IdentityBindingStore, ProviderName, VerifiedIdentity};
    use elura_providers::identity::{IdentityProvider, IdentityRegistry};

    use super::*;

    struct DemoProvider;

    #[async_trait]
    impl IdentityProvider for DemoProvider {
        fn name(&self) -> &str {
            "demo"
        }

        async fn authenticate(
            &self,
            credential: Value,
        ) -> elura_providers::ProviderResult<VerifiedIdentity> {
            if credential["secret"] != "valid" {
                return Err(ProviderError::InvalidCredentials);
            }
            Ok(VerifiedIdentity {
                provider: ProviderName::parse("demo").unwrap(),
                subject: "external-user".into(),
                union_id: None,
                attributes: Default::default(),
            })
        }
    }

    struct DemoBindings;

    #[async_trait]
    impl IdentityBindingStore for DemoBindings {
        async fn find_account(&self, identity: &VerifiedIdentity) -> Result<Option<Principal>> {
            Ok((identity.subject == "external-user").then_some(Principal {
                account_id: 17,
                generation: 4,
            }))
        }

        async fn create_account(&self, _identity: VerifiedIdentity) -> Result<Principal> {
            Err(Error::Internal("unexpected registration".into()))
        }

        async fn link(&self, _principal: Principal, _identity: VerifiedIdentity) -> Result<()> {
            Err(Error::Internal("unexpected linking".into()))
        }
    }

    struct DemoPolicy;

    #[async_trait]
    impl IdentityHttpPolicy for DemoPolicy {
        async fn login_scopes(&self, principal: Principal, provider: &str) -> Result<Vec<String>> {
            assert_eq!(principal.account_id, 17);
            assert_eq!(provider, "demo");
            Ok(vec!["game:connect".into(), "payments:write".into()])
        }

        async fn game_identity(
            &self,
            principal: Principal,
            request: &GameSessionTicketRequest,
        ) -> Result<Identity> {
            Ok(Identity {
                account_id: principal.account_id,
                user_id: request.user_id,
                region_id: request.region_id,
                realm_id: request.realm_id,
                generation: principal.generation,
            })
        }
    }

    fn backend() -> IdentityHttpBackend {
        let registry = Arc::new(IdentityRegistry::new());
        registry.register(DemoProvider).unwrap();
        let identities = Arc::new(IdentityService::new(registry, Arc::new(DemoBindings)));
        IdentityHttpBackend::new(identities, Arc::new(DemoPolicy))
    }

    #[tokio::test]
    async fn bridges_identity_login_and_application_policy() {
        let backend = backend();
        let grant = backend
            .login("demo", serde_json::json!({"secret": "valid"}))
            .await
            .unwrap();
        assert_eq!(
            grant.principal,
            Principal {
                account_id: 17,
                generation: 4,
            }
        );
        assert_eq!(grant.scopes, ["game:connect", "payments:write"]);

        let selection = GameSessionTicketRequest {
            user_id: 23,
            region_id: 1,
            realm_id: 2,
        };
        let identity = backend
            .game_identity(grant.principal, &selection)
            .await
            .unwrap();
        assert_eq!(identity.user_id, 23);
        assert_eq!(identity.account_id, 17);
    }

    #[tokio::test]
    async fn invalid_provider_credentials_remain_an_authentication_error() {
        let error = backend()
            .login("demo", serde_json::json!({"secret": "wrong"}))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Authentication));
    }

    #[test]
    fn provider_failures_are_mapped_without_leaking_messages() {
        assert!(matches!(
            map_provider_error(ProviderError::Unavailable),
            Error::Unavailable
        ));
        assert!(matches!(
            map_provider_error(ProviderError::RateLimited { retry_after: None }),
            Error::RateLimited
        ));
        assert!(matches!(
            map_provider_error(ProviderError::UnknownProvider("secret-provider".into())),
            Error::Authentication
        ));
        assert!(matches!(
            map_provider_error(ProviderError::Config("secret configuration".into())),
            Error::Internal(message) if message == "identity provider operation failed"
        ));
    }
}
