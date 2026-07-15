use std::sync::Arc;
use std::time::Duration;

use elura_core::account_version::{AccountVersionKey, AccountVersionStore};
use elura_core::session::Identity;
use elura_core::{Error, Result};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct AccountVersionSettings {
    pub check_interval: Duration,
    pub timeout: Duration,
    pub fail_open: bool,
}

impl Default for AccountVersionSettings {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            timeout: Duration::from_secs(1),
            fail_open: false,
        }
    }
}

impl AccountVersionSettings {
    pub fn validate(&self) -> Result<()> {
        if self.check_interval.is_zero() || self.timeout.is_zero() {
            return Err(Error::InvalidConfig(
                "account version interval and timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct AccountVersionPolicy {
    store: Arc<dyn AccountVersionStore>,
    settings: AccountVersionSettings,
}

impl AccountVersionPolicy {
    pub(crate) fn new(
        store: Arc<dyn AccountVersionStore>,
        settings: AccountVersionSettings,
    ) -> Result<Self> {
        settings.validate()?;
        Ok(Self { store, settings })
    }

    pub(crate) fn check_interval(&self) -> Duration {
        self.settings.check_interval
    }

    pub(crate) async fn check(&self, identity: &Identity) -> Result<()> {
        identity.validate()?;
        let key = AccountVersionKey::from_identity(identity);
        let result = tokio::time::timeout(self.settings.timeout, self.store.current(key)).await;
        match result {
            Ok(Ok(Some(version))) if version != 0 && version == identity.generation => Ok(()),
            Ok(Ok(_)) => Err(Error::SessionRevoked),
            Ok(Err(error)) if self.settings.fail_open => {
                warn!(user_id = identity.user_id, %error, "account version check failed open");
                Ok(())
            }
            Err(_) if self.settings.fail_open => {
                warn!(
                    user_id = identity.user_id,
                    "account version check timed out and failed open"
                );
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    struct Fixed(Result<Option<u64>>);

    #[async_trait]
    impl AccountVersionStore for Fixed {
        async fn current(&self, _key: AccountVersionKey) -> Result<Option<u64>> {
            match &self.0 {
                Ok(value) => Ok(*value),
                Err(_) => Err(Error::Unavailable),
            }
        }
    }

    fn identity() -> Identity {
        Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 5,
        }
    }

    #[tokio::test]
    async fn explicit_mismatch_never_fails_open() {
        let policy = AccountVersionPolicy::new(
            Arc::new(Fixed(Ok(Some(6)))),
            AccountVersionSettings {
                fail_open: true,
                ..AccountVersionSettings::default()
            },
        )
        .unwrap();
        assert!(matches!(
            policy.check(&identity()).await,
            Err(Error::SessionRevoked)
        ));
    }

    #[tokio::test]
    async fn infrastructure_failure_mode_is_explicit() {
        let closed = AccountVersionPolicy::new(
            Arc::new(Fixed(Err(Error::Unavailable))),
            AccountVersionSettings::default(),
        )
        .unwrap();
        assert!(matches!(
            closed.check(&identity()).await,
            Err(Error::Unavailable)
        ));
        let open = AccountVersionPolicy::new(
            Arc::new(Fixed(Err(Error::Unavailable))),
            AccountVersionSettings {
                fail_open: true,
                ..AccountVersionSettings::default()
            },
        )
        .unwrap();
        open.check(&identity()).await.unwrap();
    }
}
