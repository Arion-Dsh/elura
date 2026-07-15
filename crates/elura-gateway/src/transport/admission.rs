use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionStage {
    Connected,
    Authenticated,
}

#[derive(Debug, Clone)]
pub struct AdmissionRequest {
    pub stage: AdmissionStage,
    pub remote_ip: IpAddr,
    pub identity: Option<Identity>,
}

impl AdmissionRequest {
    pub fn validate(&self) -> Result<()> {
        match (&self.stage, &self.identity) {
            (AdmissionStage::Connected, None) => Ok(()),
            (AdmissionStage::Authenticated, Some(identity)) => identity.validate(),
            _ => Err(Error::InvalidConfig(
                "admission stage and identity do not match".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRejection {
    code: String,
    reason: String,
    retry_after: Option<Duration>,
}

impl AdmissionRejection {
    pub fn new(
        code: impl Into<String>,
        reason: impl Into<String>,
        retry_after: Option<Duration>,
    ) -> Result<Self> {
        let code = code.into();
        let reason = reason.into();
        if code.is_empty()
            || code.len() > 64
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || reason.trim().is_empty()
            || reason.len() > 256
            || retry_after.is_some_and(|duration| duration.is_zero())
        {
            return Err(Error::InvalidConfig("invalid admission rejection".into()));
        }
        Ok(Self {
            code,
            reason,
            retry_after,
        })
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    Allow,
    Deny(AdmissionRejection),
}

#[async_trait]
pub trait AdmissionController: Send + Sync + 'static {
    async fn admit(&self, request: &AdmissionRequest) -> Result<AdmissionDecision>;
}

/// Admission controller that permits authenticated sessions only for configured realms.
pub struct RealmAdmission {
    realms: HashSet<(u32, u32)>,
}

impl RealmAdmission {
    /// Creates a realm allowlist for a Gateway deployment.
    pub fn new(realms: impl IntoIterator<Item = (u32, u32)>) -> Result<Self> {
        let realms: HashSet<_> = realms.into_iter().collect();
        if realms.is_empty()
            || realms
                .iter()
                .any(|(region_id, realm_id)| *region_id == 0 || *realm_id == 0)
        {
            return Err(Error::InvalidConfig(
                "Gateway realm allowlist cannot be empty or zero".into(),
            ));
        }
        Ok(Self { realms })
    }
}

#[async_trait]
impl AdmissionController for RealmAdmission {
    async fn admit(&self, request: &AdmissionRequest) -> Result<AdmissionDecision> {
        if request.stage == AdmissionStage::Authenticated {
            let identity = request.identity.as_ref().ok_or_else(|| {
                Error::InvalidConfig("authenticated admission requires identity".into())
            })?;
            if !self
                .realms
                .contains(&(identity.region_id, identity.realm_id))
            {
                return Ok(AdmissionDecision::Deny(AdmissionRejection::new(
                    "wrong_realm",
                    "this Gateway does not serve the selected realm",
                    None,
                )?));
            }
        }
        Ok(AdmissionDecision::Allow)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdmissionSettings {
    pub timeout: Duration,
    pub fail_open: bool,
}

impl Default for AdmissionSettings {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(250),
            fail_open: false,
        }
    }
}

impl AdmissionSettings {
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() {
            return Err(Error::InvalidConfig(
                "admission timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct AdmissionPolicy {
    controller: Arc<dyn AdmissionController>,
    settings: AdmissionSettings,
}

impl AdmissionPolicy {
    pub(crate) fn new(
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Result<Self> {
        settings.validate()?;
        Ok(Self {
            controller,
            settings,
        })
    }

    pub(crate) async fn check(&self, request: AdmissionRequest) -> Result<()> {
        request.validate()?;
        let result =
            tokio::time::timeout(self.settings.timeout, self.controller.admit(&request)).await;
        match result {
            Ok(Ok(AdmissionDecision::Allow)) => Ok(()),
            Ok(Ok(AdmissionDecision::Deny(rejection))) => Err(Error::AdmissionDenied {
                code: rejection.code,
                reason: rejection.reason,
                retry_after_ms: rejection.retry_after.map_or(0, |duration| {
                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                }),
            }),
            Ok(Err(error)) if !self.settings.fail_open => Err(error),
            Err(_) if !self.settings.fail_open => Err(Error::Timeout),
            Ok(Err(_)) | Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elura_core::session::Identity;

    struct Failing;

    #[async_trait]
    impl AdmissionController for Failing {
        async fn admit(&self, _request: &AdmissionRequest) -> Result<AdmissionDecision> {
            Err(Error::Unavailable)
        }
    }

    fn request() -> AdmissionRequest {
        AdmissionRequest {
            stage: AdmissionStage::Connected,
            remote_ip: "127.0.0.1".parse().unwrap(),
            identity: None,
        }
    }

    #[tokio::test]
    async fn failure_mode_is_explicit() {
        let closed = AdmissionPolicy::new(Arc::new(Failing), AdmissionSettings::default()).unwrap();
        assert!(matches!(
            closed.check(request()).await,
            Err(Error::Unavailable)
        ));

        let open = AdmissionPolicy::new(
            Arc::new(Failing),
            AdmissionSettings {
                fail_open: true,
                ..AdmissionSettings::default()
            },
        )
        .unwrap();
        open.check(request()).await.unwrap();
    }

    #[test]
    fn rejection_requires_safe_public_fields() {
        assert!(AdmissionRejection::new("rate_limited", "try later", None).is_ok());
        assert!(AdmissionRejection::new("Rate-Limited", "try later", None).is_err());
    }

    #[tokio::test]
    async fn realm_admission_rejects_a_different_realm_after_authentication() {
        let admission = RealmAdmission::new([(1, 1)]).unwrap();
        let result = admission
            .admit(&AdmissionRequest {
                stage: AdmissionStage::Authenticated,
                remote_ip: "127.0.0.1".parse().unwrap(),
                identity: Some(Identity {
                    account_id: 42,
                    region_id: 2,
                    realm_id: 2,
                    user_id: 42,
                    generation: 1,
                }),
            })
            .await
            .unwrap();
        assert!(matches!(result, AdmissionDecision::Deny(_)));
    }
}
