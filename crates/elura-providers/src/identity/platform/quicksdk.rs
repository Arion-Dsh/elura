use super::{client, endpoint};
use crate::identity::registry::{IdentityProvider, ProviderName, VerifiedIdentity};
use crate::{ProviderError, ProviderResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QuickSdkIdentityConfig {
    pub endpoint: String,
    pub allow_insecure_endpoint: bool,
    pub max_response_bytes: usize,
    pub timeout: Duration,
}

impl QuickSdkIdentityConfig {
    /// Creates QuickSDK identity configuration for an explicit endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            allow_insecure_endpoint: false,
            max_response_bytes: 4 * 1024,
            timeout: Duration::from_secs(10),
        }
    }
}
pub struct QuickSdkIdentity {
    config: QuickSdkIdentityConfig,
    endpoint: String,
    client: reqwest::Client,
}
impl QuickSdkIdentity {
    pub fn new(mut config: QuickSdkIdentityConfig) -> ProviderResult<Self> {
        if config.max_response_bytes == 0 {
            config.max_response_bytes = 4 * 1024;
        }
        let endpoint = endpoint(
            config.endpoint.clone(),
            "http://checkuser.quickapi.net/v2/checkUserInfo",
            config.allow_insecure_endpoint,
        )?;
        Ok(Self {
            client: client(config.timeout)?,
            config,
            endpoint,
        })
    }
}
/// Credential accepted by [`QuickSdkIdentity`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuickSdkCredential {
    /// QuickSDK session token.
    pub token: String,
    /// QuickSDK user identifier.
    pub uid: String,
    /// Optional product code configured in QuickSDK.
    pub product_code: Option<String>,
    /// Distribution channel code.
    pub channel_code: String,
}
#[async_trait]
impl IdentityProvider for QuickSdkIdentity {
    fn name(&self) -> &str {
        "quicksdk"
    }
    async fn authenticate(&self, raw: Value) -> ProviderResult<VerifiedIdentity> {
        let credential: QuickSdkCredential =
            serde_json::from_value(raw).map_err(|_| ProviderError::InvalidCredentials)?;
        let token = credential.token.trim();
        let uid = credential.uid.trim();
        let channel = credential.channel_code.trim();
        let product = credential
            .product_code
            .as_deref()
            .unwrap_or_default()
            .trim();
        if token.is_empty()
            || token.len() > 512
            || uid.is_empty()
            || uid.len() > 256
            || channel.is_empty()
            || channel.len() > 128
            || product.len() > 128
        {
            return Err(ProviderError::InvalidCredentials);
        }
        let response = self
            .client
            .get(&self.endpoint)
            .query(&[
                ("token", token),
                ("uid", uid),
                ("product_code", product),
                ("channel_code", channel),
            ])
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if response.status().as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if !response.status().is_success() {
            return Err(ProviderError::Unavailable);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if bytes.len() > self.config.max_response_bytes
            || String::from_utf8_lossy(&bytes).trim() != "1"
        {
            return Err(ProviderError::InvalidCredentials);
        }
        let attributes = HashMap::from([
            ("channel_code".into(), channel.into()),
            ("uid".into(), uid.into()),
            ("product_code".into(), product.into()),
        ]);
        Ok(VerifiedIdentity {
            provider: ProviderName::parse(self.name())?,
            subject: format!("{channel}:{uid}"),
            union_id: None,
            attributes,
        })
    }
}
