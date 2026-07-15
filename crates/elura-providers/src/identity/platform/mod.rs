//! Platform identity providers with a shared hardened HTTP boundary.

use crate::{ProviderError, ProviderResult};
use serde::de::DeserializeOwned;
use std::time::Duration;

mod douyin;
mod quicksdk;
mod wechat;
mod wechat_mini;

pub use douyin::DouyinIdentity;
pub use quicksdk::{QuickSdkIdentity, QuickSdkIdentityConfig};
pub use wechat::WechatIdentity;
pub use wechat_mini::WechatMiniIdentity;

#[derive(Clone)]
pub struct PlatformIdentityConfig {
    pub app_id: String,
    pub app_secret: String,
    pub endpoint: String,
    pub require_union_id: bool,
    pub allow_insecure_endpoint: bool,
    pub max_response_bytes: usize,
    pub timeout: Duration,
}

fn client(timeout: Duration) -> ProviderResult<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(if timeout.is_zero() {
            Duration::from_secs(10)
        } else {
            timeout
        })
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| ProviderError::Config(error.to_string()))
}

fn endpoint(value: String, default: &str, allow_insecure: bool) -> ProviderResult<String> {
    let value = if value.is_empty() {
        default.into()
    } else {
        value
    };
    let url =
        reqwest::Url::parse(&value).map_err(|error| ProviderError::Config(error.to_string()))?;
    if url.scheme() != "https" && !allow_insecure {
        return Err(ProviderError::Config(
            "platform endpoint must use HTTPS".into(),
        ));
    }
    Ok(value)
}

async fn decode_json<T: DeserializeOwned>(
    response: reqwest::Response,
    maximum: usize,
) -> ProviderResult<T> {
    if !response.status().is_success() {
        return if response.status().is_client_error() {
            Err(ProviderError::InvalidCredentials)
        } else {
            Err(ProviderError::Unavailable)
        };
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ProviderError::Unavailable)?;
    if bytes.len() > maximum {
        return Err(ProviderError::InvalidResponse(
            "platform response too large".into(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| ProviderError::InvalidResponse("invalid platform JSON".into()))
}

fn validate_code(raw: serde_json::Value) -> ProviderResult<String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Credential {
        code: String,
    }
    let credential: Credential =
        serde_json::from_value(raw).map_err(|_| ProviderError::InvalidCredentials)?;
    let code = credential.code.trim();
    if code.is_empty() || code.len() > 2048 {
        return Err(ProviderError::InvalidCredentials);
    }
    Ok(code.into())
}

fn identity(
    provider: &str,
    subject: String,
    union_id: String,
    require_union_id: bool,
) -> ProviderResult<super::registry::VerifiedIdentity> {
    let subject = subject.trim().to_owned();
    let union_id = (!union_id.trim().is_empty()).then(|| union_id.trim().to_owned());
    if subject.is_empty() || (require_union_id && union_id.is_none()) {
        return Err(ProviderError::InvalidCredentials);
    }
    Ok(super::registry::VerifiedIdentity {
        provider: provider.into(),
        subject,
        union_id,
        attributes: Default::default(),
    })
}
