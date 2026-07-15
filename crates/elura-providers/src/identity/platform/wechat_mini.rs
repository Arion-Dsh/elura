use super::{PlatformIdentityConfig, client, decode_json, endpoint, identity, validate_code};
use crate::identity::registry::{IdentityProvider, VerifiedIdentity};
use crate::{ProviderError, ProviderResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

pub struct WechatMiniIdentity {
    config: PlatformIdentityConfig,
    endpoint: String,
    client: reqwest::Client,
}
impl WechatMiniIdentity {
    pub fn new(mut config: PlatformIdentityConfig) -> ProviderResult<Self> {
        if config.app_id.trim().is_empty() || config.app_secret.trim().is_empty() {
            return Err(ProviderError::Config(
                "WeChat Mini app id and secret are required".into(),
            ));
        }
        if config.max_response_bytes == 0 {
            config.max_response_bytes = 64 * 1024;
        }
        let endpoint = endpoint(
            config.endpoint.clone(),
            "https://api.weixin.qq.com/sns/jscode2session",
            config.allow_insecure_endpoint,
        )?;
        Ok(Self {
            client: client(config.timeout)?,
            config,
            endpoint,
        })
    }
}
#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    openid: String,
    #[serde(default)]
    unionid: String,
    #[serde(default)]
    errcode: i64,
}
#[async_trait]
impl IdentityProvider for WechatMiniIdentity {
    fn name(&self) -> &str {
        "wechat_mini"
    }
    async fn authenticate(&self, raw: Value) -> ProviderResult<VerifiedIdentity> {
        let code = validate_code(raw)?;
        let response = self
            .client
            .get(&self.endpoint)
            .query(&[
                ("appid", self.config.app_id.as_str()),
                ("secret", self.config.app_secret.as_str()),
                ("js_code", code.as_str()),
                ("grant_type", "authorization_code"),
            ])
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let response: Response = decode_json(response, self.config.max_response_bytes).await?;
        if response.errcode != 0 {
            return Err(ProviderError::InvalidCredentials);
        }
        identity(
            self.name(),
            response.openid,
            response.unionid,
            self.config.require_union_id,
        )
    }
}
