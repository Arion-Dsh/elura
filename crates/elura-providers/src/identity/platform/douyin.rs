use super::{PlatformIdentityConfig, client, decode_json, endpoint, identity, validate_code};
use crate::identity::registry::{IdentityProvider, VerifiedIdentity};
use crate::{ProviderError, ProviderResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

pub struct DouyinIdentity {
    config: PlatformIdentityConfig,
    endpoint: String,
    client: reqwest::Client,
}
impl DouyinIdentity {
    pub fn new(mut config: PlatformIdentityConfig) -> ProviderResult<Self> {
        if config.app_id.trim().is_empty() || config.app_secret.trim().is_empty() {
            return Err(ProviderError::Config(
                "Douyin app id and secret are required".into(),
            ));
        }
        if config.max_response_bytes == 0 {
            config.max_response_bytes = 64 * 1024;
        }
        let endpoint = endpoint(
            config.endpoint.clone(),
            "https://developer.toutiao.com/api/apps/v2/jscode2session",
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
    err_no: i64,
    data: Option<Data>,
}
#[derive(Deserialize)]
struct Data {
    #[serde(default)]
    openid: String,
    #[serde(default)]
    unionid: String,
}
#[async_trait]
impl IdentityProvider for DouyinIdentity {
    fn name(&self) -> &str {
        "douyin"
    }
    async fn authenticate(&self, raw: Value) -> ProviderResult<VerifiedIdentity> {
        let code = validate_code(raw)?;
        let response = self.client.post(&self.endpoint).json(&serde_json::json!({"appid": self.config.app_id, "secret": self.config.app_secret, "code": code, "anonymous_code": ""})).send().await.map_err(|_| ProviderError::Unavailable)?;
        let response: Response = decode_json(response, self.config.max_response_bytes).await?;
        let data = response.data.ok_or(ProviderError::InvalidCredentials)?;
        if response.err_no != 0 {
            return Err(ProviderError::InvalidCredentials);
        }
        identity(
            self.name(),
            data.openid,
            data.unionid,
            self.config.require_union_id,
        )
    }
}
