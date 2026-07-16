use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::registry::{IdentityProvider, ProviderName, VerifiedIdentity};
use crate::{ProviderError, ProviderResult};

#[derive(Clone)]
#[non_exhaustive]
pub struct CodeExchangeConfig {
    pub name: String,
    pub endpoint: String,
    pub client_id: String,
    pub client_secret: String,
    pub subject_field: String,
    pub union_id_field: Option<String>,
}

impl CodeExchangeConfig {
    /// Creates a code-exchange provider configuration.
    pub fn new(
        name: impl Into<String>,
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        subject_field: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            subject_field: subject_field.into(),
            union_id_field: None,
        }
    }
}

pub struct CodeExchangeProvider {
    config: CodeExchangeConfig,
    client: reqwest::Client,
}

impl CodeExchangeProvider {
    pub fn new(config: CodeExchangeConfig) -> ProviderResult<Self> {
        require_https(&config.endpoint)?;
        if config.name.trim().is_empty()
            || config.client_id.is_empty()
            || config.client_secret.is_empty()
            || config.subject_field.is_empty()
        {
            return Err(ProviderError::Config(
                "invalid code exchange configuration".into(),
            ));
        }
        Ok(Self {
            config,
            client: secure_client()?,
        })
    }
}

/// Credential accepted by code-exchange and platform identity providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeCredential {
    /// Short-lived authorization code received from the client platform.
    pub code: String,
}

impl CodeCredential {
    /// Creates a code credential.
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into() }
    }
}

#[async_trait]
impl IdentityProvider for CodeExchangeProvider {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        let credential: CodeCredential =
            serde_json::from_value(credential).map_err(|_| ProviderError::InvalidCredentials)?;
        if credential.code.trim().is_empty() || credential.code.len() > 4096 {
            return Err(ProviderError::InvalidCredentials);
        }
        let response = self
            .client
            .post(&self.config.endpoint)
            .json(&serde_json::json!({
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "code": credential.code,
            }))
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let status = response.status();
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if status.is_client_error() {
            return Err(ProviderError::InvalidCredentials);
        }
        if !status.is_success() {
            return Err(ProviderError::Unavailable);
        }
        identity_from_response(
            self.name(),
            response,
            &self.config.subject_field,
            self.config.union_id_field.as_deref(),
        )
        .await
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct OAuth2Config {
    pub name: String,
    pub client_id: String,
    pub client_secret: String,
    /// Provider-owned authorization callback endpoint.
    pub redirect_uri: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub userinfo_endpoint: String,
    pub scopes: Vec<String>,
    pub subject_field: String,
    pub union_id_field: Option<String>,
}

impl OAuth2Config {
    /// Creates OAuth 2.0 configuration for a public client with no default scopes.
    pub fn new(
        name: impl Into<String>,
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
        authorization_endpoint: impl Into<String>,
        token_endpoint: impl Into<String>,
        userinfo_endpoint: impl Into<String>,
        subject_field: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            client_id: client_id.into(),
            client_secret: String::new(),
            redirect_uri: redirect_uri.into(),
            authorization_endpoint: authorization_endpoint.into(),
            token_endpoint: token_endpoint.into(),
            userinfo_endpoint: userinfo_endpoint.into(),
            scopes: Vec::new(),
            subject_field: subject_field.into(),
            union_id_field: None,
        }
    }
}

pub struct OAuth2Provider {
    config: OAuth2Config,
    client: reqwest::Client,
}

impl OAuth2Provider {
    pub fn new(config: OAuth2Config) -> ProviderResult<Self> {
        for endpoint in [
            &config.authorization_endpoint,
            &config.token_endpoint,
            &config.userinfo_endpoint,
            &config.redirect_uri,
        ] {
            require_https(endpoint)?;
        }
        if config.name.trim().is_empty()
            || config.client_id.is_empty()
            || config.subject_field.is_empty()
        {
            return Err(ProviderError::Config(
                "OAuth2 name, client ID and subject field are required".into(),
            ));
        }
        Ok(Self {
            config,
            client: secure_client()?,
        })
    }

    pub fn authorization_url(&self, state: &str, code_verifier: &str) -> ProviderResult<String> {
        if state.is_empty() || state.len() > 1024 || !(43..=128).contains(&code_verifier.len()) {
            return Err(ProviderError::Config(
                "invalid OAuth2 state or PKCE verifier".into(),
            ));
        }
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
        let mut url = reqwest::Url::parse(&self.config.authorization_endpoint)
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.config.client_id)
            .append_pair("redirect_uri", &self.config.redirect_uri)
            .append_pair("scope", &self.config.scopes.join(" "))
            .append_pair("state", state)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256");
        Ok(url.into())
    }
}

/// OAuth 2.0 authorization-code credential with a PKCE verifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth2Credential {
    /// Authorization code returned by the provider.
    pub code: String,
    /// Original PKCE verifier used to build the authorization URL.
    pub code_verifier: String,
}

impl OAuth2Credential {
    /// Creates an OAuth 2.0 credential.
    pub fn new(code: impl Into<String>, code_verifier: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            code_verifier: code_verifier.into(),
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[async_trait]
impl IdentityProvider for OAuth2Provider {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn authenticate(&self, credential: Value) -> ProviderResult<VerifiedIdentity> {
        let credential: OAuth2Credential =
            serde_json::from_value(credential).map_err(|_| ProviderError::InvalidCredentials)?;
        if credential.code.trim().is_empty()
            || credential.code.len() > 4096
            || !(43..=128).contains(&credential.code_verifier.len())
        {
            return Err(ProviderError::InvalidCredentials);
        }
        let response = self
            .client
            .post(&self.config.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", credential.code.as_str()),
                ("redirect_uri", self.config.redirect_uri.as_str()),
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
                ("code_verifier", credential.code_verifier.as_str()),
            ])
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let status = response.status();
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if status.is_client_error() {
            return Err(ProviderError::InvalidCredentials);
        }
        if !status.is_success() {
            return Err(ProviderError::Unavailable);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if bytes.len() > 64 * 1024 {
            return Err(ProviderError::InvalidResponse(
                "OAuth token response too large".into(),
            ));
        }
        let token: TokenResponse = serde_json::from_slice(&bytes)
            .map_err(|_| ProviderError::InvalidResponse("invalid OAuth token response".into()))?;
        if token.access_token.is_empty() || token.access_token.len() > 8192 {
            return Err(ProviderError::InvalidCredentials);
        }
        let response = self
            .client
            .get(&self.config.userinfo_endpoint)
            .bearer_auth(token.access_token)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if response.status().as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if !response.status().is_success() {
            return Err(ProviderError::InvalidCredentials);
        }
        identity_from_response(
            self.name(),
            response,
            &self.config.subject_field,
            self.config.union_id_field.as_deref(),
        )
        .await
    }
}

async fn identity_from_response(
    provider: &str,
    response: reqwest::Response,
    subject_field: &str,
    union_field: Option<&str>,
) -> ProviderResult<VerifiedIdentity> {
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ProviderError::Unavailable)?;
    if bytes.len() > 64 * 1024 {
        return Err(ProviderError::InvalidResponse(
            "identity response too large".into(),
        ));
    }
    let body: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ProviderError::InvalidResponse("invalid identity response".into()))?;
    let subject = json_string(&body, subject_field)
        .ok_or_else(|| ProviderError::InvalidResponse("missing identity subject".into()))?;
    let union_id = union_field.and_then(|field| json_string(&body, field));
    Ok(VerifiedIdentity {
        provider: ProviderName::parse(provider)?,
        subject,
        union_id,
        attributes: HashMap::new(),
    })
}

fn json_string(value: &Value, path: &str) -> Option<String> {
    path.split('.')
        .try_fold(value, |current, segment| current.get(segment))?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn require_https(value: &str) -> ProviderResult<()> {
    let url =
        reqwest::Url::parse(value).map_err(|error| ProviderError::Config(error.to_string()))?;
    if url.scheme() != "https" {
        return Err(ProviderError::Config(
            "identity endpoint must use HTTPS".into(),
        ));
    }
    Ok(())
}

fn secure_client() -> ProviderResult<reqwest::Client> {
    crate::http_client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| ProviderError::Config(error.to_string()))
}
