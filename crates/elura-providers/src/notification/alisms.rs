//! Alibaba Cloud Dysmsapi RPC sender with native AccessKey signing.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use serde::Deserialize;
use sha1::Sha1;

use crate::identity::OtpSender;
use crate::{ProviderError, ProviderResult};

type HmacSha1 = Hmac<Sha1>;

#[derive(Clone)]
#[non_exhaustive]
pub struct AliSmsConfig {
    pub access_key_id: String,
    pub access_key_secret: String,
    pub endpoint: String,
    pub region_id: String,
    pub sign_name: String,
    pub templates: HashMap<String, String>,
    pub timeout: Duration,
}

impl AliSmsConfig {
    pub fn new(
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
        sign_name: impl Into<String>,
        templates: HashMap<String, String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),
            endpoint: "https://dysmsapi.aliyuncs.com/".into(),
            region_id: "cn-hangzhou".into(),
            sign_name: sign_name.into(),
            templates,
            timeout: Duration::from_secs(10),
        }
    }
}

pub struct AliSmsSender {
    config: AliSmsConfig,
    client: reqwest::Client,
}

impl AliSmsSender {
    pub fn new(mut config: AliSmsConfig) -> ProviderResult<Self> {
        if config.endpoint.is_empty() {
            config.endpoint = "https://dysmsapi.aliyuncs.com/".into();
        }
        if config.region_id.is_empty() {
            config.region_id = "cn-hangzhou".into();
        }
        if config.timeout.is_zero() {
            config.timeout = Duration::from_secs(10);
        }
        let url = reqwest::Url::parse(&config.endpoint)
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        if url.scheme() != "https"
            || config.access_key_id.trim().is_empty()
            || config.access_key_secret.len() < 16
            || config.sign_name.trim().is_empty()
            || config.templates.is_empty()
            || config
                .templates
                .iter()
                .any(|(purpose, template)| purpose.trim().is_empty() || template.trim().is_empty())
        {
            return Err(ProviderError::Config(
                "AliSMS requires HTTPS, AccessKey, sign name and templates".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        Ok(Self { config, client })
    }

    fn parameters(
        &self,
        phone: &str,
        code: &str,
        purpose: &str,
    ) -> ProviderResult<BTreeMap<String, String>> {
        let template =
            self.config.templates.get(purpose).ok_or_else(|| {
                ProviderError::Config(format!("missing SMS template for {purpose}"))
            })?;
        let mut nonce = [0_u8; 16];
        rand::rng().fill_bytes(&mut nonce);
        Ok(BTreeMap::from([
            ("AccessKeyId".into(), self.config.access_key_id.clone()),
            ("Action".into(), "SendSms".into()),
            ("Format".into(), "JSON".into()),
            ("PhoneNumbers".into(), phone.into()),
            ("RegionId".into(), self.config.region_id.clone()),
            ("SignName".into(), self.config.sign_name.clone()),
            ("SignatureMethod".into(), "HMAC-SHA1".into()),
            ("SignatureNonce".into(), hex::encode(nonce)),
            ("SignatureVersion".into(), "1.0".into()),
            ("TemplateCode".into(), template.clone()),
            (
                "TemplateParam".into(),
                serde_json::to_string(&serde_json::json!({"code": code}))
                    .map_err(|_| ProviderError::Unavailable)?,
            ),
            (
                "Timestamp".into(),
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            ("Version".into(), "2017-05-25".into()),
        ]))
    }
}

#[derive(Deserialize)]
struct SmsResponse {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message", default)]
    message: String,
}

#[async_trait]
impl OtpSender for AliSmsSender {
    async fn send_code(&self, phone: &str, code: &str, purpose: &str) -> ProviderResult<()> {
        if !phone.starts_with('+') || code.is_empty() || code.len() > 16 || purpose.is_empty() {
            return Err(ProviderError::InvalidCredentials);
        }
        let mut parameters = self.parameters(phone, code, purpose)?;
        let signature = rpc_signature(&parameters, &self.config.access_key_secret)?;
        parameters.insert("Signature".into(), signature);
        let response = self
            .client
            .post(&self.config.endpoint)
            .form(&parameters)
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
        if bytes.len() > 64 * 1024 {
            return Err(ProviderError::InvalidResponse(
                "AliSMS response too large".into(),
            ));
        }
        let body: SmsResponse = serde_json::from_slice(&bytes)
            .map_err(|_| ProviderError::InvalidResponse("invalid AliSMS response".into()))?;
        if body.code != "OK" {
            return Err(ProviderError::Rejected(format!(
                "{}: {}",
                body.code, body.message
            )));
        }
        Ok(())
    }
}

fn rpc_signature(parameters: &BTreeMap<String, String>, secret: &str) -> ProviderResult<String> {
    let canonical = parameters
        .iter()
        .map(|(key, value)| format!("{}={}", percent_encode(key), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let string_to_sign = format!("POST&%2F&{}", percent_encode(&canonical));
    let mut mac = HmacSha1::new_from_slice(format!("{secret}&").as_bytes())
        .map_err(|_| ProviderError::Config("invalid AliSMS secret".into()))?;
    mac.update(string_to_sign.as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

fn percent_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(byte));
        } else {
            output.push('%');
            output.push(char::from(HEX[(byte >> 4) as usize]));
            output.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_signature_is_stable_for_fixed_rpc_parameters() {
        let parameters = BTreeMap::from([
            ("AccessKeyId".into(), "testid".into()),
            ("Action".into(), "SendSms".into()),
            ("Format".into(), "XML".into()),
            ("SignatureMethod".into(), "HMAC-SHA1".into()),
            (
                "SignatureNonce".into(),
                "e1b44502-6d13-4433-9493-69eeb068e955".into(),
            ),
            ("SignatureVersion".into(), "1.0".into()),
            ("Timestamp".into(), "2016-02-23T12:46:24Z".into()),
            ("Version".into(), "2017-05-25".into()),
        ]);
        assert_eq!(
            rpc_signature(&parameters, "testsecret").unwrap(),
            "N+OqrWuN2yRdkE7CGaDPl1bu4AE="
        );
    }

    #[test]
    fn percent_encoding_uses_rfc3986_not_form_encoding() {
        assert_eq!(percent_encode("a b+c/"), "a%20b%2Bc%2F");
    }
}
