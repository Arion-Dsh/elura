//! WeChat Pay v3 APP checkout, query and authenticated notification handling.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use rand::Rng;
use reqwest::{Method, Url, header::HeaderMap};
use ring::{rand::SystemRandom, rsa, signature};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::rsa_signature::{parse_private_key, parse_public_key};
use super::{
    CheckoutRequest, CheckoutResult, ClientPayload, Money, NotificationRequest, Payment,
    PaymentCapabilities, PaymentEvent, PaymentLookup, PaymentProvider, PaymentStatus,
};
use crate::{ProviderError, ProviderResult};

const APP_PATH: &str = "/v3/pay/transactions/app";
const QUERY_PATH: &str = "/v3/pay/transactions/out-trade-no/";
const QUERY_ID_PATH: &str = "/v3/pay/transactions/id/";

#[derive(Clone)]
#[non_exhaustive]
pub struct WechatPayConfig {
    pub merchant_id: String,
    pub serial_number: String,
    pub api_v3_key: String,
    pub private_key_pem: String,
    pub app_id: String,
    /// Provider-owned asynchronous notification endpoint.
    pub notify_url: String,
    pub wechat_public_key_pem: String,
    pub wechat_public_key_id: String,
    pub base_url: String,
    pub timeout: Duration,
}

impl WechatPayConfig {
    /// Creates the non-secret portion of a WeChat Pay configuration.
    pub fn new(
        merchant_id: impl Into<String>,
        app_id: impl Into<String>,
        notify_url: impl Into<String>,
    ) -> Self {
        Self {
            merchant_id: merchant_id.into(),
            serial_number: String::new(),
            api_v3_key: String::new(),
            private_key_pem: String::new(),
            app_id: app_id.into(),
            notify_url: notify_url.into(),
            wechat_public_key_pem: String::new(),
            wechat_public_key_id: String::new(),
            base_url: "https://api.mch.weixin.qq.com/".into(),
            timeout: Duration::from_secs(10),
        }
    }

    /// Installs the merchant signing identity and API v3 key.
    pub fn with_merchant_identity(
        mut self,
        serial_number: impl Into<String>,
        api_v3_key: impl Into<String>,
        private_key_pem: impl Into<String>,
    ) -> Self {
        self.serial_number = serial_number.into();
        self.api_v3_key = api_v3_key.into();
        self.private_key_pem = private_key_pem.into();
        self
    }

    /// Installs the WeChat notification verification identity.
    pub fn with_wechat_identity(
        mut self,
        public_key_id: impl Into<String>,
        public_key_pem: impl Into<String>,
    ) -> Self {
        self.wechat_public_key_id = public_key_id.into();
        self.wechat_public_key_pem = public_key_pem.into();
        self
    }
}

pub struct WechatPayPayment {
    merchant_id: String,
    serial_number: String,
    api_v3_key: [u8; 32],
    app_id: String,
    notify_url: String,
    public_key_id: String,
    signing_key: rsa::KeyPair,
    verifying_key: Vec<u8>,
    base_url: Url,
    client: reqwest::Client,
}

impl WechatPayPayment {
    pub fn new(mut config: WechatPayConfig) -> ProviderResult<Self> {
        if config.base_url.is_empty() {
            config.base_url = "https://api.mch.weixin.qq.com/".into();
        }
        if config.timeout.is_zero() {
            config.timeout = Duration::from_secs(10);
        }
        let base_url = Url::parse(&config.base_url)
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        let local_http = matches!(base_url.host_str(), Some("127.0.0.1" | "localhost"));
        if base_url.scheme() != "https" && !local_http {
            return Err(ProviderError::Config(
                "WeChat Pay endpoint must use HTTPS".into(),
            ));
        }
        if config.merchant_id.trim().is_empty()
            || config.serial_number.trim().is_empty()
            || config.app_id.trim().is_empty()
            || config.notify_url.trim().is_empty()
            || config.wechat_public_key_id.trim().is_empty()
            || config.api_v3_key.len() != 32
        {
            return Err(ProviderError::Config(
                "incomplete WeChat Pay v3 configuration".into(),
            ));
        }
        let private_key = parse_private_key(&config.private_key_pem, "merchant")?;
        let public_key = parse_public_key(&config.wechat_public_key_pem, "WeChat")?;
        let api_v3_key = config
            .api_v3_key
            .as_bytes()
            .try_into()
            .map_err(|_| ProviderError::Config("APIV3 key must be 32 bytes".into()))?;
        let client = crate::http_client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        Ok(Self {
            merchant_id: config.merchant_id,
            serial_number: config.serial_number,
            api_v3_key,
            app_id: config.app_id,
            notify_url: config.notify_url,
            public_key_id: config.wechat_public_key_id,
            signing_key: private_key,
            verifying_key: public_key,
            base_url,
            client,
        })
    }

    fn nonce() -> String {
        let mut bytes = [0_u8; 16];
        rand::rng().fill_bytes(&mut bytes);
        hex::encode(bytes)
    }

    fn sign(
        &self,
        method: &str,
        path_and_query: &str,
        timestamp: u64,
        nonce: &str,
        body: &[u8],
    ) -> ProviderResult<String> {
        let message = format!(
            "{method}\n{path_and_query}\n{timestamp}\n{nonce}\n{}\n",
            String::from_utf8_lossy(body)
        );
        self.sign_message(message.as_bytes())
    }

    fn sign_message(&self, message: &[u8]) -> ProviderResult<String> {
        let mut signed = vec![0; self.signing_key.public().modulus_len()];
        self.signing_key
            .sign(
                &signature::RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                message,
                &mut signed,
            )
            .map_err(|_| ProviderError::Config("WeChat RSA signing failed".into()))?;
        Ok(STANDARD.encode(signed))
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path_and_query: &str,
        body: Option<Vec<u8>>,
    ) -> ProviderResult<T> {
        let timestamp = unix_seconds()?;
        let nonce = Self::nonce();
        let bytes = body.unwrap_or_default();
        let signature = self.sign(method.as_str(), path_and_query, timestamp, &nonce, &bytes)?;
        let authorization = format!(
            "WECHATPAY2-SHA256-RSA2048 mchid=\"{}\",nonce_str=\"{}\",timestamp=\"{}\",serial_no=\"{}\",signature=\"{}\"",
            self.merchant_id, nonce, timestamp, self.serial_number, signature
        );
        let url = self
            .base_url
            .join(path_and_query.trim_start_matches('/'))
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        let mut request = self
            .client
            .request(method, url)
            .header("Authorization", authorization)
            .header("Accept", "application/json");
        if !bytes.is_empty() {
            request = request
                .header("Content-Type", "application/json")
                .body(bytes);
        }
        let response = request
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if body.len() > 1024 * 1024 {
            return Err(ProviderError::InvalidResponse(
                "WeChat response too large".into(),
            ));
        }
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if !status.is_success() {
            return Err(ProviderError::Rejected(format!("WeChat Pay HTTP {status}")));
        }
        self.verify_http_signature(&headers, &body)?;
        serde_json::from_slice(&body)
            .map_err(|_| ProviderError::InvalidResponse("invalid WeChat response".into()))
    }

    fn verify_http_signature(&self, headers: &HeaderMap, body: &[u8]) -> ProviderResult<()> {
        let timestamp = header(headers, "Wechatpay-Timestamp")?;
        let nonce = header(headers, "Wechatpay-Nonce")?;
        let signature = header(headers, "Wechatpay-Signature")?;
        if headers
            .get("Wechatpay-Serial")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|serial| serial != self.public_key_id)
        {
            return Err(ProviderError::InvalidSignature);
        }
        let signature = STANDARD
            .decode(signature)
            .map_err(|_| ProviderError::InvalidSignature)?;
        let message = format!("{timestamp}\n{nonce}\n{}\n", String::from_utf8_lossy(body));
        signature::UnparsedPublicKey::new(
            &signature::RSA_PKCS1_2048_8192_SHA256,
            &self.verifying_key,
        )
        .verify(message.as_bytes(), &signature)
        .map_err(|_| ProviderError::InvalidSignature)
    }

    fn app_payload(&self, prepay_id: &str) -> ProviderResult<String> {
        let timestamp = unix_seconds()?;
        let nonce = Self::nonce();
        let package = format!("Sign=WXPay&prepayid={prepay_id}");
        let message = format!("{}\n{timestamp}\n{nonce}\n{package}\n", self.app_id);
        let signature = self.sign_message(message.as_bytes())?;
        Ok(serde_json::json!({
            "appid": self.app_id, "partnerid": self.merchant_id, "prepayid": prepay_id,
            "package": "Sign=WXPay", "noncestr": nonce, "timestamp": timestamp.to_string(),
            "sign": signature
        })
        .to_string())
    }
}

#[derive(Serialize)]
struct CreateBody<'a> {
    appid: &'a str,
    mchid: &'a str,
    description: &'a str,
    out_trade_no: &'a str,
    notify_url: &'a str,
    amount: CreateAmount<'a>,
}

#[derive(Serialize)]
struct CreateAmount<'a> {
    total: i64,
    currency: &'a str,
}

#[derive(Deserialize)]
struct CreateResponse {
    prepay_id: String,
}

#[async_trait]
impl PaymentProvider for WechatPayPayment {
    fn name(&self) -> &str {
        "wechatpay"
    }

    fn capabilities(&self) -> PaymentCapabilities {
        PaymentCapabilities {
            checkout: true,
            query: true,
            notification: true,
            ..Default::default()
        }
    }

    async fn create(&self, request: CheckoutRequest) -> ProviderResult<CheckoutResult> {
        request.validate()?;
        if request.amount.currency != "CNY"
            || request.merchant_order_id.trim().is_empty()
            || request.subject.trim().is_empty()
        {
            return Err(ProviderError::InvalidRequest(
                "invalid WeChat Pay checkout".into(),
            ));
        }
        let body = serde_json::to_vec(&CreateBody {
            appid: &self.app_id,
            mchid: &self.merchant_id,
            description: &request.subject,
            out_trade_no: &request.merchant_order_id,
            notify_url: &self.notify_url,
            amount: CreateAmount {
                total: request.amount.minor_units,
                currency: "CNY",
            },
        })
        .map_err(|_| ProviderError::Unavailable)?;
        let response: CreateResponse = self.request(Method::POST, APP_PATH, Some(body)).await?;
        if response.prepay_id.is_empty() {
            return Err(ProviderError::InvalidResponse("empty prepay id".into()));
        }
        Ok(CheckoutResult {
            provider_order_id: Some(response.prepay_id.clone()),
            client_payload: ClientPayload::Json(
                serde_json::from_str(&self.app_payload(&response.prepay_id)?).map_err(|_| {
                    ProviderError::InvalidResponse("invalid WeChat client payload".into())
                })?,
            ),
            expires_at: SystemTime::now().checked_add(Duration::from_secs(7200)),
        })
    }

    async fn query(&self, lookup: PaymentLookup) -> ProviderResult<Payment> {
        let order = lookup.value()?.to_owned();
        let prefix = match lookup {
            PaymentLookup::MerchantOrderId(_) => QUERY_PATH,
            PaymentLookup::ProviderOrderId(_) => QUERY_ID_PATH,
        };
        let escaped = url::form_urlencoded::byte_serialize(order.as_bytes()).collect::<String>();
        let path = format!("{prefix}{escaped}?mchid={}", self.merchant_id);
        let response: QueryResponse = self.request(Method::GET, &path, None).await?;
        response.into_payment()
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        self.verify_http_signature(&request.headers, &request.body)?;
        let envelope: NotificationEnvelope = serde_json::from_slice(&request.body)
            .map_err(|_| ProviderError::InvalidResponse("invalid WeChat notification".into()))?;
        if envelope.resource.nonce.len() != 12 {
            return Err(ProviderError::InvalidResponse(
                "invalid WeChat nonce".into(),
            ));
        }
        let cipher = Aes256Gcm::new_from_slice(&self.api_v3_key)
            .map_err(|_| ProviderError::Config("invalid APIV3 key".into()))?;
        let encrypted = STANDARD
            .decode(&envelope.resource.ciphertext)
            .map_err(|_| ProviderError::InvalidResponse("invalid ciphertext".into()))?;
        let nonce = Nonce::try_from(envelope.resource.nonce.as_bytes())
            .map_err(|_| ProviderError::InvalidResponse("invalid WeChat nonce".into()))?;
        let plain = cipher
            .decrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: &encrypted,
                    aad: envelope.resource.associated_data.as_bytes(),
                },
            )
            .map_err(|_| ProviderError::InvalidSignature)?;
        let result: QueryResponse = serde_json::from_slice(&plain)
            .map_err(|_| ProviderError::InvalidResponse("invalid decrypted payment".into()))?;
        if result.appid != self.app_id || result.mchid != self.merchant_id {
            return Err(ProviderError::InvalidResponse(
                "notification identity mismatch".into(),
            ));
        }
        let payment = result.into_payment()?;
        let provider_order_id = payment.provider_order_id.ok_or_else(|| {
            ProviderError::InvalidResponse("missing WeChat provider order id".into())
        })?;
        let amount = payment
            .amount
            .ok_or_else(|| ProviderError::InvalidResponse("missing WeChat amount".into()))?;
        Ok(PaymentEvent {
            event_id: envelope.id,
            merchant_order_id: payment.merchant_order_id,
            provider_order_id,
            original_provider_order_id: None,
            payer_id: payment.payer_id,
            product_id: None,
            quantity: 1,
            status: payment.status,
            amount,
            environment: None,
            occurred_at: payment.paid_at,
        })
    }
}

#[derive(Deserialize)]
struct QueryResponse {
    appid: String,
    mchid: String,
    out_trade_no: String,
    transaction_id: String,
    trade_state: String,
    amount: QueryAmount,
    #[serde(default)]
    success_time: Option<String>,
    #[serde(default)]
    payer: Option<Payer>,
}

#[derive(Deserialize)]
struct QueryAmount {
    total: i64,
    currency: String,
}

#[derive(Deserialize)]
struct Payer {
    openid: String,
}

impl QueryResponse {
    fn into_payment(self) -> ProviderResult<Payment> {
        let amount = Money {
            currency: self.amount.currency,
            minor_units: self.amount.total,
        };
        amount.validate_response()?;
        if amount.currency != "CNY" {
            return Err(ProviderError::InvalidResponse(
                "WeChat payment currency mismatch".into(),
            ));
        }
        Ok(Payment {
            merchant_order_id: self.out_trade_no,
            provider_order_id: Some(self.transaction_id),
            status: map_status(&self.trade_state),
            amount: Some(amount),
            paid_at: self
                .success_time
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                .map(SystemTime::from),
            payer_id: self.payer.map(|payer| payer.openid),
        })
    }
}

#[derive(Deserialize)]
struct NotificationEnvelope {
    id: String,
    resource: EncryptedResource,
}

#[derive(Deserialize)]
struct EncryptedResource {
    associated_data: String,
    nonce: String,
    ciphertext: String,
}

fn unix_seconds() -> ProviderResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ProviderError::Unavailable)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> ProviderResult<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(ProviderError::InvalidSignature)
}

fn map_status(value: &str) -> PaymentStatus {
    match value {
        "SUCCESS" => PaymentStatus::Succeeded,
        "REFUND" => PaymentStatus::Refunded,
        "CLOSED" | "REVOKED" => PaymentStatus::Closed,
        "PAYERROR" => PaymentStatus::Failed,
        _ => PaymentStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_wechat_terminal_states_are_mapped() {
        assert_eq!(map_status("SUCCESS"), PaymentStatus::Succeeded);
        assert_eq!(map_status("REFUND"), PaymentStatus::Refunded);
        assert_eq!(map_status("REVOKED"), PaymentStatus::Closed);
        assert_eq!(map_status("PAYERROR"), PaymentStatus::Failed);
    }
}
