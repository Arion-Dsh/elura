//! Douyin mini-game payment query and callback verification.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use super::{
    Money, NotificationRequest, Payment, PaymentCapabilities, PaymentEvent, PaymentLookup,
    PaymentProvider, PaymentStatus,
};
use crate::{ProviderError, ProviderResult};

const TOKEN_URL: &str = "https://minigame.zijieapi.com/mgplatform/api/apps/stable_token";
const QUERY_URL: &str = "https://developer.toutiao.com/api/apps/game/payment/queryPayState";

#[derive(Clone)]
#[non_exhaustive]
pub struct DouyinConfig {
    pub callback_token: String,
    pub app_id: String,
    pub app_secret: String,
    pub token_url: String,
    pub query_url: String,
    pub timeout: Duration,
}

impl DouyinConfig {
    /// Creates Douyin payment configuration using the production endpoints.
    pub fn new(
        callback_token: impl Into<String>,
        app_id: impl Into<String>,
        app_secret: impl Into<String>,
    ) -> Self {
        Self {
            callback_token: callback_token.into(),
            app_id: app_id.into(),
            app_secret: app_secret.into(),
            token_url: TOKEN_URL.into(),
            query_url: QUERY_URL.into(),
            timeout: Duration::from_secs(10),
        }
    }
}

struct CachedToken {
    value: String,
    refresh_at: Instant,
}

pub struct DouyinPayment {
    config: DouyinConfig,
    client: reqwest::Client,
    token: Mutex<Option<CachedToken>>,
}

impl DouyinPayment {
    pub fn new(mut config: DouyinConfig) -> ProviderResult<Self> {
        if config.token_url.is_empty() {
            config.token_url = TOKEN_URL.into();
        }
        if config.query_url.is_empty() {
            config.query_url = QUERY_URL.into();
        }
        if config.timeout.is_zero() {
            config.timeout = Duration::from_secs(10);
        }
        if config.app_id.trim().is_empty()
            || config.app_secret.trim().is_empty()
            || config.callback_token.len() < 16
        {
            return Err(ProviderError::Config(
                "Douyin app id, secret and callback token are required".into(),
            ));
        }
        validate_endpoint(&config.token_url)?;
        validate_endpoint(&config.query_url)?;
        let client = crate::http_client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        Ok(Self {
            config,
            client,
            token: Mutex::new(None),
        })
    }

    fn signature(&self, timestamp: &str, nonce: &str, message: &str) -> String {
        let mut values = [&self.config.callback_token, timestamp, nonce, message];
        values.sort_unstable();
        hex::encode(Sha1::digest(values.concat().as_bytes()))
    }

    fn decode_envelope(&self, request: &NotificationRequest) -> ProviderResult<Envelope> {
        if !request.body.iter().all(u8::is_ascii_whitespace) && !request.body.is_empty() {
            return serde_json::from_slice(&request.body)
                .map_err(|_| ProviderError::InvalidResponse("invalid Douyin callback".into()));
        }
        let values = url::form_urlencoded::parse(request.query().as_bytes())
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        Ok(Envelope {
            timestamp: values.get("timestamp").cloned().unwrap_or_default(),
            nonce: values.get("nonce").cloned().unwrap_or_default(),
            msg: values.get("msg").cloned().unwrap_or_default(),
            signature: values.get("signature").cloned().unwrap_or_default(),
            echostr: values.get("echostr").cloned().unwrap_or_default(),
        })
    }

    fn verify_envelope(&self, envelope: &Envelope) -> ProviderResult<()> {
        if envelope.timestamp.is_empty()
            || envelope.nonce.is_empty()
            || envelope.signature.is_empty()
        {
            return Err(ProviderError::InvalidSignature);
        }
        let expected = self.signature(&envelope.timestamp, &envelope.nonce, &envelope.msg);
        if expected
            .as_bytes()
            .ct_eq(envelope.signature.trim().as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(ProviderError::InvalidSignature);
        }
        Ok(())
    }

    pub fn verify_callback_url(&self, request: &NotificationRequest) -> ProviderResult<String> {
        request.validate()?;
        let envelope = self.decode_envelope(request)?;
        self.verify_envelope(&envelope)?;
        if envelope.echostr.is_empty() {
            return Err(ProviderError::InvalidResponse("empty Douyin echo".into()));
        }
        Ok(envelope.echostr)
    }

    async fn access_token(&self) -> ProviderResult<String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached
            .as_ref()
            .filter(|token| Instant::now() < token.refresh_at)
        {
            return Ok(token.value.clone());
        }
        #[derive(Serialize)]
        struct Request<'a> {
            appid: &'a str,
            secret: &'a str,
            grant_type: &'a str,
        }
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            access_token: String,
            #[serde(default)]
            expires_in: u64,
            #[serde(default)]
            err_no: i64,
            #[serde(default)]
            err_msg: String,
            data: Option<TokenData>,
        }
        #[derive(Deserialize)]
        struct TokenData {
            access_token: String,
            expires_in: u64,
        }
        let mut response: Response = self
            .post_json(
                &self.config.token_url,
                &Request {
                    appid: &self.config.app_id,
                    secret: &self.config.app_secret,
                    grant_type: "client_credential",
                },
            )
            .await?;
        if let Some(data) = response.data.take() {
            response.access_token = data.access_token;
            response.expires_in = data.expires_in;
        }
        if response.err_no != 0 || response.access_token.trim().is_empty() {
            return Err(ProviderError::Rejected(format!(
                "Douyin token: {}",
                response.err_msg
            )));
        }
        let lifetime = Duration::from_secs(response.expires_in.max(300));
        let refresh_at = Instant::now() + lifetime.saturating_sub(Duration::from_secs(300));
        *cached = Some(CachedToken {
            value: response.access_token.clone(),
            refresh_at,
        });
        Ok(response.access_token)
    }

    async fn post_json<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        url: &str,
        body: &T,
    ) -> ProviderResult<R> {
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        decode_response(response).await
    }
}

#[derive(Deserialize)]
struct Envelope {
    timestamp: String,
    nonce: String,
    msg: String,
    signature: String,
    #[serde(default)]
    echostr: String,
}

#[derive(Deserialize)]
struct Body {
    appid: String,
    cp_orderno: String,
    order_no_channel: String,
    amount_cent: i64,
    #[serde(default)]
    currency: String,
}

#[async_trait]
impl PaymentProvider for DouyinPayment {
    fn name(&self) -> &str {
        "douyin"
    }
    fn capabilities(&self) -> PaymentCapabilities {
        PaymentCapabilities {
            query: true,
            notification: true,
            ..Default::default()
        }
    }

    async fn query(&self, lookup: PaymentLookup) -> ProviderResult<Payment> {
        let order = lookup.value()?.to_owned();
        if !matches!(lookup, PaymentLookup::MerchantOrderId(_)) {
            return Err(ProviderError::Unsupported);
        }
        let token = self.access_token().await?;
        let mut url = reqwest::Url::parse(&self.config.query_url)
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("access_token", &token)
            .append_pair("orderno", &order);
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            status: String,
            #[serde(default)]
            err_no: i64,
            #[serde(default)]
            errno: i64,
            #[serde(default)]
            err_msg: String,
        }
        let response: Response = decode_response(
            self.client
                .get(url)
                .send()
                .await
                .map_err(|_| ProviderError::Unavailable)?,
        )
        .await?;
        if response.err_no != 0 || response.errno != 0 {
            return Err(ProviderError::Rejected(response.err_msg));
        }
        Ok(Payment {
            merchant_order_id: order,
            provider_order_id: None,
            status: map_status(&response.status),
            amount: None,
            payer_id: None,
            paid_at: None,
        })
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        let envelope = self.decode_envelope(&request)?;
        self.verify_envelope(&envelope)?;
        let body: Body = serde_json::from_str(&envelope.msg).map_err(|_| {
            ProviderError::InvalidResponse("invalid Douyin callback message".into())
        })?;
        let currency = if body.currency.is_empty() {
            "CNY".to_owned()
        } else {
            body.currency.to_uppercase()
        };
        if body.appid != self.config.app_id
            || body.cp_orderno.is_empty()
            || body.order_no_channel.is_empty()
            || currency != "CNY"
        {
            return Err(ProviderError::InvalidResponse(
                "Douyin callback identity mismatch".into(),
            ));
        }
        let amount = Money {
            currency,
            minor_units: body.amount_cent,
        };
        amount.validate_response()?;
        Ok(PaymentEvent {
            event_id: body.order_no_channel.clone(),
            merchant_order_id: body.cp_orderno,
            provider_order_id: body.order_no_channel,
            original_provider_order_id: None,
            payer_id: None,
            product_id: None,
            quantity: 1,
            status: PaymentStatus::Succeeded,
            amount,
            environment: None,
            occurred_at: None,
        })
    }
}

async fn decode_response<T: DeserializeOwned>(response: reqwest::Response) -> ProviderResult<T> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ProviderError::Unavailable)?;
    if bytes.len() > 1024 * 1024 {
        return Err(ProviderError::InvalidResponse(
            "Douyin response too large".into(),
        ));
    }
    if status.as_u16() == 429 {
        return Err(ProviderError::RateLimited { retry_after: None });
    }
    if !status.is_success() {
        return Err(ProviderError::Rejected(format!("Douyin HTTP {status}")));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| ProviderError::InvalidResponse("invalid Douyin response".into()))
}

fn validate_endpoint(value: &str) -> ProviderResult<()> {
    let url =
        reqwest::Url::parse(value).map_err(|error| ProviderError::Config(error.to_string()))?;
    let local_http = matches!(url.host_str(), Some("127.0.0.1" | "localhost"));
    if url.scheme() != "https" && !local_http {
        return Err(ProviderError::Config(
            "Douyin endpoints must use HTTPS".into(),
        ));
    }
    Ok(())
}

fn map_status(value: &str) -> PaymentStatus {
    match value.to_ascii_uppercase().as_str() {
        "SUCCESS" | "PAID" | "1" => PaymentStatus::Succeeded,
        "FAIL" | "FAILED" | "-1" => PaymentStatus::Failed,
        "CLOSED" | "CANCEL" | "CANCELED" => PaymentStatus::Closed,
        "REFUND" | "REFUNDED" => PaymentStatus::Refunded,
        _ => PaymentStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> DouyinPayment {
        DouyinPayment::new(DouyinConfig {
            callback_token: "0123456789abcdef".into(),
            app_id: "app".into(),
            app_secret: "secret".into(),
            token_url: String::new(),
            query_url: String::new(),
            timeout: Duration::ZERO,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn signed_callback_produces_payment_event() {
        let provider = provider();
        let message = serde_json::json!({"appid":"app","cp_orderno":"merchant","order_no_channel":"channel","amount_cent":100,"currency":"CNY"}).to_string();
        let signature = provider.signature("10", "n", &message);
        let body =
            serde_json::json!({"timestamp":"10","nonce":"n","msg":message,"signature":signature})
                .to_string()
                .into_bytes();
        let event = provider
            .verify_notification(NotificationRequest::new(
                http::Method::POST,
                "/".parse().unwrap(),
                http::HeaderMap::new(),
                body,
            ))
            .await
            .unwrap();
        assert_eq!(event.provider_order_id, "channel");
        assert_eq!(event.amount.minor_units, 100);
    }

    #[test]
    fn callback_url_echo_is_authenticated() {
        let provider = provider();
        let signature = provider.signature("10", "n", "");
        let request = NotificationRequest::new(
            http::Method::GET,
            format!("/?timestamp=10&nonce=n&echostr=echo&signature={signature}")
                .parse()
                .unwrap(),
            http::HeaderMap::new(),
            bytes::Bytes::new(),
        );
        assert_eq!(provider.verify_callback_url(&request).unwrap(), "echo");
    }
}
