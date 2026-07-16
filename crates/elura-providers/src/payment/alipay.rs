//! Alipay RSA2 checkout, query and notification verification.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::Local;
use ring::{rand::SystemRandom, rsa, signature};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::payment::rsa_signature::{parse_private_key, parse_public_key as parse_rsa_public_key};
use crate::payment::{
    CheckoutRequest, CheckoutResult, ClientPayload, Money, NotificationRequest, Payment,
    PaymentEvent, PaymentLookup, PaymentProvider, PaymentStatus,
};
use crate::{ProviderError, ProviderResult};

/// Alipay client integration used to complete checkout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AlipayClientMode {
    /// Native Alipay application SDK.
    #[default]
    App,
    /// Browser or mobile-web checkout.
    Web,
}

/// Provider-specific options for an Alipay checkout.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlipayCheckoutOptions {
    /// Client integration used to complete checkout.
    #[serde(default)]
    pub client_mode: AlipayClientMode,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct AlipayConfig {
    pub app_id: String,
    pub private_key_pem: String,
    /// Alipay RSA public key PEM or Alipay public certificate PEM.
    pub alipay_public_key_pem: String,
    pub gateway: String,
    /// Provider-owned asynchronous notification endpoint.
    pub notify_url: Option<String>,
    pub timeout: Duration,
}

impl AlipayConfig {
    pub fn production(
        app_id: impl Into<String>,
        private_key_pem: impl Into<String>,
        alipay_public_key_pem: impl Into<String>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            private_key_pem: private_key_pem.into(),
            alipay_public_key_pem: alipay_public_key_pem.into(),
            gateway: "https://openapi.alipay.com/gateway.do".into(),
            notify_url: None,
            timeout: Duration::from_secs(10),
        }
    }
}

pub struct AlipayPayment {
    app_id: String,
    private_key: rsa::KeyPair,
    public_key: Vec<u8>,
    gateway: String,
    notify_url: Option<String>,
    client: reqwest::Client,
}

impl AlipayPayment {
    pub fn new(config: AlipayConfig) -> ProviderResult<Self> {
        if config.app_id.trim().is_empty()
            || config.gateway.trim().is_empty()
            || config.timeout.is_zero()
        {
            return Err(ProviderError::Config("invalid Alipay configuration".into()));
        }
        let gateway = reqwest::Url::parse(config.gateway.trim())
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        if gateway.scheme() != "https" {
            return Err(ProviderError::Config(
                "Alipay gateway must use HTTPS".into(),
            ));
        }
        let private_key = parse_private_key(&config.private_key_pem, "Alipay")?;
        let public_key = parse_public_key(&config.alipay_public_key_pem)?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        Ok(Self {
            app_id: config.app_id.trim().into(),
            private_key,
            public_key,
            gateway: gateway.into(),
            notify_url: config.notify_url.filter(|value| !value.trim().is_empty()),
            client,
        })
    }

    fn parameters(&self, method: &str, biz_content: String) -> BTreeMap<String, String> {
        let mut values = BTreeMap::from([
            ("app_id".into(), self.app_id.clone()),
            ("biz_content".into(), biz_content),
            ("charset".into(), "utf-8".into()),
            ("format".into(), "JSON".into()),
            ("method".into(), method.into()),
            ("sign_type".into(), "RSA2".into()),
            (
                "timestamp".into(),
                Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            ),
            ("version".into(), "1.0".into()),
        ]);
        if let Some(notify_url) = &self.notify_url {
            values.insert("notify_url".into(), notify_url.clone());
        }
        values
    }

    fn sign_parameters(&self, mut values: BTreeMap<String, String>) -> ProviderResult<String> {
        let canonical = canonical(&values);
        let mut signature = vec![0; self.private_key.public().modulus_len()];
        self.private_key
            .sign(
                &signature::RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                canonical.as_bytes(),
                &mut signature,
            )
            .map_err(|_| ProviderError::Config("Alipay RSA signing failed".into()))?;
        values.insert("sign".into(), STANDARD.encode(signature));
        Ok(form_encode(&values))
    }

    fn verify(&self, content: &[u8], signature: &str) -> ProviderResult<()> {
        let signature = STANDARD
            .decode(signature.trim())
            .map_err(|_| ProviderError::InvalidSignature)?;
        signature::UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, &self.public_key)
            .verify(content, &signature)
            .map_err(|_| ProviderError::InvalidSignature)
    }
}

#[async_trait]
impl PaymentProvider for AlipayPayment {
    fn name(&self) -> &str {
        "alipay"
    }

    fn capabilities(&self) -> super::PaymentCapabilities {
        super::PaymentCapabilities {
            checkout: true,
            query: true,
            notification: true,
            refund: false,
            purchase: false,
        }
    }

    async fn create(&self, request: CheckoutRequest) -> ProviderResult<CheckoutResult> {
        request.validate()?;
        if request.amount.currency != "CNY"
            || request.merchant_order_id.trim().is_empty()
            || request.subject.trim().is_empty()
        {
            return Err(ProviderError::InvalidRequest(
                "Alipay requires order, subject and positive CNY amount".into(),
            ));
        }
        let options = request.provider_options_or_default::<AlipayCheckoutOptions>()?;
        let (method, product_code) = match options.client_mode {
            AlipayClientMode::App => ("alipay.trade.app.pay", "QUICK_MSECURITY_PAY"),
            AlipayClientMode::Web => ("alipay.trade.wap.pay", "QUICK_WAP_WAY"),
        };
        let biz_content = serde_json::json!({
            "subject": request.subject,
            "out_trade_no": request.merchant_order_id,
            "total_amount": format_amount(request.amount.minor_units),
            "product_code": product_code,
        })
        .to_string();
        let payload = self.sign_parameters(self.parameters(method, biz_content))?;
        Ok(CheckoutResult {
            provider_order_id: None,
            client_payload: ClientPayload::AppParameters(payload),
            expires_at: None,
        })
    }

    async fn query(&self, lookup: PaymentLookup) -> ProviderResult<Payment> {
        let order = lookup.value()?.to_owned();
        let biz_content = match lookup {
            PaymentLookup::MerchantOrderId(_) => serde_json::json!({ "out_trade_no": order }),
            PaymentLookup::ProviderOrderId(_) => serde_json::json!({ "trade_no": order }),
        };
        let body =
            self.sign_parameters(self.parameters("alipay.trade.query", biz_content.to_string()))?;
        let response = self
            .client
            .post(&self.gateway)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
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
        let envelope: QueryEnvelope = serde_json::from_slice(&bytes)
            .map_err(|_| ProviderError::InvalidResponse("invalid Alipay query".into()))?;
        self.verify(envelope.response.get().as_bytes(), &envelope.sign)?;
        let result: QueryBody = serde_json::from_str(envelope.response.get())
            .map_err(|_| ProviderError::InvalidResponse("invalid Alipay query body".into()))?;
        if result.code != "10000" {
            return Err(ProviderError::Rejected(
                result
                    .sub_msg
                    .unwrap_or_else(|| result.msg.unwrap_or(result.code)),
            ));
        }
        Ok(Payment {
            merchant_order_id: result.out_trade_no,
            provider_order_id: Some(result.trade_no),
            status: map_status(&result.trade_status),
            amount: Some(Money {
                currency: "CNY".into(),
                minor_units: parse_amount(&result.total_amount)?,
            }),
            payer_id: None,
            paid_at: None,
        })
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        let mut values: HashMap<String, String> = url::form_urlencoded::parse(&request.body)
            .into_owned()
            .collect();
        for (key, value) in url::form_urlencoded::parse(request.query().as_bytes()) {
            values.entry(key.into_owned()).or_insert(value.into_owned());
        }
        let signature = values
            .remove("sign")
            .ok_or(ProviderError::InvalidSignature)?;
        values.remove("sign_type");
        let ordered: BTreeMap<_, _> = values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        self.verify(canonical(&ordered).as_bytes(), &signature)?;
        if values.get("app_id") != Some(&self.app_id) {
            return Err(ProviderError::InvalidResponse(
                "Alipay app id mismatch".into(),
            ));
        }
        let merchant_order_id = required(&values, "out_trade_no")?;
        let provider_order_id = required(&values, "trade_no")?;
        let status = map_status(required(&values, "trade_status")?);
        let event_id = values
            .get("notify_id")
            .cloned()
            .unwrap_or_else(|| format!("{provider_order_id}:{}", values["trade_status"].as_str()));
        Ok(PaymentEvent {
            event_id,
            merchant_order_id: merchant_order_id.into(),
            provider_order_id: provider_order_id.into(),
            original_provider_order_id: None,
            payer_id: values.get("buyer_id").cloned(),
            product_id: None,
            quantity: 1,
            status,
            amount: Money {
                currency: "CNY".into(),
                minor_units: parse_amount(required(&values, "total_amount")?)?,
            },
            environment: None,
            occurred_at: None,
        })
    }
}

#[derive(Deserialize)]
struct QueryEnvelope {
    #[serde(rename = "alipay_trade_query_response")]
    response: Box<RawValue>,
    sign: String,
}

#[derive(Deserialize)]
struct QueryBody {
    code: String,
    msg: Option<String>,
    sub_msg: Option<String>,
    #[serde(default)]
    out_trade_no: String,
    #[serde(default)]
    trade_no: String,
    #[serde(default)]
    trade_status: String,
    #[serde(default)]
    total_amount: String,
}

fn canonical(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn form_encode(values: &BTreeMap<String, String>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in values {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

fn format_amount(minor: i64) -> String {
    format!("{}.{:02}", minor / 100, minor % 100)
}

fn parse_amount(value: &str) -> ProviderResult<i64> {
    let (major, decimal) = value.trim().split_once('.').unwrap_or((value.trim(), ""));
    if major.is_empty() || decimal.len() > 2 || major.starts_with('-') {
        return Err(ProviderError::InvalidResponse(
            "invalid Alipay amount".into(),
        ));
    }
    let major: i64 = major
        .parse()
        .map_err(|_| ProviderError::InvalidResponse("invalid Alipay amount".into()))?;
    let decimal: i64 = format!("{decimal:0<2}")
        .parse()
        .map_err(|_| ProviderError::InvalidResponse("invalid Alipay amount".into()))?;
    major
        .checked_mul(100)
        .and_then(|value| value.checked_add(decimal))
        .filter(|value| *value > 0)
        .ok_or_else(|| ProviderError::InvalidResponse("invalid Alipay amount".into()))
}

fn map_status(value: &str) -> PaymentStatus {
    match value {
        "TRADE_SUCCESS" | "TRADE_FINISHED" => PaymentStatus::Succeeded,
        "TRADE_CLOSED" => PaymentStatus::Closed,
        "WAIT_BUYER_PAY" => PaymentStatus::Pending,
        _ => PaymentStatus::Failed,
    }
}

fn required<'a>(values: &'a HashMap<String, String>, key: &str) -> ProviderResult<&'a str> {
    values
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or_else(|| ProviderError::InvalidResponse(format!("missing Alipay {key}")))
}

fn parse_public_key(pem: &str) -> ProviderResult<Vec<u8>> {
    parse_rsa_public_key(pem, "Alipay")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts_use_minor_units_without_floats() {
        assert_eq!(parse_amount("1").unwrap(), 100);
        assert_eq!(parse_amount("1.01").unwrap(), 101);
        assert_eq!(parse_amount("0.10").unwrap(), 10);
        for invalid in ["", "1.001", "-1.00", "x", "0"] {
            assert!(parse_amount(invalid).is_err());
        }
    }
}
