//! WeChat Mini Game virtual payment checkout and authenticated callbacks.

use aes::Aes256;
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use cbc::cipher::{BlockDecryptMut, KeyIvInit, block_padding::NoPadding};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use super::{
    CheckoutRequest, CheckoutResult, ClientPayload, Money, NotificationRequest,
    PaymentCapabilities, PaymentEvent, PaymentProvider, PaymentStatus,
};
use crate::{ProviderError, ProviderResult};

type HmacSha256 = Hmac<Sha256>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;
const METHOD: &str = "requestMidasPaymentGameItem";

/// Provider-specific options for WeChat Mini Game checkout.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WechatMiniCheckoutOptions {
    /// WeChat session key used to sign the client payload.
    pub session_key: String,
    /// Client platform, normally `android` or `ios`.
    pub platform: String,
    /// Opaque application data returned with the payment callback.
    #[serde(default)]
    pub attach: Option<String>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct WechatMiniConfig {
    pub app_id: String,
    pub app_key: String,
    pub offer_id: String,
    pub environment: u32,
    pub callback_token: Option<String>,
    pub encoding_aes_key: Option<String>,
}

impl WechatMiniConfig {
    /// Creates WeChat Mini Game payment configuration without callback encryption.
    pub fn new(
        app_id: impl Into<String>,
        app_key: impl Into<String>,
        offer_id: impl Into<String>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            app_key: app_key.into(),
            offer_id: offer_id.into(),
            environment: 0,
            callback_token: None,
            encoding_aes_key: None,
        }
    }
}

pub struct WechatMiniPayment {
    config: WechatMiniConfig,
}

impl WechatMiniPayment {
    pub fn new(config: WechatMiniConfig) -> ProviderResult<Self> {
        if config.app_id.trim().is_empty()
            || config.app_key.len() < 16
            || config.offer_id.trim().is_empty()
            || (config.encoding_aes_key.is_some()
                && config.callback_token.as_deref().is_none_or(str::is_empty))
        {
            return Err(ProviderError::Config(
                "invalid WeChat Mini Game payment configuration".into(),
            ));
        }
        if let Some(key) = config.encoding_aes_key.as_deref() {
            let decoded = STANDARD
                .decode(format!("{key}="))
                .map_err(|_| ProviderError::Config("invalid callback AES key".into()))?;
            if decoded.len() != 32 {
                return Err(ProviderError::Config(
                    "callback AES key must decode to 32 bytes".into(),
                ));
            }
        }
        Ok(Self { config })
    }

    pub fn verify_callback_url(&self, query: &str) -> ProviderResult<String> {
        if query.len() > 8192 {
            return Err(ProviderError::InvalidResponse(
                "WeChat callback query exceeds limit".into(),
            ));
        }
        let values = query_values(query);
        let token = self
            .config
            .callback_token
            .as_deref()
            .ok_or(ProviderError::InvalidSignature)?;
        let echo = value(&values, "echostr")?;
        let expected = sha1_signature(&[
            token,
            value(&values, "timestamp")?,
            value(&values, "nonce")?,
        ]);
        constant_equal(value(&values, "signature")?, &expected)?;
        Ok(echo.to_owned())
    }

    fn decrypt(&self, encoded: &str) -> ProviderResult<Vec<u8>> {
        let encoded_key = self
            .config
            .encoding_aes_key
            .as_deref()
            .ok_or(ProviderError::InvalidSignature)?;
        let key = STANDARD
            .decode(format!("{encoded_key}="))
            .map_err(|_| ProviderError::InvalidSignature)?;
        let mut ciphertext = STANDARD
            .decode(encoded)
            .map_err(|_| ProviderError::InvalidSignature)?;
        if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
            return Err(ProviderError::InvalidSignature);
        }
        let iv = &key[..16];
        let plain = Aes256CbcDec::new_from_slices(&key, iv)
            .map_err(|_| ProviderError::InvalidSignature)?
            .decrypt_padded_mut::<NoPadding>(&mut ciphertext)
            .map_err(|_| ProviderError::InvalidSignature)?;
        let padding = plain
            .last()
            .copied()
            .map(usize::from)
            .ok_or(ProviderError::InvalidSignature)?;
        if padding == 0
            || padding > 32
            || padding > plain.len()
            || !plain[plain.len() - padding..]
                .iter()
                .all(|byte| usize::from(*byte) == padding)
        {
            return Err(ProviderError::InvalidSignature);
        }
        let plain = &plain[..plain.len() - padding];
        if plain.len() < 20 {
            return Err(ProviderError::InvalidSignature);
        }
        let length = u32::from_be_bytes(
            plain[16..20]
                .try_into()
                .map_err(|_| ProviderError::InvalidSignature)?,
        ) as usize;
        let end = 20_usize
            .checked_add(length)
            .ok_or(ProviderError::InvalidSignature)?;
        if end > plain.len() || plain.get(end..) != Some(self.config.app_id.as_bytes()) {
            return Err(ProviderError::InvalidSignature);
        }
        Ok(plain[20..end].to_vec())
    }

    fn decode_callback(&self, body: &[u8], depth: u8) -> ProviderResult<(Callback, bool)> {
        if depth > 2 {
            return Err(ProviderError::InvalidResponse(
                "nested callback too deep".into(),
            ));
        }
        let decoded: Callback = serde_json::from_slice(body)
            .map_err(|_| ProviderError::InvalidResponse("invalid WeChat Mini callback".into()))?;
        let Some(mini_game) = decoded
            .mini_game
            .as_ref()
            .filter(|value| !value.payload.is_empty())
        else {
            return Ok((decoded, false));
        };
        let expected = hmac_hex(
            &self.config.app_key,
            format!("{}&{}", decoded.event, mini_game.payload).as_bytes(),
        )?;
        constant_equal(&mini_game.pay_event_sig, &expected)?;
        let (mut inner, _) = self.decode_callback(mini_game.payload.as_bytes(), depth + 1)?;
        if inner.event.is_empty() {
            inner.event = decoded.event;
        }
        Ok((inner, true))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignData<'a> {
    mode: &'a str,
    offer_id: &'a str,
    buy_quantity: u32,
    env: u32,
    currency_type: &'a str,
    platform: &'a str,
    product_id: &'a str,
    goods_price: i64,
    out_trade_no: &'a str,
    attach: &'a str,
}

#[async_trait]
impl PaymentProvider for WechatMiniPayment {
    fn name(&self) -> &str {
        "wechatmini"
    }
    fn capabilities(&self) -> PaymentCapabilities {
        PaymentCapabilities {
            checkout: true,
            notification: true,
            ..Default::default()
        }
    }

    async fn create(&self, request: CheckoutRequest) -> ProviderResult<CheckoutResult> {
        request.validate()?;
        if request.amount.currency != "CNY" || request.merchant_order_id.trim().is_empty() {
            return Err(ProviderError::InvalidRequest(
                "WeChat Mini requires a positive CNY order".into(),
            ));
        }
        let product_id = request
            .product_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ProviderError::InvalidRequest("product id is required".into()))?;
        let options = request.provider_options::<WechatMiniCheckoutOptions>()?;
        let session_key = options.session_key.trim();
        let platform = options.platform.trim();
        if session_key.is_empty() || session_key.len() > 512 || platform.is_empty() {
            return Err(ProviderError::InvalidRequest(
                "WeChat Mini session key and platform are required".into(),
            ));
        }
        let sign_data = serde_json::to_string(&SignData {
            mode: "goods",
            offer_id: &self.config.offer_id,
            buy_quantity: request.quantity,
            env: self.config.environment,
            currency_type: "CNY",
            platform,
            product_id,
            goods_price: request.amount.minor_units,
            out_trade_no: &request.merchant_order_id,
            attach: options.attach.as_deref().unwrap_or_default(),
        })
        .map_err(|_| ProviderError::Unavailable)?;
        Ok(CheckoutResult {
            provider_order_id: None,
            client_payload: ClientPayload::Json(serde_json::json!({
                "sign_data": sign_data, "mode": "goods",
                "pay_sig": hmac_hex(&self.config.app_key, format!("{METHOD}&{sign_data}").as_bytes())?,
                "signature": hmac_hex(session_key, sign_data.as_bytes())?
            })),
            expires_at: None,
        })
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        let values = query_values(request.query());
        let mut body = request.body;
        let mut verified = false;
        if let Some(token) = self.config.callback_token.as_deref() {
            let expected = sha1_signature(&[
                token,
                value(&values, "timestamp")?,
                value(&values, "nonce")?,
            ]);
            constant_equal(value(&values, "signature")?, &expected)?;
            verified = true;
            if values
                .get("encrypt_type")
                .is_some_and(|value| value.eq_ignore_ascii_case("aes"))
            {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Encrypted {
                    #[serde(rename = "Encrypt")]
                    encrypt: String,
                }
                let envelope: Encrypted = serde_json::from_slice(&body).map_err(|_| {
                    ProviderError::InvalidResponse("invalid encrypted callback".into())
                })?;
                body = self.decrypt(&envelope.encrypt)?.into();
            }
        }
        let (decoded, pay_event_verified) = self.decode_callback(&body, 0)?;
        verified |= pay_event_verified;
        if !verified {
            return Err(ProviderError::InvalidSignature);
        }
        if !matches!(
            decoded.event.as_str(),
            "minigame_game_pay_goods_deliver_notify"
                | "xpay_goods_deliver_notify"
                | "minigame_h5_goods_deliver_notify"
                | "minigame_deliver_h5_pay_products"
        ) {
            return Err(ProviderError::InvalidResponse(
                "unsupported WeChat Mini event".into(),
            ));
        }
        let goods = decoded
            .goods_info
            .ok_or_else(|| ProviderError::InvalidResponse("missing goods info".into()))?;
        let pay = decoded
            .pay_info
            .ok_or_else(|| ProviderError::InvalidResponse("missing payment info".into()))?;
        let merchant_order_id = if decoded.out_trade_no.is_empty() {
            pay.merchant_order_id
        } else {
            decoded.out_trade_no
        };
        if merchant_order_id.is_empty()
            || goods.product_id.is_empty()
            || pay.transaction_id.is_empty()
            || goods.quantity == 0
            || goods.actual_price <= 0
        {
            return Err(ProviderError::InvalidResponse(
                "invalid WeChat Mini payment fields".into(),
            ));
        }
        Ok(PaymentEvent {
            event_id: pay.transaction_id.clone(),
            merchant_order_id,
            provider_order_id: pay.transaction_id,
            original_provider_order_id: None,
            payer_id: nonempty(decoded.open_id),
            product_id: Some(goods.product_id),
            quantity: goods.quantity,
            status: PaymentStatus::Succeeded,
            amount: Money {
                currency: "CNY".into(),
                minor_units: goods.actual_price,
            },
            environment: Some(self.config.environment.to_string()),
            occurred_at: None,
        })
    }
}

#[derive(Deserialize, Default)]
struct Callback {
    #[serde(rename = "Event", alias = "event", default)]
    event: String,
    #[serde(rename = "OpenId", alias = "open_id", default)]
    open_id: String,
    #[serde(rename = "OutTradeNo", alias = "outTradeNo", default)]
    out_trade_no: String,
    #[serde(rename = "MiniGame", default)]
    mini_game: Option<MiniGame>,
    #[serde(rename = "GoodsInfo", alias = "goodsInfo", default)]
    goods_info: Option<GoodsInfo>,
    #[serde(rename = "WeChatPayInfo", alias = "wechatPayInfo", default)]
    pay_info: Option<PayInfo>,
}
#[derive(Deserialize)]
struct MiniGame {
    #[serde(rename = "Payload")]
    payload: String,
    #[serde(rename = "PayEventSig")]
    pay_event_sig: String,
}
#[derive(Deserialize)]
struct GoodsInfo {
    #[serde(rename = "ProductId", alias = "productId")]
    product_id: String,
    #[serde(rename = "Quantity", alias = "quantity")]
    quantity: u32,
    #[serde(rename = "ActualPrice", alias = "actualPrice")]
    actual_price: i64,
}
#[derive(Deserialize)]
struct PayInfo {
    #[serde(rename = "MchOrderNo", alias = "mchOrderNo")]
    merchant_order_id: String,
    #[serde(rename = "TransactionId", alias = "transactionId")]
    transaction_id: String,
}

fn hmac_hex(key: &str, message: &[u8]) -> ProviderResult<String> {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes())
        .map_err(|_| ProviderError::Config("invalid HMAC key".into()))?;
    mac.update(message);
    Ok(hex::encode(mac.finalize().into_bytes()))
}
fn sha1_signature(values: &[&str]) -> String {
    let mut values = values.to_vec();
    values.sort_unstable();
    hex::encode(Sha1::digest(values.concat().as_bytes()))
}
fn constant_equal(left: &str, right: &str) -> ProviderResult<()> {
    if left
        .trim()
        .to_ascii_lowercase()
        .as_bytes()
        .ct_eq(right.trim().to_ascii_lowercase().as_bytes())
        .unwrap_u8()
        == 1
    {
        Ok(())
    } else {
        Err(ProviderError::InvalidSignature)
    }
}
fn query_values(query: &str) -> std::collections::HashMap<String, String> {
    url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}
fn value<'a>(
    values: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> ProviderResult<&'a str> {
    values
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProviderError::InvalidSignature)
}
fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(token: Option<&str>) -> WechatMiniPayment {
        WechatMiniPayment::new(WechatMiniConfig {
            app_id: "wx-app".into(),
            app_key: "0123456789abcdef".into(),
            offer_id: "offer".into(),
            environment: 1,
            callback_token: token.map(str::to_owned),
            encoding_aes_key: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn nested_pay_event_signature_authenticates_callback() {
        let provider = provider(None);
        let event = "minigame_game_pay_goods_deliver_notify";
        let payload = serde_json::json!({
            "Event": event, "OpenId": "user", "OutTradeNo": "merchant-1",
            "GoodsInfo": {"ProductId": "gems", "Quantity": 2, "ActualPrice": 600},
            "WeChatPayInfo": {"MchOrderNo": "merchant-1", "TransactionId": "wx-1"}
        })
        .to_string();
        let signature =
            hmac_hex("0123456789abcdef", format!("{event}&{payload}").as_bytes()).unwrap();
        let body = serde_json::json!({"Event": event, "MiniGame": {"Payload": payload, "PayEventSig": signature}}).to_string().into_bytes();
        let payment = provider
            .verify_notification(NotificationRequest::new(
                http::Method::POST,
                "/callback".parse().unwrap(),
                http::HeaderMap::new(),
                body,
            ))
            .await
            .unwrap();
        assert_eq!(payment.merchant_order_id, "merchant-1");
        assert_eq!(payment.amount.minor_units, 600);
        assert_eq!(payment.quantity, 2);
    }

    #[test]
    fn callback_url_requires_valid_sha1_signature() {
        let provider = provider(Some("callback-secret"));
        let signature = sha1_signature(&["callback-secret", "10", "nonce"]);
        assert_eq!(
            provider
                .verify_callback_url(&format!(
                    "timestamp=10&nonce=nonce&echostr=ok&signature={signature}"
                ))
                .unwrap(),
            "ok"
        );
    }
}
