//! QuickSDK callback verification and legacy payload decoding.

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use subtle::ConstantTimeEq;

use super::{Money, NotificationRequest, PaymentEvent, PaymentProvider, PaymentStatus};
use crate::{ProviderError, ProviderResult};

pub struct QuickSdkPayment {
    md5_key: String,
    callback_key: Vec<u8>,
    test: bool,
}

impl QuickSdkPayment {
    pub fn new(
        md5_key: impl Into<String>,
        callback_key: impl Into<Vec<u8>>,
        test: bool,
    ) -> ProviderResult<Self> {
        let provider = Self {
            md5_key: md5_key.into(),
            callback_key: callback_key.into(),
            test,
        };
        if provider.md5_key.is_empty() || provider.callback_key.is_empty() {
            return Err(ProviderError::Config(
                "QuickSDK callback keys are required".into(),
            ));
        }
        Ok(provider)
    }

    fn decrypt(&self, input: &str) -> ProviderResult<String> {
        if !input.starts_with('@') {
            return Ok(input.to_owned());
        }
        let mut decoded = String::new();
        for (index, part) in input[1..]
            .split('@')
            .filter(|part| !part.is_empty())
            .enumerate()
        {
            let encoded: i32 = part
                .parse()
                .map_err(|_| ProviderError::InvalidResponse("invalid QuickSDK payload".into()))?;
            let value = encoded - i32::from(self.callback_key[index % self.callback_key.len()]);
            decoded.push(char::from_u32(value as u32).ok_or_else(|| {
                ProviderError::InvalidResponse("invalid QuickSDK character".into())
            })?);
        }
        if decoded.is_empty() {
            return Err(ProviderError::InvalidResponse(
                "empty QuickSDK payload".into(),
            ));
        }
        Ok(decoded)
    }
}

#[derive(Deserialize)]
#[serde(rename = "quicksdk_message")]
struct QuickMessage {
    message: QuickBody,
}

#[derive(Deserialize)]
struct QuickBody {
    is_test: Option<String>,
    channel_uid: String,
    game_order: String,
    order_no: String,
    amount: String,
    status: String,
}

#[async_trait]
impl PaymentProvider for QuickSdkPayment {
    fn name(&self) -> &str {
        "quicksdk"
    }

    fn capabilities(&self) -> super::PaymentCapabilities {
        super::PaymentCapabilities {
            notification: true,
            ..Default::default()
        }
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        let mut values: HashMap<String, String> = url::form_urlencoded::parse(&request.body)
            .into_owned()
            .collect();
        values.extend(url::form_urlencoded::parse(request.query().as_bytes()).into_owned());
        let data = values
            .get("nt_data")
            .ok_or_else(|| ProviderError::InvalidResponse("missing nt_data".into()))?;
        let sign = values
            .get("sign")
            .ok_or_else(|| ProviderError::InvalidResponse("missing sign".into()))?;
        let md5_sign = values
            .get("md5Sign")
            .ok_or_else(|| ProviderError::InvalidResponse("missing md5Sign".into()))?;
        use sha2::Digest;
        let expected = format!(
            "{:x}",
            md5::Md5::digest(format!("{data}{sign}{}", self.md5_key))
        );
        if expected
            .to_lowercase()
            .as_bytes()
            .ct_eq(md5_sign.trim().to_lowercase().as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(ProviderError::InvalidSignature);
        }
        let message: QuickMessage = quick_xml::de::from_str(&self.decrypt(data)?)
            .map_err(|_| ProviderError::InvalidResponse("invalid QuickSDK XML".into()))?;
        let body = message.message;
        let expected_test = if self.test { "1" } else { "0" };
        if body
            .is_test
            .as_deref()
            .is_some_and(|value| value != expected_test)
            || body.game_order.is_empty()
            || body.order_no.is_empty()
        {
            return Err(ProviderError::InvalidResponse(
                "QuickSDK callback mismatch".into(),
            ));
        }
        let minor_units = parse_decimal_minor(&body.amount)?;
        let status = match body.status.as_str() {
            "0" => PaymentStatus::Succeeded,
            "1" => PaymentStatus::Failed,
            _ => {
                return Err(ProviderError::InvalidResponse(
                    "unknown QuickSDK status".into(),
                ));
            }
        };
        Ok(PaymentEvent {
            event_id: body.order_no.clone(),
            merchant_order_id: body.game_order,
            provider_order_id: body.order_no,
            original_provider_order_id: None,
            payer_id: Some(body.channel_uid),
            product_id: None,
            quantity: 1,
            status,
            amount: Money {
                currency: "CNY".into(),
                minor_units,
            },
            environment: None,
            occurred_at: None,
        })
    }
}

fn parse_decimal_minor(value: &str) -> ProviderResult<i64> {
    let (major, fraction) = value.trim().split_once('.').unwrap_or((value.trim(), ""));
    if fraction.len() > 2 || major.is_empty() {
        return Err(ProviderError::InvalidResponse(
            "invalid decimal amount".into(),
        ));
    }
    let major: i64 = major
        .parse()
        .map_err(|_| ProviderError::InvalidResponse("invalid decimal amount".into()))?;
    let fraction: i64 = format!("{fraction:0<2}")
        .parse()
        .map_err(|_| ProviderError::InvalidResponse("invalid decimal amount".into()))?;
    let result = major
        .checked_mul(100)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| ProviderError::InvalidResponse("amount overflow".into()))?;
    if result <= 0 {
        return Err(ProviderError::InvalidResponse(
            "amount must be positive".into(),
        ));
    }
    Ok(result)
}
