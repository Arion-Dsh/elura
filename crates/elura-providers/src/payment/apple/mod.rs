//! App Store Server API and StoreKit 2 signed transaction verification.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use p256::{
    ecdsa::{
        Signature, SigningKey, VerifyingKey,
        signature::{Signer, Verifier},
    },
    pkcs8::DecodePrivateKey,
};
use serde::{Deserialize, de::DeserializeOwned};
use x509_parser::{certificate::X509Certificate, prelude::FromDer};

use super::{
    Money, NotificationRequest, PaymentCapabilities, PaymentEvent, PaymentProvider, PaymentStatus,
    Purchase, PurchaseRequest,
};
use crate::{ProviderError, ProviderResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppleEnvironment {
    Production,
    Sandbox,
}

impl AppleEnvironment {
    fn as_str(self) -> &'static str {
        match self {
            Self::Production => "Production",
            Self::Sandbox => "Sandbox",
        }
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct AppleConfig {
    pub issuer_id: String,
    pub bundle_id: String,
    pub app_apple_id: Option<i64>,
    pub key_id: String,
    pub private_key_pem: String,
    pub environment: AppleEnvironment,
    pub base_url: String,
    pub timeout: Duration,
    /// DER-encoded Apple root certificates trusted for StoreKit JWS chains.
    ///
    /// Keep this list restricted to the Apple roots documented for App Store
    /// Server API certificate chains. General WebPKI roots are not accepted.
    pub trusted_roots_der: Vec<Vec<u8>>,
}

impl AppleConfig {
    /// Creates App Store configuration without roots or an app numeric ID.
    pub fn new(
        issuer_id: impl Into<String>,
        bundle_id: impl Into<String>,
        key_id: impl Into<String>,
        private_key_pem: impl Into<String>,
        environment: AppleEnvironment,
    ) -> Self {
        Self {
            issuer_id: issuer_id.into(),
            bundle_id: bundle_id.into(),
            app_apple_id: None,
            key_id: key_id.into(),
            private_key_pem: private_key_pem.into(),
            environment,
            base_url: String::new(),
            timeout: Duration::from_secs(10),
            trusted_roots_der: Vec::new(),
        }
    }

    /// Sets the numeric App Store application identifier.
    pub fn with_app_apple_id(mut self, app_apple_id: i64) -> Self {
        self.app_apple_id = Some(app_apple_id);
        self
    }

    /// Sets the Apple root certificates trusted for StoreKit JWS chains.
    pub fn with_trusted_roots(mut self, trusted_roots_der: Vec<Vec<u8>>) -> Self {
        self.trusted_roots_der = trusted_roots_der;
        self
    }
}

pub struct ApplePayment {
    issuer_id: String,
    bundle_id: String,
    app_apple_id: Option<i64>,
    key_id: String,
    signing_key: SigningKey,
    environment: AppleEnvironment,
    base_url: reqwest::Url,
    client: reqwest::Client,
    trusted_roots_der: Vec<Vec<u8>>,
}

impl ApplePayment {
    pub fn new(mut config: AppleConfig) -> ProviderResult<Self> {
        if config.base_url.is_empty() {
            config.base_url = match config.environment {
                AppleEnvironment::Production => "https://api.storekit.apple.com/".into(),
                AppleEnvironment::Sandbox => "https://api.storekit-sandbox.apple.com/".into(),
            };
        }
        if config.timeout.is_zero() {
            config.timeout = Duration::from_secs(10);
        }
        if config.issuer_id.trim().is_empty()
            || config.bundle_id.trim().is_empty()
            || config.key_id.trim().is_empty()
            || config.private_key_pem.trim().is_empty()
            || config.trusted_roots_der.is_empty()
            || (config.environment == AppleEnvironment::Production
                && config.app_apple_id.is_none_or(|id| id <= 0))
        {
            return Err(ProviderError::Config(
                "incomplete App Store configuration".into(),
            ));
        }
        let base_url = reqwest::Url::parse(&config.base_url)
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        let local_http = matches!(base_url.host_str(), Some("127.0.0.1" | "localhost"));
        if base_url.scheme() != "https" && !local_http {
            return Err(ProviderError::Config(
                "App Store endpoint must use HTTPS".into(),
            ));
        }
        let signing_key = SigningKey::from_pkcs8_pem(&config.private_key_pem)
            .or_else(|_| {
                p256::SecretKey::from_sec1_pem(&config.private_key_pem).map(SigningKey::from)
            })
            .map_err(|_| {
                ProviderError::Config(
                    "App Store private key must be P-256 PKCS8 or SEC1 PEM".into(),
                )
            })?;
        validate_trusted_roots(&config.trusted_roots_der)?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        Ok(Self {
            issuer_id: config.issuer_id,
            bundle_id: config.bundle_id,
            app_apple_id: config.app_apple_id,
            key_id: config.key_id,
            signing_key,
            environment: config.environment,
            base_url,
            client,
            trusted_roots_der: config.trusted_roots_der,
        })
    }

    fn authorization_token(&self) -> ProviderResult<String> {
        let now = unix_seconds()?;
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "alg": "ES256", "kid": self.key_id, "typ": "JWT"
            }))
            .map_err(|_| ProviderError::Unavailable)?,
        );
        let claims = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "iss": self.issuer_id, "iat": now, "exp": now + 300,
                "aud": "appstoreconnect-v1", "bid": self.bundle_id
            }))
            .map_err(|_| ProviderError::Unavailable)?,
        );
        let input = format!("{header}.{claims}");
        let signature: Signature = self.signing_key.sign(input.as_bytes());
        Ok(format!(
            "{input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        ))
    }

    async fn get_transaction(&self, transaction_id: &str) -> ProviderResult<String> {
        if transaction_id.trim().is_empty() || transaction_id.len() > 128 {
            return Err(ProviderError::Rejected(
                "invalid App Store transaction id".into(),
            ));
        }
        let endpoint = self
            .base_url
            .join(&format!("inApps/v1/transactions/{transaction_id}"))
            .map_err(|error| ProviderError::Config(error.to_string()))?;
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(self.authorization_token()?)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if response.status().as_u16() == 429 {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if !response.status().is_success() {
            return Err(ProviderError::Rejected(format!(
                "App Store HTTP {}",
                response.status()
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if bytes.len() > 1024 * 1024 {
            return Err(ProviderError::InvalidResponse(
                "App Store response too large".into(),
            ));
        }
        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "signedTransactionInfo")]
            signed: String,
        }
        let response: Response = serde_json::from_slice(&bytes)
            .map_err(|_| ProviderError::InvalidResponse("invalid App Store response".into()))?;
        Ok(response.signed)
    }

    fn verify_transaction(&self, signed: &str) -> ProviderResult<TransactionClaims> {
        let mut claims: TransactionClaims = verify_apple_jws(signed, &self.trusted_roots_der)?;
        if claims.bundle_id != self.bundle_id
            || !claims
                .environment
                .eq_ignore_ascii_case(self.environment.as_str())
            || claims.transaction_id.is_empty()
            || claims.product_id.is_empty()
            || claims.price <= 0
            || claims.price % 10 != 0
            || claims.storefront != "CHN"
            || claims.currency != "CNY"
        {
            return Err(ProviderError::InvalidResponse(
                "App Store transaction mismatch".into(),
            ));
        }
        if claims.quantity == 0 {
            claims.quantity = 1;
        }
        Ok(claims)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionClaims {
    #[serde(default)]
    app_account_token: Option<String>,
    bundle_id: String,
    currency: String,
    environment: String,
    #[serde(default)]
    original_transaction_id: Option<String>,
    price: i64,
    product_id: String,
    #[serde(default)]
    purchase_date: i64,
    #[serde(default)]
    quantity: u32,
    #[serde(default)]
    revocation_date: i64,
    storefront: String,
    transaction_id: String,
}

#[async_trait]
impl PaymentProvider for ApplePayment {
    fn name(&self) -> &str {
        "apple"
    }

    fn capabilities(&self) -> PaymentCapabilities {
        PaymentCapabilities {
            notification: true,
            purchase: true,
            ..Default::default()
        }
    }

    async fn verify_purchase(&self, request: PurchaseRequest) -> ProviderResult<Purchase> {
        let token = request.purchase_token.trim();
        if token.is_empty() {
            return Err(ProviderError::InvalidRequest(
                "purchase token is required".into(),
            ));
        }
        let signed = if token.split('.').count() == 3 {
            token.to_owned()
        } else {
            self.get_transaction(token).await?
        };
        let claims = self.verify_transaction(&signed)?;
        if request
            .product_id
            .as_ref()
            .is_some_and(|product| product != &claims.product_id)
        {
            return Err(ProviderError::Rejected(
                "transaction product mismatch".into(),
            ));
        }
        Ok(to_purchase(request.merchant_order_id, claims))
    }

    async fn verify_notification(
        &self,
        request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            signed_payload: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Payload {
            notification_type: String,
            notification_uuid: String,
            signed_date: i64,
            data: NotificationData,
            #[serde(default)]
            subtype: Option<String>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct NotificationData {
            app_apple_id: i64,
            bundle_id: String,
            environment: String,
            signed_transaction_info: String,
        }
        let body: Body = serde_json::from_slice(&request.body)
            .map_err(|_| ProviderError::InvalidResponse("invalid App Store notification".into()))?;
        let payload: Payload = verify_apple_jws(&body.signed_payload, &self.trusted_roots_der)?;
        let _ = &payload.subtype;
        if payload.notification_uuid.is_empty()
            || payload.data.bundle_id != self.bundle_id
            || !payload
                .data
                .environment
                .eq_ignore_ascii_case(self.environment.as_str())
            || self
                .app_apple_id
                .is_some_and(|id| id != payload.data.app_apple_id)
        {
            return Err(ProviderError::InvalidResponse(
                "App Store notification mismatch".into(),
            ));
        }
        let claims = self.verify_transaction(&payload.data.signed_transaction_info)?;
        let status = if claims.revocation_date > 0 {
            PaymentStatus::Refunded
        } else {
            notification_status(&payload.notification_type)
        };
        Ok(PaymentEvent {
            event_id: payload.notification_uuid,
            merchant_order_id: String::new(),
            provider_order_id: claims.transaction_id,
            original_provider_order_id: claims.original_transaction_id,
            payer_id: claims.app_account_token,
            product_id: Some(claims.product_id),
            quantity: claims.quantity,
            status,
            amount: apple_money(claims.price),
            environment: Some(claims.environment),
            occurred_at: millis_time(payload.signed_date),
        })
    }
}

fn verify_apple_jws<T: DeserializeOwned>(
    signed: &str,
    trusted_roots_der: &[Vec<u8>],
) -> ProviderResult<T> {
    let parts = signed.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ProviderError::InvalidSignature);
    }
    #[derive(Deserialize)]
    struct Header {
        alg: String,
        x5c: Vec<String>,
    }
    let header: Header = decode_json(parts[0])?;
    if header.alg != "ES256" || header.x5c.len() < 2 || header.x5c.len() > 5 {
        return Err(ProviderError::InvalidSignature);
    }
    let chain = header
        .x5c
        .iter()
        .map(|value| {
            STANDARD
                .decode(value)
                .map_err(|_| ProviderError::InvalidSignature)
        })
        .collect::<ProviderResult<Vec<_>>>()?;
    verify_certificate_chain(&chain, trusted_roots_der)?;
    let (_, leaf) =
        X509Certificate::from_der(&chain[0]).map_err(|_| ProviderError::InvalidSignature)?;
    let key = VerifyingKey::from_sec1_bytes(leaf.public_key().subject_public_key.data.as_ref())
        .map_err(|_| ProviderError::InvalidSignature)?;
    let signature = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|_| ProviderError::InvalidSignature)?;
    let signature =
        Signature::from_slice(&signature).map_err(|_| ProviderError::InvalidSignature)?;
    key.verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .map_err(|_| ProviderError::InvalidSignature)?;
    decode_json(parts[1])
}

fn verify_certificate_chain(
    chain: &[Vec<u8>],
    trusted_roots_der: &[Vec<u8>],
) -> ProviderResult<()> {
    let now = unix_seconds()? as i64;
    let parsed = chain
        .iter()
        .map(|der| {
            X509Certificate::from_der(der)
                .map(|(_, cert)| cert)
                .map_err(|_| ProviderError::InvalidSignature)
        })
        .collect::<ProviderResult<Vec<_>>>()?;
    let now = x509_parser::time::ASN1Time::from_timestamp(now)
        .map_err(|_| ProviderError::InvalidSignature)?;
    if parsed.iter().any(|cert| !cert.validity().is_valid_at(now)) {
        return Err(ProviderError::InvalidSignature);
    }
    for pair in parsed.windows(2) {
        if pair[0].issuer() != pair[1].subject()
            || pair[0]
                .verify_signature(Some(pair[1].public_key()))
                .is_err()
        {
            return Err(ProviderError::InvalidSignature);
        }
    }
    let root = parsed.last().ok_or(ProviderError::InvalidSignature)?;
    let injected_trusted = trusted_roots_der.iter().any(|der| {
        X509Certificate::from_der(der).is_ok_and(|(_, trusted)| {
            trusted.validity().is_valid_at(now)
                && ((root.subject() == trusted.subject()
                    && root.public_key().raw == trusted.public_key().raw)
                    || (root.issuer() == trusted.subject()
                        && root.verify_signature(Some(trusted.public_key())).is_ok()))
        })
    });
    if !injected_trusted {
        return Err(ProviderError::InvalidSignature);
    }
    Ok(())
}

fn validate_trusted_roots(trusted_roots_der: &[Vec<u8>]) -> ProviderResult<()> {
    if trusted_roots_der.is_empty()
        || trusted_roots_der.iter().any(|der| {
            X509Certificate::from_der(der).is_err()
                || trusted_roots_der
                    .iter()
                    .filter(|candidate| candidate.as_slice() == der.as_slice())
                    .count()
                    != 1
        })
    {
        return Err(ProviderError::Config(
            "App Store trusted roots must be non-empty, unique DER certificates".into(),
        ));
    }
    Ok(())
}

fn decode_json<T: DeserializeOwned>(part: &str) -> ProviderResult<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| ProviderError::InvalidSignature)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| ProviderError::InvalidResponse("invalid JWS JSON".into()))
}

fn to_purchase(merchant_order_id: String, claims: TransactionClaims) -> Purchase {
    let status = if claims.revocation_date > 0 {
        PaymentStatus::Refunded
    } else {
        PaymentStatus::Succeeded
    };
    Purchase {
        merchant_order_id,
        provider_order_id: claims.transaction_id,
        original_provider_order_id: claims.original_transaction_id,
        product_id: claims.product_id,
        app_account_token: claims.app_account_token,
        quantity: claims.quantity,
        status,
        amount: apple_money(claims.price),
        environment: claims.environment,
        storefront: claims.storefront,
        purchased_at: millis_time(claims.purchase_date),
    }
}

fn apple_money(price: i64) -> Money {
    Money {
        currency: "CNY".into(),
        minor_units: price / 10,
    }
}

fn notification_status(value: &str) -> PaymentStatus {
    match value {
        "ONE_TIME_CHARGE" | "SUBSCRIBED" | "DID_RENEW" => PaymentStatus::Succeeded,
        "REFUND" | "REVOKE" => PaymentStatus::Refunded,
        "EXPIRED" | "GRACE_PERIOD_EXPIRED" => PaymentStatus::Closed,
        "DID_FAIL_TO_RENEW" => PaymentStatus::Failed,
        _ => PaymentStatus::Pending,
    }
}

fn millis_time(value: i64) -> Option<SystemTime> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .and_then(|value| UNIX_EPOCH.checked_add(Duration::from_millis(value)))
}

fn unix_seconds() -> ProviderResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ProviderError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn apple_price_is_converted_from_milliunits() {
        assert_eq!(apple_money(1230).minor_units, 123);
        assert_eq!(notification_status("REFUND"), PaymentStatus::Refunded);
    }

    #[test]
    fn general_webpki_fallback_is_not_available() {
        assert!(validate_trusted_roots(&[]).is_err());
    }
}
