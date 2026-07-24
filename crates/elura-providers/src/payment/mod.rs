//! Payment contracts, registry and channel implementations.

#[cfg(feature = "payment-alipay")]
mod alipay;
#[cfg(feature = "payment-apple")]
mod apple;
#[cfg(feature = "payment-douyin")]
mod douyin;
#[cfg(feature = "payment-quicksdk")]
mod quicksdk;
#[cfg(any(feature = "payment-alipay", feature = "payment-wechat-pay"))]
mod rsa_signature;
#[cfg(feature = "payment-wechat-mini")]
mod wechat_mini;
#[cfg(feature = "payment-wechat-pay")]
mod wechat_pay;

#[cfg(feature = "payment-alipay")]
pub use alipay::{AlipayCheckoutOptions, AlipayClientMode, AlipayConfig, AlipayPayment};
#[cfg(feature = "payment-apple")]
pub use apple::{AppleConfig, AppleEnvironment, ApplePayment};
#[cfg(feature = "payment-douyin")]
pub use douyin::{DouyinConfig, DouyinPayment};
#[cfg(feature = "payment-quicksdk")]
pub use quicksdk::QuickSdkPayment;
#[cfg(feature = "payment-wechat-mini")]
pub use wechat_mini::{WechatMiniCheckoutOptions, WechatMiniConfig, WechatMiniPayment};
#[cfg(feature = "payment-wechat-pay")]
pub use wechat_pay::{WechatPayConfig, WechatPayPayment};

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::replay_protection::ReplayProtectionStore;
use http::{HeaderMap, Method, Uri};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{ProviderError, ProviderName, ProviderResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    pub currency: String,
    pub minor_units: i64,
}

impl Money {
    /// Creates and validates a positive amount in ISO-4217-style minor units.
    pub fn new(currency: impl Into<String>, minor_units: i64) -> ProviderResult<Self> {
        let value = Self {
            currency: currency.into(),
            minor_units,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates an amount supplied by the caller.
    pub fn validate(&self) -> ProviderResult<()> {
        if !self.is_valid() {
            return Err(ProviderError::InvalidRequest(
                "money must use a three-letter uppercase currency and positive minor units".into(),
            ));
        }
        Ok(())
    }

    fn validate_response(&self) -> ProviderResult<()> {
        if !self.is_valid() {
            return Err(ProviderError::InvalidResponse("invalid money".into()));
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.currency.len() == 3
            && self.currency.chars().all(|c| c.is_ascii_uppercase())
            && self.minor_units > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PaymentStatus {
    Pending,
    Succeeded,
    Failed,
    Closed,
    Refunded,
}

/// Provider-independent checkout inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckoutRequest {
    pub merchant_order_id: String,
    pub amount: Money,
    pub subject: String,
    pub product_id: Option<String>,
    pub quantity: u32,
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub options: serde_json::Value,
}

impl CheckoutRequest {
    /// Creates a checkout request with quantity one and no provider-specific options.
    pub fn new(
        merchant_order_id: impl Into<String>,
        amount: Money,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            merchant_order_id: merchant_order_id.into(),
            amount,
            subject: subject.into(),
            product_id: None,
            quantity: 1,
            metadata: HashMap::new(),
            options: serde_json::Value::Null,
        }
    }

    /// Validates provider-independent checkout constraints.
    pub fn validate(&self) -> ProviderResult<()> {
        self.amount.validate()?;
        let options_size = serde_json::to_vec(&self.options)
            .map_err(|error| ProviderError::InvalidRequest(error.to_string()))?
            .len();
        if self.merchant_order_id.trim().is_empty()
            || self.merchant_order_id.len() > 512
            || self.subject.trim().is_empty()
            || self.subject.len() > 512
            || self
                .product_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
            || self.quantity == 0
            || self.metadata.len() > 64
            || self
                .metadata
                .iter()
                .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 2048)
            || options_size > 16 * 1024
        {
            return Err(ProviderError::InvalidRequest(
                "checkout request exceeds common limits".into(),
            ));
        }
        Ok(())
    }

    /// Decodes provider-specific options into a public provider options type.
    pub fn provider_options<T: DeserializeOwned>(&self) -> ProviderResult<T> {
        serde_json::from_value(self.options.clone()).map_err(|error| {
            ProviderError::InvalidRequest(format!("invalid checkout options: {error}"))
        })
    }

    /// Decodes provider-specific options or returns their default value when omitted.
    pub fn provider_options_or_default<T>(&self) -> ProviderResult<T>
    where
        T: DeserializeOwned + Default,
    {
        if self.options.is_null() {
            Ok(T::default())
        } else {
            self.provider_options()
        }
    }

    /// Replaces provider-specific options with a serializable options value.
    pub fn with_provider_options<T: Serialize>(mut self, options: T) -> ProviderResult<Self> {
        self.options = serde_json::to_value(options).map_err(|error| {
            ProviderError::InvalidRequest(format!("invalid checkout options: {error}"))
        })?;
        Ok(self)
    }
}

/// Client-side action required to complete a checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClientPayload {
    /// Provider-specific parameters passed to a native application SDK.
    AppParameters(String),
    /// A JSON object passed to a browser or native client SDK.
    Json(serde_json::Value),
    /// URL to which the user agent must be redirected.
    RedirectUrl(String),
    /// Text encoded into a payment QR code.
    QrCode(String),
    /// Extension payload for third-party providers.
    Opaque {
        /// Media type describing `body`.
        content_type: String,
        /// Provider-defined payload.
        body: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutResult {
    pub provider_order_id: Option<String>,
    pub client_payload: ClientPayload,
    #[serde(default, with = "optional_unix_millis")]
    pub expires_at: Option<SystemTime>,
}

impl CheckoutResult {
    fn validate_response(&self) -> ProviderResult<()> {
        let payload_size = serde_json::to_vec(&self.client_payload)
            .map_err(|_| ProviderError::InvalidResponse("invalid client payload".into()))?
            .len();
        if self
            .provider_order_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
            || payload_size > 64 * 1024
        {
            return Err(ProviderError::InvalidResponse(
                "invalid checkout result".into(),
            ));
        }
        Ok(())
    }
}

/// Lossless HTTP callback request passed to payment providers.
#[derive(Debug, Clone)]
pub struct NotificationRequest {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl NotificationRequest {
    /// Creates a callback request from standard HTTP types.
    pub fn new(method: Method, uri: Uri, headers: HeaderMap, body: impl Into<Bytes>) -> Self {
        Self {
            method,
            uri,
            headers,
            body: body.into(),
        }
    }

    /// Returns the callback path exactly as represented by the URI.
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    /// Returns the raw query without the leading question mark.
    pub fn query(&self) -> &str {
        self.uri.query().unwrap_or_default()
    }

    pub fn validate(&self) -> ProviderResult<()> {
        if self.method.as_str().len() > 16
            || self.path().len() > 2048
            || self.query().len() > 8192
            || self.headers.len() > 64
            || self
                .headers
                .iter()
                .any(|(name, value)| name.as_str().len() > 128 || value.as_bytes().len() > 4096)
            || self.body.len() > 1024 * 1024
        {
            Err(ProviderError::InvalidRequest(
                "payment notification exceeds limits".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentEvent {
    pub event_id: String,
    pub merchant_order_id: String,
    pub provider_order_id: String,
    pub original_provider_order_id: Option<String>,
    pub payer_id: Option<String>,
    pub product_id: Option<String>,
    pub quantity: u32,
    pub status: PaymentStatus,
    pub amount: Money,
    pub environment: Option<String>,
    #[serde(default, with = "optional_unix_millis")]
    pub occurred_at: Option<SystemTime>,
}

impl PaymentEvent {
    fn validate_response(&self) -> ProviderResult<()> {
        if self.event_id.trim().is_empty()
            || self.event_id.len() > 512
            || self.merchant_order_id.trim().is_empty()
            || self.merchant_order_id.len() > 512
            || self.provider_order_id.trim().is_empty()
            || self.provider_order_id.len() > 512
            || self.quantity == 0
        {
            return Err(ProviderError::InvalidResponse(
                "invalid payment callback event".into(),
            ));
        }
        self.amount.validate_response()
    }
}

/// Unambiguous identifier used to query or refund a payment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PaymentLookup {
    /// Application-owned merchant order identifier.
    MerchantOrderId(String),
    /// Identifier assigned by the payment provider.
    ProviderOrderId(String),
}

impl PaymentLookup {
    /// Looks up a payment by the application-owned order identifier.
    pub fn merchant(id: impl Into<String>) -> Self {
        Self::MerchantOrderId(id.into())
    }

    /// Looks up a payment by the provider-owned order identifier.
    pub fn provider(id: impl Into<String>) -> Self {
        Self::ProviderOrderId(id.into())
    }

    /// Returns the identifier value after validating common length constraints.
    pub fn value(&self) -> ProviderResult<&str> {
        let value = match self {
            Self::MerchantOrderId(value) | Self::ProviderOrderId(value) => value.trim(),
        };
        if value.is_empty() || value.len() > 512 {
            return Err(ProviderError::InvalidRequest(
                "payment lookup ID must contain 1..=512 bytes".into(),
            ));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub merchant_order_id: String,
    pub provider_order_id: Option<String>,
    pub status: PaymentStatus,
    pub amount: Option<Money>,
    pub payer_id: Option<String>,
    #[serde(default, with = "optional_unix_millis")]
    pub paid_at: Option<SystemTime>,
}

impl Payment {
    fn validate_response(&self) -> ProviderResult<()> {
        if self.merchant_order_id.trim().is_empty()
            || self.merchant_order_id.len() > 512
            || self
                .provider_order_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
        {
            return Err(ProviderError::InvalidResponse("invalid payment".into()));
        }
        if let Some(amount) = &self.amount {
            amount.validate_response()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundRequest {
    pub payment: PaymentLookup,
    pub merchant_refund_id: String,
    pub amount: Money,
    pub reason: Option<String>,
}

impl RefundRequest {
    /// Validates provider-independent refund constraints.
    pub fn validate(&self) -> ProviderResult<()> {
        self.payment.value()?;
        self.amount.validate()?;
        if self.merchant_refund_id.trim().is_empty()
            || self.merchant_refund_id.len() > 512
            || self.reason.as_ref().is_some_and(|value| value.len() > 1024)
        {
            return Err(ProviderError::InvalidRequest(
                "invalid refund request".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refund {
    pub merchant_refund_id: String,
    pub provider_refund_id: String,
    pub status: PaymentStatus,
}

impl Refund {
    fn validate_response(&self) -> ProviderResult<()> {
        if self.merchant_refund_id.trim().is_empty()
            || self.merchant_refund_id.len() > 512
            || self.provider_refund_id.trim().is_empty()
            || self.provider_refund_id.len() > 512
        {
            return Err(ProviderError::InvalidResponse("invalid refund".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurchaseRequest {
    pub merchant_order_id: String,
    pub product_id: Option<String>,
    pub purchase_token: String,
}

impl PurchaseRequest {
    /// Validates provider-independent purchase verification constraints.
    pub fn validate(&self) -> ProviderResult<()> {
        if self.merchant_order_id.trim().is_empty()
            || self.merchant_order_id.len() > 512
            || self
                .product_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
            || self.purchase_token.trim().is_empty()
            || self.purchase_token.len() > 64 * 1024
        {
            return Err(ProviderError::InvalidRequest(
                "invalid purchase request".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Purchase {
    pub merchant_order_id: String,
    pub provider_order_id: String,
    pub original_provider_order_id: Option<String>,
    pub product_id: String,
    pub app_account_token: Option<String>,
    pub quantity: u32,
    pub status: PaymentStatus,
    pub amount: Money,
    pub environment: String,
    pub storefront: String,
    #[serde(default, with = "optional_unix_millis")]
    pub purchased_at: Option<SystemTime>,
}

impl Purchase {
    fn validate_response(&self) -> ProviderResult<()> {
        if self.merchant_order_id.trim().is_empty()
            || self.provider_order_id.trim().is_empty()
            || self.product_id.trim().is_empty()
            || self.quantity == 0
        {
            return Err(ProviderError::InvalidResponse("invalid purchase".into()));
        }
        self.amount.validate_response()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaymentCapabilities {
    pub checkout: bool,
    pub query: bool,
    pub notification: bool,
    pub refund: bool,
    pub purchase: bool,
}

impl PaymentCapabilities {
    fn any(self) -> bool {
        self.checkout || self.query || self.notification || self.refund || self.purchase
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaymentProviderInfo {
    pub name: ProviderName,
    pub capabilities: PaymentCapabilities,
}

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> PaymentCapabilities;
    async fn create(&self, _request: CheckoutRequest) -> ProviderResult<CheckoutResult> {
        Err(ProviderError::Unsupported)
    }
    async fn query(&self, _lookup: PaymentLookup) -> ProviderResult<Payment> {
        Err(ProviderError::Unsupported)
    }
    async fn verify_notification(
        &self,
        _request: NotificationRequest,
    ) -> ProviderResult<PaymentEvent> {
        Err(ProviderError::Unsupported)
    }
    async fn refund(&self, _request: RefundRequest) -> ProviderResult<Refund> {
        Err(ProviderError::Unsupported)
    }
    async fn verify_purchase(&self, _request: PurchaseRequest) -> ProviderResult<Purchase> {
        Err(ProviderError::Unsupported)
    }
}

/// Signature verification plus durable callback replay protection.
pub struct PaymentNotificationVerifier {
    provider: Arc<dyn PaymentProvider>,
    replay: Arc<dyn ReplayProtectionStore>,
    replay_ttl: Duration,
}

impl PaymentNotificationVerifier {
    pub fn new(
        provider: Arc<dyn PaymentProvider>,
        replay: Arc<dyn ReplayProtectionStore>,
        replay_ttl: Duration,
    ) -> ProviderResult<Self> {
        if replay_ttl.is_zero() {
            return Err(ProviderError::Config(
                "payment callback replay TTL must be positive".into(),
            ));
        }
        if !provider.capabilities().notification {
            return Err(ProviderError::Unsupported);
        }
        Ok(Self {
            provider,
            replay,
            replay_ttl,
        })
    }

    pub async fn verify(&self, request: NotificationRequest) -> ProviderResult<PaymentEvent> {
        request.validate()?;
        let event = self.provider.verify_notification(request).await?;
        event.validate_response()?;
        let expires_at = SystemTime::now()
            .checked_add(self.replay_ttl)
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs())
            .ok_or(ProviderError::Unavailable)?;
        let replay_key = format!("payment:{}:{}", self.provider.name(), event.event_id);
        let fresh = self
            .replay
            .reserve(&replay_key, expires_at)
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if fresh {
            Ok(event)
        } else {
            Err(ProviderError::AlreadyProcessed)
        }
    }
}

pub type PaymentProviderFactory =
    Arc<dyn Fn() -> ProviderResult<Arc<dyn PaymentProvider>> + Send + Sync>;

#[derive(Default)]
pub struct PaymentRegistry {
    providers: RwLock<BTreeMap<ProviderName, Arc<dyn PaymentProvider>>>,
}

impl PaymentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(
        enabled: impl IntoIterator<Item = impl AsRef<str>>,
        factories: &HashMap<String, PaymentProviderFactory>,
    ) -> ProviderResult<Self> {
        let registry = Self::new();
        let mut count = 0;
        for value in enabled {
            count += 1;
            let name = ProviderName::parse(value.as_ref()).map_err(|error| {
                ProviderError::Config(format!("invalid payment provider: {error}"))
            })?;
            let factory = factories.get(name.as_str()).ok_or_else(|| {
                ProviderError::Config(format!("payment provider {name} has no factory"))
            })?;
            registry.register_arc(factory()?)?;
        }
        if count == 0 {
            return Err(ProviderError::Config(
                "at least one payment provider is required".into(),
            ));
        }
        Ok(registry)
    }

    pub fn register(&self, provider: impl PaymentProvider + 'static) -> ProviderResult<()> {
        self.register_arc(Arc::new(provider))
    }

    pub fn register_arc(&self, provider: Arc<dyn PaymentProvider>) -> ProviderResult<()> {
        let raw_name = provider.name();
        let name = ProviderName::parse(raw_name).map_err(|error| {
            ProviderError::Config(format!("invalid payment provider name: {error}"))
        })?;
        if name.as_str() != raw_name || !provider.capabilities().any() {
            return Err(ProviderError::Config(format!(
                "invalid payment provider {raw_name}"
            )));
        }
        let mut providers = self
            .providers
            .write()
            .map_err(|_| ProviderError::Unavailable)?;
        if providers.contains_key(&name) {
            return Err(ProviderError::Config(format!(
                "duplicate payment provider {name}"
            )));
        }
        providers.insert(name, provider);
        Ok(())
    }

    pub fn provider(&self, name: &str) -> ProviderResult<Arc<dyn PaymentProvider>> {
        let name = ProviderName::parse(name)?;
        self.providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?
            .get(&name)
            .cloned()
            .ok_or_else(|| ProviderError::UnknownProvider(name.to_string()))
    }

    /// Creates a checkout through a registered provider and validates its response.
    pub async fn checkout(
        &self,
        name: &str,
        request: CheckoutRequest,
    ) -> ProviderResult<CheckoutResult> {
        request.validate()?;
        let provider = self.provider(name)?;
        if !provider.capabilities().checkout {
            return Err(ProviderError::Unsupported);
        }
        let result = provider.create(request).await?;
        result.validate_response()?;
        Ok(result)
    }

    /// Queries a payment through a registered provider.
    pub async fn query(&self, name: &str, lookup: PaymentLookup) -> ProviderResult<Payment> {
        lookup.value()?;
        let provider = self.provider(name)?;
        if !provider.capabilities().query {
            return Err(ProviderError::Unsupported);
        }
        let payment = provider.query(lookup).await?;
        payment.validate_response()?;
        Ok(payment)
    }

    /// Requests a refund through a registered provider.
    pub async fn refund(&self, name: &str, request: RefundRequest) -> ProviderResult<Refund> {
        request.validate()?;
        let provider = self.provider(name)?;
        if !provider.capabilities().refund {
            return Err(ProviderError::Unsupported);
        }
        let refund = provider.refund(request).await?;
        refund.validate_response()?;
        Ok(refund)
    }

    /// Verifies an in-app purchase through a registered provider.
    pub async fn verify_purchase(
        &self,
        name: &str,
        request: PurchaseRequest,
    ) -> ProviderResult<Purchase> {
        request.validate()?;
        let provider = self.provider(name)?;
        if !provider.capabilities().purchase {
            return Err(ProviderError::Unsupported);
        }
        let purchase = provider.verify_purchase(request).await?;
        purchase.validate_response()?;
        Ok(purchase)
    }

    /// Builds a signature and replay verifier for a registered callback provider.
    pub fn notification_verifier(
        &self,
        name: &str,
        replay: Arc<dyn ReplayProtectionStore>,
        replay_ttl: Duration,
    ) -> ProviderResult<PaymentNotificationVerifier> {
        let provider = self.provider(name)?;
        if !provider.capabilities().notification {
            return Err(ProviderError::Unsupported);
        }
        PaymentNotificationVerifier::new(provider, replay, replay_ttl)
    }

    pub fn providers(&self) -> ProviderResult<Vec<PaymentProviderInfo>> {
        let providers = self
            .providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(providers
            .iter()
            .map(|(name, provider)| PaymentProviderInfo {
                name: name.clone(),
                capabilities: provider.capabilities(),
            })
            .collect())
    }
}

mod optional_unix_millis {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = value
            .map(|time| {
                time.duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .map_err(serde::ser::Error::custom)
                    .and_then(|millis| u64::try_from(millis).map_err(serde::ser::Error::custom))
            })
            .transpose()?;
        serializer.serialize_some(&millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<u64>::deserialize(deserializer)
            .map(|value| value.map(|millis| UNIX_EPOCH + Duration::from_millis(millis)))
    }
}

#[cfg(test)]
mod tests {
    use elura_core::replay_protection::MemoryReplayProtectionStore;

    use super::*;

    struct CallbackProvider;

    #[async_trait]
    impl PaymentProvider for CallbackProvider {
        fn name(&self) -> &str {
            "callback"
        }

        fn capabilities(&self) -> PaymentCapabilities {
            PaymentCapabilities {
                notification: true,
                ..PaymentCapabilities::default()
            }
        }

        async fn verify_notification(
            &self,
            _request: NotificationRequest,
        ) -> ProviderResult<PaymentEvent> {
            Ok(PaymentEvent {
                event_id: "event-1".into(),
                merchant_order_id: "order-1".into(),
                provider_order_id: "provider-1".into(),
                original_provider_order_id: None,
                payer_id: None,
                product_id: None,
                quantity: 1,
                status: PaymentStatus::Succeeded,
                amount: Money {
                    currency: "CNY".into(),
                    minor_units: 100,
                },
                environment: None,
                occurred_at: None,
            })
        }
    }

    fn notification() -> NotificationRequest {
        NotificationRequest::new(
            Method::POST,
            "/callback".parse().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    #[tokio::test]
    async fn callback_verifier_consumes_event_once() {
        let verifier = PaymentNotificationVerifier::new(
            Arc::new(CallbackProvider),
            Arc::new(MemoryReplayProtectionStore::default()),
            Duration::from_secs(60),
        )
        .unwrap();
        verifier.verify(notification()).await.unwrap();
        assert!(matches!(
            verifier.verify(notification()).await,
            Err(ProviderError::AlreadyProcessed)
        ));
    }
}
