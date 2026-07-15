//! Payment contracts, registry and channel implementations.

#[cfg(feature = "payment-alipay")]
mod alipay;
#[cfg(feature = "payment-apple")]
mod apple;
#[cfg(feature = "payment-douyin")]
mod douyin;
#[cfg(feature = "payment-quicksdk")]
mod quicksdk;
#[cfg(feature = "payment-wechat-mini")]
mod wechat_mini;
#[cfg(feature = "payment-wechat-pay")]
mod wechat_pay;

#[cfg(feature = "payment-alipay")]
pub use alipay::{AlipayConfig, AlipayPayment};
#[cfg(feature = "payment-apple")]
pub use apple::{AppleConfig, AppleEnvironment, ApplePayment};
#[cfg(feature = "payment-douyin")]
pub use douyin::{DouyinConfig, DouyinPayment};
#[cfg(feature = "payment-quicksdk")]
pub use quicksdk::QuickSdkPayment;
#[cfg(feature = "payment-wechat-mini")]
pub use wechat_mini::{WechatMiniConfig, WechatMiniPayment};
#[cfg(feature = "payment-wechat-pay")]
pub use wechat_pay::{WechatPayConfig, WechatPayPayment};

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::ticket::ReplayStore;
use serde::{Deserialize, Serialize};

use crate::{ProviderError, ProviderResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    pub currency: String,
    pub minor_units: i64,
}

impl Money {
    pub fn validate(&self) -> ProviderResult<()> {
        if self.currency.len() != 3
            || !self.currency.chars().all(|c| c.is_ascii_uppercase())
            || self.minor_units <= 0
        {
            return Err(ProviderError::InvalidResponse("invalid money".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentStatus {
    Pending,
    Succeeded,
    Failed,
    Closed,
    Refunded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Provider-independent inputs for creating a checkout.
///
/// Provider settings, including callback endpoints, belong to the selected
/// provider's configuration and cannot be overridden per request.
pub struct CheckoutRequest {
    pub merchant_order_id: String,
    pub amount: Money,
    pub subject: String,
    pub product_id: Option<String>,
    pub payer_credential: Option<String>,
    #[serde(default)]
    pub payer_id: Option<String>,
    pub quantity: u32,
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub client_mode: Option<String>,
    #[serde(default)]
    pub attach: Option<String>,
    #[serde(default)]
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutResult {
    pub provider_order_id: Option<String>,
    pub client_payload: String,
    #[serde(skip)]
    pub expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct NotificationRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl NotificationRequest {
    pub fn validate(&self) -> ProviderResult<()> {
        if self.method.len() > 16
            || self.path.len() > 2048
            || self.query.len() > 8192
            || self.headers.len() > 64
            || self
                .headers
                .iter()
                .any(|(name, value)| name.is_empty() || name.len() > 128 || value.len() > 4096)
            || self.body.len() > 1024 * 1024
        {
            Err(ProviderError::InvalidResponse(
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
    #[serde(skip)]
    pub occurred_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub merchant_order_id: Option<String>,
    pub provider_order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub merchant_order_id: String,
    pub provider_order_id: String,
    pub status: PaymentStatus,
    pub amount: Money,
    pub payer_id: Option<String>,
    #[serde(skip)]
    pub paid_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundRequest {
    pub merchant_order_id: Option<String>,
    pub provider_order_id: Option<String>,
    pub merchant_refund_id: String,
    pub amount: Money,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refund {
    pub merchant_refund_id: String,
    pub provider_refund_id: String,
    pub status: PaymentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurchaseRequest {
    pub merchant_order_id: String,
    pub product_id: Option<String>,
    pub purchase_token: String,
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
    #[serde(skip)]
    pub purchased_at: Option<SystemTime>,
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
    pub name: String,
    pub capabilities: PaymentCapabilities,
}

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> PaymentCapabilities;
    async fn create(&self, _request: CheckoutRequest) -> ProviderResult<CheckoutResult> {
        Err(ProviderError::Unsupported)
    }
    async fn query(&self, _request: QueryRequest) -> ProviderResult<Payment> {
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
    replay: Arc<dyn ReplayStore>,
    replay_ttl: Duration,
}

impl PaymentNotificationVerifier {
    pub fn new(
        provider: Arc<dyn PaymentProvider>,
        replay: Arc<dyn ReplayStore>,
        replay_ttl: Duration,
    ) -> ProviderResult<Self> {
        if replay_ttl.is_zero() {
            return Err(ProviderError::Config(
                "payment callback replay TTL must be positive".into(),
            ));
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
        if event.event_id.is_empty() || event.event_id.len() > 512 {
            return Err(ProviderError::InvalidResponse(
                "invalid payment callback event ID".into(),
            ));
        }
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
    providers: RwLock<BTreeMap<String, Arc<dyn PaymentProvider>>>,
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
            let name = value.as_ref().trim().to_ascii_lowercase();
            if !valid_provider_name(&name) {
                return Err(ProviderError::Config(format!(
                    "invalid payment provider {name}"
                )));
            }
            let factory = factories.get(&name).ok_or_else(|| {
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
        let name = provider.name().to_owned();
        if !valid_provider_name(&name)
            || name != name.trim().to_ascii_lowercase()
            || !provider.capabilities().any()
        {
            return Err(ProviderError::Config(format!(
                "invalid payment provider {name}"
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

    pub fn get(&self, name: &str) -> Option<Arc<dyn PaymentProvider>> {
        self.providers
            .read()
            .ok()?
            .get(&name.trim().to_ascii_lowercase())
            .cloned()
    }

    pub fn providers(&self) -> ProviderResult<Vec<PaymentProviderInfo>> {
        let providers = self
            .providers
            .read()
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(providers
            .values()
            .map(|provider| PaymentProviderInfo {
                name: provider.name().to_owned(),
                capabilities: provider.capabilities(),
            })
            .collect())
    }
}

fn valid_provider_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && bytes[0].is_ascii_lowercase()
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use elura_core::ticket::MemoryReplayStore;

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
        NotificationRequest {
            method: "POST".into(),
            path: "/callback".into(),
            query: String::new(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn callback_verifier_consumes_event_once() {
        let verifier = PaymentNotificationVerifier::new(
            Arc::new(CallbackProvider),
            Arc::new(MemoryReplayStore::default()),
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
