//! Identity providers and account-resolution boundaries.

mod guest;
mod oauth2;
mod password;
mod phone;
mod platform;
mod registry;

pub use guest::GuestProvider;
pub use oauth2::{CodeExchangeConfig, CodeExchangeProvider, OAuth2Config, OAuth2Provider};
pub use password::{PasswordProvider, PasswordRepository, hash_password, normalize_username};
pub use phone::{OtpVerifier, PhoneProvider};
pub use platform::{
    DouyinIdentity, PlatformIdentityConfig, QuickSdkIdentity, QuickSdkIdentityConfig,
    WechatIdentity, WechatMiniIdentity,
};
pub use registry::{
    AccountStore, IdentityProvider, IdentityProviderCapabilities, IdentityProviderFactory,
    IdentityProviderInfo, IdentityRegistry, IdentityService, Principal, VerifiedIdentity,
};

#[async_trait::async_trait]
pub trait OtpSender: Send + Sync {
    async fn send_code(&self, phone: &str, code: &str, purpose: &str) -> crate::ProviderResult<()>;
}
