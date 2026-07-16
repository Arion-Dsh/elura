//! Identity providers and account-resolution boundaries.

mod guest;
mod oauth2;
mod password;
mod phone;
mod platform;
mod registry;

pub use guest::{GuestCredential, GuestProvider};
pub use oauth2::{
    CodeCredential, CodeExchangeConfig, CodeExchangeProvider, OAuth2Config, OAuth2Credential,
    OAuth2Provider,
};
pub use password::{PasswordCredential, PasswordProvider, hash_password, normalize_username};
pub use phone::{OtpVerifier, PhoneCredential, PhoneProvider};
pub use platform::{
    DouyinIdentity, PlatformIdentityConfig, QuickSdkCredential, QuickSdkIdentity,
    QuickSdkIdentityConfig, WechatIdentity, WechatMiniIdentity,
};
pub use registry::{
    IdentityBindingStore, IdentityProvider, IdentityProviderCapabilities, IdentityProviderFactory,
    IdentityProviderInfo, IdentityRegistrationMode, IdentityRegistry, IdentityService,
    PasswordCredentialStore, Principal, ProviderName, VerifiedIdentity,
};

#[async_trait::async_trait]
/// Sends a one-time code through an external notification channel.
pub trait OtpSender: Send + Sync {
    /// Sends `code` to an E.164 phone number for the named purpose.
    async fn send_code(&self, phone: &str, code: &str, purpose: &str) -> crate::ProviderResult<()>;
}
