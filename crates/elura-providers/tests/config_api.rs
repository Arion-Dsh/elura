#[cfg(feature = "identity")]
#[test]
fn identity_configs_have_stable_constructors() {
    use elura_providers::identity::{
        CodeExchangeConfig, OAuth2Config, PlatformIdentityConfig, QuickSdkIdentityConfig,
    };

    let _ = CodeExchangeConfig::new(
        "example",
        "https://identity.example.com/exchange",
        "client",
        "secret",
        "subject",
    );
    let _ = OAuth2Config::new(
        "example",
        "client",
        "https://app.example.com/oauth/callback",
        "https://identity.example.com/authorize",
        "https://identity.example.com/token",
        "https://identity.example.com/userinfo",
        "sub",
    );
    let _ = PlatformIdentityConfig::new("app", "secret");
    let _ = QuickSdkIdentityConfig::new("https://identity.example.com/quicksdk");
}

#[cfg(feature = "notification-alisms")]
#[test]
fn notification_configs_have_stable_constructors() {
    use std::collections::HashMap;

    use elura_providers::notification::AliSmsConfig;

    let _ = AliSmsConfig::new(
        "access-key",
        "access-key-secret",
        "game",
        HashMap::from([("login".into(), "SMS_123".into())]),
    );
}

#[cfg(feature = "payment-alipay")]
#[test]
fn alipay_config_has_a_stable_constructor() {
    let _ = elura_providers::payment::AlipayConfig::production("app", "private", "public");
}

#[cfg(feature = "payment-apple")]
#[test]
fn apple_config_has_a_stable_constructor() {
    use elura_providers::payment::{AppleConfig, AppleEnvironment};

    let _ = AppleConfig::new(
        "issuer",
        "com.example.game",
        "key",
        "private",
        AppleEnvironment::Sandbox,
    )
    .with_trusted_roots(Vec::new());
}

#[cfg(feature = "payment-douyin")]
#[test]
fn douyin_config_has_a_stable_constructor() {
    let _ = elura_providers::payment::DouyinConfig::new("callback-token", "app", "secret");
}

#[cfg(feature = "payment-wechat-mini")]
#[test]
fn wechat_mini_config_has_a_stable_constructor() {
    let _ = elura_providers::payment::WechatMiniConfig::new("app", "app-key", "offer");
}

#[cfg(feature = "payment-wechat-pay")]
#[test]
fn wechat_pay_config_has_a_stable_builder() {
    let _ = elura_providers::payment::WechatPayConfig::new(
        "merchant",
        "app",
        "https://pay.example.com/callback",
    )
    .with_merchant_identity("serial", "api-v3-key", "private")
    .with_wechat_identity("wechat-key", "public");
}
