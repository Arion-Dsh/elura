#![doc = include_str!("../README.md")]
#![deny(rustdoc::broken_intra_doc_links)]

mod error;
#[cfg(any(
    feature = "identity",
    feature = "payment-alipay",
    feature = "payment-apple",
    feature = "payment-douyin",
    feature = "payment-wechat-pay"
))]
mod http_client;
#[cfg(feature = "identity")]
pub mod identity;
#[cfg(feature = "notification-alisms")]
pub mod notification;
#[cfg(feature = "otp")]
pub mod otp;
pub mod payment;

pub use elura_core::identity::ProviderName;
pub use error::{ProviderError, ProviderResult};
