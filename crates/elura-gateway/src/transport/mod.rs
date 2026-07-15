//! Extensible server transport implementations.

mod account_version;
mod admission;
mod dedup;
mod drain;
mod limits;
mod observer;
mod proxy;
pub mod quic;
mod session;
pub(crate) mod tcp;
pub mod websocket;

pub(crate) use account_version::AccountVersionPolicy;
pub use account_version::AccountVersionSettings;
pub(crate) use admission::AdmissionPolicy;
pub use admission::{
    AdmissionController, AdmissionDecision, AdmissionRejection, AdmissionRequest,
    AdmissionSettings, AdmissionStage, RealmAdmission,
};
pub(crate) use dedup::ResponseCache;
pub(crate) use drain::DrainController;
pub(crate) use limits::{ConnectionLimiter, KeyedRateLimiter};
pub(crate) use observer::notify as notify_session_observers;
pub use observer::{SessionEvent, SessionEventKind, SessionObserver};
pub(crate) use proxy::proxy_client_address;
pub use proxy::{ProxyProtocolConfig, TrustedProxies};
pub use quic::QuicConfig;
pub(crate) use quic::serve_quic;
pub(crate) use session::{SessionConnection, SessionService};
pub use websocket::WebSocketConfig;
pub(crate) use websocket::serve_websocket;
