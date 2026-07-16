//! Extensible server transport implementations.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::{Error, Result};
use elura_runtime::launch::ServerTlsFilesConfig;
use elura_runtime::security::ServerTlsConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info};

use crate::GatewayServer;

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
pub(crate) use session::{SessionConnection, SessionIoConfig, SessionService, serve_stream};
pub use websocket::WebSocketConfig;

/// A bound transport endpoint driven by the Gateway accept loop.
#[async_trait]
pub trait GatewayTransportListener: Send + 'static {
    type Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)>;
}

/// A client transport endpoint supervised by [`crate::GatewayServer`].
///
/// Built-in TCP, WebSocket and QUIC endpoints all use this contract, so adding
/// another transport does not add another method to the application-facing
/// [`crate::Gateway`] API.
#[async_trait]
pub trait GatewayTransport: Send + Sync + 'static {
    type Listener: GatewayTransportListener;

    fn name(&self) -> &'static str;

    fn listen(&self) -> SocketAddr;

    fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn bind(&self) -> Result<Self::Listener>;
}

#[async_trait]
pub(crate) trait RegisteredGatewayTransport: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn listen(&self) -> SocketAddr;
    async fn serve(
        &self,
        gateway: Arc<GatewayServer>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>;
}

struct TransportRegistration<T>(T);

pub(crate) fn register<T>(transport: T) -> Arc<dyn RegisteredGatewayTransport>
where
    T: GatewayTransport,
{
    Arc::new(TransportRegistration(transport))
}

#[async_trait]
impl<T> RegisteredGatewayTransport for TransportRegistration<T>
where
    T: GatewayTransport,
{
    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn listen(&self) -> SocketAddr {
        self.0.listen()
    }

    async fn serve(
        &self,
        gateway: Arc<GatewayServer>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut listener = self.0.bind().await?;
        info!(address = %self.0.listen(), transport = self.0.name(), "gateway listening");
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                accepted = listener.accept() => {
                    let (peer, stream) = accepted?;
                    let gateway = gateway.clone();
                    tokio::spawn(async move {
                        if let Err(error) = gateway.serve_transport_stream(peer, stream).await {
                            debug!(%peer, %error, "client disconnected");
                        }
                    });
                }
            }
        }
    }
}

/// Serializable configuration owned by the ELR2 TCP transport.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct TcpConfig {
    pub listen: SocketAddr,
    pub keepalive: Duration,
    pub tls_handshake_timeout: Duration,
    pub max_pending_handshakes: usize,
    pub tls: Option<ServerTlsFilesConfig>,
    pub proxy_protocol: Option<TcpProxyProtocolConfig>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17000".parse().expect("static address"),
            keepalive: Duration::from_secs(30),
            tls_handshake_timeout: Duration::from_secs(5),
            max_pending_handshakes: 1024,
            tls: None,
            proxy_protocol: None,
        }
    }
}

impl TcpConfig {
    fn validate(&self) -> Result<()> {
        if self.keepalive.is_zero()
            || self.tls_handshake_timeout.is_zero()
            || self.max_pending_handshakes == 0
        {
            return Err(Error::InvalidConfig(
                "TCP transport timeouts must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct TcpProxyProtocolConfig {
    pub trusted_proxy_cidrs: Vec<String>,
    pub header_timeout: Duration,
    pub max_header_bytes: usize,
}

impl Default for TcpProxyProtocolConfig {
    fn default() -> Self {
        Self {
            trusted_proxy_cidrs: Vec::new(),
            header_timeout: Duration::from_secs(5),
            max_header_bytes: 1024,
        }
    }
}

impl TcpProxyProtocolConfig {
    fn build(&self) -> Result<ProxyProtocolConfig> {
        let mut config = ProxyProtocolConfig::new(TrustedProxies::parse(
            self.trusted_proxy_cidrs.iter().map(String::as_str),
        )?)?;
        config.header_timeout = self.header_timeout;
        config.max_header_bytes = self.max_header_bytes;
        config.validate()?;
        Ok(config)
    }
}

/// The built-in ELR2 TCP transport.
#[derive(Clone)]
pub struct TcpTransport {
    config: TcpConfig,
    tls: Option<ServerTlsConfig>,
    proxy_protocol: Option<ProxyProtocolConfig>,
}

impl TcpTransport {
    pub fn new(config: TcpConfig) -> Result<Self> {
        config.validate()?;
        let tls = config.tls.clone().map(|tls| tls.build()).transpose()?;
        let proxy_protocol = config
            .proxy_protocol
            .as_ref()
            .map(TcpProxyProtocolConfig::build)
            .transpose()?;
        Ok(Self {
            config,
            tls,
            proxy_protocol,
        })
    }

    pub fn with_tls(mut self, tls: ServerTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_proxy_protocol(mut self, proxy_protocol: ProxyProtocolConfig) -> Result<Self> {
        proxy_protocol.validate()?;
        self.proxy_protocol = Some(proxy_protocol);
        Ok(self)
    }

    pub(crate) fn config(&self) -> &TcpConfig {
        &self.config
    }

    pub(crate) fn tls(&self) -> Option<ServerTlsConfig> {
        self.tls.clone()
    }

    pub(crate) fn proxy_protocol(&self) -> Option<ProxyProtocolConfig> {
        self.proxy_protocol.clone()
    }
}

#[async_trait]
impl GatewayTransport for TcpTransport {
    type Listener = tcp::TcpGatewayListener;

    fn name(&self) -> &'static str {
        "tcp"
    }

    fn listen(&self) -> SocketAddr {
        self.config.listen
    }

    fn validate(&self) -> Result<()> {
        self.config.validate()
    }

    async fn bind(&self) -> Result<Self::Listener> {
        tcp::bind(self.clone()).await
    }
}

#[async_trait]
impl GatewayTransport for WebSocketConfig {
    type Listener = websocket::WebSocketGatewayListener;

    fn name(&self) -> &'static str {
        "websocket"
    }

    fn listen(&self) -> SocketAddr {
        self.listen
    }

    fn validate(&self) -> Result<()> {
        WebSocketConfig::validate(self)
    }

    async fn bind(&self) -> Result<Self::Listener> {
        websocket::bind(self.clone()).await
    }
}

#[async_trait]
impl GatewayTransport for QuicConfig {
    type Listener = quic::QuicGatewayListener;

    fn name(&self) -> &'static str {
        "quic"
    }

    fn listen(&self) -> SocketAddr {
        self.listen
    }

    fn validate(&self) -> Result<()> {
        QuicConfig::validate(self)
    }

    async fn bind(&self) -> Result<Self::Listener> {
        quic::bind(self.clone()).await
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn tcp_config_is_independent_and_strict() {
        let config: TcpConfig = serde_json::from_str(r#"{"listen":"0.0.0.0:19000"}"#).unwrap();
        assert_eq!(config.listen.port(), 19000);
        assert_eq!(config.keepalive, TcpConfig::default().keepalive);
        assert!(serde_json::from_str::<TcpConfig>(r#"{"unknown":true}"#).is_err());
    }

    #[test]
    fn tcp_transport_rejects_invalid_protocol_settings() {
        let result = TcpTransport::new(TcpConfig {
            keepalive: Duration::ZERO,
            ..TcpConfig::default()
        });
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
