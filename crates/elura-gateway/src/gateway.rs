use std::sync::Arc;

use axum::Router;
use elura_core::account_version::AccountVersionStore;
use elura_core::gateway_world::WorldDiscovery;
use elura_core::online::OnlineDirectory;
use elura_core::ownership::OwnershipResolver;
use elura_core::push::PushTransport;
use elura_core::session::SessionControlTransport;
use elura_core::ticket::ReplayStore;
use elura_core::{Error, Result};
use elura_runtime::observability::AdminServerConfig;

use super::builder::GatewayBuilder;
use super::observability::AdmissionAdmin;
use super::transport::{
    AccountVersionSettings, AdmissionController, AdmissionSettings, GatewayTransport,
    SessionObserver,
};
use super::{
    GatewayConfig, GatewayInfrastructure, GatewayInterceptor, GatewayOnlineConfig, GatewayServer,
    WorldClient,
};

/// Application-facing Gateway assembly and startup API.
///
/// At least one [`GatewayTransport`] must be registered before the Gateway can
/// be built. TCP is explicit just like every other client protocol.
///
/// Configuration errors are retained by the fluent API and returned by
/// [`Self::build`] or [`Self::run`].
pub struct Gateway {
    builder: Option<GatewayBuilder>,
    error: Option<Error>,
    http: Vec<(String, Router)>,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Self {
        match GatewayBuilder::new(config) {
            Ok(builder) => Self {
                builder: Some(builder),
                error: None,
                http: Vec::new(),
            },
            Err(error) => Self {
                builder: None,
                error: Some(error),
                http: Vec::new(),
            },
        }
    }

    pub fn replay_store(self, replay: Arc<dyn ReplayStore>) -> Self {
        self.configure(|builder| Ok(builder.with_replay_store(replay)))
    }

    pub fn infrastructure(self, infrastructure: GatewayInfrastructure) -> Self {
        self.configure(|builder| builder.with_infrastructure(infrastructure))
    }

    pub fn online_directory(
        self,
        directory: Arc<dyn OnlineDirectory>,
        config: GatewayOnlineConfig,
    ) -> Self {
        self.configure(|builder| Ok(builder.with_online_directory(directory, config)))
    }

    pub fn push_transport(self, push: Arc<dyn PushTransport>) -> Self {
        self.configure(|builder| Ok(builder.with_push_transport(push)))
    }

    pub fn session_control_transport(
        self,
        session_control: Arc<dyn SessionControlTransport>,
    ) -> Self {
        self.configure(|builder| Ok(builder.with_session_control_transport(session_control)))
    }

    pub fn admission(
        self,
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Self {
        self.configure(|builder| Ok(builder.with_admission(controller, settings)))
    }

    pub fn ownership(self, shard_count: u32, resolver: Arc<dyn OwnershipResolver>) -> Self {
        self.configure(|builder| Ok(builder.with_ownership(shard_count, resolver)))
    }

    pub fn account_version_store(
        self,
        store: Arc<dyn AccountVersionStore>,
        settings: AccountVersionSettings,
    ) -> Self {
        self.configure(|builder| Ok(builder.with_account_version_store(store, settings)))
    }

    pub fn session_observer(self, observer: Arc<dyn SessionObserver>) -> Self {
        self.configure(|builder| Ok(builder.with_session_observer(observer)))
    }

    pub fn readiness_probe(
        self,
        name: impl Into<Arc<str>>,
        probe: Arc<dyn elura_runtime::observability::ReadinessProbe>,
    ) -> Self {
        self.configure(|builder| builder.with_readiness_probe(name, probe))
    }

    pub fn world_client(self, world: Arc<dyn WorldClient>) -> Self {
        self.configure(|builder| Ok(builder.with_world_client(world)))
    }

    pub fn world_discovery(self, discovery: Arc<dyn WorldDiscovery>) -> Self {
        self.configure(|builder| Ok(builder.with_world_discovery(discovery)))
    }

    pub fn interceptor<I>(self, interceptor: I) -> Self
    where
        I: GatewayInterceptor,
    {
        self.configure(|builder| Ok(builder.with_interceptor(interceptor)))
    }

    pub fn admission_admin(self, admission_admin: Arc<dyn AdmissionAdmin>) -> Self {
        self.configure(|builder| Ok(builder.with_admission_admin(admission_admin)))
    }

    /// Adds a client transport endpoint supervised with the Gateway lifecycle.
    pub fn transport<T>(self, transport: T) -> Self
    where
        T: GatewayTransport,
    {
        self.configure(|builder| builder.with_transport(transport))
    }

    /// Adds an application HTTP server supervised with the Gateway lifecycle.
    pub fn http(mut self, listen: impl std::fmt::Display, router: Router) -> Self {
        self.http.push((listen.to_string(), router));
        self
    }

    pub fn build(mut self) -> Result<GatewayServer> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        let mut server = self
            .builder
            .take()
            .ok_or_else(|| Error::Internal("Gateway builder is unavailable".into()))?
            .build()?;
        for (listen, router) in self.http {
            server.add_http(listen, router)?;
        }
        server.validate_listeners()?;
        Ok(server)
    }

    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        self.build()?.run(admin).await
    }

    fn configure(mut self, update: impl FnOnce(GatewayBuilder) -> Result<GatewayBuilder>) -> Self {
        if let Some(builder) = self.builder.take() {
            match update(builder) {
                Ok(builder) => self.builder = Some(builder),
                Err(error) => self.record(error),
            }
        }
        self
    }

    fn record(&mut self, error: Error) {
        if self.error.is_none() {
            self.error = Some(error);
            self.builder = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::WorldRequest;
    use crate::transport::{
        TcpConfig, TcpTransport, UdpConfig, WebSocketConfig, WebTransportConfig,
    };

    struct ReadyWorld;

    #[async_trait::async_trait]
    impl WorldClient for ReadyWorld {
        async fn command(&self, request: WorldRequest) -> Result<Bytes> {
            Ok(request.payload)
        }
    }

    fn config() -> GatewayConfig {
        let mut config = GatewayConfig::default();
        config.ticket.key = "k".repeat(32);
        config
    }

    #[test]
    fn registers_tcp_and_optional_protocols_in_one_transport_registry() {
        let tcp = TcpTransport::new(TcpConfig::default()).unwrap();
        let webtransport = WebTransportConfig::from_pem_files(
            "127.0.0.1:17005".parse().unwrap(),
            "certificate.pem",
            "key.pem",
        );
        let server = Gateway::new(config())
            .world_client(Arc::new(ReadyWorld))
            .transport(tcp)
            .transport(WebSocketConfig::default())
            .transport(webtransport)
            .build()
            .unwrap();
        let transports = server
            .transports
            .iter()
            .map(|transport| transport.name())
            .collect::<Vec<_>>();
        assert_eq!(transports, ["tcp", "websocket", "webtransport"]);
    }

    #[test]
    fn returns_deferred_configuration_and_listener_errors() {
        let mut invalid = config();
        invalid.max_connections = 0;
        assert!(Gateway::new(invalid).build().is_err());

        let tcp = TcpTransport::new(TcpConfig::default()).unwrap();
        let result = Gateway::new(config())
            .world_client(Arc::new(ReadyWorld))
            .transport(tcp)
            .http("127.0.0.1:17000", Router::new())
            .build();
        assert!(
            matches!(result, Err(Error::InvalidConfig(message)) if message.contains("conflict"))
        );
    }

    #[test]
    fn tcp_and_udp_can_share_a_numeric_port() {
        let tcp = TcpTransport::new(TcpConfig::default()).unwrap();
        let udp = UdpConfig {
            listen: TcpConfig::default().listen,
            ..UdpConfig::default()
        };
        let server = Gateway::new(config())
            .world_client(Arc::new(ReadyWorld))
            .transport(tcp)
            .transport(udp)
            .build()
            .unwrap();
        assert_eq!(
            server
                .transports
                .iter()
                .map(|transport| transport.name())
                .collect::<Vec<_>>(),
            ["tcp", "udp"]
        );
    }

    #[test]
    fn rejects_a_gateway_without_a_transport() {
        let result = Gateway::new(config())
            .world_client(Arc::new(ReadyWorld))
            .build();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig(message)) if message.contains("at least one transport")
        ));
    }
}
