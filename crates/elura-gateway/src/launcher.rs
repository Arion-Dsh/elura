use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::observability::{AdminServer, AdmissionAdmin, GatewayAdmin};
use crate::protection::ProtectionConfig;
use crate::transport::{
    AdmissionController, AdmissionSettings, ProxyProtocolConfig, QuicConfig, RealmAdmission,
    TrustedProxies,
};
use elura_core::gateway_world::{GatewayWorldRoutingConfig, WorldDiscovery};
use elura_core::online::{DuplicateLoginMode, OnlineDirectory};
use elura_core::push::PushTransport;
use elura_core::session::SessionControlTransport;
use elura_core::ticket::{MemoryReplayStore, ReplayStore, TicketService};
use elura_core::{Error, Result};
use elura_runtime::internal::{ClientTlsConfig, InternalToken};
use elura_runtime::launch::{LaunchAdminConfig, ServerTlsFilesConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::{
    Gateway, GatewayConfig, MemoryWorldRouteDirectory, RouteWorldClient, WorldClient,
    WorldRouteUpdater,
};

/// Runtime configuration assembled by the upper application.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayLaunchConfig {
    pub gateway: GatewayConfig,
    pub ticket: GatewayTicketConfig,
    #[serde(skip)]
    pub internal_token: String,
    pub admin: LaunchAdminConfig,
    pub protection: Option<ProtectionConfig>,
    pub tls: Option<ServerTlsFilesConfig>,
    pub world_tls: Option<GatewayWorldTlsConfig>,
    pub world_routing: GatewayWorldRoutingConfig,
    pub quic: Option<QuicConfig>,
    pub proxy_protocol: Option<GatewayProxyProtocolLaunchConfig>,
    pub realm_admission: Option<GatewayRealmAdmissionConfig>,
}

impl Default for GatewayLaunchConfig {
    fn default() -> Self {
        Self {
            gateway: GatewayConfig::default(),
            ticket: GatewayTicketConfig::default(),
            internal_token: String::new(),
            admin: LaunchAdminConfig {
                listen: "127.0.0.1:17001".parse().expect("static address"),
                token: None,
                component: "gateway".into(),
                instance_id: "gateway-1".into(),
            },
            protection: None,
            tls: None,
            world_tls: None,
            world_routing: GatewayWorldRoutingConfig::default(),
            quic: None,
            proxy_protocol: None,
            realm_admission: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayTicketConfig {
    #[serde(skip)]
    pub key: String,
    pub issuer: String,
    pub audience: String,
    pub ttl: Duration,
}

impl Default for GatewayTicketConfig {
    fn default() -> Self {
        Self {
            key: String::new(),
            issuer: "game-login".into(),
            audience: "game-gateway".into(),
            ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayWorldTlsConfig {
    pub ca_file: Option<PathBuf>,
    pub client_certificate_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub server_name: String,
}

impl GatewayWorldTlsConfig {
    fn build(self) -> Result<ClientTlsConfig> {
        ClientTlsConfig::from_pem_files(
            self.ca_file.as_deref(),
            self.client_certificate_file.as_deref(),
            self.client_key_file.as_deref(),
            self.server_name,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayProxyProtocolLaunchConfig {
    pub trusted_proxy_cidrs: Vec<String>,
    pub header_timeout: Duration,
    pub max_header_bytes: usize,
}

impl Default for GatewayProxyProtocolLaunchConfig {
    fn default() -> Self {
        Self {
            trusted_proxy_cidrs: Vec::new(),
            header_timeout: Duration::from_secs(5),
            max_header_bytes: 1024,
        }
    }
}

impl GatewayProxyProtocolLaunchConfig {
    fn build(self) -> Result<ProxyProtocolConfig> {
        let mut config = ProxyProtocolConfig::new(TrustedProxies::parse(
            self.trusted_proxy_cidrs.iter().map(String::as_str),
        )?)?;
        config.header_timeout = self.header_timeout;
        config.max_header_bytes = self.max_header_bytes;
        config.validate()?;
        Ok(config)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayRealmAdmissionConfig {
    pub realms: Vec<(u32, u32)>,
    #[serde(default)]
    pub settings: AdmissionSettings,
}

/// Escape hatch for infrastructure that is not represented by
/// [`GatewayLaunchConfig`].
pub trait GatewayExtension: Send + Sync + 'static {
    fn configure(&self, gateway: Gateway) -> Result<Gateway>;
}

impl<F> GatewayExtension for F
where
    F: Send + Sync + 'static + Fn(Gateway) -> Result<Gateway>,
{
    fn configure(&self, gateway: Gateway) -> Result<Gateway> {
        self(gateway)
    }
}

pub struct GatewayLauncher {
    config: GatewayLaunchConfig,
    infrastructure: GatewayInfrastructure,
    world: Option<Arc<dyn WorldClient>>,
    discovery: Option<Arc<dyn WorldDiscovery>>,
    extensions: Vec<Arc<dyn GatewayExtension>>,
}

/// Online-session services installed into a Gateway.
struct GatewayOnlineServices {
    gateway_id: String,
    directory: Arc<dyn OnlineDirectory>,
    lease_ttl: Duration,
    renew_interval: Duration,
    duplicate_login: DuplicateLoginMode,
}

/// Admission evaluation and its optional administrative mutation surface.
struct GatewayAdmissionServices {
    controller: Arc<dyn AdmissionController>,
    settings: AdmissionSettings,
}

#[derive(Default)]
pub struct GatewayInfrastructure {
    replay: Option<Arc<dyn ReplayStore>>,
    online: Option<GatewayOnlineServices>,
    push: Option<Arc<dyn PushTransport>>,
    session_control: Option<Arc<dyn SessionControlTransport>>,
    admission: Option<GatewayAdmissionServices>,
    admission_admin: Option<Arc<dyn AdmissionAdmin>>,
    readiness: Vec<(
        Arc<str>,
        Arc<dyn elura_runtime::observability::ReadinessProbe>,
    )>,
}

impl GatewayInfrastructure {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_replay_store(mut self, replay: Arc<dyn ReplayStore>) -> Self {
        self.replay = Some(replay);
        self
    }

    pub fn with_online_directory(
        mut self,
        gateway_id: impl Into<String>,
        directory: Arc<dyn OnlineDirectory>,
        lease_ttl: Duration,
        renew_interval: Duration,
        duplicate_login: DuplicateLoginMode,
    ) -> Self {
        self.online = Some(GatewayOnlineServices {
            gateway_id: gateway_id.into(),
            directory,
            lease_ttl,
            renew_interval,
            duplicate_login,
        });
        self
    }

    pub fn with_push_transport(mut self, push: Arc<dyn PushTransport>) -> Self {
        self.push = Some(push);
        self
    }

    pub fn with_session_control_transport(
        mut self,
        session_control: Arc<dyn SessionControlTransport>,
    ) -> Self {
        self.session_control = Some(session_control);
        self
    }

    pub fn with_admission(
        mut self,
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Self {
        self.admission = Some(GatewayAdmissionServices {
            controller,
            settings,
        });
        self
    }

    pub fn with_admission_admin(mut self, admin: Arc<dyn AdmissionAdmin>) -> Self {
        self.admission_admin = Some(admin);
        self
    }

    pub fn with_readiness_probe(
        mut self,
        name: impl Into<Arc<str>>,
        probe: Arc<dyn elura_runtime::observability::ReadinessProbe>,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() || self.readiness.iter().any(|(current, _)| current == &name) {
            return Err(Error::InvalidConfig(
                "infrastructure readiness probe name must be non-empty and unique".into(),
            ));
        }
        self.readiness.push((name, probe));
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if self.admission_admin.is_some() && self.admission.is_none() {
            return Err(Error::InvalidConfig(
                "admission admin requires an admission controller in the same infrastructure bundle"
                    .into(),
            ));
        }
        if let Some(online) = &self.online {
            if self.replay.is_none() {
                return Err(Error::InvalidConfig(
                    "distributed Gateway infrastructure requires an explicit shared ReplayStore"
                        .into(),
                ));
            }
            if online.gateway_id.trim().is_empty()
                || online.lease_ttl.is_zero()
                || online.renew_interval.is_zero()
                || online.renew_interval >= online.lease_ttl
            {
                return Err(Error::InvalidConfig(
                    "online directory requires a gateway ID and 0 < renew interval < TTL".into(),
                ));
            }
            if online.duplicate_login == DuplicateLoginMode::KickExisting
                && self.session_control.is_none()
            {
                return Err(Error::InvalidConfig(
                    "distributed kick_existing requires a Session control transport".into(),
                ));
            }
        }
        Ok(())
    }
}

impl GatewayLauncher {
    pub fn new(config: GatewayLaunchConfig) -> Result<Self> {
        config.gateway.validate()?;
        config.world_routing.validate()?;
        if let Some(quic) = &config.quic {
            quic.validate()?;
        }
        if config.ticket.ttl.is_zero() {
            return Err(Error::InvalidConfig(
                "gateway ticket TTL must be positive".into(),
            ));
        }
        Ok(Self {
            config,
            infrastructure: GatewayInfrastructure::default(),
            world: None,
            discovery: None,
            extensions: Vec::new(),
        })
    }

    pub fn with_replay_store(mut self, replay: Arc<dyn ReplayStore>) -> Self {
        self.infrastructure.replay = Some(replay);
        self
    }

    /// Replaces all infrastructure slots with an explicitly assembled bundle.
    pub fn with_infrastructure(mut self, infrastructure: GatewayInfrastructure) -> Result<Self> {
        infrastructure.validate()?;
        self.infrastructure = infrastructure;
        Ok(self)
    }

    pub fn with_online_directory(
        mut self,
        gateway_id: impl Into<String>,
        directory: Arc<dyn OnlineDirectory>,
        lease_ttl: Duration,
        renew_interval: Duration,
        duplicate_login: DuplicateLoginMode,
    ) -> Self {
        self.infrastructure = self.infrastructure.with_online_directory(
            gateway_id,
            directory,
            lease_ttl,
            renew_interval,
            duplicate_login,
        );
        self
    }

    pub fn with_push_transport(mut self, push: Arc<dyn PushTransport>) -> Self {
        self.infrastructure.push = Some(push);
        self
    }

    pub fn with_session_control_transport(
        mut self,
        session_control: Arc<dyn SessionControlTransport>,
    ) -> Self {
        self.infrastructure.session_control = Some(session_control);
        self
    }

    pub fn with_admission(
        mut self,
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Self {
        self.infrastructure = self.infrastructure.with_admission(controller, settings);
        self
    }

    pub fn with_readiness_probe(
        mut self,
        name: impl Into<Arc<str>>,
        probe: Arc<dyn elura_runtime::observability::ReadinessProbe>,
    ) -> Result<Self> {
        self.infrastructure = self.infrastructure.with_readiness_probe(name, probe)?;
        Ok(self)
    }

    pub fn with_world_client(mut self, world: Arc<dyn WorldClient>) -> Self {
        self.world = Some(world);
        self
    }

    pub fn with_world_discovery(mut self, discovery: Arc<dyn WorldDiscovery>) -> Self {
        self.discovery = Some(discovery);
        self
    }

    pub fn with_extension(mut self, extension: impl GatewayExtension) -> Self {
        self.extensions.push(Arc::new(extension));
        self
    }

    /// Exposes admission-policy mutations on the private admin server.
    ///
    /// Pass the same backing policy used by the Gateway's admission controller;
    /// for example, an `Arc<RedisAdmissionController>` in a Redis deployment.
    pub fn with_admission_admin(mut self, admission_admin: Arc<dyn AdmissionAdmin>) -> Self {
        self.infrastructure.admission_admin = Some(admission_admin);
        self
    }

    #[doc(hidden)]
    pub fn build_parts(self) -> Result<GatewayParts> {
        self.infrastructure.validate()?;
        let config = self.config;
        let infrastructure = self.infrastructure;
        let tickets = Arc::new(TicketService::new(
            config.ticket.key,
            config.ticket.issuer,
            config.ticket.audience,
            config.ticket.ttl,
        )?);
        let world_tls = config
            .world_tls
            .map(GatewayWorldTlsConfig::build)
            .transpose()?;
        let (world, discovery): (Arc<dyn WorldClient>, Option<DiscoveryParts>) = match self.world {
            Some(world) => (world, None),
            None => {
                let internal_token = InternalToken::new(config.internal_token)?;
                let routing = config.world_routing;
                let directory = Arc::new(MemoryWorldRouteDirectory::new());
                let updater: Arc<dyn WorldRouteUpdater> = directory.clone();
                let mut client = RouteWorldClient::new(
                    directory,
                    config.gateway.max_payload,
                    routing.pool_size,
                )?
                .with_internal_token(internal_token.clone())
                .with_max_in_flight_per_connection(routing.max_in_flight_per_connection)?;
                if let Some(tls) = world_tls.clone() {
                    client = client.with_tls(tls);
                }
                let discovery = self.discovery.ok_or_else(|| {
                    Error::InvalidConfig(
                        "World discovery was not injected; construct an adapter in the upper application and pass it with with_world_discovery".into(),
                    )
                })?;
                (
                    Arc::new(client),
                    Some(DiscoveryParts { discovery, updater }),
                )
            }
        };
        let replay = infrastructure
            .replay
            .unwrap_or_else(|| Arc::new(MemoryReplayStore::default()));
        let mut gateway = Gateway::new(config.gateway, tickets, replay, world)?;
        if let Some(protection) = config.protection {
            gateway = gateway.with_protection(protection)?;
        }
        if let Some(tls) = config.tls {
            gateway = gateway.with_tls(tls.build()?);
        }
        if let Some(proxy) = config.proxy_protocol {
            gateway = gateway.with_proxy_protocol(proxy.build()?)?;
        }
        if let Some(admission) = config.realm_admission {
            gateway = gateway.with_admission(
                Arc::new(RealmAdmission::new(admission.realms)?),
                admission.settings,
            )?;
        }
        if let Some(online) = infrastructure.online {
            gateway = gateway.with_online_directory(
                online.gateway_id,
                online.directory,
                online.lease_ttl,
                online.renew_interval,
                online.duplicate_login,
            )?;
        }
        if let Some(push) = infrastructure.push {
            gateway = gateway.with_push_transport(push);
        }
        if let Some(session_control) = infrastructure.session_control {
            gateway = gateway.with_session_control_transport(session_control);
        }
        if let Some(admission) = infrastructure.admission {
            gateway = gateway.with_admission(admission.controller, admission.settings)?;
        }
        for (name, probe) in infrastructure.readiness {
            gateway = gateway.with_readiness_probe(name, probe)?;
        }
        for extension in self.extensions {
            gateway = extension.configure(gateway)?;
        }
        let gateway = Arc::new(gateway);
        let mut gateway_admin = GatewayAdmin::new(gateway.clone());
        if let Some(admission_admin) = infrastructure.admission_admin {
            gateway_admin = gateway_admin.with_admission(admission_admin);
        }
        let admin = AdminServer::new(config.admin.into(), gateway.clone())?
            .with_gateway_admin(gateway_admin);
        Ok(GatewayParts {
            gateway,
            admin,
            discovery,
            quic: config.quic,
        })
    }

    pub async fn run(self) -> Result<()> {
        self.run_with_trigger(async {
            let _ = elura_runtime::lifecycle::shutdown_signal().await;
        })
        .await
    }

    async fn run_with_trigger(
        self,
        trigger: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let parts = self.build_parts()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let signal = tokio::spawn({
            let shutdown_tx = shutdown_tx.clone();
            async move {
                trigger.await;
                let _ = shutdown_tx.send(true);
            }
        });

        let gateway = parts.gateway;
        let mut tasks = JoinSet::new();
        let gateway_shutdown = shutdown_rx.clone();
        tasks.spawn({
            let gateway = gateway.clone();
            async move { gateway.serve_tcp(gateway_shutdown).await }
        });
        if let Some(quic) = parts.quic {
            let quic_shutdown = shutdown_rx.clone();
            let quic_gateway = gateway.clone();
            tasks.spawn(async move { quic_gateway.serve_quic(quic, quic_shutdown).await });
        }
        let admin_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { parts.admin.serve(admin_shutdown).await });
        if let Some(discovery) = parts.discovery {
            let discovery_shutdown = shutdown_rx.clone();
            tasks.spawn(async move {
                discovery
                    .discovery
                    .run(discovery.updater, discovery_shutdown)
                    .await
            });
        }
        spawn_subscriptions(&gateway, shutdown_rx, &mut tasks);

        let mut first_error = None;
        while let Some(completed) = tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(error) if first_error.is_none() => {
                    first_error = Some(Error::Internal(format!(
                        "Gateway service task panicked: {error}"
                    )))
                }
                _ => {}
            }
            let _ = shutdown_tx.send(true);
        }
        signal.abort();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn spawn_subscriptions(
    gateway: &Arc<Gateway>,
    shutdown: watch::Receiver<bool>,
    tasks: &mut JoinSet<Result<()>>,
) {
    if gateway.push.is_some() {
        let subscriber = gateway.clone();
        let push_shutdown = shutdown.clone();
        tasks.spawn(async move { subscriber.subscribe_push(push_shutdown).await });
    }
    if gateway.session_control.is_some() {
        let subscriber = gateway.clone();
        tasks.spawn(async move { subscriber.subscribe_session_control(shutdown).await });
    }
}

#[doc(hidden)]
pub struct DiscoveryParts {
    pub discovery: Arc<dyn WorldDiscovery>,
    pub updater: Arc<dyn WorldRouteUpdater>,
}

#[doc(hidden)]
pub struct GatewayParts {
    pub gateway: Arc<Gateway>,
    pub admin: AdminServer,
    pub discovery: Option<DiscoveryParts>,
    pub quic: Option<QuicConfig>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use elura_core::online::MemoryOnlineDirectory;
    use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTransport};
    use elura_core::session::{
        SessionControlEvent, SessionControlHandler, SessionControlTransport,
    };

    use super::*;

    struct ReadyWorld;

    #[async_trait::async_trait]
    impl WorldClient for ReadyWorld {
        async fn command(&self, request: super::super::WorldRequest) -> Result<Bytes> {
            Ok(request.payload)
        }
    }

    struct PendingPushTransport {
        subscriptions: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl PushTransport for PendingPushTransport {
        async fn publish(&self, request: &PushRequest) -> Result<PushReceipt> {
            Ok(PushReceipt::accepted(request, 0))
        }

        async fn subscribe(
            &self,
            _handler: Arc<dyn PushHandler>,
            mut shutdown: watch::Receiver<bool>,
        ) -> Result<()> {
            self.subscriptions.fetch_add(1, Ordering::Release);
            while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
            Ok(())
        }
    }

    struct PendingSessionControlTransport {
        subscriptions: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SessionControlTransport for PendingSessionControlTransport {
        async fn publish(&self, _event: &SessionControlEvent) -> Result<()> {
            Ok(())
        }

        async fn subscribe(
            &self,
            _handler: Arc<dyn SessionControlHandler>,
            mut shutdown: watch::Receiver<bool>,
        ) -> Result<()> {
            self.subscriptions.fetch_add(1, Ordering::Release);
            while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
            Ok(())
        }
    }

    fn config() -> GatewayLaunchConfig {
        GatewayLaunchConfig {
            ticket: GatewayTicketConfig {
                key: "k".repeat(32),
                ..GatewayTicketConfig::default()
            },
            internal_token: "t".repeat(32),
            ..GatewayLaunchConfig::default()
        }
    }

    #[test]
    fn launcher_builds_without_application_handlers() {
        GatewayLauncher::new(config())
            .unwrap()
            .with_world_client(Arc::new(crate::TcpWorldClient::new(
                "127.0.0.1:18000".parse().unwrap(),
                1024,
            )))
            .build_parts()
            .unwrap();
    }

    #[test]
    fn json_keeps_secrets_out_of_configuration() {
        let encoded = serde_json::to_string(&config()).unwrap();
        assert!(!encoded.contains(&"k".repeat(32)));
        assert!(!encoded.contains(&"t".repeat(32)));
        assert!(!encoded.contains("redis://"));
        let decoded: GatewayLaunchConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(decoded.world_routing.pool_size, 1);
        assert!(serde_json::from_str::<GatewayLaunchConfig>(r#"{"world_discovery":{}}"#).is_err());
    }

    #[test]
    fn extension_remains_the_custom_infrastructure_escape_hatch() {
        let result = GatewayLauncher::new(config())
            .unwrap()
            .with_world_client(Arc::new(crate::TcpWorldClient::new(
                "127.0.0.1:18000".parse().unwrap(),
                1024,
            )))
            .with_extension(|_| Err(Error::Unavailable))
            .build_parts();
        assert!(matches!(result, Err(Error::Unavailable)));
    }

    #[tokio::test]
    async fn launcher_runs_distributed_subscriptions_until_shutdown() {
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let push = Arc::new(PendingPushTransport {
            subscriptions: subscriptions.clone(),
        });
        let session_control = Arc::new(PendingSessionControlTransport {
            subscriptions: subscriptions.clone(),
        });
        let parts = GatewayLauncher::new(config())
            .unwrap()
            .with_world_client(Arc::new(ReadyWorld))
            .with_push_transport(push)
            .with_session_control_transport(session_control)
            .build_parts()
            .unwrap();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        spawn_subscriptions(&parts.gateway, shutdown_rx, &mut tasks);
        tokio::time::timeout(Duration::from_secs(1), async {
            while subscriptions.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(subscriptions.load(Ordering::Acquire), 2);
    }

    #[test]
    fn infrastructure_rejects_distributed_gateway_without_shared_replay_store() {
        let result = GatewayLauncher::new(config())
            .unwrap()
            .with_world_client(Arc::new(ReadyWorld))
            .with_online_directory(
                "gateway-a",
                Arc::new(MemoryOnlineDirectory::default()),
                Duration::from_secs(30),
                Duration::from_secs(10),
                DuplicateLoginMode::AllowMultiple,
            )
            .build_parts();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig(message)) if message.contains("ReplayStore")
        ));
    }

    #[test]
    fn infrastructure_rejects_cross_node_kick_without_control_transport() {
        let result = GatewayLauncher::new(config())
            .unwrap()
            .with_world_client(Arc::new(ReadyWorld))
            .with_replay_store(Arc::new(MemoryReplayStore::default()))
            .with_online_directory(
                "gateway-a",
                Arc::new(MemoryOnlineDirectory::default()),
                Duration::from_secs(30),
                Duration::from_secs(10),
                DuplicateLoginMode::KickExisting,
            )
            .build_parts();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig(message)) if message.contains("Session control")
        ));
    }
}
