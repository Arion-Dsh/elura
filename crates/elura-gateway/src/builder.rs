//! Internal Gateway assembly.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::discovery::{WorldClient, WorldDiscovery, WorldRouteUpdater};
use crate::observability::AdmissionAdmin;
use crate::presence::{DuplicateLoginMode, OnlineDirectory};
use crate::transport::{
    AccountVersionSettings, AdmissionController, AdmissionSettings, GatewayTransport,
    RealmAdmission, RegisteredGatewayTransport, SessionObserver, register,
};
use elura_core::account_version::AccountVersionStore;
use elura_core::ownership::OwnershipResolver;
use elura_core::push::PushTransport;
use elura_core::replay_protection::{MemoryReplayProtectionStore, ReplayProtectionStore};
use elura_core::session::SessionControlTransport;
use elura_core::ticket::TicketService;
use elura_core::{Error, Result};
use elura_runtime::security::{ClientTlsConfig, InternalToken};
use serde::{Deserialize, Serialize};

use super::{
    GatewayConfig, GatewayInterceptor, GatewayServer, MemoryWorldRouteDirectory, RouteWorldClient,
};

type DiscoveryBinding = (Arc<dyn WorldDiscovery>, Arc<dyn WorldRouteUpdater>);

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayTicketConfig {
    #[serde(skip)]
    pub key: String,
    pub issuer: String,
    pub audience: String,
    /// Lifetime of application-login tickets.
    pub login_ttl: Duration,
    /// Lifetime of rotating tickets used for silent reconnects.
    pub reconnect_ttl: Duration,
}

impl Default for GatewayTicketConfig {
    fn default() -> Self {
        Self {
            key: String::new(),
            issuer: "game-login".into(),
            audience: "game-gateway".into(),
            login_ttl: Duration::from_secs(60),
            reconnect_ttl: Duration::from_secs(30 * 60),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayWorldTlsConfig {
    pub ca_file: Option<PathBuf>,
    pub client_certificate_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub server_name: String,
}

impl GatewayWorldTlsConfig {
    /// Creates World-client TLS configuration using the WebPKI root set.
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            ca_file: None,
            client_certificate_file: None,
            client_key_file: None,
            server_name: server_name.into(),
        }
    }

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
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayRealmAdmissionConfig {
    pub realms: Vec<(u32, u32)>,
    #[serde(default)]
    pub settings: AdmissionSettings,
}

/// Hard authenticated-Session limit for one region and realm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealmCapacityLimit {
    pub region_id: u32,
    pub realm_id: u32,
    pub max_sessions: u64,
}

/// Online Session and final realm-capacity admission configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayOnlineConfig {
    pub gateway_id: String,
    pub lease_ttl: Duration,
    pub renew_interval: Duration,
    pub duplicate_login: DuplicateLoginMode,
    #[serde(default)]
    pub realm_capacities: Vec<RealmCapacityLimit>,
    pub full_retry_after: Duration,
}

impl GatewayOnlineConfig {
    pub fn new(
        gateway_id: impl Into<String>,
        lease_ttl: Duration,
        renew_interval: Duration,
        duplicate_login: DuplicateLoginMode,
    ) -> Self {
        Self {
            gateway_id: gateway_id.into(),
            lease_ttl,
            renew_interval,
            duplicate_login,
            realm_capacities: Vec::new(),
            full_retry_after: Duration::from_secs(1),
        }
    }

    pub fn with_realm_capacity(mut self, region_id: u32, realm_id: u32, max_sessions: u64) -> Self {
        self.realm_capacities.push(RealmCapacityLimit {
            region_id,
            realm_id,
            max_sessions,
        });
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.gateway_id.trim().is_empty()
            || self.lease_ttl.is_zero()
            || self.renew_interval.is_zero()
            || self.renew_interval >= self.lease_ttl
            || self.full_retry_after.is_zero()
        {
            return Err(Error::InvalidConfig(
                "online config requires a Gateway ID, positive retry delay, and 0 < renew interval < TTL"
                    .into(),
            ));
        }
        let mut realms = std::collections::HashSet::new();
        if self.realm_capacities.iter().any(|limit| {
            limit.region_id == 0
                || limit.realm_id == 0
                || limit.max_sessions == 0
                || !realms.insert((limit.region_id, limit.realm_id))
        }) {
            return Err(Error::InvalidConfig(
                "realm capacities must be positive and unique".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn max_sessions(&self, region_id: u32, realm_id: u32) -> Option<u64> {
        self.realm_capacities
            .iter()
            .find(|limit| limit.region_id == region_id && limit.realm_id == realm_id)
            .map(|limit| limit.max_sessions)
    }
}

impl GatewayRealmAdmissionConfig {
    /// Creates realm admission configuration with default admission settings.
    pub fn new(realms: impl IntoIterator<Item = (u32, u32)>) -> Self {
        Self {
            realms: realms.into_iter().collect(),
            settings: AdmissionSettings::default(),
        }
    }
}

pub(crate) struct GatewayBuilder {
    config: GatewayConfig,
    infrastructure: GatewayInfrastructure,
    world: Option<Arc<dyn WorldClient>>,
    discovery: Option<Arc<dyn WorldDiscovery>>,
    interceptors: Vec<Arc<dyn GatewayInterceptor>>,
    transports: Vec<Arc<dyn RegisteredGatewayTransport>>,
}

/// Online-session services installed into a Gateway.
struct GatewayOnlineServices {
    directory: Arc<dyn OnlineDirectory>,
    config: GatewayOnlineConfig,
}

/// Admission evaluation and its optional administrative mutation surface.
struct GatewayAdmissionServices {
    controller: Arc<dyn AdmissionController>,
    settings: AdmissionSettings,
}

struct GatewayOwnershipServices {
    shard_count: u32,
    resolver: Arc<dyn OwnershipResolver>,
}

struct GatewayAccountVersionServices {
    store: Arc<dyn AccountVersionStore>,
    settings: AccountVersionSettings,
}

#[derive(Default)]
pub struct GatewayInfrastructure {
    replay: Option<Arc<dyn ReplayProtectionStore>>,
    online: Option<GatewayOnlineServices>,
    push: Option<Arc<dyn PushTransport>>,
    session_control: Option<Arc<dyn SessionControlTransport>>,
    admission: Option<GatewayAdmissionServices>,
    admission_admin: Option<Arc<dyn AdmissionAdmin>>,
    ownership: Option<GatewayOwnershipServices>,
    account_versions: Option<GatewayAccountVersionServices>,
    session_observers: Vec<Arc<dyn SessionObserver>>,
    readiness: Vec<(
        Arc<str>,
        Arc<dyn elura_runtime::observability::ReadinessProbe>,
    )>,
}

impl GatewayInfrastructure {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_replay_protection(mut self, replay: Arc<dyn ReplayProtectionStore>) -> Self {
        self.replay = Some(replay);
        self
    }

    pub fn with_online_directory(
        mut self,
        directory: Arc<dyn OnlineDirectory>,
        config: GatewayOnlineConfig,
    ) -> Self {
        self.online = Some(GatewayOnlineServices { directory, config });
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

    pub fn with_ownership(
        mut self,
        shard_count: u32,
        resolver: Arc<dyn OwnershipResolver>,
    ) -> Self {
        self.ownership = Some(GatewayOwnershipServices {
            shard_count,
            resolver,
        });
        self
    }

    pub fn with_account_version_store(
        mut self,
        store: Arc<dyn AccountVersionStore>,
        settings: AccountVersionSettings,
    ) -> Self {
        self.account_versions = Some(GatewayAccountVersionServices { store, settings });
        self
    }

    pub fn with_session_observer(mut self, observer: Arc<dyn SessionObserver>) -> Self {
        self.session_observers.push(observer);
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
        if self
            .ownership
            .as_ref()
            .is_some_and(|ownership| ownership.shard_count == 0)
        {
            return Err(Error::InvalidConfig(
                "gateway shard count must be positive".into(),
            ));
        }
        if let Some(account_versions) = &self.account_versions {
            account_versions.settings.validate()?;
        }
        if let Some(online) = &self.online {
            if self.replay.is_none() {
                return Err(Error::InvalidConfig(
                    "distributed Gateway infrastructure requires an explicit shared ReplayProtectionStore"
                        .into(),
                ));
            }
            online.config.validate()?;
            if online.config.duplicate_login == DuplicateLoginMode::KickExisting
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

impl GatewayBuilder {
    pub(crate) fn new(config: GatewayConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            infrastructure: GatewayInfrastructure::default(),
            world: None,
            discovery: None,
            interceptors: Vec::new(),
            transports: Vec::new(),
        })
    }

    pub fn with_replay_protection(mut self, replay: Arc<dyn ReplayProtectionStore>) -> Self {
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
        directory: Arc<dyn OnlineDirectory>,
        config: GatewayOnlineConfig,
    ) -> Self {
        self.infrastructure = self.infrastructure.with_online_directory(directory, config);
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

    pub fn with_ownership(
        mut self,
        shard_count: u32,
        resolver: Arc<dyn OwnershipResolver>,
    ) -> Self {
        self.infrastructure = self.infrastructure.with_ownership(shard_count, resolver);
        self
    }

    pub fn with_account_version_store(
        mut self,
        store: Arc<dyn AccountVersionStore>,
        settings: AccountVersionSettings,
    ) -> Self {
        self.infrastructure = self
            .infrastructure
            .with_account_version_store(store, settings);
        self
    }

    pub fn with_session_observer(mut self, observer: Arc<dyn SessionObserver>) -> Self {
        self.infrastructure = self.infrastructure.with_session_observer(observer);
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

    pub fn with_transport<T>(mut self, transport: T) -> Result<Self>
    where
        T: GatewayTransport,
    {
        transport.validate()?;
        self.transports.push(register(transport));
        Ok(self)
    }

    pub fn with_interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: GatewayInterceptor,
    {
        self.interceptors.push(Arc::new(interceptor));
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

    pub(crate) fn build(self) -> Result<GatewayServer> {
        self.infrastructure.validate()?;
        if self.transports.is_empty() {
            return Err(Error::InvalidConfig(
                "Gateway requires at least one transport".into(),
            ));
        }
        let config = self.config;
        let infrastructure = self.infrastructure;
        let tickets = Arc::new(TicketService::new(
            config.ticket.key.clone(),
            config.ticket.issuer.clone(),
            config.ticket.audience.clone(),
            config.ticket.login_ttl,
            config.ticket.reconnect_ttl,
        )?);
        let world_tls = config
            .world_tls
            .clone()
            .map(GatewayWorldTlsConfig::build)
            .transpose()?;
        let (world, discovery): (Arc<dyn WorldClient>, Option<DiscoveryBinding>) = match self.world
        {
            Some(world) => (world, None),
            None => {
                let internal_token =
                    InternalToken::new(config.internal_token.clone().ok_or_else(|| {
                        Error::InvalidConfig("standalone Gateway requires an internal token".into())
                    })?)?;
                let routing = config.world_routing.clone();
                let directory = Arc::new(MemoryWorldRouteDirectory::new());
                let updater: Arc<dyn WorldRouteUpdater> = directory.clone();
                let mut client =
                    RouteWorldClient::new(directory, config.max_payload, routing.pool_size)?
                        .with_internal_token(internal_token.clone())
                        .with_max_in_flight_per_connection(routing.max_in_flight_per_connection)?;
                if let Some(tls) = world_tls.clone() {
                    client = client.with_tls(tls);
                }
                let discovery = self.discovery.ok_or_else(|| {
                    Error::InvalidConfig(
                        "World discovery was not injected; construct an adapter in the upper application and pass it with Gateway::world_discovery".into(),
                    )
                })?;
                (Arc::new(client), Some((discovery, updater)))
            }
        };
        let replay = infrastructure
            .replay
            .unwrap_or_else(|| Arc::new(MemoryReplayProtectionStore::default()));
        let mut gateway = GatewayServer::new(config.clone(), tickets, replay, world)?;
        if let Some(protection) = config.protection.clone() {
            gateway = gateway.with_protection(protection)?;
        }
        if let Some(admission) = config.realm_admission.clone() {
            gateway = gateway.with_admission(
                Arc::new(RealmAdmission::new(admission.realms)?),
                admission.settings,
            )?;
        }
        if let Some(online) = infrastructure.online {
            gateway = gateway.with_online_directory(online.directory, online.config)?;
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
        if let Some(ownership) = infrastructure.ownership {
            gateway = gateway.with_ownership(ownership.shard_count, ownership.resolver)?;
        }
        if let Some(account_versions) = infrastructure.account_versions {
            gateway = gateway
                .with_account_version_store(account_versions.store, account_versions.settings)?;
        }
        for observer in infrastructure.session_observers {
            gateway = gateway.with_session_observer(observer);
        }
        for (name, probe) in infrastructure.readiness {
            gateway = gateway.with_readiness_probe(name, probe)?;
        }
        for interceptor in self.interceptors {
            gateway = gateway.with_interceptor(interceptor);
        }
        Ok(gateway.with_process_config(infrastructure.admission_admin, discovery, self.transports))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::presence::MemoryOnlineDirectory;
    use bytes::Bytes;
    use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTransport};
    use elura_core::session::{
        SessionControlEvent, SessionControlHandler, SessionControlTransport,
    };
    use tokio::sync::watch;
    use tokio::task::JoinSet;

    use super::*;
    use crate::transport::{TcpConfig, TcpTransport};

    struct ReadyWorld;

    #[async_trait::async_trait]
    impl WorldClient for ReadyWorld {
        async fn command(&self, request: elura_core::gateway_world::WorldRequest) -> Result<Bytes> {
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

    fn config() -> GatewayConfig {
        GatewayConfig {
            ticket: GatewayTicketConfig {
                key: "k".repeat(32),
                ..GatewayTicketConfig::default()
            },
            internal_token: Some("t".repeat(32)),
            ..GatewayConfig::default()
        }
    }

    fn builder() -> GatewayBuilder {
        let config = config();
        let tcp = TcpTransport::new(TcpConfig::default()).unwrap();
        GatewayBuilder::new(config)
            .unwrap()
            .with_transport(tcp)
            .unwrap()
    }

    #[test]
    fn builder_builds_without_application_handlers() {
        builder()
            .with_world_client(Arc::new(crate::TcpWorldClient::new(
                "127.0.0.1:18000".parse().unwrap(),
                1024,
            )))
            .build()
            .unwrap();
    }

    #[test]
    fn json_keeps_secrets_out_of_configuration() {
        let encoded = serde_json::to_string(&config()).unwrap();
        assert!(!encoded.contains(&"k".repeat(32)));
        assert!(!encoded.contains(&"t".repeat(32)));
        assert!(!encoded.contains("redis://"));
        let decoded: GatewayConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(decoded.world_routing.pool_size, 1);
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"world_discovery":{}}"#).is_err());
    }

    #[tokio::test]
    async fn server_runs_distributed_subscriptions_until_shutdown() {
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let push = Arc::new(PendingPushTransport {
            subscriptions: subscriptions.clone(),
        });
        let session_control = Arc::new(PendingSessionControlTransport {
            subscriptions: subscriptions.clone(),
        });
        let gateway = Arc::new(
            builder()
                .with_world_client(Arc::new(ReadyWorld))
                .with_push_transport(push)
                .with_session_control_transport(session_control)
                .build()
                .unwrap(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        let push_gateway = gateway.clone();
        let push_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { push_gateway.subscribe_push(push_shutdown).await });
        tasks.spawn(async move { gateway.subscribe_session_control(shutdown_rx).await });
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
    fn infrastructure_rejects_distributed_gateway_without_shared_replay_protection() {
        let result = builder()
            .with_world_client(Arc::new(ReadyWorld))
            .with_online_directory(
                Arc::new(MemoryOnlineDirectory::default()),
                GatewayOnlineConfig::new(
                    "gateway-a",
                    Duration::from_secs(30),
                    Duration::from_secs(10),
                    DuplicateLoginMode::AllowMultiple,
                ),
            )
            .build();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig(message)) if message.contains("ReplayProtectionStore")
        ));
    }

    #[test]
    fn infrastructure_rejects_cross_node_kick_without_control_transport() {
        let result = builder()
            .with_world_client(Arc::new(ReadyWorld))
            .with_replay_protection(Arc::new(MemoryReplayProtectionStore::default()))
            .with_online_directory(
                Arc::new(MemoryOnlineDirectory::default()),
                GatewayOnlineConfig::new(
                    "gateway-a",
                    Duration::from_secs(30),
                    Duration::from_secs(10),
                    DuplicateLoginMode::KickExisting,
                ),
            )
            .build();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig(message)) if message.contains("Session control")
        ));
    }
}
