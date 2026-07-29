use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use elura_core::account_version::{AccountVersionKey, AccountVersionStore};
use elura_core::ownership::OwnershipResolver;
use elura_core::protocol::{Frame, FrameKind, SessionControlAction};
use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTarget, PushTransport};
use elura_core::replay_protection::ReplayProtectionStore;
use elura_core::session::{
    SessionControlEvent, SessionControlHandler, SessionControlKind, SessionControlTransport,
};
use elura_core::ticket::TicketService;
use elura_core::{Error, Result};
use elura_runtime::observability::{AdminServerConfig, ReadinessProbe};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;
use uuid::Uuid;

use crate::builder::GatewayOnlineConfig;
use crate::config::GatewayConfig;
use crate::connection::{ConnectionContext, client_ip};
use crate::discovery::{WorldClient, WorldDiscovery, WorldRouteUpdater};
use crate::interceptor::GatewayInterceptor;
use crate::observability::{AdminServer, AdmissionAdmin, GatewayAdmin, Readiness};
use crate::presence::OnlineDirectory;
use crate::protection::{BackendProtector, ProtectionConfig, ProtectionStats};
use crate::session_state::{
    SessionIndex, SessionSenders, SharedSessionIndex, disconnect_handle,
    disconnect_handle_with_action,
};
use crate::stats::{GatewayStats, GatewayStatsSnapshot};
use crate::transport::{
    AccountVersionPolicy, AccountVersionSettings, AdmissionController, AdmissionPolicy,
    AdmissionSettings, ConnectionLimiter, DrainController, GatewayTransport, KeyedRateLimiter,
    RegisteredGatewayTransport, SessionConnection, SessionIoConfig, SessionObserver,
    SessionService, register, serve_stream,
};

#[derive(Clone)]
pub(crate) struct OnlineConfig {
    pub(crate) directory: Arc<dyn OnlineDirectory>,
    pub(crate) config: GatewayOnlineConfig,
}

struct NamedReadinessProbe {
    name: Arc<str>,
    probe: Arc<dyn ReadinessProbe>,
}

pub struct GatewayServer {
    config: GatewayConfig,
    tickets: Arc<TicketService>,
    replay: Arc<dyn ReplayProtectionStore>,
    world: Arc<dyn WorldClient>,
    sessions: SessionSenders,
    ownership: Option<(u32, Arc<dyn OwnershipResolver>)>,
    protector: Option<Arc<BackendProtector>>,
    connections: Arc<tokio::sync::Semaphore>,
    per_ip_connections: Arc<ConnectionLimiter>,
    ip_requests: Option<Arc<KeyedRateLimiter<IpAddr>>>,
    session_index: SharedSessionIndex,
    topics: Arc<RwLock<HashMap<String, HashSet<Uuid>>>>,
    online: Option<OnlineConfig>,
    push: Option<Arc<dyn PushTransport>>,
    session_control: Option<Arc<dyn SessionControlTransport>>,
    admission: Option<AdmissionPolicy>,
    observers: Vec<Arc<dyn SessionObserver>>,
    account_versions: Option<AccountVersionPolicy>,
    interceptors: Vec<Arc<dyn GatewayInterceptor>>,
    drain: Arc<DrainController>,
    stats: Arc<GatewayStats>,
    readiness_probes: Vec<NamedReadinessProbe>,
    admin: Option<AdminServerConfig>,
    admission_admin: Option<Arc<dyn AdmissionAdmin>>,
    discovery: Option<GatewayDiscovery>,
    pub(crate) transports: Vec<Arc<dyn RegisteredGatewayTransport>>,
    http: Vec<GatewayHttpServer>,
}

struct GatewayDiscovery {
    discovery: Arc<dyn WorldDiscovery>,
    updater: Arc<dyn WorldRouteUpdater>,
}

struct GatewayHttpServer {
    listen: SocketAddr,
    router: Router,
}

impl GatewayServer {
    pub fn new(
        config: GatewayConfig,
        tickets: Arc<TicketService>,
        replay: Arc<dyn ReplayProtectionStore>,
        world: Arc<dyn WorldClient>,
    ) -> Result<Self> {
        config.validate()?;
        let max_connections = config.max_connections;
        let max_connections_per_ip = config.max_connections_per_ip;
        let ip_requests = (config.ip_request_rate != 0).then(|| {
            Arc::new(KeyedRateLimiter::new(
                config.ip_request_rate,
                config.ip_request_burst,
            ))
        });
        Ok(Self {
            config,
            tickets,
            replay,
            world,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            ownership: None,
            protector: None,
            connections: Arc::new(tokio::sync::Semaphore::new(max_connections)),
            per_ip_connections: Arc::new(ConnectionLimiter::new(max_connections_per_ip)),
            ip_requests,
            session_index: Arc::new(RwLock::new(SessionIndex::default())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            online: None,
            push: None,
            session_control: None,
            admission: None,
            observers: Vec::new(),
            account_versions: None,
            interceptors: Vec::new(),
            drain: Arc::new(DrainController::default()),
            stats: Arc::new(GatewayStats::default()),
            readiness_probes: Vec::new(),
            admin: None,
            admission_admin: None,
            discovery: None,
            transports: Vec::new(),
            http: Vec::new(),
        })
    }

    pub(crate) fn with_process_config(
        mut self,
        admission_admin: Option<Arc<dyn AdmissionAdmin>>,
        discovery: Option<(Arc<dyn WorldDiscovery>, Arc<dyn WorldRouteUpdater>)>,
        transports: Vec<Arc<dyn RegisteredGatewayTransport>>,
    ) -> Self {
        self.admission_admin = admission_admin;
        self.discovery =
            discovery.map(|(discovery, updater)| GatewayDiscovery { discovery, updater });
        self.transports.extend(transports);
        self
    }

    /// Adds a client transport endpoint to an advanced, manually assembled server.
    pub fn with_transport<T>(mut self, transport: T) -> Result<Self>
    where
        T: GatewayTransport,
    {
        transport.validate()?;
        self.transports.push(register(transport));
        Ok(self)
    }

    pub(crate) fn add_http(&mut self, listen: String, router: Router) -> Result<()> {
        let listen = listen
            .parse()
            .map_err(|_| Error::InvalidConfig(format!("invalid HTTP listen address {listen}")))?;
        self.http.push(GatewayHttpServer { listen, router });
        Ok(())
    }

    pub(crate) fn validate_listeners(&self) -> Result<()> {
        let mut listeners = Vec::new();
        if let Some(admin) = self.admin.as_ref() {
            listeners.push((
                "admin",
                admin.listen,
                crate::transport::TransportSocketKind::Stream,
            ));
        }
        for transport in &self.transports {
            listeners.push((
                transport.name(),
                transport.listen(),
                transport.socket_kind(),
            ));
        }
        for http in &self.http {
            listeners.push((
                "http",
                http.listen,
                crate::transport::TransportSocketKind::Stream,
            ));
        }
        for (index, (left_name, left, left_kind)) in listeners.iter().enumerate() {
            for (right_name, right, right_kind) in listeners.iter().skip(index + 1) {
                if left_kind == right_kind
                    && left.port() == right.port()
                    && (left.ip().is_unspecified()
                        || right.ip().is_unspecified()
                        || left.ip() == right.ip())
                {
                    return Err(Error::InvalidConfig(format!(
                        "{left_name} and {right_name} listeners conflict at port {}",
                        left.port()
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn with_ownership(
        mut self,
        shard_count: u32,
        resolver: Arc<dyn OwnershipResolver>,
    ) -> Result<Self> {
        if shard_count == 0 {
            return Err(Error::InvalidConfig(
                "gateway shard count must be positive".into(),
            ));
        }
        self.ownership = Some((shard_count, resolver));
        Ok(self)
    }

    pub fn with_protection(mut self, config: ProtectionConfig) -> Result<Self> {
        self.protector = Some(Arc::new(BackendProtector::new(config)?));
        Ok(self)
    }

    pub fn with_online_directory(
        mut self,
        directory: Arc<dyn OnlineDirectory>,
        config: GatewayOnlineConfig,
    ) -> Result<Self> {
        config.validate()?;
        self.online = Some(OnlineConfig { directory, config });
        Ok(self)
    }

    pub fn with_push_transport(mut self, push: Arc<dyn PushTransport>) -> Self {
        self.push = Some(push);
        self
    }

    pub fn with_session_control_transport(
        mut self,
        transport: Arc<dyn SessionControlTransport>,
    ) -> Self {
        self.session_control = Some(transport);
        self
    }

    pub fn with_admission(
        mut self,
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Result<Self> {
        self.admission = Some(AdmissionPolicy::new(controller, settings)?);
        Ok(self)
    }

    pub fn with_session_observer(mut self, observer: Arc<dyn SessionObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    pub fn with_interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: GatewayInterceptor,
    {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    pub fn with_account_version_store(
        mut self,
        store: Arc<dyn AccountVersionStore>,
        settings: AccountVersionSettings,
    ) -> Result<Self> {
        self.account_versions = Some(AccountVersionPolicy::new(store, settings)?);
        Ok(self)
    }

    /// Adds a required dependency to `/readyz` evaluation.
    pub fn with_readiness_probe(
        mut self,
        name: impl Into<Arc<str>>,
        probe: Arc<dyn ReadinessProbe>,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "readiness probe name cannot be empty".into(),
            ));
        }
        self.readiness_probes
            .push(NamedReadinessProbe { name, probe });
        Ok(self)
    }

    pub async fn subscribe_push(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<()> {
        let push = self.push.clone().ok_or(Error::Unavailable)?;
        push.subscribe(self, shutdown).await
    }

    pub async fn subscribe_session_control(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let transport = self.session_control.clone().ok_or(Error::Unavailable)?;
        transport.subscribe(self, shutdown).await
    }

    pub async fn publish_push(&self, request: &PushRequest) -> Result<PushReceipt> {
        match &self.push {
            Some(push) => push.publish(request).await,
            None => self.deliver(request.clone()).await,
        }
    }

    /// Disconnects local Sessions whose ticket generation is older than the
    /// authoritative account generation. Cross-node propagation can call this
    /// same method on each Gateway; periodic store checks remain the fallback.
    pub async fn revoke_account_version(
        &self,
        region_id: u32,
        realm_id: u32,
        user_id: i64,
        minimum_generation: u64,
        reason: &str,
    ) -> Result<usize> {
        AccountVersionKey::new(region_id, realm_id, user_id)?;
        if minimum_generation == 0 || reason.trim().is_empty() || reason.len() > 256 {
            return Err(Error::InvalidConfig(
                "account revocation requires a generation and reason".into(),
            ));
        }
        let event = SessionControlEvent {
            kind: SessionControlKind::VersionChanged,
            region_id,
            realm_id,
            user_id,
            generation: minimum_generation,
            session_id: None,
            keep_session_id: None,
            reason: reason.to_owned(),
        };
        event.validate()?;
        let delivered = self.apply_session_control_local(&event).await?;
        if let Some(transport) = &self.session_control {
            transport.publish(&event).await?;
        }
        Ok(delivered)
    }

    pub async fn force_logout(
        &self,
        region_id: u32,
        realm_id: u32,
        user_id: i64,
        reason: &str,
    ) -> Result<usize> {
        let event = SessionControlEvent {
            kind: SessionControlKind::ForceLogout,
            region_id,
            realm_id,
            user_id,
            generation: 0,
            session_id: None,
            keep_session_id: None,
            reason: reason.to_owned(),
        };
        event.validate()?;
        let delivered = self.apply_session_control_local(&event).await?;
        if let Some(transport) = &self.session_control {
            transport.publish(&event).await?;
        }
        Ok(delivered)
    }

    async fn apply_session_control_local(&self, event: &SessionControlEvent) -> Result<usize> {
        event.validate()?;
        let action = match event.kind {
            SessionControlKind::Login => SessionControlAction::DuplicateLogin,
            SessionControlKind::ForceLogout => SessionControlAction::ForceLogout,
            SessionControlKind::VersionChanged => SessionControlAction::AccountVersionChanged,
            _ => return Ok(0),
        };
        let index = self
            .session_index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let candidates = index
            .users
            .get(&(event.region_id, event.realm_id, event.user_id))
            .cloned()
            .unwrap_or_default();
        let session_ids = candidates
            .into_iter()
            .filter_map(|session_id| {
                let identity = index.identities.get(&session_id)?;
                let selected = match event.kind {
                    SessionControlKind::Login => event.keep_session_id != Some(session_id),
                    SessionControlKind::ForceLogout => {
                        event.session_id.is_none_or(|target| target == session_id)
                    }
                    SessionControlKind::VersionChanged => identity.generation < event.generation,
                    _ => false,
                };
                selected.then_some(session_id)
            })
            .collect::<Vec<_>>();
        drop(index);
        let handles = {
            let sessions = self
                .sessions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            session_ids
                .into_iter()
                .filter_map(|session_id| sessions.get(&session_id).cloned())
                .collect::<Vec<_>>()
        };
        let mut delivered = 0;
        for handle in handles {
            disconnect_handle_with_action(handle, action, &event.reason)?;
            delivered += 1;
        }
        Ok(delivered)
    }

    pub async fn protection_stats(&self) -> Option<ProtectionStats> {
        match &self.protector {
            Some(protector) => Some(protector.stats().await),
            None => None,
        }
    }

    pub fn begin_drain(&self) {
        self.drain.begin();
    }

    pub fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    pub fn active_session_count(&self) -> usize {
        self.drain.active()
    }

    pub fn stats(&self) -> GatewayStatsSnapshot {
        self.stats.snapshot()
    }

    pub async fn readiness(&self) -> Readiness {
        if self.is_draining() {
            return Readiness::unavailable("Gateway is draining");
        }
        if !matches!(
            timeout(self.config.readiness_timeout, self.world.readiness()).await,
            Ok(Ok(()))
        ) {
            return Readiness::unavailable("World dependency is unavailable");
        }
        for dependency in &self.readiness_probes {
            if !matches!(
                timeout(self.config.readiness_timeout, dependency.probe.check()).await,
                Ok(Ok(()))
            ) {
                return Readiness::unavailable(format!(
                    "{} dependency is unavailable",
                    dependency.name
                ));
            }
        }
        Readiness::ready()
    }

    /// Stops accepting protocol Sessions and waits for existing Sessions to
    /// finish. At the deadline, remaining Sessions receive a framework
    /// control event and are force-cancelled.
    pub async fn drain(&self) -> Result<()> {
        self.begin_drain();
        let anonymous = self
            .sessions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|handle| !handle.authenticated.load(Ordering::Acquire))
            .cloned()
            .collect::<Vec<_>>();
        for handle in anonymous {
            let _ = disconnect_handle_with_action(
                handle,
                SessionControlAction::ServerDraining,
                "Gateway is draining before authentication",
            );
        }
        if tokio::time::timeout(self.config.shutdown_timeout, self.drain.wait_empty())
            .await
            .is_ok()
        {
            return Ok(());
        }
        let handles = self
            .sessions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = disconnect_handle_with_action(
                handle,
                SessionControlAction::ServerDraining,
                "Gateway shutdown deadline exceeded",
            );
        }
        let _ = tokio::time::timeout(Duration::from_millis(250), self.drain.wait_empty()).await;
        Err(Error::Timeout)
    }

    /// Runs until Ctrl-C or until one of the supervised services exits.
    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let signal = tokio::spawn(async move {
            let _ = elura_runtime::lifecycle::shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        });
        let result = self.serve(admin, shutdown_rx).await;
        signal.abort();
        result
    }

    pub async fn serve(
        mut self,
        admin: AdminServerConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        self.admin = Some(admin);
        Arc::new(self).serve_embedded(shutdown).await
    }

    /// Serves an embedded Gateway without starting a separate administration endpoint.
    pub async fn serve_embedded(
        self: Arc<Self>,
        mut external_shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if self.transports.is_empty() {
            return Err(Error::InvalidConfig(
                "Gateway requires at least one transport".into(),
            ));
        }
        self.validate_listeners()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let forward_shutdown = shutdown_tx.clone();
        let forward = tokio::spawn(async move {
            if *external_shutdown.borrow() {
                let _ = forward_shutdown.send(true);
                return;
            }
            while external_shutdown.changed().await.is_ok() {
                if *external_shutdown.borrow() {
                    let _ = forward_shutdown.send(true);
                    return;
                }
            }
            let _ = forward_shutdown.send(true);
        });

        let mut tasks = JoinSet::new();
        for transport in &self.transports {
            let transport = transport.clone();
            let gateway = self.clone();
            let transport_shutdown = shutdown_rx.clone();
            tasks.spawn(async move { transport.serve(gateway, transport_shutdown).await });
        }
        if let Some(config) = self.admin.clone() {
            let mut gateway_admin = GatewayAdmin::new(self.clone());
            if let Some(admission_admin) = self.admission_admin.clone() {
                gateway_admin = gateway_admin.with_admission(admission_admin);
            }
            let admin = AdminServer::new(config, self.clone())?.with_gateway_admin(gateway_admin);
            let admin_shutdown = shutdown_rx.clone();
            tasks.spawn(async move { admin.serve(admin_shutdown).await });
        }
        if let Some(discovery) = self.discovery.as_ref() {
            let discovery = discovery.discovery.clone();
            let updater = self
                .discovery
                .as_ref()
                .expect("discovery was checked")
                .updater
                .clone();
            let discovery_shutdown = shutdown_rx.clone();
            tasks.spawn(async move { discovery.run(updater, discovery_shutdown).await });
        }
        if self.push.is_some() {
            let subscriber = self.clone();
            let push_shutdown = shutdown_rx.clone();
            tasks.spawn(async move { subscriber.subscribe_push(push_shutdown).await });
        }
        if self.session_control.is_some() {
            let subscriber = self.clone();
            let control_shutdown = shutdown_rx.clone();
            tasks
                .spawn(async move { subscriber.subscribe_session_control(control_shutdown).await });
        }
        for http in &self.http {
            let listen = http.listen;
            let router = http.router.clone();
            let mut http_shutdown = shutdown_rx.clone();
            tasks.spawn(async move {
                let listener = TcpListener::bind(listen).await?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        while !*http_shutdown.borrow() {
                            if http_shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                    .map_err(Error::from)
            });
        }

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
        forward.abort();
        let drain = self.drain().await;
        match first_error {
            Some(error) => Err(error),
            None => drain,
        }
    }

    pub(crate) async fn serve_transport_stream<S>(
        self: Arc<Self>,
        peer: SocketAddr,
        stream: S,
    ) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let io = SessionIoConfig {
            max_payload: self.config.max_payload,
            inbound_capacity: self.config.inbound_queue,
            response_capacity: self.config.response_queue,
            push_capacity: self.config.push_queue,
            write_timeout: self.config.write_timeout,
        };
        serve_stream(stream, peer, io, self).await
    }

    pub async fn push_session(&self, session_id: Uuid, route: u32, payload: Bytes) -> Result<()> {
        let handle = self
            .sessions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&session_id)
            .cloned()
            .ok_or(Error::Unavailable)?;
        handle
            .pushes
            .try_send(Frame {
                kind: FrameKind::Push,
                flags: 0,
                route,
                request_id: 0,
                sequence: 0,
                payload,
            })
            .map_err(|_| Error::QueueFull)
    }
}

#[async_trait]
impl SessionService for GatewayServer {
    async fn serve_session(&self, connection: SessionConnection) -> Result<()> {
        let _active_session = self.drain.enter()?;
        let _permit = match self.connections.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.stats.record_rejection();
                return Err(Error::Unavailable);
            }
        };
        let _ip_permit = match self
            .per_ip_connections
            .try_enter(client_ip(connection.peer))
        {
            Ok(permit) => permit,
            Err(error) => {
                self.stats.record_rejection();
                return Err(error);
            }
        };
        ConnectionContext {
            config: self.config.clone(),
            tickets: self.tickets.clone(),
            replay: self.replay.clone(),
            world: self.world.clone(),
            sessions: self.sessions.clone(),
            ownership: self.ownership.clone(),
            protector: self.protector.clone(),
            ip_requests: self.ip_requests.clone(),
            session_index: self.session_index.clone(),
            topics: self.topics.clone(),
            online: self.online.clone(),
            push: self.push.clone(),
            session_control: self.session_control.clone(),
            admission: self.admission.clone(),
            observers: self.observers.clone(),
            account_versions: self.account_versions.clone(),
            interceptors: self.interceptors.clone(),
            stats: self.stats.clone(),
        }
        .serve(connection)
        .await
    }
}

#[async_trait]
impl PushHandler for GatewayServer {
    async fn deliver(&self, request: PushRequest) -> Result<PushReceipt> {
        self.stats.record_push();
        let result = self.deliver_inner(request).await;
        if result.is_err() {
            self.stats.record_push_failure();
        }
        result
    }
}

impl GatewayServer {
    async fn deliver_inner(&self, request: PushRequest) -> Result<PushReceipt> {
        request.validate()?;
        let receipt = request.clone();
        match request.target {
            PushTarget::JoinTopic { session_id, topic } => {
                self.update_membership(
                    request.region_id,
                    request.realm_id,
                    session_id,
                    topic,
                    true,
                )
                .await?;
                Ok(PushReceipt::accepted(&receipt, 0))
            }
            PushTarget::LeaveTopic { session_id, topic } => {
                self.update_membership(
                    request.region_id,
                    request.realm_id,
                    session_id,
                    topic,
                    false,
                )
                .await?;
                Ok(PushReceipt::accepted(&receipt, 0))
            }
            PushTarget::Disconnect(session_id) => {
                let identity = self
                    .session_index
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .identities
                    .get(&session_id)
                    .cloned();
                if !identity.is_some_and(|identity| {
                    identity.region_id == request.region_id && identity.realm_id == request.realm_id
                }) {
                    return Err(Error::Unavailable);
                }
                let handle = self
                    .sessions
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&session_id)
                    .cloned()
                    .ok_or(Error::Unavailable)?;
                disconnect_handle(handle, String::from_utf8_lossy(&request.payload).as_ref())?;
                Ok(PushReceipt::accepted(&receipt, 1))
            }
            target => {
                let ids = self
                    .matching_sessions(request.region_id, request.realm_id, &target)
                    .await;
                if ids.is_empty() {
                    return Err(Error::Unavailable);
                }
                let handles = {
                    let sessions = self
                        .sessions
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    ids.into_iter()
                        .filter_map(|id| sessions.get(&id).cloned())
                        .collect::<Vec<_>>()
                };
                let mut delivered = 0;
                for handle in handles {
                    handle
                        .pushes
                        .try_send(Frame {
                            kind: FrameKind::Push,
                            flags: 0,
                            route: request.route,
                            request_id: 0,
                            sequence: request.sequence,
                            payload: request.payload.clone(),
                        })
                        .map_err(|_| Error::QueueFull)?;
                    delivered += 1;
                }
                if delivered == 0 {
                    return Err(Error::Unavailable);
                }
                Ok(PushReceipt::accepted(&receipt, delivered))
            }
        }
    }
}

#[async_trait]
impl SessionControlHandler for GatewayServer {
    async fn handle(&self, event: SessionControlEvent) -> Result<()> {
        self.apply_session_control_local(&event).await?;
        Ok(())
    }
}

impl GatewayServer {
    async fn matching_sessions(
        &self,
        region_id: u32,
        realm_id: u32,
        target: &PushTarget,
    ) -> Vec<Uuid> {
        let topic_candidates = match target {
            PushTarget::Topic(topic) => Some(
                self.topics
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(topic)
                    .cloned()
                    .unwrap_or_default(),
            ),
            _ => None,
        };
        let index = self
            .session_index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match target {
            PushTarget::Session(session_id) => index
                .identities
                .get(session_id)
                .filter(|identity| identity.region_id == region_id && identity.realm_id == realm_id)
                .map(|_| vec![*session_id])
                .unwrap_or_default(),
            PushTarget::User(user_id) => index
                .users
                .get(&(region_id, realm_id, *user_id))
                .map(|sessions| sessions.iter().copied().collect())
                .unwrap_or_default(),
            PushTarget::Users(user_ids) => {
                let mut sessions = HashSet::new();
                for user_id in user_ids {
                    if let Some(matches) = index.users.get(&(region_id, realm_id, *user_id)) {
                        sessions.extend(matches);
                    }
                }
                sessions.into_iter().collect()
            }
            PushTarget::Topic(_) => topic_candidates
                .unwrap_or_default()
                .into_iter()
                .filter(|session_id| {
                    index.identities.get(session_id).is_some_and(|identity| {
                        identity.region_id == region_id && identity.realm_id == realm_id
                    })
                })
                .collect(),
            PushTarget::Realm => index
                .identities
                .iter()
                .filter_map(|(session_id, identity)| {
                    (identity.region_id == region_id && identity.realm_id == realm_id)
                        .then_some(*session_id)
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    async fn update_membership(
        &self,
        region_id: u32,
        realm_id: u32,
        session_id: Uuid,
        group: String,
        join: bool,
    ) -> Result<()> {
        if group.is_empty()
            || !self
                .session_index
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .identities
                .get(&session_id)
                .is_some_and(|identity| {
                    identity.region_id == region_id && identity.realm_id == realm_id
                })
        {
            return Err(Error::Unavailable);
        }
        {
            let mut groups = self
                .topics
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if join {
                groups.entry(group.clone()).or_default().insert(session_id);
            } else if let Some(sessions) = groups.get_mut(&group) {
                sessions.remove(&session_id);
                if sessions.is_empty() {
                    groups.remove(&group);
                }
            }
        }
        if let Some(online) = &self.online {
            online
                .directory
                .track_group(session_id, &format!("topic:{group}"), join)
                .await?;
        }
        Ok(())
    }
}
