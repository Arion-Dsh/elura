//! Client-facing Gateway runtime.

#![deny(rustdoc::broken_intra_doc_links)]

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use elura_core::account_version::{AccountVersionKey, AccountVersionStore};
use elura_core::gateway_world::{GatewayWorldCommand, WorldCommand};
pub use elura_core::gateway_world::{
    GatewayWorldRoutingConfig, WorldClient, WorldDiscovery, WorldRequest, WorldRouteTarget,
    WorldRouteUpdater,
};
use elura_core::online::{DuplicateLoginMode, OnlineDirectory, SessionLease};
use elura_core::ownership::{OwnershipResolver, shard_for};
use elura_core::protocol::{
    FIRST_APPLICATION_ROUTE, Frame, FrameCodec, FrameKind, HEADER_LEN, ROUTE_AUTHENTICATE,
    ROUTE_HEARTBEAT, ROUTE_RECONNECT, ROUTE_SESSION_CONTROL, SessionControl, SessionControlAction,
};
use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTarget, PushTransport};
use elura_core::rate_limit::TokenBucket;
use elura_core::session::{
    Identity, Session, SessionControlEvent, SessionControlHandler, SessionControlKind,
    SessionControlTransport,
};
use elura_core::ticket::{ReplayStore, TicketService};
use elura_core::{Error, ErrorEnvelope, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug};
use uuid::Uuid;

use crate::protection::{BackendProtector, ProtectionConfig, ProtectionStats};
use crate::transport::{
    AccountVersionPolicy, AccountVersionSettings, AdmissionController, AdmissionPolicy,
    AdmissionRequest, AdmissionSettings, AdmissionStage, ConnectionLimiter, DrainController,
    GatewayTransport, KeyedRateLimiter, RegisteredGatewayTransport, ResponseCache,
    SessionConnection, SessionEventKind, SessionIoConfig, SessionObserver, SessionService,
    notify_session_observers, register, serve_stream,
};
use elura_runtime::observability::{AdminServerConfig, ReadinessProbe};
use elura_runtime::security::{BoxedServiceStream, ClientTlsConfig, InternalToken};
use observability::{AdminServer, AdmissionAdmin, GatewayAdmin, Readiness};
mod builder;
mod gateway;
mod interceptor;
pub mod observability;
pub mod protection;
mod routing;
pub mod transport;

pub use gateway::Gateway;
pub use interceptor::{
    GatewayInterceptContext, GatewayInterceptor, GatewayNext, GatewayRequest, GatewayResponse,
};

pub use builder::{
    GatewayInfrastructure, GatewayRealmAdmissionConfig, GatewayTicketConfig, GatewayWorldTlsConfig,
};
pub(crate) use routing::{MemoryWorldRouteDirectory, RouteWorldClient};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRateLimit {
    pub requests_per_second: u32,
    pub burst: u32,
}

/// Transport-neutral Gateway Session and process configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GatewayConfig {
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub max_payload: usize,
    pub request_rate: u32,
    pub request_burst: u32,
    pub inbound_byte_rate: u32,
    pub inbound_byte_burst: u32,
    pub ip_request_rate: u32,
    pub ip_request_burst: u32,
    pub max_rate_limit_violations: u32,
    pub max_protocol_violations: u32,
    pub route_rate_limits: HashMap<u32, RouteRateLimit>,
    pub inbound_queue: usize,
    pub response_queue: usize,
    pub push_queue: usize,
    pub idle_timeout: Duration,
    pub authentication_timeout: Duration,
    pub handler_timeout: Duration,
    pub write_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub response_cache_ttl: Duration,
    pub response_cache_capacity: usize,
    /// Maximum response payload bytes retained by one Session's replay cache.
    pub response_cache_max_bytes: usize,
    pub shutdown_timeout: Duration,
    pub readiness_timeout: Duration,
    pub ticket: GatewayTicketConfig,
    #[serde(skip)]
    pub internal_token: Option<String>,
    pub protection: Option<ProtectionConfig>,
    pub world_tls: Option<GatewayWorldTlsConfig>,
    pub world_routing: GatewayWorldRoutingConfig,
    pub realm_admission: Option<GatewayRealmAdmissionConfig>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_connections_per_ip: 100,
            max_payload: 1 << 20,
            request_rate: 200,
            request_burst: 400,
            inbound_byte_rate: 8 << 20,
            inbound_byte_burst: 2 << 20,
            ip_request_rate: 2_000,
            ip_request_burst: 4_000,
            max_rate_limit_violations: 20,
            max_protocol_violations: 3,
            route_rate_limits: HashMap::from([(
                ROUTE_AUTHENTICATE,
                RouteRateLimit {
                    requests_per_second: 5,
                    burst: 5,
                },
            )]),
            inbound_queue: 64,
            response_queue: 64,
            push_queue: 64,
            idle_timeout: Duration::from_secs(90),
            authentication_timeout: Duration::from_secs(10),
            handler_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
            response_cache_ttl: Duration::from_secs(10),
            response_cache_capacity: 128,
            response_cache_max_bytes: 1 << 20,
            shutdown_timeout: Duration::from_secs(10),
            readiness_timeout: Duration::from_secs(2),
            ticket: GatewayTicketConfig::default(),
            internal_token: None,
            protection: None,
            world_tls: None,
            world_routing: GatewayWorldRoutingConfig::default(),
            realm_admission: None,
        }
    }
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_connections == 0
            || self.max_connections_per_ip == 0
            || self.max_payload == 0
            || self.max_rate_limit_violations == 0
            || self.max_protocol_violations == 0
            || self.inbound_queue == 0
            || self.response_queue == 0
            || self.push_queue == 0
            || self.request_burst == 0
            || self.inbound_byte_rate == 0
            || (self.inbound_byte_burst as usize) < self.max_payload.saturating_add(HEADER_LEN)
            || (self.ip_request_rate == 0) != (self.ip_request_burst == 0)
            || self.idle_timeout.is_zero()
            || self.authentication_timeout.is_zero()
            || self.handler_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.response_cache_ttl.is_zero()
            || self.response_cache_capacity == 0
            || self.response_cache_max_bytes == 0
            || self.shutdown_timeout.is_zero()
            || self.readiness_timeout.is_zero()
        {
            return Err(Error::InvalidConfig(
                "gateway limits must be positive".into(),
            ));
        }
        if self
            .route_rate_limits
            .iter()
            .any(|(route, limit)| *route == 0 || limit.requests_per_second == 0 || limit.burst == 0)
        {
            return Err(Error::InvalidConfig("invalid route rate limit".into()));
        }
        if self.ticket.ttl.is_zero() {
            return Err(Error::InvalidConfig(
                "gateway ticket TTL must be positive".into(),
            ));
        }
        self.world_routing.validate()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthenticateRequest {
    ticket: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AuthenticateResponse {
    session_id: String,
    identity: Identity,
}

pub struct TcpWorldClient {
    address: SocketAddr,
    max_payload: usize,
    connect_timeout: Duration,
    max_in_flight_per_connection: usize,
    pool: Vec<Mutex<Option<mpsc::Sender<PendingWorldRequest>>>>,
    next: AtomicUsize,
    transport_request_id: AtomicU64,
    authorization: Option<InternalToken>,
    tls: Option<ClientTlsConfig>,
}

const WORLD_CONNECTION_IN_FLIGHT: usize = 64;

struct PendingWorldRequest {
    frame: Frame,
    response: oneshot::Sender<Result<Bytes>>,
    deadline: tokio::time::Instant,
}

struct PendingWorldResponse {
    response: Option<oneshot::Sender<Result<Bytes>>>,
    deadline: tokio::time::Instant,
}

#[derive(Clone)]
struct WorldConnectionConfig {
    address: SocketAddr,
    max_payload: usize,
    connect_timeout: Duration,
    max_in_flight: usize,
    tls: Option<ClientTlsConfig>,
}

impl TcpWorldClient {
    pub fn new(address: SocketAddr, max_payload: usize) -> Self {
        Self::with_pool_size(address, max_payload, 16).expect("non-zero static pool size")
    }

    pub fn with_pool_size(
        address: SocketAddr,
        max_payload: usize,
        pool_size: usize,
    ) -> Result<Self> {
        if pool_size == 0 || pool_size > 1024 {
            return Err(Error::InvalidConfig(
                "world connection pool must be in 1..=1024".into(),
            ));
        }
        Ok(Self {
            address,
            max_payload,
            connect_timeout: Duration::from_secs(2),
            max_in_flight_per_connection: WORLD_CONNECTION_IN_FLIGHT,
            pool: (0..pool_size).map(|_| Mutex::new(None)).collect(),
            next: AtomicUsize::new(0),
            transport_request_id: AtomicU64::new(1),
            authorization: None,
            tls: None,
        })
    }

    pub fn with_internal_token(mut self, token: InternalToken) -> Self {
        self.authorization = Some(token);
        self
    }

    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_max_in_flight_per_connection(mut self, limit: usize) -> Result<Self> {
        validate_world_connection_in_flight(limit)?;
        self.max_in_flight_per_connection = limit;
        Ok(self)
    }

    fn connection_sender(&self, slot: usize) -> mpsc::Sender<PendingWorldRequest> {
        let mut state = self.pool[slot]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sender) = state.as_ref().filter(|sender| !sender.is_closed()) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::channel(self.max_in_flight_per_connection);
        tokio::spawn(world_connection_worker(
            WorldConnectionConfig {
                address: self.address,
                max_payload: self.max_payload,
                connect_timeout: self.connect_timeout,
                max_in_flight: self.max_in_flight_per_connection,
                tls: self.tls.clone(),
            },
            receiver,
        ));
        *state = Some(sender.clone());
        sender
    }

    fn invalidate_connection_sender(
        &self,
        slot: usize,
        sender: &mpsc::Sender<PendingWorldRequest>,
    ) {
        let mut state = self.pool[slot]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .as_ref()
            .is_some_and(|current| current.same_channel(sender))
        {
            *state = None;
        }
    }

    fn next_transport_request_id(&self) -> u64 {
        loop {
            let current = self.transport_request_id.load(Ordering::Relaxed);
            let next = if current == u64::MAX { 1 } else { current + 1 };
            if self
                .transport_request_id
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return current;
            }
        }
    }
}

#[async_trait]
impl WorldClient for TcpWorldClient {
    async fn command(&self, request: WorldRequest) -> Result<Bytes> {
        let slot = self.next.fetch_add(1, Ordering::Relaxed) % self.pool.len();
        let transport_request_id = self.next_transport_request_id();
        let command = WorldCommand {
            authorization: self
                .authorization
                .as_ref()
                .map(|token| token.expose().to_owned()),
            identity: request.identity,
            session_id: request.session_id.to_string(),
            trace_id: request.trace_id,
            request_id: request.request_id,
            payload: request.payload,
            shard_id: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.shard_id),
            owner_id: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.world_id.clone()),
            owner_epoch: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.epoch),
            timeout: request.timeout,
        };
        let protobuf = GatewayWorldCommand::from(command).encode_frame_payload();
        let frame = Frame::request(request.route, transport_request_id, protobuf)?;
        let sender = self.connection_sender(slot);
        let (response, receiver) = oneshot::channel();
        let deadline = tokio::time::Instant::now()
            .checked_add(request.timeout)
            .ok_or_else(|| Error::InvalidConfig("World request timeout is too large".into()))?;
        let enqueue = sender.send(PendingWorldRequest {
            frame,
            response,
            deadline,
        });
        match tokio::time::timeout_at(deadline, enqueue).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Error::Unavailable),
            Err(_) => return Err(Error::Timeout),
        }
        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Unavailable),
            Err(_) => {
                self.invalidate_connection_sender(slot, &sender);
                Err(Error::Timeout)
            }
        }
    }

    async fn readiness(&self) -> Result<()> {
        let config = WorldConnectionConfig {
            address: self.address,
            max_payload: self.max_payload,
            connect_timeout: self.connect_timeout,
            max_in_flight: self.max_in_flight_per_connection,
            tls: self.tls.clone(),
        };
        let _connection = connect_world(&config).await?;
        Ok(())
    }
}

async fn world_connection_worker(
    config: WorldConnectionConfig,
    mut receiver: mpsc::Receiver<PendingWorldRequest>,
) {
    while let Some(first) = receiver.recv().await {
        let connect_deadline = first
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if connect_deadline.is_zero() {
            let _ = first.response.send(Err(Error::Timeout));
            continue;
        }
        let framed = match timeout(connect_deadline, connect_world(&config)).await {
            Ok(Ok(framed)) => framed,
            Ok(Err(error)) => {
                let _ = first.response.send(Err(error));
                continue;
            }
            Err(_) => {
                let _ = first.response.send(Err(Error::Timeout));
                continue;
            }
        };
        let (mut sink, mut source) = framed.split();
        let mut pending = HashMap::with_capacity(config.max_in_flight);
        let mut input_open = true;
        let mut next = Some(first);
        loop {
            if let Some(request) = next.take() {
                let remaining = request
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    let _ = request.response.send(Err(Error::Timeout));
                    continue;
                }
                let request_id = request.frame.request_id;
                match timeout(
                    remaining.min(config.connect_timeout),
                    sink.send(request.frame),
                )
                .await
                {
                    Ok(Ok(())) => {
                        pending.insert(
                            request_id,
                            PendingWorldResponse {
                                response: Some(request.response),
                                deadline: request.deadline,
                            },
                        );
                    }
                    Ok(Err(_)) | Err(_) => {
                        let _ = request.response.send(Err(Error::Unavailable));
                        fail_world_requests(&mut pending);
                        break;
                    }
                }
            }
            if !input_open && !has_active_world_requests(&pending) {
                return;
            }
            let next_deadline = pending
                .values()
                .filter(|request| request.response.is_some())
                .map(|request| request.deadline)
                .min();
            tokio::select! {
                request = receiver.recv(), if input_open && pending.len() < config.max_in_flight => {
                    match request {
                        Some(request) => next = Some(request),
                        None => input_open = false,
                    }
                }
                response = source.next() => {
                    let response = match response {
                        Some(Ok(response)) => response,
                        Some(Err(_)) | None => {
                            fail_world_requests(&mut pending);
                            break;
                        }
                    };
                    let Some(completion) = pending.remove(&response.request_id) else {
                        fail_world_requests(&mut pending);
                        break;
                    };
                    if let Some(completion) = completion.response {
                        let result = match response.kind {
                            FrameKind::Response => Ok(response.payload),
                            FrameKind::Error => match ErrorEnvelope::from_slice(&response.payload) {
                                Ok(envelope) => Err(envelope.into_error()),
                                Err(error) => Err(error),
                            },
                            _ => Err(Error::InvalidFrame("unexpected World response".into())),
                        };
                        let _ = completion.send(result);
                    }
                    if !pending.is_empty() && !has_active_world_requests(&pending) {
                        break;
                    }
                }
                _ = async {
                    match next_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if next_deadline.is_some() => {
                    let now = tokio::time::Instant::now();
                    for request in pending.values_mut() {
                        if request.deadline <= now
                            && let Some(response) = request.response.take()
                        {
                            let _ = response.send(Err(Error::Timeout));
                        }
                    }
                    if !pending.is_empty() && !has_active_world_requests(&pending) {
                        break;
                    }
                }
            }
        }
    }
}

fn validate_world_connection_in_flight(limit: usize) -> Result<()> {
    if !(1..=4096).contains(&limit) {
        return Err(Error::InvalidConfig(
            "World connection in-flight limit must be in 1..=4096".into(),
        ));
    }
    Ok(())
}

async fn connect_world(
    config: &WorldConnectionConfig,
) -> Result<Framed<BoxedServiceStream, FrameCodec>> {
    let stream = timeout(config.connect_timeout, TcpStream::connect(config.address))
        .await
        .map_err(|_| Error::Timeout)??;
    stream.set_nodelay(true)?;
    let stream: BoxedServiceStream = match &config.tls {
        Some(tls) => timeout(config.connect_timeout, tls.connect(stream))
            .await
            .map_err(|_| Error::Timeout)??,
        None => Box::new(stream),
    };
    Ok(Framed::new(stream, FrameCodec::new(config.max_payload)?))
}

fn fail_world_requests(pending: &mut HashMap<u64, PendingWorldResponse>) {
    for (_, completion) in pending.drain() {
        if let Some(response) = completion.response {
            let _ = response.send(Err(Error::Unavailable));
        }
    }
}

fn has_active_world_requests(pending: &HashMap<u64, PendingWorldResponse>) -> bool {
    pending.values().any(|request| request.response.is_some())
}

#[derive(Clone)]
struct SessionHandle {
    pushes: mpsc::Sender<Frame>,
    disconnect: watch::Sender<bool>,
    authenticated: Arc<AtomicBool>,
}

type SessionSenders = Arc<RwLock<HashMap<Uuid, SessionHandle>>>;
type UserKey = (u32, u32, i64);

#[derive(Default)]
struct SessionIndex {
    identities: HashMap<Uuid, Identity>,
    users: HashMap<UserKey, HashSet<Uuid>>,
}

impl SessionIndex {
    fn insert(&mut self, session_id: Uuid, identity: Identity) {
        if let Some(previous) = self.identities.insert(session_id, identity.clone()) {
            self.remove_user(session_id, &previous);
        }
        self.users
            .entry(identity_key(&identity))
            .or_default()
            .insert(session_id);
    }

    fn remove(&mut self, session_id: Uuid) -> Option<Identity> {
        let identity = self.identities.remove(&session_id)?;
        self.remove_user(session_id, &identity);
        Some(identity)
    }

    fn remove_user(&mut self, session_id: Uuid, identity: &Identity) {
        let key = identity_key(identity);
        if let Some(sessions) = self.users.get_mut(&key) {
            sessions.remove(&session_id);
            if sessions.is_empty() {
                self.users.remove(&key);
            }
        }
    }
}

type SharedSessionIndex = Arc<RwLock<SessionIndex>>;

struct GatewayStats {
    started_at: SystemTime,
    started: Instant,
    connections: AtomicU64,
    active_connections: AtomicI64,
    authenticated_sessions: AtomicI64,
    requests: AtomicU64,
    rejected: AtomicU64,
    failures: AtomicU64,
    pushes: AtomicU64,
    push_failures: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayStatsSnapshot {
    pub started_at: SystemTime,
    pub uptime_millis: u64,
    pub connections: u64,
    pub active_connections: i64,
    pub authenticated_sessions: i64,
    pub requests: u64,
    pub rejected: u64,
    pub failures: u64,
    pub pushes: u64,
    pub push_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectTicketResponse {
    pub ticket: String,
}

impl Default for GatewayStats {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now(),
            started: Instant::now(),
            connections: AtomicU64::new(0),
            active_connections: AtomicI64::new(0),
            authenticated_sessions: AtomicI64::new(0),
            requests: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            pushes: AtomicU64::new(0),
            push_failures: AtomicU64::new(0),
        }
    }
}

impl GatewayStats {
    fn connection_started(&self) -> ActiveGatewayConnection<'_> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        ActiveGatewayConnection(self)
    }

    fn record_error(&self, error: &Error) {
        match error {
            Error::Authentication | Error::RateLimited | Error::Business { .. } => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> GatewayStatsSnapshot {
        GatewayStatsSnapshot {
            started_at: self.started_at,
            uptime_millis: self.started.elapsed().as_millis() as u64,
            connections: self.connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            authenticated_sessions: self.authenticated_sessions.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            pushes: self.pushes.load(Ordering::Relaxed),
            push_failures: self.push_failures.load(Ordering::Relaxed),
        }
    }
}

struct ActiveGatewayConnection<'a>(&'a GatewayStats);

impl Drop for ActiveGatewayConnection<'_> {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct OnlineConfig {
    gateway_id: Arc<str>,
    directory: Arc<dyn OnlineDirectory>,
    lease_ttl: Duration,
    renew_interval: Duration,
    duplicate_login: DuplicateLoginMode,
}

struct NamedReadinessProbe {
    name: Arc<str>,
    probe: Arc<dyn ReadinessProbe>,
}

pub struct GatewayServer {
    config: GatewayConfig,
    tickets: Arc<TicketService>,
    replay: Arc<dyn ReplayStore>,
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
    transports: Vec<Arc<dyn RegisteredGatewayTransport>>,
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
        replay: Arc<dyn ReplayStore>,
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
            listeners.push(("admin", admin.listen));
        }
        for transport in &self.transports {
            listeners.push((transport.name(), transport.listen()));
        }
        for http in &self.http {
            listeners.push(("http", http.listen));
        }
        for (index, (left_name, left)) in listeners.iter().enumerate() {
            for (right_name, right) in listeners.iter().skip(index + 1) {
                if left.port() == right.port()
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
        gateway_id: impl Into<Arc<str>>,
        directory: Arc<dyn OnlineDirectory>,
        lease_ttl: Duration,
        renew_interval: Duration,
        duplicate_login: DuplicateLoginMode,
    ) -> Result<Self> {
        let gateway_id = gateway_id.into();
        if gateway_id.is_empty()
            || lease_ttl.is_zero()
            || renew_interval.is_zero()
            || renew_interval >= lease_ttl
        {
            return Err(Error::InvalidConfig(
                "online lease requires an id and 0 < renew interval < TTL".into(),
            ));
        }
        self.online = Some(OnlineConfig {
            gateway_id,
            directory,
            lease_ttl,
            renew_interval,
            duplicate_login,
        });
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

    /// Adds a required dependency to `/elura/readyz` evaluation.
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

struct ConnectionContext {
    config: GatewayConfig,
    tickets: Arc<TicketService>,
    replay: Arc<dyn ReplayStore>,
    world: Arc<dyn WorldClient>,
    sessions: SessionSenders,
    ownership: Option<(u32, Arc<dyn OwnershipResolver>)>,
    protector: Option<Arc<BackendProtector>>,
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
    stats: Arc<GatewayStats>,
}

impl ConnectionContext {
    async fn serve(self, mut connection: SessionConnection) -> Result<()> {
        let _active_connection = self.stats.connection_started();
        let peer = connection.peer;
        let session = Session::new(client_ip(peer));
        notify_session_observers(&self.observers, SessionEventKind::Connected, &session);
        if let Err(error) = self
            .check_admission(AdmissionRequest {
                stage: AdmissionStage::Connected,
                remote_ip: session.remote_ip(),
                identity: None,
            })
            .await
        {
            self.stats.rejected.fetch_add(1, Ordering::Relaxed);
            session.close();
            notify_session_observers(&self.observers, SessionEventKind::Closed, &session);
            return Err(error);
        }
        let response_tx = connection.responses;
        let push_tx = connection.pushes;
        let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
        let authenticated = Arc::new(AtomicBool::new(false));
        self.sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                session.id(),
                SessionHandle {
                    pushes: push_tx.clone(),
                    disconnect: disconnect_tx,
                    authenticated: authenticated.clone(),
                },
            );
        let session_id = session.id();
        let result = async {
            let mut limiter =
                TokenBucket::new(self.config.request_rate, self.config.request_burst);
            let mut byte_limiter = TokenBucket::new(
                self.config.inbound_byte_rate,
                self.config.inbound_byte_burst,
            );
            let mut route_limiters = self
                .config
                .route_rate_limits
                .iter()
                .map(|(route, limit)| {
                    (
                        *route,
                        TokenBucket::new(limit.requests_per_second, limit.burst),
                    )
                })
                .collect::<HashMap<_, _>>();
            let mut rate_limit_violations = 0_u32;
            let mut rate_limit_notified = false;
            let mut protocol_violations = 0_u32;
            let mut responses = ResponseCache::new(
                self.config.response_cache_ttl,
                self.config.response_cache_capacity,
                self.config.response_cache_max_bytes,
            );
            let renew_interval = self
                .online
                .as_ref()
                .map_or(Duration::from_secs(3600), |online| online.renew_interval);
            let mut renewal = tokio::time::interval_at(
                tokio::time::Instant::now() + renew_interval,
                renew_interval,
            );
            renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let now = tokio::time::Instant::now();
            let mut heartbeat = tokio::time::interval_at(
                now + self.config.heartbeat_interval,
                self.config.heartbeat_interval,
            );
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let authentication_deadline = now + self.config.authentication_timeout;
            let mut idle_deadline = now + self.config.idle_timeout;
            let mut heartbeat_request_id = u64::MAX;
            let mut pending_heartbeat = None;
            let mut version_checked_at = None;
            let mut lease_valid_until = None;

            loop {
            let next = tokio::select! {
                changed = disconnect_rx.changed() => {
                    if changed.is_err() || *disconnect_rx.borrow() { break Ok(()); }
                    continue;
                }
                _ = renewal.tick(), if self.online.is_some() => {
                    if let Some(identity) = session.identity() {
                        match self.renew_lease(session.id(), identity).await {
                            Ok(()) => lease_valid_until = self.lease_safety_deadline(),
                            Err(error) => {
                                debug!(session_id = %session.id(), %error, "renew online session lease");
                                if lease_valid_until.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                                    break Err(Error::Unavailable);
                                }
                            }
                        }
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(authentication_deadline), if session.identity().is_none() => {
                    break Err(Error::Authentication);
                }
                _ = heartbeat.tick() => {
                    if pending_heartbeat.is_none() {
                        let request_id = heartbeat_request_id;
                        heartbeat_request_id = heartbeat_request_id.saturating_sub(1).max(1);
                        response_tx
                            .try_send(Frame::request(ROUTE_HEARTBEAT, request_id, Bytes::new())?)
                            .map_err(|_| Error::QueueFull)?;
                        pending_heartbeat = Some(request_id);
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(idle_deadline) => break Err(Error::Timeout),
                next = connection.inbound.recv() => next,
            };
            let Some(frame) = next else {
                break Ok(());
            };
            let frame = frame?;
            let authenticated_now = session.identity().is_some();
            let action = validate_client_frame(&frame, authenticated_now, pending_heartbeat);
            let action = match action {
                Ok(action) => {
                    protocol_violations = 0;
                    action
                }
                Err(error) => {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    protocol_violations = protocol_violations.saturating_add(1);
                    if frame.kind == FrameKind::Request {
                        response_tx
                            .try_send(Frame::error(
                                &frame,
                                ErrorEnvelope::from(&error).to_bytes(),
                            ))
                            .map_err(|_| Error::QueueFull)?;
                    }
                    if protocol_violations >= self.config.max_protocol_violations {
                        break Err(error);
                    }
                    continue;
                }
            };
            idle_deadline = tokio::time::Instant::now() + self.config.idle_timeout;
            session.touch();
            if action == ClientFrameAction::HeartbeatResponse {
                pending_heartbeat = None;
                continue;
            }
            self.stats.requests.fetch_add(1, Ordering::Relaxed);
            let frame_bytes = u32::try_from(HEADER_LEN.saturating_add(frame.payload.len()))
                .unwrap_or(u32::MAX);
            let route_allowed = route_limiters
                .get_mut(&frame.route)
                .is_none_or(TokenBucket::allow);
            let byte_allowed = byte_limiter.allow_n(frame_bytes);
            let ip_allowed = self
                .ip_requests
                .as_ref()
                .is_none_or(|limiter| limiter.allow(session.remote_ip()));
            if !route_allowed
                || !limiter.allow()
                || !byte_allowed
                || !ip_allowed
            {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                rate_limit_violations = rate_limit_violations.saturating_add(1);
                if !rate_limit_notified {
                    response_tx
                        .try_send(Frame::error(
                            &frame,
                            ErrorEnvelope::from(&Error::RateLimited).to_bytes(),
                        ))
                        .map_err(|_| Error::QueueFull)?;
                    rate_limit_notified = true;
                }
                if rate_limit_violations >= self.config.max_rate_limit_violations {
                    break Err(Error::RateLimited);
                }
                continue;
            }
            rate_limit_violations = rate_limit_violations.saturating_sub(1);
            if rate_limit_violations == 0 {
                rate_limit_notified = false;
            }
            if let Some(cached) = responses.get(&frame)? {
                response_tx
                    .try_send(cached)
                    .map_err(|_| Error::QueueFull)?;
                continue;
            }
            if let Some(identity) = session.identity()
                && let Some(policy) = &self.account_versions
            {
                let now = tokio::time::Instant::now();
                let due = version_checked_at
                    .is_none_or(|checked| now.duration_since(checked) >= policy.check_interval());
                if due {
                    version_checked_at = Some(now);
                    if let Err(error) = policy.check(&identity).await {
                        if let Err(queue_error) = enqueue_session_control(
                            &push_tx,
                            SessionControlAction::AccountVersionChanged,
                            "account version changed or unavailable",
                        ) {
                            break Err(queue_error);
                        }
                        break Err(error);
                    }
                }
            }
            let was_authenticated = session.identity().is_some();
            let request_deadline = tokio::time::Instant::now() + self.config.handler_timeout;
            let response = tokio::select! {
                changed = disconnect_rx.changed() => {
                    if changed.is_err() || *disconnect_rx.borrow() {
                        break Ok(());
                    }
                    continue;
                }
                response = tokio::time::timeout_at(
                    request_deadline,
                    self.handle(&session, &frame, authenticated.as_ref(), request_deadline),
                ) => response,
            };
            let (response, cacheable) = match response {
                Ok(Ok(payload)) => (Frame::response(&frame, payload), true),
                Ok(Err(error)) => {
                    self.stats.record_error(&error);
                    error_response(&frame, &error)
                }
                Err(_) => {
                    self.stats.failures.fetch_add(1, Ordering::Relaxed);
                    (
                        Frame::error(
                            &frame,
                            ErrorEnvelope::from(&Error::Timeout).to_bytes(),
                        ),
                        false,
                    )
                }
            };
            if cacheable {
                responses.insert(&frame, response.clone());
            }
            response_tx
                .try_send(response)
                .map_err(|_| Error::QueueFull)?;
            if !was_authenticated && session.identity().is_some() {
                version_checked_at = Some(tokio::time::Instant::now());
                lease_valid_until = self.lease_safety_deadline();
            }
            }
        }
        .await;

        let identity = session.identity();
        if identity.is_some() {
            self.stats
                .authenticated_sessions
                .fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(identity) = &identity
            && timeout(
                Duration::from_millis(200),
                self.remove_online(session.id(), identity.clone()),
            )
            .await
            .is_err()
        {
            debug!(session_id = %session.id(), "online Session cleanup timed out");
        }
        session.close();
        notify_session_observers(&self.observers, SessionEventKind::Closed, &session);
        self.sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&session_id);
        self.session_index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
        self.topics
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, sessions| {
                sessions.remove(&session_id);
                !sessions.is_empty()
            });
        drop(response_tx);
        drop(push_tx);
        result
    }

    async fn handle(
        &self,
        session: &Session,
        frame: &Frame,
        authenticated: &AtomicBool,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes> {
        match frame.route {
            ROUTE_AUTHENTICATE => {
                if session.identity().is_some() {
                    return Err(Error::Authentication);
                }
                let request: AuthenticateRequest = serde_json::from_slice(&frame.payload)?;
                let verified = self.tickets.validate(&request.ticket)?;
                let pending_identity = verified.claims().identity.clone();
                self.check_admission(AdmissionRequest {
                    stage: AdmissionStage::Authenticated,
                    remote_ip: session.remote_ip(),
                    identity: Some(pending_identity.clone()),
                })
                .await?;
                if let Some(policy) = &self.account_versions {
                    policy.check(&pending_identity).await?;
                }
                let claims = verified.consume(self.replay.as_ref()).await?;
                let previous = self
                    .admit_online(session.id(), claims.identity.clone())
                    .await?;
                session.authenticate(claims.identity.clone())?;
                authenticated.store(true, Ordering::Release);
                self.stats
                    .authenticated_sessions
                    .fetch_add(1, Ordering::Relaxed);
                notify_session_observers(&self.observers, SessionEventKind::Authenticated, session);
                self.session_index
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(session.id(), claims.identity.clone());
                if let Some(transport) = &self.session_control {
                    let event = SessionControlEvent {
                        kind: SessionControlKind::Login,
                        region_id: claims.identity.region_id,
                        realm_id: claims.identity.realm_id,
                        user_id: claims.identity.user_id,
                        generation: claims.identity.generation,
                        session_id: Some(session.id()),
                        keep_session_id: Some(session.id()),
                        reason: "new login".into(),
                    };
                    if let Err(error) = transport.publish(&event).await {
                        debug!(session_id = %session.id(), %error, "publish Session login event");
                    }
                }
                self.kick_previous(previous, &claims.identity).await;
                Ok(Bytes::from(serde_json::to_vec(&AuthenticateResponse {
                    session_id: session.id().to_string(),
                    identity: claims.identity,
                })?))
            }
            ROUTE_HEARTBEAT => Ok(Bytes::new()),
            ROUTE_RECONNECT => {
                if !frame.payload.is_empty() && frame.payload != Bytes::from_static(b"{}") {
                    return Err(Error::InvalidFrame("invalid reconnect request".into()));
                }
                let identity = session.identity().ok_or(Error::Authentication)?;
                let ticket = self.tickets.issue(identity)?;
                Ok(Bytes::from(serde_json::to_vec(&ReconnectTicketResponse {
                    ticket,
                })?))
            }
            route => {
                let identity = session.identity().ok_or(Error::Authentication)?;
                let trace_id = observability::new_trace_id();
                let ownership = match &self.ownership {
                    Some((shard_count, resolver)) => {
                        let shard = shard_for(identity.user_id, *shard_count)?;
                        let assignment = resolver
                            .resolve(identity.region_id, identity.realm_id, shard)
                            .await?;
                        if assignment.region_id != identity.region_id
                            || assignment.realm_id != identity.realm_id
                            || assignment.shard_id != shard
                        {
                            return Err(Error::Unavailable);
                        }
                        Some(assignment)
                    }
                    None => None,
                };
                let span = tracing::info_span!(
                    "gateway.command",
                    trace_id = %trace_id,
                    route,
                    request_id = frame.request_id,
                    user_id = identity.user_id,
                    region_id = identity.region_id,
                    realm_id = identity.realm_id,
                );
                let context = GatewayInterceptContext::new(
                    identity,
                    session.id(),
                    session.remote_ip(),
                    trace_id,
                    ownership,
                );
                let request = GatewayRequest::new(route, frame.request_id, frame.payload.clone());
                let dispatch = WorldDispatch {
                    world: self.world.as_ref(),
                    protector: self.protector.as_deref(),
                    deadline,
                };
                async move {
                    interceptor::run_interceptors(&self.interceptors, &dispatch, &context, &request)
                        .await
                        .map(GatewayResponse::into_payload)
                }
                .instrument(span)
                .await
            }
        }
    }

    async fn check_admission(&self, request: AdmissionRequest) -> Result<()> {
        match &self.admission {
            Some(admission) => admission.check(request).await,
            None => Ok(()),
        }
    }

    fn lease(&self, session_id: Uuid, identity: Identity) -> Option<SessionLease> {
        self.online.as_ref().map(|online| SessionLease {
            session_id,
            gateway_id: online.gateway_id.to_string(),
            identity,
            expires_at: SystemTime::now() + online.lease_ttl,
        })
    }

    async fn admit_online(&self, session_id: Uuid, identity: Identity) -> Result<Vec<Uuid>> {
        let local: Vec<_> = self
            .session_index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .users
            .get(&identity_key(&identity))
            .map(|sessions| sessions.iter().copied().collect())
            .unwrap_or_default();
        let Some(online) = &self.online else {
            return Ok(local);
        };
        if online.duplicate_login == DuplicateLoginMode::RejectNew && !local.is_empty() {
            return Err(Error::DuplicateSession);
        }
        let lease = self.lease(session_id, identity).ok_or(Error::Unavailable)?;
        let mut previous = local;
        if online.duplicate_login != DuplicateLoginMode::AllowMultiple {
            let remote = online
                .directory
                .claim_single(
                    &lease,
                    online.duplicate_login == DuplicateLoginMode::KickExisting,
                )
                .await?;
            if online.duplicate_login == DuplicateLoginMode::RejectNew && remote.is_some() {
                return Err(Error::DuplicateSession);
            }
            if let Some(remote) = remote {
                previous.push(remote);
            }
        }
        online.directory.register(lease).await?;
        previous.sort_unstable();
        previous.dedup();
        Ok(previous)
    }

    async fn renew_lease(&self, session_id: Uuid, identity: Identity) -> Result<()> {
        let Some(online) = &self.online else {
            return Ok(());
        };
        online
            .directory
            .renew(self.lease(session_id, identity).ok_or(Error::Unavailable)?)
            .await
    }

    fn lease_safety_deadline(&self) -> Option<tokio::time::Instant> {
        self.online.as_ref().map(|online| {
            tokio::time::Instant::now() + online.lease_ttl.saturating_sub(online.renew_interval)
        })
    }

    async fn remove_online(&self, session_id: Uuid, identity: Identity) {
        let Some(online) = &self.online else {
            return;
        };
        let Some(lease) = self.lease(session_id, identity) else {
            return;
        };
        if let Err(error) = online.directory.unregister(&lease).await {
            debug!(%session_id, %error, "unregister online session");
        }
        if let Err(error) = online.directory.release_single(&lease).await {
            debug!(%session_id, %error, "release single-session claim");
        }
    }

    async fn kick_previous(&self, sessions: Vec<Uuid>, identity: &Identity) {
        if sessions.is_empty()
            || self
                .online
                .as_ref()
                .is_none_or(|online| online.duplicate_login != DuplicateLoginMode::KickExisting)
        {
            return;
        }
        for session_id in sessions {
            let request = PushRequest {
                region_id: identity.region_id,
                realm_id: identity.realm_id,
                target: PushTarget::Disconnect(session_id),
                route: 0,
                sequence: 0,
                trace_id: observability::new_trace_id(),
                payload: Bytes::from_static(b"duplicate_login"),
            };
            if let Some(handle) = self
                .sessions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&session_id)
                .cloned()
            {
                let _ = disconnect_handle(handle, "duplicate_login");
            } else if let Some(push) = &self.push {
                let _ = push.publish(&request).await;
            }
        }
    }
}

struct WorldDispatch<'a> {
    world: &'a dyn WorldClient,
    protector: Option<&'a BackendProtector>,
    deadline: tokio::time::Instant,
}

#[async_trait]
impl interceptor::GatewayDispatch for WorldDispatch<'_> {
    async fn dispatch(
        &self,
        context: &GatewayInterceptContext,
        request: &GatewayRequest,
    ) -> Result<GatewayResponse> {
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_millis(1));
        let command = || {
            self.world.command(WorldRequest {
                identity: context.identity().clone(),
                session_id: context.session_id(),
                trace_id: context.trace_id().to_owned(),
                route: request.route(),
                request_id: request.request_id(),
                payload: request.payload().clone(),
                ownership: context.ownership().cloned(),
                timeout: remaining,
            })
        };
        let payload = match self.protector {
            Some(protector) => {
                protector
                    .execute(command, |error| {
                        matches!(error, Error::Unavailable | Error::Timeout | Error::Io(_))
                    })
                    .await?
            }
            None => command().await?,
        };
        Ok(GatewayResponse::new(payload))
    }
}

#[async_trait]
impl SessionService for GatewayServer {
    async fn serve_session(&self, connection: SessionConnection) -> Result<()> {
        let _active_session = self.drain.enter()?;
        let _permit = match self.connections.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Unavailable);
            }
        };
        let _ip_permit = match self
            .per_ip_connections
            .try_enter(client_ip(connection.peer))
        {
            Ok(permit) => permit,
            Err(error) => {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
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
        self.stats.pushes.fetch_add(1, Ordering::Relaxed);
        let result = self.deliver_inner(request).await;
        if result.is_err() {
            self.stats.push_failures.fetch_add(1, Ordering::Relaxed);
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

fn disconnect_handle(handle: SessionHandle, reason: &str) -> Result<()> {
    disconnect_handle_with_action(handle, SessionControlAction::Kick, reason)
}

fn disconnect_handle_with_action(
    handle: SessionHandle,
    action: SessionControlAction,
    reason: &str,
) -> Result<()> {
    let notification = enqueue_session_control(&handle.pushes, action, reason);
    let disconnected = handle.disconnect.send(true).map_err(|_| Error::Unavailable);
    notification.and(disconnected)
}

fn enqueue_session_control(
    pushes: &mpsc::Sender<Frame>,
    action: SessionControlAction,
    reason: &str,
) -> Result<()> {
    let payload = SessionControl::new(action, reason)?.encode_frame_payload()?;
    pushes
        .try_send(Frame {
            kind: FrameKind::Push,
            flags: 0,
            route: ROUTE_SESSION_CONTROL,
            request_id: 0,
            sequence: 0,
            payload,
        })
        .map_err(|_| Error::QueueFull)
}

fn client_ip(peer: SocketAddr) -> IpAddr {
    peer.ip()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientFrameAction {
    Request,
    HeartbeatResponse,
}

fn validate_client_frame(
    frame: &Frame,
    authenticated: bool,
    pending_heartbeat: Option<u64>,
) -> Result<ClientFrameAction> {
    if frame.kind == FrameKind::Response && frame.route == ROUTE_HEARTBEAT {
        if frame.request_id != pending_heartbeat.unwrap_or(0)
            || frame.sequence != 0
            || !frame.payload.is_empty()
        {
            return Err(Error::InvalidFrame(
                "heartbeat response does not match an outstanding request".into(),
            ));
        }
        return Ok(ClientFrameAction::HeartbeatResponse);
    }
    if frame.kind != FrameKind::Request {
        return Err(Error::InvalidFrame(
            "gateway accepts request frames and correlated heartbeat responses only".into(),
        ));
    }
    if frame.route < FIRST_APPLICATION_ROUTE && frame.sequence != 0 {
        return Err(Error::InvalidFrame(
            "framework requests must have sequence zero".into(),
        ));
    }
    let allowed = if authenticated {
        matches!(frame.route, ROUTE_HEARTBEAT | ROUTE_RECONNECT)
            || frame.route >= FIRST_APPLICATION_ROUTE
    } else {
        frame.route == ROUTE_AUTHENTICATE
    };
    if !allowed {
        return Err(Error::InvalidFrame(
            "route is not allowed in the current session state".into(),
        ));
    }
    Ok(ClientFrameAction::Request)
}

fn identity_key(identity: &Identity) -> UserKey {
    (identity.region_id, identity.realm_id, identity.user_id)
}

fn error_response(request: &Frame, error: &Error) -> (Frame, bool) {
    let envelope = ErrorEnvelope::from(error);
    let cacheable = !envelope.retryable;
    (Frame::error(request, envelope.to_bytes()), cacheable)
}

#[cfg(test)]
mod config_tests {
    use super::*;

    fn world_request(timeout: Duration) -> WorldRequest {
        WorldRequest {
            identity: Identity {
                account_id: 1,
                user_id: 1,
                region_id: 1,
                realm_id: 1,
                generation: 1,
            },
            session_id: Uuid::new_v4(),
            trace_id: "test".into(),
            route: 100,
            request_id: 1,
            payload: Bytes::new(),
            ownership: None,
            timeout,
        }
    }

    #[test]
    fn serde_uses_defaults_and_rejects_unknown_fields() {
        let config: GatewayConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            config.max_connections,
            GatewayConfig::default().max_connections
        );
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"unknown":true}"#).is_err());
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"listen":"0.0.0.0:17000"}"#).is_err());
        assert!(serde_json::from_str::<GatewayConfig>(r#"{"admin":null}"#).is_err());
    }

    #[test]
    fn validates_resource_limits() {
        let mut config = GatewayConfig::default();
        config.inbound_byte_burst = config.max_payload as u32;
        assert!(config.validate().is_err());

        let mut config = GatewayConfig {
            ip_request_rate: 0,
            ip_request_burst: 0,
            ..GatewayConfig::default()
        };
        assert!(config.validate().is_ok());
        config.ip_request_burst = 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn client_frame_state_machine_rejects_reserved_and_out_of_state_routes() {
        let authentication = Frame::request(ROUTE_AUTHENTICATE, 1, Bytes::new()).unwrap();
        assert_eq!(
            validate_client_frame(&authentication, false, None).unwrap(),
            ClientFrameAction::Request
        );
        assert!(validate_client_frame(&authentication, true, None).is_err());

        let application = Frame::request(FIRST_APPLICATION_ROUTE, 2, Bytes::new()).unwrap();
        assert!(validate_client_frame(&application, false, None).is_err());
        assert_eq!(
            validate_client_frame(&application, true, None).unwrap(),
            ClientFrameAction::Request
        );

        for route in [ROUTE_SESSION_CONTROL, ROUTE_SESSION_CONTROL + 1] {
            let reserved = Frame::request(route, 3, Bytes::new()).unwrap();
            assert!(validate_client_frame(&reserved, true, None).is_err());
        }
    }

    #[test]
    fn framework_sequence_and_heartbeat_response_are_correlated() {
        let mut reconnect = Frame::request(ROUTE_RECONNECT, 1, Bytes::new()).unwrap();
        reconnect.sequence = 1;
        assert!(validate_client_frame(&reconnect, true, None).is_err());

        let heartbeat = Frame::request(ROUTE_HEARTBEAT, 77, Bytes::new()).unwrap();
        let response = Frame::response(&heartbeat, Bytes::new());
        assert_eq!(
            validate_client_frame(&response, true, Some(77)).unwrap(),
            ClientFrameAction::HeartbeatResponse
        );
        assert!(validate_client_frame(&response, true, Some(78)).is_err());
        assert!(validate_client_frame(&response, true, None).is_err());

        let response_with_payload = Frame::response(&heartbeat, Bytes::from_static(b"fake"));
        assert!(validate_client_frame(&response_with_payload, true, Some(77)).is_err());
    }

    #[test]
    fn only_terminal_errors_are_response_cached() {
        let request = Frame::request(100, 1, Bytes::new()).unwrap();
        assert!(!error_response(&request, &Error::Timeout).1);
        assert!(!error_response(&request, &Error::Unavailable).1);
        assert!(error_response(&request, &Error::business("DENIED", "denied"),).1);
    }

    #[tokio::test]
    async fn world_request_timeout_replaces_a_half_open_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = Framed::new(first, FrameCodec::default());
            first.next().await.unwrap().unwrap();
            assert!(first.next().await.is_none());

            let (second, _) = listener.accept().await.unwrap();
            let mut second = Framed::new(second, FrameCodec::default());
            let request = second.next().await.unwrap().unwrap();
            second
                .send(Frame::response(&request, Bytes::from_static(b"ok")))
                .await
                .unwrap();
        });
        let client = TcpWorldClient::with_pool_size(address, 1024, 1).unwrap();
        assert!(matches!(
            client
                .command(world_request(Duration::from_millis(20)))
                .await,
            Err(Error::Timeout)
        ));
        assert_eq!(
            client
                .command(world_request(Duration::from_secs(1)))
                .await
                .unwrap(),
            Bytes::from_static(b"ok")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn world_request_timeout_does_not_cancel_another_in_flight_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = Framed::new(stream, FrameCodec::default());
            let first = stream.next().await.unwrap().unwrap();
            let second = stream.next().await.unwrap().unwrap();

            tokio::time::sleep(Duration::from_millis(50)).await;
            stream
                .send(Frame::response(
                    &first,
                    Bytes::from_static(b"late-response"),
                ))
                .await
                .unwrap();
            stream
                .send(Frame::response(&second, Bytes::from_static(b"ok")))
                .await
                .unwrap();
        });
        let client = TcpWorldClient::with_pool_size(address, 1024, 1).unwrap();

        let (first, second) = tokio::join!(
            client.command(world_request(Duration::from_millis(20))),
            client.command(world_request(Duration::from_secs(1))),
        );

        assert!(matches!(first, Err(Error::Timeout)));
        assert_eq!(second.unwrap(), Bytes::from_static(b"ok"));
        server.await.unwrap();
    }
}
