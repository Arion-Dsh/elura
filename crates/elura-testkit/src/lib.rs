//! Full-stack business tests and transport-selectable load scenarios for Elura.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]

mod transport;

pub use transport::{
    TcpTestTransport, TestConnection, TestTransport, WebSocketTestTransport, loopback_address,
};

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use elura_core::protocol::{Frame, FrameKind, ROUTE_AUTHENTICATE};
use elura_core::session::Identity;
use elura_core::ticket::TicketService;
use elura_core::{Error, ErrorEnvelope, Result};
use elura_gateway::{Gateway, GatewayConfig, TcpWorldClient};
use elura_runtime::observability::AdminServerConfig;
use elura_runtime::security::InternalToken;
use elura_world::{
    Route, World, WorldConfig, WorldContext, WorldHandler, WorldMiddleware, WorldModule,
};
use futures_util::FutureExt;
use prost::Message;
use tokio::sync::{Barrier, watch};
use tokio::task::{JoinHandle, JoinSet};

/// Creates a minimal identity for full-stack tests.
pub fn test_identity(user_id: i64) -> Identity {
    Identity {
        account_id: user_id,
        user_id,
        region_id: 1,
        realm_id: 1,
        generation: 1,
    }
}

/// Builder for a real Gateway-to-World test deployment.
pub struct FullStackBuilder {
    gateway: Gateway,
    gateway_config: GatewayConfig,
    world: World,
    world_address: std::net::SocketAddr,
    internal_token: InternalToken,
}

impl FullStackBuilder {
    /// Creates a builder from explicit Gateway and World configuration.
    pub fn new(mut gateway: GatewayConfig, mut world: WorldConfig) -> Result<Self> {
        let internal_token = InternalToken::new(uuid::Uuid::new_v4().simple().to_string())?;
        world.internal_token = Some(internal_token.expose().to_owned());
        if gateway.ticket.key.is_empty() {
            gateway.ticket.key = uuid::Uuid::new_v4().simple().to_string();
        }
        let world_address = world.listen;
        Ok(Self {
            gateway: Gateway::new(gateway.clone()),
            gateway_config: gateway,
            world: World::new(world),
            world_address,
            internal_token,
        })
    }

    /// Creates a builder with default limits and automatically selected loopback ports.
    pub fn loopback() -> Result<Self> {
        let mut world = WorldConfig::default();
        world.listen = loopback_address()?;
        world.discovery_drain_delay = Duration::ZERO;
        world.shutdown_timeout = Duration::from_secs(1);
        let mut gateway = GatewayConfig::default();
        gateway.shutdown_timeout = Duration::from_secs(1);
        Self::new(gateway, world)
    }

    /// Registers a typed World business route.
    pub fn route<E, F, Fut>(mut self, route: E, handler: F) -> Self
    where
        E: Route,
        F: Fn(WorldContext, E::Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Response>> + Send + 'static,
    {
        self.world = self.world.route(route, handler);
        self
    }

    /// Registers a raw World route.
    pub fn route_raw(mut self, route: u32, handler: impl WorldHandler) -> Self {
        self.world = self.world.route_raw(route, handler);
        self
    }

    /// Adds global World middleware.
    pub fn middleware<M: WorldMiddleware>(mut self, middleware: M) -> Self {
        self.world = self.world.middleware(middleware);
        self
    }

    /// Installs a World module.
    pub fn install<M: WorldModule>(mut self, module: M) -> Self {
        self.world = self.world.install(module);
        self
    }

    /// Applies advanced Gateway configuration before the test server is built.
    pub fn gateway(mut self, configure: impl FnOnce(Gateway) -> Gateway) -> Self {
        self.gateway = configure(self.gateway);
        self
    }

    /// Starts independent World and Gateway servers using the selected client transport.
    pub async fn start<T: TestTransport>(self, transport: T) -> Result<FullStackHarness> {
        let tickets = Arc::new(TicketService::new(
            self.gateway_config.ticket.key.clone(),
            self.gateway_config.ticket.issuer.clone(),
            self.gateway_config.ticket.audience.clone(),
            self.gateway_config.ticket.login_ttl,
            self.gateway_config.ticket.reconnect_ttl,
        )?);
        let world = self.world.build()?;
        let world_client = TcpWorldClient::with_pool_size(
            self.world_address,
            self.gateway_config.max_payload,
            self.gateway_config.world_routing.pool_size,
        )?
        .with_internal_token(self.internal_token)
        .with_max_in_flight_per_connection(
            self.gateway_config
                .world_routing
                .max_in_flight_per_connection,
        )?;
        let gateway = Arc::new(
            self.gateway
                .world_client(Arc::new(world_client))
                .transport(transport.server()?)
                .build()?,
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let world_admin =
            AdminServerConfig::new(loopback_address()?, "world", "elura-testkit-world");
        let world_task = tokio::spawn(world.serve(world_admin, shutdown_rx.clone()));
        let gateway_task = tokio::spawn(gateway.serve_embedded(shutdown_rx));
        let connector: Arc<dyn Connector> = Arc::new(TransportConnector(transport));
        let harness = FullStackHarness {
            inner: Arc::new(HarnessInner {
                connector,
                tickets,
                operation_timeout: Duration::from_secs(5),
                startup_timeout: Duration::from_secs(5),
                shutdown_tx,
                tasks: Mutex::new(Some(vec![world_task, gateway_task])),
            }),
        };
        harness.wait_until_ready(self.world_address).await?;
        Ok(harness)
    }
}

#[async_trait::async_trait]
trait Connector: Send + Sync {
    fn name(&self) -> &'static str;
    async fn connect(&self) -> Result<Box<dyn TestConnection>>;
}

struct TransportConnector<T>(T);

#[async_trait::async_trait]
impl<T: TestTransport> Connector for TransportConnector<T> {
    fn name(&self) -> &'static str {
        self.0.name()
    }

    async fn connect(&self) -> Result<Box<dyn TestConnection>> {
        self.0.connect().await
    }
}

struct HarnessInner {
    connector: Arc<dyn Connector>,
    tickets: Arc<TicketService>,
    operation_timeout: Duration,
    startup_timeout: Duration,
    shutdown_tx: watch::Sender<bool>,
    tasks: Mutex<Option<Vec<JoinHandle<Result<()>>>>>,
}

impl Drop for HarnessInner {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(tasks) = self.tasks.get_mut().ok().and_then(Option::take) {
            for task in tasks {
                task.abort();
            }
        }
    }
}

/// Running independent Gateway and World servers plus a selectable client transport.
#[derive(Clone)]
pub struct FullStackHarness {
    inner: Arc<HarnessInner>,
}

impl FullStackHarness {
    /// Returns the selected client transport name.
    pub fn transport_name(&self) -> &'static str {
        self.inner.connector.name()
    }

    /// Connects and authenticates one full-stack business client.
    pub async fn client(&self, identity: Identity) -> Result<FullStackClient> {
        Ok(self.connect_client(identity).await?.client)
    }

    async fn wait_until_ready(&self, world_address: std::net::SocketAddr) -> Result<()> {
        let deadline = Instant::now() + self.inner.startup_timeout;
        loop {
            let world = tokio::net::TcpStream::connect(world_address).await;
            let gateway = self.inner.connector.connect().await;
            if world.is_ok() && gateway.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn connect_client(&self, identity: Identity) -> Result<ConnectedClient> {
        identity.validate()?;
        let connect_started = Instant::now();
        let deadline = connect_started + self.inner.startup_timeout;
        let connection = loop {
            match self.inner.connector.connect().await {
                Ok(connection) => break connection,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        };
        let connect_latency = connect_started.elapsed();
        let client = FullStackClient {
            inner: Arc::new(ClientInner {
                connection: tokio::sync::Mutex::new(connection),
                request_id: AtomicU64::new(1),
                timeout: self.inner.operation_timeout,
                identity: identity.clone(),
            }),
        };
        let ticket = self.inner.tickets.issue_login(identity)?;
        let authentication_started = Instant::now();
        client
            .exchange(Frame::request(
                ROUTE_AUTHENTICATE,
                client.next_request_id(),
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket }))?,
            )?)
            .await?;
        Ok(ConnectedClient {
            client,
            connect_latency,
            authentication_latency: authentication_started.elapsed(),
        })
    }

    /// Runs a full-stack business scenario through the selected transport.
    pub async fn load_scenario<I, S, Fut>(
        &self,
        config: FullStackLoadConfig,
        make_identity: I,
        scenario: S,
    ) -> Result<FullStackLoadReport>
    where
        I: Fn(usize) -> Identity + Send + Sync + 'static,
        S: Fn(FullStackClient, usize, usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        config.validate()?;
        let mut preparing = JoinSet::new();
        let make_identity = Arc::new(make_identity);
        for worker in 0..config.concurrency {
            let harness = self.clone();
            let make_identity = make_identity.clone();
            preparing.spawn(async move {
                (worker, harness.connect_client(make_identity(worker)).await)
            });
        }
        let mut clients = (0..config.concurrency).map(|_| None).collect::<Vec<_>>();
        while let Some(client) = preparing.join_next().await {
            let (worker, client) = client.map_err(join_error)?;
            clients[worker] = Some(client?);
        }
        let clients = clients
            .into_iter()
            .map(|client| client.expect("every full-stack client preparation task completed"))
            .collect::<Vec<_>>();
        let connect_latency = latency(clients.iter().map(|client| client.connect_latency));
        let authentication_latency =
            latency(clients.iter().map(|client| client.authentication_latency));
        let scenario = Arc::new(scenario);
        let barrier = Arc::new(Barrier::new(config.concurrency + 1));
        let mut workers = JoinSet::new();
        for (worker, connected) in clients.into_iter().enumerate() {
            let scenario = scenario.clone();
            let barrier = barrier.clone();
            workers.spawn(async move {
                barrier.wait().await;
                let mut result = WorkerResult::new(config.iterations_per_worker);
                for iteration in 0..config.iterations_per_worker {
                    let started = Instant::now();
                    let response = match catch_unwind(AssertUnwindSafe(|| {
                        scenario(connected.client.clone(), worker, iteration)
                    })) {
                        Ok(future) => AssertUnwindSafe(future)
                            .catch_unwind()
                            .await
                            .unwrap_or_else(|_| {
                                Err(Error::Internal("full-stack scenario panicked".into()))
                            }),
                        Err(_) => Err(Error::Internal("full-stack scenario panicked".into())),
                    };
                    result.samples.push(started.elapsed());
                    match response {
                        Ok(()) => result.succeeded += 1,
                        Err(error) => {
                            *result
                                .errors
                                .entry(ErrorEnvelope::from(&error).code)
                                .or_default() += 1;
                        }
                    }
                }
                result
            });
        }
        let started = Instant::now();
        barrier.wait().await;
        let mut succeeded = 0_u64;
        let mut errors = BTreeMap::new();
        let mut samples = Vec::new();
        while let Some(worker) = workers.join_next().await {
            let worker = worker.map_err(join_error)?;
            succeeded += worker.succeeded;
            samples.extend(worker.samples);
            for (code, count) in worker.errors {
                *errors.entry(code).or_default() += count;
            }
        }
        let attempted = (config.concurrency * config.iterations_per_worker) as u64;
        Ok(FullStackLoadReport {
            transport: self.transport_name(),
            attempted,
            succeeded,
            failed: attempted - succeeded,
            elapsed: started.elapsed(),
            connect_latency,
            authentication_latency,
            operation_latency: latency(samples),
            errors,
        })
    }

    /// Stops Gateway and World and waits for both service tasks.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.inner.shutdown_tx.send(true);
        let tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_default();
        for task in tasks {
            task.await.map_err(join_error)??;
        }
        Ok(())
    }
}

struct ConnectedClient {
    client: FullStackClient,
    connect_latency: Duration,
    authentication_latency: Duration,
}

struct ClientInner {
    connection: tokio::sync::Mutex<Box<dyn TestConnection>>,
    request_id: AtomicU64,
    timeout: Duration,
    identity: Identity,
}

/// Authenticated client whose calls traverse the selected transport and the real Gateway.
#[derive(Clone)]
pub struct FullStackClient {
    inner: Arc<ClientInner>,
}

impl FullStackClient {
    /// Returns the authenticated identity.
    pub fn identity(&self) -> &Identity {
        &self.inner.identity
    }

    /// Calls one typed route through Transport → Gateway → World.
    pub async fn call<E: Route>(&self, _route: E, request: E::Request) -> Result<E::Response> {
        let response = self
            .exchange(Frame::request(
                E::ID,
                self.next_request_id(),
                Bytes::from(request.encode_to_vec()),
            )?)
            .await?;
        E::Response::decode(response.payload)
            .map_err(|_| Error::InvalidFrame("invalid typed full-stack response".into()))
    }

    /// Sends a raw application route through the complete stack.
    pub async fn command_raw(&self, route: u32, payload: impl Into<Bytes>) -> Result<Bytes> {
        Ok(self
            .exchange(Frame::request(route, self.next_request_id(), payload)?)
            .await?
            .payload)
    }

    fn next_request_id(&self) -> u64 {
        loop {
            let request_id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);
            if request_id != 0 {
                return request_id;
            }
        }
    }

    async fn exchange(&self, request: Frame) -> Result<Frame> {
        let request_id = request.request_id;
        let mut connection = self.inner.connection.lock().await;
        tokio::time::timeout(self.inner.timeout, connection.send(request))
            .await
            .map_err(|_| Error::Timeout)??;
        loop {
            let response = tokio::time::timeout(self.inner.timeout, connection.receive())
                .await
                .map_err(|_| Error::Timeout)??;
            if response.request_id != request_id {
                continue;
            }
            return match response.kind {
                FrameKind::Response => Ok(response),
                FrameKind::Error => Err(ErrorEnvelope::from_slice(&response.payload)?.into_error()),
                _ => Err(Error::InvalidFrame(
                    "unexpected full-stack response kind".into(),
                )),
            };
        }
    }
}

/// Concurrent full-stack scenario settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullStackLoadConfig {
    /// Number of concurrent authenticated clients.
    pub concurrency: usize,
    /// Business-scenario iterations run by each client.
    pub iterations_per_worker: usize,
}

impl FullStackLoadConfig {
    /// Creates load settings.
    pub const fn new(concurrency: usize, iterations_per_worker: usize) -> Self {
        Self {
            concurrency,
            iterations_per_worker,
        }
    }

    fn validate(self) -> Result<()> {
        if self.concurrency == 0 || self.iterations_per_worker == 0 {
            return Err(Error::InvalidConfig(
                "full-stack load settings must be positive".into(),
            ));
        }
        self.concurrency
            .checked_mul(self.iterations_per_worker)
            .ok_or_else(|| Error::InvalidConfig("full-stack load size overflow".into()))?;
        Ok(())
    }
}

/// Latency distribution for one full-stack phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Latency {
    /// Fastest sample.
    pub min: Duration,
    /// Median sample.
    pub p50: Duration,
    /// 95th percentile.
    pub p95: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// Slowest sample.
    pub max: Duration,
}

/// Complete result of a transport-specific full-stack load run.
#[derive(Debug, Clone, PartialEq)]
pub struct FullStackLoadReport {
    /// Selected transport name. Results from different transports must not be merged.
    pub transport: &'static str,
    /// Attempted scenario operations.
    pub attempted: u64,
    /// Successful scenario operations.
    pub succeeded: u64,
    /// Failed scenario operations.
    pub failed: u64,
    /// Scenario wall-clock duration after clients authenticated.
    pub elapsed: Duration,
    /// Client connection latency.
    pub connect_latency: Latency,
    /// Ticket authentication latency.
    pub authentication_latency: Latency,
    /// Complete business-scenario iteration latency.
    pub operation_latency: Latency,
    /// Stable error-code counts.
    pub errors: BTreeMap<String, u64>,
}

impl FullStackLoadReport {
    /// Returns true when every attempted scenario operation succeeded.
    pub fn is_success(&self) -> bool {
        self.failed == 0
    }

    /// Returns successful operations divided by attempted operations.
    pub fn success_ratio(&self) -> f64 {
        self.succeeded as f64 / self.attempted.max(1) as f64
    }

    /// Returns attempted scenario operations per second.
    pub fn operations_per_second(&self) -> f64 {
        self.attempted as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }
}

struct WorkerResult {
    succeeded: u64,
    samples: Vec<Duration>,
    errors: BTreeMap<String, u64>,
}

impl WorkerResult {
    fn new(capacity: usize) -> Self {
        Self {
            succeeded: 0,
            samples: Vec::with_capacity(capacity),
            errors: BTreeMap::new(),
        }
    }
}

fn latency(samples: impl IntoIterator<Item = Duration>) -> Latency {
    let mut samples = samples.into_iter().collect::<Vec<_>>();
    if samples.is_empty() {
        return Latency::default();
    }
    samples.sort_unstable();
    Latency {
        min: samples[0],
        p50: percentile(&samples, 50),
        p95: percentile(&samples, 95),
        p99: percentile(&samples, 99),
        max: samples[samples.len() - 1],
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let rank = (samples.len() * percentile).div_ceil(100);
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}

fn join_error(error: tokio::task::JoinError) -> Error {
    Error::Internal(format!("full-stack test task failed: {error}"))
}
