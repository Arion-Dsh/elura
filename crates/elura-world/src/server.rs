use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use elura_core::ownership::{OwnershipResolver, shard_for};
use elura_core::protocol::{Frame, FrameCodec, FrameKind};
use elura_core::{Error, ErrorEnvelope, Result};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use elura_core::gateway_world::{
    GatewayWorldCommand, WorldClient, WorldCommand, WorldRegistrar, WorldRequest,
};
use elura_runtime::observability::{AdminServer, AdminServerConfig};
use elura_runtime::security::{BoxedServiceStream, InternalToken, ServerTlsConfig};

use super::WorldModule;
use super::config::WorldConfig;
use super::runtime::WorldRuntime;
use super::{RouteInfo, WorldHarness, WorldStatsSnapshot};

pub struct WorldServer {
    config: WorldConfig,
    runtime: Arc<WorldRuntime>,
    modules: Vec<Arc<dyn WorldModule>>,
    ownership: Option<WorldOwnership>,
    authorization: Option<InternalToken>,
    tls: Option<ServerTlsConfig>,
    registrar: Option<Arc<dyn WorldRegistrar>>,
    ready: Arc<AtomicBool>,
    admin: Option<AdminServer>,
    admin_listen: Option<SocketAddr>,
    http: Vec<WorldHttpServer>,
}

struct WorldHttpServer {
    listen: SocketAddr,
    router: Router,
}

pub struct WorldDiagnostics {
    runtime: Arc<WorldRuntime>,
    ready: Arc<AtomicBool>,
}

/// Direct Gateway-to-World client used by a monolithic process.
///
/// It preserves the normal World execution and ownership checks without
/// serializing or opening an internal TCP connection.
#[derive(Clone)]
pub struct InProcessWorldClient {
    runtime: Arc<WorldRuntime>,
    ownership: Option<WorldOwnership>,
    ready: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl WorldClient for InProcessWorldClient {
    async fn command(&self, request: WorldRequest) -> Result<bytes::Bytes> {
        if request.request_id == 0 {
            return Err(Error::InvalidFrame(
                "World command request ID is zero".into(),
            ));
        }
        if !self.ready.load(Ordering::Acquire) {
            return Err(Error::Unavailable);
        }
        request.identity.validate()?;
        let command = WorldCommand {
            authorization: None,
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
        validate_ownership(&command, self.ownership.as_ref()).await?;
        self.runtime
            .execute(request.route, request.request_id, command)
            .await
    }

    async fn readiness(&self) -> Result<()> {
        if self.ready.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
}

impl WorldDiagnostics {
    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn routes(&self) -> Vec<RouteInfo> {
        self.runtime.route_info()
    }

    pub fn stats(&self) -> WorldStatsSnapshot {
        self.runtime.stats()
    }
}

#[derive(Clone)]
struct WorldOwnership {
    instance: Arc<str>,
    shards: u32,
    resolver: Arc<dyn OwnershipResolver>,
}

impl WorldServer {
    pub(crate) fn from_parts(
        config: WorldConfig,
        runtime: Arc<WorldRuntime>,
        modules: Vec<Arc<dyn WorldModule>>,
    ) -> Self {
        Self {
            config,
            runtime,
            modules,
            ownership: None,
            authorization: None,
            tls: None,
            registrar: None,
            ready: Arc::new(AtomicBool::new(false)),
            admin: None,
            admin_listen: None,
            http: Vec::new(),
        }
    }

    pub(crate) fn configure_process(mut self) -> Result<Self> {
        if let Some(token) = self.config.internal_token.as_ref() {
            self.authorization = Some(InternalToken::new(token.clone())?);
        }
        if let Some(tls) = self.config.tls.clone() {
            self.tls = Some(tls.build()?);
        }
        Ok(self)
    }

    pub(crate) fn with_admin(mut self, config: AdminServerConfig) -> Result<Self> {
        self.admin_listen = Some(config.listen);
        self.admin = Some(AdminServer::new(config, self.diagnostics())?);
        Ok(self)
    }

    pub(crate) fn add_http(&mut self, listen: String, router: Router) -> Result<()> {
        let listen = listen
            .parse()
            .map_err(|_| Error::InvalidConfig(format!("invalid HTTP listen address {listen}")))?;
        self.http.push(WorldHttpServer { listen, router });
        Ok(())
    }

    pub(crate) fn validate_listeners(&self) -> Result<()> {
        let mut listeners = vec![("world", self.config.listen)];
        if let Some(admin_listen) = self.admin_listen {
            listeners.push(("admin", admin_listen));
        }
        for http in &self.http {
            listeners.push(("http", http.listen));
        }
        for (index, (left_name, left)) in listeners.iter().enumerate() {
            for (right_name, right) in listeners.iter().skip(index + 1) {
                if listeners_conflict(*left, *right) {
                    return Err(Error::InvalidConfig(format!(
                        "{left_name} and {right_name} listeners conflict at port {}",
                        left.port()
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn with_internal_token(mut self, token: InternalToken) -> Self {
        self.authorization = Some(token);
        self
    }

    pub fn with_tls(mut self, tls: ServerTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_registrar(mut self, registrar: Arc<dyn WorldRegistrar>) -> Self {
        self.registrar = Some(registrar);
        self
    }

    pub fn with_ownership(
        mut self,
        instance: impl Into<Arc<str>>,
        shards: u32,
        resolver: Arc<dyn OwnershipResolver>,
    ) -> Result<Self> {
        let instance = instance.into();
        if instance.is_empty() || shards == 0 {
            return Err(Error::InvalidConfig("World ownership".into()));
        }
        self.ownership = Some(WorldOwnership {
            instance,
            shards,
            resolver,
        });
        Ok(self)
    }

    pub fn routes(&self) -> Vec<RouteInfo> {
        self.runtime.route_info()
    }

    pub fn stats(&self) -> WorldStatsSnapshot {
        self.runtime.stats()
    }

    pub fn diagnostics(&self) -> Arc<WorldDiagnostics> {
        Arc::new(WorldDiagnostics {
            runtime: self.runtime.clone(),
            ready: self.ready.clone(),
        })
    }

    pub fn harness(&self) -> WorldHarness {
        WorldHarness::new(self.runtime.clone())
    }

    pub fn in_process_client(&self) -> InProcessWorldClient {
        InProcessWorldClient {
            runtime: self.runtime.clone(),
            ownership: self.ownership.clone(),
            ready: self.ready.clone(),
        }
    }

    /// Runs until Ctrl-C or until one of the supervised services exits.
    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        if self.authorization.is_none() {
            return Err(Error::InvalidConfig(
                "standalone World requires an internal token".into(),
            ));
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let signal = tokio::spawn(async move {
            let _ = elura_runtime::lifecycle::shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        });
        let result = self.serve(admin, shutdown_rx).await;
        signal.abort();
        result
    }

    /// Runs Module lifecycle without binding the internal World TCP listener.
    pub async fn serve_in_process(self, shutdown: watch::Receiver<bool>) -> Result<()> {
        self.supervise(shutdown, true).await
    }

    pub async fn serve(
        self,
        admin: AdminServerConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        self.with_admin(admin)?.supervise(shutdown, false).await
    }

    async fn supervise(
        mut self,
        mut external_shutdown: watch::Receiver<bool>,
        in_process: bool,
    ) -> Result<()> {
        let admin = self.admin.take();
        let http = std::mem::take(&mut self.http);
        if admin.is_none() && http.is_empty() {
            return if in_process {
                self.serve_in_process_core(external_shutdown, None).await
            } else {
                self.serve_core(external_shutdown, None).await
            };
        }

        let (core_shutdown_tx, core_shutdown_rx) = watch::channel(false);
        let (http_shutdown_tx, http_shutdown_rx) = watch::channel(false);
        let (admin_shutdown_tx, admin_shutdown_rx) = watch::channel(false);
        let forward_shutdown = core_shutdown_tx.clone();
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
        let world_shutdown = core_shutdown_rx;
        let world_http_shutdown = http_shutdown_tx.clone();
        if in_process {
            tasks.spawn(async move {
                self.serve_in_process_core(world_shutdown, Some(world_http_shutdown))
                    .await
            });
        } else {
            tasks.spawn(async move {
                self.serve_core(world_shutdown, Some(world_http_shutdown))
                    .await
            });
        }
        if let Some(admin) = admin {
            let admin_shutdown = admin_shutdown_rx.clone();
            tasks.spawn(async move { admin.serve(admin_shutdown).await });
        }
        for http in http {
            let http_shutdown = http_shutdown_rx.clone();
            tasks.spawn(async move { http.serve(http_shutdown).await });
        }

        let mut first_error = None;
        while let Some(completed) = tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(error) if first_error.is_none() => {
                    first_error = Some(Error::Internal(format!(
                        "World service task panicked: {error}"
                    )))
                }
                _ => {}
            }
            let _ = core_shutdown_tx.send(true);
            let _ = http_shutdown_tx.send(true);
            let _ = admin_shutdown_tx.send(true);
        }
        forward.abort();
        first_error.map_or(Ok(()), Err)
    }

    async fn serve_in_process_core(
        self,
        mut shutdown: watch::Receiver<bool>,
        http_shutdown: Option<watch::Sender<bool>>,
    ) -> Result<()> {
        if self.registrar.is_some() {
            return Err(Error::InvalidConfig(
                "in-process World cannot publish a network registration".into(),
            ));
        }
        let started = self.start_modules().await?;
        self.ready.store(true, Ordering::Release);
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        self.ready.store(false, Ordering::Release);
        if let Some(http_shutdown) = http_shutdown {
            let _ = http_shutdown.send(true);
        }
        self.stop_modules(started).await
    }

    async fn serve_core(
        self,
        mut shutdown: watch::Receiver<bool>,
        http_shutdown: Option<watch::Sender<bool>>,
    ) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen).await?;
        let started = self.start_modules().await?;
        if let Some(registrar) = &self.registrar
            && let Err(error) = registrar.register().await
        {
            let _ = self.stop_modules(started).await;
            return Err(error);
        }
        self.ready.store(true, Ordering::Release);

        let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
        let (renewal_shutdown_tx, renewal_shutdown_rx) = watch::channel(false);
        let mut serving = Box::pin(self.serve_inner(listener, listener_shutdown_rx));
        let mut renewal = Box::pin(renew_registration_if_configured(
            self.registrar.clone(),
            renewal_shutdown_rx,
        ));
        let mut serve_result = None;
        let mut renewal_result = None;

        tokio::select! {
            result = &mut serving => serve_result = Some(result),
            result = &mut renewal, if self.registrar.is_some() => renewal_result = Some(result),
            _ = wait_for_shutdown(&mut shutdown) => {}
        }

        // Withdraw traffic before closing the listener. Keeping the listener
        // and administration server alive during the delay lets stale
        // discovery snapshots finish routing without creating an outage.
        self.ready.store(false, Ordering::Release);
        let _ = renewal_shutdown_tx.send(true);
        if renewal_result.is_none() {
            renewal_result = Some(if self.registrar.is_some() {
                renewal.await
            } else {
                Ok(())
            });
        }
        let unregister = match &self.registrar {
            Some(registrar) => registrar.unregister().await,
            None => Ok(()),
        };

        if serve_result.is_none() {
            if !self.config.discovery_drain_delay.is_zero() {
                tokio::time::sleep(self.config.discovery_drain_delay).await;
            }
            if let Some(http_shutdown) = http_shutdown {
                let _ = http_shutdown.send(true);
            }
            let _ = listener_shutdown_tx.send(true);
            serve_result = Some(serving.await);
        }

        let stop = self.stop_modules(started).await;
        serve_result
            .expect("World serving result is always collected")
            .and(renewal_result.expect("World renewal result is always collected"))
            .and(unregister)
            .and(stop)
    }

    async fn start_modules(&self) -> Result<usize> {
        for (index, module) in self.modules.iter().enumerate() {
            if let Err(error) = module.start().await {
                let _ = self.stop_modules(index).await;
                return Err(Error::Internal(format!(
                    "start World module {}: {error}",
                    module.name()
                )));
            }
        }
        Ok(self.modules.len())
    }

    async fn stop_modules(&self, count: usize) -> Result<()> {
        let mut first = None;
        for module in self.modules[..count].iter().rev() {
            if let Err(error) = module.stop().await
                && first.is_none()
            {
                first = Some(Error::Internal(format!(
                    "stop World module {}: {error}",
                    module.name()
                )));
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn serve_inner(
        &self,
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let permits = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));
        let mut connections = JoinSet::new();
        info!(address = %self.config.listen, routes = ?self.runtime.routes(), "world listening");
        if *shutdown.borrow() {
            return Ok(());
        }
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        warn!(%error, "world connection task panicked");
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        warn!(%peer, "world connection limit reached");
                        continue;
                    };
                    let runtime = self.runtime.clone();
                    let ownership = self.ownership.clone();
                    let authorization = self.authorization.clone();
                    let tls = self.tls.clone();
                    let handshake_timeout = self.config.tls_handshake_timeout;
                    let max_payload = self.config.max_payload;
                    let max_in_flight = self.config.max_in_flight_per_connection;
                    let connection_shutdown = shutdown.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        let stream: Result<BoxedServiceStream> = match tls {
                            Some(tls) => tokio::time::timeout(handshake_timeout, tls.accept(stream))
                                .await
                                .map_err(|_| Error::Timeout)
                                .and_then(|result| result),
                            None => Ok(Box::new(stream)),
                        };
                        let result = match stream {
                            Ok(stream) => serve_connection(
                                stream,
                                runtime,
                                max_payload,
                                max_in_flight,
                                ownership,
                                authorization,
                                connection_shutdown,
                            ).await,
                            Err(error) => Err(error),
                        };
                        if let Err(error) = result {
                            warn!(%peer, %error, "world connection closed");
                        }
                    });
                }
            }
        }
        drop(listener);
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(self.config.shutdown_timeout, drain)
            .await
            .is_err()
        {
            connections.abort_all();
        }
        Ok(())
    }
}

impl WorldHttpServer {
    async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let listener = TcpListener::bind(self.listen).await?;
        axum::serve(listener, self.router)
            .with_graceful_shutdown(async move {
                if *shutdown.borrow() {
                    return;
                }
                while shutdown.changed().await.is_ok() {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            })
            .await
            .map_err(std::io::Error::other)?;
        Ok(())
    }
}

fn listeners_conflict(left: SocketAddr, right: SocketAddr) -> bool {
    left.port() == right.port()
        && (left.ip() == right.ip() || left.ip().is_unspecified() || right.ip().is_unspecified())
}

async fn renew_registration(
    registrar: Arc<dyn WorldRegistrar>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let interval = registrar.renew_interval();
    if interval.is_zero() {
        return Err(Error::InvalidConfig(
            "World registration renewal interval must be positive".into(),
        ));
    }
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            _ = ticker.tick() => registrar.renew().await?,
        }
    }
}

async fn renew_registration_if_configured(
    registrar: Option<Arc<dyn WorldRegistrar>>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    match registrar {
        Some(registrar) => renew_registration(registrar, shutdown).await,
        None => std::future::pending().await,
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn serve_connection(
    stream: BoxedServiceStream,
    runtime: Arc<WorldRuntime>,
    max_payload: usize,
    max_in_flight: usize,
    ownership: Option<WorldOwnership>,
    authorization: Option<InternalToken>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let framed = Framed::new(stream, FrameCodec::new(max_payload)?);
    let (mut sink, mut source) = framed.split();
    let mut requests = FuturesUnordered::new();
    let mut input_open = !*shutdown.borrow();
    let mut terminal = Ok(());
    loop {
        if !input_open && requests.is_empty() {
            return terminal;
        }
        tokio::select! {
            changed = shutdown.changed(), if input_open => {
                if changed.is_err() || *shutdown.borrow() {
                    input_open = false;
                }
            }
            frame = source.next(), if input_open && requests.len() < max_in_flight => {
                let frame = match frame.transpose()? {
                    Some(frame) => frame,
                    None => {
                        input_open = false;
                        continue;
                    }
                };
                if frame.kind != FrameKind::Request {
                    terminal = Err(Error::InvalidFrame(
                        "world accepts request frames only".into(),
                    ));
                    input_open = false;
                    continue;
                }
                let runtime = runtime.clone();
                let ownership = ownership.clone();
                let authorization = authorization.clone();
                requests.push(async move {
                    AssertUnwindSafe(execute_world_frame(
                        frame,
                        runtime,
                        ownership,
                        authorization,
                    ))
                    .catch_unwind()
                    .await
                    .map_err(|_| Error::Internal("World request task panicked".into()))
                });
            }
            completed = requests.next(), if !requests.is_empty() => {
                match completed {
                    Some(Ok(response)) => sink.send(response).await?,
                    Some(Err(error)) => {
                        terminal = Err(error);
                        input_open = false;
                    }
                    None => {}
                }
            }
        }
    }
}

async fn execute_world_frame(
    frame: Frame,
    runtime: Arc<WorldRuntime>,
    ownership: Option<WorldOwnership>,
    authorization: Option<InternalToken>,
) -> Frame {
    let result = async {
        let command = WorldCommand::try_from(GatewayWorldCommand::decode_frame_payload(
            frame.payload.clone(),
        )?)?;
        if command.request_id == 0 {
            return Err(Error::InvalidFrame(
                "World command request ID is zero".into(),
            ));
        }
        if let Some(expected) = &authorization {
            let candidate = command.authorization.as_deref().unwrap_or_default();
            if !expected.authorizes(candidate) {
                return Err(Error::Authentication);
            }
        }
        command.identity.validate()?;
        validate_ownership(&command, ownership.as_ref()).await?;
        runtime
            .execute(frame.route, command.request_id, command)
            .await
    }
    .await;
    match result {
        Ok(payload) => Frame::response(&frame, payload),
        Err(error) => Frame::error(&frame, error_payload(&error)),
    }
}

async fn validate_ownership(
    command: &WorldCommand,
    ownership: Option<&WorldOwnership>,
) -> Result<()> {
    let Some(owner) = ownership else {
        return Ok(());
    };
    let shard = shard_for(command.identity.user_id, owner.shards)?;
    if command.shard_id != Some(shard)
        || command.owner_id.as_deref() != Some(owner.instance.as_ref())
        || command.owner_epoch.is_none_or(|epoch| epoch == 0)
    {
        return Err(Error::Unavailable);
    }
    let current = owner
        .resolver
        .resolve(command.identity.region_id, command.identity.realm_id, shard)
        .await?;
    if current.region_id != command.identity.region_id
        || current.realm_id != command.identity.realm_id
        || current.shard_id != shard
        || current.world_id != owner.instance.as_ref()
        || Some(current.epoch) != command.owner_epoch
    {
        return Err(Error::Unavailable);
    }
    Ok(())
}

fn error_payload(error: &Error) -> Vec<u8> {
    ErrorEnvelope::from(error).to_bytes()
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use elura_core::Result;
    use elura_core::protocol::FIRST_APPLICATION_ROUTE;
    use elura_core::session::Identity;
    use prost::Message;
    use tokio::sync::Notify;

    use super::*;
    use crate::player::{PlayerLoader, PlayerSnapshot, PlayerStateMiddleware};
    use crate::runtime::WorldBuilder;
    use crate::{
        ContextKey, Next, Route, RouteInfo, TransactionFactory, UnitOfWorkMiddleware, WorldContext,
        WorldMiddleware, WorldModule, WorldModuleRegistry, WorldTransaction,
    };
    use elura_core::session::PlayerKey;

    fn identity(user_id: i64) -> Identity {
        Identity {
            account_id: user_id,
            user_id,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        }
    }

    fn admin_config() -> AdminServerConfig {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        AdminServerConfig::new(listener.local_addr().unwrap(), "world", "world-test")
    }

    fn command(user_id: i64, payload: &[u8]) -> WorldCommand {
        WorldCommand {
            authorization: None,
            identity: identity(user_id),
            session_id: uuid::Uuid::nil().to_string(),
            trace_id: "test".into(),
            request_id: 1,
            payload: Bytes::copy_from_slice(payload),
            shard_id: None,
            owner_id: None,
            owner_epoch: None,
            timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn rejects_reserved_and_duplicate_routes() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        assert!(matches!(
            builder.register_raw(
                FIRST_APPLICATION_ROUTE - 1,
                |_context, payload: Bytes| async move { Ok(payload) },
            ),
            Err(Error::InvalidConfig(_))
        ));

        builder
            .register_raw(
                FIRST_APPLICATION_ROUTE,
                |_context, payload: Bytes| async move { Ok(payload) },
            )
            .unwrap();
        assert!(matches!(
            builder.register_raw(
                FIRST_APPLICATION_ROUTE,
                |_context, payload: Bytes| async move { Ok(payload) },
            ),
            Err(Error::DuplicateRoute(FIRST_APPLICATION_ROUTE))
        ));
    }

    #[tokio::test]
    async fn serializes_players_and_reexecutes_duplicate_requests() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register_raw(100, {
                let active = active.clone();
                let maximum = maximum.clone();
                let calls = calls.clone();
                move |_context, payload: Bytes| {
                    let active = active.clone();
                    let maximum = maximum.clone();
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(payload)
                    }
                }
            })
            .unwrap();
        let server = builder.build().unwrap();
        let first = server.runtime.execute(100, 7, command(9, b"same"));
        let second = server.runtime.execute(100, 7, command(9, b"same"));
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), Bytes::from_static(b"same"));
        assert_eq!(second.unwrap(), Bytes::from_static(b"same"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);

        let _ = tokio::join!(
            server.runtime.execute(100, 8, command(9, b"a")),
            server.runtime.execute(100, 9, command(9, b"b")),
        );
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    #[derive(Clone, PartialEq, Message)]
    struct Echo {
        #[prost(string, tag = "1")]
        value: String,
    }

    struct EchoEndpoint;

    impl Route for EchoEndpoint {
        const ID: u32 = 100;
        const NAME: &'static str = "echo";

        type Request = Echo;
        type Response = Echo;
    }

    #[tokio::test]
    async fn handles_typed_protobuf() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoEndpoint, |_context, request| async move {
                Ok(Echo {
                    value: request.value.to_uppercase(),
                })
            })
            .unwrap();
        let server = builder.build().unwrap();
        assert_eq!(
            server.routes(),
            vec![RouteInfo {
                id: EchoEndpoint::ID,
                name: EchoEndpoint::NAME.into(),
            }]
        );
        let response = server
            .runtime
            .execute(
                100,
                1,
                command(
                    1,
                    &Echo {
                        value: "hello".into(),
                    }
                    .encode_to_vec(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(Echo::decode(response).unwrap().value, "HELLO");
    }

    struct LifecycleModule {
        starts: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
    }

    struct CountingRegistrar {
        registered: Arc<AtomicUsize>,
        renewed: Arc<AtomicUsize>,
        unregistered: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WorldRegistrar for CountingRegistrar {
        fn renew_interval(&self) -> Duration {
            Duration::from_millis(5)
        }

        async fn register(&self) -> Result<()> {
            self.registered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn renew(&self) -> Result<()> {
            self.renewed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn unregister(&self) -> Result<()> {
            self.unregistered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn registers_renews_and_unregisters_world_presence() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap();
        drop(listener);
        let registered = Arc::new(AtomicUsize::new(0));
        let renewed = Arc::new(AtomicUsize::new(0));
        let unregistered = Arc::new(AtomicUsize::new(0));
        let mut builder = WorldBuilder::new(WorldConfig {
            listen,
            discovery_drain_delay: Duration::ZERO,
            ..WorldConfig::default()
        })
        .unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap();
        let server = builder
            .build()
            .unwrap()
            .with_registrar(Arc::new(CountingRegistrar {
                registered: registered.clone(),
                renewed: renewed.clone(),
                unregistered: unregistered.clone(),
            }));
        let diagnostics = server.diagnostics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.serve(admin_config(), shutdown_rx));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !diagnostics.ready() || renewed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(registered.load(Ordering::SeqCst), 1);
        assert!(renewed.load(Ordering::SeqCst) >= 1);
        assert_eq!(unregistered.load(Ordering::SeqCst), 1);
        assert!(!diagnostics.ready());
    }

    struct BlockingRegistrar {
        ready: Arc<AtomicBool>,
        ready_at_unregister: Arc<AtomicBool>,
        unregister_started: Arc<Notify>,
        allow_unregister: Arc<Notify>,
    }

    #[async_trait]
    impl WorldRegistrar for BlockingRegistrar {
        fn renew_interval(&self) -> Duration {
            Duration::from_secs(60)
        }

        async fn register(&self) -> Result<()> {
            Ok(())
        }

        async fn renew(&self) -> Result<()> {
            Ok(())
        }

        async fn unregister(&self) -> Result<()> {
            self.ready_at_unregister
                .store(self.ready.load(Ordering::Acquire), Ordering::Release);
            self.unregister_started.notify_one();
            self.allow_unregister.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn withdraws_readiness_before_unregister_and_keeps_listeners_alive() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap();
        drop(listener);
        let mut builder = WorldBuilder::new(WorldConfig {
            listen,
            discovery_drain_delay: Duration::ZERO,
            ..WorldConfig::default()
        })
        .unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap();

        let server = builder.build().unwrap();
        let diagnostics = server.diagnostics();
        let ready_at_unregister = Arc::new(AtomicBool::new(true));
        let unregister_started = Arc::new(Notify::new());
        let allow_unregister = Arc::new(Notify::new());
        let ready = server.ready.clone();
        let server = server.with_registrar(Arc::new(BlockingRegistrar {
            ready,
            ready_at_unregister: ready_at_unregister.clone(),
            unregister_started: unregister_started.clone(),
            allow_unregister: allow_unregister.clone(),
        }));
        let admin = admin_config();
        let admin_listen = admin.listen;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.serve(admin, shutdown_rx));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !diagnostics.ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), unregister_started.notified())
            .await
            .unwrap();

        assert!(!diagnostics.ready());
        assert!(!ready_at_unregister.load(Ordering::Acquire));
        tokio::net::TcpStream::connect(listen).await.unwrap();
        tokio::net::TcpStream::connect(admin_listen).await.unwrap();

        allow_unregister.notify_one();
        task.await.unwrap().unwrap();
    }

    #[async_trait]
    impl WorldModule for LifecycleModule {
        fn name(&self) -> &str {
            "lifecycle"
        }

        fn register(&self, world: &mut WorldModuleRegistry<'_>) -> Result<()> {
            world.route_raw(100, |_context, payload: Bytes| async move { Ok(payload) })?;
            Ok(())
        }

        async fn start(&self) -> Result<()> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn starts_and_stops_modules() {
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .install(Arc::new(LifecycleModule {
                starts: starts.clone(),
                stops: stops.clone(),
            }))
            .unwrap();
        let server = builder.build().unwrap();
        assert_eq!(server.start_modules().await.unwrap(), 1);
        server.stop_modules(1).await.unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn in_process_client_runs_without_binding_world_tcp() {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut builder = WorldBuilder::new(WorldConfig {
            listen: occupied.local_addr().unwrap(),
            ..WorldConfig::default()
        })
        .unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap();
        let server = builder.build().unwrap();
        let client = server.in_process_client();
        let diagnostics = server.diagnostics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.serve_in_process(shutdown_rx));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !diagnostics.ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let response = client
            .command(WorldRequest {
                identity: identity(7),
                session_id: uuid::Uuid::new_v4(),
                trace_id: "monolith-test".into(),
                route: 100,
                request_id: 1,
                payload: Bytes::from_static(b"direct"),
                ownership: None,
                timeout: Duration::from_secs(5),
            })
            .await
            .unwrap();
        assert_eq!(response, Bytes::from_static(b"direct"));
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert!(!diagnostics.ready());
        drop(occupied);
    }

    struct RecordingMiddleware {
        name: &'static str,
        events: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl WorldMiddleware for RecordingMiddleware {
        async fn handle(
            &self,
            context: WorldContext,
            payload: Bytes,
            next: Next<'_>,
        ) -> Result<Bytes> {
            self.events.lock().unwrap().push(self.name);
            next.run(context, payload).await
        }
    }

    #[tokio::test]
    async fn runs_global_then_route_middleware() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .use_middleware(Arc::new(RecordingMiddleware {
                name: "global",
                events: events.clone(),
            }))
            .unwrap()
            .register(EchoEndpoint, |_context, request| async move { Ok(request) })
            .unwrap()
            .use_route_middleware(
                EchoEndpoint,
                Arc::new(RecordingMiddleware {
                    name: "route",
                    events: events.clone(),
                }),
            )
            .unwrap();
        let server = builder.build().unwrap();
        server
            .runtime
            .execute(
                100,
                1,
                command(1, &Echo { value: "ok".into() }.encode_to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(*events.lock().unwrap(), vec!["global", "route"]);
    }

    struct FailingModule {
        events: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl WorldModule for FailingModule {
        fn name(&self) -> &str {
            "failing"
        }

        fn register(&self, world: &mut WorldModuleRegistry<'_>) -> Result<()> {
            world.route_raw(101, |_context, payload: Bytes| async move { Ok(payload) })?;
            world.middleware(RecordingMiddleware {
                name: "leaked",
                events: self.events.clone(),
            })?;
            Err(Error::Internal("registration failed".into()))
        }
    }

    #[tokio::test]
    async fn failed_module_registration_rolls_back_routes_and_middleware() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap();
        assert!(
            builder
                .install(Arc::new(FailingModule {
                    events: events.clone()
                }))
                .is_err()
        );
        let server = builder.build().unwrap();
        assert_eq!(server.routes().len(), 1);
        server
            .runtime
            .execute(100, 1, command(1, b"ok"))
            .await
            .unwrap();
        assert!(events.lock().unwrap().is_empty());
    }

    struct TestTransaction {
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
        accesses: Arc<AtomicUsize>,
        slow_commit: bool,
    }

    #[async_trait]
    impl WorldTransaction for TestTransaction {
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        async fn commit(&mut self) -> Result<()> {
            if self.slow_commit {
                std::future::pending::<()>().await;
            }
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback(&mut self) -> Result<()> {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct TestTransactions {
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
        accesses: Arc<AtomicUsize>,
        slow_commit: bool,
    }

    #[async_trait]
    impl TransactionFactory for TestTransactions {
        async fn begin(&self, _context: &WorldContext) -> Result<Box<dyn WorldTransaction>> {
            Ok(Box::new(TestTransaction {
                commits: self.commits.clone(),
                rollbacks: self.rollbacks.clone(),
                accesses: self.accesses.clone(),
                slow_commit: self.slow_commit,
            }))
        }
    }

    #[tokio::test]
    async fn transaction_commits_or_rolls_back() {
        let commits = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let accesses = Arc::new(AtomicUsize::new(0));
        let mut builder = WorldBuilder::new(WorldConfig {
            handler_timeout: Duration::from_millis(10),
            ..WorldConfig::default()
        })
        .unwrap();
        builder
            .register_raw(100, |context: WorldContext, payload: Bytes| async move {
                let mut transaction = context.transaction::<TestTransaction>().await?;
                tokio::task::yield_now().await;
                transaction
                    .get_mut()
                    .accesses
                    .fetch_add(1, Ordering::SeqCst);
                drop(transaction);
                if payload == Bytes::from_static(b"timeout") {
                    std::future::pending::<Result<Bytes>>().await
                } else if payload == Bytes::from_static(b"fail") {
                    Err(Error::business("REJECTED", "no"))
                } else {
                    Ok(payload)
                }
            })
            .unwrap()
            .use_middleware(Arc::new(UnitOfWorkMiddleware::new(Arc::new(
                TestTransactions {
                    commits: commits.clone(),
                    rollbacks: rollbacks.clone(),
                    accesses: accesses.clone(),
                    slow_commit: false,
                },
            ))))
            .unwrap();
        let server = builder.build().unwrap();
        server
            .runtime
            .execute(100, 1, command(1, b"ok"))
            .await
            .unwrap();
        assert!(matches!(
            server.runtime.execute(100, 2, command(1, b"fail")).await,
            Err(Error::Business { .. })
        ));
        assert!(matches!(
            server.runtime.execute(100, 3, command(1, b"timeout")).await,
            Err(Error::Timeout)
        ));
        while rollbacks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(rollbacks.load(Ordering::SeqCst), 2);
        assert_eq!(accesses.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancelled_commit_still_rolls_back() {
        let commits = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let accesses = Arc::new(AtomicUsize::new(0));
        let mut builder = WorldBuilder::new(WorldConfig {
            handler_timeout: Duration::from_millis(10),
            ..WorldConfig::default()
        })
        .unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap()
            .use_middleware(Arc::new(UnitOfWorkMiddleware::new(Arc::new(
                TestTransactions {
                    commits: commits.clone(),
                    rollbacks: rollbacks.clone(),
                    accesses,
                    slow_commit: true,
                },
            ))))
            .unwrap();
        let server = builder.build().unwrap();
        assert!(matches!(
            server.runtime.execute(100, 1, command(1, b"ok")).await,
            Err(Error::Timeout)
        ));
        while rollbacks.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(commits.load(Ordering::SeqCst), 0);
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
    }

    struct StaticPlayerLoader;

    #[async_trait]
    impl PlayerLoader<String> for StaticPlayerLoader {
        async fn load(
            &self,
            _context: &WorldContext,
            player: PlayerKey,
        ) -> Result<PlayerSnapshot<String>> {
            Ok(PlayerSnapshot {
                value: format!("player-{}", player.user_id),
                version: 4,
            })
        }
    }

    #[tokio::test]
    async fn injects_player_state_and_reports_routes_and_stats() {
        struct PlayerGet;

        impl Route for PlayerGet {
            const ID: u32 = 100;
            const NAME: &'static str = "player.get";

            type Request = Echo;
            type Response = Echo;
        }

        let key = ContextKey::new("player-state").unwrap();
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .use_middleware(Arc::new(PlayerStateMiddleware::new(
                key.clone(),
                Arc::new(StaticPlayerLoader),
            )))
            .unwrap()
            .register(PlayerGet, move |context: WorldContext, _request| {
                let key = key.clone();
                async move {
                    let snapshot = context.value(&key).unwrap();
                    Ok(Echo {
                        value: snapshot.value.clone(),
                    })
                }
            })
            .unwrap();
        let server = builder.build().unwrap();
        assert_eq!(server.routes()[0].name, "player.get");
        assert_eq!(
            Echo::decode(
                server
                    .runtime
                    .execute(100, 1, command(7, b""))
                    .await
                    .unwrap()
            )
            .unwrap()
            .value,
            "player-7"
        );
        let stats = server.stats();
        assert_eq!(stats.commands, 1);
        assert_eq!(stats.succeeded, 1);
        assert_eq!(stats.active_commands, 0);
    }
}
