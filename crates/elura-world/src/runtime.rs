use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use elura_core::protocol::FIRST_APPLICATION_ROUTE;
use elura_core::push::PushTransport;
use elura_core::{Error, Result};
use futures_util::FutureExt;
use prost::Message;
use tracing::Instrument;

use super::WorldCommand;
use super::config::WorldConfig;
use super::keyed::KeyedExecutor;
use super::server::WorldServer;
use super::stats::{WorldStats, WorldStatsSnapshot};
use super::{Next, WorldContext, WorldHandler, WorldMiddleware, WorldModule};
use super::{RouteCatalog, RouteManifest, WorldRoute};

pub struct WorldBuilder {
    pub(crate) config: WorldConfig,
    handlers: HashMap<u32, Arc<dyn WorldHandler>>,
    middleware: Vec<Arc<dyn WorldMiddleware>>,
    route_middleware: HashMap<u32, Vec<Arc<dyn WorldMiddleware>>>,
    modules: Vec<Arc<dyn WorldModule>>,
    module_names: HashSet<String>,
    pusher: Option<Arc<dyn PushTransport>>,
    catalog: Option<RouteCatalog>,
}

impl WorldBuilder {
    pub fn new(config: WorldConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            handlers: HashMap::new(),
            middleware: Vec::new(),
            route_middleware: HashMap::new(),
            modules: Vec::new(),
            module_names: HashSet::new(),
            pusher: None,
            catalog: None,
        })
    }

    pub fn with_push_transport(mut self, pusher: Arc<dyn PushTransport>) -> Self {
        self.pusher = Some(pusher);
        self
    }

    pub fn with_route_catalog(mut self, catalog: RouteCatalog) -> Result<Self> {
        if self.handlers.keys().any(|route| !catalog.contains(*route)) {
            return Err(Error::InvalidConfig(
                "registered World route is absent from catalog".into(),
            ));
        }
        self.catalog = Some(catalog);
        Ok(self)
    }

    pub fn register(&mut self, route: u32, handler: impl WorldHandler) -> Result<&mut Self> {
        if route < FIRST_APPLICATION_ROUTE {
            return Err(Error::InvalidConfig(format!("route {route} is reserved")));
        }
        if self
            .catalog
            .as_ref()
            .is_some_and(|catalog| !catalog.contains(route))
        {
            return Err(Error::InvalidConfig(format!(
                "route {route} is absent from World catalog"
            )));
        }
        match self.handlers.entry(route) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(handler));
            }
            Entry::Occupied(_) => return Err(Error::DuplicateRoute(route)),
        }
        Ok(self)
    }

    /// Registers a typed Protobuf request handler for an application route.
    pub fn register_handler<Request, Response, F, Fut>(
        &mut self,
        route: u32,
        handler: F,
    ) -> Result<&mut Self>
    where
        Request: Message + Default + Send + 'static,
        Response: Message + Send + 'static,
        F: Fn(WorldContext, Request) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Response>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register(route, move |context, payload: Bytes| {
            let handler = handler.clone();
            async move {
                let request = Request::decode(payload)
                    .map_err(|_| Error::business("INVALID_PAYLOAD", "invalid Protobuf payload"))?;
                Ok(Bytes::from(
                    handler(context, request).await?.encode_to_vec(),
                ))
            }
        })
    }

    pub fn use_middleware(&mut self, middleware: Arc<dyn WorldMiddleware>) -> Result<&mut Self> {
        self.middleware.push(middleware);
        Ok(self)
    }

    pub fn use_route_middleware(
        &mut self,
        route: u32,
        middleware: Arc<dyn WorldMiddleware>,
    ) -> Result<&mut Self> {
        if !self.handlers.contains_key(&route) {
            return Err(Error::RouteNotFound(route));
        }
        self.route_middleware
            .entry(route)
            .or_default()
            .push(middleware);
        Ok(self)
    }

    pub fn register_with_middleware(
        &mut self,
        route: u32,
        handler: impl WorldHandler,
        middleware: impl IntoIterator<Item = Arc<dyn WorldMiddleware>>,
    ) -> Result<&mut Self> {
        self.register(route, handler)?;
        self.route_middleware
            .entry(route)
            .or_default()
            .extend(middleware);
        Ok(self)
    }

    pub fn install(&mut self, module: Arc<dyn WorldModule>) -> Result<&mut Self> {
        let name = module.name().trim();
        if name.is_empty() {
            return Err(Error::InvalidConfig("world module name is empty".into()));
        }
        if self.module_names.contains(name) {
            return Err(Error::InvalidConfig(format!(
                "duplicate world module {name}"
            )));
        }
        let handlers = self.handlers.clone();
        let middleware_count = self.middleware.len();
        let route_middleware = self.route_middleware.clone();
        if let Err(error) = module.register(self) {
            self.handlers = handlers;
            self.middleware.truncate(middleware_count);
            self.route_middleware = route_middleware;
            return Err(error);
        }
        self.module_names.insert(name.to_owned());
        self.modules.push(module);
        Ok(self)
    }

    pub fn build(self) -> Result<WorldServer> {
        let handlers = self.handlers;
        let mut routes = handlers.keys().copied().collect::<Vec<_>>();
        routes.sort_unstable();
        if routes.is_empty() {
            return Err(Error::InvalidConfig(
                "world requires at least one route".into(),
            ));
        }
        let manifest = match &self.catalog {
            Some(catalog) => {
                let manifest = catalog.routes();
                if manifest.len() != routes.len()
                    || manifest.iter().any(|entry| !routes.contains(&entry.id))
                {
                    return Err(Error::InvalidConfig(
                        "World route catalog and handlers are incomplete".into(),
                    ));
                }
                manifest
            }
            None => routes
                .iter()
                .map(|id| WorldRoute {
                    id: *id,
                    name: String::new(),
                })
                .collect(),
        };
        let middleware_by_route = routes
            .iter()
            .map(|route| {
                let mut chain = self.middleware.clone();
                if let Some(route_middleware) = self.route_middleware.get(route) {
                    chain.extend(route_middleware.iter().cloned());
                }
                (*route, Arc::<[Arc<dyn WorldMiddleware>]>::from(chain))
            })
            .collect();
        let runtime = Arc::new(WorldRuntime {
            handlers,
            middleware_by_route,
            keyed: KeyedExecutor::default(),
            pusher: self.pusher,
            handler_timeout: self.config.handler_timeout,
            routes,
            manifest,
            stats: Arc::new(WorldStats::default()),
        });
        Ok(WorldServer::from_parts(self.config, runtime, self.modules))
    }
}

pub(crate) struct WorldRuntime {
    handlers: HashMap<u32, Arc<dyn WorldHandler>>,
    middleware_by_route: HashMap<u32, Arc<[Arc<dyn WorldMiddleware>]>>,
    keyed: KeyedExecutor,
    pusher: Option<Arc<dyn PushTransport>>,
    handler_timeout: std::time::Duration,
    routes: Vec<u32>,
    manifest: RouteManifest,
    stats: Arc<WorldStats>,
}

impl WorldRuntime {
    pub fn routes(&self) -> &[u32] {
        &self.routes
    }

    pub fn route_manifest(&self) -> RouteManifest {
        self.manifest.clone()
    }

    pub fn stats(&self) -> WorldStatsSnapshot {
        self.stats.snapshot()
    }

    pub async fn execute(
        &self,
        route: u32,
        request_id: u64,
        command: WorldCommand,
    ) -> Result<Bytes> {
        let started = self.stats.begin();
        let span = tracing::info_span!(
            "world.command",
            trace_id = %command.trace_id,
            route,
            request_id,
            user_id = command.identity.user_id,
            region_id = command.identity.region_id,
            realm_id = command.identity.realm_id,
        );
        let result = self
            .execute_inner(route, request_id, command)
            .instrument(span)
            .await;
        self.stats.finish(started, &result);
        result
    }

    async fn execute_inner(
        &self,
        route: u32,
        request_id: u64,
        command: WorldCommand,
    ) -> Result<Bytes> {
        let session_id = uuid::Uuid::parse_str(&command.session_id)
            .map_err(|_| Error::InvalidFrame("invalid World session ID".into()))?;
        let payload = command.payload;
        let timeout = command.timeout.min(self.handler_timeout);
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let context = WorldContext {
            identity: command.identity,
            session_id,
            trace_id: command.trace_id,
            route,
            request_id,
            shard_id: command.shard_id,
            owner_id: command.owner_id,
            owner_epoch: command.owner_epoch,
            pusher: None,
            transaction: None,
            state: Arc::new(HashMap::new()),
        }
        .with_pusher(self.pusher.clone());
        let player = context.identity.player_key();
        self.keyed
            .execute(player, self.execute_serial(context, payload, timeout))
            .await
    }

    async fn execute_serial(
        &self,
        context: WorldContext,
        payload: Bytes,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        let handler = self
            .handlers
            .get(&context.route)
            .ok_or(Error::RouteNotFound(context.route))?
            .clone();
        let middleware = self
            .middleware_by_route
            .get(&context.route)
            .ok_or(Error::RouteNotFound(context.route))?;
        let execution = Next {
            middleware,
            handler,
        }
        .run(context, payload);
        let response = tokio::time::timeout(timeout, AssertUnwindSafe(execution).catch_unwind())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| {
                self.stats.panic();
                Error::Internal("world handler panicked".into())
            })??;
        Ok(response)
    }
}
