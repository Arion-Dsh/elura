use std::sync::Arc;

use crate::registration::WorldRegistrar;
use axum::Router;
use elura_core::ownership::OwnershipResolver;
use elura_core::push::PushTransport;
use elura_core::{Error, Result};
use elura_runtime::observability::AdminServerConfig;

use super::runtime::WorldBuilder;
use super::{
    Route, WorldConfig, WorldContext, WorldHandler, WorldMiddleware, WorldModule, WorldServer,
};

/// Application-facing World assembly and startup API.
///
/// Registration is intentionally infallible at each call site. Configuration,
/// duplicate route and module errors are retained and returned by [`Self::build`]
/// or [`Self::run`].
pub struct World {
    builder: Option<WorldBuilder>,
    error: Option<Error>,
    registrar: Option<Arc<dyn WorldRegistrar>>,
    ownership: Option<(Arc<str>, u32, Arc<dyn OwnershipResolver>)>,
    http: Vec<(String, Router)>,
}

impl World {
    pub fn new(config: WorldConfig) -> Self {
        match WorldBuilder::new(config) {
            Ok(builder) => Self {
                builder: Some(builder),
                error: None,
                registrar: None,
                ownership: None,
                http: Vec::new(),
            },
            Err(error) => Self {
                builder: None,
                error: Some(error),
                registrar: None,
                ownership: None,
                http: Vec::new(),
            },
        }
    }

    pub fn route<E, F, Fut>(mut self, route: E, handler: F) -> Self
    where
        E: Route,
        F: Fn(WorldContext, E::Request) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::Response>> + Send + 'static,
    {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.register(route, handler)
        {
            self.record(error);
        }
        self
    }

    /// Registers a low-level byte route for protocol tools and non-Protobuf integrations.
    pub fn route_raw(mut self, route: u32, handler: impl WorldHandler) -> Self {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.register_raw(route, handler)
        {
            self.record(error);
        }
        self
    }

    pub fn middleware<M>(mut self, middleware: M) -> Self
    where
        M: WorldMiddleware,
    {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.use_middleware(Arc::new(middleware))
        {
            self.record(error);
        }
        self
    }

    pub fn route_middleware<E, M>(mut self, route: E, middleware: M) -> Self
    where
        E: Route,
        M: WorldMiddleware,
    {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.use_route_middleware(route, Arc::new(middleware))
        {
            self.record(error);
        }
        self
    }

    /// Adds middleware to a raw route ID.
    pub fn route_middleware_raw<M>(mut self, route: u32, middleware: M) -> Self
    where
        M: WorldMiddleware,
    {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.use_route_middleware_raw(route, Arc::new(middleware))
        {
            self.record(error);
        }
        self
    }

    pub fn install<M>(mut self, module: M) -> Self
    where
        M: WorldModule,
    {
        if let Some(builder) = self.builder.as_mut()
            && let Err(error) = builder.install(Arc::new(module))
        {
            self.record(error);
        }
        self
    }

    /// Installs the transport used by handlers for cross-Gateway Push.
    pub fn push_transport(mut self, transport: Arc<dyn PushTransport>) -> Self {
        if let Some(builder) = self.builder.take() {
            self.builder = Some(builder.with_push_transport(transport));
        }
        self
    }

    /// Installs distributed World service registration.
    pub fn registrar(mut self, registrar: Arc<dyn WorldRegistrar>) -> Self {
        self.registrar = Some(registrar);
        self
    }

    /// Installs the shard-ownership resolver used to validate routed commands.
    pub fn ownership(
        mut self,
        instance: impl Into<Arc<str>>,
        shards: u32,
        resolver: Arc<dyn OwnershipResolver>,
    ) -> Self {
        self.ownership = Some((instance.into(), shards, resolver));
        self
    }

    /// Adds an application HTTP server supervised with the World lifecycle.
    pub fn http(mut self, listen: impl std::fmt::Display, router: Router) -> Self {
        self.http.push((listen.to_string(), router));
        self
    }

    pub fn build(mut self) -> Result<WorldServer> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        let mut server = self
            .builder
            .take()
            .ok_or_else(|| Error::Internal("World builder is unavailable".into()))?
            .build()?
            .configure_process()?;
        if let Some(registrar) = self.registrar {
            server = server.with_registrar(registrar);
        }
        if let Some((instance, shards, resolver)) = self.ownership {
            server = server.with_ownership(instance, shards, resolver)?;
        }
        for (listen, router) in self.http {
            server.add_http(listen, router)?;
        }
        server.validate_listeners()?;
        Ok(server)
    }

    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        self.build()?.run(admin).await
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
    use std::time::Duration;

    use axum::routing::get;
    use bytes::Bytes;
    use prost::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::{Next, WorldModuleRegistry};

    #[derive(Clone, PartialEq, Message)]
    struct Echo {
        #[prost(string, tag = "1")]
        value: String,
    }

    struct EchoRoute;

    impl Route for EchoRoute {
        const ID: u32 = 100;
        const NAME: &'static str = "test.echo";

        type Request = Echo;
        type Response = Echo;
    }

    struct OtherRoute;

    impl Route for OtherRoute {
        const ID: u32 = 101;
        const NAME: &'static str = "test.other";

        type Request = Echo;
        type Response = Echo;
    }

    struct Pass;

    #[async_trait::async_trait]
    impl WorldMiddleware for Pass {
        async fn handle(
            &self,
            context: WorldContext,
            payload: Bytes,
            next: Next<'_>,
        ) -> Result<Bytes> {
            next.run(context, payload).await
        }
    }

    struct EchoModule;

    #[async_trait::async_trait]
    impl WorldModule for EchoModule {
        fn name(&self) -> &str {
            "echo"
        }

        fn register(&self, world: &mut WorldModuleRegistry<'_>) -> Result<()> {
            world.route(EchoRoute, |_context, request| async move { Ok(request) })?;
            Ok(())
        }
    }

    fn config() -> WorldConfig {
        WorldConfig::default()
    }

    fn free_address() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    fn admin_config() -> AdminServerConfig {
        AdminServerConfig::new(free_address(), "world", "world-test")
    }

    #[test]
    fn builds_routes_modules_and_order_independent_route_middleware() {
        let server = World::new(config())
            .route_middleware(EchoRoute, Pass)
            .install(EchoModule)
            .build()
            .unwrap();
        assert_eq!(server.routes()[0].name, EchoRoute::NAME);
    }

    #[test]
    fn returns_deferred_registration_errors_from_build() {
        let duplicate = World::new(config())
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .build();
        assert!(matches!(duplicate, Err(Error::DuplicateRoute(100))));

        let missing_route = World::new(config())
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .route_middleware(OtherRoute, Pass)
            .build();
        assert!(matches!(missing_route, Err(Error::RouteNotFound(101))));
    }

    #[test]
    fn rejects_conflicting_world_and_http_listeners() {
        let address = free_address();
        let mut config = config();
        config.listen = address;
        let result = World::new(config)
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .http(address, Router::new())
            .build();
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn standalone_run_requires_internal_authentication() {
        let result = World::new(config())
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .run(admin_config())
            .await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn supervises_application_http_with_the_world_server() {
        let world_address = free_address();
        let http_address = free_address();
        let mut config = config();
        config.listen = world_address;
        let server = World::new(config)
            .route(EchoRoute, |_context, request| async move { Ok(request) })
            .http(
                http_address,
                Router::new().route("/hello", get(|| async { "world" })),
            )
            .build()
            .unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(server.serve(admin_config(), shutdown_rx));

        let mut stream = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::TcpStream::connect(http_address).await {
                    Ok(stream) => break stream,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        stream
            .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8(response).unwrap().contains("world"));

        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }
}
