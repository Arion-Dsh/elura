//! Single-process Gateway and World assembly.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]

use std::future::Future;
use std::sync::Arc;

use elura_core::{Error, Result};
use elura_gateway::observability::{
    AdminDiagnostics, AdminServer, AdmissionAdmin, GatewayAdmin, Readiness,
};
use elura_gateway::transport::GatewayTransport;
use elura_gateway::{Gateway, GatewayConfig, GatewayServer};
use elura_runtime::observability::{AdminDiagnostics as WorldAdminDiagnostics, AdminServerConfig};
use elura_world::{
    Route, World, WorldConfig, WorldContext, WorldDiagnostics, WorldHandler, WorldMiddleware,
    WorldModule, WorldServer,
};
use tokio::task::JoinSet;

/// Application-facing single-process Gateway and World.
///
/// Client protocols are explicit: install at least one [`GatewayTransport`]
/// before building or running the Monolith.
pub struct Monolith {
    gateway: Gateway,
    world: World,
    admission_admin: Option<Arc<dyn AdmissionAdmin>>,
}

impl Monolith {
    /// Creates a Monolith from the same configurations used by standalone
    /// Gateway and World processes.
    ///
    /// Standalone World networking, authorization and TLS settings are not started.
    pub fn new(mut gateway: GatewayConfig, mut world: WorldConfig) -> Self {
        gateway.internal_token = None;
        gateway.world_tls = None;

        world.internal_token = None;
        world.tls = None;

        Self {
            gateway: Gateway::new(gateway),
            world: World::new(world),
            admission_admin: None,
        }
    }

    /// Adds a client transport endpoint supervised with the Monolith lifecycle.
    pub fn transport<T>(mut self, transport: T) -> Self
    where
        T: GatewayTransport,
    {
        self.gateway = self.gateway.transport(transport);
        self
    }

    /// Applies advanced Gateway assembly without exposing an internal builder.
    pub fn gateway(mut self, configure: impl FnOnce(Gateway) -> Gateway) -> Self {
        self.gateway = configure(self.gateway);
        self
    }

    /// Applies advanced World assembly without exposing an internal builder.
    pub fn world(mut self, configure: impl FnOnce(World) -> World) -> Self {
        self.world = configure(self.world);
        self
    }

    /// Registers a typed World route and handler.
    pub fn route<E, F, Fut>(mut self, route: E, handler: F) -> Self
    where
        E: Route,
        F: Fn(WorldContext, E::Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Response>> + Send + 'static,
    {
        self.world = self.world.route(route, handler);
        self
    }

    /// Registers a low-level byte route for protocol tools and non-Protobuf integrations.
    pub fn route_raw(mut self, route: u32, handler: impl WorldHandler) -> Self {
        self.world = self.world.route_raw(route, handler);
        self
    }

    /// Adds middleware applied to every World route.
    pub fn middleware<M>(mut self, middleware: M) -> Self
    where
        M: WorldMiddleware,
    {
        self.world = self.world.middleware(middleware);
        self
    }

    /// Adds middleware to one typed World route.
    pub fn route_middleware<E, M>(mut self, route: E, middleware: M) -> Self
    where
        E: Route,
        M: WorldMiddleware,
    {
        self.world = self.world.route_middleware(route, middleware);
        self
    }

    /// Adds middleware to a raw route ID.
    pub fn route_middleware_raw<M>(mut self, route: u32, middleware: M) -> Self
    where
        M: WorldMiddleware,
    {
        self.world = self.world.route_middleware_raw(route, middleware);
        self
    }

    /// Installs a reusable World module.
    pub fn install<M>(mut self, module: M) -> Self
    where
        M: WorldModule,
    {
        self.world = self.world.install(module);
        self
    }

    /// Adds admission-policy mutations to the combined administration API.
    pub fn admission_admin(mut self, admin: Arc<dyn AdmissionAdmin>) -> Self {
        self.gateway = self.gateway.admission_admin(admin.clone());
        self.admission_admin = Some(admin);
        self
    }

    /// Validates and assembles the in-process Gateway and World runtime.
    pub fn build(self) -> Result<MonolithServer> {
        let world = self.world.build()?;
        let client = Arc::new(world.in_process_client());
        let world_diagnostics = world.diagnostics();
        let gateway = Arc::new(self.gateway.world_client(client).build()?);

        Ok(MonolithServer {
            gateway,
            admission_admin: self.admission_admin,
            world,
            world_diagnostics,
        })
    }

    /// Builds and runs the Monolith until shutdown.
    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        self.build()?.run(admin).await
    }
}

/// Advanced, fully assembled Monolith runtime.
pub struct MonolithServer {
    gateway: Arc<GatewayServer>,
    admission_admin: Option<Arc<dyn AdmissionAdmin>>,
    world: WorldServer,
    world_diagnostics: Arc<WorldDiagnostics>,
}

impl MonolithServer {
    /// Returns the running Gateway handle used for push and session control.
    pub fn gateway(&self) -> Arc<GatewayServer> {
        self.gateway.clone()
    }

    /// Returns the World diagnostics handle.
    pub fn world_diagnostics(&self) -> Arc<WorldDiagnostics> {
        self.world_diagnostics.clone()
    }

    /// Runs until Ctrl-C or until one of the supervised services exits.
    pub async fn run(self, admin: AdminServerConfig) -> Result<()> {
        self.run_with_trigger(admin, async {
            let _ = elura_runtime::lifecycle::shutdown_signal().await;
        })
        .await
    }

    /// Serves until an embedding application closes or sets the supplied signal.
    pub async fn serve(
        self,
        admin: AdminServerConfig,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        self.run_with_trigger(admin, async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
    }

    async fn run_with_trigger(
        self,
        admin: AdminServerConfig,
        trigger: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = tokio::spawn({
            let shutdown_tx = shutdown_tx.clone();
            async move {
                trigger.await;
                let _ = shutdown_tx.send(true);
            }
        });

        let mut tasks = JoinSet::new();
        let mut gateway_admin = GatewayAdmin::new(self.gateway.clone());
        if let Some(admission_admin) = self.admission_admin {
            gateway_admin = gateway_admin.with_admission(admission_admin);
        }
        let diagnostics = Arc::new(MonolithDiagnostics {
            gateway: self.gateway.clone(),
            world: self.world_diagnostics.clone(),
        });
        let admin = AdminServer::new(admin, diagnostics)?.with_gateway_admin(gateway_admin);
        let gateway_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { self.gateway.serve_embedded(gateway_shutdown).await });
        let admin_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { admin.serve(admin_shutdown).await });
        tasks.spawn(async move { self.world.serve_in_process(shutdown_rx).await });

        let mut first_error = None;
        while let Some(completed) = tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(error) if first_error.is_none() => {
                    first_error = Some(Error::Internal(format!(
                        "monolith service task panicked: {error}"
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

struct MonolithDiagnostics {
    gateway: Arc<GatewayServer>,
    world: Arc<WorldDiagnostics>,
}

#[async_trait::async_trait]
impl AdminDiagnostics for MonolithDiagnostics {
    async fn readiness(&self) -> Readiness {
        AdminDiagnostics::readiness(self.gateway.as_ref()).await
    }

    async fn prometheus(&self) -> String {
        let mut output = AdminDiagnostics::prometheus(self.gateway.as_ref()).await;
        if !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(&WorldAdminDiagnostics::prometheus(self.world.as_ref()).await);
        output
    }

    async fn stats(&self) -> serde_json::Value {
        serde_json::json!({
            "gateway": AdminDiagnostics::stats(self.gateway.as_ref()).await,
            "world": WorldAdminDiagnostics::stats(self.world.as_ref()).await,
        })
    }

    async fn backend(&self) -> Option<serde_json::Value> {
        AdminDiagnostics::backend(self.gateway.as_ref()).await
    }

    async fn routes(&self) -> Option<serde_json::Value> {
        WorldAdminDiagnostics::routes(self.world.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use elura_gateway::transport::{TcpConfig, TcpTransport};

    fn gateway_config() -> GatewayConfig {
        let mut config = GatewayConfig::default();
        config.ticket.key = "k".repeat(32);
        config
    }

    #[test]
    fn builds_without_discovery_or_internal_token() {
        Monolith::new(gateway_config(), WorldConfig::default())
            .transport(TcpTransport::new(TcpConfig::default()).unwrap())
            .route_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .build()
            .unwrap();
    }
}
