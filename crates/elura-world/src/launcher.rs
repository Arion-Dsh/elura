use std::future::Future;
use std::sync::Arc;

use elura_core::push::PushTransport;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

use elura_core::gateway_world::WorldRegistrar;
use elura_runtime::internal::InternalToken;
use elura_runtime::launch::{LaunchAdminConfig, ServerTlsFilesConfig};
use elura_runtime::observability::AdminServer;

use super::{WorldBuilder, WorldConfig, WorldServer};

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorldLaunchConfig {
    pub world: WorldConfig,
    #[serde(skip)]
    pub internal_token: String,
    pub admin: LaunchAdminConfig,
    pub tls: Option<ServerTlsFilesConfig>,
}

impl Default for WorldLaunchConfig {
    fn default() -> Self {
        Self {
            world: WorldConfig::default(),
            internal_token: String::new(),
            admin: LaunchAdminConfig {
                listen: "127.0.0.1:18001".parse().expect("static address"),
                token: None,
                component: "world".into(),
                instance_id: "world-1".into(),
            },
            tls: None,
        }
    }
}

/// Standard World process assembly. Business routes remain explicit through
/// [`WorldLauncher::configure`]; transport, Admin and shutdown are handled once
/// by the runtime.
pub struct WorldLauncher {
    config: WorldLaunchConfig,
    builder: WorldBuilder,
    push: Option<Arc<dyn PushTransport>>,
    registrar: Option<Arc<dyn WorldRegistrar>>,
}

impl WorldLauncher {
    pub fn new(config: WorldLaunchConfig) -> Result<Self> {
        let builder = WorldBuilder::new(config.world.clone())?;
        Ok(Self {
            config,
            builder,
            push: None,
            registrar: None,
        })
    }

    pub fn configure(
        mut self,
        configure: impl FnOnce(&mut WorldBuilder) -> Result<()>,
    ) -> Result<Self> {
        configure(&mut self.builder)?;
        Ok(self)
    }

    /// Installs the transport used by World handlers for cross-Gateway Push.
    pub fn with_push_transport(mut self, transport: Arc<dyn PushTransport>) -> Self {
        self.push = Some(transport);
        self
    }

    pub fn with_registrar(mut self, registrar: Arc<dyn WorldRegistrar>) -> Self {
        self.registrar = Some(registrar);
        self
    }

    pub fn build(self) -> Result<(WorldServer, AdminServer)> {
        let mut builder = self.builder;
        if let Some(push) = self.push {
            builder = builder.with_push_transport(push);
        }
        let mut world = builder
            .build()?
            .with_internal_token(InternalToken::new(self.config.internal_token)?);
        if let Some(registrar) = self.registrar {
            world = world.with_registrar(registrar);
        }
        if let Some(tls) = self.config.tls {
            world = world.with_tls(tls.build()?);
        }
        let admin = AdminServer::new(self.config.admin.into(), world.diagnostics())?;
        Ok((world, admin))
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
        let (world, admin) = self.build()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = tokio::spawn({
            let shutdown_tx = shutdown_tx.clone();
            async move {
                trigger.await;
                let _ = shutdown_tx.send(true);
            }
        });
        let mut tasks = tokio::task::JoinSet::new();
        let world_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { world.serve(world_shutdown).await });
        tasks.spawn(async move { admin.serve(shutdown_rx).await });
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
            let _ = shutdown_tx.send(true);
        }
        signal.abort();
        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> WorldLaunchConfig {
        WorldLaunchConfig {
            internal_token: "t".repeat(32),
            ..WorldLaunchConfig::default()
        }
    }

    #[test]
    fn launcher_builds_with_business_route() {
        WorldLauncher::new(config())
            .unwrap()
            .configure(|builder| {
                builder.register_raw(100, |_context, payload| async move { Ok(payload) })?;
                Ok(())
            })
            .unwrap()
            .build()
            .unwrap();
    }

    #[test]
    fn launcher_requires_business_route() {
        assert!(WorldLauncher::new(config()).unwrap().build().is_err());
    }

    #[test]
    fn json_keeps_secrets_out_of_configuration() {
        let encoded = serde_json::to_string(&config()).unwrap();
        assert!(!encoded.contains(&"t".repeat(32)));
        assert!(!encoded.contains("redis://"));
        let decoded: WorldLaunchConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(decoded.world.listen, WorldConfig::default().listen);
    }
}
