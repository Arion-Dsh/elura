//! Single-process Gateway and World assembly.

#![deny(rustdoc::broken_intra_doc_links)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use elura_core::ticket::ReplayStore;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use elura_gateway::observability::{AdminDiagnostics, AdmissionAdmin, Readiness};
use elura_gateway::protection::ProtectionConfig;
use elura_gateway::transport::{AdmissionController, AdmissionSettings, QuicConfig};
use elura_gateway::{
    GatewayExtension, GatewayLaunchConfig, GatewayLauncher, GatewayProxyProtocolLaunchConfig,
    GatewayRealmAdmissionConfig, GatewayTicketConfig,
};
use elura_runtime::launch::{LaunchAdminConfig, ServerTlsFilesConfig};
use elura_runtime::observability::AdminDiagnostics as WorldAdminDiagnostics;
use elura_world::{WorldBuilder, WorldConfig, WorldServer};

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonolithLaunchConfig {
    pub gateway: elura_gateway::GatewayConfig,
    pub world: MonolithWorldConfig,
    pub ticket: GatewayTicketConfig,
    pub admin: LaunchAdminConfig,
    pub protection: Option<ProtectionConfig>,
    pub tls: Option<ServerTlsFilesConfig>,
    pub quic: Option<QuicConfig>,
    pub proxy_protocol: Option<GatewayProxyProtocolLaunchConfig>,
    pub realm_admission: Option<GatewayRealmAdmissionConfig>,
}

impl Default for MonolithLaunchConfig {
    fn default() -> Self {
        Self {
            gateway: elura_gateway::GatewayConfig::default(),
            world: MonolithWorldConfig::default(),
            ticket: GatewayTicketConfig::default(),
            admin: LaunchAdminConfig {
                listen: "127.0.0.1:17001".parse().expect("static address"),
                token: None,
                component: "monolith".into(),
                instance_id: "monolith-1".into(),
            },
            protection: None,
            tls: None,
            quic: None,
            proxy_protocol: None,
            realm_admission: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonolithWorldConfig {
    pub handler_timeout: Duration,
}

impl Default for MonolithWorldConfig {
    fn default() -> Self {
        let world = WorldConfig::default();
        Self {
            handler_timeout: world.handler_timeout,
        }
    }
}

impl MonolithWorldConfig {
    fn runtime_config(&self) -> WorldConfig {
        WorldConfig {
            handler_timeout: self.handler_timeout,
            ..WorldConfig::default()
        }
    }
}

impl MonolithLaunchConfig {
    fn gateway_launch_config(&self) -> GatewayLaunchConfig {
        GatewayLaunchConfig {
            gateway: self.gateway.clone(),
            ticket: self.ticket.clone(),
            internal_token: String::new(),
            admin: self.admin.clone(),
            protection: self.protection.clone(),
            tls: self.tls.clone(),
            world_tls: None,
            world_routing: Default::default(),
            quic: self.quic.clone(),
            proxy_protocol: self.proxy_protocol.clone(),
            realm_admission: self.realm_admission.clone(),
        }
    }
}

/// Runs Gateway and World in one process with direct in-memory dispatch.
pub struct MonolithLauncher {
    gateway: GatewayLauncher,
    world: WorldBuilder,
}

impl MonolithLauncher {
    pub fn new(config: MonolithLaunchConfig) -> Result<Self> {
        Ok(Self {
            gateway: GatewayLauncher::new(config.gateway_launch_config())?,
            world: WorldBuilder::new(config.world.runtime_config())?,
        })
    }

    pub fn configure_world(
        mut self,
        configure: impl FnOnce(&mut WorldBuilder) -> Result<()>,
    ) -> Result<Self> {
        configure(&mut self.world)?;
        Ok(self)
    }

    pub fn with_replay_store(mut self, replay: Arc<dyn ReplayStore>) -> Self {
        self.gateway = self.gateway.with_replay_store(replay);
        self
    }

    pub fn with_admission(
        mut self,
        controller: Arc<dyn AdmissionController>,
        settings: AdmissionSettings,
    ) -> Self {
        self.gateway = self.gateway.with_admission(controller, settings);
        self
    }

    pub fn with_gateway_extension(mut self, extension: impl GatewayExtension) -> Self {
        self.gateway = self.gateway.with_extension(extension);
        self
    }

    pub fn with_admission_admin(mut self, admin: Arc<dyn AdmissionAdmin>) -> Self {
        self.gateway = self.gateway.with_admission_admin(admin);
        self
    }

    fn build(self) -> Result<MonolithParts> {
        let world = self.world.build()?;
        let client = Arc::new(world.in_process_client());
        let world_diagnostics = world.diagnostics();
        let mut gateway = self.gateway.with_world_client(client).build_parts()?;
        debug_assert!(gateway.discovery.is_none());
        gateway.admin = gateway
            .admin
            .with_diagnostics(Arc::new(MonolithDiagnostics {
                gateway: gateway.gateway.clone(),
                world: world_diagnostics,
            }));
        Ok(MonolithParts { gateway, world })
    }

    pub async fn run(self) -> Result<()> {
        self.run_with_trigger(async {
            let _ = elura_runtime::lifecycle::shutdown_signal().await;
        })
        .await
    }

    /// Runs until an embedding application closes or sets the supplied signal.
    pub async fn run_until(self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        self.run_with_trigger(async move {
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
        trigger: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let parts = self.build()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = tokio::spawn({
            let shutdown_tx = shutdown_tx.clone();
            async move {
                trigger.await;
                let _ = shutdown_tx.send(true);
            }
        });

        let mut tasks = JoinSet::new();
        let gateway_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { parts.gateway.gateway.serve_tcp(gateway_shutdown).await });
        let admin_shutdown = shutdown_rx.clone();
        tasks.spawn(async move { parts.gateway.admin.serve(admin_shutdown).await });
        tasks.spawn(async move { parts.world.serve_in_process(shutdown_rx).await });

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

struct MonolithParts {
    gateway: elura_gateway::GatewayParts,
    world: WorldServer,
}

struct MonolithDiagnostics {
    gateway: Arc<elura_gateway::Gateway>,
    world: Arc<elura_world::WorldDiagnostics>,
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

    fn config() -> MonolithLaunchConfig {
        let mut config = MonolithLaunchConfig::default();
        config.ticket.key = "k".repeat(32);
        config
    }

    #[test]
    fn builds_without_discovery_or_internal_token() {
        MonolithLauncher::new(config())
            .unwrap()
            .configure_world(|world| {
                world.register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })?;
                Ok(())
            })
            .unwrap()
            .build()
            .unwrap();
    }

    #[test]
    fn json_rejects_discovery_configuration() {
        assert!(serde_json::from_str::<MonolithLaunchConfig>(r#"{"world_discovery":{}}"#).is_err());
    }
}
