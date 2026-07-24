use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::Result;
use elura_core::ownership::{Assignment, OwnershipResolver};
use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTransport};
use elura_core::replay_protection::ReplayProtectionStore;
use elura_core::session::Identity;
use elura_gateway::discovery::{WorldClient, WorldDiscovery, WorldRequest, WorldRouteUpdater};
use elura_gateway::observability::AdmissionAdmin;
use elura_gateway::presence::{
    DuplicateLoginMode, OnlineAdmission, OnlineAdmissionPolicy, OnlineDirectory, SessionLease,
};
use elura_gateway::session::{
    AccountVersionKey, AccountVersionStore, MutableAccountVersionStore, SessionControlEvent,
    SessionControlHandler, SessionControlTransport,
};
use elura_gateway::transport::{
    AccountVersionSettings, AdmissionController, AdmissionDecision, AdmissionRequest,
    AdmissionSettings,
};
use elura_gateway::{
    Gateway, GatewayConfig, GatewayInterceptContext, GatewayInterceptor, GatewayNext,
    GatewayRequest, GatewayResponse,
};
use elura_runtime::observability::AdminServerConfig;
use tokio::sync::watch;
use uuid::Uuid;

struct ApplicationReplayProtectionStore;

#[allow(dead_code)]
async fn run_gateway(gateway: Gateway, admin: AdminServerConfig) -> Result<()> {
    gateway.run(admin).await
}

#[async_trait]
impl ReplayProtectionStore for ApplicationReplayProtectionStore {
    async fn reserve(&self, _ticket_id: &str, _expires_at: u64) -> Result<bool> {
        Ok(true)
    }
}

struct ApplicationAccountVersionStore;

#[async_trait]
impl AccountVersionStore for ApplicationAccountVersionStore {
    async fn current(&self, _key: AccountVersionKey) -> Result<Option<u64>> {
        Ok(Some(1))
    }
}

#[async_trait]
impl MutableAccountVersionStore for ApplicationAccountVersionStore {
    async fn set(&self, _key: AccountVersionKey, _version: u64) -> Result<()> {
        Ok(())
    }

    async fn increment(&self, _key: AccountVersionKey) -> Result<u64> {
        Ok(1)
    }
}

struct ApplicationOwnershipResolver;

#[async_trait]
impl OwnershipResolver for ApplicationOwnershipResolver {
    async fn resolve(&self, region_id: u32, realm_id: u32, shard: u32) -> Result<Assignment> {
        Ok(Assignment {
            region_id,
            realm_id,
            shard_id: shard,
            world_id: "world-1".into(),
            epoch: 1,
        })
    }
}

struct ApplicationOnlineDirectory;

#[async_trait]
impl OnlineDirectory for ApplicationOnlineDirectory {
    async fn acquire(
        &self,
        _lease: SessionLease,
        _policy: OnlineAdmissionPolicy,
    ) -> Result<OnlineAdmission> {
        Ok(OnlineAdmission::Accepted {
            previous_session: None,
        })
    }

    async fn renew(&self, _lease: SessionLease) -> Result<()> {
        Ok(())
    }

    async fn unregister(&self, _lease: &SessionLease) -> Result<()> {
        Ok(())
    }

    async fn session(&self, _session_id: Uuid) -> Result<Option<SessionLease>> {
        Ok(None)
    }

    async fn user_sessions(
        &self,
        _region_id: u32,
        _realm_id: u32,
        _user_id: i64,
    ) -> Result<Vec<SessionLease>> {
        Ok(Vec::new())
    }

    async fn group_sessions(&self, _group: &str) -> Result<Vec<SessionLease>> {
        Ok(Vec::new())
    }

    async fn track_group(&self, _session_id: Uuid, _group: &str, _join: bool) -> Result<()> {
        Ok(())
    }
}

struct ApplicationPushTransport;

#[async_trait]
impl PushTransport for ApplicationPushTransport {
    async fn publish(&self, request: &PushRequest) -> Result<PushReceipt> {
        Ok(PushReceipt::accepted(request, 0))
    }

    async fn subscribe(
        &self,
        _handler: Arc<dyn PushHandler>,
        _shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        Ok(())
    }
}

struct ApplicationSessionControlTransport;

#[async_trait]
impl SessionControlTransport for ApplicationSessionControlTransport {
    async fn publish(&self, _event: &SessionControlEvent) -> Result<()> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _handler: Arc<dyn SessionControlHandler>,
        _shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        Ok(())
    }
}

struct ApplicationAdmission;

#[async_trait]
impl AdmissionController for ApplicationAdmission {
    async fn admit(&self, _request: &AdmissionRequest) -> Result<AdmissionDecision> {
        Ok(AdmissionDecision::Allow)
    }
}

#[async_trait]
impl AdmissionAdmin for ApplicationAdmission {
    async fn ban_ip(&self, _ip: IpAddr, _ttl: Duration, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn unban_ip(&self, _ip: IpAddr) -> Result<()> {
        Ok(())
    }

    async fn ban_user(&self, _identity: &Identity, _ttl: Duration, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn unban_user(&self, _identity: &Identity) -> Result<()> {
        Ok(())
    }

    async fn set_maintenance(&self, _ttl: Duration, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn clear_maintenance(&self) -> Result<()> {
        Ok(())
    }
}

struct ApplicationWorldClient;

#[async_trait]
impl WorldClient for ApplicationWorldClient {
    async fn command(&self, request: WorldRequest) -> Result<bytes::Bytes> {
        Ok(request.payload)
    }
}

struct ApplicationWorldDiscovery;

#[async_trait]
impl WorldDiscovery for ApplicationWorldDiscovery {
    async fn run(
        &self,
        _updater: Arc<dyn WorldRouteUpdater>,
        _shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        Ok(())
    }
}

struct ApplicationInterceptor;

#[async_trait]
impl GatewayInterceptor for ApplicationInterceptor {
    async fn intercept(
        &self,
        _context: &GatewayInterceptContext,
        _request: &GatewayRequest,
        next: GatewayNext<'_>,
    ) -> Result<GatewayResponse> {
        next.run().await
    }
}

#[test]
fn application_can_inject_its_own_gateway_adapters() {
    let admission = Arc::new(ApplicationAdmission);
    let _gateway = Gateway::new(GatewayConfig::default())
        .replay_protection(Arc::new(ApplicationReplayProtectionStore))
        .online_directory(
            Arc::new(ApplicationOnlineDirectory),
            elura_gateway::GatewayOnlineConfig::new(
                "gateway-1",
                Duration::from_secs(60),
                Duration::from_secs(20),
                DuplicateLoginMode::RejectNew,
            ),
        )
        .push_transport(Arc::new(ApplicationPushTransport))
        .session_control_transport(Arc::new(ApplicationSessionControlTransport))
        .admission(admission.clone(), AdmissionSettings::default())
        .admission_admin(admission)
        .account_version_store(
            Arc::new(ApplicationAccountVersionStore),
            AccountVersionSettings::default(),
        )
        .ownership(32, Arc::new(ApplicationOwnershipResolver))
        .session_observer(Arc::new(|_| Ok(())))
        .readiness_probe("application", Arc::new(|| async { Ok(()) }))
        .interceptor(ApplicationInterceptor)
        .world_client(Arc::new(ApplicationWorldClient));

    let _standalone_gateway =
        Gateway::new(GatewayConfig::default()).world_discovery(Arc::new(ApplicationWorldDiscovery));
}

#[test]
fn gateway_configs_have_stable_constructors() {
    use elura_gateway::{GatewayRealmAdmissionConfig, GatewayWorldTlsConfig};

    let _ = GatewayWorldTlsConfig::new("world.internal");
    let _ = GatewayRealmAdmissionConfig::new([(1, 1)]);
}
