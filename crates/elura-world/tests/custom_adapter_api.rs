use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::Result;
use elura_core::ownership::{Assignment, OwnershipResolver};
use elura_core::push::{PushHandler, PushReceipt, PushRequest, PushTransport};
use elura_runtime::observability::AdminServerConfig;
use elura_world::player::{
    InvalidationBus, InvalidationHandler, PlayerCache, PlayerCacheConfig, PlayerCacheSynchronizer,
    PlayerInvalidation,
};
use elura_world::registration::WorldRegistrar;
use elura_world::{World, WorldConfig};
use tokio::sync::watch;

struct ApplicationRegistrar;

#[allow(dead_code)]
async fn run_world(world: World, admin: AdminServerConfig) -> Result<()> {
    world.run(admin).await
}

#[async_trait]
impl WorldRegistrar for ApplicationRegistrar {
    fn renew_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn register(&self) -> Result<()> {
        Ok(())
    }

    async fn renew(&self) -> Result<()> {
        Ok(())
    }

    async fn unregister(&self) -> Result<()> {
        Ok(())
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

struct ApplicationInvalidationBus;

#[async_trait]
impl InvalidationBus for ApplicationInvalidationBus {
    async fn publish(&self, _invalidation: &PlayerInvalidation) -> Result<()> {
        Ok(())
    }

    async fn subscribe(&self, _handler: Arc<dyn InvalidationHandler>) -> Result<()> {
        Ok(())
    }
}

#[test]
fn application_can_inject_its_own_world_adapters() {
    let _world = World::new(WorldConfig::default())
        .push_transport(Arc::new(ApplicationPushTransport))
        .registrar(Arc::new(ApplicationRegistrar))
        .ownership("world-1", 32, Arc::new(ApplicationOwnershipResolver));

    let cache = Arc::new(PlayerCache::<()>::new(PlayerCacheConfig::default()).unwrap());
    let _synchronizer = PlayerCacheSynchronizer::new(
        cache,
        Arc::new(ApplicationInvalidationBus),
        "player",
        "world-1",
    )
    .unwrap();
}
