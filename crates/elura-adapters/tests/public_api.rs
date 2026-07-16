#[cfg(feature = "redis")]
#[test]
fn redis_adapters_are_grouped_by_capability() {
    use elura_adapters::account_version::RedisAccountVersionStore;
    use elura_adapters::admission::RedisAdmissionController;
    use elura_adapters::discovery::{RedisWorldDiscovery, RedisWorldRegistrar};
    use elura_adapters::invalidation::RedisInvalidationBus;
    use elura_adapters::online::RedisOnlineDirectory;
    use elura_adapters::otp::RedisOtpStore;
    use elura_adapters::outbox::{RedisIdempotencyStore, RedisOutbox};
    use elura_adapters::push::RedisStreamPushBus;
    use elura_adapters::redis::RedisHealth;
    use elura_adapters::replay::RedisReplayStore;
    use elura_adapters::session_control::RedisSessionControlBus;

    fn type_exists<T>() {}

    type_exists::<RedisAccountVersionStore>();
    type_exists::<RedisAdmissionController>();
    type_exists::<RedisWorldDiscovery>();
    type_exists::<RedisWorldRegistrar>();
    type_exists::<RedisHealth>();
    type_exists::<RedisInvalidationBus>();
    type_exists::<RedisOnlineDirectory>();
    type_exists::<RedisOtpStore>();
    type_exists::<RedisOutbox>();
    type_exists::<RedisIdempotencyStore>();
    type_exists::<RedisStreamPushBus>();
    type_exists::<RedisReplayStore>();
    type_exists::<RedisSessionControlBus>();
}

#[cfg(feature = "redis")]
#[test]
fn redis_discovery_configs_have_stable_constructors() {
    use elura_adapters::discovery::{RedisWorldDiscoveryConfig, RedisWorldRegistrationConfig};

    let _ = RedisWorldDiscoveryConfig::new("elura:worlds");
    let _ = RedisWorldRegistrationConfig::new("elura:worlds", "127.0.0.1:18000", 1, 1);
}

#[test]
fn dns_discovery_config_has_a_stable_constructor() {
    let _ =
        elura_adapters::discovery::DnsWorldDiscoveryConfig::new("world.example.com:18000", 1, 1);
}

#[cfg(feature = "kubernetes")]
#[test]
fn kubernetes_configs_have_stable_constructors() {
    use elura_adapters::kubernetes::{
        EndpointWatcherConfig, LeaderElectionConfig, OwnershipCoordinatorConfig,
        OwnershipObserverConfig,
    };

    let _ = EndpointWatcherConfig::new("games", "world", "elr2", 1, 1);
    let _ = LeaderElectionConfig::new("games", 1, 1, "gateway-1");
    let _ = OwnershipObserverConfig::new("games", 1, 1, 64);
    let _ = OwnershipCoordinatorConfig::new("games", 1, 1, 64);
}

#[cfg(feature = "redis")]
struct CustomPushTargetResolver;

#[cfg(feature = "redis")]
#[async_trait::async_trait]
impl elura_core::push::PushTargetResolver for CustomPushTargetResolver {
    async fn resolve_gateways(
        &self,
        _request: &elura_core::push::PushRequest,
    ) -> elura_core::Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[cfg(feature = "redis")]
#[test]
fn push_adapter_accepts_an_application_target_resolver() {
    use std::sync::Arc;

    use elura_adapters::push::{RedisStreamPushBus, RedisStreamPushConfig};

    let _connection = RedisStreamPushBus::connect(
        "redis://127.0.0.1/",
        "push",
        Arc::new(CustomPushTargetResolver),
        "gateway-1",
        RedisStreamPushConfig::default(),
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_adapters_have_simple_public_types() {
    use elura_adapters::account_version::SqlAccountVersionStore;
    use elura_adapters::outbox::SqlOutbox;

    fn type_exists<T>() {}

    type_exists::<SqlAccountVersionStore>();
    type_exists::<SqlOutbox>();
}

struct CustomReplayStore;

#[async_trait::async_trait]
impl elura_core::ticket::ReplayStore for CustomReplayStore {
    async fn reserve(&self, _ticket_id: &str, _expires_at: u64) -> elura_core::Result<bool> {
        Ok(true)
    }
}

#[test]
fn applications_can_supply_their_own_adapter() {
    fn accepts_replay_store<T: elura_core::ticket::ReplayStore>() {}
    accepts_replay_store::<CustomReplayStore>();
}
