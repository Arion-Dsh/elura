#[cfg(feature = "core")]
#[test]
fn prelude_contains_game_facing_types() {
    use elura::prelude::{AuthoritativeSimulation, Error, Identity, PlayerKey, Result};

    fn contract_exists<T: ?Sized>() {}
    fn result_contract(_: Result<()>) {}

    let _identity: Option<Identity> = None;
    let _player: Option<PlayerKey> = None;
    let _error: Option<Error> = None;
    contract_exists::<dyn AuthoritativeSimulation<Command = (), Snapshot = ()>>();
    result_contract(Ok(()));
}

#[cfg(feature = "gateway")]
#[test]
fn gateway_owns_the_ports_consumed_by_its_runtime() {
    use elura::gateway::admission::AdmissionController;
    use elura::gateway::discovery::{WorldClient, WorldDiscovery};
    use elura::gateway::presence::OnlineDirectory;
    use elura::gateway::session::{AccountVersionStore, SessionControlTransport};
    use elura::gateway::transport::{GatewayTransport, TcpTransport};

    fn contract_exists<T: ?Sized>() {}
    fn transport_contract<T: GatewayTransport>() {}

    contract_exists::<dyn AccountVersionStore>();
    contract_exists::<dyn AdmissionController>();
    transport_contract::<TcpTransport>();
    contract_exists::<dyn OnlineDirectory>();
    contract_exists::<dyn SessionControlTransport>();
    contract_exists::<dyn WorldClient>();
    contract_exists::<dyn WorldDiscovery>();
}

#[cfg(feature = "world")]
#[test]
fn world_owns_gameplay_and_registration_contracts() {
    use elura::world::registration::WorldRegistrar;
    use elura::world::{WorldHandler, WorldMiddleware, WorldModule};

    fn contract_exists<T: ?Sized>() {}

    contract_exists::<dyn WorldHandler>();
    contract_exists::<dyn WorldMiddleware>();
    contract_exists::<dyn WorldModule>();
    contract_exists::<dyn WorldRegistrar>();
}

#[cfg(feature = "runtime")]
#[test]
fn cross_cutting_contracts_live_with_their_capability() {
    use elura::outbox::{EventHandler, IdempotencyStore, OutboxStore};
    use elura::ownership::OwnershipResolver;
    use elura::push::PushTransport;
    use elura::replay_protection::ReplayProtectionStore;

    fn contract_exists<T: ?Sized>() {}

    contract_exists::<dyn EventHandler>();
    contract_exists::<dyn IdempotencyStore>();
    contract_exists::<dyn OutboxStore>();
    contract_exists::<dyn OwnershipResolver>();
    contract_exists::<dyn PushTransport>();
    contract_exists::<dyn ReplayProtectionStore>();
}

#[cfg(feature = "providers")]
#[test]
fn provider_contracts_live_with_their_domain() {
    use elura::providers::payment::PaymentProvider;

    fn contract_exists<T: ?Sized>() {}

    contract_exists::<dyn PaymentProvider>();
}

#[cfg(all(
    feature = "room",
    feature = "aoi",
    feature = "simulation",
    feature = "netcode",
    feature = "replication",
    feature = "lag-compensation",
    feature = "net-sim"
))]
#[test]
fn gameplay_primitives_share_one_namespace() {
    use elura::gameplay::aoi::{AoiGrid, AoiIndex};
    use elura::gameplay::lag_compensation::LagCompensationHistory;
    use elura::gameplay::net_sim::SimulatedLink;
    use elura::gameplay::netcode::PredictionBuffer;
    use elura::gameplay::replication::ReplicationSender;
    use elura::gameplay::room::Room;
    use elura::gameplay::simulation::FixedStepClock;

    fn type_exists<T>() {}
    fn contract_exists<T: ?Sized>() {}

    type_exists::<AoiGrid<u64>>();
    contract_exists::<dyn AoiIndex<u64, Position = (), Error = ()>>();
    type_exists::<FixedStepClock>();
    type_exists::<LagCompensationHistory<u8>>();
    type_exists::<PredictionBuffer<u8, u8>>();
    type_exists::<ReplicationSender<u64, u8, u8>>();
    type_exists::<Room<u64, u64, ()>>();
    type_exists::<SimulatedLink<u8>>();
}
