#![cfg(feature = "world")]

use elura::world::testing::{
    WorldLoadConfig, WorldLoadLatency, WorldLoadReport, WorldTestClient, test_identity,
};
use elura::world::{Route, World, WorldConfig};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct Echo {
    #[prost(uint64, tag = "1")]
    sequence: u64,
}

struct EchoRoute;

impl Route for EchoRoute {
    const ID: u32 = 100;
    const NAME: &'static str = "test.echo";

    type Request = Echo;
    type Response = Echo;
}

#[test]
fn facade_exposes_world_load_testing_types() {
    let config = WorldLoadConfig::new(8, 100);
    let latency = WorldLoadLatency::default();
    let report_type: Option<WorldLoadReport> = None;

    assert_eq!(config.concurrency, 8);
    assert_eq!(config.iterations_per_worker, 100);
    assert_eq!(latency.min, std::time::Duration::ZERO);
    assert!(report_type.is_none());
    let client_type: Option<WorldTestClient> = None;
    assert!(client_type.is_none());
}

#[tokio::test]
async fn upper_layer_can_run_an_in_process_typed_load() {
    let harness = World::new(WorldConfig::default())
        .route(EchoRoute, |_context, request| async move { Ok(request) })
        .build()
        .unwrap()
        .harness();
    let report = harness
        .load_route(
            EchoRoute,
            WorldLoadConfig::new(2, 3),
            |worker| test_identity(worker as i64 + 1),
            |_worker, iteration| Echo {
                sequence: iteration as u64,
            },
        )
        .await
        .unwrap();

    assert_eq!(report.attempted, 6);
    assert_eq!(report.succeeded, 6);
    assert!(report.is_success());
}
