#![cfg(feature = "world")]

use elura::world::testing::{WorldTestClient, test_identity};
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
fn facade_exposes_world_unit_testing_types() {
    let client_type: Option<WorldTestClient> = None;
    assert!(client_type.is_none());
    assert_eq!(test_identity(7).user_id, 7);
}

#[tokio::test]
async fn upper_layer_can_run_a_typed_business_test() {
    let harness = World::new(WorldConfig::default())
        .route(EchoRoute, |_context, request| async move { Ok(request) })
        .build()
        .unwrap()
        .harness();
    let client = harness.client(test_identity(1)).unwrap();

    let first = client.call(EchoRoute, Echo { sequence: 1 }).await.unwrap();
    let second = client.call(EchoRoute, Echo { sequence: 2 }).await.unwrap();

    assert_eq!(first.sequence, 1);
    assert_eq!(second.sequence, 2);
    assert_eq!(harness.stats().commands, 2);
}
