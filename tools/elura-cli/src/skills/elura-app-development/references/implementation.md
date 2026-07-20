# Elura application implementation patterns

## Typed routes

Define a zero-sized route marker and Prost messages. Keep the numeric ID stable once a client can depend on it.

```rust
use elura::prelude::*;
use prost::Message;

pub struct ListItems;

impl Route for ListItems {
    const ID: u32 = 120;
    const NAME: &'static str = "inventory.list_items";
    type Request = ListItemsRequest;
    type Response = ListItemsResponse;
}

#[derive(Clone, PartialEq, Message)]
pub struct ListItemsRequest {}

#[derive(Clone, PartialEq, Message)]
pub struct ListItemsResponse {
    #[prost(int64, repeated, tag = "1")]
    pub item_ids: Vec<i64>,
}
```

Take identity from `WorldContext`; never accept an authoritative user ID from the client when the authenticated identity is intended.

```rust
async fn handle(
    context: WorldContext,
    _request: ListItemsRequest,
) -> elura::Result<ListItemsResponse> {
    let user_id = context.identity.user_id;
    // Query an application-owned service using user_id.
    Ok(ListItemsResponse { item_ids: vec![] })
}
```

Use `route_raw` only for protocol tooling or deliberate non-Protobuf integrations.

## Modules

Group a cohesive feature behind `WorldModule`. Keep mount functions beside their route handlers and let registration return errors.

```rust
use elura::world::{WorldModule, WorldModuleRegistry};

pub struct InventoryModule;

impl WorldModule for InventoryModule {
    fn name(&self) -> &str { "inventory" }

    fn register(&self, world: &mut WorldModuleRegistry<'_>) -> elura::Result<()> {
        world.route(ListItems, handle)?;
        Ok(())
    }
}
```

Use `start` and `stop` only for module-owned lifecycle work. Prefer runtime-supervised services and explicit application composition for shared infrastructure.

## Middleware and context

Implement `WorldMiddleware` for tracing, policy, state loading, rate decisions, or transaction boundaries. Middleware operates on encoded bytes, so keep request-specific validation in typed handlers unless the middleware genuinely applies across routes.

Use `ContextKey<T>` to pass typed, request-scoped values from middleware to handlers. Use `UnitOfWorkMiddleware` plus `context.transaction::<T>().await?` when a command must commit or roll back as a unit. Avoid calling external services while holding a database transaction unless atomicity requires it.

Built-in player-state helpers live under `elura::world::player`: `PlayerCache`, `CachedPlayerLoader`, `PlayerStateMiddleware`, and invalidation types. Define cache ownership and invalidation behavior before introducing a cache in distributed deployments.

## Stateful scene execution

Use `SceneRuntime` when the consistency boundary is a shared scene rather than one player. Define
application-owned scene state and strongly typed commands:

```rust
use async_trait::async_trait;
use elura::world::scene::{Scene, SceneCommand, SceneRuntime, SceneRuntimeConfig};

struct BattleScene {
    id: u64,
    score: u32,
}

#[async_trait]
impl Scene for BattleScene {
    type Id = u64;

    fn id(&self) -> &Self::Id { &self.id }

    async fn tick(&mut self, elapsed: std::time::Duration) -> elura::Result<()> {
        // Advance application-owned simulation state.
        Ok(())
    }
}

struct AddScore(u32);

#[async_trait]
impl SceneCommand<BattleScene> for AddScore {
    type Output = u32;

    async fn execute(self, scene: &mut BattleScene) -> elura::Result<u32> {
        scene.score += self.0;
        Ok(scene.score)
    }
}

let scenes = SceneRuntime::new(SceneRuntimeConfig::default())?;
scenes.spawn(BattleScene { id: 7, score: 0 }).await?;
let score = scenes.call(&7, AddScore(10)).await?;
scenes.shutdown().await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

One scene never executes two commands or a command and tick concurrently. A handler timeout
cancels that operation's future, so do not perform irreversible external side effects without an
idempotency strategy. A command panic terminates the affected scene because its mutable state may
no longer satisfy application invariants. `SceneRuntime` deliberately does not provide scene
placement, distributed ownership, persistence, or recovery; compose those policies explicitly.
`SceneError` converts into `elura::Error`, so World route handlers can propagate scene failures with
`?` and still override that mapping when application-specific client errors are required.

## Room, AOI, fixed-step, netcode, and replication composition

Enable only the primitives the application uses:

```toml
elura = { version = "0.2.4", features = ["world", "room", "aoi", "simulation", "netcode", "replication", "lag-compensation"] }
```

- `elura::room::Room` stores an application-owned roster, readiness, deterministic leader
  succession, and `Open -> Active -> Closed` lifecycle. It does not allocate rooms to Worlds or
  broadcast room events.
- `elura::aoi::AoiGrid` indexes arbitrary entity IDs in two dimensions and returns circular query
  results or `entered`/`left` movement deltas. The application decides how those deltas become Push
  events.
- `elura::simulation::FixedStepClock` converts elapsed wall time into bounded deterministic steps.
  It owns no task or timer and can be advanced from `Scene::tick` or a test.
- `elura::netcode::TickSynchronizer` estimates the authoritative server Tick from correlated probes
  and network RTT. `InputSender` retains unacknowledged inputs and includes recent frames in every
  packet; one `InputReceiver` per client stream validates Tick bounds, accepts reordered sequences,
  ignores redundant duplicates, and returns cumulative ACKs.
- `elura::replication::ReplicationSender` reconciles one observer's complete visible entity states
  into ordered Spawn, Despawn, Update, or Keyframe batches. Its matching receiver buffers reordered
  batches until sequence gaps close and returns cumulative ACKs. Use `elura::aoi::AoiGrid` to select
  IDs, then resolve those IDs into application-owned `VersionedState` values before reconciliation.
  `elura::core::replication` remains the lower-level whole-room byte snapshot stream; it does not
  replace per-observer entity replication.

Keep these values inside the scene that owns their mutable state. Use scene ownership and recovery
policy before running the same logical room or simulation on more than one World. The application
  still owns serialization, transport routes, player-to-receiver association, snapshot policy, and
  the meaning of each input value.

## Prediction, interpolation, and lag compensation

- `elura::netcode::PredictionBuffer` stores locally simulated inputs and predicted states. Feed it
  an authoritative state to discard confirmed inputs and replay the remainder through an
  application-owned deterministic simulation callback.
- `elura::netcode::InterpolationBuffer` orders remote states by Tick, measures arrival jitter and
  late-sample pressure, adapts a bounded render delay, and returns two states plus `alpha`. The
  application performs position, rotation, animation, or custom interpolation.
- `elura::netcode::PredictedEntityMatcher` maps a client `PredictionKey` and temporary entity to an
  authoritative replicated entity, or expires the prediction after a bounded Tick timeout.
- `elura::lag_compensation::LagCompensationHistory` retains immutable application snapshots by
  authoritative Tick and validates bounded rewind queries. The callback performs application-owned
  ray casts, collision checks, hit validation, and damage rules without mutating the live scene.

Use `elura::net_sim::SimulatedLink` in tests to inject deterministic latency, jitter, loss,
duplication, reordering delay, bandwidth serialization, and queue pressure. Supply explicit
monotonic time and fixed seeds so failures remain reproducible.

## Push events

Define typed `Event` implementations and publish through `WorldContext` methods such as `push_session`, `push_user`, `push_users`, `push_realm`, and `push_topic`. Treat event IDs and message schemas as client-facing compatibility contracts. Configure `World::push_transport` when World is not connected to Gateway in process.

## Runtime composition

- Gateway requires at least one explicit client transport.
- A distributed Gateway needs a World client or World discovery implementation.
- Register business routes on `World`; use `Monolith::route`/`install` as convenience forwarding.
- Use `.http(address, router)` for application HTTP routers that should share runtime supervision. Listener conflicts are returned at build time.
- Use `.build()` when tests or an embedding host need runtime handles; use `.run(admin)` for the normal process lifecycle.

## Testing World behavior

Build a World and call routes without binding sockets:

```rust
let server = World::new(WorldConfig::default())
    .route(ListItems, handle)
    .build()?;
let harness = server.harness();
let response = harness.call(ListItems, identity, ListItemsRequest {}).await?;
```

Use `call_in_session` when session continuity matters and `command_raw` only for wire/error-path tests. Assert response data and application state; use `routes()` and `stats()` for registration and execution diagnostics. Keep Gateway transport and end-to-end protocol tests separate from handler unit tests.
