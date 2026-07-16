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
