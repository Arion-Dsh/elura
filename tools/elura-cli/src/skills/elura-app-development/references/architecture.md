# Elura application architecture

## Runtime choice

| Shape | Choose it when | Assembly |
| --- | --- | --- |
| Monolith | Local development, small deployments, or one-process operation | `Monolith::new(gateway, world).transport(...).install(...).run(admin)` |
| Gateway + World | Connections and game logic need independent scaling or failure domains | `Gateway::new(...).transport(...).world_discovery(...)` and `World::new(...).registrar(...)` |
| Embedded/custom | The application supplies transports, discovery, storage, or lifecycle integration | Configure the public traits and call `build`/`serve` APIs explicitly |

Gateway owns client transports, tickets, sessions, admission, routing to Worlds, and connection-oriented push delivery. World owns typed commands, modules, middleware, player state, and game behavior. Do not move business rules into Gateway interceptors merely because requests pass through Gateway.

## Stateful scenes

Use `elura::world::scene::SceneRuntime` when multiple players mutate one scene, room, battle, or
simulation and player-key serialization is not a sufficient consistency boundary. Each live scene
has one bounded mailbox and one Tokio task; typed commands, lifecycle hooks, and optional ticks run
serially within that scene, while different scenes remain independently schedulable.

The runtime does not choose a World, acquire distributed ownership, persist snapshots, restore a
failed scene, or define game rules. The application must establish a unique scene owner before
spawning it and decide how scene state is recovered after process failure. Capture a cloned runtime
in World route handlers and shut it down from application or module lifecycle code.

## Login and reconnect ownership

The application login service authenticates credentials, owns durable login or refresh sessions,
selects the player realm, and calls `TicketService::issue_login`. Login tickets are short-lived and
single-use. After Gateway authentication, the response includes a longer-lived single-use
reconnect ticket. An authenticated client renews it through `ROUTE_RECONNECT` by sending the
current reconnect ticket; renewal consumes that ticket and returns its replacement. Reconnecting
with a reconnect ticket rotates it again in the authentication response.

Client integrations should retain only the latest reconnect ticket and renew it shortly before
`expires_in_seconds`. If it is unavailable or expired, the client asks the application login
service for a new login ticket using the application-owned refresh session. Only expiration or
revocation of that refresh session should require interactive login. Elura intentionally does not
store refresh tokens, device login sessions, or credential-provider state in Gateway.

## Login queue and realm capacity

The application login service owns queue order, priority, queue tokens, wait estimates, and the
client polling or notification API. A queued client should not hold an anonymous Gateway
connection: Gateway authentication has a short deadline, and login tickets should be issued only
when the application decides that the client may attempt admission.

Gateway owns the final hard-cap check. Configure an online directory with per-realm limits; its
single atomic `OnlineDirectory::acquire` operation applies duplicate-login policy, checks the realm
capacity, and registers the accepted Session. This prevents concurrent login services or Gateway
processes from oversubscribing a realm after observing the same online count.

```rust
let online = GatewayOnlineConfig::new(
    "gateway-1",
    Duration::from_secs(60),
    Duration::from_secs(20),
    DuplicateLoginMode::RejectNew,
)
.with_realm_capacity(1, 1, 10_000);

let gateway = Gateway::new(config)
    .online_directory(directory, online);
```

When full, Gateway returns the retryable `REALM_FULL` client error with `retry_after_ms`. The
rejected login ticket is not consumed, so the client can retry it after the queue service grants
admission or the retry delay elapses. Treat online statistics as advisory UI data; only the atomic
acquire result is authoritative.

Monolith uses the same Gateway and World configuration models but connects them in process. Standalone World networking, internal authorization, and World TLS are not started by `Monolith`.

## Cargo feature selection

The `elura` facade defaults to `gateway` and `world`. Enable only the capabilities the application uses:

| Feature | Adds |
| --- | --- |
| `monolith` | Combined single-process runtime |
| `room` | Room roster, readiness, leadership, and lifecycle primitives |
| `aoi` | Sparse-grid two-dimensional AOI queries and visibility deltas |
| `simulation` | Deterministic fixed-step timing and bounded catch-up |
| `netcode` | Client/server Tick estimation, redundant inputs, ACK, and reordering |
| `replication` | Per-observer entity visibility, delta/keyframe state, and ACK streams |
| `lag-compensation` | Bounded server history and immutable rewind query contexts |
| `net-sim` | Deterministic adverse network simulation for tests and development |
| `adapters` | Adapter facade without selecting Redis/SQL/Kubernetes implementations |
| `redis` | Redis-backed distributed implementations |
| `sql` | SQL-backed implementations |
| `kubernetes` | Kubernetes discovery/ownership implementations |
| `admin` | Adapter administration support |
| `identity`, `otp`, `notification-alisms` | Identity and messaging provider surfaces |
| `payment-*` | One payment provider integration |
| `full` | All optional integrations; avoid by default in applications |

Confirm the feature list in the pinned `elura` crate version before changing it.

## Ownership boundaries

The upper application owns:

- configuration file and environment loading;
- secret acquisition and rotation policy;
- adapter/provider choice and credentials;
- route IDs, protobuf namespaces, compatibility, and migrations;
- dependency injection and business state;
- deployment topology and observability policy.

Elura owns protocol/session invariants, runtime orchestration, typed route dispatch, transport implementations, and the extension contracts exposed through the facade.

## Configuration flow

Deserialize an application-level config that embeds `GatewayConfig`, `WorldConfig`, transport configs, `AdminServerConfig`, and application settings. Load secrets separately, merge environment overrides explicitly, validate addresses and required values, then construct the runtime.

For distributed mode, configure matching internal tokens and TLS expectations on Gateway and World. Add World discovery to Gateway and registration to World. Add ownership/admission/account-version components only as required by the deployment; do not silently choose an infrastructure backend in shared application code.

## Public API navigation

- Common types: `elura::prelude`
- World APIs: `elura::world`, including `middleware`, `player`, `scene`, and `testing`
- Gameplay primitives: `elura::room`, `elura::aoi`, `elura::simulation`, `elura::netcode`, `elura::replication`, and `elura::lag_compensation`
- Network test utilities: `elura::net_sim`
- Gateway APIs: `elura::gateway`, `elura::transport`, `elura::protection`
- Runtime administration/security: `elura::observability`, `elura::security`, `elura::launch`
- Distributed contracts: `elura::discovery`
- Infrastructure implementations: `elura::adapters`
- External identity/payment/notification implementations: `elura::providers`
