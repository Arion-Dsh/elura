# Elura application architecture

## Runtime choice

| Shape | Choose it when | Assembly |
| --- | --- | --- |
| Monolith | Local development, small deployments, or one-process operation | `Monolith::new(gateway, world).transport(...).install(...).run(admin)` |
| Gateway + World | Connections and game logic need independent scaling or failure domains | `Gateway::new(...).transport(...).world_discovery(...)` and `World::new(...).registrar(...)` |
| Embedded/custom | The application supplies transports, discovery, storage, or lifecycle integration | Configure the public traits and call `build`/`serve` APIs explicitly |

Gateway owns client transports, tickets, sessions, admission, routing to Worlds, and connection-oriented push delivery. World owns typed commands, modules, middleware, player state, and game behavior. Do not move business rules into Gateway interceptors merely because requests pass through Gateway.

## Client SDK transport boundary

Official client SDKs own the ELR2 wire contract, authentication and reconnect payloads, heartbeat
handling, and Session Control encoding. The high-level Rust `elura-client` crate additionally owns
TCP socket creation, timeouts, sequential request-to-response correlation, push queuing, and
automatic reconnect-ticket rotation. The application still owns application-route payloads and
decides whether an interrupted request is safe to retry; it does not schedule ticket renewal or
ordinary transport reconnects. When the Client reports `ReauthenticationRequired`, the application
must acquire a fresh login ticket and pass it to `reauthenticate`.

The Rust SDK workspace separates these concerns into `elura-client` and
runtime-independent `elura-protocol` crates. Use `Elr2Codec::encode` and
`Elr2Codec::decode` from `elura-protocol` for WebSocket messages, UDP packets,
and QUIC Datagrams. For a custom TCP, TLS, or compatible QUIC byte stream in a
Tokio application, enable `elura-protocol`'s `tokio-codec` feature and wrap the
stream in `tokio_util::codec::Framed`.

## Stateful scenes

Use `elura::world::scene::SceneRuntime` when multiple players mutate one scene, room, battle, or
simulation and player-key serialization is not a sufficient consistency boundary. Each live scene
has one bounded mailbox and one Tokio task; typed commands, lifecycle hooks, and optional ticks run
serially within that scene, while different scenes remain independently schedulable.

The runtime does not choose a World, acquire distributed ownership, persist snapshots, restore a
failed scene, or define game rules. The application must establish a unique scene owner before
spawning it and decide how scene state is recovered after process failure. Capture a cloned runtime
in World route handlers and shut it down from application or module lifecycle code.

## Realtime gameplay composition

Treat realtime gameplay as a composition of optional primitives, not as a second mandatory
runtime. `Room`, `AoiGrid`, `FixedStepClock`, netcode buffers, replication streams, lag-compensation
history, and `SimulatedLink` own no Tokio task, socket, persistence, or World dependency. They can
run inside a `Scene`, inside another application executor, or in client-side Rust code.

| Stage | Framework primitive | Application responsibility |
| --- | --- | --- |
| Connection | Gateway TCP, UDP, WebSocket, WebTransport, or QUIC transport | Select protocol and encode gameplay messages |
| Input delivery | `TickSynchronizer`, `InputSender`, `InputReceiver` | Define input values and apply each accepted input once |
| Simulation | `FixedStepClock`, optional `SceneRuntime` | Implement deterministic movement, physics, abilities, and AI |
| Visibility | `AoiGrid` | Resolve visible IDs to authoritative entity state |
| Replication | `ReplicationSender`, `ReplicationReceiver` | Serialize batches and associate one observer stream with one client |
| Client correction | `PredictionBuffer`, `InterpolationBuffer`, `PredictedEntityMatcher` | Simulate inputs, interpolate concrete state, and update presentation |
| Hit validation | `LagCompensationHistory` | Record compact collision snapshots and perform ray casts or overlap tests |
| Network testing | `SimulatedLink` | Choose reproducible weak-network profiles and assertions |

For native action games that need both ordered business messages and loss-tolerant realtime
updates, use QUIC hybrid mode and list only realtime application route IDs in `datagram_routes`.
Authentication and every other route remain on the reliable stream:

```rust
use elura::transport::{QuicConfig, QuicMode};

let mut quic = QuicConfig::from_pem_files(
    "0.0.0.0:17003".parse()?,
    "certs/quic-cert.pem",
    "certs/quic-key.pem",
);
quic.mode = QuicMode::Hybrid;
quic.datagram_routes = vec![120, 121]; // input and replication packets
```

The client must select the same routes for its outbound QUIC Datagrams. Keep inventory, rewards,
match lifecycle, and other durable commands off that list. Datagram payloads must fit
`max_datagram_bytes`; the default is 1100 bytes to avoid IP fragmentation on common paths.

An authoritative action-game path normally accepts redundant inputs at Gateway, routes them to the
owning World and Scene, consumes them on fixed simulation ticks, selects entities through AOI, and
emits per-observer replication batches. The client predicts its local entity, reconciles against
authoritative state, and interpolates remote entities. A server may answer a fire command against a
bounded historical collision snapshot without mutating the live scene.

Do not create one `SceneRuntime` per primitive. Keep related room, simulation, AOI, replication, and
history state in the single scene that owns the match or map partition. If no shared mutable scene
exists, use the primitives without `SceneRuntime` or `World`.

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
| `netcode` | Tick estimation, redundant inputs, prediction/replay, interpolation, and predicted entities |
| `replication` | Per-observer lifecycle, delta/keyframe state, prediction keys, reordering, and ACK streams |
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
