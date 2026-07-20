<div align="center">

# Elura

**A modular Rust framework for online game servers.**

[![CI](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml/badge.svg)](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-dea584.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

[Documentation](https://elura.rustyspottedcat.dev/) · [API reference](https://docs.rs/elura) · [Crates.io](https://crates.io/crates/elura)

</div>

Elura separates client connections from game logic. Gateway processes own connections and
sessions; World processes execute commands and manage player state. They can scale independently
or run together as a monolith.

## Highlights

- TCP, UDP, WebSocket, WebTransport and QUIC transports, sessions, routing, middleware, and graceful shutdown.
- Atomic per-realm Session capacity for application-owned login queues.
- Distributed Gateway and World deployment or single-process monolith mode.
- Optional actor-style scene mailboxes with typed commands, lifecycle hooks, and ticks.
- Fixed-step simulation, AOI, entity replication, client prediction/interpolation, and lag compensation primitives.
- Application-owned infrastructure with optional Redis, SQL, and Kubernetes adapters.
- Optional identity, notification, OTP, and payment providers.

```text
Clients ── TCP / UDP / WebSocket / WebTransport / QUIC ──▶ Gateway ── routed commands ──▶ World
                                sessions                         game logic
                                    │                                │
                                    └── Redis · SQL · Kubernetes ────┘
```

## Realtime gameplay pipeline

Elura now covers the transport-neutral path from a client input to an authoritative correction:

```text
Client input
  └─ Tick sync + redundant input ─▶ Gateway route ─▶ World / Scene mailbox
                                                   └─ Fixed simulation Tick
                                                      ├─ Room lifecycle
                                                      ├─ Application game rules
                                                      ├─ AOI visibility
                                                      └─ Replication batches
                                                           ├─ Spawn / Despawn
                                                           ├─ Delta / Keyframe
                                                           └─ cumulative ACK

Client presentation
  ├─ local prediction + authoritative replay
  ├─ predicted-spawn matching
  └─ remote interpolation + adaptive jitter delay

Server validation
  └─ bounded historical rewind for lag-compensated queries
```

The primitives are bounded, transport-independent, and can be embedded in a World scene, a
standalone simulation, or client-side Rust code. Rooms, AOI, simulation clocks, and netcode do not
require `World`; `SceneRuntime` is the optional World-owned execution host when a scene needs one
serial mailbox and lifecycle-managed ticks.

| Elura provides | The application still owns |
| --- | --- |
| Connections, sessions, routing, admission, and reconnect | Login UI, refresh sessions, and game-specific admission policy |
| Scene serialization, fixed-step timing, Tick estimation, input ACK/redundancy | Movement, physics, abilities, AI, and deterministic simulation rules |
| AOI queries and per-observer replication protocol state | Entity schemas, serialization routes, and mapping AOI changes to client messages |
| Prediction history, replay orchestration, interpolation sampling | Position/rotation interpolation, animation, rendering, and client engine integration |
| Historical rewind validation | Collision snapshots, ray casts, hit rules, and damage application |
| Redis, SQL, Kubernetes, identity, OTP, notification, and payment integrations | Product data models, quests, inventory, matchmaking, ranking, and economy rules |

This foundation fits authoritative arenas, room games, co-op instances, MMO map shards, and
FPS/TPS or MOBA simulation backends. It is not a prebuilt game: game state and behavior remain in
the upper application.

## Quick start

Install the CLI and scaffold an application:

```bash
cargo install elura-cli
elura skill install
elura init all --dir .
```

The skill is installed in `.agents/skills/elura-app-development` for project-level coding agents.
The generated project includes Gateway, World, and monolith binaries, configuration, Docker
assets, Kubernetes manifests, and client SDK generation. Continue with the
[documentation](https://elura.rustyspottedcat.dev/).

## Crates and features

The `elura` facade enables `gateway` and `world` by default. Everything else is opt-in.

| Crate | Purpose |
| --- | --- |
| `elura-core` | Protocol, session, routing, and cross-process contracts |
| `elura-runtime` | Lifecycle, security, administration, and observability |
| `elura-gateway` | Client connection and session runtime |
| `elura-world` | Command and player-state runtime |
| `elura-room` | Application-owned room roster and lifecycle state machine |
| `elura-aoi` | Sparse-grid two-dimensional visibility indexing |
| `elura-simulation` | Deterministic fixed-step simulation timing |
| `elura-netcode` | Tick synchronization, redundant input, prediction/replay, interpolation, and predicted-entity matching |
| `elura-replication` | Per-observer Spawn, Despawn, delta, keyframe, prediction-key, reorder, and ACK streams |
| `elura-lag-compensation` | Bounded authoritative history and server rewind queries |
| `elura-net-sim` | Deterministic latency, jitter, loss, reorder, and bandwidth simulation |
| `elura-monolith` | Single-process Gateway and World composition |
| `elura-adapters` | Redis, SQL, and Kubernetes implementations |
| `elura-providers` | Identity, notification, OTP, and payment integrations |
| `elura-cli` | Project and client-SDK generator |

Example feature selection:

```toml
[dependencies]
elura = { version = "0.2.4", features = ["monolith", "room", "aoi", "simulation", "netcode", "replication", "lag-compensation", "redis"] }
```

Applications may implement the public storage and transport contracts themselves; using
`elura-adapters` or `elura-providers` is not required.

## Development

Rust `1.97` or newer is required. Run the full verification suite with:

```bash
make verify
```

This checks formatting, Clippy, tests, Rustdoc, and package contents.

## Example game

[`examples/tiny-network-game`](examples/tiny-network-game) combines an Elura authoritative
server with two native Spottedcat clients in a tiny multiplayer movement demo.

## License

Elura is dual-licensed under your choice of:

- [Apache License 2.0](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-APACHE)
- [MIT License](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-MIT)
