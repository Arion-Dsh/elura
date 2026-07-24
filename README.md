<div align="center">

# Elura

**An open-source, modular Rust framework for authoritative realtime gameplay and extensible online game services.**

[![CI](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml/badge.svg)](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/elura.svg)](https://crates.io/crates/elura)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-dea584.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

[Documentation](https://elura.rustyspottedcat.dev/) · [API reference](https://docs.rs/elura) · [Crates.io](https://crates.io/crates/elura) · [简体中文](README.zh-CN.md)

</div>

Elura is an open-source, modular Rust framework for authoritative realtime gameplay and extensible
online game services. It separates client connections from game logic: Gateway processes own
connections and sessions, while World processes execute commands and manage player state. They can
scale independently or run together as a monolith.

> [!IMPORTANT]
> The `0.3.x` API line is frozen. Patch releases preserve public API
> compatibility; breaking changes are reserved for `0.4.0`.

## Architecture

```text
Clients ── TCP / UDP / WebSocket / WebTransport / QUIC ──▶ Gateway
                                                               │
                                                        routed commands
                                                               │
                                                               ▼
                                                            World
                                                      authoritative game logic
```

Elura provides server-side building blocks rather than fixed product rules or data models. It is
not a client game engine, hosted backend, or game-server fleet orchestrator.

## Features

- TCP, UDP, WebSocket, WebTransport, and QUIC transports.
- Distributed Gateway and World deployment, or a single-process monolith.
- Rooms, fixed-step simulation, AOI, replication, prediction, and lag compensation.
- HTTP authentication and one-time ELR2 session-ticket exchange.
- Optional Redis, SQL, Kubernetes, identity, notification, OTP, and payment integrations.

## Who Elura is for

Elura is a good fit when:

- The authoritative server and game logic should be written in Rust.
- A project needs realtime sessions, rooms, simulation, AOI, replication, prediction, or lag
  compensation in one modular stack.
- Realtime gameplay, social systems, competitive systems, identity, and commerce should share one
  extensible server framework while retaining game-specific rules and data models.
- Gateway connections and World gameplay processes need to scale independently.
- The application must own its infrastructure, persistence, identity policies, and deployment.

Consider another stack when:

- You want a managed backend with no server operations.
- You want fixed, turnkey APIs and data models for social graphs, matchmaking, leaderboards,
  tournaments, and administration without implementing game-specific policies or persistence.
- You require a stable `1.x` API today and cannot accommodate changes during Elura's `0.x`
  development.

## Quick start

Install the CLI and scaffold an application:

```bash
cargo install elura-cli --version 0.3.1
elura init all --dir .
```

Or add Elura directly:

```toml
[dependencies]
elura = "0.3.1"
```

See the [documentation](https://elura.rustyspottedcat.dev/) for concepts, configuration, crate
features, deployment, and tutorials.

The facade assigns each contract to the runtime or capability that consumes it:

- `elura::gateway` owns connection, discovery, presence, session, and ticket APIs.
- `elura::world` owns routes, middleware, scenes, players, and World registration.
- `elura::outbox`, `elura::push`, `elura::ownership`, `elura::replay_protection`, and
  `elura::providers` own cross-cutting services.
- `elura::gameplay` groups room, AOI, simulation, netcode, replication, and test primitives.
- `elura::prelude` remains a small convenience import for common game-facing types.

The corresponding subcrates follow the same ownership: Gateway presence and discovery ports are
declared by `elura-gateway`, World registration ports by `elura-world`, and `elura-core` retains
only shared wire/domain contracts.

## Examples

- [`tiny-network-game`](examples/tiny-network-game): authoritative multiplayer movement.
- [`realtime-gameplay`](examples/realtime-gameplay): a transport-neutral gameplay pipeline.

## Development

Rust `1.97` or newer is required.

```bash
make verify
```

## License

Elura is dual-licensed under your choice of:

- [Apache License 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)
