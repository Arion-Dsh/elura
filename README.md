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

> [!IMPORTANT]
> Elura is under active `0.x` development. Minor releases may contain breaking API changes.

## Highlights

- WebSocket and QUIC transports, sessions, routing, middleware, and graceful shutdown.
- Atomic per-realm Session capacity for application-owned login queues.
- Distributed Gateway and World deployment or single-process monolith mode.
- Application-owned infrastructure with optional Redis, SQL, and Kubernetes adapters.
- Optional identity, notification, OTP, and payment providers.

```text
Clients ── WebSocket / QUIC ──▶ Gateway ── routed commands ──▶ World
                                sessions                         game logic
                                    │                                │
                                    └── Redis · SQL · Kubernetes ────┘
```

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
| `elura-monolith` | Single-process Gateway and World composition |
| `elura-adapters` | Redis, SQL, and Kubernetes implementations |
| `elura-providers` | Identity, notification, OTP, and payment integrations |
| `elura-cli` | Project and client-SDK generator |

Example feature selection:

```toml
[dependencies]
elura = { version = "0.2.2", features = ["monolith", "redis"] }
```

Applications may implement the public storage and transport contracts themselves; using
`elura-adapters` or `elura-providers` is not required.

## Development

Rust `1.97` or newer is required. Run the full verification suite with:

```bash
make verify
```

This checks formatting, Clippy, tests, Rustdoc, and package contents.

## License

Elura is dual-licensed under your choice of:

- [Apache License 2.0](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-APACHE)
- [MIT License](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-MIT)
