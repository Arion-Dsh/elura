<div align="center">

# Elura

**A modular Rust framework for online game servers.**

[![CI](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml/badge.svg)](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-dea584.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

[Documentation](https://arion-dsh.github.io/horizon-rs-doc/) · [API reference](https://docs.rs/elura) · [Crates.io](https://crates.io/crates/elura)

</div>

Elura separates connection handling from game logic: Gateway processes own client connections and
sessions, while World processes execute application commands and manage player state. Deploy them
independently for horizontal scaling or run both in one process during development.

> [!IMPORTANT]
> Elura is under active `0.x` development. APIs may change between minor releases.

## Why Elura?

- **Purpose-built runtime** — WebSocket and QUIC transports, sessions, reconnect tickets, routing,
  middleware, graceful shutdown, and observability are available as composable building blocks.
- **Flexible topology** — use separate Gateway and World services in production, or a monolith for
  local development and smaller deployments.
- **Optional infrastructure** — enable Redis, SQL, Kubernetes, admin, identity, notification, OTP,
  and payment integrations only when the application needs them.
- **Project generation** — scaffold Rust binaries, configuration, containers, Kubernetes manifests,
  and C++, C#, or TypeScript protocol SDKs from one CLI.

## Architecture

```text
                    routed commands
Clients ─────────▶ Gateway ─────────────▶ World
 WebSocket / QUIC   connections,          routes, middleware,
                    sessions, admission   game and player state

                    Redis · SQL · Kubernetes · external providers
```

Gateway and World share protocol and discovery contracts from `elura-core`; neither runtime needs
to depend on the other. `elura-monolith` is the composition layer that deliberately brings both
runtimes into one process.

## Quick start

Install the project generator:

```bash
cargo install elura-cli
```

Generate a complete application in the current directory:

```bash
elura init all --dir .
```

The generated project includes compilable Gateway, World, and monolith binaries, local
configuration, a multi-stage Dockerfile, Docker Compose services, and Kubernetes manifests.
Preview every generated file without writing anything:

```bash
elura init all --dir . --dry-run
```

Generate client protocol SDKs together or select a language with `--language`:

```bash
elura init sdk --dir .
# --language cpp | csharp | typescript
```

See the [documentation](https://arion-dsh.github.io/horizon-rs-doc/) for application routes, the
ELR2 wire protocol, deployment topologies, configuration, and operations.

## Workspace

| Package | Role |
| --- | --- |
| `elura` | Application-facing facade and feature selection |
| `elura-core` | Protocol, session, routing, and cross-process contracts |
| `elura-runtime` | Shared lifecycle, security, admin, and observability runtime |
| `elura-gateway` | Client-facing connection and session runtime |
| `elura-world` | Command routing and player-state runtime |
| `elura-monolith` | Single-process Gateway and World composition |
| `elura-adapters` | Redis, SQL, Kubernetes, and admin adapters |
| `elura-providers` | Identity, notification, OTP, and payment providers |
| `elura-cli` | Project and client-SDK generator |
| `elura-load` / `elura-perf` | Load generation and performance tools |

## Development

Rust `1.97` or newer is required. Run the complete local verification suite with:

```bash
make verify
```

This checks formatting, Clippy, tests, Rustdoc, and package contents. Integration tests can also use
Redis, PostgreSQL, and MySQL; CI defines the required service configuration in
[`ci.yml`](https://github.com/Arion-Dsh/elura/blob/main/.github/workflows/ci.yml).

## License

Elura is dual-licensed under your choice of:

- [Apache License, Version 2.0](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-APACHE)
- [MIT License](https://github.com/Arion-Dsh/elura/blob/main/LICENSE-MIT)
