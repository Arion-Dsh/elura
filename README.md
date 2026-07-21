<div align="center">

# Elura

**A modular Rust framework for online game servers.**

[![CI](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml/badge.svg)](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/elura.svg)](https://crates.io/crates/elura)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-dea584.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

[Documentation](https://elura.rustyspottedcat.dev/) · [API reference](https://docs.rs/elura) · [Crates.io](https://crates.io/crates/elura)

</div>

Elura separates client connections from game logic. Gateway processes own connections and
sessions; World processes execute commands and manage player state. They can scale independently
or run together as a monolith.

## Features

- TCP, UDP, WebSocket, WebTransport, and QUIC transports.
- Distributed Gateway and World deployment, or a single-process monolith.
- Rooms, fixed-step simulation, AOI, replication, prediction, and lag compensation.
- Optional Redis, SQL, Kubernetes, identity, notification, OTP, and payment integrations.

## Quick start

Install the CLI and scaffold an application:

```bash
cargo install elura-cli --version 0.2.5
elura skill install
elura init all --dir .
```

Or add the framework directly:

```toml
[dependencies]
elura = "0.2.5"
```

The skill is installed in `.agents/skills/elura-app-development` for project-level coding agents.
See the [documentation](https://elura.rustyspottedcat.dev/) for concepts, configuration, crate
features, deployment, and tutorials.

## Examples

- [`tiny-network-game`](examples/tiny-network-game): an authoritative multiplayer movement demo.
- [`realtime-gameplay`](examples/realtime-gameplay): a transport-neutral gameplay pipeline walkthrough.

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
