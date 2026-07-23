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
- One-login HTTP access/refresh tokens with one-time ELR2 Session-ticket exchange.
- Rooms, fixed-step simulation, AOI, replication, prediction, and lag compensation.
- Optional Redis, SQL, Kubernetes, identity, notification, OTP, and payment integrations.

## HTTP APIs alongside realtime connections

HTTP business APIs and ELR2 connections use separate credentials derived from
one application login:

```text
POST /elura/auth/login
  -> reusable HTTP access token
  -> rotating refresh token
  -> optional one-time Gateway login ticket

HTTP business request
  -> Authorization: Bearer <access token>

ELR2 connection
  -> AUTHENTICATE(<Gateway login or reconnect ticket>)
```

[`HttpAuthApi`](https://docs.rs/elura/latest/elura/gateway/http_auth/struct.HttpAuthApi.html)
provides `/elura/auth/login`, `/elura/auth/refresh`, and `/elura/game/session-ticket`. With both the
`gateway` and `identity` features enabled, `IdentityHttpBackend` connects the built-in
`IdentityService` and its password, phone, OAuth2, WeChat, Douyin, and QuickSDK providers directly
to this API. Applications implement only `IdentityHttpPolicy` to grant scopes and verify that a
selected player belongs to the authenticated account. Custom identity systems can still implement
`HttpLoginBackend` directly. `HttpBearerAuth` protects application-owned Axum routes such as
payments, while the existing TCP, WebSocket, QUIC, and WebTransport Session flow remains
unchanged.

```rust,ignore
let login_backend = Arc::new(IdentityHttpBackend::new(
    identity_service,
    Arc::new(GameIdentityPolicy::new(account_store)),
));
let auth_api = HttpAuthApi::new(http_tokens, tickets, shared_replay, login_backend);
```

The adapter performs existing-account login once. Registration and linking remain explicit
application flows, which prevents one-time third-party authorization codes from being consumed
twice by a fallback login-then-register sequence.

Refresh rotation uses the same `ReplayStore` contract as Gateway tickets. Inject a shared Redis
implementation before running multiple HTTP or Gateway instances.

Mount the authentication API and protected business routes next to an existing realtime
transport:

```rust,ignore
let payments = Router::new()
    .route("/elura/payments/orders", post(create_order))
    .route_layer(middleware::from_fn_with_state(
        auth_api.bearer_auth(),
        require_bearer,
    ));

Gateway::new(gateway_config)
    .replay_store(shared_replay.clone())
    .transport(TcpTransport::new(tcp_config)?)
    .http("0.0.0.0:8080", auth_api.router().merge(payments))
    .run(admin_config)
    .await?;
```

The HTTP listener and ELR2 listener are supervised together, but their authentication semantics
remain independent. Payment handlers receive `AuthenticatedHttp`; realtime handlers continue to
receive the server-owned ELR2 `Session` identity.

## Quick start

Install the CLI and scaffold an application:

```bash
cargo install elura-cli --version 0.2.10
elura skill install
elura init all --dir .
```

Or add the framework directly:

```toml
[dependencies]
elura = "0.2.10"
```

The skill is installed in `.agents/skills/elura-app-development` for project-level coding agents.
See the [documentation](https://elura.rustyspottedcat.dev/) for concepts, configuration, crate
features, deployment, and tutorials.

## Client SDKs

Generate standalone ELR2 client SDKs without depending on the server-side Elura crates:

```bash
elura init sdk --language rust --dir .
elura init sdk --language typescript --dir .
elura init sdk --language csharp --dir .
elura init sdk --language cpp --dir .
```

The Rust SDK is runtime-independent by default. Its frame encoding, authentication, reconnect,
heartbeat, and Session Control APIs work without Tokio. WebSocket, UDP, and QUIC Datagram clients
can use `Elr2Codec::encode` and `Elr2Codec::decode` directly.

Enable the optional `tokio-codec` feature when a Tokio byte stream needs ELR2 framing:

```toml
[dependencies]
elura-protocol = { path = "sdk/rust", features = ["tokio-codec"] }
futures-util = { version = "0.3", features = ["sink"] }
tokio = { version = "1", features = ["net", "macros", "rt-multi-thread"] }
tokio-util = { version = "0.7", features = ["codec"] }
```

```rust,ignore
use elura_protocol::Elr2Codec;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

let stream = TcpStream::connect(gateway_address).await?;
let connection = Framed::new(stream, Elr2Codec::default());
```

See [`tiny-network-game`](examples/tiny-network-game) for authentication, request timeouts,
reconnects, and application-route calls over `Framed<TcpStream, Elr2Codec>`.

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
