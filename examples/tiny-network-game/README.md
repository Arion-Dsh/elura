# Tiny network game: Elura + Spottedcat

This example is a deliberately small authoritative multiplayer arena:

- Elura accepts TCP clients, authenticates demo tickets, routes movement commands, and owns player positions.
- The client uses the generated, standalone Rust SDK in `sdk/rust` for ELR2 framing and authentication.
- Spottedcat opens the native game window and reads WASD/arrow input.
- Remote players use snapshot interpolation; the local player uses input prediction with gentle server reconciliation.
- Green is the local player; pink squares are other connected players.

It uses the published `spottedcat` crate from crates.io; no sibling checkout is required.
The checked-in Rust SDK is generated with:

```bash
cargo run -p elura-cli -- init sdk --language rust --dir examples/tiny-network-game
```

## Rust SDK stream integration

The SDK keeps Tokio support optional. This application enables its `tokio-codec`
feature and depends directly on `tokio-util`:

```toml
elura-protocol = { path = "sdk/rust", features = ["tokio-codec"] }
tokio-util = { version = "0.7.18", features = ["codec"] }
```

The client wraps `TcpStream` with the SDK codec:

```rust
let stream = TcpStream::connect(address).await?;
let mut connection = Framed::new(stream, Elr2Codec::default());

connection
    .send(EluraProtocol::authenticate(1, login_ticket)?)
    .await?;
let response = connection.next().await;
```

See `src/bin/client.rs` for authentication, request timeouts, reconnects, and
application-route calls.

## Run

From the `horizon-rs` repository root, start the server:

```bash
cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin server
```

Then open two more terminals:

```bash
cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin client -- 1
cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin client -- 2
```

Move either square with WASD or the arrow keys. The server listens on
`127.0.0.1:17000`; its local admin endpoint uses port `17001`.

Both binaries accept an optional address, which is useful for another machine on the LAN:

```bash
cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin server -- 0.0.0.0:17000
cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin client -- 3 192.168.1.20:17000
```

The hard-coded ticket key is intentionally only for this local example. A real game should issue
short-lived login tickets from a trusted login service and load the Gateway key from a secret.

## Server logs

Startup and player joins are logged by default. Enable per-command movement logs with:

```bash
RUST_LOG=debug cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin server
```
