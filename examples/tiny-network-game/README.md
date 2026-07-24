# Tiny network game: Elura + Spottedcat

This example is a deliberately small authoritative multiplayer arena:

- Elura accepts TCP clients, authenticates demo tickets, routes movement commands, and owns player positions.
- The client uses the checked-in `elura-client` crate in `sdk/rust` for
  connections, authentication, requests, heartbeats, pushes, and reconnect
  tickets.
- Spottedcat opens the native game window and reads WASD/arrow input.
- Remote players use snapshot interpolation; the local player uses input prediction with gentle server reconciliation.
- Green is the local player; pink squares are other connected players.

It uses the published `spottedcat` crate from crates.io; no sibling checkout is required.
The checked-in Rust SDK is a vendored copy of the official
[`elura-sdk-rust`](https://github.com/Arion-Dsh/elura-sdk-rust) repository.

## Rust SDK workspace

The SDK contains the runtime-independent `elura-protocol` crate and
the high-level `elura-client` crate. This application uses the latter:

```toml
elura-client = { path = "sdk/rust/crates/elura-client" }
```

The high-level client owns the TCP stream and ELR2 Session behavior:

```rust
use elura_client::EluraClient;

let client = EluraClient::connect(address, login_ticket).await?;
let snapshot: Snapshot = client.request_protobuf(ROUTE_MOVE, &request).await?;
```

See `src/bin/client.rs` for the game loop integration.

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

## Client stress test

The ignored stress test starts a real in-process Gateway, authenticates the
Rust Client, and keeps a configurable number of requests in flight.
For a short local run:

```bash
ELURA_CLIENT_STRESS_SECONDS=10 \
ELURA_CLIENT_STRESS_CONCURRENCY=1024 \
cargo test --release --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress sustained_sdk_load_against_real_gateway \
  -- --ignored --nocapture
```

Use `ELURA_CLIENT_STRESS_SECONDS=1800` for the 30-minute stability run. The
output includes completed requests, errors, throughput, and bounded-memory
p50/p95/p99 latency estimates.

The second ignored test measures many independent authenticated Client
connections. A 1,000-connection baseline with 100 requests per connection:

```bash
ELURA_CLIENT_CONNECTIONS=1000 \
ELURA_CLIENT_REQUESTS_PER_CONNECTION=100 \
cargo test --release \
  --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress many_sdk_clients_against_real_gateway \
  -- --ignored --nocapture
```

For a 10,000-connection capacity check, use batches to avoid measuring a single
connection storm:

```bash
ELURA_CLIENT_CONNECTIONS=10000 \
ELURA_CLIENT_REQUESTS_PER_CONNECTION=10 \
ELURA_CLIENT_CONNECT_BATCH=200 \
ELURA_CLIENT_CONNECT_RAMP_MS=10 \
cargo test --release \
  --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress many_sdk_clients_against_real_gateway \
  -- --ignored --nocapture
```

Hold 10,000 idle authenticated connections for 30 seconds (including
heartbeats) with:

```bash
ELURA_CLIENT_CONNECTIONS=10000 \
ELURA_CLIENT_REQUESTS_PER_CONNECTION=1 \
ELURA_CLIENT_CONNECT_BATCH=200 \
ELURA_CLIENT_IDLE_SECONDS=30 \
ELURA_CLIENT_CHANNEL_CAPACITY=64 \
cargo test --release \
  --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress many_sdk_clients_against_real_gateway \
  -- --ignored --nocapture
```

For a game-like 10Hz steady load, requests are phase-spread across each tick by
default instead of being emitted as one synchronized burst:

```bash
ELURA_CLIENT_CONNECTIONS=10000 \
ELURA_CLIENT_REQUESTS_PER_CONNECTION=100 \
ELURA_CLIENT_REQUEST_INTERVAL_MS=100 \
cargo test --release \
  --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress many_sdk_clients_against_real_gateway \
  -- --ignored --nocapture
```

The reconnect-storm test stops and restarts the real Gateway on the same port,
waits for every Client state machine to recover, and verifies one request from
each reconnected Client:

```bash
ELURA_CLIENT_RECONNECT_CONNECTIONS=10000 \
ELURA_CLIENT_GATEWAY_RESTART_DELAY_MS=30 \
cargo test --release \
  --manifest-path examples/tiny-network-game/Cargo.toml \
  --test client_stress sdk_clients_reconnect_after_gateway_restart \
  -- --ignored --nocapture
```

## Server logs

Startup and player joins are logged by default. Enable per-command movement logs with:

```bash
RUST_LOG=debug cargo run --manifest-path examples/tiny-network-game/Cargo.toml --bin server
```
