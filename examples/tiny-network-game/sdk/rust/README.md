# Elura SDK for Rust

This repository is the official Rust SDK workspace for Elura. It contains two
separately publishable crates:

- `elura-protocol`: runtime-independent ELR2 v2 types, framing,
  authentication and reconnect payloads, heartbeat helpers, errors, and
  Session Control encoding.
- `elura-client`: high-level Tokio TCP client built on `elura-protocol`.

## Quick start

Use `elura-client` when the application needs a ready-to-use Gateway client:

```toml
[dependencies]
elura-client = "0.2.10"
```

```rust
use elura_client::EluraClient;

let client = EluraClient::connect(gateway_address, login_ticket).await?;
let response = client.request(100, application_payload).await?;
```

Typed Protobuf requests can be sent directly:

```rust
let snapshot: Snapshot = client
    .request_protobuf(100, &MoveRequest { dx: 1, dy: 0 })
    .await?;
```

The client runs the socket, heartbeats, ticket renewal, response correlation, and
reconnect state machine in one background Tokio task. `EluraClient` is a cheap,
cloneable handle, so requests and event handling can run concurrently:

```rust
let mut events = client.subscribe();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        handle(event);
    }
});

let response = client.request(100, application_payload).await?;
```

Each subscriber receives its own bounded broadcast stream. A subscriber that
cannot keep up receives Tokio's `Lagged` error instead of applying unbounded
memory pressure.

Reconnect manually with the latest automatically rotated ticket when automatic
reconnect is disabled:

```rust
client.reconnect().await?;
```

Transport loss is handled by an internal connection state machine with bounded
exponential backoff and per-client jitter to avoid synchronized reconnect
storms. An in-flight application request is never replayed automatically; it
returns `ClientError::RequestInterrupted` after the connection is lost,
allowing the application to decide whether that operation is safe to retry.
New requests are accepted after the state returns to
`ConnectionState::Connected`.

Connection lifecycle changes are available through the event subscription:

```rust
while let Ok(event) = events.recv().await {
    match event {
        ClientEvent::Reconnected => resume_gameplay(),
        ClientEvent::ReauthenticationRequired => {
            let login_ticket = fetch_fresh_login_ticket().await?;
            client.reauthenticate(login_ticket).await?;
        }
        event => handle(event),
    }
}
```

Use `subscribe_state()` when code only needs to wait for a state transition.
`ClientConfig` exposes bounded command, event, and in-flight request capacities
for application-specific backpressure tuning. Command and event queues default
to 64 entries; `reconnect_jitter_percent` defaults to 20.

## Protocol-only use

Use `elura-protocol` when integrating another transport or async runtime:

```toml
[dependencies]
elura-protocol = "0.2.10"
```

```rust
use elura_protocol::{Elr2Codec, Elr2Frame};

let request = Elr2Frame::request(100, request_id, payload)?;
let bytes = Elr2Codec::encode(&request)?;
```

## Development

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo bench --workspace
```
