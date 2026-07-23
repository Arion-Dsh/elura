# Elura protocol for Rust

`elura-protocol` is a standalone Rust implementation of the public Elura client
ELR2 v{{ELR2_VERSION}} contract. It includes frame encoding, authentication and
reconnect payloads, error envelopes, heartbeat handling, and Session Control
protobuf encoding. Optional Tokio stream framing is available through the
`tokio-codec` feature.

It does not open sockets or dispatch application routes, and it does not depend
on the server-side `elura` crates or an async runtime by default.

## Quick start

```rust
use elura_protocol::{Elr2Codec, EluraProtocol};

let request = EluraProtocol::authenticate(next_request_id, login_ticket)?;
let message = Elr2Codec::encode(&request)?;
```

Application routes start at `EluraRoutes::FIRST_APPLICATION`:

```rust
use elura_protocol::Elr2Frame;

let request = Elr2Frame::request(100, next_request_id, application_payload)?;
```

Reply to a server heartbeat with:

```rust
let response = EluraProtocol::heartbeat_response(&heartbeat)?;
```

For WebSocket and datagram transports, use `Elr2Codec::encode` and
`Elr2Codec::decode`.

Tokio applications can enable the optional stream adapter:

```toml
[dependencies]
elura-protocol = { version = "{{ELURA_VERSION}}", features = ["tokio-codec"] }
futures-util = { version = "0.3", features = ["sink"] }
tokio = { version = "1", features = ["net", "macros", "rt-multi-thread"] }
tokio-util = { version = "0.7", features = ["codec"] }
```

Then use `Elr2Codec` with `tokio_util::codec::Framed` for TCP or QUIC streams:

```rust
use elura_protocol::{Elr2Codec, EluraProtocol};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

let stream = TcpStream::connect(gateway_address).await?;
let mut connection = Framed::new(stream, Elr2Codec::default());

connection
    .send(EluraProtocol::authenticate(1, login_ticket)?)
    .await?;
let response = connection
    .next()
    .await
    .ok_or_else(|| std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "server closed",
    ))??;
let authenticated = EluraProtocol::decode_authenticate(&response)?;
```

Retain only the latest reconnect ticket, renew it before `expires_in_seconds`,
and replace it with the ticket returned by the renewal response.

Run the golden-vector tests:

```sh
cargo test
cargo test --features tokio-codec
```
