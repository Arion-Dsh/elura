# Elura protocol for Rust

`elura-protocol` is a standalone Rust implementation of the public Elura client
ELR2 v2 contract. It includes frame encoding, Tokio stream
framing, authentication and reconnect payloads, error envelopes, heartbeat
handling, and Session Control protobuf encoding.

It does not open sockets or dispatch application routes, and it does not depend
on the server-side `elura` crates.

## Quick start

```rust
use elura_protocol::{Elr2Codec, EluraProtocol};
use tokio_util::codec::Framed;

let connection = Framed::new(stream, Elr2Codec::default());
let request = EluraProtocol::authenticate(next_request_id, login_ticket)?;
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
`Elr2Codec::decode`. For TCP or QUIC streams, use `Elr2Codec` with
`tokio_util::codec::Framed`.

Retain only the latest reconnect ticket, renew it before `expires_in_seconds`,
and replace it with the ticket returned by the renewal response.

Run the golden-vector tests:

```sh
cargo test
```
