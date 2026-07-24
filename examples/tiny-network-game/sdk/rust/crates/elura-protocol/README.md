# elura-protocol

Runtime-independent ELR2 v2 framing, authentication and reconnect
payloads, heartbeat helpers, error envelopes, and Session Control encoding for
Elura clients.

Enable the optional `tokio-codec` feature when integrating ELR2 with a Tokio
byte stream. Use the sibling `elura-client` crate for the complete high-level
TCP client.

Frame encode/decode benchmarks for 64-byte, 1-KiB, and 64-KiB payloads are
included:

```sh
cargo bench -p elura-protocol --bench protocol
```
