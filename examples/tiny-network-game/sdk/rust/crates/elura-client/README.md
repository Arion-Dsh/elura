# elura-client

High-level Tokio TCP client for Elura. A single background task owns the socket,
correlates concurrent application requests and responses, answers Gateway
heartbeats, broadcasts pushes and Session Control events, automatically rotates
reconnect tickets before expiry, and reconnects with the latest ticket.

`EluraClient` is a cheap cloneable handle. Use `subscribe()` for events and
`subscribe_state()` for connection state changes. Both command and event paths
are bounded, and `max_in_flight_requests` prevents unbounded pending requests.

Transport loss starts bounded exponential-backoff reconnection automatically.
Reconnect deadlines include per-client jitter so a Gateway restart does not
make every Client reconnect simultaneously. Interrupted application requests
are not replayed. If the reconnect ticket is expired, consumed, or revoked, the Client emits
`ClientEvent::ReauthenticationRequired`; provide a fresh login ticket through
`EluraClient::reauthenticate`.

The command and event queues default to 64 entries. Tune `command_capacity`,
`event_capacity`, `max_in_flight_requests`, and `reconnect_jitter_percent`
through `ClientConfig` when a workload needs different backpressure or
reconnection behavior.

The crate re-exports `elura-protocol`, so most applications need only one
dependency:

```toml
[dependencies]
elura-client = "0.2.10"
```

Run the loopback concurrency benchmark with:

```sh
cargo bench -p elura-client --bench client
```
