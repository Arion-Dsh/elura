# Application Configuration

The generated JSON files are safe, non-secret development defaults and can be used directly.
Secrets remain in `elura.env.example`; copy it to the ignored `config/elura.env`, replace every
placeholder, and load it into each process before starting:

```bash
cp config/elura.env.example config/elura.env
# Edit config/elura.env, then in two terminals:
set -a; . config/elura.env; set +a
cargo run --bin world

set -a; . config/elura.env; set +a
cargo run --bin gateway
```

Use `cargo run --bin monolith` instead when the monolith scaffold has been generated.

The generated `gateway.rs`, `world.rs`, and `monolith.rs` files define the application's own
`AppConfig`. Elura does not read environment variables, JSON/YAML files, configuration services,
or secrets, and it does not install Redis components automatically. Applications may replace
`AppConfig::load()`, but should preserve these composition boundaries:

1. `AppConfig` combines application settings, `elura-runtime` settings, and the configuration for
   each selected adapter.
2. The application reads configuration and secrets, then passes sensitive values to the relevant
   configuration objects.
3. Each adapter is constructed from its own configuration.
4. The application injects components through the fluent `Gateway` and `World` APIs. Reusable
   infrastructure composition modules may bundle related services with `GatewayInfrastructure`;
   normal application startup should use the fluent methods directly.

The generated Gateway example uses `DnsWorldDiscovery::new(config)`. For Kubernetes, construct
`KubernetesWorldDiscovery::new(EndpointWatcherConfig { ... })?`. For VMs or bare-metal deployments,
use `RedisWorldDiscovery::connect(redis_url, config)` and
`RedisWorldRegistrar::connect(redis_url, world_id, config)`. Provider selection belongs to
the application; `GatewayConfig` contains only protocol-neutral Session and runtime settings.
TCP, UDP, WebSocket, WebTransport, QUIC, and future client protocols own their configuration
independently.

On shutdown, World first becomes not ready and unregisters, then waits
`runtime.discovery_drain_delay` before closing its traffic listeners and draining in-flight
requests. Set this delay to at least the service-discovery refresh or propagation interval, with a
small safety margin. Keep the platform termination grace period longer than this delay plus
`runtime.shutdown_timeout`.

To enable the optional QUIC listener, add a top-level `quic` object in `gateway.json` or
`monolith.json` and mount the certificate files into the container:

```json
"quic": {
  "listen": "0.0.0.0:17003",
  "certificate_file": "/run/secrets/gateway-cert.pem",
  "key_file": "/run/secrets/gateway-key.pem"
}
```

Expose the same port as UDP in Docker, Kubernetes Service, and NetworkPolicy configuration. QUIC
uses the `elura.v2` ALPN identifier by default and carries one ELR2 session on the first
client-initiated bidirectional stream.

To enable the optional best-effort UDP listener, add a top-level `udp` object. Each datagram must
contain exactly one complete ELR2 frame, and each source `IP:port` is treated as a Session:

```json
"udp": {
  "listen": "0.0.0.0:17004",
  "max_datagram_bytes": 1200,
  "max_sessions": 4096,
  "per_session_queue": 64
}
```

Expose this port as UDP. Keep latency-sensitive, replaceable messages on UDP; use TCP, WebSocket,
or QUIC when the application requires reliable and ordered delivery.

To accept browser WebTransport connections, configure a separate HTTP/3 endpoint and expose its
port as UDP:

```json
"webtransport": {
  "listen": "0.0.0.0:17005",
  "path": "/elura/game",
  "allowed_origins": ["https://game.example.com"],
  "certificate_file": "/run/secrets/gateway-cert.pem",
  "key_file": "/run/secrets/gateway-key.pem",
  "mode": "reliable_stream"
}
```

In `reliable_stream` mode, the browser must open one bidirectional stream and carry the normal
ELR2 byte stream on it. In `datagram` mode, use `transport.datagrams`; every Datagram must contain
exactly one ELR2 frame and delivery may be lost or reordered. WebTransport always uses TLS, and
browser Origin requests are same-origin by default unless `allowed_origins` explicitly permits
them.

Redis replay protection, the online directory, push, session control, rate limiting, bans, OTP,
and the outbox are independent components. Construct and inject only the components the application
needs. When using Redis Cluster, the application must ensure that related components use compatible
seeds, prefixes, and deployment constraints. Gateway and World do not start a Redis outbox
dispatcher automatically.

Never commit real secrets. Ticket keys and internal tokens must contain at least 32 bytes and use
different random values. An admin endpoint listening on a non-loopback address requires a token of
at least 32 bytes. Health and readiness checks do not require a token; metrics and debugging
endpoints require authentication.
