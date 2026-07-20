# Realtime gameplay primitives

This transport-neutral executable demonstrates the complete Elura realtime primitive set without
opening sockets or starting a World:

- room membership, readiness, and lifecycle;
- Tick synchronization and redundant input recovery;
- deterministic fixed-step input consumption;
- AOI visibility resolved into per-observer replication;
- client prediction reconciliation and remote interpolation;
- predicted entity matching;
- server-side historical rewind queries;
- deterministic latency and packet-loss simulation.

Run it from the Elura repository root:

```bash
cargo run --manifest-path examples/realtime-gameplay/Cargo.toml
```

Run the compile-tested assertions with:

```bash
cargo test --manifest-path examples/realtime-gameplay/Cargo.toml
```

The example deliberately keeps game state small and uses integer movement so the networking and
state-management responsibilities stay visible. A real game supplies serialization, transport
routes, physics, collision, rendering, persistence, and scene placement.
