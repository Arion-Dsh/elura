# Tiny network game: Elura + Spottedcat

This example is a deliberately small authoritative multiplayer arena:

- Elura accepts TCP clients, authenticates demo tickets, routes movement commands, and owns player positions.
- Spottedcat opens the native game window and reads WASD/arrow input.
- Remote players use snapshot interpolation; the local player uses input prediction with gentle server reconciliation.
- Green is the local player; pink squares are other connected players.

It uses the published `spottedcat` crate from crates.io; no sibling checkout is required.

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
