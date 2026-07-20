---
name: elura-app-development
description: Build, extend, and test upper-layer Rust game services on the Elura framework. Use when an agent works in an Elura application on World routes or modules, scenes, rooms, AOI, fixed-step simulations, realtime netcode, client prediction/interpolation, entity replication, lag compensation, network simulation, Gateway or Monolith assembly, middleware, player state, push events, application configuration, adapters/providers, deployment scaffolds, or Elura-focused tests and debugging.
---

# Elura App Development

Build application behavior through Elura's public facade and generated project conventions.

## Establish the local contract

1. Inspect `Cargo.toml`, `Cargo.lock`, existing application modules, configuration, and generated entry points before editing.
2. Treat the installed Elura version as authoritative. Do not assume APIs from another release.
3. Prefer `elura::prelude::*` for common application types and explicit `elura::world`, `elura::gateway`, `elura::adapters`, or `elura::providers` imports when ownership should stay visible.
4. When the framework source is present, inspect the `elura` facade exports and CLI templates. In a downstream application, use rustdoc and compiler feedback to confirm less-common APIs.

Read [references/architecture.md](references/architecture.md) when choosing runtime topology, Cargo features, integration boundaries, or configuration ownership. Read [references/implementation.md](references/implementation.md) when implementing routes, modules, middleware, push, player state, or tests.

## Start from the CLI scaffold

Use the smallest generator matching the task:

```bash
elura init all --dir .
elura init module --name inventory --dir .
elura init route --module inventory --name list_items --id 120 --dir .
elura init sdk --language typescript --dir .
```

Run with `--dry-run` before generating into a non-empty project. Do not use `--force` until the conflicting files have been inspected and overwriting them is intended.

Adapt generated code instead of recreating framework bootstrapping from memory. Keep application policy, configuration loading, adapter selection, and secrets in the upper application.

## Implement application behavior

1. Model every client command as a typed `Route` with stable, unique ID `>= 100`, a stable dotted name, and Prost request/response messages.
2. Put cohesive routes in a `WorldModule`; mount each route through `WorldModuleRegistry` and install the module on `World` or `Monolith`.
3. Keep handlers focused on game behavior. Take authenticated identity and request metadata from `WorldContext`; inject application services through captured `Arc` values, middleware context values, or typed transactions.
4. Use middleware for cross-cutting behavior. Call `next.run(context, payload).await` exactly once unless short-circuiting intentionally.
5. Use typed `Event` and `WorldContext` push methods for server-initiated messages. Configure a push transport in distributed deployments.
6. Use `elura::world::scene` only when a stateful scene needs a serial mailbox or tick lifecycle. Keep placement, persistence, recovery, and game rules in the application.
7. Compose the opt-in `elura::room`, `elura::aoi`, `elura::simulation`, `elura::netcode`, `elura::replication`, and `elura::lag_compensation` primitives around application-owned scenes and client simulations. Use `elura::net_sim` only in tests or local development; do not treat these primitives as networked services or persistence layers.
8. Assemble `Gateway`, `World`, or `Monolith` with explicit transports and infrastructure. Let `build`/`run` surface deferred configuration and duplicate-registration errors.

## Preserve framework boundaries

- Keep Gateway connection/session concerns separate from World game logic.
- Prefer public `elura` APIs over direct dependencies on internal crates.
- Never hard-code production secrets. Load them from environment or a secret provider and merge them into deserialized application config.
- Use `#[serde(deny_unknown_fields)]` for application config where rejecting stale keys is desirable.
- Avoid blocking I/O and holding unrelated locks across `.await`.
- Do not assign a new route ID without checking the project's existing route/proto registry.
- Do not invent configuration fields or adapter constructors; verify them against the pinned version.

## Verify proportionally

Run focused tests while iterating, then the application checks:

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
```

For World business logic, prefer `World::build()?.harness()` and typed harness calls over opening network listeners. Add tests for success, invalid requests, authorization/identity rules, duplicate registrations, middleware effects, and relevant state changes. Use integration tests for transport, distributed adapter, TLS, or lifecycle behavior that the harness cannot cover.

Finish by reporting changed behavior, public contracts introduced or changed, configuration/migration requirements, and the exact verification performed.
