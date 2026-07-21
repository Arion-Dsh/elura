# Elura internal load generator

> **Framework-internal tool.** This package is `publish = false` and is not an application
> dependency or part of Elura's supported upper-layer API.

Elura maintainers use `elura-load` to detect framework connection, authentication, transport,
Gateway routing, and request-latency regressions against a running deployment. It supports TCP,
UDP, WebSocket, QUIC, and WebTransport and keeps their reports separate.

Application projects should use `WorldHarness` for business unit tests and `elura-testkit` for
local full-stack business scenarios. Use an application-owned load platform when measuring a
deployed application's business capacity.

Run `cargo run -p elura-load -- --help` from the Elura workspace for internal options.
