# Elura

Elura is a Rust framework for online game servers. It provides gateways, world servers,
sessions, routing, real-time communication, infrastructure adapters, and third-party
service integrations.

The project is currently under active `0.x` development, and its API may change.

Gateway and World are independent runtime crates in one Cargo workspace. They share
protocol and discovery contracts through `elura-core`, while `elura-monolith` is the
only composition crate that depends on both concrete runtimes.

## Workspace

| Crate | Purpose |
| --- | --- |
| `elura` | Unified entry point for applications |
| `elura-core` | Protocols, sessions, routing, and cross-process contracts |
| `elura-runtime` | Shared lifecycle, security, admin, and observability runtime |
| `elura-gateway` | Client-facing Gateway runtime |
| `elura-world` | World command and player-state runtime |
| `elura-monolith` | Single-process Gateway and World assembly |
| `elura-adapters` | Redis, SQL, and Kubernetes adapters |
| `elura-providers` | Identity, messaging, and payment providers |
| `elura-cli` | Project scaffolding tool |
| `elura-load` | Connection and request load-testing tool |

## Usage

Install the project generator from crates.io:

```bash
cargo install elura-cli
```

Then run it in the application directory:

```bash
elura init all --dir .
```

The `all` target creates a compilable Rust project, local runtime configuration,
a multi-stage Dockerfile, Docker Compose services, and Kubernetes manifests.

Inspect the files that would be generated before writing them:

```bash
elura init all --dir . --dry-run
```

Generate the ELR2 Gateway-to-client protocol SDKs for C++, C#, and TypeScript:

```bash
elura init sdk --dir .
# Or select one: --language cpp, csharp, or typescript
```

Run `elura --help` for the target list, or `elura init route --help` for
target-specific options. Existing files are preserved unless `--force` is provided.

## Development

```bash
make verify
```

This runs formatting, Clippy, tests, Rustdoc, and package validation.

## Documentation

Detailed integration, protocol, deployment, and operations documentation will be maintained
in a separate GitHub Pages project alongside this repository. This repository only keeps the
brief information required for development and the Rustdoc embedded in the source code.

## License

[MIT](LICENSE)
