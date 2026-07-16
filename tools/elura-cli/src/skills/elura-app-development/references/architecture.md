# Elura application architecture

## Runtime choice

| Shape | Choose it when | Assembly |
| --- | --- | --- |
| Monolith | Local development, small deployments, or one-process operation | `Monolith::new(gateway, world).transport(...).install(...).run(admin)` |
| Gateway + World | Connections and game logic need independent scaling or failure domains | `Gateway::new(...).transport(...).world_discovery(...)` and `World::new(...).registrar(...)` |
| Embedded/custom | The application supplies transports, discovery, storage, or lifecycle integration | Configure the public traits and call `build`/`serve` APIs explicitly |

Gateway owns client transports, tickets, sessions, admission, routing to Worlds, and connection-oriented push delivery. World owns typed commands, modules, middleware, player state, and game behavior. Do not move business rules into Gateway interceptors merely because requests pass through Gateway.

Monolith uses the same Gateway and World configuration models but connects them in process. Standalone World networking, internal authorization, and World TLS are not started by `Monolith`.

## Cargo feature selection

The `elura` facade defaults to `gateway` and `world`. Enable only the capabilities the application uses:

| Feature | Adds |
| --- | --- |
| `monolith` | Combined single-process runtime |
| `adapters` | Adapter facade without selecting Redis/SQL/Kubernetes implementations |
| `redis` | Redis-backed distributed implementations |
| `sql` | SQL-backed implementations |
| `kubernetes` | Kubernetes discovery/ownership implementations |
| `admin` | Adapter administration support |
| `identity`, `otp`, `notification-alisms` | Identity and messaging provider surfaces |
| `payment-*` | One payment provider integration |
| `full` | All optional integrations; avoid by default in applications |

Confirm the feature list in the pinned `elura` crate version before changing it.

## Ownership boundaries

The upper application owns:

- configuration file and environment loading;
- secret acquisition and rotation policy;
- adapter/provider choice and credentials;
- route IDs, protobuf namespaces, compatibility, and migrations;
- dependency injection and business state;
- deployment topology and observability policy.

Elura owns protocol/session invariants, runtime orchestration, typed route dispatch, transport implementations, and the extension contracts exposed through the facade.

## Configuration flow

Deserialize an application-level config that embeds `GatewayConfig`, `WorldConfig`, transport configs, `AdminServerConfig`, and application settings. Load secrets separately, merge environment overrides explicitly, validate addresses and required values, then construct the runtime.

For distributed mode, configure matching internal tokens and TLS expectations on Gateway and World. Add World discovery to Gateway and registration to World. Add ownership/admission/account-version components only as required by the deployment; do not silently choose an infrastructure backend in shared application code.

## Public API navigation

- Common types: `elura::prelude`
- World APIs: `elura::world`, including `middleware`, `player`, and `testing`
- Gateway APIs: `elura::gateway`, `elura::transport`, `elura::protection`
- Runtime administration/security: `elura::observability`, `elura::security`, `elura::launch`
- Distributed contracts: `elura::discovery`
- Infrastructure implementations: `elura::adapters`
- External identity/payment/notification implementations: `elura::providers`
