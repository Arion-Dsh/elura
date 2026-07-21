# Elura performance environment

This directory contains an isolated performance-only HAProxy, two Gateways, one World, Redis,
load generator, and Docker Compose definition. The Gateways share Redis-backed ticket replay and
online-session state. None of these binaries are published with the runtime crates.

```bash
docker compose -f tools/elura-perf/compose.yml build
docker compose -f tools/elura-perf/compose.yml up -d redis world gateway-1 gateway-2 load-balancer

docker compose -f tools/elura-perf/compose.yml --profile load run --rm load \
  --address load-balancer:17000 --connections 1000 --requests 100 --route 1000 \
  --batch-size 200 --ramp-ms 100

docker compose -f tools/elura-perf/compose.yml --profile load run --rm load \
  --address load-balancer:17000 --connections 10000 --requests 100 --route 1000 \
  --batch-size 200 --ramp-ms 100
```

To run the load generator from another host, expose HAProxy on the Docker host and point a local
`elura-load` binary at it:

```bash
ELURA_PERF_PORT=17000 docker compose -f tools/elura-perf/compose.yml \
  -f tools/elura-perf/compose.host.yml up -d redis world gateway-1 gateway-2 load-balancer

ELURA_LOAD_TICKET_KEY=elura-rs-perf-ticket-key-at-least-32-bytes-2026 \
  cargo run --release -p elura-load -- --address DOCKER_HOST:17000 \
  --connections 1000 --requests 100 --route 1000 --batch-size 200 --ramp-ms 100
```

Each Gateway uses a 32-connection Gateway-to-World pool and an in-flight limit of 64 commands per
internal connection. The performance configuration disables the shared per-source-IP request
limit with `0/0` because every load connection originates from one load container; normal runtime
defaults keep this protection enabled. The application route is an echo handler with no artificial
delay.

Both Gateway-to-World limits are configurable without rebuilding the image:

```bash
ELURA_WORLD_POOL_SIZE=16 ELURA_WORLD_IN_FLIGHT=64 \
  docker compose -f tools/elura-perf/compose.yml up -d --force-recreate gateway-1 gateway-2
```

`ELURA_WORLD_POOL_SIZE` accepts `1..=1024`, and `ELURA_WORLD_IN_FLIGHT` accepts `1..=4096`.
The latter is also applied to the World service when it is recreated. Other `ELURA_*` values in
`compose.yml` can likewise be overridden for focused benchmarks. Applications using dynamic World
discovery can set the same limits through `GatewayConfig.world_routing.pool_size` and
`GatewayConfig.world_routing.max_in_flight_per_connection`.
