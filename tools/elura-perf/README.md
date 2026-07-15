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

Each Gateway uses a 16-connection Gateway-to-World pool and an in-flight limit of 64 commands per
internal connection. The performance configuration disables the shared per-source-IP request
limit with `0/0` because every load connection originates from one load container; normal runtime
defaults keep this protection enabled. The application route is an echo handler with no artificial
delay. Override the `ELURA_*` environment variables in `compose.yml` when testing other pool sizes,
concurrency limits, or handler delays.
