FROM rust:1.94-slim-bookworm AS builder

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p elura-perf -p elura-load \
    && mkdir /out \
    && cp /src/target/release/elura-perf-gateway /out/ \
    && cp /src/target/release/elura-perf-world /out/ \
    && cp /src/target/release/elura-load /out/

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/elura-perf-gateway /app/elura-perf-gateway
COPY --from=builder /out/elura-perf-world /app/elura-perf-world
COPY --from=builder /out/elura-load /app/elura-load
