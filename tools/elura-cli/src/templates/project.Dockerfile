FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bins

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /src/target/release/gateway /app/gateway
COPY --from=builder /src/target/release/world /app/world
USER app
