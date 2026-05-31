# NOTE: Use Debian/glibc for now.
# Verification path for future reevaluation:
# - `sqlite-vec` currently fails to compile on Alpine/musl with fixed-width type
#   errors (`u_int8_t`/`uint8_t`) in its C source.
# - Re-check by running:
#   `docker compose --profile tei build ask-server` with the former musl target
#   (`rust:*-alpine` + `--target x86_64-unknown-linux-musl`).
# - If that upstream issue is fixed (or if sqlite-vec is removed/migrated away),
#   we can reconsider Alpine to reduce image size.
FROM rust:1.86-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ask-core ./crates/ask-core
COPY crates/ask-server ./crates/ask-server

RUN cargo build --locked --release --quiet -p ask-server --bin ask-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/ask-server /usr/local/bin/ask-server

ENV ASK_SERVER_DATA_DIR=/data
ENV ASK_SERVER_RESOURCE_DIR=/resources

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
    CMD wget -qO- http://127.0.0.1:3000/health >/dev/null || exit 1

CMD sh -c 'echo "http://localhost:${ASK_SERVER_EXPOSE_PORT:-13000}" && exec ask-server'
