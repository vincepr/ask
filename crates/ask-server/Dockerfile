FROM rust:1.85-alpine AS builder

RUN apk add --no-cache build-base musl-dev
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ask-core ./crates/ask-core
COPY crates/ask-server ./crates/ask-server

RUN cargo build --locked --release --quiet --target x86_64-unknown-linux-musl -p ask-server --bin ask-server

FROM alpine:3.22

RUN apk add --no-cache ca-certificates wget

WORKDIR /app
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/ask-server /usr/local/bin/ask-server

ENV ASK_SERVER_DATA_DIR=/data
ENV ASK_SERVER_RESOURCE_DIR=/resources

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
    CMD wget -qO- http://127.0.0.1:3000/health >/dev/null || exit 1

CMD sh -c 'echo "http://localhost:${ASK_SERVER_EXPOSE_PORT:-13000}" && exec ask-server'
