# syntax=docker/dockerfile:1

# ---- build stage ----
# Same image as the CI test jobs (see .forgejo/workflows/ci.yaml).
# The -bookworm suffix pins the Debian suite, not just the compiler: the
# binary links against this base's glibc, and the runtime stage below is
# bookworm-slim. A moving default base would break the image at startup
# ("GLIBC_x.y not found"), not at build time.
FROM rust:1.98-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Cache mounts keep the cargo registry/git checkouts and the build tree warm
# between runs (the Forgejo packaging job builds against the host daemon, so
# the caches persist there). The finished binary is copied out of the cache
# mount into a real layer - files written inside a cache mount do not land
# in the image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/versa-bot /usr/local/bin/versa-bot

# ---- runtime stage ----
FROM debian:bookworm-slim

# TLS roots for the Discord gateway/API.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user; /data is the default home for the SQLite file.
# /app stays writable so `sqlite://versabot.db` (CWD-relative) also works.
RUN useradd --system --create-home --home-dir /app versa \
    && mkdir -p /data \
    && chown -R versa:versa /app /data

WORKDIR /app
COPY --from=build /usr/local/bin/versa-bot versa-bot
# The sqlx migrator loads from the CWD-relative ./migrations.
COPY migrations migrations
COPY versabot.example.toml versabot.example.toml

USER versa
# EnvFilter falls back to "info" when unset; set explicitly for discoverability.
ENV RUST_LOG=info
VOLUME /data

ENTRYPOINT ["/app/versa-bot"]
