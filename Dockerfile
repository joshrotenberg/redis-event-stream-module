# syntax=docker/dockerfile:1

# Preloaded image (issue #102): build the module, build the server from the
# CI-pinned source, and assemble a slim runtime that starts a server with the
# module already loaded, so `docker run <image>` just works.
#
# Why from source rather than FROM redis:8 / valkey/valkey:8: the module builds
# and loads on every vanilla Redis 7.2+/Valkey 8+ the CI integration matrix
# covers (SPEC.md section 14), but the official Redis Ltd `redis:8` image ships
# a build that rejects the module's config initialization at load ("Module ...
# initialization failed") where vanilla 8.8.0-from-source and the official
# Valkey image both accept the identical .so. Building the server from the same
# pinned source the CI lanes use makes the image provably match a
# module-loads-clean target and removes the dependence on a third party's image
# build. SERVER_KIND/SERVER_VERSION select which server; both are pinned to the
# versions in the CI matrix.

ARG RUST_VERSION=1.88
ARG SERVER_KIND=redis
ARG SERVER_VERSION=8.8.0

# --- Stage 1: the module cdylib -------------------------------------------
# Must be >= the crate MSRV (1.88): a transitive dep (home 0.5.12) carries a
# manifest older Cargo cannot parse. Debian bookworm (glibc 2.36) keeps the
# broad-compatibility posture of the release-artifacts build.
FROM rust:${RUST_VERSION}-bookworm AS module-build
WORKDIR /src
# redismodule-rs's build script runs bindgen over the module C headers, which
# needs libclang; the slim rust image does not ship it. git fetches the
# git-pinned redis-module dependency.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev git \
    && rm -rf /var/lib/apt/lists/*
# The root Cargo.toml is a workspace (issue #82: module crate + consumer
# client); cargo must see every member manifest to load the workspace, so
# crates/ is copied even though this stage only builds the module. `-p` scopes
# the build to the module package, so the client crate (and its extra
# dependencies) is not compiled into the image.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY crates ./crates
RUN cargo build --release --lib -p redis-event-stream-module \
    && cp target/release/libredis_event_stream_module.so /module.so

# --- Stage 2: the server, built from source -------------------------------
# Mirrors the CI "Build from source" step (.github/workflows/ci.yml): the same
# tarball/tag and BUILD_TLS=no, so the image runs the exact server build the
# integration suite proves the module loads on. Normalizes the produced
# server/cli to redis-server/redis-cli regardless of kind, so the runtime stage
# and smoke test are server-agnostic.
FROM debian:bookworm-slim AS server-build
ARG SERVER_KIND
ARG SERVER_VERSION
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential pkg-config ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /server
RUN set -eux; \
    if [ "$SERVER_KIND" = "redis" ]; then \
      url="https://download.redis.io/releases/redis-${SERVER_VERSION}.tar.gz"; \
      srv=src/redis-server; cli=src/redis-cli; \
    else \
      url="https://github.com/valkey-io/valkey/archive/refs/tags/${SERVER_VERSION}.tar.gz"; \
      srv=src/valkey-server; cli=src/valkey-cli; \
    fi; \
    curl -fsSL "$url" | tar xz --strip-components=1; \
    make -j"$(nproc)" BUILD_TLS=no; \
    install -Dm755 "$srv" /out/redis-server; \
    install -Dm755 "$cli" /out/redis-cli

# --- Stage 3: runtime -----------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=server-build /out/redis-server /usr/local/bin/redis-server
COPY --from=server-build /out/redis-cli /usr/local/bin/redis-cli
COPY --from=module-build /module.so \
     /usr/local/lib/redis/modules/libredis_event_stream_module.so
# Default configuration is expirations-only, per the README "Run" section. To
# widen the filter or set other options, override CMD with the full server
# line, e.g.:
#   docker run <image> redis-server \
#     --loadmodule /usr/local/lib/redis/modules/libredis_event_stream_module.so \
#     events 'expired,set' maxlen 1000000
EXPOSE 6379
CMD ["redis-server", "--loadmodule", \
     "/usr/local/lib/redis/modules/libredis_event_stream_module.so"]
