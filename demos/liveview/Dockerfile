# syntax=docker/dockerfile:1

# Build from the repository root:
#   docker build -f demos/liveview/Dockerfile -t redis-event-stream-liveview .
#   docker run --rm -p 4000:4000 redis-event-stream-liveview

ARG RUST_VERSION=1.88
ARG REDIS_VERSION=8.8.0
ARG ELIXIR_IMAGE=elixir:1.19.5-otp-28-slim

# --- Native Redis module --------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS module-build
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY crates ./crates

RUN cargo build --release --lib -p redis-event-stream-module \
    && cp target/release/libredis_event_stream_module.so /module.so

# --- Redis server ---------------------------------------------------------
# Match the repository's primary Docker image: use the CI-pinned vanilla
# server build known to load the module cleanly.
FROM debian:bookworm-slim AS redis-build
ARG REDIS_VERSION
WORKDIR /redis

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz" \
      | tar xz --strip-components=1 \
    && make -j"$(nproc)" BUILD_TLS=no \
    && install -Dm755 src/redis-server /out/redis-server \
    && install -Dm755 src/redis-cli /out/redis-cli

# --- Phoenix release ------------------------------------------------------
FROM ${ELIXIR_IMAGE} AS app-build
ENV MIX_ENV=prod
ENV EVENTSTREAM_MODULE_PATH=/usr/local/lib/redis/modules/libredis_event_stream_module.so
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mix local.hex --force \
    && mix local.rebar --force

COPY demos/liveview/mix.exs demos/liveview/mix.lock ./
RUN mkdir config
COPY demos/liveview/config/config.exs demos/liveview/config/prod.exs config/
RUN mix deps.get --only prod \
    && mix deps.compile

COPY demos/liveview/assets assets
COPY demos/liveview/lib lib
COPY demos/liveview/priv priv
COPY demos/liveview/config/runtime.exs config/

RUN mix compile \
    && mix esbuild.install --if-missing \
    && mix assets.deploy \
    && mix release

# --- Self-contained demo runtime -----------------------------------------
# The release already contains ERTS, so the final image only needs its shared
# libraries plus the Redis binaries and native module.
FROM debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates libncurses6 libstdc++6 lsof openssl procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system eventstream \
    && useradd --system --gid eventstream --home-dir /app eventstream \
    && mkdir -p /app /usr/local/lib/redis/modules \
    && chown eventstream:eventstream /app

COPY --from=redis-build /out/redis-server /usr/local/bin/redis-server
COPY --from=redis-build /out/redis-cli /usr/local/bin/redis-cli
COPY --from=module-build /module.so \
     /usr/local/lib/redis/modules/libredis_event_stream_module.so
COPY --from=app-build --chown=eventstream:eventstream \
     /app/_build/prod/rel/eventstream_demo /app

ENV PHX_SERVER=true
ENV PHX_HOST=localhost
ENV PORT=4000
ENV LANG=C.UTF-8
ENV EVENTSTREAM_MODULE_PATH=/usr/local/lib/redis/modules/libredis_event_stream_module.so
ENV LOCAL_DEMO=true

USER eventstream
WORKDIR /app

EXPOSE 4000

CMD ["/app/bin/eventstream_demo", "start"]
