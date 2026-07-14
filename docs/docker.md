# Docker image

A preloaded image is published to
`ghcr.io/joshrotenberg/redis-event-stream-module` on each release. It is an
official Redis (or Valkey) image with the module `.so` copied to a fixed path
and `loadmodule` wired into the default command, so a bare `docker run` starts
a server with the module already loaded.

## Quick start

```sh
docker run --rm -p 6379:6379 ghcr.io/joshrotenberg/redis-event-stream-module:latest
```

The default configuration captures expirations only (the same default as a
freshly loaded module). Read the mirrored events back from another terminal:

```sh
docker run -d --name es -p 6379:6379 \
  ghcr.io/joshrotenberg/redis-event-stream-module:latest
docker exec es redis-cli SET foo bar PX 100
sleep 0.3 && docker exec es redis-cli GET foo   # forces lazy expiry
docker exec es redis-cli XREAD COUNT 10 STREAMS events:expired 0
```

## Passing module arguments

Module arguments pass through unchanged by overriding the command. The module
path inside the image is
`/usr/local/lib/redis/modules/libredis_event_stream_module.so`:

```sh
docker run --rm -p 6379:6379 \
  ghcr.io/joshrotenberg/redis-event-stream-module:latest \
  redis-server \
  --loadmodule /usr/local/lib/redis/modules/libredis_event_stream_module.so \
  events 'expired,set' maxlen 1000000
```

Any server flag can follow, so the usual `redis.conf` overrides work the same
way (for example `--appendonly yes` for AOF persistence).

## Tags and variants

| Tag | Server | Notes |
|-----|--------|-------|
| `<version>`, `latest` | Redis 8.8.0 | e.g. `0.2.0` |
| `<version>-valkey8`, `latest-valkey8` | Valkey 8.1.6 | Valkey variant |

The server is built from source inside the image (the CI-pinned versions),
rather than layered onto an upstream `redis`/`valkey` image: the module builds
and loads on every vanilla Redis 7.2+/Valkey 8+ the CI matrix covers, but the
official Redis Ltd `redis:8` image ships a build that rejects the module's
config initialization at load, where vanilla 8.8.0-from-source and the official
Valkey image both accept the identical `.so`. Building from source makes the
image provably match a server the module loads on.

Images are multi-arch manifests for `linux/amd64` and `linux/arm64`, matching
the linux-x86_64/linux-aarch64 release binaries; there is no macOS image (the
`.dylib` is out of scope for containers). Pin by digest for reproducible
deployments (`docker pull ...@sha256:...`).

Only Redis 7.2+/Valkey 8.x servers are published. On pre-7.2 servers the module
load is a process abort at startup, not a clean refusal (SPEC.md section 14),
so lower versions are never built.

## Building locally

The image is a multi-stage build (Rust builder → server-from-source builder →
slim runtime). `SERVER_KIND` and `SERVER_VERSION` select and pin the server:

```sh
make docker                                       # Redis 8.8.0
make docker-valkey                                # Valkey 8.1.6
# or directly:
docker build --build-arg SERVER_KIND=redis \
  --build-arg SERVER_VERSION=8.8.0 -t eventstream:local .
```
