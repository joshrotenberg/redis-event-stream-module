# Quickstart

Load the module, expire a key, and read the mirrored event back out of a
durable stream. Requires Redis 7.2+.

## Install

Prebuilt modules for Linux (x86_64, aarch64) and macOS (x86_64, aarch64) are
attached to each
[release](https://github.com/joshrotenberg/redis-event-stream-module/releases)
with SHA-256 checksums and a Sigstore provenance attestation:

```sh
curl -LO https://github.com/joshrotenberg/redis-event-stream-module/releases/latest/download/redis-event-stream-module-<version>-linux-x86_64.so
curl -LO https://github.com/joshrotenberg/redis-event-stream-module/releases/latest/download/redis-event-stream-module-<version>-linux-x86_64.so.sha256
shasum -a 256 -c redis-event-stream-module-<version>-linux-x86_64.so.sha256
```

Or build from source (Rust 1.88+, the MSRV declared in `Cargo.toml`):

```sh
cargo build --release
# module at target/release/libredis_event_stream_module.so (.dylib on macOS)
```

A preloaded Docker image is also available — see [Docker image](./docker.md).

## Load

Point `loadmodule` at the `.so`. The default configuration captures
expirations only; widen it with module arguments (see
[Configuration](./configuration.md)):

```sh
redis-server --loadmodule /path/to/libredis_event_stream_module.so
# or, capturing more:
redis-server --loadmodule /path/to/libredis_event_stream_module.so events 'expired,set'
```

`notify-keyspace-events` does **not** need to be set: module subscribers
receive keyspace events regardless of that setting, which gates pub/sub
delivery only. `MODULE LIST` reports the crate version as `ver`, encoded
`major*10000 + minor*100 + patch` (0.2.0 → `ver 200`).

## Capture an event

In `redis-cli`, with the default expirations-only filter:

```
> SET foo bar PX 100
> GET foo            # after ~100ms; the lookup forces lazy expiry
> XREAD COUNT 10 STREAMS events:expired 0
1) 1) "events:expired"
   2) 1) 1) "1720512345784-0"
         2) 1) "event"
            2) "expired"
            3) "key"
            4) "foo"
            5) "db"
            6) "0"
```

The expired event for `foo` is now a durable stream entry — replayable with
`XRANGE events:expired - +` and consumable with a consumer group (see
[Consumer patterns](./consumer-patterns.md)).

## Next steps

- [Configuration](./configuration.md) — every `eventstream.*` key and the
  filter grammar.
- [Consumer patterns](./consumer-patterns.md) — live tail, work queues, replay.
- [Scripted demo](./demo.md) — the end-to-end demo script;
  [Preflight checks](./preflight.md) — the deployment health check.
- [Counters](./counters.md) and [Monitoring](./monitoring.md) — what to watch.
- [Loss windows and reconciliation](./loss-windows.md) — before production:
  capture is at-most-once, and exact recovery of expirations missed during a
  gap needs an application-maintained expiry index, since expired keys are gone
  from the keyspace and cannot be reconstructed by a scan.
