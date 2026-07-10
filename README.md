# redis-event-stream-module

A Redis module that mirrors keyspace notifications into per-event Redis
Streams. Each selected event (key expiration, `SET`, `DEL`, ...) becomes a
stream entry, written atomically with the keyspace change, so events are
durable, replayable, and consumable with `XREAD` or consumer groups. Runs on
Redis 7.2+ and Valkey 8.x, standalone or with replicas.

Status: early release. The code implements the v0.1 scope of
[SPEC.md](SPEC.md), the authoritative design, plus the introspection commands
added after it. Interfaces may change before 1.0.

## Install

Prebuilt modules for Linux (x86_64, aarch64) and macOS (aarch64) are attached
to each [release](https://github.com/joshrotenberg/redis-event-stream-module/releases)
with sha256 checksums:

```sh
curl -LO https://github.com/joshrotenberg/redis-event-stream-module/releases/latest/download/redis-event-stream-module-<version>-linux-x86_64.so
curl -LO https://github.com/joshrotenberg/redis-event-stream-module/releases/latest/download/redis-event-stream-module-<version>-linux-x86_64.so.sha256
shasum -a 256 -c redis-event-stream-module-<version>-linux-x86_64.so.sha256
```

Or build from source (stable Rust):

```sh
cargo build --release
# module at target/release/libredis_event_stream_module.so (.dylib on macOS)
```

## Run

```sh
redis-server --loadmodule ./redis-event-stream-module-<version>-linux-x86_64.so
```

The server's `notify-keyspace-events` setting is not required: module
subscribers receive keyspace events regardless of that setting, which gates
pub/sub delivery only (SPEC.md section 7).

Quick check in `redis-cli` (the default configuration captures expirations
only):

```
> SET foo bar PX 100
> GET foo          (after ~100ms; forces lazy expiry)
> XREAD COUNT 10 STREAMS events:expired 0
```

The expired event for `foo` is now a stream entry.

- `./demo.sh` runs a scripted end-to-end demonstration on a local server.
- `./demo-preflight.sh -h host -p port` checks an existing deployment
  (reachability, module presence, config, an end-to-end probe expiration,
  discovery, counters) and exits nonzero on any failure. All arguments pass
  through to `redis-cli`.

## Configuration

Set at load (module arguments, or `--eventstream.<name> <value>` on the server
command line) and, except where noted, live via `CONFIG SET`:

| Config | Type | Default | Meaning |
|--------|------|---------|---------|
| `eventstream.enabled` | bool | `yes` | master on/off switch |
| `eventstream.stream-prefix` | string | `events:` | destination stream prefix; immutable, load-time only |
| `eventstream.events` | string | `expired` | `*` for everything, `@class` tokens, or a comma list of event names, e.g. `expired,del` |
| `eventstream.maxlen` | i64 | `10000` | approximate per-stream `MAXLEN`; `0` disables trimming |

The full filter grammar is in SPEC.md section 7. The high-volume `@missed`
(read misses) and `@new` (new-key) classes are opt-in and must be named at load
time; a `*` or explicit `@missed`/`@new` in the load-time filter subscribes to
them.

Counters (forwarded, dropped and skipped by reason, active streams, gap
markers) are exposed in a module INFO section: `INFO eventstream`. Module
sections do not appear in plain `INFO`; name the section or use
`INFO everything`. Two readonly commands are also registered:
`EVENTSTREAM.STATS` returns the counters as a structured reply, and
`EVENTSTREAM.STREAMS` lists the destination streams written so far, backed by
a persistent registry that survives restart.

## How it works

Events route by event name into `<prefix><event>`:

| Event | Stream |
|-------|--------|
| key expiration | `events:expired` |
| `SET` | `events:set` |
| `HSET` | `events:hset` |
| `DEL` | `events:del` |
| eviction | `events:evicted` |

Each entry has three fields: `event` (the event name), `key` (the affected
key, binary-safe), and `db` (the database the event fired in). All destination
streams live in database 0; the `db` field records the origin. The stream
entry ID carries the event's millisecond timestamp.

Delivery semantics (SPEC.md section 9): on a healthy capturing master, each
selected event produces exactly one entry, atomic with the keyspace change.
Overall capture is at-most-once; consumption through consumer groups is
at-least-once within the retention window, so consumers must be idempotent on
stream name plus entry ID. Mirrored entries replicate to replicas and the AOF.

The module writes capture-gap markers (`loaded`, `disabled`, `enabled`,
`unloading`) to a control stream at `<prefix>#control`, so consumers can bound
reconciliation to known gap windows (SPEC.md section 9).

## Comparison with other approaches

| | Periodic keyspace scan | Pub/sub keyspace notifications | This module |
|---|---|---|---|
| Consumer disconnect | no effect (stateless poll) | events during the gap are lost | entries wait in the stream |
| Replay after restart | rescan everything | none | `XRANGE` / group from any ID |
| Server load per detection | full or partial keyspace scan | one pub/sub publish | one `XADD` (plus approximate trim) |
| Detection latency | up to one scan interval | immediate | immediate |
| Consumer scaling | manual sharding | fan-out only, no work splitting | consumer groups |
| Loss detectable | n/a (always rescans) | no | yes (gap markers, counters) |
| Needs `notify-keyspace-events` | no | yes | no |

RedisGears / Triggers-and-Functions could script similar capture in-server and
is deprecated by Redis.

This module does not provide exactly-once delivery, and it does not backfill
events that occur while the module is unloaded, disabled, or the server is
down. It is a live mirror, not a write-ahead log. See
[docs/loss-windows.md](docs/loss-windows.md) for the loss windows and how to
reconcile over them.

## Supported servers

Requires `RM_AddPostNotificationJob` (Redis 7.2, Valkey 8.x lineage). CI runs
the full integration suite against each pinned version:

| Server | Version in CI |
|--------|---------------|
| Redis 7.2 | 7.2.8 (minimum supported) |
| Redis 7.4 | 7.4.5 |
| Redis 8.x | 8.8.0 |
| Valkey 8.x | 8.1.6 |

Servers below 7.2 fail to load the module (SPEC.md section 14 describes the
failure mode).

## Limitations

- `expired` fires when Redis actually removes the key, not at the TTL instant.
- Capture is at-most-once: events during unloaded or disabled windows are not
  recoverable. The control stream makes the windows detectable, not the
  events.
- Clean restarts and crashes are indistinguishable: neither writes a closing
  marker (SPEC.md section 9).
- Cluster mode is unsupported; the module refuses to load (SPEC.md section 10;
  a design proposal is in [docs/cluster-design.md](docs/cluster-design.md)).

## Performance

The cost per captured event is one `XADD` plus an inline approximate trim, on
the main thread, in a post-notification job. When loaded but not capturing,
the per-event cost is the gate checks in the notification callback.

Measured with `bench/run.sh` across the SPEC.md section 11 scenarios (S0: no
module; S1: loaded, default filter, `SET` workload so nothing is captured;
S2: `events=set`, every `SET` captured):

| Scenario | ops/sec | vs S0 | p50 (ms) | p99 (ms) |
|---|---|---|---|---|
| S0 baseline (no module) | 133262 | - | 0.199 | 0.399 |
| S1 loaded, no capture | 133262 | +0.0% | 0.207 | 0.407 |
| S2 loaded, full capture | 114260 | -14.3% | 0.335 | 0.623 |

S1 is within measurement noise of S0. Capture cost scales with captured write
volume; the default filter captures expirations only.

Method: median of 3 runs, `redis-benchmark -t set -n 1000000 -c 50 --threads 4
-d 64 -r 100000` per run, on Apple M4 Pro (14 cores, 24 GB, macOS 26.5.2)
against Redis 8.8.0. Numbers vary with hardware and workload; run
`bench/run.sh` on your own host to reproduce. SPEC.md section 11 specifies
`memtier_benchmark`; the script uses `redis-benchmark` because it ships with
every Redis and Valkey.

Mass expiry is the heaviest case: each expiration becomes an `XADD` on the
main thread, paced by the server's expire-cycle throttling. The integration
suite includes a 2000-key staggered-expiry scenario captured with zero drops;
drain-latency profiling and CI-gated regression thresholds are future work
(SPEC.md section 16).

## Documentation

- [SPEC.md](SPEC.md): the authoritative design (architecture, routing, entry
  schema, configuration, delivery semantics, failure modes).
- [docs/consumer-patterns.md](docs/consumer-patterns.md): live tail, durable
  work queue with consumer groups, replay, discovery, and `maxlen` sizing.
- [docs/loss-windows.md](docs/loss-windows.md): every way an event can be
  lost, how to detect it, and how to reconcile a gap window without a full
  scan.
- [docs/cluster-design.md](docs/cluster-design.md): proposed cluster support
  (not implemented).
- [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
