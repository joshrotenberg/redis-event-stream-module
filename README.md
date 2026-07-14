# redis-event-stream-module

[![CI](https://github.com/joshrotenberg/redis-event-stream-module/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/joshrotenberg/redis-event-stream-module/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/joshrotenberg/redis-event-stream-module)](https://github.com/joshrotenberg/redis-event-stream-module/releases/latest)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
![Redis 7.2+ | Valkey 8.x/9.x](https://img.shields.io/badge/Redis_7.2%2B_%7C_Valkey_8.x%2F9.x-informational)

A Redis module that mirrors keyspace notifications into per-event Redis
Streams. Each selected event (key expiration, `SET`, `DEL`, ...) becomes a
stream entry, written atomically with the keyspace change, so events are
durable, replayable, and consumable with `XREAD` or consumer groups. Runs on
Redis 7.2+ and Valkey 8.x/9.x, standalone or with replicas.

Status: early release. The code implements the [SPEC.md](SPEC.md) design, the
authoritative reference: the v0.1 capture and introspection surface, plus v0.2
opt-in cluster per-node support. Interfaces may change before 1.0.

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

`MODULE LIST` reports the crate version as `ver`, encoded
`major*10000 + minor*100 + patch` (0.2.0 reports `ver 200`), so the loaded
release is auditable server-side (SPEC.md section 14).

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
- `cargo run --example eventstream_client -- <command>` drives events into the
  module and reads them back, against a standalone server or a per-node cluster
  (auto-detected). Commands: `info`, `produce` (drive sets, expirations, a
  mass-expiry burst, or an enabled-toggle gap-marker pair), `consume` (discover
  streams cluster-wide and tail them merged by entry ID), `watch` (a live
  counters-and-lengths dashboard), and `soak` (sustained produce, then verify
  capture). It doubles as a consumer reference for the cluster fan-out; see
  [docs/consumer-patterns.md](docs/consumer-patterns.md).

## Configuration

Set at load (module arguments, or `--eventstream.<name> <value>` on the server
command line) and, except where noted, live via `CONFIG SET`:

| Config | Type | Default | Meaning |
|--------|------|---------|---------|
| `eventstream.enabled` | bool | `yes` | master on/off switch |
| `eventstream.firehose` | bool | `no` | also mirror every captured event into one combined `<prefix>#firehose` stream; doubles write amplification (SPEC.md section 11) |
| `eventstream.stream-prefix` | string | `events:` | destination stream prefix; immutable, load-time only |
| `eventstream.events` | string | `expired` | `*` for everything, `@class` tokens, or a comma list of event names, e.g. `expired,del` |
| `eventstream.key-filter` | string | `*` | comma list of key-name globs, ANDed with `events`; matched against raw key bytes, e.g. `session:*,cache:*` |
| `eventstream.source-dbs` | string | `*` | `*` for all databases, or a comma list of db indexes, e.g. `0,2`; standalone only |
| `eventstream.maxlen` | i64 | `10000` | approximate per-stream `MAXLEN`; `0` disables trimming |
| `eventstream.max-streams` | i64 | `0` | cap on distinct destination streams; `0` is unlimited, new streams beyond the cap are dropped and counted |
| `eventstream.cluster-streams` | string | `refuse` | cluster behavior: `refuse` (default) or `per-node` (see Limitations); immutable, load-time only |

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
a persistent registry that survives restart; `EVENTSTREAM.STREAMS WITHSTATS`
adds this process's per-stream forwarded and dropped counts. Write failures
log per stream, rate-limited to one warning per stream per 60 seconds with
suppressed-count summaries, plus a notice when a failing stream recovers.

## How it works

Events route by event name into `<prefix><event>`:

| Event | Stream |
|-------|--------|
| key expiration | `events:expired` |
| hash-field expiration (Redis 7.4+) | `events:hexpired` |
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

Requires `RM_AddPostNotificationJob` (Redis 7.2, Valkey 8.x/9.x lineages). CI
runs the full integration suite against each pinned version:

| Server | Version in CI |
|--------|---------------|
| Redis 7.2 | 7.2.8 (minimum supported) |
| Redis 7.4 | 7.4.5 |
| Redis 8.x | 8.8.0 |
| Valkey 8.x | 8.1.6 |
| Valkey 9.x | 9.1.0 |

Servers below 7.2 fail to load the module (SPEC.md section 14 describes the
failure mode).

## Limitations

- `expired` fires when Redis actually removes the key, not at the TTL instant.
- Hash-field expirations (Redis 7.4+) fire `hexpired`, a distinct event under
  the hash class that the default `expired` filter does not match: durable
  field expiry needs `expired,hexpired` (or `@hash`) in `eventstream.events`.
  `hexpired` has the same removal-time (not TTL-instant) timing as `expired`,
  and the entry's `key` is the hash key — the expired field name is not part
  of the keyspace notification (SPEC.md sections 5 and 6).
- Capture is at-most-once: events during unloaded or disabled windows are not
  recoverable. The control stream makes the windows detectable, not the
  events.
- Clean restarts and crashes are indistinguishable: neither writes a closing
  marker (structural: the shutdown event fires after the final save; see
  SPEC.md section 9).
- Cluster mode: the module refuses to load by default. Setting
  `eventstream.cluster-streams per-node` enables per-node capture, where each
  master pins its streams to a slot it owns via a shared hash tag; single-shard
  clusters are the safest deployment. After a reshard that moves a node's pinned
  slot, the node re-pins to a slot it still owns and resumes capture on a new
  tag (the old entries move with the slot to its new owner); events refused
  during the brief migration window are counted drops delimited by gap markers
  (SPEC.md section 10; design in
  [docs/cluster-design.md](docs/cluster-design.md)).

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
suite includes a 2000-key staggered-expiry scenario captured with zero drops,
and `bench/run.sh` measures the drain itself (S3: foreground GET p50/p99 and
drain duration while an expiring backlog drains, with and without the module)
plus maxlen sensitivity (S4: the full-capture workload across
`eventstream.maxlen` values). A scheduled CI job
([bench.yml](.github/workflows/bench.yml)) runs the reduced matrix nightly
and gates on relative thresholds only — ratios within one run survive runner
noise where absolute ops/sec cannot; the thresholds and their rationale live
in [bench/gate.sh](bench/gate.sh), so changing one is a reviewed change.

## Documentation

- [SPEC.md](SPEC.md): the authoritative design (architecture, routing, entry
  schema, configuration, delivery semantics, failure modes).
- [docs/consumer-patterns.md](docs/consumer-patterns.md): live tail, durable
  work queue with consumer groups, replay, discovery, and `maxlen` sizing.
- [docs/loss-windows.md](docs/loss-windows.md): every way an event can be
  lost, how to detect it, and how to reconcile a gap window without a full
  scan.
- [docs/cluster-design.md](docs/cluster-design.md): the cluster per-node design
  (refuse-by-default, slot-pinned per-node tags, re-pinning on reshard).
- [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
