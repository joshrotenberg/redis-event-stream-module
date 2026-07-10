# redis-event-stream-module

A Redis module that mirrors keyspace notifications into per-event Redis
Streams, so ephemeral events become a durable, replayable log.

Status: pre-alpha. [SPEC.md](SPEC.md) is the authoritative design; the current
code is a working baseline that predates parts of it.

## Why

Keyspace notifications (including the `expired` event) are delivered over
Pub/Sub, which is fire and forget. A consumer that is disconnected when an
event fires never sees it. This module subscribes to keyspace events inside the
server and re-emits each one as an `XADD` into a per-event stream, so consumers
can use `XREAD` or consumer groups and never miss an event, even across
restarts.

The previous in-server way to do this was a RedisGears / Triggers-and-Functions
script, which is deprecated. This module is a small, purpose-built replacement
built on [redismodule-rs](https://github.com/RedisLabsModules/redismodule-rs).

## Choosing a capture approach

Reacting reliably to key expirations usually means one of two approaches, and
both have a failure mode. A pub/sub subscriber on `__keyevent@*__` loses events
whenever it disconnects or its client output buffer overflows. Falling back to
periodically scanning the keyspace for expired entries is expensive and only
finds expirations after the fact, on the scan's own schedule.

This module replaces both. It captures each event into a durable stream at the
moment the keyspace changes, so a consumer reacts per expiration instead of
scanning, and a disconnected consumer resumes from where it left off.

| | Periodic keyspace scan | Pub/sub keyspace notifications | This module |
|---|---|---|---|
| Consumer disconnect | no effect (stateless poll) | events during the gap are lost | entries wait in the stream |
| Replay after restart | rescan everything | none | `XRANGE` / group from any ID |
| Server load per detection | full or partial keyspace scan | one pub/sub publish | one `XADD` (plus approximate trim) |
| Detection latency | up to one scan interval | immediate | immediate |
| Consumer scaling | manual sharding | fan-out only, no work splitting | consumer groups |
| Loss detectable | n/a (always rescans) | no | yes (gap markers, counters) |
| Needs `notify-keyspace-events` | no | yes | no |

Module delivery does not depend on `notify-keyspace-events`: module keyspace
subscribers receive events regardless of that setting, which only gates pub/sub
delivery (SPEC.md section 7, verified against Redis `src/notify.c` and enforced
across the CI matrix). Pub/sub delivery does depend on it.

What this module does not provide: exactly-once delivery (consumers must be
idempotent on stream name plus entry ID), and backfill of events that occur
while the module is unloaded, disabled, or the server is down. It is a live
mirror, not a write-ahead log. See
[docs/loss-windows.md](docs/loss-windows.md) for the exact windows and how to
reconcile over them, and [docs/consumer-patterns.md](docs/consumer-patterns.md)
for consumer recipes.

## Routing

Events route by event name into `<prefix><event>`. With the default `events:`
prefix:

| Event | Stream |
|-------|--------|
| key expiration | `events:expired` |
| `SET` | `events:set` |
| `HSET` | `events:hset` |
| `DEL` | `events:del` |
| eviction | `events:evicted` |

Each entry has three fields: `event` (the event name), `key` (the affected key,
binary-safe), and `db` (the database the event fired in). All destination
streams live in database 0; the `db` field records the origin. The stream entry
ID supplies the timestamp.

## Configuration

Set at load (module arguments, or `--eventstream.<name> <value>` on the server
command line) and, except where noted, live via `CONFIG SET`:

| Config | Type | Default | Meaning |
|--------|------|---------|---------|
| `eventstream.enabled` | bool | `yes` | master on/off switch |
| `eventstream.stream-prefix` | string | `events:` | destination stream prefix; immutable, load-time only |
| `eventstream.events` | string | `expired` | `*` for everything, `@class` tokens, or a comma list of event names, e.g. `expired,del` |
| `eventstream.maxlen` | i64 | `10000` | approximate per-stream `MAXLEN`; `0` disables trimming |

The default filter is `expired`, so loading the module with no arguments
captures only key expirations. See
[SPEC.md](SPEC.md) section 7 for the full filter grammar.

Configuration is `CONFIG GET/SET eventstream.*`. Counters (forwarded, dropped
and skipped by reason, active streams, gap markers) live in a module INFO
section: `INFO eventstream` (module sections do not appear in plain `INFO`; use
`INFO everything` or name the section). Two readonly introspection commands are
also registered: `EVENTSTREAM.STATS` returns those counters as a structured
reply, and `EVENTSTREAM.STREAMS` lists the destination streams written so far
(backed by a persistent registry that survives restart).

Capture-gap boundaries are machine-readable: the module writes markers
(`loaded`, `disabled`, `enabled`, `unloading`) to a control stream at
`<prefix>#control`, so consumers can bound reconciliation to known gap windows.
See SPEC.md section 9.

## Build and run

```sh
cargo build --release
redis-server --loadmodule ./target/release/libredis_event_stream_module.dylib
```

(`.so` on Linux.) The server's `notify-keyspace-events` setting does not need
to be enabled: module subscribers receive keyspace events regardless of that
setting, which only gates pub/sub delivery. Verified empirically on Redis 8.8
and documented in SPEC.md.

Quick check in `redis-cli`:

```
> SET foo bar PX 100
> XREAD COUNT 10 STREAMS events:set 0
```

Wait for the key to expire, then:

```
> XREAD COUNT 10 STREAMS events:expired 0
```

See `demo.sh` for a scripted end-to-end run.

## Documentation

- [SPEC.md](SPEC.md): the authoritative design (architecture, routing, entry
  schema, configuration, delivery semantics, failure modes).
- [docs/consumer-patterns.md](docs/consumer-patterns.md): live tail, durable
  work queue with consumer groups, replay, discovery, and `maxlen` sizing.
- [docs/loss-windows.md](docs/loss-windows.md): every way an event can be lost,
  how to detect it, and how to reconcile a gap window without a full scan.

## Requirements

Redis 7.2 or newer, for `RM_AddPostNotificationJob`. Valkey 8.x works too; it
shares the module ABI and post-notification-job API. CI runs the full
integration suite on each server below, so these are verified, not just
claimed:

| Server | Verified in CI |
|--------|----------------|
| Redis 7.2 | 7.2.8 (minimum supported) |
| Redis 7.4 | 7.4.5 |
| Redis 8.x | 8.8.0 |
| Valkey 8.x | 8.1.6 |

Servers below 7.2 are not supported: the module fails to load (see SPEC.md
section 14 for the exact failure mode).

## Known limitations

- `expired` fires when Redis actually removes the key, not at the TTL instant.
- Capture is at-most-once: events during unloaded or disabled windows are not
  recoverable (the control stream makes the windows detectable, not the events).
- Clean restarts and crashes are indistinguishable in v0.1: neither writes a
  closing marker (see SPEC.md section 9).
- Cluster mode is unsupported; the module refuses to load (SPEC.md section 10).

## Performance

The cost of the module is one extra `XADD` (plus an inline approximate trim) per
captured event, on the main thread, in a post-notification job. Loaded but not
capturing, it adds a few cheap gate checks per keyspace event and nothing else.

Measured with `bench/run.sh` across the three SPEC.md section 11 scenarios (S0:
no module; S1: loaded, default `expired` filter, so a `SET` workload captures
nothing; S2: `events=set`, so every `SET` is captured):

| Scenario | ops/sec | vs S0 | p50 (ms) | p99 (ms) |
|---|---|---|---|---|
| S0 baseline (no module) | 124969 | - | 0.223 | 0.407 |
| S1 loaded, no capture | 124938 | -0.0% | 0.223 | 0.415 |
| S2 loaded, full capture | 111086 | -11.1% | 0.311 | 0.623 |

The gate tax (S1) is within noise: a deployment that loads the module but does
not match the workload pays effectively nothing. Full capture (S2) costs about
11% throughput and roughly doubles p99 latency on this workload, well inside the
worst case. Capture cost scales with captured write volume, and the default
filter captures only expirations, so a typical deployment sits between S1 and
S2, near S1.

Method: median of 3 runs, `redis-benchmark -t set -n 1000000 -c 50 --threads 4
-d 64 -r 100000` per run. These figures are indicative, measured on a dev laptop
(Apple M4 Pro, 24 GB, macOS 26.5.2, Redis 8.8.0) under light use; treat them as a
shape, not a spec. Re-run `bench/run.sh` on a quiet, representative host for
authoritative numbers. SPEC.md section 11 specifies `memtier_benchmark`; the
script uses `redis-benchmark` because it ships with every Redis and Valkey.

Mass-expiry storms (a large backlog of keys expiring at once) are the worst case:
each expiration becomes an `XADD` on the main thread, paced by the server's
expire-cycle throttling. The `tests/observability.rs` mass-expiry test captures
2000 staggered expirations with zero drops; drain-latency profiling under an
adversarial expiry burst, and CI-gated regression thresholds, are future work
(SPEC.md section 16).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
