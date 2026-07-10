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

Each entry has `event` and `key` fields (binary-safe); `Verbose` format adds a
`class` field. The stream entry ID supplies the timestamp.

## Configuration

Settable at load (`--eventstream.<name> <value>`) and live via `CONFIG SET`:

| Config | Type | Default | Meaning |
|--------|------|---------|---------|
| `eventstream.enabled` | bool | `yes` | master on/off switch |
| `eventstream.prefix` | string | `events:` | destination stream prefix |
| `eventstream.events` | string | `all` | `all`/`*`, or comma list of event names, e.g. `expired,del` |
| `eventstream.maxlen` | i64 | `10000` | approximate per-stream `MAXLEN`; `0` disables trimming |
| `eventstream.format` | enum | `Minimal` | `Minimal` or `Verbose` |

The module registers no commands. Configuration is `CONFIG GET/SET
eventstream.*`; counters (forwarded, dropped and skipped by reason, active
streams, gap markers) live in a module INFO section: `INFO eventstream` (module
sections do not appear in plain `INFO`; use `INFO everything` or name the
section).

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

## Requirements

- Redis 7.2+ (`RM_AddPostNotificationJob`)

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
