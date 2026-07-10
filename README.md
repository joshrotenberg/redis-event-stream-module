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

The launch use case was a customer replacing a client that periodically scanned
roughly 10 million keys to find expired entries. They had first tried a pub/sub
subscriber on `__keyevent@*__`, but it lost events whenever the subscriber
disconnected or its client output buffer overflowed, so they fell back to
scanning. The scan is expensive and only finds expirations after the fact, on
its own schedule.

This module replaces both. It captures each event into a durable stream at the
moment the keyspace changes, so a consumer reacts per expiration instead of
scanning, and a disconnected consumer resumes from where it left off.

| | Periodic sweeper scan | Pub/sub keyspace notifications | This module |
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

The default filter is `expired` (the launch use case), so loading the module
with no arguments captures only key expirations. See
[SPEC.md](SPEC.md) section 7 for the full filter grammar.

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

## Documentation

- [SPEC.md](SPEC.md): the authoritative design (architecture, routing, entry
  schema, configuration, delivery semantics, failure modes).
- [docs/consumer-patterns.md](docs/consumer-patterns.md): live tail, durable
  work queue with consumer groups, replay, discovery, and `maxlen` sizing.
- [docs/loss-windows.md](docs/loss-windows.md): every way an event can be lost,
  how to detect it, and how to reconcile a gap window without a full scan.

## Requirements

- Redis 7.2+ (`RM_AddPostNotificationJob`)

## Known limitations

- `expired` fires when Redis actually removes the key, not at the TTL instant.
- Capture is at-most-once: events during unloaded or disabled windows are not
  recoverable (the control stream makes the windows detectable, not the events).
- Clean restarts and crashes are indistinguishable in v0.1: neither writes a
  closing marker (see SPEC.md section 9).
- Cluster mode is unsupported; the module refuses to load (SPEC.md section 10).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
