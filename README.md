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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
