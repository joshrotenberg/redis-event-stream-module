# Example client

`crates/eventstream-client` is a workspace member that drives events into the
module and reads them back. It is both a runnable CLI and a published consumer
library, and it doubles as a reference for cluster-wide fan-out (it discovers
per-node streams and merges them by entry ID). It works against a standalone
server or a per-node cluster, auto-detected.

Run it from source or from a release build:

```sh
cargo run -p eventstream-client -- <command>
# or, from a built/installed binary:
eventstream-client <command>
```

Global options: `--url <redis://…>` (scheme added if absent) and `--prefix
<stream-prefix>` (must match the module's `eventstream.stream-prefix`).

## Commands

| Command | What it does |
|---|---|
| `info` | Topology, each master's module counters, and the discovered streams. |
| `produce` | Drive events into the module: `--set N` fires N `SET`s; `--expire N` sets N keys with a TTL and forces their expiry; `--burst N` is a mass-expiry burst; `--ttl-ms` sets the TTL for `--expire`/`--burst`; `--toggle` flips `eventstream.enabled` off then on, writing a gap-marker pair. |
| `consume` | Discover streams cluster-wide and tail them merged by entry ID. `--only <a,b>` restricts to event types; `--from 0` replays from the beginning (default `$` = new only); `--count N` stops after N entries. |
| `watch` | A live dashboard of counters and stream lengths. |
| `soak` | Sustained produce, then verify capture. `--events <a,b>` chooses what to drive; `--rate N` caps events/sec (`0` = unlimited). |

Before producing, the client widens the module's event filter on every master
so what it drives is captured, without narrowing an existing filter. See
[Consumer patterns](./consumer-patterns.md) and
[Durable work queues](./work-queues.md) for the underlying `XREADGROUP`
recipes it demonstrates.
