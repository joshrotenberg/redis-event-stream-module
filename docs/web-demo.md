# Web demo

A browser page that shows captured events lighting up live: a lane per event
type filling in real time, counter tiles, and colored bands when capture is
toggled off and on. It is the most demo-able artifact in the project and,
because it is strictly read-only against Redis, doubles as a worked consumer
reference for readers who think in HTTP rather than raw stream reads.

It ships as one small bridge plus one static page, both under the
`eventstream-client` crate's examples:

- `crates/eventstream-client/examples/eventstream_web.rs` — a
  `std::net::TcpListener` HTTP loop (no web framework) that serves the page and
  a [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
  feed. It reuses the shipped consumer library for discovery and the merged
  read, so it sees a per-node cluster the same way the real consumer does.
- `crates/eventstream-client/examples/eventstream_web.html` — one static page,
  vanilla JavaScript `EventSource`, no external assets or build step.

## Run it

The bridge does not drive events; the existing
[scripted producer](./example-client.md) does. The recipe is: confirm the
module is loaded, start the bridge, open the browser, then produce events in a
second terminal.

```sh
# 1. Against a server that already has the module loaded (see the Quickstart):
./demo-preflight.sh                        # optional sanity check

# 2. Start the bridge (defaults: --url redis://127.0.0.1:6379, --prefix events:).
cargo run -p eventstream-client --example eventstream_web -- --listen 127.0.0.1:8080

# 3. Open http://127.0.0.1:8080 in a browser.

# 4. In another terminal, drive some events:
eventstream-client produce --sets 12 --expire 15 --toggle
```

`--expire` sets keys with a short TTL and forces their expiry (the `expired`
lane fills), `--sets` fires plain `SET`s (the `set` lane fills), and `--toggle`
flips `eventstream.enabled` off then on, which writes a `disabled`/`enabled`
gap-marker pair — the page renders those as a red band and a green band.

The bridge widens nothing and writes nothing: it only reads (`XREAD`, `XLEN`,
`INFO`, `EVENTSTREAM.STREAMS`, `CLUSTER NODES`). Point `--url` at any node of a
per-node cluster and it discovers and merges every node's streams, picking up a
re-pinned node's new streams without a restart (see
[Discovery and cluster consumers](./cluster-consumers.md)).

## What you see

- **Lanes** — one column per event type, newest entry at the top, each row
  showing the key, origin `db`, and entry ID. The `expired` lane is highlighted
  because expirations are the module's primary use case.
- **Counter tiles** — `forwarded`, `events_lost`, `dropped writes`, the number
  of discovered `streams`, and total `retained` entries, refreshed every ~1.5s
  from `INFO eventstream` (summed across masters) and `XLEN`.
- **Gap-marker bands** — when capture is toggled or a flush/swap/re-pin
  happens, the marker appears as a colored band, so a capture gap is visible
  rather than silently missing.

## Notes and constraints

- Read-only: no `XADD`, no `CONFIG SET`, no consumer groups. This is the
  fan-in observer pattern, not the durable work-queue pattern
  ([Consumer patterns](./consumer-patterns.md)).
- Keys are raw bytes; the bridge decodes them lossily (invalid UTF-8 becomes
  the U+FFFD marker) before sending them over the text-only SSE channel.
- A page that connects to a server with existing streams starts at each
  stream's tail, so it shows only events from that moment on; a stream that
  first appears while the page is open is read from its beginning, so a
  first-ever event of a new type is not missed.
