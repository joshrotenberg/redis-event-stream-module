# Sizing and retention

How much stream history to keep, and what it costs. Retention is the operator
half of the delivery contract: consumers get at-least-once delivery only
within the window sized here. The read-side patterns that depend on it are in
[Consumer patterns](./consumer-patterns.md).

## Sizing maxlen

`maxlen` is a retention cap, not a delivery guarantee. An entry is trimmed once
the stream exceeds `maxlen`, whether or not a consumer has read it. So retention
must exceed your worst case:

```
maxlen >= peak_event_rate * worst_case_consumer_downtime
```

Worked example: a mass-expiry burst produces 1000 expirations/sec, and your
worst-case consumer outage (deploy, crash, network partition) is 10 minutes:

```
1000 events/sec * 600 sec = 600000
```

Set `maxlen` above 600000 for that stream, or the overrun is trimmed and lost
before a recovered consumer can read it. Trade this against memory: total memory
is roughly `distinct_event_names * maxlen * bytes_per_entry`, and a three-field
entry with a 32-byte key is about 150 bytes (SPEC.md section 11). At
`maxlen=600000` one stream is about 90 MB.

Approximate trimming (`MAXLEN ~`, which the module always uses) trims at whole
listpack-node boundaries, so the stream can overshoot the cap by up to about one
node (roughly `stream-node-max-entries`, default 100). Treat `maxlen` as a
floor on retained entries, not an exact ceiling.

### Per-event overrides

A single global `maxlen` forces the worst-case stream's size onto every stream:
if `expired` needs 600000 to survive a mass-expiry drain, `set`, `del`, and
every other event pay that too, even where 1000 would do. `eventstream.maxlen-overrides`
breaks the coupling — a comma list of `event=cap` pairs keyed by the stream
suffix, falling back to the global `maxlen` for any stream not named:

```
CONFIG SET eventstream.maxlen-overrides expired=600000,set=1000
```

Now `events:expired` retains 600000 (~90 MB) and `events:set` only 1000, while
every other stream keeps the global cap. A cap of `0` disables trimming for that
one stream (like the global `maxlen 0`). The control stream is addressable as
`#control`; the firehose is not — it aggregates every event type and stays sized
by the global `maxlen` for the total rate. Total memory becomes the sum of the
per-stream caps rather than one cap times the stream count.

### Time-based retention

Retention is often expressed in time ("keep 24h"), not entry counts. Under
bursty traffic a fixed `maxlen` gives an unpredictable replay window — a burst
can flush hours of history in seconds. `eventstream.retention-ms` trims by age
instead: every entry ID already carries the event's millisecond timestamp, so
the module can drop entries older than the window with `XADD ... MINID ~`:

```
CONFIG SET eventstream.retention-ms 86400000   # keep ~24h
```

When set (`>0`), time-based retention takes precedence over `maxlen` and any
per-event override — a stream trims by age, not count, and the memory bound
becomes `event_rate × window` rather than a fixed count. `0` (the default)
disables it, leaving count-based `maxlen` in charge. One caveat: trimming is
inline (folded into each `XADD`), so a stream that stops receiving events is
never re-trimmed and can retain entries past the window until its next write.
If that matters for an idle stream, an external periodic `XTRIM <stream> MINID ~ <ms>`
closes the gap.

## Monitoring consumer lag

Alert before a slow consumer falls off the retention window. Useful signals:

```
XINFO GROUPS events:expired      # per-group `lag`: undelivered entries (Redis 7.0+)
XINFO STREAM events:expired      # `entries-added`, `max-deleted-entry-id`, `length`
```

Compare your resume ID against the stream's first entry ID (`XINFO STREAM`
`first-entry`): if your resume point is older than the first retained entry, you
have already lost data. A practical threshold is to alert when group `lag`
exceeds roughly half of `maxlen`, which leaves time to react before trimming
starts dropping unread entries (SPEC.md section 13).

For the module's own health counters (forwarded, dropped by reason), see
`INFO eventstream` and [docs/loss-windows.md](loss-windows.md).
