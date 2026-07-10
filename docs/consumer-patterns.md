# Consumer patterns

How to read the mirrored streams this module produces. Every command block runs
against a server with the module loaded; unless noted, the default
configuration is assumed (filter `expired`, prefix `events:`), so the examples
use `events:expired`. Where an example needs another event type, it says so and
you widen `eventstream.events` first.

The module only writes streams. Everything on the read side is ordinary Redis
Streams, so any Streams-capable client works and nothing here is
module-specific. See [SPEC.md](../SPEC.md) section 9 for the delivery semantics
these patterns rely on.

## Entry shape

Each entry has three fields and an ID:

```
1) "1730000000123-0"        <- entry ID: <milliseconds>-<sequence>
2) 1) "event"   2) "expired"
   3) "key"     4) "session:abc"
   5) "db"      6) "0"
```

The ID's millisecond component is the event time (the write runs atomically
with the keyspace change), so there is deliberately no separate timestamp
field. `key` is raw bytes and may be binary. `db` is the database the event
fired in; all streams live in database 0 regardless (SPEC.md section 6).

## Live tail (pub/sub replacement)

The direct replacement for a `__keyevent@0__:expired` pub/sub subscriber. Block
for new entries and process them as they arrive:

```
XREAD BLOCK 0 STREAMS events:expired $
```

`$` means "only entries added after this call blocks". On the next call, pass
the ID of the last entry you received, not `$`, or you will skip everything
that arrived between calls:

```
XREAD BLOCK 0 STREAMS events:expired 1730000000123-0
```

Live tail has no acknowledgement and no per-consumer cursor: if the client dies,
it resumes from wherever it chooses to, and nothing tracks what it missed. For
at-least-once processing across restarts, use a consumer group.

## Durable work queue (consumer groups)

This is the pattern that replaces the sweeper: each expiration becomes a unit of
work, delivered at least once, split across a pool of workers, surviving worker
restarts.

Create the group once. `MKSTREAM` creates the stream if the first event has not
been captured yet, so setup does not race against capture:

```
XGROUP CREATE events:expired workers $ MKSTREAM
```

Use `$` to process only events from now on, or `0` to also process everything
currently retained in the stream (see Replay).

Each worker loops. First drain its own pending list (entries it received but
never acknowledged, for example because it crashed mid-processing), then switch
to new entries:

```
# once at startup: claim back anything this worker had in flight
XREADGROUP GROUP workers worker-1 COUNT 100 STREAMS events:expired 0

# steady state: new, never-delivered entries
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:expired >
```

`>` means "entries never delivered to any consumer in this group". After
processing an entry, acknowledge it so it leaves the pending list:

```
XACK events:expired workers 1730000000123-0
```

Ack only after the work is durably done. An entry stays pending until acked, so
a crash between processing and `XACK` results in redelivery, which is why
consumers must be idempotent (natural key: stream name plus entry ID).

### Recovering stuck work

If a worker dies without acking, its entries sit in the group's pending list
under a dead consumer name. Periodically reassign entries idle longer than a
timeout to a live worker:

```
XAUTOCLAIM events:expired workers worker-2 60000 0 COUNT 100
```

`XAUTOCLAIM` also clears dead references: if an entry was trimmed out of the
stream while still pending (see sizing, below), it reads back with a nil field
list and `XAUTOCLAIM` drops it from the pending list as it scans. Treat a
nil-field claimed entry as lost, not as work to do (SPEC.md section 9,
slow-consumer contract).

## Replay

Because entries persist, you can reprocess history. Read a whole stream:

```
XRANGE events:expired - +
```

Read a time window (IDs are millisecond timestamps, so a bare millisecond value
is a valid range bound):

```
XRANGE events:expired 1730000000000 1730000600000
```

Reprocess retained history through a group by creating it at `0` instead of `$`:

```
XGROUP CREATE events:expired replay 0 MKSTREAM
```

## Multiple event types

Streams are per event name. To capture more than expirations, widen the filter:

```
CONFIG SET eventstream.events "expired,del,hset"
```

Each type lands in its own stream (`events:expired`, `events:del`,
`events:hset`), so a consumer subscribes at exactly the granularity it filters
at. There is no combined stream in v0.1. To reconstruct cross-type order for a
key, merge the per-type streams by entry ID; entries in the same millisecond can
tie, and merging cannot break those ties (SPEC.md section 9, ordering).

## Origin database

All destination streams live in database 0. If your keys span multiple
databases, the origin is in each entry's `db` field; filter on it rather than
expecting per-database streams (SPEC.md section 6):

```
# only expirations that fired in db 2
XRANGE events:expired - + | ... filter entries where db == "2"
```

## Discovery

With a known filter the stream names are deterministic
(`<prefix><event-name>`). To enumerate them at runtime:

```
SCAN 0 MATCH events:* TYPE stream
```

Skip keys under `events:#`: that namespace is the module's own control stream
(`events:#control`), not an event stream. The sanitizer never produces `#` in an
event-derived name, so `events:#*` is always internal.

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

## Restricting consumer access

Consumers only need to read the event streams. A least-privilege ACL:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo
```

`~events:*` scopes the user to the module's streams; the command set covers
reading, consumer-group processing, and introspection without any write or
admin access (SPEC.md section 12).

## Handling gaps

None of the above recovers events the module never captured (module disabled,
OOM refusal, crash before fsync, and so on). Those windows are bounded and
detectable, and reconciling over them is its own topic:
[docs/loss-windows.md](loss-windows.md).
