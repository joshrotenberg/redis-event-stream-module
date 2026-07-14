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

This is the pattern that replaces periodic keyspace scanning: each expiration
becomes a unit of work, delivered at least once, split across a pool of workers,
surviving worker restarts.

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

### Letting the module create the group (`eventstream.auto-group`)

The `XGROUP CREATE ... $ MKSTREAM` recipe above is race-free only when the
consumers deploy *before* the module: `MKSTREAM` makes an empty stream and the
group at `$` sees every later entry. In the common order — enable capture, then
roll out workers — the stream already holds entries when the recipe runs, and a
group at `$` silently skips everything captured before it. The fix is to create
the group at `0` (see Replay), but that requires knowing which ordering you are
in.

`eventstream.auto-group` removes the decision. Name a group and the module
creates it at `0` on each destination stream the first time it writes to that
stream, so the group exists from the stream's first entry no matter which side
deployed first:

```
CONFIG SET eventstream.auto-group workers
# or as a load-time arg: --loadmodule ... auto-group workers
```

Workers then skip `XGROUP CREATE` entirely — drain their pending list, then
tail:

```
XREADGROUP GROUP workers worker-1 COUNT 100 STREAMS events:expired 0   # backlog
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:expired >
```

Notes:

- Off by default; empty means group creation stays operator-side (this page's
  manual recipe still works unchanged).
- The group is created with the same replicated, memory-checked write options as
  a mirrored entry, so it appears on replicas and survives an AOF replay.
- Idempotent: re-creating an existing group is a no-op (`BUSYGROUP` is treated as
  success), and a `FLUSHALL` that wiped the stream re-provisions the group on the
  next write.
- It covers per-event streams and the firehose (`events:#firehose`), but not the
  control stream (`events:#control`), which is not a work queue.
- Setting it at runtime provisions the group on each stream's **next** write, not
  retroactively: a stream that never fires again keeps no group.
- It does not upgrade the delivery guarantee. A group at `0` still loses entries
  trimmed by `maxlen` before a slow consumer catches up (SPEC.md section 9,
  slow-consumer contract). The win is operational: the group exists from birth,
  so deployment ordering stops mattering.
- Watch `eventstream_autogroup_created` / `eventstream_autogroup_failed` in
  `INFO eventstream` to confirm provisioning.

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
at. To consume across types, either open one reader per stream, or enable the
firehose (next section) and read a single combined stream.

### Hash-field expirations (Redis 7.4+)

Hash-field TTLs (`HEXPIRE` and friends) fire their own event, `hexpired`,
under the hash class — the default `expired` filter does not match it, so a
consumer relying on `events:expired` sees nothing when fields expire. Widen
the filter to cover both:

```
CONFIG SET eventstream.events "expired,hexpired"
```

Entries land in `events:hexpired` with `key` set to the hash key; the expired
field name is not part of the keyspace notification, so consumers that need it
must track field membership themselves (SPEC.md section 6). When the last
field expires the emptied hash is deleted and a separate `del` event fires.
Servers without hash-field TTLs (Redis 7.2, Valkey 8.x) never fire
`hexpired`.

## One stream for everything (firehose)

To cover every captured event with a single key — one consumer group, no
per-type discovery — enable the firehose (off by default):

```
CONFIG SET eventstream.firehose yes
```

Every captured event is then also written to `events:#firehose`, with the same
fields as its per-event copy (the same `entry-format`, and the same `seq` when
enabled). The per-event streams keep working unchanged, and all the patterns
above apply to the firehose as-is:

```
XGROUP CREATE events:#firehose workers $ MKSTREAM
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:#firehose >
```

`eventstream.auto-group` (above) covers the firehose too: with it set, the
module creates the group on `events:#firehose` at first write, so the
`XGROUP CREATE` line is unnecessary.

Ordering. The firehose is a single stream, so its entry IDs give a total order
across all event types on the node — including entries that landed in the same
millisecond, which merging per-event streams by ID cannot order (SPEC.md
section 9). To reconstruct cross-type order for a key, read the firehose and
filter on the `key` field instead of merging per-type streams.

Cost and sizing. Enabling the firehose doubles write amplification: each
captured event becomes two stream writes plus trim instead of one (SPEC.md
section 11). The firehose retains the last `maxlen` entries across all event
types combined, so a busy type can crowd a quiet one out of the window; size
`maxlen` for the total event rate, not the per-type rate.

Toggling at runtime takes effect on the next captured event; events captured
while the firehose was off are not replayed into it. In cluster per-node mode
the firehose is per node (`events:{tag}#firehose`) and re-pins with the node's
other streams; cross-node order is still not provided (see Cluster consumers).

## Alternative entry shapes

The default entry is three fields — `event`, `key`, `db` (see the top of this
doc). `eventstream.entry-format` (SPEC.md section 6) selects another shape when
a consumer needs one:

```
CONFIG SET eventstream.entry-format minimal   # drop the redundant event field
CONFIG SET eventstream.entry-format verbose   # add a class field
CONFIG SET eventstream.entry-format json       # one JSON document field
```

- `fixed` (default): `event`, `key`, `db`, byte-for-byte the historical schema.
- `minimal`: `format=minimal`, `key`, `db` — the stream name already encodes the
  event, so `event` is dropped.
- `verbose`: adds a `class` field (`string`, `hash`, `expired`, …).
- `json`: `format=json` plus a single `data` field holding
  `{"event":…,"key":<base64>,"db":…}`. The key is base64 because keys are
  arbitrary binary; decode it client-side.

Every non-`fixed` entry carries a leading `format` field, so you can tell the
shapes apart in a stream that mixes them. That matters because `entry-format` is
live-settable: a `CONFIG SET` changes the shape of subsequent entries only, so a
single stream can hold `fixed` entries (no `format`) followed by, say, `json`
entries (`format=json`). Read the `format` field first and branch on it; treat a
missing `format` as `fixed`. If you never change the format at runtime, the
stream is uniform and you can skip the check.

## Total order within a node (seq)

Entry IDs order entries within one stream, but two entries in different streams
that share a millisecond have no order from their IDs alone (SPEC.md section 9).
Enable `eventstream.entry-seq` at load time to append a `seq` field — a
process-global monotonic counter — to every entry:

```
# load-time only (immutable):  loadmodule ... entry-seq yes
```

Merging per-type streams by `seq` then totally orders same-millisecond entries
on one node. `seq` is per node and per process: it resets to 0 on restart, and
in cluster mode each node has its own counter, so it does **not** order entries
across nodes or across a restart — for those, the entry ID (and, across nodes,
an application timestamp) remains the answer below. For the `json` format `seq`
appears inside the document rather than as a separate field.

## Origin database

All destination streams live in database 0. If your keys span multiple
databases, the origin is in each entry's `db` field; filter on it rather than
expecting per-database streams (SPEC.md section 6):

```
# only expirations that fired in db 2
XRANGE events:expired - + | ... filter entries where db == "2"
```

If you only care about a subset of databases, filter at the source instead with
`eventstream.source-dbs` (SPEC.md section 7): events from other databases are
then never captured, trimmed, replicated, or read, rather than filtered
client-side after the write:

```
CONFIG SET eventstream.source-dbs 2      # capture only db 2
CONFIG SET eventstream.source-dbs 0,2,5  # or a set of databases
```

Client-side `db` filtering still works and remains the right choice when a
single consumer wants most databases but distinguishes a few; the module-side
filter is for cutting the write and memory cost of databases no consumer wants.

## Discovery

The module tracks every destination stream it has written in a persistent
registry, exposed through a command:

```
EVENTSTREAM.STREAMS
```

This returns the registered stream names, survives restart (RDB or AOF), and
works on replicas. It is an append-only log of names ever written, so a listed
stream may since have been trimmed to empty or deleted; check `XLEN` if you need
liveness.

With a known filter the stream names are also deterministic
(`<prefix><event-name>`), and a `SCAN` fallback works:

```
SCAN 0 MATCH events:* TYPE stream
```

Skip keys under `events:#` when enumerating per-event streams: that namespace
holds the module's own control stream (`events:#control`), registry
(`events:#streams`), and the opt-in firehose (`events:#firehose`), never an
event-derived stream. The sanitizer never produces `#` in an event-derived
name, so `events:#*` is always module-written.

In cluster per-node mode the registry is itself per-node and tagged
(`events:{tag}#streams`), so `EVENTSTREAM.STREAMS` returns only the streams of
the node it runs on. Enumerating the whole cluster is a fan-out; see the next
section.

## Cluster consumers

In cluster mode with `eventstream.cluster-streams per-node`, capture is
node-local: each master pins a hash tag `{tag}` that hashes to a slot it owns and
writes all of its streams under it (`events:{tag}expired`, `events:{tag}#control`,
`events:{tag}#streams`). One logical event type is therefore spread across N
streams, one per master, with distinct tags. A cluster consumer reads all of
them and merges.

Discovery is a client-side fan-out. A module command runs locally and cannot
read another master's keyspace, so `EVENTSTREAM.STREAMS` reports only its own
node. Enumerate the masters and union their answers:

```
# for each master (from CLUSTER SHARDS / your client's topology):
redis-cli -h <master> -p <port> EVENTSTREAM.STREAMS
# union the results; each name already carries the owning node's {tag}
```

Or use the shipped client/library instead of hand-rolling this. The
`eventstream-client` crate (`crates/eventstream-client`) packages the fan-out,
the merged-by-entry-ID reader, and `#control` gap-marker reads (including
`repinned`-driven re-discovery) as a library, and ships a binary over it:
`eventstream-client consume --url redis://<any-node>:<port>` discovers the
streams cluster-wide and tails them merged, so operators get the fan-out
without a `redis-cli` loop and callers get the logic without reimplementing
SPEC.md sections 9-10.

Once you have the names, read them from any node: a cluster-aware client routes
each `events:{tag}event` to its slot owner by the tag. To follow one logical
event type, `XREAD` across that type's per-node streams and merge by entry ID:

```
XREAD COUNT 100 BLOCK 1000 STREAMS \
  events:{tag_a}expired events:{tag_b}expired events:{tag_c}expired \
  $ $ $
```

Merge caveat. Entry IDs are millisecond timestamps assigned independently on
each node, so two entries from different nodes can share a millisecond. Merging
by entry ID orders within a node but cannot totally order a same-millisecond tie
across nodes (SPEC.md section 9, ordering). The `seq` field (`entry-seq`, above)
tiebreaks per node, not across nodes: each node's counter is independent, so it
does not resolve a cross-node tie. Treat cross-node order within one millisecond
as unspecified; if you need a total order, carry an application timestamp in the
value, not the entry ID.

Re-pinning. A reshard that moves a node's pinned slot makes the node re-pin to a
new tag and continue under a new stream name; it writes a `repinned` marker to
the new control stream (`events:{new_tag}#control`) at the boundary. Consumers
that cache the discovered stream set should re-run discovery periodically, or
when they observe a `repinned` marker, so they pick up the new name. The old
stream's history is not lost: it lives in the migrated slot's keys, which moved
to the slot's new owner and remain readable by name through the cluster.

Failover. Tag selection is deterministic in a node's owned-slot set (the module
picks the first slot it owns in a fixed walk), so a replica promoted to master
owns the same slots and re-derives the same tag, continuing the same `{tag}`
streams (which replicated to it before promotion). Stream names are stable across
a failover, and the MASTER-only gate means the demoted node stops capturing, so
there is no double capture.

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

## Restricting consumer access

Consumers only need to read the event streams. A least-privilege ACL:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo
```

`~events:*` scopes the user to the module's streams; the command set covers
reading, consumer-group processing, and introspection without any write or
admin access (SPEC.md section 12).

To also let the consumer call the module's own introspection commands
(`EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`), grant the custom `@eventstream`
ACL category, which carries both as a group:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo +@eventstream
```

The `@eventstream` category needs `RM_AddACLCategory` (Redis 7.4+, and Valkey
builds that expose it); run `ACL CAT` to confirm it exists on your server. On
Redis 7.2/7.3 the category is unavailable — the module loads without it and you
grant the commands by name instead (`+eventstream.stats +eventstream.streams`).
See SPEC.md section 8.

## Handling gaps

None of the above recovers events the module never captured (module disabled,
OOM refusal, crash before fsync, and so on). Those windows are bounded and
detectable, and reconciling over them is its own topic:
[docs/loss-windows.md](loss-windows.md).
