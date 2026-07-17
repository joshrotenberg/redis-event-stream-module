# Entry shapes, firehose, and ordering

The default entry shape and the per-event streams cover most consumers. This
page covers the read-side variations: one combined stream instead of many, the
alternative entry formats, cross-stream ordering with `seq`, and the origin
database. The basics are in [Consumer patterns](./consumer-patterns.md).

## One stream for everything (firehose)

To cover every captured event with a single key — one consumer group, no
per-type discovery — enable the firehose (off by default):

```
CONFIG SET eventstream.firehose yes
```

Every captured event is then also written to `events:#firehose`, with the same
fields as its per-event copy (the same `entry-format`, and the same `seq` when
enabled). The per-event streams keep working unchanged, and the consumer
patterns apply to the firehose as-is:

```
XGROUP CREATE events:#firehose workers $ MKSTREAM
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:#firehose >
```

`eventstream.auto-group` ([Durable work queues](./work-queues.md)) covers the
firehose too: with it set, the module creates the group on `events:#firehose`
at first write, so the `XGROUP CREATE` line is unnecessary.

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
other streams; cross-node order is still not provided (see
[Discovery and cluster consumers](./cluster-consumers.md)).

## Alternative entry shapes

The default entry is three fields — `event`, `key`, `db` (see
[Entry shape](./consumer-patterns.md#entry-shape)). `eventstream.entry-format`
(SPEC.md section 6) selects another shape when a consumer needs one:

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
an application timestamp) remains the answer
([Discovery and cluster consumers](./cluster-consumers.md)). For the `json`
format `seq` appears inside the document rather than as a separate field.

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
