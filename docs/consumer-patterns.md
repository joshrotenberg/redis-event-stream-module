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

This page covers what every consumer needs: the entry shape, live tailing,
replay, event selection, retention, and access control. Three companion pages
go deeper:

- [Durable work queues](./work-queues.md): consumer groups, `auto-group`,
  stuck-work recovery, and dead-lettering.
- [Entry shapes, firehose, and ordering](./entry-shapes.md): the combined
  firehose stream, alternative entry formats, `seq`, and the origin database.
- [Discovery and cluster consumers](./cluster-consumers.md): finding the
  destination streams and consuming across a cluster.

Runnable versions of the patterns — live tail, the consumer-group work
queue with stuck-work recovery, and gap-marker reconciliation — ship as small
per-language programs under
[`examples/`](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples):
[Python](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/python)
(redis-py),
[Go](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/go)
(go-redis), and
[Node.js](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/node)
(ioredis), each demonstrating binary-safe `key` handling for its client. The
Rust [`eventstream-client`](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/crates/eventstream-client)
crate additionally covers cluster fan-out.

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
at-least-once processing across restarts, use a consumer group
([Durable work queues](./work-queues.md)).

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
firehose and read a single combined stream
([Entry shapes, firehose, and ordering](./entry-shapes.md)).

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
Servers without hash-field TTLs (Redis before 7.4) never fire `hexpired`.

## Retention and consumer downtime

Retention is a cap, not a delivery guarantee: an entry is trimmed once the
stream exceeds its cap, whether or not anyone has read it. If your consumer
can be down for T seconds, the stream's `maxlen` must exceed
`peak_event_rate * T`, or the backlog is trimmed before the consumer recovers.
The capacity math, per-event overrides, time-based retention, and lag alerting
are covered in [Sizing and retention](./sizing.md).

## Restricting consumer access

Consumers only need to read the event streams. A least-privilege ACL:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo
```

`~events:*` scopes the user to the module's streams; the command set covers
reading, consumer-group processing, and introspection without any write or
admin access (SPEC.md section 12).

To also let the consumer call the module's own introspection commands
(`EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`), grant them by name, which keeps
the user strictly read-only:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo \
  +eventstream.stats +eventstream.streams
```

The custom `@eventstream` ACL category grants the module's commands as a group
instead, but note what it carries: all module commands, including the `write`
command `EVENTSTREAM.PRUNE`, and any future module command automatically.
`EVENTSTREAM.PRUNE` is keyless, so the `~events:*` key pattern does not
constrain it. Its write is limited to reconciling the stream registry
(removing registered names whose destination key is absent), never stream
data, but a consumer meant to be strictly read-only should not carry it. If
registry reconciliation from this user is acceptable:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo +@eventstream
```

The category needs `RM_AddACLCategory` (Redis 7.4+); run `ACL CAT` to confirm
it exists on your server. On Redis 7.2/7.3 the category is unavailable: the
module loads without it and commands are granted by name only (the category's
full equivalent is
`+eventstream.stats +eventstream.streams +eventstream.prune`). See SPEC.md
section 8.

## Handling gaps

None of the above recovers events the module never captured (module disabled,
OOM refusal, crash before fsync, and so on). Those windows are bounded and
detectable, and reconciling over them is its own topic:
[docs/loss-windows.md](loss-windows.md).
