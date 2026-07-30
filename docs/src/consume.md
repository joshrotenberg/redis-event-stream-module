# Consume events

The module writes ordinary Redis Streams. Any client with Streams support can
tail new events, replay retained history, or process work through a consumer
group.

## Read the default entry

A fixed-format entry has three fields:

```text
1785349757949-0
event
expired
key
session:42
db
0
```

The ID's millisecond component is the event time. `key` is binary-safe, and
`db` is the database in which the source event occurred.

## Decode Preview batch envelopes

The stable default writes one fixed-format stream entry per logical event.
Preview `eventstream.write-mode envelope` can combine a compatible run into
one physical entry:

```text
1785349757949-0
format
batch-v1
count
2
events
[{"event":"set","key":"c2Vzc2lvbjo0Mg==","db":0,"class":"string"},{"event":"set","key":"Y2FjaGU6Nw==","db":0,"class":"string"}]
```

`events` is an ordered JSON array, and `key` is padded RFC 4648 base64 because
Redis keys are arbitrary bytes. The natural identity of one logical event is:

```text
<stream name>/<entry ID>/<zero-based array index>
```

Queue-pressure fallback entries retain the default `event`/`key`/`db` shape,
so an envelope-mode consumer must decode both shapes in the same stream.
Consumer-group acknowledgement remains per physical entry: acknowledge an
envelope only after every logical event in it has been handled durably and
idempotently.

Envelope mode is default-off Preview. It uses more Redis process CPU, relaxes
command-unit atomicity, and makes `maxlen` count physical envelopes rather than
logical events. Use the stable default unless that trade is intentional.

## Filter by the current value

The module filters on event name, key name, and source database. It does not
read or store the affected value. Keeping capture metadata-only avoids adding a
type-specific read to Redis's atomic notification path and works equally for
strings, hashes, JSON documents, and module-defined event types.

For a write event, a consumer can apply a content predicate before its side
effect and `XACK`:

1. read the binary-safe `key` and origin `db` from the stream entry;
2. select the origin database, when using standalone Redis;
3. read the value with the appropriate command, such as `GET`, `HGET`, or
   `JSON.GET`; and
4. evaluate the predicate in the consumer.

This observes the value at consumption time. A later write or deletion may
have changed it since the captured event. Removal events such as `expired`,
`evicted`, and `del` have no readable value at all. When event-time content is
part of the correctness contract, have the application write an immutable
domain event or payload to a stream as part of its original operation instead
of reconstructing it from a keyspace notification.

## Tail new events

Block until the next expiration:

```text
XREAD BLOCK 0 STREAMS events:expired $
```

Use `$` only for the first call. After receiving an entry, pass its ID to the
next `XREAD`; using `$` again skips events that arrived between calls.

`XREAD` has no acknowledgement or server-side cursor. The application chooses
where to resume after a restart.

## Replay retained history

Read the complete retained stream:

```text
XRANGE events:expired - +
```

Read a time window by using millisecond IDs:

```text
XRANGE events:expired 1785349700000 1785349800000
```

Retention is still a hard boundary. History trimmed before the requested ID
cannot be replayed.

## Distribute work with a consumer group

Create a group at `0` to process retained history, or at `$` to process only
future entries:

```text
XGROUP CREATE events:expired workers 0 MKSTREAM
```

Each worker first drains entries already pending under its own name, then reads
new work:

```text
XREADGROUP GROUP workers worker-1 \
  COUNT 100 STREAMS events:expired 0

XREADGROUP GROUP workers worker-1 \
  COUNT 100 BLOCK 5000 STREAMS events:expired >
```

Acknowledge only after the side effect is durable:

```text
XACK events:expired workers 1785349757949-0
```

A crash between processing and `XACK` causes redelivery. Make the handler
idempotent using the stream name and entry ID as the natural fixed-entry
identity, or stream name, entry ID, and array index for a Preview envelope.

Recover work abandoned by another consumer:

```text
XAUTOCLAIM events:expired workers worker-2 60000 0 COUNT 100
```

If an entry was trimmed while pending, Redis can return its ID without fields.
Treat that as lost work, not an empty event.

## Avoid group-creation races

Creating a group at `$` after capture has already begun silently skips retained
entries. Either create it at `0`, deploy the group before capture starts, or
let the module create it on a stream's first write:

```text
CONFIG SET eventstream.auto-group workers
```

Auto-group creation is Preview. It creates the named group at `0` for each
event stream and the firehose, but not for the control stream.

## Read multiple event types

Per-event streams let consumers choose their own granularity. One reader can
pass multiple streams and IDs to `XREAD`, or an application can run one reader
per stream.

Enable `eventstream.firehose` when one combined node-local stream is preferable
to per-type reads. If ordering across streams matters, remember:

- IDs totally order entries within one stream.
- Two per-event streams can produce the same millisecond ID.
- `eventstream.entry-seq` adds a node-local sequence, but not a cluster-wide
  order.
- No mode provides a global order across cluster masters.

## Discover streams

Avoid reconstructing names from configuration. Ask the module for the
registered destinations:

```text
EVENTSTREAM.STREAMS
EVENTSTREAM.STREAMS VERBOSE
```

In cluster per-node mode the command is node-local. A cluster consumer
enumerates masters, calls `EVENTSTREAM.STREAMS` directly on each master, unions
the names, and refreshes discovery after topology changes. The shipped Rust
client performs this fan-out:

```bash
eventstream-client consume \
  --url redis://127.0.0.1:7000 \
  --from 0
```

## Use a language example

Small runnable consumers live under `examples/`:

- [Python with redis-py](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/python)
- [Go with go-redis](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/go)
- [Node.js with ioredis](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/examples/node)
- [Rust eventstream-client](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/crates/eventstream-client)
- Rust webhook sink: `examples/webhook-sink`

Each example demonstrates live reads, consumer-group recovery, gap inspection,
and binary-safe key handling. The webhook sink adds ack-after-delivery and
downstream gap propagation; see
[Forward streams to a webhook](sink-connector.md). Read
[Reliability and delivery](reliability.md) before selecting retention and retry
behavior.
