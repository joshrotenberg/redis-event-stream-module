# Reliability and delivery

Redis Event Stream separates capture from consumption. Capture mirrors a Redis
event into a stream; consumption reads an entry and performs application work.
Those two stages have different guarantees.

## The guarantee in one paragraph

With the stable default write mode, each selected event on a healthy capturing
master produces exactly one canonical per-event stream entry, atomically with
the keyspace change. Overall capture is at-most-once because events cannot be
recovered when the module is unloaded, disabled, unable to write, or lost with
unpersisted Redis state. Consumer groups provide at-least-once processing for
entries that remain inside the retention window, so handlers must be
idempotent.

Preview worker modes deliberately change that first sentence. Their records
are written after the triggering command and an accepted in-memory backlog can
be lost in a Redis process crash. Preview `envelope` can represent several
logical events in one physical stream entry.

## Where events can be lost

| Window | Signal | Response |
|---|---|---|
| Module unloaded or capture disabled | Control-stream markers | Reconcile the bounded window |
| Redis crash before persistence | Restart boundary and Redis persistence policy | Accept the configured durability window or use stronger persistence |
| Stream write refused at the memory limit | `dropped_oom` and `events_lost` | Restore memory headroom and reconcile |
| Destination stream cap reached | `dropped_max_streams` | Narrow capture, prune stale registry entries, or raise the cap |
| Cluster slot migration refuses the retry | `dropped_migrating` and a re-pin marker | Reconcile the migration window |
| Preview worker backlog lost in a Redis process crash | Restart boundary; the backlog is not persisted | Use the stable default when command-unit atomicity is required |
| Consumer falls behind retention | Resume ID predates the oldest stream entry | Rebuild from an independent source of truth |

The aggregate `events_lost` counter answers the application-level question:
how many selected source events produced no canonical entry? Detailed
`dropped_*` and `skipped_*` counters explain why.

## Persistence belongs to Redis

The module creates normal Redis Stream entries. Replication, RDB snapshots, AOF
policy, and failover determine their crash durability.

| Redis policy | Approximate crash exposure |
|---|---|
| No persistence | Everything since process start |
| RDB only | Everything since the last snapshot |
| AOF `everysec` | Usually about one second |
| AOF `always` | Smallest window, highest write cost |

AOF `everysec` is the practical baseline for most durable-work deployments.
Replication is asynchronous, so a promoted replica can still miss entries that
had not reached it.

## Read gap markers

Lifecycle discontinuities are written to `<prefix>#control`. Marker actions
include:

- `loaded`
- `disabled` and `enabled`
- `flushed`
- `swapdb`
- `unloading`
- `repinned` in cluster mode

Read them like any other stream:

```text
XRANGE events:#control - +
```

A disable/re-enable pair bounds an intentional capture gap. A module
unload/load pair bounds an upgrade. A re-pin marker identifies where a cluster
node changed its stream tag.

Two cases cannot write a closing marker: a crash and a clean server shutdown.
The next `loaded` marker still tells a consumer that the process restarted, but
Redis alone cannot distinguish the reason.

## Reconcile from an independent source

A marker identifies *when* capture was incomplete. It does not list the missed
keys.

This matters most for expirations. A key that expired during a gap is already
gone after the gap, so a post-gap `SCAN` cannot reconstruct it. Exact recovery
requires an application-owned index or source of truth that outlives the key.
For example, maintain a sorted set of session IDs by expiry time, then query the
marker window with `ZRANGEBYSCORE`.

For non-expiration events, a scoped scan can sometimes rebuild current state,
but it still cannot recreate the exact event sequence. Choose one explicit
policy:

- Reconcile from an application database or expiry index.
- Rebuild a bounded cache from current source state.
- Accept the gap for a best-effort workload.
- Stop downstream processing and require operator intervention.

## Protect slow consumers

Retention applies whether or not a consumer has read an entry. Size each stream
for peak rate multiplied by the longest tolerated outage, with headroom for
bursts.

Under Preview `envelope`, `maxlen` counts physical entries rather than logical
events. The same `maxlen` therefore retains a variable number of logical events
as achieved batch size changes.

Monitor the oldest retained ID and consumer-group lag. If a pending entry is
trimmed, its pending-list reference can survive without fields; recovery tools
must recognize that as data loss.

## Make processing idempotent

Consumer groups redeliver after worker failures. Store or derive an idempotency
key from:

```text
<stream name>/<entry ID>[/<envelope array index>]
```

Acknowledge only after the downstream side effect commits. For poison entries,
move the original stream name, ID, delivery count, and fields to an
application-owned dead-letter stream before acknowledging the source entry.

The complete normative behavior is in
[SPEC.md](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/SPEC.md).
