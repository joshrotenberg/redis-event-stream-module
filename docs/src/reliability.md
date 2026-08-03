# Reliability and delivery

Redis Event Stream separates capture from consumption. Capture mirrors a Redis
event into a stream; consumption reads an entry and performs application work.
Those two stages have different guarantees.

## The guarantee in one paragraph

Redis Event Stream is a best-effort, gap-aware live feed. "Best effort" does
not mean silent loss: healthy stable-mode capture targets exact canonical
records, definite failures increment `events_lost`, and generation checkpoints
can identify an uncertain restart window. It does mean the module is not a
transactional CDC log, application outbox, WAL, or Kafka replacement.

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

## Persist health checkpoints

Set a load-time cadence to persist low-volume status entries in the same
control stream:

```text
loadmodule /path/to/libredis_event_stream_module.so \
  control-checkpoint-ms 5000
```

`0` (the default) disables checkpoint writes. Positive values range from 100
milliseconds to 24 hours and are immutable until the next module load. Start
with one to five seconds unless the reconciliation objective requires a tighter
uncertainty bound; write and persistence cost scales with checkpoint rate.

Every lifecycle marker and checkpoint carries a random `generation` ID for the
current module load. Checkpoints additionally carry `forwarded`, `events-lost`,
`dropped`, Preview queue counters, `handler-panics`, and `last-error-time`.
Interpret them as evidence:

| State | Evidence |
|---|---|
| Healthy | Same generation advances and `events-lost` stays flat |
| Known loss | `events-lost` increases or the consumer cursor was trimmed |
| Uncertain restart | A new `loaded` generation follows one without `unloading` |
| Graceful boundary | The old generation wrote `unloading` before the new one loaded |
| Stale | Expected checkpoints stop while Redis remains reachable |

The terms are intentionally narrow:

- A **generation** is one module load on one Redis node.
- A **checkpoint** is a durable, generation-local observation, not an
  acknowledgement of every source command.
- A generation is **closed** only when its `unloading` entry survives.
- **Uncertain** means a later `loaded` generation has no durable closure for
  the prior one; it does not assert that loss occurred.
- **Retention overrun** means the consumer's resume point or the checkpoint
  that bounded a gap is older than the first retained entry.

## Consumer assessment algorithm

Track each control stream independently; cluster nodes have independent
generations and checkpoint histories.

1. Persist the last processed control ID, generation, checkpoint counters, and
   data-stream resume IDs.
2. On each checkpoint in the same generation, compare `events-lost` with the
   prior value. An increase is known loss. Otherwise the result is only “no
   loss observed.”
3. Treat checkpoints as stale when Redis is reachable but none arrives within
   the locally chosen grace period, normally a small multiple of
   `checkpoint-ms`.
4. On a new `loaded` generation, classify the boundary as graceful only when
   the previous generation wrote `unloading`; otherwise classify it as
   uncertain. Bound reconciliation below by the prior generation's last
   retained checkpoint when one exists.
5. Before resuming any stream, compare the saved ID with its oldest retained
   ID. If the saved position or the lower checkpoint bound was trimmed, report
   retention overrun and reconcile from an independent source.
6. Advance saved positions only after the downstream handling and state update
   commit together.

The last durable checkpoint narrows an uncertain window. It cannot prove that
no events were lost, identify missing keys, or provide an exact missing count.
In Preview worker modes, `async-queue-depth` is only the instantaneous queue:
worker-held events may already have left it without reaching a stream.

Checkpoint writes replicate, persist, trim, and fail like other writes. Size
the `#control` retention window accordingly, for example with a
`maxlen-overrides` entry. Absence of an error checkpoint is not proof of health
because the same OOM or topology condition may block both data and control
writes.

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
