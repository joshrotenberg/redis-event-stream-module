# Discovery and cluster consumers

How a consumer finds the destination streams, and how to read them in cluster
mode, where one logical event type is spread across per-node streams. The
operator-side view of the cluster mechanism (slot pinning, re-pinning after a
reshard, failover) is [Cluster support](./cluster-support.md).

## Discovery

The module tracks every destination stream it has written in a persistent
registry, exposed through a command:

```
EVENTSTREAM.STREAMS
```

This returns the registered stream names and survives restart (RDB or AOF). It
is an append-only log of names ever written, so a listed stream may since have
been trimmed to empty or deleted. It is `readonly`, so it works on replicas. For
a liveness-aware answer in one round-trip, use `VERBOSE`:

```
EVENTSTREAM.STREAMS VERBOSE
```

Each row is `[name, exists, length]` — `exists` is `0`/`1` (`EXISTS`) and
`length` is the current `XLEN`. The two are independent: an absent (deleted) key
reads `0, 0`, while a present-but-empty or foreign non-stream key reads `1, 0`.
This replaces the older client-side join (one `EVENTSTREAM.STREAMS` plus an
`EXISTS`/`XLEN` per name, in db 0) with a single command; it mutates nothing and,
like the bare and `WITHSTATS` forms, runs on replicas.

To reconcile the registry itself — dropping dead names so its size does not grow
without bound on a long-lived deployment — opt in explicitly with the separate
command `EVENTSTREAM.PRUNE`:

```
EVENTSTREAM.PRUNE
```

`EVENTSTREAM.PRUNE` removes the registered names whose key is **absent**
(deleted, `EXISTS 0`) and returns the count removed. Absence is the only trigger:
a present key that is empty, or a foreign non-stream key parked at the name, is
not pruned. It is the only command that mutates the registry, is never automatic,
and its `SREM` replicates like the original registration (minus the verify-oom
flag, since pruning frees memory), so replicas and the AOF/RDB converge. A pruned
name re-registers on its next captured write, so pruning is safe to run
periodically. `EVENTSTREAM.PRUNE` is a `write` command and runs on a primary, not
a replica; keeping it separate is what lets the bare, `VERBOSE`, and `WITHSTATS`
forms of `EVENTSTREAM.STREAMS` stay `readonly` and serve discovery on replicas.

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
across nodes (SPEC.md section 9, ordering). The `seq` field (`entry-seq`, see
[Entry shapes, firehose, and ordering](./entry-shapes.md)) tiebreaks per node,
not across nodes: each node's counter is independent, so it does not resolve a
cross-node tie. Treat cross-node order within one millisecond as unspecified; if
you need a total order, carry an application timestamp in the value, not the
entry ID.

Re-pinning. A reshard that moves a node's pinned slot makes the node re-pin to a
new tag and continue under a new stream name; it writes a `repinned` marker to
the new control stream (`events:{new_tag}#control`) at the boundary. Consumers
that cache the discovered stream set should re-run discovery periodically, or
when they observe a `repinned` marker, so they pick up the new name. The old
stream's history is not lost: it lives in the migrated slot's keys, which moved
to the slot's new owner and remain readable by name through the cluster. A
`{tag}` stream that stopped growing because its slot migrated away is drained
to its end and then dropped from the read set once the consumer confirms (via
discovery) that no master pins that tag any more.

Failover. Tag selection is deterministic in a node's owned-slot set (the module
picks the first slot it owns in a fixed walk), so a replica promoted to master
owns the same slots and re-derives the same tag, continuing the same `{tag}`
streams (which replicated to it before promotion). Stream names are stable across
a failover, and the MASTER-only gate means the demoted node stops capturing, so
there is no double capture.

### Consumer groups in per-node mode

Consumer groups still work per stream: a work queue over `expired` in cluster
mode is N consumer groups, one per per-node stream, or one group per stream
consumed by a per-node worker pool.

`eventstream.auto-group` ([Durable work queues](./work-queues.md)) composes
with per-node mode: each node creates the named group on its own `{tag}`-pinned
streams as it writes them, so the N per-node streams come with their group
already present — no operator-side `XGROUP CREATE` fan-out, and no need to
re-run it after a reshard, since a node re-pinned to a new tag provisions the
group on the new stream's first write. This is exactly the case where
module-side creation at stream birth beats an external sweep: the per-node
stream names change after resharding. The group is created at `0` on each
stream, so the same slow-consumer caveats apply per node (SPEC.md section 9).
