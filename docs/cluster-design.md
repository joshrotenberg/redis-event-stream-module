# Cluster support: design proposal

Status: proposal, not implemented. This document is the design deliverable for
issue #19. It needs maintainer acceptance before any implementation issue is
filed. v0.1 refuses to load in cluster mode (SPEC.md section 10); this proposes
how a later version could support it.

## The problem

In a Redis Cluster, a key belongs to one of 16384 slots (`CRC16(key) mod
16384`, or `CRC16` of the substring inside the first `{...}` hashtag if present),
and each slot is owned by exactly one master. Two facts collide:

1. Keyspace notifications are node-local: a `set foo` event fires on the master
   that owns `slot(foo)`.
2. A fixed destination stream name like `events:set` hashes to one slot, owned
   by one master. `RM_Call("XADD", "events:set", ...)` executes on the node
   where the event fired, which is almost never the node owning
   `slot(events:set)`. Cluster mode forbids a node from writing keys outside the
   slots it owns, so the XADD fails on every node except the one that happens to
   own `slot(events:set)`.

So a naive port loses capture on N-1 of N masters. The destination must hash to
a slot the firing node owns.

## Proposed design: slot-pinned per-node hashtags

Each master writes its captures into streams whose name carries a hashtag chosen
so the stream hashes to a slot that master owns:

```
destination = <prefix>{<tag>}<sanitize(event)>
```

where `<tag>` is a short string with `CRC16(<tag>) mod 16384 == S` for some slot
`S` the node owns. Because the destination now hashes to an owned slot, the XADD
stays local and succeeds. Routing is still per event name, now also per node:
each `(node, event)` pair has its own stream, for example
`events:{a1}:expired` on one master and `events:{b7}:expired` on another.

The `stream-prefix` charset already reserves `{` and `}` (SPEC.md section 7) for
exactly this, so no config-grammar change is needed to express these names.

### Choosing the hashtag

The module needs, at load and after every topology change, a tag whose CRC16
maps to a currently owned slot.

1. Determine owned slots. Read them from the cluster topology: `RM_Call("CLUSTER
   SHARDS")` (or `CLUSTER SLOTS`), or the module cluster API if it exposes the
   local node's slot ranges. Pick one owned slot `S` deterministically (for
   example the lowest owned slot number) so the choice is stable across reloads
   while the topology is unchanged.
2. Map `S` to a tag. Ship a precomputed table `slot -> short tag` (16384 short
   strings, a few tens of KB, generated once by brute-forcing `CRC16`). The tag
   is opaque; it exists only to steer slot placement. Using the lowest owned
   slot keeps the tag stable, which keeps the stream name stable, which matters
   for consumers (below).
3. No owned slots. A master that owns zero slots (possible transiently during
   resharding, or a misconfigured empty shard) cannot place a local stream at
   all. In that state the module captures nothing and counts the drops under a
   new `dropped_no_owned_slot` counter, logging once. It re-pins as soon as it
   owns a slot again (topology-change handling, below). This is a documented
   capture gap, delimited by gap markers like any other.

Alternative tag sources considered and rejected are in the last section.

### Topology changes and resharding

Slot ownership changes during resharding and failover. The module must react so
its pinned slot stays locally owned.

- Detection. Subscribe to the cluster topology-change server event if the
  wrapper exposes one; otherwise re-derive owned slots on a low-frequency timer
  (for example the cron server event) and on any `RM_Call` that returns a
  cluster redirection. When the currently pinned slot `S` is no longer owned,
  re-pin: pick a new owned slot `S'`, switch new writes to `<prefix>{tag(S')}`.
- Fate of existing entries. The streams under `{tag(S)}` are ordinary keys in
  slot `S`. When slot `S` migrates to another node, those streams migrate with
  it (that is what slot migration does). So no entries are lost; the history for
  `{tag(S)}` simply now lives on the node that received `S`, and this node
  starts a fresh `{tag(S')}` stream. Consumers following `{tag(S)}` streams find
  them on the new owner (they address the stream by name through the cluster,
  which routes to the current slot owner).
- Capture window during migration. Slot migration moves keys and briefly makes
  a slot unavailable for writes on the source (`MIGRATING`/`IMPORTING`). Events
  that fire on the source node for keys in the migrating slot during that window
  may fail to capture (the XADD to `{tag(S)}` can hit `TRYAGAIN`/`ASK`). These
  are counted drops, delimited by gap markers, and reconciled like any other
  loss window (SPEC.md section 9). This window is the one data-safety caveat and
  must be documented as such.

### Failover

Replica promotion in cluster mode is compatible with the current design. The
MASTER-only gate (SPEC.md section 4, gate 3) already means only masters capture.
When a replica is promoted, it takes over the same slots the failed master
owned, so it derives the same pinned slot `S` (deterministic lowest-owned-slot
choice) and continues writing to the same `{tag(S)}` streams, which it now hosts
(they replicated to it before promotion). No consumer-visible name change, no
double capture: the demoted or dead old master is no longer a master and its
gate is closed.

### Discovery across nodes

A consumer that wants all `expired` events must now read one stream per master:
`<prefix>{tag(S_node)}expired` for each master's pinned slot. Discovery becomes a
cluster-wide operation:

- Per-node registry. Extend the persistent registry (issue #21): each node
  SADDs its own destination streams to a registry key that hashes to that node's
  pinned slot, for example `<prefix>{tag(S)}#streams`, so the registry write is
  also local. `EVENTSTREAM.STREAMS` on a given node returns that node's streams.
- Cluster-wide enumeration. `EVENTSTREAM.STREAMS` gains a cluster mode that
  fans out: read each master's local registry (the client library already routes
  per-slot, or the command itself iterates `CLUSTER SHARDS` and reads each
  node), union the results, and return the full set of `(node, event)` streams.
  A consumer then subscribes to all streams for the event types it wants.

### Consumer guidance

- Read the N per-node streams for one logical event type and merge by entry ID.
  Entry IDs are millisecond timestamps with a sequence, assigned independently
  per node, so cross-node ordering is only as good as clock alignment; entries
  within the same millisecond across nodes cannot be totally ordered (the
  same-millisecond tie caveat from SPEC.md section 9, now also across nodes).
- Consumer groups still work per stream. A work queue over `expired` in cluster
  mode is N consumer groups, one per per-node stream, or one group per stream
  consumed by a per-node worker pool.
- After a reshard, the set of per-node streams changes. Consumers re-run
  discovery periodically (or subscribe to the topology-change signal) and adjust
  which streams they read. A `{tag(S)}` stream that stopped growing because `S`
  migrated is drained to its end and then dropped from the read set once the
  consumer confirms (via discovery) that no master pins `S` any more.

### Config surface

- Replace the hard refuse-to-load on `ContextFlags::CLUSTER` with cluster
  support gated by a config, for example `eventstream.cluster-streams`
  (enum: `refuse` | `per-node`, default `refuse` for a conservative rollout).
  `refuse` preserves current behavior; `per-node` enables this design.
- New counter `dropped_no_owned_slot` (above) and an INFO field exposing the
  current pinned slot and tag, so operators can see where a node is writing.
- No change to `stream-prefix` validation: `{` and `}` are already permitted.

## Rejected alternatives

- Source-key hashtag (`<prefix>{foo}set` using the source key `foo` as the
  tag). This does keep writes local (the source key and its stream share a
  slot), but it produces one stream per source key, which defeats per-event
  consolidation and explodes the stream count to the cardinality of the
  keyspace. Rejected.
- Plain node-id prefix (`<prefix><node-id>:set`). A node-id in the name does not
  contain a hashtag, so the whole name is hashed and the stream still lands in
  an arbitrary slot the node likely does not own. It does not solve the slot
  placement problem at all. Rejected.
- Cross-slot write with redirection handling (let the XADD go to
  `<prefix>event` wherever it hashes, and follow the MOVED). Every capture would
  become a cross-node round trip on the hot path, on the main thread, inside the
  post-notification job. Rejected on latency grounds; the whole point is a local
  write.

## Open questions for the maintainer

1. Is cluster support wanted for the first stable release, or is single-node
   plus documented cluster-unsupported acceptable for longer? This design is
   non-trivial and adds a data-safety caveat (the migration window).
2. Does the target `redismodule-rs` version expose a cluster topology-change
   server event and a local-slots API, or must the module derive both from
   `RM_Call("CLUSTER ...")` on a timer? This determines how promptly re-pinning
   happens.
3. Is the precomputed `slot -> tag` table (a generated source file) acceptable,
   or is a runtime CRC16 search preferred to avoid shipping the table?
