# Cluster support

Cluster support shipped in v0.2 (issues #45, #46, #47 under epic #19). v0.1
refused to load in cluster mode; v0.2 keeps that refusal as the default and
adds opt-in per-node capture via `eventstream.cluster-streams per-node`.
SPEC.md section 10 is the normative description of this behavior; this page
explains the mechanism, the consumer-facing consequences, and the design
history.

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

Both halves of this are confirmed against a live 3-master cluster in
`tests/cluster.rs`. The exact failure is `ERR Attempted to access a non local
key in a cluster node` (a hard local refusal, not a followable MOVED), and it
hits every module-written key independently: because `events:expired`,
`events:set`, `events:#control`, and `events:#streams` each hash to different
slots, even the node that owns one of them fails on the others. The test also
confirms the fix: a per-node hashtag chosen to hash to a slot the node owns
(`events:{tag}:expired`) makes the write succeed locally on every node. A
corollary the test makes concrete: all of a node's module-written keys (event
streams, the control stream, and the registry) must share one node tag so they
co-locate on that node.

## Design: slot-pinned per-node hashtags

Each master writes its captures into streams whose name carries a hashtag
chosen so the stream hashes to a slot that master owns:

```
destination = <prefix>{<tag>}<sanitize(event)>
```

where `<tag>` is a string with `CRC16(<tag>) mod 16384 == S` for some slot `S`
the node owns. Because the destination hashes to an owned slot, the XADD stays
local and succeeds. Routing is still per event name, now also per node: each
`(node, event)` pair has its own stream, for example `events:{a1}expired` on
one master and `events:{b7}expired` on another. The tag is shared across the
node's event streams, control stream, and registry (`events:{tag}expired`,
`events:{tag}#control`, `events:{tag}#streams`) so they co-locate; distinct
nodes pin distinct tags, because a tag's slot is owned by exactly one node.

The `stream-prefix` charset already reserves `{` and `}` (SPEC.md section 7)
for exactly this, so these names need no config-grammar change to express.

### Choosing the hashtag

Tag selection is lazy. A node owns no slots at module load (it joins the
cluster afterward), so the tag is selected on the first captured event, when
slots are known, and then cached.

- Ownership is established by probing, not by reading topology. For a
  candidate tag, the module issues a non-destructive replicated write, `XADD
  {tag}#slotprobe NOMKSTREAM`, with the same call options as the real mirrored
  writes. The slot-ownership check that rejects a non-local key applies to
  replicated writes, not reads (a plain read runs locally and would falsely
  pass on every node), so the probe must be a write; `NOMKSTREAM` on a
  non-existent stream is a no-op that creates nothing. An owned slot answers
  Ok; a non-owned slot answers the local-refusal error; a slot mid-migration
  answers `TRYAGAIN`/`ASK`, so selection never picks a slot that is leaving.
- The candidate tag for each slot comes from
  `RedisModule_ClusterCanonicalKeyNameInSlot(slot)`, which yields a key name
  hashing to a given slot, so scanning slots has guaranteed coverage. That API
  was added after Redis 7.2: on 7.2 the bound function pointer is null (calling
  it would panic across the FFI boundary and abort the server, issue #45), so
  the module falls back to a runtime CRC16 search (issue #116). It carries the
  ~15-line CRC16-CCITT key-hashing function and builds a slot-to-tag table
  once, at first fallback use, by hashing synthetic candidates `es{i}` until
  every slot has one (64 KiB, a few milliseconds; an exhaustive unit test
  proves every slot's tag hashes back to that slot). Coverage is therefore
  exhaustive on both paths: a node that owns any slot finds a tag, however
  skewed the slot ownership.
- Slots are visited in a fixed scattered order (an odd stride coprime with
  16384), so an owned slot is found within a few probes on a typical cluster
  while the walk still covers all 16384 slots in the worst case. The walk is
  deterministic in the node's owned-slot set, which keeps the tag — and so the
  stream names — stable across restarts and failovers while the topology is
  unchanged.
- A master that owns zero slots (possible transiently during resharding, or a
  misconfigured empty shard) cannot place a local stream at all. In that state
  the module captures nothing: each event is counted under
  `dropped_no_owned_slot`, the first occurrence is logged, and selection is
  retried on the next captured event, so capture resumes as soon as the node
  owns a slot again.

Alternative tag sources considered and rejected are in the design-history
section.

### Topology changes and resharding

Slot ownership changes during resharding and failover. The module reacts so
its pinned slot stays locally owned.

Detection is reactive and error-driven: there is no timer and no
topology-change event subscription. Re-pinning only matters when there is an
event to capture, so the module detects departure on the failing mirrored
`XADD` itself. Three triggers lead to the same re-pin path:

- The local-refusal error (`Attempted to access a non local key in a cluster
  node`): the pinned slot has migrated away (issue #46).
- A `TRYAGAIN`/`ASK` refusal (issue #75): the pinned slot is still
  `MIGRATING`/`IMPORTING`, an earlier signal of the same departure, so the
  module re-pins immediately instead of dropping events until the migration
  completes.
- A failing ownership probe on an unclassified `XADD` failure (issue #76). The
  local-refusal text is observed behavior, not a documented error code, so
  detection does not depend on it alone: when an `XADD` fails with a message
  the module does not recognize, it re-verifies ownership of the pinned tag
  with the same selection probe. A failing probe triggers the re-pin
  regardless of the message text, counted in `repins_probe_detected` in
  addition to `repins` (a nonzero value means the string match stopped
  working; report the new message form upstream). The probe is budgeted: a
  tag the probe verifies as owned is cached and not re-probed, and every
  successful mirrored write resets that cache, so the fallback costs at most
  one probe per streak of unclassified failures while a stale verification
  can never mask a later migration.

On detection the module re-pins: it clears the cached tag, increments
`repins`, re-selects a currently owned slot (the selection probe fails with
`TRYAGAIN`/`ASK` on a slot mid-migration, so the leaving slot is never
re-picked), writes a `repinned` gap marker to the new control stream, and
retries the entry once on the new tag, so the triggering event is usually
captured rather than dropped. The retry is bounded: a refusal on the retry is
a counted drop, never another re-pin.

Fate of existing entries: the streams under `{tag}` are ordinary keys in the
pinned slot. When that slot migrates to another node, those streams migrate
with it (that is what slot migration does). No entries are lost; the history
simply now lives on the node that received the slot, and this node starts
fresh streams under the new tag. Consumers following the old streams find them
on the new owner, because they address the stream by name through the cluster,
which routes to the current slot owner.

Capture window during migration: this is the one data-safety caveat. While a
slot is `MIGRATING`/`IMPORTING`, a write can be refused (`TRYAGAIN`/`ASK`);
the refusal triggers the early re-pin above, and an event still refused after
the one retry is counted under `dropped_migrating` — not the generic
`dropped_xadd_error`, so routine resharding does not read as a broken write
path — delimited by the gap markers and reconciled like any other loss window
(SPEC.md section 9). Single-shard clusters (one master owning all slots) never
reshard the pinned slot and are the safest deployment.

### Failover

Replica promotion in cluster mode needs no extra work. The MASTER-only gate
(SPEC.md section 4, gate 3) already means only masters capture. When a replica
is promoted, it takes over the same slots the failed master owned; tag
selection is deterministic in the owned-slot set (the fixed scattered walk),
so the promoted node re-derives the same tag on its first captured event and
continues writing to the same `{tag}` streams, which it already hosts (they
replicated to it before promotion). No consumer-visible name change, no double
capture: the demoted or dead old master is no longer a master and its gate is
closed.

### Discovery across nodes

A consumer that wants all `expired` events reads one stream per master:
`<prefix>{tag}expired` for each master's pinned tag. Discovery is per node
plus a client-side union:

- Per-node registry. Each node SADDs its destination streams to a registry key
  that shares its pinned tag, `<prefix>{tag}#streams`, so the registry write
  is also local. `EVENTSTREAM.STREAMS` on a given node returns that node's
  registry.
- Cluster-wide enumeration is a client-side fan-out. A module command runs
  locally and cannot read another master's keyspace, so `EVENTSTREAM.STREAMS`
  is node-local by design (this resolved issue #47): the consumer enumerates
  the masters (`CLUSTER SHARDS`, or the client library's topology), runs
  `EVENTSTREAM.STREAMS` on each, and unions the results into the full set of
  `(node, event)` streams. See "Cluster consumers" in
  [Consumer patterns](./consumer-patterns.md) for the recipe.

### Consumer guidance

- Read the N per-node streams for one logical event type and merge by entry
  ID. Entry IDs are millisecond timestamps with a sequence, assigned
  independently per node, so cross-node ordering is only as good as clock
  alignment; entries within the same millisecond across nodes cannot be
  totally ordered (the same-millisecond tie caveat from SPEC.md section 9, now
  also across nodes).
- Consumer groups still work per stream. A work queue over `expired` in
  cluster mode is N consumer groups, one per per-node stream, or one group per
  stream consumed by a per-node worker pool.
- `eventstream.auto-group` composes with per-node mode: each node creates the
  named group on its own `{tag}`-pinned streams as it writes them, so the N
  per-node streams come with their group already present — no operator-side
  `XGROUP CREATE` fan-out, and no need to re-run it after a reshard, since a
  node re-pinned to a new tag provisions the group on the new stream's first
  write. This is exactly the case where module-side creation at stream birth
  beats an external sweep: the per-node stream names change after resharding.
  The group is created at `0` on each stream, so the same slow-consumer caveats
  apply per node (SPEC.md section 9).
- After a reshard, the set of per-node streams changes. Consumers re-run
  discovery periodically, or when they observe a `repinned` marker on a
  control stream, and adjust which streams they read. A `{tag}` stream that
  stopped growing because its slot migrated is drained to its end and then
  dropped from the read set once the consumer confirms (via discovery) that no
  master pins that tag any more.

### Config surface and observability

- `eventstream.cluster-streams` (enum: `refuse` | `per-node`, IMMUTABLE,
  load-time only) chooses the behavior. `refuse` (the default) keeps the v0.1
  refusal to load in cluster mode: no silent loss, no half-working
  deployments. `per-node` enables the design on this page.
- Counters, in `INFO eventstream` and `EVENTSTREAM.STATS`:
  `dropped_no_owned_slot` (events dropped for want of an owned slot),
  `dropped_migrating` (events still refused after the re-pin retry, dropped in
  the migration window), `repins` (times the node re-pinned), and
  `repins_probe_detected` (re-pins triggered by the probe fallback rather than
  the recognized error text). The INFO section also exposes
  `cluster_per_node` and the current `cluster_pinned_tag`, so operators can
  see where a node is writing.
- Each re-pin writes a `repinned` gap marker to the new control stream
  (`<prefix>{tag}#control`), delimiting the discontinuity in the observable
  trail (SPEC.md section 9).
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

## Design history

This page began as a pre-implementation proposal. The shipped v0.2 follows its
slot-pinning scheme, but several proposed mechanisms were replaced by simpler
ones during implementation. Recorded here so the reasoning is not lost:

- Topology-change detection: the proposal listed a topology-change server
  event subscription and a low-frequency timer (the cron server event) as
  candidate mechanisms. Neither was built. Detection is purely error-driven on
  the failing mirrored `XADD` (above): re-pinning only matters when there is
  an event to capture, so detecting on the write needs no timer or
  topology-event plumbing, and the migration-window (`TRYAGAIN`/`ASK`) and
  probe-fallback triggers were added later (issues #75, #76) to catch the
  departure earlier and without depending on the exact error text.
- Owned-slot derivation: the proposal read owned slots from `RM_Call("CLUSTER
  SHARDS")` and picked the lowest owned slot. The shipped module never parses
  topology output; it probes candidate tags with the same replicated-write
  locality rule the real writes obey, which cannot disagree with what an
  actual capture would experience. Stability across restarts and failovers
  comes from the deterministic fixed-order slot walk instead of the
  lowest-slot rule.
- Slot-to-tag table: the proposal shipped a precomputed 16384-entry `slot ->
  tag` table as a generated source file. Not built: on servers with the
  canonical-name API the server itself supplies a name for any slot, and the
  Redis 7.2 fallback builds its table at runtime in a few milliseconds (issue
  #116) — no `build.rs`, no 16384-entry generated file to review.
- Cluster-wide `EVENTSTREAM.STREAMS`: the proposal gave the command a cluster
  mode that fans out to every master server-side. Resolved instead (issue #47)
  as client-side fan-out over the per-node registries, keeping the command
  node-local, readonly, and free of cross-node calls; see "Discovery across
  nodes" above.

### Resolved questions

The proposal closed with three open questions; all were answered by the v0.2
implementation:

1. Is cluster support wanted for the first stable release? Yes — shipped in
   v0.2, opt-in via `eventstream.cluster-streams per-node` with `refuse` as
   the conservative default.
2. Does the module get a topology-change server event or a local-slots API, or
   must it derive topology on a timer? Neither is needed: detection is
   error-driven on the failing write, so re-pinning happens on the first
   captured event after the pinned slot leaves (or earlier, mid-migration, via
   `TRYAGAIN`/`ASK`).
3. Precomputed `slot -> tag` table or runtime CRC16 search? Runtime CRC16
   search (issue #116), built once at first fallback use and only on Redis 7.2
   (later servers ask the canonical-name API); an exhaustive unit test proves
   every slot's tag hashes back to that slot.
