# Cluster design history

[Cluster support](./cluster-support.md) documents the shipped per-node design
as built. That documentation began as a pre-implementation proposal; the
shipped v0.2 follows its slot-pinning scheme, but several proposed mechanisms
were replaced by simpler ones during implementation. This page preserves the
reasoning: the alternatives rejected outright, the proposal mechanisms that
were replaced, and the open questions the implementation resolved.

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

## Replaced mechanisms

Proposed mechanisms replaced by simpler ones during implementation, recorded
here so the reasoning is not lost:

- Topology-change detection: the proposal listed a topology-change server
  event subscription and a low-frequency timer (the cron server event) as
  candidate mechanisms. Neither was built. Detection is purely error-driven on
  the failing mirrored `XADD` (see
  [Cluster support](./cluster-support.md)): re-pinning only matters when there
  is an event to capture, so detecting on the write needs no timer or
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
  nodes" in [Cluster support](./cluster-support.md).

## Resolved questions

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
