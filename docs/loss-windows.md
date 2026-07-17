# Loss windows and gap reconciliation

This module is a live mirror, not a write-ahead log. Within the retention
window, capture is at-least-once for events the module actually saw; overall,
capture is at-most-once, because some events are never seen or never written.
This document lists every way an event can be lost, how to detect it, and how to
reconcile a gap without rescanning the whole keyspace.

All claims here match [SPEC.md](../SPEC.md) sections 9 and 10 exactly. If this
document and the spec ever disagree, the spec wins.

## The guarantee in one sentence

On a healthy capturing master, each selected event produces exactly one stream
entry, atomically with the keyspace change. Across restarts, disabled windows,
memory pressure, and failovers, some selected events produce no entry, and those
are not recoverable from the streams. Consumer groups then deliver whatever was
captured at least once, within the retention window.

## Loss windows

| Window | Cause | Counter | Detection and reconciliation |
|---|---|---|---|
| Module not loaded / `enabled no` | Nothing listens | (none) | Bounded by gap markers (below); reconcile over the window |
| Filter mismatch | Event name not in `eventstream.events`, key excluded by `eventstream.key-filter`, or database excluded by `eventstream.source-dbs` | `skipped_filtered`, `skipped_key_filtered`, `skipped_db` | By design; widen the relevant filter if the events matter |
| `XADD` refused under `maxmemory` | The `M` flag refuses writes at the memory limit | `dropped_oom` | Alert on any increase; free memory or raise `maxmemory`; reconcile over the pressure window. `eventstream.verify-oom no` closes this window — writes proceed at the limit — but the module then adds memory during eviction (SPEC.md section 11), and `dropped_oom` never moves, so alert on `used_memory` instead |
| `XADD` failed (`WRONGTYPE` etc.) | A non-stream key already occupies the destination name | `dropped_xadd_error` | The module never deletes the offending key; remove or rename it, then reconcile |
| Job scheduling failed | `add_post_notification_job` returned an error | `dropped_defer_error` | Rare; alert on any increase |
| Stream cap reached | Creating the destination stream would exceed `eventstream.max-streams` | `dropped_max_streams` | Alert on any increase (SPEC.md section 13); raise the cap, narrow the filter, or run `EVENTSTREAM.PRUNE` to drop dead registry names, which frees slots in the currently-registered count backing the cap |
| Entry encoding failed | The configured `entry-format` could not encode the event; with the shipped formats only `json` can fail, on a non-UTF-8 event name | `dropped_encode_error` | First failure is logged; alert on any increase and fix the event-name source or switch `entry-format` (SPEC.md section 6) |
| Cluster migration window (per-node mode) | `XADD` refused with `TRYAGAIN`/`ASK` while the pinned slot is mid-migration, and the one post-re-pin retry was also refused | `dropped_migrating` | Delimited by `repinned` gap markers; reconcile over the reshard window (SPEC.md section 10) |
| Stream trimming | `MAXLEN` (or `MINID` under `eventstream.retention-ms`) evicts entries before a slow consumer reads them | (see below) | Size `maxlen`/`retention-ms` for the slowest consumer; detectable via resume ID vs first entry ID |
| Crash before fsync | Server persistence config | (none) | Bounded by `appendfsync` (see Persistence); reconcile since last durable point |
| Failover | Entries not yet replicated to the promoted replica | (none) | Standard async-replication caveat; reconcile over the failover window |
| `FLUSHALL`, or `FLUSHDB` in db 0 | No per-key notifications fire, and the destination streams (with their consumer groups) are deleted | `control_markers` | Delimited by a `flushed` marker (`db -1`) on the recreated control stream (below); recreate groups; full reconcile, the streams themselves are gone |
| `FLUSHDB` in a non-zero db | No per-key notifications fire for the flushed keys; db 0 streams are unaffected | `control_markers` | Read `flushed` markers filtered on `db`; reconcile over the flushed database |
| `SWAPDB` involving db 0 | The destination streams (with their groups) move to the swapped database; the module writes fresh streams in db 0 | `control_markers` | Delimited by a `swapdb` marker on the fresh db 0 control stream (below); read the swapped database to recover db 0 history |

Timing caveat: `expired` fires when Redis actually removes the key (lazy access
or the active expire cycle), not at the nominal TTL instant. The entry ID
timestamp is the removal time, which is what you reconcile against.

## Delivery semantics summary

- Healthy capturing node, event selected: exactly one entry, atomic with the
  change.
- Overall (restarts, disabled, OOM, failover): at-most-once.
- Consumption with `XREADGROUP` + `XACK`: at-least-once within the retention
  window. Be idempotent on stream name plus entry ID.

## Persistence

Destination streams are ordinary keys: included in RDB, AOF, replication, and
`DUMP`/`RESTORE`. The module has no storage of its own and never calls fsync, so
durability is entirely the server's:

| Server persistence | Worst-case loss on crash |
|---|---|
| AOF, `appendfsync always` | None |
| AOF, `appendfsync everysec` (recommended minimum) | About 1 second of entries |
| RDB only | Everything since the last snapshot |
| None | Everything, on restart |

Eviction warning: `allkeys-*` maxmemory policies can evict the event streams
themselves. Run this module with `noeviction` or a `volatile-*` policy. The
module makes this visible: the `eventstream_eviction_risk` INFO field reads `1`
under any `allkeys-*` policy (recomputed live on config changes), and a warning
naming the policy is logged. Alert on the field flipping to 1.

## Detecting loss

- Module counters, via `INFO eventstream` (module sections do not appear in
  plain `INFO`; name the section or use `INFO everything`):

  ```
  INFO eventstream
  # eventstream_dropped, eventstream_dropped_oom,
  # eventstream_dropped_xadd_error, eventstream_dropped_defer_error,
  # eventstream_dropped_max_streams, eventstream_dropped_encode_error,
  # eventstream_last_error_time, eventstream_forwarded, eventstream_enabled
  ```

  Alert on: any increase in `eventstream_dropped`; `eventstream_enabled` equal
  to 0 when it should be 1; `eventstream_forwarded` flat while the server's own
  `expired_keys` (from `INFO stats`) keeps rising, which means the filter is not
  selecting what you think it is (the `skipped_filtered`, `skipped_key_filtered`,
  and `skipped_db` counters tell you which filter is too narrow).

- Trimming loss, per stream: compare your consumer's resume ID against
  `XINFO STREAM events:expired` `first-entry`. If your resume point is older
  than the first retained entry, entries were trimmed before you read them. See
  [sizing.md](sizing.md) for lag alerting.

## Gap markers

Capture-gap boundaries are machine-readable through the control stream
`events:#control` (`<stream-prefix>#control`): the module writes a marker
entry at each capture-boundary lifecycle point (load, disable/enable, flush,
swap, unload, and cluster re-pins). The trigger table, marker fields, delivery
mechanics, and limitations are documented once, in
[Gap markers](./gap-markers.md); this section covers only what reconciliation
adds on top.

A marker's `module-version` reflects marker-write time, not necessarily the
module currently loaded. To audit which release a running server has loaded,
use `MODULE LIST`: the module registers its crate version as the `ver` field,
encoded `major*10000 + minor*100 + patch`, so 0.2.0 reports `ver 200` and a
future 1.3.7 would report `ver 10307`.

### Delimiting a gap window

The window between a `disabled` or `unloading` marker and the next `enabled` or
`loaded` marker is a capture gap. Read the control stream and pair the markers:

```
XRANGE events:#control - +
```

A marker's entry ID timestamps the first event at that boundary: a `disabled`
marker's ID is the first event dropped after the disable, an `enabled` or
`loaded` marker's ID is the first event captured after capture resumed. That is
exactly the edge of the gap, so the two marker IDs bound the window in time.

Two caveats when pairing markers, both documented in
[Gap markers](./gap-markers.md): crashes and clean shutdowns write no closing
marker, so treat a `loaded` marker with no preceding `unloading` or `disabled`
as the end of a gap that opened at the last entry across your streams before
it; and markers are written lazily by the next captured notification, so a
window in which no event ever fired carries no marker (nothing was mirrored in
that window either, so the absence is correct).

## Reconciling a gap without a full scan

The point is to avoid rescanning the whole keyspace. Given a gap window
`[t_start, t_end]` in milliseconds (from a marker pair, a restart, or a
`dropped_oom` alert), reconcile only over that window:

1. Extract the window. For a disable/enable pair:

   ```
   XRANGE events:#control - +
   # find the `disabled` marker ID (t_start) and the next `enabled`/`loaded` ID (t_end)
   ```

2. Bound the reconciliation to keys whose state could have changed in the
   window. For expirations, that is keys whose TTL elapsed inside
   `[t_start, t_end]`. If your application maintains an index of keys and their
   expiry times (a common pattern for exactly this reason), query that index for
   the window instead of scanning. Absent such an index, a scoped `SCAN` with
   per-key `PTTL` checks is still far cheaper than a full sweep because you only
   act on keys expiring in a narrow window.

3. Re-derive the missed events from that bounded set and feed them to the same
   consumer logic that processes the streams, so reconciliation and steady-state
   share one code path.

This turns "rescan everything periodically" into "reconcile a bounded window,
only when a gap actually occurred", which is the improvement the module exists
to deliver.

## Executable reference

The behaviors above are pinned by the integration suite: `tests/markers.rs`
(marker lifecycle, crash-gap detection, restart safety, flush and SWAPDB gap
markers) and `tests/replication.rs` (replication and AOF durability). If a claim here ever
drifts from those tests, the tests are correct.
