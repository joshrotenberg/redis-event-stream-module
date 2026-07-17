# Upgrading

The module supports an **in-place upgrade**: swap the `.so` on a live server
with `MODULE UNLOAD` + `MODULE LOAD`, no restart. It registers no native data
types, so `MODULE UNLOAD` is never refused with `EBUSY`. The swap opens a
short, machine-readable capture gap (the `unloading`→`loaded` marker pair);
events that fire during it are not captured and not recoverable, so drain or
quiesce writers first if the gap matters.

If a capture gap is unacceptable and you can schedule capture downtime anyway, a
full server restart with the new `.so` in `loadmodule` is the alternative — the
loss window is then the whole restart instead of a sub-second swap.

## Procedure

```sh
# 1. Confirm the running version. MODULE LIST reports `ver` encoded
#    major*10000 + minor*100 + patch (0.2.0 -> 200).
redis-cli MODULE LIST

# 2. Quiesce or drain writers if the capture gap matters (see below).

# 3. Unload. This writes the `unloading` gap marker to <prefix>#control.
redis-cli MODULE UNLOAD eventstream

# 4. Load the new build. Re-supply the SAME values for every IMMUTABLE config
#    (see the warning below) as load args.
redis-cli MODULE LOAD /path/to/new/libredis_event_stream_module.so \
  events 'expired,set' stream-prefix events:

# 5. Verify: the version changed and capture is live again.
redis-cli MODULE LIST
redis-cli INFO eventstream
```

`MODULE UNLOAD`/`MODULE LOAD` require `enable-module-command` to be `yes` or
`local` (a server setting).

## What resets, what persists

| Survives the swap (ordinary keyspace) | Resets on load |
|---|---|
| Destination streams (`<prefix><event>`) and their entries | All INFO counters (`forwarded`, `dropped_*`, `skipped_*`, …) — process-lifetime `AtomicU64` statics, never persisted |
| The registry set `<prefix>#streams`, so `EVENTSTREAM.STREAMS` output is continuous | Per-stream `WITHSTATS` counters |
| The control stream `<prefix>#control` and its markers | Cluster per-node pinned tag (re-selected on load) |
| Consumer groups on the destination streams | |

Because counters reset, monitoring that alerts on "any increase in
`eventstream_dropped`" must tolerate a reset to zero across an upgrade — the
shipped Prometheus rules use `rate()`/`increase()`, which do (see
[Monitoring](./monitoring.md) and [Counters](./counters.md)).

> On Linux, Redis unloads the module image and the counters genuinely reset. On
> macOS, `dlclose` does not unload a dylib, so an in-process reload keeps the
> old counter values — a local-development detail, not a production concern.

## Re-supply IMMUTABLE configs

`eventstream.stream-prefix`, `eventstream.cluster-streams`, and
`eventstream.entry-seq` are IMMUTABLE: they cannot be `CONFIG SET`, only
supplied at load. The new `MODULE LOAD` must pass the **same values** the old
one used. If the prefix or cluster mode differ, the reloaded module points at
a different set of streams (and, in per-node cluster mode, a different
registry and pinned tag) — capture continues, but into new stream names, and
the pre-upgrade history is orphaned under the old prefix. If `entry-seq` is
not re-supplied, the reload reverts it to the default `no` and the `seq`
field silently disappears from every subsequent entry. `CONFIG GET
eventstream.stream-prefix eventstream.cluster-streams eventstream.entry-seq`
before the swap to record the values in use.

## The gap marker pair

The swap writes two markers to `<prefix>#control`:

- `unloading` — written directly in `deinit`, during the `MODULE UNLOAD` call.
- `loaded` — a pending marker from the new module's `init`, flushed on the first
  captured event after load.

Each carries `action` and `module-version`. The `unloading`→`loaded` pair is the
machine-readable bound on the upgrade's loss window: a consumer reconciling
history treats the span between them as a gap and can bound its reconcile to it
rather than scanning the keyspace (see [Gap markers](./gap-markers.md) and
[Loss windows and reconciliation](./loss-windows.md)). Diff the `module-version`
across the pair to confirm the swap changed versions.

## Cluster (per-node)

Run the procedure on **each node**. Each node re-selects its pinned hash tag on
load and writes its own `unloading`/`loaded` pair to its own control stream. Its
registry set (`<prefix>{tag}#streams`) persists like any keyspace data, so
per-node discovery is continuous across the swap.

## Notes

The `@eventstream` ACL category is registered so that an in-place reload
succeeds: Redis keeps a module's ACL categories across `MODULE UNLOAD`, so the
module tolerates the already-present category on reload rather than failing the
load. No operator action is needed.
