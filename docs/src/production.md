# Capacity and operations

This is the canonical guide for sizing, canarying, monitoring, and rolling
back Redis Event Stream. Read [Reliability and delivery](reliability.md) first:
the module is a best-effort, gap-aware live feed, not an independent durable
log.

Three labels keep the evidence honest:

- **Measured** means a linked campaign directly observed the result in its
  named environment.
- **Inference** explains what those observations probably mean without
  extending them to every deployment.
- **Recommendation** is a conservative starting policy to validate against
  the exact production workload.

## Latest evidence

The latest capacity campaign completed on **2026-08-04**. It exercised package
version **0.5.0 development snapshots** at source commits `f850358`, `3db9a42`,
`237bad8`, and `e4f2378`. The exact commit and module checksum in each linked
report are more precise identifiers than the package version.

The primary cloud environment was AWS `us-west-2a` on x86-64 Intel Xeon
Platinum 8488C hosts:

| Campaign | Module revision | Redis host | Generator | Storage and topology |
|---|---|---|---|---|
| Offered-load knees, 2026-08-03 | `14d7bc6`, `a3351a0`, `ca20d73` | `c7i.large`, 2 vCPU | Separate `c7i.large` or `c7i.xlarge` | Standalone Redis 8.8.0, persistence off, `MAXLEN ~ 10000` |
| Persistence and memory, 2026-08-04 | `f850358`, `237bad8`, `e4f2378` | `c7i.large`, 2 vCPU | Separate `c7i.large` | 16 GiB gp3, 3,000 IOPS, 125 MiB/s; standalone Redis 8.8.0 |
| Replication, 2026-08-04 | `3db9a42` | Primary and replica each `c7i.large` | Separate `c7i.large` | One asynchronous replica, persistence off, Redis 8.8.0 |
| Preview envelope soak, 2026-07-30 | `c80926c`, `1fe4221` | `c7i.large`, 2 vCPU | Separate `c7i.large` | Standalone Redis 8.8.0, AOF `everysec`, 30 minutes |

The offered-load campaigns used memtier 2.5.1 with the precise timer, pipeline
depth 1, 200 connections for the focused runs, a 100,000-key space, 3- or
10-second warmups, and three randomized repetitions of 12 or 30 seconds per
cell. Correctness was a hard gate. The envelope soak used 32 clients selected
by calibration, a 64-byte value, periodic bursts, a paused consumer, and 1,815
seconds of telemetry.

Compatibility CI is broader than the capacity evidence. It exercises Redis
7.2.8, 7.4.5, and 8.8.0 plus Valkey 8.1.6 and 9.1.0. That proves API behavior,
not equivalent throughput. No capacity number on this page is a Valkey, ARM,
managed-cloud, or Redis Enterprise result.

## Measured operating envelope

### Synchronous full capture

**Measured.** On one `c7i.large`, synchronous capture of every 64-byte `SET`
had a reproducible upper operating band of 110,000 to 120,000 offered
operations per second. At the preserved 120,000 target it achieved 117,776
operations per second, p99 was 2.335 ms, and the Redis main thread used 95.79%
of one core. At 130,000, Redis used 98.78% and delivered only 93.89% of target,
so throughput rather than latency failed the health criterion.

Selection density moved the boundary in a separate campaign:

| Workload on the same `c7i.large` | Selected commands | Highest observed healthy target | Next observation |
|---|---:|---:|---|
| `SET:GET = 1:9` | 10% | About 210,000 ops/s | 220,000 unhealthy; curve was non-monotonic |
| `SET:GET = 1:1` | 50% | About 170,000 ops/s | No higher focused point; curve was non-monotonic |
| `SET:HSET = 4:1` | 100% | 130,000 ops/s | 135,000 unhealthy |

Across the workload-mix campaign, 61,023,792 expected selected events were all
forwarded in 74 full-capture trials. Across the single-node knee campaign,
32,775,025 selected events were all forwarded. Both campaigns ended with zero
reported loss, drops, handler panics, worker errors, command errors, and
connection errors.

**Measured.** At comparable upper healthy points, capture increased Redis CPU
per source operation by about 8.8% at 10% selection, 33.7% at 50%, and 57.1%
at 100%. Loading the module while selecting none of the balanced workload
commands added about 2.6% CPU per operation in a 200,000 ops/s pilot. In the
focused all-`SET` campaign, full capture at the 120,000 target added 62.36%
CPU per operation over the unloaded control.

**Inference.** The dominant cost follows events actually appended, not module
presence or value size. Redis stream insertion and allocation were the largest
profiled contributors. Filtering narrowly is therefore the first and most
reliable capacity control.

### Payloads and optional write modes

**Measured.** Moving balanced values from 64 bytes to 1 KiB only moved the
full-capture bracket from about 170,000 to 160,000 operations per second. Event
entries do not contain source values. A 16 KiB pilot hit a shared generator or
base-workload limit around 48,000 operations per second in both the unloaded
and capture cases, so it did not establish a module knee.

**Measured.** Preview `batch-v1` envelopes at batch size 128 and a 1 ms wait
improved end-to-end logical-event throughput by 13.2% over synchronous capture
in one saturated `SET` workload. It also used 63.3% more total Redis-process
CPU and raised p99 by 20.2%. An asynchronous worker that still issued one
`XADD` per event was slower than synchronous capture.

**Recommendation.** Keep synchronous `fixed` entries as the production
starting point. Treat envelope mode as a workload-specific throughput trade
when a second core is available and every consumer accepts mixed fixed and
`batch-v1` entries. Do not enable async `individual` as an optimization.

The campaigns did **not** isolate the cost of `minimal`, `verbose`, or `json`
entry formats, `entry-seq yes`, the firehose, auto-groups, time-based
retention, per-stream overrides, or combinations of those features. The
firehose performs a second destination write, so higher CPU, persistence,
replication, and memory cost is a design inference, not a measured multiplier.

### Persistence and replication

**Measured.** On the gp3-backed `c7i.large` at 100,000 offered `SET` operations
per second:

| Mode | Unloaded achieved | Full capture achieved | Full-capture p99 | Extra recorded bytes per captured operation |
|---|---:|---:|---:|---:|
| Persistence off | 98,811 ops/s | 98,632 ops/s | 2.719 ms | 0 |
| AOF `everysec` | 98,837 ops/s | 97,675 ops/s | 2.863 ms | About 218 AOF B/op |
| AOF `always` | About 40,100 ops/s | About 30,900 ops/s | About 8.3 ms | About 218 AOF B/op |
| One healthy replica, persistence off | 98,821 ops/s | 98,552 ops/s | 2.703 ms | About 218 replication B/op |

AOF `everysec` reduced captured throughput by about 1% at this point. AOF
`always` was storage-bound and full capture reduced the observed ceiling by
about 23.5%. One healthy asynchronous replica changed achieved throughput by
less than 0.1% at the tested loads, while adding 2.55% to 6.27% primary CPU per
operation. A paused replica caught up a 1,175,147-byte gap in 318 ms and
reconciled exactly 5,000 source keys and 5,000 entries.

**Measured.** Against an approximately 560 MiB resident dataset, an overlapping
`BGSAVE` reduced captured throughput by 0.50%; `BGREWRITEAOF` reduced it by
0.69%. Both increased copy-on-write memory, but all background operations and
restart checks completed exactly. This does not characterize multi-gigabyte
datasets, fragmented memory, slow disks, or repeated overlapping forks.

**Recommendation.** Use AOF `everysec` as the initial durability policy unless
the application accepts a larger crash window. Benchmark `always` on the exact
storage. Reserve CPU, network, and replica memory for the additional stream
write, and alert on replication lag before relying on promotion.

### Retention and memory

**Measured.** The fixed-entry campaigns retained approximately 10,000 entries
at about 71 to 79 bytes per event for their short keys and field sets. This is
an observed `MEMORY USAGE / XLEN` range, not a universal encoding constant.
Allocator state, key length, entry format, stream-node packing, consumers,
groups, and optional copies change it.

Size each destination independently:

```text
logical entries = peak selected events/second × maximum outage seconds
measured bytes  = MEMORY USAGE <stream> / XLEN <stream>
stream budget   = logical entries × measured bytes × safety factor
```

For example, one stream at 20,000 selected events per second and a 15-minute
outage needs 18 million logical entries. At the observed 79 bytes per event,
that is about 1.32 GiB before safety margin. A provisional 2x factor makes the
starting stream budget about 2.65 GiB; measure it again with representative
keys, groups, and fragmentation before setting `maxlen`.

**Measured.** In envelope mode, `maxlen` counts physical envelopes. A 100,000
physical-entry limit lost 4,957,362 logical events from a paused consumer even
though module capture was exact. Raising the bound to 750,000 retained and
decoded all 162,641,176 logical events; the stream occupied 583 to 806 MiB and
absorbed a peak lag of 5,769,389 logical events.

**Recommendation.** For envelopes, size from observed logical occupancy and
the oldest resume point, never `maxlen × configured batch size`. Warn when
consumer lag reaches 50% of the tested retention window and page before the
saved resume ID reaches the oldest retained ID.

### Maxmemory

**Measured.** In deterministic pressure tests, `noeviction` with
`verify-oom yes` counted exactly 5,195 refused generated writes while all
15,000 source `DEL`s succeeded, then resumed capture as memory became
available. With `verify-oom no`, all 15,000 events were written above the
configured limit. Under `volatile-lru`, expiring source keys were evicted while
the non-expiring stream remained. Under `allkeys-lru`, Redis later evicted an
already-successful 15,000-entry stream without changing module loss counters;
`eventstream_eviction_risk` was `1`.

**Recommendation.** Prefer `noeviction` or a `volatile-*` policy. Keep
`verify-oom yes` unless continued capture is explicitly more important than
enforcing the memory limit. Begin with at least 30% memory headroom after the
retention budget, replication buffers, and expected fork copy-on-write are
included. That 30% is an operational starting rule, not a measured safe bound.

## Establish a site-specific limit

The Redis main thread saturated before the generator in the full-capture
campaigns, and p99 remained inside the test budget after achieved throughput
had started to miss its target. Do not use latency alone to find the knee.

1. Run a matched baseline with the module unloaded or disabled.
2. Repeat with the intended filters and options using
   [`bench/saturation.sh`](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/bench/saturation.sh).
3. Hold payload, connection count, pipeline depth, retention, persistence,
   replica count, and consumer load constant while increasing offered rate.
4. Require exact accounting and zero error counters at every point.
5. Mark the first repeatable point where achieved throughput falls below the
   target, p99 crosses the application budget, Redis main-thread CPU has no
   headroom, persistence stalls, replication lag grows, or consumer lag fails
   to recover.
6. Repeat around the transition in randomized order and preserve the full
   environment manifest.

**Recommendation.** Set the first steady-state ceiling to the lower of 60% of
the conservative knee or the rate that keeps Redis main-thread use below 75%.
For the exact all-`SET` AWS lab, 60% of the lower 110,000 boundary is 66,000
events per second; later campaigns used 70,000 to 75,000 as deliberately
below-knee comparison points. Neither number transfers to different hardware.

Signs that the site-specific envelope is being exhausted include:

- Redis main-thread CPU staying above the canary budget while offered and
  achieved throughput diverge;
- p99, p99.9, or maximum latency moving during persistence or fork activity;
- `async_queue_depth`, `async_queue_high_water`, or the fallback ratio growing
  in a Preview worker mode;
- replication offsets diverging or a full resynchronization beginning;
- `aof_delayed_fsync`, pending fsyncs, fork time, copy-on-write memory, or RSS
  rising beyond the tested range;
- `used_memory` approaching `maxmemory`, evictions starting, or
  `eventstream_eviction_risk` becoming `1`; and
- consumer lag growing across steady intervals instead of recovering.

## Required alerts

Collect Redis `INFO`, `INFO eventstream`, `EVENTSTREAM.STATS`,
`EVENTSTREAM.STREAMS WITHSTATS`, and consumer-group state from every master.
The included [monitoring stack](monitoring.md) provides starter Prometheus
rules and a Grafana dashboard.

| Signal | Required condition and response |
|---|---|
| `eventstream_events_lost` | Page on any increase; a selected event has no canonical entry |
| `eventstream_dropped` and every `dropped_*` reason | Page on any increase and classify OOM, destination, scheduling, cap, encoding, no-slot, or migration failures |
| `eventstream_handler_panics` | Page when nonzero; this is a module bug |
| `eventstream_registry_errors` | Page on any increase; capture may exist but discovery is incomplete |
| `eventstream_async_worker_errors` | Page when nonzero in a Preview worker mode |
| `eventstream_enabled` | Alert when `0` outside an approved gap |
| `eventstream_eviction_risk` and Redis evictions | Page when risk is `1` or retained streams can be evicted |
| Stream oldest ID, `XLEN`, and memory | Warn at 50% of the tested outage budget; page before retention crosses a saved resume ID |
| Consumer-group lag and pending age | Alert when lag cannot recover inside the remaining retention window |
| Replication offsets and link state | Alert before backlog exhaustion; page on disconnect, full sync, or unexpected promotion |
| AOF/RDB status, delayed fsyncs, fork time, COW, and RSS | Alert when they exceed the site-specific persistence baseline |
| Control checkpoints | Alert when stale; classify new generations without a durable `unloading` marker as uncertain |

Counters reset when the module reloads. Use increases or rates, retain the
generation from `<prefix>#control`, and never treat a flat in-process counter
across a restart as continuity proof.

## Canary and rollback

Begin with the module loaded but capture disabled:

```text
MODULE LOAD /path/to/libredis_event_stream_module.so \
  enabled no \
  control-checkpoint-ms 5000
```

Then use a staged canary:

1. **Loaded, not capturing.** Compare CPU, latency, persistence, replication,
   and memory with the pre-load baseline. Verify `EVENTSTREAM.STATS` and the
   monitoring path.
2. **Narrow selection.** Configure one event type plus a small key glob or
   source database. Size retention for the canary and enable capture.
3. **Prove continuity.** Reconcile source successes, `forwarded`, retained
   entries, control checkpoints, and consumer progress. Require zero loss,
   drops, panics, worker errors, and registry errors.
4. **Expand deliberately.** Increase selected-event density in steps while
   preserving the steady-state CPU and memory headroom. Exercise a consumer
   pause, persistence operation, replica interruption, and restart.
5. **Hold before broad rollout.** Run through the longest expected peak and
   outage window. Stop expansion when any saturation or correctness gate moves.

The fastest kill switch is:

```text
CONFIG SET eventstream.enabled no
```

It stops new capture and writes a `disabled` control marker; existing streams
remain available to consumers. Preserve `INFO eventstream`, configuration,
control entries, stream boundaries, Redis logs, persistence state, replication
offsets, and consumer positions before changing more state.

If disabling is insufficient, unload the module:

```text
MODULE UNLOAD eventstream
```

A clean unload writes `unloading`. The stable path has no accepted async
backlog; the Preview worker drains accepted work before unloading. One observed
envelope unload drained 12,459 queued events in 124 ms, but that is not a
timeout guarantee. A restart may be required when the module is configured in
`redis.conf`, when an immutable option must change, or when the shared library
cannot be replaced in place.

After rollback, classify the gap as known, uncertain, retention overrun, or no
loss observed using [the control-stream algorithm](reliability.md#consumer-assessment-algorithm).
Reconcile from an application-owned source of truth before resuming downstream
work that cannot tolerate a gap.

## Incident triage

1. Disable capture if Redis safety, latency, or memory is deteriorating.
2. Snapshot the signals listed in the rollback section before counters reset.
3. Determine whether `events_lost`, a `dropped_*` reason, registry failure,
   eviction, restart generation, replication gap, or retention overrun moved.
4. Restore headroom: narrow the filter, lower retention, repair the destination
   or registry key, recover a consumer, resolve persistence pressure, or repair
   replication.
5. Reconcile the bounded window externally. A control checkpoint narrows the
   window but cannot identify missing keys.
6. Re-enable the narrow canary and expand only after counters and lag remain
   stable.

## Known uncharacterized combinations

Do not infer production capacity from this guide for:

- Valkey capacity, ARM servers, other CPU families, managed Redis, or networked
  storage outside the named gp3 configuration;
- Redis 7.2 or 7.4 capacity, Sentinel-driven failover, promoted-primary loss
  windows, prolonged replica lag, backlog exhaustion, or full resync;
- OSS Cluster, resharding under sustained load, Redis Enterprise packaging,
  or multi-shard Enterprise databases;
- expiration storms, `evicted` storms, large multi-key commands, long keys,
  many event types, multiple active consumers, or mixed source databases;
- firehose, alternate entry formats, sequence fields, auto-groups, time-based
  retention, unbounded streams, or combined optional features; and
- multi-gigabyte datasets, long-run fragmentation, slow storage, repeated
  background forks, or repeated crash distributions.

Compatibility tests cover many of these code paths. They are not capacity or
failure-envelope measurements.

## Campaign reports and artifacts

- [Single-node offered-load knee](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-03-single-node-knee.md)
  and its [machine-readable artifact](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-03-single-node-knee.json)
- [Workload and payload knees](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-03-workload-payload-knee.md)
  and its [machine-readable artifact](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-03-workload-payload-knee.json)
- [Persistence baseline](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-04-persistence-baseline.md),
  [replication baseline](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-04-replication-baseline.md),
  [background persistence](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-04-background-persistence.md),
  and [maxmemory pressure](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-08-04-maxmemory-pressure.md),
  with machine-readable artifacts for
  [persistence](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-04-persistence-baseline.json),
  [replication](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-04-replication-baseline.json),
  [background persistence](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-04-background-persistence.json),
  and [maxmemory](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-08-04-maxmemory-pressure.json)
- [Preview envelope soak](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/2026-07-30-envelope-soak.md)
  and its [machine-readable artifact](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/infra/aws-smoke/observations/artifacts/2026-07-30-envelope-soak.json)

The committed JSON files are normalized manifests with environment, source,
result, correctness, cleanup, and raw-archive checksum data. The larger raw
archives were intentionally not committed; use their recorded checksums when
retaining copies from a new campaign.
