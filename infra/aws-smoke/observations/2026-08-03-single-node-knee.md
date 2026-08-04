# Single-node offered-load knee: 2026-08-03

This campaign turns the earlier saturation plateau into an offered-load
operating boundary for one narrow workload. On a single `c7i.large`, capturing
every `SET` remained healthy at a 120,000 operations/second target and became
unhealthy at 130,000. A separate completed campaign put the transition one
step earlier, so the reproducible operating band is **110,000–120,000
operations/second**, not one universal capacity number.

This is a workload-specific result, not a production sizing claim. Persistence,
replication, payload size, command mix, retention, maxmemory policy, topology,
and hardware remain important unmeasured axes.

## Method

- Region and zone: `us-west-2`, `us-west-2a`
- Redis host: one `c7i.large`
- Load-generator host: one `c7i.large`
- CPU: Intel Xeon Platinum 8488C, two logical CPUs per host
- Redis: 8.8.0 with module SHA-256
  `47f0e6f8e32c60e84b62df5f6209e05d8dc7d279d17337cb1467707bcb29f3d5`
- Generator: memtier 2.5.1 with `EVENT_PRECISE_TIMER=1`
- Workload: `SET`, 64-byte values, 100,000-key space, 200 connections,
  pipeline depth 1
- Retention: approximate `MAXLEN` 10,000
- Targets: 50,000, 80,000, 100,000, 110,000, 120,000, 130,000, 140,000,
  and 160,000 operations/second
- Repetitions: three randomized repetitions at each target and configuration
- Timing: 3-second warmup and 12-second measurement
- Healthy criterion: median p99 no more than 6 ms and median achieved rate at
  least 98% of target

The 120 trials covered five configurations: module not loaded, module loaded
but disabled, enabled with 0% selected, enabled with 10% selected, and enabled
with every event selected. Correctness was a hard trial gate before a point
could be classified as healthy.

## Boundary

| Configuration | Classification | Highest healthy target | Actual rate | Target achieved | p99 | Redis main thread |
|---|---|---:|---:|---:|---:|---:|
| Module not loaded | Ceiling not reached | 160,000 | 158,022 | 98.76% | 2.719 ms | 75.99% |
| Module loaded, disabled | Ceiling not reached | 160,000 | 157,726 | 98.58% | 2.671 ms | 79.72% |
| Enabled, 0% selected | Ceiling not reached | 160,000 | 157,728 | 98.58% | 2.655 ms | 79.90% |
| Enabled, 10% selected | Ceiling not reached | 160,000 | 156,836 | 98.02% | 2.463 ms | 85.90% |
| Enabled, 100% selected | Bracketed | 120,000 | 117,776 | 98.15% | 2.335 ms | 95.79% |

The full-capture transition was:

| Target | Actual rate | Target achieved | p50 / p95 / p99 | p99.9 | Redis main thread | CPU per operation | Load-generator headroom | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 110,000 | 108,260 | 98.42% | 1.631 / 2.207 / 2.527 ms | 10.047 ms | 91.16% | 8.415 us | 44.37% | Healthy |
| 120,000 | 117,776 | 98.15% | 1.591 / 2.063 / 2.335 ms | 10.239 ms | 95.79% | 8.123 us | 49.21% | Healthy |
| 130,000 | 122,062 | 93.89% | 1.615 / 2.007 / 2.255 ms | 11.263 ms | 98.78% | 8.083 us | 47.53% | Unhealthy |

Latency did not trigger the boundary. At 130,000, p99 was still well inside
the 6 ms budget, while Redis was nearly consuming its main-thread core and
throughput achieved only 93.89% of target. The load generator retained almost
half of its host capacity. This identifies Redis-side full-capture work as the
limiting resource for this workload.

An earlier precise-timer campaign completed all trials and bracketed the same
full-capture transition at 110,000/120,000. Its full raw archive was lost when
one Systems Manager chunk fetch failed after the run. The second campaign is
the formal preserved result; the cross-campaign movement is why 110,000–120,000
is the more honest operating band. The fetch path now retries chunks and
preserves a compact summary before transferring the full archive.

## Cost scales with selected events

CPU cost relative to the unloaded baseline was:

| Configuration | At 100,000 target | At 120,000 target |
|---|---:|---:|
| Module loaded, disabled | +4.73% | +6.17% |
| Enabled, 0% selected | +6.34% | +7.64% |
| Enabled, 10% selected | +16.15% | +16.75% |
| Enabled, 100% selected | +67.24% | +62.36% |

Full capture used 8.123 microseconds of Redis main-thread CPU per operation at
the 120,000 target, compared with 5.003 microseconds without the module. This
independently reproduces the earlier approximately 62.4% CPU-per-operation
delta. The disabled and zero-selection controls are measurable but small; the
dominant cost follows the number of events actually written.

## Correctness, latency, and retention

Across 156,439,048 operations:

- all 32,775,025 selected events were forwarded;
- 91,916,641 events were counted as filtered;
- `events_lost`, `dropped`, handler panics, asynchronous worker errors,
  command errors, and connection errors were all zero; and
- the largest trial p99 was 4.127 ms, p99.9 was 15.295 ms, and maximum latency
  was 37.887 ms.

At the full-capture boundary the approximately 10,000 retained entries used
about 715 KiB, or 71.4 bytes per retained event. This is useful for bounded
retention estimates, but it does not measure an unbounded stream, persistence
rewrite amplification, replica memory, or eviction behavior.

## Measurement correction

memtier's `--rate-limiting` value is per connection, so the harness computes
the aggregate target as rate times clients per thread times threads. Amazon
Linux 2023 also uses a coarse 100 Hz kernel timer. An initial paced run showed
non-monotonic target achievement despite spare load-generator CPU. This is the
known behavior tracked in
[memtier issue #361](https://github.com/redis/memtier_benchmark/issues/361);
all results above use `EVENT_PRECISE_TIMER=1`.

The runner now records both host and core-normalized load-generator CPU,
classifies knees from achieved rate plus latency, records Redis CPU per
operation and retained-stream memory, supports workload command files, and
preserves compact results independently from the larger raw archive.

## What this changes in the backlog

This resolves the first narrow goal in issue 260: a repeatable single-node
offered-load boundary for one write-only workload. It leaves issue 260 open for
payload size, read-heavy and balanced mixes, pipeline and concurrency effects,
filtered-command families, expiration-heavy traffic, and longer validation.

For the persistence and replication matrix in issue 259, use approximately
70,000–75,000 operations/second as a steady full-capture point and 100,000 as a
near-knee point. Those are intentionally fractions of the conservative
110,000 end of the observed band, leaving room for AOF, replication, retention,
and maxmemory overhead to become visible without beginning every trial already
saturated.

## Cleanup and artifacts

Terraform ended with zero managed resources. Both instances were terminated,
and an authoritative EC2 check found no attached volumes.

The compact machine-readable result is
[`artifacts/2026-08-03-single-node-knee.json`](artifacts/2026-08-03-single-node-knee.json).
It records the source and module hashes, representative levels, classification,
correctness totals, measurement controls, and cleanup evidence. The ignored
local raw archive is checksummed in that artifact.
