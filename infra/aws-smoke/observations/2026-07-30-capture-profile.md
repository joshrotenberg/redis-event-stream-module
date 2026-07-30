# Full-capture CPU profile: 2026-07-30

This profile followed the instrumented single-node sweep. It narrows the
full-capture plateau to module code versus Redis core work for one `SET`
workload. It is not a general cost model for other commands, formats,
retention policies, or hardware.

## Environment and workload

- Region and zone: `us-west-2`, `us-west-2a`
- Server: `c7i.large`
- Load generator: `c7i.large`
- Redis/module image:

  ```text
  ghcr.io/joshrotenberg/redis-event-stream-module:0.4.0@sha256:1466a273fd64321ca3eba4447db3a16e2eb231be2afdb3a620c5cb2229be4db2
  ```

- Load-generator image:

  ```text
  redis:8.8.0@sha256:234c902a2db49461a129e2d4aeff85b28cf20187ed274a67f6e50995fa713c7b
  ```

- Workload per trial: 5,000,000 `SET` requests, 100 clients, four load
  threads, 64-byte values, and a 100,000-key keyspace
- Retention: approximate `MAXLEN` 10,000
- Profiler: Linux `perf` 6.1.176, `cpu-clock` at 99 Hz, and DWARF call graphs

Redis restarted before every trial. The matched run profiled the filtered
module control (`s1`) and full capture (`s2`) on the same host. An earlier
capture-only run exercised the collection and artifact-transfer path but is
not used in the differential below.

## Results

| Scenario | Operations/s | Redis main thread | CPU per operation | p99 |
|---|---:|---:|---:|---:|
| Module not loaded | 201,939 | 88.63% | 4.389 us | 0.983 ms |
| `SET` filtered | 201,922 | 92.66% | 4.589 us | 0.959 ms |
| Capture every `SET` | 133,280 | 99.33% | 7.453 us | 1.111 ms |

The filtered and unloaded cases had the same observed throughput and both
used about 190% of the two-vCPU load generator. Their throughput therefore
remains a conservative server baseline. Full capture left load-generator
headroom and saturated the Redis main thread, reproducing the earlier
Redis-side plateau.

Full capture added 2.864 microseconds of Redis main-thread CPU per operation
over the filtered control, an increase of 62.4%.

## Profile attribution

The reports contained about 2,000 filtered samples and 3,000 full-capture
samples, with zero lost samples. Symbols resolved for Redis, the Linux kernel,
and the Rust module. The report omitted leaf symbols below 0.1%, so the
following percentages intentionally do not add to 100%.

| Shared object | Filtered self | Full-capture self | Incremental CPU/op | Share of measured delta |
|---|---:|---:|---:|---:|
| Redis server | 24.34% | 39.78% | +1.849 us | 64.6% |
| Event-stream module | 1.21% | 7.63% | +0.513 us | 17.9% |
| libc | 3.34% | 3.33% | +0.095 us | 3.3% |
| vDSO | 1.56% | 1.63% | +0.050 us | 1.7% |
| Linux kernel | 57.73% | 33.35% | -0.163 us | -5.7% |
| Symbols below the report threshold | — | — | +0.521 us | 18.2% |

Kernel CPU per operation fell slightly because the slower capture trial
processed fewer client requests and replies per second. The added CPU was
instead concentrated in Redis and the module.

The largest positive Redis leaf deltas included `zfree`,
`zmalloc_usable`, `lpInsert`, `streamAppendItem`, reference counting, and
`RM_Call`. Module samples were distributed across `mirror_entry`, formatting,
allocation, `Context::call_ext`, and post-notification job handling. The
profile therefore points to per-event object churn and stream insertion, not
one isolated Rust function.

## Correctness

- All 5,000,000 selected events were forwarded.
- All 5,000,000 filtered notifications were counted as filtered.
- `events_lost`, `dropped`, and `handler_panics` remained zero.
- The capture stream finished at exactly 10,000 entries.
- Both profile files reported zero lost samples.

The profiler reduced full-capture throughput to 119,683 operations/s in the
earlier collection shakedown, but the matched run reached 133,280
operations/s. The prior unprofiled median at 100 clients was 128,949
operations/s. Throughput under profiling should remain diagnostic; the
per-operation differential and call-stack attribution are the decision
signals.

## Interpretation and next experiment

Moving only formatting and event construction off the Redis main thread has a
bounded upside: module code accounted for about 18% of the incremental CPU.
An asynchronous worker that still issues one `XADD` per event would also
retain most of the Redis allocation, listpack, and stream-insertion cost. It
may improve foreground latency temporarily by moving work behind a queue, but
it is unlikely to improve steady-state throughput on the same Redis instance.

The next spike should use one bounded handoff and compare:

1. current synchronous one-event/one-`XADD` capture;
2. asynchronous one-event/one-`XADD` drain, as the handoff control; and
3. asynchronous N-event/one-`XADD` batched envelopes.

The envelope case has the clearest path to reducing the dominant Redis work,
but it changes consumer parsing, per-event IDs, retention granularity,
visibility latency, and crash-loss behavior. The prototype should expose
queue depth, batch size, maximum wait, high-water marks, synchronous
fallbacks, and dropped-event counters. Healthy runs should continue to require
zero unreported loss.

## Cleanup

Terraform destroyed all 16 managed resources after each run and left an empty
state. Authoritative EC2 and IAM checks found no active tagged instances,
volumes, roles, or instance profiles afterward.
