# Instrumented single-node sweep: 2026-07-30

This sweep followed the first smoke run and higher-concurrency probe with
repeated, mixed-order trials and host telemetry. It identifies the first
full-capture bottleneck for one narrow workload. It is not a general capacity
claim for other commands, payloads, retention policies, or hardware.

## Environment and workload

- Region and zone: `us-west-2`, `us-west-2a`
- Server: `c7i.large`
- Load generator: `c7i.large`
- Redis/module image:
  `ghcr.io/joshrotenberg/redis-event-stream-module:0.4.0@sha256:1466a273fd64321ca3eba4447db3a16e2eb231be2afdb3a620c5cb2229be4db2`
- Load-generator image:
  `redis:8.8.0@sha256:234c902a2db49461a129e2d4aeff85b28cf20187ed274a67f6e50995fa713c7b`
- Workload per trial: 3,000,000 `SET` requests, four load threads, 64-byte
  values, a 100,000-key keyspace, and 25, 50, 100, or 200 clients
- Retention: `MAXLEN` 10,000

The sweep ran three repetitions of the unloaded baseline (`s0`) and
full-capture configuration (`s2`) at each client level, plus one
filtered-module control (`s1`). A deterministic hash mixed all 28 trials rather
than running scenarios in blocks.

## Results

Operations per second are medians. Parenthesized values are the minimum and
maximum for the three repeated scenarios.

| Clients | Module not loaded | `SET` filtered | Capture every `SET` | Capture change | Capture p99 | Capture Redis main thread |
|---:|---:|---:|---:|---:|---:|---:|
| 25 | 162,127 (162,127–162,127) | 159,966 | 121,183 (119,976–121,188) | -25.25% | 0.319 ms | 97.94% |
| 50 | 199,920 (199,907–199,920) | 196,644 | 121,168 (121,158–124,948) | -39.39% | 0.567 ms | 98.89% |
| 100 | 199,734 (199,667–199,760) | 199,840 | 128,949 (127,589–128,949) | -35.44% | 1.039 ms | 99.48% |
| 200 | 192,517 (189,681–195,605) | 189,657 | 127,497 (126,082–127,502) | -33.77% | 2.119 ms | 99.18% |

At 100 clients, where each high-throughput scenario was at or near its
observed plateau:

| Scenario | Operations/s | Redis main thread | Load-generator CPU |
|---|---:|---:|---:|
| Module not loaded | 199,734 | 92.56% | 194.78% |
| `SET` filtered | 199,840 | 94.44% | 193.77% |
| Capture every `SET` | 128,949 | 99.48% | 111.54% |

The two-vCPU load generator was close to fully occupied in the unloaded and
filtered cases, so their approximately 200,000 operations/s plateau is a
conservative server baseline. Full capture showed the opposite shape: Redis
used essentially one full main-thread core while the load generator had ample
headroom. Its approximately 121,000–129,000 operations/s plateau is therefore
a credible Redis-side ceiling for this configuration.

The filtered configuration stayed between -1.64% and +0.05% of the unloaded
baseline from 50 through 200 clients. That supports the earlier observation
that notification filtering is inexpensive, although a larger load generator
is needed to measure its saturated cost precisely.

## Correctness and latency

The sweep executed 84,000,000 total `SET` requests:

- all 36,000,000 selected events were forwarded;
- all 12,000,000 filtered notifications were counted as filtered;
- `events_lost`, `dropped`, and `handler_panics` totaled zero; and
- every full-capture trial finished with `events:set` at exactly 10,000
  entries.

Capture p99 rose with concurrency after throughput had flattened: 0.319 ms at
25 clients, 0.567 ms at 50, 1.039 ms at 100, and 2.119 ms at 200. The largest
full-capture maximum was 24.047 ms. The approximately 220 ms maxima from the
previous single ramp did not recur; the largest maximum in any scenario was
35.711 ms in the unloaded 200-client baseline.

Redis peak memory remained between about 14.25 MiB and 14.71 MiB across the
full-capture medians with the 10,000-entry cap. This short bounded run did not
show memory growth as a limiting resource.

## Measurement note

The runner records Redis main-thread CPU seconds around the benchmark. Core
utilization above is recomputed from those raw seconds and the benchmark's
reported throughput. This excludes container startup and Docker telemetry
teardown time, which made the first generated summary understate utilization
by several percentage points. The harness now performs that correction
directly.

## Interpretation and next experiment

The synchronous capture path, not the filter gate or load generator, is the
first observed constraint. This makes an optimization spike worthwhile before
spending more money simply increasing client concurrency:

1. Profile the full-capture run to divide main-thread time among event
   construction, encoding, `XADD`, and trimming.
2. Prototype a bounded asynchronous handoff with explicit backpressure and
   loss counters.
3. Compare draining that queue as individual `XADD`s with an optional batched
   envelope containing multiple events per stream entry.

Individual deferred `XADD`s may reduce dispatch or lock overhead but still pay
for every stream insertion. A batched envelope has the larger plausible upside
because it reduces stream operations and allocations, but it also changes
latency, retention semantics, consumer parsing, and crash-loss behavior. The
profile should determine which prototype is justified, and correctness must
continue to require zero unreported loss.

## Cleanup

Terraform destroyed all 16 managed resources and left an empty state.
Authoritative EC2 and IAM checks found no active tagged instances, volumes,
roles, or instance profiles afterward.
