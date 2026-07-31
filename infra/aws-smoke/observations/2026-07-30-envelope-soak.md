# Preview envelope soak and restart-loss probe

Issue #270 extended the short tuning trials into a bounded 30-minute workload
with an active first-party decoder, periodic bursts, an intentional consumer
pause, and graceful and abrupt lifecycle boundaries.

The main result is two-sided:

- Preview envelope capture remained exact for more than 162 million logical
  events at the selected load, with zero module loss, drops, panics, or worker
  errors.
- Graceful module unload drained a nonempty queue exactly, while abrupt process
  termination lost stream events even when their source writes survived AOF
  recovery.

The experiment also found that a physical-entry retention limit must be sized
from achieved envelope occupancy. The configured batch maximum alone is not a
safe estimate of the logical retention window.

## Method

Both runs used separate `c7i.large` Redis and load-generator instances in
`us-west-2a`, Redis 8.8.0, a 64-byte value, a 100,000-key random keyspace, and
the selected Preview configuration:

- queue capacity 1,048,576;
- batch size 128; and
- maximum wait 1 ms.

The harness calibrated 1, 2, 4, 8, 16, 32, 64, and 100 clients around 80,000
requests/s. Both full runs selected 32 clients. The seven workload phases were
steady load, a 2x offered burst, steady load, a paused consumer, catch-up under
continued load, a second 2x burst, and a final steady interval.

The module and first-party decoder were built from the exact source commit for
each run. The first run used commit
`c80926c7acd6675e9f6ff9c1ba7cef4d32682a91`; the adjusted run used
`1fe4221631852f181579d47f1f67d571625dbc8d`.

## Retention finding

The initial 100,000-physical-entry run captured all 162,726,015 source events,
but the decoder finished at 157,768,653. Its peak lag was 6,158,284 logical
events and its permanent gap was 4,957,362.

This was not module loss: the capture queue settled, the forwarded count was
exact, and every module error counter remained zero. During the consumer pause,
older physical entries passed out of the retained window. When the decoder
resumed, its stream cursor advanced to retained history, permanently skipping
the trimmed logical events. It then kept pace at an apparently stable
4.96-million-event count deficit.

The adjusted run used a still-bounded 750,000 physical entries. It captured and
decoded all 162,641,176 logical events exactly. Peak consumer lag was 5,769,389
and returned to zero. During the catch-up phase, the decoder processed
26,551,779 logical events while the producer created 20,330,147, erasing the
pause backlog under continued load.

The stream reached its physical bound early and remained near it for 240
five-second samples. While bounded:

- stream memory ranged from 583 MB to 806 MB and finished at 751 MB;
- Redis RSS ranged from 680 MB to 896 MB and finished at 844 MB; and
- the physical length finished at exactly 750,000 entries.

The range reflects changing entry sizes as envelope occupancy varies. The
important operator-facing quantity is therefore the observed logical retention
window for the workload, not `maxlen × configured batch size`.

## Throughput and resource observations

The successful run calibrated 32 clients at 86,881 requests/s with a 0.695 ms
p99. Its three steady phases delivered 87,477–88,188 requests/s with
0.751–0.767 ms p99. The two offered bursts saturated around 125,000 requests/s
with 1.735–1.743 ms p99.

Across the 1,815-second telemetry window:

- Redis main-thread CPU averaged 74.7% of one core;
- total Redis-process CPU averaged 117.5% of one core;
- maximum sampled async queue depth was 116;
- the module-reported queue high-water mark was 1,996; and
- final queue depth and consumer lag were both zero.

These numbers confirm the earlier tuning result: batching raises useful
throughput, but the worker consumes additional CPU. They are workload-specific,
single-host observations rather than general capacity claims.

## Lifecycle boundaries

The graceful probe observed 12,459 queued events, then issued
`MODULE UNLOAD`. Unload completed in 124 ms, the module was reloaded, and the
first-party decoder recovered exactly 1,000,000 of 1,000,000 unique source
events.

The abrupt probe observed 19,518 queued events before `SIGKILL` on an AOF
`everysec` server. After restart:

- 111,047 unique source writes were durable;
- 72,344 logical stream events were durable; and
- 38,703 durable source writes had no recovered stream event.

The missing count was about 1.98 times the instantaneous observed queue depth.
Queue depth does not include every event already held by the worker, and AOF
may retain a source command without retaining its later derived `XADD`. The
observed queue is therefore useful boundary evidence, but not an upper bound on
abrupt-loss exposure.

This establishes the current lifecycle contract clearly:

- graceful unload drains Preview work without loss in this workload;
- abrupt process loss is not lossless, even when source data survives; and
- applications that require reconstruction or exactly-once event durability
  need a stronger design than the current in-memory worker queue.

## Limits and next decision

This run covered one standalone Redis 8.8.0 server, `SET` events, one
first-party consumer, one instance type, and AOF `everysec`. It did not cover
mixed event types, multiple consumers, cluster mode, Redis Enterprise,
replication/failover, long hardware soaks, or repeated crash distributions.

The next useful work is lifecycle semantics, not another undifferentiated load
ramp: define the supported abrupt-loss contract and decide whether durable
handoff, replay/reconstruction, or explicit at-most-once documentation is the
right product behavior. Broader workload and cluster matrices can follow that
decision.

## Artifact

- [Normalized run comparison](artifacts/2026-07-30-envelope-soak.json)
