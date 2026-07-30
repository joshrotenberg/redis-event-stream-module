# Preview envelope tuning

Issue #269 asked whether the Preview envelope worker's batch size and maximum
drain wait could be tuned beyond the first async spike. The short answer is:

- keep synchronous delivery as the stable default;
- use a batch size of 128 and a 1 ms maximum wait as the Preview starting point;
- do not wait longer merely to fill the last slot in an envelope.

These are workload-specific tuning results, not a general capacity claim.

## Method

The experiment used separate `c7i.large` Redis and load-generator instances in
`us-west-2a`, Redis 8.8.0, 100 clients, four load-generator threads, 64-byte
values, and a 1,048,576-event worker queue. The module was built from commit
`c8c80b348f9f0f5da8d400d604517bc78715cb13`; the loaded artifact's SHA-256 was
`1aa59cc7043f7a337c0e8e6fd9803040096437a47417a339182fc92a8a3cc25b`.

The randomized coarse pass covered every combination of:

- batch sizes 32, 64, 128, 256, and 512;
- maximum waits 1, 2, 5, and 10 ms; and
- an explicit synchronous control.

Each coarse trial sent 2 million `SET` requests. A second randomized pass sent
5 million requests in each of three repetitions for the useful region:
`64:1`, `64:5`, `128:1`, `128:2`, `128:10`, and sync. In total, the two passes
captured 132 million logical events across 39 trials.

The table reports medians from the repeated 5-million-request pass. Throughput
includes the time needed for the capture stream to settle after the load
generator completes.

| Mode | End-to-end ops/s | vs sync | p99 | vs sync | Redis process CPU | vs sync | Achieved envelope |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sync | 116,206 | — | 1.111 ms | — | 99.4% | — | — |
| 64 / 1 ms | 128,148 | +10.3% | 1.167 ms | +5.0% | 180.2% | +81.2% | 64.0 |
| 64 / 5 ms | 126,532 | +8.9% | 1.175 ms | +5.8% | 174.2% | +75.2% | 64.0 |
| 128 / 1 ms | 131,551 | +13.2% | 1.335 ms | +20.2% | 162.4% | +63.3% | 127.0 |
| 128 / 2 ms | 131,570 | +13.2% | 1.335 ms | +20.2% | 161.2% | +62.1% | 128.0 |
| 128 / 10 ms | 127,448 | +9.7% | 1.367 ms | +23.0% | 147.3% | +48.2% | 128.0 |

## Observations

Batching improves throughput, but it moves rather than removes the cost.
`128:1` delivered 13.2% more logical events per second than sync while using
63.3% more total Redis-process CPU and adding 20.2% to p99 latency.

A 1 ms wait was already enough to produce approximately 127 events per
128-event envelope. Waiting 2 ms filled the remaining slot but produced no
meaningful throughput or p99 improvement. At 10 ms, CPU demand fell, but
throughput was 3.1% below `128:2` and p99 was higher. The coarse pass showed the
same pattern more strongly: larger 256- and 512-event targets often filled only
about 149 slots at 1 ms, while longer waits filled them at the cost of sharply
worse throughput and tail latency.

The 64-event configuration is a possible latency-biased alternative: `64:1`
kept the p99 increase to 5.0%, but its many smaller stream writes made it much
less CPU-efficient than the 128-event configurations.

Every trial reported:

- exact logical event counts;
- zero lost or dropped events;
- zero handler panics and async worker errors; and
- a queue high-water mark below 2,200 events in the repeated pass.

## Recommendation

Keep sync as the default because the Preview worker still trades additional
CPU and latency for throughput. For operators explicitly choosing envelope
mode under a saturated, single-host `SET` workload, recommend batch size 128
and maximum wait 1 ms as the initial configuration.

Expose both values as workload knobs rather than claiming a universal optimum.
The next validation should hold `128:1` under a longer mixed workload and
probe restart behavior; this experiment did not test recovery, partial load,
mixed event types, cluster mode, or Redis Enterprise.

## Artifacts

- [Coarse normalized run](artifacts/2026-07-30-envelope-tuning-coarse.json)
- [Focused normalized run](artifacts/2026-07-30-envelope-tuning-focused.json)
- [Combined machine-readable analysis][combined-analysis]

[combined-analysis]: artifacts/2026-07-30-envelope-tuning-analysis.json
