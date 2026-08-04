# Time-based saturation harness: first cloud smoke

Date: 2026-08-03 (America/Los_Angeles; artifact completed 2026-08-04 UTC)

This run validates the issue #254 harness contract on the disposable two-host
AWS lab. It is a short functional sample, not a stable performance claim: one
repetition, two-second warmups, five-second measurement windows, and one client
geometry (50 clients per thread, four threads, pipeline one).

The exact branch commit was `654fc2b1653537f10fedc733094f94bf0e8e17a8`.
The server and load generator were `c7i.large` instances in `us-west-2a`, both
reporting an Intel Xeon Platinum 8488C and two logical CPUs. Private-network
PING p99 was 0.103 ms; server intrinsic maximum latency was 73 microseconds.
The generator was memtier 2.5.1 from its pinned multi-architecture image.

## Results

| Scenario | Ops/sec | vs S0 | p50 ms | p95 ms | p99 ms | p99.9 ms | max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| S0, no module | 202,122 | — | 0.495 | 4.223 | 10.623 | 20.607 | 37.631 |
| S1, loaded / filtered | 198,690 | -1.7% | 0.503 | 4.223 | 10.367 | 17.407 | 34.047 |
| S2, 100% stable sync capture | 126,106 | -37.6% | 1.567 | 1.959 | 2.207 | 11.647 | 35.327 |

S2 forwarded exactly 632,429 of 632,429 expected SET events. S1 forwarded
zero. Every loaded trial reported zero connection errors, command errors,
`events_lost`, drops, handler panics, and worker errors.

The p99 values are not evidence that S2 improves tail latency. This is an
unlimited closed-loop saturation run: S0 completed substantially more
operations and built a different queueing regime. The follow-up knee search
must compare offered-load points below saturation before interpreting latency
between scenarios.

## Expiry overlap

| Scenario | Foreground ops/sec | p99 ms | drain seconds | Full overlap proven |
|---|---:|---:|---:|:---:|
| Expiry S0 | 61,011 | 0.335 | 4.872 | yes |
| Expiry S2 | 59,209 | 0.351 | 5.160 | yes |

At this scale (50,000 staggered TTLs), capture reduced foreground throughput
by 3.0%, added 0.016 ms to p99, and lengthened the observed drain by 5.9%.
Expiry S2 forwarded exactly 50,000 events with zero loss, drops, or panics.
The recorded foreground intervals begin before the first observed expiry and
end after the final TTL key drained, removing the prior fixed-request
dilution.

## Harness findings

The cloud smoke caught two controller-only defects before the final pass:

1. the containerized `redis-cli --pipe` preload needed stdin attached; and
2. attaching stdin to every checkpoint command consumed the remaining lines
   of the redirected trial plan.

The wrapper now attaches stdin only for `--pipe`. The core harness also fails
unless executed trial count equals planned trial count, so a partial matrix
cannot produce a passing campaign artifact.

The final campaign produced all five randomized trials, fetched the compressed
artifact, destroyed all 20 Terraform resources, and left an empty state. AWS's
Resource Groups Tagging API continued to list terminated/deleting resource
records briefly after destroy; direct EC2 checks confirmed both instances were
terminated and the volumes were deleting.

The concise normalized sample is
[`artifacts/2026-08-03-time-based-saturation-smoke.json`](artifacts/2026-08-03-time-based-saturation-smoke.json).
