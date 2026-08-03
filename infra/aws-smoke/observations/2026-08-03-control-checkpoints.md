# Control-checkpoint overhead probe (2026-08-03)

Issue #277 adds opt-in durable status checkpoints to the existing control
stream. This focused local probe measures the incremental foreground and
persistence cost before choosing a default cadence.

## Method

- Apple arm64 host, Redis and `redis-benchmark` 8.8.0, Rust 1.97.1.
- Release module, stable synchronous mode, `events=set`.
- Foreground: 500,000 SETs, 50 clients, 4 benchmark threads, 64-byte payload,
  100,000-key range; median of three fresh-server runs.
- Persistence: otherwise-idle fresh server, AOF `everysec`, observed for 10
  seconds plus one final fsync interval.
- Cadences: disabled, 1,000 ms, and the minimum accepted 100 ms.
- Reproduction: `bench/checkpoints.sh`. Raw output is in
  `observations/artifacts/2026-08-03-control-checkpoints.json`.

## Results

| Cadence | SET ops/sec | vs disabled | p50 | p99 | Idle checkpoints | Control memory | AOF over disabled |
|---:|---:|---:|---:|---:|---:|---:|---:|
| disabled | 133,333.33 | baseline | 0.327 ms | 0.455 ms | 0 | 0 B | 0 B |
| 1,000 ms | 133,333.33 | 0.00% | 0.327 ms | 0.447 ms | 10 | 3,550 B | 5,601 B |
| 100 ms | 133,297.80 | -0.03% | 0.327 ms | 0.463 ms | 109 | 14,197 B | 58,567 B |

The disabled case creates no cron subscription and, while idle, leaves its
`loaded` marker pending. Enabled cases contain one `loaded` entry in addition
to the reported checkpoint count.

## Observations

- No foreground effect was measurable at a practical one-second cadence on
  this host. The 100-ms minimum changed median throughput by -0.03%; that is
  below what a three-repetition `redis-benchmark` probe can distinguish from
  normal variance.
- Foreground tail latency did not materially change: 0.455 ms disabled versus
  0.447 ms at one second and 0.463 ms at 100 ms.
- Persistence amplification is linear with checkpoint rate. The observed AOF
  cost was about 560 bytes per one-second checkpoint and about 537 bytes per
  100-ms checkpoint, including the one-time `loaded` entry.
- The 100-ms floor is intentionally conservative. A one-to-five-second
  production cadence should make checkpoint cost negligible relative to any
  meaningful captured-event workload while providing a useful uncertainty
  bound.
- These are local single-node results, not an AWS saturation claim. The probe
  is reproducible and should be included in a later cloud matrix if checkpoint
  defaults or payload fields change.

## Decision

Keep checkpointing disabled by default in the first release. Document one to
five seconds as a practical starting range. The evidence supports proceeding
with the signal without coupling issue #277 to a larger performance campaign.
