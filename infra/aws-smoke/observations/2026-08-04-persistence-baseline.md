# Single-node persistence baseline

Date: 2026-08-04

Issue: #259

Artifact: `artifacts/2026-08-04-persistence-baseline.json`

## Question

How much do Redis AOF policies change the cost of synchronous full capture,
and do source keys and generated stream entries recover exactly without the
module capturing AOF replay a second time?

This is the first narrow slice of issue #259. It compares persistence disabled,
`appendfsync everysec`, and `appendfsync always` on one standalone host. It does
not cover RDB, AOF rewrite, replication, retention pressure, or maxmemory.

## Method

- Redis 8.8.0 and the module built from commit `f850358`.
- One `c7i.large` Redis host and one `c7i.large` load generator in
  `us-west-2a`.
- A 16 GiB gp3 root volume with 3,000 IOPS and 125 MiB/s throughput.
- memtier 2.5.1, 200 connections, pipeline 1, 64-byte SET values, and a
  100,000-key space.
- Matched unloaded S0 and synchronous full-capture S2 (`events set`, MAXLEN
  10,000) at 75k and 100k offered operations/s.
- Three repetitions per cell, with 10-second warmups and 30-second measured
  windows.
- One persistence policy per clean run. Automatic AOF rewrite was disabled so
  an overlapping rewrite could not contaminate file-growth accounting.
- After each run, a controlled restart probe wrote 5,000 unique SETs and
  required exact key/stream state before and after restart. The post-replay
  module counter had to remain zero.

## Results

All values below are medians of three trials.

| Persistence | Scenario | Offered ops/s | Achieved ops/s | p99 ms | CPU µs/op | AOF B/op |
|---|---:|---:|---:|---:|---:|---:|
| off | S0 | 75,000 | 73,135 | 3.535 | 5.61 | 0 |
| off | S2 | 75,000 | 72,907 | 2.831 | 9.02 | 0 |
| off | S0 | 100,000 | 98,811 | 3.759 | 5.33 | 0 |
| off | S2 | 100,000 | 98,632 | 2.719 | 8.66 | 0 |
| AOF everysec | S0 | 75,000 | 73,000 | 3.439 | 6.29 | 143.89 |
| AOF everysec | S2 | 75,000 | 72,871 | 3.247 | 10.89 | 361.69 |
| AOF everysec | S0 | 100,000 | 98,837 | 3.151 | 6.03 | 143.89 |
| AOF everysec | S2 | 100,000 | 97,675 | 2.863 | 10.02 | 361.72 |
| AOF always | S0 | 75,000 | 40,083 | 6.303 | 5.10 | 143.89 |
| AOF always | S2 | 75,000 | 30,972 | 8.255 | 10.80 | 361.62 |
| AOF always | S0 | 100,000 | 40,284 | 6.207 | 4.97 | 143.89 |
| AOF always | S2 | 100,000 | 30,825 | 8.319 | 10.89 | 361.62 |

Across 36 trials, the generator completed 74,567,172 operations. The 18 S2
trials expected and forwarded exactly 36,376,496 selected events. Every trial
finished with zero reported loss, drops, worker errors, handler panics,
command errors, or connection errors.

## Observations

### AOF every-second preserves the tested operating range

At 100k offered load, S2 achieved 97,675 ops/s versus 98,632 with persistence
off: a 0.97% difference. Median p99 increased by 0.144 ms. Redis CPU rose from
8.66 to 10.02 µs/op, so every-second persistence was not free, but it did not
produce a storage ceiling at this load.

The AOF recorded about 143.89 bytes per baseline SET and 361.72 bytes per
captured operation. Full capture therefore added about 217.83 AOF bytes per
source command for the generated stream write and its entry fields.

### AOF always is storage-bound and makes capture materially more expensive

Both overdriven offered-load points converged on approximately 40.1k ops/s for
S0 and 30.9k ops/s for S2. The module therefore reduced the fsync-bound ceiling
by about 23.5% on this gp3 volume. S2 median p99 was about 8.3 ms, versus about
6.2 ms for S0.

This is a hardware/configuration result, not a universal AOF-always limit. It
does show that the extra event-stream write matters even when fsync is the main
bottleneck. CPU per operation is not a complete cost model in this mode because
the main thread spends substantial wall time waiting on durability.

### Replay was exact and did not duplicate capture

The persistence-off control had 5,000 source keys and 5,000 stream entries
before restart and zero of each afterward. Both AOF modes recovered exactly
5,000 source keys and 5,000 stream entries. In every case the module loaded with
`forwarded = 0` and `events_lost = 0`, proving that AOF replay did not re-capture
the replayed SET commands.

No AOF trial reported a delayed fsync, a pending background fsync at the post
checkpoint, or a failed last-write status.

### The 75k pacing point remains non-monotonic

As in the prior workload campaign, memtier achieved only about 97.2–97.5% of
the nominal 75k rate while reaching about 98.6–98.8% at 100k without the
AOF-always ceiling. The configured 98% classifier therefore marks the lower
point unhealthy and the higher point healthy. These runs are matched-load
comparisons, not clean knee brackets; use achieved medians and do not interpret
the classifier status as a capacity inversion.

## Practical takeaway

On this host and workload, AOF every-second is a credible durability option:
it adds CPU and roughly 218 bytes of AOF write amplification per captured
event, but only about 1% throughput cost at the tested 100k point. AOF always
is a fundamentally different operating envelope and should be capacity-planned
around storage latency; synchronous capture reduced its observed ceiling from
about 40k to 31k ops/s.

The clean restart result strengthens the module's best-effort position rather
than turning it into a durable log: when Redis itself persists the source write
and generated stream, they recover together without duplicates. Abrupt failure
windows and replication still need separate characterization.

## Remaining issue #259 work

This report leaves #259 open for:

- RDB snapshots and BGSAVE interference;
- AOF rewrite and rewrite-induced latency;
- primary/replica throughput, replica lag, restart, and promotion;
- retention and stream growth under bounded and unbounded MAXLEN;
- maxmemory policies plus `verify-oom` behavior;
- firehose write amplification; and
- longer runs and abrupt-failure durability windows.

The disposable lab was destroyed after artifact collection. Terraform state is
empty, both campaign instances are terminated, and both root volumes are
deleted; the Resource Groups Tagging API continued to list stale deleted ARNs,
as documented by the orphan helper.
