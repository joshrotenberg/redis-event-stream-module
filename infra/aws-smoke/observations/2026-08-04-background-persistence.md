# Background persistence interference

Date: 2026-08-04

Issue: #259

Artifact: `artifacts/2026-08-04-background-persistence.json`

## Question

How much do an overlapping RDB `BGSAVE` and AOF `BGREWRITEAOF` disturb
foreground traffic while the module performs synchronous full capture? Do the
source data and generated stream still recover exactly after Redis restarts?

This is the background-persistence slice of issue #259. It adds a controlled
resident-dataset prefill and compares matched runs with and without each
background operation. It does not establish behavior for multi-gigabyte
datasets, memory pressure, slow storage, repeated forks, or simultaneous
replication and persistence work.

## Method

- Redis 8.8.0 and the module built from commit `237bad8`.
- One `c7i.large` Redis host and one `c7i.large` load generator in
  `us-west-2a`, each with a 16 GiB gp3 root volume configured for 3,000 IOPS
  and 125 MiB/s throughput.
- A shared clean lab ran four campaigns: RDB control, RDB plus `BGSAVE`, AOF
  every-second control, and AOF every-second plus `BGREWRITEAOF`.
- Before every measured trial, the runner loaded 250,000 keys with 1,024-byte
  payloads while the module was unloaded. Redis used about 533 MiB with a
  roughly 560 MiB RSS after prefill.
- memtier 2.5.1, 200 connections, pipeline 1, 64-byte SET values, and a
  100,000-key measurement key space.
- Unloaded S0 and synchronous full-capture S2 (`events set`, MAXLEN 10,000)
  at 100,000 offered operations/s.
- Three repetitions per cell, with 10-second warmups and 30-second measured
  windows.
- Each action started five seconds into the measured window and had to report
  successful completion before foreground load ended. The runner recorded
  wall duration, Redis-reported duration, fork time, copy-on-write bytes, and
  persistence status.
- Each campaign ended with a 5,000-event restart probe. RDB took an explicit
  snapshot; AOF waited for persistence. Recovery required exact key and stream
  counts and zero capture during replay.

## Results

All values below are medians of three trials. Deltas compare the background
action with its matched no-action campaign.

| Mode | Scenario | Control ops/s | Action ops/s | Ops delta | Control p99 ms | Action p99 ms | p99.9 delta | Max delta | CPU delta | Action ms | COW MiB | Fork ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| RDB / BGSAVE | S0 | 98,999 | 98,966 | -0.03% | 5.215 | 4.863 | +0.256 ms | +3.456 ms | +1.22% | 978 | 1.52 | 6.246 |
| RDB / BGSAVE | S2 | 98,731 | 98,234 | -0.50% | 2.623 | 2.927 | +0.448 ms | +5.504 ms | +0.90% | 952 | 2.64 | 6.187 |
| AOF / rewrite | S0 | 98,811 | 98,724 | -0.09% | 4.799 | 4.991 | +0.832 ms | +11.264 ms | +1.85% | 952 | 1.70 | 6.896 |
| AOF / rewrite | S2 | 97,672 | 96,998 | -0.69% | 2.959 | 3.391 | +0.064 ms | +0.256 ms | -0.32% | 920 | 2.95 | 6.865 |

Across 24 trials, the generator completed 70,882,379 operations. The 12 S2
trials expected and forwarded exactly 35,267,744 selected events. Every trial
reported zero loss, drops, handler panics, registry errors, command errors, or
connection errors. All 12 background operations completed successfully and
overlapped the measured foreground window.

## Observations

### Neither background operation changed the tested throughput envelope

The largest matched throughput reduction was 0.69% for full capture during
`BGREWRITEAOF`; BGSAVE reduced full-capture throughput by 0.50%. The p99 shift
was +0.304 ms for captured BGSAVE and +0.432 ms for captured AOF rewrite.
These runs remained near the requested 100k operations/s and had no storage or
load-generator saturation signal.

There was some movement at the extreme tail. The S0 AOF rewrite cell's median
maximum latency increased by 11.264 ms and its p99.9 by 0.832 ms. With three
short repetitions, that is a useful signal to retain, not enough evidence to
assign a stable tail-latency tax.

### Full capture increased fork copy-on-write work, but the absolute work was small

Median BGSAVE copy-on-write grew from 1.52 MiB in S0 to 2.64 MiB in S2.
Median AOF rewrite copy-on-write grew from 1.70 MiB to 2.95 MiB. Median fork
time stayed between 6.187 and 6.896 ms, and background operations completed in
920 to 978 ms.

The direction is expected: full capture mutates both the source key and a
stream while the child process owns a snapshot of memory. The absolute values
are reassuring for this approximately 560 MiB RSS, but they do not define safe
headroom for a large or fragmented production dataset. The next memory-focused
campaign should vary resident size and record allocator fragmentation,
evictions, and peak RSS.

### AOF growth cannot be read as write amplification during a rewrite

The rewrite replaced a larger incremental AOF with a smaller compacted file
inside the measurement window, so start-to-end AOF growth was negative in the
rewrite trials. `aof_bytes_per_operation` is therefore intentionally not used
for the action comparison. The no-rewrite controls remained consistent with
the earlier baseline: about 143.89 AOF bytes per S0 operation and 361.72 bytes
per captured S2 operation.

No AOF trial reported a delayed fsync, a pending background fsync at the final
checkpoint, or a failed AOF write/rewrite status.

### RDB and AOF recovered exact paired state without replay capture

All four restart probes had exactly 5,000 source keys and 5,000 generated
stream entries before restart and recovered the same counts afterward. The
module reported 5,000 forwarded events before restart and zero after loading
the persisted data, with zero loss or drops. This confirms the tested RDB and
AOF replay paths restore the paired state without re-capturing historical
SETs.

## Practical takeaway

For this host, dataset size, and load, ordinary background persistence was a
small throughput and p99 tax rather than a cliff. Synchronous full capture did
increase copy-on-write work, and AOF rewrite exposed an isolated extreme-tail
movement, but neither operation caused loss, duplication, fsync delay, or a
meaningful reduction in achieved rate.

This reinforces a best-effort operational position: Redis can persist the
source mutation and generated stream together, while operators still need
enough memory for fork copy-on-write and telemetry for fork latency, RSS,
persistence status, and module loss/drop counters.

## Remaining issue #259 work

This report leaves #259 open for:

- retention and stream growth under bounded, time-based, and unbounded modes;
- maxmemory policies plus `verify-oom` behavior and memory-headroom guidance;
- firehose write amplification;
- prolonged replica lag, backlog exhaustion, and full resynchronization;
- primary failure, promotion, and post-promotion capture behavior;
- larger resident datasets, slower storage, and repeated background work; and
- longer runs and abrupt-failure durability windows.

The shared disposable lab was destroyed after artifact collection. Terraform
state is empty, both instances are terminated, and both root volumes are
deleted, verified directly through EC2.
