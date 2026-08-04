# One-replica baseline

Date: 2026-08-04

Issue: #259

Artifact: `artifacts/2026-08-04-replication-baseline.json`

This report records the first matched standalone and replicated performance
campaign for the module.

## Question

How much does one healthy asynchronous Redis replica change primary throughput,
latency, and CPU while the module performs synchronous full capture? Can a
paused replica catch up with exact source and stream state without its local
module capturing replicated commands a second time?

This is the next narrow slice of issue #259. It compares matched standalone
and primary/replica campaigns and adds a deterministic pause/catch-up probe. It
does not cover prolonged replica lag, full resynchronization, failover,
promotion, replica restart, or persistence on the replica.

## Method

- Redis 8.8.0 and the module built from commit `3db9a42` on both Redis hosts.
- Two matched campaigns in `us-west-2a`: standalone used one `c7i.large` Redis
  host and one `c7i.large` load generator; replication added one `c7i.large`
  replica host.
- Each host used a 16 GiB gp3 root volume with 3,000 IOPS and 125 MiB/s
  throughput. Persistence was disabled.
- memtier 2.5.1, 200 connections, pipeline 1, 64-byte SET values, and a
  100,000-key space.
- Matched unloaded S0 and synchronous full-capture S2 (`events set`, MAXLEN
  10,000) at 75k and 100k offered operations/s.
- Three repetitions per cell, with 10-second warmups and 30-second measured
  windows.
- The replica loaded the identical module artifact with capture disabled, then
  followed the primary. Each trial required an online link and zero post-trial
  offset lag.
- After the replicated campaign, the controller paused the replica container,
  wrote 5,000 unique captured SETs to the primary, required a nonzero offset
  gap, resumed the replica, and reconciled exact key, stream, and module state
  on both nodes.

## Results

All values below are medians of three trials. Deltas compare the one-replica
campaign with its matched standalone cell.

| Scenario | Offered ops/s | Standalone ops/s | Replica ops/s | Ops delta | Standalone p99 ms | Replica p99 ms | Primary CPU delta | Repl. B/op | Replica core % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S0 | 75,000 | 73,088 | 73,118 | +0.04% | 3.279 | 3.055 | +2.55% | 143.89 | 7.78 |
| S0 | 100,000 | 98,896 | 98,821 | -0.08% | 3.087 | 3.823 | +6.27% | 143.89 | 9.61 |
| S2 | 75,000 | 72,890 | 72,864 | -0.04% | 2.783 | 2.943 | +4.71% | 361.76 | 20.05 |
| S2 | 100,000 | 98,635 | 98,552 | -0.08% | 2.623 | 2.703 | +2.76% | 361.81 | 26.52 |

Across 24 trials, the generator completed 61,857,852 operations. The 12 S2
trials expected and forwarded exactly 30,886,911 selected events. Every trial
finished with zero reported loss, drops, worker errors, handler panics,
command errors, or connection errors. The primary and replica both reported
zero byte lag at every post-trial checkpoint.

## Observations

### One healthy replica did not reduce throughput at these loads

The achieved-throughput difference was under 0.1% in all four matched cells.
The S2 p99 shift was +0.160 ms at 75k and +0.080 ms at 100k. The S0 100k cell
shifted by +0.736 ms, but with only three repetitions, no throughput change,
and no corresponding S2 spike, this campaign does not establish replication
as the cause of that isolated latency movement.

These are below-knee, rate-paced comparisons, not a replicated capacity
ceiling. As in prior campaigns, the 75k pacing point fell slightly below the
98% target-achievement classifier while 100k passed, so the classifier is
non-monotonic and must not be read as a knee bracket.

### Replication moved measurable work to both Redis hosts

Primary CPU per operation increased by 2.55% to 6.27% across the matched
cells. At 100k, the replica used a median 9.61% of one core for S0 and 26.52%
for S2. The capture path therefore remained within the primary's available
headroom while making the replicated stream write visible on the replica.

S0 generated about 143.89 replication bytes per SET. S2 generated about
361.81 bytes per source operation, an additional 217.92 bytes for the captured
stream write and entry fields. This closely matches the AOF byte accounting in
the persistence campaign and gives a concrete network-cost estimate for this
payload and event schema.

### A bounded interruption caught up exactly

Pausing the replica while 5,000 captured SETs ran created a 1,175,147-byte
replication gap. After resume, the replica reached the primary's offset in
318 ms. Both nodes contained exactly 5,000 source keys and 5,000 stream
entries. The primary module reported exactly 5,000 forwarded events, while the
replica module reported zero, confirming that applying replicated commands did
not re-capture the source SETs.

The probe reported no producer errors, loss, drops, worker errors, or handler
panics. This validates one short partial-resynchronization path; it is not a
bound on recoverable backlog and does not exercise a full resynchronization.

## Practical takeaway

On these hosts and at these loads, one healthy asynchronous replica is a small
primary-side CPU tax rather than a throughput tax. Full capture roughly 2.5x'd
replication traffic per source operation and increased replica CPU, but the
primary maintained the same achieved rate and the replica stayed current.

This supports the module's best-effort positioning: Redis replication can
carry the source mutation and generated event stream together, and a bounded
lag window reconciled exactly. It does not turn the event stream into an
independent durable log. Clients still need gap/error telemetry and a recovery
strategy for failure windows that Redis replication itself cannot preserve.

## Remaining issue #259 work

This report leaves #259 open for:

- prolonged replica lag, backlog exhaustion, and full resynchronization;
- primary failure, replica promotion, and post-promotion capture behavior;
- RDB snapshots and BGSAVE interference;
- AOF rewrite and rewrite-induced latency;
- retention and stream growth under bounded and unbounded MAXLEN;
- maxmemory policies plus `verify-oom` behavior;
- firehose write amplification; and
- longer runs and abrupt-failure durability windows.

Both disposable labs were destroyed after artifact collection. Terraform state
is empty, all five campaign instances are terminated, and all five root
volumes are deleted. The Resource Groups Tagging API may continue to list stale
deleted ARNs, so cleanup was verified directly through EC2.
