# Workload mix and payload knee: 2026-08-03

This campaign extends the single-node `SET` result across representative
command mixes and value sizes. The main result is that capacity follows the
number of commands that actually produce events: on one `c7i.large`, the
highest observed healthy full-capture target moved from about 210,000
operations/second at 10% selected events, to about 170,000 at 50%, and to
130,000 at 100%. The exact curves include paced-load irregularities, so these
are workload-specific operating boundaries rather than universal limits.

Correctness remained exact throughout the campaign. Across 152 passing trials
and 266,879,888 operations, all 61,023,792 expected selected events in the 74
full-capture trials were forwarded. Event loss, drops, handler panics,
asynchronous worker errors, command errors, and connection errors were zero.

## Method

- Region and zone: `us-west-2`, `us-west-2a`
- Redis host: one `c7i.large`, two logical CPUs
- Load-generator host: one `c7i.xlarge`, four logical CPUs
- CPU: Intel Xeon Platinum 8488C
- Redis: 8.8.0 with module SHA-256
  `47f0e6f8e32c60e84b62df5f6209e05d8dc7d279d17337cb1467707bcb29f3d5`
- Generator: memtier 2.5.1 with `EVENT_PRECISE_TIMER=1`
- Geometry: 200 connections, four threads, pipeline depth 1, 100,000-key
  space
- Retention: approximate `MAXLEN` 10,000
- Focused runs: three repetitions, 3-second warmup, 12-second measurement
- Healthy criterion: median p99 no more than 6 ms and median achieved rate at
  least 98% of target

The primary matrix compares the module-not-loaded control (`custom-s0`) with
full capture (`custom-s2`) at equal offered load. A separate balanced pilot
also measured the module loaded with its default filter (`custom-s1`), where
none of the workload commands were selected.

## Capacity follows selected-event density

| Mix | Selected commands | Highest observed healthy target | Next upper point | Curve status |
|---|---:|---:|---:|---|
| Read-heavy, `SET:GET = 1:9` | 10% | 210,000 | 220,000 unhealthy | Non-monotonic at 205,000 |
| Balanced, `SET:GET = 1:1` | 50% | 170,000 | Not measured above | Non-monotonic at 155,000 and 165,000 |
| Mixed writes, `SET:HSET = 4:1` | 100% | 130,000 | 135,000 unhealthy | Non-monotonic at 115,000; clean upper transition |

Representative full-capture points were:

| Mix and target | Actual rate | Target achieved | p99 | Redis main thread | CPU per operation | Load-generator headroom | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| Read-heavy at 210,000 | 207,246 | 98.69% | 1.415 ms | 98.22% | 4.740 us | 41.72% | Healthy |
| Read-heavy at 220,000 | 211,183 | 95.99% | 1.367 ms | 99.26% | 4.694 us | 43.46% | Unhealthy |
| Balanced at 170,000 | 167,687 | 98.64% | 1.647 ms | 98.99% | 5.899 us | 57.41% | Healthy |
| Mixed writes at 130,000 | 128,466 | 98.82% | 2.063 ms | 97.35% | 7.572 us | 67.41% | Healthy |
| Mixed writes at 135,000 | 132,059 | 97.82% | 1.983 ms | 99.26% | 7.511 us | 64.43% | Unhealthy |

Latency did not trigger these boundaries. Redis approached one full
main-thread core while the load generator retained material CPU headroom, and
the achieved-rate ratio crossed the health threshold. This is consistent with
ordinary Redis-side saturation rather than a latency cliff or a load-generator
CPU ceiling.

At comparable upper healthy points, full capture increased Redis CPU per
operation by about 8.8% for 10% selection, 33.7% for 50% selection, and 57.1%
for 100% selection relative to the matched unloaded control. The relationship
is not perfectly linear because `GET`, `SET`, and `HSET` have different base
costs, but selected-event density is clearly the dominant axis.

The balanced 200,000 operations/second pilot supplies the control on the
other side: merely loading the module with no selected workload commands
increased CPU per operation from 4.510 to 4.626 microseconds, about 2.6%, while
both configurations achieved 99.1% of target. The substantial cost appears
when notifications pass the filter and are written, not just because the
module is present.

## Payload size

The balanced 1 KiB campaign produced a clean focused boundary:

| Payload and target | Configuration | Actual rate | Target achieved | p99 | Redis main thread | CPU per operation | Result |
|---|---|---:|---:|---:|---:|---:|---|
| 1 KiB at 160,000 | Module not loaded | 159,360 | 99.60% | 1.575 ms | 73.15% | 4.587 us | Healthy |
| 1 KiB at 160,000 | Full capture | 158,171 | 98.86% | 1.751 ms | 97.09% | 6.134 us | Healthy |
| 1 KiB at 170,000 | Module not loaded | 169,229 | 99.55% | 1.527 ms | 76.62% | 4.524 us | Healthy |
| 1 KiB at 170,000 | Full capture | 161,840 | 95.20% | 1.703 ms | 99.30% | 6.131 us | Unhealthy |

That 160,000/170,000 bracket is only modestly below the 64-byte balanced
result. The module does not copy Redis values into event entries; the default
entry contains event name, key, source database, and sequence. Retained event
size therefore stayed close to 79 bytes at 64-byte, 1 KiB, and 16 KiB workload
values. Value size affects the original Redis command and response, not event
stream storage.

The 16 KiB pilot reached a shared approximately 48,000 operations/second
plateau in both the unloaded control and full capture. At a 60,000 target, S0
and full capture achieved 48,066 and 48,079 operations/second respectively,
while Redis main-thread use was only 49.89% and 60.06%. The fixed 6 ms p99
budget was also invalid for this workload: S0 itself measured 8.4-10.8 ms.
This is a base-workload or lab-envelope limit, not a module knee. It shows
about 20% additional module CPU per operation, but no distinguishable
throughput boundary before the shared cap.

## Non-monotonic paced curves

Several sweeps contained an unhealthy point below a later healthy point:

- read-heavy S0 and full capture both missed at 205,000, then passed at
  210,000;
- balanced full capture narrowly missed the 98% ratio at 155,000 and 165,000,
  but passed at 160,000 and 170,000; and
- both mixed-write configurations missed at 115,000 before passing at higher
  targets.

These are offered-load pacing or threshold-sensitivity artifacts, not physical
capacity reversals. They occurred with spare load-generator CPU and, in two
cases, in both S0 and capture. The harness now reports `non-monotonic` and lists
the offending levels instead of presenting a later healthy point as a clean
knee. The upper mixed-write 130,000/135,000 transition remains useful because
that local transition is monotonic and coincides with Redis reaching 99.3% of
one core. The read-heavy and balanced numbers should be reported as approximate
boundaries.

## Correctness and retention

Across all nine completed pilot and focused runs:

- 152 of 152 trials passed their correctness gates;
- all 61,023,792 expected selected events in full-capture trials were
  forwarded;
- `events_lost`, `dropped`, handler panics, asynchronous worker errors,
  command errors, and connection errors were zero;
- the largest observed p99 was 10.815 ms, from the 16 KiB baseline-limited
  workload; and
- approximate retention remained close to 10,000 entries and about 79 bytes
  per event for the custom workload keys.

This is strong evidence for short-run exact accounting, including points past
the healthy throughput boundary. It is not evidence for crash recovery,
replica promotion, persistence durability, or long-run best-effort delivery;
those belong to the next backlog slices.

## Harness and infrastructure corrections

The first lab was terminated by account automation because the infrastructure
emitted uppercase `Owner` while the policy requires lowercase `owner`. AWS tag
keys are case-sensitive for EC2 policy matching, while some IAM resources
reject case-only duplicate keys, so the Terraform contract now emits only
lowercase `owner`. The recreated lab retained the correct tag for its entire
campaign.

The generic AWS resource-tagging index continued to list deleted ARNs after
Terraform destroy. The orphan reporter now calls this an eventually consistent
index and reads the lowercase owner key. Authoritative checks found both
current-lab instances terminated, no project-tagged live volumes, and zero
Terraform state resources.

## What this changes in the backlog

This completes the workload-mix and representative payload-size slice of
issue 260. It leaves issue 260 open for pipeline and concurrency geometry,
expiration-heavy traffic, filtered command families, key length, retention
policy, and any hardware or Redis-version matrix we decide is worth funding.

Issue 259 remains the best next performance task: persistence and replication
at a steady full-capture point around 70,000-75,000 operations/second and a
near-knee point around 100,000. Those loads remain conservative against the
lowest repeatable 100%-capture boundary observed to date. After that, issue
257 covers soak behavior, issue 256 covers fault injection, and issue 255 can
turn the evidence into a production operating guide under the issue 253 epic.

## Cleanup and artifacts

Terraform ended with zero managed resources. Both current-lab instances were
terminated and no project-tagged live volume remained.

The compact machine-readable result is
[`artifacts/2026-08-03-workload-payload-knee.json`](artifacts/2026-08-03-workload-payload-knee.json).
It records the primary archive checksums, representative levels,
classifications, correctness totals, measurement notes, and cleanup evidence.
The full raw archives remain in the ignored local results directory.
