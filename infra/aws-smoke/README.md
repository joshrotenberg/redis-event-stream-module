# Minimal AWS smoke lab

This is the smallest cloud experiment for the event-stream module: one
non-burstable Redis host and one separate load-generator host in the same
availability zone. It is a smoke-quality comparison, not a published capacity
claim.

The lab deliberately excludes Redis Enterprise, EKS, bare metal, replicas,
cluster mode, Prometheus, long soaks, and failure injection. Those remain in the
larger performance backlog.

## What it creates

- One VPC, public subnet, route table, and internet gateway.
- One `c7i.large` server and one `c7i.large` load generator by default.
- No inbound SSH or management access. AWS Systems Manager drives both hosts.
- Redis port 6379 is reachable only from the load-generator security group.
- An encrypted, delete-on-termination gp3 root volume on each instance.
- An EC2 role containing only the AWS-managed SSM core policy.

The instances need public egress to install Docker and pull pinned public
images. Public IPv4 addresses exist, but the security groups have no inbound
management rules.

All taggable resources carry project, environment, owner, and expiry tags. The
expiry is advisory; it does **not** automatically delete resources. Always run
the destroy and orphan-check steps.

## Prerequisites

- Terraform 1.7 or newer
- AWS CLI v2 with an authenticated profile
- `jq`, `base64`, and Git
- Permission to manage EC2 networking, instances, instance profiles, and the
  `AmazonSSMManagedInstanceCore` role attachment

Choose an owner value used for cost attribution and cleanup:

```sh
export AWS_PROFILE=your-profile
export TF_VAR_owner=you@example.com
```

The defaults use `us-west-2`. Override with `TF_VAR_aws_region`.

## Review, create, run, destroy

```sh
cd infra/aws-smoke

./lab.sh plan
./lab.sh up
./lab.sh run
./lab.sh down
./lab.sh orphans
```

`plan`, `up`, and `down` accept additional Terraform arguments. For unattended
execution, explicitly add `-auto-approve`; it is never implied by the wrapper.

The run waits for instance health, SSM registration, cloud-init, and pinned
image pulls. It then executes one million `SET` requests in each scenario:

| Scenario | Server |
|---|---|
| `s0` | Same Redis binary from the module image, module not loaded |
| `s1` | Module loaded with its default expiration-only filter |
| `s2` | Module loaded with `events set`, so every request is captured through the stable default |
| `s2-sync` | Explicit stable `write-mode sync` control |
| `s2-individual` | Preview bounded worker, one `XADD` per event |
| `s2-envelope` | Preview bounded worker with `batch-v1` envelopes |

The default workload is 50 clients, two load-generator threads, 64-byte values,
and a 100,000-key keyspace. Override it with `BENCH_REQUESTS`,
`BENCH_CLIENTS`, `BENCH_THREADS`, `BENCH_PAYLOAD`, `BENCH_KEYSPACE`, or
`BENCH_MAXLEN`.

`BENCH_CAPTURE_SCENARIOS` replaces the default `s2` capture scenario with one
or more whitespace-separated `s2*` variants. Preview worker tuning is supplied
through `BENCH_ASYNC_QUEUE_CAPACITY`, `BENCH_ASYNC_BATCH_SIZE`, and
`BENCH_ASYNC_MAX_WAIT_MS`.

For an envelope tuning matrix, `BENCH_ASYNC_CONFIGS` accepts
whitespace-separated `<batch-size>:<max-wait-ms>` pairs. Each pair becomes a
separate randomized `s2-envelope` trial with a unique ID. Sync controls remain
ordinary `s2-sync` trials:

```sh
BENCH_REQUESTS=5000000 \
BENCH_CLIENT_LEVELS=100 \
BENCH_THREADS=4 \
BENCH_CAPTURE_SCENARIOS="s2-sync s2-envelope" \
BENCH_ASYNC_CONFIGS="32:1 64:1 128:1 256:1 512:1 128:2 256:5" \
BENCH_ASYNC_QUEUE_CAPACITY=1048576 \
BENCH_REPETITIONS=1 \
BENCH_BASELINE_REPETITIONS=0 \
BENCH_FILTERED_REPETITIONS=0 \
./lab.sh run
```

`BENCH_BASELINE_REPETITIONS` controls unloaded `s0` trials independently.
Both baseline and filtered repetitions may be zero when a focused experiment
needs only its explicit sync control. Set `BENCH_PLAN_ONLY=yes` to validate and
print the randomized plan without reading Terraform state or contacting AWS.

To benchmark code that has not yet shipped in the pinned image, set
`BENCH_MODULE_SOURCE_COMMIT` to `HEAD` or a pushed Git commit. The server
downloads that revision, builds the module in the repository's container build
stage, and bind-mounts the resulting artifact into the otherwise pinned runtime
image:

```sh
BENCH_MODULE_SOURCE_COMMIT=HEAD \
BENCH_CAPTURE_SCENARIOS="s2-sync s2-envelope" \
BENCH_ASYNC_QUEUE_CAPACITY=1048576 \
BENCH_ASYNC_BATCH_SIZE=256 \
BENCH_ASYNC_MAX_WAIT_MS=1 \
./lab.sh run
```

The exact source commit and built artifact SHA-256 are recorded in
`result.json`. The commit must be available from
`BENCH_MODULE_SOURCE_REPO`, which defaults to this GitHub repository.

For a repeated concurrency sweep, provide whitespace-separated client levels
and repetition counts:

```sh
BENCH_REQUESTS=3000000 \
BENCH_THREADS=4 \
BENCH_CLIENT_LEVELS="25 50 100 200" \
BENCH_REPETITIONS=3 \
BENCH_FILTERED_REPETITIONS=1 \
BENCH_ORDER_SEED=260 \
./lab.sh run
```

This runs three baseline (`s0`) and full-capture (`s2`) trials at each client
level, plus one filtered control (`s1`). The runner mixes trial order
deterministically from `BENCH_ORDER_SEED`, restarts Redis before every trial,
and records:

- Redis main-thread CPU seconds and estimated single-core utilization;
- Redis current, resident, and peak memory;
- sampled load-generator container CPU and peak memory; and
- benchmark elapsed time, throughput, and latency percentiles.

Redis utilization uses the benchmark's reported throughput to exclude
container startup and telemetry teardown time. The CPU figures remain
diagnostic estimates from Redis `INFO` and Docker sampling, not
laboratory-grade hardware counters.

To attach Linux `perf` to one or more scenarios, set
`BENCH_PROFILE_SCENARIO` to a comma-separated list:

```sh
BENCH_REQUESTS=5000000 \
BENCH_CLIENT_LEVELS=100 \
BENCH_THREADS=4 \
BENCH_REPETITIONS=1 \
BENCH_FILTERED_REPETITIONS=1 \
BENCH_PROFILE_SCENARIO=s1,s2 \
BENCH_PROFILE_FREQUENCY=99 \
./lab.sh run
```

The server records `cpu-clock` samples with DWARF call graphs only for matching
trials. A matched `s1,s2` pair is useful for separating the filtered control
from the incremental full-capture work. Compressed raw `perf report` output
and decompressed text land under
`results/<run-id>/profile/<trial-id>/`; the normalized trial JSON includes
profiler metadata. Profiling is opt-in because call-stack collection adds
overhead and changes the measured throughput.

Results land under `results/<UTC-run-id>/` and are ignored by Git:

- `result.json`: normalized run manifest, raw trials, and grouped summaries
- `trials.json`: normalized per-trial results
- `summary.json`: min/median/max values grouped by scenario and client level
- `raw/*.invocation.json`: complete SSM command responses
- `raw/*.stdout` and `raw/*.stderr`: raw command streams

Matrix summaries are also grouped by batch size and maximum wait, and include
achieved envelope size, percent of logical events written in envelopes,
fallback rate, and queue high-water. Combine a coarse and focused run into a
machine-readable analysis and Markdown table with:

```sh
infra/aws-smoke/scripts/analyze-tuning.sh \
  /tmp/eventstream-tuning \
  results/<coarse-run>/result.json \
  results/<focused-run>/result.json
```

The analysis reports the three-dimensional Pareto frontier: maximize
end-to-end throughput while minimizing p99 latency and total Redis-process CPU.
It fails if any input configuration reports loss, drops, handler panics, or
worker errors.

The runner fails unless:

- `s0` has no module loaded;
- `s1` loads the module and forwards no `SET` events;
- every selected `s2*` capture variant forwards exactly one logical event per
  request; and
- `events_lost`, `dropped`, and `handler_panics` remain zero.

## Observed runs

- [2026-07-30: first AWS smoke run][first-run]
- [2026-07-30: higher-concurrency ramp probe][ramp]
- [2026-07-30: instrumented single-node sweep][sweep]
- [2026-07-30: full-capture CPU profile][capture-profile]
- [2026-07-30: async and batch write spike][async-batch]
- [2026-07-30: Preview envelope tuning][envelope-tuning]

[first-run]: observations/2026-07-30.md
[ramp]: observations/2026-07-30-ramp.md
[sweep]: observations/2026-07-30-instrumented-sweep.md
[capture-profile]: observations/2026-07-30-capture-profile.md
[async-batch]: observations/2026-07-30-async-batch-spike.md
[envelope-tuning]: observations/2026-07-30-envelope-tuning.md

## Image pins

The defaults use manifest digests, not moving tags:

- `ghcr.io/joshrotenberg/redis-event-stream-module:0.4.0`
- `redis:8.8.0` for `redis-cli` and `redis-benchmark`

The module image contains the same vanilla Redis 8.8.0-from-source build used by
the release and integration workflows. In `s0`, its command is overridden to
start that server without `--loadmodule`, keeping the baseline binary constant.

## Cost and cleanup

This creates billable EC2 instances, EBS volumes, and public IPv4 addresses.
The four-hour expiry tag is a cleanup signal, not a shutdown mechanism.

After every run:

```sh
./lab.sh down
./lab.sh orphans
```

If Terraform is interrupted, rerun `./lab.sh down`. The orphan check is
read-only and lists any tagged resources still present in the selected region.
