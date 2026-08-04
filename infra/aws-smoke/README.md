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
- A tagged EventBridge Scheduler group and one-time action that stops both
  instances at the configured expiry.

The instances need public egress to install Docker and pull pinned public
images. Public IPv4 addresses exist, but the security groups have no inbound
management rules.

All taggable resources carry repository, campaign, environment, owner, and
expiry tags. The owner tag uses lowercase `owner`: EC2 tag keys are
case-sensitive, the account cleanup policy requires that spelling, and IAM
rejects case-only duplicate tag keys. The expiry action stops compute, but it
does **not** destroy the
remaining network, storage, IAM, or Terraform state. Always run the destroy and
orphan-check steps.

## Reference shape and cost

The reference campaign uses two
[`c7i.large` instances][EC2 C7i instances] in one availability zone: one for
Redis and one for the generator. This is a non-burstable x86_64
shape with two vCPUs, matching the initial release target without CPU-credit
noise. In the instrumented sweep, full capture saturated the Redis main thread
at roughly 99% while the separate generator used about 112% of its available
200% CPU. That is enough generator headroom to expose the observed Redis-side
knee. The unloaded baseline approached the generator's limit, so a larger
generator remains appropriate for measuring an absolute no-module ceiling.

At the 2026-08-03 `us-west-2` on-demand rates, a campaign destroyed at the
default four-hour limit is approximately $0.77 before data transfer and tax:
$0.714 for two `c7i.large`
hosts at $0.08925 per host-hour, about $0.04 for two public IPv4 addresses, and
about $0.014 for 32 GiB of gp3 storage. Prices change; verify them against the
[EC2 on-demand pricing], [VPC pricing], and [EBS pricing] pages before a larger
campaign.

`ttl_hours` defaults to four and is constrained to 1–24 hours. The scheduled
hard stop limits EC2 and public-IPv4 runtime if the controller disappears.
Stopped instances and their EBS volumes still require `./lab.sh down`.

## Prerequisites

- Terraform 1.7 or newer
- AWS CLI v2 with an authenticated profile
- `jq`, `base64`, and Git
- Permission to manage EC2 networking, instances, instance profiles,
  EventBridge Scheduler schedules/groups, and the narrow IAM roles used by SSM
  and the expiry action, including permission to pass those roles

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
./lab.sh saturation
./lab.sh down
./lab.sh orphans
```

The orphan report uses AWS's eventually consistent resource-tagging index, so
recently deleted ARNs can remain visible briefly. Treat it as a discovery aid;
verify a reported ARN with its owning AWS service before treating it as live.

`plan`, `up`, and `down` accept additional Terraform arguments. For unattended
execution, explicitly add `-auto-approve`; it is never implied by the wrapper.

## Time-based saturation campaign

`./lab.sh saturation` runs the endpoint-agnostic memtier harness from
[`bench/saturation/README.md`](../../bench/saturation/README.md) across the same
two-host private network. It builds the exact checked-out commit on the server,
starts an unloaded Redis baseline with dynamic module commands enabled, and
uses the pinned memtier 2.5.1 image on the load-generator host.

Review the full randomized S0/S1/S2, selectivity, and mass-expiry plan without
AWS access:

```sh
SATURATION_PLAN_ONLY=yes ./lab.sh saturation
```

After `./lab.sh up`, a short cloud smoke run is:

```sh
SATURATION_SCENARIOS="s0 s1 s2 expiry-s0 expiry-s2" \
SATURATION_SELECTIVITIES="0 10 100" \
SATURATION_REPETITIONS=1 \
SATURATION_WARMUP_SECONDS=5 \
SATURATION_MEASUREMENT_SECONDS=15 \
SATURATION_EXPIRY_KEYS=50000 \
./lab.sh saturation
```

Omit the overrides for the evidence-producing defaults: five independent
repetitions, 10-second warmups, and 60-second measurement windows. The cloud
artifact embeds the existing versioned hardware/environment manifest, the
exact module commit and artifact digest, and the pinned generator image. It
otherwise has the same raw/normalized/summary layout and hard reconciliation
checks as a local or non-AWS run.

Set one persistence policy for a complete campaign. The runner configures the
server before load, records per-trial AOF growth and fsync health, then performs
a controlled restart with 5,000 captured SETs. `off` must lose the probe keys
and stream; either AOF policy must recover both exactly, with the module's
forwarded counter reset to zero after replay so AOF loading cannot duplicate
events. The result is saved as `restart-probe.json` and embedded in the
manifest.

```sh
SATURATION_PERSISTENCE_MODE=aof-everysec \
SATURATION_SCENARIOS="s0 s2" \
SATURATION_SELECTIVITIES=100 \
SATURATION_CLIENT_LEVELS=50 \
SATURATION_THREAD_LEVELS=4 \
SATURATION_RATE_LIMIT_LEVELS="375 500" \
SATURATION_REPETITIONS=3 \
SATURATION_WARMUP_SECONDS=10 \
SATURATION_MEASUREMENT_SECONDS=30 \
./lab.sh saturation
```

Repeat the same matrix with `off` and `aof-always` rather than combining
policies in one invocation. `SATURATION_PERSISTENCE_PROBE_EVENTS` changes the
restart sample size and must not exceed `SATURATION_MAXLEN`.

The one-command disposable form applies the lab, runs saturation, destroys the
resources even when the workload fails, and performs the orphan check:

```sh
SATURATION_REPETITIONS=1 \
SATURATION_WARMUP_SECONDS=5 \
SATURATION_MEASUREMENT_SECONDS=15 \
./lab.sh saturation-campaign -auto-approve
```

For an equal-offered-load knee sweep, keep the connection geometry fixed and
set per-connection rate levels. With 50 clients on four threads, the following
targets 50k, 100k, 125k, 150k, and 180k requests/s. The harness writes the
repeated health classification and adjacent below/at/above points to
`knee.json`. The AWS wrapper also records load-generator CPU and headroom
around every trial:

```sh
SATURATION_SCENARIOS="s0 s1 s2" \
SATURATION_SELECTIVITIES="0 10 100" \
SATURATION_CLIENT_LEVELS=50 \
SATURATION_THREAD_LEVELS=4 \
SATURATION_RATE_LIMIT_LEVELS="250 500 625 750 900" \
SATURATION_REPETITIONS=3 \
SATURATION_WARMUP_SECONDS=10 \
SATURATION_MEASUREMENT_SECONDS=30 \
SATURATION_P99_BUDGET_MS=6 \
SATURATION_ACHIEVEMENT_RATIO=0.98 \
./lab.sh saturation-campaign -auto-approve
```

The AWS load generator uses Amazon Linux 2023. Rate-limited campaigns enable
`EVENT_PRECISE_TIMER=1` inside the memtier container to avoid the timer
oscillation documented in
[memtier issue #361](https://github.com/redis/memtier_benchmark/issues/361).
The manifest records this setting; use `SATURATION_PRECISE_TIMER=0` only to
produce an explicit control.

After a campaign, the controller first fetches and verifies a compact archive
containing `manifest.json`, `trials.json`, `summary.json`, `knee.json`, and
`result.json`. It then fetches the full raw/checkpoint archive. Each metadata
and data chunk is retried up to five times, so a transient SSM error does not
discard a completed run; the compact evidence remains local even if the larger
raw transfer ultimately fails.

Choose the absolute p99 budget from a coarse matched S0 run on the same client
geometry. A budget below the paced S0 tail makes every module comparison
unhealthy and does not describe a module knee; the 6 ms example reflects the
observed coarse S0 envelope on the documented `c7i.large` lab, not a portable
service-level objective.

Use `SATURATION_WORKLOAD_NAME` to label separate campaigns and
`SATURATION_PAYLOAD_BYTES` for matched value-size points. The portable harness
also ships read-heavy, balanced, and write-heavy command specifications under
`bench/saturation/workloads/`. `SATURATION_COMMANDS_FILE` is copied to the
load-generator host through SSM (up to 8 KiB), so these specifications work
unchanged in the AWS runner.

For a disposable smoke campaign, use the combined path. The explicit
`-auto-approve` opts into unattended provisioning and cleanup; an exit trap
attempts destroy and stale-resource detection if the run fails:

```sh
./lab.sh campaign -auto-approve
```

The run waits for instance health, SSM registration, cloud-init, and pinned
image pulls. By default it builds the module from the current checked-out Git
commit in release mode, records the artifact digest, and then executes one
million `SET` requests in each scenario:

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

`BENCH_MODULE_SOURCE_COMMIT` accepts `HEAD` or a pushed Git commit. The server
downloads that exact revision, builds the module in the repository's container
build stage, and bind-mounts the resulting artifact into the otherwise pinned
runtime image. Set a commit explicitly when reproducing an earlier result:

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

Before the trials, the harness also measures intrinsic scheduler latency on the
server and single-client Redis PING latency over the private network. The host
probe and trial collector record the OS, AMI, kernel, architecture, CPU model,
Docker and load-generator versions, exact generator command, Redis version,
complete Redis configuration, topology, persistence settings, storage, and
network placement.

Results land under `results/<UTC-run-id>/` and are ignored by Git:

- `environment.json`: versioned physical/software environment contract
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

## Focused soak and restart-loss probe

The bounded follow-up to the short comparison trials uses the selected Preview
envelope configuration (`batch-size 128`, `max-wait-ms 1`) for a 30-minute
single-node exercise:

```sh
SOAK_MODULE_SOURCE_COMMIT=HEAD ./lab.sh soak
```

The driver calibrates 1 through 100 clients around 80,000 requests/s, then
restarts the server for clean counters. Its seven phases contain sustained
load, two 2x bursts, a paused decoder-aware consumer, and a catch-up interval.
The first-party client decodes mixed fixed and `batch-v1` entries without
printing each event and atomically checkpoints its logical count.

After the main run, two isolated probes use unique source keys:

- `MODULE UNLOAD` observes a nonempty Preview queue, drains it, reloads the
  module, and checks the retained stream with the first-party decoder;
- `SIGKILL` observes a nonempty queue in an AOF `everysec` server, restarts the
  process, and separately reports acknowledged commands, durable source keys,
  decoded logical events, and the missing-event count.

The default stream retention is 750,000 physical entries. This intentionally
bounds memory while leaving headroom for the decoder pause: the logical
capacity of a physical-entry limit depends on the envelope occupancy achieved
by the workload, not merely the configured batch maximum. Telemetry samples
Redis CPU, RSS, stream length/memory, worker queue depth, consumer lag, and
load-generator container use every five seconds. Normalized and raw results
land under `results/<UTC-run-id>-soak/`; the soak result embeds the same
versioned environment contract as the short campaign.

Validate the phase and probe configuration without AWS access:

```sh
SOAK_PLAN_ONLY=yes ./lab.sh soak
```

Useful overrides include `SOAK_SECONDS` (minimum 70), `SOAK_TARGET_RPS`,
`SOAK_MAXLEN`, `SOAK_PROBE_DEPTH`, and `SOAK_PROBE_BATCH_EVENTS`. The
30-minute default is the evidence-producing configuration; shorter values are
useful only for harness validation.

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
- [2026-07-30: Preview envelope soak and restart-loss probe][envelope-soak]

[first-run]: observations/2026-07-30.md
[ramp]: observations/2026-07-30-ramp.md
[sweep]: observations/2026-07-30-instrumented-sweep.md
[capture-profile]: observations/2026-07-30-capture-profile.md
[async-batch]: observations/2026-07-30-async-batch-spike.md
[envelope-tuning]: observations/2026-07-30-envelope-tuning.md
[envelope-soak]: observations/2026-07-30-envelope-soak.md

## Image pins

The defaults use manifest digests, not moving tags:

- `ghcr.io/joshrotenberg/redis-event-stream-module:0.4.0`
- `redis:8.8.0` for `redis-cli` and `redis-benchmark`
- `redislabs/memtier_benchmark` 2.5.1 for time-based saturation runs

The module image contains the same vanilla Redis 8.8.0-from-source build used by
the release and integration workflows. In `s0`, its command is overridden to
start that server without `--loadmodule`, keeping the baseline binary constant.

## Cost and cleanup

This creates billable EC2 instances, EBS volumes, and public IPv4 addresses.
The one-time expiry action stops both EC2 instances after four hours by default;
it is a runtime backstop, not a substitute for resource deletion.

After every run:

```sh
./lab.sh down
./lab.sh orphans
```

If Terraform is interrupted, rerun `./lab.sh down`. The orphan check is
read-only and lists any tagged resources still present in the selected region.

The remote server, environment, and benchmark scripts accept an endpoint and
container image and contain no AWS API calls. AWS-specific provisioning and SSM
transport stay in the controller layer, preserving a path to another cloud or
bare-metal controller without rewriting the workload contract.

[EC2 on-demand pricing]: https://aws.amazon.com/ec2/pricing/on-demand/
[EC2 C7i instances]: https://aws.amazon.com/ec2/instance-types/c7i/
[VPC pricing]: https://aws.amazon.com/vpc/pricing/
[EBS pricing]: https://aws.amazon.com/ebs/pricing/
