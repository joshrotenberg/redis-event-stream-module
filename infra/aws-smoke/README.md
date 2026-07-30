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
| `s2` | Module loaded with `events set`, so every request is captured |

The default workload is 50 clients, two load-generator threads, 64-byte values,
and a 100,000-key keyspace. Override it with `BENCH_REQUESTS`,
`BENCH_CLIENTS`, `BENCH_THREADS`, `BENCH_PAYLOAD`, `BENCH_KEYSPACE`, or
`BENCH_MAXLEN`.

Results land under `results/<UTC-run-id>/` and are ignored by Git:

- `result.json`: normalized run manifest and scenario results
- `scenarios.json`: normalized scenario results
- `raw/*.invocation.json`: complete SSM command responses
- `raw/*.stdout` and `raw/*.stderr`: raw command streams

The runner fails unless:

- `s0` has no module loaded;
- `s1` loads the module and forwards no `SET` events;
- `s2` forwards exactly one event per request; and
- `events_lost`, `dropped`, and `handler_panics` remain zero.

## Observed runs

- [2026-07-30: first AWS smoke run](observations/2026-07-30.md)

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
