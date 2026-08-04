# Time-based saturation harness

`bench/saturation.sh` is the reproducible performance runner. It complements
the fast, request-counted `bench/run.sh`; it does not replace that CI-oriented
check.

The default campaign runs five randomized repetitions of:

- `s0`: Redis/Valkey without the module;
- `s1`: the module's filtered fast path;
- `s2`: stable synchronous capture at deterministic 0%, 1%, 10%, and 100%
  selectivity; and
- `expiry-s0` / `expiry-s2`: a foreground GET workload that is kept running
  until a staggered TTL backlog has fully drained.

Each ordinary trial has an explicit 10-second warmup and 60-second measurement
window. The runner sweeps clients per thread, threads, and pipeline depth, and
reports throughput plus p50, p95, p99, p99.9, and maximum latency from
memtier's HDR data. Defaults are deliberately campaign-sized; use the smoke
example below for a quick local check.

## Prerequisites

- Redis or Valkey 7.2 or newer, with module commands enabled
- `memtier_benchmark` 2.5.1
- `redis-cli`, `jq`, and standard Unix tools
- Rust/Cargo when the local runner should build the checked-out module

The cloud runner uses the pinned multi-architecture image
`redislabs/memtier_benchmark@sha256:5f15b74f657fd30ee73453af9caa1781de1614f4d934d46feee711dc19b758af`.

## Local use

Review the deterministic plan without starting Redis:

```sh
SATURATION_PLAN_ONLY=yes bench/saturation.sh
```

Run a short smoke campaign:

```sh
SATURATION_SCENARIOS="s0 s1 s2 expiry-s0 expiry-s2" \
SATURATION_SELECTIVITIES="0 10 100" \
SATURATION_REPETITIONS=1 \
SATURATION_WARMUP_SECONDS=1 \
SATURATION_MEASUREMENT_SECONDS=5 \
SATURATION_EXPIRY_KEYS=10000 \
bench/saturation.sh
```

By default the harness builds the release module, starts an isolated local
server on port 6394, and tears it down at exit. Set
`SATURATION_BUILD_MODULE=no` to reuse the existing release artifact.

## Reachable remote endpoint

The same runner can drive a self-managed remote endpoint without source edits:

```sh
SATURATION_START_SERVER=no \
SATURATION_HOST=10.0.0.10 \
SATURATION_PORT=6379 \
SATURATION_SERVER_MODULE_PATH=/var/lib/eventstream/libredis_event_stream_module.so \
SATURATION_RESULTS_DIR=bench/results/remote-01 \
bench/saturation.sh
```

The path names the module on the **server**, not on the load-generator host.
The target must allow `MODULE LOAD` and `MODULE UNLOAD` so every randomized
trial can switch between S0, S1, and S2 without restarting the process. A URI,
including credentials and TLS, can be supplied with `SATURATION_URI`.

## Sweep controls

All lists are whitespace-separated:

| Variable | Default | Meaning |
|---|---|---|
| `SATURATION_SCENARIOS` | `s0 s1 s2 expiry-s0 expiry-s2` | Scenarios to run |
| `SATURATION_SELECTIVITIES` | `0 1 10 100` | S2 SET percentages; the remainder is HSET |
| `SATURATION_CLIENT_LEVELS` | `50` | Connections per memtier thread |
| `SATURATION_THREAD_LEVELS` | `4` | memtier threads |
| `SATURATION_PIPELINE_LEVELS` | `1` | Requests in flight per connection |
| `SATURATION_RATE_LIMIT_LEVELS` | `0` | Requests/s per connection; `0` is unlimited |
| `SATURATION_REPETITIONS` | `5` | Independent trials per matrix cell |
| `SATURATION_WARMUP_SECONDS` | `10` | Warmup duration |
| `SATURATION_MEASUREMENT_SECONDS` | `60` | Measured duration |
| `SATURATION_P99_BUDGET_MS` | `2` | Healthy-knee median p99 ceiling |
| `SATURATION_ACHIEVEMENT_RATIO` | `0.98` | Minimum median achieved/target rate |
| `SATURATION_WORKLOAD_NAME` | `builtin-write-only` | Artifact label for separate workload runs |
| `SATURATION_SEED` | `254` | Deterministic trial order and expiry jitter |

The 0/1/10/100 mix uses separate string and hash keyspaces, avoiding accidental
`WRONGTYPE` errors. The exact number of SET operations reported by memtier is
reconciled with the module's `forwarded` counter; ratios are not inferred from
elapsed time.

## Offered-load knee search

Set `SATURATION_RATE_LIMIT_LEVELS` to compare scenarios at the same offered
load. memtier applies each value to every connection, so the aggregate target
is `clients per thread * threads * rate limit`. The manifest and every trial
record both the per-connection limit and aggregate target. For example, 50
clients on four threads with levels `250 500 625 750` targets 50k, 100k, 125k,
and 150k requests/s:

```sh
SATURATION_SCENARIOS="s0 s1 s2" \
SATURATION_SELECTIVITIES="0 10 100" \
SATURATION_RATE_LIMIT_LEVELS="250 500 625 750" \
SATURATION_REPETITIONS=3 \
SATURATION_P99_BUDGET_MS=2 \
SATURATION_ACHIEVEMENT_RATIO=0.98 \
bench/saturation.sh
```

`knee.json` classifies each repeated rate level as healthy when its median p99
is within `SATURATION_P99_BUDGET_MS` and its median achieved throughput is at
least `SATURATION_ACHIEVEMENT_RATIO` of the target. It reports the highest
healthy level plus its adjacent lower and higher points. A status of
`ceiling-not-reached` means the sweep needs a higher coarse point;
`no-healthy-point` means it needs a lower point or a revised latency budget;
only `bracketed` locates both sides of the knee.

The normalized trial also includes Redis process and, when the server exposes
them, main-thread CPU deltas, CPU per operation, and post-trial memory/RSS.
These values are meaningful only for a sufficiently long measured window;
short smoke runs validate artifact shape rather than capacity.

## Mass-expiry contract

The expiry cases preload `SATURATION_EXPIRY_KEYS` keys with TTLs distributed
across `SATURATION_EXPIRY_TTL_MIN_MS` plus
`SATURATION_EXPIRY_TTL_SPREAD_MS`. A low-client GET workload starts before the
first expiry and has a hard `SATURATION_EXPIRY_TIMEOUT_SECONDS` bound. The
harness polls `INFO keyspace`, records the first decrease and zero-key
timestamps, then interrupts memtier only after the drain completes.

The trial fails unless:

- the foreground started before the first observed expiry;
- its JSON measurement finished after the last observed expiry;
- all keys drained;
- memtier reported no connection or Redis command errors; and
- `expiry-s2` forwarded exactly one event per expired key with zero drops,
  losses, worker errors, or panics.

This removes the old fixed-request dilution where foreground traffic could
finish before most expirations happened.

## Arbitrary commands and replay

Use `custom-s0`, `custom-s1`, and/or `custom-s2` with a JSON command file. Each
entry is passed directly to memtier and may declare how many selected module
events one successful command should create:

```sh
SATURATION_SCENARIOS="custom-s0 custom-s1 custom-s2" \
SATURATION_COMMANDS_FILE=bench/saturation/workloads/mixed-writes.json \
SATURATION_CAPTURE_EVENTS="set,hset" \
bench/saturation.sh
```

This supports HSET, DEL, MSET, multi-key DEL, and module-generated event probes;
the command text determines the workload and `SATURATION_CAPTURE_EVENTS`
determines the module filter. For deterministic reconciliation, set
`expected_events_per_command` to the number of selected notifications emitted
by each successful command. Leave it at zero for an intentionally unselected
command.

For a Redis MONITOR capture, set `SATURATION_MONITOR_INPUT` instead of a command
file and provide the known aggregate with `SATURATION_EXPECTED_EVENTS`. Replay
preserves the raw per-command output. Do not combine separately aggregated
command percentiles as if they were an exact percentile for their union; use
the `Totals` distribution or the saved server-side latency histograms.

The checked-in `read-heavy.json`, `balanced.json`, and `write-heavy.json`
specifications provide small representative mixes for knee campaigns. Use
`custom-s0 custom-s1 custom-s2`, label the run with
`SATURATION_WORKLOAD_NAME`, and set `SATURATION_CAPTURE_EVENTS` to the selected
write events. Value size remains an explicit campaign control through
`SATURATION_PAYLOAD_BYTES`; run matched campaigns rather than blending sizes
into percentiles that cannot be separated later.

## Hooks and artifacts

`SATURATION_METRICS_HOOK` runs at every pre/post checkpoint with
`SATURATION_HOOK_PHASE` and `SATURATION_HOOK_DIR`. `SATURATION_PROFILER_HOOK`
runs with `SATURATION_HOOK_ACTION=start|stop`, `SATURATION_TRIAL_ID`, and
`SATURATION_HOOK_DIR`. A nonzero hook exit fails the campaign.

Each versioned result directory contains:

- `manifest.json`: source, target, generator version, and exact matrix;
- `plan.tsv`: randomized execution order;
- `raw/<trial>/`: exact memtier argv, JSON/text/HDR output, stderr, Redis INFO,
  `EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS WITHSTATS`, Redis latency data, hook
  output, and expiry timeline;
- `trials.json`: normalized per-trial results with counter before/after/delta;
- `summary.json`: min/median/max distributions by matrix cell; and
- `knee.json`: equal-offered-load health classification and adjacent points;
- `result.json`: machine-readable campaign status.

Redis command errors are detected in memtier stderr because memtier can exit
zero after receiving `-ERR` replies. Any connection error, command error,
counter mismatch, drop, lost event, worker error, panic, or expiry-overlap
failure stops the run immediately.
