# Benchmarks

`bench/run.sh` measures the performance model from
[the specification](./specification.md) (section 11): the fixed cost every
non-capturing deployment pays, the per-event capture cost, and the mass-expiry
drain case. CI gates on the ratios via `bench/gate.sh`.

## Scenarios

| Scenario | Setup | Measures |
|---|---|---|
| `S0` | No module loaded | Baseline `SET` throughput. |
| `S1` | Module loaded, default filter (captures nothing) | The gate tax: the cost of the notification handler on events it does not mirror. |
| `S2` | Module loaded, `events=set` (100% capture) | Full-capture cost: one extra `XADD` plus inline approximate `MAXLEN` per `SET`. |
| `S3` | Mass-expiry drain, with and without the module | Foreground `GET` p50/p99 and drain duration while an expiring-key backlog drains — the storm case section 11 says to watch. |
| `S4` | The `S2` workload across `eventstream.maxlen` values | Trim-cost sensitivity; section 11 predicts near-zero amortized trim cost at any value. |

## Running

```sh
bench/run.sh                          # all scenarios, defaults
BENCH_SCENARIOS="s0 s1 s2" bench/run.sh
BENCH_REQUESTS=2000000 bench/run.sh
BENCH_JSON=results.json bench/run.sh  # also emit JSON for gating
```

Each scenario runs `BENCH_REPS` times (default 3) and the median rep is
reported. Knobs: `BENCH_PORT`, `BENCH_REQUESTS`, `BENCH_CLIENTS`,
`BENCH_THREADS`, `BENCH_KEYSPACE`, `BENCH_PAYLOAD`, and the S3-specific TTL/
foreground settings documented in the script header.

## Reading results

Results print as a table of ops/sec and p50/p99, with S1/S2/S4 shown as a
percentage of the S0 baseline. The tool used (`redis-benchmark` by default,
since it ships with every Redis and Valkey) is printed and should be cited
alongside any published numbers; section 11 specifies `memtier_benchmark` with
60-second runs for formal measurement, which you can swap in via the knobs.
When `BENCH_JSON` is set, one JSON object per scenario is written so CI (and
`bench/gate.sh`) can gate on ratios without parsing prose.

> The S1/S2 ratio is unstable below roughly one million requests; keep
> `BENCH_REQUESTS` high for meaningful capture-overhead numbers.
