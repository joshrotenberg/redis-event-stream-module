# Monitoring

Deployable Prometheus + Grafana artifacts for `redis-event-stream-module`. This
is the [SPEC.md](../../SPEC.md) section 13 alerting-guidance table turned into
files you can load, instead of prose each operator re-derives by hand.

```
exporter/eventstream-textfile.sh    scrape INFO eventstream -> Prometheus text
prometheus/eventstream.rules.yml    recording + alert rules (the section 13 table)
prometheus/prometheus.yml           example scrape config for the stack below
grafana/eventstream-dashboard.json  importable dashboard
grafana/provisioning/               datasource + dashboard auto-provisioning
docker-compose.yml                  a working stack: redis+module -> exporters -> Prometheus -> Grafana
```

## The `INFO everything` caveat (read this first)

Module INFO sections **do not appear** in `INFO` or `INFO all` — only in
`INFO everything`, `INFO eventstream`, `INFO eventstream_stats`, or
`INFO modules` (SPEC.md section 13). Any monitoring that scrapes the default
`INFO` will silently see none of the `eventstream_*` fields. This is the single
most common monitoring surprise with this module.

## Why not a stock redis_exporter?

`redis_exporter` **cannot** export the `eventstream_*` counters. This was
verified against the current release:

- Its `--include-modules-metrics` flag does run `INFO MODULES`, and this
  module's section *is* present there. But the exporter then filters every
  field through an internal allow-list of metric names it already knows
  (`includeMetric` in `exporter/metrics.go`). The `eventstream_*` fields are not
  in that list, so they are dropped. RediSearch's `search_*` fields work only
  because they were hard-coded into that allow-list; there is no flag to add
  arbitrary INFO field names.
- The one thing a stock `redis_exporter` *does* give you for this module is the
  `module_info{name="eventstream",ver="..."}` gauge — presence and version, not
  counters. Keep `redis_exporter` in the deployment anyway: the rules need its
  standard `redis_expired_keys_total`, and `--check-streams=events:*` provides
  `redis_stream_length` / `redis_stream_group_lag` for two of the alerts.

So the module counters come from the **sidecar collector** below, and
`redis_exporter` covers everything else. (A `redis_exporter --script` Lua metric
that parses `INFO eventstream` is theoretically possible but is not shipped here;
the sidecar is dependency-light and directly testable.)

## The sidecar collector

`exporter/eventstream-textfile.sh` runs `INFO eventstream` and writes each
numeric field as a metric named exactly as in the INFO section — so
`eventstream_forwarded`, `eventstream_dropped`, … map 1:1 to the SPEC.md
section 13 table. Counter fields deliberately keep no `_total` suffix, to keep
that mapping literal. The string field `cluster_pinned_tag` has no numeric
form and is skipped. When the server is unreachable or the module is not
loaded, the script emits `eventstream_up 0` and nothing else.

Deploy it as a [node_exporter textfile
collector](https://github.com/prometheus/node_exporter#textfile-collector) — a
cron job or loop writing atomically into the collector directory:

```sh
export REDIS_HOST=127.0.0.1 REDIS_PORT=6379   # REDIS_PASSWORD / REDIS_CLI_ARGS optional
DIR=/var/lib/node_exporter/textfile
exporter/eventstream-textfile.sh > "$DIR/eventstream.prom.$$" \
  && mv "$DIR/eventstream.prom.$$" "$DIR/eventstream.prom"
```

Run it on the same interval as your Prometheus scrape (e.g. every 15s). The
`docker-compose.yml` stack wires exactly this pattern.

## Rules

`prometheus/eventstream.rules.yml` — 3 recording rules and 12 alerts, one per
row of the section 13 table (plus `handler_panics`, which the spec calls out as
always-zero, and a collector-health alert). Load it via `rule_files:` and lint
it with `promtool check rules prometheus/eventstream.rules.yml`.

Counters reset to 0 when the module reloads (process-lifetime, SPEC.md
section 13). Every rule that reads a counter uses `rate()`/`increase()`, which
tolerate resets; no rule reads an absolute counter value.

`EventstreamFilterMisconfigured` correlates a sidecar metric with a
`redis_exporter` metric, so both scrape jobs must carry a shared label
identifying the Redis instance (`redis_instance` in the provided
`prometheus.yml`); the match is `on(redis_instance)`. If the label is absent or
differs, the rule never matches — no false positives. Two thresholds
(`EventstreamStreamGrowth`, `EventstreamConsumerLag`) are deployment specific;
the defaults assume the module's default `maxlen` of 10000 — tune them.

## Dashboard

`grafana/eventstream-dashboard.json` imports against any Prometheus datasource
(it exposes a `datasource` variable). Panels: capture/collector/eviction state,
forwarded/dropped/skipped rates, `events:*` stream length and consumer lag
(from `redis_exporter`), and the cluster per-node fields.

## Local stack

```sh
docker compose -f contrib/monitoring/docker-compose.yml up
```

Grafana at http://localhost:3000 (anonymous), Prometheus at
http://localhost:9090 (**Status → Rules** shows the alerts). The `redis`
service is built from the repo `Dockerfile` (module + server from source), so
the stack runs from a fresh checkout and doubles as an end-to-end check that
the artifacts here work against a real module. Drive some traffic to watch the
panels move:

```sh
for i in $(seq 1 500); do redis-cli -p 6379 set k$i v PX 200 >/dev/null; done
```
