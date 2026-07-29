# Monitoring

Monitor capture health, retained capacity, and consumer progress separately. A
healthy Redis server does not by itself prove that selected events reached
their streams.

## Collect the module section

Module fields do not appear in plain `INFO` or `INFO all`. Use one of:

```text
INFO eventstream
INFO eventstream_stats
INFO modules
INFO everything
```

`EVENTSTREAM.STATS` returns the same counters as a structured Redis reply for
tools that do not parse INFO output.

A stock `redis_exporter` exposes module presence but does not currently export
arbitrary `eventstream_*` fields. The repository therefore includes a small
textfile collector under `contrib/monitoring/exporter/`. Keep
`redis_exporter` for ordinary Redis metrics and stream/group measurements.

## Alert on loss first

These are the primary signals:

| Signal | Alert when |
|---|---|
| `eventstream_events_lost` | Any increase |
| `eventstream_dropped` | Any increase |
| `eventstream_enabled` | `0` when capture should be active |
| `eventstream_handler_panics` | Nonzero |
| `eventstream_eviction_risk` | `1` |
| `eventstream_autogroup_failed` | Any increase when auto-group is enabled |
| `eventstream_dropped_migrating` | Any increase outside a planned reshard |

`events_lost` counts selected logical events without a canonical per-event
entry. `dropped` counts failed destination writes, so it can also move for a
failed auxiliary firehose copy.

Counters reset when the module reloads. Alert with `rate()` or `increase()`
rather than assuming an absolute value remains monotonic across upgrades.

## Watch capacity and lag

Use `XLEN` to observe stream growth and `XINFO GROUPS` to monitor group lag.
Alert before the oldest retained ID passes a consumer's resume point.

Compare `eventstream_forwarded` with the source workload. For an
expiration-only setup, a flat forwarded counter while Redis
`expired_keys` rises usually indicates a filter or deployment problem.

In cluster mode, scrape every master and preserve a node label. Aggregate
application-level loss for the overall service while retaining per-node detail
for diagnosis.

## Use the included stack

The repository provides Prometheus rules, a Grafana dashboard, the textfile
collector, and a local compose stack:

```bash
docker compose \
  -f contrib/monitoring/docker-compose.yml \
  up
```

Grafana starts on [http://localhost:3000](http://localhost:3000). The included
rules cover loss, disabled capture, eviction risk, stream growth, consumer lag,
cluster migration, and collector availability.

Exact field definitions are in
[Commands and observability](reference/commands-observability.md).
