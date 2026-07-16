# Monitoring

The module exposes its counters through the `eventstream_stats` INFO section
(see the [Specification](./specification.md), section 13). Deployable Prometheus
alert rules, a Grafana dashboard, a metrics collector, and a ready-to-run
`docker-compose` stack live in
[`contrib/monitoring/`](https://github.com/joshrotenberg/redis-event-stream-module/tree/main/contrib/monitoring).

## The `INFO everything` caveat

Module INFO sections **do not appear** in `INFO` or `INFO all` — only in
`INFO everything`, `INFO eventstream`, `INFO eventstream_stats`, or
`INFO modules`. Monitoring that scrapes the default `INFO` sees none of the
`eventstream_*` fields. This is the most common monitoring surprise with this
module.

## redis_exporter cannot export the counters

A stock [`redis_exporter`](https://github.com/oliver006/redis_exporter) cannot
export the `eventstream_*` counters. Its `--include-modules-metrics` flag runs
`INFO MODULES` (which does contain this module's section), but the exporter then
filters every field through an internal allow-list of metric names it already
knows; the `eventstream_*` fields are not in it, and there is no flag to add
them. All you get from `redis_exporter` for this module is the
`module_info{name="eventstream",ver="..."}` gauge — presence and version, not
counters.

Keep `redis_exporter` in the deployment anyway: the alert rules use its standard
`redis_expired_keys_total`, and `--check-streams=events:*` gives
`redis_stream_length` and `redis_stream_group_lag` for the stream-growth and
consumer-lag alerts. The module counters themselves come from the sidecar
collector below.

## The sidecar collector

[`contrib/monitoring/exporter/eventstream-textfile.sh`](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/contrib/monitoring/exporter/eventstream-textfile.sh)
runs `INFO eventstream` and writes each numeric field as a metric named exactly
as in the INFO section (`eventstream_forwarded`, `eventstream_dropped`, …), so
each metric maps 1:1 to the section 13 counter table. Deploy it as a
[node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector)
(a cron job or loop writing atomically into the collector directory):

```sh
export REDIS_HOST=127.0.0.1 REDIS_PORT=6379
DIR=/var/lib/node_exporter/textfile
eventstream-textfile.sh > "$DIR/eventstream.prom.$$" \
  && mv "$DIR/eventstream.prom.$$" "$DIR/eventstream.prom"
```

## Alerts

[`contrib/monitoring/prometheus/eventstream.rules.yml`](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/contrib/monitoring/prometheus/eventstream.rules.yml)
is the section 13 alerting-guidance table made executable:

| Alert | Fires when | Meaning |
|---|---|---|
| `EventstreamDropped` | `increase(eventstream_dropped[5m]) > 0` | Events were not mirrored; break down by the `dropped_*` reasons. |
| `EventstreamMaxStreamsRefusing` | `increase(eventstream_dropped_max_streams[5m]) > 0` | The `max-streams` cap is refusing new streams. |
| `EventstreamDisabled` | `eventstream_enabled == 0` | Module loaded but capture is off. |
| `EventstreamEvictionRisk` | `eventstream_eviction_risk == 1` | `maxmemory-policy` is `allkeys-*` and can evict the streams. |
| `EventstreamHandlerPanic` | `eventstream_handler_panics > 0` | A caught panic — always a module bug. |
| `EventstreamFilterMisconfigured` | forwarded flat while keys expire | Event/class or key filter too narrow, or notifications off. |
| `EventstreamStreamGrowth` | `events:*` length past threshold | `maxlen` is 0/too high or a consumer stalled. |
| `EventstreamConsumerLag` | group lag past threshold | A consumer is falling behind the write rate. |
| `EventstreamMigratingDrops` | `increase(eventstream_dropped_migrating[5m]) > 0` | Slot-migration drops; expected only during a planned reshard. |
| `EventstreamRepinProbeFallback` | `eventstream_repins_probe_detected > 0` | The migration error-string match may have broken. |
| `EventstreamAutogroupFailing` | `increase(eventstream_autogroup_failed[15m]) > 0` | `eventstream.auto-group` could not create a group. |
| `EventstreamCollectorDown` | `eventstream_up == 0` | The collector cannot reach the module. |

Counters reset to 0 on module reload; every rule uses `rate()`/`increase()` and
never reads an absolute counter value. Validate the file with
`promtool check rules`.

## Local stack

```sh
docker compose -f contrib/monitoring/docker-compose.yml up
```

Brings up Redis with the module, the sidecar collector, `node_exporter`,
`redis_exporter`, Prometheus (with the rules loaded), and Grafana (with the
dashboard provisioned) at `http://localhost:3000`. See
[`contrib/monitoring/README.md`](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/contrib/monitoring/README.md)
for details.
