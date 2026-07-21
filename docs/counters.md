# Counters

The module exposes one INFO section, `eventstream_stats`. The same values are
available as a flat array from `EVENTSTREAM.STATS` and are logged at unload. All
counters are process-lifetime: monotonic, reset to zero on load, never
persisted or replicated — so monitoring must alert on *increases*
(`rate()`/`increase()`), not absolute values, and tolerate a reset to zero
across a module reload or [in-place upgrade](./upgrading.md).

> **Module INFO sections do not appear in default `INFO` or `INFO all`.** The
> accepted forms and the full caveat are in
> [Monitoring](./monitoring.md#the-info-everything-caveat).

## Fields

{{#include ../SPEC.md:counters-info}}

## Meaning and derivation

The definitions below are included from the authoritative
[specification](./specification.md) (section 13). In short: `events_lost` is the
total-loss SLO — one per selected event that produced no canonical entry, the
field to alert on for "did this node lose an event" (it includes the zero-slot
case that `dropped` omits); `dropped` is a per-*write* count of failed
destination writes (it also counts firehose-copy failures, so it can exceed or
diverge from `events_lost`); `handler_panics` should always be zero and any
nonzero value is a module bug; and `dropped_no_owned_slot`, `dropped_migrating`,
`repins`, `repins_probe_detected`, `cluster_per_node`, and `cluster_pinned_tag`
are cluster per-node fields.

{{#include ../SPEC.md:counters-explanation}}

## Alerting

{{#include ../SPEC.md:alerting-table}}

Deployable Prometheus rules and a Grafana dashboard implementing this table
ship in [`contrib/monitoring/`](./monitoring.md).
