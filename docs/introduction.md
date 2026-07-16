# Introduction

`redis-event-stream-module` is a Redis module that mirrors keyspace
notifications into per-event Redis Streams. Each selected event (key
expiration, `SET`, `DEL`, ...) becomes a stream entry, written atomically with
the keyspace change, so events are durable, replayable, and consumable with
`XREAD` or consumer groups. It runs on Redis 7.2+, standalone, with replicas,
or in a cluster with opt-in per-node capture.

This site is the reference documentation. New here? Start with the
[Quickstart](./quickstart.md) — install, load, and capture your first event.

## What is here

- **[Quickstart](./quickstart.md)** - install from a release artifact or source,
  load the module, and read a mirrored event back.
- **[Demo and preflight](./demo.md)** - the scripted end-to-end demo and the
  deployment health check.
- **[Docker image](./docker.md)** - the preloaded image: `docker run` one-liner,
  passing module arguments, tags and variants.
- **[Redis Enterprise](./enterprise.md)** - the RAMP bundle: uploading to a
  self-managed Enterprise cluster, the Cloud exclusion, and multi-shard
  (per-shard-stream) semantics.
- **[Consumer patterns](./consumer-patterns.md)** - reading the mirrored
  streams: live tail, durable work queues with consumer groups, replay,
  discovery, and cluster fan-out-and-merge.
- **[Loss windows and reconciliation](./loss-windows.md)** - every way an event
  can be lost, how to detect it, and how to reconcile a gap window without a
  full keyspace scan.
- **[Cluster support](./cluster-design.md)** - the slot-pinned per-node design:
  hash-tag selection, re-pinning after a reshard, failover, and discovery.
- **[Monitoring](./monitoring.md)** - Prometheus rules, a Grafana dashboard, and
  a metrics collector for the INFO counters.
- **Reference** - lookup-ordered pages for
  [configuration](./configuration.md), [commands](./commands.md),
  [counters](./counters.md), [gap markers](./gap-markers.md), the
  [example client](./example-client.md), and [benchmarks](./benchmarks.md),
  plus the full authoritative [specification](./specification.md).

The module writes only streams; everything on the read side is ordinary Redis
Streams, so any Streams-capable client works.
