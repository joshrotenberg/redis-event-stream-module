# Introduction

`redis-event-stream-module` is a Redis module that mirrors keyspace
notifications into per-event Redis Streams. Each selected event (key
expiration, `SET`, `DEL`, ...) becomes a stream entry, written atomically with
the keyspace change, so events are durable, replayable, and consumable with
`XREAD` or consumer groups. It runs on Redis 7.2+ and Valkey 8.x/9.x, standalone,
with replicas, or in a cluster with opt-in per-node capture.

This site is the reference documentation. For a quick overview, install steps,
and a runnable demo, start with the
[README](https://github.com/joshrotenberg/redis-event-stream-module#readme).

## What is here

- **[Consumer patterns](./consumer-patterns.md)** - reading the mirrored
  streams: live tail, durable work queues with consumer groups, replay,
  discovery, and cluster fan-out-and-merge.
- **[Loss windows and reconciliation](./loss-windows.md)** - every way an event
  can be lost, how to detect it, and how to reconcile a gap window without a
  full keyspace scan.
- **[Cluster support](./cluster-design.md)** - the slot-pinned per-node design:
  hash-tag selection, re-pinning after a reshard, failover, and discovery.
- **[Specification](./specification.md)** - the authoritative design:
  architecture, event routing, entry schema, configuration, delivery semantics,
  failure modes, and observability.

The module writes only streams; everything on the read side is ordinary Redis
Streams, so any Streams-capable client works.
