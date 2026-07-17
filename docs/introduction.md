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

Each section addresses one audience:

- **Getting started** - install, load, and read a mirrored event back
  ([Quickstart](./quickstart.md)), then exercise the whole path with the
  [scripted demo](./demo.md).
- **Consuming events** - for the developer reading the streams:
  [consumer patterns](./consumer-patterns.md) with companion pages on
  [work queues](./work-queues.md), [entry shapes and the
  firehose](./entry-shapes.md), and
  [cluster consumers](./cluster-consumers.md);
  [loss windows and reconciliation](./loss-windows.md); and the
  [example client](./example-client.md).
- **Deployment and operations** - for the operator running the module: the
  [Docker image](./docker.md), [Redis Enterprise](./enterprise.md),
  [cluster support](./cluster-support.md),
  [preflight checks](./preflight.md), [sizing and retention](./sizing.md),
  [monitoring](./monitoring.md), and [upgrading](./upgrading.md).
- **Reference** - lookup-ordered pages for
  [configuration](./configuration.md), [commands](./commands.md),
  [counters](./counters.md), [gap markers](./gap-markers.md), and
  [benchmarks](./benchmarks.md), plus the full authoritative
  [specification](./specification.md).
- **Project** - the [cluster design history](./cluster-design-history.md),
  [changelog](./changelog.md), [contributing](./contributing.md), and
  [security](./security.md).

The module writes only streams; everything on the read side is ordinary Redis
Streams, so any Streams-capable client works.
