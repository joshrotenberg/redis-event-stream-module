# Overview

Redis Event Stream is a Redis module that mirrors selected keyspace events into
Redis Streams. An expiration, `SET`, `DEL`, or another selected event becomes a
bounded stream entry that can be read live, replayed, or distributed through a
consumer group.

The module is especially useful when an expiration is also a unit of work. A
session timeout can trigger cleanup, a lease expiration can release a resource,
or a cache invalidation can be replayed after a worker restart.

## The idea

Redis already emits keyspace notifications, but pub/sub subscribers lose every
message sent while they are disconnected. Redis Event Stream mirrors the same
events into ordinary Redis Streams:

```text
SET session:42 active PX 30000
            │
            └─ expiration event
                    │
                    ▼
             events:expired
             1785349757949-0
             event=expired
             key=session:42
             db=0
```

The write to the destination stream happens on the Redis server, atomically
with the keyspace change. Consumers use normal Redis commands such as `XREAD`,
`XRANGE`, and `XREADGROUP`; no module-specific client is required.

## Why use it

- **Survive consumer disconnects.** Entries wait in a bounded stream instead of
  disappearing when a subscriber is offline.
- **Replay retained history.** Resume from a known stream ID or inspect a time
  range after an incident.
- **Scale workers with consumer groups.** Redis tracks delivery and
  acknowledgement without a separate queue.
- **Detect capture gaps.** A control stream and counters make disabled,
  reconfigured, or failed capture visible.

## Where it fits

| Need | Fit |
|---|---|
| Replay recent expirations or writes | Good fit |
| Replace a fragile keyspace pub/sub subscriber | Good fit |
| Feed an idempotent background work queue | Good fit |
| Keep an independent, permanent audit log | Not a fit |
| Recover events that occurred while Redis or the module was down | Not a fit |
| Provide exactly-once delivery | Not a fit |

This is a best-effort, gap-aware live feed, not change data capture, an
application outbox, a write-ahead log, or a Kafka replacement. Exact capture
is the healthy-path target; definite loss and uncertain restart windows are
observable, not recoverable by the module. Retention is bounded, and crash
durability is whatever the Redis persistence configuration provides.

## Supported environments

Redis 7.2 or newer is required. Standalone Redis and replication/failover are
the primary deployment targets. OSS Cluster per-node capture and multi-shard
Redis Enterprise deployments are available as Preview capabilities with extra
consumer and operational requirements.

The project is pre-1.0. Stable surfaces aim to remain compatible; Preview
surfaces can change while their lifecycle evidence is completed. The
[stability contract](stability.md) classifies the module's wire, operational,
Preview, and internal surfaces and defines how each can evolve.

## Choose a next step

- [Run the Quickstart](quickstart.md) to capture one expiration.
- [Open the Live observatory](observatory.md) to configure Redis, generate
  traffic, and watch the streams update.
- [Configure capture](configure.md) for your event and key patterns.
- Read [Reliability and delivery](reliability.md) before using the module for
  production work.
