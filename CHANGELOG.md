# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-11

Cluster support and a load-testing example client. Standalone behavior is
unchanged and the new capabilities are opt-in.

### Added

- Cluster per-node capture (`eventstream.cluster-streams`): the module still
  refuses to load in cluster mode by default; setting `per-node` enables
  capture where each master pins its streams to a hash tag whose slot it owns,
  so mirrored writes stay local. The tag is selected lazily on the first
  captured event and shared across the node's event streams, control stream,
  and registry so they co-locate.
- Dynamic re-pinning after a reshard: when a mirrored write hits the cluster
  local-refusal error, the node re-pins to a slot it still owns, writes a
  `repinned` gap marker to the new control stream, and retries so the
  triggering event is captured rather than dropped. Old streams migrate with
  their slot to its new owner. A `repins` counter is added to `INFO
  eventstream` and `EVENTSTREAM.STATS`.
- Cluster-wide discovery and consumer guidance: each master's
  `EVENTSTREAM.STREAMS` reports its own tagged streams, and
  [docs/consumer-patterns.md](docs/consumer-patterns.md) documents the
  client-side fan-out-and-merge across masters, the same-millisecond cross-node
  tie caveat, and the failover behavior (a promoted replica re-derives the same
  tag, so stream names are stable).
- `@missed` (read misses) and `@new` (new-key) event classes: opt-in
  high-volume classes, subscribable at load time.
- Example client (`cargo run --example eventstream_client`): drives events and
  reads them back against a standalone server or a per-node cluster
  (auto-detected), with `info`, `produce`, `consume`, `watch`, and `soak`
  subcommands. Doubles as the cluster consumer reference and a soak driver.
- Cluster paths stress-tested under load: 40k events through a live reshard
  (zero loss, one clean re-pin), a master kill with replica promotion (stream
  names stable, no double capture), and 50k mass expirations (zero loss). CI
  continues to run the full suite against Redis 7.2.8, 7.4.5, 8.8.0, and Valkey
  8.1.6.

### Fixed

- A module panic in a post-notification job can no longer abort the server:
  both job bodies are wrapped in `catch_unwind` and count into `handler_panics`
  instead of unwinding across the FFI boundary. This surfaced as a Redis 7.2
  crash in per-node cluster mode, where `select_owned_tag` called the
  `RedisModule_ClusterCanonicalKeyNameInSlot` API (added after 7.2) through
  `.unwrap()`; the optional pointer is now null-checked with a 7.2-compatible
  fallback.

## [0.1.0] - 2026-07-10

Initial release: the full v0.1 scope of [SPEC.md](SPEC.md), plus the
introspection commands added immediately after it.

### Added

- Capture path: keyspace notifications mirrored as `XADD`s into per-event
  streams (`events:expired`, `events:set`, ...), atomically with the
  triggering change, replicated to replicas and the AOF, with `maxmemory`
  respected (refusals are counted drops, never forced writes).
- Event filter (`eventstream.events`): `*`, `@class` tokens, or exact event
  names; validated at `CONFIG SET` with parse errors surfaced to the caller.
  Default `expired`.
- Configuration: `eventstream.enabled`, `eventstream.stream-prefix`
  (immutable, validated), `eventstream.events`, `eventstream.maxlen`
  (approximate per-stream trimming), settable as module arguments and, except
  the prefix, live.
- Database consolidation: all destination streams live in database 0; each
  entry records its origin database in the `db` field.
- Gap markers: a control stream at `<prefix>#control` records `loaded`,
  `disabled`, `enabled`, and `unloading` boundaries so consumers can reconcile
  over known capture gaps instead of rescanning the keyspace.
- Observability: a module INFO section (`INFO eventstream`) with forwarded,
  per-reason dropped and skipped counters, active streams, control markers,
  and last error time.
- Introspection commands: `EVENTSTREAM.STATS` (structured counters) and
  `EVENTSTREAM.STREAMS` (destination streams, backed by a persistent registry
  that survives restart and works on replicas).
- Safety gates: master-only capture, no capture during dataset loading, a
  prefix feedback guard, refusal to load below Redis 7.2 or in cluster mode.
- Tooling: `demo.sh` (scripted end-to-end run), `demo-preflight.sh` (live
  deployment verifier), `bench/run.sh` (the SPEC section 11 measurement plan).
- Docs: SPEC.md (authoritative design), consumer patterns, loss windows and
  gap reconciliation, cluster design proposal (v0.2+).
- Verified in CI against Redis 7.2.8, 7.4.5, 8.8.0, and Valkey 8.1.6, with a
  46-test suite (unit plus integration against real servers), including
  replication, AOF durability, crash-gap, and OOM loss-window scenarios.

[0.2.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.2.0
[0.1.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.1.0
