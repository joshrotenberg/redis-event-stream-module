# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[0.1.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.1.0
