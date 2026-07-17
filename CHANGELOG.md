# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-07-16

Packaging and operability release: a preloaded container image, a Redis
Enterprise RAMP bundle, a shipped consumer client, signed release artifacts, and
a monitoring stack, plus new capture filters, retention controls, and an ACL
category. Standalone and cluster capture behavior from 0.2.0 is unchanged; the
new surface is opt-in.

### Added

- Preloaded Docker image published to
  `ghcr.io/joshrotenberg/redis-event-stream-module` on each release: a Redis
  server built from source with the module `.so` loaded, so a bare `docker run`
  starts a server that is already capturing. Multi-arch (`linux/amd64` and
  `linux/arm64`), with a Valkey 8 variant. See [docs/docker.md](docs/docker.md).
- Redis Enterprise RAMP bundle (`ramp pack`) so the module installs through the
  Enterprise `POST /v1/modules` API rather than a bare `.so`.
- Shipped consumer client (`eventstream-client`), promoted from the example to a
  workspace crate: a standalone binary with `info`, `produce`, `consume`,
  `watch`, and `soak` subcommands that discovers streams, fans out and merges
  reads across cluster masters, and doubles as a soak driver. Attached to
  releases for Linux and macOS.
- Opt-in firehose stream that mirrors every captured event into a single ordered
  stream for consumers that want one feed instead of per-event streams.
- Capture filters applied at load time: key-name glob, source-db, and
  max-streams limits to scope what is mirrored.
- Retention controls: per-event `maxlen`, time-based retention, and an optional
  `verify-oom` guard.
- Configurable entry format (an entry-format enum) and a global monotonic `seq`
  field on mirrored entries.
- `@eventstream` ACL category (with a 7.2/7.3 fallback) so operators can grant
  the module's commands as a unit.
- Optional consumer-group auto-provisioning via `eventstream.auto-group`.
- Gap markers on `FLUSHALL`/`FLUSHDB`, and pinned `SWAPDB` db0 behavior, so
  consumers see an explicit discontinuity instead of silent loss.
- Introspection: `EVENTSTREAM.STREAMS` liveness (`VERBOSE`) with per-stream
  counters, a separate `EVENTSTREAM.PRUNE` command that removes registered
  stream names whose key no longer exists, the crate version reported through
  `MODULE LIST`, and per-stream failure logging.
- Monitoring stack: Prometheus recording/alerting rules, a Grafana dashboard,
  and a metrics collector (see [contrib/monitoring](contrib/monitoring)).
- Startup warning when `maxmemory-policy` is `allkeys-*` (`eviction_risk`),
  since eviction can drop keys before they are captured.
- Defense-in-depth pre-7.2 load gates that pin the SPEC section 15 refusal.
- Signed, attested release artifacts: keyless Sigstore build-provenance
  attestations, an automated tag-to-release workflow, and a macOS x86_64
  artifact.

### Changed

- `INFO`, `EVENTSTREAM.STATS`, and the deinit log are now driven by a single
  counter table, so the three views stay consistent.
- `src/lib.rs` split into `config`, `capture`, `cluster`, `markers`, `stats`,
  and `commands` modules (no behavior change).
- Documentation reorganized around Redis, with a new mdBook site (quickstart,
  reference, and tooling pages) and an as-built cluster-design document.

### Fixed

- Exhaustive slot-tag mapping for the Redis 7.2 cluster fallback.
- Cluster migration-window refusals are now classified and handled with a
  probe-based re-pin fallback instead of dropping the triggering event.
- In-place module upgrade (`MODULE UNLOAD` then `MODULE LOAD` in the same
  process) no longer fails on the second load. Redis keeps a module's
  `@eventstream` ACL category across unload, so re-adding it aborted the reload;
  the category is now registered tolerantly at init. Documented in the new
  [upgrading](docs/upgrading.md) runbook and pinned by an integration test.
- The release workflow now publishes the container image and RAMP bundle on the
  automated tag-push release path. The docker and ramp jobs were gated on a
  `release` event that a `GITHUB_TOKEN`-created release never emits, so they are
  now gated on the workflow-call path the same way the binary upload already is.

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

[Unreleased]: https://github.com/joshrotenberg/redis-event-stream-module/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.3.0
[0.2.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.2.0
[0.1.0]: https://github.com/joshrotenberg/redis-event-stream-module/releases/tag/v0.1.0
