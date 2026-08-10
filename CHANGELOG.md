# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0](https://github.com/joshrotenberg/redis-event-stream-module/compare/v0.5.0...v0.6.0) (2026-08-10)


### Added

* add time-based saturation harness ([#284](https://github.com/joshrotenberg/redis-event-stream-module/issues/284)) ([9af2452](https://github.com/joshrotenberg/redis-event-stream-module/commit/9af245273fb4cc7458215bfe6484932ad8d952e4))
* complete the disposable AWS lab contract ([#283](https://github.com/joshrotenberg/redis-event-stream-module/issues/283)) ([6c71039](https://github.com/joshrotenberg/redis-event-stream-module/commit/6c710397ca7a713d3eaa9030a74ed543893c4cc6))
* **eventstream-client:** decode mixed fixed and batch-v1 entries ([#273](https://github.com/joshrotenberg/redis-event-stream-module/issues/273)) ([30a67b5](https://github.com/joshrotenberg/redis-event-stream-module/commit/30a67b5bcb9a8acebe10131d16bbceb3e8bc2bb8))
* expose restart uncertainty with control checkpoints ([#282](https://github.com/joshrotenberg/redis-event-stream-module/issues/282)) ([9dcf36e](https://github.com/joshrotenberg/redis-event-stream-module/commit/9dcf36ee792d996251fcf9ee5270ab13eeb6c079))


### Fixed

* reset observability state on module reload ([#292](https://github.com/joshrotenberg/redis-event-stream-module/issues/292)) ([51d8546](https://github.com/joshrotenberg/redis-event-stream-module/commit/51d85460010c9a651acd58c74672e9c501422949))


### Performance

* characterize background persistence interference ([#289](https://github.com/joshrotenberg/redis-event-stream-module/issues/289)) ([c0f4227](https://github.com/joshrotenberg/redis-event-stream-module/commit/c0f422794c4a180b6118cc14669ba066f4cabcaa))
* characterize maxmemory pressure and verify-oom ([#290](https://github.com/joshrotenberg/redis-event-stream-module/issues/290)) ([09e3e7c](https://github.com/joshrotenberg/redis-event-stream-module/commit/09e3e7cd804c5c5f47fb0b683bdebc6fea3bd02c))
* compare workload-mix and payload knees ([#286](https://github.com/joshrotenberg/redis-event-stream-module/issues/286)) ([a1f197c](https://github.com/joshrotenberg/redis-event-stream-module/commit/a1f197c6f5d9814fb638f8790d8ebf640c4f850c))
* establish single-node AOF baseline ([#287](https://github.com/joshrotenberg/redis-event-stream-module/issues/287)) ([175f99e](https://github.com/joshrotenberg/redis-event-stream-module/commit/175f99e5cb3953d46230058461a4d4c234aeaab1))
* establish single-replica baseline ([#288](https://github.com/joshrotenberg/redis-event-stream-module/issues/288)) ([cf815b7](https://github.com/joshrotenberg/redis-event-stream-module/commit/cf815b7a7837ed7b7ca38c991f94d27e49899341))
* locate the representative single-node knee ([#285](https://github.com/joshrotenberg/redis-event-stream-module/issues/285)) ([85f9f7a](https://github.com/joshrotenberg/redis-event-stream-module/commit/85f9f7afa24d995254f772e8a12a3da076406ec2))
* run Preview envelope soak and restart-loss probe ([#276](https://github.com/joshrotenberg/redis-event-stream-module/issues/276)) ([15c920e](https://github.com/joshrotenberg/redis-event-stream-module/commit/15c920e9653c4425accb7e40ca0d3c9517d3369a))
* tune Preview envelope batch size and drain wait ([#275](https://github.com/joshrotenberg/redis-event-stream-module/issues/275)) ([c8fa455](https://github.com/joshrotenberg/redis-event-stream-module/commit/c8fa4555a0811cbbedc99c0114209d4d896b40bc))


### Documentation

* publish the tested capacity and operations guide ([#294](https://github.com/joshrotenberg/redis-event-stream-module/issues/294)) ([0f95a10](https://github.com/joshrotenberg/redis-event-stream-module/commit/0f95a10c1929bb83a5623d496ff1171bcd4be0e7))


### Continuous Integration

* reduce multi-architecture release image latency ([#293](https://github.com/joshrotenberg/redis-event-stream-module/issues/293)) ([46d99e1](https://github.com/joshrotenberg/redis-event-stream-module/commit/46d99e13e154d4ea282ad0587bad3e038a061982))

## [0.5.0](https://github.com/joshrotenberg/redis-event-stream-module/compare/v0.4.0...v0.5.0) (2026-07-30)

This release adds Preview asynchronous write modes while keeping `sync` as the
stable default. Preview `envelope` mode can mix stable fixed-field entries with
versioned `batch-v1` envelopes in the same stream, so consumers that opt in
must decode both physical shapes. See the
[consumer guide](https://joshrotenberg.github.io/redis-event-stream-module/consume.html#decode-preview-batch-envelopes)
and [reliability guide](https://joshrotenberg.github.io/redis-event-stream-module/reliability.html)
before enabling it.

### Performance

* add a minimal AWS smoke lab ([#261](https://github.com/joshrotenberg/redis-event-stream-module/issues/261)) ([fe0f854](https://github.com/joshrotenberg/redis-event-stream-module/commit/fe0f8548d7e554d97bee0a653460d3ff5b7a0b50))
* profile the single-node capture hot path ([#264](https://github.com/joshrotenberg/redis-event-stream-module/issues/264)) ([f84c14a](https://github.com/joshrotenberg/redis-event-stream-module/commit/f84c14abc83d36455072c2a991963a4bafbf828b))
* run a higher-concurrency AWS ramp probe ([#263](https://github.com/joshrotenberg/redis-event-stream-module/issues/263)) ([7c4cfd7](https://github.com/joshrotenberg/redis-event-stream-module/commit/7c4cfd777d5f03c697026583b22a0ab4c8d01a7a))
* spike async capture and batched envelopes ([#266](https://github.com/joshrotenberg/redis-event-stream-module/issues/266)) ([34269ce](https://github.com/joshrotenberg/redis-event-stream-module/commit/34269cefa34f5114b681387cc51bbb2fe154d283))


### Miscellaneous

* prepare the 0.5.0 release ([#271](https://github.com/joshrotenberg/redis-event-stream-module/issues/271)) ([2ed7a38](https://github.com/joshrotenberg/redis-event-stream-module/commit/2ed7a38ab08372fec1c5cad030f70ba8e7ebe1be))

## [0.4.0](https://github.com/joshrotenberg/redis-event-stream-module/compare/v0.3.0...v0.4.0) (2026-07-29)


### Added

* add a lifecycle-managed Phoenix LiveView observatory ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* add an at-least-once webhook sink example ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* add at-least-once webhook sink example ([#244](https://github.com/joshrotenberg/redis-event-stream-module/issues/244)) ([09b54e5](https://github.com/joshrotenberg/redis-event-stream-module/commit/09b54e57693cd290608fe10126f581f3d57beea4)), closes [#112](https://github.com/joshrotenberg/redis-event-stream-module/issues/112)
* add events_lost, the per-event total-loss metric ([#218](https://github.com/joshrotenberg/redis-event-stream-module/issues/218)) ([#226](https://github.com/joshrotenberg/redis-event-stream-module/issues/226)) ([cd473c2](https://github.com/joshrotenberg/redis-event-stream-module/commit/cd473c23063d01a474fca2a8fed09fef5018f687))
* cargo-fuzz targets for the filter parser and sanitizer ([#131](https://github.com/joshrotenberg/redis-event-stream-module/issues/131)) ([#163](https://github.com/joshrotenberg/redis-event-stream-module/issues/163)) ([0b5e9ff](https://github.com/joshrotenberg/redis-event-stream-module/commit/0b5e9ff07634e9cead79dfb30d8d7beecc0a5809))
* web live-events demo (SSE bridge + static page) ([#113](https://github.com/joshrotenberg/redis-event-stream-module/issues/113)) ([#230](https://github.com/joshrotenberg/redis-event-stream-module/issues/230)) ([f0ebe1f](https://github.com/joshrotenberg/redis-event-stream-module/commit/f0ebe1fda873b1c9d0519a03c450fca3b8870ca3))


### Fixed

* discover control streams and refresh across re-pins ([#215](https://github.com/joshrotenberg/redis-event-stream-module/issues/215)) ([#228](https://github.com/joshrotenberg/redis-event-stream-module/issues/228)) ([dd66723](https://github.com/joshrotenberg/redis-event-stream-module/commit/dd667239c26eef91f92fb55efdc2011e704608e6))
* keep generated release PR checks reproducible ([#250](https://github.com/joshrotenberg/redis-event-stream-module/issues/250)) ([3738287](https://github.com/joshrotenberg/redis-event-stream-module/commit/373828752860940366b8f28465f9e281587eb0ad))
* make registry registration failures observable and keep stream-cap accounting correct ([#216](https://github.com/joshrotenberg/redis-event-stream-module/issues/216)) ([#227](https://github.com/joshrotenberg/redis-event-stream-module/issues/227)) ([5e1a7dd](https://github.com/joshrotenberg/redis-event-stream-module/commit/5e1a7dd1ba509527128e31a584628e2dfbac7a46))
* preserve arbitrary Redis key bytes in the Rust consumer ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))


### Removed

* retire the legacy Rust/SSE demo in favor of the LiveView observatory ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))


### Documentation

* add Python, Go, and Node consumer examples ([#110](https://github.com/joshrotenberg/redis-event-stream-module/issues/110)) ([#165](https://github.com/joshrotenberg/redis-event-stream-module/issues/165)) ([f37d7ab](https://github.com/joshrotenberg/redis-event-stream-module/commit/f37d7abfaf1a9d2ee8ca25f4c5cbdf13498daeda))
* complete the loss-windows table and de-duplicate the INFO caveat ([#173](https://github.com/joshrotenberg/redis-event-stream-module/issues/173), [#178](https://github.com/joshrotenberg/redis-event-stream-module/issues/178)) ([#197](https://github.com/joshrotenberg/redis-event-stream-module/issues/197)) ([0fe64aa](https://github.com/joshrotenberg/redis-event-stream-module/commit/0fe64aa6de77fa6415b277ca05d8e35633d0449f))
* correct expiration-gap reconciliation claims ([#221](https://github.com/joshrotenberg/redis-event-stream-module/issues/221)) ([610acbc](https://github.com/joshrotenberg/redis-event-stream-module/commit/610acbcfa9c6943aed6fb22a161907e183df6b32)), closes [#217](https://github.com/joshrotenberg/redis-event-stream-module/issues/217)
* define the 1.0 stability contract ([#243](https://github.com/joshrotenberg/redis-event-stream-module/issues/243)) ([733f616](https://github.com/joshrotenberg/redis-event-stream-module/commit/733f616302806d7ed80654083622cdc621bb5e13))
* define the 1.0 stability contract and content-filter boundary ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* define the content-filter boundary ([#245](https://github.com/joshrotenberg/redis-event-stream-module/issues/245)) ([86ea29a](https://github.com/joshrotenberg/redis-event-stream-module/commit/86ea29a24d671a93e17c5bbfbc5c705d6b20c7fc)), closes [#167](https://github.com/joshrotenberg/redis-event-stream-module/issues/167)
* define the supported-surface contract as Stable/Preview/Internal tiers ([#219](https://github.com/joshrotenberg/redis-event-stream-module/issues/219)) ([#229](https://github.com/joshrotenberg/redis-event-stream-module/issues/229)) ([cb10075](https://github.com/joshrotenberg/redis-event-stream-module/commit/cb100757181f8f51874a3a83323617f61eb87b62))
* document a dead-letter pattern for poison entries ([#111](https://github.com/joshrotenberg/redis-event-stream-module/issues/111)) ([#166](https://github.com/joshrotenberg/redis-event-stream-module/issues/166)) ([062086f](https://github.com/joshrotenberg/redis-event-stream-module/commit/062086f3cf56f79493064cb3e8bfd7c4c49e2abf))
* document the black-box-untriggerable counters as asserted-unreachable ([#185](https://github.com/joshrotenberg/redis-event-stream-module/issues/185)) ([#206](https://github.com/joshrotenberg/redis-event-stream-module/issues/206)) ([784eea5](https://github.com/joshrotenberg/redis-event-stream-module/commit/784eea5d6aa416870190bbbac240c51a4ae83700))
* fix example-client flags, README config table, and CONTRIBUTING fuzz claim ([#169](https://github.com/joshrotenberg/redis-event-stream-module/issues/169), [#172](https://github.com/joshrotenberg/redis-event-stream-module/issues/172), [#175](https://github.com/joshrotenberg/redis-event-stream-module/issues/175)) ([#195](https://github.com/joshrotenberg/redis-event-stream-module/issues/195)) ([47ba8a0](https://github.com/joshrotenberg/redis-event-stream-module/commit/47ba8a0575603f53d76b6049184c17f1926d71c0))
* fix links that 404 in the rendered book and cover CHANGELOG.md in the docs workflow ([#176](https://github.com/joshrotenberg/redis-event-stream-module/issues/176), [#177](https://github.com/joshrotenberg/redis-event-stream-module/issues/177)) ([#198](https://github.com/joshrotenberg/redis-event-stream-module/issues/198)) ([cc1fb93](https://github.com/joshrotenberg/redis-event-stream-module/commit/cc1fb936b555fb55abf6609dc5dcd335b93a8df1))
* PRUNE-aware ACL guidance and complete IMMUTABLE/mutable config lists ([#170](https://github.com/joshrotenberg/redis-event-stream-module/issues/170), [#171](https://github.com/joshrotenberg/redis-event-stream-module/issues/171)) ([#196](https://github.com/joshrotenberg/redis-event-stream-module/issues/196)) ([d4639b7](https://github.com/joshrotenberg/redis-event-stream-module/commit/d4639b7a851269bace99a6f79d3a384866b385a5))
* restructure the book around reader roles ([#168](https://github.com/joshrotenberg/redis-event-stream-module/issues/168)) ([#192](https://github.com/joshrotenberg/redis-event-stream-module/issues/192)) ([1e383d8](https://github.com/joshrotenberg/redis-event-stream-module/commit/1e383d898af555d1fafc2bf8d893eab9e5f4995e))
* rewrite product journey and add observatory ([#242](https://github.com/joshrotenberg/redis-event-stream-module/issues/242)) ([22bda55](https://github.com/joshrotenberg/redis-event-stream-module/commit/22bda550ea97fa67e3a4452bbf753889beded649))
* rewrite the README and mdBook around evaluation, integration, reliability, and production ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* SPEC.md staleness sweep ([#174](https://github.com/joshrotenberg/redis-event-stream-module/issues/174)) ([#199](https://github.com/joshrotenberg/redis-event-stream-module/issues/199)) ([9e33817](https://github.com/joshrotenberg/redis-event-stream-module/commit/9e33817224b1549bc9c8d69a55cefbe6c94f4b4f))


### Continuous Integration

* automate release PRs, tags, GitHub releases, and artifacts with Release Please ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* grant packages: write to the release-artifacts workflow_call ([#200](https://github.com/joshrotenberg/redis-event-stream-module/issues/200)) ([8c405bb](https://github.com/joshrotenberg/redis-event-stream-module/commit/8c405bb16f017b3b0c96da53aef8fca6f3ef5e47))
* lint every tracked README and check every Markdown link ([73ed1a2](https://github.com/joshrotenberg/redis-event-stream-module/commit/73ed1a2ef9ad8cc402cbb8ae3112a34a803ae0e6))
* skip the CI workflow on docs-only changes ([#193](https://github.com/joshrotenberg/redis-event-stream-module/issues/193)) ([#194](https://github.com/joshrotenberg/redis-event-stream-module/issues/194)) ([cbc3fcf](https://github.com/joshrotenberg/redis-event-stream-module/commit/cbc3fcffc5fe8a491bf9998e2dae312bda8d6c9c))
* stop Dependabot bumping the MSRV toolchain pin ([#220](https://github.com/joshrotenberg/redis-event-stream-module/issues/220)) ([9726d1a](https://github.com/joshrotenberg/redis-event-stream-module/commit/9726d1a49ab97269f6ae3a34e81b2deafc8f608c))

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
  `linux/arm64`), with a Valkey 8 variant. See the
  [Quickstart](docs/src/quickstart.md).
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
  [upgrading](docs/src/upgrading.md) runbook and pinned by an integration test.
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
  [consumer guide](docs/src/consume.md) documents the
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
