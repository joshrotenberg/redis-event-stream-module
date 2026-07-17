# Contributing

Contributions are welcome. This project is spec-first: [SPEC.md](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/SPEC.md) is
the authoritative design, and behavior changes should update it in the same
pull request. If a change needs a design decision first, open a decision issue
(there is a template) rather than encoding the decision silently in code.

## Building and testing

Requirements: Rust 1.88 or newer (the MSRV, declared as `rust-version` in
`Cargo.toml` and gated by the `msrv` leg in CI), and `redis-server`/`redis-cli`
7.2 or newer on PATH. The integration tests spawn real
servers; nothing is mocked. MSRV raises are deliberate, reviewed commits
triggered by a CI failure on the `msrv` leg, and are treated as minor-version
events.

```sh
cargo build --release          # builds the module cdylib
cargo test --lib               # unit tests, no server needed
cargo test --release --tests   # integration suite (spawns servers per test)
./demo.sh                      # scripted end-to-end run
```

To run the integration suite against a specific server build:

```sh
TEST_REDIS_SERVER_BIN=/path/to/redis-server \
TEST_REDIS_CLI_BIN=/path/to/redis-cli \
cargo test --release --tests
```

CI runs the full suite against pinned Redis 7.2, 7.4, and 8.x, so a change must
hold across that matrix, not just your local server.

The unit tests include property tests (proptest) over the events filter
grammar, the event-name sanitizer, and the prefix validator; `cargo test
--lib` runs them at 256 cases per property. For a longer randomized search,
raise the case count:

```sh
PROPTEST_CASES=100000 cargo test --lib property_tests
```

Coverage-guided (libFuzzer) fuzz targets for the parsers that ingest
untrusted input live under [fuzz/](fuzz/README.md); the weekly `Fuzz` CI
workflow runs each target with a cached corpus.

Dependency advisories, license policy, and source provenance are enforced by
`cargo deny check` (policy in [deny.toml](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/deny.toml)); run it locally with
[cargo-deny](https://github.com/EmbarkStudios/cargo-deny) installed.

## Before pushing

All three must pass, unmodified and unfiltered:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --release --tests
```

## Conventions

- Conventional-commit prefixes on commit messages and PR titles (`feat:`,
  `fix:`, `docs:`, `test:`, `ci:`, `chore:`). The CHANGELOG is hand-written, not
  generated, but the prefix groups the change and is user-visible in PR history,
  so choose it deliberately.
- Work on a feature branch and open a pull request; PRs that change behavior
  include tests that pin the new behavior and SPEC.md updates in the same PR.
- Integration tests must converge through polling (`wait_until` in
  `tests/common`), never assert after a bare sleep.
- Prose style, everywhere including docs and PR bodies: factual, no marketing
  language, no em dashes.
- Dependency updates for GitHub Actions and crates arrive as weekly dependabot
  PRs. The `redis-module`/`redis-module-macros` git-tag pin in Cargo.toml is
  excluded from that automation and is bumped by hand (see the pin policy
  below).

### Source layout

`src/lib.rs` holds only the module wiring: the `redis_module!` registration, the
version encoding, `init`, and `deinit`. The rest is split by concern (#86), each
file owning its own statics, helpers, and unit tests, mirroring the `tests/`
partition so a change to one concern touches one file:

- `src/config.rs`: config value types and statics, the event/key/source-db
  filter and prefix/auto-group grammars and validators, and the `enabled`
  on-changed callback.
- `src/capture.rs`: the capture hot path (sanitization, entry-format encoding,
  the mirrored `XADD` writers, the keyspace-notification callback, and the
  flush/SWAPDB handlers).
- `src/cluster.rs`: per-node cluster mode (slot-pinned hash-tag selection, the
  CRC16 slot math and Redis 7.2 fallback table, migration classification, and
  re-pin-and-retry).
- `src/markers.rs`: the gap-marker queue and its deferred write to the
  `#control` stream, plus the FFI panic guards.
- `src/stats.rs`: the counters and per-stream records behind `INFO eventstream`
  and the `WITHSTATS` join, plus the drop-counting helpers.
- `src/commands.rs`: the `EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`, and
  `EVENTSTREAM.PRUNE` command handlers.

Cross-module items are `pub(crate)`; the split changes no runtime behavior.

### redis-module pin policy

`redis-module`/`redis-module-macros` are pinned to a redismodule-rs git tag
(`v2.1.3`), not a crates.io version, because the crates.io releases lag the repo
badly and Redis's own modules consume the git tags (Cargo.toml comment; SPEC.md
section 1). The pin also carries live workarounds for upstream defects — the
null-pointer guard on the raw keyspace callback's event-name pointer, and the
`catch_unwind` wrappers around handler bodies (including the non-UTF-8
event-name panic, RedisLabsModules/redismodule-rs#472) — so it is never bumped
blindly. Standing policy:

- **Re-verify cadence.** At each release prep, check whether redismodule-rs has
  tagged a release newer than the pin. (Optional low-noise automation, not yet
  added: a scheduled workflow that opens an issue when a newer tag appears.)
- **Bump gate.** Every pin bump must pass the full CI integration matrix
  (SPEC.md section 14) *and* a re-check that each workaround above is still
  needed against the new tag — a fixed defect means the workaround becomes
  removable, a regressed one means the bump is unsafe. A green matrix alone is
  not sufficient; the workaround review is manual.
- **Unpin trigger (git tag → crates.io).** Switch off the git tag only once a
  crates.io `redis-module` release covers everything this crate consumes: the
  `min-redis-compatibility-version-7-2` feature, the
  `module_args_as_configuration` configuration API (all config types plus the
  `enum` block), the raw keyspace-notification subscription surface, and
  `info_command_handler`. Unpinning is also the precondition that would unblock
  an automated release-plz flow (release-plz.toml).
- **Publishing this crate to crates.io stays disabled** (`publish = false`): it
  is a cdylib loaded as a compiled artifact, not a Rust library dependency
  (release-plz.toml). This is settled; revisit only if the crate grows a
  reusable library surface.

## Releasing

Releases are two manual steps plus one automated step:

1. Open a "chore: release prep for vX.Y.Z" PR that bumps `version` in
   `Cargo.toml` and adds a `## [X.Y.Z]` section at the top of `CHANGELOG.md`.
2. Merge it to main.
3. Tag the merged commit and push the tag:

   ```sh
   git tag vX.Y.Z && git push origin vX.Y.Z
   ```

The tag push triggers `.github/workflows/release.yml`, which verifies that the
tag, `Cargo.toml`, and the top CHANGELOG section all agree, creates the GitHub
release from that CHANGELOG section, then builds and attaches the prebuilt
`.so`/`.dylib` artifacts (linux-x86_64, linux-aarch64, macos-aarch64,
macos-x86_64) with sha256 checksums and Sigstore build-provenance attestations.
release-plz does not release this crate; see `release-plz.toml` for why.

## Reporting problems

Use the issue templates. For bugs, the `INFO eventstream` counters and the
exact `loadmodule` line are usually the difference between a one-round-trip
fix and a guessing game. For security reports, see [SECURITY.md](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/SECURITY.md).
