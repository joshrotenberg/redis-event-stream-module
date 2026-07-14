# Contributing

Contributions are welcome. This project is spec-first: [SPEC.md](SPEC.md) is
the authoritative design, and behavior changes should update it in the same
pull request. If a change needs a design decision first, open a decision issue
(there is a template) rather than encoding the decision silently in code.

## Building and testing

Requirements: stable Rust, and `redis-server`/`redis-cli` 7.2 or newer on PATH
(Valkey 8.x works too). The integration tests spawn real servers; nothing is
mocked.

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

CI runs the full suite against pinned Redis 7.2, 7.4, 8.x, Valkey 8.x, and
Valkey 9.x, so a change must hold across that matrix, not just your local
server.

The unit tests include property tests (proptest) over the events filter
grammar, the event-name sanitizer, and the prefix validator; `cargo test
--lib` runs them at 256 cases per property. For a longer randomized search,
raise the case count:

```sh
PROPTEST_CASES=100000 cargo test --lib property_tests
```

There is no coverage-guided fuzzing (cargo-fuzz) target; adding one would
require an `rlib` crate type alongside the `cdylib` (issue #131).

Dependency advisories, license policy, and source provenance are enforced by
`cargo deny check` (policy in [deny.toml](deny.toml)); run it locally with
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
  `fix:`, `docs:`, `test:`, `ci:`, `chore:`). Releases and the changelog are
  generated from them by release-plz, so the prefix you choose is
  user-visible.
- Work on a feature branch and open a pull request; PRs that change behavior
  include tests that pin the new behavior and SPEC.md updates in the same PR.
- Integration tests must converge through polling (`wait_until` in
  `tests/common`), never assert after a bare sleep.
- Prose style, everywhere including docs and PR bodies: factual, no marketing
  language, no em dashes.
- Dependency updates for GitHub Actions and crates arrive as weekly dependabot
  PRs. The `redis-module`/`redis-module-macros` git-tag pin in Cargo.toml is
  excluded from that automation and is bumped by hand when
  RedisLabsModules/redismodule-rs tags a new release.

## Reporting problems

Use the issue templates. For bugs, the `INFO eventstream` counters and the
exact `loadmodule` line are usually the difference between a one-round-trip
fix and a guessing game. For security reports, see [SECURITY.md](SECURITY.md).
