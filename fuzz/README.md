# Fuzzing

Coverage-guided (libFuzzer) fuzz targets for the pure functions that ingest
untrusted input (issue #131), complementing the property tests from #94:

| Target | Function | Source |
|---|---|---|
| `parse_filter` | `eventstream.events` grammar parser | SPEC.md §7 |
| `validate_prefix` | `eventstream.stream-prefix` validator | SPEC.md §7 |
| `sanitize` | event-name → stream-suffix sanitizer | SPEC.md §5 |

Each must never panic — a parser fed hostile `CONFIG SET` values or a sanitizer
fed a hostile co-loaded-module event name may only return `Err` or a safe
string. The targets link the module as an `rlib` through its `fuzzing` feature,
which compiles out the `redis_module!` macro (and its Redis-only global
allocator) so the harness runs as an ordinary binary.

## Running

Requires a nightly toolchain and `cargo-fuzz`:

```sh
cargo install cargo-fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run parse_filter          # runs until Ctrl-C
cargo +nightly fuzz run sanitize -- -max_total_time=60
```

A crash writes a reproducer to `fuzz/artifacts/<target>/`; replay it with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`. The weekly
`Fuzz` CI workflow runs each target for a bounded time with a cached corpus.
