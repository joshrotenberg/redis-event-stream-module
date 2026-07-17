# Scripted demo

`demo.sh` in the repo root exercises the module end to end against a throwaway
local server. It builds the module, starts a `redis-server` on port 6399 with
the filter widened to `expired,set`, then walks through the full path: set a
key with a short TTL, force its lazy expiry, and read both the mirrored
`expired` event and the `set` event back out of their streams, finishing with
the INFO counters.

```sh
./demo.sh
```

The script runs from a repo checkout: it builds `target/release` first
(`cargo build --release`), so it needs the Rust toolchain in addition to
`redis-server` (7.2+) and `redis-cli` on `PATH`. It cleans up the server on
exit. `notify-keyspace-events` is deliberately left unset, demonstrating that
module capture does not depend on it.

To validate an existing deployment instead of a throwaway server, see
[Preflight checks](./preflight.md).
