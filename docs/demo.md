# Demo and preflight

Two scripts in the repo root exercise the module end to end — one against a
throwaway local server, one against an existing deployment.

## `demo.sh` — scripted local demo

Builds the module, starts a throwaway `redis-server` on port 6399 with the
filter widened to `expired,set`, then walks through the full path: set a key
with a short TTL, force its lazy expiry, and read both the mirrored `expired`
event and the `set` event back out of their streams, finishing with the INFO
counters.

```sh
./demo.sh
```

Requires `redis-server` (7.2+) and `redis-cli` on `PATH`. It builds
`target/release` first (`cargo build --release`) and cleans up the server on
exit. `notify-keyspace-events` is deliberately left unset, demonstrating that
module capture does not depend on it.

## `demo-preflight.sh` — check an existing deployment

Validates a running server rather than starting one: reachability, module
presence, configuration, an end-to-end probe expiration, stream discovery, and
the counters. It exits nonzero on the first failure, so it works as a
deployment gate.

```sh
./demo-preflight.sh -h <host> -p <port>
```

All arguments pass through to `redis-cli`, so TLS and auth flags work the same
way (for example `-a <password>` or `--tls`).
