# Preflight checks

`demo-preflight.sh` in the repo root validates a running server rather than
starting one: reachability, module presence, configuration, an end-to-end
probe expiration, stream discovery, and the counters. It exits nonzero on the
first failure, so it works as a deployment gate.

```sh
./demo-preflight.sh -h <host> -p <port>
```

All arguments pass through to `redis-cli`, so TLS and auth flags work the same
way (for example `-a <password>` or `--tls`).

The script ships in the repo root, so running it requires a repo checkout, but
it needs only `redis-cli` on `PATH`: it builds nothing and loads nothing on
the target server. It is read-mostly, writing one probe key with a short TTL
to confirm capture end to end.
