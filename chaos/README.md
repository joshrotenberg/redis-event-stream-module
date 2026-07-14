# Chaos and load scenarios

Heavier-than-CI scenarios for the cluster capture paths (SPEC.md section 10,
issues #45/#46/#47). Each one stands up real servers, drives tens of thousands
of events through the [example client](../examples/eventstream_client.rs),
injects a topology change or failure, and asserts the data-safety property that
must hold. These run for a few minutes and spin up multi-node clusters, so they
live here rather than in `cargo test`.

## Run

```sh
chaos/run.sh              # all scenarios
chaos/run.sh reshard      # one scenario (reshard | failover | massexpiry | repeated)
```

Requires `redis-server` (7.2+) and `redis-cli` on `PATH`; override with
`REDIS_SERVER_BIN` / `REDIS_CLI_BIN` to pin a specific build. The module and the
example client are built in release automatically. The script exits nonzero if
any assertion fails.

## CI

The suite runs on a weekly schedule (and on demand) via
[`.github/workflows/chaos.yml`](../.github/workflows/chaos.yml), not on pull
requests: its multi-minute runtime and the timing sensitivity of `failover`
would slow and flake the PR gate. The workflow builds one server (the newest
line in the CI matrix) from source, then runs the three deterministic scenarios
(`reshard`, `massexpiry`, `repeated`) as a gating step and `failover` as a
separate step retried once, so a single replica-promotion timing flake does not
fail the run while two consecutive failures still do. A failed scheduled run
opens (or comments on) a `kind:bug`/`area:ci` issue and uploads the per-node
`redis.log` / `soak.log` as an artifact. `workflow_dispatch` takes an optional
`scenario` input to run a single scenario.

## Scenarios

| Scenario | What it does | Asserts |
|---|---|---|
| `reshard` | 40k events through a live slot migration | zero loss, one clean re-pin (#46) |
| `failover` | kills a master mid-run | the promoted replica re-derives the same tag, so the stream name is stable and nothing double-captures (#47) |
| `massexpiry` | 50k expirations, the heaviest capture path | every expiration captured, zero drops |
| `repeated` | migrates a node's pinned slot several times in a row | one clean re-pin per migration, capture continues throughout (#46) |

Each scenario cleans up its servers on exit.
