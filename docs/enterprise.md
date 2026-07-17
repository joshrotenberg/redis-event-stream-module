# Redis Enterprise

Redis Enterprise Software loads modules from a RAMP bundle, not a bare `.so`. A
RAMP bundle is a zip containing the shared object plus a generated `module.json`
manifest; the cluster validates that manifest at upload time. A
`redis-event-stream-module-<version>-linux-x86_64.zip` bundle is attached to
each GitHub release, built from [`ramp.yml`](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/ramp.yml)
with [`ramp-packer`](https://pypi.org/project/ramp-packer/) (the reference tool
Redis's own modules use).

## Scope

- **Self-managed Redis Enterprise Software only.** Redis Cloud does not accept
  uncertified custom modules; this bundle does not make the module deployable
  on Cloud.
- **Enterprise sharding is not OSS cluster mode.** A sharded Enterprise
  database places each shard behind the proxy as an ordinary non-clustered
  Redis process, so `ContextFlags::CLUSTER` is not set, the default
  `eventstream.cluster-streams=refuse` gate does not trip, and the per-node
  capture path (SPEC.md section 10) does not apply. Each shard mirrors its own
  events into shard-local streams, so a multi-shard database exposes per-shard
  streams behind one endpoint. A consumer must fan out across shards to see
  every event (the [cluster consumers](./cluster-consumers.md) fan-out-and-merge
  approach applies). Databases created with the OSS Cluster API enabled are a
  separate case and are not yet validated.
- **Minimum Redis 7.2.** The manifest declares `min_redis_version: 7.2`
  (SPEC.md section 14). On a pre-7.2 core the load is a process abort inside the
  wrapper's registration path; the RAMP metadata front-runs that with a clean
  refusal at upload time.

## Upload

Through the cluster REST API (the UI upload does the same thing):

```sh
curl -k -u "<user>:<password>" -F "module=@redis-event-stream-module-<version>-linux-x86_64.zip" \
  https://<cluster>:9443/v1/modules
```

Then create a database with the module enabled. IMMUTABLE configs
(`eventstream.stream-prefix`, `eventstream.cluster-streams`) can only be set as
load-time module arguments, supplied through Enterprise's module-args field at
database-create time; the mutable configs (`eventstream.enabled`,
`eventstream.events`, `eventstream.maxlen`, `eventstream.firehose`) can also be
changed per-database with `CONFIG SET`.

## Verifying capture

After the database is up, the basic capture flow is the same as OSS:

```
> SET foo bar PX 100
> GET foo            (after ~100ms; forces lazy expiry)
> XREAD COUNT 10 STREAMS events:expired 0
> EVENTSTREAM.STATS
> EVENTSTREAM.STREAMS
> INFO eventstream
```

Validate this against both a single-shard and a multi-shard database; on
multi-shard, confirm the per-shard stream semantics above.

## Building the bundle locally

The maintainer's `redis-up` Enterprise tooling (`start_enterprise`) makes local
validation practical. To rebuild the bundle from source:

```sh
pip install ramp-packer          # needs a redis-server 7.2+ on PATH
cargo build --release --lib
make ramp                        # -> dist/redis-event-stream-module-<version>-linux-x86_64.zip
```

`ramp pack` loads the module into a throwaway `redis-server` to enumerate the
registered commands (`EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`) and the module
version, so the `redis-server` on `PATH` must be 7.2 or newer.
