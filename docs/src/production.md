# Production checklist

Use this checklist after the Quickstart and before making captured events part
of an application workflow.

## Confirm compatibility

- Run Redis 7.2 or newer.
- Confirm the loaded version with `MODULE LIST`.
- Start with standalone Redis or ordinary replication/failover.
- Treat OSS Cluster per-node mode and multi-shard Redis Enterprise as Preview.
- Test the exact Redis version and topology used in production.

Prebuilt release artifacts include checksums and Sigstore build-provenance
attestations. Verify both when downloading a module binary:

```bash
shasum -a 256 -c redis-event-stream-module-<version>-linux-x86_64.so.sha256
gh attestation verify \
  redis-event-stream-module-<version>-linux-x86_64.so \
  --repo joshrotenberg/redis-event-stream-module
```

## Set the durability policy

Stream entries inherit Redis persistence and replication. AOF `everysec` is the
recommended minimum for most durable-work use cases. Record the accepted crash
window explicitly; the module cannot make Redis state more durable than the
server configuration.

Replication is asynchronous. Test failover with realistic write pressure and
decide how the application handles an entry that had not reached the promoted
replica.

## Size memory and retention

Estimate each event stream independently:

```text
entries required = peak events/second × maximum consumer outage
memory required  ≈ entries required × average encoded entry size
```

A fixed-format entry with a 32-byte key is roughly 150 bytes before allocator
and stream-node overhead. Measure with representative keys and `MEMORY USAGE`.

Prefer `noeviction` or a `volatile-*` maxmemory policy. An `allkeys-*` policy
can evict the event streams themselves; the module exposes
`eventstream_eviction_risk` when it detects that configuration.

Keep `eventstream.verify-oom yes` unless the workload explicitly prefers
continued capture over memory-limit enforcement. Turning it off lets module
writes add memory while Redis is already evicting.

## Restrict access

The module writes with server privileges. A client that changes a watched key
can indirectly cause an event-stream write even if that client cannot access
the stream key.

A consumer needs only the stream keys and selected read commands:

```text
ACL SETUSER events-consumer on >secret \
  ~events:* \
  +xread +xreadgroup +xack +xautoclaim +xinfo +xlen \
  +eventstream.stats +eventstream.streams
```

Grant `EVENTSTREAM.PRUNE` only to an operator role. The Redis 7.4+
`@eventstream` category includes that write command and can include future
module commands, so it is broader than the two explicit read-only grants.

Protect Redis itself with network controls, authentication, and TLS as
appropriate. The Live observatory's `LOCAL_DEMO=true` configuration is for
local disposable instances only.

## Run the preflight check

The repository includes a deployment check:

```bash
./demo-preflight.sh -h <host> -p <port>
```

It verifies reachability, module presence, effective configuration, an
end-to-end probe expiration, stream discovery, and the main counters. All
arguments pass through to `redis-cli`, including authentication and TLS flags.

Run it on every master in per-node cluster mode.

## Verify the operational signals

Before accepting traffic:

```text
CONFIG GET eventstream.*
EVENTSTREAM.STATS
EVENTSTREAM.STREAMS VERBOSE
INFO eventstream
```

Confirm:

- capture is enabled;
- the event and key filters match the intended workload;
- the stream prefix and cluster mode match consumer configuration;
- `events_lost`, `dropped`, and `handler_panics` are zero;
- the expected streams appear after a probe;
- monitoring can read the module INFO section;
- consumer lag alerts fire before retention is exhausted.

## Exercise failure paths

Test the recovery policy, not only the happy path:

- pause and resume capture;
- restart or fail over Redis;
- stop a consumer longer than a normal deployment;
- approach the memory limit;
- flush a disposable instance;
- reshard and fail over a Preview cluster;
- verify that gap markers, counters, and alerts tell the same story.

The [reliability guide](reliability.md) explains what each window can recover.
