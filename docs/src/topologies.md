# Deployment topologies

The module runs inside each Redis process, so topology determines where capture
happens and how consumers find the resulting streams.

## Support at a glance

| Topology | Status | Consumer shape |
|---|---|---|
| Standalone | Stable | One stream per event |
| Replication and failover | Stable | Read the current primary; entries replicate |
| OSS Cluster per-node mode | Preview | Discover and merge streams from every master |
| Redis Enterprise, one shard | Preview deployment packaging | One ordinary Redis process behind the proxy |
| Redis Enterprise, multiple shards | Preview | Discover and merge shard-local streams |

The final 1.0 stability contract is tracked in
[issue #114](https://github.com/joshrotenberg/redis-event-stream-module/issues/114).

## Standalone

Standalone Redis is the simplest and primary target. Every captured event type
has one destination stream under the configured prefix, and all destinations
live in database 0:

```text
events:expired
events:set
events:#control
events:#streams
```

Consumers connect to the same Redis endpoint and use ordinary Streams
commands.

## Replication and failover

Only the master captures. Mirrored entries and control markers replicate like
normal Redis writes, so replicas do not independently create duplicates.

After promotion, the new master begins capture. Any entry that had not reached
the replica before failover is lost according to normal asynchronous
replication semantics. Stream names remain unchanged.

## OSS Cluster

The module refuses to load in cluster mode by default. Enable Preview per-node
capture at load time:

```bash
redis-server \
  --cluster-enabled yes \
  --loadmodule /path/to/libredis_event_stream_module.so \
  cluster-streams per-node
```

A module callback cannot follow a `MOVED` response while Redis is handling the
source event. Each master therefore pins its module-owned keys to a hash slot
that it owns:

```text
events:{06S}expired
events:{06S}set
events:{06S}#control
events:{06S}#streams
```

Every master uses a different tag. The result is one stream per
`(master, event type)` pair.

### Consumer requirements

A complete cluster consumer:

1. Enumerates the current masters.
2. Connects directly to each master.
3. Calls `EVENTSTREAM.STREAMS` on each node.
4. Unions the returned names.
5. Reads the tagged streams through a cluster-aware client.
6. Refreshes discovery after topology changes.

The shipped `eventstream-client` implements that fan-out and merged read. A
merged stream-ID order is useful for display, but entries from different
masters have no global total order.

### Resharding

If a pinned slot moves away, the next affected write causes the node to choose
a new owned slot, write a `repinned` marker, and retry once. Existing streams
move with their old slot and remain addressable by name. Consumers must retain
historical names and discover the new tag.

An event still refused during the migration retry is counted in
`dropped_migrating` and `events_lost`. Single-shard clusters avoid cross-master
fan-out and pinned-slot resharding, making them the lowest-risk cluster shape.

### Failover

A promoted replica inherits the failed master's slots and replicated streams.
Tag selection is deterministic for the owned-slot set, so it normally resumes
the same names without double capture.

## Redis Enterprise

Self-managed Redis Enterprise Software loads a RAMP bundle rather than a bare
shared library. Release artifacts include a
`redis-event-stream-module-<version>-linux-x86_64.zip` bundle.

Redis Cloud does not accept arbitrary uncertified custom modules.

Enterprise sharding is not OSS Cluster mode. Each shard appears to the module
as an ordinary Redis process behind the proxy, so the OSS
`cluster-streams=refuse` gate does not apply. A multi-shard database still
produces shard-local streams, and a complete consumer must discover every
shard rather than assuming one logical proxy endpoint owns one stream.

Validate Enterprise packaging, module arguments, failover, discovery, and
consumer routing against the exact target release before production use.

## Explore the topology

The [Live observatory](observatory.md) can replace its standalone Redis with a
real three-master cluster. It displays command routing, node-local stream
counts, pinned tags, and the merged event lanes without requiring a manual
cluster setup.
