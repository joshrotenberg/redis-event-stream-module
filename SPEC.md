# SPEC: redis-event-stream-module

## 1. Summary

`redis-event-stream-module` is a Redis module, written in Rust on the `redis-module` crate (redismodule-rs 2.0.8), that subscribes to keyspace notifications inside the server and mirrors each selected notification as an `XADD` into a Redis Stream. Keyspace notifications over pub/sub are fire-and-forget: a disconnected subscriber misses events permanently. This module makes those events durable, replayable, and consumable through consumer groups, using only standard Redis Streams on the read side. The originating use case, and the v0.1 default configuration, is reliable capture of key expiration events (`expired`) for consumers that must not miss one across restarts.

## 2. Goals and non-goals

### Goals

- Mirror keyspace notifications into Redis Streams, atomically with the triggering keyspace change, on the same node.
- Per-event-name routing: one stream per event name (`events:expired`, `events:hset`), so consumers subscribe at exactly the granularity they filter at.
- Config-driven behavior (enable switch, event filter, retention cap) settable at load and, where safe, live via `CONFIG SET`.
- At-least-once delivery to consumers within a bounded retention window, using plain `XREADGROUP`/`XACK` with no custom read commands.
- Zero overhead worth measuring when loaded but capturing nothing beyond the configured filter.

### Non-goals

- Exactly-once delivery. Consumers must be idempotent (natural key: stream name plus entry ID).
- Backfill. Events that occur while the module is unloaded, disabled, or the server is down are not recoverable. This is a live mirror, not a write-ahead log.
- Capturing key values or payloads. The notification API delivers only the event name and key; for `expired` the value is already gone.
- Cluster support in v0.1 (see section 10).
- Capturing `MISSED`, `NEW`, `LOADED`, or `TRIMMED` class events in v0.1 (outside `REDISMODULE_NOTIFY_ALL`, see section 5).

## 3. Prior art

No existing module provides this. RedisGears / Triggers-and-Functions could script equivalent behavior but is deprecated by Redis. `RedisLabs/rmnotify` is an adjacent C helper library for firing notifications, not a forwarder. The thing this replaces is application-side pub/sub subscribers on `__keyevent@*__` channels, which lose events whenever the subscriber is disconnected. An earlier proof of concept (`joshrotenberg/redis-keyspace-stream`) routed per notification class; this design supersedes it with per-event-name routing.

## 4. Architecture overview

All work happens on the main thread, inside the execution unit of the command that caused the notification.

```
 command (SET/DEL/expire cycle/...)
        |
        v
 keyspace notification fires
        |
        v
 +---------------------------- notification callback (no writes allowed) ---+
 | 1. enabled == yes?                 no -> return                          |
 | 2. key starts with stream-prefix?  yes -> skipped_self++, return  (guard)|
 | 3. MASTER flag set, LOADING clear? no -> return                          |
 | 4. event matches filter?           no -> skipped_filtered++, return      |
 | 5. sanitize(event) non-empty?      no -> skipped_invalid++, return       |
 | 6. capture db index (raw RedisModule_GetSelectedDb)                      |
 | 7. ctx.add_post_notification_job(closure)                                |
 +---------------------------------------------------------------------------+
        |
        v   (runs atomically alongside the notification, writes now safe)
 post-notification job:
   ctx.call_ext("XADD",
     [<prefix><event>, "MAXLEN", "~", <maxlen>, "*",
      "event", <raw event>, "key", <key bytes>, "db", <db>],
     CallOptions{ replicate('!'), errors_as_replies('E'), verify_oom('M') })
        |
        +-- ok  -> forwarded++, entry replicated to replicas and AOF
        +-- err -> dropped_* counter++, first-failure log line
```

The deferred write is mandatory: writing to the keyspace inside a notification callback is unsafe. `Context::add_post_notification_job` (wrapping `RedisModule_AddPostNotificationJob`, Redis 7.2+) runs the closure when writes are safe, atomically alongside the notification. Redis makes no attempt to protect modules from notification-to-job feedback loops, so the prefix guard in step 2 is a correctness requirement, not an optimization: the module's own `XADD` (and any resulting `xtrim`) fires notifications on `<prefix>*` keys, which the guard drops.

## 5. Event routing

### Stream naming

```
destination = <stream-prefix> + sanitize(<event-name>)
```

`stream-prefix` defaults to `events:`. Examples:

| Trigger | Event class | Event name | Destination stream |
|---|---|---|---|
| key TTL removal | EXPIRED | `expired` | `events:expired` |
| maxmemory eviction | EVICTED | `evicted` | `events:evicted` |
| `SET foo bar` | STRING | `set` | `events:set` |
| `DEL foo` | GENERIC | `del` | `events:del` |
| `HSET h f v` | HASH | `hset` | `events:hset` |
| `XGROUP CREATE ...` | STREAM | `xgroup-create` | `events:xgroup-create` |
| RedisJSON `JSON.SET` | MODULE | `json.set` | `events:json.set` |

### Event name universe

| Source | Event names | Notes |
|---|---|---|
| Command-generated | `set`, `del`, `hset`, `lpush`, `sadd`, `zadd`, `xadd`, `rename_from`, `rename_to`, ... | Fixed set defined by Redis, roughly 60 to 80 names in 7.x. Lowercase ASCII plus `_` and `-`. |
| Expiration / eviction | `expired`, `evicted` | `expired` fires when Redis actually removes the key (lazy access or active expire cycle), not at the nominal TTL instant. |
| Module-defined | Arbitrary strings via `RM_NotifyKeyspaceEvent`, e.g. `json.set` | The only unbounded source. Any co-loaded module can fire any name under any class within `NOTIFY_ALL` (redismodule-rs's own `examples/events.rs` fires `events.send` under GENERIC), so excluding the MODULE class does not bound custom names. The real bounds are the 128-byte sanitized-name cap and per-stream `maxlen` trimming; total memory grows with distinct names, not event volume. |

Not capturable in v0.1: `keymiss` (MISSED), `new` (NEW, 7.0.1+), `loaded` (LOADED), and TRIMMED-class events. `REDISMODULE_NOTIFY_ALL` excludes all four (verified at `src/include/redismodule.h:260`: ALL is GENERIC|STRING|LIST|SET|HASH|ZSET|EXPIRED|EVICTED|STREAM|MODULE), and the wrapper's `redis_event_handler!` intersects any requested mask with `RedisModule_GetKeyspaceNotificationFlagsAll()`, which is the server's own NOTIFY_ALL, so requesting them through the `event_handlers:` macro silently strips them. Capturing them later requires calling `raw::RedisModule_SubscribeToKeyspaceEvents` directly (see Future work).

Byte-level guarantees: `RM_NotifyKeyspaceEvent` takes a C string, so event names cannot contain NUL. The wrapper's generated callback converts with `CStr::from_ptr(...).to_str().unwrap()` (`src/macros.rs`), so by the time the handler runs the name is valid UTF-8. A non-UTF-8 event name from a hostile module would panic inside the wrapper-generated handler before this module's code executes; this is a redismodule-rs limitation (see Open questions).

### Sanitization

`sanitize()` maps the event name to the stream key suffix:

1. Characters in `A-Z a-z 0-9 _ . : -` pass through unchanged. Every built-in event name and every known module event name (dotted names included) passes through byte-identical.
2. Any other character becomes a single `_`.
3. Result truncated to 128 bytes (pure ASCII after step 2, so no boundary issues).
4. An empty result is not routed; the notification is dropped and `skipped_invalid` is incremented.

Two distinct raw names can collide after sanitization (`foo bar` and `foo?bar` both become `foo_bar`). This is accepted because every entry carries the raw event name in its `event` field (section 6), so consumers can always distinguish.

`#` is deliberately outside the sanitizer output alphabet, so the `<prefix>#...` namespace remains reserved for internal module keys in future versions without any possibility of collision from event names.

Escaping the prefix is impossible by construction: the destination is plain concatenation of a validated prefix and a sanitized suffix. There is no parsing step an event string could exploit.

### Discovery

v0.1 discovery is deterministic naming. With the default configuration the only stream is `events:expired`. For wider filters, the documented fallback is:

```
SCAN 0 MATCH events:* TYPE stream
```

(The prefix validation rules in section 7 reject glob metacharacters precisely so this pattern never needs escaping.) A persistent registry set and an `EVENTSTREAM.STREAMS` command are deferred to Future work.

### Namespace ownership

Keys under `<stream-prefix>` belong to the module. If a user key already exists at a destination name: a non-stream key causes `WRONGTYPE` errors (entries dropped and counted, the module never deletes or overwrites a non-stream key); a pre-existing stream will receive module entries and be trimmed under the module's `maxlen` policy. Deployment docs recommend restricting write access to `<prefix>*` via ACLs.

## 6. Entry schema

v0.1 ships exactly one fixed entry format. Fields are always emitted in the same order, because Redis stream listpack nodes store field names once per node when consecutive entries share the field set (the `SAMEFIELDS` optimization), so a stable schema keeps per-entry overhead near the payload size.

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | `event` | raw event name, pre-sanitization, e.g. `expired`, `hset` | Disambiguates sanitizer collisions and keeps entries self-contained if forwarded elsewhere |
| 2 | `key` | raw key bytes | Exact bytes of the affected key, no encoding, no escaping |
| 3 | `db` | decimal string, e.g. `"0"` | Database index where the event fired |

There is deliberately no timestamp field: the auto-generated entry ID (`<ms>-<seq>`) carries a millisecond timestamp assigned at write time, and since the write runs atomically alongside the notification, that is the event time for practical purposes. `XRANGE` by time works natively against it. These three values plus the ID are everything the notification callback receives; there is no value payload, old value, or TTL available at notification time, and the schema does not pretend otherwise.

Binary safety: the wrapper hands the handler the key as `&[u8]`, and `ctx.call_ext` accepts `&[&[u8]]` argument slices (`StrCallArgs` implements `From<&[&T]> for T: AsRef<[u8]>`), so key bytes pass through untouched. Consumers must read `key` with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will mangle non-UTF-8 keys, which is a client configuration issue, not stream data loss.

Database placement: each destination stream lives in the database where the event fired (`events:expired` in db 0 records db 0 expirations, and so on). `RM_AddPostNotificationJob` captures the db id and Redis selects it on the job's context, so the `XADD` lands in the right db with no explicit `SELECT`. The `db` index for the entry field is captured at notification time via the raw `RedisModule_GetSelectedDb` binding (present in `redismodule.h`, no safe wrapper in 2.0.8) and moved into the job closure. In cluster mode only db 0 exists, and cluster is unsupported in v0.1 anyway.

SWAPDB caveat: `SWAPDB` atomically swaps entire keyspaces, including destination streams, so after a swap the streams and their consumer groups describe the other database. The per-entry `db` field remains the historical truth of where each event fired. redismodule-rs 2.0.8 has no safe wrapper for the SwapDB server event, so v0.1 documents this rather than detecting it.

Alternatives considered and rejected for v0.1: JSON-encoded single field (keys are arbitrary bytes and would need base64, and the `SAMEFIELDS` compaction is lost), value capture (unbounded size, impossible for `expired`), per-entry timestamp (duplicates the entry ID), and a minimal/verbose format pair behind a config (mixed-format streams need a discriminator and a second code path with no v0.1 user; deferred).

## 7. Configuration

The module name is `eventstream`; Redis registers module configs as `<module-name>.<key>`, so all keys read `eventstream.<key>`. This is the single authoritative table; every name and default elsewhere in this document matches it.

| Key | Type | Default | Live-settable | Validation |
|---|---|---|---|---|
| `eventstream.enabled` | bool | `yes` | yes | `yes` / `no` |
| `eventstream.stream-prefix` | string | `events:` | no (IMMUTABLE) | non-empty; at most 128 bytes; characters limited to `A-Z a-z 0-9 : . _ - { }`; glob metacharacters (`*`, `?`, `[`, `]`, `\`) rejected |
| `eventstream.events` | string | `expired` | yes | filter grammar below; empty string rejected |
| `eventstream.maxlen` | i64 | `10000` | yes | `0` to `i64::MAX`; `0` disables trimming (range enforced by Redis's numeric config registration) |

**`eventstream.enabled`.** Master kill switch. There is no unsubscribe API for keyspace notifications, so `no` is an early return at the top of the notification handler (one atomic load per event). Flipping back to `yes` does not replay events that occurred while disabled.

**`eventstream.stream-prefix`.** Registered with `ConfigurationFlags::IMMUTABLE`: settable via module args, a `loadmodule` line, redis.conf directive, or `MODULE LOADEX CONFIG`, but not via `CONFIG SET`. Rationale: a runtime-mutable prefix drags in dual-prefix feedback-guard machinery, old-stream cleanup semantics, and registry-reset questions, all for no v0.1 user; relaxing IMMUTABLE to mutable later is non-breaking. An empty prefix is rejected because the feedback guard (skip keys starting with the prefix) would then match every key and blackhole all events. Braces are allowed in the charset; they are reserved for the future cluster design (section 10), not a working cluster recipe in v0.1.

**`eventstream.events`.** Which events to mirror. Default `expired` matches the originating use case and creates exactly one stream; mirroring everything by default would silently add write amplification to any production workload the moment the module loads. Operators widen it deliberately.

**`eventstream.maxlen`.** Per-stream retention cap, applied inline as `XADD ... MAXLEN ~ <n>` on every write. Default 10000 bounds worst-case memory (section 11) while degrading to "recent history" rather than degrading to an outage. Alternative considered: periodic `XTRIM`. Rejected: inline approximate `MAXLEN` achieves the same bound with no extra writes and no timer.

Example:

```
127.0.0.1:6379> CONFIG GET eventstream.*
1) "eventstream.enabled"
2) "yes"
3) "eventstream.stream-prefix"
4) "events:"
5) "eventstream.events"
6) "expired"
7) "eventstream.maxlen"
8) "10000"
```

### Events filter grammar

The subscription mask is fixed at load (the module subscribes to `@ALL`; there is no resubscribe API), so the filter is a module-side predicate evaluated per notification. Class tokens only select; they never change stream naming, which is always per event name.

```
filter := token ( "," token )*
token  := "*" | "@" class | event-name
class  := generic | string | list | set | hash | zset | stream
        | expired | evicted | module
event-name := any non-empty run of characters except "," and whitespace
```

- Whitespace around tokens is trimmed; duplicates ignored.
- `*` matches every delivered event.
- `@class` matches the `NotifyEvent` bitmask the wrapper passes to the handler. The class list above is exactly the classes inside `NOTIFY_ALL`; `missed`, `new`, `loaded`, and `trimmed` are outside it and are not accepted (not capturable in v1).
- A bare token is an exact, case-sensitive byte comparison against the delivered event name. Bare names are not validated against a closed list because the namespace is open (modules can fire custom names).
- Unknown `@class` tokens, empty tokens, and the empty string are rejected at `CONFIG SET` time. To pause the module, use `eventstream.enabled no`; an empty filter is a mistake, not a state.

| Value | Captures |
|---|---|
| `expired` | expirations only, into `events:expired` |
| `expired,evicted` | expirations and evictions |
| `@hash` | every hash-class event, each to its own stream |
| `*` | everything the subscription delivers |

### Validation mechanics

The wrapper's stock `ConfigurationValue` impls never reject beyond UTF-8 conversion, and `on_changed` fires after the value is stored and cannot veto. Rejection is only possible from `ConfigurationValue::set` returning `Err`, which the wrapper surfaces as the `CONFIG SET` error reply (`ConfigrationPrivateData::set_val`, redismodule-rs `src/configuration.rs`). `eventstream.stream-prefix` and `eventstream.events` therefore bind to custom static types implementing `ConfigurationValue<RedisString>`: `set()` parses and validates, storing both the raw string (for `CONFIG GET`) and the parsed form (class bitmask plus name set) behind a `RedisGILGuard`, which the notification handler (always run with the GIL held) reads without extra locking.

```
127.0.0.1:6379> CONFIG SET eventstream.events "expired,@hsah"
(error) ERR CONFIG SET failed - unknown event class '@hsah'
```

### Load-time args

Precedence at load, lowest first: compiled default; unprefixed module args (`loadmodule .../libredis_event_stream_module.so events "expired,evicted" maxlen 50000`, enabled by `module_args_as_configuration: true`); prefixed standard config sources (`eventstream.events` directive in redis.conf, or `MODULE LOADEX ... CONFIG eventstream.events ...`, applied by `RedisModule_LoadConfigs` after registration); then `CONFIG SET` at runtime for mutable keys. `CONFIG REWRITE` persists current values.

Operator quirks to document: bool module args are true only for the literal string `yes` (anything else silently parses as false, `get_bool_default_config_value`), and a malformed module-arg value aborts module load with a logged error. Implementation note: the macro grammar makes each config-type block optional; if the expansion with `module_args_as_configuration` requires all four type lists, register an empty `enum: []` block (the module has no enum configs in v0.1). The macro's optional `module_config_get`/`module_config_set` convenience commands are not registered; `CONFIG GET/SET eventstream.*` covers the need.

### Interplay with notify-keyspace-events

Module delivery does not depend on `notify-keyspace-events`. Verified against Redis 7.2 `src/notify.c`: `notifyKeyspaceEvent()` calls `moduleNotifyKeyspaceEvent()` before the `server.notify_keyspace_events & type` check, with a source comment noting this deliberately bypasses the notification configuration; the module engine filters only by each subscriber's own mask. With the server default `notify-keyspace-events ""`, this module still captures everything its filter selects. Consequences:

- No warning, error, or auto-set logic keyed on `notify-keyspace-events` exists anywhere in the module.
- The module never calls `CONFIG SET notify-keyspace-events`. Doing so would silently enable pub/sub fan-out for every client on the server and race concurrent `CONFIG SET`s.
- The only load-time intersection the wrapper performs is capability, not configuration: `redis_event_handler!` intersects the requested mask with `RedisModule_GetKeyspaceNotificationFlagsAll()` (classes this server build supports) and logs a notice for anything unsupported.
- An integration test pins this behavior on the minimum supported server (section 15): with `notify-keyspace-events ""`, an expiring key must still produce an entry in `events:expired`.

### Live-change semantics

| Key changed | Next event | Jobs enqueued before the change |
|---|---|---|
| `enabled` to `no` | dropped at handler entry | still execute |
| `events` | new predicate applies | still execute (matched under old filter) |
| `maxlen` | new cap on each `XADD` | old cap; an idle stream is re-trimmed only on its next write |

Since post-notification jobs run atomically within the triggering command and `CONFIG SET` is a separate serialized command, the enqueue-to-execute window never spans a config change in a way that needs special handling. The prefix cannot change at runtime, so the feedback guard always matches the single current prefix.

## 8. Commands

v0.1 registers no commands. Everything that changes behavior goes through `CONFIG SET`; everything observable is exposed through the module INFO section (section 13) and standard Redis commands on the destination streams (`XLEN`, `XRANGE`, `XINFO STREAM`, `XINFO GROUPS`). This keeps one source of truth for behavior, requires no arity or ACL story, and works from redis.conf and orchestration tooling with no module-specific verbs.

`EVENTSTREAM.STATS` and `EVENTSTREAM.STREAMS` (a stats echo command and a stream-discovery command backed by a persistent registry set) are specified in Future work.

## 9. Delivery semantics

| Scope | Guarantee |
|---|---|
| Event to stream entry, module enabled, event matched, node healthy | Exactly one entry, atomic with the triggering change |
| Event to stream entry, overall (restarts, disabled windows, OOM) | At-most-once |
| Stream entry to consumer, `XREADGROUP` + `XACK` | At-least-once within the retention window |
| Stream entry to consumer, plain `XREAD`/`XRANGE` | At-most-once (trimming can outrun the reader) |
| End to end | At-least-once for captured events; exactly-once is not provided and cannot be layered on by this module |

The module cannot be more durable than the Redis server it runs in; every guarantee is bounded by the server's persistence and replication configuration.

### Atomicity

The triggering command, its notification, and the mirrored `XADD` complete within the same execution unit on the same node. No other client can observe the keyspace change (for example the key gone after `expired`) while the stream entry is still pending, except in the loss windows below. One exception: post-notification jobs run at the end of the execution unit, so a later command in the same `MULTI`/`EXEC` or a later `redis.call` in the same Lua script observes the keyspace change before the mirrored entry exists. Scripts that both mutate keys and read the module's streams see pre-event stream state.

### Ordering

- Per stream: entries appear in exactly notification order (single command-execution thread, monotonic IDs), preserved on replicas and through AOF replay because IDs propagate verbatim.
- Per key within one event name: total order.
- Per key across event names: not directly readable as one sequence (`hset k`, `del k`, `expired k` land in three streams). Merging streams by entry ID reconstructs order except for ties within the same millisecond.
- Cross-stream, cross-key: no guarantee beyond entry ID timestamps.

### Loss windows

| Window | Cause | Mitigation |
|---|---|---|
| Module not loaded / `enabled no` | Nothing listens | Load at startup via `loadmodule`; no replay on re-enable |
| Filter mismatch | Event name not selected | By design; counted as `skipped_filtered` |
| `XADD` refused: OOM | With the `M` flag, writes are refused under `maxmemory` | Dropped and counted (`dropped_oom`); deliberate, see section 11 |
| `XADD` failed: `WRONGTYPE` etc. | Non-stream key at the destination name | Dropped and counted (`dropped_xadd_error`); module never deletes the offending key |
| Job scheduling failed | `add_post_notification_job` returned `Status::Err` | Dropped and counted (`dropped_defer_error`) |
| Stream trimming | `MAXLEN` evicts entries before a slow consumer reads them | Bounded, configurable; size `maxlen` for the slowest consumer; loss is detectable (below) |
| Crash before fsync | Server persistence config | `appendfsync everysec` bounds loss to about 1 second (section 10) |
| Failover | Entries not yet replicated to the promoted replica | Standard async replication caveat |
| `FLUSHALL`/`FLUSHDB` | No per-key notifications fire, and the destination streams themselves are deleted | Documented capture gap |

Semantic caveat inherited from Redis: `expired` fires when Redis actually removes the key (lazy access or active expire cycle), not at the nominal TTL instant. The entry ID timestamp is the removal time.

### Slow-consumer contract

1. The module never blocks, delays, or drops writes because a consumer is slow. The keyspace sets the pace.
2. Delivery is at-least-once within the last `maxlen` (approximately) entries per stream. A consumer whose lag exceeds the window loses the overrun permanently.
3. Loss is detectable, not silent: compare the resume ID against the stream's first entry ID, or use `XINFO STREAM` (`entries-added`, `max-deleted-entry-id`) and `XINFO GROUPS` `lag` (Redis 7.0+) to alert before it happens.
4. Pending entries are not protected from trimming. A trimmed unacknowledged entry reads back from the PEL with a nil field list; `XAUTOCLAIM` removes such dead references while scanning. Ack promptly, keep PELs small.

### Consumer patterns

Live tail (replaces pub/sub subscribe):

```
XREAD BLOCK 0 STREAMS events:expired $
# subsequent calls pass the last ID received, not $
```

Consumer group work queue (recommended), end to end for the flagship use case:

```
# once per deployment (MKSTREAM makes this race-free against first capture):
XGROUP CREATE events:expired expiry-workers $ MKSTREAM

# each worker, on startup, drain own leftovers from a previous crash:
XREADGROUP GROUP expiry-workers worker-1 COUNT 100 STREAMS events:expired 0
#   process and XACK each, repeat until empty

# steady state:
XREADGROUP GROUP expiry-workers worker-1 COUNT 32 BLOCK 5000 STREAMS events:expired >
XACK events:expired expiry-workers <id>

# periodically, adopt entries stuck with dead workers:
XAUTOCLAIM events:expired expiry-workers worker-1 60000 0-0 COUNT 32
```

Smoke test:

```
SET session:abc123 x PX 100
# shortly after, the blocked XREADGROUP returns:
1) 1) "events:expired"
   2) 1) 1) "1720512345784-0"
         2) 1) "event"
            2) "expired"
            3) "key"
            4) "session:abc123"
            5) "db"
            6) "0"
```

Replay: `XRANGE events:expired - +`, `XRANGE events:expired <ms> +`, or `XGROUP CREATE ... 0` for a group that must process retained history. Note that `notify-keyspace-events` needs no configuration for any of this (section 7); set it only if pub/sub consumers also need the events during a migration.

## 10. Replication, persistence, and cluster behavior

### Replication and AOF

Every keyspace write the module performs goes through `ctx.call_ext` with one `CallOptions` built once:

```rust
CallOptionsBuilder::new()
    .replicate()          // '!' : propagate to replicas and AOF
    .errors_as_replies()  // 'E' : failures come back as error replies
    .verify_oom()         // 'M' : respect maxmemory
    .build()
```

Plain `ctx.call` uses format `"v"` only and does not replicate or reach the AOF (`src/raw.rs`), so it is never used for keyspace writes. Redis rewrites `XADD <key> ... *` to the concrete generated ID before propagation, so entry IDs are identical on master, replicas, and after AOF replay.

Replica rule: the handler returns early unless `ContextFlags::MASTER` is set. Replicas receive stream content exclusively via replication of the master's `XADD`. Replica-local mirroring was rejected: it diverges from the master dataset, breaks failover consistency, and produces different IDs per node. The replicated `XADD` fires stream events on the replica; the prefix guard drops them, and the master gate makes this a double safety. The handler also returns early when `ContextFlags::LOADING` is set, so AOF replay (which replays the mirrored `XADD`s themselves) cannot double-mirror.

After failover, the promoted replica's MASTER flag flips and it begins mirroring; events acknowledged on the old master but not yet replicated are lost, exactly as for any Redis write.

### Persistence

Destination streams are ordinary keys: included in RDB, AOF, replication, `DUMP`/`RESTORE`. The module has no storage of its own and never calls fsync.

| Server persistence | Worst-case loss on crash |
|---|---|
| AOF, `appendfsync always` | None |
| AOF, `appendfsync everysec` (recommended minimum) | About 1 second of entries |
| RDB only | Everything since the last snapshot |
| None | Everything, on restart |

Eviction warning: `allkeys-*` policies can evict the event streams themselves. Recommend `noeviction` or `volatile-*` on instances running this module.

### Cluster: unsupported in v0.1, refuse to load

Three facts collide in cluster mode: notifications are node-local (every master sees only its own shard of events); the destination stream is a fixed key name, hashing to one slot owned by one master; and `RM_Call` executes locally with no MOVED handling, failing on non-local slots. Net effect with no countermeasure: on an N-master cluster, N-1 masters fail every capture and the remaining one captures only local events. That is silent loss of most traffic.

| Option | Verdict |
|---|---|
| Source-key hashtag (`events:{<key>}:expired`) | Writes always local, but one stream per source key defeats the consolidation model |
| Slot-pinned per-node hashtag (`events:{s1234}:expired`) | Correct, preserves per-event streams per node, but needs topology awareness, re-pinning on reshard, and per-node discovery |
| Refuse to load when `ContextFlags::CLUSTER` is set | No silent loss, no half-working deployments |

Decision: refuse to load in cluster mode, with a clear error at `MODULE LOAD` time. Failing at deploy time beats an incident postmortem. The slot-pinned design is the documented v0.2+ direction; a plain node-id name prefix does not work (it does not change which slot the key hashes to), only a hashtag pinned to a locally owned slot does.

## 11. Performance and memory model

All added work runs on the main thread, synchronously within the triggering command's execution unit.

| Path | Work | Estimated cost |
|---|---|---|
| Not captured (guarded or filtered) | atomic load, prefix memcmp, filter lookup | order of 100 ns; under 1 percent CPU at 100k ops/sec |
| Captured | closure allocation, job registration, one internal `XADD` via `call_ext` with inline `MAXLEN ~` | comparable to the core work of a cheap write command, minus RESP parsing and socket I/O; roughly 40 to 80 percent added CPU per captured cheap write |

Worst case is a workload of nothing but cheap captured writes (pure `SET` with `set` in the filter): throughput can approach half of baseline. Mixed and read-heavy workloads dilute proportionally. The default filter (`expired` only) makes a fresh load capture one event class, so loading the module does not silently amplify a production write workload.

Storm cases:

- Mass expiry is the real storm: a backlog of a million expiring keys becomes a million `XADD`s spread over the drain period, all on the main thread. The server's expire-effort throttling bounds the burst rate; foreground p99 during a drain is the number to watch.
- Multi-key commands (`DEL k1 ... kN`, `MSET`) fire N events in one execution unit; that command's latency grows roughly linearly with N when captured.
- Eviction pressure: `evicted` fires while the server is trying to free memory. The `M` flag (`verify_oom`) makes the module drop and count rather than force `XADD`s past `maxmemory`, which would grow memory exactly when the server is shrinking it. Bounded, counted loss wins.

Trimming is folded into the append (`XADD ... MAXLEN ~ <n>`); approximate trimming only trims at whole listpack-node boundaries, so amortized trim cost is near zero and actual length can overshoot by up to one node (about `stream-node-max-entries`, default 100).

Memory bound: `total ≈ distinct_event_names × maxlen × bytes_per_entry`. A three-field entry with a 32-byte key costs roughly 150 bytes.

| maxlen | Distinct event names | Estimated total |
|---|---|---|
| 10000 (default) | 1 (default filter) | ~1.5 MB |
| 10000 | 20 (typical wide filter) | ~30 MB |
| 10000 | 200 (worst case, all classes plus module names) | ~300 MB |

Measurement plan (one-time, documented in the README, not CI-gated in v0.1): memtier_benchmark, 60 second runs, 3 repetitions, ops/sec and p50/p99: S0 baseline without the module; S1 module loaded with the default filter against a non-expiring SET workload (the tax every non-capturing deployment pays, expected within a few percent of S0); S2 filter `set` for 100 percent capture (expected within the 50 percent budget above). The full matrix (mass-expiry drain p99, maxlen sensitivity, CI thresholds) is Future work.

## 12. Failure modes and mitigations

| Failure | Behavior | Counter | Mitigation / operator action |
|---|---|---|---|
| Feedback loop (module's own `XADD`/`xtrim` events, consumer `xack`/`xclaim` events on `events:*`) | Dropped by prefix guard, first check in the callback | `skipped_self` | None needed; by design |
| Non-stream key at destination | `XADD` returns `WRONGTYPE`, entry dropped | `dropped_xadd_error` | Rename or delete the offending key; restrict `~events:*` writes via ACL |
| `maxmemory` reached | `XADD` refused via `M` flag, entry dropped | `dropped_oom` | Raise `maxmemory`, lower `maxlen`, or narrow the filter |
| Job scheduling failure | Entry dropped | `dropped_defer_error` | Investigate via log; not expected in practice |
| Empty event name after sanitization | Not routed | `skipped_invalid` | None; hostile or buggy co-loaded module |
| Slow consumer | Trimming outruns it; detectable via first-entry ID and `XINFO GROUPS` `lag` | n/a | Alert on lag over ~50 percent of `maxlen`; scale consumers in the group |
| Non-UTF-8 module event name | Panic inside the wrapper-generated handler, before module code runs | n/a | redismodule-rs limitation; see Open questions |
| Cluster mode | Module refuses to load | n/a | Deploy on standalone/replicated topologies |
| Server below 7.2 | Module refuses to load | n/a | Upgrade; see section 14 |
| Events during unload/downtime | Not mirrored, not recoverable | n/a | Documented gap; this is not a write-ahead log |

The module's own writes run server-side with module privileges and are not subject to any client's ACL: a user with no access to `events:*` can still cause writes to those keys by touching watched keys. That is by design (a server-level facility), documented for security review. Consumers need explicit grants, for example `ACL SETUSER consumer on >pw ~events:* +xread +xreadgroup +xack +xautoclaim +xinfo +xlen`.

## 13. Observability

### INFO section

One module INFO section via the wrapper's `InfoContext` builder (`#[info_command_handler]`). Redis prefixes module sections and fields with the module name. All counters are `AtomicU64` statics: process-lifetime, monotonic, reset on load, never persisted or replicated; `skipped_*` counters are incremented inside the notification callback (safe; only keyspace writes are not), `forwarded` and `dropped_*` inside the job.

```
# eventstream_stats
eventstream_enabled:1
eventstream_forwarded:48211
eventstream_dropped:3
eventstream_dropped_xadd_error:3
eventstream_dropped_oom:0
eventstream_dropped_defer_error:0
eventstream_skipped_self:1204
eventstream_skipped_filtered:220
eventstream_skipped_invalid:0
eventstream_active_streams:1
eventstream_last_error_time:1752071011
```

`dropped` is the sum of the three `dropped_*` reasons. `active_streams` counts distinct destination streams written since load. Config values are not duplicated into INFO (`CONFIG GET eventstream.*` covers them), and free-form error text stays in the log, not INFO.

Documentation must state plainly: module sections do not appear in default `INFO` or `INFO all`; use `INFO everything`, `INFO eventstream`, or `INFO eventstream_stats`. This is otherwise a recurring support question.

Alerting guidance:

| Signal | Source | Condition |
|---|---|---|
| `eventstream_dropped` | INFO | any increase |
| `eventstream_enabled` | INFO | 0 when expected 1 |
| `eventstream_forwarded` | INFO | flat while `expired_keys` in `INFO stats` rises (filter misconfigured) |
| Stream size | `XLEN` on `events:*` | unbounded growth (`maxlen` 0 or too high) |
| Consumer lag | `XINFO GROUPS` `lag` | over threshold |

### Logging policy

| Event | Level |
|---|---|
| Module loaded: effective config (prefix, filter, maxlen) | notice |
| `enabled` toggled via `CONFIG SET` | notice |
| First failure per drop reason (`dropped_xadd_error`, `dropped_oom`, `dropped_defer_error`): full error text | warning |
| Subsequent failures | counted in the drop counters, not logged |
| Per-event trace: event, key, destination | debug |
| Final counter values at unload | notice |

Per-stream rate-limited logging with recovery notices (one warning per stream per 60 seconds, suppressed-count summaries) is Future work; the counters never lose data even when the log says nothing.

### Lifecycle

Load: the `redis_module!` `init:` hook runs after commands, configs, and the keyspace subscription are registered; it performs the version and cluster checks (sections 10, 14) and logs the effective config. Unload is supported: the module registers no native data types, so `MODULE UNLOAD` is not refused with EBUSY; Redis removes the subscription and configs; post-notification jobs cannot be pending across an unload (they run atomically with their notification, and `MODULE UNLOAD` is itself a command on the main thread). `deinit` logs final counters and never vetoes.

## 14. Version requirements

The safe deferred-write path requires `RedisModule_AddPostNotificationJob`, mapped to server 7.2.0 in the wrapper's API version table (`redismodule-rs-macros-internals/src/api_versions.rs`).

| Server | Status |
|---|---|
| 8.x, 7.4 | Supported, same code path |
| 7.2 | Minimum supported |
| 7.0 and below | Module refuses to load with an error naming the 7.2 requirement |

The crate builds with the wrapper's `min-redis-compatibility-version-7-2` feature, under which the generated binding unwraps the raw function pointer directly and would panic at capture time on an older server; the guardrail is therefore an explicit `ctx.get_redis_version()` check in `init`, returning an error so `MODULE LOAD` aborts with a clear log line. Alternatives rejected: writing inside the callback on older servers (documented unsafe, loses atomicity) and buffering through a `DetachedContext` background thread (loses atomicity, can drop on crash, adds GIL contention).

## 15. v0.1 scope

The one validated user need is durable expiration events. v0.1 is the smallest module that serves it correctly.

### Ships

- Four configs: `eventstream.enabled`, `eventstream.stream-prefix` (IMMUTABLE), `eventstream.events`, `eventstream.maxlen`, exactly as in section 7.
- Per-event routing `prefix + sanitize(event)` with the sanitizer of section 5.
- One fixed entry format: `event`, `key`, `db` (section 6).
- Deferred `XADD` via `add_post_notification_job`, through `call_ext` with `!`, `E`, `M` flags and inline `MAXLEN ~`.
- Gates: enabled check, prefix feedback guard, MASTER-only, not-LOADING, filter predicate.
- Refuse load on Redis below 7.2 and on cluster.
- Drop/skip counters, one module INFO section, plain logging per section 13.
- Zero custom commands.
- Docs: build and quickstart (below), the expired end-to-end example (section 9), consumer patterns, loss windows.

### Build and quickstart

The crate is a `cdylib`. Build and load:

```
cargo build --release
# produces target/release/libredis_event_stream_module.so (.dylib on macOS)

# redis.conf (Redis 7.2+ required):
loadmodule /path/to/libredis_event_stream_module.so events expired maxlen 10000
```

Smoke test:

```
redis-cli SET session:demo x PX 100
sleep 1
redis-cli XRANGE events:expired - +
```

### Test plan

Integration tests spawn a real redis-server 7.2+ and load the built module (redismodule-rs's own `tests/` directory shows the pattern). Minimum coverage:

- An expiring key produces exactly one entry in `events:expired` with correct `event`, `key`, `db` fields, with `notify-keyspace-events` set to the empty string (pins the bypass behavior of section 7).
- Events not matching the filter are not mirrored.
- Writes to `<prefix>*` keys are never mirrored (feedback guard).
- `MAXLEN` trimming takes effect at the configured cap.
- `eventstream.enabled no` drops events; re-enabling resumes without replay.
- Invalid `CONFIG SET eventstream.events` values are rejected with an error reply.
- Binary (non-UTF-8) key bytes round-trip exactly through the `key` field.
- Load refusal on a pre-7.2 server (or the version check asserted via a mocked version if spawning old servers is impractical).

## 16. Future work

Each item is additive (new config key, counter, command, or entry field), so nothing needs reserving now.

- `EVENTSTREAM.STATS` and `EVENTSTREAM.STREAMS` commands (readonly, fast, keyless).
- Persistent stream registry: a Redis set at `<prefix>#streams`, SADD-ed (replicated) alongside first write, with in-process dedupe cache invalidated on flush via `FlushSubevent`; source of truth for discovery, joined with process-local per-stream counters.
- Firehose stream at `<prefix>#firehose` behind a bool config, for one consumer group over all events (the `#` namespace is already protected by the sanitizer).
- Runtime-mutable `stream-prefix`, with the current-plus-previous-prefix guard and documented old-stream cleanup semantics.
- Additional entry formats (minimal without `event`, verbose with `class`, JSON) behind an `entry-format` enum config, with a format discriminator and a `dropped_encode_error` counter.
- `MISSED`/`NEW`/`LOADED`/`TRIMMED` capture via direct `raw::RedisModule_SubscribeToKeyspaceEvents` (bypassing the `event_handlers:` macro, which intersects away anything outside NOTIFY_ALL); a hand-written handler also fixes the non-UTF-8 panic via lossy decode.
- Cluster support: the slot-pinned per-node hashtag design (section 10, option B), with per-node discovery and reshard handling.
- Key-name glob filter, per-event maxlen overrides, source-db filter, max-streams cap on distinct event names, an option to disable `verify_oom`, a global monotonic `seq` entry field for cross-stream same-millisecond ordering.
- Per-stream rate-limited failure logging with recovery notices; custom `@eventstream` ACL category (needs `RM_AddACLCategory`, Redis 7.4+, with 7.2/7.3 fallback).
- Full benchmark matrix (mass-expiry drain p99, maxlen sensitivity) with CI regression gates.

## 17. Open questions for the maintainer

1. **Non-UTF-8 module event names panic in the wrapper.** The macro-generated handler calls `to_str().unwrap()` before module code runs (`redismodule-rs/src/macros.rs`). Options: accept and document (no known module fires non-UTF-8 names), subscribe via the raw API with a hand-written handler, or upstream a lossy-decode fix to redismodule-rs. Recommendation: accept and document for v0.1, and file the upstream issue; the raw-API path is already the Future-work route for MISSED/NEW capture and can absorb this then.
2. **notify-keyspace-events bypass across versions.** The bypass is verified in Redis 7.2 `src/notify.c`; the integration test in section 15 pins it on 7.2. Recommendation: run that same test against 7.4 and 8.x in CI before claiming the full support matrix, since this is the one behavior the docs assert that depends on server internals rather than the module API.
3. **Is an immutable `stream-prefix` acceptable for the launch customer?** IMMUTABLE deletes real complexity (dual-prefix guard, cleanup semantics) and relaxing later is non-breaking. Recommendation: keep IMMUTABLE unless the customer states a concrete need to re-prefix without a restart.
4. **`module_args_as_configuration` with three config types.** The macro grammar makes each config-type block optional, but the established wrapper guidance says all four sections must be present when module args are enabled. Recommendation: try omitting the enum block first; if the expansion fails, register an empty `enum: []` list and note it in a code comment.
