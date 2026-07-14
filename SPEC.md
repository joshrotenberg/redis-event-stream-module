# SPEC: redis-event-stream-module

## 1. Summary

`redis-event-stream-module` is a Redis module, written in Rust on the `redis-module` crate (redismodule-rs, pinned at the v2.1.3 git tag), that subscribes to keyspace notifications inside the server and mirrors each selected notification as an `XADD` into a Redis Stream. Keyspace notifications over pub/sub are fire-and-forget: a disconnected subscriber misses events permanently. This module makes those events durable, replayable, and consumable through consumer groups, using only standard Redis Streams on the read side. The v0.1 default configuration is reliable capture of key expiration events (`expired`) for consumers that must not miss one across restarts.

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
- Capturing `LOADED` or `TRIMMED` class events: `LOADED` fires only while the server loads its dataset, when stream writes are unavailable, and `TRIMMED` fires only during cluster reshard trimming, which is unsupported (section 5). `MISSED` and `NEW` are capturable as of the raw-subscription change (section 5).

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
 | 4. event matches events filter?    no -> skipped_filtered++, return      |
 | 5. key matches key-filter?         no -> skipped_key_filtered++, return  |
 | 6. capture db; db in source-dbs?   no -> skipped_db++, return            |
 | 7. sanitize(event) non-empty?      no -> skipped_invalid++, return       |
 | 8. ctx.add_post_notification_job(closure)                                |
 +---------------------------------------------------------------------------+
        |
        v   (runs atomically alongside the notification, writes now safe)
 post-notification job:
   SelectDb(0)                      (raw binding; failure -> dropped_xadd_error)
   new stream and max-streams cap reached? -> dropped_max_streams++, drop
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
| Hash-field expiration (7.4+) | `hexpired`, plus command-generated `hexpire`, `hpersist` | Fire under the HASH class, not EXPIRED, so the default `expired` filter does not match `hexpired` (section 7). `hexpired` has the same lazy/active timing as `expired` and carries only the hash key, not the field name (section 6). When the last field expires the emptied hash is deleted and a `del` event fires. Absent on servers without hash-field TTLs (Redis 7.2, Valkey 8). |
| Module-defined | Arbitrary strings via `RM_NotifyKeyspaceEvent`, e.g. `json.set` | The only unbounded source. Any co-loaded module can fire any name under any class within `NOTIFY_ALL` (redismodule-rs's own `examples/events.rs` fires `events.send` under GENERIC), so excluding the MODULE class does not bound custom names. The real bounds are the 128-byte sanitized-name cap and per-stream `maxlen` trimming; total memory grows with distinct names, not event volume. |

Subscription mechanism. `REDISMODULE_NOTIFY_ALL` covers GENERIC|STRING|LIST|SET|HASH|ZSET|EXPIRED|EVICTED|STREAM|MODULE but excludes `keymiss` (MISSED), `new` (NEW, 7.0.1+), `loaded` (LOADED), and TRIMMED (verified against the vendored `redismodule.h`), and the wrapper's `event_handlers:` macro intersects any requested mask with the server's NOTIFY_ALL, silently stripping those four. The module therefore does not use that macro: it calls `raw::RedisModule_SubscribeToKeyspaceEvents` directly in `init` with a hand-written callback. This lets it request MISSED and NEW, and makes the FFI boundary panic-safe (below).

`MISSED` (`keymiss`, one event per read miss) and `NEW` (`new`, one event per newly created key) are high-volume, so they are opt-in: the subscription mask is `NOTIFY_ALL` plus MISSED and/or NEW only when the load-time filter names them (`@missed`, `@new`, or `*`, which subscribes to both). The mask is fixed at load; `RedisModule_SubscribeToKeyspaceEvents` has no unsubscribe, so a runtime `CONFIG SET eventstream.events` that names a MISSED or NEW class the load did not subscribe to is rejected (a bare `*` at runtime is accepted and captures only what is subscribed). `LOADED` and `TRIMMED` remain uncapturable and their `@class` tokens are rejected with a reason: `LOADED` fires only during dataset load, when the not-LOADING gate and the deferred-write API both refuse writes, and `TRIMMED` fires only during cluster reshard trimming (cluster capture is refuse-by-default with a per-node opt-in, section 10, and TRIMMED remains uncapturable either way).

Byte-level guarantees and panic safety: `RM_NotifyKeyspaceEvent` takes a C string, so event names cannot contain NUL, but they can be non-UTF-8. The wrapper's macro-generated callback would convert the name with `to_str().unwrap()` and panic on non-UTF-8, which is undefined behavior across the FFI boundary and aborts the server (redismodule-rs#472). The module's hand-written callback avoids this two ways: it decodes the name with `String::from_utf8_lossy` (replacement characters for invalid bytes, so the entry's `event` field is always written), and it wraps the whole handler in `catch_unwind`, counting any caught panic as `handler_panics` (a nonzero value is a bug in this module) rather than letting it unwind into Redis. A non-UTF-8 name is therefore captured, not a crash. The post-notification job the callback enqueues runs through a separate FFI trampoline the wrapper does not guard, so its body is wrapped in the same `catch_unwind` and shares the `handler_panics` counter (issue #45 hit this: a null optional-API pointer, `ClusterCanonicalKeyNameInSlot` on 7.2, panicked in the job and aborted the node).

### Sanitization

`sanitize()` maps the event name to the stream key suffix:

1. Characters in `A-Z a-z 0-9 _ . : -` pass through unchanged. Every built-in event name and every known module event name (dotted names included) passes through byte-identical.
2. Any other character becomes a single `_`.
3. Result truncated to 128 bytes (pure ASCII after step 2, so no boundary issues).
4. An empty result is not routed; the notification is dropped and `skipped_invalid` is incremented.

Two distinct raw names can collide after sanitization (`foo bar` and `foo?bar` both become `foo_bar`). This is accepted because every entry carries the raw event name in its `event` field (section 6), so consumers can always distinguish.

`#` is deliberately outside the sanitizer output alphabet, so the `<prefix>#...` namespace is reserved for internal module keys, used in v0.1 by the gap-marker control stream (section 9) and available for future keys, without any possibility of collision from event names.

Escaping the prefix is impossible by construction: the destination is plain concatenation of a validated prefix and a sanitized suffix. There is no parsing step an event string could exploit.

### Discovery

Discovery has two paths. The `EVENTSTREAM.STREAMS` command (section 8) returns every destination stream registered since the registry existed, read live from a persistent set at `<prefix>#streams`. The set is SADD-ed on the first write to each stream through the same replicated, OOM-checked call as the mirrored entry, so it survives restart under RDB or AOF and is present on replicas; an in-process cache suppresses the SADD on subsequent writes and is cleared on flush so a `FLUSHALL` that deleted the set rebuilds it on the next capture. The registry is an append-only log of stream names ever written, not a liveness check: a listed stream may since have been trimmed to empty or deleted.

Deterministic naming still works when the filter is known. With the default configuration the streams are `events:expired` plus the control stream `events:#control` (section 9). A `SCAN` fallback also works:

```
SCAN 0 MATCH events:* TYPE stream
```

(The prefix validation rules in section 7 reject glob metacharacters precisely so this pattern never needs escaping.) The pattern also matches the control and registry keys; consumers enumerating event streams should skip keys under `<prefix>#`, which is safe because the sanitizer can never emit `#` in an event-derived name.

### Firehose stream

`eventstream.firehose yes` (section 7; default `no`) adds a combined stream at `<prefix>#firehose` (default `events:#firehose`), so one consumer group over a single key covers every captured event. The `#` namespace is reserved by the sanitizer (above), so no event-derived stream name can collide with it. In cluster per-node mode the name composes with the node's tag segment exactly like the control stream and the registry (`<prefix>{tag}#firehose`, section 10) and re-pins along with them.

Write path: in the post-notification job, after the per-event write's outcome is settled (including any re-pin it performed), the module issues a second `XADD` to the firehose with fields identical to the per-event entry (`event`, `key`, `db`, section 6), the same `MAXLEN ~` trimming, and the same call options. The two writes succeed or fail independently: a firehose failure never affects the per-event entry, and vice versa. A successful copy counts in `firehose_forwarded` (section 13), never in `forwarded`, which keeps meaning captured events rather than XADDs issued; a failed copy counts in the existing `dropped_*` counters under the same classification and first-failure logging as per-event writes. Drop accounting is per write, not per event: an event whose per-event entry and firehose copy are both refused counts two drops (in per-node mode this includes `dropped_no_owned_slot` counting twice for one event when no slot is owned after a re-pin), so with the firehose enabled, drop counters bound XADD failures, not lost events. The firehose registers in `<prefix><seg>#streams` on its first write, so `EVENTSTREAM.STREAMS` discovers it, and it counts in `active_streams` like any destination stream. The feedback guard covers it (the key is under the prefix), so the firehose's own `xadd` notifications are never mirrored.

Cluster interaction: the firehose copy resolves the tag segment after the per-event write, so when that write triggered a re-pin the copy already lands on the new tag. A cluster refusal of the copy itself is counted (`dropped_migrating` or `dropped_xadd_error`), never re-pinned: slot ownership cannot change between the two XADDs (they run in one execution unit), so such a refusal only occurs in a migration window the per-event write already re-pinned through once, and the one-re-pin-per-event bound holds.

Ordering property: the firehose is a single stream, so its entry IDs give a total per-node order across all event types — including entries within the same millisecond, which merging per-event streams by ID cannot order (section 9). Cost: enabling it doubles write amplification per captured event and adds one stream to the memory bound (section 11).

### Namespace ownership

Keys under `<stream-prefix>` belong to the module. If a user key already exists at a destination name: a non-stream key causes `WRONGTYPE` errors (entries dropped and counted, the module never deletes or overwrites a non-stream key); a pre-existing stream will receive module entries and be trimmed under the module's `maxlen` policy. Deployment docs recommend restricting write access to `<prefix>*` via ACLs.

## 6. Entry schema

v0.1 ships exactly one fixed entry format for mirrored events (gap markers on the control stream use their own schema, section 9: two fields, plus a `db` field on the `flushed` marker). Fields are always emitted in the same order, because Redis stream listpack nodes store field names once per node when consecutive entries share the field set (the `SAMEFIELDS` optimization), so a stable schema keeps per-entry overhead near the payload size.

| # | Field | Value | Notes |
|---|---|---|---|
| 1 | `event` | raw event name, pre-sanitization, e.g. `expired`, `hset` | Disambiguates sanitizer collisions and keeps entries self-contained if forwarded elsewhere |
| 2 | `key` | raw key bytes | Exact bytes of the affected key, no encoding, no escaping |
| 3 | `db` | decimal string, e.g. `"0"` | Database index where the event fired |

There is deliberately no timestamp field: the auto-generated entry ID (`<ms>-<seq>`) carries a millisecond timestamp assigned at write time, and since the write runs atomically alongside the notification, that is the event time for practical purposes. `XRANGE` by time works natively against it. These three values plus the ID are everything the notification callback receives; there is no value payload, old value, or TTL available at notification time, and the schema does not pretend otherwise. In particular, for hash-field expiration (`hexpired`, section 5) the `key` field carries the hash key; the expired field name is not part of the keyspace notification and has no slot here — unlike key-level `expired`, where the key is the expired thing.

Binary safety: the wrapper hands the handler the key as `&[u8]`, and `ctx.call_ext` accepts `&[&[u8]]` argument slices (`StrCallArgs` implements `From<&[&T]> for T: AsRef<[u8]>`), so key bytes pass through untouched. Consumers must read `key` with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will mangle non-UTF-8 keys, which is a client configuration issue, not stream data loss.

Database placement: all destination streams live in database 0, regardless of which database the event fired in; the entry's `db` field preserves the origin. Rationale: one stream per event name total, so one consumer group, one discovery scan, and one blocked `XREAD` cover the whole instance, and the section 11 memory bound stays `distinct_event_names x maxlen` rather than multiplying by the number of active databases. Consumers that care about origin filter on the `db` field. Mechanics: the origin db index is captured in the notification callback via the raw `RedisModule_GetSelectedDb` binding (no safe wrapper in the pinned tag) and moved into the job closure; the job then explicitly selects database 0 via the raw `RedisModule_SelectDb` binding before the `XADD`. (The job context also arrives selected to the origin database, but that is undocumented server behavior, `module.c` stores and restores the db id around post-execution-unit jobs, so the spec does not rely on it for the `db` field; the section 15 non-zero-db integration test pins the whole path either way.) A select failure is not expected on a standalone server (db 0 always exists); if it occurs, the entry is dropped, counted as `dropped_xadd_error`, and logged on first failure like any other write error. In cluster mode only db 0 exists, and cluster is unsupported in v0.1 anyway. Alternative rejected: per-db placement (streams live where the event fired) needs no select logic but forces a reader, a consumer group, and a discovery scan per database, and a consumer watching only db 0 silently misses every other database's events.

SWAPDB caveat: `SWAPDB` involving database 0 atomically moves the existing destination streams (and their consumer groups) into the other database, while the module continues writing to db 0, creating fresh streams there. Consumers connected to db 0 lose history but keep receiving new entries; the moved streams retain the history in the swapped database. The per-entry `db` field remains the historical truth of where each event fired. The module detects this through a raw `REDISMODULE_EVENT_SWAPDB` subscription (redismodule-rs still has no safe wrapper) and delimits the boundary with a `swapdb` gap marker on the fresh db 0 control stream (section 9); a swap not involving db 0 is ignored.

Alternatives considered and rejected for v0.1: JSON-encoded single field (keys are arbitrary bytes and would need base64, and the `SAMEFIELDS` compaction is lost), value capture (unbounded size, impossible for `expired`), per-entry timestamp (duplicates the entry ID), and a minimal/verbose format pair behind a config (mixed-format streams need a discriminator and a second code path with no v0.1 user; deferred). The format pair and the JSON encoding shipped after v0.1 behind the `eventstream.entry-format` enum, with the discriminator the deferral called for (issue #60, below); `fixed` above remains the default and the byte-for-byte-compatible baseline.

### Alternative entry formats

`eventstream.entry-format` (section 7; default `fixed`) selects the mirrored entry's field set. This is the module's first enum config. New formats are additive: the enum can grow without breaking existing values, and the default never changes shape, so a consumer written against `fixed` is unaffected until an operator opts in.

| Value | Fields (in order) | Notes |
|---|---|---|
| `fixed` (default) | `event`, `key`, `db` | The schema above, byte-for-byte unchanged. No `format` discriminator, so existing consumers see no difference. |
| `minimal` | `format`, `key`, `db` | Drops the redundant `event` field (the stream name already encodes it); consumers lose sanitizer-collision disambiguation. |
| `verbose` | `format`, `event`, `key`, `db`, `class` | Adds `class`: the notification class name(s) from the `NotifyEvent` bitmask (`string`, `hash`, `expired`, …; comma-joined if more than one), matching the section 7 filter grammar. |
| `json` | `format`, `data` | `data` is one JSON document: `{"event":<string>,"key":<base64>,"db":<number>}` (and `"seq":<number>` when `entry-seq` is on). Key bytes are arbitrary binary, so they are base64-encoded (RFC 4648, padded) — the reason JSON was rejected as the *only* format. |

Every non-`fixed` entry carries a leading `format` discriminator, so a stream that mixes formats — after a live `CONFIG SET` (the config is DEFAULT, not IMMUTABLE), or across restarts with different module args — is self-describing per entry. `fixed` entries stay discriminator-free to preserve the existing schema exactly.

`SAMEFIELDS` caveat: any format whose field set varies between consecutive entries forfeits the listpack `SAMEFIELDS` compaction (above). Within a run of same-format entries the field order is stable, so the compaction still applies; the loss is at the format-change boundary, which a live `CONFIG SET` makes possible. An `entry-format` change mid-stream is therefore a schema change consumers must tolerate: the discriminator on every non-`fixed` entry is what lets them. Gap markers on the control stream (section 9) keep their own two/three-field schema and are unaffected by `entry-format`.

An entry that cannot be encoded is dropped and counted in `dropped_encode_error` (section 13); with the shipped formats only `json` can fail, on a non-UTF-8 event name, which the raw callback's lossy decode makes unreachable through the normal notification path.

### Global sequence field

`eventstream.entry-seq` (section 7; default `no`) appends a `seq` field to every mirrored entry, carrying a process-global monotonic counter. For the `json` format the value goes inside the document (`"seq"`) rather than as a separate field, keeping json a single document field.

| # | Field | Value |
|---|---|---|
| — | `seq` | decimal string of a module-global `AtomicU64`, e.g. `"48212"` |

`seq` gives fan-in consumers a per-node tiebreaker for entries that share a millisecond across different destination streams (the entry ID's `seq` component only orders within one stream; section 9). Semantics:

- **Strictly increasing in notification order** across all destination streams within one process lifetime; assigned once per captured event in the same execution unit as the `XADD` (section 9 atomicity), so per-stream `seq` order matches entry-ID order. The per-event entry and its firehose copy represent one event and carry the same `seq`.
- **Per process.** The counter is an in-process `AtomicU64` that resets to 0 on module load/reload; it is never persisted or replicated as state (the assigned value replicates verbatim inside the `XADD`, so replicas and AOF replay preserve it). Entries from before a restart compare against entries after it only by entry ID, as today.
- **Per source node in cluster mode.** Each node's module instance has its own counter, so `seq` orders entries per node, not across nodes. Cross-node same-millisecond ties still need an application timestamp (section 9, docs/consumer-patterns.md).
- **Increasing, not gapless.** The number is drawn when the event is captured, before the write is attempted, so an event dropped at the write (OOM, `max-streams`, `XADD` error, encode error) still consumes its value and leaves a gap. `seq` is a tiebreaker for ordering, never a completeness signal: gaps do not indicate loss (the control stream and drop counters do), and consumers must not treat a missing `seq` as a lost entry.

`entry-seq` is registered IMMUTABLE (load-time only) precisely so that within one process every stream's field set is uniform — every entry either always carries `seq` or never does — which preserves the `SAMEFIELDS` compaction. Gap markers do not consume or carry a sequence number. The current counter value is not exposed in INFO; it is an implementation-internal ordering token, not an operator metric.

## 7. Configuration

The module name is `eventstream`; Redis registers module configs as `<module-name>.<key>`, so all keys read `eventstream.<key>`. This is the single authoritative table; every name and default elsewhere in this document matches it.

| Key | Type | Default | Live-settable | Validation |
|---|---|---|---|---|
| `eventstream.enabled` | bool | `yes` | yes | `yes` / `no` |
| `eventstream.firehose` | bool | `no` | yes | `yes` / `no` |
| `eventstream.stream-prefix` | string | `events:` | no (IMMUTABLE) | non-empty; at most 128 bytes; characters limited to `A-Z a-z 0-9 : . _ - { }`; glob metacharacters (`*`, `?`, `[`, `]`, `\`) rejected |
| `eventstream.events` | string | `expired` | yes | filter grammar below; empty string rejected |
| `eventstream.key-filter` | string | `*` | yes | comma-separated list of glob patterns (key-filter grammar below); whitespace around patterns trimmed; empty patterns and the empty string rejected |
| `eventstream.source-dbs` | string | `*` | yes | `*` (all databases) or a comma-separated list of non-negative decimal database indexes; whitespace trimmed; empty tokens, the empty string, and non-integer tokens rejected. Only meaningful on standalone (cluster has only db 0) |
| `eventstream.maxlen` | i64 | `10000` | yes | `0` to `i64::MAX`; `0` disables trimming. Redis enforces the registered range on `CONFIG SET` and redis.conf paths only; a module-arg value becomes the registered default and bypasses the boundary check (verified against redis 7.2 `module.c`/`config.c`), so the module's config binding re-validates and rejects negatives, aborting the load |
| `eventstream.max-streams` | i64 | `0` | yes | `0` (unlimited) to `i64::MAX`; caps the number of distinct destination streams created. Same module-arg boundary-bypass quirk as `maxlen`, so the binding re-validates and rejects negatives, aborting the load |
| `eventstream.cluster-streams` | string | `refuse` | no (IMMUTABLE) | `refuse` or `per-node` (section 10). Only meaningful in cluster mode |
| `eventstream.entry-format` | enum | `fixed` | yes | `fixed`, `minimal`, `verbose`, or `json` (section 6). The module's first enum config |
| `eventstream.entry-seq` | bool | `no` | no (IMMUTABLE) | `yes` / `no`; appends the global `seq` field to every entry (section 6) |

**`eventstream.enabled`.** Master kill switch. There is no unsubscribe API for keyspace notifications, so `no` is an early return at the top of the notification handler (one atomic load per event). Flipping back to `yes` does not replay events that occurred while disabled.

**`eventstream.firehose`.** Opt-in combined stream at `<prefix>#firehose` (section 5): when `yes`, every captured event is written a second time to the firehose with fields identical to its per-event entry. Off by default because enabling it doubles write amplification per captured event (section 11). Runtime-mutable; toggling takes effect on the next captured event, and events captured while it was off are not replayed into the firehose. Toggling opens no capture gap (per-event mirroring continues either way), so unlike `enabled` it records no gap marker.

**`eventstream.stream-prefix`.** Registered with `ConfigurationFlags::IMMUTABLE`: settable via module args, a `loadmodule` line, redis.conf directive, or `MODULE LOADEX CONFIG`, but not via `CONFIG SET`. Rationale: a runtime-mutable prefix drags in dual-prefix feedback-guard machinery, old-stream cleanup semantics, and registry-reset questions, all for no v0.1 user; relaxing IMMUTABLE to mutable later is non-breaking. An empty prefix is rejected because the feedback guard (skip keys starting with the prefix) would then match every key and blackhole all events. Braces are allowed in the charset; they are reserved for the future cluster design (section 10), not a working cluster recipe in v0.1.

**`eventstream.events`.** Which events to mirror. Default `expired` captures only key expirations and creates exactly one stream; mirroring everything by default would silently add write amplification to any production workload the moment the module loads. Operators widen it deliberately.

**`eventstream.key-filter`.** A key-name glob filter, ANDed with `eventstream.events`: an event is mirrored only if it passes both. The default `*` is a match-all no-op, so existing deployments are unaffected. It selects by the affected key (e.g. `session:*`), an axis `eventstream.events` cannot express, and it cuts writes at the source — non-matching events cost one predicate evaluation and one counter increment (`skipped_key_filtered`), never an `XADD`. Patterns match the raw key **bytes** (keys are arbitrary binary), not a UTF-8 decode, and the skip path allocates nothing (section 11). Evaluated in the notification callback alongside the events-filter predicate (gate 5, section 4), before sanitize and defer.

**`eventstream.source-dbs`.** Restricts capture to events whose origin database is in the named set. The default `*` captures every database (the historical behavior). Otherwise a comma-separated list of decimal indexes, e.g. `0` or `0,2,5`; an event fired in any other database is dropped and counted (`skipped_db`) before the `XADD`, so a busy multi-database instance no longer pays capture, write, memory, and replication cost for databases a consumer will discard. The server's database count is not known at config-set time, so any non-negative `u32` index is accepted and an out-of-range one simply never matches. Streams still live in db 0 and each entry still records its origin in the `db` field (section 6). Only meaningful on standalone: in cluster mode only db 0 exists, so any list containing `0` is equivalent to `*` (same register as the `cluster-streams` "only meaningful in cluster mode" note).

**`eventstream.maxlen`.** Per-stream retention cap, applied inline as `XADD ... MAXLEN ~ <n>` on every write. Default 10000 bounds worst-case memory (section 11) while degrading to "recent history" rather than degrading to an outage. Alternative considered: periodic `XTRIM`. Rejected: inline approximate `MAXLEN` achieves the same bound with no extra writes and no timer.

**`eventstream.max-streams`.** Caps the number of distinct destination streams the module will create. Module-defined event names are the only unbounded source of streams (section 5): a wide-open filter against a buggy or hostile co-loaded module grows memory linearly with distinct names (`total ≈ distinct_event_names × maxlen × bytes_per_entry`, section 11), and neither the 128-byte name cap nor per-stream `maxlen` bounds the *count*. The default `0` is unlimited. When the cap is nonzero and a captured event would create a new stream beyond it, the event is dropped and counted (`dropped_max_streams`, part of the `dropped` sum) rather than captured; streams already registered keep receiving events. The count is process-local (like `active_streams`) and matches its semantics: it resets on flush and on restart, so the first `max-streams` distinct names seen afterward win, and the persistent registry stops growing once the in-process count re-fills. Lowering the cap below the current count at runtime is accepted: existing streams continue and no new streams are created until the count is under the cap again. The control stream and gap markers are exempt (the `#` namespace is not event-derived and markers must never be dropped); the firehose, being a single fixed `#`-namespaced stream, is likewise never itself blocked, but it does register like any destination stream and so occupies one cap slot (matching `active_streams`, which counts it) — size `max-streams` as one larger when the firehose is enabled. Enforced at the point a first write registers a new stream (section 5 Discovery), before the `XADD`.

**`eventstream.entry-format`.** Selects the mirrored entry's field set (section 6): `fixed` (default, the historical `event`/`key`/`db` schema), `minimal`, `verbose`, or `json`. Registered DEFAULT (live-settable): a `CONFIG SET` takes effect on the next captured event. Live-settable is deliberate — it is what makes the per-entry `format` discriminator load-bearing, since a mid-stream change produces a mixed-format stream that consumers read entry by entry. The default never changes shape, so existing deployments are unaffected until an operator opts in.

**`eventstream.entry-seq`.** When `yes`, every mirrored entry carries a `seq` field with a process-global monotonic counter (section 6), giving fan-in consumers a per-node same-millisecond tiebreaker. Registered IMMUTABLE (load-time only, like `stream-prefix`) so that within one process the entry field set stays uniform — every stream either always has `seq` or never does — which preserves the `SAMEFIELDS` compaction. Default `no`, so existing deployments see no schema change. Resets to 0 on load; not persisted or replicated as state.

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

The subscription mask is fixed at load (there is no resubscribe API), so the filter is a module-side predicate evaluated per notification. Class tokens only select; they never change stream naming, which is always per event name.

```
filter := token ( "," token )*
token  := "*" | "@" class | event-name
class  := generic | string | list | set | hash | zset | stream
        | expired | evicted | module | missed | new
event-name := any non-empty run of characters except "," and whitespace
```

- Whitespace around tokens is trimmed; duplicates ignored.
- `*` matches every delivered event (that is, every event in the subscribed classes).
- `@class` matches the `NotifyEvent` bitmask passed to the handler. `generic` through `module` are the `NOTIFY_ALL` classes, always subscribed. `missed` and `new` are opt-in and must be named at load (section 5); naming one at runtime that the load did not subscribe to is rejected. `@loaded` and `@trimmed` are always rejected with a reason (section 5).
- A bare token is an exact, case-sensitive byte comparison against the delivered event name. Bare names are not validated against a closed list because the namespace is open (modules can fire custom names).
- Unknown `@class` tokens, empty tokens, and the empty string are rejected at `CONFIG SET` time. To pause the module, use `eventstream.enabled no`; an empty filter is a mistake, not a state.

| Value | Captures |
|---|---|
| `expired` | expirations only, into `events:expired` |
| `expired,evicted` | expirations and evictions |
| `expired,hexpired` | key and hash-field expirations (section 5); the default `expired` alone does not match `hexpired`, which is a distinct name under the HASH class |
| `@hash` | every hash-class event, each to its own stream |
| `*` | everything the subscription delivers |

### Key-filter grammar

`eventstream.key-filter` is a comma-separated list of glob patterns, evaluated against the raw key bytes and ANDed with the events filter:

```
key-filter := pattern ( "," pattern )*
pattern    := any non-empty run of bytes except ","
```

- Whitespace around each pattern is trimmed; empty patterns and the empty string are rejected (to pause the module use `eventstream.enabled no`, the same rule as `eventstream.events`).
- Patterns use Redis `stringmatchlen` glob semantics: `*` (any run, including empty), `?` (one byte), `[...]` classes with `[^...]` negation and `a-z` ranges, and `\` to escape a metacharacter. Matching is case-sensitive and byte-exact — patterns are compared against the raw key bytes, never a UTF-8 decode, so binary keys match predictably.
- Multiple patterns OR together; a key mirrored if it matches any one. A bare `*` is the match-all default and short-circuits without a byte scan.
- Because tokens split on `,`, a pattern cannot contain a literal comma; this matches the multi-token shape of the events grammar.

| Value | Captures |
|---|---|
| `*` | every key (default, no-op) |
| `session:*` | only keys beginning `session:` |
| `session:*,cache:*` | keys beginning `session:` or `cache:` |
| `user:[0-9]*` | `user:` followed by a digit and any suffix |

### Source-db grammar

`eventstream.source-dbs` is `*` (every database) or a comma-separated list of non-negative decimal database indexes (`0`, `0,2,5`). Whitespace around tokens is trimmed; empty tokens, the empty string, and non-integer tokens are rejected. Any in-`u32`-range index is accepted at config time; an index beyond the server's `databases` count simply never matches (the count is a server config not known to the module binding).

### Validation mechanics

The wrapper's stock `ConfigurationValue` impls never reject beyond UTF-8 conversion, and `on_changed` fires after the value is stored and cannot veto. Rejection is only possible from `ConfigurationValue::set` returning `Err`, which the wrapper surfaces as the `CONFIG SET` error reply (`ConfigrationPrivateData::set_val`, redismodule-rs `src/configuration.rs`). `eventstream.stream-prefix` and `eventstream.events` therefore bind to custom static types implementing `ConfigurationValue<RedisString>`: `set()` parses and validates, storing both the raw string (for `CONFIG GET`) and the parsed form (class bitmask plus name set) behind a `RedisGILGuard`, which the notification handler (always run with the GIL held) reads without extra locking.

```
127.0.0.1:6379> CONFIG SET eventstream.events "expired,@hsah"
(error) ERR CONFIG SET failed - unknown event class '@hsah'
```

### Load-time args

Precedence at load, lowest first: compiled default; unprefixed module args (`loadmodule .../libredis_event_stream_module.so events "expired,evicted" maxlen 50000`, enabled by `module_args_as_configuration: true`); prefixed standard config sources (`eventstream.events` directive in redis.conf, or `MODULE LOADEX ... CONFIG eventstream.events ...`, applied by `RedisModule_LoadConfigs` after registration); then `CONFIG SET` at runtime for mutable keys. `CONFIG REWRITE` persists current values.

Operator quirks to document: bool module args are true only for the literal string `yes` (anything else silently parses as false, `get_bool_default_config_value`), and a malformed module-arg value aborts module load with a logged error. Implementation note: with `module_args_as_configuration: true` the macro expansion requires all four config-type lists to be present (verified against v2.1.3; omitting one fails to compile, section 17 question 4), so the module registers an empty `enum: []` block, with a code comment, since it has no enum configs in v0.1. The macro's optional `module_config_get`/`module_config_set` convenience commands are not registered; `CONFIG GET/SET eventstream.*` covers the need.

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
| `firehose` | firehose copy written or skipped per the new value | still execute (completed before the change) |
| `events` | new predicate applies | still execute (matched under old filter) |
| `key-filter` | new glob set applies | still execute (matched under old filter) |
| `source-dbs` | new db set applies | still execute (matched under old set) |
| `maxlen` | new cap on each `XADD` | old cap; an idle stream is re-trimmed only on its next write |
| `max-streams` | new cap on new-stream creation | still execute; a stream already registered keeps writing even if the cap was lowered below the current count |

Since post-notification jobs run atomically within the triggering command and `CONFIG SET` is a separate serialized command, the enqueue-to-execute window never spans a config change in a way that needs special handling. The prefix cannot change at runtime, so the feedback guard always matches the single current prefix.

## 8. Commands

Behavior changes go through `CONFIG SET`; nothing observable requires a module command, since the INFO section (section 13) and standard stream commands (`XLEN`, `XRANGE`, `XINFO STREAM`, `XINFO GROUPS`) cover it. v0.1 shipped with no commands on that basis.

Two readonly, keyless introspection commands were added after v0.1:

| Command | Reply | Flags |
|---|---|---|
| `EVENTSTREAM.STATS` | The section 13 counters as a flat array of field/value pairs, agreeing with the INFO section at call time | `readonly fast`, keyless |
| `EVENTSTREAM.STREAMS [WITHSTATS]` | The registered destination streams (section 5 Discovery), read live from `<prefix>#streams`; with `WITHSTATS`, per-stream counters (below) | `readonly`, keyless (O(N) in registered streams, so not `fast`) |

`EVENTSTREAM.STREAMS` reads a set that lives in database 0; the command selects database 0 for the read and restores the caller's database before returning. Both commands touch only keys under the prefix (or no keys), so a least-privilege consumer ACL that already grants `~<prefix>*` covers them; grant the `eventstream.*` commands explicitly if the ACL restricts by command.

The bare reply is the flat array of stream names, unchanged. `WITHSTATS` returns one array per registered stream: `[name, "forwarded", <n>, "dropped", <n>]` — the entries this process wrote to that stream and the entries a refused `XADD` to it dropped (the `dropped_xadd_error` and `dropped_oom` scopes; drops with no destination stream in hand, like defer errors, have no stream to land on). The counters are process-local: reset on load and cleared with the flush invalidation that clears the registry cache (per-stream counts therefore read "since load or last flush", while the section 13 process-wide counters remain strictly since-load; after a flush the two can disagree by the pre-flush writes). The registry itself is append-only across restarts, so a registered stream with no writes since load reports zeros. The firehose stream is a registered destination stream: its row's `forwarded` is the per-stream view of `firehose_forwarded`. The control stream is not in the registry and is not listed; a stream whose every write failed is likewise absent until its first successful write registers it (its drops are counted, but attributed rows exist only for registered streams). In per-node cluster mode the reply covers the local node's registry and this process's counters only (section 10; cluster-wide fan-out is client-side, issue #47).

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
- Per key across event names: not directly readable as one sequence (`hset k`, `del k`, `expired k` land in three streams). Merging streams by entry ID reconstructs order except for ties within the same millisecond. Two ways close that tie: the firehose (section 5), when enabled, is a single stream carrying every captured event, so its entry order is a total per-node order across event types, ties included; and the `seq` field (`eventstream.entry-seq`, section 6), when enabled, tags every entry with a per-node monotonic counter, so merging by `seq` totally orders same-millisecond entries across streams on one node. Both are per-node in cluster mode.
- Cross-stream, cross-key: no guarantee beyond entry ID timestamps, plus per-node `seq` when `entry-seq` is on (a per-node total order, not cross-node).

### Loss windows

| Window | Cause | Mitigation |
|---|---|---|
| Module not loaded / `enabled no` | Nothing listens | Load at startup via `loadmodule`; no replay on re-enable; window boundaries are machine-readable via gap markers (below) |
| Filter mismatch | Event name not selected | By design; counted as `skipped_filtered` |
| `XADD` refused: OOM | With the `M` flag, writes are refused under `maxmemory` | Dropped and counted (`dropped_oom`); deliberate, see section 11 |
| `XADD` failed: `WRONGTYPE` etc. | Non-stream key at the destination name | Dropped and counted (`dropped_xadd_error`); module never deletes the offending key |
| Job scheduling failed | `add_post_notification_job` returned `Status::Err` | Dropped and counted (`dropped_defer_error`) |
| Stream trimming | `MAXLEN` evicts entries before a slow consumer reads them | Bounded, configurable; size `maxlen` for the slowest consumer; loss is detectable (below) |
| Crash before fsync | Server persistence config | `appendfsync everysec` bounds loss to about 1 second (section 10) |
| Failover | Entries not yet replicated to the promoted replica | Standard async replication caveat |
| `FLUSHALL`, or `FLUSHDB` in db 0 | No per-key notifications fire, and the destination streams (with their consumer groups) are deleted | `flushed` marker (`db -1`) on the recreated control stream (below); full reconcile |
| `FLUSHDB` in a non-zero db | No per-key notifications fire for the flushed keys; db 0 streams are unaffected | `flushed` marker carrying the flushed `db` (below); reconcile over that database |
| `SWAPDB` involving db 0 | The destination streams (with their groups) move to the swapped database; the module writes fresh streams in db 0 | `swapdb` marker on the fresh db 0 control stream (below); reconcile db 0 history from the swapped database |

Semantic caveat inherited from Redis: `expired` fires when Redis actually removes the key (lazy access or active expire cycle), not at the nominal TTL instant. The entry ID timestamp is the removal time.

### Gap markers

Capture gaps are made machine-readable through a control stream at `<stream-prefix>#control` (default `events:#control`). The `#` character is outside the sanitizer alphabet (section 5), so no event name can collide with it. The module writes a marker entry at each capture-boundary lifecycle point:

| Trigger | `action` value |
|---|---|
| Module load (`init`) | `loaded` |
| `eventstream.enabled` set `yes` to `no` | `disabled` |
| `eventstream.enabled` set `no` to `yes` | `enabled` |
| `FLUSHALL`, or `FLUSHDB` in any db | `flushed` |
| `SWAPDB` involving db 0 | `swapdb` |
| `MODULE UNLOAD` (`deinit`) | `unloading` |

Each marker carries two fields, `action` and `module-version`, with one additive exception: the `flushed` marker carries a third `db` field, the decimal flushed database number (`-1` for `FLUSHALL`), so consumers can bound the reconcile to the flushed database. Consumers reading markers by `action` are unaffected by the extra field. Markers are written through the same `call_ext` options as mirrored entries (`!`, `E`, `M`), so they replicate, respect `maxmemory`, and persist like any other entry; the same `maxlen` trimming applies to the control stream, and the MASTER-and-not-LOADING gate applies to marker writes exactly as it does to mirrored entries (replicas receive markers only via replication of the master's writes). Marker-write failures follow the same drop-counter and first-failure-log policy as mirrored entries. Markers count in `control_markers` (section 13). The prefix feedback guard already drops the control stream's own keyspace notifications.

The flush and `SWAPDB` events are captured through raw `RedisModule_SubscribeToServerEvent` subscriptions (`REDISMODULE_EVENT_FLUSHDB`, `REDISMODULE_EVENT_SWAPDB`), not the wrapper's flush-event macro: the safe wrapper discards the `RedisModuleFlushInfo`/`RedisModuleSwapDbInfo` payload, and the flushed/swapped database numbers are exactly what these markers turn on. This is the same raw-binding rationale as the keyspace subscription (section 5), and both callbacks catch panics at the FFI boundary. A `SWAPDB` that does not involve db 0 leaves the streams' database untouched and writes no marker. The `flushed`/`swapdb` markers are recorded only when this node is the capturing master (the same MASTER-and-not-LOADING gate that governs marker writes): the flush event also fires when a replica empties its dataset for a full resync (no capture gap on that node) and when a replica replays a replicated `FLUSHALL`/`FLUSHDB`/`SWAPDB` (the master's own marker already reaches the replica through replication), so recording one on the replica would spuriously duplicate it on failover. Registry invalidation is unconditional by contrast, since a replayed flush really does clear the replica's registry.

Delivery mechanics. Direct writes are impossible or unsafe at most of these lifecycle points, so markers are deferred, not hedged: at v2.1.3 the config on-changed callback receives only a `ConfigurationContext`, a deliberately restricted type with no command-call capability, and a direct write in `init` is a startup hazard (with `loadmodule` at startup the module initializes before the dataset loads; creating the control stream in the empty keyspace makes the subsequent RDB load hit a duplicate key and abort the server). A direct write at flush time would itself be flushed (for `FLUSHALL` or `FLUSHDB` in db 0, which delete the control stream). The `loaded`, `disabled`, `enabled`, `flushed`, and `swapdb` markers therefore go through a pending-marker mechanism: the lifecycle point records the pending action (a `Vec`, so overlapping points accumulate rather than clobber each other), and the notification callback, which keeps running while disabled, checks it ahead of the enabled gate and enqueues a post-notification job that writes the pending markers before that event's mirrored entry. The marker's entry ID consequently timestamps the first event at the boundary, which is exactly the boundary that matters: the first lost event after a disable, the first captured event after an enable, load, flush, or swap. If no notification fires, the pending marker is never written, and nothing was mirrored into the void in that window either (for a flush that deleted the streams the pre-flush contents are still gone). The only direct write is `unloading` in `deinit`, which runs inside the `MODULE UNLOAD` command on a live server, where writes are safe and no future notification exists to defer to.

Consumers delimit gap windows by reading marker pairs: the window between a `disabled` or `unloading` marker and the next `enabled` or `loaded` marker is a capture gap, and reconciliation can be bounded to it instead of sweeping the keyspace. A `flushed` marker opens a gap that a full reconcile of the flushed database closes (the pre-flush stream contents are gone); a `swapdb` marker means entries before it may now live in another database (read the swapped database to recover db 0 history). In cluster per-node mode a `repinned` marker (section 10) appears on a node's new control stream when its pinned slot migrated away, delimiting the point where that node's stream name changed and any migration-window events were lost. Two limitations, both documented: crashes write no closing marker, and clean server shutdowns cannot write one — a shutdown marker is structurally impossible (investigated and rejected in #67). `finishShutdown` in server.c (verified at Redis 7.2.0 and 8.0.0; Valkey inherits the path) orders the final AOF flush, then the final RDB save, then the Shutdown module event, then the replica output-buffer flush, so a write from the event handler never reaches the persisted dataset; replicating that write instead trips `propagateNow`'s shutdown-pause assertion when replicas are attached and not fully acked (`prepareForShutdown` pauses client writes and `finishShutdown` never unpauses), aborting the server. Clean restarts and crashes are therefore indistinguishable, permanently: both appear as a `loaded` marker with no preceding `unloading` or `disabled`, bounded below by the last entry ID across the mirrored streams.

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

### Cluster: refuse by default, opt-in per-node capture

Three facts collide in cluster mode: notifications are node-local (every master sees only its own shard of events); a fixed destination stream key name hashes to one slot owned by one master; and `RM_Call` refuses a non-local key (the observed error is `Attempted to access a non local key in a cluster node`, a hard local refusal, not a followable MOVED). Worse, because each distinct destination name (event streams, `#control`, `#streams`) hashes to a different slot, even the node owning one of them fails on the others. Net effect with no countermeasure on an N-master cluster: nothing captures reliably. This was confirmed against a live cluster (`tests/cluster.rs`).

Behavior is chosen by `eventstream.cluster-streams` (IMMUTABLE, load-time):

- `refuse` (default): the module refuses to load when `ContextFlags::CLUSTER` is set, with a clear error at load time. No silent loss, no half-working deployments.
- `per-node` (issue #45): each master pins all of its keys to a hash tag that hashes to a slot the node owns, so its writes stay local. The tag is shared across the node's event streams, control stream, and registry (`events:{tag}expired`, `events:{tag}#control`, `events:{tag}#streams`) so they co-locate; distinct nodes pin distinct tags (a tag's slot is owned by exactly one node).

Tag selection is lazy. A node owns no slots at module load (it joins the cluster afterward), so the tag is selected on the first captured event, when slots are known: the module walks slots and probes ownership with a non-destructive replicated write (`XADD {tag}#slotprobe NOMKSTREAM`), which is the same locality rule the real writes obey (a plain read is not slot-checked and would falsely pass on every node). The candidate tag for each slot comes from `RedisModule_ClusterCanonicalKeyNameInSlot`, which guarantees slot coverage; that API was added after 7.2, so on 7.2 (where its pointer is null and calling it would abort the server) the module falls back to a runtime CRC16 search (issue #116): a slot-to-tag table, built once at first fallback use by brute-forcing the CRC16 key-hashing function, maps each probed slot to a synthetic tag hashing to it. Coverage is therefore exhaustive on both paths: a node that owns any slot finds a tag, however skewed the slot ownership. If the node owns no slot yet, events are dropped and counted as `dropped_no_owned_slot`.

Re-pinning after a reshard (issue #46). Slot ownership changes during resharding and failover. Detection is reactive and lazy: when a mirrored `XADD` returns the local-refusal error (`Attempted to access a non local key in a cluster node`), the pinned slot has migrated away. Re-pinning only matters when there is an event to capture, so detecting on the failing write needs no timer or topology-event plumbing. On that error the module clears the cached tag, re-selects a currently owned slot, writes a `repinned` gap marker to the new control stream, and retries the entry once on the new tag, so the triggering event is captured rather than dropped (counted in `repins`). A `TRYAGAIN`/`ASK` refusal is treated the same way (issue #75): it fires while the pinned slot is still `MIGRATING`/`IMPORTING`, an earlier signal of the same departure, so the module re-pins immediately instead of dropping until the migration completes; the ownership probe itself fails with `TRYAGAIN`/`ASK` on a slot mid-migration, so re-selection never picks one, and the single-retry bound holds. Detection is also hardened against error-text rewording (issue #76): the local-refusal message is observed behavior, not a documented error code, so on an unclassified `XADD` failure the module re-verifies ownership of the pinned tag with the same probe (at most once per streak of unclassified failures: the verified tag is cached, and the cache resets on a re-pin or on any successful mirrored write, so a stale verification cannot mask a later migration); a failing probe triggers the same re-pin path regardless of text, counted in `repins_probe_detected` in addition to `repins` (a nonzero value means the string match stopped working; report the new message form upstream). If the node owns no slot at all after re-pinning, the event is a `dropped_no_owned_slot` and capture resumes on a later event. The old `{tag}` streams are ordinary keys in the migrated slot: they move with it to the new owner and stay reachable there by name through the cluster, so no history is lost on the migration itself. The one data-safety caveat is the migration window: while a slot is `MIGRATING`/`IMPORTING`, a write can be refused (`TRYAGAIN`/`ASK`); the refusal triggers the early re-pin above, and an event still refused after the one retry is counted as `dropped_migrating` (not the generic `dropped_xadd_error`, so routine resharding does not read as a broken write path), delimited by the gap markers (SPEC.md section 9). Single-shard clusters (one master owning all slots) never reshard the pinned slot and are the safest deployment.

Failover is compatible without extra work: the MASTER-only gate (section 4, gate 3) means only masters capture, and a promoted replica re-selects an owned slot on its first captured event, writing to streams it already hosts (they replicated to it before promotion).

Rejected alternatives: a source-key hashtag (`events:{<key>}:expired`) keeps writes local but produces one stream per source key, defeating consolidation; a plain node-id name prefix does not change which slot the key hashes to, so it does not solve placement at all.

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

Firehose amplification: `eventstream.firehose yes` (section 5) turns every captured event into two `XADD`s plus trim instead of one, roughly doubling the added CPU per captured event; the storm cases above double with it. Off by default for exactly this reason.

Memory bound: `total ≈ distinct_event_names × maxlen × bytes_per_entry`. A three-field entry with a 32-byte key costs roughly 150 bytes. Streams are consolidated in database 0 (section 6), so the bound is independent of how many databases fire events; the control stream (section 9) adds one more stream under the same `maxlen`, and the firehose, when enabled, one more (its `maxlen` window spans all event types combined, so a busy type can crowd a quiet one out of it; size `maxlen` for the total event rate).

| maxlen | Distinct event names | Estimated total |
|---|---|---|
| 10000 (default) | 1 (default filter) | ~1.5 MB |
| 10000 | 20 (typical wide filter) | ~30 MB |
| 10000 | 200 (worst case, all classes plus module names) | ~300 MB |

Measurement plan (documented in the README): memtier_benchmark, 60 second runs, 3 repetitions, ops/sec and p50/p99: S0 baseline without the module; S1 module loaded with the default filter against a non-expiring SET workload (the tax every non-capturing deployment pays, expected within a few percent of S0); S2 filter `set` for 100 percent capture (expected within the 50 percent budget above). The full matrix adds S3, foreground p50/p99 and drain duration while a staggered mass-expiry backlog drains, with and without the module; and S4, the S2 workload across `maxlen` values, where amortized trim cost should stay near zero. A scheduled CI job runs a reduced matrix and gates only on relative properties (S1/S0 within the few-percent budget plus noise headroom, S2/S0 and S4/S0 within the 50 percent budget, zero S3 drops), because ratios within one run survive shared-runner noise where absolute numbers do not.

## 12. Failure modes and mitigations

| Failure | Behavior | Counter | Mitigation / operator action |
|---|---|---|---|
| Feedback loop (module's own `XADD`/`xtrim` events, consumer `xack`/`xclaim` events on `events:*`) | Dropped by prefix guard, first check in the callback | `skipped_self` | None needed; by design |
| Non-stream key at destination | `XADD` returns `WRONGTYPE`, entry dropped | `dropped_xadd_error` | Rename or delete the offending key; restrict `~events:*` writes via ACL |
| `maxmemory` reached | `XADD` refused via `M` flag, entry dropped | `dropped_oom` | Raise `maxmemory`, lower `maxlen`, or narrow the filter |
| Job scheduling failure | Entry dropped | `dropped_defer_error` | Investigate via log; not expected in practice |
| Empty event name after sanitization | Not routed | `skipped_invalid` | None; hostile or buggy co-loaded module |
| `max-streams` cap reached | Entry to a *new* destination stream dropped; existing streams unaffected | `dropped_max_streams` | Raise `eventstream.max-streams`, narrow the filter, or investigate the event-name source (a buggy or hostile co-loaded module firing unbounded names, section 5) |
| Slow consumer | Trimming outruns it; detectable via first-entry ID and `XINFO GROUPS` `lag` | n/a | Alert on lag over ~50 percent of `maxlen`; scale consumers in the group |
| Non-UTF-8 module event name | Captured: the hand-written raw callback decodes the name with `from_utf8_lossy`, so the entry's `event` field carries replacement characters for the invalid bytes (section 5) | n/a | None needed; resolved, see section 17 Q1 |
| Cluster mode | Refuses to load by default; opt-in slot-pinned per-node capture via `eventstream.cluster-streams per-node` (section 10) | n/a | Deploy on standalone/replicated topologies, or opt in to per-node capture on a cluster |
| Node owns no slot (per-node mode) | Tag selection finds no slot that accepts a local write; entry dropped, selection retried on the next captured event | `dropped_no_owned_slot` | Transient while a node joins the cluster or owns no slots; investigate if it persists (section 10) |
| Slot migration window (per-node mode) | Write refused while the pinned slot is `MIGRATING`/`IMPORTING`; the module re-pins and retries once, and an entry still refused is dropped | `dropped_migrating` | Expected during a reshard; alert on increases outside planned resharding (section 10) |
| Panic in module handler code | Caught by `catch_unwind` at the FFI boundary (notification callback and post-notification job); the event is dropped, never an unwind into Redis | `handler_panics` | Any nonzero value is a bug in this module; report it (section 5) |
| Server below 7.2 | Module load fails; process abort at startup, not a clean refusal | n/a | Upgrade; see section 14 |
| Events during unload/downtime | Not mirrored, not recoverable | n/a | Documented gap; not a write-ahead log. Window boundaries are machine-readable via the gap-marker control stream (section 9) |

The module's own writes run server-side with module privileges and are not subject to any client's ACL: a user with no access to `events:*` can still cause writes to those keys by touching watched keys. That is by design (a server-level facility), documented for security review. Consumers need explicit grants, for example `ACL SETUSER consumer on >pw ~events:* +xread +xreadgroup +xack +xautoclaim +xinfo +xlen`.

## 13. Observability

### INFO section

One module INFO section via the wrapper's `InfoContext` builder (`#[info_command_handler]`). Redis prefixes module sections and fields with the module name. All counters are `AtomicU64` statics: process-lifetime, monotonic, reset on load, never persisted or replicated; `skipped_*` counters are incremented inside the notification callback (safe; only keyspace writes are not); `forwarded`, `control_markers`, and `dropped_*` at the write sites (the post-notification job for mirrored entries and pending markers, `deinit` for the unloading marker).

```
# eventstream_stats
eventstream_enabled:1
eventstream_forwarded:48211
eventstream_firehose_forwarded:0
eventstream_dropped:3
eventstream_dropped_xadd_error:3
eventstream_dropped_oom:0
eventstream_dropped_defer_error:0
eventstream_dropped_max_streams:0
eventstream_dropped_encode_error:0
eventstream_skipped_self:1204
eventstream_skipped_filtered:220
eventstream_skipped_key_filtered:0
eventstream_skipped_db:0
eventstream_skipped_invalid:0
eventstream_active_streams:1
eventstream_control_markers:2
eventstream_handler_panics:0
eventstream_dropped_no_owned_slot:0
eventstream_dropped_migrating:0
eventstream_repins:0
eventstream_repins_probe_detected:0
eventstream_cluster_per_node:0
eventstream_cluster_pinned_tag:
eventstream_last_error_time:1752071011
```

`dropped` is the sum of `dropped_xadd_error`, `dropped_oom`, `dropped_defer_error`, `dropped_migrating`, `dropped_max_streams`, and `dropped_encode_error`. `dropped_max_streams` counts events dropped because creating their destination stream would exceed `eventstream.max-streams` (section 7); the stream was never created and existing streams are unaffected. `dropped_encode_error` counts entries dropped because the configured `entry-format` could not encode the event (section 6); with the shipped formats only `json` can fail, on a non-UTF-8 event name, and it is first-failure-logged once per process like the other no-destination drop reasons. `skipped_key_filtered` and `skipped_db` count events dropped in the notification callback by the key-name glob filter and the source-db filter respectively (section 7), kept separate from `skipped_filtered` (the event-name/class filter) so a "forwarded flat while `expired_keys` rises" diagnosis can tell which filter is too narrow. `firehose_forwarded` counts copies written to the firehose stream (section 5) and is not included in `forwarded`, which remains a pure per-event mirrored count; failed firehose copies count in the `dropped_*` counters above. `active_streams` counts stream registrations since load, excluding the control stream: normally the number of distinct destination streams written, but the counter never resets — a flush clears the in-process registry cache, so a stream re-registered after a flush counts again and the value can exceed the number of currently distinct streams (section 5). The firehose, when enabled, is a destination stream and counts. `control_markers` counts gap markers written since load (section 9); marker writes are not counted in `forwarded`, which remains a pure mirrored-event count. `handler_panics` counts panics caught at an FFI boundary, in either the notification callback or a post-notification job (section 5); it should always be 0, and any nonzero value is a bug in this module. `dropped_no_owned_slot`, `dropped_migrating`, `repins`, `repins_probe_detected`, `cluster_per_node`, and `cluster_pinned_tag` are cluster per-node fields (section 10): the count of events dropped for want of an owned slot, events refused in a migration window even after the re-pin retry, the number of times the node re-pinned after its slot migrated away, the subset of re-pins detected by the ownership-probe fallback rather than the recognized error text (nonzero means the string match stopped working), whether per-node mode is active (0/1), and the hash tag this node pinned to (empty until selected). `last_error_time` is the unix-seconds timestamp of the most recent `dropped_*` count (caught handler panics do not stamp it); 0 until the first drop. Config values are otherwise not duplicated into INFO (`CONFIG GET eventstream.*` covers them), and free-form error text stays in the log, not INFO. Per-stream forwarded/dropped counters live in the `EVENTSTREAM.STREAMS WITHSTATS` reply (section 8), never in INFO: one field set per event type ever seen is unbounded cardinality, hostile to INFO scrapers.

Documentation must state plainly: module sections do not appear in default `INFO` or `INFO all`; use `INFO everything`, `INFO eventstream`, or `INFO eventstream_stats`. This is otherwise a recurring support question.

Alerting guidance:

| Signal | Source | Condition |
|---|---|---|
| `eventstream_dropped` | INFO | any increase |
| `eventstream_dropped_max_streams` | INFO | any increase: the `max-streams` cap is refusing new streams (section 7); raise it, narrow the filter, or investigate the name source |
| `eventstream_enabled` | INFO | 0 when expected 1 |
| `eventstream_forwarded` | INFO | flat while `expired_keys` in `INFO stats` rises (filter misconfigured) |
| `eventstream_dropped_migrating` | INFO | any increase outside a planned reshard (section 10 migration window) |
| `eventstream_repins_probe_detected` | INFO | nonzero: the re-pin error-string match stopped working (section 10) |
| Stream size | `XLEN` on `events:*` | unbounded growth (`maxlen` 0 or too high) |
| Consumer lag | `XINFO GROUPS` `lag` | over threshold |

### Logging policy

| Event | Level |
|---|---|
| Module loaded: effective config (prefix, filter, maxlen) | notice |
| `enabled` toggled via `CONFIG SET` | notice |
| `XADD` refused by a destination stream (`dropped_xadd_error`, `dropped_oom` write sites, including gap markers and firehose copies): first failure per stream, with the server's error text; then at most one warning per stream per 60 seconds, carrying the count of failures suppressed since the last warning | warning |
| A previously failing stream writes successfully again: recovery line with the drop count of the ended streak | notice |
| First failure per drop reason with no destination stream in hand (`dropped_defer_error`, `dropped_migrating`, `dropped_no_owned_slot`, `dropped_encode_error`, `SelectDb(0)` failures): once per process | warning |
| First caught handler panic (`handler_panics`) | warning |
| Further failures inside a stream's 60s window, or after a once-per-process reason latched | counted in the drop counters, not logged |
| Per-event trace: event, key, destination | debug |
| Final counter values at unload | notice |

The per-stream state (issue #68) is keyed by destination stream name, one warning window per stream rather than per (stream, reason): the logged line carries the server's error text, which names the reason. A recovery resets the stream's window, so a recurrence hours later logs immediately rather than into a stale window. `dropped_defer_error` stays once-per-process by construction: a job-registration failure happens before the job that resolves the destination name ever runs. `dropped_migrating` and `dropped_no_owned_slot` are node-level conditions (the pinned slot, slot ownership), not stream-level, and keep their process latches. The counters never lose data even when the log says nothing.

### Lifecycle

Load: the `redis_module!` `init:` hook runs after commands, configs, and the keyspace subscription are registered; it performs the version and cluster checks (sections 10, 14), logs the effective config, and records a pending `loaded` gap marker (section 9; never a direct write, which would abort a startup load when the RDB later loads a persisted control stream). Unload is supported: the module registers no native data types, so `MODULE UNLOAD` is not refused with EBUSY; Redis removes the subscription and configs; post-notification jobs queued by prior commands cannot be pending across an unload (they run atomically with their notification), but a job created during the unload itself would fire after the module is dlclosed. `deinit` therefore flushes any pending gap markers directly and clears the pending flag before writing anything: its own writes fire notifications that re-enter the callback during `OnUnload` (Redis does not suppress re-entry there), and with the flag cleared the re-entrant callback cannot register a job. `deinit` then writes the `unloading` gap marker (the one safe direct-write point), logs final counters, and never vetoes. A module loaded with `enabled no` queues `loaded` followed by `disabled`, so the control stream never shows a bare `loaded` closing a gap while capture is off. The `eventstream.enabled` on-changed callback records pending `disabled`/`enabled` markers; it cannot write (its `ConfigurationContext` has no command capability), and it also fires during `RedisModule_LoadConfigs` inside OnLoad, which the pending-marker logic tolerates by design.

## 14. Version requirements

The safe deferred-write path requires `RedisModule_AddPostNotificationJob`, mapped to server 7.2.0 in the wrapper's API version table (`redismodule-rs-macros-internals/src/api_versions.rs`).

| Server | Status |
|---|---|
| 8.x, 7.4 | Supported, same code path; CI runs the full suite on Redis 7.4.5 and 8.8.0 |
| 7.2 | Minimum supported; CI runs the full suite on Redis 7.2.8 |
| Valkey 8.x | Supported; CI runs the full suite on Valkey 8.1.6 (same module ABI and post-notification-job API) |
| Valkey 9.x | Supported; CI runs the full suite on Valkey 9.1.0 (no version-specific code path; the capture path is identical across the matrix) |
| 7.1 and below | Module load fails; on pre-7.2 servers the failure is a process abort at startup, not a clean refusal (below) |

The crate builds with the wrapper's `min-redis-compatibility-version-7-2` feature. Under it, the macro-generated registration path (commands, then configs) unwraps 7.2-only API pointers before `init` ever runs, so on any pre-7.2 server the load panics inside the wrapper and aborts the redis-server process; with `loadmodule` in redis.conf that is a startup abort with the panic in the log, not the polite error message a clean refusal would give (verified against the wrapper source at v2.1.3). The explicit `ctx.get_redis_version()` check in `init` is retained as defense in depth: it is the path that would fire if the wrapper's registration behavior ever becomes graceful, and it documents the requirement in code. Alternatives rejected: writing inside the callback on older servers (documented unsafe, loses atomicity) and buffering through a `DetachedContext` background thread (loses atomicity, can drop on crash, adds GIL contention).

### Module version

The integer version registered with `RedisModule_Init` — the `ver` field in `MODULE LIST` — is the crate version encoded as `major*10000 + minor*100 + patch`, the convention Redis's own modules use: 0.2.0 registers 200, 1.3.7 would register 10307. The value is computed at compile time from `CARGO_PKG_VERSION`, so it cannot drift from Cargo.toml; a version the encoding cannot represent (a non-`major.minor.patch` shape, a pre-release or build suffix, or minor/patch of 100 or more, which would collide with a neighboring release) fails the build rather than registering something wrong. `MODULE LIST` is the server-side surface for auditing which release a running server actually loaded — the check an in-place upgrade runbook needs. Gap markers' `module-version` field (section 9) carries the semver string but only appears when a marker is written and reflects marker-write time, not necessarily the currently loaded module.

### Rust toolchain

MSRV is 1.88, declared as `rust-version` in `Cargo.toml` and gated by the `msrv` leg in `.github/workflows/ci.yml` (a `cargo check --all-targets` on a pinned 1.88 toolchain). The floor is not a taste choice: it is the maximum `rust-version` in the resolved build graph. The git-pinned `redis-module` v2.1.3 pulls `bindgen` → `which` → `home` 0.5.12 (rust-version 1.88) as a build dependency, and the `redis` 1.3 test client (dev-dependency) is also 1.88, so no older toolchain compiles this lockfile. The gate exists so a dependency bump that raises the floor fails loudly on a dedicated leg instead of silently moving the MSRV; a failure there is resolved by a deliberate `rust-version` bump commit, treated as a minor-version event, keeping the Cargo.toml value and the CI toolchain pin in lockstep.

Edition stays 2021, not 2024, despite the maintainer's convention of edition 2024 for new projects in this tree. Bumping to edition 2024 would force rustc >= 1.85 as a hard floor (moot here, since the dependency floor is already 1.88) and require an edition migration of `src/lib.rs` (`cargo fix --edition`) plus re-verification of the pinned wrapper's macro expansions (e.g. `info_command_handler` → `linkme::distributed_slice`) under the new edition. No edition-2024 language feature is currently needed, so the migration buys nothing this crate uses while adding churn and a second thing to verify against the wrapper pin. The edition bump is deferred to whenever the `redis-module` pin next moves and the wrapper is confirmed edition-2024 clean.

## 15. v0.1 scope

The primary need is durable expiration events. v0.1 is the smallest module that serves it correctly.

### Ships

- Four configs: `eventstream.enabled`, `eventstream.stream-prefix` (IMMUTABLE), `eventstream.events`, `eventstream.maxlen`, exactly as in section 7.
- Per-event routing `prefix + sanitize(event)` with the sanitizer of section 5.
- One fixed entry format: `event`, `key`, `db` (section 6).
- Deferred `XADD` via `add_post_notification_job`, through `call_ext` with `!`, `E`, `M` flags and inline `MAXLEN ~`.
- Gates: enabled check, prefix feedback guard, MASTER-only, not-LOADING, filter predicate.
- Gap-marker control stream at `<stream-prefix>#control`: markers on load, enable, disable, and `MODULE UNLOAD`, with `action` and `module-version` fields, delivered via the pending-marker mechanism (section 9).
- Refuse load on Redis below 7.2 and on cluster.
- Drop/skip counters, one module INFO section, plain logging per section 13.
- Zero custom commands in the original v0.1 cut; the v0.1.0 release additionally carries the two readonly introspection commands and the persistent registry added just after (section 8).
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
- `maxlen 0` disables trimming: N writes yield exactly N entries, on the per-event stream and the firehose, via both the module-arg and `CONFIG SET` paths.
- Non-UTF-8 module event names are captured, not a crash: the lossy-decoded name lands in `event` and the `_`-substituted stream, with `handler_panics` and `skipped_invalid` both zero; an empty name increments `skipped_invalid` and registers no stream (fired via the companion notifytest module, the only source of such names).
- `hexpired` routes under an explicit bare name and under `@hash`; the default `expired` filter does not match it (counted `skipped_filtered`); the `del` accompanying the last field's expiry is captured. Capability-gated on `HEXPIRE` presence, not server version. Cross-server caveat: this class membership is Redis-specific — Valkey 9 emits `hexpired` under a different keyspace-notification class, so `@hash` does not select it there; the explicit `hexpired` bare name matches on both lineages. Filter by name, not class, for portable hash-field-expiry capture.
- `eventstream.enabled no` drops events; re-enabling resumes without replay.
- Invalid `CONFIG SET eventstream.events` values are rejected with an error reply.
- Binary (non-UTF-8) key bytes round-trip exactly through the `key` field.
- Events fired in a non-zero database are mirrored to the db 0 streams with the correct `db` field (section 6).
- Gap markers: `loaded` appears after the first post-load notification, `disabled`/`enabled` pair appears after toggling plus one subsequent event each, `unloading` on `MODULE UNLOAD`; any restart (clean or crash) yields a `loaded` marker with no preceding `unloading` or `disabled` (gap detection, section 9); a marker written before a restart survives in the persisted control stream and the server starts normally (pins the no-direct-write-in-init rule).
- Load refusal on a pre-7.2 server (or the version check asserted via a mocked version if spawning old servers is impractical).

## 16. Future work

Each item is additive (new config key, counter, command, or entry field), so nothing needs reserving now.

- `EVENTSTREAM.STATS` and `EVENTSTREAM.STREAMS` commands are implemented (section 8; readonly and keyless as planned, though `STREAMS` is O(N) in registered streams and therefore not flagged `fast`).
- Persistent stream registry: a Redis set at `<prefix>#streams`, SADD-ed (replicated) alongside first write, with in-process dedupe cache invalidated on flush via `FlushSubevent`; source of truth for discovery. Implemented (sections 5 and 8; carried in the v0.1.0 release, section 15). The join with process-local per-stream counters is implemented as `EVENTSTREAM.STREAMS WITHSTATS` (section 8, issue #71).
- Firehose stream at `<prefix>#firehose` behind a bool config is implemented (`eventstream.firehose`, section 5, issue #58): one consumer group over all captured events, off by default; the `#` namespace is protected by the sanitizer as planned.
- Runtime-mutable `stream-prefix`, with the current-plus-previous-prefix guard and documented old-stream cleanup semantics (issue #59).
- Additional entry formats (minimal without `event`, verbose with `class`, JSON) behind an `entry-format` enum config, with a format discriminator and a `dropped_encode_error` counter, are implemented (section 6, issue #60): the module's first enum config, DEFAULT/live-settable, filling the reserved `enum: []` block (section 17 Q4).
- `MISSED`/`NEW` capture via a direct `raw::RedisModule_SubscribeToKeyspaceEvents` subscription is implemented (section 5); the hand-written handler also fixed the non-UTF-8 panic via lossy decode. `LOADED` and `TRIMMED` remain uncapturable by construction (dataset-load and cluster-reshard only).
- Cluster support: the slot-pinned per-node hashtag design shipped in v0.2.0 as `eventstream.cluster-streams per-node` (section 10), including per-node discovery and reshard handling. The original proposal in [docs/cluster-design.md](docs/cluster-design.md) is retained as design history (rewriting it as as-built documentation is issue #83).
- Key-name glob filter (`eventstream.key-filter`, issue #61), source-db filter (`eventstream.source-dbs`, issue #63), and a max-streams cap on distinct event names (`eventstream.max-streams`, issue #64) are implemented (section 7; counters `skipped_key_filtered`, `skipped_db`, and `dropped_max_streams`, section 13). The global monotonic `seq` entry field for cross-stream same-millisecond ordering is implemented (`eventstream.entry-seq`, sections 6 and 9, issue #66): an IMMUTABLE bool, default off, per-node/per-process. Still future work: per-event maxlen overrides (issue #62) and an option to disable `verify_oom` (issue #65).
- Shutdown gap marker via the Shutdown server-event hook: investigated and rejected in #67; no delivery channel survives shutdown (section 9).
- Custom `@eventstream` ACL category (needs `RM_AddACLCategory`, Redis 7.4+, with 7.2/7.3 fallback) (issue #69). Per-stream rate-limited failure logging with recovery notices is implemented (section 13, issue #68).
- Full benchmark matrix (mass-expiry drain p99, maxlen sensitivity) with CI regression gates (issue #70).

## 17. Open questions for the maintainer

1. **Non-UTF-8 module event names panic in the wrapper.** Resolved. The module no longer uses the wrapper's `event_handlers:` macro; its hand-written raw callback (section 5) decodes the event name with `from_utf8_lossy` and wraps the handler in `catch_unwind`, so a non-UTF-8 name is captured (with replacement characters) rather than crashing the server. The upstream issue RedisLabsModules/redismodule-rs#472 remains filed for the benefit of modules that still use the macro.
2. **notify-keyspace-events bypass across versions.** Resolved. The integration suite never sets `notify-keyspace-events`, so it only passes if module keyspace subscribers receive events with the setting empty. CI runs the full suite against Redis 7.2.8, 7.4.5, 8.8.0, Valkey 8.1.6, and Valkey 9.1.0, so every supported server line empirically pins the bypass. Originally verified by reading Redis 7.2 `src/notify.c` (`moduleNotifyKeyspaceEvent()` runs before the config check); now enforced across the matrix rather than asserted.
3. **Is an immutable `stream-prefix` acceptable for launch?** IMMUTABLE deletes real complexity (dual-prefix guard, cleanup semantics) and relaxing later is non-breaking. Recommendation: keep IMMUTABLE unless there is a concrete need to re-prefix without a restart.
4. **`module_args_as_configuration` with three config types.** The macro grammar makes each config-type block optional, but the established wrapper guidance says all four sections must be present when module args are enabled. Resolved 2026-07-09 by experiment against the pinned v2.1.3 tag: omitting the enum section fails to compile, but an empty `enum: []` list compiles and works (module loads, `CONFIG GET eventstream.*` lists the real configs, unprefixed module args are applied). The block was kept as `enum: []` with a code comment until issue #60 added the first real enum config (`eventstream.entry-format`); it now holds that entry, defined with the wrapper's `enum_configuration!` macro (variant names are the byte-exact config strings, so they are lowercase).
