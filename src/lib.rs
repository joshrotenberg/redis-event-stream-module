//! # redis-event-stream-module
//!
//! A Redis module that mirrors keyspace notifications into per-event Redis
//! Streams, making ephemeral pub/sub-style notifications durable, replayable,
//! and consumable through consumer groups.
//!
//! SPEC.md is the authoritative design. Summary of the capture path:
//!
//! ```text
//! notification -> enabled? -> prefix guard -> MASTER/not-LOADING -> filter
//!   -> sanitize -> capture db -> post-notification job:
//!        SelectDb(0); XADD <prefix><event> MAXLEN ~ <n> * event <e> key <k> db <d>
//!        (call_ext with replicate + errors-as-replies + verify-oom)
//! ```
//!
//! All destination streams live in database 0; the entry `db` field records the
//! database where the event fired. The mirrored `XADD` replicates to replicas
//! and the AOF. Requires Redis 7.2+ (`RM_AddPostNotificationJob`). In cluster
//! mode it refuses to load by default; `eventstream.cluster-streams per-node`
//! enables per-node capture with slot-pinned hash tags (SPEC.md section 10,
//! issue #45).

// In test builds the redis_module! macro is compiled out (its global allocator
// requires a live Redis), which leaves the handlers unreferenced.
// `fuzzing` builds, like test builds, compile out the redis_module! macro, so
// the init/deinit/command handlers it would reference become unused.
#![cfg_attr(any(test, feature = "fuzzing"), allow(dead_code, unused_imports))]
mod capture;
mod cluster;
#[cfg(not(test))]
mod commands;
mod config;
mod markers;
mod stats;
// `deinit` is compiled in every build (unlike `init`, which is gated out of
// unit-test builds along with the `redis_module!` macro), so the cross-module
// items and the `Context`/`Status` types it uses stay ungated; the imports only
// the macro/`init` reach are `#[cfg(not(test))]`.
use crate::{capture::*, cluster::*, config::*, markers::*, stats::*};
use redis_module::{Context, ContextFlags, Status};
use std::sync::atomic::Ordering;

/// Public entry points for the coverage-guided fuzz harness (issue #131),
/// compiled only under the `fuzzing` feature the `fuzz/` crate enables — off by
/// default, so the shipped cdylib is unchanged. Each wrapper drives one pure
/// function that ingests untrusted input (`CONFIG SET` values, possibly-hostile
/// co-loaded-module event names) and discards the result: the fuzzer is looking
/// for panics, not return values. Wrapping keeps the internal functions and
/// their return types `pub(crate)` — only these `()` -returning shims are `pub`.
#[cfg(feature = "fuzzing")]
pub mod fuzz_targets {
    /// Drive `parse_filter` (the `eventstream.events` grammar, SPEC.md §7).
    pub fn parse_filter(input: &str) {
        let _ = crate::config::parse_filter(input);
    }
    /// Drive `validate_prefix` (the `eventstream.stream-prefix` check, SPEC.md §7).
    pub fn validate_prefix(input: &str) {
        let _ = crate::config::validate_prefix(input);
    }
    /// Drive `sanitize` (the event-name-to-stream-suffix mapping, SPEC.md §5).
    pub fn sanitize(input: &str) {
        let _ = crate::capture::sanitize(input);
    }
    /// Drive `glob_match` (the key-filter matcher, SPEC.md §7): a hand-written
    /// recursive port of Redis stringmatchlen, fed raw pattern and key bytes.
    pub fn glob_match(pattern: &[u8], key: &[u8]) {
        let _ = crate::config::glob_match(pattern, key);
    }
}

#[cfg(not(test))]
use crate::commands::{cmd_prune, cmd_stats, cmd_streams};
#[cfg(not(test))]
use redis_module::{
    configuration::ConfigurationFlags, raw, redis_module, NotifyEvent, RedisString,
};
/// Integer module version registered with `RedisModule_Init` and reported as
/// `ver` by `MODULE LIST` (SPEC.md section 14): `CARGO_PKG_VERSION` encoded as
/// `major*10000 + minor*100 + patch`, the convention Redis's own modules use
/// (0.2.0 -> 200). Derived at compile time so it cannot drift from Cargo.toml;
/// a version the encoder cannot represent fails the build, never registers 0.
const MODULE_VERSION: i32 = encode_semver(env!("CARGO_PKG_VERSION"));

/// Encodes a plain `major.minor.patch` semver string as
/// `major*10000 + minor*100 + patch`. Evaluated in const context, so every
/// rejection below is a compile error: non-digit bytes (including pre-release
/// or build suffixes, whose ordering the integer cannot express), a component
/// count other than three, an empty component, or minor/patch >= 100, which
/// would collide with a neighboring release's encoding.
const fn encode_semver(v: &str) -> i32 {
    let bytes = v.as_bytes();
    let mut parts = [0i64; 3];
    let mut part = 0;
    let mut digits = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'.' {
            assert!(digits > 0, "empty version component");
            assert!(part < 2, "more than three version components");
            part += 1;
            digits = 0;
        } else {
            assert!(b.is_ascii_digit(), "non-digit in version");
            parts[part] = parts[part] * 10 + (b - b'0') as i64;
            digits += 1;
            // Bounds the accumulator, so overflow checks are unnecessary.
            assert!(parts[part] <= 9999, "version component out of range");
        }
        i += 1;
    }
    assert!(part == 2 && digits > 0, "version must be major.minor.patch");
    assert!(
        parts[1] < 100 && parts[2] < 100,
        "minor and patch must be under 100 to encode unambiguously"
    );
    (parts[0] * 10000 + parts[1] * 100 + parts[2]) as i32
}

/// The 7.2 floor, where `RedisModule_AddPostNotificationJob` (the safe
/// deferred-write path, SPEC.md section 14) first appears. Pulled out of `init`
/// so the boundary is unit-testable: `init` is `#[cfg(not(test))]`, so the
/// mocked-version variant of the SPEC.md section 15 refusal check has to assert
/// against a pure function rather than the gate in context (issue #77). This is
/// defense in depth on a real pre-7.2 server: the wrapper unwraps 7.2-only API
/// pointers during macro-generated registration, before `init` runs, so the
/// load aborts in the wrapper and this gate never fires there — it is the path
/// that would refuse cleanly if registration ever became graceful (SPEC.md
/// section 14).
fn version_supported(major: i32, minor: i32) -> bool {
    (major, minor) >= (7, 2)
}

/// Register the `@eventstream` ACL category (issues #69, #107) and tag the
/// module's commands into it. Done here rather than through the redis_module!
/// macro's `acl_categories` field because the macro makes an RM_AddACLCategory
/// failure fatal, and that call fails on every in-process reload: Redis keeps a
/// module's ACL categories across MODULE UNLOAD, so the category survives and
/// re-adding it errors. The category name is a compile-time constant, so the
/// only possible error is "already exists" (a reload) — benign — and the
/// commands re-tag into the surviving category, so an in-place upgrade loads
/// cleanly. On 7.2/7.3 the API pointer is null: log once and leave the commands
/// individually grantable (SPEC.md section 8). GetCommand + SetCommandACLCategories
/// mirror exactly what the macro would have done, so first-load behavior is
/// unchanged.
#[cfg(not(test))]
fn setup_acl_category(ctx: &Context) {
    let Some(add_category) = (unsafe { raw::RedisModule_AddACLCategory }) else {
        ctx.log_notice(
            "eventstream: this server predates RM_AddACLCategory (Redis 7.4+); the \
             @eventstream ACL category is unavailable, so grant the module's commands \
             individually (+eventstream.stats +eventstream.streams +eventstream.prune)",
        );
        return;
    };
    let category = c"eventstream";
    if unsafe { add_category(ctx.ctx, category.as_ptr()) } != raw::REDISMODULE_OK as i32 {
        // The name is a constant, so this can only be "already registered" from
        // a prior load in this process (an in-place reload); the category is
        // present either way, so tag into it below.
        ctx.log_notice(
            "eventstream: @eventstream ACL category already present (module reloaded \
             in-process); reusing it",
        );
    }
    let (Some(get_command), Some(set_categories)) =
        (unsafe { raw::RedisModule_GetCommand }, unsafe {
            raw::RedisModule_SetCommandACLCategories
        })
    else {
        return;
    };
    for name in [
        c"eventstream.stats",
        c"eventstream.streams",
        c"eventstream.prune",
    ] {
        let command = unsafe { get_command(ctx.ctx, name.as_ptr()) };
        if command.is_null() {
            ctx.log_warning(&format!(
                "eventstream: could not resolve {} to tag it @eventstream",
                name.to_string_lossy()
            ));
            continue;
        }
        if unsafe { set_categories(command, category.as_ptr()) } != raw::REDISMODULE_OK as i32 {
            ctx.log_warning(&format!(
                "eventstream: failed to tag {} into @eventstream",
                name.to_string_lossy()
            ));
        }
    }
}

/// Module init: version and topology gates (SPEC.md sections 10 and 14), the
/// keyspace subscription, then log the effective configuration. Compiled out
/// of unit-test builds along with the raw callback it registers.
#[cfg(not(test))]
fn init(ctx: &Context, _args: &[RedisString]) -> Status {
    match ctx.get_redis_version() {
        Ok(v) => {
            if !version_supported(v.major, v.minor) {
                ctx.log_warning(&format!(
                    "eventstream requires Redis 7.2 or newer (RM_AddPostNotificationJob); \
                     running server is {}.{}.{}",
                    v.major, v.minor, v.patch
                ));
                return Status::Err;
            }
        }
        Err(e) => {
            ctx.log_warning(&format!("eventstream: cannot determine Redis version: {e}"));
            return Status::Err;
        }
    }

    // Direct API-pointer check beside the version gate: the safe deferred-write
    // path is `RedisModule_AddPostNotificationJob`, whose bound pointer is null
    // on any server predating it (7.2) — the same null-pointer signal the 7.4+
    // canonical-name API gives in `select_owned_tag`. Checking the pointer
    // rather than the version string refuses cleanly even if a server ever
    // reports >= 7.2 without providing the symbol. Like the version gate this
    // is unreachable on real old servers today (the wrapper unwraps the pointer
    // during registration, before `init`, and aborts there); retained for the
    // day registration becomes graceful (SPEC.md section 14, issue #77).
    if unsafe { raw::RedisModule_AddPostNotificationJob }.is_none() {
        ctx.log_warning(
            "eventstream requires RedisModule_AddPostNotificationJob (Redis 7.2 or newer); the \
             running server does not provide it; refusing to load.",
        );
        return Status::Err;
    }

    if ctx.get_flags().contains(ContextFlags::CLUSTER) {
        // `eventstream.cluster-streams` decides: `refuse` (default) keeps the
        // historical refusal; `per-node` pins each node's streams to a slot it
        // owns via a shared hash tag (issue #45).
        if CLUSTER_STREAMS.is_per_node(ctx) {
            PER_NODE.store(true, Ordering::Relaxed);
            ctx.log_notice(
                "eventstream cluster per-node mode: this node pins its streams to a slot it \
                 owns via a shared hash tag; the tag is selected on the first captured event \
                 (issue #45). No dynamic re-pinning yet (#46).",
            );
        } else {
            ctx.log_warning(
                "eventstream refuses to load in cluster mode (keyspace notifications are \
                 node-local and a fixed stream name hashes to a slot this node may not own, \
                 SPEC.md section 10). Set eventstream.cluster-streams per-node to enable \
                 per-node capture; refusing to load.",
            );
            return Status::Err;
        }
    }

    // Subscribe to keyspace events. NOTIFY_ALL always, plus MISSED and NEW only
    // when the load-time filter names them (a `*` filter counts): both classes
    // fire on high-volume paths (every read miss, every new key), so subscribing
    // to them unconditionally would tax deployments that do not want them
    // (SPEC.md section 5). The mask is fixed here; FILTER::set rejects widening
    // it at runtime.
    let load_filter = FILTER.parsed.lock(ctx).clone();
    let mut extra = NotifyEvent::empty();
    if load_filter.star || load_filter.classes.contains(NotifyEvent::MISSED) {
        extra |= NotifyEvent::MISSED;
    }
    if load_filter.star || load_filter.classes.contains(NotifyEvent::NEW) {
        extra |= NotifyEvent::NEW;
    }
    let mask = NotifyEvent::ALL | extra;
    let rc = unsafe {
        raw::RedisModule_SubscribeToKeyspaceEvents.unwrap()(
            ctx.ctx,
            mask.bits(),
            Some(raw_keyspace_event),
        )
    };
    if rc != raw::REDISMODULE_OK as i32 {
        ctx.log_warning("eventstream: failed to subscribe to keyspace events; refusing to load");
        return Status::Err;
    }
    SUBSCRIBED_EXTRA.store(extra.bits() as i64, Ordering::Relaxed);

    // @eventstream ACL category (issues #69, #107). Registered here, not through
    // the redis_module! macro's `acl_categories` field: the macro treats an
    // RM_AddACLCategory failure as fatal, and that call fails on every in-process
    // reload. Redis does not remove a module's ACL categories on MODULE UNLOAD,
    // so the category from the previous load survives and re-adding it errors —
    // which would abort an in-place upgrade (UNLOAD then LOAD). Doing it here
    // lets the load tolerate the already-exists error and re-tag the commands
    // into the surviving category.
    setup_acl_category(ctx);

    // Subscribe to the flush and SWAPDB server events through the raw
    // `RedisModule_SubscribeToServerEvent` binding rather than the wrapper's
    // `#[flush_event_handler]` macro. The safe wrapper delivers only
    // `FlushSubevent::Started/Ended` and discards the `RedisModuleFlushInfo`
    // payload, and has no SwapDB wrapper at all; the flushed and swapped db
    // numbers are exactly what the `flushed`/`swapdb` markers carry (issues
    // #74, #73). Same raw-binding rationale as the keyspace subscription above,
    // and the same panic-safety boundary (both callbacks catch panics). A
    // failure here is not fatal: capture still works, only the flush/swap gap
    // markers are lost, so log and continue rather than refusing to load.
    let subscribe_server_event = |event_id: u64, cb: raw::RedisModuleEventCallback| unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: event_id,
                dataver: 1,
            },
            cb,
        )
    };
    let flush_rc = subscribe_server_event(raw::REDISMODULE_EVENT_FLUSHDB, Some(raw_flush_event));
    let swapdb_rc = subscribe_server_event(raw::REDISMODULE_EVENT_SWAPDB, Some(raw_swapdb_event));
    if flush_rc != raw::REDISMODULE_OK as i32 || swapdb_rc != raw::REDISMODULE_OK as i32 {
        ctx.log_warning(
            "eventstream: failed to subscribe to a flush/swapdb server event; the \
             corresponding gap markers will not be written",
        );
    }
    // Eviction-risk tracking (issue #106): subscribe to config changes so the
    // flag follows runtime `CONFIG SET maxmemory-policy`, then read it once now
    // for the load-time state (which also emits the warning if already risky). A
    // subscription failure only stops runtime tracking; the load-time read below
    // still runs.
    if subscribe_server_event(raw::REDISMODULE_EVENT_CONFIG, Some(raw_config_event))
        != raw::REDISMODULE_OK as i32
    {
        ctx.log_warning(
            "eventstream: failed to subscribe to the config-change server event; \
             eviction_risk will not update on runtime maxmemory-policy changes",
        );
    }
    recheck_eviction_risk(ctx);

    let prefix = PREFIX.value.lock(ctx).clone();
    let filter = FILTER.raw.lock(ctx).clone();
    let key_filter = KEY_FILTER.raw.lock(ctx).clone();
    let source_dbs = SOURCE_DBS.raw.lock(ctx).clone();
    // Surface the per-event override count (issue #62) alongside the effective
    // retention config (issues #108, #65) in the load notice; the full override
    // map is available via `CONFIG GET eventstream.maxlen-overrides`.
    let overrides_count = MAXLEN_OVERRIDES.parsed.lock(ctx).len();
    ctx.log_notice(&format!(
        "eventstream loaded: stream-prefix='{prefix}' events='{filter}' key-filter='{key_filter}' \
         source-dbs='{source_dbs}' maxlen={} maxlen-overrides={overrides_count} retention-ms={} \
         verify-oom={} max-streams={} enabled={} extra-classes={:?}",
        MAXLEN.value.load(Ordering::Relaxed),
        RETENTION_MS.value.load(Ordering::Relaxed),
        VERIFY_OOM.load(Ordering::Relaxed),
        MAX_STREAMS.value.load(Ordering::Relaxed),
        ENABLED.load(Ordering::Relaxed),
        extra,
    ));

    // Record the loaded gap marker as pending; a direct write here would
    // abort startup loads when the RDB later loads a persisted control stream
    // (SPEC.md section 9 delivery mechanics). Clear anything queued by
    // LoadConfigs-time transitions first: the fresh-load state supersedes
    // them. If the effective config starts disabled (enabled no as a module
    // arg), re-queue the disabled state after loaded, otherwise a bare loaded
    // marker would close the capture gap while every event is being dropped.
    {
        let mut pending = PENDING_MARKERS.lock(ctx);
        pending.clear();
        pending.push(PendingMarker::Simple("loaded"));
        if !ENABLED.load(Ordering::Relaxed) {
            pending.push(PendingMarker::Simple("disabled"));
        }
    }
    MARKERS_DIRTY.store(true, Ordering::Relaxed);

    Status::Ok
}

/// Module deinit, run inside `MODULE UNLOAD`: the one lifecycle point where a
/// direct write is safe and no future notification exists to defer to. Writes
/// the `unloading` gap marker and logs final counters (SPEC.md section 13).
fn deinit(ctx: &Context) -> Status {
    let flags = ctx.get_flags();
    if flags.contains(ContextFlags::MASTER) && !flags.contains(ContextFlags::LOADING) {
        // Drain pending markers and clear the dirty flag BEFORE any direct
        // write: the write's own xadd notification re-enters the callback
        // during OnUnload (Redis does not suppress re-entry there), and a
        // step-0 drain at that point would register a post-notification job
        // that fires after the module is dlclosed, a use-after-free. With the
        // flag cleared, the re-entrant callback falls through to the prefix
        // guard. Writing the drained markers directly here also preserves
        // them instead of orphaning them at unload.
        let drained: Vec<PendingMarker> = std::mem::take(&mut *PENDING_MARKERS.lock(ctx));
        MARKERS_DIRTY.store(false, Ordering::Relaxed);
        // deinit runs inside MODULE UNLOAD, a write-safe context, so the tag
        // can be resolved here. If the node owns no slot, skip the markers
        // rather than fail the unload.
        if let Some(seg) = tag_segment(ctx) {
            let control_stream = format!("{}{seg}#control", PREFIX.value.lock(ctx).as_str());
            // Same control-stream retention resolution as the live path (issues
            // #62, #108): `#control` override else the global cap, plus
            // time-based retention when set.
            let control_ret = Retention {
                maxlen: effective_maxlen(
                    &MAXLEN_OVERRIDES.parsed.lock(ctx),
                    "#control",
                    MAXLEN.value.load(Ordering::Relaxed),
                ),
                retention_ms: RETENTION_MS.value.load(Ordering::Relaxed),
            };
            for marker in drained {
                write_marker(
                    ctx,
                    &control_stream,
                    marker.action(),
                    marker.db(),
                    control_ret,
                );
            }
            write_marker(ctx, &control_stream, "unloading", None, control_ret);
        }
    }
    // The final counters as one complete snapshot, from the same shared source
    // as the INFO section and EVENTSTREAM.STATS (issue #88), so the unload line
    // never drifts from them and carries every diagnostic counter — notably
    // handler_panics, whose nonzero value means a bug in this module (SPEC.md
    // section 13), and the cluster per-node counters.
    let counters = stats_snapshot()
        .into_iter()
        .map(|(name, value)| match value {
            StatValue::Int(i) => format!("{name}={i}"),
            StatValue::Text(s) => format!("{name}={s}"),
        })
        .collect::<Vec<_>>()
        .join(" ");
    ctx.log_notice(&format!("eventstream unloading: {counters}"));
    Status::Ok
}

// The macro installs the Redis allocator as the global allocator, which aborts
// outside a running Redis; compile it out of unit-test builds.
// Excluded from `fuzzing` builds as well as `test` builds: the macro installs
// a #[global_allocator] (RedisAlloc) that delegates to the RedisModule_Alloc
// pointer, which is null outside a live server — a libFuzzer binary would abort
// on its first allocation. The rest of the module compiles as inert dead code
// in the fuzz rlib; the fuzz targets call only the pure parser/sanitizer
// functions (issue #131).
#[cfg(not(any(test, feature = "fuzzing")))]
redis_module! {
    name: "eventstream",
    version: MODULE_VERSION,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    // The @eventstream ACL category (issue #69) is NOT declared here. The macro
    // makes RM_AddACLCategory fatal, which breaks in-place reload (issue #107):
    // Redis keeps module ACL categories across MODULE UNLOAD, so re-adding the
    // category on a second load aborts. `init` registers it manually instead
    // (see setup_acl_category), tolerating the already-exists error and tagging
    // the commands into the category itself.
    init: init,
    deinit: deinit,
    // Keyless commands (SPEC.md sections 5, 8). STATS is O(1) and `readonly
    // fast`. STREAMS is O(N) in the number of registered streams (so not
    // flagged `fast`) and stays `readonly`: bare/WITHSTATS/VERBOSE issue no
    // writes, so discovery must keep working on replicas (issue #81). PRUNE is
    // the separate opt-in cleanup: it mutates the registry with a replicated
    // `SREM`, so it — and only it — is registered `write` (routed to the
    // primary), keeping STREAMS off the write path. The @eventstream ACL
    // category is attached in `init` (setup_acl_category), not via the macro's
    // optional 8th tuple field, so an in-place reload survives (issue #107); the
    // 7th (mandatory) field stays empty so no server fails the load over ACL
    // wiring (issue #69).
    commands: [
        ["eventstream.stats", cmd_stats, "readonly fast", 0, 0, 0, ""],
        ["eventstream.streams", cmd_streams, "readonly", 0, 0, 0, ""],
        ["eventstream.prune", cmd_prune, "write", 0, 0, 0, ""],
    ],
    // No event_handlers: the module subscribes to keyspace events itself in
    // init, via a raw callback, so it can request MISSED and NEW (which the
    // macro intersects away) and make the FFI boundary panic-safe.
    configurations: [
        i64: [
            ["maxlen", &MAXLEN, 10000, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
            ["max-streams", &MAX_STREAMS, 0, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
            // Time-based retention (issue #108): 0 disables (count-based only),
            // >0 trims by MINID over a `retention-ms` window, taking precedence
            // over `maxlen`. Same module-arg boundary-bypass re-validation as
            // `maxlen`/`max-streams`.
            ["retention-ms", &RETENTION_MS, 0, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["stream-prefix", &*PREFIX, "events:", ConfigurationFlags::IMMUTABLE, None],
            ["events", &*FILTER, "expired", ConfigurationFlags::DEFAULT, None],
            ["key-filter", &*KEY_FILTER, "*", ConfigurationFlags::DEFAULT, None],
            ["source-dbs", &*SOURCE_DBS, "*", ConfigurationFlags::DEFAULT, None],
            // Per-event maxlen overrides (issue #62): `event=cap` pairs keyed by
            // destination stream suffix, overriding the global `maxlen`. Default
            // empty. Runtime-mutable like `maxlen`.
            ["maxlen-overrides", &*MAXLEN_OVERRIDES, "", ConfigurationFlags::DEFAULT, None],
            ["cluster-streams", &*CLUSTER_STREAMS, "refuse", ConfigurationFlags::IMMUTABLE, None],
            // Consumer-group auto-provisioning (issue #109): empty (default)
            // leaves group creation operator-side, unchanged. A non-empty name
            // makes the module `XGROUP CREATE <stream> <name> 0` on each
            // destination stream at first write. Runtime-mutable; setting it
            // provisions on the next write to each stream, not retroactively.
            // No on-changed callback: the ConfigurationContext cannot issue
            // commands, and next-write provisioning needs none (the write path
            // reads `AUTO_GROUP_ENABLED`, set in `set()`).
            ["auto-group", &*AUTO_GROUP, "", ConfigurationFlags::DEFAULT, None],
        ],
        bool: [
            ["enabled", &ENABLED, true, ConfigurationFlags::DEFAULT, Some(Box::new(enabled_changed))],
            // No on-changed callback: unlike `enabled`, toggling the firehose
            // opens no capture gap (per-event mirroring continues), so there
            // is no marker to record (issue #58).
            ["firehose", &FIREHOSE, false, ConfigurationFlags::DEFAULT, None],
            // `verify-oom` (issue #65): default yes keeps the `M` flag on
            // mirrored writes (refuse-and-count under maxmemory). No on-changed
            // callback: `xadd_call_options` reads the atomic per XADD, so a live
            // toggle needs no cached-options invalidation.
            ["verify-oom", &VERIFY_OOM, true, ConfigurationFlags::DEFAULT, None],
            // `entry-seq` (issue #66): IMMUTABLE, so within one process every
            // stream's field set is uniform (always has `seq` or never), which
            // preserves the SAMEFIELDS compaction. Default off, so existing
            // deployments see no schema change.
            ["entry-seq", &ENTRY_SEQ, false, ConfigurationFlags::IMMUTABLE, None],
        ],
        // The module's first enum config (issue #60), filling the block the
        // empty `enum: []` placeholder reserved (SPEC.md section 17 Q4). The
        // default `fixed` variant reproduces the historical schema; the enum
        // is runtime-mutable (DEFAULT), so the `format` discriminator carries
        // the load-bearing weight for mixed-format streams (SPEC.md section 6).
        enum: [
            ["entry-format", &*ENTRY_FORMAT, EntryFormat::fixed, ConfigurationFlags::DEFAULT, None],
        ],
        module_args_as_configuration: true,
    ]
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_gate_refuses_below_7_2() {
        // The SPEC.md section 15 refusal check, mocked-version variant (issue
        // #77): the real pre-7.2 refusal aborts in the wrapper before `init`
        // runs and CI has no pre-7.2 server, so the 7.2 floor is pinned here
        // against the extracted gate. Below 7.2 refuses; 7.2 (where
        // `RM_AddPostNotificationJob` lands) and every later line the CI matrix
        // and the supported Redis/Valkey lineages report is accepted. 7.0 is
        // closed issue #9's example server.
        assert!(!version_supported(6, 2));
        assert!(!version_supported(7, 0));
        assert!(!version_supported(7, 1));
        assert!(version_supported(7, 2));
        assert!(version_supported(7, 4));
        assert!(version_supported(8, 0));
        assert!(version_supported(9, 0));
    }

    #[test]
    fn semver_encoding_pins() {
        // The `MODULE LIST` `ver` encoding (SPEC.md section 14): these vectors
        // are the documented mapping, so a change here is a breaking change to
        // the operator-facing version surface.
        assert_eq!(encode_semver("0.1.0"), 100);
        assert_eq!(encode_semver("0.2.0"), 200);
        assert_eq!(encode_semver("1.3.7"), 10307);
        assert_eq!(encode_semver("12.34.56"), 123456);
    }

    #[test]
    fn module_version_tracks_crate_version() {
        // Independent parse of CARGO_PKG_VERSION through std, so a bug in the
        // const encoder cannot silently agree with itself.
        let mut parts = env!("CARGO_PKG_VERSION")
            .split('.')
            .map(|p| p.parse::<i32>().expect("numeric component"));
        let (maj, min, pat) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        assert_eq!(parts.next(), None);
        assert_eq!(MODULE_VERSION, maj * 10000 + min * 100 + pat);
    }

    #[test]
    #[should_panic(expected = "major.minor.patch")]
    fn semver_encoding_rejects_two_components() {
        encode_semver("0.2");
    }

    #[test]
    #[should_panic(expected = "non-digit")]
    fn semver_encoding_rejects_prerelease_suffix() {
        encode_semver("1.0.0-rc.1");
    }

    #[test]
    #[should_panic(expected = "under 100")]
    fn semver_encoding_rejects_wide_minor() {
        encode_semver("1.100.0");
    }
}
