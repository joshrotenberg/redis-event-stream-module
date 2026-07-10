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
//! and the AOF. Requires Redis 7.2+ (`RM_AddPostNotificationJob`); refuses to
//! load in cluster mode (SPEC.md section 10).

// In test builds the redis_module! macro is compiled out (its global allocator
// requires a live Redis), which leaves the handlers unreferenced.
#![cfg_attr(test, allow(dead_code))]

use lazy_static::lazy_static;
use redis_module::configuration::ConfigurationContext;
#[cfg(not(test))]
use redis_module::{
    configuration::ConfigurationFlags, redis_module, server_events::FlushSubevent, InfoContext,
    RedisResult, RedisValue,
};
use redis_module::{
    raw, CallOptions, CallOptionsBuilder, CallResult, ConfigurationValue, Context, ContextFlags,
    NotifyEvent, RedisError, RedisGILGuard, RedisString, Status,
};
#[cfg(not(test))]
use redis_module_macros::{flush_event_handler, info_command_handler};
use std::collections::HashSet;
#[cfg(not(test))]
use std::ffi::CStr;
#[cfg(not(test))]
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// Longest stream-key suffix the sanitizer will emit, in bytes (SPEC.md section 5).
const MAX_EVENT_NAME_LEN: usize = 128;
/// Maximum prefix length in bytes (SPEC.md section 7).
const MAX_PREFIX_LEN: usize = 128;

// Counters (SPEC.md section 13): process-lifetime, monotonic, reset on load.
static FORWARDED: AtomicU64 = AtomicU64::new(0);
static DROPPED_XADD_ERROR: AtomicU64 = AtomicU64::new(0);
static DROPPED_OOM: AtomicU64 = AtomicU64::new(0);
static DROPPED_DEFER_ERROR: AtomicU64 = AtomicU64::new(0);
static SKIPPED_SELF: AtomicU64 = AtomicU64::new(0);
static SKIPPED_FILTERED: AtomicU64 = AtomicU64::new(0);
static SKIPPED_INVALID: AtomicU64 = AtomicU64::new(0);
static CONTROL_MARKERS: AtomicU64 = AtomicU64::new(0);
/// Distinct destination streams written since load, excluding the control
/// stream; the membership set lives in `KNOWN_STREAMS`.
static ACTIVE_STREAMS: AtomicU64 = AtomicU64::new(0);
/// Unix seconds of the most recent drop, 0 if none (SPEC.md section 13).
static LAST_ERROR_TIME: AtomicU64 = AtomicU64::new(0);

/// Previous value of `eventstream.enabled`, used by the on-changed callback to
/// detect transitions. Initialized to the default so the LoadConfigs-time set
/// of the default produces no spurious marker (SPEC.md section 13 lifecycle).
static LAST_ENABLED: AtomicBool = AtomicBool::new(true);
/// Cheap dirty flag so the notification hot path pays one atomic load unless
/// a gap marker is actually pending (SPEC.md section 9 delivery mechanics).
static MARKERS_DIRTY: AtomicBool = AtomicBool::new(false);

// First-failure log latches, one per drop reason (SPEC.md section 13 logging policy).
static LOGGED_XADD_ERROR: AtomicBool = AtomicBool::new(false);
static LOGGED_OOM: AtomicBool = AtomicBool::new(false);
static LOGGED_DEFER_ERROR: AtomicBool = AtomicBool::new(false);
static LOGGED_PANIC: AtomicBool = AtomicBool::new(false);

/// Panics caught at the notification-callback FFI boundary (SPEC.md section 5).
/// A nonzero value is a bug in this module; the counter exists so it surfaces
/// in INFO instead of aborting the server.
static HANDLER_PANICS: AtomicU64 = AtomicU64::new(0);

/// The `MISSED`/`NEW` bits the module subscribed to at load. The keyspace
/// subscription mask is fixed when the module loads and cannot be widened at
/// runtime, so these classes are only capturable if the load-time filter asked
/// for them; a runtime `CONFIG SET` that names an unsubscribed one is rejected
/// (SPEC.md section 5). `EXTRA_UNINIT` until `init` subscribes: the load-time
/// filter `set()` runs before `init` and must not reject.
const EXTRA_UNINIT: i64 = i64::MIN;
static SUBSCRIBED_EXTRA: AtomicI64 = AtomicI64::new(EXTRA_UNINIT);

/// The `MISSED`/`NEW` bits a parsed filter explicitly names via `@class`
/// tokens (not `*`, which adapts to whatever is subscribed).
fn extra_classes_named(f: &ParsedFilter) -> NotifyEvent {
    f.classes & (NotifyEvent::MISSED | NotifyEvent::NEW)
}

static ENABLED: AtomicBool = AtomicBool::new(true);
static MAXLEN: MaxlenConfig = MaxlenConfig {
    value: AtomicI64::new(10_000),
};

/// `eventstream.maxlen` config binding. Redis enforces the registered 0 to
/// i64::MAX range on CONFIG SET and redis.conf paths, but a module-arg value
/// becomes the registered default and bypasses that boundary check entirely
/// (verified against the wrapper at v2.1.3 and redis 7.2 module.c/config.c),
/// so `set()` re-validates: a negative value would silently disable trimming,
/// the module's only memory bound. Rejection aborts the load like any other
/// malformed module arg.
struct MaxlenConfig {
    value: AtomicI64,
}

impl ConfigurationValue<i64> for MaxlenConfig {
    fn get(&self, _ctx: &ConfigurationContext) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
    fn set(&self, _ctx: &ConfigurationContext, val: i64) -> Result<(), RedisError> {
        if val < 0 {
            return Err(RedisError::String(format!(
                "maxlen must be 0 (trimming disabled) or positive, got {val}"
            )));
        }
        self.value.store(val, Ordering::Relaxed);
        Ok(())
    }
}

/// Parsed form of the `eventstream.events` filter (SPEC.md section 7 grammar).
#[derive(Clone, Debug)]
struct ParsedFilter {
    star: bool,
    classes: NotifyEvent,
    names: HashSet<String>,
}

impl Default for ParsedFilter {
    fn default() -> Self {
        ParsedFilter {
            star: false,
            classes: NotifyEvent::empty(),
            names: HashSet::new(),
        }
    }
}

impl ParsedFilter {
    fn matches(&self, event_type: NotifyEvent, event: &str) -> bool {
        self.star || self.classes.intersects(event_type) || self.names.contains(event)
    }
}

/// Map an `@class` token to its `NotifyEvent` bit (SPEC.md section 7 grammar).
/// `missed` and `new` are outside `NOTIFY_ALL`; the module subscribes to them
/// through its own raw subscription, gated at load (SPEC.md section 5).
fn class_bit(class: &str) -> Option<NotifyEvent> {
    // Byte-exact lowercase literals per the SPEC.md section 7 grammar; no
    // case folding (`@HASH` is an unknown class token and is rejected).
    match class {
        "generic" => Some(NotifyEvent::GENERIC),
        "string" => Some(NotifyEvent::STRING),
        "list" => Some(NotifyEvent::LIST),
        "set" => Some(NotifyEvent::SET),
        "hash" => Some(NotifyEvent::HASH),
        "zset" => Some(NotifyEvent::ZSET),
        "stream" => Some(NotifyEvent::STREAM),
        "expired" => Some(NotifyEvent::EXPIRED),
        "evicted" => Some(NotifyEvent::EVICTED),
        "module" => Some(NotifyEvent::MODULE),
        "missed" => Some(NotifyEvent::MISSED),
        "new" => Some(NotifyEvent::NEW),
        _ => None,
    }
}

/// Classes the notification API defines but this module cannot turn into
/// stream entries, each with the reason surfaced in the CONFIG SET error
/// (SPEC.md section 5).
fn uncapturable_class(class: &str) -> Option<&'static str> {
    match class {
        "loaded" => Some(
            "'@loaded' fires only while the server loads its dataset, when stream \
             writes are unavailable (the not-LOADING gate and the deferred-write \
             API both refuse during load); it cannot be captured",
        ),
        "trimmed" => Some(
            "'@trimmed' fires only during cluster reshard trimming, and cluster \
             mode is unsupported (SPEC.md section 10); it cannot be captured",
        ),
        _ => None,
    }
}

/// Parse the filter grammar: `token ("," token)*` where a token is `*`,
/// `@class`, or an exact event name. Rejects empty tokens, unknown classes,
/// and names containing whitespace (SPEC.md section 7).
fn parse_filter(s: &str) -> Result<ParsedFilter, RedisError> {
    let mut filter = ParsedFilter::default();
    for raw_token in s.split(',') {
        let token = raw_token.trim();
        if token.is_empty() {
            return Err(RedisError::String(
                "empty event filter token; to pause the module use 'eventstream.enabled no'"
                    .to_owned(),
            ));
        }
        if token == "*" {
            filter.star = true;
        } else if let Some(class) = token.strip_prefix('@') {
            match class_bit(class) {
                Some(bit) => filter.classes |= bit,
                None => {
                    if let Some(reason) = uncapturable_class(class) {
                        return Err(RedisError::String(reason.to_owned()));
                    }
                    return Err(RedisError::String(format!(
                        "unknown event class '@{class}'"
                    )));
                }
            }
        } else if token.chars().any(char::is_whitespace) {
            return Err(RedisError::String(format!(
                "event name '{token}' contains whitespace"
            )));
        } else {
            filter.names.insert(token.to_owned());
        }
    }
    Ok(filter)
}

/// Sanitize an event name into a stream-key suffix (SPEC.md section 5):
/// `A-Z a-z 0-9 _ . : -` pass through, anything else becomes `_`, truncated
/// to 128 bytes. Every built-in and known module event name passes through
/// byte-identical. An empty result means the event is not routable.
fn sanitize(event: &str) -> String {
    event
        .chars()
        .take(MAX_EVENT_NAME_LEN)
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '.' | ':' | '-' => c,
            _ => '_',
        })
        .collect()
}

/// Validate the stream prefix (SPEC.md section 7): non-empty, at most 128
/// bytes, charset `A-Z a-z 0-9 : . _ - { }`. Glob metacharacters are outside
/// the charset, so the discovery `SCAN MATCH <prefix>*` pattern never needs
/// escaping. An empty prefix would make the feedback guard match every key.
fn validate_prefix(prefix: &str) -> Result<(), RedisError> {
    if prefix.is_empty() {
        return Err(RedisError::Str("stream-prefix must not be empty"));
    }
    if prefix.len() > MAX_PREFIX_LEN {
        return Err(RedisError::String(format!(
            "stream-prefix exceeds {MAX_PREFIX_LEN} bytes"
        )));
    }
    if let Some(bad) = prefix.chars().find(
        |c| !matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | ':' | '.' | '_' | '-' | '{' | '}'),
    ) {
        return Err(RedisError::String(format!(
            "stream-prefix contains disallowed character '{bad}'"
        )));
    }
    Ok(())
}

/// `eventstream.events` config binding: validates via the grammar and stores
/// both the raw string (for CONFIG GET) and the parsed form, which the
/// notification handler (GIL held) reads without extra locking. Rejection from
/// `set()` surfaces as the CONFIG SET error reply (SPEC.md section 7).
struct FilterConfig {
    raw: RedisGILGuard<String>,
    parsed: RedisGILGuard<ParsedFilter>,
}

impl ConfigurationValue<RedisString> for FilterConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.raw.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        let parsed = parse_filter(s)?;
        // At runtime (after init subscribed), reject naming a MISSED/NEW class
        // the load-time subscription does not cover: the mask is fixed at load,
        // so the event would never fire (SPEC.md section 5). Skipped during the
        // load-time set, which runs before init and defines the subscription.
        let subscribed = SUBSCRIBED_EXTRA.load(Ordering::Relaxed);
        if subscribed != EXTRA_UNINIT {
            let missing = extra_classes_named(&parsed).bits() & !(subscribed as i32);
            if missing != 0 {
                let mut names = Vec::new();
                if missing & NotifyEvent::MISSED.bits() != 0 {
                    names.push("@missed");
                }
                if missing & NotifyEvent::NEW.bits() != 0 {
                    names.push("@new");
                }
                return Err(RedisError::String(format!(
                    "{} must be enabled as a load-time module argument; the keyspace \
                     subscription mask is fixed at load and cannot be widened at runtime",
                    names.join(" and ")
                )));
            }
        }
        *self.parsed.lock(ctx) = parsed;
        *self.raw.lock(ctx) = s.to_owned();
        Ok(())
    }
}

/// `eventstream.stream-prefix` config binding. Registered IMMUTABLE, so `set`
/// runs only at load time (defaults and module args); validation failure
/// aborts the load.
struct PrefixConfig {
    value: RedisGILGuard<String>,
}

impl ConfigurationValue<RedisString> for PrefixConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.value.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        validate_prefix(s)?;
        *self.value.lock(ctx) = s.to_owned();
        Ok(())
    }
}

lazy_static! {
    static ref FILTER: FilterConfig = FilterConfig {
        raw: RedisGILGuard::new("expired".to_owned()),
        // Defensive: initialized to the parsed default so the handler behaves
        // correctly even before LoadConfigs applies the registered default.
        parsed: RedisGILGuard::new(
            parse_filter("expired").expect("default filter must parse")
        ),
    };
    static ref PREFIX: PrefixConfig = PrefixConfig {
        value: RedisGILGuard::new("events:".to_owned()),
    };
    /// Gap markers recorded at lifecycle points and written by the next
    /// notification callback's post-notification job (SPEC.md section 9).
    static ref PENDING_MARKERS: RedisGILGuard<Vec<&'static str>> =
        RedisGILGuard::new(Vec::new());
    /// Membership set behind `ACTIVE_STREAMS`; only touched on the capture
    /// path, with the GIL held.
    static ref KNOWN_STREAMS: RedisGILGuard<HashSet<String>> =
        RedisGILGuard::new(HashSet::new());
}

fn xadd_call_options() -> CallOptions {
    // `!` replicate, `E` errors as replies, `M` respect maxmemory
    // (SPEC.md section 10).
    CallOptionsBuilder::new()
        .replicate()
        .errors_as_replies()
        .verify_oom()
        .build()
}

/// Log the first failure per drop reason at warning; subsequent failures are
/// only counted (SPEC.md section 13). Stamps `LAST_ERROR_TIME`.
fn count_drop(ctx: &Context, counter: &AtomicU64, latch: &AtomicBool, detail: &str) {
    counter.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    LAST_ERROR_TIME.store(now, Ordering::Relaxed);
    if latch
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        ctx.log_warning(&format!("eventstream: {detail}"));
    }
}

/// Record a pending gap marker; the next notification callback writes it
/// (SPEC.md section 9 delivery mechanics).
fn record_pending_marker<G: redis_module::RedisLockIndicator>(lock: &G, action: &'static str) {
    PENDING_MARKERS.lock(lock).push(action);
    MARKERS_DIRTY.store(true, Ordering::Relaxed);
}

/// `eventstream.enabled` on-changed callback. Cannot write to the keyspace
/// (the ConfigurationContext has no command capability at v2.1.3), so enable
/// and disable transitions record pending markers. Also fires during
/// LoadConfigs inside OnLoad; `LAST_ENABLED` starting at the default makes
/// that a no-op unless the load args change the value.
fn enabled_changed(config_ctx: &ConfigurationContext, _name: &str, _val: &'static AtomicBool) {
    let now = ENABLED.load(Ordering::Relaxed);
    let before = LAST_ENABLED.swap(now, Ordering::Relaxed);
    if before != now {
        // The ConfigurationContext cannot log through a Context; the
        // module-wide logger works without one (SPEC.md section 13: toggles
        // log at notice).
        redis_module::logging::log_notice(format!(
            "eventstream: enabled set to {}",
            if now { "yes" } else { "no" }
        ));
        record_pending_marker(config_ctx, if now { "enabled" } else { "disabled" });
    }
}

/// Write one gap marker to the control stream. Runs where keyspace writes are
/// safe (a post-notification job or the deinit hook). Same call options,
/// trimming, and drop accounting as mirrored entries (SPEC.md section 9).
fn write_marker(ctx: &Context, control_stream: &str, action: &str, maxlen: i64) {
    let rc = unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) };
    if rc != raw::REDISMODULE_OK as i32 {
        count_drop(
            ctx,
            &DROPPED_XADD_ERROR,
            &LOGGED_XADD_ERROR,
            "SelectDb(0) failed; gap marker dropped",
        );
        return;
    }
    let maxlen_s = maxlen.to_string();
    let mut args: Vec<&[u8]> = Vec::with_capacity(10);
    args.push(control_stream.as_bytes());
    if maxlen > 0 {
        args.push(&b"MAXLEN"[..]);
        args.push(&b"~"[..]);
        args.push(maxlen_s.as_bytes());
    }
    args.push(&b"*"[..]);
    args.push(&b"action"[..]);
    args.push(action.as_bytes());
    args.push(&b"module-version"[..]);
    args.push(env!("CARGO_PKG_VERSION").as_bytes());

    let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
    match res {
        Ok(_) => {
            CONTROL_MARKERS.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_default();
            if msg.starts_with("OOM") {
                count_drop(
                    ctx,
                    &DROPPED_OOM,
                    &LOGGED_OOM,
                    &format!("gap marker '{action}' refused under maxmemory: {msg}"),
                );
            } else {
                count_drop(
                    ctx,
                    &DROPPED_XADD_ERROR,
                    &LOGGED_XADD_ERROR,
                    &format!("gap marker '{action}' failed: {msg}"),
                );
            }
        }
    }
}

/// Drain pending gap markers into a marker-writing post-notification job.
/// Called at the top of the notification callback, ahead of the enabled gate
/// (markers must flush even while disabled), and gated MASTER/not-LOADING
/// like every other write (SPEC.md section 9). Enqueued before any mirrored
/// entry job from the same notification, so markers land first (jobs run in
/// FIFO order).
fn drain_pending_markers(ctx: &Context) {
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }
    let drained: Vec<&'static str> = std::mem::take(&mut *PENDING_MARKERS.lock(ctx));
    MARKERS_DIRTY.store(false, Ordering::Relaxed);
    if drained.is_empty() {
        return;
    }
    let control_stream = format!("{}#control", PREFIX.value.lock(ctx).as_str());
    let maxlen = MAXLEN.value.load(Ordering::Relaxed);
    let dropped_count = drained.len() as u64;
    let status = ctx.add_post_notification_job(move |ctx| {
        for action in &drained {
            write_marker(ctx, &control_stream, action, maxlen);
        }
    });
    if !matches!(status, Status::Ok) {
        // One increment per dropped marker (SPEC.md section 9: marker-write
        // failures follow the same drop-counter policy as mirrored entries).
        count_drop(
            ctx,
            &DROPPED_DEFER_ERROR,
            &LOGGED_DEFER_ERROR,
            "failed to register gap-marker job; markers dropped",
        );
        if dropped_count > 1 {
            DROPPED_DEFER_ERROR.fetch_add(dropped_count - 1, Ordering::Relaxed);
        }
    }
}

/// Keyspace notification callback. Runs with the GIL held; keyspace writes are
/// unsafe here, so the XADD is deferred to a post-notification job. Gate order
/// follows the SPEC.md section 4 diagram.
fn on_keyspace_event(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    // 0. Pending gap markers flush ahead of the enabled gate: the first event
    // after a disable is exactly the boundary the disabled marker timestamps.
    if MARKERS_DIRTY.load(Ordering::Relaxed) {
        drain_pending_markers(ctx);
    }

    // 1. Master switch.
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }

    // 2. Feedback guard: our own XADD/XTRIM activity fires notifications on
    // `<prefix>*` keys; mirroring those would loop forever. Borrow, do not
    // clone: skip paths stay allocation-free (SPEC.md section 11 cost model).
    let prefix = PREFIX.value.lock(ctx);
    if key.starts_with(prefix.as_bytes()) {
        SKIPPED_SELF.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 3. Only a master that is not loading mirrors events; replicas receive
    // the mirrored entries via replication of the master's writes.
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }

    // 4. Filter predicate.
    if !FILTER.parsed.lock(ctx).matches(event_type, event) {
        SKIPPED_FILTERED.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 5. Routable name.
    let suffix = sanitize(event);
    if suffix.is_empty() {
        SKIPPED_INVALID.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 6. Origin database, recorded in the entry's `db` field. The stream
    // itself always lives in db 0 (SPEC.md section 6).
    let db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };

    let stream = format!("{}{}", prefix.as_str(), suffix);
    let registry = format!("{}#streams", prefix.as_str());
    let maxlen = MAXLEN.value.load(Ordering::Relaxed);
    let event_owned = event.to_owned();
    let key_owned = key.to_vec();

    // 7. Deferred write, atomic with the notification.
    let status = ctx.add_post_notification_job(move |ctx| {
        // All destination streams are consolidated in db 0.
        let rc = unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) };
        if rc != raw::REDISMODULE_OK as i32 {
            count_drop(
                ctx,
                &DROPPED_XADD_ERROR,
                &LOGGED_XADD_ERROR,
                "SelectDb(0) failed; entry dropped",
            );
            return;
        }

        // XADD <stream> [MAXLEN ~ <n>] * event <event> key <key> db <db>
        let maxlen_s = maxlen.to_string();
        let db_s = db.to_string();
        let mut args: Vec<&[u8]> = Vec::with_capacity(12);
        args.push(stream.as_bytes());
        if maxlen > 0 {
            args.push(&b"MAXLEN"[..]);
            args.push(&b"~"[..]);
            args.push(maxlen_s.as_bytes());
        }
        args.push(&b"*"[..]);
        args.push(&b"event"[..]);
        args.push(event_owned.as_bytes());
        args.push(&b"key"[..]);
        args.push(key_owned.as_slice());
        args.push(&b"db"[..]);
        args.push(db_s.as_bytes());

        // Per-event trace (SPEC.md section 13); the server filters by
        // loglevel. Key bytes are ASCII-escaped: the wrapper's logger builds
        // a CString and panics across the FFI boundary on interior NUL, so
        // raw key bytes (which may contain NUL) must never reach it.
        ctx.log_debug(&format!(
            "eventstream: {} key={} -> {}",
            event_owned,
            key_owned.escape_ascii(),
            stream
        ));

        let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
        match res {
            Ok(_) => {
                FORWARDED.fetch_add(1, Ordering::Relaxed);
                // First write to a destination stream: register it in the
                // persistent set at `<prefix>#streams` (replicated, so
                // EVENTSTREAM.STREAMS survives restart and works on replicas)
                // and count it. KNOWN_STREAMS is the in-process dedupe cache;
                // it is cleared on flush (see the flush handler) so a FLUSHALL
                // that deleted the registry rebuilds it on the next write. The
                // registry key is under the prefix, so its own SADD
                // notification is dropped by the feedback guard.
                let mut known = KNOWN_STREAMS.lock(ctx);
                if !known.contains(&stream) {
                    let sadd: CallResult = ctx.call_ext(
                        "SADD",
                        &xadd_call_options(),
                        &[registry.as_bytes(), stream.as_bytes()][..],
                    );
                    if sadd.is_ok() {
                        known.insert(stream.clone());
                        ACTIVE_STREAMS.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(e) => {
                let msg = e.to_utf8_string().unwrap_or_default();
                if msg.starts_with("OOM") {
                    count_drop(
                        ctx,
                        &DROPPED_OOM,
                        &LOGGED_OOM,
                        &format!("XADD to '{stream}' refused under maxmemory: {msg}"),
                    );
                } else {
                    count_drop(
                        ctx,
                        &DROPPED_XADD_ERROR,
                        &LOGGED_XADD_ERROR,
                        &format!("XADD to '{stream}' failed: {msg}"),
                    );
                }
            }
        }
    });
    if !matches!(status, Status::Ok) {
        count_drop(
            ctx,
            &DROPPED_DEFER_ERROR,
            &LOGGED_DEFER_ERROR,
            "failed to register post-notification job; event dropped",
        );
    }
}

/// Raw keyspace-notification callback, registered directly rather than through
/// the wrapper's `event_handlers:` macro so the module can subscribe to
/// `MISSED` and `NEW` (which the macro intersects away) and so the FFI boundary
/// is panic-safe: a panic here is undefined behavior that would abort the
/// server, and the wrapper's own handler `unwrap`s a non-UTF-8 event name into
/// exactly such a panic (redismodule-rs#472). This decodes the name lossily and
/// catches any panic, counting it instead (SPEC.md section 5).
#[cfg(not(test))]
extern "C" fn raw_keyspace_event(
    ctx: *mut raw::RedisModuleCtx,
    event_type: c_int,
    event: *const c_char,
    key: *mut raw::RedisModuleString,
) -> c_int {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = Context::new(ctx);
        let key_slice = RedisString::string_as_slice(key);
        let event_name = String::from_utf8_lossy(unsafe { CStr::from_ptr(event) }.to_bytes());
        on_keyspace_event(
            &context,
            NotifyEvent::from_bits_truncate(event_type),
            &event_name,
            key_slice,
        );
    }));
    if outcome.is_err() {
        HANDLER_PANICS.fetch_add(1, Ordering::Relaxed);
        if LOGGED_PANIC
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            redis_module::logging::log_warning(
                "eventstream: notification handler panicked (caught); event dropped",
            );
        }
    }
    raw::Status::Ok as c_int
}

/// Module init: version and topology gates (SPEC.md sections 10 and 14), the
/// keyspace subscription, then log the effective configuration. Compiled out
/// of unit-test builds along with the raw callback it registers.
#[cfg(not(test))]
fn init(ctx: &Context, _args: &[RedisString]) -> Status {
    match ctx.get_redis_version() {
        Ok(v) => {
            if (v.major, v.minor) < (7, 2) {
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

    if ctx.get_flags().contains(ContextFlags::CLUSTER) {
        ctx.log_warning(
            "eventstream does not support cluster mode in v0.1: keyspace notifications are \
             node-local and the destination streams hash to slots this node may not own \
             (SPEC.md section 10); refusing to load",
        );
        return Status::Err;
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

    let prefix = PREFIX.value.lock(ctx).clone();
    let filter = FILTER.raw.lock(ctx).clone();
    ctx.log_notice(&format!(
        "eventstream loaded: stream-prefix='{prefix}' events='{filter}' maxlen={} \
         enabled={} extra-classes={:?}",
        MAXLEN.value.load(Ordering::Relaxed),
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
        pending.push("loaded");
        if !ENABLED.load(Ordering::Relaxed) {
            pending.push("disabled");
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
        let drained: Vec<&'static str> = std::mem::take(&mut *PENDING_MARKERS.lock(ctx));
        MARKERS_DIRTY.store(false, Ordering::Relaxed);
        let control_stream = format!("{}#control", PREFIX.value.lock(ctx).as_str());
        let maxlen = MAXLEN.value.load(Ordering::Relaxed);
        for action in drained {
            write_marker(ctx, &control_stream, action, maxlen);
        }
        write_marker(ctx, &control_stream, "unloading", maxlen);
    }
    ctx.log_notice(&format!(
        "eventstream unloading: forwarded={} dropped={} skipped_self={} skipped_filtered={} \
         skipped_invalid={} control_markers={} active_streams={}",
        FORWARDED.load(Ordering::Relaxed),
        DROPPED_XADD_ERROR.load(Ordering::Relaxed)
            + DROPPED_OOM.load(Ordering::Relaxed)
            + DROPPED_DEFER_ERROR.load(Ordering::Relaxed),
        SKIPPED_SELF.load(Ordering::Relaxed),
        SKIPPED_FILTERED.load(Ordering::Relaxed),
        SKIPPED_INVALID.load(Ordering::Relaxed),
        CONTROL_MARKERS.load(Ordering::Relaxed),
        ACTIVE_STREAMS.load(Ordering::Relaxed),
    ));
    Status::Ok
}

/// Module INFO section (SPEC.md section 13). Redis prefixes the section and
/// every field with the module name: `INFO eventstream` shows
/// `# eventstream_stats` with `eventstream_forwarded` etc. Module sections do
/// not appear in plain `INFO`; use `INFO everything` or `INFO eventstream`.
#[cfg(not(test))]
#[info_command_handler]
fn info_stats(ctx: &InfoContext, _for_crash_report: bool) -> RedisResult<()> {
    let dropped = DROPPED_XADD_ERROR.load(Ordering::Relaxed)
        + DROPPED_OOM.load(Ordering::Relaxed)
        + DROPPED_DEFER_ERROR.load(Ordering::Relaxed);
    ctx.builder()
        .add_section("stats")
        .field("enabled", ENABLED.load(Ordering::Relaxed) as i64)?
        .field("forwarded", FORWARDED.load(Ordering::Relaxed))?
        .field("dropped", dropped)?
        .field(
            "dropped_xadd_error",
            DROPPED_XADD_ERROR.load(Ordering::Relaxed),
        )?
        .field("dropped_oom", DROPPED_OOM.load(Ordering::Relaxed))?
        .field(
            "dropped_defer_error",
            DROPPED_DEFER_ERROR.load(Ordering::Relaxed),
        )?
        .field("skipped_self", SKIPPED_SELF.load(Ordering::Relaxed))?
        .field("skipped_filtered", SKIPPED_FILTERED.load(Ordering::Relaxed))?
        .field("skipped_invalid", SKIPPED_INVALID.load(Ordering::Relaxed))?
        .field("active_streams", ACTIVE_STREAMS.load(Ordering::Relaxed))?
        .field("control_markers", CONTROL_MARKERS.load(Ordering::Relaxed))?
        .field("handler_panics", HANDLER_PANICS.load(Ordering::Relaxed))?
        .field("last_error_time", LAST_ERROR_TIME.load(Ordering::Relaxed))?
        .build_section()?
        .build_info()?;
    Ok(())
}

/// Invalidate the in-process stream registry cache on flush. FLUSHALL (or
/// FLUSHDB in db 0) deletes the `<prefix>#streams` set, so the cache must be
/// cleared for the next capture to re-register its stream. A FLUSHDB in
/// another database does not delete the registry, so clearing here is
/// conservative: the following re-SADD is idempotent, at the cost of
/// re-counting `active_streams`, which is therefore "distinct streams written
/// since load or last flush" (SPEC.md section 5).
#[cfg(not(test))]
#[flush_event_handler]
fn on_flush(ctx: &Context, event: FlushSubevent) {
    if let FlushSubevent::Started = event {
        KNOWN_STREAMS.lock(ctx).clear();
    }
}

/// `EVENTSTREAM.STATS`: the section 13 counters as a flat array of
/// field/value pairs, agreeing with the INFO section at the moment of the
/// call. Readonly, fast, keyless.
#[cfg(not(test))]
fn cmd_stats(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let dropped = DROPPED_XADD_ERROR.load(Ordering::Relaxed)
        + DROPPED_OOM.load(Ordering::Relaxed)
        + DROPPED_DEFER_ERROR.load(Ordering::Relaxed);
    let pairs: [(&str, i64); 13] = [
        ("enabled", ENABLED.load(Ordering::Relaxed) as i64),
        ("forwarded", FORWARDED.load(Ordering::Relaxed) as i64),
        ("dropped", dropped as i64),
        (
            "dropped_xadd_error",
            DROPPED_XADD_ERROR.load(Ordering::Relaxed) as i64,
        ),
        ("dropped_oom", DROPPED_OOM.load(Ordering::Relaxed) as i64),
        (
            "dropped_defer_error",
            DROPPED_DEFER_ERROR.load(Ordering::Relaxed) as i64,
        ),
        ("skipped_self", SKIPPED_SELF.load(Ordering::Relaxed) as i64),
        (
            "skipped_filtered",
            SKIPPED_FILTERED.load(Ordering::Relaxed) as i64,
        ),
        (
            "skipped_invalid",
            SKIPPED_INVALID.load(Ordering::Relaxed) as i64,
        ),
        (
            "active_streams",
            ACTIVE_STREAMS.load(Ordering::Relaxed) as i64,
        ),
        (
            "control_markers",
            CONTROL_MARKERS.load(Ordering::Relaxed) as i64,
        ),
        (
            "handler_panics",
            HANDLER_PANICS.load(Ordering::Relaxed) as i64,
        ),
        (
            "last_error_time",
            LAST_ERROR_TIME.load(Ordering::Relaxed) as i64,
        ),
    ];
    let mut out = Vec::with_capacity(pairs.len() * 2);
    for (name, value) in pairs {
        out.push(RedisValue::SimpleStringStatic(name));
        out.push(RedisValue::Integer(value));
    }
    Ok(RedisValue::Array(out))
}

/// `EVENTSTREAM.STREAMS`: the destination streams registered since the
/// registry existed, read live from the persistent `<prefix>#streams` set so
/// the answer survives restart and works on replicas. The registry is an
/// append-only log of stream names ever written; a listed stream may since
/// have been trimmed to empty or deleted, so this is not a liveness check.
/// Readonly, keyless. The registry lives in db 0, so the command selects db 0
/// for the read and restores the caller's database.
#[cfg(not(test))]
fn cmd_streams(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let registry = format!("{}#streams", PREFIX.value.lock(ctx).as_str());
    let orig_db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    if unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) } != raw::REDISMODULE_OK as i32 {
        return Err(RedisError::Str("failed to select database 0"));
    }
    let members: RedisResult = ctx.call("SMEMBERS", &[registry.as_str()][..]);
    // Restore the caller's database before returning on any path.
    unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, orig_db) };
    // Set membership is unordered; return it as SMEMBERS produced it.
    members
}

// The macro installs the Redis allocator as the global allocator, which aborts
// outside a running Redis; compile it out of unit-test builds.
#[cfg(not(test))]
redis_module! {
    name: "eventstream",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    // Readonly, keyless introspection commands (SPEC.md sections 5, 8). STATS
    // is O(1); STREAMS is O(N) in the number of registered streams, so it is
    // not flagged fast.
    commands: [
        ["eventstream.stats", cmd_stats, "readonly fast", 0, 0, 0, ""],
        ["eventstream.streams", cmd_streams, "readonly", 0, 0, 0, ""],
    ],
    // No event_handlers: the module subscribes to keyspace events itself in
    // init, via a raw callback, so it can request MISSED and NEW (which the
    // macro intersects away) and make the FFI boundary panic-safe.
    configurations: [
        i64: [
            ["maxlen", &MAXLEN, 10000, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["stream-prefix", &*PREFIX, "events:", ConfigurationFlags::IMMUTABLE, None],
            ["events", &*FILTER, "expired", ConfigurationFlags::DEFAULT, None],
        ],
        bool: [
            ["enabled", &ENABLED, true, ConfigurationFlags::DEFAULT, Some(Box::new(enabled_changed))],
        ],
        // The expansion with module_args_as_configuration requires all four
        // config-type lists (verified against v2.1.3; SPEC.md section 17 Q4).
        // The module has no enum configs in v0.1.
        enum: [],
        module_args_as_configuration: true,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_star() {
        let f = parse_filter("*").unwrap();
        assert!(f.matches(NotifyEvent::HASH, "hset"));
        assert!(f.matches(NotifyEvent::empty(), "anything"));
    }

    #[test]
    fn filter_names_exact_case_sensitive() {
        let f = parse_filter("expired,del").unwrap();
        assert!(f.matches(NotifyEvent::EXPIRED, "expired"));
        assert!(f.matches(NotifyEvent::GENERIC, "del"));
        assert!(!f.matches(NotifyEvent::GENERIC, "DEL"));
        assert!(!f.matches(NotifyEvent::HASH, "hset"));
    }

    #[test]
    fn filter_classes() {
        let f = parse_filter("@hash,@expired").unwrap();
        assert!(f.matches(NotifyEvent::HASH, "hset"));
        assert!(f.matches(NotifyEvent::EXPIRED, "expired"));
        assert!(!f.matches(NotifyEvent::STRING, "set"));
        // Class tokens are byte-exact lowercase literals per the grammar.
        assert!(parse_filter("@HASH").is_err());
    }

    #[test]
    fn filter_mixed_and_whitespace_trim() {
        let f = parse_filter(" expired , @hash , json.set ").unwrap();
        assert!(f.matches(NotifyEvent::EXPIRED, "expired"));
        assert!(f.matches(NotifyEvent::HASH, "hdel"));
        assert!(f.matches(NotifyEvent::MODULE, "json.set"));
    }

    #[test]
    fn filter_rejections() {
        assert!(parse_filter("").is_err());
        assert!(parse_filter("expired,").is_err());
        assert!(parse_filter("expired,,del").is_err());
        assert!(parse_filter("@hsah").is_err());
        assert!(parse_filter("foo bar").is_err());
    }

    #[test]
    fn filter_missed_and_new_classes_parse() {
        let f = parse_filter("@missed,@new").unwrap();
        assert!(f.matches(NotifyEvent::MISSED, "keymiss"));
        assert!(f.matches(NotifyEvent::NEW, "new"));
        assert!(!f.matches(NotifyEvent::STRING, "set"));
        assert_eq!(
            extra_classes_named(&f),
            NotifyEvent::MISSED | NotifyEvent::NEW
        );
    }

    #[test]
    fn star_does_not_name_extra_classes() {
        // `*` matches everything delivered but does not force MISSED/NEW into
        // the subscription mask; only explicit tokens do.
        let f = parse_filter("*").unwrap();
        assert_eq!(extra_classes_named(&f), NotifyEvent::empty());
    }

    #[test]
    fn uncapturable_classes_rejected_with_reason() {
        for (token, needle) in [("@loaded", "loads its dataset"), ("@trimmed", "reshard")] {
            let err = parse_filter(token).unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains(needle),
                "reject reason for {token} should mention {needle}, got: {msg}"
            );
        }
    }

    #[test]
    fn sanitize_passthrough_and_replacement() {
        assert_eq!(sanitize("expired"), "expired");
        assert_eq!(sanitize("json.set"), "json.set");
        assert_eq!(sanitize("rename_from"), "rename_from");
        assert_eq!(sanitize("xgroup-create"), "xgroup-create");
        assert_eq!(sanitize("foo bar"), "foo_bar");
        assert_eq!(sanitize("foo?bar"), "foo_bar");
        assert_eq!(sanitize("a#b"), "a_b");
    }

    #[test]
    fn sanitize_truncates_at_128() {
        let long = "x".repeat(300);
        assert_eq!(sanitize(&long).len(), 128);
    }

    #[test]
    fn prefix_validation() {
        assert!(validate_prefix("events:").is_ok());
        assert!(validate_prefix("{tag}events:").is_ok());
        assert!(validate_prefix("").is_err());
        assert!(validate_prefix("ev*ents:").is_err());
        assert!(validate_prefix("ev?ents:").is_err());
        assert!(validate_prefix(&"p".repeat(129)).is_err());
    }
}
