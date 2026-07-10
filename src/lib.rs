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
use redis_module::{configuration::ConfigurationFlags, redis_module};
use redis_module::{
    raw, CallOptions, CallOptionsBuilder, CallResult, ConfigurationValue, Context, ContextFlags,
    NotifyEvent, RedisError, RedisGILGuard, RedisResult, RedisString, RedisValue, Status,
};
use std::collections::HashSet;
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

// First-failure log latches, one per drop reason (SPEC.md section 13 logging policy).
static LOGGED_XADD_ERROR: AtomicBool = AtomicBool::new(false);
static LOGGED_OOM: AtomicBool = AtomicBool::new(false);
static LOGGED_DEFER_ERROR: AtomicBool = AtomicBool::new(false);

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
#[derive(Clone)]
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

/// Map an `@class` token to its `NotifyEvent` bit. Only classes inside
/// `NOTIFY_ALL` are accepted; `missed`/`new`/`loaded`/`trimmed` are not
/// capturable in v0.1 (SPEC.md section 5).
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
                    return Err(RedisError::String(format!(
                        "unknown event class '@{class}'"
                    )))
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
/// only counted (SPEC.md section 13).
fn count_drop(ctx: &Context, counter: &AtomicU64, latch: &AtomicBool, detail: &str) {
    counter.fetch_add(1, Ordering::Relaxed);
    if latch
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        ctx.log_warning(&format!("eventstream: {detail}"));
    }
}

/// Keyspace notification callback. Runs with the GIL held; keyspace writes are
/// unsafe here, so the XADD is deferred to a post-notification job. Gate order
/// follows the SPEC.md section 4 diagram.
fn on_keyspace_event(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
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

    let stream = format!("{}{}", &*prefix, suffix);
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

        let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
        match res {
            Ok(_) => {
                FORWARDED.fetch_add(1, Ordering::Relaxed);
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

/// Module init: version and topology gates (SPEC.md sections 10 and 14), then
/// log the effective configuration.
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

    let prefix = PREFIX.value.lock(ctx).clone();
    let filter = FILTER.raw.lock(ctx).clone();
    ctx.log_notice(&format!(
        "eventstream loaded: stream-prefix='{prefix}' events='{filter}' maxlen={} enabled={}",
        MAXLEN.value.load(Ordering::Relaxed),
        ENABLED.load(Ordering::Relaxed),
    ));
    Status::Ok
}

/// `EVENTSTREAM.STATS`: temporary observability surface. Replaced by the
/// module INFO section in the observability PR (issue #10); v0.1 ships zero
/// custom commands per SPEC.md section 8.
fn stats(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let prefix = PREFIX.value.lock(ctx).clone();
    let filter = FILTER.raw.lock(ctx).clone();
    Ok(RedisValue::Array(vec![
        RedisValue::SimpleStringStatic("enabled"),
        RedisValue::Integer(ENABLED.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("stream-prefix"),
        RedisValue::BulkString(prefix),
        RedisValue::SimpleStringStatic("events"),
        RedisValue::BulkString(filter),
        RedisValue::SimpleStringStatic("maxlen"),
        RedisValue::Integer(MAXLEN.value.load(Ordering::Relaxed)),
        RedisValue::SimpleStringStatic("forwarded"),
        RedisValue::Integer(FORWARDED.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("dropped_xadd_error"),
        RedisValue::Integer(DROPPED_XADD_ERROR.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("dropped_oom"),
        RedisValue::Integer(DROPPED_OOM.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("dropped_defer_error"),
        RedisValue::Integer(DROPPED_DEFER_ERROR.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("skipped_self"),
        RedisValue::Integer(SKIPPED_SELF.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("skipped_filtered"),
        RedisValue::Integer(SKIPPED_FILTERED.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("skipped_invalid"),
        RedisValue::Integer(SKIPPED_INVALID.load(Ordering::Relaxed) as i64),
    ]))
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
    commands: [
        ["eventstream.stats", stats, "readonly", 0, 0, 0, ""],
    ],
    event_handlers: [
        [@ALL: on_keyspace_event],
    ],
    configurations: [
        i64: [
            ["maxlen", &MAXLEN, 10000, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["stream-prefix", &*PREFIX, "events:", ConfigurationFlags::IMMUTABLE, None],
            ["events", &*FILTER, "expired", ConfigurationFlags::DEFAULT, None],
        ],
        bool: [
            ["enabled", &ENABLED, true, ConfigurationFlags::DEFAULT, None],
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
        assert!(parse_filter("@missed").is_err());
        assert!(parse_filter("@new").is_err());
        assert!(parse_filter("foo bar").is_err());
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
