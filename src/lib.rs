//! # redis-event-stream-module
//!
//! A Redis module that turns ephemeral keyspace notifications into a durable,
//! replayable log by mirroring each event into a Redis Stream, routed per event
//! name.
//!
//! Redis keyspace notifications are delivered over Pub/Sub, which is fire and
//! forget: a client that is disconnected when an event fires never sees it.
//! This module subscribes to keyspace events inside the server and re-emits each
//! one as an `XADD` into a per-event stream, so consumers can read them with
//! `XREAD` or consumer groups and never miss one, even across restarts.
//!
//! ## Routing
//!
//! Events are routed by event name into separate streams named
//! `<prefix><event>`. With the default `events:` prefix:
//!
//! - key expirations -> `events:expired`
//! - `SET` commands  -> `events:set`
//! - `HSET` commands -> `events:hset`
//! - `DEL` commands  -> `events:del`
//!
//! Each entry carries `event` (the event name) and `key` (the affected key,
//! binary-safe); `Verbose` format adds a `class` field with the notification
//! class (e.g. `expired`, `hash`, `generic`). The stream entry ID supplies the
//! timestamp.
//!
//! ## Configuration (settable at load and live via `CONFIG SET`)
//!
//! - `eventstream.enabled` (bool, default `yes`): master on/off switch
//! - `eventstream.prefix`  (string, default `events:`): destination stream prefix
//! - `eventstream.events`  (string, default `all`): `all`/`*`, or a comma list
//!   of event names to capture, e.g. `expired,del,hset`
//! - `eventstream.maxlen`  (i64, default `10000`): approximate `MAXLEN` per
//!   stream so streams self-trim; `0` disables trimming
//! - `eventstream.format`  (enum, default `Minimal`): `Minimal` or `Verbose`
//!
//! ## Requirements and caveats
//!
//! This scaffold is the working baseline; SPEC.md is the authoritative design.
//!
//! - The server must have keyspace notifications enabled for the classes you
//!   want, e.g. `notify-keyspace-events "KEA"` (or at least `Ex` for
//!   expirations). This module reads them; it does not enable them.
//! - `expired` fires when Redis actually removes the key (lazy or active
//!   expire), not at the exact instant the TTL elapses.
//! - Writes use `RM_Call("XADD", ...)` and are not flagged for replication, so
//!   mirrored streams live on the primary only in this baseline.
//! - Requires `RM_AddPostNotificationJob` (Redis 7.2+).

// In test builds the redis_module! macro is compiled out (its global allocator
// requires a live Redis), which leaves the handlers unreferenced.
#![cfg_attr(test, allow(dead_code))]

use lazy_static::lazy_static;
#[cfg(not(test))]
use redis_module::{configuration::ConfigurationFlags, redis_module};
use redis_module::{
    enum_configuration, Context, NotifyEvent, RedisResult, RedisString, RedisValue, Status,
};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;

enum_configuration! {
    /// Controls how much detail each mirrored stream entry carries.
    enum EntryFormat {
        /// Fields: `event`, `key`.
        Minimal = 0,
        /// Adds a `class` field with the notification class (e.g. `expired`, `hash`).
        Verbose = 1,
    }
}

static ENABLED: AtomicBool = AtomicBool::new(true);
static MAXLEN: AtomicI64 = AtomicI64::new(10_000);
static FORWARDED: AtomicI64 = AtomicI64::new(0);

lazy_static! {
    static ref PREFIX: Mutex<String> = Mutex::new("events:".to_string());
    static ref EVENTS: Mutex<String> = Mutex::new("all".to_string());
    static ref FORMAT: Mutex<EntryFormat> = Mutex::new(EntryFormat::Minimal);
}

fn format_is_verbose() -> bool {
    i32::from(FORMAT.lock().unwrap().clone()) == i32::from(EntryFormat::Verbose)
}

/// Map a notification class bit to a human-readable class name, used by the
/// `Verbose` entry format.
fn class_name(t: NotifyEvent) -> &'static str {
    if t.contains(NotifyEvent::EXPIRED) {
        "expired"
    } else if t.contains(NotifyEvent::EVICTED) {
        "evicted"
    } else if t.contains(NotifyEvent::GENERIC) {
        "generic"
    } else if t.contains(NotifyEvent::STRING) {
        "string"
    } else if t.contains(NotifyEvent::LIST) {
        "list"
    } else if t.contains(NotifyEvent::SET) {
        "set"
    } else if t.contains(NotifyEvent::HASH) {
        "hash"
    } else if t.contains(NotifyEvent::ZSET) {
        "zset"
    } else if t.contains(NotifyEvent::STREAM) {
        "stream"
    } else if t.contains(NotifyEvent::MODULE) {
        "module"
    } else {
        "other"
    }
}

/// Decide whether an event should be captured, given the `events` config value
/// (`all`/`*` or a comma list of event names).
fn should_capture(event: &str, events_cfg: &str) -> bool {
    let e = events_cfg.trim();
    if e.eq_ignore_ascii_case("all") || e == "*" {
        return true;
    }
    e.split(',').any(|c| c.trim().eq_ignore_ascii_case(event))
}

/// Keyspace notification callback. Runs with the GIL held; writing to Redis
/// here is unsafe, so the actual `XADD` is deferred to a post-notification job.
fn on_keyspace_event(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }

    let prefix = PREFIX.lock().unwrap().clone();

    // Feedback guard: our own destination streams live under `prefix`, and
    // every XADD/XTRIM we perform generates further keyspace events. Never
    // mirror those, otherwise the module feeds itself in an infinite loop.
    if key.starts_with(prefix.as_bytes()) {
        return;
    }

    let events_cfg = EVENTS.lock().unwrap().clone();
    if !should_capture(event, &events_cfg) {
        return;
    }

    let stream = format!("{prefix}{event}");
    let maxlen = MAXLEN.load(Ordering::Relaxed);
    let verbose = format_is_verbose();
    let class = class_name(event_type);
    let event_owned = event.to_owned();
    let key_owned = key.to_vec();

    let status = ctx.add_post_notification_job(move |ctx| {
        // XADD <stream> [MAXLEN ~ <n>] * event <event> key <key> [class <class>]
        let maxlen_s = maxlen.to_string();
        let mut args: Vec<&[u8]> = Vec::with_capacity(11);
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
        if verbose {
            args.push(&b"class"[..]);
            args.push(class.as_bytes());
        }

        match ctx.call("XADD", args.as_slice()) {
            Ok(_) => {
                FORWARDED.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                ctx.log_warning(&format!("eventstream: XADD to '{stream}' failed: {e}"));
            }
        }
    });
    if !matches!(status, Ok(Status::Ok)) {
        ctx.log_warning("eventstream: failed to register post-notification job; event dropped");
    }
}

/// `EVENTSTREAM.STATS`: report current config and the forwarded counter.
fn stats(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let prefix = PREFIX.lock().unwrap().clone();
    let events = EVENTS.lock().unwrap().clone();
    let format = if format_is_verbose() {
        "verbose"
    } else {
        "minimal"
    };
    Ok(RedisValue::Array(vec![
        RedisValue::SimpleStringStatic("enabled"),
        RedisValue::Integer(ENABLED.load(Ordering::Relaxed) as i64),
        RedisValue::SimpleStringStatic("prefix"),
        RedisValue::BulkString(prefix),
        RedisValue::SimpleStringStatic("events"),
        RedisValue::BulkString(events),
        RedisValue::SimpleStringStatic("maxlen"),
        RedisValue::Integer(MAXLEN.load(Ordering::Relaxed)),
        RedisValue::SimpleStringStatic("format"),
        RedisValue::SimpleStringStatic(format),
        RedisValue::SimpleStringStatic("forwarded"),
        RedisValue::Integer(FORWARDED.load(Ordering::Relaxed)),
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
    commands: [
        ["eventstream.stats", stats, "readonly", 0, 0, 0, ""],
    ],
    event_handlers: [
        [@ALL: on_keyspace_event],
    ],
    configurations: [
        i64: [
            ["maxlen", &MAXLEN, 10000, 0, 1_000_000_000, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["prefix", &*PREFIX, "events:", ConfigurationFlags::DEFAULT, None],
            ["events", &*EVENTS, "all", ConfigurationFlags::DEFAULT, None],
        ],
        bool: [
            ["enabled", &ENABLED, true, ConfigurationFlags::DEFAULT, None],
        ],
        enum: [
            ["format", &*FORMAT, EntryFormat::Minimal, ConfigurationFlags::DEFAULT, None],
        ],
        module_args_as_configuration: true,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_all() {
        assert!(should_capture("expired", "all"));
        assert!(should_capture("hset", "*"));
        assert!(should_capture("del", " ALL "));
    }

    #[test]
    fn capture_list() {
        assert!(should_capture("expired", "expired,del"));
        assert!(should_capture("del", "expired, del"));
        assert!(should_capture("EXPIRED", "expired"));
        assert!(!should_capture("hset", "expired,del"));
        assert!(!should_capture("set", ""));
    }

    #[test]
    fn class_names_route() {
        assert_eq!(class_name(NotifyEvent::EXPIRED), "expired");
        assert_eq!(class_name(NotifyEvent::HASH), "hash");
        assert_eq!(class_name(NotifyEvent::empty()), "other");
    }
}
