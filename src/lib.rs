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
use std::sync::{Mutex, OnceLock};

/// Longest stream-key suffix the sanitizer will emit, in bytes (SPEC.md section 5).
const MAX_EVENT_NAME_LEN: usize = 128;
/// Maximum prefix length in bytes (SPEC.md section 7).
const MAX_PREFIX_LEN: usize = 128;

// Counters (SPEC.md section 13): process-lifetime, monotonic, reset on load.
static FORWARDED: AtomicU64 = AtomicU64::new(0);
/// Copies written to the combined firehose stream when `eventstream.firehose`
/// is on (issue #58). Kept apart from `FORWARDED` so that counter keeps
/// meaning "captured events", not "XADDs issued"; firehose write failures
/// share the existing `dropped_*` counters.
static FIREHOSE_FORWARDED: AtomicU64 = AtomicU64::new(0);
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

/// Panics caught at an FFI boundary, in either the notification callback or a
/// post-notification job (SPEC.md section 5). A nonzero value is a bug in this
/// module; the counter exists so it surfaces in INFO instead of aborting the
/// server.
static HANDLER_PANICS: AtomicU64 = AtomicU64::new(0);

/// Cluster per-node mode (issue #45): true when `eventstream.cluster-streams`
/// is `per-node` and the server is in cluster mode. Set once in init (the
/// config is IMMUTABLE), read on the hot path.
static PER_NODE: AtomicBool = AtomicBool::new(false);
/// Events dropped in per-node mode because the node owns no slot to pin to
/// (SPEC.md section 5). Distinct from the write-failure drops.
static DROPPED_NO_OWNED_SLOT: AtomicU64 = AtomicU64::new(0);
static LOGGED_NO_OWNED_SLOT: AtomicBool = AtomicBool::new(false);
/// Times the node re-pinned to a new owned slot after its pinned slot migrated
/// away (issue #46). Each re-pin writes a `repinned` gap marker and changes the
/// destination stream name; a nonzero value records reshard activity.
static REPINS: AtomicU64 = AtomicU64::new(0);
/// Re-pins triggered by the ownership-probe fallback rather than the
/// recognized error text (issue #76), counted in addition to `repins`. A
/// nonzero value means the server's local-refusal message is no longer
/// recognized; report the new form upstream.
static REPINS_PROBE_DETECTED: AtomicU64 = AtomicU64::new(0);
/// Events refused while the pinned slot was mid-migration (issue #75):
/// `TRYAGAIN`/`ASK` refusals that persisted through the one re-pin retry.
/// Distinct from `dropped_xadd_error` so routine resharding does not read as
/// a broken write path; included in the `dropped` sum.
static DROPPED_MIGRATING: AtomicU64 = AtomicU64::new(0);
static LOGGED_MIGRATING: AtomicBool = AtomicBool::new(false);
/// Redis has 16384 hash slots.
const SLOT_COUNT: u32 = 16384;
/// The hash tag this node pins its streams to in per-node cluster mode (issue
/// #45). `None` until selected: a node owns no slots at load, so selection is
/// lazy, on the first captured write when slots are known. A plain `Mutex`
/// (not a `RedisGILGuard`) so the INFO handler, whose context is not a lock
/// indicator, can read it; the GIL already serializes all access.
static NODE_TAG: Mutex<Option<String>> = Mutex::new(None);
/// The pinned tag most recently probe-verified as owned (issue #76): bounds
/// the text-independent fallback at one ownership probe per pinned tag, so an
/// unrelated persistent `XADD` failure costs one extra probe total, not one
/// per failure. Cleared on re-pin. Same locking rationale as `NODE_TAG`.
static PROBE_VERIFIED_TAG: Mutex<Option<String>> = Mutex::new(None);
/// Slot -> synthetic-tag table for the Redis 7.2 fallback, which has no
/// canonical-name API (issue #116). Filled once, on first fallback use, by
/// hashing candidates `es{i}` until every slot has one; stores the candidate
/// index per slot (64 KiB). CRC16 is a fixed function, so the fill
/// deterministically completes (at candidate index 156393, a few ms); the
/// exhaustive unit test proves completion and coverage of all 16384 slots.
static FALLBACK_SLOT_TAGS: OnceLock<Vec<u32>> = OnceLock::new();

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
/// `eventstream.firehose` (issue #58): when on, every captured event is also
/// written to the combined `<prefix><seg>#firehose` stream. Off by default
/// (it doubles write amplification per captured event, SPEC.md section 11);
/// runtime-mutable, read in the post-notification job.
static FIREHOSE: AtomicBool = AtomicBool::new(false);
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
            "'@trimmed' fires during cluster reshard trimming, on the source node \
             for keys leaving in the migration window; it is reshard bookkeeping, \
             not a user keyspace change, and is not captured (SPEC.md section 10)",
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

/// `eventstream.cluster-streams` config binding (issue #45): `refuse` (default,
/// the module refuses to load in cluster mode) or `per-node` (each node pins
/// its streams to a slot it owns). IMMUTABLE, load-time only, validated in
/// `set()`.
struct ClusterStreamsConfig {
    value: RedisGILGuard<String>,
}

impl ClusterStreamsConfig {
    fn is_per_node<G: redis_module::RedisLockIndicator>(&self, ctx: &G) -> bool {
        self.value.lock(ctx).as_str() == "per-node"
    }
}

impl ConfigurationValue<RedisString> for ClusterStreamsConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.value.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        if s != "refuse" && s != "per-node" {
            return Err(RedisError::String(format!(
                "cluster-streams must be 'refuse' or 'per-node', got '{s}'"
            )));
        }
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
    static ref CLUSTER_STREAMS: ClusterStreamsConfig = ClusterStreamsConfig {
        value: RedisGILGuard::new("refuse".to_owned()),
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

/// The hash-tag segment inserted between the prefix and the rest of a
/// destination key so all of a node's keys co-locate on a slot it owns (issue
/// #45). Empty in standalone/refuse mode. In per-node cluster mode, `{tag}`,
/// selecting the tag lazily on first use (a node owns no slots at load) and
/// caching it. Returns `None` only in per-node mode when the node currently
/// owns no slot; the caller drops the event as `dropped_no_owned_slot`.
///
/// Must be called from a write-safe context (a post-notification job or a
/// command), because selection probes the keyspace.
fn tag_segment(ctx: &Context) -> Option<String> {
    if !PER_NODE.load(Ordering::Relaxed) {
        return Some(String::new());
    }
    let mut cached = NODE_TAG.lock().unwrap();
    if cached.is_none() {
        *cached = select_owned_tag(ctx);
    }
    cached.as_ref().map(|t| format!("{{{t}}}"))
}

/// Like [`tag_segment`] but never selects (never writes): returns the cached
/// segment or `None`. For read-only contexts, such as the `EVENTSTREAM.STREAMS`
/// command, where triggering the write probe would violate the readonly
/// contract.
fn tag_segment_cached() -> Option<String> {
    if !PER_NODE.load(Ordering::Relaxed) {
        return Some(String::new());
    }
    NODE_TAG
        .lock()
        .unwrap()
        .as_ref()
        .map(|t| format!("{{{t}}}"))
}

/// CRC16-CCITT (XMODEM: polynomial 0x1021, initial value 0, no reflection),
/// the exact variant Redis uses for key hash slots (`CRC16(key) mod 16384`).
/// Only the 7.2 fallback below needs it; unit-tested against the cluster
/// spec's reference vector and the well-known slot anchors.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// The synthetic tag for `slot` on the 7.2 fallback path: the lowest-index
/// `es{i}` whose CRC16 lands on it, from [`FALLBACK_SLOT_TAGS`]. Tags are
/// ASCII alphanumerics, so they never contain `{` or `}` and cannot break the
/// `{tag}` wrap.
fn fallback_tag_for_slot(slot: u32) -> String {
    let table = FALLBACK_SLOT_TAGS.get_or_init(|| {
        let mut table = vec![u32::MAX; SLOT_COUNT as usize];
        let mut remaining = SLOT_COUNT;
        let mut i: u32 = 0;
        while remaining > 0 {
            let s = u32::from(crc16(format!("es{i}").as_bytes())) % SLOT_COUNT;
            if table[s as usize] == u32::MAX {
                table[s as usize] = i;
                remaining -= 1;
            }
            i += 1;
        }
        table
    });
    format!("es{}", table[slot as usize])
}

/// Find a hash tag whose slot this node owns, probing ownership with a
/// non-destructive write: `XADD {tag}#slotprobe NOMKSTREAM * f v`. The
/// slot-ownership check that rejects a non-local key applies to writes, not
/// reads (a plain read runs locally and would falsely pass on every node), so
/// the probe must be a write; `NOMKSTREAM` on a non-existent stream is a no-op
/// that creates nothing, and this is the same locality rule that governs the
/// real mirrored writes. One-time, then cached.
///
/// The candidate tag for each slot comes from
/// `RedisModule_ClusterCanonicalKeyNameInSlot(slot)`, which yields a key name
/// hashing to a specific slot, so scanning slots has guaranteed coverage. That
/// API was added after Redis 7.2, though: on 7.2 the bound function pointer is
/// null (bindgen declares it from the vendored header, but the server does not
/// provide it), and calling it would panic across the FFI boundary and abort
/// the server (issue #45). When it is unavailable, fall back to the
/// [`FALLBACK_SLOT_TAGS`] runtime CRC16 search (issue #116), which maps each
/// probed slot to a synthetic tag hashing to it, so coverage is exhaustive on
/// both paths: a node that owns any slot finds a tag. Slots are visited in a
/// scattered order (odd stride, coprime with 16384) so an owned slot is found
/// within a few probes on a typical cluster while still covering all slots in
/// the worst case.
#[cfg(not(test))]
fn select_owned_tag(ctx: &Context) -> Option<String> {
    let canonical = unsafe { raw::RedisModule_ClusterCanonicalKeyNameInSlot };
    let mut slot: u32 = 0;
    for _ in 0..SLOT_COUNT {
        // A candidate tag whose hash slot we then test for ownership.
        let candidate: Option<String> = match canonical {
            Some(canonical_in_slot) => {
                let name_ptr = unsafe { canonical_in_slot(slot) };
                if name_ptr.is_null() {
                    None
                } else {
                    let bytes = unsafe { CStr::from_ptr(name_ptr) }.to_bytes();
                    // The canonical name is expected to be simple ASCII with no
                    // braces; guard against anything that would break the tag.
                    if bytes.is_empty() || bytes.contains(&b'{') || bytes.contains(&b'}') {
                        None
                    } else {
                        Some(String::from_utf8_lossy(bytes).into_owned())
                    }
                }
            }
            // Redis 7.2: no canonical-name API. The table gives a synthetic
            // tag hashing to the probed slot, so the walk covers every slot a
            // node could own, however skewed the ownership (issue #116).
            None => Some(fallback_tag_for_slot(slot)),
        };
        if let Some(tag) = candidate {
            if probe_tag_owned(ctx, &tag).is_ok() {
                return Some(tag);
            }
        }
        slot = (slot + 2609) % SLOT_COUNT;
    }
    None
}

// Test builds compile out the raw cluster call; tag selection never runs there.
#[cfg(test)]
fn select_owned_tag(_ctx: &Context) -> Option<String> {
    None
}

/// Probe whether this node owns `tag`'s slot, with a non-destructive write:
/// `XADD {tag}#slotprobe NOMKSTREAM * f v`, using the SAME call options as the
/// real mirrored write (the replicate flag is what makes RM_Call enforce slot
/// ownership; a plain call runs locally and passes on every node). NOMKSTREAM
/// makes it a no-op on a non-existent stream, so nothing is written. Owned
/// slot -> Ok; non-owned -> Err (the non-local-key error); mid-migration ->
/// Err (`TRYAGAIN`/`ASK`), so selection never picks a slot that is leaving
/// (issue #75). The Err carries the error text for the caller to classify.
fn probe_tag_owned(ctx: &Context, tag: &str) -> Result<(), String> {
    let probe = format!("{{{tag}}}#slotprobe");
    let res: CallResult = ctx.call_ext(
        "XADD",
        &xadd_call_options(),
        &[
            probe.as_bytes(),
            &b"NOMKSTREAM"[..],
            &b"*"[..],
            &b"f"[..],
            &b"v"[..],
        ][..],
    );
    res.map(|_| ())
        .map_err(|e| e.to_utf8_string().unwrap_or_default())
}

/// Text-independent migration check (issue #76): true when the node's pinned
/// tag no longer probes as owned, meaning an unclassified `XADD` failure was
/// really the local-refusal in a message form `is_slot_migrated` does not
/// recognize. A tag the probe verifies as owned is cached in
/// `PROBE_VERIFIED_TAG` and not re-probed until a re-pin or a successful
/// mirrored write resets the cache; the reset bounds probes to one per
/// unclassified-failure streak rather than one per pinned tag, so a stale
/// verification cannot mask a later migration. An OOM probe error is
/// inconclusive (the probe was refused, not the slot) and neither
/// reclassifies nor caches.
fn pinned_tag_lost_by_probe(ctx: &Context) -> bool {
    if !PER_NODE.load(Ordering::Relaxed) {
        return false;
    }
    let Some(tag) = NODE_TAG.lock().unwrap().clone() else {
        return false;
    };
    if PROBE_VERIFIED_TAG.lock().unwrap().as_deref() == Some(tag.as_str()) {
        return false;
    }
    match probe_tag_owned(ctx, &tag) {
        Ok(()) => {
            *PROBE_VERIFIED_TAG.lock().unwrap() = Some(tag);
            false
        }
        Err(msg) => !msg.starts_with("OOM"),
    }
}

/// Record an event dropped for want of an owned slot (per-node mode), logging
/// the first occurrence. The message states what was observed (the walk found
/// no slot that accepted a local write), not an inference about ownership
/// (issue #116); on 7.4+ a slot whose canonical name is unusable is skipped
/// rather than probed, so "probed every slot" would overstate it.
fn count_no_slot_drop(ctx: &Context) {
    count_drop(
        ctx,
        &DROPPED_NO_OWNED_SLOT,
        &LOGGED_NO_OWNED_SLOT,
        "tag selection walked all 16384 slots and found none that accepted a \
         local write; event dropped (dropped_no_owned_slot). Selection is \
         retried on the next captured event",
    );
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

/// Run a post-notification job body, catching any panic so a bug in module
/// code cannot unwind across the FFI job trampoline and abort the server. The
/// redis-module wrapper makes the notification callback panic-safe but not the
/// post-notification job it schedules; issue #45 found a null optional-API
/// pointer (`ClusterCanonicalKeyNameInSlot` on Redis 7.2) panicking here and
/// aborting the node, so the guard belongs with every job body. A caught panic
/// is counted and logged once, sharing the handler-panic counters.
fn guard_job(body: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_err() {
        HANDLER_PANICS.fetch_add(1, Ordering::Relaxed);
        if LOGGED_PANIC
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            redis_module::logging::log_warning(
                "eventstream: post-notification job panicked (caught); event dropped",
            );
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
    let prefix_owned = PREFIX.value.lock(ctx).as_str().to_owned();
    let maxlen = MAXLEN.value.load(Ordering::Relaxed);
    let dropped_count = drained.len() as u64;
    let status = ctx.add_post_notification_job(move |ctx| {
        guard_job(move || {
            // Resolve the per-node tag in the job (write-safe context); the
            // control stream shares the node tag with the event streams so they
            // co-locate.
            let seg = match tag_segment(ctx) {
                Some(s) => s,
                None => {
                    for _ in &drained {
                        count_no_slot_drop(ctx);
                    }
                    return;
                }
            };
            let control_stream = format!("{prefix_owned}{seg}#control");
            for action in &drained {
                write_marker(ctx, &control_stream, action, maxlen);
            }
        });
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

/// True if an `XADD` failure is the cluster local-refusal error, which in
/// per-node mode means the node no longer owns the pinned tag's slot (it
/// migrated away in a reshard, issue #46). The full text is "Attempted to
/// access a non local key in a cluster node" (observed empirically, #19); match
/// a stable substring so a leading error code does not matter.
fn is_slot_migrated(msg: &str) -> bool {
    msg.contains("non local key")
}

/// True if an `XADD` failure is a migration-window refusal (issue #75): while
/// the pinned slot is `MIGRATING`/`IMPORTING`, a cluster write is refused with
/// `TRYAGAIN` or redirected with `ASK <slot> <node>`. Both are error codes, so
/// they lead the message; either way the slot is leaving this node, an earlier
/// signal of the same departure `is_slot_migrated` detects after the fact.
fn is_migration_refusal(msg: &str) -> bool {
    msg.starts_with("TRYAGAIN ") || msg == "TRYAGAIN" || msg.starts_with("ASK ")
}

/// Classification of one mirrored-write attempt, so the caller can decide
/// whether to re-pin and retry.
enum MirrorOutcome {
    /// The entry was written (and its stream registered on first sight).
    Written,
    /// The pinned slot is no longer local: re-pin to a new owned slot and retry.
    SlotMigrated,
    /// The pinned slot is mid-migration (`TRYAGAIN`/`ASK`, issue #75): an
    /// early re-pin signal, handled like [`MirrorOutcome::SlotMigrated`] but
    /// counted as `dropped_migrating` if the retry is also refused. Carries
    /// the server's error text so the first-failure log names the actual
    /// refusal (TRYAGAIN vs ASK, slot, target node).
    Migrating(String),
    /// Refused under `maxmemory`.
    Oom(String),
    /// Any other `XADD` failure.
    Failed(String),
}

/// Write one mirrored entry to `<prefix><seg><suffix>`, and on the first write
/// to a stream register it in `<prefix><seg>#streams`. A success increments
/// `counter`: `FORWARDED` for per-event entries, `FIREHOSE_FORWARDED` for
/// firehose copies (issue #58), so `forwarded` stays a pure captured-event
/// count. Returns a classified outcome; the caller counts drops and, on
/// [`MirrorOutcome::SlotMigrated`], re-pins. Runs only in a write-safe context
/// (a post-notification job).
#[allow(clippy::too_many_arguments)]
fn mirror_entry(
    ctx: &Context,
    prefix: &str,
    seg: &str,
    suffix: &str,
    event: &[u8],
    key: &[u8],
    db: &str,
    maxlen: i64,
    counter: &AtomicU64,
) -> MirrorOutcome {
    let stream = format!("{prefix}{seg}{suffix}");
    let registry = format!("{prefix}{seg}#streams");

    let maxlen_s = maxlen.to_string();
    let mut args: Vec<&[u8]> = Vec::with_capacity(12);
    args.push(stream.as_bytes());
    if maxlen > 0 {
        args.push(&b"MAXLEN"[..]);
        args.push(&b"~"[..]);
        args.push(maxlen_s.as_bytes());
    }
    args.push(&b"*"[..]);
    args.push(&b"event"[..]);
    args.push(event);
    args.push(&b"key"[..]);
    args.push(key);
    args.push(&b"db"[..]);
    args.push(db.as_bytes());

    // Per-event trace (SPEC.md section 13); the server filters by loglevel. Key
    // bytes are ASCII-escaped: the wrapper's logger builds a CString and panics
    // across the FFI boundary on interior NUL, so raw key bytes (which may
    // contain NUL) must never reach it.
    ctx.log_debug(&format!(
        "eventstream: {} key={} -> {}",
        String::from_utf8_lossy(event),
        key.escape_ascii(),
        stream
    ));

    let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
    match res {
        Ok(_) => {
            counter.fetch_add(1, Ordering::Relaxed);
            // A successful write proves current ownership, so reset the probe
            // budget: without this, a tag verified once (after an unrelated
            // transient failure) would never be re-probed, and a later
            // migration under a reworded refusal message would degrade into
            // permanent unclassified drops — the exact failure issue #76
            // exists to prevent. Cost: one uncontended lock per written event
            // in per-node mode, matching the KNOWN_STREAMS lock below.
            if PER_NODE.load(Ordering::Relaxed) {
                *PROBE_VERIFIED_TAG.lock().unwrap() = None;
            }
            // First write to a destination stream: register it in the persistent
            // set at `<prefix><seg>#streams` (replicated, so EVENTSTREAM.STREAMS
            // survives restart and works on replicas) and count it. KNOWN_STREAMS
            // is the in-process dedupe cache; it is cleared on flush so a FLUSHALL
            // that deleted the registry rebuilds it on the next write. The
            // registry key is under the prefix, so its own SADD notification is
            // dropped by the feedback guard.
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
            MirrorOutcome::Written
        }
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_default();
            if msg.starts_with("OOM") {
                MirrorOutcome::Oom(format!("XADD to '{stream}' refused under maxmemory: {msg}"))
            } else if is_slot_migrated(&msg) {
                MirrorOutcome::SlotMigrated
            } else if is_migration_refusal(&msg) {
                MirrorOutcome::Migrating(format!("XADD to '{stream}' refused mid-migration: {msg}"))
            } else {
                MirrorOutcome::Failed(format!("XADD to '{stream}' failed: {msg}"))
            }
        }
    }
}

/// Re-pin after the pinned slot left this node, either because a migration
/// completed (the local-refusal error, issue #46) or is in progress
/// (`TRYAGAIN`/`ASK`, issue #75): clear the cached tag, re-select a currently
/// owned slot (the selection probe fails with the same refusal on a slot
/// mid-migration, so the leaving slot is never re-picked), delimit the
/// discontinuity with a `repinned` gap marker on the new control stream, and
/// retry the entry once so the triggering event is captured rather than
/// dropped. Bounded: a refusal on the retry is a counted drop, never another
/// re-pin. Runs only in a write-safe context (a post-notification job).
fn repin_and_retry(
    ctx: &Context,
    prefix: &str,
    suffix: &str,
    event: &[u8],
    key: &[u8],
    db: &str,
    maxlen: i64,
) {
    *NODE_TAG.lock().unwrap() = None;
    *PROBE_VERIFIED_TAG.lock().unwrap() = None;
    REPINS.fetch_add(1, Ordering::Relaxed);
    let seg = match tag_segment(ctx) {
        Some(s) => s,
        None => {
            // No slot owned now; capture resumes on a later event once this
            // node owns a slot again.
            count_no_slot_drop(ctx);
            return;
        }
    };
    // A `repinned` gap marker on the new control stream delimits the window
    // where this node's stream name changed (SPEC.md section 9).
    write_marker(ctx, &format!("{prefix}{seg}#control"), "repinned", maxlen);
    match mirror_entry(
        ctx, prefix, &seg, suffix, event, key, db, maxlen, &FORWARDED,
    ) {
        MirrorOutcome::Written => {}
        MirrorOutcome::Oom(msg) => count_drop(ctx, &DROPPED_OOM, &LOGGED_OOM, &msg),
        // Still refused (slot in flux): a migration-window drop, delimited by
        // the marker above (SPEC.md section 10).
        MirrorOutcome::SlotMigrated => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            "XADD still refused as non-local after re-pin; entry dropped in \
             migration window (dropped_migrating)",
        ),
        MirrorOutcome::Migrating(msg) => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            &format!("{msg} (after re-pin; entry dropped in migration window)"),
        ),
        MirrorOutcome::Failed(msg) => {
            count_drop(ctx, &DROPPED_XADD_ERROR, &LOGGED_XADD_ERROR, &msg)
        }
    }
}

/// Write the firehose copy of a captured event (issue #58): a second `XADD`
/// to `<prefix><seg>#firehose` with fields identical to the per-event entry,
/// the same `MAXLEN ~` trimming and call options. Runs after the per-event
/// outcome is settled and succeeds or fails independently of it; a success
/// counts in `firehose_forwarded`, a failure in the existing `dropped_*`
/// counters. The tag segment is re-resolved so a re-pin performed by the
/// per-event write lands the copy on the new tag. A cluster refusal here is
/// counted, never re-pinned: slot ownership cannot change between the two
/// XADDs (both run inside one execution unit), so a refusal only occurs in a
/// migration window the per-event write already re-pinned through, and a
/// second re-pin for the same event would double the `repinned` marker and
/// the one-retry bound. The ownership-probe fallback is likewise left to the
/// per-event path, which hits the same failure on the same tag first.
fn mirror_firehose(ctx: &Context, prefix: &str, event: &[u8], key: &[u8], db: &str, maxlen: i64) {
    let seg = match tag_segment(ctx) {
        Some(s) => s,
        None => {
            count_no_slot_drop(ctx);
            return;
        }
    };
    match mirror_entry(
        ctx,
        prefix,
        &seg,
        "#firehose",
        event,
        key,
        db,
        maxlen,
        &FIREHOSE_FORWARDED,
    ) {
        MirrorOutcome::Written => {}
        MirrorOutcome::Oom(msg) => count_drop(ctx, &DROPPED_OOM, &LOGGED_OOM, &msg),
        MirrorOutcome::SlotMigrated => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            "firehose XADD refused as non-local; copy dropped in migration \
             window (dropped_migrating); one re-pin per event, owned by the \
             per-event write",
        ),
        MirrorOutcome::Migrating(msg) => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            &format!("{msg} (firehose copy dropped in migration window)"),
        ),
        MirrorOutcome::Failed(msg) => {
            count_drop(ctx, &DROPPED_XADD_ERROR, &LOGGED_XADD_ERROR, &msg)
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

    // Names are resolved in the job, not here: in per-node cluster mode the
    // hash tag is selected lazily (this node may own no slots yet), and that
    // probe must run in a write-safe context.
    let prefix_owned = prefix.as_str().to_owned();
    let maxlen = MAXLEN.value.load(Ordering::Relaxed);
    let event_owned = event.to_owned();
    let key_owned = key.to_vec();

    // 7. Deferred write, atomic with the notification.
    let status = ctx.add_post_notification_job(move |ctx| {
        guard_job(move || {
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
            let db_s = db.to_string();

            // In per-node cluster mode, `{tag}` co-locates this node's streams on
            // an owned slot; empty otherwise. `None` means no owned slot yet.
            let seg = match tag_segment(ctx) {
                Some(s) => s,
                None => {
                    count_no_slot_drop(ctx);
                    return;
                }
            };
            match mirror_entry(
                ctx,
                &prefix_owned,
                &seg,
                &suffix,
                event_owned.as_bytes(),
                &key_owned,
                &db_s,
                maxlen,
                &FORWARDED,
            ) {
                MirrorOutcome::Written => {}
                MirrorOutcome::Oom(msg) => count_drop(ctx, &DROPPED_OOM, &LOGGED_OOM, &msg),
                // The pinned slot migrated away in a reshard (issue #46) or is
                // mid-migration (issue #75): re-pin and retry once.
                MirrorOutcome::SlotMigrated | MirrorOutcome::Migrating(_) => repin_and_retry(
                    ctx,
                    &prefix_owned,
                    &suffix,
                    event_owned.as_bytes(),
                    &key_owned,
                    &db_s,
                    maxlen,
                ),
                MirrorOutcome::Failed(msg) => {
                    // The re-pin trigger is an empirically observed error
                    // string, so an unclassified failure could be the
                    // local-refusal in a reworded message. Re-verify ownership
                    // of the pinned tag before counting the drop (issue #76);
                    // a failing probe re-pins exactly as if the text had
                    // matched, counted in `repins_probe_detected`.
                    if pinned_tag_lost_by_probe(ctx) {
                        REPINS_PROBE_DETECTED.fetch_add(1, Ordering::Relaxed);
                        repin_and_retry(
                            ctx,
                            &prefix_owned,
                            &suffix,
                            event_owned.as_bytes(),
                            &key_owned,
                            &db_s,
                            maxlen,
                        );
                    } else {
                        count_drop(ctx, &DROPPED_XADD_ERROR, &LOGGED_XADD_ERROR, &msg);
                    }
                }
            }
            // The firehose copy runs after the per-event outcome is settled,
            // gated on the runtime-mutable config; the two writes succeed or
            // fail independently (issue #58).
            if FIREHOSE.load(Ordering::Relaxed) {
                mirror_firehose(
                    ctx,
                    &prefix_owned,
                    event_owned.as_bytes(),
                    &key_owned,
                    &db_s,
                    maxlen,
                );
            }
        });
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
        // deinit runs inside MODULE UNLOAD, a write-safe context, so the tag
        // can be resolved here. If the node owns no slot, skip the markers
        // rather than fail the unload.
        if let Some(seg) = tag_segment(ctx) {
            let control_stream = format!("{}{seg}#control", PREFIX.value.lock(ctx).as_str());
            let maxlen = MAXLEN.value.load(Ordering::Relaxed);
            for action in drained {
                write_marker(ctx, &control_stream, action, maxlen);
            }
            write_marker(ctx, &control_stream, "unloading", maxlen);
        }
    }
    ctx.log_notice(&format!(
        "eventstream unloading: forwarded={} firehose_forwarded={} dropped={} skipped_self={} \
         skipped_filtered={} skipped_invalid={} control_markers={} active_streams={}",
        FORWARDED.load(Ordering::Relaxed),
        FIREHOSE_FORWARDED.load(Ordering::Relaxed),
        DROPPED_XADD_ERROR.load(Ordering::Relaxed)
            + DROPPED_OOM.load(Ordering::Relaxed)
            + DROPPED_DEFER_ERROR.load(Ordering::Relaxed)
            + DROPPED_MIGRATING.load(Ordering::Relaxed),
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
        + DROPPED_DEFER_ERROR.load(Ordering::Relaxed)
        + DROPPED_MIGRATING.load(Ordering::Relaxed);
    ctx.builder()
        .add_section("stats")
        .field("enabled", ENABLED.load(Ordering::Relaxed) as i64)?
        .field("forwarded", FORWARDED.load(Ordering::Relaxed))?
        .field(
            "firehose_forwarded",
            FIREHOSE_FORWARDED.load(Ordering::Relaxed),
        )?
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
        .field(
            "dropped_no_owned_slot",
            DROPPED_NO_OWNED_SLOT.load(Ordering::Relaxed),
        )?
        .field(
            "dropped_migrating",
            DROPPED_MIGRATING.load(Ordering::Relaxed),
        )?
        .field("repins", REPINS.load(Ordering::Relaxed))?
        .field(
            "repins_probe_detected",
            REPINS_PROBE_DETECTED.load(Ordering::Relaxed),
        )?
        .field("cluster_per_node", PER_NODE.load(Ordering::Relaxed) as i64)?
        .field(
            "cluster_pinned_tag",
            NODE_TAG.lock().unwrap().clone().unwrap_or_default(),
        )?
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
        + DROPPED_DEFER_ERROR.load(Ordering::Relaxed)
        + DROPPED_MIGRATING.load(Ordering::Relaxed);
    let pairs: [(&str, i64); 19] = [
        ("enabled", ENABLED.load(Ordering::Relaxed) as i64),
        ("forwarded", FORWARDED.load(Ordering::Relaxed) as i64),
        (
            "firehose_forwarded",
            FIREHOSE_FORWARDED.load(Ordering::Relaxed) as i64,
        ),
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
            "dropped_no_owned_slot",
            DROPPED_NO_OWNED_SLOT.load(Ordering::Relaxed) as i64,
        ),
        (
            "dropped_migrating",
            DROPPED_MIGRATING.load(Ordering::Relaxed) as i64,
        ),
        ("repins", REPINS.load(Ordering::Relaxed) as i64),
        (
            "repins_probe_detected",
            REPINS_PROBE_DETECTED.load(Ordering::Relaxed) as i64,
        ),
        ("cluster_per_node", PER_NODE.load(Ordering::Relaxed) as i64),
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
/// for the read and restores the caller's database. In per-node cluster mode
/// this returns only the local node's registry (`<prefix>{tag}#streams`);
/// cluster-wide discovery is resolved client-side — callers fan out over the
/// masters and merge (issue #47; docs/consumer-patterns.md, cluster
/// consumers).
#[cfg(not(test))]
fn cmd_streams(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    // No owned slot selected yet in per-node mode: nothing local to report.
    // Use the non-probing lookup: this is a readonly command and must not
    // trigger the write that tag selection performs.
    let seg = match tag_segment_cached() {
        Some(s) => s,
        None => return Ok(RedisValue::Array(vec![])),
    };
    let registry = format!("{}{seg}#streams", PREFIX.value.lock(ctx).as_str());
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
            ["cluster-streams", &*CLUSTER_STREAMS, "refuse", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["enabled", &ENABLED, true, ConfigurationFlags::DEFAULT, Some(Box::new(enabled_changed))],
            // No on-changed callback: unlike `enabled`, toggling the firehose
            // opens no capture gap (per-event mirroring continues), so there
            // is no marker to record (issue #58).
            ["firehose", &FIREHOSE, false, ConfigurationFlags::DEFAULT, None],
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
    fn sanitize_cannot_alias_firehose() {
        // `#` is outside the sanitizer alphabet, so no event name can route
        // to the reserved firehose key (issue #58): a raw `#firehose` event
        // lands in `<prefix>_firehose`, never `<prefix>#firehose`.
        assert_eq!(sanitize("#firehose"), "_firehose");
        assert!(!sanitize("evil#firehose").contains('#'));
    }

    #[test]
    fn sanitize_truncates_at_128() {
        let long = "x".repeat(300);
        assert_eq!(sanitize(&long).len(), 128);
    }

    #[test]
    fn slot_migrated_matches_known_local_refusal_forms() {
        // The bare text observed empirically (#19), and with a leading
        // error-code token, which the substring match must tolerate.
        assert!(is_slot_migrated(
            "Attempted to access a non local key in a cluster node"
        ));
        assert!(is_slot_migrated(
            "ERR Attempted to access a non local key in a cluster node"
        ));
    }

    #[test]
    fn slot_migrated_rejects_unrelated_errors() {
        // None of these may trigger a re-pin.
        assert!(!is_slot_migrated(
            "OOM command not allowed when used memory > 'maxmemory'."
        ));
        assert!(!is_slot_migrated("ERR some arbitrary error"));
        assert!(!is_slot_migrated(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ));
        assert!(!is_slot_migrated(""));
    }

    #[test]
    fn migration_refusal_matches_tryagain_and_ask() {
        // TRYAGAIN and ASK are error codes, so they lead the message.
        assert!(is_migration_refusal(
            "TRYAGAIN Multiple keys request during rehashing of slot"
        ));
        assert!(is_migration_refusal("TRYAGAIN"));
        assert!(is_migration_refusal("ASK 3999 127.0.0.1:6381"));
    }

    #[test]
    fn migration_refusal_rejects_unrelated_errors() {
        assert!(!is_migration_refusal(
            "Attempted to access a non local key in a cluster node"
        ));
        assert!(!is_migration_refusal(
            "OOM command not allowed when used memory > 'maxmemory'."
        ));
        assert!(!is_migration_refusal("ERR some arbitrary error"));
        assert!(!is_migration_refusal(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ));
        // Codes lead the message; a mention elsewhere is not a refusal, and a
        // word merely prefixed with the code is not the code (ASKING is a real
        // command; TRYAGAINX guards any hypothetical future sibling code).
        assert!(!is_migration_refusal("ERR TRYAGAIN is not a code here"));
        assert!(!is_migration_refusal("ASKING requires a cluster"));
        assert!(!is_migration_refusal("TRYAGAINX something else"));
        assert!(!is_migration_refusal(""));
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

    #[test]
    fn crc16_matches_redis_key_hashing() {
        // Reference vector from the cluster spec's CRC16 appendix, plus the
        // well-known slot anchors from the spec's key-distribution examples.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(u32::from(crc16(b"foo")) % SLOT_COUNT, 12182);
        assert_eq!(u32::from(crc16(b"bar")) % SLOT_COUNT, 5061);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn fallback_tag_covers_every_slot() {
        // The 7.2 fallback's exhaustiveness guarantee (issue #116). Completing
        // at all proves the candidate walk terminates; the round trip proves
        // every slot's tag hashes back to it; the charset check proves no tag
        // can break the `{tag}` wrap.
        for slot in 0..SLOT_COUNT {
            let tag = fallback_tag_for_slot(slot);
            assert_eq!(u32::from(crc16(tag.as_bytes())) % SLOT_COUNT, slot);
            assert!(tag.bytes().all(|b| b.is_ascii_alphanumeric()), "{tag}");
        }
    }
}
