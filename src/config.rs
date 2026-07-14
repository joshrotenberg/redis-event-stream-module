// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Configuration surface (#86): config value types and statics, the event/
//! key/source-db filter grammars and the prefix/auto-group validators, plus the
//! `enabled` on-changed callback that records the enable/disable gap markers.

use crate::markers::{record_pending_marker, PendingMarker};
use lazy_static::lazy_static;
use redis_module::configuration::ConfigurationContext;
use redis_module::{
    enum_configuration, ConfigurationValue, NotifyEvent, RedisError, RedisGILGuard, RedisString,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Longest stream-key suffix the sanitizer will emit, in bytes (SPEC.md section 5).
pub(crate) const MAX_EVENT_NAME_LEN: usize = 128;

/// Maximum prefix length in bytes (SPEC.md section 7).
pub(crate) const MAX_PREFIX_LEN: usize = 128;

/// Previous value of `eventstream.enabled`, used by the on-changed callback to
/// detect transitions. Initialized to the default so the LoadConfigs-time set
/// of the default produces no spurious marker (SPEC.md section 13 lifecycle).
pub(crate) static LAST_ENABLED: AtomicBool = AtomicBool::new(true);

/// The `MISSED`/`NEW` bits the module subscribed to at load. The keyspace
/// subscription mask is fixed when the module loads and cannot be widened at
/// runtime, so these classes are only capturable if the load-time filter asked
/// for them; a runtime `CONFIG SET` that names an unsubscribed one is rejected
/// (SPEC.md section 5). `EXTRA_UNINIT` until `init` subscribes: the load-time
/// filter `set()` runs before `init` and must not reject.
pub(crate) const EXTRA_UNINIT: i64 = i64::MIN;

pub(crate) static SUBSCRIBED_EXTRA: AtomicI64 = AtomicI64::new(EXTRA_UNINIT);

/// The `MISSED`/`NEW` bits a parsed filter explicitly names via `@class`
/// tokens (not `*`, which adapts to whatever is subscribed).
pub(crate) fn extra_classes_named(f: &ParsedFilter) -> NotifyEvent {
    f.classes & (NotifyEvent::MISSED | NotifyEvent::NEW)
}

pub(crate) static ENABLED: AtomicBool = AtomicBool::new(true);

/// `eventstream.firehose` (issue #58): when on, every captured event is also
/// written to the combined `<prefix><seg>#firehose` stream. Off by default
/// (it doubles write amplification per captured event, SPEC.md section 11);
/// runtime-mutable, read in the post-notification job.
pub(crate) static FIREHOSE: AtomicBool = AtomicBool::new(false);

/// Cheap gate for `eventstream.auto-group` (issue #109) so the write path pays
/// one atomic load, not a string-guard lock, while the feature is off (the
/// default). Kept in sync from the config's `set()` on every transition
/// between empty (disabled) and a group name; read per successful write to
/// decide whether to consult the group name at all.
pub(crate) static AUTO_GROUP_ENABLED: AtomicBool = AtomicBool::new(false);

/// `eventstream.verify-oom` (issue #65): when on (the default), mirrored writes
/// carry the `M` flag so an `XADD` is refused under `maxmemory` and counted as
/// `dropped_oom` (SPEC.md sections 10, 11) — bounded, counted loss. When off,
/// `xadd_call_options` drops the `M` flag so capture continues at the memory
/// limit; growth stays bounded by `maxlen`, but the module now adds memory
/// while the server is evicting, so `dropped_oom` becomes unreachable and the
/// `evicted`-storm amplification of SPEC.md section 11 applies. Runtime-mutable,
/// read per `XADD`. Default `true` preserves today's behavior.
pub(crate) static VERIFY_OOM: AtomicBool = AtomicBool::new(true);

/// `eventstream.entry-seq` (issue #66): when on, every mirrored entry carries a
/// `seq` field with the value of `SEQ` below. Registered IMMUTABLE (load-time
/// only, like `stream-prefix`/`cluster-streams`) so that within one process the
/// entry field set is uniform — every stream either always has `seq` or never
/// does, preserving the `SAMEFIELDS` listpack compaction (SPEC.md section 6).
/// Default off, so existing deployments see no schema change.
pub(crate) static ENTRY_SEQ: AtomicBool = AtomicBool::new(false);

pub(crate) static MAXLEN: MaxlenConfig = MaxlenConfig {
    value: AtomicI64::new(10_000),
};

/// `eventstream.maxlen` config binding. Redis enforces the registered 0 to
/// i64::MAX range on CONFIG SET and redis.conf paths, but a module-arg value
/// becomes the registered default and bypasses that boundary check entirely
/// (verified against the wrapper at v2.1.3 and redis 7.2 module.c/config.c),
/// so `set()` re-validates: a negative value would silently disable trimming,
/// the module's only memory bound. Rejection aborts the load like any other
/// malformed module arg.
pub(crate) struct MaxlenConfig {
    pub(crate) value: AtomicI64,
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

pub(crate) static MAX_STREAMS: MaxStreamsConfig = MaxStreamsConfig {
    value: AtomicI64::new(0),
};

/// `eventstream.max-streams` config binding (issue #64): the cap on the number
/// of distinct destination streams the module will create; `0` is unlimited.
/// Re-validates negatives in `set()` for the same reason as `MaxlenConfig`: a
/// module-arg value becomes the registered default and bypasses the registered
/// 0..i64::MAX boundary check (redis 7.2 `module.c`/`config.c`), and a negative
/// cap is meaningless. Rejection aborts the load like any malformed module arg.
pub(crate) struct MaxStreamsConfig {
    pub(crate) value: AtomicI64,
}

impl ConfigurationValue<i64> for MaxStreamsConfig {
    fn get(&self, _ctx: &ConfigurationContext) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
    fn set(&self, _ctx: &ConfigurationContext, val: i64) -> Result<(), RedisError> {
        if val < 0 {
            return Err(RedisError::String(format!(
                "max-streams must be 0 (unlimited) or positive, got {val}"
            )));
        }
        self.value.store(val, Ordering::Relaxed);
        Ok(())
    }
}

pub(crate) static RETENTION_MS: RetentionMsConfig = RetentionMsConfig {
    value: AtomicI64::new(0),
};

/// `eventstream.retention-ms` config binding (issue #108): time-based retention.
/// When `> 0`, mirrored writes trim by `MINID ~ <now_ms - retention_ms>` instead
/// of `MAXLEN`, dropping entries older than the window (destination streams use
/// auto IDs, so every entry ID carries the event's millisecond timestamp). `0`
/// disables it, preserving the count-based-only behavior. Re-validates negatives
/// in `set()` for the same reason as `MaxlenConfig`: a module-arg value becomes
/// the registered default and bypasses the registered 0..i64::MAX boundary
/// check. When it transitions to `> 0` while `maxlen > 0`, `set()` logs a notice
/// that `maxlen` is now ignored (MINID takes precedence, SPEC.md section 7).
pub(crate) struct RetentionMsConfig {
    pub(crate) value: AtomicI64,
}

impl ConfigurationValue<i64> for RetentionMsConfig {
    fn get(&self, _ctx: &ConfigurationContext) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
    fn set(&self, _ctx: &ConfigurationContext, val: i64) -> Result<(), RedisError> {
        if val < 0 {
            return Err(RedisError::String(format!(
                "retention-ms must be 0 (disabled) or positive, got {val}"
            )));
        }
        // Precedence notice (issue #108): MINID trimming wins over MAXLEN when
        // both are set, so activating retention-ms silently ignores maxlen for
        // the XADD clause. Log it at the config-change point (the switch that
        // activates precedence) so the ignored cap is not a silent surprise;
        // the module-wide logger works without a Context, as in enabled_changed.
        if val > 0 && MAXLEN.value.load(Ordering::Relaxed) > 0 {
            redis_module::logging::log_notice(
                "eventstream: retention-ms > 0; maxlen is ignored, streams trim by \
                 MINID (time-based) instead (SPEC.md section 7)",
            );
        }
        self.value.store(val, Ordering::Relaxed);
        Ok(())
    }
}

/// Parsed form of the `eventstream.events` filter (SPEC.md section 7 grammar).
#[derive(Clone, Debug)]
pub(crate) struct ParsedFilter {
    pub(crate) star: bool,
    pub(crate) classes: NotifyEvent,
    pub(crate) names: HashSet<String>,
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
    pub(crate) fn matches(&self, event_type: NotifyEvent, event: &str) -> bool {
        self.star || self.classes.intersects(event_type) || self.names.contains(event)
    }
}

/// Map an `@class` token to its `NotifyEvent` bit (SPEC.md section 7 grammar).
/// `missed` and `new` are outside `NOTIFY_ALL`; the module subscribes to them
/// through its own raw subscription, gated at load (SPEC.md section 5).
pub(crate) fn class_bit(class: &str) -> Option<NotifyEvent> {
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
pub(crate) fn uncapturable_class(class: &str) -> Option<&'static str> {
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
pub(crate) fn parse_filter(s: &str) -> Result<ParsedFilter, RedisError> {
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

/// Validate the stream prefix (SPEC.md section 7): non-empty, at most 128
/// bytes, charset `A-Z a-z 0-9 : . _ - { }`. Glob metacharacters are outside
/// the charset, so the discovery `SCAN MATCH <prefix>*` pattern never needs
/// escaping. An empty prefix would make the feedback guard match every key.
pub(crate) fn validate_prefix(prefix: &str) -> Result<(), RedisError> {
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
pub(crate) struct FilterConfig {
    pub(crate) raw: RedisGILGuard<String>,
    pub(crate) parsed: RedisGILGuard<ParsedFilter>,
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

/// Byte-glob match with Redis `stringmatchlen` semantics (`*`, `?`, `[...]`
/// classes and ranges, `\` escape), case-sensitive, operating on raw bytes so
/// it never allocates or lossy-decodes and works on arbitrary binary keys
/// (issue #61, SPEC.md section 7 key-filter grammar). A faithful port of
/// Redis's `stringmatchlen_impl`, including the `skip_longer` early-out that
/// bounds the cost of adversarial patterns (`a*a*a*...`) and a recursion-depth
/// guard against a hostile pattern exhausting the stack.
pub(crate) fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut skip_longer = false;
    glob_match_impl(pattern, 0, string, 0, &mut skip_longer, 0)
}

pub(crate) fn glob_match_impl(
    pat: &[u8],
    mut pi: usize,
    s: &[u8],
    mut si: usize,
    skip_longer: &mut bool,
    nesting: u32,
) -> bool {
    if nesting > 1000 {
        return false;
    }
    while pi < pat.len() && si < s.len() {
        match pat[pi] {
            b'*' => {
                while pi + 1 < pat.len() && pat[pi + 1] == b'*' {
                    pi += 1;
                }
                if pi + 1 == pat.len() {
                    return true;
                }
                while si < s.len() {
                    if glob_match_impl(pat, pi + 1, s, si, skip_longer, nesting + 1) {
                        return true;
                    }
                    if *skip_longer {
                        return false;
                    }
                    si += 1;
                }
                *skip_longer = true;
                return false;
            }
            b'?' => {
                si += 1;
            }
            b'[' => {
                pi += 1;
                let negate = pi < pat.len() && pat[pi] == b'^';
                if negate {
                    pi += 1;
                }
                let mut matched = false;
                loop {
                    if pi >= pat.len() {
                        // Unterminated class: back up so the trailing byte is
                        // consumed by the outer advance, matching Redis.
                        pi -= 1;
                        break;
                    } else if pat[pi] == b'\\' && pat.len() - pi >= 2 {
                        pi += 1;
                        if pat[pi] == s[si] {
                            matched = true;
                        }
                    } else if pat[pi] == b']' {
                        break;
                    } else if pat.len() - pi >= 3 && pat[pi + 1] == b'-' {
                        let mut start = pat[pi];
                        let mut end = pat[pi + 2];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        pi += 2;
                        if s[si] >= start && s[si] <= end {
                            matched = true;
                        }
                    } else if pat[pi] == s[si] {
                        matched = true;
                    }
                    pi += 1;
                }
                if negate {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                si += 1;
            }
            b'\\' if pat.len() - pi >= 2 => {
                pi += 1;
                if pat[pi] != s[si] {
                    return false;
                }
                si += 1;
            }
            c => {
                if c != s[si] {
                    return false;
                }
                si += 1;
            }
        }
        pi += 1;
        if si == s.len() {
            while pi < pat.len() && pat[pi] == b'*' {
                pi += 1;
            }
            break;
        }
    }
    pi == pat.len() && si == s.len()
}

/// Parsed form of the `eventstream.key-filter` glob list (issue #61). `star`
/// is set when any pattern is a bare `*`, letting the notification hot path
/// short-circuit the default match-all without a byte scan. Patterns are the
/// raw bytes of each token, matched against the raw key bytes via
/// [`glob_match`]; multiple patterns OR together.
#[derive(Clone, Debug, Default)]
pub(crate) struct ParsedKeyFilter {
    pub(crate) star: bool,
    pub(crate) patterns: Vec<Vec<u8>>,
}

impl ParsedKeyFilter {
    pub(crate) fn matches(&self, key: &[u8]) -> bool {
        self.star || self.patterns.iter().any(|p| glob_match(p, key))
    }
}

/// Parse the key-filter grammar: `glob ("," glob)*`. Whitespace around each
/// pattern is trimmed; empty patterns and the empty string are rejected, the
/// same rule as `eventstream.events` (to pause the module use
/// `eventstream.enabled no`). A bare `*` sets the match-all short-circuit.
pub(crate) fn parse_key_filter(s: &str) -> Result<ParsedKeyFilter, RedisError> {
    let mut filter = ParsedKeyFilter::default();
    for raw_token in s.split(',') {
        let token = raw_token.trim();
        if token.is_empty() {
            return Err(RedisError::String(
                "empty key-filter pattern; to pause the module use 'eventstream.enabled no'"
                    .to_owned(),
            ));
        }
        if token == "*" {
            filter.star = true;
        } else {
            filter.patterns.push(token.as_bytes().to_vec());
        }
    }
    Ok(filter)
}

/// `eventstream.key-filter` config binding (issue #61): stores the raw string
/// (for `CONFIG GET`) and the parsed pattern list behind a `RedisGILGuard`, the
/// notification handler reading the parsed form under the GIL without extra
/// locking. Same shape as [`FilterConfig`]; rejection from `set()` surfaces as
/// the `CONFIG SET` error reply (SPEC.md section 7).
pub(crate) struct KeyFilterConfig {
    pub(crate) raw: RedisGILGuard<String>,
    pub(crate) parsed: RedisGILGuard<ParsedKeyFilter>,
}

impl ConfigurationValue<RedisString> for KeyFilterConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.raw.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        let parsed = parse_key_filter(s)?;
        *self.parsed.lock(ctx) = parsed;
        *self.raw.lock(ctx) = s.to_owned();
        Ok(())
    }
}

/// Parsed form of the `eventstream.source-dbs` filter (issue #63). `star` makes
/// the filter a match-all no-op (the default); otherwise `dbs` holds the named
/// database indexes and an event's origin db must be a member.
#[derive(Clone, Debug, Default)]
pub(crate) struct ParsedDbFilter {
    pub(crate) star: bool,
    pub(crate) dbs: HashSet<u32>,
}

impl ParsedDbFilter {
    pub(crate) fn matches(&self, db: i32) -> bool {
        // `RedisModule_GetSelectedDb` yields a non-negative index; guard the
        // cast regardless. Out-of-range indexes simply never match (issue #63).
        self.star || (db >= 0 && self.dbs.contains(&(db as u32)))
    }
}

/// Parse the source-db grammar: `*` or `index ("," index)*`, each index a
/// non-negative decimal integer. Whitespace around tokens is trimmed; empty
/// tokens, the empty string, and non-`u32` tokens are rejected. The server's
/// databases count is not known at load, so any in-`u32`-range index is
/// accepted and an out-of-range one simply never matches (issue #63).
pub(crate) fn parse_source_dbs(s: &str) -> Result<ParsedDbFilter, RedisError> {
    let mut filter = ParsedDbFilter::default();
    for raw_token in s.split(',') {
        let token = raw_token.trim();
        if token.is_empty() {
            return Err(RedisError::String(
                "empty source-dbs token; to pause the module use 'eventstream.enabled no'"
                    .to_owned(),
            ));
        }
        if token == "*" {
            filter.star = true;
        } else {
            let db: u32 = token.parse().map_err(|_| {
                RedisError::String(format!(
                    "source-dbs token '{token}' is not a non-negative database index"
                ))
            })?;
            filter.dbs.insert(db);
        }
    }
    Ok(filter)
}

/// `eventstream.source-dbs` config binding (issue #63): same shape as
/// [`FilterConfig`], storing the raw string and the parsed index set behind a
/// `RedisGILGuard`.
pub(crate) struct SourceDbConfig {
    pub(crate) raw: RedisGILGuard<String>,
    pub(crate) parsed: RedisGILGuard<ParsedDbFilter>,
}

impl ConfigurationValue<RedisString> for SourceDbConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.raw.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        let parsed = parse_source_dbs(s)?;
        *self.parsed.lock(ctx) = parsed;
        *self.raw.lock(ctx) = s.to_owned();
        Ok(())
    }
}

/// Parse the maxlen-overrides grammar (issue #62): `event=cap ("," event=cap)*`,
/// each `cap` a non-negative i64 (`0` disables trimming for that stream, exactly
/// as the global `maxlen 0` does). Whitespace around tokens is trimmed; the
/// empty string yields an empty map (no overrides, the default). Rejects empty
/// tokens, a token with no `=`, an empty event name, and a non-integer or
/// negative cap. Caps are re-validated here (not only by the registered range)
/// because module-arg values bypass the boundary check, the same rationale as
/// `MaxlenConfig`. The key is matched against the destination stream *suffix*,
/// so it is the raw (sanitized) event name, e.g. `expired`; a literal `#control`
/// targets the control stream (SPEC.md section 9). The `#` namespace cannot
/// collide with a real event name (the sanitizer never emits `#`), so no name is
/// stripped or sanitized here.
pub(crate) fn parse_maxlen_overrides(s: &str) -> Result<HashMap<String, i64>, RedisError> {
    let mut map = HashMap::new();
    if s.trim().is_empty() {
        return Ok(map);
    }
    for raw_token in s.split(',') {
        let token = raw_token.trim();
        if token.is_empty() {
            return Err(RedisError::String(
                "empty maxlen-overrides token; leave the value empty to clear all overrides"
                    .to_owned(),
            ));
        }
        let (name, cap_s) = token.split_once('=').ok_or_else(|| {
            RedisError::String(format!(
                "maxlen-overrides entry '{token}' is not 'event=cap'"
            ))
        })?;
        let name = name.trim();
        let cap_s = cap_s.trim();
        if name.is_empty() {
            return Err(RedisError::String(format!(
                "maxlen-overrides entry '{token}' has an empty event name"
            )));
        }
        let cap: i64 = cap_s.parse().map_err(|_| {
            RedisError::String(format!(
                "maxlen-overrides cap for '{name}' is not an integer: '{cap_s}'"
            ))
        })?;
        if cap < 0 {
            return Err(RedisError::String(format!(
                "maxlen-overrides cap for '{name}' must be 0 (trimming disabled) or positive, \
                 got {cap}"
            )));
        }
        map.insert(name.to_owned(), cap);
    }
    Ok(map)
}

/// `eventstream.maxlen-overrides` config binding (issue #62): per-event `maxlen`
/// caps keyed by destination stream suffix, overriding the global
/// `eventstream.maxlen` for the named streams. Same shape as [`FilterConfig`],
/// storing the raw string (for `CONFIG GET`) and the parsed map behind a
/// `RedisGILGuard` the write path reads under the GIL. Default empty (no
/// overrides); rejection from `set()` surfaces as the `CONFIG SET` error reply.
pub(crate) struct MaxlenOverridesConfig {
    pub(crate) raw: RedisGILGuard<String>,
    pub(crate) parsed: RedisGILGuard<HashMap<String, i64>>,
}

impl ConfigurationValue<RedisString> for MaxlenOverridesConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.raw.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        let parsed = parse_maxlen_overrides(s)?;
        *self.parsed.lock(ctx) = parsed;
        *self.raw.lock(ctx) = s.to_owned();
        Ok(())
    }
}

/// The effective count cap for a destination stream (issue #62): the per-event
/// override when the map names this stream *suffix*, else the global
/// `eventstream.maxlen`. The suffix is the sanitized event name for data streams
/// or a `#`-namespaced literal (`#control`) for the module's own streams; the
/// `#` namespace cannot collide with a real event name, so `#control` is
/// addressable while an ordinary event name never is by accident. The firehose
/// deliberately does not consult overrides (it aggregates every event type, so
/// its window is sized for the total rate, SPEC.md section 11) and passes the
/// global cap directly.
pub(crate) fn effective_maxlen(overrides: &HashMap<String, i64>, suffix: &str, global: i64) -> i64 {
    overrides.get(suffix).copied().unwrap_or(global)
}

/// `eventstream.stream-prefix` config binding. Registered IMMUTABLE, so `set`
/// runs only at load time (defaults and module args); validation failure
/// aborts the load.
pub(crate) struct PrefixConfig {
    pub(crate) value: RedisGILGuard<String>,
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
pub(crate) struct ClusterStreamsConfig {
    pub(crate) value: RedisGILGuard<String>,
}

impl ClusterStreamsConfig {
    pub(crate) fn is_per_node<G: redis_module::RedisLockIndicator>(&self, ctx: &G) -> bool {
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

/// Validate `eventstream.auto-group` (issue #109). Empty disables the feature
/// (the default); a non-empty value names the consumer group the module
/// auto-creates on each destination stream. Borrows the spirit of
/// [`validate_prefix`]: at most 128 bytes over `A-Z a-z 0-9 : . _ -`, which
/// rejects empty tokens and whitespace (neither is in the charset) so the name
/// never needs quoting in the `XGROUP CREATE` call. `{`/`}` are excluded (a
/// group name is a plain token, not a key needing a hash tag). Rejection
/// surfaces as the CONFIG SET / load error.
pub(crate) fn validate_auto_group(name: &str) -> Result<(), RedisError> {
    if name.is_empty() {
        return Ok(());
    }
    if name.len() > MAX_PREFIX_LEN {
        return Err(RedisError::String(format!(
            "auto-group exceeds {MAX_PREFIX_LEN} bytes"
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | ':' | '.' | '_' | '-'))
    {
        return Err(RedisError::String(format!(
            "auto-group contains disallowed character '{bad}'"
        )));
    }
    Ok(())
}

/// `eventstream.auto-group` config binding (issue #109). Runtime-mutable
/// (DEFAULT): setting a group name provisions it on each stream's next write
/// (`AUTO_GROUP_ENABLED` gates the write path; the per-stream `group_created`
/// bit dedupes). Setting it does not retroactively sweep the registry (SPEC.md
/// section 9): a registered stream that never fires again keeps no group.
pub(crate) struct AutoGroupConfig {
    pub(crate) value: RedisGILGuard<String>,
}

impl AutoGroupConfig {
    /// The configured group name, or `None` when disabled (empty). Read on the
    /// write path only after `AUTO_GROUP_ENABLED` confirms a name is set, so
    /// the disabled default never locks this guard.
    pub(crate) fn group_name<G: redis_module::RedisLockIndicator>(
        &self,
        ctx: &G,
    ) -> Option<String> {
        let g = self.value.lock(ctx);
        if g.is_empty() {
            None
        } else {
            Some(g.clone())
        }
    }
}

impl ConfigurationValue<RedisString> for AutoGroupConfig {
    fn get(&self, ctx: &ConfigurationContext) -> RedisString {
        RedisString::create(None, self.value.lock(ctx).as_str())
    }
    fn set(&self, ctx: &ConfigurationContext, val: RedisString) -> Result<(), RedisError> {
        let s = val.try_as_str()?;
        validate_auto_group(s)?;
        // The cheap write-path gate mirrors the stored value; set before the
        // guard so a concurrent reader that sees the flag also sees the name.
        AUTO_GROUP_ENABLED.store(!s.is_empty(), Ordering::Relaxed);
        *self.value.lock(ctx) = s.to_owned();
        Ok(())
    }
}

/// `eventstream.enabled` on-changed callback. Cannot write to the keyspace
/// (the ConfigurationContext has no command capability at v2.1.3), so enable
/// and disable transitions record pending markers. Also fires during
/// LoadConfigs inside OnLoad; `LAST_ENABLED` starting at the default makes
/// that a no-op unless the load args change the value.
pub(crate) fn enabled_changed(
    config_ctx: &ConfigurationContext,
    _name: &str,
    _val: &'static AtomicBool,
) {
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
        record_pending_marker(
            config_ctx,
            PendingMarker::Simple(if now { "enabled" } else { "disabled" }),
        );
    }
}

enum_configuration! {
    /// `eventstream.entry-format` values (issue #60), the module's first enum
    /// config. Variant names are the byte-exact config strings (`stringify!`
    /// in the wrapper's `enum_configuration!` grammar), so they are lowercase
    /// rather than `UpperCamelCase`. The discriminants are the wire values the
    /// wrapper stores; they are never emitted, so their order is free.
    ///
    /// `fixed` is the historical three-field schema, emitted byte-for-byte
    /// unchanged and without a `format` discriminator so existing consumers are
    /// unaffected by default. The other three each carry a leading `format`
    /// field so a stream that mixes formats — after a live `CONFIG SET`, which
    /// this config allows (DEFAULT, not IMMUTABLE) — is self-describing per
    /// entry (SPEC.md section 6).
    #[allow(non_camel_case_types)]
    #[derive(Copy, PartialEq, Eq, Debug)]
    pub(crate) enum EntryFormat {
        fixed = 0,
        minimal = 1,
        verbose = 2,
        json = 3,
    }
}
lazy_static! {
    pub(crate) static ref FILTER: FilterConfig = FilterConfig {
        raw: RedisGILGuard::new("expired".to_owned()),
        // Defensive: initialized to the parsed default so the handler behaves
        // correctly even before LoadConfigs applies the registered default.
        parsed: RedisGILGuard::new(
            parse_filter("expired").expect("default filter must parse")
        ),
    };

    /// `eventstream.key-filter` (issue #61). Default `*` matches every key, so
    /// the filter is a no-op until an operator narrows it.
    pub(crate) static ref KEY_FILTER: KeyFilterConfig = KeyFilterConfig {
        raw: RedisGILGuard::new("*".to_owned()),
        parsed: RedisGILGuard::new(
            parse_key_filter("*").expect("default key-filter must parse")
        ),
    };

    /// `eventstream.source-dbs` (issue #63). Default `*` captures every
    /// database, the pre-filter behavior.
    pub(crate) static ref SOURCE_DBS: SourceDbConfig = SourceDbConfig {
        raw: RedisGILGuard::new("*".to_owned()),
        parsed: RedisGILGuard::new(
            parse_source_dbs("*").expect("default source-dbs must parse")
        ),
    };

    /// `eventstream.maxlen-overrides` (issue #62). Default empty: no per-event
    /// caps, every stream trims under the global `eventstream.maxlen`.
    pub(crate) static ref MAXLEN_OVERRIDES: MaxlenOverridesConfig = MaxlenOverridesConfig {
        raw: RedisGILGuard::new(String::new()),
        parsed: RedisGILGuard::new(
            parse_maxlen_overrides("").expect("empty maxlen-overrides must parse")
        ),
    };

    pub(crate) static ref PREFIX: PrefixConfig = PrefixConfig {
        value: RedisGILGuard::new("events:".to_owned()),
    };

    pub(crate) static ref CLUSTER_STREAMS: ClusterStreamsConfig = ClusterStreamsConfig {
        value: RedisGILGuard::new("refuse".to_owned()),
    };

    /// `eventstream.auto-group` (issue #109). Default empty: no group is
    /// created, today's operator-side `XGROUP CREATE` behavior unchanged.
    pub(crate) static ref AUTO_GROUP: AutoGroupConfig = AutoGroupConfig {
        value: RedisGILGuard::new(String::new()),
    };

    /// `eventstream.entry-format` (issue #60), the module's first enum config.
    /// Default `fixed` reproduces the historical three-field schema exactly, so
    /// existing consumers are unaffected until an operator opts into another
    /// format. Runtime-mutable (DEFAULT): a live `CONFIG SET` takes effect on
    /// the next captured event, and the `format` discriminator on every
    /// non-`fixed` entry keeps the resulting mixed-format stream self-describing
    /// (SPEC.md section 6).
    pub(crate) static ref ENTRY_FORMAT: RedisGILGuard<EntryFormat> =
        RedisGILGuard::new(EntryFormat::fixed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_format_config_strings_are_lowercase() {
        // The config accepts the byte-exact variant names, so they must be the
        // documented `fixed`/`minimal`/`verbose`/`json` (SPEC.md section 7).
        use redis_module::configuration::EnumConfigurationValue;
        let (names, _vals) = EntryFormat::fixed.get_options();
        assert_eq!(names, vec!["fixed", "minimal", "verbose", "json"]);
    }

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

    // --- Key-name glob filter (issue #61) ---

    #[test]
    fn glob_literal_and_wildcards() {
        assert!(glob_match(b"session:*", b"session:abc123"));
        assert!(!glob_match(b"session:*", b"cache:xyz"));
        assert!(glob_match(b"*", b"anything"));
        // Faithful to Redis `stringmatchlen`: a leading `*` against an empty
        // string returns false (the match loop is entered only while the string
        // has bytes left). The empty-key case is covered by the filter's `star`
        // short-circuit (see `key_filter_default_matches_all`), not this path.
        assert!(!glob_match(b"*", b""));
        assert!(glob_match(b"foo", b"foo"));
        assert!(!glob_match(b"foo", b"foo!"));
        // Trailing literal after a star must still match (regression guard for
        // the trailing-`*` handling in the port).
        assert!(glob_match(b"a*z", b"az"));
        assert!(glob_match(b"a*z", b"abcz"));
        assert!(!glob_match(b"a*z", b"abc"));
    }

    #[test]
    fn glob_question_class_and_escape() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(glob_match(b"[abc]at", b"bat"));
        assert!(!glob_match(b"[abc]at", b"dat"));
        assert!(glob_match(b"[^abc]at", b"dat"));
        assert!(!glob_match(b"[^abc]at", b"bat"));
        assert!(glob_match(b"[a-z]9", b"m9"));
        assert!(!glob_match(b"[a-z]9", b"M9"));
        // Backslash escapes a metacharacter so it matches literally.
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
    }

    #[test]
    fn glob_matches_raw_binary_bytes() {
        // Keys are arbitrary bytes; a `?` matches one byte regardless of UTF-8.
        assert!(glob_match(b"k:?", &[b'k', b':', 0xff]));
        assert!(glob_match(b"*", &[0x00, 0xfe, 0xff]));
        assert!(glob_match(&[0xff, b'*'], &[0xff, 0x01, 0x02]));
        assert!(!glob_match(&[0xff, b'*'], &[0xfe, 0x01]));
    }

    #[test]
    fn key_filter_default_matches_all() {
        let f = parse_key_filter("*").unwrap();
        assert!(f.star);
        assert!(f.matches(b"anything"));
        assert!(f.matches(&[0xff, 0x00]));
        // The default filter matches even an empty key via the star
        // short-circuit, without calling into `glob_match`.
        assert!(f.matches(b""));
    }

    #[test]
    fn key_filter_multiple_patterns_or_together() {
        let f = parse_key_filter("session:*, cache:*").unwrap();
        assert!(!f.star);
        assert!(f.matches(b"session:1"));
        assert!(f.matches(b"cache:1"));
        assert!(!f.matches(b"user:1"));
    }

    #[test]
    fn key_filter_trims_and_rejects_empty() {
        assert!(parse_key_filter("").is_err());
        assert!(parse_key_filter("session:*,").is_err());
        assert!(parse_key_filter("a,,b").is_err());
        // Whitespace around patterns is trimmed.
        let f = parse_key_filter("  session:*  ").unwrap();
        assert!(f.matches(b"session:x"));
    }

    // --- Source-db filter (issue #63) ---

    #[test]
    fn source_dbs_default_matches_all() {
        let f = parse_source_dbs("*").unwrap();
        assert!(f.star);
        assert!(f.matches(0));
        assert!(f.matches(15));
    }

    #[test]
    fn source_dbs_named_membership() {
        let f = parse_source_dbs("0,2,5").unwrap();
        assert!(!f.star);
        assert!(f.matches(0));
        assert!(f.matches(2));
        assert!(f.matches(5));
        assert!(!f.matches(1));
        assert!(!f.matches(3));
        // A negative or out-of-range origin never matches a named list.
        assert!(!f.matches(-1));
        assert!(!f.matches(9));
    }

    #[test]
    fn source_dbs_rejects_empty_and_non_integer() {
        assert!(parse_source_dbs("").is_err());
        assert!(parse_source_dbs("0,").is_err());
        assert!(parse_source_dbs("0,,2").is_err());
        assert!(parse_source_dbs("0,foo").is_err());
        assert!(parse_source_dbs("-1").is_err());
        // Whitespace around indexes is trimmed.
        let f = parse_source_dbs(" 0 , 2 ").unwrap();
        assert!(f.matches(0) && f.matches(2));
    }

    // --- per-event maxlen overrides (issue #62) ---

    #[test]
    fn maxlen_overrides_default_empty_falls_back_to_global() {
        // The default (empty) map names no stream, so every suffix resolves to
        // the global cap: today's single-cap behavior, unchanged.
        let m = parse_maxlen_overrides("").unwrap();
        assert!(m.is_empty());
        assert_eq!(effective_maxlen(&m, "expired", 10_000), 10_000);
        assert_eq!(effective_maxlen(&m, "set", 10_000), 10_000);
    }

    #[test]
    fn maxlen_overrides_parse_and_resolve_per_event() {
        // A named stream takes its override; an unnamed one falls back to the
        // global cap (issue #62). Whitespace around tokens and the `=` is
        // trimmed.
        let m = parse_maxlen_overrides("expired=600000, set = 1000 ").unwrap();
        assert_eq!(effective_maxlen(&m, "expired", 10_000), 600_000);
        assert_eq!(effective_maxlen(&m, "set", 10_000), 1_000);
        assert_eq!(effective_maxlen(&m, "del", 10_000), 10_000);
    }

    #[test]
    fn maxlen_overrides_zero_disables_for_that_stream() {
        // A `0` override disables trimming for just that stream (like global
        // `maxlen 0`), while others keep the global cap.
        let m = parse_maxlen_overrides("audit=0").unwrap();
        assert_eq!(effective_maxlen(&m, "audit", 10_000), 0);
        assert_eq!(effective_maxlen(&m, "set", 10_000), 10_000);
    }

    #[test]
    fn maxlen_overrides_control_stream_is_addressable() {
        // The `#` namespace cannot collide with a sanitized event name, so a
        // literal `#control` targets the control stream (issue #62, SPEC.md
        // section 9).
        let m = parse_maxlen_overrides("#control=50").unwrap();
        assert_eq!(effective_maxlen(&m, "#control", 10_000), 50);
        assert_eq!(effective_maxlen(&m, "expired", 10_000), 10_000);
    }

    #[test]
    fn maxlen_overrides_reject_malformed_and_negative() {
        assert!(parse_maxlen_overrides("expired").is_err()); // no '='
        assert!(parse_maxlen_overrides("=100").is_err()); // empty name
        assert!(parse_maxlen_overrides("expired=").is_err()); // empty cap
        assert!(parse_maxlen_overrides("expired=abc").is_err()); // non-integer
        assert!(parse_maxlen_overrides("expired=-1").is_err()); // negative
        assert!(parse_maxlen_overrides("expired=100,").is_err()); // trailing empty
        assert!(parse_maxlen_overrides("expired=100,,set=1").is_err()); // empty middle
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
    fn auto_group_validation() {
        // Empty is the disabled default and must validate (issue #109).
        assert!(validate_auto_group("").is_ok());
        assert!(validate_auto_group("workers").is_ok());
        assert!(validate_auto_group("expiry-workers").is_ok());
        assert!(validate_auto_group("grp.1:v2_a").is_ok());
        // Whitespace and glob/quoting metacharacters are outside the charset,
        // so a name never needs quoting in the XGROUP CREATE call.
        assert!(validate_auto_group("two words").is_err());
        assert!(validate_auto_group("grp*").is_err());
        assert!(validate_auto_group("{tag}").is_err());
        assert!(validate_auto_group(&"g".repeat(129)).is_err());
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;

    // Every `@class` token the grammar accepts, paired with the subscription
    // bit the parser must map it to (SPEC.md section 7). Kept in sync with
    // `class_bit` by the constructed-filter property below.
    const CAPTURABLE_CLASSES: [(&str, NotifyEvent); 12] = [
        ("generic", NotifyEvent::GENERIC),
        ("string", NotifyEvent::STRING),
        ("list", NotifyEvent::LIST),
        ("set", NotifyEvent::SET),
        ("hash", NotifyEvent::HASH),
        ("zset", NotifyEvent::ZSET),
        ("stream", NotifyEvent::STREAM),
        ("expired", NotifyEvent::EXPIRED),
        ("evicted", NotifyEvent::EVICTED),
        ("module", NotifyEvent::MODULE),
        ("missed", NotifyEvent::MISSED),
        ("new", NotifyEvent::NEW),
    ];

    // The sanitizer's output alphabet and the bare-name charset are the same
    // set (SPEC.md section 5): names drawn from it cannot collide with the
    // `*`, `@class`, or `,` grammar structure, and cannot carry whitespace.
    const NAME_PATTERN: &str = "[A-Za-z0-9_.:-]{1,24}";

    /// One grammar token together with the parse outcome it must produce.
    #[derive(Clone, Debug)]
    enum Token {
        Star,
        Class(&'static str, NotifyEvent),
        Name(String),
    }

    impl Token {
        fn text(&self) -> String {
            match self {
                Token::Star => "*".to_owned(),
                Token::Class(class, _) => format!("@{class}"),
                Token::Name(name) => name.clone(),
            }
        }
    }

    fn token() -> impl Strategy<Value = Token> {
        prop_oneof![
            1 => Just(Token::Star),
            4 => prop::sample::select(&CAPTURABLE_CLASSES[..])
                .prop_map(|(class, bit)| Token::Class(class, bit)),
            4 => NAME_PATTERN.prop_map(Token::Name),
        ]
    }

    proptest! {
        // The default case count (256, PROPTEST_CASES overrides for longer
        // local searches; CONTRIBUTING.md) rides the existing `cargo test
        // --lib` budget. Shrinking is capped by iteration count so a failure
        // cannot stall CI, and regression persistence is off: there is no
        // corpus directory in a cdylib crate and CI runners would discard it.
        #![proptest_config(ProptestConfig {
            max_shrink_iters: 2048,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn parse_is_total_and_accepted_names_are_wellformed(s in any::<String>()) {
            // Err is the only rejection channel (it becomes the CONFIG SET
            // error reply); anything accepted as an exact name must be a
            // plausible routable event name.
            if let Ok(f) = parse_filter(&s) {
                for name in &f.names {
                    prop_assert!(!name.is_empty());
                    prop_assert!(!name.chars().any(char::is_whitespace), "{:?}", name);
                    prop_assert!(!name.contains(','), "{:?}", name);
                    prop_assert!(!name.starts_with('@'), "{:?}", name);
                    prop_assert_ne!(name.as_str(), "*");
                }
            }
        }

        // The idempotence property from issue #94, stated over the raw string:
        // `ParsedFilter` has no Display and CONFIG GET returns the stored raw
        // string, so a parse -> format -> parse round trip is not expressible.
        #[test]
        fn accepted_filters_reparse_identically(s in any::<String>()) {
            if let Ok(a) = parse_filter(&s) {
                let b = parse_filter(&s).expect("second parse of an accepted string");
                prop_assert_eq!(a.star, b.star);
                prop_assert_eq!(a.classes, b.classes);
                prop_assert_eq!(a.names, b.names);
            }
        }

        #[test]
        fn constructed_valid_filters_parse_exactly(
            parts in prop::collection::vec((token(), "[ \t]{0,2}", "[ \t]{0,2}"), 1..8)
        ) {
            let raw = parts
                .iter()
                .map(|(t, lead, trail)| format!("{lead}{}{trail}", t.text()))
                .collect::<Vec<_>>()
                .join(",");
            let mut star = false;
            let mut classes = NotifyEvent::empty();
            let mut names = HashSet::new();
            for (t, _, _) in &parts {
                match t {
                    Token::Star => star = true,
                    Token::Class(_, bit) => classes |= *bit,
                    Token::Name(name) => {
                        names.insert(name.clone());
                    }
                }
            }
            let parsed = parse_filter(&raw);
            prop_assert!(parsed.is_ok(), "rejected {:?}", raw);
            let f = parsed.unwrap();
            prop_assert_eq!(f.star, star);
            prop_assert_eq!(f.classes, classes);
            prop_assert_eq!(f.names, names);
        }

        #[test]
        fn empty_tokens_always_rejected(
            front in prop::collection::vec(token(), 0..4),
            back in prop::collection::vec(token(), 0..4),
            pad in "[ \t]{0,3}",
        ) {
            let mut tokens: Vec<String> = front.iter().map(Token::text).collect();
            tokens.push(pad);
            tokens.extend(back.iter().map(Token::text));
            prop_assert!(parse_filter(&tokens.join(",")).is_err());
        }

        // Covers both arms behind `@`: unknown classes and the two known
        // uncapturable ones; every `@` token outside `class_bit` is rejected.
        #[test]
        fn non_capturable_classes_always_rejected(class in "[A-Za-z0-9]{0,12}") {
            prop_assume!(class_bit(&class).is_none());
            let token = format!("@{class}");
            prop_assert!(parse_filter(&token).is_err());
        }

        #[test]
        fn whitespace_inside_names_always_rejected(
            a in NAME_PATTERN,
            ws in "[ \t]{1,2}",
            b in NAME_PATTERN,
        ) {
            // prop_assert! stringifies its condition into a format string, so
            // the format! call must live outside the macro's argument.
            let name = format!("{a}{ws}{b}");
            prop_assert!(parse_filter(&name).is_err());
        }

        // Matching runs on the notification hot path under the GIL; it must
        // be a plain bool for any event name against any subscription bit.
        #[test]
        fn matching_is_total(s in any::<String>(), event in any::<String>()) {
            if let Ok(f) = parse_filter(&s) {
                let _ = f.matches(NotifyEvent::empty(), &event);
                for (_, bit) in CAPTURABLE_CLASSES {
                    let _ = f.matches(bit, &event);
                }
            }
        }

        // Exact acceptance predicate (SPEC.md section 7): non-empty, at most
        // MAX_PREFIX_LEN bytes, charset without glob metacharacters so the
        // discovery `SCAN MATCH <prefix>*` pattern never needs escaping.
        #[test]
        fn prefix_validation_is_total_and_exact(s in any::<String>()) {
            let accepted = validate_prefix(&s).is_ok();
            let wellformed = !s.is_empty()
                && s.len() <= MAX_PREFIX_LEN
                && s.chars().all(|c| {
                    matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | ':' | '.' | '_' | '-' | '{' | '}')
                });
            prop_assert_eq!(accepted, wellformed);
        }

        #[test]
        fn charset_prefixes_always_accepted(s in "[A-Za-z0-9:._{}-]{1,128}") {
            prop_assert!(validate_prefix(&s).is_ok());
        }
    }
}
