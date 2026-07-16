// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Observability counters and per-stream accounting (#86): the process-lifetime
//! `AtomicU64`/`AtomicBool` counters and latches (SPEC.md section 13), the
//! per-stream `StreamStats` record behind the WITHSTATS join, the drop-counting
//! helpers, and the `INFO eventstream` section handler.

use lazy_static::lazy_static;
use redis_module::{Context, RedisGILGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

// Counters owned by sibling modules that the shared `stats_snapshot` reads.
// Un-gated (unlike the INFO-only imports below) because `stats_snapshot` feeds
// the deinit log too, which compiles in test builds (issue #88).
use crate::cluster::{
    DROPPED_MIGRATING, DROPPED_NO_OWNED_SLOT, NODE_TAG, PER_NODE, REPINS, REPINS_PROBE_DETECTED,
};
use crate::config::ENABLED;
use crate::markers::CONTROL_MARKERS;
// INFO-section-only: `info_stats` is `#[cfg(not(test))]`, so these stay gated.
#[cfg(not(test))]
use redis_module::{InfoContext, RedisResult};
#[cfg(not(test))]
use redis_module_macros::info_command_handler;

// Counters (SPEC.md section 13): process-lifetime, monotonic, reset on load.
pub(crate) static FORWARDED: AtomicU64 = AtomicU64::new(0);

/// Copies written to the combined firehose stream when `eventstream.firehose`
/// is on (issue #58). Kept apart from `FORWARDED` so that counter keeps
/// meaning "captured events", not "XADDs issued"; firehose write failures
/// share the existing `dropped_*` counters.
pub(crate) static FIREHOSE_FORWARDED: AtomicU64 = AtomicU64::new(0);

/// Consumer groups auto-provisioned on destination streams when
/// `eventstream.auto-group` names one (issue #109): one `XGROUP CREATE` per
/// stream on the write that first sees the config set (or the write that
/// re-creates a stream a flush destroyed). Counts genuine creations only; a
/// BUSYGROUP reply — the group already existed, e.g. on a stream that survived
/// a restart — is idempotent success and not counted here. Kept apart from
/// `forwarded` because provisioning a group is not capturing an event.
pub(crate) static AUTOGROUP_CREATED: AtomicU64 = AtomicU64::new(0);

/// `XGROUP CREATE` failures other than BUSYGROUP (issue #109). The triggering
/// event was still captured (the group provisioning is a side effect of a
/// successful `XADD`), so this stays out of the `dropped_*` sum; it follows the
/// drop-counter and first-failure-log policy (SPEC.md section 13), latching on
/// `LOGGED_AUTOGROUP`.
pub(crate) static AUTOGROUP_FAILED: AtomicU64 = AtomicU64::new(0);

pub(crate) static DROPPED_XADD_ERROR: AtomicU64 = AtomicU64::new(0);

pub(crate) static DROPPED_OOM: AtomicU64 = AtomicU64::new(0);

pub(crate) static DROPPED_DEFER_ERROR: AtomicU64 = AtomicU64::new(0);

/// Entries dropped because the configured `entry-format` could not encode the
/// event (issue #60): with the shipped formats only `json` can fail, on a
/// non-UTF-8 event name. Part of the `dropped` sum; a format+event pair that
/// fails to encode fails identically on the per-event write and the firehose
/// copy, so with the firehose on one such event counts two drops (SPEC.md
/// section 5 drop-per-write accounting). First-failure logging latches on
/// `LOGGED_ENCODE_ERROR`.
pub(crate) static DROPPED_ENCODE_ERROR: AtomicU64 = AtomicU64::new(0);

pub(crate) static SKIPPED_SELF: AtomicU64 = AtomicU64::new(0);

pub(crate) static SKIPPED_FILTERED: AtomicU64 = AtomicU64::new(0);

/// Events dropped by the key-name glob filter (`eventstream.key-filter`, issue
/// #61). Distinct from `SKIPPED_FILTERED` so operators can tell an over-narrow
/// event-name filter from an over-narrow key filter (SPEC.md section 13).
pub(crate) static SKIPPED_KEY_FILTERED: AtomicU64 = AtomicU64::new(0);

/// Events dropped because their origin database is outside
/// `eventstream.source-dbs` (issue #63).
pub(crate) static SKIPPED_DB: AtomicU64 = AtomicU64::new(0);

pub(crate) static SKIPPED_INVALID: AtomicU64 = AtomicU64::new(0);

/// Events dropped because creating their destination stream would exceed
/// `eventstream.max-streams` (issue #64); part of the `dropped` sum. Streams
/// already registered keep receiving events; only new-stream creation is
/// blocked. First-failure logging latches on `LOGGED_MAX_STREAMS`.
pub(crate) static DROPPED_MAX_STREAMS: AtomicU64 = AtomicU64::new(0);

pub(crate) static LOGGED_MAX_STREAMS: AtomicBool = AtomicBool::new(false);

/// Distinct destination streams written since load, excluding the control
/// stream; the membership records live in `STREAM_STATS`. Never resets, so it
/// can exceed the current distinct count after a flush (SPEC.md section 13).
pub(crate) static ACTIVE_STREAMS: AtomicU64 = AtomicU64::new(0);

/// Currently-registered distinct destination streams: like `ACTIVE_STREAMS`
/// but reset to 0 on flush (when `STREAM_STATS` is cleared), so it tracks the
/// streams the in-process registry cache currently knows about. This is the
/// basis for the `eventstream.max-streams` cap (issue #64): "streams already
/// known keep receiving events; only creation of new streams is blocked."
pub(crate) static CURRENT_STREAMS: AtomicI64 = AtomicI64::new(0);

/// Unix seconds of the most recent drop, 0 if none (SPEC.md section 13).
pub(crate) static LAST_ERROR_TIME: AtomicU64 = AtomicU64::new(0);

/// Whether the server's `maxmemory-policy` is an `allkeys-*` policy (issue
/// #106). Such a policy can evict the destination streams themselves, silently
/// destroying captured history — a different failure from the `M`-flag write
/// refusal counted in `dropped_oom`. Computed from the server config at load
/// and on each config change (capture.rs), surfaced as the derived 0/1
/// `eviction_risk` INFO field; the policy name itself stays in the log, not
/// INFO (SPEC.md section 13). `volatile-*` is not flagged: it evicts only keys
/// with a TTL, and the destination streams carry none.
pub(crate) static EVICTION_RISK: AtomicBool = AtomicBool::new(false);

// First-failure log latches for drop reasons with no destination stream in
// hand (SPEC.md section 13 logging policy). Stream-scoped failures (an XADD
// refused by a named destination) log through the per-stream rate-limited
// state in `STREAM_STATS` instead (issue #68); these latches remain for the
// process-level failures: SelectDb(0) in a job (`LOGGED_XADD_ERROR`) and
// job-registration failure (`LOGGED_DEFER_ERROR`), where name resolution is
// exactly what never ran.
pub(crate) static LOGGED_XADD_ERROR: AtomicBool = AtomicBool::new(false);

pub(crate) static LOGGED_DEFER_ERROR: AtomicBool = AtomicBool::new(false);

pub(crate) static LOGGED_PANIC: AtomicBool = AtomicBool::new(false);

/// First-failure latch for `dropped_encode_error` (issue #60): an encode
/// failure is a property of the format and the event, not of a destination
/// stream, so it uses a process-level latch like the other no-destination
/// drop reasons rather than the per-stream window (SPEC.md section 13).
pub(crate) static LOGGED_ENCODE_ERROR: AtomicBool = AtomicBool::new(false);

/// First-failure latch for `autogroup_failed` (issue #109). A group-creation
/// failure depends on the configured group name and the server's `XGROUP`
/// support, not on a specific destination, so it uses a process-level latch
/// like the other no-destination reasons rather than the per-stream window;
/// the logged text names the stream and the server's error (SPEC.md section 13).
pub(crate) static LOGGED_AUTOGROUP: AtomicBool = AtomicBool::new(false);

/// Repeat-failure warning window per destination stream, in seconds (issue
/// #68, SPEC.md section 13): after a stream's first-failure warning, further
/// failures on that stream are counted silently until the window elapses,
/// then the next failure logs again with the suppressed count.
pub(crate) const LOG_WINDOW_SECS: u64 = 60;

/// Panics caught at an FFI boundary, in either the notification callback or a
/// post-notification job (SPEC.md section 5). A nonzero value is a bug in this
/// module; the counter exists so it surfaces in INFO instead of aborting the
/// server.
pub(crate) static HANDLER_PANICS: AtomicU64 = AtomicU64::new(0);

/// Per-stream in-process state (issues #68 and #71), keyed by destination
/// stream name in `STREAM_STATS`. One record carries the per-stream counters
/// (the `EVENTSTREAM.STREAMS WITHSTATS` join, SPEC.md section 8), the
/// registry-membership bit the write path dedupes SADD on, and the
/// failure-logging window (SPEC.md section 13). One map so the per-event
/// success path pays its single existing lock acquisition, never two.
#[derive(Default)]
pub(crate) struct StreamStats {
    /// Entries written to this stream since load or the last flush
    /// invalidation. For the firehose stream this is the per-stream view of
    /// `firehose_forwarded`; for the control stream markers are counted in
    /// `control_markers`, never here.
    pub(crate) forwarded: u64,
    /// Entries dropped by a refused XADD to this stream (`dropped_xadd_error`
    /// and `dropped_oom` scopes) since load or the last flush invalidation.
    pub(crate) dropped: u64,
    /// Present in the persistent `<prefix><seg>#streams` set. Cleared with
    /// the whole map on flush so the next write re-registers.
    pub(crate) registered: bool,
    /// The `eventstream.auto-group` consumer group has been provisioned on
    /// this stream (issue #109). Deduped separately from `registered` so
    /// enabling the config provisions an already-registered ("warm") stream on
    /// its next write, and a transient `XGROUP CREATE` failure retries on the
    /// next write instead of waiting for a flush. Cleared with the whole map on
    /// flush, so a `FLUSHALL` that destroyed the group re-creates it. Never set
    /// while the config is empty, so the next write after it is set provisions.
    pub(crate) group_created: bool,
    /// Drops since the last successful write to this stream; nonzero means
    /// the stream is failing, and the next success logs the recovery notice.
    pub(crate) failing_drops: u64,
    /// Unix seconds of this stream's last logged failure warning, 0 if none.
    /// Reset on recovery so a recurrence logs immediately, not into a stale
    /// window.
    pub(crate) last_warned: u64,
    /// Failures counted but not logged since `last_warned`.
    pub(crate) suppressed: u64,
}

impl StreamStats {
    /// Failure half of the per-stream logging state machine (issue #68):
    /// count the drop and decide whether it may log. `Some(n)` means log
    /// now, `n` being the failures suppressed since the last warning. The
    /// window is per stream, not per (stream, reason): the logged text
    /// carries the server's error, which names the reason, and one lock plus
    /// one timestamp per stream keeps the state minimal.
    pub(crate) fn record_failure(&mut self, now: u64) -> Option<u64> {
        self.dropped += 1;
        self.failing_drops += 1;
        if self.last_warned == 0 || now.saturating_sub(self.last_warned) >= LOG_WINDOW_SECS {
            let suppressed = self.suppressed;
            self.last_warned = now;
            self.suppressed = 0;
            Some(suppressed)
        } else {
            self.suppressed += 1;
            None
        }
    }

    /// Success half: if the stream was failing, end the streak and return its
    /// drop count for the recovery notice. Resets the warning window so the
    /// next failing streak starts with a fresh first-failure warning. Does
    /// not count the success; `forwarded` is the write path's concern (marker
    /// writes recover a stream without counting as forwarded).
    pub(crate) fn record_success(&mut self) -> Option<u64> {
        let streak = self.failing_drops;
        if streak == 0 {
            return None;
        }
        self.failing_drops = 0;
        self.last_warned = 0;
        self.suppressed = 0;
        Some(streak)
    }
}

/// Unix seconds for `LAST_ERROR_TIME` and the per-stream warning window.
/// Only called on drop paths, so the syscall never taxes a healthy event.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Log the first failure per drop reason at warning; subsequent failures are
/// only counted (SPEC.md section 13). Stamps `LAST_ERROR_TIME`. For drops
/// with no destination stream in hand; a refused XADD to a named stream goes
/// through [`count_stream_drop`] instead.
pub(crate) fn count_drop(ctx: &Context, counter: &AtomicU64, latch: &AtomicBool, detail: &str) {
    counter.fetch_add(1, Ordering::Relaxed);
    LAST_ERROR_TIME.store(unix_now(), Ordering::Relaxed);
    if latch
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        ctx.log_warning(&format!("eventstream: {detail}"));
    }
}

/// Count a drop against its destination stream (issue #68): the global
/// counter and `LAST_ERROR_TIME` as in [`count_drop`], plus the per-stream
/// `dropped` counter, rate-limited per stream instead of latched per reason —
/// the first failure on each stream logs the full error text at warning, then
/// at most one warning per stream per [`LOG_WINDOW_SECS`], carrying the
/// suppressed count. Runs GIL-held (a post-notification job or deinit).
pub(crate) fn count_stream_drop(ctx: &Context, stream: &str, counter: &AtomicU64, detail: &str) {
    counter.fetch_add(1, Ordering::Relaxed);
    let now = unix_now();
    LAST_ERROR_TIME.store(now, Ordering::Relaxed);
    let mut stats = STREAM_STATS.lock(ctx);
    if !stats.contains_key(stream) {
        // A stream can fail before it ever succeeds (say, WRONGTYPE on first
        // write); its record starts unregistered and joins the registry on
        // its first successful write.
        stats.insert(stream.to_owned(), StreamStats::default());
    }
    let entry = stats.get_mut(stream).expect("inserted above");
    if let Some(suppressed) = entry.record_failure(now) {
        if suppressed > 0 {
            ctx.log_warning(&format!(
                "eventstream: {detail} ({suppressed} earlier failures on this \
                 stream suppressed in the last {LOG_WINDOW_SECS}s)"
            ));
        } else {
            ctx.log_warning(&format!("eventstream: {detail}"));
        }
    }
}

/// One field on the counter surface. `Int` covers every counter and the two
/// 0/1 gauges (`enabled`, `cluster_per_node`); `Text` is the sole string field,
/// `cluster_pinned_tag` (empty until a slot is pinned). Kept small so the three
/// emitters below can each render it their own way.
pub(crate) enum StatValue {
    Int(i64),
    Text(String),
}

/// The complete, ordered counter surface, read once. This is the single source
/// of truth for all three emitters — the `INFO eventstream` section, the
/// `EVENTSTREAM.STATS` reply, and the deinit final-counters log — so they cannot
/// drift out of agreement (issue #88; SPEC.md sections 8 and 13). The order here
/// is the wire order of both the INFO section and the STATS reply. `dropped` is
/// the sum of the `dropped_*` reasons, matching SPEC.md section 13.
pub(crate) fn stats_snapshot() -> Vec<(&'static str, StatValue)> {
    use StatValue::{Int, Text};
    let load = |c: &AtomicU64| Int(c.load(Ordering::Relaxed) as i64);
    let dropped = DROPPED_XADD_ERROR.load(Ordering::Relaxed)
        + DROPPED_OOM.load(Ordering::Relaxed)
        + DROPPED_DEFER_ERROR.load(Ordering::Relaxed)
        + DROPPED_MIGRATING.load(Ordering::Relaxed)
        + DROPPED_MAX_STREAMS.load(Ordering::Relaxed)
        + DROPPED_ENCODE_ERROR.load(Ordering::Relaxed);
    vec![
        ("enabled", Int(ENABLED.load(Ordering::Relaxed) as i64)),
        (
            "eviction_risk",
            Int(EVICTION_RISK.load(Ordering::Relaxed) as i64),
        ),
        ("forwarded", load(&FORWARDED)),
        ("firehose_forwarded", load(&FIREHOSE_FORWARDED)),
        ("autogroup_created", load(&AUTOGROUP_CREATED)),
        ("autogroup_failed", load(&AUTOGROUP_FAILED)),
        ("dropped", Int(dropped as i64)),
        ("dropped_xadd_error", load(&DROPPED_XADD_ERROR)),
        ("dropped_oom", load(&DROPPED_OOM)),
        ("dropped_defer_error", load(&DROPPED_DEFER_ERROR)),
        ("dropped_max_streams", load(&DROPPED_MAX_STREAMS)),
        ("dropped_encode_error", load(&DROPPED_ENCODE_ERROR)),
        ("skipped_self", load(&SKIPPED_SELF)),
        ("skipped_filtered", load(&SKIPPED_FILTERED)),
        ("skipped_key_filtered", load(&SKIPPED_KEY_FILTERED)),
        ("skipped_db", load(&SKIPPED_DB)),
        ("skipped_invalid", load(&SKIPPED_INVALID)),
        ("active_streams", load(&ACTIVE_STREAMS)),
        ("control_markers", load(&CONTROL_MARKERS)),
        ("handler_panics", load(&HANDLER_PANICS)),
        ("dropped_no_owned_slot", load(&DROPPED_NO_OWNED_SLOT)),
        ("dropped_migrating", load(&DROPPED_MIGRATING)),
        ("repins", load(&REPINS)),
        ("repins_probe_detected", load(&REPINS_PROBE_DETECTED)),
        (
            "cluster_per_node",
            Int(PER_NODE.load(Ordering::Relaxed) as i64),
        ),
        (
            "cluster_pinned_tag",
            Text(NODE_TAG.lock().unwrap().clone().unwrap_or_default()),
        ),
        ("last_error_time", load(&LAST_ERROR_TIME)),
    ]
}

/// Module INFO section (SPEC.md section 13). Redis prefixes the section and
/// every field with the module name: `INFO eventstream` shows
/// `# eventstream_stats` with `eventstream_forwarded` etc. Module sections do
/// not appear in plain `INFO`; use `INFO everything` or `INFO eventstream`.
#[cfg(not(test))]
#[info_command_handler]
pub(crate) fn info_stats(ctx: &InfoContext, _for_crash_report: bool) -> RedisResult<()> {
    let mut section = ctx.builder().add_section("stats");
    for (name, value) in stats_snapshot() {
        section = match value {
            StatValue::Int(i) => section.field(name, i)?,
            StatValue::Text(s) => section.field(name, s)?,
        };
    }
    section.build_section()?.build_info()?;
    Ok(())
}
lazy_static! {
    /// Per-stream records behind `ACTIVE_STREAMS` and the WITHSTATS join
    /// (issues #68, #71): registry membership, counters, failure-log state.
    /// Touched on the capture and marker write paths, with the GIL held.
    /// Bounded by the distinct destination names, and by
    /// `eventstream.max-streams` when that cap is set (issue #64).
    pub(crate) static ref STREAM_STATS: RedisGILGuard<HashMap<String, StreamStats>> =
        RedisGILGuard::new(HashMap::new());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_stats_first_failure_logs_then_window_suppresses() {
        let mut st = StreamStats::default();
        // First failure on the stream logs, with nothing suppressed.
        assert_eq!(st.record_failure(1000), Some(0));
        // Repeats inside the window are counted, never logged.
        assert_eq!(st.record_failure(1001), None);
        assert_eq!(st.record_failure(1030), None);
        assert_eq!(st.record_failure(1000 + LOG_WINDOW_SECS - 1), None);
        // The first failure at or past the window logs the suppressed count.
        assert_eq!(st.record_failure(1000 + LOG_WINDOW_SECS), Some(3));
        assert_eq!(st.dropped, 5);
        // The new warning opens a fresh window.
        assert_eq!(st.record_failure(1000 + LOG_WINDOW_SECS + 1), None);
    }

    #[test]
    fn stream_stats_recovery_reports_streak_and_rearms_logging() {
        let mut st = StreamStats::default();
        assert_eq!(st.record_failure(1000), Some(0));
        assert_eq!(st.record_failure(1001), None);
        assert_eq!(st.record_failure(1002), None);
        // Success after drops ends the streak: recovery notice carries all
        // three drops, logged or suppressed.
        assert_eq!(st.record_success(), Some(3));
        // The recovery reset the window, so a recurrence one second later
        // logs immediately instead of falling into the stale window.
        assert_eq!(st.record_failure(1003), Some(0));
        // Counters are cumulative across streaks.
        assert_eq!(st.dropped, 4);
    }

    #[test]
    fn stream_stats_success_without_failures_is_silent() {
        let mut st = StreamStats::default();
        assert_eq!(st.record_success(), None);
        assert_eq!(st.record_success(), None);
        // A clock that goes backwards suppresses rather than floods: the
        // saturating difference reads as within-window.
        assert_eq!(st.record_failure(1000), Some(0));
        assert_eq!(st.record_failure(900), None);
    }
}
