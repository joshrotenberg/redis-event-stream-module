// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Gap markers and FFI panic guards (#86): the pending-marker queue and its
//! deferred write to the `#control` stream (SPEC.md section 9), plus the
//! panic-catching guards shared by the post-notification job and the raw
//! server-event callbacks.

use crate::capture::{server_now_ms, xadd_call_options, Retention};
use crate::cluster::{count_no_slot_drop, tag_segment};
use crate::config::{effective_maxlen, MAXLEN, MAXLEN_OVERRIDES, PREFIX, RETENTION_MS};
use crate::stats::{
    count_drop, count_stream_drop, DROPPED_DEFER_ERROR, DROPPED_OOM, DROPPED_XADD_ERROR,
    HANDLER_PANICS, LOGGED_DEFER_ERROR, LOGGED_PANIC, LOGGED_XADD_ERROR, STREAM_STATS,
};
use lazy_static::lazy_static;
use redis_module::{raw, CallResult, Context, ContextFlags, RedisGILGuard, Status};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(crate) static CONTROL_MARKERS: AtomicU64 = AtomicU64::new(0);

/// Cheap dirty flag so the notification hot path pays one atomic load unless
/// a gap marker is actually pending (SPEC.md section 9 delivery mechanics).
pub(crate) static MARKERS_DIRTY: AtomicBool = AtomicBool::new(false);

/// A gap marker awaiting delivery through the pending-marker mechanism. Most
/// carry only an `action`; `Flushed` additionally carries the flushed database
/// number (`-1` for `FLUSHALL`), the one marker whose reconcile scope is
/// per-database (issues #74, #73). The extra `db` field appears only on the
/// `flushed` action, so consumers reading markers by `action` are unaffected
/// (SPEC.md section 9 marker schema).
#[derive(Clone, Copy)]
pub(crate) enum PendingMarker {
    /// A lifecycle marker carrying only `action` (+ `module-version`):
    /// `loaded`, `enabled`, `disabled`, `swapdb`.
    Simple(&'static str),
    /// A `flushed` marker carrying the flushed db (`-1` == `FLUSHALL`).
    Flushed(i32),
}

impl PendingMarker {
    /// The `action` field value.
    pub(crate) fn action(&self) -> &'static str {
        match self {
            PendingMarker::Simple(a) => a,
            PendingMarker::Flushed(_) => "flushed",
        }
    }
    /// The optional `db` field value; `Some` only for `flushed`.
    pub(crate) fn db(&self) -> Option<i32> {
        match self {
            PendingMarker::Flushed(db) => Some(*db),
            PendingMarker::Simple(_) => None,
        }
    }
}

/// Record a pending gap marker; the next notification callback writes it
/// (SPEC.md section 9 delivery mechanics).
pub(crate) fn record_pending_marker<G: redis_module::RedisLockIndicator>(
    lock: &G,
    marker: PendingMarker,
) {
    PENDING_MARKERS.lock(lock).push(marker);
    MARKERS_DIRTY.store(true, Ordering::Relaxed);
}

/// Write one gap marker to the control stream. Runs where keyspace writes are
/// safe (a post-notification job or the deinit hook). Same call options,
/// trimming, and drop accounting as mirrored entries (SPEC.md section 9). `db`
/// is `Some` only for the `flushed` action, which carries the flushed database
/// (`-1` for `FLUSHALL`) as an additive third field so consumers can bound the
/// reconcile to that database (issues #74, #73).
pub(crate) fn write_marker(
    ctx: &Context,
    control_stream: &str,
    action: &str,
    db: Option<i32>,
    retention: Retention,
) {
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
    // The control stream follows the same trim strategy as data streams so
    // replay-window reasoning is uniform (issue #108, SPEC.md section 9). Held
    // outside the args vec so their bytes outlive the `&[u8]` borrows below.
    let trim = retention.trim_clause(if retention.is_time_based() {
        server_now_ms()
    } else {
        0
    });
    let db_s = db.map(|d| d.to_string());
    let mut args: Vec<&[u8]> = Vec::with_capacity(12);
    args.push(control_stream.as_bytes());
    if let Some((keyword, ref threshold)) = trim {
        args.push(keyword);
        args.push(&b"~"[..]);
        args.push(threshold.as_bytes());
    }
    args.push(&b"*"[..]);
    args.push(&b"action"[..]);
    args.push(action.as_bytes());
    args.push(&b"module-version"[..]);
    args.push(env!("CARGO_PKG_VERSION").as_bytes());
    if let Some(ref s) = db_s {
        args.push(&b"db"[..]);
        args.push(s.as_bytes());
    }

    let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
    match res {
        Ok(_) => {
            CONTROL_MARKERS.fetch_add(1, Ordering::Relaxed);
            // The control stream participates in the per-stream failure log
            // (issue #68): if it was dropping markers, this success ends the
            // streak. Markers are never counted as forwarded, and the control
            // stream is not in the registry, so its record stays out of the
            // WITHSTATS join (SPEC.md section 8).
            if let Some(entry) = STREAM_STATS.lock(ctx).get_mut(control_stream) {
                if let Some(drops) = entry.record_success() {
                    ctx.log_notice(&format!(
                        "eventstream: {control_stream} recovered after {drops} drops"
                    ));
                }
            }
        }
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_default();
            if msg.starts_with("OOM") {
                count_stream_drop(
                    ctx,
                    control_stream,
                    &DROPPED_OOM,
                    &format!("gap marker '{action}' refused under maxmemory: {msg}"),
                );
            } else {
                count_stream_drop(
                    ctx,
                    control_stream,
                    &DROPPED_XADD_ERROR,
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
pub(crate) fn guard_job(body: impl FnOnce()) {
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
pub(crate) fn drain_pending_markers(ctx: &Context) {
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }
    let drained: Vec<PendingMarker> = std::mem::take(&mut *PENDING_MARKERS.lock(ctx));
    MARKERS_DIRTY.store(false, Ordering::Relaxed);
    if drained.is_empty() {
        return;
    }
    let prefix_owned = PREFIX.value.lock(ctx).as_str().to_owned();
    // The control stream's retention (issues #62, #108): its `#control` suffix is
    // addressable as a per-event override, else the global cap; time-based
    // retention applies to it as to any stream.
    let control_ret = Retention {
        maxlen: effective_maxlen(
            &MAXLEN_OVERRIDES.parsed.lock(ctx),
            "#control",
            MAXLEN.value.load(Ordering::Relaxed),
        ),
        retention_ms: RETENTION_MS.value.load(Ordering::Relaxed),
    };
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
            for marker in &drained {
                write_marker(
                    ctx,
                    &control_stream,
                    marker.action(),
                    marker.db(),
                    control_ret,
                );
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

/// Wrap a raw server-event callback body so a panic cannot unwind across the
/// FFI boundary (undefined behavior that would abort the server); a caught
/// panic is counted and logged once, sharing the handler-panic accounting
/// (SPEC.md section 5), exactly as the keyspace callback does.
#[cfg(not(test))]
pub(crate) fn guard_server_event(body: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_err() {
        HANDLER_PANICS.fetch_add(1, Ordering::Relaxed);
        if LOGGED_PANIC
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            redis_module::logging::log_warning(
                "eventstream: server-event handler panicked (caught)",
            );
        }
    }
}
lazy_static! {
    /// Gap markers recorded at lifecycle points and written by the next
    /// notification callback's post-notification job (SPEC.md section 9).
    /// A `Vec`, not a single slot, so overlapping lifecycle points (e.g. a
    /// `FLUSHALL` and a `SWAPDB` before any event drains them, or a `disabled`
    /// still queued when a flush arrives) accumulate rather than clobber each
    /// other (issue #73 pending-collision note).
    pub(crate) static ref PENDING_MARKERS: RedisGILGuard<Vec<PendingMarker>> =
        RedisGILGuard::new(Vec::new());
}
