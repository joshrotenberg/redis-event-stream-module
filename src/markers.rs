// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Gap markers and FFI panic guards (#86): the pending-marker queue and its
//! deferred write to the `#control` stream (SPEC.md section 9), plus the
//! panic-catching guards shared by the post-notification job and the raw
//! server-event callbacks.

use crate::capture::{server_now_ms, xadd_call_options, Retention};
use crate::cluster::{count_no_slot_drop, tag_segment};
use crate::config::{
    effective_maxlen, CONTROL_CHECKPOINT_MS, MAXLEN, MAXLEN_OVERRIDES, PREFIX, RETENTION_MS,
};
use crate::stats::{
    count_drop, count_stream_drop, dropped_total, ASYNC_DRAIN_EVENTS, ASYNC_ENQUEUED,
    ASYNC_QUEUE_DEPTH, ASYNC_WORKER_ERRORS, DROPPED_DEFER_ERROR, DROPPED_OOM, DROPPED_XADD_ERROR,
    EVENTS_LOST, FORWARDED, HANDLER_PANICS, LAST_ERROR_TIME, LOGGED_DEFER_ERROR, LOGGED_PANIC,
    LOGGED_XADD_ERROR, STREAM_STATS,
};
use lazy_static::lazy_static;
use redis_module::{
    raw, CallResult, Context, ContextFlags, RedisGILGuard, RedisLockIndicator, Status,
};
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

pub(crate) static CONTROL_MARKERS: AtomicU64 = AtomicU64::new(0);
pub(crate) static CONTROL_CHECKPOINTS: AtomicU64 = AtomicU64::new(0);

static LAST_CHECKPOINT_MS: AtomicI64 = AtomicI64::new(0);

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

/// Initialize the opaque identity shared by every control entry from this
/// module load. Redis supplies the random bytes, avoiding another dependency
/// and making collisions between nodes and restarts negligible (issue #277).
pub(crate) fn initialize_generation(ctx: &Context) {
    let mut bytes = [0_u8; 32];
    unsafe {
        raw::RedisModule_GetRandomHexChars.unwrap()(bytes.as_mut_ptr().cast(), bytes.len());
    }
    *GENERATION_ID.lock(ctx) = String::from_utf8(bytes.to_vec())
        .expect("RedisModule_GetRandomHexChars must return ASCII hex");
}

/// Clear lifecycle state retained by an in-process dynamic-library reload.
/// `initialize_generation` immediately assigns the new opaque identity after
/// dispatch startup succeeds (issue #291).
pub(crate) fn reset_marker_generation<G: RedisLockIndicator>(lock: &G) {
    CONTROL_MARKERS.store(0, Ordering::Relaxed);
    CONTROL_CHECKPOINTS.store(0, Ordering::Relaxed);
    LAST_CHECKPOINT_MS.store(0, Ordering::Relaxed);
    MARKERS_DIRTY.store(false, Ordering::Relaxed);
    GENERATION_ID.lock(lock).clear();
    PENDING_MARKERS.lock(lock).clear();
}

fn control_retention<G: redis_module::RedisLockIndicator>(lock: &G) -> Retention {
    Retention {
        maxlen: effective_maxlen(
            &MAXLEN_OVERRIDES.parsed.lock(lock),
            "#control",
            MAXLEN.value.load(Ordering::Relaxed),
        ),
        retention_ms: RETENTION_MS.value.load(Ordering::Relaxed),
    }
}

/// Shared low-volume `#control` writer for lifecycle markers and checkpoints.
/// Every record carries the additive generation field. `extra` owns its values
/// so the call argument borrows remain valid through `RM_Call`.
fn write_control_entry(
    ctx: &Context,
    control_stream: &str,
    action: &str,
    retention: Retention,
    extra: &[(&str, String)],
    success_counter: &AtomicU64,
    kind: &str,
) {
    let rc = unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) };
    if rc != raw::REDISMODULE_OK as i32 {
        count_drop(
            ctx,
            &DROPPED_XADD_ERROR,
            &LOGGED_XADD_ERROR,
            &format!("SelectDb(0) failed; {kind} dropped"),
        );
        return;
    }
    let trim = retention.trim_clause(if retention.is_time_based() {
        server_now_ms()
    } else {
        0
    });
    let generation = GENERATION_ID.lock(ctx).clone();
    let mut owned = Vec::<Vec<u8>>::with_capacity(11 + extra.len() * 2);
    owned.push(control_stream.as_bytes().to_vec());
    if let Some((keyword, threshold)) = trim {
        owned.push(keyword.to_vec());
        owned.push(b"~".to_vec());
        owned.push(threshold.into_bytes());
    }
    owned.push(b"*".to_vec());
    owned.push(b"action".to_vec());
    owned.push(action.as_bytes().to_vec());
    owned.push(b"module-version".to_vec());
    owned.push(env!("CARGO_PKG_VERSION").as_bytes().to_vec());
    owned.push(b"generation".to_vec());
    owned.push(generation.into_bytes());
    for (name, value) in extra {
        owned.push(name.as_bytes().to_vec());
        owned.push(value.as_bytes().to_vec());
    }
    let args = owned.iter().map(Vec::as_slice).collect::<Vec<_>>();

    let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
    match res {
        Ok(_) => {
            success_counter.fetch_add(1, Ordering::Relaxed);
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
                    &format!("{kind} '{action}' refused under maxmemory: {msg}"),
                );
            } else {
                count_stream_drop(
                    ctx,
                    control_stream,
                    &DROPPED_XADD_ERROR,
                    &format!("{kind} '{action}' failed: {msg}"),
                );
            }
        }
    }
}

/// Write one lifecycle marker in a write-safe context. `db` is present only
/// on `flushed`; `generation` is present on every action (issue #277).
pub(crate) fn write_marker(
    ctx: &Context,
    control_stream: &str,
    action: &str,
    db: Option<i32>,
    retention: Retention,
) {
    let extra = db
        .map(|value| vec![("db", value.to_string())])
        .unwrap_or_default();
    write_control_entry(
        ctx,
        control_stream,
        action,
        retention,
        &extra,
        &CONTROL_MARKERS,
        "gap marker",
    );
}

/// Persist a generation-local status snapshot. `resolved-events` is
/// `forwarded + events-lost`: events accepted by an async worker but not yet
/// settled are intentionally excluded and represented by the queue telemetry.
fn write_checkpoint(ctx: &Context, control_stream: &str, retention: Retention) {
    let forwarded = FORWARDED.load(Ordering::Relaxed);
    let lost = EVENTS_LOST.load(Ordering::Relaxed);
    let extra = vec![
        ("reason", "periodic".to_owned()),
        (
            "checkpoint-ms",
            CONTROL_CHECKPOINT_MS
                .value
                .load(Ordering::Relaxed)
                .to_string(),
        ),
        (
            "resolved-events",
            forwarded.saturating_add(lost).to_string(),
        ),
        ("forwarded", forwarded.to_string()),
        ("events-lost", lost.to_string()),
        ("dropped", dropped_total().to_string()),
        (
            "async-enqueued",
            ASYNC_ENQUEUED.load(Ordering::Relaxed).to_string(),
        ),
        (
            "async-queue-depth",
            ASYNC_QUEUE_DEPTH.load(Ordering::Relaxed).to_string(),
        ),
        (
            "async-drain-events",
            ASYNC_DRAIN_EVENTS.load(Ordering::Relaxed).to_string(),
        ),
        (
            "async-worker-errors",
            ASYNC_WORKER_ERRORS.load(Ordering::Relaxed).to_string(),
        ),
        (
            "handler-panics",
            HANDLER_PANICS.load(Ordering::Relaxed).to_string(),
        ),
        (
            "last-error-time",
            LAST_ERROR_TIME.load(Ordering::Relaxed).to_string(),
        ),
    ];
    write_control_entry(
        ctx,
        control_stream,
        "checkpoint",
        retention,
        &extra,
        &CONTROL_CHECKPOINTS,
        "control checkpoint",
    );
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

fn guard_checkpoint_cron(body: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_err() {
        HANDLER_PANICS.fetch_add(1, Ordering::Relaxed);
        if LOGGED_PANIC
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            redis_module::logging::log_warning(
                "eventstream: control-checkpoint cron callback panicked (caught)",
            );
        }
    }
}

/// One due checkpoint. Cron callbacks run on Redis's main thread with a normal
/// module context, so direct writes are safe once LOADING has cleared. Pending
/// lifecycle markers are emitted first; this lets an idle checkpoint-enabled
/// deployment persist its `loaded` boundary without waiting for a key event.
fn checkpoint_tick(ctx: &Context) {
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }
    let Some(seg) = tag_segment(ctx) else {
        // A cluster node can load before slots are assigned. Preserve pending
        // markers and retry on the next tick; no source event was lost here.
        return;
    };
    let control_stream = format!("{}{seg}#control", PREFIX.value.lock(ctx).as_str());
    let retention = control_retention(ctx);
    let drained: Vec<PendingMarker> = std::mem::take(&mut *PENDING_MARKERS.lock(ctx));
    MARKERS_DIRTY.store(false, Ordering::Relaxed);
    for marker in drained {
        write_marker(
            ctx,
            &control_stream,
            marker.action(),
            marker.db(),
            retention,
        );
    }
    write_checkpoint(ctx, &control_stream, retention);
}

/// Initialize the elapsed-time gate after module load. The Redis cron-loop
/// server event is used instead of `RedisModule_CreateTimer`: Redis refuses
/// MODULE UNLOAD before deinit while any module timer is pending, so a
/// self-rescheduling timer would revoke the module's unload capability.
pub(crate) fn start_checkpoint_schedule() {
    let millis = CONTROL_CHECKPOINT_MS.value.load(Ordering::Relaxed);
    if millis == 0 {
        return;
    }
    LAST_CHECKPOINT_MS.store(server_now_ms(), Ordering::Relaxed);
}

/// Raw `REDISMODULE_EVENT_CRON_LOOP` callback. Redis supplies an approximate
/// hz in `data`, but wall-clock gating is both simpler and robust to runtime hz
/// changes. The event subscription itself exists only when checkpoints are
/// enabled at load, leaving the default path with no cron callback.
pub(crate) extern "C" fn raw_checkpoint_cron_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    _subevent: u64,
    _data: *mut c_void,
) {
    guard_checkpoint_cron(|| {
        let millis = CONTROL_CHECKPOINT_MS.value.load(Ordering::Relaxed);
        if millis == 0 {
            return;
        }
        let now = server_now_ms();
        let last = LAST_CHECKPOINT_MS.load(Ordering::Relaxed);
        if now.saturating_sub(last) < millis {
            return;
        }
        LAST_CHECKPOINT_MS.store(now, Ordering::Relaxed);
        checkpoint_tick(&Context::new(ctx));
    });
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
                    // Gap markers are control-plane writes, not selected
                    // events, so a no-slot marker loss counts in
                    // dropped_no_owned_slot but never in events_lost (issue
                    // #218: markers are not lost source events).
                    for _ in &drained {
                        count_no_slot_drop(ctx, false);
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
    /// Opaque 128-bit (32 hex character) identity for one loaded module
    /// generation. It is additive on every control-stream record (#277).
    pub(crate) static ref GENERATION_ID: RedisGILGuard<String> =
        RedisGILGuard::new(String::new());

    /// Gap markers recorded at lifecycle points and written by the next
    /// notification callback's post-notification job (SPEC.md section 9).
    /// A `Vec`, not a single slot, so overlapping lifecycle points (e.g. a
    /// `FLUSHALL` and a `SWAPDB` before any event drains them, or a `disabled`
    /// still queued when a flush arrives) accumulate rather than clobber each
    /// other (issue #73 pending-collision note).
    pub(crate) static ref PENDING_MARKERS: RedisGILGuard<Vec<PendingMarker>> =
        RedisGILGuard::new(Vec::new());
}
