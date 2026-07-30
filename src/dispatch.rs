//! Experimental bounded capture dispatch (issue #265).
//!
//! The default `sync` mode still registers the historical post-notification
//! job. `individual` moves accepted events through a bounded FIFO to one
//! worker, drains several events under one thread-safe Redis lock, and retains
//! one XADD per logical event.
//!
//! The worker deliberately does not remove events from the FIFO until it owns
//! Redis's lock. If the FIFO is full (or briefly contended), the main-thread
//! fallback job can therefore drain every earlier accepted event before it
//! writes the current one. That makes the nonblocking fallback lossless
//! without reordering a destination stream.

use crate::capture::{
    count_defer_failure, defer_pending_event, encode_batch_fields, process_pending_envelope,
    process_pending_event, PendingEvent,
};
use crate::config::{
    EntryFormat, WriteMode, ASYNC_BATCH_SIZE, ASYNC_MAX_WAIT_MS, ASYNC_QUEUE_CAPACITY,
    ENTRY_FORMAT, ENTRY_SEQ, FIREHOSE, MAXLEN_OVERRIDES, RETENTION_MS, WRITE_MODE,
};
use crate::markers::guard_job;
use crate::stats::{
    ASYNC_DRAINS, ASYNC_DRAIN_EVENTS, ASYNC_ENQUEUED, ASYNC_FALLBACKS, ASYNC_QUEUE_DEPTH,
    ASYNC_QUEUE_HIGH_WATER, ASYNC_WORKER_ERRORS, EVENTS_LOST,
};
use redis_module::{raw, Context, RedisLockIndicator, Status};
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, TryLockError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

static EVENT_QUEUE: OnceLock<Arc<EventQueue>> = OnceLock::new();
static WORKER: Mutex<Option<JoinHandle<Vec<Arc<PendingEvent>>>>> = Mutex::new(None);
static STOP_WORKER: AtomicBool = AtomicBool::new(false);
static ACTIVE_MODE: AtomicU8 = AtomicU8::new(WriteMode::sync as u8);
static NEXT_QUEUE_ID: AtomicU64 = AtomicU64::new(0);
const QUEUE_LOCK_SPINS: usize = 64;

struct QueuedEvent {
    id: u64,
    event: Arc<PendingEvent>,
    enqueued_at: Instant,
}

struct PreparedGroup {
    len: usize,
    fields: Vec<Vec<u8>>,
}

struct EnvelopeSnapshot {
    first_id: u64,
    last_id: u64,
    count: usize,
    events: Vec<Arc<PendingEvent>>,
    groups: Vec<PreparedGroup>,
}

struct EventQueue {
    capacity: usize,
    events: Mutex<VecDeque<QueuedEvent>>,
    ready: Condvar,
}

impl EventQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            ready: Condvar::new(),
        }
    }

    /// Spend only a small, fixed spin budget in the notification callback.
    /// Contention beyond that bound is treated exactly like a full queue and
    /// goes through the ordered fallback.
    fn try_push(&self, event: Arc<PendingEvent>) -> Result<usize, Arc<PendingEvent>> {
        for _ in 0..QUEUE_LOCK_SPINS {
            match self.events.try_lock() {
                Ok(mut events) => {
                    if events.len() >= self.capacity {
                        return Err(event);
                    }
                    events.push_back(QueuedEvent {
                        id: NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed),
                        event,
                        enqueued_at: Instant::now(),
                    });
                    let depth = events.len();
                    drop(events);
                    self.ready.notify_one();
                    return Ok(depth);
                }
                Err(TryLockError::Poisoned(_)) => return Err(event),
                Err(TryLockError::WouldBlock) => spin_loop(),
            }
        }
        Err(event)
    }

    /// Wait until a full batch is available or the oldest queued event reaches
    /// its latency bound. Stop is checked after every notification.
    fn wait_until_ready(&self, batch_size: usize, max_wait: Duration) -> bool {
        let mut events = self.events.lock().unwrap();
        loop {
            if STOP_WORKER.load(Ordering::Acquire) {
                return false;
            }
            if events.len() >= batch_size {
                return true;
            }
            let Some(oldest) = events.front() else {
                events = self.ready.wait(events).unwrap();
                continue;
            };
            let remaining = max_wait.saturating_sub(oldest.enqueued_at.elapsed());
            if remaining.is_zero() {
                return true;
            }
            let (next, timeout) = self.ready.wait_timeout(events, remaining).unwrap();
            events = next;
            if timeout.timed_out() && !events.is_empty() {
                return true;
            }
        }
    }

    fn drain(&self, limit: usize) -> Vec<Arc<PendingEvent>> {
        let mut events = self.events.lock().unwrap();
        let count = events.len().min(limit);
        let drained = events
            .drain(..count)
            .map(|queued| queued.event)
            .collect::<Vec<_>>();
        ASYNC_QUEUE_DEPTH.fetch_sub(count as i64, Ordering::Relaxed);
        drained
    }

    fn drain_all(&self) -> Vec<Arc<PendingEvent>> {
        self.drain(usize::MAX)
    }

    /// Encode the current prefix while leaving it in the FIFO. A fallback can
    /// still drain that prefix before the worker owns Redis's lock; IDs let the
    /// worker detect that race and discard the now-stale encoding.
    fn prepare_envelopes(&self, limit: usize) -> Option<EnvelopeSnapshot> {
        let (count, first_id, last_id, selected) = {
            let events = self.events.lock().unwrap();
            let count = events.len().min(limit);
            if count == 0 {
                return None;
            }
            let first_id = events.front().unwrap().id;
            let last_id = events.get(count - 1).unwrap().id;
            let selected = events
                .iter()
                .take(count)
                .map(|queued| Arc::clone(&queued.event))
                .collect::<Vec<_>>();
            (count, first_id, last_id, selected)
        };

        // The queue mutex is intentionally released before JSON/base64 work.
        let mut groups = Vec::new();
        let mut start = 0;
        while start < selected.len() {
            let mut end = start + 1;
            while end < selected.len()
                && envelope_compatible(selected[start].as_ref(), selected[end].as_ref())
            {
                end += 1;
            }
            let refs = selected[start..end]
                .iter()
                .map(Arc::as_ref)
                .collect::<Vec<_>>();
            groups.push(PreparedGroup {
                len: refs.len(),
                fields: encode_batch_fields(&refs),
            });
            start = end;
        }
        Some(EnvelopeSnapshot {
            first_id,
            last_id,
            count,
            events: selected,
            groups,
        })
    }

    fn drain_snapshot(&self, snapshot: &EnvelopeSnapshot) -> bool {
        let mut events = self.events.lock().unwrap();
        let matches = events
            .front()
            .is_some_and(|event| event.id == snapshot.first_id)
            && events
                .get(snapshot.count.saturating_sub(1))
                .is_some_and(|event| event.id == snapshot.last_id);
        if !matches {
            return false;
        }
        events.drain(..snapshot.count);
        ASYNC_QUEUE_DEPTH.fetch_sub(snapshot.count as i64, Ordering::Relaxed);
        true
    }

    fn wake(&self) {
        self.ready.notify_all();
    }
}

/// One detached Redis module context owned by the worker thread.
struct RawThreadContext(*mut raw::RedisModuleCtx);

impl RawThreadContext {
    fn new() -> Result<Self, &'static str> {
        let Some(get_context) = (unsafe { raw::RedisModule_GetThreadSafeContext }) else {
            return Err("RedisModule_GetThreadSafeContext is unavailable");
        };
        let ctx = unsafe { get_context(ptr::null_mut()) };
        if ctx.is_null() {
            Err("RedisModule_GetThreadSafeContext returned null")
        } else {
            Ok(Self(ctx))
        }
    }

    /// Try rather than block: during MODULE UNLOAD the main thread holds the
    /// Redis lock while asking this worker to stop. A blocking lock here would
    /// deadlock unload before it could join the thread.
    fn try_lock(&self) -> Option<RawContextGuard> {
        let try_lock = unsafe { raw::RedisModule_ThreadSafeContextTryLock? };
        let status = unsafe { try_lock(self.0) };
        if status == raw::REDISMODULE_OK as i32 {
            Some(RawContextGuard(Context::new(self.0)))
        } else {
            None
        }
    }
}

impl Drop for RawThreadContext {
    fn drop(&mut self) {
        if let Some(free) = unsafe { raw::RedisModule_FreeThreadSafeContext } {
            unsafe { free(self.0) };
        }
    }
}

struct RawContextGuard(Context);

unsafe impl RedisLockIndicator for RawContextGuard {}

impl Deref for RawContextGuard {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for RawContextGuard {
    fn drop(&mut self) {
        if let Some(unlock) = unsafe { raw::RedisModule_ThreadSafeContextUnlock } {
            unsafe { unlock(self.0.ctx) };
        }
    }
}

/// Validate the deliberately narrow first-spike surface. Unsupported feature
/// combinations fail module load rather than silently changing their wire or
/// lifecycle semantics.
pub(crate) fn start_dispatch_worker(ctx: &Context) -> Result<(), String> {
    let mode = *WRITE_MODE.lock(ctx);
    if mode == WriteMode::sync {
        ACTIVE_MODE.store(WriteMode::sync as u8, Ordering::Release);
        return Ok(());
    }
    if ctx
        .get_flags()
        .contains(redis_module::ContextFlags::CLUSTER)
    {
        return Err("experimental async write modes do not support cluster mode".to_owned());
    }
    if FIREHOSE.load(Ordering::Relaxed) {
        return Err("experimental async write modes require firehose no".to_owned());
    }
    if ENTRY_SEQ.load(Ordering::Relaxed) {
        return Err("experimental async write modes require entry-seq no".to_owned());
    }
    if *ENTRY_FORMAT.lock(ctx) != EntryFormat::fixed {
        return Err("experimental async write modes require entry-format fixed".to_owned());
    }
    if RETENTION_MS.value.load(Ordering::Relaxed) != 0 {
        return Err("experimental async write modes require retention-ms 0".to_owned());
    }
    if !MAXLEN_OVERRIDES.parsed.lock(ctx).is_empty() {
        return Err("experimental async write modes require empty maxlen-overrides".to_owned());
    }
    if unsafe { raw::RedisModule_ThreadSafeContextTryLock }.is_none() {
        return Err("RedisModule_ThreadSafeContextTryLock is unavailable".to_owned());
    }

    let capacity = ASYNC_QUEUE_CAPACITY.value.load(Ordering::Relaxed) as usize;
    let batch_size = ASYNC_BATCH_SIZE.value.load(Ordering::Relaxed) as usize;
    let max_wait = Duration::from_millis(ASYNC_MAX_WAIT_MS.value.load(Ordering::Relaxed) as u64);
    let queue = Arc::new(EventQueue::new(capacity));

    STOP_WORKER.store(false, Ordering::Release);
    let worker_queue = Arc::clone(&queue);
    let handle = thread::Builder::new()
        .name("eventstream-writer".to_owned())
        .spawn(move || worker_main(worker_queue, batch_size, max_wait, mode))
        .map_err(|e| format!("failed to spawn async writer: {e}"))?;
    EVENT_QUEUE
        .set(queue)
        .map_err(|_| "async writer queue was already initialized".to_owned())?;
    *WORKER.lock().unwrap() = Some(handle);
    ACTIVE_MODE.store(mode as u8, Ordering::Release);
    Ok(())
}

/// Route one event through the selected dispatch.
pub(crate) fn dispatch_pending_event(ctx: &Context, event: PendingEvent) {
    if ACTIVE_MODE.load(Ordering::Acquire) == WriteMode::sync as u8 {
        defer_pending_event(ctx, event);
        return;
    }

    let Some(queue) = EVENT_QUEUE.get() else {
        ASYNC_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        defer_pending_event(ctx, event);
        return;
    };
    match queue.try_push(Arc::new(event)) {
        Ok(depth) => {
            ASYNC_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
            ASYNC_ENQUEUED.fetch_add(1, Ordering::Relaxed);
            ASYNC_QUEUE_HIGH_WATER.fetch_max(depth as u64, Ordering::Relaxed);
        }
        Err(event) => {
            ASYNC_FALLBACKS.fetch_add(1, Ordering::Relaxed);
            defer_ordered_fallback(ctx, Arc::clone(queue), event);
        }
    }
}

/// Redis runs this job before it releases the main-thread lock. The worker
/// cannot own that lock concurrently, so draining the still-queued prefix and
/// then the current event preserves notification order.
fn defer_ordered_fallback(ctx: &Context, queue: Arc<EventQueue>, event: Arc<PendingEvent>) {
    let status = ctx.add_post_notification_job(move |ctx| {
        guard_job(move || {
            let accepted = queue.drain_all();
            if !accepted.is_empty() {
                ASYNC_DRAINS.fetch_add(1, Ordering::Relaxed);
                ASYNC_DRAIN_EVENTS.fetch_add(accepted.len() as u64, Ordering::Relaxed);
            }
            for pending in accepted {
                process_pending_event(ctx, pending.as_ref());
            }
            process_pending_event(ctx, event.as_ref());
        });
    });
    if !matches!(status, Status::Ok) {
        count_defer_failure(ctx);
    }
}

fn worker_main(
    queue: Arc<EventQueue>,
    batch_size: usize,
    max_wait: Duration,
    mode: WriteMode,
) -> Vec<Arc<PendingEvent>> {
    let thread_ctx = match RawThreadContext::new() {
        Ok(ctx) => ctx,
        Err(_) => {
            ASYNC_WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            return queue.drain_all();
        }
    };

    loop {
        if !queue.wait_until_ready(batch_size, max_wait) {
            return queue.drain_all();
        }

        let envelope = if mode == WriteMode::envelope {
            queue.prepare_envelopes(batch_size)
        } else {
            None
        };
        loop {
            if STOP_WORKER.load(Ordering::Acquire) {
                return queue.drain_all();
            }
            if let Some(ctx) = thread_ctx.try_lock() {
                if let Some(snapshot) = envelope {
                    if queue.drain_snapshot(&snapshot) {
                        ASYNC_DRAINS.fetch_add(1, Ordering::Relaxed);
                        ASYNC_DRAIN_EVENTS.fetch_add(snapshot.count as u64, Ordering::Relaxed);
                        let mut start = 0;
                        for group in snapshot.groups {
                            let end = start + group.len;
                            let compatible = snapshot.events[start..end]
                                .iter()
                                .map(Arc::as_ref)
                                .collect::<Vec<_>>();
                            guard_job(|| process_pending_envelope(&ctx, &compatible, group.fields));
                            start = end;
                        }
                    }
                } else {
                    let batch = queue.drain(batch_size);
                    if !batch.is_empty() {
                        ASYNC_DRAINS.fetch_add(1, Ordering::Relaxed);
                        ASYNC_DRAIN_EVENTS.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        for event in batch {
                            guard_job(|| process_pending_event(&ctx, event.as_ref()));
                        }
                    }
                }
                break;
            }
            thread::yield_now();
        }
    }
}

fn envelope_compatible(left: &PendingEvent, right: &PendingEvent) -> bool {
    left.prefix == right.prefix
        && left.suffix == right.suffix
        && left.data_retention == right.data_retention
        && left.max_streams == right.max_streams
}

/// Stop without deadlocking on the Redis lock, then finish every event the
/// channel had accepted through the canonical synchronous processor.
pub(crate) fn stop_dispatch_worker(ctx: &Context) {
    if ACTIVE_MODE.load(Ordering::Acquire) == WriteMode::sync as u8 {
        return;
    }

    STOP_WORKER.store(true, Ordering::Release);
    ACTIVE_MODE.store(WriteMode::sync as u8, Ordering::Release);
    if let Some(queue) = EVENT_QUEUE.get() {
        queue.wake();
    }
    let Some(handle) = WORKER.lock().unwrap().take() else {
        return;
    };
    match handle.join() {
        Ok(pending) => {
            ASYNC_QUEUE_DEPTH.store(0, Ordering::Relaxed);
            for event in pending {
                guard_job(|| process_pending_event(ctx, event.as_ref()));
            }
        }
        Err(_) => {
            ASYNC_WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            let unaccounted = ASYNC_QUEUE_DEPTH.swap(0, Ordering::Relaxed).max(0) as u64;
            EVENTS_LOST.fetch_add(unaccounted, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_defaults_are_bounded() {
        assert_eq!(ASYNC_QUEUE_CAPACITY.value.load(Ordering::Relaxed), 65_536);
        assert_eq!(ASYNC_BATCH_SIZE.value.load(Ordering::Relaxed), 64);
        assert_eq!(ASYNC_MAX_WAIT_MS.value.load(Ordering::Relaxed), 1);
    }
}
