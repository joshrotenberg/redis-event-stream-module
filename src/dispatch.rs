//! Experimental bounded capture dispatch (issue #265).
//!
//! The default `sync` mode still registers the historical post-notification
//! job. `individual` moves accepted events through a bounded channel to one
//! worker, drains several events under one thread-safe Redis lock, and retains
//! one XADD per logical event. Queue-full/disconnected events fall back to the
//! historical job, so enqueue pressure is observable but not event loss.

use crate::capture::{defer_pending_event, process_pending_event, PendingEvent};
use crate::config::{
    EntryFormat, WriteMode, ASYNC_BATCH_SIZE, ASYNC_MAX_WAIT_MS, ASYNC_QUEUE_CAPACITY,
    ENTRY_FORMAT, ENTRY_SEQ, FIREHOSE, MAXLEN_OVERRIDES, RETENTION_MS, WRITE_MODE,
};
use crate::markers::guard_job;
use crate::stats::{
    ASYNC_DRAINS, ASYNC_DRAIN_EVENTS, ASYNC_ENQUEUED, ASYNC_FALLBACKS, ASYNC_QUEUE_DEPTH,
    ASYNC_QUEUE_HIGH_WATER, ASYNC_WORKER_ERRORS, EVENTS_LOST,
};
use redis_module::{raw, Context, RedisLockIndicator};
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

static EVENT_SENDER: OnceLock<SyncSender<PendingEvent>> = OnceLock::new();
static WORKER: Mutex<Option<JoinHandle<Vec<PendingEvent>>>> = Mutex::new(None);
static STOP_WORKER: AtomicBool = AtomicBool::new(false);
static ACTIVE_MODE: AtomicU8 = AtomicU8::new(WriteMode::sync as u8);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(1);

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
    if mode == WriteMode::envelope {
        return Err("write-mode envelope is not implemented yet".to_owned());
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
    let (sender, receiver) = mpsc::sync_channel(capacity);

    STOP_WORKER.store(false, Ordering::Release);
    let handle = thread::Builder::new()
        .name("eventstream-writer".to_owned())
        .spawn(move || worker_main(receiver, batch_size, max_wait))
        .map_err(|e| format!("failed to spawn async writer: {e}"))?;
    EVENT_SENDER
        .set(sender)
        .map_err(|_| "async writer sender was already initialized".to_owned())?;
    *WORKER.lock().unwrap() = Some(handle);
    ACTIVE_MODE.store(mode as u8, Ordering::Release);
    Ok(())
}

/// Route one event through the selected dispatch. Increment depth before send
/// so a very fast receiver cannot decrement the gauge before the producer's
/// increment becomes visible.
pub(crate) fn dispatch_pending_event(ctx: &Context, event: PendingEvent) {
    if ACTIVE_MODE.load(Ordering::Acquire) == WriteMode::sync as u8 {
        defer_pending_event(ctx, event);
        return;
    }

    ASYNC_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
    let result = match EVENT_SENDER.get() {
        Some(sender) => match sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(event) | TrySendError::Disconnected(event)) => Err(event),
        },
        None => Err(event),
    };
    match result {
        Ok(()) => {
            ASYNC_ENQUEUED.fetch_add(1, Ordering::Relaxed);
            let depth = ASYNC_QUEUE_DEPTH.load(Ordering::Relaxed).max(0) as u64;
            ASYNC_QUEUE_HIGH_WATER.fetch_max(depth, Ordering::Relaxed);
        }
        Err(event) => {
            ASYNC_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
            ASYNC_FALLBACKS.fetch_add(1, Ordering::Relaxed);
            defer_pending_event(ctx, event);
        }
    }
}

fn worker_main(
    receiver: Receiver<PendingEvent>,
    batch_size: usize,
    max_wait: Duration,
) -> Vec<PendingEvent> {
    let thread_ctx = match RawThreadContext::new() {
        Ok(ctx) => ctx,
        Err(_) => {
            ASYNC_WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            return collect_pending(&receiver, Vec::new());
        }
    };

    loop {
        if STOP_WORKER.load(Ordering::Acquire) {
            return collect_pending(&receiver, Vec::new());
        }

        let first = match receiver.recv_timeout(max_wait.min(STOP_POLL_INTERVAL)) {
            Ok(event) => {
                ASYNC_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
                event
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return Vec::new(),
        };
        let mut batch = Vec::with_capacity(batch_size);
        batch.push(first);
        let deadline = Instant::now() + max_wait;

        while batch.len() < batch_size {
            if STOP_WORKER.load(Ordering::Acquire) {
                return collect_pending(&receiver, batch);
            }
            match receiver.try_recv() {
                Ok(event) => {
                    ASYNC_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
                    batch.push(event);
                }
                Err(TryRecvError::Empty) => {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    match receiver.recv_timeout(remaining.min(STOP_POLL_INTERVAL)) {
                        Ok(event) => {
                            ASYNC_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
                            batch.push(event);
                        }
                        Err(RecvTimeoutError::Timeout) if Instant::now() < deadline => continue,
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        loop {
            if STOP_WORKER.load(Ordering::Acquire) {
                return collect_pending(&receiver, batch);
            }
            if let Some(ctx) = thread_ctx.try_lock() {
                ASYNC_DRAINS.fetch_add(1, Ordering::Relaxed);
                ASYNC_DRAIN_EVENTS.fetch_add(batch.len() as u64, Ordering::Relaxed);
                for event in batch {
                    guard_job(|| process_pending_event(&ctx, event));
                }
                break;
            }
            thread::yield_now();
        }
    }
}

fn collect_pending(
    receiver: &Receiver<PendingEvent>,
    mut pending: Vec<PendingEvent>,
) -> Vec<PendingEvent> {
    while let Ok(event) = receiver.try_recv() {
        ASYNC_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
        pending.push(event);
    }
    pending
}

/// Stop without deadlocking on the Redis lock, then finish every event the
/// channel had accepted through the canonical synchronous processor.
pub(crate) fn stop_dispatch_worker(ctx: &Context) {
    if ACTIVE_MODE.load(Ordering::Acquire) == WriteMode::sync as u8 {
        return;
    }

    STOP_WORKER.store(true, Ordering::Release);
    ACTIVE_MODE.store(WriteMode::sync as u8, Ordering::Release);
    let Some(handle) = WORKER.lock().unwrap().take() else {
        return;
    };
    match handle.join() {
        Ok(pending) => {
            ASYNC_QUEUE_DEPTH.store(0, Ordering::Relaxed);
            for event in pending {
                guard_job(|| process_pending_event(ctx, event));
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
