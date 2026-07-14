//! Companion test module (issue #91): `NOTIFYTEST.FIRE <event-bytes> <key>`
//! fires `RM_NotifyKeyspaceEvent` with the event-name bytes passed verbatim.
//!
//! Command arguments are binary-safe, so a test client can deliver a
//! non-UTF-8 or empty event name into the main module's raw keyspace
//! callback — input shapes no built-in command produces, which is exactly
//! what the hand-written subscription exists to survive (SPEC.md section 5).
//! Built as a second cdylib via the `[[example]]` entry in Cargo.toml and
//! loaded alongside the main module by `tests/module_events.rs`.

use redis_module::{
    raw, redis_module, Context, NotifyEvent, RedisError, RedisResult, RedisString, RedisValue,
};
use std::ffi::CString;

fn fire(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    // The wrapper's safe `notify_keyspace_event` takes `&str`, which cannot
    // carry the invalid bytes this module exists to deliver; go through the
    // raw binding. A C string cannot contain NUL, everything else (including
    // invalid UTF-8 and the empty string) passes through untouched.
    let event = CString::new(args[1].as_slice())
        .map_err(|_| RedisError::Str("event name must not contain NUL"))?;
    let rc = unsafe {
        raw::RedisModule_NotifyKeyspaceEvent.unwrap()(
            ctx.ctx,
            NotifyEvent::GENERIC.bits(),
            event.as_ptr(),
            args[2].inner,
        )
    };
    if rc == raw::REDISMODULE_OK as i32 {
        Ok(RedisValue::SimpleStringStatic("OK"))
    } else {
        Err(RedisError::Str("RM_NotifyKeyspaceEvent failed"))
    }
}

redis_module! {
    name: "notifytest",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        // Writes no keys itself; `write` because it emits a keyspace event.
        ["notifytest.fire", fire, "write", 0, 0, 0, ""],
    ],
}
