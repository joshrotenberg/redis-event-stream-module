#![no_main]

//! Fuzz the `eventstream.stream-prefix` validator (SPEC.md section 7). Untrusted
//! load-time / `CONFIG SET` input; must return `Err`, never panic.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        redis_event_stream_module::fuzz_targets::validate_prefix(s);
    }
});
