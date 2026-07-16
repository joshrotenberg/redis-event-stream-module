#![no_main]

//! Fuzz the event-name sanitizer (SPEC.md section 5), which maps a
//! possibly-hostile co-loaded-module event name to a stream-key-safe suffix.
//! It must always produce safe output and never panic; the property tests in
//! #94 assert the output invariants, this explores the input space continuously.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        redis_event_stream_module::fuzz_targets::sanitize(s);
    }
});
