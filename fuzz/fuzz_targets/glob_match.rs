#![no_main]

//! Fuzz the key-filter glob matcher (SPEC.md section 7), a hand-written
//! recursive port of Redis stringmatchlen evaluated against raw key bytes on
//! the notification hot path whenever `eventstream.key-filter` is non-`*`.
//! It must never panic and never overflow the stack, whatever the pattern
//! (the recursion guard and the `skip_longer` early-out are exactly the
//! paths example-based tests do not reach); the first input byte selects a
//! split point so the fuzzer controls the pattern and the key independently.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let split = (sel as usize) % (rest.len() + 1);
    let (pattern, key) = rest.split_at(split);
    redis_event_stream_module::fuzz_targets::glob_match(pattern, key);
});
