#![no_main]

//! Fuzz the `eventstream.events` filter grammar (SPEC.md section 7). The parser
//! ingests untrusted `CONFIG SET` input, so it must never panic — only return
//! `Err`. libFuzzer drives valid UTF-8 (the config value is a `RedisString`
//! decoded to `&str`); the harness discards the result and watches for crashes.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        redis_event_stream_module::fuzz_targets::parse_filter(s);
    }
});
