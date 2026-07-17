//! Non-UTF-8 and empty module event names, end to end (issue #91). The
//! companion notifytest module (examples/notifytest.rs) fires
//! `RM_NotifyKeyspaceEvent` with arbitrary bytes — input no built-in command
//! produces, and the reason the hand-written raw callback exists: the
//! wrapper's macro-generated callback panics a non-UTF-8 name across the FFI
//! boundary and aborts the server (redismodule-rs#472, SPEC.md section 5).

mod common;

use common::*;

#[test]
fn non_utf8_event_name_is_captured_not_a_crash() {
    let s = TestServer::start_with_notifytest(&["events", "*"]);
    let mut c = s.conn();

    // One invalid byte: `from_utf8_lossy` yields one U+FFFD, which `sanitize`
    // maps to one `_`, so the destination is `events:js_on.set`.
    let _: () = redis::cmd("NOTIFYTEST.FIRE")
        .arg(&b"js\xffon.set"[..])
        .arg("victim")
        .query(&mut c)
        .expect("FIRE with a non-UTF-8 event name");
    wait_until(CAPTURE_WAIT, "non-UTF-8 event mirrored", || {
        xlen(&mut c, "events:js_on.set") > 0
    });

    // The entry's `event` field carries the lossy-decoded raw name: U+FFFD
    // (UTF-8 bytes EF BF BD) where the invalid byte was (SPEC.md section 6,
    // the field that disambiguates sanitizer collisions).
    assert_eq!(
        stream_field_values(&mut c, "events:js_on.set", "event"),
        vec!["js\u{FFFD}on.set".as_bytes().to_vec()]
    );
    assert_eq!(
        stream_field_strings(&mut c, "events:js_on.set", "key"),
        vec!["victim"]
    );
    // Captured, not a crash and not a skip: the lossy decode produces a
    // non-empty sanitized suffix, so `skipped_invalid` must not move.
    assert_eq!(info_field(&mut c, "handler_panics"), 0);
    assert_eq!(info_field(&mut c, "skipped_invalid"), 0);
}

#[test]
fn empty_event_name_increments_skipped_invalid() {
    // The filter gate runs before sanitization, so this needs `*`: under the
    // default filter an empty name would be counted as filtered instead.
    let s = TestServer::start_with_notifytest(&["events", "*"]);
    let mut c = s.conn();

    let _: () = redis::cmd("NOTIFYTEST.FIRE")
        .arg("")
        .arg("victim")
        .query(&mut c)
        .expect("FIRE with an empty event name");
    wait_until(CAPTURE_WAIT, "empty name counted invalid", || {
        info_field(&mut c, "skipped_invalid") == 1
    });

    // Not routable: no destination stream may register.
    let streams: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut c)
        .expect("STREAMS");
    assert!(
        streams.is_empty(),
        "an empty event name must register no stream, got {streams:?}"
    );
    assert_eq!(info_field(&mut c, "handler_panics"), 0);
}
