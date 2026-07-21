//! Capture-time filter knobs: the key-name glob filter (issue #61), the
//! source-db filter (issue #63), and the max-streams cap (issue #64).

mod common;

use common::*;
use redis::Commands;

// --- Key-name glob filter (issue #61) ---

#[test]
fn key_filter_captures_only_matching_keys() {
    let s = TestServer::start(&["events", "set", "key-filter", "session:*"]);
    let mut c = s.conn();

    let _: () = c.set("session:abc", "v").expect("SET matching");
    wait_until(CAPTURE_WAIT, "matching key mirrored", || {
        xlen(&mut c, "events:set") == 1
    });

    let _: () = c.set("cache:xyz", "v").expect("SET non-matching");
    wait_until(CAPTURE_WAIT, "non-matching key skipped", || {
        info_field(&mut c, "skipped_key_filtered") >= 1
    });
    // The non-matching write never produced an entry; only the matching one did.
    assert_eq!(xlen(&mut c, "events:set"), 1, "only session:* captured");
    let keys = stream_field_strings(&mut c, "events:set", "key");
    assert_eq!(keys, vec!["session:abc"]);
}

#[test]
fn key_filter_matches_binary_key_bytes() {
    // The pattern is matched against raw key bytes, so a `?` matches one
    // arbitrary (non-UTF-8) byte.
    let s = TestServer::start(&["events", "set", "key-filter", "k:?"]);
    let mut c = s.conn();

    let key: Vec<u8> = vec![b'k', b':', 0xff];
    let _: () = c.set(key.clone(), "v").expect("SET binary key");
    wait_until(CAPTURE_WAIT, "binary key matched", || {
        xlen(&mut c, "events:set") == 1
    });
    let keys = stream_field_values(&mut c, "events:set", "key");
    assert_eq!(keys, vec![key]);
}

#[test]
fn key_filter_live_change() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.key-filter")
        .arg("keep:*")
        .query(&mut c)
        .expect("CONFIG SET key-filter");

    let _: () = c.set("drop:1", "v").expect("SET dropped");
    wait_until(CAPTURE_WAIT, "dropped key counted", || {
        info_field(&mut c, "skipped_key_filtered") >= 1
    });
    let _: () = c.set("keep:1", "v").expect("SET kept");
    wait_until(CAPTURE_WAIT, "kept key mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
}

#[test]
fn key_filter_rejects_empty() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    for bad in ["", "session:*,", "a,,b"] {
        let res: Result<(), _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.key-filter")
            .arg(bad)
            .query(&mut c);
        assert!(res.is_err(), "CONFIG SET key-filter must reject {bad:?}");
    }
    // Unchanged after rejected sets.
    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.key-filter")
        .query(&mut c)
        .expect("CONFIG GET");
    assert_eq!(pair[1], "*");
}

// --- Source-db filter (issue #63) ---

#[test]
fn source_db_captures_only_named_db() {
    let s = TestServer::start(&["events", "set", "source-dbs", "2"]);
    let mut c0 = s.conn();
    let mut c2 = s.conn_db(2);

    let _: () = c0.set("in0", "v").expect("SET db0");
    wait_until(CAPTURE_WAIT, "db0 event skipped", || {
        info_field(&mut c0, "skipped_db") >= 1
    });
    assert_eq!(xlen(&mut c0, "events:set"), 0, "db0 must be excluded");

    let _: () = c2.set("in2", "v").expect("SET db2");
    wait_until(CAPTURE_WAIT, "db2 event mirrored", || {
        xlen(&mut c0, "events:set") == 1
    });
    let dbs = stream_field_strings(&mut c0, "events:set", "db");
    assert_eq!(dbs, vec!["2"], "only the db-2 event lands");
}

#[test]
fn source_db_live_change() {
    let s = TestServer::start(&["events", "set"]);
    let mut c0 = s.conn();
    let mut c2 = s.conn_db(2);

    // Default `*` captures db 0.
    let _: () = c0.set("a", "v").expect("SET db0 default");
    wait_until(CAPTURE_WAIT, "default captures db0", || {
        xlen(&mut c0, "events:set") == 1
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.source-dbs")
        .arg("2")
        .query(&mut c0)
        .expect("CONFIG SET source-dbs 2");

    let _: () = c0.set("b", "v").expect("SET db0 after change");
    wait_until(CAPTURE_WAIT, "db0 now skipped", || {
        info_field(&mut c0, "skipped_db") >= 1
    });
    assert_eq!(xlen(&mut c0, "events:set"), 1, "db0 no longer captured");

    let _: () = c2.set("c", "v").expect("SET db2 after change");
    wait_until(CAPTURE_WAIT, "db2 captured after change", || {
        xlen(&mut c0, "events:set") == 2
    });
}

#[test]
fn source_dbs_rejects_bad_values() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    for bad in ["", "0,", "0,foo", "-1"] {
        let res: Result<(), _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.source-dbs")
            .arg(bad)
            .query(&mut c);
        assert!(res.is_err(), "CONFIG SET source-dbs must reject {bad:?}");
    }
}

// --- Max-streams cap (issue #64) ---

#[test]
fn max_streams_caps_new_stream_creation() {
    // With the cap at 2, the first two distinct event names register their
    // streams; a third is dropped and counted, while the already-registered
    // streams keep receiving events.
    let s = TestServer::start(&["events", "set,del,lpush", "max-streams", "2"]);
    let mut c = s.conn();

    let _: () = c.set("k1", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "events:set registered", || {
        info_field(&mut c, "active_streams") == 1
    });
    let _: () = c.del("k1").expect("DEL");
    wait_until(CAPTURE_WAIT, "events:del registered", || {
        info_field(&mut c, "active_streams") == 2
    });

    // The third distinct name would create a third stream: dropped.
    let _: () = c.lpush("k2", "v").expect("LPUSH");
    wait_until(CAPTURE_WAIT, "third stream dropped", || {
        info_field(&mut c, "dropped_max_streams") >= 1
    });
    assert_eq!(xlen(&mut c, "events:lpush"), 0, "capped stream not created");
    assert_eq!(info_field(&mut c, "active_streams"), 2, "still two streams");

    // An already-registered stream keeps receiving events past the cap.
    let forwarded_before = info_field(&mut c, "forwarded");
    let _: () = c.set("k3", "v").expect("SET into existing stream");
    wait_until(CAPTURE_WAIT, "existing stream still fed", || {
        info_field(&mut c, "forwarded") > forwarded_before
    });
    assert!(xlen(&mut c, "events:set") >= 2);
    // The cap drop is part of the dropped sum.
    assert!(info_field(&mut c, "dropped") >= 1);
    // A max-streams refusal produces no canonical entry, so it is a lost
    // event (issue #218).
    assert!(
        info_field(&mut c, "events_lost") >= 1,
        "a max-streams drop is a lost event"
    );
}

#[test]
fn max_streams_zero_is_unlimited() {
    let s = TestServer::start(&["events", "set,del,lpush", "max-streams", "0"]);
    let mut c = s.conn();

    let _: () = c.set("k1", "v").expect("SET");
    let _: () = c.del("k1").expect("DEL");
    let _: () = c.lpush("k2", "v").expect("LPUSH");
    wait_until(CAPTURE_WAIT, "three streams created", || {
        info_field(&mut c, "active_streams") == 3
    });
    assert_eq!(info_field(&mut c, "dropped_max_streams"), 0);
}

#[test]
fn max_streams_control_stream_exempt() {
    // The control stream is never event-derived, so gap markers are written
    // even when the cap is full.
    let s = TestServer::start(&["events", "set", "max-streams", "1"]);
    let mut c = s.conn();

    let _: () = c.set("k1", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "cap filled by events:set", || {
        info_field(&mut c, "active_streams") == 1
    });

    // Toggling enabled records disabled/enabled gap markers; the next event
    // after each flushes them to the control stream despite the full cap.
    let markers_before = info_field(&mut c, "control_markers");
    for state in ["no", "yes"] {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.enabled")
            .arg(state)
            .query(&mut c)
            .expect("toggle enabled");
        let _: () = c.set("k1", "v").expect("SET to flush marker");
    }
    wait_until(CAPTURE_WAIT, "markers written past cap", || {
        info_field(&mut c, "control_markers") > markers_before
    });
    // The control stream exists and is not counted against active_streams.
    assert_eq!(info_field(&mut c, "active_streams"), 1);
}

#[test]
fn max_streams_rejects_negative_module_arg() {
    let err = TestServer::try_start(&["max-streams", "-1"])
        .err()
        .expect("loadmodule with max-streams -1 must abort the server start");
    assert!(
        err.contains("max-streams must be 0 (unlimited) or positive"),
        "the abort must come from the max-streams validator, not an unrelated startup failure: {err}"
    );
}

#[test]
fn max_streams_negative_config_set_rejected() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    let res: Result<(), _> = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.max-streams")
        .arg(-1)
        .query(&mut c);
    assert!(res.is_err(), "negative max-streams must be rejected");
}

#[test]
fn max_streams_runtime_lowering_is_accepted() {
    // SPEC.md section 7: lowering the cap below the current count at runtime
    // is accepted; existing streams continue, and no new stream is created
    // until the count is back under the cap.
    let s = TestServer::start(&["events", "set,del,lpush"]);
    let mut c = s.conn();
    let _: () = c.set("k1", "v").expect("SET");
    let _: () = c.del("k1").expect("DEL");
    wait_until(CAPTURE_WAIT, "two streams registered", || {
        info_field(&mut c, "active_streams") == 2
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.max-streams")
        .arg(1)
        .query(&mut c)
        .expect("CONFIG SET max-streams below the current count is accepted");

    // Already-registered streams keep receiving events.
    let before = info_field(&mut c, "forwarded");
    let _: () = c.set("k2", "v").expect("SET into existing stream");
    wait_until(CAPTURE_WAIT, "existing stream still fed", || {
        info_field(&mut c, "forwarded") > before
    });

    // A new event name is refused while the count sits over the cap.
    let _: () = c.lpush("k3", "v").expect("LPUSH");
    wait_until(
        CAPTURE_WAIT,
        "new stream dropped over the lowered cap",
        || info_field(&mut c, "dropped_max_streams") >= 1,
    );
    assert_eq!(xlen(&mut c, "events:lpush"), 0, "capped stream not created");

    // Raising the cap re-admits new names.
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.max-streams")
        .arg(5)
        .query(&mut c)
        .expect("CONFIG SET max-streams 5");
    let _: () = c.lpush("k4", "v").expect("LPUSH after raise");
    wait_until(CAPTURE_WAIT, "new stream admitted after the raise", || {
        xlen(&mut c, "events:lpush") > 0
    });
}
