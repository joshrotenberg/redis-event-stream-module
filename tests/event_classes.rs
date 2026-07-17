//! MISSED and NEW capture through the module's own raw keyspace subscription,
//! and the load-time gating that keeps them opt-in (issue #20).

mod common;

use common::*;
use redis::Commands;

#[test]
fn missed_class_captures_keymiss() {
    let s = TestServer::start(&["events", "@missed"]);
    let mut c = s.conn();

    // A read miss on a key that does not exist fires a keymiss notification.
    let _: Option<String> = c.get("no_such_key").expect("GET miss");
    wait_until(CAPTURE_WAIT, "keymiss captured", || {
        xlen(&mut c, "events:keymiss") > 0
    });
    let events = stream_field_strings(&mut c, "events:keymiss", "event");
    assert_eq!(events, vec!["keymiss"]);
    let keys = stream_field_strings(&mut c, "events:keymiss", "key");
    assert_eq!(keys, vec!["no_such_key"]);
    assert_eq!(info_field(&mut c, "handler_panics"), 0);
}

#[test]
fn new_class_captures_new_key() {
    let s = TestServer::start(&["events", "@new"]);
    let mut c = s.conn();

    let _: () = c.set("fresh", "1").expect("SET new key");
    wait_until(CAPTURE_WAIT, "new-key event captured", || {
        xlen(&mut c, "events:new") > 0
    });
    let events = stream_field_strings(&mut c, "events:new", "event");
    assert_eq!(events, vec!["new"]);
    let keys = stream_field_strings(&mut c, "events:new", "key");
    assert_eq!(keys, vec!["fresh"]);
}

#[test]
fn missed_and_new_not_delivered_by_default() {
    // The default filter does not name these classes, so the module does not
    // subscribe to them: no keymiss or new stream ever appears.
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let _: Option<String> = c.get("absent").expect("GET miss");
    let _: () = c.set("created", "1").expect("SET");
    // Capture an expiration to prove the handler is live, then assert the
    // extra-class streams never materialized.
    expire_key_and_wait(&s, "doomed", "events:expired", 0);
    assert_eq!(xlen(&mut c, "events:keymiss"), 0);
    assert_eq!(xlen(&mut c, "events:new"), 0);
}

#[test]
fn runtime_widening_to_extra_class_is_rejected() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    for token in ["@missed", "@new"] {
        let res: Result<(), _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.events")
            .arg(token)
            .query(&mut c);
        assert!(
            res.is_err(),
            "runtime CONFIG SET events {token} must be rejected (mask fixed at load)"
        );
    }

    // The filter is unchanged after the rejections.
    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.events")
        .query(&mut c)
        .expect("CONFIG GET");
    assert_eq!(pair[1], "expired");
}

#[test]
fn star_adapts_at_runtime_without_extra_classes() {
    // `*` does not name MISSED/NEW, so setting it at runtime is allowed; it
    // captures whatever is subscribed (the NOTIFY_ALL classes).
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.events")
        .arg("*")
        .query(&mut c)
        .expect("CONFIG SET events *");

    let _: () = c.set("k", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "set captured under *", || {
        xlen(&mut c, "events:set") > 0
    });
    // A read miss is not captured: MISSED was not subscribed at load.
    let _: Option<String> = c.get("gone").expect("GET miss");
    assert_eq!(xlen(&mut c, "events:keymiss"), 0);
}

#[test]
fn uncapturable_classes_rejected_at_load_and_runtime() {
    // At runtime: a clear rejection.
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    for token in ["@loaded", "@trimmed"] {
        let res: Result<(), _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.events")
            .arg(token)
            .query(&mut c);
        assert!(res.is_err(), "{token} must be rejected");
    }

    // At load: an invalid filter arg aborts the module load.
    assert!(
        TestServer::try_start(&["events", "@loaded"]).is_err(),
        "loading with events @loaded must abort"
    );
}

#[test]
fn star_at_load_subscribes_missed_and_new() {
    // SPEC.md section 5: a load-time `events *` subscribes MISSED and NEW in
    // addition to NOTIFY_ALL. Pins the init star branch, which the unit test
    // star_does_not_name_extra_classes structurally cannot see: that test
    // pins extra_classes_named(*) as EMPTY, so an init refactor reusing it
    // would silently drop this subscription while the rest of the suite
    // stayed green.
    let s = TestServer::start(&["events", "*"]);
    let mut c = s.conn();

    let _: Option<String> = c.get("no_such_key").expect("GET miss");
    wait_until(CAPTURE_WAIT, "keymiss captured under load-time *", || {
        xlen(&mut c, "events:keymiss") > 0
    });

    let _: () = c.set("fresh", "1").expect("SET new key");
    wait_until(
        CAPTURE_WAIT,
        "new-key event captured under load-time *",
        || xlen(&mut c, "events:new") > 0,
    );
}
