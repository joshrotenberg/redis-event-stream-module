//! Hash-field expiration capture (issue #93). Field-level TTLs (`HEXPIRE`
//! and friends, Redis 7.4+) fire `hexpired` under the HASH class, not
//! EXPIRED, so the default `expired` filter silently misses them (SPEC.md
//! sections 5 and 7). Each test probes for `HEXPIRE` and skips cleanly on
//! servers without hash-field TTLs (Redis 7.2, Valkey 8).

mod common;

use common::*;
use std::time::Duration;

/// Capability gate, not a version gate: `COMMAND INFO HEXPIRE` covers both
/// the 7.2 lane (predates 7.4) and the Valkey lane (has not shipped
/// hash-field TTLs) without CI matrix changes.
fn hexpire_supported(s: &TestServer) -> bool {
    let mut c = s.conn();
    if server_has_command(&mut c, "HEXPIRE") {
        return true;
    }
    eprintln!("skipping: server has no HEXPIRE (hash-field TTLs are Redis 7.4+)");
    false
}

/// Set hash field `f` on key `h` with a short TTL, then converge on `done`.
/// Unlike whole-key `expired`, `hexpired` is delivered by the active
/// field-expiry cycle; a lazy access (HGET) removes the expired field without
/// firing the notification, so — opposite to `expire_key_and_wait` — this must
/// NOT touch the field while waiting, or it races the active cycle and
/// silently suppresses the event (the field is gone, `hexpired` never fires).
fn expire_field_and_wait(
    c: &mut redis::Connection,
    what: &str,
    mut done: impl FnMut(&mut redis::Connection) -> bool,
) {
    let _: () = redis::cmd("HSET")
        .arg("h")
        .arg("f")
        .arg("v")
        .query(&mut *c)
        .expect("HSET");
    let _: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(50)
        .arg("FIELDS")
        .arg(1)
        .arg("f")
        .query(&mut *c)
        .expect("HPEXPIRE");
    wait_until(Duration::from_secs(10), what, || done(c));
}

#[test]
fn hexpired_routes_under_explicit_name() {
    let s = TestServer::start(&["events", "hexpired"]);
    if !hexpire_supported(&s) {
        return;
    }
    let mut c = s.conn();

    expire_field_and_wait(&mut c, "hexpired mirrored", |c| {
        xlen(c, "events:hexpired") > 0
    });

    assert_eq!(
        stream_field_strings(&mut c, "events:hexpired", "event"),
        vec!["hexpired"]
    );
    // The notification carries only the hash key; the expired field name is
    // not available at notification time (SPEC.md section 6).
    assert_eq!(
        stream_field_strings(&mut c, "events:hexpired", "key"),
        vec!["h"]
    );
    assert_eq!(
        stream_field_strings(&mut c, "events:hexpired", "db"),
        vec!["0"]
    );
}

#[test]
fn hash_class_selects_hexpired() {
    // On Redis, `hexpired` fires under the HASH class, so `@hash` selects it
    // (alongside the command-generated events like `hexpire`, each to its own
    // stream). This is a Redis-specific classification: Valkey 9 emits
    // `hexpired` under a different class, so `@hash` does not capture it there
    // (the explicit-name filter in `hexpired_routes_under_explicit_name`, which
    // matches the event name rather than a class, works on both). Gate to Redis;
    // the divergence is a documented cross-server caveat (SPEC.md section 5).
    let s = TestServer::start(&["events", "@hash"]);
    if !hexpire_supported(&s) {
        return;
    }
    let mut c = s.conn();
    if is_valkey(&mut c) {
        eprintln!("skipping: Valkey classifies hexpired outside the HASH class");
        return;
    }

    expire_field_and_wait(&mut c, "hexpired mirrored via @hash", |c| {
        xlen(c, "events:hexpired") > 0
    });
    assert_eq!(
        stream_field_strings(&mut c, "events:hexpired", "key"),
        vec!["h"]
    );
}

#[test]
fn default_filter_does_not_capture_hexpired() {
    // The trap this suite pins explicitly (SPEC.md section 7): bare filter
    // tokens are exact byte comparisons, so the default `expired` does not
    // match `hexpired` — durable hash-field expiry needs `expired,hexpired`
    // or `@hash`.
    let s = TestServer::start(&[]);
    if !hexpire_supported(&s) {
        return;
    }
    let mut c = s.conn();

    // hset, hexpire, and the eventual hexpired all miss the default filter;
    // converge on the field being gone (its removal fires hexpired), then on
    // the filtered count covering at least those three.
    expire_field_and_wait(&mut c, "field expired", |c| {
        let n: i64 = redis::cmd("HEXISTS")
            .arg("h")
            .arg("f")
            .query(c)
            .expect("HEXISTS");
        n == 0
    });
    wait_until(Duration::from_secs(10), "hexpired counted filtered", || {
        info_field(&mut c, "skipped_filtered") >= 3
    });

    assert_eq!(
        xlen(&mut c, "events:hexpired"),
        0,
        "default `expired` filter must not match `hexpired`"
    );
    let streams: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut c)
        .expect("STREAMS");
    assert!(
        !streams.contains(&"events:hexpired".to_string()),
        "no hexpired stream may register under the default filter"
    );
}

#[test]
fn last_field_expiry_deletes_the_hash_with_a_del_event() {
    // When the only field expires the hash becomes empty and Redis deletes
    // the key, firing `del`; consumers watching `hexpired` alone still see
    // the field expiry, but the key removal is a separate `del` event.
    let s = TestServer::start(&["events", "hexpired,del"]);
    if !hexpire_supported(&s) {
        return;
    }
    let mut c = s.conn();

    expire_field_and_wait(&mut c, "hexpired and del mirrored", |c| {
        xlen(c, "events:hexpired") > 0 && xlen(c, "events:del") > 0
    });

    assert_eq!(
        stream_field_strings(&mut c, "events:del", "key"),
        vec!["h"],
        "the del event accompanying the last field's expiry names the hash key"
    );
    let exists: i64 = redis::cmd("EXISTS").arg("h").query(&mut c).expect("EXISTS");
    assert_eq!(exists, 0, "the hash is gone once its last field expired");
}
