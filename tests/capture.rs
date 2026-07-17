//! Core capture-path behavior (issue #12): filtering, routing, db
//! consolidation, config validation, guards, and trimming.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

#[test]
fn default_filter_captures_expired_only() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let _: () = c.set("plain", "v").expect("SET");
    expire_key_and_wait(&s, "doomed", "events:expired", 0);

    assert_eq!(xlen(&mut c, "events:set"), 0, "set must not be captured");
    let events = stream_field_strings(&mut c, "events:expired", "event");
    assert_eq!(events, vec!["expired"]);
    let keys = stream_field_strings(&mut c, "events:expired", "key");
    assert_eq!(keys, vec!["doomed"]);
}

#[test]
fn cross_db_event_lands_in_db0_with_origin_field() {
    let s = TestServer::start(&[]);
    let mut c3 = s.conn_db(3);
    let mut c0 = s.conn();

    let _: () = redis::cmd("SET")
        .arg("dbkey")
        .arg("v")
        .arg("PX")
        .arg(80)
        .query(&mut c3)
        .expect("SET in db 3");
    wait_until(
        Duration::from_secs(10),
        "db3 expiry mirrored to db0",
        || {
            let _: Option<String> = c3.get("dbkey").ok().flatten();
            xlen(&mut c0, "events:expired") > 0
        },
    );

    let dbs = stream_field_strings(&mut c0, "events:expired", "db");
    assert_eq!(dbs, vec!["3"], "db field records origin database");
    let exists_in_db3: i64 = redis::cmd("EXISTS")
        .arg("events:expired")
        .query(&mut c3)
        .expect("EXISTS");
    assert_eq!(exists_in_db3, 0, "no stream in the origin db");
}

#[test]
fn filter_validation_rejects_bad_values() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    for bad in ["expired,@hsah", "", "expired,", "foo bar", "@HASH"] {
        let res: Result<(), _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.events")
            .arg(bad)
            .query(&mut c);
        assert!(res.is_err(), "CONFIG SET must reject {bad:?}");
    }

    // The filter is unchanged after rejected sets.
    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.events")
        .query(&mut c)
        .expect("CONFIG GET");
    assert_eq!(pair[1], "expired");
}

#[test]
fn filter_live_change_to_class() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.events")
        .arg("@hash")
        .query(&mut c)
        .expect("CONFIG SET @hash");

    let _: () = c.hset("h", "f", "v").expect("HSET");
    wait_until(CAPTURE_WAIT, "hset mirrored", || {
        xlen(&mut c, "events:hset") > 0
    });
    let _: () = c.set("plain", "v").expect("SET");
    assert_eq!(xlen(&mut c, "events:set"), 0, "@hash must not capture set");
}

#[test]
fn filter_star_captures_multiple_event_types() {
    let s = TestServer::start(&["events", "*"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "set and del mirrored", || {
        xlen(&mut c, "events:set") > 0 && xlen(&mut c, "events:del") > 0
    });
}

#[test]
fn prefix_is_immutable_at_runtime() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    let res: Result<(), _> = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.stream-prefix")
        .arg("other:")
        .query(&mut c);
    assert!(res.is_err(), "stream-prefix must be immutable");
}

#[test]
fn prefix_module_arg_routes() {
    let s = TestServer::start(&["stream-prefix", "ks:", "events", "expired,set"]);
    let mut c = s.conn();
    let _: () = c.set("x", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "custom prefix routing", || {
        xlen(&mut c, "ks:set") > 0
    });
    assert_eq!(xlen(&mut c, "events:set"), 0);
}

#[test]
fn negative_maxlen_module_arg_aborts_load() {
    let err = TestServer::try_start(&["maxlen", "-1"]);
    assert!(
        err.is_err(),
        "loadmodule with maxlen -1 must abort the server start"
    );
}

#[test]
fn maxlen_bounds_stream_growth() {
    let s = TestServer::start(&["events", "set", "maxlen", "100"]);
    let mut c = s.conn();
    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(
        Duration::from_secs(10),
        "500 sets mirrored or trimmed",
        || info_field(&mut c, "forwarded") >= 500,
    );
    let len = xlen(&mut c, "events:set");
    assert!(
        len < 500,
        "approximate MAXLEN 100 must trim well below 500, got {len}"
    );
}

#[test]
fn maxlen_zero_disables_trimming() {
    // `maxlen 0` disables trimming (SPEC.md section 7): the XADD carries no
    // MAXLEN clause at all. Exact length at 500 is the property that
    // distinguishes 0 from every positive cap — the sibling test shows
    // approximate `MAXLEN ~ 100` demonstrably trims below 500.
    let s = TestServer::start(&["events", "set", "maxlen", "0"]);
    let mut c = s.conn();

    // The key is live-settable; 0 must also be accepted on the runtime path
    // (same validation as the module-arg path just exercised).
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.maxlen")
        .arg(0)
        .query(&mut c)
        .expect("CONFIG SET maxlen 0 must be accepted");

    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(CAPTURE_WAIT, "500 sets mirrored", || {
        info_field(&mut c, "forwarded") >= 500
    });
    assert_eq!(
        xlen(&mut c, "events:set"),
        500,
        "maxlen 0 must never trim: 500 writes yield exactly 500 entries"
    );
}

#[test]
fn feedback_guard_never_mirrors_own_prefix() {
    let s = TestServer::start(&["events", "*"]);
    let mut c = s.conn();

    let _: () = redis::cmd("XADD")
        .arg("events:manual")
        .arg("*")
        .arg("f")
        .arg("v")
        .query(&mut c)
        .expect("XADD");
    // The manual write fires an xadd notification on a prefix key; give the
    // skipped_self counter time to move, then confirm no mirror happened.
    wait_until(CAPTURE_WAIT, "skipped_self counted", || {
        info_field(&mut c, "skipped_self") > 0
    });
    assert_eq!(
        xlen(&mut c, "events:xadd"),
        0,
        "own-prefix activity must never be mirrored"
    );
}

#[test]
fn binary_keys_round_trip() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();
    let key: Vec<u8> = vec![0xff, 0x00, 0xfe, b'k'];
    let _: () = c.set(key.clone(), "v").expect("SET binary key");
    wait_until(CAPTURE_WAIT, "binary-key set mirrored", || {
        xlen(&mut c, "events:set") > 0
    });
    let keys = stream_field_values(&mut c, "events:set", "key");
    assert_eq!(keys, vec![key], "key bytes must round-trip exactly");
}

#[test]
fn enabled_toggle_drops_and_resumes() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("one", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "first set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("no")
        .query(&mut c)
        .expect("disable");
    let _: () = c.set("two", "v").expect("SET while disabled");
    // No convergence to wait for; the event is dropped synchronously.
    assert_eq!(xlen(&mut c, "events:set"), 1, "disabled must drop events");

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("yes")
        .query(&mut c)
        .expect("enable");
    let _: () = c.set("three", "v").expect("SET after enable");
    wait_until(CAPTURE_WAIT, "capture resumes", || {
        xlen(&mut c, "events:set") == 2
    });
}

#[test]
fn multi_exec_sees_pre_event_stream_state() {
    // SPEC.md section 9: post-notification jobs run at the end of the
    // execution unit, so a later command in the same MULTI/EXEC observes the
    // keyspace change before the mirrored entry exists. Consumer-facing and
    // easy to trip over from Lua/transactions, so pinned here; a change in
    // post-notification-job timing must fail this test, not silently change
    // semantics.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    // Prime one captured event so the stream exists with a known length.
    let _: () = c.set("prime", "v").expect("SET prime");
    wait_until(CAPTURE_WAIT, "prime mirrored", || {
        xlen(&mut c, "events:set") == 1
    });

    // Inside one transaction: mutate, read the key, read the stream. The GET
    // sees the keyspace change; the XLEN still reads the pre-event length.
    let (got, len_inside): (String, i64) = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("k")
        .arg("v")
        .ignore()
        .cmd("GET")
        .arg("k")
        .cmd("XLEN")
        .arg("events:set")
        .query(&mut c)
        .expect("MULTI/EXEC");
    assert_eq!(got, "v", "the keyspace change is visible inside the MULTI");
    assert_eq!(
        len_inside, 1,
        "the mirrored entry must not be visible inside the same MULTI/EXEC"
    );

    // The job runs at the end of the execution unit; the entry lands after
    // EXEC.
    wait_until(CAPTURE_WAIT, "entry lands after EXEC", || {
        xlen(&mut c, "events:set") == 2
    });
}

#[test]
fn lua_script_sees_pre_event_stream_state() {
    // The EVAL analogue of the MULTI/EXEC exception (SPEC.md section 9): a
    // script that both mutates a key and reads the module's streams sees
    // pre-event stream state; the entry lands after the script completes.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("prime", "v").expect("SET prime");
    wait_until(CAPTURE_WAIT, "prime mirrored", || {
        xlen(&mut c, "events:set") == 1
    });

    let len_inside: i64 = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], 'v'); return redis.call('XLEN', KEYS[2])")
        .arg(2)
        .arg("k")
        .arg("events:set")
        .query(&mut c)
        .expect("EVAL");
    assert_eq!(
        len_inside, 1,
        "the mirrored entry must not be visible inside the script"
    );

    wait_until(CAPTURE_WAIT, "entry lands after the script", || {
        xlen(&mut c, "events:set") == 2
    });
}
