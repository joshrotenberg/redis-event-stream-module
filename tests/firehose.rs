//! Firehose stream behavior (issue #58): the opt-in combined stream at
//! `<prefix>#firehose` that mirrors every captured event alongside its
//! per-event stream.

mod common;

use common::*;
use redis::Commands;

#[test]
fn firehose_off_by_default() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.firehose")
        .query(&mut c)
        .expect("CONFIG GET");
    assert_eq!(pair[1], "no", "firehose must default to no");

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "set mirrored", || {
        info_field(&mut c, "forwarded") == 1
    });
    assert_eq!(
        xlen(&mut c, "events:#firehose"),
        0,
        "no firehose key may appear while the config is off"
    );
    assert_eq!(info_field(&mut c, "firehose_forwarded"), 0);
}

#[test]
fn firehose_mirrors_every_event_with_identical_fields() {
    // Enabled as an unprefixed module arg; every captured event must land in
    // both its per-event stream and the firehose, same three fields.
    let s = TestServer::start(&["events", "set,del", "firehose", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "both copies written", || {
        info_field(&mut c, "forwarded") == 2 && info_field(&mut c, "firehose_forwarded") == 2
    });

    // The firehose carries both events in notification order, one stream.
    assert_eq!(
        stream_field_strings(&mut c, "events:#firehose", "event"),
        vec!["set", "del"]
    );
    assert_eq!(
        stream_field_strings(&mut c, "events:#firehose", "key"),
        vec!["a", "a"]
    );
    assert_eq!(
        stream_field_strings(&mut c, "events:#firehose", "db"),
        vec!["0", "0"]
    );
    // The per-event streams still get their own copies, unchanged.
    assert_eq!(
        stream_field_strings(&mut c, "events:set", "event"),
        vec!["set"]
    );
    assert_eq!(
        stream_field_strings(&mut c, "events:del", "event"),
        vec!["del"]
    );

    // The firehose registers in the discovery registry on first write.
    let mut streams: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut c)
        .expect("STREAMS");
    streams.sort();
    assert_eq!(
        streams,
        vec!["events:#firehose", "events:del", "events:set"]
    );
}

#[test]
fn firehose_toggles_at_runtime() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("one", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "first set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(xlen(&mut c, "events:#firehose"), 0);

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.firehose")
        .arg("yes")
        .query(&mut c)
        .expect("enable firehose");
    let _: () = c.set("two", "v").expect("SET while on");
    wait_until(CAPTURE_WAIT, "firehose copy written", || {
        xlen(&mut c, "events:#firehose") == 1
    });
    assert_eq!(
        stream_field_strings(&mut c, "events:#firehose", "key"),
        vec!["two"],
        "only events captured while the firehose is on get a copy; no replay"
    );

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.firehose")
        .arg("no")
        .query(&mut c)
        .expect("disable firehose");
    let _: () = c.set("three", "v").expect("SET while off");
    wait_until(CAPTURE_WAIT, "per-event capture continues", || {
        xlen(&mut c, "events:set") == 3
    });
    assert_eq!(
        xlen(&mut c, "events:#firehose"),
        1,
        "no firehose copy after the toggle off"
    );
    assert_eq!(info_field(&mut c, "firehose_forwarded"), 1);
    assert_eq!(
        info_field(&mut c, "forwarded"),
        3,
        "forwarded keeps meaning captured events, not XADDs issued"
    );
}

#[test]
fn maxlen_bounds_firehose_growth() {
    let s = TestServer::start(&["events", "set", "maxlen", "100", "firehose", "yes"]);
    let mut c = s.conn();
    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(CAPTURE_WAIT, "500 firehose copies written", || {
        info_field(&mut c, "firehose_forwarded") >= 500
    });
    let len = xlen(&mut c, "events:#firehose");
    assert!(
        len < 500,
        "approximate MAXLEN 100 must trim the firehose well below 500, got {len}"
    );
}

#[test]
fn maxlen_zero_disables_firehose_trimming() {
    // The firehose XADD gates its MAXLEN clause on the same `maxlen > 0`
    // branch as the per-event write (SPEC.md section 7: 0 disables trimming);
    // exact length at 500 distinguishes 0 from every positive cap.
    let s = TestServer::start(&["events", "set", "maxlen", "0", "firehose", "yes"]);
    let mut c = s.conn();
    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(CAPTURE_WAIT, "500 firehose copies written", || {
        info_field(&mut c, "firehose_forwarded") >= 500
    });
    assert_eq!(
        xlen(&mut c, "events:#firehose"),
        500,
        "maxlen 0 must never trim the firehose"
    );
    assert_eq!(
        xlen(&mut c, "events:set"),
        500,
        "maxlen 0 must never trim the per-event stream"
    );
}

#[test]
fn event_name_with_hash_cannot_alias_the_firehose() {
    // The filter accepts a bare name containing '#', but the sanitizer maps
    // '#' to '_' (SPEC.md section 5), so even if such an event ever fired it
    // would route to events:_firehose; the firehose key itself stays
    // unreachable from event input and holds only genuine copies.
    let s = TestServer::start(&["events", "set", "firehose", "yes"]);
    let mut c = s.conn();

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.events")
        .arg("set,#firehose")
        .query(&mut c)
        .expect("a name containing '#' is a valid bare filter token");

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "copy written", || {
        info_field(&mut c, "firehose_forwarded") == 1
    });
    assert_eq!(
        stream_field_strings(&mut c, "events:#firehose", "event"),
        vec!["set"],
        "the firehose holds only module-written copies"
    );
}

#[test]
fn firehose_occupies_one_max_streams_slot() {
    // SPEC.md section 7: the firehose registers like any destination stream
    // and occupies one cap slot, but is itself never blocked by the cap.
    let s = TestServer::start(&["events", "set,del", "firehose", "yes", "max-streams", "2"]);
    let mut c = s.conn();

    // One captured SET registers both events:set and the firehose: cap full.
    let _: () = c.set("k1", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "set stream plus firehose registered", || {
        info_field(&mut c, "active_streams") == 2
    });
    assert!(
        xlen(&mut c, "events:#firehose") > 0,
        "the firehose is never itself blocked by the cap"
    );

    // A second event name would need a third slot: refused and counted.
    let _: () = c.del("k1").expect("DEL");
    wait_until(CAPTURE_WAIT, "second event name dropped at the cap", || {
        info_field(&mut c, "dropped_max_streams") >= 1
    });
    assert_eq!(xlen(&mut c, "events:del"), 0, "capped stream not created");

    // Copies keep flowing to the firehose for events on registered streams.
    let copies = info_field(&mut c, "firehose_forwarded");
    let _: () = c.set("k2", "v").expect("SET");
    wait_until(
        CAPTURE_WAIT,
        "firehose copy written past the full cap",
        || info_field(&mut c, "firehose_forwarded") > copies,
    );
}
