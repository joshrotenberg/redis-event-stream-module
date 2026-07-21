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

#[test]
fn firehose_failure_leaves_per_event_entry_intact() {
    // SPEC.md section 5: the per-event write and the firehose copy succeed
    // or fail independently; a failed copy counts in the dropped_* counters
    // and is attributed to the firehose's own WITHSTATS row (section 8).
    let s = TestServer::start(&["events", "set", "firehose", "yes"]);
    let mut c = s.conn();

    // One successful event registers both destinations.
    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "firehose registered", || {
        info_field(&mut c, "firehose_forwarded") == 1
    });

    // Break only the firehose: WRONGTYPE occupant at its key. Writes to
    // prefix keys are guarded, never mirrored, so this cannot loop.
    let _: () = redis::cmd("DEL")
        .arg("events:#firehose")
        .query(&mut c)
        .expect("DEL firehose");
    let _: () = c.set("events:#firehose", "occupied").expect("SET occupant");

    let _: () = c.set("b", "2").expect("SET with broken firehose");
    wait_until(CAPTURE_WAIT, "failed copy counted", || {
        info_field(&mut c, "dropped_xadd_error") >= 1
    });

    // Independence: the per-event entry landed; only the copy was dropped.
    assert_eq!(xlen(&mut c, "events:set"), 2, "per-event write unaffected");
    assert_eq!(
        info_field(&mut c, "firehose_forwarded"),
        1,
        "no copy written while the firehose is occupied"
    );
    let st = streams_withstats(&mut c);
    assert_eq!(
        st["events:#firehose"],
        (1, 1),
        "the drop lands on the firehose's own row"
    );
    assert_eq!(
        st["events:set"],
        (2, 0),
        "the per-event row records no drop"
    );
    // The canonical entry was written, so this is an auxiliary-copy failure,
    // not a lost event (issue #218): events_lost must stay 0 even though
    // dropped moved.
    assert_eq!(
        info_field(&mut c, "events_lost"),
        0,
        "a firehose-only failure is not a lost event"
    );
    assert!(
        info_field(&mut c, "dropped") >= 1,
        "the failed copy still counts as a failed destination write"
    );

    // Clear the occupant: the copy path recovers on the next event.
    let _: () = redis::cmd("DEL")
        .arg("events:#firehose")
        .query(&mut c)
        .expect("DEL occupant");
    let _: () = c.set("d", "3").expect("SET after fix");
    wait_until(CAPTURE_WAIT, "copy path recovers", || {
        info_field(&mut c, "firehose_forwarded") == 2
    });
    assert_eq!(xlen(&mut c, "events:set"), 3);
}

#[test]
fn both_writes_refused_counts_two_drops() {
    // SPEC.md section 5: drop accounting is per write, not per event — one
    // event whose per-event entry and firehose copy are both refused counts
    // two drops, one on each stream's WITHSTATS row.
    let s = TestServer::start(&["events", "set", "firehose", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "both destinations registered", || {
        info_field(&mut c, "firehose_forwarded") == 1
    });

    // Occupy both destinations.
    for key in ["events:set", "events:#firehose"] {
        let _: () = redis::cmd("DEL").arg(key).query(&mut c).expect("DEL");
        let _: () = c.set(key, "occupied").expect("SET occupant");
    }

    // One event, two refused writes, two drops.
    let _: () = c.set("b", "2").expect("SET with both broken");
    wait_until(CAPTURE_WAIT, "two drops counted for one event", || {
        info_field(&mut c, "dropped_xadd_error") == 2
    });
    assert_eq!(info_field(&mut c, "forwarded"), 1, "no per-event write");
    assert_eq!(info_field(&mut c, "firehose_forwarded"), 1, "no copy");
    let st = streams_withstats(&mut c);
    assert_eq!(st["events:set"], (1, 1));
    assert_eq!(st["events:#firehose"], (1, 1));
    assert_eq!(
        info_field(&mut c, "dropped"),
        2,
        "the dropped sum sees both writes"
    );
    // The per-event vs per-write distinction (issue #218): the two failed
    // writes belong to one selected event, so events_lost is 1 while dropped
    // is 2.
    assert_eq!(
        info_field(&mut c, "events_lost"),
        1,
        "one selected event lost, even though two destination writes failed"
    );
}
