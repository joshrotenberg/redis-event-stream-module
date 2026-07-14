//! Entry-format enum and the global `seq` field (issues #60, #66): the
//! `eventstream.entry-format` config selects the mirrored entry's field set,
//! and `eventstream.entry-seq` adds a process-global monotonic `seq` field for
//! cross-stream same-millisecond ordering. Both change the entry contents and
//! apply identically to the per-event write and its firehose copy (SPEC.md
//! section 6).

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

/// The field names of the first entry in `key`, in stream order. Reads the raw
/// XRANGE reply so field presence and order are both observable.
fn entry_field_names(conn: &mut redis::Connection, key: &str) -> Vec<String> {
    let reply: redis::Value = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(1)
        .query(conn)
        .expect("XRANGE");
    // XRANGE reply: [ [id, [f, v, f, v, ...]], ... ].
    let redis::Value::Array(entries) = reply else {
        panic!("XRANGE did not return an array");
    };
    let redis::Value::Array(first) = &entries[0] else {
        panic!("entry is not an array");
    };
    let redis::Value::Array(fields) = &first[1] else {
        panic!("field list is not an array");
    };
    fields
        .iter()
        .step_by(2)
        .map(|v| match v {
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
            redis::Value::SimpleString(s) => s.clone(),
            other => panic!("unexpected field name: {other:?}"),
        })
        .collect()
}

/// The first entry's value for `field` in `key`, as a lossy string.
fn first_field(conn: &mut redis::Connection, key: &str, field: &str) -> String {
    stream_field_strings(conn, key, field)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("field {field} missing from first entry of {key}"))
}

#[test]
fn default_format_is_fixed_and_byte_identical() {
    // The default must reproduce the historical event/key/db schema exactly:
    // no `format` discriminator, no `seq`, same three fields in order.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.entry-format")
        .query(&mut c)
        .expect("CONFIG GET entry-format");
    assert_eq!(pair[1], "fixed", "entry-format must default to fixed");

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:set"),
        vec!["event", "key", "db"],
        "fixed entry must be exactly event/key/db with no discriminator"
    );
    assert_eq!(first_field(&mut c, "events:set", "event"), "set");
    assert_eq!(first_field(&mut c, "events:set", "key"), "a");
    assert_eq!(first_field(&mut c, "events:set", "db"), "0");
}

#[test]
fn minimal_format_drops_event_and_adds_discriminator() {
    let s = TestServer::start(&["events", "set", "entry-format", "minimal"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:set"),
        vec!["format", "key", "db"],
        "minimal drops event and carries the format discriminator"
    );
    assert_eq!(first_field(&mut c, "events:set", "format"), "minimal");
    assert_eq!(first_field(&mut c, "events:set", "key"), "a");
}

#[test]
fn verbose_format_adds_class() {
    let s = TestServer::start(&["events", "set", "entry-format", "verbose"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:set"),
        vec!["format", "event", "key", "db", "class"],
    );
    assert_eq!(first_field(&mut c, "events:set", "format"), "verbose");
    // `set` is a string-class keyspace event.
    assert_eq!(first_field(&mut c, "events:set", "class"), "string");
}

#[test]
fn json_format_is_one_document_field_with_base64_key() {
    let s = TestServer::start(&["events", "set", "entry-format", "json"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:set"),
        vec!["format", "data"],
        "json is a single document field behind the discriminator"
    );
    assert_eq!(first_field(&mut c, "events:set", "format"), "json");
    // key "a" (0x61) base64-encodes to "YQ=="; db is a JSON number.
    assert_eq!(
        first_field(&mut c, "events:set", "data"),
        r#"{"event":"set","key":"YQ==","db":0}"#
    );
}

#[test]
fn format_applies_to_the_firehose_copy_too() {
    // The per-event write and the firehose copy share one EntrySpec, so the
    // firehose entry carries the same format field set (SPEC.md section 6).
    let s = TestServer::start(&[
        "events",
        "set",
        "entry-format",
        "verbose",
        "firehose",
        "yes",
    ]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "both copies written", || {
        info_field(&mut c, "forwarded") == 1 && info_field(&mut c, "firehose_forwarded") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:#firehose"),
        vec!["format", "event", "key", "db", "class"],
        "the firehose copy is shaped by the same entry-format"
    );
    assert_eq!(first_field(&mut c, "events:#firehose", "class"), "string");
}

#[test]
fn entry_format_is_runtime_mutable_producing_a_mixed_stream() {
    // entry-format is DEFAULT (live-settable); the discriminator makes the
    // resulting mixed-format stream self-describing per entry.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET fixed");
    wait_until(Duration::from_secs(10), "first entry", || {
        xlen(&mut c, "events:set") == 1
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.entry-format")
        .arg("minimal")
        .query(&mut c)
        .expect("entry-format is live-settable");
    let _: () = c.set("b", "2").expect("SET minimal");
    wait_until(Duration::from_secs(10), "second entry", || {
        xlen(&mut c, "events:set") == 2
    });

    // The stream now holds one fixed entry (no discriminator) followed by one
    // minimal entry (format=minimal): consumers tell them apart by `format`.
    let formats = stream_field_strings(&mut c, "events:set", "format");
    assert_eq!(
        formats,
        vec!["minimal"],
        "only the second entry carries a discriminator; fixed stays bare"
    );
    let events = stream_field_strings(&mut c, "events:set", "event");
    assert_eq!(
        events,
        vec!["set"],
        "only the fixed entry has an event field"
    );
}

#[test]
fn invalid_entry_format_is_rejected() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();
    let res: redis::RedisResult<()> = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.entry-format")
        .arg("bogus")
        .query(&mut c);
    assert!(
        res.is_err(),
        "an unknown entry-format value must be refused"
    );
}

#[test]
fn seq_is_off_by_default() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.entry-seq")
        .query(&mut c)
        .expect("CONFIG GET entry-seq");
    assert_eq!(pair[1], "no", "entry-seq must default to no");

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert!(
        stream_field_strings(&mut c, "events:set", "seq").is_empty(),
        "no seq field unless entry-seq is on"
    );
}

#[test]
fn seq_is_monotonic_across_different_streams() {
    // Two different event names land in two streams; their seq values give a
    // total per-node order the entry IDs cannot when they share a millisecond.
    let s = TestServer::start(&["events", "set,del", "entry-seq", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(Duration::from_secs(10), "both events mirrored", || {
        xlen(&mut c, "events:set") == 1 && xlen(&mut c, "events:del") == 1
    });

    let set_seq: u64 = first_field(&mut c, "events:set", "seq").parse().unwrap();
    let del_seq: u64 = first_field(&mut c, "events:del", "seq").parse().unwrap();
    assert!(
        set_seq < del_seq,
        "seq must strictly increase in notification order across streams: \
         set={set_seq} del={del_seq}"
    );
}

#[test]
fn seq_matches_across_a_per_event_and_firehose_pair() {
    // The per-event entry and its firehose copy represent one event, so they
    // carry the same seq (issue #66).
    let s = TestServer::start(&["events", "set", "entry-seq", "yes", "firehose", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "both copies written", || {
        info_field(&mut c, "forwarded") == 1 && info_field(&mut c, "firehose_forwarded") == 1
    });
    assert_eq!(
        first_field(&mut c, "events:set", "seq"),
        first_field(&mut c, "events:#firehose", "seq"),
        "one event, one seq, written to both streams"
    );
}

#[test]
fn seq_is_appended_after_db() {
    let s = TestServer::start(&["events", "set", "entry-seq", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(10), "set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert_eq!(
        entry_field_names(&mut c, "events:set"),
        vec!["event", "key", "db", "seq"],
        "seq is the trailing field on the fixed format"
    );
}

#[test]
fn entry_seq_is_immutable() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();
    let res: redis::RedisResult<()> = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.entry-seq")
        .arg("yes")
        .query(&mut c);
    assert!(
        res.is_err(),
        "entry-seq is IMMUTABLE and must reject a runtime CONFIG SET"
    );
}

#[test]
fn seq_resets_on_reload() {
    // The counter is process-lifetime: it resets to 0 on load, so entries
    // captured after a restart start numbering afresh (SPEC.md section 9). AOF
    // keeps the pre-restart entries (with their seq values written verbatim) so
    // the two runs are comparable.
    let s = TestServer::start_aof(&["events", "set", "entry-seq", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.set("b", "2").expect("SET");
    wait_until(Duration::from_secs(10), "two events mirrored", || {
        xlen(&mut c, "events:set") == 2
    });
    let pre: Vec<u64> = stream_field_strings(&mut c, "events:set", "seq")
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(pre, vec![0, 1], "seq starts at 0 and increments");

    let s = s.restart_aof(&["events", "set", "entry-seq", "yes"]);
    let mut c = s.conn();
    wait_until(Duration::from_secs(10), "aof reloaded", || {
        xlen(&mut c, "events:set") == 2
    });
    let _: () = c.set("cc", "3").expect("SET after restart");
    wait_until(Duration::from_secs(10), "third event mirrored", || {
        xlen(&mut c, "events:set") == 3
    });
    let post: Vec<u64> = stream_field_strings(&mut c, "events:set", "seq")
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(
        post,
        vec![0, 1, 0],
        "the post-restart entry restarts the sequence at 0"
    );
}
