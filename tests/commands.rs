//! EVENTSTREAM.STATS and EVENTSTREAM.STREAMS, and the persistent stream
//! registry that backs discovery (issue #21).

mod common;

use common::*;
use redis::Commands;
use std::collections::HashMap;
use std::time::Duration;

/// Parse the flat `[name, value, ...]` reply of EVENTSTREAM.STATS into a map.
fn stats(conn: &mut redis::Connection) -> HashMap<String, i64> {
    let flat: Vec<redis::Value> = redis::cmd("EVENTSTREAM.STATS").query(conn).expect("STATS");
    let mut m = HashMap::new();
    let mut i = 0;
    while i + 1 < flat.len() {
        let name = match &flat[i] {
            redis::Value::SimpleString(s) => s.clone(),
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("unexpected stats name: {other:?}"),
        };
        let val = match &flat[i + 1] {
            redis::Value::Int(n) => *n,
            other => panic!("unexpected stats value: {other:?}"),
        };
        m.insert(name, val);
        i += 2;
    }
    m
}

fn streams(conn: &mut redis::Connection) -> Vec<String> {
    let mut v: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(conn)
        .expect("STREAMS");
    v.sort();
    v
}

#[test]
fn stats_agrees_with_info() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(5), "one forwarded", || {
        info_field(&mut c, "forwarded") == 1
    });

    let st = stats(&mut c);
    for f in [
        "enabled",
        "forwarded",
        "firehose_forwarded",
        "dropped",
        "skipped_self",
        "active_streams",
        "control_markers",
    ] {
        assert_eq!(
            st[f],
            info_field(&mut c, f),
            "STATS.{f} must equal INFO.{f}"
        );
    }
    assert_eq!(st["forwarded"], 1);
    assert_eq!(st["dropped"], 0);
}

#[test]
fn streams_lists_registered_streams() {
    let s = TestServer::start(&["events", "set,del"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(Duration::from_secs(5), "two streams registered", || {
        streams(&mut c).len() == 2
    });
    assert_eq!(streams(&mut c), vec!["events:del", "events:set"]);

    // The registry set is itself under the prefix, so its writes are never
    // mirrored back (the feedback guard drops them).
    assert!(info_field(&mut c, "skipped_self") > 0);
}

#[test]
fn streams_reads_db0_from_any_client_db() {
    let s = TestServer::start(&["events", "set"]);
    let mut c0 = s.conn();
    let _: () = c0.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(5), "stream registered", || {
        !streams(&mut c0).is_empty()
    });

    // A client on db 3 still sees the db-0 registry.
    let mut c3 = s.conn_db(3);
    assert_eq!(streams(&mut c3), vec!["events:set"]);
    // Confirm the command did not leave the connection on db 0: a key set on
    // c3 must live in db 3, invisible to a fresh db-0 connection.
    let _: () = c3.set("db3only", "x").expect("SET on db3");
    let exists0: i64 = redis::cmd("EXISTS")
        .arg("db3only")
        .query(&mut s.conn())
        .expect("EXISTS");
    assert_eq!(exists0, 0, "STREAMS must restore the caller's database");
}

#[test]
fn registry_survives_restart_under_aof() {
    let s = TestServer::start_aof(&["events", "set,del"]);
    {
        let mut c = s.conn();
        let _: () = c.set("a", "1").expect("SET");
        let _: () = c.del("a").expect("DEL");
        wait_until(Duration::from_secs(5), "two streams", || {
            streams(&mut c).len() == 2
        });
        let _ = redis::cmd("SHUTDOWN").arg("NOSAVE").query::<()>(&mut c);
    }

    let s = s.restart_aof(&["events", "set,del"]);
    let mut c = s.conn();
    assert_eq!(
        streams(&mut c),
        vec!["events:del", "events:set"],
        "registry must replay from the AOF"
    );
}

#[test]
fn registry_rebuilds_after_flushall() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(5), "registered", || {
        !streams(&mut c).is_empty()
    });

    let _: () = redis::cmd("FLUSHALL").query(&mut c).expect("FLUSHALL");
    assert!(
        streams(&mut c).is_empty(),
        "FLUSHALL deletes the registry set"
    );

    // The flush handler cleared the dedupe cache, so the next capture
    // re-registers its stream.
    let _: () = c.set("b", "2").expect("SET after flush");
    wait_until(Duration::from_secs(5), "registry rebuilt", || {
        streams(&mut c) == vec!["events:set"]
    });
}
