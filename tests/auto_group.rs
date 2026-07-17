//! Consumer-group auto-provisioning (issue #109): the opt-in
//! `eventstream.auto-group` config that makes the module `XGROUP CREATE
//! <stream> <name> 0` on each destination stream at first write, so consumers
//! can `XREADGROUP` without a manual setup step and deployment ordering between
//! the module and its consumers stops mattering.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

/// The consumer-group names present on a stream, via `XINFO GROUPS`. A missing
/// key or a stream with no groups yields an empty vec. Parses the RESP2 flat
/// field/value layout each group element carries.
fn group_names(conn: &mut redis::Connection, key: &str) -> Vec<String> {
    let as_string = |v: &redis::Value| -> Option<String> {
        match v {
            redis::Value::SimpleString(s) => Some(s.clone()),
            redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        }
    };
    let reply: redis::Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(key)
        .query(conn)
        .unwrap_or(redis::Value::Array(vec![]));
    let groups = match reply {
        redis::Value::Array(g) => g,
        _ => vec![],
    };
    let mut names = Vec::new();
    for g in groups {
        if let redis::Value::Array(fields) = g {
            let mut it = fields.iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                if as_string(k).as_deref() == Some("name") {
                    if let Some(s) = as_string(v) {
                        names.push(s);
                    }
                }
            }
        }
    }
    names
}

#[test]
fn auto_group_off_by_default() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let pair: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("eventstream.auto-group")
        .query(&mut c)
        .expect("CONFIG GET");
    assert_eq!(pair[1], "", "auto-group must default to empty (disabled)");

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "set mirrored", || {
        info_field(&mut c, "forwarded") == 1
    });
    assert!(
        group_names(&mut c, "events:set").is_empty(),
        "no consumer group may be created while auto-group is off"
    );
    assert_eq!(info_field(&mut c, "autogroup_created"), 0);
}

#[test]
fn auto_group_creates_group_on_each_written_stream() {
    // Enabled as an unprefixed module arg; each destination stream gets the
    // named group, created at 0 so a fresh consumer reading ">" sees the head.
    let s = TestServer::start(&["events", "set,del", "auto-group", "workers"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "both events mirrored", || {
        info_field(&mut c, "forwarded") == 2
    });

    assert_eq!(group_names(&mut c, "events:set"), vec!["workers"]);
    assert_eq!(group_names(&mut c, "events:del"), vec!["workers"]);
    assert_eq!(
        info_field(&mut c, "autogroup_created"),
        2,
        "one group per distinct destination stream"
    );
    assert_eq!(info_field(&mut c, "autogroup_failed"), 0);

    // The group was created at 0, so a consumer reading ">" receives the entry
    // written before it ever connected — the ordering-independence guarantee.
    let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("workers")
        .arg("c1")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg("events:set")
        .arg(">")
        .query(&mut c)
        .expect("XREADGROUP");
    let count: usize = reply.keys.iter().map(|k| k.ids.len()).sum();
    assert_eq!(count, 1, "the group at 0 delivers the retained head entry");
}

#[test]
fn auto_group_is_idempotent_across_writes() {
    // Many writes to the same stream must create the group exactly once and
    // never error with BUSYGROUP.
    let s = TestServer::start(&["events", "set", "auto-group", "workers"]);
    let mut c = s.conn();

    for i in 0..50 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(CAPTURE_WAIT, "50 events mirrored", || {
        info_field(&mut c, "forwarded") == 50
    });

    assert_eq!(group_names(&mut c, "events:set"), vec!["workers"]);
    assert_eq!(
        info_field(&mut c, "autogroup_created"),
        1,
        "the group is created once per stream, deduped like the registry SADD"
    );
    assert_eq!(info_field(&mut c, "autogroup_failed"), 0);
}

#[test]
fn auto_group_covers_the_firehose() {
    // The firehose is a destination stream too (issue #58), so it gets the
    // group just like the per-event streams.
    let s = TestServer::start(&["events", "set", "firehose", "yes", "auto-group", "workers"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "firehose copy written", || {
        info_field(&mut c, "firehose_forwarded") == 1
    });

    assert_eq!(group_names(&mut c, "events:set"), vec!["workers"]);
    assert_eq!(group_names(&mut c, "events:#firehose"), vec!["workers"]);
}

#[test]
fn auto_group_excludes_the_control_stream() {
    // The control stream is not a work queue; markers write through a separate
    // path, so it never gets the group. A disable/enable toggle guarantees a
    // marker lands on events:#control.
    let s = TestServer::start(&["events", "set", "auto-group", "workers"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("no")
        .query(&mut c)
        .expect("disable");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("yes")
        .query(&mut c)
        .expect("re-enable");
    let _: () = c.set("b", "1").expect("SET flushes the pending markers");
    wait_until(CAPTURE_WAIT, "control markers written", || {
        xlen(&mut c, CONTROL) > 0
    });

    assert_eq!(group_names(&mut c, "events:set"), vec!["workers"]);
    assert!(
        group_names(&mut c, CONTROL).is_empty(),
        "the control stream must never get an auto-provisioned group"
    );
}

#[test]
fn auto_group_provisions_warm_streams_on_next_write_after_config_set() {
    // Module-before-config order: a stream exists without a group, then the
    // operator sets auto-group at runtime; the group appears on the stream's
    // next write, not retroactively and not only after a flush.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("one", "v").expect("SET");
    wait_until(CAPTURE_WAIT, "first set mirrored", || {
        xlen(&mut c, "events:set") == 1
    });
    assert!(
        group_names(&mut c, "events:set").is_empty(),
        "no group while auto-group is empty"
    );

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.auto-group")
        .arg("workers")
        .query(&mut c)
        .expect("enable auto-group");
    let _: () = c.set("two", "v").expect("SET after enable");
    wait_until(CAPTURE_WAIT, "group provisioned on next write", || {
        group_names(&mut c, "events:set") == vec!["workers".to_string()]
    });
    assert_eq!(info_field(&mut c, "autogroup_created"), 1);
}

#[test]
fn auto_group_recreates_group_after_flushall() {
    // A FLUSHALL destroys the stream and its group; the next write re-registers
    // the stream and re-creates the group (the dedupe cache is invalidated on
    // flush, so provisioning is cold again).
    let s = TestServer::start(&["events", "set", "auto-group", "workers"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "group created", || {
        group_names(&mut c, "events:set") == vec!["workers".to_string()]
    });

    let _: () = redis::cmd("FLUSHALL").query(&mut c).expect("FLUSHALL");
    assert!(group_names(&mut c, "events:set").is_empty());

    let _: () = c.set("b", "1").expect("SET after flush");
    wait_until(CAPTURE_WAIT, "group re-created", || {
        group_names(&mut c, "events:set") == vec!["workers".to_string()]
    });
    assert_eq!(
        info_field(&mut c, "autogroup_created"),
        2,
        "one creation before the flush and one after"
    );
}

#[test]
fn auto_group_replicates_to_replica() {
    // The group is created with the same replicated call options as the
    // mirrored XADD, so a replica shows it too (SPEC.md section 10).
    let master = TestServer::start(&["events", "set", "auto-group", "workers"]);
    let mut mc = master.conn();
    let _: () = mc.set("a", "1").expect("SET on master");
    wait_until(CAPTURE_WAIT, "group on master", || {
        group_names(&mut mc, "events:set") == vec!["workers".to_string()]
    });

    let replica = TestServer::start_replica_of(&master, &["events", "set"]);
    let mut rc = replica.conn();
    wait_until(Duration::from_secs(10), "group replicated", || {
        group_names(&mut rc, "events:set") == vec!["workers".to_string()]
    });
}
