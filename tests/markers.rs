//! Gap-marker lifecycle (issues #12, #23, and #67): pending-marker delivery,
//! marker ordering, unload semantics, crash-gap detection, restart safety.
//! The shutdown tests pin the #67 finding: no closing marker is writable at
//! server shutdown, so clean restarts and crashes read identically.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

const CONTROL: &str = "events:#control";

fn marker_actions(conn: &mut redis::Connection) -> Vec<String> {
    stream_field_strings(conn, CONTROL, "action")
}

#[test]
fn loaded_marker_flushes_on_first_event_not_before() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let exists: i64 = redis::cmd("EXISTS").arg(CONTROL).query(&mut c).unwrap();
    assert_eq!(exists, 0, "no marker may be written before the first event");

    let _: () = c.set("x", "1").expect("SET");
    wait_until(Duration::from_secs(5), "loaded marker flushed", || {
        xlen(&mut c, CONTROL) > 0
    });
    assert_eq!(marker_actions(&mut c), vec!["loaded"]);

    // The marker job was enqueued before the entry job in the same
    // notification, so its entry ID must not be newer than the first entry.
    let marker_id = first_entry_id(&mut c, CONTROL);
    let entry_id = first_entry_id(&mut c, "events:set");
    assert!(
        marker_id <= entry_id,
        "marker {marker_id} must land before entry {entry_id}"
    );
}

fn first_entry_id(conn: &mut redis::Connection, key: &str) -> String {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(1)
        .query(conn)
        .expect("XRANGE");
    reply.ids.first().expect("entry").id.clone()
}

#[test]
fn disabled_enabled_pair() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(Duration::from_secs(5), "loaded marker", || {
        xlen(&mut c, CONTROL) == 1
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("no")
        .query(&mut c)
        .expect("disable");
    let _: () = c.set("b", "1").expect("SET while disabled");
    wait_until(Duration::from_secs(5), "disabled marker flushed", || {
        xlen(&mut c, CONTROL) == 2
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.enabled")
        .arg("yes")
        .query(&mut c)
        .expect("enable");
    let _: () = c.set("c", "1").expect("SET after enable");
    wait_until(Duration::from_secs(5), "enabled marker flushed", || {
        xlen(&mut c, CONTROL) == 3
    });

    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded", "disabled", "enabled"]
    );
    // Marker entries carry the module version.
    let versions = stream_field_strings(&mut c, CONTROL, "module-version");
    assert!(versions.iter().all(|v| !v.is_empty()));
}

#[test]
fn unloading_marker_on_module_unload() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();
    let _: () = c.set("x", "1").expect("SET");
    wait_until(Duration::from_secs(5), "loaded marker", || {
        xlen(&mut c, CONTROL) == 1
    });

    let _: () = redis::cmd("MODULE")
        .arg("UNLOAD")
        .arg("eventstream")
        .query(&mut c)
        .expect("MODULE UNLOAD");
    assert_eq!(marker_actions(&mut c), vec!["loaded", "unloading"]);
}

#[test]
fn unload_with_pending_markers_survives() {
    // Regression for the use-after-free: an idle load leaves the loaded
    // marker pending; MODULE UNLOAD must not create a post-notification job
    // that outlives the module. deinit flushes pending markers directly.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = redis::cmd("MODULE")
        .arg("UNLOAD")
        .arg("eventstream")
        .query(&mut c)
        .expect("MODULE UNLOAD with pending markers");

    let pong: String = redis::cmd("PING").query(&mut c).expect("server alive");
    assert_eq!(pong, "PONG");
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded", "unloading"],
        "pending loaded marker must be flushed by deinit, then unloading"
    );
}

#[test]
fn enabled_no_load_queues_loaded_then_disabled() {
    let s = TestServer::start(&["events", "set", "enabled", "no"]);
    let mut c = s.conn();

    let _: () = c.set("x", "1").expect("SET while disabled");
    wait_until(Duration::from_secs(5), "loaded+disabled flushed", || {
        xlen(&mut c, CONTROL) == 2
    });
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded", "disabled"],
        "a bare loaded marker must not close the gap while capture is off"
    );
    assert_eq!(xlen(&mut c, "events:set"), 0, "capture must be off");
}

#[test]
fn clean_shutdown_leaves_no_closing_marker_rdb() {
    // Pins the issue #67 finding, RDB path: no closing marker is writable at
    // clean shutdown. finishShutdown (server.c, verified at Redis 7.2.0 and
    // 8.0.0; Valkey inherits) orders the final AOF flush, then the final RDB
    // save, then the Shutdown module event, then the replica output-buffer
    // flush, so a marker written from the shutdown event can never reach the
    // persisted dataset — and a replicated write from that handler trips
    // propagateNow's shutdown-pause assertion when replicas are attached,
    // aborting the server. The module therefore writes nothing at shutdown,
    // and a clean restart reads exactly like a crash (SPEC.md section 9):
    // the pre-shutdown markers, then the post-restart loaded marker, with no
    // closing marker between them.
    let s = TestServer::start(&["events", "set"]);
    {
        let mut c = s.conn();
        let _: () = c.set("x", "1").expect("SET");
        wait_until(Duration::from_secs(5), "loaded marker", || {
            xlen(&mut c, CONTROL) == 1
        });
        // SAVE forces the final RDB even with save points disabled, making
        // the persistence path deterministic.
        let _ = redis::cmd("SHUTDOWN").arg("SAVE").query::<()>(&mut c);
    }

    let s = s.restart(&["events", "set"]);
    let mut c = s.conn();
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded"],
        "no closing marker may follow the pre-shutdown markers"
    );

    let _: () = c.set("y", "1").expect("SET after restart");
    wait_until(Duration::from_secs(5), "second loaded marker", || {
        xlen(&mut c, CONTROL) == 2
    });
    assert_eq!(marker_actions(&mut c), vec!["loaded", "loaded"]);
}

#[test]
fn clean_shutdown_leaves_no_closing_marker_aof() {
    // The AOF side of the #67 finding (full citation on the RDB variant
    // above): finishShutdown flushes and fsyncs the AOF before firing the
    // Shutdown event and the process exits without flushing again, so a
    // shutdown-time XADD would die in the AOF buffer. Same shape as the RDB
    // variant: no closing marker between the two loaded markers.
    let s = TestServer::start_aof(&["events", "set"]);
    {
        let mut c = s.conn();
        let _: () = c.set("x", "1").expect("SET");
        wait_until(Duration::from_secs(5), "loaded marker", || {
            xlen(&mut c, CONTROL) == 1
        });
        // Plain SHUTDOWN: no save points and no SAVE flag, so durability is
        // the AOF's alone.
        let _ = redis::cmd("SHUTDOWN").query::<()>(&mut c);
    }

    let s = s.restart_aof(&["events", "set"]);
    let mut c = s.conn();
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded"],
        "no closing marker may follow the pre-shutdown markers"
    );

    let _: () = c.set("y", "1").expect("SET after restart");
    wait_until(Duration::from_secs(5), "second loaded marker", || {
        xlen(&mut c, CONTROL) == 2
    });
    assert_eq!(marker_actions(&mut c), vec!["loaded", "loaded"]);
}

#[test]
fn crash_leaves_no_closing_marker() {
    // The crash side of the #67 finding: SIGKILL never runs the server's
    // shutdown path, so nothing could write a closing marker even if clean
    // shutdown had one. Deliberately the same shape as the clean-shutdown
    // tests above — SPEC.md section 9 documents that the two are permanently
    // indistinguishable from the node's own control stream.
    let s = TestServer::start_aof(&["events", "set"]);
    {
        let mut c = s.conn();
        let _: () = c.set("x", "1").expect("SET");
        // Once XLEN observes the marker, a later event-loop iteration has
        // already written the AOF buffer to the page cache, which survives a
        // process kill (only an OS crash would need the fsync).
        wait_until(Duration::from_secs(5), "loaded marker", || {
            xlen(&mut c, CONTROL) == 1
        });
    }
    s.kill9();

    let s = s.restart_aof(&["events", "set"]);
    let mut c = s.conn();
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded"],
        "a crash must leave no closing marker"
    );

    let _: () = c.set("y", "1").expect("SET after restart");
    wait_until(Duration::from_secs(5), "second loaded marker", || {
        xlen(&mut c, CONTROL) == 2
    });
    assert_eq!(marker_actions(&mut c), vec!["loaded", "loaded"]);
}

#[test]
fn restart_reads_as_gap_and_persisted_control_stream_is_safe() {
    // Restart safety: a persisted control stream must not break startup
    // (this pins the no-direct-write-in-init rule), and any restart reads as
    // a gap: a loaded marker with no closing marker before it.
    let s = TestServer::start(&["events", "set"]);
    {
        let mut c = s.conn();
        let _: () = c.set("x", "1").expect("SET");
        wait_until(Duration::from_secs(5), "loaded marker", || {
            xlen(&mut c, CONTROL) == 1
        });
        let _: () = redis::cmd("SAVE").query(&mut c).expect("SAVE");
        // SHUTDOWN NOSAVE drops the connection; both clean shutdown and
        // crash write no marker, so this stands in for either.
        let _ = redis::cmd("SHUTDOWN").arg("NOSAVE").query::<()>(&mut c);
    }

    let s = s.restart(&["events", "set"]);
    let mut c = s.conn();
    let pong: String = redis::cmd("PING").query(&mut c).expect("restart survives");
    assert_eq!(pong, "PONG");
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded"],
        "persisted control stream must reload intact"
    );

    let _: () = c.set("y", "1").expect("SET after restart");
    wait_until(Duration::from_secs(5), "second loaded marker", || {
        xlen(&mut c, CONTROL) == 2
    });
    let actions = marker_actions(&mut c);
    assert_eq!(actions, vec!["loaded", "loaded"]);
    // Gap detection rule: newest marker is loaded and the one before it is
    // neither unloading nor disabled, so the window between them is a gap.
    assert_ne!(actions[0], "unloading");
}
