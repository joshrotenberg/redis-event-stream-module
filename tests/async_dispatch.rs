//! Experimental bounded writer correctness and lifecycle (issue #265).

mod common;

use common::*;
use std::time::{Duration, Instant};

const EVENT_COUNT: i64 = 2_000;

fn set_pipeline(conn: &mut redis::Connection, count: i64) {
    let mut pipe = redis::pipe();
    for i in 0..count {
        pipe.cmd("SET").arg(format!("async:{i}")).arg(i).ignore();
    }
    let _: () = pipe.query(conn).expect("pipeline SET");
}

fn assert_lossless(conn: &mut redis::Connection, expected: i64) {
    wait_until(Duration::from_secs(15), "all events forwarded", || {
        info_field(conn, "forwarded") == expected
    });
    assert_eq!(xlen(conn, "events:set"), expected);
    assert_eq!(info_field(conn, "events_lost"), 0);
    assert_eq!(info_field(conn, "dropped"), 0);
    assert_eq!(info_field(conn, "handler_panics"), 0);
    assert_eq!(info_field(conn, "async_worker_errors"), 0);
}

fn assert_pipeline_order(conn: &mut redis::Connection, expected: i64) {
    let keys = stream_field_strings(conn, "events:set", "key");
    assert_eq!(keys.len(), expected as usize);
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(key, &format!("async:{i}"), "event order changed at {i}");
    }
}

#[test]
fn individual_mode_preserves_every_logical_event() {
    let s = TestServer::start(&["events", "set", "maxlen", "0", "write-mode", "individual"]);
    let mut c = s.conn();

    set_pipeline(&mut c, EVENT_COUNT);
    assert_lossless(&mut c, EVENT_COUNT);
    assert_pipeline_order(&mut c, EVENT_COUNT);

    let enqueued = info_field(&mut c, "async_enqueued");
    let fallbacks = info_field(&mut c, "async_fallbacks");
    assert_eq!(enqueued + fallbacks, EVENT_COUNT);
    assert_eq!(info_field(&mut c, "async_drain_events"), enqueued);
    assert!(info_field(&mut c, "async_drains") > 0);
    assert_eq!(info_field(&mut c, "async_queue_depth"), 0);
}

#[test]
fn full_queue_falls_back_without_losing_or_reordering_events() {
    let s = TestServer::start(&[
        "events",
        "set",
        "maxlen",
        "0",
        "write-mode",
        "individual",
        "async-queue-capacity",
        "1",
        "async-batch-size",
        "64",
        "async-max-wait-ms",
        "1000",
    ]);
    let mut c = s.conn();

    set_pipeline(&mut c, EVENT_COUNT);
    assert_lossless(&mut c, EVENT_COUNT);
    assert_pipeline_order(&mut c, EVENT_COUNT);

    let enqueued = info_field(&mut c, "async_enqueued");
    let fallbacks = info_field(&mut c, "async_fallbacks");
    assert!(enqueued > 0, "the worker should accept at least one event");
    assert!(
        fallbacks > 0,
        "capacity one under a pipeline should exercise the fallback"
    );
    assert_eq!(enqueued + fallbacks, EVENT_COUNT);
    assert!(info_field(&mut c, "async_queue_high_water") <= 1);
}

#[test]
fn unload_drains_accepted_backlog_without_waiting_for_batch_deadline() {
    let s = TestServer::start(&[
        "events",
        "set",
        "maxlen",
        "0",
        "write-mode",
        "individual",
        "async-queue-capacity",
        "4096",
        "async-batch-size",
        "4096",
        "async-max-wait-ms",
        "60000",
    ]);
    let mut c = s.conn();

    set_pipeline(&mut c, EVENT_COUNT);
    let started = Instant::now();
    let _: () = redis::cmd("MODULE")
        .arg("UNLOAD")
        .arg("eventstream")
        .query(&mut c)
        .expect("MODULE UNLOAD with async backlog");

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "unload should interrupt the batch wait, not wait for 60 seconds"
    );
    assert_eq!(
        xlen(&mut c, "events:set"),
        EVENT_COUNT,
        "deinit must synchronously finish every accepted event"
    );
    let pong: String = redis::cmd("PING").query(&mut c).expect("server alive");
    assert_eq!(pong, "PONG");
}
