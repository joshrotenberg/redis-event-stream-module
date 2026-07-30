//! Experimental bounded writer correctness and lifecycle (issue #265).

mod common;

use common::*;
use redis::Commands;
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
    assert_eq!(info_field(conn, "events_lost"), 0);
    assert_eq!(info_field(conn, "dropped"), 0);
    assert_eq!(info_field(conn, "handler_panics"), 0);
    assert_eq!(info_field(conn, "async_worker_errors"), 0);
}

fn value_bytes(value: &redis::Value) -> &[u8] {
    match value {
        redis::Value::BulkString(bytes) => bytes,
        other => panic!("expected bulk string, got {other:?}"),
    }
}

fn decode_base64(input: &str) -> Vec<u8> {
    let sextet = |byte: u8| -> u32 {
        match byte {
            b'A'..=b'Z' => (byte - b'A') as u32,
            b'a'..=b'z' => (byte - b'a' + 26) as u32,
            b'0'..=b'9' => (byte - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => 0,
        }
    };
    let mut out = Vec::new();
    for chunk in input.as_bytes().chunks(4) {
        let n = (sextet(chunk[0]) << 18)
            | (sextet(chunk[1]) << 12)
            | (sextet(*chunk.get(2).unwrap_or(&b'=')) << 6)
            | sextet(*chunk.get(3).unwrap_or(&b'='));
        out.push((n >> 16) as u8);
        if chunk.get(2) != Some(&b'=') {
            out.push((n >> 8) as u8);
        }
        if chunk.get(3) != Some(&b'=') {
            out.push(n as u8);
        }
    }
    out
}

fn expanded_keys(conn: &mut redis::Connection) -> (Vec<String>, usize, i64) {
    let reply: redis::streams::StreamRangeReply = conn.xrange_all("events:set").expect("XRANGE");
    let mut keys = Vec::new();
    let mut envelopes = 0;
    let mut logical_in_envelopes = 0;
    for entry in reply.ids {
        if let Some(key) = entry.map.get("key") {
            keys.push(String::from_utf8_lossy(value_bytes(key)).into_owned());
            continue;
        }
        let data = String::from_utf8_lossy(value_bytes(
            entry.map.get("events").expect("fixed or batch-v1 entry"),
        ));
        let count =
            String::from_utf8_lossy(value_bytes(entry.map.get("count").expect("batch-v1 count")))
                .parse::<i64>()
                .expect("numeric batch count");
        envelopes += 1;
        logical_in_envelopes += count;
        let mut rest = data.as_ref();
        let mut decoded = 0;
        while let Some(start) = rest.find("\"key\":\"") {
            rest = &rest[start + 7..];
            let end = rest.find('"').expect("closing key quote");
            keys.push(String::from_utf8_lossy(&decode_base64(&rest[..end])).into_owned());
            decoded += 1;
            rest = &rest[end + 1..];
        }
        assert_eq!(decoded, count, "batch count must match its event array");
    }
    (keys, envelopes, logical_in_envelopes)
}

fn assert_pipeline_order(conn: &mut redis::Connection, expected: i64) {
    let (keys, _, _) = expanded_keys(conn);
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
    assert_eq!(xlen(&mut c, "events:set"), EVENT_COUNT);
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
    assert_eq!(xlen(&mut c, "events:set"), EVENT_COUNT);
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
fn envelope_mode_reduces_xadds_but_preserves_logical_order_and_accounting() {
    let s = TestServer::start(&[
        "events",
        "set",
        "maxlen",
        "0",
        "write-mode",
        "envelope",
        "async-queue-capacity",
        "4096",
        "async-batch-size",
        "64",
        "async-max-wait-ms",
        "5",
    ]);
    let mut c = s.conn();

    set_pipeline(&mut c, EVENT_COUNT);
    assert_lossless(&mut c, EVENT_COUNT);
    let (keys, envelopes, logical_in_envelopes) = expanded_keys(&mut c);
    assert_eq!(keys.len(), EVENT_COUNT as usize);
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(key, &format!("async:{i}"), "event order changed at {i}");
    }
    assert!(
        envelopes > 0,
        "at least one batch-v1 entry should be written"
    );
    assert_eq!(info_field(&mut c, "async_envelopes"), envelopes as i64);
    assert!(logical_in_envelopes > envelopes as i64);
    assert!(
        xlen(&mut c, "events:set") < EVENT_COUNT,
        "batching should reduce destination XADDs"
    );
    assert_eq!(
        info_field(&mut c, "async_enqueued") + info_field(&mut c, "async_fallbacks"),
        EVENT_COUNT
    );
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

#[test]
fn async_modes_reject_unsupported_wire_and_topology_combinations() {
    let firehose = TestServer::try_start(&["write-mode", "individual", "firehose", "yes"])
        .err()
        .expect("firehose combination must refuse load");
    assert!(firehose.contains("require firehose no"), "{firehose}");

    let format = TestServer::try_start(&["write-mode", "envelope", "entry-format", "json"])
        .err()
        .expect("entry-format combination must refuse load");
    assert!(format.contains("require entry-format fixed"), "{format}");

    let cluster = TestServer::try_start_cluster_enabled(&[
        "cluster-streams",
        "per-node",
        "write-mode",
        "individual",
    ])
    .err()
    .expect("cluster combination must refuse load");
    assert!(cluster.contains("do not support cluster mode"), "{cluster}");
}
