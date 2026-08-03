//! End-to-end coverage for the shipped Rust consumer library. Unit tests cover
//! its parsers; these tests pin discovery, observability, cursor, marker, and
//! binary-key behavior against a live module.

mod common;

use common::*;
use eventstream_client::{
    counter_sum, discover, discover_all, discover_streams, node_counters, read_gap_markers,
    scan_streams, MergedReader, Target,
};
use redis::Commands;

#[test]
fn standalone_client_preserves_binary_entries_and_observability() {
    let server = TestServer::start(&["events", "set,del"]);
    let mut redis = server.conn();
    let key = vec![0xff, 0x00, b'a'];

    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("value")
        .query(&mut redis)
        .expect("SET binary key");
    let _: () = redis::cmd("DEL")
        .arg(&key)
        .query(&mut redis)
        .expect("DEL binary key");

    wait_until(CAPTURE_WAIT, "binary-key events captured", || {
        info_field(&mut redis, "forwarded") == 2
    });

    let url = format!("redis://127.0.0.1:{}/", server.port);
    let target = Target::detect(&url, "events:").expect("detect standalone target");
    assert!(!target.is_cluster);
    assert_eq!(target.masters, vec![format!("127.0.0.1:{}", server.port)]);

    assert_eq!(
        discover_streams(&target),
        vec!["events:del".to_string(), "events:set".to_string()]
    );
    let discovered = discover(&target);
    assert_eq!(discovered.len(), 2);
    assert!(discovered.iter().all(|stream| {
        stream.node == target.masters[0] && stream.tag.is_none() && stream.event.is_some()
    }));

    let all = discover_all(&target);
    assert!(all.contains(&"events:#control".to_string()));
    assert!(all.contains(&"events:set".to_string()));
    assert!(all.contains(&"events:del".to_string()));

    let counters = node_counters(&target.masters[0]);
    assert_eq!(counters.get("forwarded").map(String::as_str), Some("2"));
    assert_eq!(counter_sum(&target, "forwarded"), 2);

    let mut conn = target.open_rw().expect("open consumer connection");
    let mut reader = MergedReader::new(
        &mut conn,
        vec!["events:set".to_string(), "events:del".to_string()],
        true,
    );
    let entries = reader.poll(&mut conn, 100);
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.key == key));
    let mut events: Vec<&str> = entries.iter().map(|entry| entry.event.as_str()).collect();
    events.sort_unstable();
    assert_eq!(events, vec!["del", "set"]);
    assert!(reader.poll(&mut conn, 100).is_empty());

    let markers = read_gap_markers(&target, "0").expect("read control markers");
    let loaded = markers
        .iter()
        .find(|marker| marker.action == "loaded")
        .expect("loaded marker");
    assert_eq!(loaded.generation.as_deref().map(str::len), Some(32));
}

#[test]
fn client_parses_durable_control_checkpoint() {
    let server = TestServer::start(&["events", "set", "control-checkpoint-ms", "100"]);
    let mut redis = server.conn();
    let _: () = redis.set("checkpointed", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "checkpoint after capture", || {
        info_field(&mut redis, "control_checkpoints") >= 1
            && stream_field_strings(&mut redis, CONTROL, "forwarded")
                .last()
                .is_some_and(|value| value == "1")
    });

    let url = format!("redis://127.0.0.1:{}/", server.port);
    let target = Target::detect(&url, "events:").expect("detect target");
    let markers = read_gap_markers(&target, "0").expect("read control markers");
    let checkpoint = markers
        .iter()
        .rev()
        .find(|marker| marker.action == "checkpoint")
        .expect("checkpoint marker");
    assert_eq!(checkpoint.reason.as_deref(), Some("periodic"));
    assert_eq!(checkpoint.checkpoint_ms, Some(100));
    assert_eq!(checkpoint.resolved_events, Some(1));
    assert_eq!(checkpoint.forwarded, Some(1));
    assert_eq!(checkpoint.events_lost, Some(0));
    assert_eq!(checkpoint.async_queue_depth, Some(0));
    assert_eq!(checkpoint.generation.as_deref().map(str::len), Some(32));
}

#[test]
fn merged_reader_tails_and_adds_streams_without_replay() {
    let server = TestServer::start(&["events", "set,hset"]);
    let mut redis = server.conn();

    let _: () = redis::cmd("SET")
        .arg("before")
        .arg("value")
        .query(&mut redis)
        .expect("SET before reader");
    wait_until(CAPTURE_WAIT, "initial set captured", || {
        xlen(&mut redis, "events:set") == 1
    });

    let url = format!("redis://127.0.0.1:{}/", server.port);
    let target = Target::detect(&url, "events:").expect("detect standalone target");
    let mut conn = target.open_rw().expect("open consumer connection");
    let mut reader = MergedReader::new(&mut conn, vec!["events:set".to_string()], false);
    assert!(reader.poll(&mut conn, 100).is_empty());

    let _: () = redis::cmd("SET")
        .arg("after")
        .arg("value")
        .query(&mut redis)
        .expect("SET after reader");
    wait_until(CAPTURE_WAIT, "new set captured", || {
        xlen(&mut redis, "events:set") == 2
    });
    let entries = reader.poll(&mut conn, 100);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"after");

    let _: () = redis::cmd("HSET")
        .arg("profile")
        .arg("name")
        .arg("Ada")
        .query(&mut redis)
        .expect("HSET new event type");
    wait_until(CAPTURE_WAIT, "hset captured", || {
        xlen(&mut redis, "events:hset") == 1
    });

    let hset = "events:hset".to_string();
    assert_eq!(
        reader.add_streams(&mut conn, std::slice::from_ref(&hset), true),
        vec![hset.clone()]
    );
    assert!(reader
        .add_streams(&mut conn, std::slice::from_ref(&hset), true)
        .is_empty());
    let entries = reader.poll(&mut conn, 100);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].event, "hset");
    assert_eq!(entries[0].key, b"profile");
    assert!(reader.poll(&mut conn, 100).is_empty());
    assert_eq!(reader.streams().len(), 2);

    let scanned = scan_streams(&mut redis, "events:");
    assert!(scanned.contains(&"events:set".to_string()));
    assert!(scanned.contains(&"events:hset".to_string()));
}
