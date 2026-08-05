//! In-place module upgrade (issue #107): UNLOAD followed by LOAD on the same
//! live server, without a restart. Pins the full swap path that no other test
//! exercises — the marker tests stop at UNLOAD, the restart tests replace the
//! whole process. The same `.so` stands in for "a new version"; the swap
//! mechanics, not a binary diff, are what this pins. See docs/src/upgrading.md for
//! the operator runbook this test guards.

mod common;

use common::*;
use redis::Commands;

#[test]
fn in_place_unload_load_swap() {
    let s = TestServer::start(&["events", "set,del", "max-streams", "1"]);
    let mut c = s.conn();

    // --- Before the swap: capture some history and register a stream. ---
    let _: () = c.set("before", "1").expect("SET before");
    wait_until(CAPTURE_WAIT, "pre-upgrade capture", || {
        info_field(&mut c, "forwarded") == 1
    });
    let _: i64 = c.del("before").expect("DEL before");
    wait_until(CAPTURE_WAIT, "pre-upgrade max-streams drop", || {
        info_field(&mut c, "events_lost") == 1
    });
    let before = stats_map(&mut c);
    assert_eq!(before["forwarded"], "1");
    assert_eq!(before["events_lost"], "1");
    assert_eq!(before["dropped_max_streams"], "1");
    assert_ne!(before["skipped_self"], "0");
    assert_eq!(marker_actions(&mut c), vec!["loaded"]);
    assert!(
        redis::cmd("EVENTSTREAM.STREAMS")
            .query::<Vec<String>>(&mut c)
            .expect("EVENTSTREAM.STREAMS")
            .contains(&"events:set".to_string()),
        "events:set must be registered before the upgrade"
    );

    // --- Unload. deinit writes the `unloading` marker directly. ---
    let _: () = redis::cmd("MODULE")
        .arg("UNLOAD")
        .arg("eventstream")
        .query(&mut c)
        .expect("MODULE UNLOAD");
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded", "unloading"],
        "unload must append the unloading marker"
    );

    // --- The loss window: an event fired while the module is unloaded is not
    // captured and is not recoverable (SPEC.md sections 9, 12). ---
    let _: () = c.set("during_gap", "1").expect("SET during the gap");

    // --- Load the same .so again (a real upgrade points at a new path; the
    // lifecycle mechanics are identical). ---
    let _: () = redis::cmd("MODULE")
        .arg("LOAD")
        .arg(module_path().to_str().expect("module path is utf-8"))
        .arg("events")
        // Widen the extra-class subscription too: the previous generation's
        // fixed subscription mask must not make this load-time setter look
        // like a forbidden runtime CONFIG SET (#291).
        .arg("set,del,@missed")
        .arg("max-streams")
        .arg("1")
        .query(&mut c)
        .expect("MODULE LOAD");

    // Every observable value belongs to the new loaded generation even when
    // the platform retains the dylib and its statics across dlclose (#291).
    let after = stats_map(&mut c);
    for (name, value) in &after {
        match name.as_str() {
            "enabled" => assert_eq!(value, "1"),
            "cluster_pinned_tag" => assert!(value.is_empty()),
            _ => assert_eq!(value, "0", "{name} leaked across module reload"),
        }
    }
    // The registry set is ordinary keyspace and survives the swap, so
    // discovery is continuous across it.
    assert!(
        redis::cmd("EVENTSTREAM.STREAMS")
            .query::<Vec<String>>(&mut c)
            .expect("EVENTSTREAM.STREAMS after load")
            .contains(&"events:set".to_string()),
        "the registry must survive the swap"
    );
    assert_eq!(
        streams_withstats(&mut c).get("events:set"),
        Some(&(0, 0)),
        "persisted discovery must not retain per-generation stream counters"
    );

    // --- After the swap: capture resumes. ---
    let _: () = c.set("after", "1").expect("SET after");
    wait_until(CAPTURE_WAIT, "post-upgrade capture resumes", || {
        info_field(&mut c, "forwarded") == 1
    });

    // The control stream now shows the full pair: the pre-upgrade loaded, the
    // unloading, and the post-upgrade loaded — the machine-readable window.
    assert_eq!(
        marker_actions(&mut c),
        vec!["loaded", "unloading", "loaded"],
        "the upgrade must leave an unloading/loaded pair bounding the gap"
    );
    // Every marker carries a module-version (the field an operator diffs to
    // confirm the swap).
    let versions = stream_field_strings(&mut c, CONTROL, "module-version");
    assert_eq!(versions.len(), 3);
    assert!(
        versions.iter().all(|v| !v.is_empty()),
        "every marker must carry module-version"
    );
    let generations = stream_field_strings(&mut c, CONTROL, "generation");
    assert_eq!(generations.len(), 3);
    assert_eq!(
        generations[0], generations[1],
        "loaded and unloading must close one generation"
    );
    assert_ne!(
        generations[1], generations[2],
        "the reloaded module must open a new generation"
    );

    // The loss window is real and bounded: the gap event never made it into a
    // stream, while the before/after events did.
    let captured = stream_field_strings(&mut c, "events:set", "key");
    assert!(
        captured.contains(&"before".to_string()) && captured.contains(&"after".to_string()),
        "events on both sides of the swap are captured"
    );
    assert!(
        !captured.contains(&"during_gap".to_string()),
        "the event fired while unloaded must be absent (loss window)"
    );
}

#[test]
fn async_dispatch_can_unload_and_reload_in_process() {
    let args = [
        "events",
        "set",
        "write-mode",
        "individual",
        "async-batch-size",
        "8",
        "async-max-wait-ms",
        "1",
    ];
    let s = TestServer::start(&args);
    let mut c = s.conn();

    for i in 0..64 {
        let _: () = c.set(format!("before:{i}"), i).expect("pre-reload SET");
    }
    wait_until(CAPTURE_WAIT, "async pre-reload drain", || {
        info_field(&mut c, "forwarded") == 64
    });
    assert!(info_field(&mut c, "async_enqueued") > 0);

    let _: () = redis::cmd("MODULE")
        .arg("UNLOAD")
        .arg("eventstream")
        .query(&mut c)
        .expect("MODULE UNLOAD");
    let mut load = redis::cmd("MODULE");
    load.arg("LOAD")
        .arg(module_path().to_str().expect("module path is utf-8"));
    for arg in args {
        load.arg(arg);
    }
    let _: () = load.query(&mut c).expect("MODULE LOAD in async mode");

    let after = stats_map(&mut c);
    for name in [
        "forwarded",
        "async_enqueued",
        "async_fallbacks",
        "async_queue_depth",
        "async_queue_high_water",
        "async_drains",
        "async_drain_events",
        "async_worker_errors",
    ] {
        assert_eq!(after[name], "0", "{name} leaked across async reload");
    }

    let _: () = c.set("after", "1").expect("post-reload SET");
    wait_until(CAPTURE_WAIT, "async capture resumes after reload", || {
        info_field(&mut c, "forwarded") == 1
    });
}
