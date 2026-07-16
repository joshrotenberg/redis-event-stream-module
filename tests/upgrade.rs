//! In-place module upgrade (issue #107): UNLOAD followed by LOAD on the same
//! live server, without a restart. Pins the full swap path that no other test
//! exercises — the marker tests stop at UNLOAD, the restart tests replace the
//! whole process. The same `.so` stands in for "a new version"; the swap
//! mechanics, not a binary diff, are what this pins. See docs/upgrading.md for
//! the operator runbook this test guards.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

#[test]
fn in_place_unload_load_swap() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    // --- Before the swap: capture some history and register a stream. ---
    let _: () = c.set("before", "1").expect("SET before");
    wait_until(Duration::from_secs(5), "pre-upgrade capture", || {
        info_field(&mut c, "forwarded") == 1
    });
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

    // --- Load the same .so again with the same args (a real upgrade points at
    // a new path; the mechanics are identical). ---
    let _: () = redis::cmd("MODULE")
        .arg("LOAD")
        .arg(module_path().to_str().expect("module path is utf-8"))
        .arg("events")
        .arg("set")
        .query(&mut c)
        .expect("MODULE LOAD");

    // Counters are process-lifetime statics that reset when the module image is
    // actually unloaded (SPEC.md section 13). Redis unloads the image on Linux
    // (the CI platform), so the counter is back to zero there. macOS dlclose
    // does not truly unload a dylib, so the statics survive an in-process
    // UNLOAD/LOAD locally — assert the reset only where the platform unloads,
    // and assert the universal property (capture resumes) everywhere.
    let forwarded_after_load = info_field(&mut c, "forwarded");
    if cfg!(target_os = "linux") {
        assert_eq!(
            forwarded_after_load, 0,
            "counters must reset to zero when the module image is unloaded"
        );
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

    // --- After the swap: capture resumes. ---
    let _: () = c.set("after", "1").expect("SET after");
    wait_until(
        Duration::from_secs(5),
        "post-upgrade capture resumes",
        || info_field(&mut c, "forwarded") > forwarded_after_load,
    );

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
