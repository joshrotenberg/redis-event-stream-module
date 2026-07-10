//! INFO section and counters (issues #10 and #12), plus the storm and OOM
//! loss-window behaviors from the section 15 test plan.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

#[test]
fn info_section_has_all_fields_and_stats_command_is_gone() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    for f in [
        "enabled",
        "forwarded",
        "dropped",
        "dropped_xadd_error",
        "dropped_oom",
        "dropped_defer_error",
        "skipped_self",
        "skipped_filtered",
        "skipped_invalid",
        "active_streams",
        "control_markers",
        "last_error_time",
    ] {
        let _ = info_field(&mut c, f); // panics if missing
    }

    let res: Result<redis::Value, _> = redis::cmd("EVENTSTREAM.STATS").query(&mut c);
    assert!(res.is_err(), "the stats command must not exist");
}

#[test]
fn counters_track_capture_activity() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    assert_eq!(info_field(&mut c, "forwarded"), 0);

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL"); // filtered out
    wait_until(Duration::from_secs(5), "forwarded counts the set", || {
        info_field(&mut c, "forwarded") == 1
    });
    assert!(
        info_field(&mut c, "skipped_filtered") >= 1,
        "the del must count as filtered"
    );
    assert_eq!(info_field(&mut c, "active_streams"), 1);
    assert_eq!(
        info_field(&mut c, "control_markers"),
        1,
        "the loaded marker counts separately"
    );
    assert_eq!(info_field(&mut c, "enabled"), 1);
    assert_eq!(info_field(&mut c, "dropped"), 0);
}

#[test]
fn mass_expiry_storm_is_fully_captured() {
    // Section 15 test plan: a slow storm through the active expire cycle.
    // 2000 keys with staggered short TTLs; every expiration must be mirrored
    // (bounded only by maxlen, which is left at the 10000 default).
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let mut pipe = redis::pipe();
    for i in 0..2000 {
        pipe.cmd("SET")
            .arg(format!("storm{i}"))
            .arg("v")
            .arg("PX")
            .arg(100 + (i % 400))
            .ignore();
    }
    let _: () = pipe.query(&mut c).expect("pipeline SET PX");

    wait_until(
        Duration::from_secs(60),
        "all 2000 expirations mirrored",
        || xlen(&mut c, "events:expired") >= 2000,
    );
    assert_eq!(info_field(&mut c, "forwarded"), 2000);
    assert_eq!(info_field(&mut c, "dropped"), 0);
}

#[test]
fn oom_refusal_is_a_counted_drop() {
    // Loss-window row: with the M flag, XADD is refused under maxmemory and
    // the event becomes a counted drop, not a forced write.
    let s = TestServer::start(&["events", "*"]);
    let mut c = s.conn();

    // Fill beyond the limit we are about to set, then squeeze.
    let payload = "x".repeat(64 * 1024);
    for i in 0..64 {
        let _: () = c.set(format!("fill{i}"), &payload).expect("SET fill");
    }
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("1mb")
        .query(&mut c)
        .expect("CONFIG SET maxmemory");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("noeviction")
        .query(&mut c)
        .expect("CONFIG SET policy");

    // DEL works under noeviction and fires an event; the mirror XADD must be
    // refused by verify_oom while used memory exceeds maxmemory.
    let _: () = c.del("fill0").expect("DEL under OOM");
    wait_until(Duration::from_secs(5), "oom drop counted", || {
        info_field(&mut c, "dropped_oom") >= 1
    });
    assert!(info_field(&mut c, "last_error_time") > 0);

    // Release the pressure and confirm capture recovers.
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("0")
        .query(&mut c)
        .expect("CONFIG SET maxmemory 0");
    let forwarded_before = info_field(&mut c, "forwarded");
    let _: () = c.del("fill1").expect("DEL after recovery");
    wait_until(Duration::from_secs(5), "capture recovers after OOM", || {
        info_field(&mut c, "forwarded") > forwarded_before
    });
}
