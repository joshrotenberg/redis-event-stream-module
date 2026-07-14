//! Retention and mirrored-write options (issues #62, #108, #65): per-event
//! `maxlen` overrides, time-based `retention-ms` (MINID trimming), and the
//! `verify-oom` opt-out. All three shape the inline trim clause / call options
//! in `mirror_entry`, and each composes with the firehose copy.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

/// The three new keys default to today's behavior: no overrides, no time-based
/// retention, and OOM-verified writes (issues #62, #108, #65).
#[test]
fn new_retention_options_default_to_todays_behavior() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let get = |c: &mut redis::Connection, key: &str| -> String {
        let pair: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg(key)
            .query(c)
            .expect("CONFIG GET");
        pair[1].clone()
    };

    assert_eq!(get(&mut c, "eventstream.maxlen-overrides"), "");
    assert_eq!(get(&mut c, "eventstream.retention-ms"), "0");
    assert_eq!(get(&mut c, "eventstream.verify-oom"), "yes");
    assert_eq!(get(&mut c, "eventstream.maxlen"), "10000");
}

/// A per-event override caps its named stream independently of the global
/// `maxlen` (issue #62): with a small global cap and a large override on `del`,
/// the `set` stream trims under the global while `del` keeps everything.
#[test]
fn per_event_override_caps_named_stream_differently_from_global() {
    let s = TestServer::start(&[
        "events",
        "set,del",
        "maxlen",
        "100",
        "maxlen-overrides",
        "del=100000",
    ]);
    let mut c = s.conn();

    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
        let _: () = c.del(format!("k{i}")).expect("DEL");
    }
    wait_until(Duration::from_secs(15), "1000 events mirrored", || {
        info_field(&mut c, "forwarded") >= 1000
    });

    // `set` trims under the global cap of 100 (approximate, below 500); `del`,
    // overridden to 100000, retains all 500.
    let set_len = xlen(&mut c, "events:set");
    let del_len = xlen(&mut c, "events:del");
    assert!(
        set_len < 500,
        "global maxlen 100 must trim events:set below 500, got {set_len}"
    );
    assert_eq!(
        del_len, 500,
        "the del=100000 override must retain all 500 del entries, got {del_len}"
    );
}

/// A live `CONFIG SET eventstream.maxlen-overrides` takes effect on subsequent
/// writes, and a malformed value is rejected (issue #62).
#[test]
fn per_event_override_is_live_settable_and_validated() {
    let s = TestServer::start(&["events", "set", "maxlen", "100"]);
    let mut c = s.conn();

    // Malformed values are rejected without changing the running config.
    for bad in ["set", "=100", "set=", "set=abc", "set=-1", "set=1,"] {
        let res: redis::RedisResult<()> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("eventstream.maxlen-overrides")
            .arg(bad)
            .query(&mut c);
        assert!(res.is_err(), "CONFIG SET must reject {bad:?}");
    }

    // Raise the cap for `set` at runtime; subsequent writes retain all of them.
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.maxlen-overrides")
        .arg("set=100000")
        .query(&mut c)
        .expect("CONFIG SET maxlen-overrides");
    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(Duration::from_secs(15), "500 sets mirrored", || {
        info_field(&mut c, "forwarded") >= 500
    });
    assert_eq!(
        xlen(&mut c, "events:set"),
        500,
        "the live set=100000 override must retain all 500 entries"
    );
}

/// The firehose uses the global `maxlen`, not a per-event override (issue #62,
/// SPEC.md section 11): it aggregates every event type, so its window is sized
/// for the total rate. An override that raises the per-event `set` cap must not
/// raise the firehose window.
#[test]
fn firehose_uses_global_maxlen_not_the_override() {
    let s = TestServer::start(&[
        "events",
        "set",
        "maxlen",
        "100",
        "maxlen-overrides",
        "set=100000",
        "firehose",
        "yes",
    ]);
    let mut c = s.conn();

    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(Duration::from_secs(15), "500 firehose copies", || {
        info_field(&mut c, "firehose_forwarded") >= 500
    });

    // `set` keeps all 500 via its override; the firehose trims under the global
    // cap of 100 because the override does not apply to it.
    assert_eq!(xlen(&mut c, "events:set"), 500);
    let fh = xlen(&mut c, "events:#firehose");
    assert!(
        fh < 500,
        "firehose must trim under the global maxlen 100, not the override, got {fh}"
    );
}

/// Time-based retention trims entries older than the window on the next write
/// (issue #108). With `maxlen` well above the entry count, only MINID trimming
/// can drop entries, so a shrunken stream proves age-based trimming. The
/// firehose follows the same policy.
#[test]
fn retention_ms_trims_entries_by_age() {
    // Window 2s, global maxlen at its 10000 default (never reached here), so
    // any trimming is time-based (MINID takes precedence over maxlen anyway).
    let s = TestServer::start(&["events", "set", "retention-ms", "2000", "firehose", "yes"]);
    let mut c = s.conn();

    // A batch of "old" entries, enough to fill several listpack nodes so
    // approximate MINID trimming has whole nodes to drop.
    for i in 0..250 {
        let _: () = c.set(format!("old{i}"), "v").expect("SET old");
    }
    wait_until(Duration::from_secs(10), "old batch mirrored", || {
        info_field(&mut c, "forwarded") >= 250
    });

    // Let the window elapse so every old entry is older than `retention-ms`.
    // This is a deliberate wall-clock wait for a time-based feature, not a
    // convergence poll.
    std::thread::sleep(Duration::from_millis(2500));

    // A batch of "new" entries; each XADD carries MINID ~ (now - 2000), so the
    // now-aged old nodes are trimmed.
    for i in 0..250 {
        let _: () = c.set(format!("new{i}"), "v").expect("SET new");
    }
    wait_until(Duration::from_secs(10), "new batch mirrored", || {
        info_field(&mut c, "forwarded") >= 500
    });

    let set_len = xlen(&mut c, "events:set");
    assert!(
        set_len < 500,
        "MINID retention must trim the aged old entries; 500 writes left {set_len}"
    );
    let fh_len = xlen(&mut c, "events:#firehose");
    assert!(
        fh_len < 500,
        "retention-ms must trim the firehose by age too, got {fh_len}"
    );
}

/// A negative `retention-ms` module arg aborts the load, matching the `maxlen`
/// re-validation (issue #108).
#[test]
fn negative_retention_ms_module_arg_aborts_load() {
    let err = TestServer::try_start(&["retention-ms", "-1"]);
    assert!(
        err.is_err(),
        "loadmodule with retention-ms -1 must abort the server start"
    );
}

/// `verify-oom no` lets mirrored writes proceed at the memory limit where the
/// default `yes` refuses and counts them (issue #65). The firehose copy is
/// likewise admitted.
#[test]
fn verify_oom_no_admits_writes_that_yes_refuses() {
    let s = TestServer::start(&["events", "*", "firehose", "yes"]);
    let mut c = s.conn();

    // Fill beyond the limit we are about to set, then squeeze under noeviction.
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

    // Default verify-oom yes: the del event's mirror XADD is refused and
    // counted, not written.
    let _: () = c.del("fill0").expect("DEL under OOM");
    wait_until(Duration::from_secs(5), "oom drop counted", || {
        info_field(&mut c, "dropped_oom") >= 1
    });
    let dropped_oom_before = info_field(&mut c, "dropped_oom");
    let forwarded_before = info_field(&mut c, "forwarded");
    let firehose_before = info_field(&mut c, "firehose_forwarded");

    // Flip verify-oom off at runtime: the next event's writes proceed despite
    // used_memory exceeding maxmemory.
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("eventstream.verify-oom")
        .arg("no")
        .query(&mut c)
        .expect("CONFIG SET verify-oom no");
    let _: () = c.del("fill1").expect("DEL with verify-oom no");
    wait_until(
        Duration::from_secs(5),
        "write admitted under pressure",
        || info_field(&mut c, "forwarded") > forwarded_before,
    );
    assert!(
        info_field(&mut c, "firehose_forwarded") > firehose_before,
        "the firehose copy must also be admitted under verify-oom no"
    );
    assert_eq!(
        info_field(&mut c, "dropped_oom"),
        dropped_oom_before,
        "no further OOM drops once verify-oom is off"
    );
}
