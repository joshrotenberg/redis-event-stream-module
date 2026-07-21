//! INFO section and counters (issues #10 and #12), plus the storm and OOM
//! loss-window behaviors from the section 15 test plan.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

#[test]
fn info_section_has_all_fields() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    // The complete counter surface (issue #88). This is the INDEPENDENT guard:
    // INFO, EVENTSTREAM.STATS, and the deinit log all derive from one shared
    // snapshot, so they cannot drift from each other — but a field dropped from
    // that snapshot would silently vanish from all three at once. This explicit
    // list, with the exact-count assertion, forces a conscious test update
    // whenever the surface changes, so a removed field fails CI.
    let expected = [
        "enabled",
        "eviction_risk",
        "forwarded",
        "events_lost",
        "firehose_forwarded",
        "autogroup_created",
        "autogroup_failed",
        "dropped",
        "dropped_xadd_error",
        "dropped_oom",
        "dropped_defer_error",
        "dropped_max_streams",
        "dropped_encode_error",
        "skipped_self",
        "skipped_filtered",
        "skipped_key_filtered",
        "skipped_db",
        "skipped_invalid",
        "active_streams",
        "registry_errors",
        "control_markers",
        "handler_panics",
        "dropped_no_owned_slot",
        "dropped_migrating",
        "repins",
        "repins_probe_detected",
        "cluster_per_node",
        "cluster_pinned_tag",
        "last_error_time",
    ];
    let info = info_map(&mut c);
    for f in expected {
        assert!(info.contains_key(f), "INFO eventstream missing field {f}");
    }
    assert_eq!(
        info.len(),
        expected.len(),
        "INFO field count changed; update the expected list and the SPEC \
         section 13 example (INFO fields: {:?})",
        info.keys().collect::<Vec<_>>()
    );
}

#[test]
fn module_list_reports_encoded_crate_version() {
    // The `ver` field is CARGO_PKG_VERSION encoded major*10000 + minor*100 +
    // patch (SPEC.md section 14, issue #87): the server-side check an upgrade
    // runbook uses to confirm which release is actually loaded.
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let mut parts = env!("CARGO_PKG_VERSION")
        .split('.')
        .map(|p| p.parse::<i64>().expect("numeric component"));
    let expected =
        parts.next().unwrap() * 10000 + parts.next().unwrap() * 100 + parts.next().unwrap();

    let as_str = |v: &redis::Value| match v {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    };
    // RESP2 reply: one flat [name, <name>, ver, <ver>, ...] array per module.
    let modules: Vec<Vec<redis::Value>> = redis::cmd("MODULE")
        .arg("LIST")
        .query(&mut c)
        .expect("MODULE LIST");
    let entry = modules
        .iter()
        .find(|m| {
            m.chunks(2).any(|kv| {
                kv.len() == 2
                    && as_str(&kv[0]).as_deref() == Some("name")
                    && as_str(&kv[1]).as_deref() == Some("eventstream")
            })
        })
        .expect("eventstream listed");
    let ver = entry
        .chunks(2)
        .find_map(|kv| match kv {
            [k, redis::Value::Int(n)] if as_str(k).as_deref() == Some("ver") => Some(*n),
            _ => None,
        })
        .expect("ver field present");
    assert_eq!(ver, expected);
}

#[test]
fn counters_track_capture_activity() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    assert_eq!(info_field(&mut c, "forwarded"), 0);

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL"); // filtered out
    wait_until(CAPTURE_WAIT, "forwarded counts the set", || {
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
fn wrongtype_destination_counts_per_stream_and_recovers() {
    // Per-stream failure accounting (issues #68 and #71): break one
    // destination stream with a WRONGTYPE occupant, watch its per-stream
    // dropped counter move while forwarded stands still, then fix it and
    // watch forwarded resume (the recovery notice fires here; the harness
    // has no log access, so the counters carry the assertion).
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    // A first successful write registers events:set in the registry.
    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "stream registered", || {
        info_field(&mut c, "forwarded") == 1
    });

    // Replace the destination stream with a plain string: the module's next
    // XADD to it gets WRONGTYPE. Writes to prefix keys are guarded, never
    // mirrored, so this cannot loop.
    let _: () = redis::cmd("DEL")
        .arg("events:set")
        .query(&mut c)
        .expect("DEL stream");
    let _: () = c.set("events:set", "occupied").expect("SET occupant");

    let _: () = c.set("b", "2").expect("SET into broken stream");
    wait_until(CAPTURE_WAIT, "wrongtype drop counted", || {
        info_field(&mut c, "dropped_xadd_error") >= 1
    });
    let st = streams_withstats(&mut c);
    assert_eq!(
        st["events:set"],
        (1, 1),
        "the drop lands on the failing stream's row"
    );
    assert!(info_field(&mut c, "last_error_time") > 0);
    // The canonical per-event write failed (firehose off), so this is a lost
    // event: one canonical failure, one events_lost (issue #218).
    assert_eq!(
        info_field(&mut c, "events_lost"),
        1,
        "a canonical-write failure is one lost event"
    );

    // Fix the destination; the next capture succeeds and ends the streak.
    let _: () = redis::cmd("DEL")
        .arg("events:set")
        .query(&mut c)
        .expect("DEL occupant");
    let _: () = c.set("d", "3").expect("SET after fix");
    wait_until(CAPTURE_WAIT, "capture recovers", || {
        streams_withstats(&mut c)["events:set"] == (2, 1)
    });
    assert_eq!(info_field(&mut c, "dropped_xadd_error"), 1);
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
    wait_until(CAPTURE_WAIT, "oom drop counted", || {
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
    wait_until(CAPTURE_WAIT, "capture recovers after OOM", || {
        info_field(&mut c, "forwarded") > forwarded_before
    });
}

#[test]
fn eviction_risk_tracks_maxmemory_policy() {
    // Issue #106: the derived `eviction_risk` flag flips with `maxmemory-policy`.
    // An `allkeys-*` policy can evict the destination streams themselves,
    // silently destroying captured history; the module surfaces a 0/1 risk flag
    // (the policy name stays in the log, not INFO, per SPEC.md section 13). The
    // config-change server event recomputes it, so the flag follows CONFIG SET
    // at runtime. `volatile-*` is not flagged: it evicts only keys with a TTL,
    // and the streams carry none.
    let s = TestServer::start(&[]);
    let mut c = s.conn();

    let set_policy = |c: &mut redis::Connection, policy: &str| {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg(policy)
            .query(c)
            .unwrap_or_else(|e| panic!("CONFIG SET maxmemory-policy {policy}: {e}"));
    };

    set_policy(&mut c, "noeviction");
    wait_until(
        Duration::from_secs(10),
        "eviction_risk 0 under noeviction",
        || info_field(&mut c, "eviction_risk") == 0,
    );

    set_policy(&mut c, "allkeys-lru");
    wait_until(
        Duration::from_secs(10),
        "eviction_risk 1 under allkeys-lru",
        || info_field(&mut c, "eviction_risk") == 1,
    );

    set_policy(&mut c, "volatile-lru");
    wait_until(
        Duration::from_secs(10),
        "eviction_risk 0 under volatile-lru",
        || info_field(&mut c, "eviction_risk") == 0,
    );
}
