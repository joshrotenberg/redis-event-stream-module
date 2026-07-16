//! Replication and durability behavior (issue #13): mirrored entries and
//! markers replicate byte-identically, replicas never double-mirror, a
//! promoted replica takes over capture, and entries survive restarts under
//! AOF.

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

fn entry_ids(conn: &mut redis::Connection, key: &str) -> Vec<String> {
    let reply: redis::streams::StreamRangeReply = conn.xrange_all(key).expect("XRANGE");
    reply.ids.iter().map(|e| e.id.clone()).collect()
}

#[test]
fn entries_and_markers_replicate_identically() {
    let master = TestServer::start(&["events", "set"]);
    let replica = TestServer::start_replica_of(&master, &["events", "set"]);
    let mut mc = master.conn();
    let mut rc = replica.conn();

    let _: () = mc.set("a", "1").expect("SET on master");
    let _: () = mc.set("b", "2").expect("SET on master");
    wait_until(Duration::from_secs(10), "entries replicate", || {
        xlen(&mut rc, "events:set") == 2 && xlen(&mut rc, CONTROL) == 1
    });

    // Entry IDs propagate verbatim (SPEC.md section 9 ordering), so the
    // replica's streams are byte-identical, markers included.
    assert_eq!(
        entry_ids(&mut mc, "events:set"),
        entry_ids(&mut rc, "events:set")
    );
    assert_eq!(entry_ids(&mut mc, CONTROL), entry_ids(&mut rc, CONTROL));
    assert_eq!(
        stream_field_strings(&mut rc, CONTROL, "action"),
        vec!["loaded"]
    );
}

#[test]
fn replica_never_mirrors_locally() {
    let master = TestServer::start(&["events", "set"]);
    let replica = TestServer::start_replica_of(&master, &["events", "set"]);
    let mut mc = master.conn();
    let mut rc = replica.conn();

    for i in 0..10 {
        let _: () = mc.set(format!("k{i}"), "v").expect("SET");
    }
    wait_until(Duration::from_secs(10), "replication catches up", || {
        xlen(&mut rc, "events:set") == 10
    });

    // The replicated SET commands fire notifications on the replica too; the
    // MASTER gate must drop them, so the replica's own module forwards
    // nothing and the stream lengths match exactly (no double-mirroring).
    assert_eq!(info_field(&mut rc, "forwarded"), 0);
    assert_eq!(xlen(&mut mc, "events:set"), xlen(&mut rc, "events:set"));
}

#[test]
fn promoted_replica_takes_over_capture() {
    let master = TestServer::start(&["events", "set"]);
    let replica = TestServer::start_replica_of(&master, &["events", "set"]);
    let mut mc = master.conn();
    let mut rc = replica.conn();

    let _: () = mc.set("pre", "v").expect("SET before promotion");
    wait_until(
        Duration::from_secs(10),
        "pre-promotion entry replicates",
        || xlen(&mut rc, "events:set") == 1,
    );

    let _: () = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query(&mut rc)
        .expect("promote replica");

    let _: () = rc.set("post", "v").expect("SET on promoted node");
    wait_until(Duration::from_secs(10), "promoted node captures", || {
        xlen(&mut rc, "events:set") == 2
    });
    assert!(
        info_field(&mut rc, "forwarded") >= 1,
        "the promoted node must mirror its own events"
    );

    // The replica-side module kept its own loaded marker pending while it
    // was a replica (markers only flush on a master); after promotion the
    // next event flushes it on top of the replicated one.
    let actions = stream_field_strings(&mut rc, CONTROL, "action");
    assert_eq!(
        actions,
        vec!["loaded", "loaded"],
        "replicated marker plus the promoted node's own pending marker"
    );
}

#[test]
fn flush_marker_replicates_and_is_not_duplicated_on_promotion() {
    // #74 with replication: a FLUSHALL on the master writes one `flushed`
    // marker, which replicates to the replica like any other entry. The
    // replica replays the replicated FLUSHALL (firing its own flush event) but
    // must NOT record a second marker — markers are recorded only on the
    // capturing master — so after promotion the promoted node's control stream
    // carries exactly one `flushed`, not two.
    let master = TestServer::start(&["events", "set"]);
    let replica = TestServer::start_replica_of(&master, &["events", "set"]);
    let mut mc = master.conn();
    let mut rc = replica.conn();

    let _: () = mc.set("a", "1").expect("SET before flush");
    wait_until(Duration::from_secs(10), "loaded marker replicates", || {
        xlen(&mut rc, CONTROL) == 1
    });

    let _: () = redis::cmd("FLUSHALL")
        .query(&mut mc)
        .expect("FLUSHALL on master");
    let _: () = mc.set("b", "1").expect("SET after flush");
    // The recreated control stream carries only the flushed marker, and it
    // replicates to the replica.
    wait_until(Duration::from_secs(10), "flushed marker replicates", || {
        stream_field_strings(&mut rc, CONTROL, "action") == vec!["flushed"]
    });

    let _: () = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query(&mut rc)
        .expect("promote replica");
    let _: () = rc.set("c", "1").expect("SET on promoted node");
    wait_until(
        Duration::from_secs(10),
        "promoted node drains own marker",
        || stream_field_strings(&mut rc, CONTROL, "action").len() == 2,
    );
    // Replicated `flushed`, then the promoted node's own pending `loaded`; the
    // replica-side flush replay recorded nothing, so `flushed` appears once.
    assert_eq!(
        stream_field_strings(&mut rc, CONTROL, "action"),
        vec!["flushed", "loaded"],
        "the replayed flush must not duplicate the replicated flushed marker"
    );
}

#[test]
fn aof_preserves_streams_across_restart() {
    let s = TestServer::start_aof(&["events", "set"]);
    {
        let mut c = s.conn();
        for i in 0..5 {
            let _: () = c.set(format!("k{i}"), "v").expect("SET");
        }
        wait_until(Duration::from_secs(10), "five entries mirrored", || {
            xlen(&mut c, "events:set") == 5
        });
        // No SAVE: durability must come from the AOF alone.
        let _ = redis::cmd("SHUTDOWN").arg("NOSAVE").query::<()>(&mut c);
    }

    let s = s.restart_aof(&["events", "set"]);
    let mut c = s.conn();
    assert_eq!(
        xlen(&mut c, "events:set"),
        5,
        "mirrored entries must replay from the AOF"
    );
    let keys = stream_field_strings(&mut c, "events:set", "key");
    assert_eq!(keys, vec!["k0", "k1", "k2", "k3", "k4"]);

    // Replaying the AOF must not re-trigger capture (not-LOADING gate):
    // exactly the original five entries, forwarded counter reset on load.
    assert_eq!(info_field(&mut c, "forwarded"), 0);
}
