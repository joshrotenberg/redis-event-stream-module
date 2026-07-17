//! Cluster-mode behavior (issue #19 / #45): the refuse-to-load default, the
//! raw slot mechanic the design rests on, and per-node capture with a live
//! multi-master cluster.

mod common;

use common::*;
use std::time::Duration;

/// A node "owns" a key's slot if a direct (non-redirected) write returns OK
/// rather than a MOVED redirection.
fn owns(reply: &str) -> bool {
    let r = reply.trim();
    !r.contains("MOVED") && !r.starts_with("ERR") && !r.is_empty()
}

/// Find a hashtag whose slot the given node owns, by probing candidates.
fn owned_tag(cluster: &TestCluster, node: usize) -> Option<String> {
    for i in 0..300 {
        let tag = format!("n{i}");
        let probe = format!("{{{tag}}}probe");
        if let Ok(reply) = cluster.node_run(node, &["SET", &probe, "x"]) {
            if owns(&reply) {
                let _ = cluster.node_run(node, &["DEL", &probe]);
                return Some(tag);
            }
        }
    }
    None
}

#[test]
fn module_refuses_to_load_in_cluster_mode() {
    // The module returns an error from its init when the CLUSTER context
    // flag is set and cluster-streams is the default `refuse` (SPEC.md
    // section 10). A single cluster-enabled node pins that flag without
    // forming a cluster, keeping the test fast and independent of formation
    // (version-fragile on the oldest supported server; the same routing the
    // cluster-streams abort test uses), and the log assertion proves the
    // module's own refusal fired rather than any startup failure (#188).
    let err = TestServer::try_start_cluster_enabled(&["events", "*"])
        .err()
        .expect("the module must refuse to load in cluster mode");
    assert!(
        err.contains("refuses to load in cluster mode"),
        "the abort must come from the module's cluster refusal: {err}"
    );
}

#[test]
fn fixed_name_fails_off_owner_but_hashtag_stays_local() {
    // No module here: this is the raw mechanic the cluster design rests on.
    let cluster = TestCluster::try_start(3, None).expect("plain cluster forms");
    let n = cluster.num_masters();
    assert_eq!(n, 3);

    // A fixed stream name hashes to one slot, owned by exactly one node.
    let fixed_owners = (0..n)
        .filter(|&i| {
            let r = cluster
                .node_run(i, &["XADD", "events:expired", "*", "event", "expired"])
                .unwrap_or_default();
            owns(&r)
        })
        .count();
    assert_eq!(
        fixed_owners,
        1,
        "a fixed stream name is writable on exactly one node; the other {} fail",
        n - 1
    );

    // A per-node hashtag chosen to hash to a slot that node owns keeps the
    // write local on every node.
    for i in 0..n {
        let tag = owned_tag(&cluster, i).expect("each node owns some slot");
        let stream = format!("events:{{{tag}}}:expired");
        let reply = cluster
            .node_run(i, &["XADD", &stream, "*", "event", "expired"])
            .unwrap_or_default();
        assert!(
            owns(&reply),
            "node {i} must write its own hashtag stream {stream} locally, got: {reply}"
        );
    }
}

#[test]
fn per_node_mode_forms_cluster_and_captures_on_every_node() {
    // With cluster-streams=per-node the module loads, and every master pins its
    // streams to a slot it owns and captures locally (issue #45).
    let cluster =
        TestCluster::try_start(3, Some(&["events", "set", "cluster-streams", "per-node"]))
            .expect("per-node cluster forms with the module loaded");
    let n = cluster.num_masters();
    assert_eq!(n, 3);

    // Seed keys across the whole cluster; each SET fires one `set` event on its
    // owning node.
    let mut conn = cluster.cluster_conn();
    let total = 120;
    for i in 0..total {
        let _: () = redis::cmd("SET")
            .arg(format!("key:{i}"))
            .arg("v")
            .query(&mut conn)
            .expect("SET via cluster");
    }

    // Every mirrored write stays local: the forwarded counts sum to the total
    // and no node reports a non-local drop or a missing-slot drop.
    wait_until(
        Duration::from_secs(15),
        "all sets captured across nodes",
        || {
            (0..n)
                .map(|i| cluster.node_info_field(i, "forwarded"))
                .sum::<i64>()
                == total
        },
    );
    for i in 0..n {
        assert_eq!(
            cluster.node_info_field(i, "dropped_xadd_error"),
            0,
            "node {i} must not hit non-local-key errors in per-node mode"
        );
        assert_eq!(cluster.node_info_field(i, "dropped_no_owned_slot"), 0);
        // Steady state, no reshard: no migration-window drops and no
        // probe-detected re-pins (issues #75, #76).
        assert_eq!(cluster.node_info_field(i, "dropped_migrating"), 0);
        assert_eq!(cluster.node_info_field(i, "repins_probe_detected"), 0);
        assert_eq!(cluster.node_info_field(i, "cluster_per_node"), 1);
        assert!(
            cluster.node_info_field(i, "forwarded") > 0,
            "every node owns some slots and should capture something"
        );
    }

    // Each node pins a distinct, non-empty tag (a tag's slot is owned by exactly
    // one node, so they cannot collide).
    let tags: Vec<String> = (0..n).map(|i| cluster.node_pinned_tag(i)).collect();
    assert!(
        tags.iter().all(|t| !t.is_empty()),
        "every node selects a tag"
    );
    let unique: std::collections::HashSet<&String> = tags.iter().collect();
    assert_eq!(unique.len(), n, "per-node tags must be distinct: {tags:?}");

    // The destination streams carry the node tag.
    for i in 0..n {
        let tag = cluster.node_pinned_tag(i);
        let stream = format!("events:{{{tag}}}set");
        let xlen: i64 = cluster
            .node_run(i, &["XLEN", &stream])
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(-1);
        assert!(
            xlen > 0,
            "node {i} tagged stream {stream} should have entries"
        );
    }
}

#[test]
fn per_node_single_shard_captures() {
    // Single shard: one master owns all 16384 slots. This is the safest cluster
    // deployment for per-node mode and must just work, with a normal client.
    // The firehose rides along to pin its per-node placement (issue #58).
    let s = TestServer::start_single_shard_cluster(&[
        "events",
        "set",
        "cluster-streams",
        "per-node",
        "firehose",
        "yes",
    ]);
    let mut c = s.conn();
    for i in 0..20 {
        let _: () = redis::cmd("SET")
            .arg(format!("k:{i}"))
            .arg("v")
            .query(&mut c)
            .expect("SET");
    }
    wait_until(Duration::from_secs(10), "single-shard capture", || {
        info_field(&mut c, "forwarded") == 20
    });
    assert_eq!(info_field(&mut c, "dropped_xadd_error"), 0);
    assert_eq!(info_field(&mut c, "dropped_no_owned_slot"), 0);
    assert_eq!(info_field(&mut c, "cluster_per_node"), 1);
    // The one node owns every slot, so it captures into a tagged stream.
    let tag = pinned_tag(&mut c);
    assert!(!tag.is_empty(), "single node must pin a tag");
    assert!(xlen(&mut c, &format!("events:{{{tag}}}set")) > 0);
    // The firehose composes with the tag segment exactly like the per-event
    // streams (`events:{tag}#firehose`) and holds a copy of every capture;
    // no untagged `events:#firehose` may appear in per-node mode.
    assert_eq!(info_field(&mut c, "firehose_forwarded"), 20);
    assert_eq!(xlen(&mut c, &format!("events:{{{tag}}}#firehose")), 20);
    assert_eq!(xlen(&mut c, "events:#firehose"), 0);
}

#[test]
fn per_node_repins_after_slot_migration() {
    // A reshard that moves a node's pinned slot must not stop capture: the node
    // detects the local-refusal on its next mirrored write, re-pins to a slot it
    // still owns, and keeps capturing on a new tag. The old entries follow the
    // migrated slot to its new owner and stay reachable (issue #46). The
    // firehose rides along to pin that it survives the re-pin (issue #58).
    let cluster = TestCluster::try_start(
        3,
        Some(&[
            "events",
            "set",
            "cluster-streams",
            "per-node",
            "firehose",
            "yes",
        ]),
    )
    .expect("per-node cluster forms");
    let n = cluster.num_masters();
    assert_eq!(n, 3);
    let mut conn = cluster.cluster_conn();

    // First batch: every node pins a tag and writes its streams.
    for i in 0..120 {
        let _: () = redis::cmd("SET")
            .arg(format!("a:{i}"))
            .arg("v")
            .query(&mut conn)
            .expect("SET a");
    }
    wait_until(Duration::from_secs(15), "first batch captured", || {
        (0..n)
            .map(|i| cluster.node_info_field(i, "forwarded"))
            .sum::<i64>()
            == 120
    });

    // Pick a node that captured, and note its pinned tag, slot, and stream.
    let victim = (0..n)
        .find(|&i| cluster.node_info_field(i, "forwarded") > 0)
        .expect("some node captured");
    let old_tag = cluster.node_pinned_tag(victim);
    assert!(!old_tag.is_empty(), "victim pinned a tag");
    let old_slot = cluster.keyslot(&format!("{{{old_tag}}}"));
    let old_stream = format!("events:{{{old_tag}}}set");
    let old_len: i64 = redis::cmd("XLEN")
        .arg(&old_stream)
        .query(&mut conn)
        .expect("old stream len");
    assert!(old_len > 0, "victim wrote its tagged stream");
    let victim_forwarded = cluster.node_info_field(victim, "forwarded");

    // Move the victim's pinned slot to another node.
    let other = (0..n).find(|&i| i != victim).unwrap();
    cluster.migrate_slot(old_slot, victim, other);

    // Second batch: the victim still owns other slots, so it captures some of
    // these, hits the local-refusal writing to the migrated old tag, and re-pins.
    for i in 0..200 {
        let _: () = redis::cmd("SET")
            .arg(format!("b:{i}"))
            .arg("v")
            .query(&mut conn)
            .expect("SET b");
    }
    wait_until(
        Duration::from_secs(20),
        "victim re-pins and resumes",
        || {
            cluster.node_info_field(victim, "repins") >= 1
                && cluster.node_info_field(victim, "forwarded") > victim_forwarded
        },
    );

    // Re-pin is a clean handoff: the triggering event is captured on the new
    // tag, not dropped, and the node never ran out of owned slots.
    assert_eq!(
        cluster.node_info_field(victim, "dropped_xadd_error"),
        0,
        "the re-pin retry captures the event; no leaked write errors"
    );
    assert_eq!(cluster.node_info_field(victim, "dropped_no_owned_slot"), 0);
    // The migration completed before the second batch, so the recognized
    // error text (not the probe fallback) triggered the re-pin, and no event
    // was refused in a migration window (issues #75, #76).
    assert_eq!(cluster.node_info_field(victim, "repins_probe_detected"), 0);
    assert_eq!(cluster.node_info_field(victim, "dropped_migrating"), 0);

    // The victim pinned a new, different tag and captures on it.
    let new_tag = cluster.node_pinned_tag(victim);
    assert_ne!(new_tag, old_tag, "victim re-pinned to a different tag");
    assert!(!new_tag.is_empty());
    let new_len: i64 = redis::cmd("XLEN")
        .arg(format!("events:{{{new_tag}}}set"))
        .query(&mut conn)
        .expect("new stream len");
    assert!(new_len > 0, "capture resumed on the new tag");

    // The firehose re-pinned with everything else: the copy of the event that
    // triggered the re-pin (and of every later capture) lands under the new
    // tag, because the copy resolves the tag segment after the per-event
    // write settled (issue #58).
    let firehose_len: i64 = redis::cmd("XLEN")
        .arg(format!("events:{{{new_tag}}}#firehose"))
        .query(&mut conn)
        .expect("new firehose len");
    assert!(firehose_len > 0, "firehose resumed on the new tag");

    // The re-pin boundary is marked on the new control stream.
    let control_len: i64 = redis::cmd("XLEN")
        .arg(format!("events:{{{new_tag}}}#control"))
        .query(&mut conn)
        .unwrap_or(0);
    assert!(
        control_len >= 1,
        "a repinned gap marker delimits the window"
    );

    // The old entries migrated with the slot and are still reachable by name
    // (they now live on the node that received the slot).
    let old_len_after: i64 = redis::cmd("XLEN")
        .arg(&old_stream)
        .query(&mut conn)
        .expect("old stream len after");
    assert!(
        old_len_after >= old_len,
        "old entries survive the migration ({old_len_after} >= {old_len})"
    );
}

#[test]
fn cluster_wide_discovery_unions_per_node_streams() {
    // Read side (issue #47): each master reports only its own tagged streams
    // from EVENTSTREAM.STREAMS, and the union across masters is the complete
    // set. Reading every discovered stream recovers every captured event.
    let cluster =
        TestCluster::try_start(3, Some(&["events", "set", "cluster-streams", "per-node"]))
            .expect("per-node cluster forms");
    let n = cluster.num_masters();
    assert_eq!(n, 3);
    let mut conn = cluster.cluster_conn();

    let total = 150;
    for i in 0..total {
        let _: () = redis::cmd("SET")
            .arg(format!("k:{i}"))
            .arg("v")
            .query(&mut conn)
            .expect("SET");
    }
    wait_until(Duration::from_secs(15), "all captured", || {
        (0..n)
            .map(|i| cluster.node_info_field(i, "forwarded"))
            .sum::<i64>()
            == total
    });

    // Discovery fan-out: each node reports only its own tag, and the union has
    // exactly one `set` stream per master (distinct tags).
    let mut union: std::collections::HashSet<String> = std::collections::HashSet::new();
    for i in 0..n {
        let tag = cluster.node_pinned_tag(i);
        let local = cluster.node_streams(i);
        assert!(
            local.iter().all(|s| s.contains(&format!("{{{tag}}}"))),
            "node {i} reports only its own tag {tag}: {local:?}"
        );
        assert!(
            local.contains(&format!("events:{{{tag}}}set")),
            "node {i} lists its own set stream"
        );
        union.extend(local);
    }
    let set_streams: Vec<&String> = union.iter().filter(|s| s.ends_with("set")).collect();
    assert_eq!(
        set_streams.len(),
        n,
        "the union has one set stream per master: {set_streams:?}"
    );

    // Completeness: reading every discovered stream (routed to its owner) and
    // summing the entries recovers every seeded event, with none double-counted.
    let merged: i64 = set_streams
        .iter()
        .map(|s| {
            redis::cmd("XLEN")
                .arg(s.as_str())
                .query::<i64>(&mut conn)
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(
        merged, total,
        "the union of per-node streams contains every event exactly once"
    );
}

#[test]
fn per_node_captures_on_a_node_owning_a_single_slot() {
    // Skewed ownership (issue #116): a node owning a single slot must still
    // find a tag. Slot 16377 is chosen because none of the candidates the old
    // probabilistic 7.2 fallback generated (`es{i}` for i in 0..16384) hashes
    // to it, so on 7.2 this deterministically dropped everything before the
    // exhaustive mapping; the 7.4+ canonical-name path always covered it.
    // `redis-cli --cluster create` only deals in equal splits, so the topology
    // is built by hand: two bare nodes, MEET, manual slot assignment.
    let a = TestServer::start_cluster_node(&["events", "set", "cluster-streams", "per-node"]);
    let b = TestServer::start_cluster_node(&["events", "set", "cluster-streams", "per-node"]);
    let mut ca = a.conn();
    let mut cb = b.conn();
    let _: () = redis::cmd("CLUSTER")
        .arg("MEET")
        .arg("127.0.0.1")
        .arg(b.port)
        .query(&mut ca)
        .expect("meet");
    let _: () = redis::cmd("CLUSTER")
        .arg("ADDSLOTSRANGE")
        .arg(0)
        .arg(16376)
        .arg(16378)
        .arg(16383)
        .query(&mut ca)
        .expect("assign all but one slot to node a");
    let _: () = redis::cmd("CLUSTER")
        .arg("ADDSLOTS")
        .arg(16377)
        .query(&mut cb)
        .expect("assign the single slot to node b");
    // Same epoch settling as the single-shard helper (7.2 stays in fail with
    // config epoch 0); a colliding bump self-resolves.
    for c in [&mut ca, &mut cb] {
        let _: () = redis::cmd("CLUSTER")
            .arg("BUMPEPOCH")
            .query(c)
            .expect("bump epoch");
    }
    wait_until(
        Duration::from_secs(20),
        "skewed cluster ok on both nodes",
        || {
            let ok = |c: &mut redis::Connection| {
                redis::cmd("CLUSTER")
                    .arg("INFO")
                    .query::<String>(c)
                    .unwrap_or_default()
                    .contains("cluster_state:ok")
            };
            ok(&mut ca) && ok(&mut cb)
        },
    );

    // Sanity: the key about to be written really lives in node b's only slot,
    // as the server computes it.
    let slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("k21277")
        .query(&mut cb)
        .expect("keyslot");
    assert_eq!(slot, 16377, "test fixture: k21277 must hash to slot 16377");

    // One event on the single-slot node; tag selection runs on it and must
    // find the one owned slot.
    let _: () = redis::cmd("SET")
        .arg("k21277")
        .arg("v")
        .query(&mut cb)
        .expect("SET on the single-slot node");
    wait_until(Duration::from_secs(10), "single-slot node captures", || {
        info_field(&mut cb, "forwarded") == 1
    });
    assert_eq!(info_field(&mut cb, "dropped_no_owned_slot"), 0);
    assert_eq!(info_field(&mut cb, "dropped_xadd_error"), 0);

    // The pinned tag hashes to the node's only slot and the tagged stream has
    // the entry.
    let tag = pinned_tag(&mut cb);
    assert!(!tag.is_empty(), "the single-slot node must pin a tag");
    let tag_slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg(format!("{{{tag}}}x"))
        .query(&mut cb)
        .expect("tag keyslot");
    assert_eq!(tag_slot, 16377, "the tag must hash to the only owned slot");
    assert_eq!(xlen(&mut cb, &format!("events:{{{tag}}}set")), 1);
}

#[test]
fn per_node_drops_on_a_zero_slot_node_and_resumes_once_it_gains_one() {
    // A master owning ZERO slots (issue #89): freshly met, or every slot moved
    // away. It has no slot to pin its streams to, yet it can still capture
    // events — ASK'd writes into a slot it is importing execute locally, which
    // is exactly how a new node sees its first traffic during a reshard. Those
    // events must be dropped and counted as `dropped_no_owned_slot` (never
    // captured under a foreign tag, never a crash), the read commands must
    // still answer, and capture must resume once the node gains a slot,
    // because selection is retried on the next captured event.
    let a = TestServer::start_cluster_node(&["events", "set", "cluster-streams", "per-node"]);
    let b = TestServer::start_cluster_node(&["events", "set", "cluster-streams", "per-node"]);
    let mut ca = a.conn();
    let mut cb = b.conn();
    let _: () = redis::cmd("CLUSTER")
        .arg("MEET")
        .arg("127.0.0.1")
        .arg(b.port)
        .query(&mut ca)
        .expect("meet");
    // All 16384 slots to node a: b is a slotless master. Full coverage means
    // both nodes still reach cluster_state:ok and serve commands.
    let _: () = redis::cmd("CLUSTER")
        .arg("ADDSLOTSRANGE")
        .arg(0)
        .arg(16383)
        .query(&mut ca)
        .expect("assign every slot to node a");
    // Same epoch settling as the single-shard helper (7.2 stays in fail with
    // config epoch 0); b owns nothing, so only a needs the bump.
    let _: () = redis::cmd("CLUSTER")
        .arg("BUMPEPOCH")
        .query(&mut ca)
        .expect("bump epoch");
    wait_until(
        Duration::from_secs(20),
        "cluster ok with a slotless master",
        || {
            let ok = |c: &mut redis::Connection| {
                redis::cmd("CLUSTER")
                    .arg("INFO")
                    .query::<String>(c)
                    .unwrap_or_default()
                    .contains("cluster_state:ok")
            };
            ok(&mut ca) && ok(&mut cb)
        },
    );

    // Begin importing one slot into b — the ASK dance a reshard performs.
    // While the import is open, ASKING-prefixed writes run on b even though
    // it owns zero slots, and each fires a `set` notification there.
    let a_id: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query(&mut ca)
        .expect("a id");
    let b_id: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query(&mut cb)
        .expect("b id");
    let slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("k0")
        .query(&mut cb)
        .expect("keyslot");
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("IMPORTING")
        .arg(&a_id)
        .query(&mut cb)
        .expect("mark importing on b");
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("MIGRATING")
        .arg(&b_id)
        .query(&mut ca)
        .expect("mark migrating on a");

    // A handful of events is enough to pin the counter: with no tag cached,
    // every captured event re-runs selection, which probes up to all 16384
    // slots; hundreds of writes would only make the test slow. The module's
    // probe does not send ASKING, so on the slot b is importing it is refused
    // (the node does not own the slot yet — a MOVED redirect surfaces as the
    // module-level non-local-key error), not OK, so selection cannot pick the
    // slot mid-import.
    for k in ["a", "b", "c"] {
        let _: () = redis::cmd("ASKING").query(&mut cb).expect("ASKING");
        let _: () = redis::cmd("SET")
            .arg(format!("{{k0}}{k}"))
            .arg("v")
            .query(&mut cb)
            .expect("ASK'd SET on the slotless node");
    }
    // 4 = the 3 events plus the `loaded` control marker: pending markers are
    // drained at the top of the first notification, ahead of the event's own
    // job, and a marker with no owned slot is dropped under the same policy
    // as a mirrored entry (SPEC.md section 9).
    wait_until(Duration::from_secs(15), "slotless drops counted", || {
        info_field(&mut cb, "dropped_no_owned_slot") == 4
    });
    assert_eq!(
        info_field(&mut cb, "forwarded"),
        0,
        "nothing may be captured while the node owns no slot"
    );
    assert_eq!(info_field(&mut cb, "dropped_xadd_error"), 0);
    assert_eq!(info_field(&mut cb, "dropped_migrating"), 0);
    assert_eq!(info_field(&mut cb, "repins"), 0);
    assert_eq!(info_field(&mut cb, "cluster_per_node"), 1);
    let pinned_while_slotless = pinned_tag(&mut cb);
    assert!(
        pinned_while_slotless.is_empty(),
        "no tag may be pinned while the node owns no slot"
    );

    // The read commands stay usable. STREAMS takes the never-selected fast
    // path (the non-probing cached lookup) and answers an empty array rather
    // than erroring; STATS is keyless and needs no pinned tag.
    let streams: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut cb)
        .expect("STREAMS answers on a slotless node");
    assert!(
        streams.is_empty(),
        "no tag was ever selected: the registry lookup short-circuits to an \
         empty array, got {streams:?}"
    );
    let stats: Vec<redis::Value> = redis::cmd("EVENTSTREAM.STATS")
        .query(&mut cb)
        .expect("STATS answers on a slotless node");
    assert!(
        !stats.is_empty() && stats.len().is_multiple_of(2),
        "STATS returns the flat name/value array"
    );

    // The one-time log line states the observation — the walk found no slot
    // that accepted a local write — not an ownership inference (issue #116),
    // and points at the retry semantics.
    let log = b.log();
    assert!(
        log.contains("walked all 16384 slots"),
        "the first drop logs the walked-all-slots line"
    );
    assert!(log.contains("retried on the next captured event"));

    // Finish the migration: the importing side claims the slot first (which
    // also bumps its epoch), then the source relinquishes it. b now owns its
    // first slot.
    for c in [&mut cb, &mut ca] {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(&b_id)
            .query(c)
            .expect("assign the slot to b");
    }

    // Selection is retried on the next captured event: one plain write in the
    // newly owned slot is captured, and the node pins a tag hashing to it.
    let _: () = redis::cmd("SET")
        .arg("{k0}resume")
        .arg("v")
        .query(&mut cb)
        .expect("plain SET once b owns the slot");
    wait_until(
        Duration::from_secs(10),
        "capture resumes on the gained slot",
        || info_field(&mut cb, "forwarded") == 1,
    );
    assert_eq!(
        info_field(&mut cb, "dropped_no_owned_slot"),
        4,
        "the drop counter is history, not state; resuming does not reset it"
    );
    // The dropped `loaded` marker stays dropped: this first-ever selection is
    // not a re-pin, so nothing writes the control stream retroactively.
    assert_eq!(info_field(&mut cb, "control_markers"), 0);
    assert_eq!(info_field(&mut cb, "dropped_xadd_error"), 0);
    let tag = pinned_tag(&mut cb);
    assert!(!tag.is_empty(), "the node pins a tag once it owns a slot");
    let tag_slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg(format!("{{{tag}}}x"))
        .query(&mut cb)
        .expect("tag keyslot");
    assert_eq!(tag_slot, slot, "the tag must hash to the only owned slot");
    assert_eq!(xlen(&mut cb, &format!("events:{{{tag}}}set")), 1);
    assert_eq!(xlen(&mut cb, &format!("events:{{{tag}}}#control")), 0);
    // And the registry reports the stream now that a tag exists.
    let streams: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut cb)
        .expect("STREAMS after resume");
    assert!(
        streams.contains(&format!("events:{{{tag}}}set")),
        "the resumed stream is discoverable: {streams:?}"
    );
}

#[test]
fn invalid_cluster_streams_value_aborts_load() {
    // A bad cluster-streams value fails config validation, which happens during
    // module load ahead of the cluster-mode check, so it aborts the load in
    // plain standalone mode too. Asserting it here (no cluster) keeps the test
    // fast and independent of cluster formation, which is version-fragile in
    // the harness on the oldest supported server.
    let result = TestServer::try_start(&["cluster-streams", "bogus"]);
    assert!(
        result.is_err(),
        "an invalid cluster-streams value must abort load"
    );
}
