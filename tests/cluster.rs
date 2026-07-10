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
    // Every node loads the module; the module returns an error from its init
    // in cluster mode (SPEC.md section 10), so the nodes fail to start and the
    // cluster never forms.
    let result = TestCluster::try_start(3, Some(&["events", "*"]));
    assert!(
        result.is_err(),
        "the module must refuse to load in cluster mode, so the cluster fails to form"
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
    let s =
        TestServer::start_single_shard_cluster(&["events", "set", "cluster-streams", "per-node"]);
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
    let tag: String = {
        let raw: String = redis::cmd("INFO").arg("eventstream").query(&mut c).unwrap();
        raw.lines()
            .find_map(|l| l.strip_prefix("eventstream_cluster_pinned_tag:"))
            .unwrap()
            .trim()
            .to_string()
    };
    assert!(!tag.is_empty(), "single node must pin a tag");
    assert!(xlen(&mut c, &format!("events:{{{tag}}}set")) > 0);
}

#[test]
fn invalid_cluster_streams_value_aborts_load() {
    // A bad cluster-streams value is a malformed module arg and must abort the
    // node startup, so the cluster does not form.
    let result = TestCluster::try_start(3, Some(&["cluster-streams", "bogus"]));
    assert!(
        result.is_err(),
        "an invalid cluster-streams value must abort load"
    );
}
