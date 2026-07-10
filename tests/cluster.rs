//! Cluster-mode behavior (issue #19). Today the module refuses to load in
//! cluster mode; this pins that refusal and documents, with a live 3-master
//! cluster, why a fixed destination stream name cannot work and why the
//! slot-pinned per-node hashtag design (docs/cluster-design.md) is needed.

mod common;

use common::*;

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
