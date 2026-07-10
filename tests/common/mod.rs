//! Integration-test harness (issue #11), built on redis-server-wrapper's
//! blocking API. Each test gets an isolated server: unique port, tempdir
//! working directory, kill-on-drop via the wrapper handle.

// Each test binary compiles this module separately and uses a different
// subset of the helpers, so per-binary dead-code analysis is meaningless.
#![allow(dead_code)]

use redis::Commands;
use redis_server_wrapper::blocking::{
    RedisCluster, RedisClusterHandle, RedisServer, RedisServerHandle,
};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Once;
use std::time::{Duration, Instant};

/// Ask the OS for a free port. The tiny window between releasing the probe
/// listener and the server binding it is acceptable for tests, and unlike a
/// fixed counter it cannot collide with a server leaked by an earlier binary.
pub fn next_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind probe")
        .local_addr()
        .expect("probe addr")
        .port()
}

/// Base client port for the next test cluster. Clusters need a contiguous run
/// of client ports plus their bus ports at `+10000`, so a random OS-assigned
/// port will not do; this band (16000+) sits below the ephemeral range the
/// single-server tests draw from, so the two do not collide.
static CLUSTER_BASE: AtomicU16 = AtomicU16::new(16000);

fn next_cluster_base() -> u16 {
    CLUSTER_BASE.fetch_add(50, Ordering::Relaxed)
}

/// An in-process Redis Cluster for cluster-mode tests (issue #19). Kill-on-drop
/// via the wrapper handle.
pub struct TestCluster {
    pub handle: RedisClusterHandle,
}

impl TestCluster {
    /// Start an `masters`-master cluster (no replicas). When `module_args` is
    /// `Some`, every node loads the module with those arguments; the cluster
    /// builder has no dedicated `loadmodule`, so this goes through `.extra`.
    /// Returns `Err` if the cluster does not form, which is the expected
    /// outcome while the module refuses to load in cluster mode.
    pub fn try_start(masters: u16, module_args: Option<&[&str]>) -> Result<TestCluster, String> {
        let mut b = RedisCluster::builder()
            .masters(masters)
            .replicas_per_master(0)
            .base_port(next_cluster_base())
            .save(false)
            .cluster_node_timeout(2000);
        // The CI version matrix (issue #14) points these at a specific build.
        // Cluster formation shells out to redis-cli, so both must be set.
        if let Ok(bin) = std::env::var("TEST_REDIS_SERVER_BIN") {
            b = b.redis_server_bin(bin);
        }
        if let Ok(bin) = std::env::var("TEST_REDIS_CLI_BIN") {
            b = b.redis_cli_bin(bin);
        }
        if let Some(args) = module_args {
            let directive = format!("{} {}", module_path().display(), args.join(" "));
            b = b.extra("loadmodule", directive.trim().to_string());
        }
        b.start()
            .map(|handle| TestCluster { handle })
            .map_err(|e| e.to_string())
    }

    /// Run a command against one specific node without cluster redirection,
    /// so the reply reveals whether that node owns the key's slot.
    pub fn node_run(&self, index: usize, args: &[&str]) -> Result<String, String> {
        self.handle.node_run(index, args).map_err(|e| e.to_string())
    }

    pub fn num_masters(&self) -> usize {
        self.handle.num_masters() as usize
    }

    pub fn node_ports(&self) -> Vec<u16> {
        self.handle
            .node_addrs()
            .iter()
            .filter_map(|a| a.rsplit(':').next().and_then(|p| p.parse().ok()))
            .collect()
    }

    /// A cluster-aware connection that follows MOVED/ASK redirections, for
    /// seeding keys across the whole cluster.
    pub fn cluster_conn(&self) -> redis::cluster::ClusterConnection {
        let nodes: Vec<String> = self
            .handle
            .node_addrs()
            .iter()
            .map(|a| format!("redis://{a}/"))
            .collect();
        redis::cluster::ClusterClient::new(nodes)
            .expect("cluster client")
            .get_connection()
            .expect("cluster connection")
    }

    /// Read one numeric field from a specific node's module INFO section.
    pub fn node_info_field(&self, index: usize, field: &str) -> i64 {
        let raw = self
            .node_run(index, &["INFO", "eventstream"])
            .unwrap_or_default();
        let prefix = format!("eventstream_{field}:");
        raw.lines()
            .find_map(|l| l.strip_prefix(&prefix))
            .unwrap_or("0")
            .trim()
            .parse()
            .unwrap_or(0)
    }

    /// The per-node pinned hash tag reported in INFO (empty if unselected).
    pub fn node_pinned_tag(&self, index: usize) -> String {
        let raw = self
            .node_run(index, &["INFO", "eventstream"])
            .unwrap_or_default();
        raw.lines()
            .find_map(|l| l.strip_prefix("eventstream_cluster_pinned_tag:"))
            .unwrap_or("")
            .trim()
            .to_string()
    }

    /// A node's cluster id (`CLUSTER MYID`).
    pub fn node_id(&self, index: usize) -> String {
        self.node_run(index, &["CLUSTER", "MYID"])
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    /// The hash slot a key maps to, as the cluster computes it.
    pub fn keyslot(&self, key: &str) -> u16 {
        self.node_run(0, &["CLUSTER", "KEYSLOT", key])
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(0)
    }

    /// Migrate a single slot from node `from` to node `to`, moving any keys in
    /// it, via the manual `CLUSTER SETSLOT` dance (redis-server-wrapper 0.4.3
    /// exposes no reshard helper). After this returns, `to` owns the slot: the
    /// source node's writes to keys in it get the local-refusal error, which is
    /// what the module re-pins on (issue #46).
    pub fn migrate_slot(&self, slot: u16, from: usize, to: usize) {
        let from_id = self.node_id(from);
        let to_id = self.node_id(to);
        let slot_s = slot.to_string();
        let to_port = self.node_ports()[to].to_string();

        self.node_run(to, &["CLUSTER", "SETSLOT", &slot_s, "IMPORTING", &from_id])
            .expect("mark importing on destination");
        self.node_run(from, &["CLUSTER", "SETSLOT", &slot_s, "MIGRATING", &to_id])
            .expect("mark migrating on source");

        // Move every key in the slot to the destination.
        loop {
            let reply = self
                .node_run(from, &["CLUSTER", "GETKEYSINSLOT", &slot_s, "100"])
                .unwrap_or_default();
            let keys: Vec<&str> = reply
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .collect();
            if keys.is_empty() {
                break;
            }
            let mut args: Vec<&str> =
                vec!["MIGRATE", "127.0.0.1", &to_port, "", "0", "5000", "KEYS"];
            args.extend(keys.iter().copied());
            self.node_run(from, &args).expect("migrate keys");
        }

        // Assign the slot to the destination everywhere. Destination first so it
        // claims the slot before the source relinquishes it; then the source;
        // then the rest, though gossip would also spread it.
        let mut order = vec![to, from];
        order.extend((0..self.num_masters()).filter(|&i| i != to && i != from));
        for i in order {
            self.node_run(i, &["CLUSTER", "SETSLOT", &slot_s, "NODE", &to_id])
                .expect("assign slot to destination");
        }
    }
}

/// Build the module cdylib once per test-binary run and return its path.
/// The module is always built in release; the test profile is independent.
pub fn module_path() -> PathBuf {
    static BUILD: Once = Once::new();
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    BUILD.call_once(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(&manifest)
            .status()
            .expect("failed to run cargo build");
        assert!(status.success(), "module build failed");
    });
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| manifest.join("target"));
    let ext = if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    target
        .join("release")
        .join(format!("libredis_event_stream_module.{ext}"))
}

/// A running server with the module loaded, plus its working dir (kept alive
/// so persistence tests can restart on the same dataset).
pub struct TestServer {
    pub handle: RedisServerHandle,
    pub port: u16,
    pub dir: tempfile::TempDir,
}

impl TestServer {
    /// Start a server with the module loaded with `module_args`
    /// (e.g. `["events", "expired,set"]`; empty for defaults).
    pub fn start(module_args: &[&str]) -> TestServer {
        let port = next_port();
        let dir = tempfile::tempdir().expect("tempdir");
        let handle = Self::builder(port, &dir, module_args)
            .start()
            .expect("failed to start redis-server with module");
        TestServer { handle, port, dir }
    }

    /// Like `start`, but return the error instead of panicking (for tests
    /// that assert a load abort).
    pub fn try_start(module_args: &[&str]) -> Result<TestServer, String> {
        let port = next_port();
        let dir = tempfile::tempdir().expect("tempdir");
        match Self::builder(port, &dir, module_args).start() {
            Ok(handle) => Ok(TestServer { handle, port, dir }),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Start a single-node cluster (one master owning all 16384 slots) with the
    /// module loaded. This is the single-shard case: `redis-cli --cluster
    /// create` refuses fewer than 3 nodes, so the slots are assigned directly.
    /// A normal (non-redirecting) client works, since one node owns everything.
    pub fn start_single_shard_cluster(module_args: &[&str]) -> TestServer {
        // A cluster node needs its bus port at client-port + 10000, so the
        // client port must be <= 55535 or the server refuses to start. Draw
        // from the low cluster band, not `next_port()`, whose OS-assigned
        // ephemeral port can exceed that ceiling (flaky by platform).
        let port = next_cluster_base();
        let dir = tempfile::tempdir().expect("tempdir");
        let handle = Self::builder(port, &dir, module_args)
            .extra("cluster-enabled", "yes")
            .extra("cluster-config-file", "nodes.conf")
            .extra("cluster-node-timeout", "2000")
            .start()
            .expect("failed to start cluster-enabled server");
        let server = TestServer { handle, port, dir };
        let mut c = server.conn();
        let _: () = redis::cmd("CLUSTER")
            .arg("ADDSLOTSRANGE")
            .arg(0)
            .arg(16383)
            .query(&mut c)
            .expect("assign all slots");
        // Redis 7.2 keeps a manually slotted single node in cluster_state:fail
        // until its config epoch is nonzero; bumping the epoch settles it to ok.
        // Newer servers reach ok on ADDSLOTSRANGE alone and treat this as a
        // harmless no-op.
        let _: () = redis::cmd("CLUSTER")
            .arg("BUMPEPOCH")
            .query(&mut c)
            .expect("bump config epoch");
        wait_until(Duration::from_secs(10), "single-shard cluster ok", || {
            let info: String = redis::cmd("CLUSTER")
                .arg("INFO")
                .query(&mut c)
                .unwrap_or_default();
            info.contains("cluster_state:ok")
        });
        server
    }

    /// Restart on the same working directory (persistence across restarts).
    /// The previous process must already be stopped.
    pub fn restart(self, module_args: &[&str]) -> TestServer {
        let TestServer { handle, port, dir } = self;
        drop(handle);
        let handle = Self::builder(port, &dir, module_args)
            .start()
            .expect("failed to restart redis-server");
        TestServer { handle, port, dir }
    }

    fn builder(port: u16, dir: &tempfile::TempDir, module_args: &[&str]) -> RedisServer {
        let mut b = RedisServer::new()
            .port(port)
            .bind("127.0.0.1")
            .dir(dir.path())
            .save(false)
            .enable_module_command("yes")
            .loadmodule_with_args(module_path(), module_args.iter().map(|s| s.to_string()));
        // The CI version matrix points these at a specific Redis or Valkey
        // build (issue #14); unset, the wrapper resolves redis-server/redis-cli
        // from PATH, which is the local-development default.
        if let Ok(bin) = std::env::var("TEST_REDIS_SERVER_BIN") {
            b = b.redis_server_bin(bin);
        }
        if let Ok(bin) = std::env::var("TEST_REDIS_CLI_BIN") {
            b = b.redis_cli_bin(bin);
        }
        b
    }

    pub fn conn(&self) -> redis::Connection {
        let client =
            redis::Client::open(format!("redis://127.0.0.1:{}/", self.port)).expect("client open");
        // The server is already accepting connections when start() returns;
        // retry briefly anyway to absorb scheduler noise.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client.get_connection() {
                Ok(c) => return c,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("cannot connect to test server: {e}"),
            }
        }
    }

    pub fn conn_db(&self, db: u32) -> redis::Connection {
        let mut c = self.conn();
        let _: () = redis::cmd("SELECT").arg(db).query(&mut c).expect("SELECT");
        c
    }

    /// Start a replica of `master` with the module loaded, and wait until the
    /// replication link is up and the initial sync has completed.
    pub fn start_replica_of(master: &TestServer, module_args: &[&str]) -> TestServer {
        let port = next_port();
        let dir = tempfile::tempdir().expect("tempdir");
        let handle = Self::builder(port, &dir, module_args)
            .replicaof("127.0.0.1", master.port)
            .start()
            .expect("failed to start replica");
        let replica = TestServer { handle, port, dir };
        let mut c = replica.conn();
        wait_until(Duration::from_secs(15), "replica link up", || {
            let info: String = redis::cmd("INFO")
                .arg("replication")
                .query(&mut c)
                .unwrap_or_default();
            info.contains("master_link_status:up")
        });
        replica
    }

    /// Start with append-only durability enabled (for restart-survival tests).
    pub fn start_aof(module_args: &[&str]) -> TestServer {
        let port = next_port();
        let dir = tempfile::tempdir().expect("tempdir");
        let handle = Self::builder(port, &dir, module_args)
            .appendonly(true)
            .start()
            .expect("failed to start redis-server with AOF");
        TestServer { handle, port, dir }
    }

    /// Restart an AOF server on the same working directory.
    pub fn restart_aof(self, module_args: &[&str]) -> TestServer {
        let TestServer { handle, port, dir } = self;
        drop(handle);
        let handle = Self::builder(port, &dir, module_args)
            .appendonly(true)
            .start()
            .expect("failed to restart redis-server with AOF");
        TestServer { handle, port, dir }
    }
}

/// Poll `f` until it returns true or the deadline passes. Panics with `what`
/// on timeout. Never assert on raw sleeps; always converge through this.
pub fn wait_until(timeout: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for: {what}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Read one numeric field from the module INFO section.
pub fn info_field(conn: &mut redis::Connection, field: &str) -> i64 {
    let raw: String = redis::cmd("INFO")
        .arg("eventstream")
        .query(conn)
        .expect("INFO eventstream");
    let prefix = format!("eventstream_{field}:");
    raw.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("field {field} missing from INFO: {raw}"))
        .trim()
        .parse()
        .expect("numeric INFO field")
}

/// XLEN that treats a missing key as 0.
pub fn xlen(conn: &mut redis::Connection, key: &str) -> i64 {
    redis::cmd("XLEN").arg(key).query(conn).unwrap_or(0)
}

/// All values of `field` across a stream's entries, in order.
pub fn stream_field_values(conn: &mut redis::Connection, key: &str, field: &str) -> Vec<Vec<u8>> {
    let reply: redis::streams::StreamRangeReply = conn.xrange_all(key).expect("XRANGE");
    reply
        .ids
        .iter()
        .filter_map(|entry| {
            entry.map.get(field).map(|v| match v {
                redis::Value::BulkString(b) => b.clone(),
                other => format!("{other:?}").into_bytes(),
            })
        })
        .collect()
}

/// Convenience: `stream_field_values` as lossy strings.
pub fn stream_field_strings(conn: &mut redis::Connection, key: &str, field: &str) -> Vec<String> {
    stream_field_values(conn, key, field)
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect()
}

/// Set a key with a short TTL and force lazy expiry, then wait for the
/// expired event to land in `stream`, growing its length past `prior_len`.
pub fn expire_key_and_wait(server: &TestServer, key: &str, stream: &str, prior_len: i64) {
    let mut c = server.conn();
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg("v")
        .arg("PX")
        .arg(80)
        .query(&mut c)
        .expect("SET PX");
    wait_until(Duration::from_secs(10), "expired event mirrored", || {
        // Touch the key so lazy expiration fires even if the active cycle
        // has not reached it yet.
        let _: Option<String> = c.get(key).ok().flatten();
        xlen(&mut c, stream) > prior_len
    });
}
