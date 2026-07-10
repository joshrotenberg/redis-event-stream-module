//! Integration-test harness (issue #11), built on redis-server-wrapper's
//! blocking API. Each test gets an isolated server: unique port, tempdir
//! working directory, kill-on-drop via the wrapper handle.

// Each test binary compiles this module separately and uses a different
// subset of the helpers, so per-binary dead-code analysis is meaningless.
#![allow(dead_code)]

use redis::Commands;
use redis_server_wrapper::blocking::{RedisServer, RedisServerHandle};
use std::net::TcpListener;
use std::path::PathBuf;
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
        RedisServer::new()
            .port(port)
            .bind("127.0.0.1")
            .dir(dir.path())
            .save(false)
            .enable_module_command("yes")
            .loadmodule_with_args(module_path(), module_args.iter().map(|s| s.to_string()))
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
