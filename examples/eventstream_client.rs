//! Example client that exercises and observes redis-event-stream-module.
//!
//! Drives events into the module and reads the mirrored streams back, against a
//! standalone server or a per-node cluster (auto-detected). It doubles as the
//! read-only consumer reference the cluster docs point to and as the driver for
//! longer soak tests.
//!
//! Run with `cargo run --example eventstream_client -- <command> [options]`.
//!
//! Commands:
//!   info                      Topology, each master's module counters, streams.
//!   produce [opts]            Drive events into the module.
//!     --sets N                N `SET`s (fire `set` events).
//!     --expire N --ttl-ms MS  N keys with a TTL, then force their expiry.
//!     --burst N               Mass-expiry: N keys with tiny TTLs, forced.
//!     --toggle                Flip eventstream.enabled off then on (gap markers).
//!   consume [opts]            Discover streams cluster-wide and tail them merged.
//!     --events a,b            Only these event types (default: all discovered).
//!     --from 0|$              Start at the beginning (0) or only new ($, default).
//!     --count N               Stop after N entries (default: run until Ctrl-C).
//!   watch                     Live dashboard of counters and stream lengths.
//!   soak [opts]               Sustained produce, then verify capture.
//!     --count N               Events to drive (default 5000).
//!     --rate N                Cap at N events/sec (default: unlimited).
//!
//! Common options: --url redis://host:port (default redis://127.0.0.1:6379),
//! --prefix events: (must match the module's stream-prefix).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::thread::sleep;
use std::time::{Duration, Instant};

use redis::cluster::{ClusterClient, ClusterConnection};
use redis::streams::StreamReadReply;
use redis::{Cmd, Connection, FromRedisValue, RedisResult, Value};

fn main() {
    let args = Args::parse();
    let target = Target::detect(&args);
    let result = match args.command.as_str() {
        "info" => cmd_info(&target),
        "produce" => cmd_produce(&target, &args),
        "consume" => cmd_consume(&target, &args),
        "watch" => cmd_watch(&target),
        "soak" => cmd_soak(&target, &args),
        other => {
            eprintln!("unknown command '{other}'. Try: info, produce, consume, watch, soak.");
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Argument parsing (hand-rolled; no CLI framework).
// ---------------------------------------------------------------------------

struct Args {
    command: String,
    url: String,
    prefix: String,
    values: HashMap<String, String>,
    flags: HashSet<String>,
}

impl Args {
    fn parse() -> Args {
        let mut it = std::env::args().skip(1);
        let command = it.next().unwrap_or_else(|| "info".to_string());
        let mut values = HashMap::new();
        let mut flags = HashSet::new();
        let rest: Vec<String> = it.collect();
        let mut i = 0;
        while i < rest.len() {
            let tok = &rest[i];
            if let Some(name) = tok.strip_prefix("--") {
                if i + 1 < rest.len() && !rest[i + 1].starts_with("--") {
                    values.insert(name.to_string(), rest[i + 1].clone());
                    i += 2;
                } else {
                    flags.insert(name.to_string());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        let url = values
            .get("url")
            .cloned()
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let prefix = values
            .get("prefix")
            .cloned()
            .unwrap_or_else(|| "events:".to_string());
        Args {
            command,
            url,
            prefix,
            values,
            flags,
        }
    }

    fn geti(&self, key: &str, default: i64) -> i64 {
        self.values
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    fn has(&self, flag: &str) -> bool {
        self.flags.contains(flag)
    }
}

// ---------------------------------------------------------------------------
// Topology and connections.
// ---------------------------------------------------------------------------

struct Target {
    is_cluster: bool,
    /// `host:port` of each master (one entry when standalone).
    masters: Vec<String>,
    prefix: String,
    url: String,
}

impl Target {
    fn detect(args: &Args) -> Target {
        let mut conn = open_single(&args.url).expect("connect to --url");
        let info: String = Cmd::new()
            .arg("INFO")
            .arg("cluster")
            .query(&mut conn)
            .unwrap_or_default();
        let is_cluster = info.contains("cluster_enabled:1");
        let masters = if is_cluster {
            let nodes: String = Cmd::new()
                .arg("CLUSTER")
                .arg("NODES")
                .query(&mut conn)
                .expect("CLUSTER NODES");
            masters_from_cluster_nodes(&nodes)
        } else {
            vec![host_port(&args.url)]
        };
        Target {
            is_cluster,
            masters,
            prefix: args.prefix.clone(),
            url: args.url.clone(),
        }
    }

    /// A connection for reading and writing streams by name. Cluster-aware in
    /// cluster mode, so each `{tag}` stream routes to its owner.
    fn open_rw(&self) -> RedisResult<Conn> {
        if self.is_cluster {
            let urls: Vec<String> = self
                .masters
                .iter()
                .map(|m| format!("redis://{m}"))
                .collect();
            let client = ClusterClient::new(urls)?;
            Ok(Conn::Cluster(client.get_connection()?))
        } else {
            Ok(Conn::Single(open_single(&self.url)?))
        }
    }
}

/// Extract the `host:port` of every master from `CLUSTER NODES` output.
fn masters_from_cluster_nodes(nodes: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in nodes.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 3 || !f[2].contains("master") {
            continue;
        }
        // field 1 is ip:port@busport; strip the bus port.
        if let Some(addr) = f[1].split('@').next() {
            out.push(addr.to_string());
        }
    }
    out.sort();
    out
}

/// Normalize `redis://host:port` or `host:port` to `host:port`.
fn host_port(url: &str) -> String {
    url.trim_start_matches("redis://")
        .trim_end_matches('/')
        .to_string()
}

fn open_single(url: &str) -> RedisResult<Connection> {
    let full = if url.starts_with("redis://") {
        url.to_string()
    } else {
        format!("redis://{url}")
    };
    redis::Client::open(full)?.get_connection()
}

/// A read/write connection that is either a single node or the whole cluster.
/// The variants differ a lot in size, but only one exists per run, so the
/// enum is not worth boxing.
#[allow(clippy::large_enum_variant)]
enum Conn {
    Single(Connection),
    Cluster(ClusterConnection),
}

impl redis::ConnectionLike for Conn {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        match self {
            Conn::Single(c) => c.req_packed_command(cmd),
            Conn::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        match self {
            Conn::Single(c) => c.req_packed_commands(cmd, offset, count),
            Conn::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Conn::Single(c) => c.get_db(),
            Conn::Cluster(c) => c.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self {
            Conn::Single(c) => c.check_connection(),
            Conn::Cluster(c) => c.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self {
            Conn::Single(c) => c.is_open(),
            Conn::Cluster(c) => c.is_open(),
        }
    }
}

// ---------------------------------------------------------------------------
// Discovery and module INFO.
// ---------------------------------------------------------------------------

/// Every destination stream across the cluster: union of each master's local
/// `EVENTSTREAM.STREAMS`. A module command runs node-locally, so cluster-wide
/// discovery is this client-side fan-out (see docs/consumer-patterns.md).
fn discover_streams(target: &Target) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for master in &target.masters {
        let Ok(mut c) = open_single(master) else {
            continue;
        };
        let streams: Vec<String> = Cmd::new()
            .arg("EVENTSTREAM.STREAMS")
            .query(&mut c)
            .unwrap_or_default();
        set.extend(streams);
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}

/// The event name a destination stream carries: strip the prefix and, in
/// cluster mode, the leading `{tag}`. `events:{06S}set` -> `set`,
/// `events:expired` -> `expired`.
fn event_name(prefix: &str, stream: &str) -> Option<String> {
    let rest = stream.strip_prefix(prefix)?;
    let rest = if rest.starts_with('{') {
        rest.split_once('}').map(|(_, r)| r).unwrap_or(rest)
    } else {
        rest
    };
    Some(rest.to_string())
}

/// The module INFO counters for one node, as field -> value.
fn node_counters(addr: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(mut c) = open_single(addr) else {
        return out;
    };
    let raw: String = Cmd::new()
        .arg("INFO")
        .arg("eventstream")
        .query(&mut c)
        .unwrap_or_default();
    for line in raw.lines() {
        if let Some((k, v)) = line.split_once(':') {
            if let Some(field) = k.strip_prefix("eventstream_") {
                out.insert(field.to_string(), v.trim().to_string());
            }
        }
    }
    out
}

/// Sum one numeric counter across all masters.
fn counter_sum(target: &Target, field: &str) -> i64 {
    target
        .masters
        .iter()
        .map(|m| {
            node_counters(m)
                .get(field)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0)
        })
        .sum()
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------

fn cmd_info(target: &Target) -> RedisResult<()> {
    println!(
        "topology: {} ({} master{})",
        if target.is_cluster {
            "cluster (per-node)"
        } else {
            "standalone"
        },
        target.masters.len(),
        if target.masters.len() == 1 { "" } else { "s" }
    );
    for m in &target.masters {
        let c = node_counters(m);
        let get = |k: &str| c.get(k).cloned().unwrap_or_else(|| "-".to_string());
        println!(
            "  {m}: forwarded={} dropped={} repins={} enabled={} tag={}",
            get("forwarded"),
            get("dropped"),
            get("repins"),
            get("enabled"),
            get("cluster_pinned_tag")
        );
    }
    let streams = discover_streams(target);
    println!("streams ({}):", streams.len());
    let mut conn = target.open_rw()?;
    for s in &streams {
        let len: i64 = Cmd::new().arg("XLEN").arg(s).query(&mut conn).unwrap_or(0);
        println!("  {s}  (len {len})");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// produce
// ---------------------------------------------------------------------------

/// Widen the module's event filter on every master so what we produce is
/// captured, without narrowing an existing filter.
fn ensure_events(target: &Target, want: &[&str]) {
    for m in &target.masters {
        let Ok(mut c) = open_single(m) else { continue };
        let current: Vec<String> = Cmd::new()
            .arg("CONFIG")
            .arg("GET")
            .arg("eventstream.events")
            .query(&mut c)
            .unwrap_or_default();
        let current = current.get(1).cloned().unwrap_or_default();
        if current == "*" {
            continue;
        }
        let mut have: HashSet<String> = current.split(',').map(|s| s.trim().to_string()).collect();
        let before = have.len();
        for w in want {
            have.insert((*w).to_string());
        }
        if have.len() != before {
            let joined = have.into_iter().collect::<Vec<_>>().join(",");
            let _: RedisResult<()> = Cmd::new()
                .arg("CONFIG")
                .arg("SET")
                .arg("eventstream.events")
                .arg(&joined)
                .query(&mut c);
        }
    }
}

/// Set the module's event filter to exactly `value` on every master. Used by
/// soak so each produced SET yields exactly one captured event, making the
/// forwarded count directly comparable to the produced count.
fn set_events(target: &Target, value: &str) {
    for m in &target.masters {
        let Ok(mut c) = open_single(m) else { continue };
        let _: RedisResult<()> = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.events")
            .arg(value)
            .query(&mut c);
    }
}

fn cmd_produce(target: &Target, args: &Args) -> RedisResult<()> {
    let sets = args.geti("sets", 0);
    let expire = args.geti("expire", 0);
    let burst = args.geti("burst", 0);
    let ttl_ms = args.geti("ttl-ms", 200).max(1);
    let toggle = args.has("toggle");
    if sets == 0 && expire == 0 && burst == 0 && !toggle {
        println!("nothing to do; pass --sets N, --expire N, --burst N, or --toggle");
        return Ok(());
    }

    let mut needed = vec![];
    if sets > 0 {
        needed.push("set");
    }
    if expire > 0 || burst > 0 {
        needed.push("expired");
    }
    ensure_events(target, &needed);
    let mut conn = target.open_rw()?;

    if sets > 0 {
        for i in 0..sets {
            let _: () = Cmd::new()
                .arg("SET")
                .arg(format!("demo:set:{i}"))
                .arg("v")
                .query(&mut conn)?;
        }
        println!("produced {sets} set events");
    }

    if expire > 0 {
        force_expiry(&mut conn, "demo:exp", expire, ttl_ms)?;
        println!("produced {expire} expired events (ttl {ttl_ms}ms)");
    }

    if burst > 0 {
        let start = Instant::now();
        force_expiry(&mut conn, "demo:burst", burst, ttl_ms)?;
        let secs = start.elapsed().as_secs_f64().max(0.001);
        println!(
            "burst: {burst} expirations in {:.2}s ({:.0}/s attempted)",
            secs,
            burst as f64 / secs
        );
    }

    if toggle {
        toggle_enabled(target)?;
        println!("toggled eventstream.enabled off then on (a gap-marker pair)");
    }
    Ok(())
}

/// Set `n` keys with a TTL, then read them back after the TTL so lazy expiry
/// fires promptly even if the active cycle has not reached them. Keys are not
/// hash-tagged, so in cluster mode they spread across masters and every node
/// captures its share.
fn force_expiry(conn: &mut Conn, label: &str, n: i64, ttl_ms: i64) -> RedisResult<()> {
    for i in 0..n {
        let _: () = Cmd::new()
            .arg("SET")
            .arg(format!("{label}:{i}"))
            .arg("v")
            .arg("PX")
            .arg(ttl_ms)
            .query(conn)?;
    }
    sleep(Duration::from_millis(ttl_ms as u64 + 50));
    for i in 0..n {
        let _: Option<String> = Cmd::new()
            .arg("GET")
            .arg(format!("{label}:{i}"))
            .query(conn)?;
    }
    Ok(())
}

/// Flip `eventstream.enabled` off then on across all masters, writing a key in
/// between whose event is lost, so the control stream gets a disabled/enabled
/// marker pair.
fn toggle_enabled(target: &Target) -> RedisResult<()> {
    let mut conn = target.open_rw()?;
    for m in &target.masters {
        let mut c = open_single(m)?;
        let _: () = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.enabled")
            .arg("no")
            .query(&mut c)?;
    }
    let _: () = Cmd::new()
        .arg("SET")
        .arg("demo:lost")
        .arg("v")
        .query(&mut conn)?;
    for m in &target.masters {
        let mut c = open_single(m)?;
        let _: () = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.enabled")
            .arg("yes")
            .query(&mut c)?;
    }
    // One more write so the `enabled` marker lands (it timestamps the first
    // event after re-enabling).
    let _: () = Cmd::new()
        .arg("SET")
        .arg("demo:resumed")
        .arg("v")
        .query(&mut conn)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// consume
// ---------------------------------------------------------------------------

fn cmd_consume(target: &Target, args: &Args) -> RedisResult<()> {
    // A set of event names, e.g. {"set", "expired"}.
    let wanted: Option<HashSet<String>> = args
        .get("events")
        .map(|s| s.split(',').map(|e| e.trim().to_string()).collect());
    let from_zero = args.get("from") == Some("0");
    let limit = args.geti("count", i64::MAX);

    let mut streams = discover_streams(target);
    if let Some(w) = &wanted {
        // Match by event name, which in cluster mode sits after the {tag}.
        streams.retain(|s| event_name(&target.prefix, s).is_some_and(|e| w.contains(&e)));
    }
    // Skip the module's own control/registry keys.
    streams.retain(|s| !s.contains('#'));
    if streams.is_empty() {
        println!("no matching streams yet; produce some events first, then re-run");
        return Ok(());
    }
    println!(
        "tailing {} stream(s): {}",
        streams.len(),
        streams.join(", ")
    );

    let mut conn = target.open_rw()?;
    // Per-stream cursor. "0" reads history; otherwise start at the current tail.
    let mut cursors: HashMap<String, String> = HashMap::new();
    for s in &streams {
        let start = if from_zero {
            "0".to_string()
        } else {
            last_id(&mut conn, s)
        };
        cursors.insert(s.clone(), start);
    }

    let mut seen = 0i64;
    loop {
        let mut batch: Vec<Entry> = Vec::new();
        for s in &streams {
            let cursor = cursors.get(s).cloned().unwrap_or_else(|| "0".to_string());
            let reply: StreamReadReply = match Cmd::new()
                .arg("XREAD")
                .arg("COUNT")
                .arg(200)
                .arg("STREAMS")
                .arg(s)
                .arg(&cursor)
                .query(&mut conn)
            {
                Ok(r) => r,
                Err(_) => continue,
            };
            for key in reply.keys {
                for id in key.ids {
                    cursors.insert(s.clone(), id.id.clone());
                    batch.push(Entry::from(s, &id.id, &id.map));
                }
            }
        }
        // Merge this poll's entries by entry ID across streams.
        batch.sort_by_key(|e| e.sort_key());
        for e in batch {
            println!("{e}");
            seen += 1;
            if seen >= limit {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(200));
    }
}

fn last_id(conn: &mut Conn, stream: &str) -> String {
    // The largest existing ID, so we start strictly after it. "$" is not usable
    // here because we poll per stream without BLOCK.
    let raw: Value = Cmd::new()
        .arg("XREVRANGE")
        .arg(stream)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query(conn)
        .unwrap_or(Value::Nil);
    first_id_of_xrange(&raw).unwrap_or_else(|| "0".to_string())
}

/// Pull the entry ID out of the first element of an XRANGE/XREVRANGE reply.
fn first_id_of_xrange(v: &Value) -> Option<String> {
    if let Value::Array(entries) = v {
        if let Some(Value::Array(pair)) = entries.first() {
            if let Some(id) = pair.first() {
                return String::from_redis_value(id.clone()).ok();
            }
        }
    }
    None
}

struct Entry {
    stream: String,
    id: String,
    event: String,
    key: String,
    db: String,
}

impl Entry {
    fn from(stream: &str, id: &str, map: &HashMap<String, Value>) -> Entry {
        let field = |name: &str| {
            map.get(name)
                .map(|v| match v {
                    Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    other => format!("{other:?}"),
                })
                .unwrap_or_default()
        };
        Entry {
            stream: stream.to_string(),
            id: id.to_string(),
            event: field("event"),
            key: field("key"),
            db: field("db"),
        }
    }

    /// (ms, seq) parsed from the entry ID, for cross-stream merge order.
    fn sort_key(&self) -> (u64, u64) {
        let mut parts = self.id.splitn(2, '-');
        let ms = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<16} {:>14}  {:<8} db={} key={}",
            self.stream, self.id, self.event, self.db, self.key
        )
    }
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

fn cmd_watch(target: &Target) -> RedisResult<()> {
    loop {
        // Clear screen, home cursor.
        print!("\x1b[2J\x1b[H");
        println!(
            "eventstream watch  ({})  Ctrl-C to exit",
            if target.is_cluster {
                "cluster"
            } else {
                "standalone"
            }
        );
        println!();
        println!(
            "{:<22} {:>10} {:>8} {:>7} {:>8} {:>8}",
            "node", "forwarded", "dropped", "repins", "enabled", "tag"
        );
        for m in &target.masters {
            let c = node_counters(m);
            let g = |k: &str| c.get(k).cloned().unwrap_or_else(|| "-".to_string());
            println!(
                "{:<22} {:>10} {:>8} {:>7} {:>8} {:>8}",
                m,
                g("forwarded"),
                g("dropped"),
                g("repins"),
                g("enabled"),
                g("cluster_pinned_tag")
            );
        }
        println!();
        let streams = discover_streams(target);
        let mut conn = target.open_rw()?;
        println!("{:<28} {:>10}", "stream", "length");
        for s in &streams {
            let len: i64 = Cmd::new().arg("XLEN").arg(s).query(&mut conn).unwrap_or(0);
            println!("{s:<28} {len:>10}");
        }
        sleep(Duration::from_millis(1000));
    }
}

// ---------------------------------------------------------------------------
// soak
// ---------------------------------------------------------------------------

fn cmd_soak(target: &Target, args: &Args) -> RedisResult<()> {
    let count = args.geti("count", 5000).max(1);
    let rate = args.geti("rate", 0); // 0 = unlimited
                                     // Capture exactly `set` so each produced SET yields one captured event and
                                     // the forwarded delta is directly comparable to the produced count.
    set_events(target, "set");
    println!("soak: set eventstream.events=set on all masters for an exact count");

    let forwarded_before = counter_sum(target, "forwarded");
    let mut conn = target.open_rw()?;

    println!(
        "soak: producing {count} set events{}...",
        if rate > 0 {
            format!(" at up to {rate}/s")
        } else {
            String::new()
        }
    );
    let start = Instant::now();
    let min_per = if rate > 0 {
        Duration::from_secs_f64(1.0 / rate as f64)
    } else {
        Duration::ZERO
    };
    for i in 0..count {
        let iter_start = Instant::now();
        let _: () = Cmd::new()
            .arg("SET")
            .arg(format!("soak:{i}"))
            .arg("v")
            .query(&mut conn)?;
        if i % 1000 == 999 {
            println!("  produced {}/{count}", i + 1);
        }
        if !min_per.is_zero() {
            if let Some(rem) = min_per.checked_sub(iter_start.elapsed()) {
                sleep(rem);
            }
        }
    }
    let produce_secs = start.elapsed().as_secs_f64();
    println!(
        "produced {count} in {:.2}s ({:.0}/s); waiting for capture to settle...",
        produce_secs,
        count as f64 / produce_secs.max(0.001)
    );

    // Wait until forwarded stops rising.
    let mut last = -1i64;
    let mut stable_since = Instant::now();
    loop {
        let now = counter_sum(target, "forwarded") - forwarded_before;
        if now != last {
            last = now;
            stable_since = Instant::now();
        } else if stable_since.elapsed() > Duration::from_secs(2) {
            break;
        }
        if start.elapsed() > Duration::from_secs(120) {
            println!("  (giving up waiting after 120s)");
            break;
        }
        sleep(Duration::from_millis(200));
    }

    let captured = counter_sum(target, "forwarded") - forwarded_before;
    let dropped = counter_sum(target, "dropped");
    let repins = counter_sum(target, "repins");
    println!("---");
    println!("produced : {count}");
    println!(
        "captured : {captured}  ({:.2}%)",
        100.0 * captured as f64 / count as f64
    );
    println!("dropped  : {dropped} (cumulative, all reasons)");
    if target.is_cluster {
        println!("repins   : {repins} (cumulative)");
    }
    if captured >= count {
        println!("result   : OK, every produced event was captured");
    } else {
        println!(
            "result   : {} event(s) not captured (see dropped_* counters via `info`)",
            count - captured
        );
    }
    Ok(())
}
