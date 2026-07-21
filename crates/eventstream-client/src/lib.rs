//! Consumer library for redis-event-stream-module.
//!
//! Packages the consumer logic every reader of the module's streams would
//! otherwise reimplement from SPEC.md sections 9-10: cluster-wide discovery via
//! per-master `EVENTSTREAM.STREAMS` fan-out, a merged-by-entry-ID reader across
//! a logical event type's per-node `{tag}` streams, and gap-marker reads from
//! the `#control` stream. A module command runs node-locally, so the union of
//! streams and the merge across nodes must be computed client-side; this crate
//! is that client side.
//!
//! The crate depends only on the `redis` client (with the `cluster` feature),
//! not on `redis-module`, so it carries no git-pinned dependency and can be
//! published independently of the (unpublishable) module crate.

use std::collections::{BTreeMap, HashMap, HashSet};

use redis::cluster::{ClusterClient, ClusterConnection};
use redis::streams::StreamReadReply;
use redis::{Cmd, Connection, FromRedisValue, RedisResult, Value};

// ---------------------------------------------------------------------------
// Topology and connections.
// ---------------------------------------------------------------------------

/// A discovered deployment: standalone (auto-detected) or a per-node cluster,
/// with the address of every serving master and the stream prefix the module
/// was loaded with.
pub struct Target {
    /// True when `INFO cluster` reported `cluster_enabled:1` on `url`.
    pub is_cluster: bool,
    /// `host:port` of each master (one entry when standalone).
    pub masters: Vec<String>,
    /// The module's `stream-prefix` (must match the module's config).
    pub prefix: String,
    /// The `--url` the target was detected from; the standalone connection.
    pub url: String,
}

impl Target {
    /// Detect topology by connecting to `url` and reading `INFO cluster`. In
    /// cluster mode the master set is derived from `CLUSTER NODES`; standalone
    /// is a single master (the `url` host:port).
    pub fn detect(url: &str, prefix: &str) -> RedisResult<Target> {
        let mut conn = open_single(url)?;
        let info: String = Cmd::new()
            .arg("INFO")
            .arg("cluster")
            .query(&mut conn)
            .unwrap_or_default();
        let is_cluster = info.contains("cluster_enabled:1");
        let masters = if is_cluster {
            let nodes: String = Cmd::new().arg("CLUSTER").arg("NODES").query(&mut conn)?;
            masters_from_cluster_nodes(&nodes)
        } else {
            vec![host_port(url)]
        };
        Ok(Target {
            is_cluster,
            masters,
            prefix: prefix.to_string(),
            url: url.to_string(),
        })
    }

    /// A connection for reading and writing streams by name. Cluster-aware in
    /// cluster mode, so each `{tag}` stream routes to its owner.
    pub fn open_rw(&self) -> RedisResult<Conn> {
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

/// Extract the `host:port` of every serving master from `CLUSTER NODES` output.
/// Skips replicas and any node that is not currently reachable and serving:
/// `fail`/`fail?` (down or suspected), `noaddr` (address unknown), and
/// `handshake` (not yet joined). Without this a just-killed master lingers in
/// the listing until the cluster forgets it, so a chaos run would query a dead
/// node.
pub fn masters_from_cluster_nodes(nodes: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in nodes.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 3 {
            continue;
        }
        let flags = f[2];
        if !flags.contains("master")
            || flags.contains("fail")
            || flags.contains("noaddr")
            || flags.contains("handshake")
        {
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
pub fn host_port(url: &str) -> String {
    url.trim_start_matches("redis://")
        .trim_end_matches('/')
        .to_string()
}

/// Open a single-node blocking connection to `url` (a `redis://` scheme is
/// added if absent).
pub fn open_single(url: &str) -> RedisResult<Connection> {
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
pub enum Conn {
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
// Discovery.
// ---------------------------------------------------------------------------

/// One destination stream as reported by the master that owns it, with the
/// `{tag}` and event-name attribution parsed out (SPEC.md section 10).
pub struct StreamInfo {
    /// Full stream name, e.g. `events:{06S}set` or `events:expired`.
    pub name: String,
    /// The master (`host:port`) whose `EVENTSTREAM.STREAMS` reported it.
    pub node: String,
    /// The `{tag}` segment in cluster mode; `None` when standalone.
    pub tag: Option<String>,
    /// The event name after the prefix and any `{tag}`, e.g. `set`; `None` when
    /// the name does not carry the configured prefix.
    pub event: Option<String>,
}

/// Every destination stream across the cluster with per-node attribution: the
/// union of each master's local `EVENTSTREAM.STREAMS`. A module command runs
/// node-locally, so cluster-wide discovery is this client-side fan-out (see
/// docs/cluster-consumers.md). A node that cannot be reached is skipped rather
/// than failing the whole discovery, matching the chaos suite's expectation
/// that a just-killed master does not abort a consumer.
pub fn discover(target: &Target) -> Vec<StreamInfo> {
    let mut out = Vec::new();
    for master in &target.masters {
        let Ok(mut c) = open_single(master) else {
            continue;
        };
        let streams: Vec<String> = Cmd::new()
            .arg("EVENTSTREAM.STREAMS")
            .query(&mut c)
            .unwrap_or_default();
        for name in streams {
            out.push(StreamInfo {
                tag: stream_tag(&target.prefix, &name),
                event: event_name(&target.prefix, &name),
                node: master.clone(),
                name,
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The sorted, de-duplicated set of destination stream names across the
/// cluster. Convenience over [`discover`] for callers that only need names.
pub fn discover_streams(target: &Target) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for s in discover(target) {
        set.insert(s.name);
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}

/// Every stream on one node found by scanning the keyspace, not the registry:
/// `SCAN MATCH <prefix>* TYPE stream`, looping the cursor. Finds streams by
/// name regardless of which registry (if any) records them, so it sees the
/// `#control` streams — never in the data-stream registry (issue #215) — and
/// old-tag data streams a reshard moved onto this node. O(keyspace) on the
/// node, so it is a discovery-time cost, not a hot-path one.
pub fn scan_streams(conn: &mut Connection, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next, keys): (String, Vec<String>) = match Cmd::new()
            .arg("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(format!("{prefix}*"))
            .arg("TYPE")
            .arg("stream")
            .arg("COUNT")
            .arg(256)
            .query(conn)
        {
            Ok(v) => v,
            Err(_) => break,
        };
        out.extend(keys);
        if next == "0" {
            break;
        }
        cursor = next;
    }
    out
}

/// Cluster-wide discovery by keyspace scan: the sorted union of
/// [`scan_streams`] over every master. Unlike [`discover_streams`] (the
/// `EVENTSTREAM.STREAMS` registry fan-out), this finds streams the registry
/// can miss after a reshard — a migrated old-tag stream now living on its new
/// owner — and the `#control` streams, which are never registered (issue
/// #215). It is the basis for gap-marker discovery and for a consumer
/// refreshing its stream set across a re-pin. An unreachable master is skipped,
/// matching [`discover`], so a just-killed node does not abort discovery.
pub fn discover_all(target: &Target) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for master in &target.masters {
        let Ok(mut c) = open_single(master) else {
            continue;
        };
        for name in scan_streams(&mut c, &target.prefix) {
            set.insert(name);
        }
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}

/// The event name a destination stream carries: strip the prefix and, in
/// cluster mode, the leading `{tag}`. `events:{06S}set` -> `set`,
/// `events:expired` -> `expired`.
pub fn event_name(prefix: &str, stream: &str) -> Option<String> {
    let rest = stream.strip_prefix(prefix)?;
    let rest = if rest.starts_with('{') {
        rest.split_once('}').map(|(_, r)| r).unwrap_or(rest)
    } else {
        rest
    };
    Some(rest.to_string())
}

/// The `{tag}` a cluster stream is pinned under: the segment between `{` and
/// `}` right after the prefix. `events:{06S}set` -> `06S`; `None` for a
/// standalone (untagged) name.
pub fn stream_tag(prefix: &str, stream: &str) -> Option<String> {
    let rest = stream.strip_prefix(prefix)?;
    let rest = rest.strip_prefix('{')?;
    rest.split_once('}').map(|(tag, _)| tag.to_string())
}

// ---------------------------------------------------------------------------
// Module INFO counters.
// ---------------------------------------------------------------------------

/// The module INFO counters for one node, as field -> value (the
/// `eventstream_` prefix stripped).
pub fn node_counters(addr: &str) -> BTreeMap<String, String> {
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
pub fn counter_sum(target: &Target, field: &str) -> i64 {
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
// Merged reader.
// ---------------------------------------------------------------------------

/// One mirrored stream entry, with the fields the module writes decoded.
pub struct Entry {
    pub stream: String,
    pub id: String,
    pub event: String,
    pub key: String,
    pub db: String,
}

impl Entry {
    /// Decode an entry from an `XREAD` reply's field map.
    pub fn from(stream: &str, id: &str, map: &HashMap<String, Value>) -> Entry {
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

    /// (ms, seq) parsed from the entry ID, for cross-stream merge order. The
    /// entry ID orders totally within a node; a same-millisecond tie across
    /// nodes is unspecified (SPEC.md section 9, ordering), so this key does not
    /// impose a cross-node total order.
    pub fn sort_key(&self) -> (u64, u64) {
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

/// A merged reader over a logical event type's per-node streams. Each `poll`
/// does one `XREAD` round across all streams from their per-stream cursors and
/// returns the batch merged by entry ID. Merge orders within a node; a
/// same-millisecond tie across nodes is unspecified (SPEC.md section 9).
pub struct MergedReader {
    streams: Vec<String>,
    cursors: HashMap<String, String>,
}

impl MergedReader {
    /// Build a reader over `streams`. When `from_zero`, each cursor starts at
    /// `0` (full history); otherwise at each stream's current tail, so only new
    /// entries are yielded (`$` is not usable because polling is per stream
    /// without `BLOCK`).
    pub fn new(conn: &mut Conn, streams: Vec<String>, from_zero: bool) -> MergedReader {
        let mut cursors: HashMap<String, String> = HashMap::new();
        for s in &streams {
            let start = if from_zero {
                "0".to_string()
            } else {
                last_id(conn, s)
            };
            cursors.insert(s.clone(), start);
        }
        MergedReader { streams, cursors }
    }

    /// The streams this reader covers.
    pub fn streams(&self) -> &[String] {
        &self.streams
    }

    /// Add any streams in `candidates` not already covered, initializing only
    /// their cursors (issue #215): a running consumer that re-discovers after a
    /// re-pin picks up the new-tag data and control streams without disturbing
    /// the per-stream cursors of streams it is already reading. New streams
    /// start at `0` when `from_zero` (so a migrated stream's retained history
    /// is read), otherwise at their current tail. Returns the names added.
    pub fn add_streams(
        &mut self,
        conn: &mut Conn,
        candidates: &[String],
        from_zero: bool,
    ) -> Vec<String> {
        let mut added = Vec::new();
        for s in candidates {
            if self.cursors.contains_key(s) {
                continue;
            }
            let start = if from_zero {
                "0".to_string()
            } else {
                last_id(conn, s)
            };
            self.cursors.insert(s.clone(), start);
            self.streams.push(s.clone());
            added.push(s.clone());
        }
        added
    }

    /// One `XREAD COUNT count` round across every stream, advancing each
    /// cursor, returning the round's entries sorted by entry ID. A stream that
    /// errors on this round (e.g. a node that just went away) is skipped.
    pub fn poll(&mut self, conn: &mut Conn, count: usize) -> Vec<Entry> {
        let mut batch: Vec<Entry> = Vec::new();
        for s in &self.streams {
            let cursor = self
                .cursors
                .get(s)
                .cloned()
                .unwrap_or_else(|| "0".to_string());
            let reply: StreamReadReply = match Cmd::new()
                .arg("XREAD")
                .arg("COUNT")
                .arg(count)
                .arg("STREAMS")
                .arg(s)
                .arg(&cursor)
                .query(conn)
            {
                Ok(r) => r,
                Err(_) => continue,
            };
            for key in reply.keys {
                for id in key.ids {
                    self.cursors.insert(s.clone(), id.id.clone());
                    batch.push(Entry::from(s, &id.id, &id.map));
                }
            }
        }
        batch.sort_by_key(|e| e.sort_key());
        batch
    }
}

/// The largest existing ID in `stream`, so a reader can start strictly after
/// it. `$` is not usable when polling per stream without `BLOCK`.
pub fn last_id(conn: &mut Conn, stream: &str) -> String {
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
pub fn first_id_of_xrange(v: &Value) -> Option<String> {
    if let Value::Array(entries) = v {
        if let Some(Value::Array(pair)) = entries.first() {
            if let Some(id) = pair.first() {
                return String::from_redis_value(id.clone()).ok();
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Gap-window markers.
// ---------------------------------------------------------------------------

/// A gap marker read from a `#control` stream. Callers bound "events may be
/// missing here" windows from these and re-run discovery when they see a
/// `repinned` marker after a reshard moved a node's pinned slot (SPEC.md
/// sections 9-10).
pub struct GapMarker {
    /// The `#control` stream the marker was read from.
    pub stream: String,
    /// The marker's entry ID.
    pub id: String,
    /// `enabled`, `disabled`, `repinned`, or `flushed`.
    pub action: String,
    /// For `flushed`, the flushed database (`-1` == `FLUSHALL`); else `None`.
    pub db: Option<i64>,
    /// The module version that wrote the marker.
    pub module_version: Option<String>,
}

/// Read gap markers from every `#control` stream in the deployment, starting
/// after `from` on each (`0` for full history, or a prior marker's ID to read
/// only newer ones). A `repinned` marker means a node re-pinned to a new tag
/// and its streams have new names, so a caller should re-run [`discover`].
pub fn read_gap_markers(target: &Target, from: &str) -> RedisResult<Vec<GapMarker>> {
    // Control streams are never in the data-stream registry, so they must be
    // found by keyspace scan, not `EVENTSTREAM.STREAMS` (issue #215): the old
    // filter over `discover_streams` was always empty.
    let controls: Vec<String> = discover_all(target)
        .into_iter()
        .filter(|s| s.ends_with("#control"))
        .collect();
    if controls.is_empty() {
        return Ok(Vec::new());
    }
    let mut conn = target.open_rw()?;
    let mut out = Vec::new();
    for stream in &controls {
        let reply: StreamReadReply = match Cmd::new()
            .arg("XREAD")
            .arg("STREAMS")
            .arg(stream)
            .arg(from)
            .query(&mut conn)
        {
            Ok(r) => r,
            Err(_) => continue,
        };
        for key in reply.keys {
            for id in key.ids {
                let field = |name: &str| match id.map.get(name) {
                    Some(Value::BulkString(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    Some(other) => Some(format!("{other:?}")),
                    None => None,
                };
                out.push(GapMarker {
                    stream: stream.clone(),
                    id: id.id.clone(),
                    action: field("action").unwrap_or_default(),
                    db: field("db").and_then(|d| d.parse().ok()),
                    module_version: field("module-version"),
                });
            }
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_name_strips_prefix_and_tag() {
        assert_eq!(
            event_name("events:", "events:expired").as_deref(),
            Some("expired")
        );
        assert_eq!(
            event_name("events:", "events:{06S}set").as_deref(),
            Some("set")
        );
        assert_eq!(event_name("events:", "other:set"), None);
    }

    #[test]
    fn stream_tag_parses_cluster_tag_only() {
        assert_eq!(
            stream_tag("events:", "events:{06S}set").as_deref(),
            Some("06S")
        );
        assert_eq!(stream_tag("events:", "events:expired"), None);
        assert_eq!(stream_tag("events:", "other:{x}set"), None);
    }

    #[test]
    fn sort_key_orders_by_ms_then_seq() {
        let mk = |id: &str| Entry {
            stream: String::new(),
            id: id.to_string(),
            event: String::new(),
            key: String::new(),
            db: String::new(),
        };
        assert!(mk("100-0").sort_key() < mk("100-1").sort_key());
        assert!(mk("100-9").sort_key() < mk("101-0").sort_key());
        assert_eq!(mk("bad").sort_key(), (0, 0));
    }

    #[test]
    fn masters_skips_replicas_and_unreachable() {
        let nodes = "\
id1 127.0.0.1:7001@17001 master - 0 0 1 connected 0-5460\n\
id2 127.0.0.1:7002@17002 myself,master - 0 0 2 connected 5461-10922\n\
id3 127.0.0.1:7003@17003 slave id1 0 0 3 connected\n\
id4 127.0.0.1:7004@17004 master,fail - 0 0 4 disconnected\n";
        assert_eq!(
            masters_from_cluster_nodes(nodes),
            vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()]
        );
    }
}
