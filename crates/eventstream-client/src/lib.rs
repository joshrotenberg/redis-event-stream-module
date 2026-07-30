//! Consumer library for redis-event-stream-module.
//!
//! Packages the consumer logic every reader of the module's streams would
//! otherwise reimplement from SPEC.md sections 9-10: cluster-wide discovery via
//! per-master `EVENTSTREAM.STREAMS` fan-out, a merged-by-entry-ID reader across
//! a logical event type's per-node `{tag}` streams, and gap-marker reads from
//! the `#control` stream. It also expands the stable fixed schema and Preview
//! `batch-v1` envelopes into one binary-safe logical-event type. A module
//! command runs node-locally, so the union of streams and the merge across
//! nodes must be computed client-side; this crate is that client side.
//!
//! The crate depends only on the `redis` client (with the `cluster` feature),
//! not on `redis-module`, so it carries no git-pinned dependency and can be
//! published independently of the (unpublishable) module crate.

use std::collections::{BTreeMap, HashMap, HashSet};

use base64::Engine;
use redis::cluster::{ClusterClient, ClusterConnection};
use redis::streams::StreamReadReply;
use redis::{Cmd, Connection, FromRedisValue, RedisResult, Value};
use serde::Deserialize;

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
/// docs/src/topologies.md). A node that cannot be reached is skipped rather
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

/// The Redis Stream entry that must be acknowledged as one unit.
///
/// A fixed entry contains one logical event. A `batch-v1` entry can contain
/// many, but Redis consumer groups still acknowledge this physical position
/// with one `XACK`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalPosition {
    pub stream: String,
    pub entry_id: String,
}

impl std::fmt::Display for PhysicalPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.stream, self.entry_id)
    }
}

/// Stable identity of one decoded logical event.
///
/// `envelope_index` is `None` for a fixed entry and the zero-based array index
/// for a `batch-v1` event. All events with the same [`PhysicalPosition`] share
/// one consumer-group acknowledgement boundary.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourcePosition {
    pub physical: PhysicalPosition,
    pub envelope_index: Option<usize>,
}

impl std::fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.physical)?;
        if let Some(index) = self.envelope_index {
            write!(f, "/{index}")?;
        }
        Ok(())
    }
}

/// One logical keyspace event decoded from either a fixed entry or a
/// `batch-v1` envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalEvent {
    pub source: SourcePosition,
    pub event: String,
    /// The exact Redis key bytes after decoding. Redis keys are binary-safe.
    pub key: Vec<u8>,
    pub db: i64,
    /// Notification class, present in `batch-v1` envelopes.
    pub class: Option<String>,
}

impl std::fmt::Display for LogicalEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  {:<8} db={} key={}",
            self.source,
            self.event,
            self.db,
            self.key.escape_ascii()
        )
    }
}

/// Machine-readable reason a physical entry could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeErrorKind {
    MissingField(&'static str),
    InvalidFieldType(&'static str),
    InvalidUtf8(&'static str),
    InvalidInteger {
        field: &'static str,
        value: String,
    },
    UnsupportedFormat(String),
    InvalidJson(String),
    CountMismatch {
        declared: usize,
        decoded: usize,
    },
    InvalidBase64 {
        envelope_index: usize,
        message: String,
    },
}

impl std::fmt::Display for DecodeErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "missing required field `{field}`"),
            Self::InvalidFieldType(field) => {
                write!(f, "field `{field}` is not a Redis string")
            }
            Self::InvalidUtf8(field) => write!(f, "field `{field}` is not valid UTF-8"),
            Self::InvalidInteger { field, value } => {
                write!(f, "field `{field}` is not a valid integer: {value:?}")
            }
            Self::UnsupportedFormat(format) => write!(
                f,
                "unsupported entry format {format:?}; expected fixed or batch-v1"
            ),
            Self::InvalidJson(message) => write!(f, "invalid `events` JSON: {message}"),
            Self::CountMismatch { declared, decoded } => write!(
                f,
                "field `count` declares {declared} events but `events` contains {decoded}"
            ),
            Self::InvalidBase64 {
                envelope_index,
                message,
            } => write!(
                f,
                "event {envelope_index} has invalid padded RFC 4648 base64 in `key`: {message}"
            ),
        }
    }
}

/// A decode failure attributed to the physical stream entry that caused it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError {
    pub physical: PhysicalPosition,
    pub kind: DecodeErrorKind,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.physical, self.kind)
    }
}

impl std::error::Error for DecodeError {}

/// One physical Redis Stream entry before its logical events are expanded.
#[derive(Clone, Debug)]
pub struct PhysicalEntry {
    pub stream: String,
    pub id: String,
    pub fields: HashMap<String, Value>,
}

impl PhysicalEntry {
    pub fn new(
        stream: impl Into<String>,
        id: impl Into<String>,
        fields: HashMap<String, Value>,
    ) -> Self {
        Self {
            stream: stream.into(),
            id: id.into(),
            fields,
        }
    }

    /// The physical consumer-group acknowledgement position.
    pub fn position(&self) -> PhysicalPosition {
        PhysicalPosition {
            stream: self.stream.clone(),
            entry_id: self.id.clone(),
        }
    }

    /// Decode this entry into one or more logical events.
    pub fn decode(&self) -> Result<Vec<LogicalEvent>, DecodeError> {
        decode_entry(self)
    }

    fn sort_key(&self) -> (u64, u64) {
        entry_id_sort_key(&self.id)
    }

    fn error(&self, kind: DecodeErrorKind) -> DecodeError {
        DecodeError {
            physical: self.position(),
            kind,
        }
    }
}

#[derive(Deserialize)]
struct BatchEvent {
    event: String,
    key: String,
    db: i64,
    class: String,
}

fn field_bytes<'a>(entry: &'a PhysicalEntry, name: &'static str) -> Result<&'a [u8], DecodeError> {
    match entry.fields.get(name) {
        Some(Value::BulkString(bytes)) => Ok(bytes),
        Some(Value::SimpleString(value)) => Ok(value.as_bytes()),
        Some(_) => Err(entry.error(DecodeErrorKind::InvalidFieldType(name))),
        None => Err(entry.error(DecodeErrorKind::MissingField(name))),
    }
}

fn field_utf8<'a>(entry: &'a PhysicalEntry, name: &'static str) -> Result<&'a str, DecodeError> {
    std::str::from_utf8(field_bytes(entry, name)?)
        .map_err(|_| entry.error(DecodeErrorKind::InvalidUtf8(name)))
}

fn optional_field_utf8<'a>(
    entry: &'a PhysicalEntry,
    name: &'static str,
) -> Result<Option<&'a str>, DecodeError> {
    if entry.fields.contains_key(name) {
        field_utf8(entry, name).map(Some)
    } else {
        Ok(None)
    }
}

fn parse_integer(entry: &PhysicalEntry, name: &'static str) -> Result<i64, DecodeError> {
    let value = field_utf8(entry, name)?;
    value.parse::<i64>().map_err(|_| {
        entry.error(DecodeErrorKind::InvalidInteger {
            field: name,
            value: value.to_string(),
        })
    })
}

/// Decode one physical stream entry.
///
/// The stable discriminator-free fixed schema expands to one event.
/// Preview `format=batch-v1` expands its ordered JSON array. Other
/// `entry-format` variants are intentionally rejected instead of being
/// mistaken for either shape.
pub fn decode_entry(entry: &PhysicalEntry) -> Result<Vec<LogicalEvent>, DecodeError> {
    let format = optional_field_utf8(entry, "format")?;
    match format {
        None => decode_fixed_entry(entry),
        Some("batch-v1") => decode_batch_v1_entry(entry),
        Some(other) => Err(entry.error(DecodeErrorKind::UnsupportedFormat(other.to_string()))),
    }
}

fn decode_fixed_entry(entry: &PhysicalEntry) -> Result<Vec<LogicalEvent>, DecodeError> {
    let event = field_utf8(entry, "event")?.to_string();
    let key = field_bytes(entry, "key")?.to_vec();
    let db = parse_integer(entry, "db")?;
    let class = optional_field_utf8(entry, "class")?.map(str::to_string);
    Ok(vec![LogicalEvent {
        source: SourcePosition {
            physical: entry.position(),
            envelope_index: None,
        },
        event,
        key,
        db,
        class,
    }])
}

fn decode_batch_v1_entry(entry: &PhysicalEntry) -> Result<Vec<LogicalEvent>, DecodeError> {
    let count_text = field_utf8(entry, "count")?;
    let declared = count_text.parse::<usize>().map_err(|_| {
        entry.error(DecodeErrorKind::InvalidInteger {
            field: "count",
            value: count_text.to_string(),
        })
    })?;
    let events: Vec<BatchEvent> = serde_json::from_slice(field_bytes(entry, "events")?)
        .map_err(|error| entry.error(DecodeErrorKind::InvalidJson(error.to_string())))?;
    if declared != events.len() {
        return Err(entry.error(DecodeErrorKind::CountMismatch {
            declared,
            decoded: events.len(),
        }));
    }

    events
        .into_iter()
        .enumerate()
        .map(|(envelope_index, event)| {
            let key = base64::engine::general_purpose::STANDARD
                .decode(event.key)
                .map_err(|error| {
                    entry.error(DecodeErrorKind::InvalidBase64 {
                        envelope_index,
                        message: error.to_string(),
                    })
                })?;
            Ok(LogicalEvent {
                source: SourcePosition {
                    physical: entry.position(),
                    envelope_index: Some(envelope_index),
                },
                event: event.event,
                key,
                db: event.db,
                class: Some(event.class),
            })
        })
        .collect()
}

/// One mirrored stream entry, with the fields the module writes decoded.
///
/// This is the original fixed-format view retained for compatibility. New
/// consumers that can encounter Preview envelopes should use
/// [`PhysicalEntry::decode`] or [`MergedReader::poll_decoded`].
pub struct Entry {
    pub stream: String,
    pub id: String,
    pub event: String,
    /// The exact Redis key bytes. Redis keys are binary-safe, so consumers
    /// must choose their own text or encoding representation at the boundary.
    pub key: Vec<u8>,
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
            key: map
                .get("key")
                .map(|value| match value {
                    Value::BulkString(bytes) => bytes.clone(),
                    Value::SimpleString(value) => value.as_bytes().to_vec(),
                    _ => Vec::new(),
                })
                .unwrap_or_default(),
            db: field("db"),
        }
    }

    /// (ms, seq) parsed from the entry ID, for cross-stream merge order. The
    /// entry ID orders totally within a node; a same-millisecond tie across
    /// nodes is unspecified (SPEC.md section 9, ordering), so this key does not
    /// impose a cross-node total order.
    pub fn sort_key(&self) -> (u64, u64) {
        entry_id_sort_key(&self.id)
    }
}

fn entry_id_sort_key(id: &str) -> (u64, u64) {
    let mut parts = id.splitn(2, '-');
    let ms = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (ms, seq)
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<16} {:>14}  {:<8} db={} key={}",
            self.stream,
            self.id,
            self.event,
            self.db,
            self.key.escape_ascii()
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
    /// cursor, returning physical entries sorted by entry ID. A stream that
    /// errors on this round (e.g. a node that just went away) is skipped.
    pub fn poll_physical(&mut self, conn: &mut Conn, count: usize) -> Vec<PhysicalEntry> {
        let mut batch: Vec<PhysicalEntry> = Vec::new();
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
                    batch.push(PhysicalEntry::new(s, id.id, id.map));
                }
            }
        }
        batch.sort_by_key(|e| e.sort_key());
        batch
    }

    /// Read and expand one round of fixed and `batch-v1` physical entries.
    ///
    /// Logical events preserve physical entry order and, inside an envelope,
    /// JSON array order. A decode failure names the source stream and entry ID.
    /// The physical read cursor has already advanced, so callers should treat
    /// a failure as a poison entry and stop or record it explicitly.
    pub fn poll_decoded(
        &mut self,
        conn: &mut Conn,
        count: usize,
    ) -> Result<Vec<LogicalEvent>, DecodeError> {
        let physical = self.poll_physical(conn, count);
        let mut logical = Vec::new();
        for entry in physical {
            logical.extend(entry.decode()?);
        }
        Ok(logical)
    }

    /// Original fixed-format reader retained for compatibility.
    ///
    /// Use [`Self::poll_decoded`] when a stream can contain Preview
    /// `batch-v1` envelopes.
    pub fn poll(&mut self, conn: &mut Conn, count: usize) -> Vec<Entry> {
        self.poll_physical(conn, count)
            .into_iter()
            .map(|entry| Entry::from(&entry.stream, &entry.id, &entry.fields))
            .collect()
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
    /// `loaded`, `unloading`, `enabled`, `disabled`, `repinned`, or `flushed`.
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
            key: Vec::new(),
            db: String::new(),
        };
        assert!(mk("100-0").sort_key() < mk("100-1").sort_key());
        assert!(mk("100-9").sort_key() < mk("101-0").sort_key());
        assert_eq!(mk("bad").sort_key(), (0, 0));
    }

    #[test]
    fn entry_preserves_binary_key_bytes() {
        let key = vec![0xff, 0x00, b'a'];
        let map = HashMap::from([
            ("event".to_string(), Value::BulkString(b"set".to_vec())),
            ("key".to_string(), Value::BulkString(key.clone())),
            ("db".to_string(), Value::BulkString(b"0".to_vec())),
        ]);

        let entry = Entry::from("events:set", "1-0", &map);

        assert_eq!(entry.key, key);
        assert_eq!(entry.event, "set");
        assert_eq!(entry.db, "0");
        assert!(format!("{entry}").ends_with(r"key=\xff\x00a"));
    }

    fn batch_entry(id: &str, count: &str, events: &str) -> PhysicalEntry {
        PhysicalEntry::new(
            "events:set",
            id,
            HashMap::from([
                (
                    "format".to_string(),
                    Value::BulkString(b"batch-v1".to_vec()),
                ),
                (
                    "count".to_string(),
                    Value::BulkString(count.as_bytes().to_vec()),
                ),
                (
                    "events".to_string(),
                    Value::BulkString(events.as_bytes().to_vec()),
                ),
            ]),
        )
    }

    #[test]
    fn decoder_expands_fixed_entry_with_physical_identity() {
        let key = vec![0xff, 0x00, b'a'];
        let entry = PhysicalEntry::new(
            "events:set",
            "10-0",
            HashMap::from([
                ("event".to_string(), Value::BulkString(b"set".to_vec())),
                ("key".to_string(), Value::BulkString(key.clone())),
                ("db".to_string(), Value::BulkString(b"2".to_vec())),
            ]),
        );

        let decoded = entry.decode().expect("decode fixed entry");

        assert_eq!(
            decoded,
            vec![LogicalEvent {
                source: SourcePosition {
                    physical: PhysicalPosition {
                        stream: "events:set".to_string(),
                        entry_id: "10-0".to_string(),
                    },
                    envelope_index: None,
                },
                event: "set".to_string(),
                key,
                db: 2,
                class: None,
            }]
        );
    }

    #[test]
    fn decoder_expands_batch_v1_in_array_order() {
        let entry = batch_entry(
            "20-0",
            "2",
            r#"[{"event":"set","key":"/wBh","db":0,"class":"string"},{"event":"set","key":"Yg==","db":1,"class":"string"}]"#,
        );

        let decoded = entry.decode().expect("decode batch-v1");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, vec![0xff, 0x00, b'a']);
        assert_eq!(decoded[1].key, b"b");
        assert_eq!(decoded[0].source.envelope_index, Some(0));
        assert_eq!(decoded[1].source.envelope_index, Some(1));
        assert_eq!(decoded[0].source.physical, decoded[1].source.physical);
        assert_eq!(decoded[1].db, 1);
        assert_eq!(decoded[1].class.as_deref(), Some("string"));
    }

    #[test]
    fn decoder_preserves_mixed_physical_and_envelope_order() {
        let fixed = PhysicalEntry::new(
            "events:set",
            "30-0",
            HashMap::from([
                ("event".to_string(), Value::BulkString(b"set".to_vec())),
                ("key".to_string(), Value::BulkString(b"fallback".to_vec())),
                ("db".to_string(), Value::BulkString(b"0".to_vec())),
            ]),
        );
        let batch = batch_entry(
            "31-0",
            "2",
            r#"[{"event":"set","key":"Zmlyc3Q=","db":0,"class":"string"},{"event":"set","key":"c2Vjb25k","db":0,"class":"string"}]"#,
        );

        let decoded: Vec<LogicalEvent> = [&fixed, &batch]
            .into_iter()
            .flat_map(|entry| entry.decode().expect("decode mixed entry"))
            .collect();

        assert_eq!(
            decoded
                .iter()
                .map(|event| event.key.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"fallback".as_slice(),
                b"first".as_slice(),
                b"second".as_slice()
            ]
        );
        assert_eq!(
            decoded
                .iter()
                .map(|event| event.source.envelope_index)
                .collect::<Vec<_>>(),
            vec![None, Some(0), Some(1)]
        );
    }

    #[test]
    fn decoder_rejects_malformed_batch_metadata_with_context() {
        let invalid_count = batch_entry("40-0", "two", "[]")
            .decode()
            .expect_err("invalid count must fail");
        assert_eq!(
            invalid_count.physical,
            PhysicalPosition {
                stream: "events:set".to_string(),
                entry_id: "40-0".to_string(),
            }
        );
        assert!(matches!(
            invalid_count.kind,
            DecodeErrorKind::InvalidInteger { field: "count", .. }
        ));

        let invalid_json = batch_entry("41-0", "1", "{")
            .decode()
            .expect_err("invalid JSON must fail");
        assert!(matches!(invalid_json.kind, DecodeErrorKind::InvalidJson(_)));

        let mismatch = batch_entry(
            "42-0",
            "2",
            r#"[{"event":"set","key":"YQ==","db":0,"class":"string"}]"#,
        )
        .decode()
        .expect_err("count mismatch must fail");
        assert_eq!(
            mismatch.kind,
            DecodeErrorKind::CountMismatch {
                declared: 2,
                decoded: 1,
            }
        );
    }

    #[test]
    fn decoder_rejects_unknown_format_and_invalid_key_encoding() {
        let unknown = PhysicalEntry::new(
            "events:set",
            "50-0",
            HashMap::from([(
                "format".to_string(),
                Value::BulkString(b"batch-v2".to_vec()),
            )]),
        )
        .decode()
        .expect_err("unknown format must fail");
        assert_eq!(
            unknown.kind,
            DecodeErrorKind::UnsupportedFormat("batch-v2".to_string())
        );

        let invalid_key = batch_entry(
            "51-0",
            "1",
            r#"[{"event":"set","key":"not base64","db":0,"class":"string"}]"#,
        )
        .decode()
        .expect_err("invalid key encoding must fail");
        assert!(matches!(
            invalid_key.kind,
            DecodeErrorKind::InvalidBase64 {
                envelope_index: 0,
                ..
            }
        ));
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
