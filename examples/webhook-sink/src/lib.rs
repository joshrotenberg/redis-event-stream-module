//! Reusable pieces of the webhook sink example.
//!
//! The binary keeps Redis orchestration intentionally small; this library
//! carries the record decoder, HTTP delivery boundary, retry policy, and
//! consumer-group commands so those semantics can be unit-tested.

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use eventstream_client::{discover_streams, event_name, Conn, Target};
use redis::streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadReply};
use redis::{Cmd, Commands, RedisError, RedisResult, Value};
use serde::{Deserialize, Serialize};

/// The JSON envelope sent downstream.
///
/// `stream` plus `id` is the natural idempotency identity. Keys are always
/// standard padded base64 because Redis keys are arbitrary bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutboundRecord {
    Event {
        stream: String,
        id: String,
        event: String,
        key_base64: String,
        db: i64,
    },
    Gap {
        stream: String,
        id: String,
        action: String,
        module_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        db: Option<i64>,
    },
}

impl OutboundRecord {
    /// Stable retry/deduplication key sent as `Idempotency-Key`.
    pub fn idempotency_key(&self) -> String {
        match self {
            Self::Event { stream, id, .. } | Self::Gap { stream, id, .. } => {
                format!("{stream}/{id}")
            }
        }
    }
}

#[derive(Deserialize)]
struct JsonEntry {
    event: String,
    key: String,
    db: i64,
}

fn field_bytes(map: &HashMap<String, Value>, name: &str) -> Option<Vec<u8>> {
    match map.get(name)? {
        Value::BulkString(bytes) => Some(bytes.clone()),
        Value::SimpleString(value) => Some(value.as_bytes().to_vec()),
        Value::Int(value) => Some(value.to_string().into_bytes()),
        _ => None,
    }
}

fn field_text(map: &HashMap<String, Value>, name: &str) -> Option<String> {
    field_bytes(map, name).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

fn parse_i64(map: &HashMap<String, Value>, name: &str) -> Result<i64, String> {
    let raw = field_text(map, name).ok_or_else(|| format!("missing {name} field"))?;
    raw.parse()
        .map_err(|_| format!("invalid {name} field {raw:?}"))
}

/// Decode a module entry into the downstream envelope.
///
/// Fixed, minimal, verbose, and JSON entry formats are supported. The sink
/// reads per-event streams only, so a minimal entry can recover its event name
/// from the stream suffix without the firehose ambiguity.
pub fn decode_record(
    prefix: &str,
    stream: &str,
    id: &str,
    map: &HashMap<String, Value>,
) -> Result<OutboundRecord, String> {
    if event_name(prefix, stream).as_deref() == Some("#control") {
        return Ok(OutboundRecord::Gap {
            stream: stream.to_string(),
            id: id.to_string(),
            action: field_text(map, "action").ok_or_else(|| "missing action field".to_string())?,
            module_version: field_text(map, "module-version"),
            db: field_text(map, "db").and_then(|value| value.parse().ok()),
        });
    }

    let format = field_text(map, "format").unwrap_or_else(|| "fixed".to_string());
    let (event, key_base64, db) = match format.as_str() {
        "json" => {
            let document =
                field_bytes(map, "data").ok_or_else(|| "missing data field".to_string())?;
            let decoded: JsonEntry = serde_json::from_slice(&document)
                .map_err(|error| format!("invalid json entry: {error}"))?;
            let key = BASE64_STANDARD
                .decode(decoded.key.as_bytes())
                .map_err(|error| format!("invalid base64 key in json entry: {error}"))?;
            (decoded.event, BASE64_STANDARD.encode(key), decoded.db)
        }
        "fixed" | "minimal" | "verbose" => {
            let event = field_text(map, "event")
                .or_else(|| event_name(prefix, stream))
                .filter(|name| !name.starts_with('#'))
                .ok_or_else(|| "entry has no event identity".to_string())?;
            let key = field_bytes(map, "key").ok_or_else(|| "missing key field".to_string())?;
            (event, BASE64_STANDARD.encode(key), parse_i64(map, "db")?)
        }
        other => return Err(format!("unsupported entry format {other:?}")),
    };

    Ok(OutboundRecord::Event {
        stream: stream.to_string(),
        id: id.to_string(),
        event,
        key_base64,
        db,
    })
}

/// Event streams from the persistent registry, excluding the module-owned
/// `#` namespace (control, registry, and firehose).
pub fn data_streams(target: &Target) -> Vec<String> {
    discover_streams(target)
        .into_iter()
        .filter(|stream| {
            event_name(&target.prefix, stream)
                .map(|event| !event.starts_with('#'))
                .unwrap_or(false)
        })
        .collect()
}

/// One entry returned by a consumer-group read or claim.
pub struct RedisEntry {
    pub stream: String,
    pub id: String,
    pub fields: HashMap<String, Value>,
}

fn flatten(reply: StreamReadReply) -> Vec<RedisEntry> {
    let mut entries = Vec::new();
    for key in reply.keys {
        for id in key.ids {
            entries.push(RedisEntry {
                stream: key.key.clone(),
                id: id.id,
                fields: id.map,
            });
        }
    }
    entries
}

fn claimed(stream: &str, ids: Vec<StreamId>) -> Vec<RedisEntry> {
    ids.into_iter()
        .map(|id| RedisEntry {
            stream: stream.to_string(),
            id: id.id,
            fields: id.map,
        })
        .collect()
}

/// Create a group at `0` so retained history is eligible. Existing groups are
/// accepted; the example never resets an operator's group cursor.
pub fn ensure_group(conn: &mut Conn, stream: &str, group: &str) -> RedisResult<()> {
    let result: RedisResult<()> = Cmd::new()
        .arg("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query(conn);
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
        Err(error) => Err(error),
    }
}

/// Drain entries already pending under this consumer name before reading `>`.
pub fn read_own_pending(
    conn: &mut Conn,
    stream: &str,
    group: &str,
    consumer: &str,
    count: usize,
) -> RedisResult<Vec<RedisEntry>> {
    let reply: StreamReadReply = Cmd::new()
        .arg("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(count)
        .arg("STREAMS")
        .arg(stream)
        .arg("0")
        .query(conn)?;
    Ok(flatten(reply))
}

/// Claim abandoned work. `next` is fed into the next call until Redis returns
/// `0-0`; `deleted_ids` were trimmed while pending and cannot be delivered.
pub struct ClaimBatch {
    pub next: String,
    pub entries: Vec<RedisEntry>,
    pub deleted_ids: Vec<String>,
}

pub fn claim_abandoned(
    conn: &mut Conn,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
    start: &str,
    count: usize,
) -> RedisResult<ClaimBatch> {
    let options = StreamAutoClaimOptions::default().count(count);
    let reply: StreamAutoClaimReply =
        conn.xautoclaim_options(stream, group, consumer, min_idle_ms, start, options)?;
    Ok(ClaimBatch {
        next: reply.next_stream_id,
        entries: claimed(stream, reply.claimed),
        deleted_ids: reply.deleted_ids,
    })
}

/// Read never-delivered entries from all currently discovered streams.
pub fn read_new(
    conn: &mut Conn,
    streams: &[String],
    group: &str,
    consumer: &str,
    count: usize,
    block_ms: u64,
) -> RedisResult<Vec<RedisEntry>> {
    if streams.is_empty() {
        return Ok(Vec::new());
    }
    let mut command = Cmd::new();
    command
        .arg("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(count);
    if block_ms > 0 {
        command.arg("BLOCK").arg(block_ms);
    }
    command.arg("STREAMS");
    for stream in streams {
        command.arg(stream);
    }
    for _ in streams {
        command.arg(">");
    }
    let reply: StreamReadReply = command.query(conn)?;
    Ok(flatten(reply))
}

/// Acknowledge only after downstream delivery succeeds.
pub fn acknowledge(conn: &mut Conn, stream: &str, group: &str, id: &str) -> RedisResult<i64> {
    Cmd::new()
        .arg("XACK")
        .arg(stream)
        .arg(group)
        .arg(id)
        .query(conn)
}

/// HTTP delivery boundary: any 2xx response is durable acceptance.
pub struct WebhookSink {
    client: reqwest::blocking::Client,
    url: String,
}

impl WebhookSink {
    pub fn new(url: impl Into<String>, timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?,
            url: url.into(),
        })
    }

    pub fn deliver(&self, record: &OutboundRecord) -> Result<(), String> {
        let response = self
            .client
            .post(&self.url)
            .header("Idempotency-Key", record.idempotency_key())
            .json(record)
            .send()
            .map_err(|error| error.to_string())?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().unwrap_or_default();
        let excerpt: String = body.chars().take(256).collect();
        Err(format!("webhook returned {status}: {excerpt}"))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// `0` retries forever. A finite failure exits without acknowledging.
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

/// Retry one record with capped exponential backoff.
///
/// `on_retry` makes failures observable without baking a logging framework
/// into the example. The returned count includes the successful attempt.
pub fn deliver_with_retry<Deliver, Sleep, Notify>(
    record: &OutboundRecord,
    policy: RetryPolicy,
    mut deliver: Deliver,
    mut sleep: Sleep,
    mut on_retry: Notify,
) -> Result<u32, String>
where
    Deliver: FnMut(&OutboundRecord) -> Result<(), String>,
    Sleep: FnMut(Duration),
    Notify: FnMut(u32, &str, Duration),
{
    let mut attempt = 0u32;
    let mut delay = policy.initial_delay;
    loop {
        attempt += 1;
        match deliver(record) {
            Ok(()) => return Ok(attempt),
            Err(error) if policy.max_attempts > 0 && attempt >= policy.max_attempts => {
                return Err(error);
            }
            Err(error) => {
                on_retry(attempt, &error, delay);
                sleep(delay);
                delay = delay.saturating_mul(2).min(policy.max_delay);
            }
        }
    }
}

/// Production sleeper passed to [`deliver_with_retry`].
pub fn sleep(delay: Duration) {
    thread::sleep(delay);
}

/// Convert a decode failure into a Redis-style client error for callers that
/// want a single error type.
pub fn decode_error(message: String) -> RedisError {
    RedisError::from((
        redis::ErrorKind::UnexpectedReturnType,
        "invalid stream entry",
        message,
    ))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    use super::*;

    fn bulk(value: impl AsRef<[u8]>) -> Value {
        Value::BulkString(value.as_ref().to_vec())
    }

    #[test]
    fn fixed_entry_preserves_binary_key_as_base64() {
        let map = HashMap::from([
            ("event".to_string(), bulk("set")),
            ("key".to_string(), bulk([0xff, 0x00, b'a'])),
            ("db".to_string(), bulk("2")),
        ]);
        let record = decode_record("events:", "events:set", "10-0", &map).unwrap();
        assert_eq!(
            record,
            OutboundRecord::Event {
                stream: "events:set".to_string(),
                id: "10-0".to_string(),
                event: "set".to_string(),
                key_base64: "/wBh".to_string(),
                db: 2,
            }
        );
    }

    #[test]
    fn minimal_entry_derives_event_from_stream() {
        let map = HashMap::from([
            ("format".to_string(), bulk("minimal")),
            ("key".to_string(), bulk("session:1")),
            ("db".to_string(), bulk("0")),
        ]);
        let record = decode_record("events:", "events:expired", "11-0", &map).unwrap();
        assert!(matches!(
            record,
            OutboundRecord::Event { event, .. } if event == "expired"
        ));
    }

    #[test]
    fn json_entry_validates_and_normalizes_base64() {
        let map = HashMap::from([
            ("format".to_string(), bulk("json")),
            (
                "data".to_string(),
                bulk(r#"{"event":"hset","key":"a2V5","db":0}"#),
            ),
        ]);
        let record = decode_record("events:", "events:hset", "12-0", &map).unwrap();
        assert!(matches!(
            record,
            OutboundRecord::Event {
                event,
                key_base64,
                ..
            } if event == "hset" && key_base64 == "a2V5"
        ));
    }

    #[test]
    fn gap_marker_is_a_distinct_record_type() {
        let map = HashMap::from([
            ("action".to_string(), bulk("flushed")),
            ("module-version".to_string(), bulk("0.3.0")),
            ("db".to_string(), bulk("-1")),
        ]);
        let record = decode_record("events:", "events:#control", "13-0", &map).unwrap();
        assert_eq!(
            record,
            OutboundRecord::Gap {
                stream: "events:#control".to_string(),
                id: "13-0".to_string(),
                action: "flushed".to_string(),
                module_version: Some("0.3.0".to_string()),
                db: Some(-1),
            }
        );
    }

    #[test]
    fn retry_stops_after_success() {
        let record = OutboundRecord::Gap {
            stream: "events:#control".to_string(),
            id: "14-0".to_string(),
            action: "loaded".to_string(),
            module_version: None,
            db: None,
        };
        let mut calls = 0;
        let mut delays = Vec::new();
        let attempts = deliver_with_retry(
            &record,
            RetryPolicy {
                max_attempts: 3,
                initial_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(100),
            },
            |_| {
                calls += 1;
                if calls < 3 {
                    Err("not yet".to_string())
                } else {
                    Ok(())
                }
            },
            |delay| delays.push(delay),
            |_, _, _| {},
        )
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            delays,
            vec![Duration::from_millis(10), Duration::from_millis(20)]
        );
    }

    #[test]
    fn webhook_posts_json_with_idempotency_key() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sent, received) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let header_end = request.windows(4).position(|w| w == b"\r\n\r\n");
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&request[..end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + content_length {
                        break;
                    }
                }
            }
            sent.send(String::from_utf8_lossy(&request).into_owned())
                .unwrap();
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });

        let sink = WebhookSink::new(url, Duration::from_secs(2)).unwrap();
        let record = OutboundRecord::Event {
            stream: "events:set".to_string(),
            id: "15-0".to_string(),
            event: "set".to_string(),
            key_base64: "a2V5".to_string(),
            db: 0,
        };
        sink.deliver(&record).unwrap();
        server.join().unwrap();
        let request = received.recv().unwrap();
        assert!(request.contains("idempotency-key: events:set/15-0"));
        assert!(request.contains(r#""key_base64":"a2V5""#));
    }
}
