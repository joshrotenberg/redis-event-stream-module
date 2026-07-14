// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! The capture hot path (#86): event-name sanitization, entry-format encoding
//! and the `EntrySpec`/`MirrorOutcome` types, the mirrored `XADD` writers
//! (`mirror_entry`/`mirror_firehose`), the keyspace-notification callback, and
//! the flush/SWAPDB server-event handlers.

use crate::cluster::*;
use crate::config::*;
use crate::markers::*;
use crate::stats::*;
use redis_module::{
    raw, CallOptions, CallOptionsBuilder, CallResult, Context, ContextFlags, NotifyEvent, Status,
};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(test))]
use redis_module::RedisString;
#[cfg(not(test))]
use std::ffi::CStr;
#[cfg(not(test))]
use std::os::raw::{c_char, c_int, c_void};

/// Process-global monotonic sequence (issue #66): a per-node, per-process total
/// order across all destination streams for entries that share a millisecond
/// (the stream entry ID only orders within one stream). `fetch_add`-ed once per
/// captured event when `entry-seq` is on, and the same value is written to both
/// the per-event entry and its firehose copy (one event, one number). Starts at
/// 0 and resets on load like the section 13 counters: it is never persisted or
/// replicated as state (the assigned value replicates verbatim inside the
/// `XADD`, so replicas and AOF replay preserve it). A consumer uses `seq` only
/// for intra-process same-ms tiebreaking; cross-restart and cross-node ties
/// still fall back to the entry ID / an application timestamp (SPEC.md section
/// 9).
pub(crate) static SEQ: AtomicU64 = AtomicU64::new(0);

/// Sanitize an event name into a stream-key suffix (SPEC.md section 5):
/// `A-Z a-z 0-9 _ . : -` pass through, anything else becomes `_`, truncated
/// to 128 bytes. Every built-in and known module event name passes through
/// byte-identical. An empty result means the event is not routable.
pub(crate) fn sanitize(event: &str) -> String {
    event
        .chars()
        .take(MAX_EVENT_NAME_LEN)
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '.' | ':' | '-' => c,
            _ => '_',
        })
        .collect()
}

pub(crate) fn xadd_call_options() -> CallOptions {
    // `!` replicate, `E` errors as replies, `M` respect maxmemory
    // (SPEC.md section 10). The `M` flag is conditional on `eventstream.verify-oom`
    // (issue #65): with it off, mirrored writes are not refused under maxmemory,
    // so capture continues at the memory limit at the documented cost (SPEC.md
    // section 11). Read per XADD, so a live CONFIG SET needs no cached-options
    // invalidation and applies to mirrored entries, gap markers, and firehose
    // copies uniformly.
    let builder = CallOptionsBuilder::new().replicate().errors_as_replies();
    let builder = if VERIFY_OOM.load(Ordering::Relaxed) {
        builder.verify_oom()
    } else {
        builder
    };
    builder.build()
}

/// Server wall-clock in milliseconds via `RedisModule_Milliseconds` — the same
/// clock the server uses to stamp auto-generated stream entry IDs (`<ms>-<seq>`,
/// SPEC.md section 6), so a `MINID` threshold derived from it selects entries by
/// their own timestamps (issue #108). Called only on the write path (GIL held,
/// write-safe context); `mstime_t` is `i64`.
pub(crate) fn server_now_ms() -> i64 {
    unsafe { raw::RedisModule_Milliseconds.unwrap()() }
}

/// The inline `XADD` trim policy for one write (issues #62, #108). `maxlen` is
/// the count cap already resolved for this stream (the per-event override or the
/// global `eventstream.maxlen`, issue #62). `retention_ms` is the time window
/// (`eventstream.retention-ms`, issue #108): when `> 0` the write trims by
/// `MINID` (time-based) and `maxlen` is ignored for that `XADD`, otherwise it
/// trims by `MAXLEN`. Redis accepts only one of the two clauses per `XADD`, so
/// the two are mutually exclusive by construction (SPEC.md section 7). `Copy`,
/// so it threads through the write helpers without borrow bookkeeping.
#[derive(Clone, Copy)]
pub(crate) struct Retention {
    pub(crate) maxlen: i64,
    pub(crate) retention_ms: i64,
}

impl Retention {
    /// Whether this write trims by time (`MINID`) rather than count (`MAXLEN`).
    pub(crate) fn is_time_based(&self) -> bool {
        self.retention_ms > 0
    }

    /// The inline trim clause: `Some((keyword, threshold))` to push as
    /// `<keyword> ~ <threshold>`, or `None` for no trimming. Time-based `MINID`
    /// takes precedence over count-based `MAXLEN` (issue #108). `now_ms` is the
    /// server clock ([`server_now_ms`]), consulted only on the `MINID` path; the
    /// threshold is `now_ms - retention_ms` clamped at 0 and formatted `<ms>-0`
    /// so it names a full stream ID (`<ms>-<seq>`, SPEC.md section 6). Returns an
    /// owned threshold string the caller borrows into the `XADD` arg vector.
    pub(crate) fn trim_clause(&self, now_ms: i64) -> Option<(&'static [u8], String)> {
        if self.retention_ms > 0 {
            let threshold = now_ms.saturating_sub(self.retention_ms).max(0);
            Some((&b"MINID"[..], format!("{threshold}-0")))
        } else if self.maxlen > 0 {
            Some((&b"MAXLEN"[..], self.maxlen.to_string()))
        } else {
            None
        }
    }
}

/// The field-shaping inputs for one mirrored entry (issues #60, #66): captured
/// once per captured event so the per-event entry and its firehose copy get an
/// identical field set and the same `seq`. `event`/`key`/`db` are the section 6
/// values; `class` carries the notification-class bits for the `verbose`
/// format's `class` field; `seq` is `Some` only when `entry-seq` is on.
#[derive(Clone, Copy)]
pub(crate) struct EntrySpec<'a> {
    pub(crate) format: EntryFormat,
    pub(crate) event: &'a [u8],
    pub(crate) key: &'a [u8],
    pub(crate) db: &'a str,
    pub(crate) class: NotifyEvent,
    pub(crate) seq: Option<u64>,
}

/// Human-readable class name(s) for the `verbose` format's `class` field (issue
/// #60): the notification-class bit(s) Redis delivered the event under, joined
/// by `,` if more than one is set, empty if none is recognized. Names match the
/// `class_bit` grammar (SPEC.md section 7); a single keyspace event normally
/// carries exactly one class bit.
pub(crate) fn class_names(class: NotifyEvent) -> String {
    let named = [
        (NotifyEvent::GENERIC, "generic"),
        (NotifyEvent::STRING, "string"),
        (NotifyEvent::LIST, "list"),
        (NotifyEvent::SET, "set"),
        (NotifyEvent::HASH, "hash"),
        (NotifyEvent::ZSET, "zset"),
        (NotifyEvent::STREAM, "stream"),
        (NotifyEvent::EXPIRED, "expired"),
        (NotifyEvent::EVICTED, "evicted"),
        (NotifyEvent::MODULE, "module"),
        (NotifyEvent::MISSED, "missed"),
        (NotifyEvent::NEW, "new"),
    ];
    named
        .iter()
        .filter(|(bit, _)| class.intersects(*bit))
        .map(|(_, name)| *name)
        .collect::<Vec<_>>()
        .join(",")
}

/// Append `s` to `out` as a JSON string literal, quotes included, escaping the
/// characters JSON requires (`"`, `\`, and the C0 controls). Used only by the
/// `json` entry format.
pub(crate) fn json_escape_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Append `bytes` to `out` as standard (RFC 4648) base64 with padding. Key
/// bytes are arbitrary binary, so the `json` format base64-encodes them (the
/// reason JSON was rejected as the only format, SPEC.md section 6); hand-rolled
/// rather than adding a dependency (no `cargo fetch` on this crate's cadence).
pub(crate) fn base64_into(bytes: &[u8], out: &mut String) {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
}

/// Encode one entry as a compact JSON object for the `json` format (issue #60):
/// `event` JSON-string-escaped, `key` base64-encoded (arbitrary binary), `db`
/// as a JSON number (always a valid non-negative integer here), and `seq`
/// (issue #66) as a number only when `entry-seq` is on. Kept as one document
/// field so the format stays "a single field holding a JSON document".
pub(crate) fn json_document(event: &str, key: &[u8], db: &str, seq: Option<u64>) -> String {
    let mut s = String::from("{\"event\":");
    json_escape_into(event, &mut s);
    s.push_str(",\"key\":\"");
    base64_into(key, &mut s);
    s.push_str("\",\"db\":");
    s.push_str(db);
    if let Some(seq) = seq {
        s.push_str(",\"seq\":");
        s.push_str(&seq.to_string());
    }
    s.push('}');
    s
}

/// Build the field name/value pairs for one mirrored entry, in the stable order
/// the chosen format defines (issue #60). Returns owned buffers the caller
/// borrows into the `XADD` argument vector; a stable per-format order keeps the
/// `SAMEFIELDS` listpack compaction within a run of same-format entries (SPEC.md
/// section 6). `Err` means the entry could not be encoded and must be dropped
/// and counted in `dropped_encode_error`: with the shipped formats only `json`
/// can fail, on a non-UTF-8 event name (nothing type-guarantees UTF-8 at this
/// layer, so the check is real, though the raw callback's lossy decode makes it
/// unreachable through the normal notification path).
pub(crate) fn encode_entry_fields(spec: &EntrySpec) -> Result<Vec<Vec<u8>>, &'static str> {
    // `json` carries `seq` inside its document, so it returns before the
    // trailing top-level `seq` field the other formats append.
    if spec.format == EntryFormat::json {
        let event = std::str::from_utf8(spec.event).map_err(|_| "event name is not valid UTF-8")?;
        let doc = json_document(event, spec.key, spec.db, spec.seq);
        return Ok(vec![
            b"format".to_vec(),
            b"json".to_vec(),
            b"data".to_vec(),
            doc.into_bytes(),
        ]);
    }
    let mut f: Vec<Vec<u8>> = Vec::with_capacity(10);
    match spec.format {
        EntryFormat::fixed => {
            // Historical schema, no discriminator (byte-identical to pre-#60).
            f.push(b"event".to_vec());
            f.push(spec.event.to_vec());
            f.push(b"key".to_vec());
            f.push(spec.key.to_vec());
            f.push(b"db".to_vec());
            f.push(spec.db.as_bytes().to_vec());
        }
        EntryFormat::minimal => {
            // Drops the `event` field (redundant on a per-event stream); the
            // `format` discriminator disambiguates it from a `fixed` entry that
            // happens to omit fields (SPEC.md section 6).
            f.push(b"format".to_vec());
            f.push(b"minimal".to_vec());
            f.push(b"key".to_vec());
            f.push(spec.key.to_vec());
            f.push(b"db".to_vec());
            f.push(spec.db.as_bytes().to_vec());
        }
        EntryFormat::verbose => {
            // Adds the notification `class` after the fixed fields.
            f.push(b"format".to_vec());
            f.push(b"verbose".to_vec());
            f.push(b"event".to_vec());
            f.push(spec.event.to_vec());
            f.push(b"key".to_vec());
            f.push(spec.key.to_vec());
            f.push(b"db".to_vec());
            f.push(spec.db.as_bytes().to_vec());
            f.push(b"class".to_vec());
            f.push(class_names(spec.class).into_bytes());
        }
        EntryFormat::json => unreachable!("json handled above"),
    }
    // Global `seq` field (issue #66), appended last so the per-format field
    // order stays stable; emitted for every non-json format when enabled.
    if let Some(seq) = spec.seq {
        f.push(b"seq".to_vec());
        f.push(seq.to_string().into_bytes());
    }
    Ok(f)
}

/// Classification of one mirrored-write attempt, so the caller can decide
/// whether to re-pin and retry.
pub(crate) enum MirrorOutcome {
    /// The entry was written (and its stream registered on first sight).
    Written,
    /// The pinned slot is no longer local: re-pin to a new owned slot and retry.
    SlotMigrated,
    /// The pinned slot is mid-migration (`TRYAGAIN`/`ASK`, issue #75): an
    /// early re-pin signal, handled like [`MirrorOutcome::SlotMigrated`] but
    /// counted as `dropped_migrating` if the retry is also refused. Carries
    /// the server's error text so the first-failure log names the actual
    /// refusal (TRYAGAIN vs ASK, slot, target node).
    Migrating(String),
    /// Refused under `maxmemory`. Carries the destination stream so the
    /// caller can attribute the drop per stream (issue #68) without
    /// reassembling the name.
    Oom { stream: String, msg: String },
    /// Any other `XADD` failure; stream carried as in [`MirrorOutcome::Oom`].
    Failed { stream: String, msg: String },
    /// Refused before the `XADD` because the destination stream is new and
    /// `eventstream.max-streams` is reached (issue #64): the stream was never
    /// created. Carries the rejected name so the first-failure log names it.
    MaxStreams { stream: String },
    /// The configured `entry-format` could not encode the event (issue #60):
    /// with the shipped formats only `json` fails, on a non-UTF-8 event name.
    /// Refused before the `XADD`; counted in `dropped_encode_error`. Carries
    /// the destination stream and the static reason for the first-failure log.
    EncodeError {
        stream: String,
        reason: &'static str,
    },
}

/// Provision the `eventstream.auto-group` consumer group on a destination
/// stream just written (issue #109): `XGROUP CREATE <stream> <group> 0`, at ID
/// `0` so the group sees the whole retained stream (not just entries after its
/// creation — the module-before-consumers deployment order then matches the
/// consumers-first order, SPEC.md section 9). Uses the same replicated,
/// OOM-checked options as the mirrored `XADD`, so the group replicates and
/// persists with the entry that triggered it. Idempotent: BUSYGROUP (the group
/// already exists, e.g. on a stream that outlived a restart, or a concurrent
/// write beat this one) is treated as success. Returns `true` when the group
/// now exists (created or already present) so the caller marks it done; `false`
/// on a real failure, which is counted and left un-deduped so the next write to
/// the stream retries. Runs GIL-held in a write-safe context (a
/// post-notification job), with `STREAM_STATS` locked by the caller — the same
/// context and lock discipline as the sibling registry `SADD`.
pub(crate) fn create_auto_group(ctx: &Context, stream: &str, group: &str) -> bool {
    let res: CallResult = ctx.call_ext(
        "XGROUP",
        &xadd_call_options(),
        &[
            &b"CREATE"[..],
            stream.as_bytes(),
            group.as_bytes(),
            &b"0"[..],
        ][..],
    );
    match res {
        Ok(_) => {
            AUTOGROUP_CREATED.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_default();
            if msg.starts_with("BUSYGROUP") {
                // The group already exists: idempotent success, not a new
                // creation, so it is deduped but not counted (SPEC.md section 9).
                true
            } else {
                count_drop(
                    ctx,
                    &AUTOGROUP_FAILED,
                    &LOGGED_AUTOGROUP,
                    &format!(
                        "XGROUP CREATE {group} on '{stream}' failed: {msg} \
                         (autogroup_failed); event captured, group not provisioned"
                    ),
                );
                false
            }
        }
    }
}

/// Write one mirrored entry to `<prefix><seg><suffix>`, and on the first write
/// to a stream register it in `<prefix><seg>#streams`. A success increments
/// `counter`: `FORWARDED` for per-event entries, `FIREHOSE_FORWARDED` for
/// firehose copies (issue #58), so `forwarded` stays a pure captured-event
/// count; the stream's own record in `STREAM_STATS` counts either kind, so
/// the firehose entry's per-stream `forwarded` is the per-stream view of
/// `firehose_forwarded` (issue #71). Returns a classified outcome; the caller
/// counts drops and, on [`MirrorOutcome::SlotMigrated`], re-pins. Runs only
/// in a write-safe context (a post-notification job).
#[allow(clippy::too_many_arguments)]
pub(crate) fn mirror_entry(
    ctx: &Context,
    prefix: &str,
    seg: &str,
    suffix: &str,
    spec: &EntrySpec,
    retention: Retention,
    max_streams: i64,
    counter: &AtomicU64,
) -> MirrorOutcome {
    let stream = format!("{prefix}{seg}{suffix}");
    let registry = format!("{prefix}{seg}#streams");

    // Max-streams cap (issue #64): before the XADD, refuse to create a new
    // event-derived destination stream once the cap is reached. Streams already
    // registered in this process keep receiving events; only new-stream
    // creation is blocked. `CURRENT_STREAMS` is the currently-registered count
    // (resets on flush), so the cap tracks distinct streams the registry cache
    // knows about, matching `active_streams`. `max_streams` 0 is unlimited; the
    // firehose, control stream, and gap markers pass 0 (exempt: the `#`
    // namespace is not event-derived and markers must never be dropped).
    if max_streams > 0 {
        let stats = STREAM_STATS.lock(ctx);
        let known = stats.get(&stream).is_some_and(|s| s.registered);
        if !known && CURRENT_STREAMS.load(Ordering::Relaxed) >= max_streams {
            return MirrorOutcome::MaxStreams { stream };
        }
    }

    // Build the format's field set (issues #60, #66) before the XADD. An
    // unencodable entry (only `json`, on a non-UTF-8 event name) is refused
    // here and counted in `dropped_encode_error`; it never reaches the server.
    let fields = match encode_entry_fields(spec) {
        Ok(f) => f,
        Err(reason) => return MirrorOutcome::EncodeError { stream, reason },
    };

    // The inline trim clause (issues #62, #108): count-based `MAXLEN ~ <cap>`
    // with the per-event-resolved cap, or time-based `MINID ~ <now-window>` when
    // `retention-ms` is set (MINID wins). Held outside the args vec so its bytes
    // outlive the `&[u8]` borrow below.
    let trim = retention.trim_clause(if retention.is_time_based() {
        server_now_ms()
    } else {
        0
    });
    let mut args: Vec<&[u8]> = Vec::with_capacity(5 + fields.len());
    args.push(stream.as_bytes());
    if let Some((keyword, ref threshold)) = trim {
        args.push(keyword);
        args.push(&b"~"[..]);
        args.push(threshold.as_bytes());
    }
    args.push(&b"*"[..]);
    for f in &fields {
        args.push(f.as_slice());
    }

    // Per-event trace (SPEC.md section 13); the server filters by loglevel. Key
    // bytes are ASCII-escaped: the wrapper's logger builds a CString and panics
    // across the FFI boundary on interior NUL, so raw key bytes (which may
    // contain NUL) must never reach it.
    ctx.log_debug(&format!(
        "eventstream: {} key={} -> {}",
        String::from_utf8_lossy(spec.event),
        spec.key.escape_ascii(),
        stream
    ));

    let res: CallResult = ctx.call_ext("XADD", &xadd_call_options(), args.as_slice());
    match res {
        Ok(_) => {
            counter.fetch_add(1, Ordering::Relaxed);
            // A successful write proves current ownership, so reset the probe
            // budget: without this, a tag verified once (after an unrelated
            // transient failure) would never be re-probed, and a later
            // migration under a reworded refusal message would degrade into
            // permanent unclassified drops — the exact failure issue #76
            // exists to prevent. Cost: one uncontended lock per written event
            // in per-node mode, matching the KNOWN_STREAMS lock below.
            if PER_NODE.load(Ordering::Relaxed) {
                *PROBE_VERIFIED_TAG.lock().unwrap() = None;
            }
            // Per-stream accounting under the map's one lock (issues #68,
            // #71): the counter, the recovery notice if the stream was
            // failing, and on the first write the registration in the
            // persistent set at `<prefix><seg>#streams` (replicated, so
            // EVENTSTREAM.STREAMS survives restart and works on replicas).
            // STREAM_STATS is the in-process cache behind the dedupe; it is
            // cleared on flush so a FLUSHALL that deleted the registry
            // rebuilds it on the next write. The registry key is under the
            // prefix, so its own SADD notification is dropped by the
            // feedback guard.
            let mut stats = STREAM_STATS.lock(ctx);
            if !stats.contains_key(&stream) {
                stats.insert(stream.clone(), StreamStats::default());
            }
            let entry = stats.get_mut(&stream).expect("inserted above");
            entry.forwarded += 1;
            if let Some(drops) = entry.record_success() {
                // Notice-level recovery line (issue #68): the stream was
                // dropping and writes again.
                ctx.log_notice(&format!(
                    "eventstream: {stream} recovered after {drops} drops"
                ));
            }
            if !entry.registered {
                let sadd: CallResult = ctx.call_ext(
                    "SADD",
                    &xadd_call_options(),
                    &[registry.as_bytes(), stream.as_bytes()][..],
                );
                if sadd.is_ok() {
                    entry.registered = true;
                    ACTIVE_STREAMS.fetch_add(1, Ordering::Relaxed);
                    // Currently-registered count backing the max-streams cap
                    // (issue #64); resets on flush with STREAM_STATS.
                    CURRENT_STREAMS.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Auto-group provisioning (issue #109): once `eventstream.auto-group`
            // names a group, create it on this stream, deduped per stream by
            // `group_created` (separate from `registered`, so enabling the
            // config provisions an already-registered stream on its next write,
            // and a flush that cleared the map re-creates a group it destroyed).
            // Runs under the same lock as the registry SADD. The control stream
            // never reaches here — markers write through `write_marker` — so it
            // is excluded by construction; per-event and firehose streams both
            // provision. Gated by the cheap atomic so the default-off path never
            // locks the config guard.
            if AUTO_GROUP_ENABLED.load(Ordering::Relaxed) && !entry.group_created {
                if let Some(group) = AUTO_GROUP.group_name(ctx) {
                    if create_auto_group(ctx, &stream, &group) {
                        entry.group_created = true;
                    }
                }
            }
            MirrorOutcome::Written
        }
        Err(e) => {
            let msg = e.to_utf8_string().unwrap_or_default();
            if msg.starts_with("OOM") {
                MirrorOutcome::Oom {
                    msg: format!("XADD to '{stream}' refused under maxmemory: {msg}"),
                    stream,
                }
            } else if is_slot_migrated(&msg) {
                MirrorOutcome::SlotMigrated
            } else if is_migration_refusal(&msg) {
                MirrorOutcome::Migrating(format!("XADD to '{stream}' refused mid-migration: {msg}"))
            } else {
                MirrorOutcome::Failed {
                    msg: format!("XADD to '{stream}' failed: {msg}"),
                    stream,
                }
            }
        }
    }
}

/// Write the firehose copy of a captured event (issue #58): a second `XADD`
/// to `<prefix><seg>#firehose` with fields identical to the per-event entry,
/// the same `MAXLEN ~` trimming and call options. Runs after the per-event
/// outcome is settled and succeeds or fails independently of it; a success
/// counts in `firehose_forwarded`, a failure in the existing `dropped_*`
/// counters. The tag segment is re-resolved so a re-pin performed by the
/// per-event write lands the copy on the new tag. A cluster refusal here is
/// counted, never re-pinned: slot ownership cannot change between the two
/// XADDs (both run inside one execution unit), so a refusal only occurs in a
/// migration window the per-event write already re-pinned through, and a
/// second re-pin for the same event would double the `repinned` marker and
/// the one-retry bound. The ownership-probe fallback is likewise left to the
/// per-event path, which hits the same failure on the same tag first.
pub(crate) fn mirror_firehose(ctx: &Context, prefix: &str, spec: &EntrySpec, retention: Retention) {
    let seg = match tag_segment(ctx) {
        Some(s) => s,
        None => {
            count_no_slot_drop(ctx);
            return;
        }
    };
    match mirror_entry(
        ctx,
        prefix,
        &seg,
        "#firehose",
        // The same `EntrySpec` as the per-event write, so the firehose copy has
        // an identical field set and the same `seq` (issues #60, #66): they are
        // one event written twice.
        spec,
        retention,
        // The firehose is exempt from the max-streams cap (issue #64): it is a
        // single `#`-namespaced stream, not event-derived, so it never
        // consumes a cap slot and is never blocked by it.
        0,
        &FIREHOSE_FORWARDED,
    ) {
        MirrorOutcome::Written => {}
        MirrorOutcome::Oom { stream, msg } => count_stream_drop(ctx, &stream, &DROPPED_OOM, &msg),
        MirrorOutcome::SlotMigrated => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            "firehose XADD refused as non-local; copy dropped in migration \
             window (dropped_migrating); one re-pin per event, owned by the \
             per-event write",
        ),
        MirrorOutcome::Migrating(msg) => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            &format!("{msg} (firehose copy dropped in migration window)"),
        ),
        MirrorOutcome::Failed { stream, msg } => {
            count_stream_drop(ctx, &stream, &DROPPED_XADD_ERROR, &msg)
        }
        // Unreachable: the firehose passes max_streams 0 (exempt), so the cap
        // gate never fires for it.
        MirrorOutcome::MaxStreams { .. } => {}
        // The per-event write with the same spec already hit and counted this
        // encode failure, but the two writes count drops independently (SPEC.md
        // section 5), so count the copy's failure too (issue #60).
        MirrorOutcome::EncodeError { stream, reason } => count_drop(
            ctx,
            &DROPPED_ENCODE_ERROR,
            &LOGGED_ENCODE_ERROR,
            &format!("entry-format encode failed for firehose copy '{stream}': {reason}; copy dropped (dropped_encode_error)"),
        ),
    }
}

/// Keyspace notification callback. Runs with the GIL held; keyspace writes are
/// unsafe here, so the XADD is deferred to a post-notification job. Gate order
/// follows the SPEC.md section 4 diagram.
pub(crate) fn on_keyspace_event(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    // 0. Pending gap markers flush ahead of the enabled gate: the first event
    // after a disable is exactly the boundary the disabled marker timestamps.
    if MARKERS_DIRTY.load(Ordering::Relaxed) {
        drain_pending_markers(ctx);
    }

    // 1. Master switch.
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }

    // 2. Feedback guard: our own XADD/XTRIM activity fires notifications on
    // `<prefix>*` keys; mirroring those would loop forever. Borrow, do not
    // clone: skip paths stay allocation-free (SPEC.md section 11 cost model).
    let prefix = PREFIX.value.lock(ctx);
    if key.starts_with(prefix.as_bytes()) {
        SKIPPED_SELF.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 3. Only a master that is not loading mirrors events; replicas receive
    // the mirrored entries via replication of the master's writes.
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }

    // 4. Event-name/class filter predicate.
    if !FILTER.parsed.lock(ctx).matches(event_type, event) {
        SKIPPED_FILTERED.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 5. Key-name glob filter (issue #61): AND with the events filter, checked
    // against the raw key bytes. The default `*` short-circuits (no byte scan);
    // a borrowed match keeps the skip path allocation-free (SPEC.md section 11).
    if !KEY_FILTER.parsed.lock(ctx).matches(key) {
        SKIPPED_KEY_FILTERED.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 6. Source-db filter (issue #63): the origin database, also recorded in
    // the entry's `db` field, is captured once here and reused for the entry.
    // The default `*` short-circuits; only db 0 exists in cluster mode. The
    // stream itself always lives in db 0 (SPEC.md section 6).
    let db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    if !SOURCE_DBS.parsed.lock(ctx).matches(db) {
        SKIPPED_DB.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // 7. Routable name.
    let suffix = sanitize(event);
    if suffix.is_empty() {
        SKIPPED_INVALID.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // Names are resolved in the job, not here: in per-node cluster mode the
    // hash tag is selected lazily (this node may own no slots yet), and that
    // probe must run in a write-safe context.
    let prefix_owned = prefix.as_str().to_owned();
    // Retention config snapshotted here so a mid-event CONFIG SET lands on whole
    // events (issues #62, #108), and resolved to the three destinations here,
    // where the event suffix is already final: the data stream takes its
    // per-event override (else the global cap), the control stream takes the
    // `#control` override, and the firehose uses the global cap (it aggregates
    // every event type, so it is not per-event-overridable, SPEC.md section 11).
    // `retention_ms`, when set, selects time-based MINID trimming for all three.
    // Resolving here keeps the job's captures `Copy` and avoids cloning the
    // override map into the job when overrides are configured.
    let global_maxlen = MAXLEN.value.load(Ordering::Relaxed);
    let retention_ms = RETENTION_MS.value.load(Ordering::Relaxed);
    let (data_ret, control_ret, firehose_ret) = {
        let overrides = MAXLEN_OVERRIDES.parsed.lock(ctx);
        (
            Retention {
                maxlen: effective_maxlen(&overrides, &suffix, global_maxlen),
                retention_ms,
            },
            Retention {
                maxlen: effective_maxlen(&overrides, "#control", global_maxlen),
                retention_ms,
            },
            Retention {
                maxlen: global_maxlen,
                retention_ms,
            },
        )
    };
    let max_streams = MAX_STREAMS.value.load(Ordering::Relaxed);
    let event_owned = event.to_owned();
    let key_owned = key.to_vec();
    // Entry-shaping config snapshotted here so a mid-stream change lands on
    // whole events, and so the per-event write and its firehose copy share one
    // format (issue #60). `entry-seq` (issue #66) is IMMUTABLE, but reading it
    // once here keeps the hot path uniform. `event_type` is moved into the job
    // for the `verbose` format's `class` field.
    let format = *ENTRY_FORMAT.lock(ctx);
    let entry_seq = ENTRY_SEQ.load(Ordering::Relaxed);

    // 8. Deferred write, atomic with the notification.
    let status = ctx.add_post_notification_job(move |ctx| {
        guard_job(move || {
            // All destination streams are consolidated in db 0.
            let rc = unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) };
            if rc != raw::REDISMODULE_OK as i32 {
                count_drop(
                    ctx,
                    &DROPPED_XADD_ERROR,
                    &LOGGED_XADD_ERROR,
                    "SelectDb(0) failed; entry dropped",
                );
                return;
            }
            let db_s = db.to_string();

            // In per-node cluster mode, `{tag}` co-locates this node's streams on
            // an owned slot; empty otherwise. `None` means no owned slot yet.
            let seg = match tag_segment(ctx) {
                Some(s) => s,
                None => {
                    count_no_slot_drop(ctx);
                    return;
                }
            };
            // One `seq` per captured event (issue #66), assigned after the slot
            // check so a no-owned-slot drop does not burn a number, and reused
            // by the firehose copy so both writes carry the same value. Only
            // consumed when `entry-seq` is on; otherwise the field is absent.
            let seq = if entry_seq {
                Some(SEQ.fetch_add(1, Ordering::Relaxed))
            } else {
                None
            };
            let spec = EntrySpec {
                format,
                event: event_owned.as_bytes(),
                key: &key_owned,
                db: &db_s,
                class: event_type,
                seq,
            };
            // Resolve the trim policy per destination now the suffix is final
            // (issues #62, #108): `data_ret`/`control_ret`/`firehose_ret` were
            // resolved before the job was enqueued (the suffix was already
            // final), so nothing per-event needs recomputing here.
            match mirror_entry(
                ctx,
                &prefix_owned,
                &seg,
                &suffix,
                &spec,
                data_ret,
                max_streams,
                &FORWARDED,
            ) {
                MirrorOutcome::Written => {}
                MirrorOutcome::Oom { stream, msg } => {
                    count_stream_drop(ctx, &stream, &DROPPED_OOM, &msg)
                }
                // The pinned slot migrated away in a reshard (issue #46) or is
                // mid-migration (issue #75): re-pin and retry once.
                MirrorOutcome::SlotMigrated | MirrorOutcome::Migrating(_) => repin_and_retry(
                    ctx,
                    &prefix_owned,
                    &suffix,
                    &spec,
                    data_ret,
                    control_ret,
                    max_streams,
                ),
                MirrorOutcome::Failed { stream, msg } => {
                    // The re-pin trigger is an empirically observed error
                    // string, so an unclassified failure could be the
                    // local-refusal in a reworded message. Re-verify ownership
                    // of the pinned tag before counting the drop (issue #76);
                    // a failing probe re-pins exactly as if the text had
                    // matched, counted in `repins_probe_detected`.
                    if pinned_tag_lost_by_probe(ctx) {
                        REPINS_PROBE_DETECTED.fetch_add(1, Ordering::Relaxed);
                        repin_and_retry(
                            ctx,
                            &prefix_owned,
                            &suffix,
                            &spec,
                            data_ret,
                            control_ret,
                            max_streams,
                        );
                    } else {
                        count_stream_drop(ctx, &stream, &DROPPED_XADD_ERROR, &msg);
                    }
                }
                // Max-streams cap reached (issue #64): the new stream was never
                // created. First-failure-per-reason log, then counted silently.
                MirrorOutcome::MaxStreams { stream } => count_drop(
                    ctx,
                    &DROPPED_MAX_STREAMS,
                    &LOGGED_MAX_STREAMS,
                    &format!(
                        "max-streams cap ({max_streams}) reached; new stream '{stream}' \
                         not created; entry dropped (dropped_max_streams)"
                    ),
                ),
                // The configured entry-format could not encode this event
                // (issue #60): dropped before any XADD, counted, first failure
                // logged once per process.
                MirrorOutcome::EncodeError { stream, reason } => count_drop(
                    ctx,
                    &DROPPED_ENCODE_ERROR,
                    &LOGGED_ENCODE_ERROR,
                    &format!(
                        "entry-format encode failed for '{stream}': {reason}; \
                         entry dropped (dropped_encode_error)"
                    ),
                ),
            }
            // The firehose copy runs after the per-event outcome is settled,
            // gated on the runtime-mutable config; the two writes succeed or
            // fail independently (issue #58). It reuses `spec`, so its field set
            // and `seq` match the per-event entry (issues #60, #66).
            if FIREHOSE.load(Ordering::Relaxed) {
                mirror_firehose(ctx, &prefix_owned, &spec, firehose_ret);
            }
        });
    });
    if !matches!(status, Status::Ok) {
        count_drop(
            ctx,
            &DROPPED_DEFER_ERROR,
            &LOGGED_DEFER_ERROR,
            "failed to register post-notification job; event dropped",
        );
    }
}

/// Raw keyspace-notification callback, registered directly rather than through
/// the wrapper's `event_handlers:` macro so the module can subscribe to
/// `MISSED` and `NEW` (which the macro intersects away) and so the FFI boundary
/// is panic-safe: a panic here is undefined behavior that would abort the
/// server, and the wrapper's own handler `unwrap`s a non-UTF-8 event name into
/// exactly such a panic (redismodule-rs#472). This decodes the name lossily and
/// catches any panic, counting it instead (SPEC.md section 5).
#[cfg(not(test))]
pub(crate) extern "C" fn raw_keyspace_event(
    ctx: *mut raw::RedisModuleCtx,
    event_type: c_int,
    event: *const c_char,
    key: *mut raw::RedisModuleString,
) -> c_int {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = Context::new(ctx);
        let key_slice = RedisString::string_as_slice(key);
        let event_name = String::from_utf8_lossy(unsafe { CStr::from_ptr(event) }.to_bytes());
        on_keyspace_event(
            &context,
            NotifyEvent::from_bits_truncate(event_type),
            &event_name,
            key_slice,
        );
    }));
    if outcome.is_err() {
        HANDLER_PANICS.fetch_add(1, Ordering::Relaxed);
        if LOGGED_PANIC
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            redis_module::logging::log_warning(
                "eventstream: notification handler panicked (caught); event dropped",
            );
        }
    }
    raw::Status::Ok as c_int
}

/// Handle the start of a flush. Invalidates the in-process per-stream state and
/// records a `flushed` gap marker carrying the flushed db (`-1` == `FLUSHALL`).
///
/// Registry invalidation: FLUSHALL (or FLUSHDB in db 0) deletes the
/// `<prefix>#streams` set, so the registry-membership bits must be cleared for
/// the next capture to re-register its stream. The whole record goes with them
/// (issue #71): the per-stream counters count "since load or last flush", the
/// simplest semantics consistent with the registry itself being deleted; the
/// process-wide counters in INFO/STATS remain strictly since-load. A FLUSHDB in
/// another database does not delete the registry, so clearing here is
/// conservative: the following re-SADD is idempotent, at the cost of
/// re-counting `active_streams`, which is therefore "distinct streams written
/// since load or last flush" (SPEC.md section 5).
///
/// Marker delivery (issues #74, #73): the marker is deferred through the
/// pending-marker mechanism, never written directly here. A direct write at
/// flush time would itself be flushed (for FLUSHALL/db-0) or would fire before
/// the control stream is safe to touch; the pending path lands the marker on
/// the recreated control stream ahead of the first post-flush mirrored entry,
/// which is exactly the boundary consumers reconcile against. Both cases warrant
/// a marker: FLUSHALL/db-0 deletes the streams (full reconcile), a non-zero-db
/// flush loses only that database's source keys (reconcile over `db`). If no
/// event ever follows the flush, no marker is written, and nothing was mirrored
/// into the void either (SPEC.md section 9 delivery mechanics).
///
/// The marker is recorded only when this node is the capturing master
/// (MASTER-and-not-LOADING, the gate that governs every marker write). The flush
/// event also fires when a replica empties its dataset for a full resync and
/// when a replica replays a replicated `FLUSHALL`/`FLUSHDB`; in the resync case
/// no capture gap exists on this node, and in the replayed case the master's own
/// marker already reaches the replica through replication, so recording one here
/// would duplicate it on failover. Registry invalidation, by contrast, applies
/// on any role (a replayed flush really does clear the replica's registry), so
/// the `STREAM_STATS` clear stays unconditional.
#[cfg(not(test))]
pub(crate) fn on_flush_started(ctx: &Context, dbnum: i32) {
    STREAM_STATS.lock(ctx).clear();
    // The registry cache is empty again, so the max-streams cap (issue #64)
    // counts distinct streams from zero: the first `max-streams` names seen
    // after the flush re-register and win.
    CURRENT_STREAMS.store(0, Ordering::Relaxed);
    let flags = ctx.get_flags();
    if flags.contains(ContextFlags::MASTER) && !flags.contains(ContextFlags::LOADING) {
        record_pending_marker(ctx, PendingMarker::Flushed(dbnum));
    }
}

/// Handle a `SWAPDB` that moves database 0 (issue #73). SWAPDB involving db 0
/// atomically moves the destination streams and their consumer groups into the
/// other database while the module keeps writing fresh streams in db 0
/// (SPEC.md section 6 SWAPDB caveat). A swap that does not touch db 0 leaves
/// the streams' database untouched and is ignored. When db 0 is involved, log a
/// warning and record a pending `swapdb` marker: `action=swapdb` alone tells
/// consumers that entries before this marker may now live in another database
/// (SPEC.md section 9 two-field schema). Deferred for the same reason as the
/// flush marker: the marker lands on the fresh db 0 control stream ahead of the
/// first post-swap entry, timestamping the boundary where db 0 history diverged.
///
/// Gated to the capturing master for the same reason as the flush marker: a
/// replica replays the replicated `SWAPDB` and would otherwise record a second
/// marker that duplicates the replicated one on failover.
#[cfg(not(test))]
pub(crate) fn on_swapdb(ctx: &Context, first: i32, second: i32) {
    if first != 0 && second != 0 {
        return;
    }
    let flags = ctx.get_flags();
    if !flags.contains(ContextFlags::MASTER) || flags.contains(ContextFlags::LOADING) {
        return;
    }
    ctx.log_warning(&format!(
        "eventstream: SWAPDB {first} {second} moved database 0; the destination \
         streams and their consumer groups now live in the swapped database, and \
         fresh streams will be created in db 0 (SPEC.md section 6). A 'swapdb' gap \
         marker delimits the boundary."
    ));
    // The db-0 registry set (`<prefix>#streams`) moved out with the swap, so the
    // dedupe cache is stale: without clearing it, fresh db-0 streams keep their
    // `registered` bit and never re-SADD into the rebuilt registry, so
    // EVENTSTREAM.STREAMS would not list them. Same rationale as the flush clear,
    // including the max-streams cap counter (issue #64) which counts from zero
    // against the rebuilt db-0 registry.
    STREAM_STATS.lock(ctx).clear();
    CURRENT_STREAMS.store(0, Ordering::Relaxed);
    record_pending_marker(ctx, PendingMarker::Simple("swapdb"));
}

/// Raw flush server-event callback (`REDISMODULE_EVENT_FLUSHDB`). Reads the
/// flushed db from `RedisModuleFlushInfoV1` (`dbnum`, `-1` for `FLUSHALL`),
/// which the wrapper's `#[flush_event_handler]` discards; acts only on the
/// start subevent, matching the former handler.
#[cfg(not(test))]
pub(crate) extern "C" fn raw_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    subevent: u64,
    data: *mut c_void,
) {
    guard_server_event(|| {
        if subevent != raw::REDISMODULE_SUBEVENT_FLUSHDB_START {
            return;
        }
        // `data` is a `RedisModuleFlushInfoV1*`; a null pointer would be a
        // server contract violation, so treat it as `FLUSHALL` (-1) rather
        // than dereference it.
        let dbnum = if data.is_null() {
            -1
        } else {
            unsafe { (*(data as *const raw::RedisModuleFlushInfoV1)).dbnum }
        };
        on_flush_started(&Context::new(ctx), dbnum);
    });
}

/// Raw SWAPDB server-event callback (`REDISMODULE_EVENT_SWAPDB`). Reads
/// `dbnum_first`/`dbnum_second` from `RedisModuleSwapDbInfoV1`; there is no safe
/// wrapper for this event in the pinned tag (issue #73).
#[cfg(not(test))]
pub(crate) extern "C" fn raw_swapdb_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    _subevent: u64,
    data: *mut c_void,
) {
    guard_server_event(|| {
        if data.is_null() {
            return;
        }
        let info = unsafe { &*(data as *const raw::RedisModuleSwapDbInfoV1) };
        on_swapdb(&Context::new(ctx), info.dbnum_first, info.dbnum_second);
    });
}
#[cfg(test)]
mod tests {
    use super::*;

    // --- entry-format enum and seq field (issues #60, #66) ---

    /// Decode the `Vec<Vec<u8>>` an entry encodes to into a `Vec<(String,
    /// Vec<u8>)>` of field name/value pairs, preserving order.
    fn pairs(fields: &[Vec<u8>]) -> Vec<(String, Vec<u8>)> {
        assert_eq!(fields.len() % 2, 0, "field list must be name/value pairs");
        fields
            .chunks(2)
            .map(|c| (String::from_utf8(c[0].clone()).unwrap(), c[1].clone()))
            .collect()
    }

    fn spec<'a>(format: EntryFormat, seq: Option<u64>) -> EntrySpec<'a> {
        EntrySpec {
            format,
            event: b"hset",
            key: b"user:1",
            db: "3",
            class: NotifyEvent::HASH,
            seq,
        }
    }

    #[test]
    fn entry_format_fixed_is_byte_identical_to_today() {
        // The default format must reproduce the historical event/key/db schema
        // exactly: no `format` discriminator, no `seq`, same order (SPEC.md
        // section 6). This is the backward-compatibility pin.
        let fields = encode_entry_fields(&spec(EntryFormat::fixed, None)).unwrap();
        assert_eq!(
            pairs(&fields),
            vec![
                ("event".to_owned(), b"hset".to_vec()),
                ("key".to_owned(), b"user:1".to_vec()),
                ("db".to_owned(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn entry_format_minimal_drops_event_and_carries_discriminator() {
        let fields = encode_entry_fields(&spec(EntryFormat::minimal, None)).unwrap();
        assert_eq!(
            pairs(&fields),
            vec![
                ("format".to_owned(), b"minimal".to_vec()),
                ("key".to_owned(), b"user:1".to_vec()),
                ("db".to_owned(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn entry_format_verbose_adds_class() {
        let fields = encode_entry_fields(&spec(EntryFormat::verbose, None)).unwrap();
        assert_eq!(
            pairs(&fields),
            vec![
                ("format".to_owned(), b"verbose".to_vec()),
                ("event".to_owned(), b"hset".to_vec()),
                ("key".to_owned(), b"user:1".to_vec()),
                ("db".to_owned(), b"3".to_vec()),
                ("class".to_owned(), b"hash".to_vec()),
            ]
        );
    }

    #[test]
    fn entry_format_json_base64_encodes_the_key() {
        let fields = encode_entry_fields(&spec(EntryFormat::json, None)).unwrap();
        let p = pairs(&fields);
        assert_eq!(p[0], ("format".to_owned(), b"json".to_vec()));
        assert_eq!(p[1].0, "data");
        // key "user:1" base64 is "dXNlcjox"; db is a JSON number.
        assert_eq!(
            String::from_utf8(p[1].1.clone()).unwrap(),
            r#"{"event":"hset","key":"dXNlcjox","db":3}"#
        );
    }

    #[test]
    fn seq_field_appended_last_for_non_json_formats() {
        // `seq` (issue #66) is the trailing field so the per-format field order
        // stays stable for SAMEFIELDS.
        for format in [
            EntryFormat::fixed,
            EntryFormat::minimal,
            EntryFormat::verbose,
        ] {
            let fields = encode_entry_fields(&spec(format, Some(48212))).unwrap();
            let p = pairs(&fields);
            assert_eq!(
                *p.last().unwrap(),
                ("seq".to_owned(), b"48212".to_vec()),
                "format {format:?} must end with seq"
            );
        }
    }

    #[test]
    fn seq_embedded_in_json_document_not_a_top_level_field() {
        let fields = encode_entry_fields(&spec(EntryFormat::json, Some(7))).unwrap();
        let p = pairs(&fields);
        // Still exactly two fields (format, data): seq lives inside the doc.
        assert_eq!(p.len(), 2);
        assert_eq!(
            String::from_utf8(p[1].1.clone()).unwrap(),
            r#"{"event":"hset","key":"dXNlcjox","db":3,"seq":7}"#
        );
    }

    #[test]
    fn json_encode_errors_on_non_utf8_event_name() {
        // The one reachable-in-principle encode failure feeding
        // `dropped_encode_error` (issue #60); the raw callback's lossy decode
        // makes it unreachable through the normal path, but nothing at this
        // layer type-guarantees UTF-8, so the guard is real.
        let mut s = spec(EntryFormat::json, None);
        s.event = &[0xff, 0xfe];
        assert!(encode_entry_fields(&s).is_err());
        // The other formats treat the event as raw bytes and never fail.
        s.format = EntryFormat::fixed;
        assert!(encode_entry_fields(&s).is_ok());
        s.format = EntryFormat::verbose;
        assert!(encode_entry_fields(&s).is_ok());
    }

    #[test]
    fn base64_matches_known_vectors() {
        let cases = [
            (&b""[..], ""),
            (&b"f"[..], "Zg=="),
            (&b"fo"[..], "Zm8="),
            (&b"foo"[..], "Zm9v"),
            (&b"foob"[..], "Zm9vYg=="),
            (&b"fooba"[..], "Zm9vYmE="),
            (&b"foobar"[..], "Zm9vYmFy"),
        ];
        for (input, expected) in cases {
            let mut out = String::new();
            base64_into(input, &mut out);
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn json_escape_handles_quotes_backslashes_and_controls() {
        let mut out = String::new();
        json_escape_into("a\"b\\c\nd\t", &mut out);
        assert_eq!(out, r#""a\"b\\c\nd\t""#);
    }

    #[test]
    fn class_names_single_and_multiple_bits() {
        assert_eq!(class_names(NotifyEvent::EXPIRED), "expired");
        assert_eq!(class_names(NotifyEvent::GENERIC), "generic");
        // Multiple bits join in the grammar's order.
        assert_eq!(
            class_names(NotifyEvent::STRING | NotifyEvent::HASH),
            "string,hash"
        );
        assert_eq!(class_names(NotifyEvent::empty()), "");
    }

    // --- retention (MINID vs MAXLEN) trim clause (issues #62, #108) ---

    #[test]
    fn retention_maxlen_clause_when_time_disabled() {
        // retention-ms 0 => count-based MAXLEN with the resolved cap; now_ms is
        // ignored on this path.
        let r = Retention {
            maxlen: 1000,
            retention_ms: 0,
        };
        assert!(!r.is_time_based());
        let (kw, threshold) = r.trim_clause(0).expect("clause");
        assert_eq!(kw, b"MAXLEN");
        assert_eq!(threshold, "1000");
    }

    #[test]
    fn retention_maxlen_zero_emits_no_clause() {
        let r = Retention {
            maxlen: 0,
            retention_ms: 0,
        };
        assert!(r.trim_clause(0).is_none());
    }

    #[test]
    fn retention_minid_takes_precedence_over_maxlen() {
        // retention-ms > 0 => time-based MINID, formatted `<ms>-0`, ignoring the
        // maxlen cap entirely (issue #108 precedence rule).
        let r = Retention {
            maxlen: 1000,
            retention_ms: 60_000,
        };
        assert!(r.is_time_based());
        let (kw, threshold) = r.trim_clause(1_000_000).expect("clause");
        assert_eq!(kw, b"MINID");
        assert_eq!(threshold, "940000-0");
    }

    #[test]
    fn retention_minid_threshold_clamps_at_zero() {
        // A window wider than the current clock never yields a negative MINID.
        let r = Retention {
            maxlen: 0,
            retention_ms: 5000,
        };
        let (kw, threshold) = r.trim_clause(1000).expect("clause");
        assert_eq!(kw, b"MINID");
        assert_eq!(threshold, "0-0");
    }

    #[test]
    fn sanitize_passthrough_and_replacement() {
        assert_eq!(sanitize("expired"), "expired");
        assert_eq!(sanitize("json.set"), "json.set");
        assert_eq!(sanitize("rename_from"), "rename_from");
        assert_eq!(sanitize("xgroup-create"), "xgroup-create");
        assert_eq!(sanitize("foo bar"), "foo_bar");
        assert_eq!(sanitize("foo?bar"), "foo_bar");
        assert_eq!(sanitize("a#b"), "a_b");
    }

    #[test]
    fn sanitize_cannot_alias_firehose() {
        // `#` is outside the sanitizer alphabet, so no event name can route
        // to the reserved firehose key (issue #58): a raw `#firehose` event
        // lands in `<prefix>_firehose`, never `<prefix>#firehose`.
        assert_eq!(sanitize("#firehose"), "_firehose");
        assert!(!sanitize("evil#firehose").contains('#'));
    }

    #[test]
    fn sanitize_truncates_at_128() {
        let long = "x".repeat(300);
        assert_eq!(sanitize(&long).len(), 128);
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // The default case count (256, PROPTEST_CASES overrides for longer
        // local searches; CONTRIBUTING.md) rides the existing `cargo test
        // --lib` budget. Shrinking is capped by iteration count so a failure
        // cannot stall CI, and regression persistence is off: there is no
        // corpus directory in a cdylib crate and CI runners would discard it.
        #![proptest_config(ProptestConfig {
            max_shrink_iters: 2048,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn sanitize_output_is_always_stream_key_safe(
            bytes in prop::collection::vec(any::<u8>(), 0..300)
        ) {
            let event = String::from_utf8_lossy(&bytes);
            let out = sanitize(&event);
            prop_assert!(out.len() <= MAX_EVENT_NAME_LEN);
            // The output alphabet excludes `#` (no event name can alias the
            // reserved firehose key, issue #58) and `{`/`}` (no event name
            // can inject into or break the destination key's hash tag).
            prop_assert!(
                out.bytes().all(
                    |b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b':' | b'-')
                ),
                "{:?}",
                out
            );
            // Empty-after-sanitization (the skipped_invalid path) happens
            // only for an empty name: every input char maps to some output
            // char, and lossy decode never empties a non-empty byte string.
            prop_assert_eq!(out.is_empty(), bytes.is_empty());
        }

        // The SPEC.md section 5 "every built-in event name passes through
        // byte-identical" guarantee, generalized to the whole alphabet.
        #[test]
        fn sanitize_passes_alphabet_input_through(s in "[A-Za-z0-9_.:-]{0,128}") {
            prop_assert_eq!(sanitize(&s), s);
        }

        #[test]
        fn sanitize_is_idempotent(s in any::<String>()) {
            let once = sanitize(&s);
            prop_assert_eq!(sanitize(&once), once);
        }
    }
}
