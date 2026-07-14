// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Cluster per-node capture (#86): slot-pinned hash-tag selection and caching,
//! the CRC16 slot math and Redis 7.2 fallback table, the migration-refusal
//! classifiers, and the re-pin-and-retry path (SPEC.md section 10, issues #45,
//! #46, #75, #76, #116).

use crate::capture::{mirror_entry, xadd_call_options, EntrySpec, MirrorOutcome, Retention};
use crate::markers::write_marker;
use crate::stats::{
    count_drop, count_stream_drop, DROPPED_ENCODE_ERROR, DROPPED_MAX_STREAMS, DROPPED_OOM,
    DROPPED_XADD_ERROR, FORWARDED, LOGGED_ENCODE_ERROR, LOGGED_MAX_STREAMS,
};
use redis_module::{CallResult, Context};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[cfg(not(test))]
use redis_module::raw;
#[cfg(not(test))]
use std::ffi::CStr;

/// Cluster per-node mode (issue #45): true when `eventstream.cluster-streams`
/// is `per-node` and the server is in cluster mode. Set once in init (the
/// config is IMMUTABLE), read on the hot path.
pub(crate) static PER_NODE: AtomicBool = AtomicBool::new(false);

/// Events dropped in per-node mode because the node owns no slot to pin to
/// (SPEC.md section 5). Distinct from the write-failure drops.
pub(crate) static DROPPED_NO_OWNED_SLOT: AtomicU64 = AtomicU64::new(0);

pub(crate) static LOGGED_NO_OWNED_SLOT: AtomicBool = AtomicBool::new(false);

/// Times the node re-pinned to a new owned slot after its pinned slot migrated
/// away (issue #46). Each re-pin writes a `repinned` gap marker and changes the
/// destination stream name; a nonzero value records reshard activity.
pub(crate) static REPINS: AtomicU64 = AtomicU64::new(0);

/// Re-pins triggered by the ownership-probe fallback rather than the
/// recognized error text (issue #76), counted in addition to `repins`. A
/// nonzero value means the server's local-refusal message is no longer
/// recognized; report the new form upstream.
pub(crate) static REPINS_PROBE_DETECTED: AtomicU64 = AtomicU64::new(0);

/// Events refused while the pinned slot was mid-migration (issue #75):
/// `TRYAGAIN`/`ASK` refusals that persisted through the one re-pin retry.
/// Distinct from `dropped_xadd_error` so routine resharding does not read as
/// a broken write path; included in the `dropped` sum.
pub(crate) static DROPPED_MIGRATING: AtomicU64 = AtomicU64::new(0);

pub(crate) static LOGGED_MIGRATING: AtomicBool = AtomicBool::new(false);

/// Redis has 16384 hash slots.
pub(crate) const SLOT_COUNT: u32 = 16384;

/// The hash tag this node pins its streams to in per-node cluster mode (issue
/// #45). `None` until selected: a node owns no slots at load, so selection is
/// lazy, on the first captured write when slots are known. A plain `Mutex`
/// (not a `RedisGILGuard`) so the INFO handler, whose context is not a lock
/// indicator, can read it; the GIL already serializes all access.
pub(crate) static NODE_TAG: Mutex<Option<String>> = Mutex::new(None);

/// The pinned tag most recently probe-verified as owned (issue #76): bounds
/// the text-independent fallback at one ownership probe per pinned tag, so an
/// unrelated persistent `XADD` failure costs one extra probe total, not one
/// per failure. Cleared on re-pin. Same locking rationale as `NODE_TAG`.
pub(crate) static PROBE_VERIFIED_TAG: Mutex<Option<String>> = Mutex::new(None);

/// Slot -> synthetic-tag table for the Redis 7.2 fallback, which has no
/// canonical-name API (issue #116). Filled once, on first fallback use, by
/// hashing candidates `es{i}` until every slot has one; stores the candidate
/// index per slot (64 KiB). CRC16 is a fixed function, so the fill
/// deterministically completes (at candidate index 156393, a few ms); the
/// exhaustive unit test proves completion and coverage of all 16384 slots.
pub(crate) static FALLBACK_SLOT_TAGS: OnceLock<Vec<u32>> = OnceLock::new();

/// The hash-tag segment inserted between the prefix and the rest of a
/// destination key so all of a node's keys co-locate on a slot it owns (issue
/// #45). Empty in standalone/refuse mode. In per-node cluster mode, `{tag}`,
/// selecting the tag lazily on first use (a node owns no slots at load) and
/// caching it. Returns `None` only in per-node mode when the node currently
/// owns no slot; the caller drops the event as `dropped_no_owned_slot`.
///
/// Must be called from a write-safe context (a post-notification job or a
/// command), because selection probes the keyspace.
pub(crate) fn tag_segment(ctx: &Context) -> Option<String> {
    if !PER_NODE.load(Ordering::Relaxed) {
        return Some(String::new());
    }
    let mut cached = NODE_TAG.lock().unwrap();
    if cached.is_none() {
        *cached = select_owned_tag(ctx);
    }
    cached.as_ref().map(|t| format!("{{{t}}}"))
}

/// Like [`tag_segment`] but never selects (never writes): returns the cached
/// segment or `None`. For read-only contexts, such as the `EVENTSTREAM.STREAMS`
/// command, where triggering the write probe would violate the readonly
/// contract.
pub(crate) fn tag_segment_cached() -> Option<String> {
    if !PER_NODE.load(Ordering::Relaxed) {
        return Some(String::new());
    }
    NODE_TAG
        .lock()
        .unwrap()
        .as_ref()
        .map(|t| format!("{{{t}}}"))
}

/// CRC16-CCITT (XMODEM: polynomial 0x1021, initial value 0, no reflection),
/// the exact variant Redis uses for key hash slots (`CRC16(key) mod 16384`).
/// Only the 7.2 fallback below needs it; unit-tested against the cluster
/// spec's reference vector and the well-known slot anchors.
pub(crate) fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// The synthetic tag for `slot` on the 7.2 fallback path: the lowest-index
/// `es{i}` whose CRC16 lands on it, from [`FALLBACK_SLOT_TAGS`]. Tags are
/// ASCII alphanumerics, so they never contain `{` or `}` and cannot break the
/// `{tag}` wrap.
pub(crate) fn fallback_tag_for_slot(slot: u32) -> String {
    let table = FALLBACK_SLOT_TAGS.get_or_init(|| {
        let mut table = vec![u32::MAX; SLOT_COUNT as usize];
        let mut remaining = SLOT_COUNT;
        let mut i: u32 = 0;
        while remaining > 0 {
            let s = u32::from(crc16(format!("es{i}").as_bytes())) % SLOT_COUNT;
            if table[s as usize] == u32::MAX {
                table[s as usize] = i;
                remaining -= 1;
            }
            i += 1;
        }
        table
    });
    format!("es{}", table[slot as usize])
}

/// Find a hash tag whose slot this node owns, probing ownership with a
/// non-destructive write: `XADD {tag}#slotprobe NOMKSTREAM * f v`. The
/// slot-ownership check that rejects a non-local key applies to writes, not
/// reads (a plain read runs locally and would falsely pass on every node), so
/// the probe must be a write; `NOMKSTREAM` on a non-existent stream is a no-op
/// that creates nothing, and this is the same locality rule that governs the
/// real mirrored writes. One-time, then cached.
///
/// The candidate tag for each slot comes from
/// `RedisModule_ClusterCanonicalKeyNameInSlot(slot)`, which yields a key name
/// hashing to a specific slot, so scanning slots has guaranteed coverage. That
/// API was added after Redis 7.2, though: on 7.2 the bound function pointer is
/// null (bindgen declares it from the vendored header, but the server does not
/// provide it), and calling it would panic across the FFI boundary and abort
/// the server (issue #45). When it is unavailable, fall back to the
/// [`FALLBACK_SLOT_TAGS`] runtime CRC16 search (issue #116), which maps each
/// probed slot to a synthetic tag hashing to it, so coverage is exhaustive on
/// both paths: a node that owns any slot finds a tag. Slots are visited in a
/// scattered order (odd stride, coprime with 16384) so an owned slot is found
/// within a few probes on a typical cluster while still covering all slots in
/// the worst case.
#[cfg(not(test))]
pub(crate) fn select_owned_tag(ctx: &Context) -> Option<String> {
    let canonical = unsafe { raw::RedisModule_ClusterCanonicalKeyNameInSlot };
    let mut slot: u32 = 0;
    for _ in 0..SLOT_COUNT {
        // A candidate tag whose hash slot we then test for ownership.
        let candidate: Option<String> = match canonical {
            Some(canonical_in_slot) => {
                let name_ptr = unsafe { canonical_in_slot(slot) };
                if name_ptr.is_null() {
                    None
                } else {
                    let bytes = unsafe { CStr::from_ptr(name_ptr) }.to_bytes();
                    // The canonical name is expected to be simple ASCII with no
                    // braces; guard against anything that would break the tag.
                    if bytes.is_empty() || bytes.contains(&b'{') || bytes.contains(&b'}') {
                        None
                    } else {
                        Some(String::from_utf8_lossy(bytes).into_owned())
                    }
                }
            }
            // Redis 7.2: no canonical-name API. The table gives a synthetic
            // tag hashing to the probed slot, so the walk covers every slot a
            // node could own, however skewed the ownership (issue #116).
            None => Some(fallback_tag_for_slot(slot)),
        };
        if let Some(tag) = candidate {
            if probe_tag_owned(ctx, &tag).is_ok() {
                return Some(tag);
            }
        }
        slot = (slot + 2609) % SLOT_COUNT;
    }
    None
}

// Test builds compile out the raw cluster call; tag selection never runs there.
#[cfg(test)]
pub(crate) fn select_owned_tag(_ctx: &Context) -> Option<String> {
    None
}

/// Probe whether this node owns `tag`'s slot, with a non-destructive write:
/// `XADD {tag}#slotprobe NOMKSTREAM * f v`, using the SAME call options as the
/// real mirrored write (the replicate flag is what makes RM_Call enforce slot
/// ownership; a plain call runs locally and passes on every node). NOMKSTREAM
/// makes it a no-op on a non-existent stream, so nothing is written. Owned
/// slot -> Ok; non-owned -> Err (the non-local-key error); mid-migration ->
/// Err (`TRYAGAIN`/`ASK`), so selection never picks a slot that is leaving
/// (issue #75). The Err carries the error text for the caller to classify.
pub(crate) fn probe_tag_owned(ctx: &Context, tag: &str) -> Result<(), String> {
    let probe = format!("{{{tag}}}#slotprobe");
    let res: CallResult = ctx.call_ext(
        "XADD",
        &xadd_call_options(),
        &[
            probe.as_bytes(),
            &b"NOMKSTREAM"[..],
            &b"*"[..],
            &b"f"[..],
            &b"v"[..],
        ][..],
    );
    res.map(|_| ())
        .map_err(|e| e.to_utf8_string().unwrap_or_default())
}

/// Text-independent migration check (issue #76): true when the node's pinned
/// tag no longer probes as owned, meaning an unclassified `XADD` failure was
/// really the local-refusal in a message form `is_slot_migrated` does not
/// recognize. A tag the probe verifies as owned is cached in
/// `PROBE_VERIFIED_TAG` and not re-probed until a re-pin or a successful
/// mirrored write resets the cache; the reset bounds probes to one per
/// unclassified-failure streak rather than one per pinned tag, so a stale
/// verification cannot mask a later migration. An OOM probe error is
/// inconclusive (the probe was refused, not the slot) and neither
/// reclassifies nor caches.
pub(crate) fn pinned_tag_lost_by_probe(ctx: &Context) -> bool {
    if !PER_NODE.load(Ordering::Relaxed) {
        return false;
    }
    let Some(tag) = NODE_TAG.lock().unwrap().clone() else {
        return false;
    };
    if PROBE_VERIFIED_TAG.lock().unwrap().as_deref() == Some(tag.as_str()) {
        return false;
    }
    match probe_tag_owned(ctx, &tag) {
        Ok(()) => {
            *PROBE_VERIFIED_TAG.lock().unwrap() = Some(tag);
            false
        }
        Err(msg) => !msg.starts_with("OOM"),
    }
}

/// Record an event dropped for want of an owned slot (per-node mode), logging
/// the first occurrence. The message states what was observed (the walk found
/// no slot that accepted a local write), not an inference about ownership
/// (issue #116); on 7.4+ a slot whose canonical name is unusable is skipped
/// rather than probed, so "probed every slot" would overstate it.
pub(crate) fn count_no_slot_drop(ctx: &Context) {
    count_drop(
        ctx,
        &DROPPED_NO_OWNED_SLOT,
        &LOGGED_NO_OWNED_SLOT,
        "tag selection walked all 16384 slots and found none that accepted a \
         local write; event dropped (dropped_no_owned_slot). Selection is \
         retried on the next captured event",
    );
}

/// True if an `XADD` failure is the cluster local-refusal error, which in
/// per-node mode means the node no longer owns the pinned tag's slot (it
/// migrated away in a reshard, issue #46). The full text is "Attempted to
/// access a non local key in a cluster node" (observed empirically, #19); match
/// a stable substring so a leading error code does not matter.
pub(crate) fn is_slot_migrated(msg: &str) -> bool {
    msg.contains("non local key")
}

/// True if an `XADD` failure is a migration-window refusal (issue #75): while
/// the pinned slot is `MIGRATING`/`IMPORTING`, a cluster write is refused with
/// `TRYAGAIN` or redirected with `ASK <slot> <node>`. Both are error codes, so
/// they lead the message; either way the slot is leaving this node, an earlier
/// signal of the same departure `is_slot_migrated` detects after the fact.
pub(crate) fn is_migration_refusal(msg: &str) -> bool {
    msg.starts_with("TRYAGAIN ") || msg == "TRYAGAIN" || msg.starts_with("ASK ")
}

/// Re-pin after the pinned slot left this node, either because a migration
/// completed (the local-refusal error, issue #46) or is in progress
/// (`TRYAGAIN`/`ASK`, issue #75): clear the cached tag, re-select a currently
/// owned slot (the selection probe fails with the same refusal on a slot
/// mid-migration, so the leaving slot is never re-picked), delimit the
/// discontinuity with a `repinned` gap marker on the new control stream, and
/// retry the entry once so the triggering event is captured rather than
/// dropped. Bounded: a refusal on the retry is a counted drop, never another
/// re-pin. Runs only in a write-safe context (a post-notification job).
pub(crate) fn repin_and_retry(
    ctx: &Context,
    prefix: &str,
    suffix: &str,
    spec: &EntrySpec,
    retention: Retention,
    control: Retention,
    max_streams: i64,
) {
    *NODE_TAG.lock().unwrap() = None;
    *PROBE_VERIFIED_TAG.lock().unwrap() = None;
    REPINS.fetch_add(1, Ordering::Relaxed);
    let seg = match tag_segment(ctx) {
        Some(s) => s,
        None => {
            // No slot owned now; capture resumes on a later event once this
            // node owns a slot again.
            count_no_slot_drop(ctx);
            return;
        }
    };
    // A `repinned` gap marker on the new control stream delimits the window
    // where this node's stream name changed (SPEC.md section 9).
    write_marker(
        ctx,
        &format!("{prefix}{seg}#control"),
        "repinned",
        None,
        control,
    );
    match mirror_entry(ctx, prefix, &seg, suffix, spec, retention, max_streams, &FORWARDED) {
        MirrorOutcome::Written => {}
        MirrorOutcome::Oom { stream, msg } => count_stream_drop(ctx, &stream, &DROPPED_OOM, &msg),
        // Still refused (slot in flux): a migration-window drop, delimited by
        // the marker above (SPEC.md section 10). Migration refusals stay on
        // the process-wide latch: the condition is node-level (the pinned
        // slot), not stream-level (issue #68 scope).
        MirrorOutcome::SlotMigrated => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            "XADD still refused as non-local after re-pin; entry dropped in \
             migration window (dropped_migrating)",
        ),
        MirrorOutcome::Migrating(msg) => count_drop(
            ctx,
            &DROPPED_MIGRATING,
            &LOGGED_MIGRATING,
            &format!("{msg} (after re-pin; entry dropped in migration window)"),
        ),
        MirrorOutcome::Failed { stream, msg } => {
            count_stream_drop(ctx, &stream, &DROPPED_XADD_ERROR, &msg)
        }
        // The re-pinned name is new after a tag change; if the cap is reached
        // the entry is dropped like any other new stream (issue #64).
        MirrorOutcome::MaxStreams { stream } => count_drop(
            ctx,
            &DROPPED_MAX_STREAMS,
            &LOGGED_MAX_STREAMS,
            &format!("max-streams cap reached after re-pin; new stream '{stream}' not created; entry dropped (dropped_max_streams)"),
        ),
        // Encode failure is deterministic per format+event, so the retry would
        // fail identically; count it once and stop (issue #60).
        MirrorOutcome::EncodeError { stream, reason } => count_drop(
            ctx,
            &DROPPED_ENCODE_ERROR,
            &LOGGED_ENCODE_ERROR,
            &format!("entry-format encode failed after re-pin for '{stream}': {reason}; entry dropped (dropped_encode_error)"),
        ),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_migrated_matches_known_local_refusal_forms() {
        // The bare text observed empirically (#19), and with a leading
        // error-code token, which the substring match must tolerate.
        assert!(is_slot_migrated(
            "Attempted to access a non local key in a cluster node"
        ));
        assert!(is_slot_migrated(
            "ERR Attempted to access a non local key in a cluster node"
        ));
    }

    #[test]
    fn slot_migrated_rejects_unrelated_errors() {
        // None of these may trigger a re-pin.
        assert!(!is_slot_migrated(
            "OOM command not allowed when used memory > 'maxmemory'."
        ));
        assert!(!is_slot_migrated("ERR some arbitrary error"));
        assert!(!is_slot_migrated(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ));
        assert!(!is_slot_migrated(""));
    }

    #[test]
    fn migration_refusal_matches_tryagain_and_ask() {
        // TRYAGAIN and ASK are error codes, so they lead the message.
        assert!(is_migration_refusal(
            "TRYAGAIN Multiple keys request during rehashing of slot"
        ));
        assert!(is_migration_refusal("TRYAGAIN"));
        assert!(is_migration_refusal("ASK 3999 127.0.0.1:6381"));
    }

    #[test]
    fn migration_refusal_rejects_unrelated_errors() {
        assert!(!is_migration_refusal(
            "Attempted to access a non local key in a cluster node"
        ));
        assert!(!is_migration_refusal(
            "OOM command not allowed when used memory > 'maxmemory'."
        ));
        assert!(!is_migration_refusal("ERR some arbitrary error"));
        assert!(!is_migration_refusal(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ));
        // Codes lead the message; a mention elsewhere is not a refusal, and a
        // word merely prefixed with the code is not the code (ASKING is a real
        // command; TRYAGAINX guards any hypothetical future sibling code).
        assert!(!is_migration_refusal("ERR TRYAGAIN is not a code here"));
        assert!(!is_migration_refusal("ASKING requires a cluster"));
        assert!(!is_migration_refusal("TRYAGAINX something else"));
        assert!(!is_migration_refusal(""));
    }

    #[test]
    fn crc16_matches_redis_key_hashing() {
        // Reference vector from the cluster spec's CRC16 appendix, plus the
        // well-known slot anchors from the spec's key-distribution examples.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(u32::from(crc16(b"foo")) % SLOT_COUNT, 12182);
        assert_eq!(u32::from(crc16(b"bar")) % SLOT_COUNT, 5061);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn fallback_tag_covers_every_slot() {
        // The 7.2 fallback's exhaustiveness guarantee (issue #116). Completing
        // at all proves the candidate walk terminates; the round trip proves
        // every slot's tag hashes back to it; the charset check proves no tag
        // can break the `{tag}` wrap.
        for slot in 0..SLOT_COUNT {
            let tag = fallback_tag_for_slot(slot);
            assert_eq!(u32::from(crc16(tag.as_bytes())) % SLOT_COUNT, slot);
            assert!(tag.bytes().all(|b| b.is_ascii_alphanumeric()), "{tag}");
        }
    }
}
