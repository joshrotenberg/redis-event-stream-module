// Part of the module split from the former single `src/lib.rs` (#86):
// behavior-preserving code movement only -- see CONTRIBUTING.md "Source layout".
//! Introspection commands (#86): `EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`
//! (bare / WITHSTATS / VERBOSE), and the write-path `EVENTSTREAM.PRUNE`. The
//! whole module compiles only outside unit-test builds (the commands are
//! registered through the `redis_module!` macro, which needs a live server).

use crate::cluster::*;
use crate::config::*;
use crate::stats::*;
use redis_module::{
    raw, CallOptions, CallOptionsBuilder, CallResult, Context, RedisError, RedisResult,
    RedisString, RedisValue,
};
use std::sync::atomic::Ordering;

/// Call options for the `EVENTSTREAM.PRUNE` registry `SREM` (issue #81):
/// replicated (`!`) and errors-as-replies (`E`) like the registration `SADD`,
/// but deliberately WITHOUT the verify-oom `M` flag. Pruning frees memory, so
/// gating the `SREM` under `maxmemory` is wrong — a registry that has grown is
/// exactly the case where the reconciling remove must still run. Only the
/// removal drops `M`; the registration `SADD` keeps it via `xadd_call_options`
/// (a newly registered stream is new memory that should respect the limit).
pub(crate) fn prune_call_options() -> CallOptions {
    CallOptionsBuilder::new()
        .replicate()
        .errors_as_replies()
        .build()
}

/// `EVENTSTREAM.STATS`: the section 13 counters as a flat array of
/// field/value pairs, agreeing with the INFO section at the moment of the
/// call. Readonly, fast, keyless.
#[cfg(not(test))]
pub(crate) fn cmd_stats(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    // Driven by the shared `stats_snapshot` (issue #88) so the reply agrees with
    // the INFO section field-for-field (SPEC.md section 8), including the one
    // string field `cluster_pinned_tag`. RESP arrays may mix value types.
    let snapshot = stats_snapshot();
    let mut out = Vec::with_capacity(snapshot.len() * 2);
    for (name, value) in snapshot {
        out.push(RedisValue::SimpleStringStatic(name));
        out.push(match value {
            StatValue::Int(i) => RedisValue::Integer(i),
            StatValue::Text(s) => RedisValue::BulkString(s),
        });
    }
    Ok(RedisValue::Array(out))
}

/// The mode `EVENTSTREAM.STREAMS` runs in, chosen by its optional argument.
/// `Bare` and `WithStats` are unchanged (SPEC.md section 8); `Verbose` is the
/// liveness annotation (issue #81). All three are read-only, so
/// `EVENTSTREAM.STREAMS` stays `readonly` and works on replicas; registry
/// cleanup is the separate `write` command `EVENTSTREAM.PRUNE` (`cmd_prune`).
#[cfg(not(test))]
pub(crate) enum StreamsMode {
    /// Bare: the flat array of registered stream names, unchanged.
    Bare,
    /// `WITHSTATS` (issue #71): per-stream process counters.
    WithStats,
    /// `VERBOSE` (issue #81): per-stream liveness — name, existence, `XLEN`.
    Verbose,
}

/// One registered stream's liveness for `VERBOSE`, read inside the db-0 window
/// (issue #81): `exists` is `EXISTS` (0/1) and `length` is `XLEN` (0 for a
/// missing key, an empty stream, or a non-stream key left at the name — the
/// module owns the namespace, SPEC.md section 5, but a foreign key's `XLEN`
/// error is defensively reported as length 0 rather than surfaced). The two are
/// independent and both are returned as-is: a wrong-type key reads
/// `exists == 1, length == 0` (present but not a live stream), which is NOT the
/// same as absent. Absence (`exists == 0`) is the only "dead" signal, and it —
/// not an `XLEN` of 0 — is what `EVENTSTREAM.PRUNE` acts on.
#[cfg(not(test))]
pub(crate) fn stream_liveness(ctx: &Context, name: &str) -> (i64, i64) {
    let exists = match ctx.call("EXISTS", &[name][..]) {
        Ok(RedisValue::Integer(n)) => n,
        _ => 0,
    };
    let length = match ctx.call("XLEN", &[name][..]) {
        Ok(RedisValue::Integer(n)) => n,
        _ => 0,
    };
    (exists, length)
}

/// The db-0 body of `cmd_streams` (issue #81): db 0 is already selected and
/// will be restored by the caller on every path, so the registry read and the
/// per-stream liveness probes live here. This is entirely read-only — registry
/// mutation is `prune_in_db0`, behind the separate `EVENTSTREAM.PRUNE` command.
/// `SMEMBERS` is the source of truth for discovery; the control stream and any
/// stream whose every write failed are not members and so never appear.
#[cfg(not(test))]
pub(crate) fn streams_in_db0(ctx: &Context, registry: &str, mode: StreamsMode) -> RedisResult {
    let members: RedisResult = ctx.call("SMEMBERS", &[registry][..]);
    if let StreamsMode::Bare = mode {
        // Set membership is unordered; return it as SMEMBERS produced it.
        return members;
    }
    let RedisValue::Array(names) = members? else {
        return Err(RedisError::Str("unexpected registry reply"));
    };
    match mode {
        StreamsMode::Bare => unreachable!("handled above"),
        // Join the registry with the process-local counters, in the registry's
        // order (issue #71).
        StreamsMode::WithStats => {
            let stats = STREAM_STATS.lock(ctx);
            let out = names
                .into_iter()
                .map(|name| {
                    let name: String = name.try_into()?;
                    let (forwarded, dropped) = stats
                        .get(&name)
                        .map_or((0, 0), |s| (s.forwarded, s.dropped));
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(name),
                        RedisValue::SimpleStringStatic("forwarded"),
                        RedisValue::Integer(forwarded as i64),
                        RedisValue::SimpleStringStatic("dropped"),
                        RedisValue::Integer(dropped as i64),
                    ]))
                })
                .collect::<Result<Vec<_>, RedisError>>()?;
            Ok(RedisValue::Array(out))
        }
        // Annotate each registered name with its live existence and length
        // (issue #81). No registry mutation: this is a trustworthy discovery
        // read in one round-trip, replacing the client-side XLEN join.
        StreamsMode::Verbose => {
            let out = names
                .into_iter()
                .map(|name| {
                    let name: String = name.try_into()?;
                    let (exists, length) = stream_liveness(ctx, &name);
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(name),
                        RedisValue::Integer(exists),
                        RedisValue::Integer(length),
                    ]))
                })
                .collect::<Result<Vec<_>, RedisError>>()?;
            Ok(RedisValue::Array(out))
        }
    }
}

/// `EVENTSTREAM.STREAMS [WITHSTATS | VERBOSE]`: the destination streams
/// registered since the registry existed, read live from the persistent
/// `<prefix>#streams` set so the answer survives restart and works on
/// replicas. The registry is an append-only log of stream names ever written;
/// a listed stream may since have been trimmed to empty or deleted, so the
/// bare list is not a liveness check. Modes:
///
/// - bare (unchanged): the flat array of stream names.
/// - `WITHSTATS` (issue #71): per-stream process counters
///   `[name, "forwarded", n, "dropped", n]`. Process-local (reset on load and
///   flush invalidation), so a registered stream with no writes since load
///   reports zeros.
/// - `VERBOSE` (issue #81): per-stream liveness `[name, exists (0/1), length]`,
///   `length` being the current `XLEN` (0 for a missing or empty key). One
///   round-trip replaces the client-side `XLEN` join (SPEC.md section 5).
///
/// All modes are read-only, so the command is registered `readonly` and runs on
/// replicas — the whole point of discovery surviving to a replica. Opt-in
/// registry cleanup is the separate `write` command `EVENTSTREAM.PRUNE`
/// (`cmd_prune`); this command never mutates the registry, and an unknown
/// argument (including `PRUNE`) is rejected.
///
/// Keyless. The registry lives in db 0, so the command selects db 0 for the
/// read and restores the caller's database on every path. In per-node cluster
/// mode this reads only the local node's registry (`<prefix>{tag}#streams`);
/// cluster-wide discovery is resolved client-side — callers fan out over the
/// masters and merge (issue #47; docs/cluster-consumers.md).
#[cfg(not(test))]
pub(crate) fn cmd_streams(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mode = match args.len() {
        1 => StreamsMode::Bare,
        2 => {
            let arg = args[1].try_as_str()?;
            if arg.eq_ignore_ascii_case("withstats") {
                StreamsMode::WithStats
            } else if arg.eq_ignore_ascii_case("verbose") {
                StreamsMode::Verbose
            } else {
                return Err(RedisError::String(format!(
                    "unknown EVENTSTREAM.STREAMS argument '{arg}'"
                )));
            }
        }
        _ => return Err(RedisError::WrongArity),
    };
    // No owned slot selected yet in per-node mode: nothing local to report.
    // Use the non-probing lookup so this readonly introspection never triggers
    // the write that tag selection performs.
    let seg = match tag_segment_cached() {
        Some(s) => s,
        None => return Ok(RedisValue::Array(vec![])),
    };
    let registry = format!("{}{seg}#streams", PREFIX.value.lock(ctx).as_str());
    let orig_db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    if unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) } != raw::REDISMODULE_OK as i32 {
        return Err(RedisError::Str("failed to select database 0"));
    }
    let out = streams_in_db0(ctx, &registry, mode);
    // Restore the caller's database before returning on any path.
    unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, orig_db) };
    out
}

/// The db-0 body of [`cmd_prune`] (issue #81): db 0 is already selected and
/// will be restored by the caller on every path. `SMEMBERS` the registry and,
/// for each member whose destination key is ABSENT (`EXISTS 0` — deleted),
/// `SREM` it from the registry. Absence read via `EXISTS` is the only prune
/// trigger: a key that EXISTS but is a non-stream type (so `XLEN` would error)
/// is a foreign key parked at the name, not a dead stream, and is left
/// registered (issue #81 review); deadness is never inferred from an `XLEN`
/// error. Each removed name's in-process dedupe is invalidated in the same
/// operation — its `registered` bit is cleared and `CURRENT_STREAMS` (the
/// currently-registered count backing `max-streams`) decremented — so a later
/// write re-registers it. `ACTIVE_STREAMS` is a since-load lifetime counter
/// (SPEC.md section 13) and is intentionally left untouched; the re-register
/// bumps it as a fresh distinct-stream event. Returns the number of members
/// removed.
#[cfg(not(test))]
pub(crate) fn prune_in_db0(ctx: &Context, registry: &str) -> RedisResult {
    let RedisValue::Array(names) = ctx.call("SMEMBERS", &[registry][..])? else {
        return Err(RedisError::Str("unexpected registry reply"));
    };
    let mut pruned = 0i64;
    for name in names {
        let name: String = name.try_into()?;
        // EXISTS decides deadness, not an XLEN error: a present wrong-type key
        // must not be pruned (issue #81 review). Treat a non-Integer reply as
        // "exists" so a probe hiccup never removes a member.
        let absent = matches!(
            ctx.call("EXISTS", &[name.as_str()][..]),
            Ok(RedisValue::Integer(0))
        );
        if !absent {
            continue;
        }
        // The prune SREM drops the verify-oom `M` flag (pruning frees memory);
        // it stays replicated (`!`) and errors-as-replies (`E`) so replicas and
        // the AOF/RDB converge, exactly as the registration SADD does.
        let srem: CallResult = ctx.call_ext(
            "SREM",
            &prune_call_options(),
            &[registry.as_bytes(), name.as_bytes()][..],
        );
        if srem.is_ok() {
            pruned += 1;
            // Invalidate the in-process dedupe in the same operation (issue #81
            // hazard 2): clear the membership bit and drop the currently-
            // registered count so a later write to this name re-registers it.
            // ACTIVE_STREAMS is a since-load lifetime counter that never resets
            // (SPEC.md section 13), so it is intentionally left untouched; the
            // re-register on the next write bumps it as a fresh distinct-stream
            // event.
            let mut stats = STREAM_STATS.lock(ctx);
            if let Some(entry) = stats.get_mut(&name) {
                if entry.registered {
                    entry.registered = false;
                    CURRENT_STREAMS.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }
    Ok(RedisValue::Integer(pruned))
}

/// `EVENTSTREAM.PRUNE`: opt-in registry cleanup (issue #81). Removes from the
/// persistent `<prefix>#streams` set the registered names whose destination key
/// no longer exists (`EXISTS 0`: deleted), returning the integer count removed.
/// It is the only command that mutates the registry, and it is never automatic
/// — the append-only default (a name, once written, stays listed) is otherwise
/// intact, which is what lets the `readonly` `EVENTSTREAM.STREAMS` (bare,
/// `WITHSTATS`, `VERBOSE`) keep serving discovery on replicas. A present but
/// wrong-type key is not dead and is never pruned (issue #81 review).
///
/// Keyless. The registry lives in db 0, so the command selects db 0 for the
/// `SMEMBERS`/`EXISTS`/`SREM` and restores the caller's database on every path.
/// Registered `write`: its `SREM`s replicate (without the verify-oom `M` flag,
/// see [`prune_call_options`]) so replicas and the AOF/RDB converge. Because the
/// existence check and the `SREM` run within one command execution, a stream
/// recreated between them is not pruned (Redis is single-threaded here). In
/// per-node cluster mode this prunes only the local node's registry
/// (`<prefix>{tag}#streams`), matching `EVENTSTREAM.STREAMS` (issue #47).
#[cfg(not(test))]
pub(crate) fn cmd_prune(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 1 {
        return Err(RedisError::WrongArity);
    }
    // No owned slot selected yet in per-node mode: nothing local to prune. Use
    // the non-probing lookup (like EVENTSTREAM.STREAMS) — a node that has never
    // captured a local write has an empty local registry, so 0 is correct.
    let seg = match tag_segment_cached() {
        Some(s) => s,
        None => return Ok(RedisValue::Integer(0)),
    };
    let registry = format!("{}{seg}#streams", PREFIX.value.lock(ctx).as_str());
    let orig_db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    if unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, 0) } != raw::REDISMODULE_OK as i32 {
        return Err(RedisError::Str("failed to select database 0"));
    }
    let out = prune_in_db0(ctx, &registry);
    // Restore the caller's database before returning on any path.
    unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, orig_db) };
    out
}
