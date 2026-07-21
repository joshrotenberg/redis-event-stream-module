//! EVENTSTREAM.STATS and EVENTSTREAM.STREAMS, and the persistent stream
//! registry that backs discovery (issue #21).

mod common;

use common::*;
use redis::Commands;
use std::time::Duration;

fn streams(conn: &mut redis::Connection) -> Vec<String> {
    let mut v: Vec<String> = redis::cmd("EVENTSTREAM.STREAMS")
        .query(conn)
        .expect("STREAMS");
    v.sort();
    v
}

#[test]
fn stats_agrees_with_info() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "one forwarded", || {
        info_field(&mut c, "forwarded") == 1
    });

    // Every field, both directions, exact set + values + count (issue #88).
    // The two surfaces are built from one shared snapshot, so this guards
    // against either being re-hand-rolled out of agreement, and covers the one
    // string field, cluster_pinned_tag, that STATS carries as a BulkString and
    // that the old integer-only comparison could not see.
    assert_eq!(
        stats_map(&mut c),
        info_map(&mut c),
        "EVENTSTREAM.STATS must agree with the INFO section field-for-field"
    );
    // Sanity on the values themselves, not just cross-surface agreement.
    let st = stats_map(&mut c);
    assert_eq!(st["forwarded"], "1");
    assert_eq!(st["dropped"], "0");
}

#[test]
fn streams_lists_registered_streams() {
    let s = TestServer::start(&["events", "set,del"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "two streams registered", || {
        streams(&mut c).len() == 2
    });
    assert_eq!(streams(&mut c), vec!["events:del", "events:set"]);

    // The registry set is itself under the prefix, so its writes are never
    // mirrored back (the feedback guard drops them).
    assert!(info_field(&mut c, "skipped_self") > 0);
}

#[test]
fn streams_reads_db0_from_any_client_db() {
    let s = TestServer::start(&["events", "set"]);
    let mut c0 = s.conn();
    let _: () = c0.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "stream registered", || {
        !streams(&mut c0).is_empty()
    });

    // A client on db 3 still sees the db-0 registry.
    let mut c3 = s.conn_db(3);
    assert_eq!(streams(&mut c3), vec!["events:set"]);
    // Confirm the command did not leave the connection on db 0: a key set on
    // c3 must live in db 3, invisible to a fresh db-0 connection.
    let _: () = c3.set("db3only", "x").expect("SET on db3");
    let exists0: i64 = redis::cmd("EXISTS")
        .arg("db3only")
        .query(&mut s.conn())
        .expect("EXISTS");
    assert_eq!(exists0, 0, "STREAMS must restore the caller's database");
}

#[test]
fn prune_operates_on_db0_from_any_client_db() {
    // The PRUNE analogue of streams_reads_db0_from_any_client_db: the
    // registry lives in db 0, so PRUNE selects db 0 for its existence checks
    // and reconciling SREMs and restores the caller's database on every path
    // (SPEC.md section 8).
    let s = TestServer::start(&["events", "set"]);
    let mut c0 = s.conn();
    let _: () = c0.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "stream registered", || {
        !streams(&mut c0).is_empty()
    });

    // Delete the destination stream so the registry holds one dead name.
    let _: () = redis::cmd("DEL")
        .arg("events:set")
        .query(&mut c0)
        .expect("DEL stream");

    // PRUNE issued from a client on db 3 reconciles the db-0 registry.
    let mut c3 = s.conn_db(3);
    let pruned: i64 = redis::cmd("EVENTSTREAM.PRUNE")
        .query(&mut c3)
        .expect("PRUNE from a db-3 client");
    assert_eq!(pruned, 1, "the dead db-0 name is pruned from a db-3 client");
    assert!(
        streams(&mut c0).is_empty(),
        "the db-0 registry is reconciled"
    );

    // The client's SELECTed database is restored after the write path too.
    let _: () = c3.set("db3only", "x").expect("SET on db3");
    let exists0: i64 = redis::cmd("EXISTS")
        .arg("db3only")
        .query(&mut s.conn())
        .expect("EXISTS");
    assert_eq!(exists0, 0, "PRUNE must restore the caller's database");
}

#[test]
fn streams_withstats_reports_per_stream_counters() {
    let s = TestServer::start(&["events", "set,del"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.set("a", "2").expect("SET again");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "three forwarded", || {
        info_field(&mut c, "forwarded") == 3
    });

    // The bare reply is unchanged by the WITHSTATS addition.
    assert_eq!(streams(&mut c), vec!["events:del", "events:set"]);

    let st = streams_withstats(&mut c);
    assert_eq!(st.len(), 2, "one row per registered stream");
    assert_eq!(st["events:set"], (2, 0));
    assert_eq!(st["events:del"], (1, 0));

    // The per-stream forwarded counts partition the global counter.
    assert_eq!(
        st.values().map(|(f, _)| f).sum::<i64>(),
        info_field(&mut c, "forwarded")
    );
}

#[test]
fn streams_withstats_counts_firehose_copies_per_stream() {
    let s = TestServer::start(&["events", "set", "firehose", "yes"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "firehose copy written", || {
        info_field(&mut c, "firehose_forwarded") == 1
    });

    // The firehose stream is a registered destination stream: its row's
    // forwarded is the per-stream view of firehose_forwarded.
    let st = streams_withstats(&mut c);
    assert_eq!(st["events:#firehose"], (1, 0));
    assert_eq!(st["events:set"], (1, 0));
}

#[test]
fn streams_rejects_unknown_argument() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let err = redis::cmd("EVENTSTREAM.STREAMS")
        .arg("BOGUS")
        .query::<Vec<String>>(&mut c)
        .expect_err("BOGUS must be rejected");
    assert!(
        err.to_string().contains("BOGUS"),
        "error names the argument: {err}"
    );
    // PRUNE is now its own command (EVENTSTREAM.PRUNE); STREAMS must reject it
    // as an unknown argument like any other (issue #81 rework).
    let err = redis::cmd("EVENTSTREAM.STREAMS")
        .arg("PRUNE")
        .query::<Vec<String>>(&mut c)
        .expect_err("PRUNE is not a STREAMS argument");
    assert!(
        err.to_string().contains("PRUNE"),
        "error names the rejected argument: {err}"
    );
    let _ = redis::cmd("EVENTSTREAM.STREAMS")
        .arg("WITHSTATS")
        .arg("extra")
        .query::<Vec<Vec<redis::Value>>>(&mut c)
        .expect_err("extra arguments must be rejected");
}

#[test]
fn streams_is_readonly_and_prune_is_write() {
    // The #81 replica regression guard: bare/WITHSTATS/VERBOSE discovery must
    // keep working on replicas, so EVENTSTREAM.STREAMS must stay `readonly` and
    // never silently pick up `write`. The mutating cleanup is the separate
    // EVENTSTREAM.PRUNE, which must be `write`.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let sflags = command_flags(&mut c, "eventstream.streams");
    assert!(
        sflags.iter().any(|f| f == "readonly"),
        "EVENTSTREAM.STREAMS must be readonly: {sflags:?}"
    );
    assert!(
        !sflags.iter().any(|f| f == "write"),
        "EVENTSTREAM.STREAMS must NOT be a write command (replica regression): {sflags:?}"
    );

    let pflags = command_flags(&mut c, "eventstream.prune");
    assert!(
        pflags.iter().any(|f| f == "write"),
        "EVENTSTREAM.PRUNE must be a write command: {pflags:?}"
    );
}

/// Sorted registry members read straight from the persistent set with a raw
/// SMEMBERS, so replica convergence is observed independently of the module's
/// own commands.
fn registry_members(conn: &mut redis::Connection) -> Vec<String> {
    let mut m: Vec<String> = redis::cmd("SMEMBERS")
        .arg("events:#streams")
        .query(conn)
        .expect("SMEMBERS registry");
    m.sort();
    m
}

#[test]
fn streams_verbose_flags_dead_streams() {
    let s = TestServer::start(&["events", "set,del"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "two streams registered", || {
        streams(&mut c).len() == 2
    });

    // Both live: each has one entry.
    let v = streams_verbose(&mut c);
    assert_eq!(v["events:set"], (1, 1), "live: exists with one entry");
    assert_eq!(v["events:del"], (1, 1));

    // Deleting the destination key: exists 0, length 0 (absent). The DEL fires
    // on a key under the prefix, so the feedback guard drops it — no
    // re-mirroring.
    let _: () = redis::cmd("DEL")
        .arg("events:set")
        .query(&mut c)
        .expect("DEL");
    // Trimming a destination to empty leaves the stream key present but empty:
    // exists 1, length 0. VERBOSE reports EXISTS and XLEN independently, so this
    // reads (1, 0) — present-but-empty, distinct from the absent case above.
    let _: () = redis::cmd("XTRIM")
        .arg("events:del")
        .arg("MAXLEN")
        .arg(0)
        .query(&mut c)
        .expect("XTRIM");

    let v = streams_verbose(&mut c);
    assert_eq!(
        v["events:set"],
        (0, 0),
        "deleted stream: exists 0, length 0"
    );
    assert_eq!(
        v["events:del"],
        (1, 0),
        "trimmed to empty: exists 1, length 0"
    );

    // VERBOSE mutates nothing: the append-only registry still lists both names,
    // and the bare and WITHSTATS replies are unchanged.
    assert_eq!(streams(&mut c), vec!["events:del", "events:set"]);
    assert_eq!(streams_withstats(&mut c).len(), 2);
}

#[test]
fn prune_removes_dead_and_replicates() {
    let master = TestServer::start(&["events", "set,del"]);
    let mut mc = master.conn();
    let _: () = mc.set("a", "1").expect("SET");
    let _: () = mc.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "two streams registered", || {
        streams(&mut mc).len() == 2
    });

    let replica = TestServer::start_replica_of(&master, &["events", "set,del"]);
    let mut rc = replica.conn();
    wait_until(Duration::from_secs(10), "registry replicated", || {
        registry_members(&mut rc).len() == 2
    });

    // The readonly discovery commands must run on the replica (the #81
    // regression: a write-flagged STREAMS would fail here with -READONLY).
    assert_eq!(
        streams(&mut rc),
        vec!["events:del", "events:set"],
        "bare STREAMS works on a replica"
    );
    assert_eq!(
        streams_withstats(&mut rc).len(),
        2,
        "WITHSTATS works on a replica"
    );
    assert_eq!(
        streams_verbose(&mut rc).len(),
        2,
        "VERBOSE works on a replica"
    );
    // PRUNE is a write command and must be refused on the replica.
    redis::cmd("EVENTSTREAM.PRUNE")
        .query::<i64>(&mut rc)
        .expect_err("PRUNE must be refused on a replica (write command)");

    // Delete events:del so its key is absent; keep events:set live (it holds
    // the SET entry). The DEL replicates to the replica like any write.
    let _: () = redis::cmd("DEL")
        .arg("events:del")
        .query(&mut mc)
        .expect("DEL");

    assert_eq!(prune(&mut mc), 1, "one dead member removed");
    assert_eq!(
        streams(&mut mc),
        vec!["events:set"],
        "live stream retained, dead one pruned"
    );

    // The SREM replicated (replicated like the registration SADD, minus the
    // verify-oom M flag), so the replica's registry converges.
    wait_until(Duration::from_secs(10), "prune replicated", || {
        registry_members(&mut rc) == vec!["events:set"]
    });

    // A second prune with nothing dead removes nothing.
    assert_eq!(prune(&mut mc), 0);
}

#[test]
fn prune_lets_a_pruned_stream_reregister() {
    let s = TestServer::start(&["events", "set,del"]);
    let mut c = s.conn();
    let _: () = c.set("a", "1").expect("SET");
    let _: () = c.del("a").expect("DEL");
    wait_until(CAPTURE_WAIT, "two streams registered", || {
        streams(&mut c).len() == 2
    });
    // active_streams is the since-load distinct-stream count.
    assert_eq!(info_field(&mut c, "active_streams"), 2);

    // Delete events:del, then prune it out of the registry.
    let _: () = redis::cmd("DEL")
        .arg("events:del")
        .query(&mut c)
        .expect("DEL");
    assert_eq!(prune(&mut c), 1);
    assert_eq!(streams(&mut c), vec!["events:set"]);

    // The in-process dedupe was invalidated in the same operation (issue #81
    // hazard 2), so a later del event re-registers events:del rather than being
    // suppressed by a stale "already registered" cache entry.
    let _: () = c.set("x", "1").expect("SET");
    let _: () = c.del("x").expect("DEL");
    wait_until(CAPTURE_WAIT, "events:del re-registered", || {
        streams(&mut c) == vec!["events:del", "events:set"]
    });

    // ACTIVE_STREAMS is a lifetime counter that prune does not decrement, so
    // the re-registration bumps it to 3.
    assert_eq!(info_field(&mut c, "active_streams"), 3);
}

#[test]
fn prune_keeps_a_wrong_type_key_registered() {
    // A foreign, non-stream key parked at a registered name EXISTS but its XLEN
    // errors (WRONGTYPE). Prune keys off absence (EXISTS 0), not an XLEN error,
    // so such a key must NOT be pruned and must not be reported dead (issue #81
    // review): only truly absent keys are removed.
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();
    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "one stream registered", || {
        streams(&mut c) == vec!["events:set"]
    });

    // Replace the destination stream with a string at the same name: delete the
    // stream, then SET a string there. Writes under the prefix are dropped by
    // the feedback guard, so this does not re-mirror.
    let _: () = redis::cmd("DEL")
        .arg("events:set")
        .query(&mut c)
        .expect("DEL");
    let _: () = redis::cmd("SET")
        .arg("events:set")
        .arg("foreign-non-stream-value")
        .query(&mut c)
        .expect("SET");

    // Present wrong-type key: prune removes nothing, the registry keeps it.
    assert_eq!(
        prune(&mut c),
        0,
        "a present wrong-type key must not be pruned"
    );
    assert_eq!(streams(&mut c), vec!["events:set"], "name stays registered");

    // VERBOSE annotates it exists=1, length=0 — present but not a live stream,
    // distinct from an absent (dead) key.
    let v = streams_verbose(&mut c);
    assert_eq!(
        v["events:set"],
        (1, 0),
        "wrong-type key: exists 1 (EXISTS), length 0 (XLEN errored)"
    );
}

#[test]
fn prune_persists_across_restart_under_aof() {
    let s = TestServer::start_aof(&["events", "set,del"]);
    {
        let mut c = s.conn();
        let _: () = c.set("a", "1").expect("SET");
        let _: () = c.del("a").expect("DEL");
        wait_until(CAPTURE_WAIT, "two streams", || streams(&mut c).len() == 2);
        let _: () = redis::cmd("DEL")
            .arg("events:del")
            .query(&mut c)
            .expect("DEL");
        assert_eq!(prune(&mut c), 1);
        assert_eq!(streams(&mut c), vec!["events:set"]);
        let _ = redis::cmd("SHUTDOWN").arg("NOSAVE").query::<()>(&mut c);
    }

    // The prune SREM replicated into the AOF, so the pruned name does not come
    // back on replay.
    let s = s.restart_aof(&["events", "set,del"]);
    let mut c = s.conn();
    assert_eq!(
        streams(&mut c),
        vec!["events:set"],
        "pruned member must not reappear from the AOF"
    );
}

#[test]
fn registry_survives_restart_under_aof() {
    let s = TestServer::start_aof(&["events", "set,del"]);
    {
        let mut c = s.conn();
        let _: () = c.set("a", "1").expect("SET");
        let _: () = c.del("a").expect("DEL");
        wait_until(CAPTURE_WAIT, "two streams", || streams(&mut c).len() == 2);
        let _ = redis::cmd("SHUTDOWN").arg("NOSAVE").query::<()>(&mut c);
    }

    let s = s.restart_aof(&["events", "set,del"]);
    let mut c = s.conn();
    assert_eq!(
        streams(&mut c),
        vec!["events:del", "events:set"],
        "registry must replay from the AOF"
    );
}

#[test]
fn registry_rebuilds_after_flushall() {
    let s = TestServer::start(&["events", "set"]);
    let mut c = s.conn();

    let _: () = c.set("a", "1").expect("SET");
    wait_until(CAPTURE_WAIT, "registered", || !streams(&mut c).is_empty());

    let _: () = redis::cmd("FLUSHALL").query(&mut c).expect("FLUSHALL");
    assert!(
        streams(&mut c).is_empty(),
        "FLUSHALL deletes the registry set"
    );

    // The flush handler cleared the dedupe cache, so the next capture
    // re-registers its stream.
    let _: () = c.set("b", "2").expect("SET after flush");
    wait_until(CAPTURE_WAIT, "registry rebuilt", || {
        streams(&mut c) == vec!["events:set"]
    });

    // Per-stream counters count since load or last flush (issue #71): the
    // pre-flush write is gone from the per-stream row, while the global
    // counter remains strictly since-load.
    assert_eq!(streams_withstats(&mut c)["events:set"], (1, 0));
    assert_eq!(info_field(&mut c, "forwarded"), 2);
}

#[test]
fn registry_failure_is_observable_and_cap_still_bounds() {
    // Issue #216: a WRONGTYPE occupant at the registry key fails the SADD
    // after the per-event XADD succeeds. The event is still captured (not a
    // drop, not a lost event), the failure is observable via registry_errors,
    // and max-streams still bounds created streams because the cap counts on
    // XADD success, not on SADD success.
    let s = TestServer::start(&["events", "set,del", "max-streams", "1"]);
    let mut c = s.conn();
    // Occupy the registry key with a string before any capture. Writes under
    // the prefix are guarded, so this is never mirrored back.
    let _: () = c
        .set("events:#streams", "occupied")
        .expect("SET registry occupant");

    // One event: its XADD lands even though the registry SADD gets WRONGTYPE.
    let _: () = c.set("k1", "v").expect("SET k1");
    wait_until(
        CAPTURE_WAIT,
        "event captured despite registry failure",
        || xlen(&mut c, "events:set") >= 1,
    );
    wait_until(CAPTURE_WAIT, "registry failure observable", || {
        info_field(&mut c, "registry_errors") >= 1
    });
    // The event was captured: not a lost event, not a failed destination
    // write (the registry SADD is an auxiliary side effect, issue #218).
    assert_eq!(info_field(&mut c, "events_lost"), 0);
    assert_eq!(info_field(&mut c, "dropped"), 0);
    // Not discoverable while the registry key is the wrong type: EVENTSTREAM.STREAMS
    // reads that key, so it errors WRONGTYPE (the issue's own symptom) rather
    // than omitting the stream. The recovery step below confirms it registers
    // once the key is cleared.
    let streams_err: Result<Vec<String>, _> = redis::cmd("EVENTSTREAM.STREAMS").query(&mut c);
    assert!(
        streams_err.is_err(),
        "STREAMS reads the wrong-type registry key and must surface the error"
    );

    // The cap counted events:set on its XADD, so a second event name is
    // refused even though the first never registered.
    let _: () = c.del("k1").expect("DEL k1");
    wait_until(CAPTURE_WAIT, "second stream refused at the cap", || {
        info_field(&mut c, "dropped_max_streams") >= 1
    });
    assert_eq!(
        xlen(&mut c, "events:del"),
        0,
        "the cap bounds created streams even while the registry is failing"
    );

    // Recovery: clear the occupant and write again; the SADD now succeeds and
    // the stream becomes discoverable.
    let _: () = redis::cmd("DEL")
        .arg("events:#streams")
        .query(&mut c)
        .expect("DEL occupant");
    let _: () = c.set("k2", "v").expect("SET k2");
    wait_until(CAPTURE_WAIT, "stream registers after recovery", || {
        streams(&mut c).contains(&"events:set".to_string())
    });
}
