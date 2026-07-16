#!/usr/bin/env python3
"""Consumer examples for redis-event-stream-module (issue #110), redis-py.

Three subcommands, mapping 1:1 to docs/consumer-patterns.md:
    tail       live tail (pub/sub replacement)
    work       durable work queue (consumer groups) + stuck-work recovery
    reconcile  delimit capture gaps from the control stream's markers
    discover   list destination streams via EVENTSTREAM.STREAMS

Run against a server with the module loaded (default: expirations only):
    python3 consumer.py tail
    REDIS_HOST=10.0.0.5 REDIS_PORT=6380 python3 consumer.py work

Binary-safe keys (SPEC.md section 6): "Consumers must read `key` with a
bytes-typed client API; clients that eagerly decode replies as UTF-8 will
mangle non-UTF-8 keys, which is a client configuration issue, not stream data
loss." So this client is created WITHOUT decode_responses: replies stay bytes,
and the `key` field round-trips exactly. Field names are bytes too (b"event").
"""

import os
import sys
import time

import redis

STREAM = b"events:expired"       # the default-config destination stream
GROUP = "workers"
CONSUMER = os.environ.get("CONSUMER", "worker-1")
CONTROL = "events:#control"      # the gap-marker control stream (SPEC.md section 9)


def connect():
    # decode_responses is deliberately left off (defaults False): keep bytes so
    # a non-UTF-8 key is preserved rather than mangled.
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "127.0.0.1"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
    )


def show(entry_id, fields):
    """Print one mirrored entry. `fields` keys/values are bytes."""
    event = fields.get(b"event", b"").decode("utf-8", "replace")
    db = fields.get(b"db", b"").decode("ascii", "replace")
    key_bytes = fields.get(b"key", b"")
    # key is raw bytes; decode only for display, and only lossily.
    key_display = key_bytes.decode("utf-8", "replace")
    eid = entry_id.decode("ascii") if isinstance(entry_id, bytes) else entry_id
    print(f"  {eid}  event={event} db={db} key={key_display!r} ({len(key_bytes)} bytes)")


def tail(r):
    """Blocked XREAD, resuming from the last delivered ID (never re-passing $)."""
    last = b"$"  # only entries added after the first blocking call
    print(f"tailing {STREAM.decode()} (Ctrl-C to stop)")
    while True:
        resp = r.xread({STREAM: last}, block=0)
        if not resp:
            continue
        for _stream, entries in resp:
            for entry_id, fields in entries:
                show(entry_id, fields)
                last = entry_id  # resume from here, not $


def work(r):
    """Consumer-group work queue: drain own PEL, then tail >, ack, reclaim."""
    try:
        # MKSTREAM makes setup race-free against first capture; $ means "from
        # now" (use 0 to also process retained history — see Replay).
        r.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise  # the group already exists — idempotent

    # Startup: drain this consumer's own pending list (entries delivered but
    # never acked, e.g. a previous crash) by reading from id 0.
    pending_start = b"0"
    while True:
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: pending_start}, count=100)
        entries = resp[0][1] if resp else []
        if not entries:
            break
        for entry_id, fields in entries:
            process_and_ack(r, entry_id, fields)
            pending_start = entry_id  # advance within the PEL

    print(f"draining done; steady-state read as {CONSUMER} (Ctrl-C to stop)")
    sweeps = 0
    while True:
        # > = entries never delivered to any consumer in this group.
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: b">"}, count=100, block=5000)
        for _stream, entries in (resp or []):
            for entry_id, fields in entries:
                process_and_ack(r, entry_id, fields)
        # Periodically reclaim entries stuck with dead workers.
        sweeps += 1
        if sweeps % 4 == 0:
            reclaim(r)


def process_and_ack(r, entry_id, fields):
    show(entry_id, fields)
    # ... do the durable work here ...
    # Ack only after the work is durably done; a crash before this redelivers,
    # so processing must be idempotent (natural key: stream + entry ID).
    r.xack(STREAM, GROUP, entry_id)


def reclaim(r):
    """Reassign entries idle > 60s from dead workers; drop trimmed (nil) ones."""
    # redis-py returns (next_cursor, claimed_entries, deleted_ids). Entries
    # trimmed out of the stream while still pending come back with no fields;
    # treat those as lost, not as work (SPEC.md section 9, slow-consumer contract).
    result = r.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=60000,
                          start_id="0-0", count=100)
    claimed = result[1] if len(result) > 1 else []
    for entry_id, fields in claimed:
        if not fields:
            # nil-field entry: trimmed before we read it. XAUTOCLAIM already
            # removed it from the PEL; do not process it.
            continue
        process_and_ack(r, entry_id, fields)


def reconcile(r):
    """Pair open markers (disabled/unloading) with the next close (enabled/loaded)
    to print bounded capture-gap windows. Marker IDs are ms timestamps, so a
    window is directly usable as an XRANGE bound (see docs/loss-windows.md)."""
    entries = r.xrange(CONTROL, "-", "+")
    if not entries:
        print("no control stream yet (module never wrote a marker)")
        return
    open_marker = None
    print(f"markers on {CONTROL}:")
    for entry_id, fields in entries:
        action = fields.get(b"action", b"").decode()
        version = fields.get(b"module-version", b"").decode()
        eid = entry_id.decode("ascii")
        print(f"  {eid}  action={action} module-version={version}")
        if action in ("disabled", "unloading"):
            open_marker = (eid, action)
        elif action in ("enabled", "loaded"):
            if open_marker:
                print(f"    -> gap window [{open_marker[0]} .. {eid}] "
                      f"({open_marker[1]} -> {action}); reconcile this range")
                open_marker = None
    if open_marker:
        print(f"    -> open gap since {open_marker[0]} ({open_marker[1]}); "
              "capture still down or crashed (no closing marker)")


def discover(r):
    """List destination streams, skipping the module's own events:#* namespace."""
    names = r.execute_command("EVENTSTREAM.STREAMS")
    for name in names:
        # name is bytes; the control/firehose streams live under events:# and
        # are not event data (docs/consumer-patterns.md, Discovery).
        if name.startswith(b"events:#"):
            continue
        length = r.xlen(name)
        print(f"  {name.decode('utf-8', 'replace')}  xlen={length}")


def main():
    cmds = {"tail": tail, "work": work, "reconcile": reconcile, "discover": discover}
    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        print(f"usage: {sys.argv[0]} {{{'|'.join(cmds)}}}", file=sys.stderr)
        return 2
    r = connect()
    try:
        cmds[sys.argv[1]](r)
    except KeyboardInterrupt:
        print("\nstopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
