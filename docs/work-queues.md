# Durable work queues

This is the pattern that replaces periodic keyspace scanning: each expiration
becomes a unit of work, delivered at least once, split across a pool of workers,
surviving worker restarts. It builds on the basics in
[Consumer patterns](./consumer-patterns.md).

Create the group once. `MKSTREAM` creates the stream if the first event has not
been captured yet, so setup does not race against capture:

```
XGROUP CREATE events:expired workers $ MKSTREAM
```

Use `$` to process only events from now on, or `0` to also process everything
currently retained in the stream (see
[Replay](./consumer-patterns.md#replay)).

Each worker loops. First drain its own pending list (entries it received but
never acknowledged, for example because it crashed mid-processing), then switch
to new entries:

```
# once at startup: claim back anything this worker had in flight
XREADGROUP GROUP workers worker-1 COUNT 100 STREAMS events:expired 0

# steady state: new, never-delivered entries
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:expired >
```

`>` means "entries never delivered to any consumer in this group". After
processing an entry, acknowledge it so it leaves the pending list:

```
XACK events:expired workers 1730000000123-0
```

Ack only after the work is durably done. An entry stays pending until acked, so
a crash between processing and `XACK` results in redelivery, which is why
consumers must be idempotent (natural key: stream name plus entry ID).

## Letting the module create the group (`eventstream.auto-group`)

The `XGROUP CREATE ... $ MKSTREAM` recipe above is race-free only when the
consumers deploy *before* the module: `MKSTREAM` makes an empty stream and the
group at `$` sees every later entry. In the common order — enable capture, then
roll out workers — the stream already holds entries when the recipe runs, and a
group at `$` silently skips everything captured before it. The fix is to create
the group at `0` (see [Replay](./consumer-patterns.md#replay)), but that
requires knowing which ordering you are in.

`eventstream.auto-group` removes the decision. Name a group and the module
creates it at `0` on each destination stream the first time it writes to that
stream, so the group exists from the stream's first entry no matter which side
deployed first:

```
CONFIG SET eventstream.auto-group workers
# or as a load-time arg: --loadmodule ... auto-group workers
```

Workers then skip `XGROUP CREATE` entirely — drain their pending list, then
tail:

```
XREADGROUP GROUP workers worker-1 COUNT 100 STREAMS events:expired 0   # backlog
XREADGROUP GROUP workers worker-1 COUNT 100 BLOCK 5000 STREAMS events:expired >
```

Notes:

- Off by default; empty means group creation stays operator-side (this page's
  manual recipe still works unchanged).
- The group is created with the same replicated, memory-checked write options as
  a mirrored entry, so it appears on replicas and survives an AOF replay.
- Idempotent: re-creating an existing group is a no-op (`BUSYGROUP` is treated as
  success), and a `FLUSHALL` that wiped the stream re-provisions the group on the
  next write.
- It covers per-event streams and the firehose (`events:#firehose`), but not the
  control stream (`events:#control`), which is not a work queue.
- Setting it at runtime provisions the group on each stream's **next** write, not
  retroactively: a stream that never fires again keeps no group.
- It does not upgrade the delivery guarantee. A group at `0` still loses entries
  trimmed by `maxlen` before a slow consumer catches up (SPEC.md section 9,
  slow-consumer contract). The win is operational: the group exists from birth,
  so deployment ordering stops mattering.
- Watch `eventstream_autogroup_created` / `eventstream_autogroup_failed` in
  `INFO eventstream` to confirm provisioning.

## Recovering stuck work

If a worker dies without acking, its entries sit in the group's pending list
under a dead consumer name. Periodically reassign entries idle longer than a
timeout to a live worker:

```
XAUTOCLAIM events:expired workers worker-2 60000 0 COUNT 100
```

`XAUTOCLAIM` also clears dead references: if an entry was trimmed out of the
stream while still pending (see [Sizing and retention](./sizing.md)), it reads
back with a nil field list and `XAUTOCLAIM` drops it from the pending list as
it scans. Treat a nil-field claimed entry as lost, not as work to do (SPEC.md
section 9, slow-consumer contract).

## Dead-lettering poison entries

Pending entries fail in three distinct ways, and only the third needs a
dead-letter:

1. **Crash before `XACK`.** The entry is redelivered; idempotent processing
   (natural key: stream + entry ID) makes this benign.
2. **Trimmed while pending.** The entry reads back with nil fields and
   `XAUTOCLAIM` drops it — handled above; treat as lost, not work.
3. **Poison entry.** The entry is delivered fine but *fails processing on every
   attempt* (malformed downstream state, an unactionable key, a bug triggered by
   one payload). Under the patterns above it cycles forever: claimed, fails,
   goes idle, is `XAUTOCLAIM`ed to another worker, fails again — pinning workers
   and growing the PEL without bound. Break the cycle with a dead-letter.

Redis Streams already track what you need: `XPENDING` and `XAUTOCLAIM` return a
per-entry **delivery counter**, incremented on each `XREADGROUP` delivery and
each non-`JUSTID` `XAUTOCLAIM`. Inspect it without inflating it using
`XAUTOCLAIM ... JUSTID` (which does not increment) or `XPENDING` — the fourth
element per entry is the delivery count:

```
XPENDING events:expired workers IDLE 60000 - + 100
```

After N deliveries (keep N small — 3 to 5; retries beyond that rarely succeed
and each costs a worker slot), copy the entry to an application-owned
dead-letter stream and acknowledge the original so it leaves the PEL:

```
# entry 1730000000123-0 has reached delivery-count N:
XADD myapp:dead-letter * source-stream events:expired source-id 1730000000123-0 \
  deliveries 5 event expired key session:abc db 0
XACK events:expired workers 1730000000123-0
```

Carry the original stream name, entry ID, delivery count, and all three entry
fields, so the dead-letter record stands alone after the source entry is
trimmed. Do the delivery-count check in both the startup PEL drain and the
periodic `XAUTOCLAIM` sweep — those are where old entries resurface.

**The dead-letter stream must live outside the module's prefix** (not
`events:*`). Two reasons: the prefix feedback guard only protects the module's
own streams, so an `events:*`-named dead-letter would itself be captured if the
filter is ever widened to `xadd`-family events; and the least-privilege
consumer ACL
([Consumer patterns](./consumer-patterns.md#restricting-consumer-access)) is
deliberately read-only over `events:*`. A dead-lettering consumer therefore
also needs write access to its own stream:

```
ACL SETUSER events-consumer on >secret ~events:* +@read +xreadgroup +xack +xautoclaim +xinfo \
  ~myapp:dead-letter +xadd
```

The dead-letter stream is itself an ordinary stream: `XLEN`/`XRANGE` it, alert on
growth, and drain it with the replay patterns in
[Consumer patterns](./consumer-patterns.md#replay).
