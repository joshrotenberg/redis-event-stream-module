#!/usr/bin/env node
// Consumer examples for redis-event-stream-module (issue #110), ioredis.
//
// Subcommands map 1:1 to docs/consumer-patterns.md:
//   tail       live tail (pub/sub replacement)
//   work       durable work queue (consumer groups) + stuck-work recovery
//   reconcile  delimit capture gaps from the control stream's markers
//   discover   list destination streams via EVENTSTREAM.STREAMS
//
// Run against a server with the module loaded (default: expirations only):
//   node consumer.js tail
//   REDIS_HOST=10.0.0.5 REDIS_PORT=6380 node consumer.js work
//
// Why ioredis (not node-redis): its `*Buffer` command variants return replies
// as Buffers, which makes the binary-safe key requirement below trivial to
// honor. Binary-safe keys (SPEC.md section 6): "Consumers must read key with a
// bytes-typed client API; clients that eagerly decode replies as UTF-8 will
// mangle non-UTF-8 keys." So every read here uses the Buffer variant and the
// key field is kept as a Buffer; it is decoded only for display.

const Redis = require("ioredis");

const STREAM = "events:expired"; // the default-config destination stream
const GROUP = "workers";
const CONTROL = "events:#control"; // gap-marker control stream (SPEC.md section 9)
const CONSUMER = process.env.CONSUMER || "worker-1";

function connect() {
  return new Redis({
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: parseInt(process.env.REDIS_PORT || "6379", 10),
  });
}

// Build a { field: Buffer } map from a flat [field, value, ...] Buffer array.
function toFields(flat) {
  const m = {};
  for (let i = 0; i + 1 < flat.length; i += 2) m[flat[i].toString()] = flat[i + 1];
  return m;
}

// Print one mirrored entry. Field values are Buffers.
function show(id, fields) {
  const event = (fields.event || Buffer.alloc(0)).toString("utf8");
  const db = (fields.db || Buffer.alloc(0)).toString("ascii");
  const keyBuf = fields.key || Buffer.alloc(0); // raw key bytes
  const keyDisplay = JSON.stringify(keyBuf.toString("utf8"));
  console.log(`  ${id}  event=${event} db=${db} key=${keyDisplay} (${keyBuf.length} bytes)`);
}

// Blocked XREAD, resuming from the last delivered ID (never re-passing $).
async function tail(r) {
  let last = "$"; // only entries added after the first blocking call
  console.log(`tailing ${STREAM} (Ctrl-C to stop)`);
  for (;;) {
    const res = await r.xreadBuffer("BLOCK", 0, "STREAMS", STREAM, last);
    if (!res) continue;
    for (const [, entries] of res) {
      for (const [idBuf, flat] of entries) {
        const id = idBuf.toString();
        show(id, toFields(flat));
        last = id; // resume from here, not $
      }
    }
  }
}

async function processAndAck(r, id, fields) {
  show(id, fields);
  // ... do the durable work here ...
  // Ack only after the work is durably done; a crash before this redelivers,
  // so processing must be idempotent (natural key: stream + entry ID).
  await r.xack(STREAM, GROUP, id);
}

// Reassign entries idle > 60s from dead workers, dropping trimmed (nil-field)
// ones — treat those as lost, not work (SPEC.md section 9, slow-consumer contract).
async function reclaim(r) {
  // [nextCursor, claimedEntries, deletedIds]; trimmed entries come back with a
  // null field list (or land in deletedIds) — skip them.
  const res = await r.xautoclaimBuffer(STREAM, GROUP, CONSUMER, 60000, "0-0", "COUNT", 100);
  const claimed = res[1] || [];
  for (const [idBuf, flat] of claimed) {
    if (!flat || flat.length === 0) continue; // trimmed before we read it
    await processAndAck(r, idBuf.toString(), toFields(flat));
  }
}

// Consumer-group work queue: drain own PEL, then tail >, ack, reclaim.
async function work(r) {
  try {
    // MKSTREAM makes setup race-free against first capture; $ = "from now"
    // (use 0 to also process retained history).
    await r.xgroup("CREATE", STREAM, GROUP, "$", "MKSTREAM");
  } catch (e) {
    if (!String(e.message).includes("BUSYGROUP")) throw e; // idempotent
  }

  // Startup: drain this consumer's own pending list (delivered-but-unacked,
  // e.g. a previous crash) by reading from id 0.
  let pendingStart = "0";
  for (;;) {
    const res = await r.xreadgroupBuffer(
      "GROUP", GROUP, CONSUMER, "COUNT", 100, "STREAMS", STREAM, pendingStart);
    const entries = res && res[0] ? res[0][1] : [];
    if (!entries || entries.length === 0) break;
    for (const [idBuf, flat] of entries) {
      const id = idBuf.toString();
      await processAndAck(r, id, toFields(flat));
      pendingStart = id;
    }
  }

  console.log(`draining done; steady-state read as ${CONSUMER} (Ctrl-C to stop)`);
  let sweeps = 0;
  for (;;) {
    // > = entries never delivered to any consumer in this group.
    const res = await r.xreadgroupBuffer(
      "GROUP", GROUP, CONSUMER, "COUNT", 100, "BLOCK", 5000, "STREAMS", STREAM, ">");
    for (const [, entries] of res || []) {
      for (const [idBuf, flat] of entries) {
        await processAndAck(r, idBuf.toString(), toFields(flat));
      }
    }
    if (++sweeps % 4 === 0) await reclaim(r);
  }
}

// Pair open markers (disabled/unloading) with the next close (enabled/loaded)
// to print bounded capture-gap windows. Marker IDs are ms timestamps, usable
// directly as XRANGE bounds (see docs/loss-windows.md).
async function reconcile(r) {
  const entries = await r.xrangeBuffer(CONTROL, "-", "+");
  if (!entries || entries.length === 0) {
    console.log("no control stream yet (module never wrote a marker)");
    return;
  }
  console.log(`markers on ${CONTROL}:`);
  let open = null;
  for (const [idBuf, flat] of entries) {
    const f = toFields(flat);
    const id = idBuf.toString();
    const action = (f.action || Buffer.alloc(0)).toString();
    const version = (f["module-version"] || Buffer.alloc(0)).toString();
    console.log(`  ${id}  action=${action} module-version=${version}`);
    if (action === "disabled" || action === "unloading") {
      open = { id, action };
    } else if (action === "enabled" || action === "loaded") {
      if (open) {
        console.log(`    -> gap window [${open.id} .. ${id}] (${open.action} -> ${action}); reconcile this range`);
        open = null;
      }
    }
  }
  if (open) {
    console.log(`    -> open gap since ${open.id} (${open.action}); capture still down or crashed (no closing marker)`);
  }
}

// List destination streams, skipping the module's own events:#* namespace.
async function discover(r) {
  const names = await r.call("EVENTSTREAM.STREAMS");
  for (const name of names) {
    if (String(name).startsWith("events:#")) continue; // control/firehose, not data
    const len = await r.xlen(name);
    console.log(`  ${name}  xlen=${len}`);
  }
}

async function main() {
  const cmds = { tail, work, reconcile, discover };
  const cmd = process.argv[2];
  if (!cmds[cmd]) {
    console.error("usage: consumer.js {tail|work|reconcile|discover}");
    process.exit(2);
  }
  const r = connect();
  try {
    await cmds[cmd](r);
  } finally {
    r.disconnect();
  }
}

main().catch((e) => {
  console.error("error:", e);
  process.exit(1);
});
