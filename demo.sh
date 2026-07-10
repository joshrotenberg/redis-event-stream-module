#!/usr/bin/env bash
# End-to-end demo: load the module, expire a key, read the mirrored event back
# out of the durable stream. Requires redis-server (7.2+) and redis-cli on PATH.
set -euo pipefail

cd "$(dirname "$0")"

EXT=so
[[ "$(uname)" == "Darwin" ]] && EXT=dylib
MODULE="target/release/libredis_event_stream_module.${EXT}"
PORT=6399

cargo build --release

# Note: notify-keyspace-events is deliberately NOT set. Module subscribers
# receive keyspace events regardless of that setting (it only gates pub/sub).
redis-server --port "$PORT" --daemonize no \
    --loadmodule "$PWD/$MODULE" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

sleep 1
r() { redis-cli -p "$PORT" "$@"; }

echo "== module config =="
r EVENTSTREAM.STATS

echo "== set a key with a 100ms TTL =="
r SET foo bar PX 100

echo "== wait for expiry, then force the lazy check with a lookup =="
sleep 0.3
r GET foo >/dev/null || true
sleep 0.2

echo "== durable expired-event stream =="
r XREAD COUNT 10 STREAMS events:expired 0

echo "== durable set-event stream (the SET) =="
r XREAD COUNT 10 STREAMS events:set 0

echo "== stats =="
r EVENTSTREAM.STATS
