#!/usr/bin/env bash
# Pre-flight check against a server that should already have the module
# loaded. Read-mostly: writes one probe key with a short TTL, waits for the
# expired event to land in the stream, and reports pass/fail per check.
#
# Usage:
#   ./demo-preflight.sh                          # 127.0.0.1:6379
#   ./demo-preflight.sh -h host -p port [-a pw]  # any redis-cli connect args
#
# All arguments pass through to redis-cli, so TLS flags etc. work too.
set -uo pipefail

R=(redis-cli "$@")
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "ok    $1"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL  $1${2:+: $2}"; }

# 0. Reachable.
if [[ "$("${R[@]}" PING 2>/dev/null)" != "PONG" ]]; then
  fail "server reachable (PING)"; echo "----"; echo "PASS=$PASS FAIL=$FAIL"; exit 1
fi
ok "server reachable (PING)"

# 1. Module loaded.
if [[ "$("${R[@]}" MODULE LIST 2>/dev/null | grep -c eventstream)" -ge 1 ]]; then
  ok "module loaded (MODULE LIST)"
else
  fail "module loaded (MODULE LIST)" "eventstream not present"
  echo "----"; echo "PASS=$PASS FAIL=$FAIL"; exit 1
fi

# 2. Effective config, echoed for eyeballing.
echo "----  CONFIG GET eventstream.*"
"${R[@]}" CONFIG GET 'eventstream.*' | paste - - | sed 's/^/      /'
PREFIX=$("${R[@]}" CONFIG GET eventstream.stream-prefix | tail -1)
EVENTS=$("${R[@]}" CONFIG GET eventstream.events | tail -1)
ENABLED=$("${R[@]}" CONFIG GET eventstream.enabled | tail -1)
[[ "$ENABLED" == "yes" ]] && ok "capture enabled" || fail "capture enabled" "eventstream.enabled=$ENABLED"

# 3. Filter covers expirations (the demo events).
if [[ "$EVENTS" == "*" || ",$EVENTS," == *",expired,"* || "$EVENTS" == *"@expired"* ]]; then
  ok "filter includes expired (events=$EVENTS)"
else
  fail "filter includes expired" "events=$EVENTS captures no expirations"
fi

# 4. End to end: probe key with a short TTL must land in <prefix>expired.
STREAM="${PREFIX}expired"
BEFORE=$("${R[@]}" XLEN "$STREAM" 2>/dev/null || echo 0)
"${R[@]}" SET demo:preflight:probe 1 PX 150 >/dev/null
DEADLINE=$((SECONDS + 10))
CAPTURED=no
while (( SECONDS < DEADLINE )); do
  "${R[@]}" GET demo:preflight:probe >/dev/null 2>&1  # force lazy expiry
  AFTER=$("${R[@]}" XLEN "$STREAM" 2>/dev/null || echo 0)
  if (( AFTER > BEFORE )); then CAPTURED=yes; break; fi
  sleep 0.2
done
[[ "$CAPTURED" == "yes" ]] && ok "probe expiration captured ($STREAM XLEN $BEFORE -> $AFTER)" \
                           || fail "probe expiration captured" "$STREAM did not grow within 10s"

# 5. Discovery and counters.
STREAMS=$("${R[@]}" EVENTSTREAM.STREAMS 2>/dev/null | tr '\n' ' ')
[[ -n "${STREAMS// /}" ]] && ok "EVENTSTREAM.STREAMS lists: $STREAMS" || fail "EVENTSTREAM.STREAMS" "empty reply"
FWD=$("${R[@]}" INFO eventstream 2>/dev/null | grep '^eventstream_forwarded:' | cut -d: -f2 | tr -d '\r')
DROP=$("${R[@]}" INFO eventstream 2>/dev/null | grep '^eventstream_dropped:' | cut -d: -f2 | tr -d '\r')
[[ "${FWD:-0}" -ge 1 ]] && ok "forwarded counter climbing (forwarded=$FWD)" || fail "forwarded counter" "forwarded=${FWD:-missing}"
[[ "${DROP:-0}" -eq 0 ]] && ok "no drops (dropped=0)" || fail "no drops" "dropped=$DROP, check INFO eventstream and the server log"

echo "----"
echo "PASS=$PASS FAIL=$FAIL"
(( FAIL == 0 )) && echo "preflight clean" || echo "preflight FAILED"
exit $FAIL
