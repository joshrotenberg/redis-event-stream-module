#!/usr/bin/env bash
# Performance measurement for the SPEC.md section 11 plan.
#
# Scenarios against a local server (select with BENCH_SCENARIOS), each run
# BENCH_REPS times; the median rep is reported:
#
#   S0  baseline, no module loaded          (pure SET workload)
#   S1  module loaded, default filter        (captures nothing: the gate tax
#                                             every non-capturing deployment pays)
#   S2  module loaded, events=set            (100% capture: one extra XADD plus
#                                             inline approximate MAXLEN per SET)
#   S3  mass-expiry drain                    (foreground GET p50/p99 and drain
#                                             duration while an expiring-key
#                                             backlog drains, with and without
#                                             the module: the storm case whose
#                                             foreground p99 section 11 says to
#                                             watch)
#   S4  maxlen sensitivity                   (the S2 workload across
#                                             eventstream.maxlen values; section
#                                             11 predicts near-zero amortized
#                                             trim cost at any value)
#
# SPEC.md section 11 specifies memtier_benchmark with 60-second runs. This
# script uses redis-benchmark, which ships with every Redis and Valkey and so
# needs no extra install; override the knobs below (or swap in memtier) to match
# the spec exactly. The tool used is printed and should be cited alongside any
# published numbers.
#
# When BENCH_JSON names a file, results are also written there as a JSON array
# (one object per scenario) so CI can gate on ratios without parsing prose;
# see bench/gate.sh for the thresholds.
#
# Usage:
#   bench/run.sh                          # all scenarios, defaults below
#   BENCH_SCENARIOS="s0 s1 s2" bench/run.sh
#   BENCH_REQUESTS=2000000 bench/run.sh
#   BENCH_JSON=results.json bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${BENCH_PORT:-6392}"
REPS="${BENCH_REPS:-3}"
REQUESTS="${BENCH_REQUESTS:-1000000}"
CLIENTS="${BENCH_CLIENTS:-50}"
THREADS="${BENCH_THREADS:-4}"
KEYSPACE="${BENCH_KEYSPACE:-100000}"
PAYLOAD="${BENCH_PAYLOAD:-64}"
SCENARIOS="${BENCH_SCENARIOS:-s0 s1 s2 s3 s4}"
JSON_OUT="${BENCH_JSON:-}"

# S3 knobs: EXPIRE_KEYS keys get PX TTLs staggered uniformly across
# [TTL_MIN_MS, TTL_MIN_MS+TTL_SPREAD_MS], then FG_REQUESTS GETs run as the
# foreground workload while the backlog drains. FG_CLIENTS is deliberately
# small: the scenario measures foreground latency under drain pressure, not
# GET throughput.
# Known dilution: the fg pass is not coupled to the drain window, so on a fast
# host it can finish before most keys expire, diluting the p99 the gate
# watches — always in the pass direction (never a flake), but it weakens the
# signal. Revisit FG_REQUESTS/TTL_MIN_MS coupling once nightly variance data
# exists; the exact gates (dropped == 0, forwarded == expected) are
# unaffected.
EXPIRE_KEYS="${BENCH_EXPIRE_KEYS:-200000}"
TTL_MIN_MS="${BENCH_TTL_MIN_MS:-1000}"
TTL_SPREAD_MS="${BENCH_TTL_SPREAD_MS:-4000}"
FG_REQUESTS="${BENCH_FG_REQUESTS:-500000}"
FG_CLIENTS="${BENCH_FG_CLIENTS:-8}"

# S4 maxlen values: below one listpack node, the default, large, unbounded.
MAXLENS="${BENCH_MAXLENS:-100 10000 1000000 0}"

SERVER_BIN="${TEST_REDIS_SERVER_BIN:-redis-server}"
CLI_BIN="${TEST_REDIS_CLI_BIN:-redis-cli}"
BENCH_BIN="${BENCH_TOOL:-redis-benchmark}"

command -v "$SERVER_BIN" >/dev/null || { echo "no server binary: $SERVER_BIN"; exit 1; }
command -v "$CLI_BIN" >/dev/null || { echo "no cli binary: $CLI_BIN"; exit 1; }
command -v "$BENCH_BIN" >/dev/null || { echo "no benchmark binary: $BENCH_BIN"; exit 1; }

EXT=so
[[ "$(uname)" == "Darwin" ]] && EXT=dylib
MODULE="$PWD/target/release/libredis_event_stream_module.$EXT"

echo "building module..."
cargo build --release >/dev/null

WORKDIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

want() { [[ " $SCENARIOS " == *" $1 "* ]]; }

start_server() { # extra loadmodule args, or none for no module
  # Pass "$@" directly: an empty "$@" is safe under `set -u`, unlike an empty
  # named-array expansion on bash 3.2 (the macOS default).
  "$SERVER_BIN" --port "$PORT" --dir "$WORKDIR" --save '' --appendonly no \
    --enable-module-command yes --logfile "$WORKDIR/server.log" "$@" &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    "$CLI_BIN" -p "$PORT" ping >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server failed to start"; cat "$WORKDIR/server.log"; exit 1
}

stop_server() {
  "$CLI_BIN" -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

# Module counter from INFO; empty when the module is not loaded (the `|| true`
# absorbs grep's no-match status under pipefail).
es_field() { "$CLI_BIN" -p "$PORT" info eventstream 2>/dev/null | grep "eventstream_$1:" | cut -d: -f2 | tr -d '\r' || true; }

# Run one redis-benchmark pass; echo "rps p50 p99".
# CSV output is one header line plus one data row:
#   "SET","rps","avg","min","p50","p95","p99","max"
bench_once() { # test-name requests clients threads
  local out
  out="$("$BENCH_BIN" -p "$PORT" -t "$1" -n "$2" -c "$3" \
    --threads "$4" -d "$PAYLOAD" -r "$KEYSPACE" --csv 2>/dev/null)"
  local parsed
  parsed="$(awk -F',' 'NR==2 { gsub(/"/, ""); print $2, $5, $7 }' <<<"$out")"
  echo "${parsed:-NA NA NA}"
}

# Median of REPS runs by rps; echo the median "rps p50 p99".
scenario() { # label ; then loadmodule args
  local label="$1"; shift
  echo "== $label ==" >&2
  start_server "$@"
  local rows=()
  for r in $(seq 1 "$REPS"); do
    local row; row="$(bench_once set "$REQUESTS" "$CLIENTS" "$THREADS")"
    echo "  rep $r: $row" >&2
    rows+=("$row")
  done
  stop_server
  # median by rps (column 1)
  printf '%s\n' "${rows[@]}" | sort -n -k1 | awk -v n="${#rows[@]}" 'NR==int((n+1)/2)'
}

# Preload EXPIRE_KEYS keys with staggered PX TTLs via the inline protocol;
# seeded srand keeps the stagger reproducible across reps.
preload_expiring() {
  awk -v n="$EXPIRE_KEYS" -v min="$TTL_MIN_MS" -v spread="$TTL_SPREAD_MS" \
    'BEGIN{srand(42); for(i=0;i<n;i++) printf "SET exp:%d v PX %d\r\n", i, min+int(rand()*spread)}' \
    | "$CLI_BIN" -p "$PORT" --pipe >/dev/null
}

# Expiring keys still alive: INFO keyspace `expires` counts keys with a TTL in
# the main dict, so it tracks active-expiry reclamation without a SCAN (which
# would both perturb the drain and lazily expire keys itself). The module's
# streams have no TTL and so never count.
expires_left() {
  "$CLI_BIN" -p "$PORT" info keyspace | awk -F'[=,]' '/^db0:/ {print $4; found=1} END{if(!found) print 0}' | tr -d '\r'
}

# One S3 rep: build the expiring backlog, run the foreground GET pass while it
# drains, then wait out the drain. Echo "rps p50 p99 drain_s"; the drain clock
# runs from backlog completion to the last reclaimed key, so it overlaps the
# foreground pass by design. The GETs target the key:* keyspace (all misses):
# a miss still crosses the main thread, which is where drain pressure lands.
mass_expiry_once() {
  "$CLI_BIN" -p "$PORT" flushall >/dev/null
  preload_expiring
  local t0 parsed
  t0="$(date +%s)"
  parsed="$(bench_once get "$FG_REQUESTS" "$FG_CLIENTS" 2)"
  local left
  for _ in $(seq 1 600); do
    left="$(expires_left)"
    [[ "${left:-1}" -eq 0 ]] && break
    sleep 0.2
  done
  echo "$parsed $(( $(date +%s) - t0 ))"
}

# S3 wrapper: median of REPS by foreground p99, the number section 11 says to
# watch during a drain. Counters accumulate across reps (the server stays up),
# so forwarded/dropped are read once at the end and compared against
# REPS * EXPIRE_KEYS by the caller; both echo as NA without the module.
mass_expiry_scenario() { # label ; then loadmodule args
  local label="$1"; shift
  echo "== $label ==" >&2
  start_server "$@"
  local rows=()
  for r in $(seq 1 "$REPS"); do
    local row; row="$(mass_expiry_once)"
    echo "  rep $r: $row" >&2
    rows+=("$row")
  done
  local fwd drop
  fwd="$(es_field forwarded)"
  drop="$(es_field dropped)"
  stop_server
  local med
  med="$(printf '%s\n' "${rows[@]}" | sort -n -k3 | awk -v n="${#rows[@]}" 'NR==int((n+1)/2)')"
  echo "$med ${fwd:-NA} ${drop:-NA}"
}

pct() { # value baseline -> signed percent vs baseline
  awk -v v="$1" -v b="$2" 'BEGIN{ if(b+0==0){print "NA"} else {printf "%+.1f%%", (v-b)/b*100} }'
}

num() { # NA/empty -> null, so a failed pass yields valid (and gate-failing) JSON
  case "$1" in ''|NA) echo null ;; *) echo "$1" ;; esac
}

ROWS="$WORKDIR/rows.jsonl"
: > "$ROWS"
emit_row() { # scenario rps p50 p99 [extra-json-fields]
  printf '{"scenario":"%s","tool":"%s","ops_per_sec":%s,"p50_ms":%s,"p99_ms":%s,"reps":%s%s}\n' \
    "$1" "$BENCH_BIN" "$(num "$2")" "$(num "$3")" "$(num "$4")" "$REPS" "${5:-}" >> "$ROWS"
}

echo "tool: $BENCH_BIN | server: $SERVER_BIN | scenarios: $SCENARIOS"
echo "reqs=$REQUESTS clients=$CLIENTS threads=$THREADS keyspace=$KEYSPACE payload=${PAYLOAD}B reps=$REPS"
if want s3; then
  echo "s3: expire_keys=$EXPIRE_KEYS ttl=${TTL_MIN_MS}+${TTL_SPREAD_MS}ms fg_reqs=$FG_REQUESTS fg_clients=$FG_CLIENTS"
fi
if want s4; then
  echo "s4: maxlens=$MAXLENS"
fi
echo

TABLE="$WORKDIR/table.md"
: > "$TABLE"
s0r=""

if want s0; then
  read -r s0r s0p50 s0p99 <<<"$(scenario "S0 baseline (no module)")"
  printf "| S0 baseline (no module) | %s | - | %s | %s |\n" "$s0r" "$s0p50" "$s0p99" >> "$TABLE"
  emit_row s0 "$s0r" "$s0p50" "$s0p99"
fi
if want s1; then
  read -r s1r s1p50 s1p99 <<<"$(scenario "S1 module loaded, default filter (no capture)" --loadmodule "$MODULE")"
  printf "| S1 loaded, no capture | %s | %s | %s | %s |\n" "$s1r" "$(pct "$s1r" "$s0r")" "$s1p50" "$s1p99" >> "$TABLE"
  emit_row s1 "$s1r" "$s1p50" "$s1p99"
fi
if want s2; then
  read -r s2r s2p50 s2p99 <<<"$(scenario "S2 module loaded, events=set (full capture)" --loadmodule "$MODULE" events set)"
  printf "| S2 loaded, full capture | %s | %s | %s | %s |\n" "$s2r" "$(pct "$s2r" "$s0r")" "$s2p50" "$s2p99" >> "$TABLE"
  emit_row s2 "$s2r" "$s2p50" "$s2p99"
fi

if want s4; then
  for ml in $MAXLENS; do
    read -r s4r s4p50 s4p99 <<<"$(scenario "S4 events=set maxlen=$ml (full capture)" --loadmodule "$MODULE" events set maxlen "$ml")"
    printf "| S4 full capture, maxlen=%s | %s | %s | %s | %s |\n" "$ml" "$s4r" "$(pct "$s4r" "$s0r")" "$s4p50" "$s4p99" >> "$TABLE"
    emit_row "s4_maxlen_$ml" "$s4r" "$s4p50" "$s4p99"
  done
fi

TABLE3="$WORKDIR/table3.md"
: > "$TABLE3"
if want s3; then
  read -r b3r b3p50 b3p99 b3drain _ _ <<<"$(mass_expiry_scenario "S3 mass-expiry drain (no module)")"
  printf "| no module | %s | %s | %s | %s | - | - |\n" "$b3r" "$b3p50" "$b3p99" "$b3drain" >> "$TABLE3"
  emit_row s3_nomodule "$b3r" "$b3p50" "$b3p99" ",\"drain_s\":$(num "$b3drain")"
  read -r m3r m3p50 m3p99 m3drain m3fwd m3drop <<<"$(mass_expiry_scenario "S3 mass-expiry drain (events=expired)" --loadmodule "$MODULE" events expired)"
  printf "| events=expired | %s | %s | %s | %s | %s | %s |\n" "$m3r" "$m3p50" "$m3p99" "$m3drain" "$m3fwd" "$m3drop" >> "$TABLE3"
  emit_row s3_module "$m3r" "$m3p50" "$m3p99" \
    ",\"drain_s\":$(num "$m3drain"),\"forwarded\":$(num "$m3fwd"),\"dropped\":$(num "$m3drop"),\"expected_forwarded\":$((REPS * EXPIRE_KEYS))"
fi

if [[ -s "$TABLE" ]]; then
  echo
  echo "## Results (median of $REPS)"
  echo
  echo "| Scenario | ops/sec | vs S0 | p50 (ms) | p99 (ms) |"
  echo "|---|---|---|---|---|"
  cat "$TABLE"
fi

if [[ -s "$TABLE3" ]]; then
  echo
  echo "## S3 mass-expiry drain (median of $REPS by fg p99)"
  echo
  echo "$EXPIRE_KEYS keys, TTLs staggered across ${TTL_MIN_MS}-$((TTL_MIN_MS + TTL_SPREAD_MS))ms;"
  echo "foreground: $FG_REQUESTS GETs at $FG_CLIENTS clients during the drain."
  echo
  echo "| Variant | fg GET ops/sec | fg p50 (ms) | fg p99 (ms) | drain (s) | forwarded | dropped |"
  echo "|---|---|---|---|---|---|---|"
  cat "$TABLE3"
fi

if [[ -n "$JSON_OUT" ]]; then
  { echo '['; sed '$!s/$/,/' "$ROWS"; echo ']'; } > "$JSON_OUT"
  echo
  echo "json: $JSON_OUT"
fi
