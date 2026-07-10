#!/usr/bin/env bash
# Performance measurement for the SPEC.md section 11 plan.
#
# Three scenarios against a local server, each run BENCH_REPS times; the median
# rep is reported:
#
#   S0  baseline, no module loaded          (pure SET workload)
#   S1  module loaded, default filter        (captures nothing: the gate tax
#                                             every non-capturing deployment pays)
#   S2  module loaded, events=set            (100% capture: one extra XADD plus
#                                             inline approximate MAXLEN per SET)
#
# SPEC.md section 11 specifies memtier_benchmark with 60-second runs. This
# script uses redis-benchmark, which ships with every Redis and Valkey and so
# needs no extra install; override the knobs below (or swap in memtier) to match
# the spec exactly. The tool used is printed and should be cited alongside any
# published numbers.
#
# Usage:
#   bench/run.sh                 # defaults below
#   BENCH_REQUESTS=2000000 bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${BENCH_PORT:-6392}"
REPS="${BENCH_REPS:-3}"
REQUESTS="${BENCH_REQUESTS:-1000000}"
CLIENTS="${BENCH_CLIENTS:-50}"
THREADS="${BENCH_THREADS:-4}"
KEYSPACE="${BENCH_KEYSPACE:-100000}"
PAYLOAD="${BENCH_PAYLOAD:-64}"

SERVER_BIN="${TEST_REDIS_SERVER_BIN:-redis-server}"
BENCH_BIN="${BENCH_TOOL:-redis-benchmark}"

command -v "$SERVER_BIN" >/dev/null || { echo "no server binary: $SERVER_BIN"; exit 1; }
command -v "$BENCH_BIN" >/dev/null || { echo "no benchmark binary: $BENCH_BIN"; exit 1; }

EXT=so
[[ "$(uname)" == "Darwin" ]] && EXT=dylib
MODULE="$PWD/target/release/libredis_event_stream_module.$EXT"

echo "building module..."
cargo build --release >/dev/null

WORKDIR="$(mktemp -d)"
SERVER_PID=""
cleanup() { [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true; rm -rf "$WORKDIR"; }
trap cleanup EXIT

start_server() { # extra loadmodule args, or none for no module
  # Pass "$@" directly: an empty "$@" is safe under `set -u`, unlike an empty
  # named-array expansion on bash 3.2 (the macOS default).
  "$SERVER_BIN" --port "$PORT" --dir "$WORKDIR" --save '' --appendonly no \
    --enable-module-command yes --logfile "$WORKDIR/server.log" "$@" &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    redis-cli -p "$PORT" ping >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server failed to start"; cat "$WORKDIR/server.log"; exit 1
}

stop_server() {
  redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

# Run one redis-benchmark SET pass; echo "rps p50 p99".
# CSV output is one header line plus one data row:
#   "SET","rps","avg","min","p50","p95","p99","max"
bench_once() {
  local out
  out="$("$BENCH_BIN" -p "$PORT" -t set -n "$REQUESTS" -c "$CLIENTS" \
    --threads "$THREADS" -d "$PAYLOAD" -r "$KEYSPACE" --csv 2>/dev/null)"
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
    local row; row="$(bench_once)"
    echo "  rep $r: $row" >&2
    rows+=("$row")
  done
  stop_server
  # median by rps (column 1)
  printf '%s\n' "${rows[@]}" | sort -n -k1 | awk -v n="${#rows[@]}" 'NR==int((n+1)/2)'
}

echo "tool: $BENCH_BIN | server: $SERVER_BIN | reqs=$REQUESTS clients=$CLIENTS threads=$THREADS keyspace=$KEYSPACE payload=${PAYLOAD}B reps=$REPS"
echo

S0="$(scenario "S0 baseline (no module)")"
S1="$(scenario "S1 module loaded, default filter (no capture)" --loadmodule "$MODULE")"
S2="$(scenario "S2 module loaded, events=set (full capture)" --loadmodule "$MODULE" events set)"

read -r s0r s0p50 s0p99 <<<"$S0"
read -r s1r s1p50 s1p99 <<<"$S1"
read -r s2r s2p50 s2p99 <<<"$S2"

pct() { # value baseline -> signed percent vs baseline
  awk -v v="$1" -v b="$2" 'BEGIN{ if(b+0==0){print "NA"} else {printf "%+.1f%%", (v-b)/b*100} }'
}

echo
echo "## Results (median of $REPS)"
echo
echo "| Scenario | ops/sec | vs S0 | p50 (ms) | p99 (ms) |"
echo "|---|---|---|---|---|"
printf "| S0 baseline (no module) | %s | - | %s | %s |\n" "$s0r" "$s0p50" "$s0p99"
printf "| S1 loaded, no capture | %s | %s | %s | %s |\n" "$s1r" "$(pct "$s1r" "$s0r")" "$s1p50" "$s1p99"
printf "| S2 loaded, full capture | %s | %s | %s | %s |\n" "$s2r" "$(pct "$s2r" "$s0r")" "$s2p50" "$s2p99"
