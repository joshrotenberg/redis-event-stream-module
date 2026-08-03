#!/usr/bin/env bash
# Focused control-checkpoint overhead probe for issue #277.
#
# For each cadence, this records two independent costs:
#   1. foreground SET throughput/latency while every SET is captured;
#   2. idle AOF and stream-memory growth caused only by durable checkpoints.
#
# Usage:
#   bench/checkpoints.sh
#   CHECKPOINT_CADENCES="0 5000 1000 100" CHECKPOINT_JSON=result.json \
#     bench/checkpoints.sh
set -euo pipefail
cd "$(dirname "$0")/.."

port="${CHECKPOINT_PORT:-6393}"
reps="${CHECKPOINT_REPS:-3}"
requests="${CHECKPOINT_REQUESTS:-500000}"
clients="${CHECKPOINT_CLIENTS:-50}"
threads="${CHECKPOINT_THREADS:-4}"
keyspace="${CHECKPOINT_KEYSPACE:-100000}"
payload="${CHECKPOINT_PAYLOAD:-64}"
idle_seconds="${CHECKPOINT_IDLE_SECONDS:-10}"
cadences="${CHECKPOINT_CADENCES:-0 1000 100}"
json_out="${CHECKPOINT_JSON:-}"

server_bin="${TEST_REDIS_SERVER_BIN:-redis-server}"
cli_bin="${TEST_REDIS_CLI_BIN:-redis-cli}"
bench_bin="${BENCH_TOOL:-redis-benchmark}"

for tool in "$server_bin" "$cli_bin" "$bench_bin" cargo jq; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

ext=so
[[ "$(uname)" == "Darwin" ]] && ext=dylib
module="$PWD/target/release/libredis_event_stream_module.$ext"

echo "building module..." >&2
cargo build --release >/dev/null

workdir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

start_server() {
  local cadence="$1"
  local persistence="$2"
  local run_dir
  run_dir="$workdir/server-${cadence}-${persistence}-$(date +%s%N)"
  local persistence_args=(--appendonly no)
  mkdir -p "$run_dir"
  if [[ "$persistence" == yes ]]; then
    persistence_args=(--appendonly yes --appendfsync everysec)
  fi
  "$server_bin" \
    --port "$port" \
    --dir "$run_dir" \
    --save '' \
    --enable-module-command yes \
    --logfile "$run_dir/server.log" \
    "${persistence_args[@]}" \
    --loadmodule "$module" \
      events set \
      control-checkpoint-ms "$cadence" &
  server_pid=$!
  for _ in $(seq 1 100); do
    if "$cli_bin" -p "$port" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "server failed to start; log: $run_dir/server.log" >&2
  return 1
}

stop_server() {
  "$cli_bin" -p "$port" shutdown nosave >/dev/null 2>&1 || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
}

module_field() {
  local field="$1"
  "$cli_bin" -p "$port" info eventstream 2>/dev/null |
    awk -F: -v key="eventstream_${field}" '$1 == key { gsub(/\r/, "", $2); print $2; exit }'
}

redis_field() {
  local section="$1"
  local field="$2"
  "$cli_bin" -p "$port" info "$section" 2>/dev/null |
    awk -F: -v key="$field" '$1 == key { gsub(/\r/, "", $2); print $2; exit }'
}

benchmark_once() {
  local output
  output="$(
    "$bench_bin" \
      -p "$port" \
      -t set \
      -n "$requests" \
      -c "$clients" \
      --threads "$threads" \
      -d "$payload" \
      -r "$keyspace" \
      --csv 2>/dev/null
  )"
  awk -F, 'NR == 2 { gsub(/"/, ""); print $2, $5, $7 }' <<<"$output"
}

foreground() {
  local cadence="$1"
  local rows="$workdir/foreground-$cadence"
  : >"$rows"
  for rep in $(seq 1 "$reps"); do
    start_server "$cadence" no
    local result
    result="$(benchmark_once)"
    local checkpoints
    checkpoints="$(module_field control_checkpoints)"
    printf '%s %s\n' "$result" "${checkpoints:-0}" >>"$rows"
    echo "  cadence=${cadence}ms rep=$rep foreground: $result checkpoints=${checkpoints:-0}" >&2
    stop_server
  done
  sort -n -k1,1 "$rows" | awk -v count="$reps" 'NR == int((count + 1) / 2)'
}

idle_persistence() {
  local cadence="$1"
  start_server "$cadence" yes
  sleep "$idle_seconds"
  # appendfsync=everysec: allow the last generated entry to pass one full
  # fsync interval before reading persistence accounting.
  sleep 1
  local checkpoints control_len control_memory aof_size
  checkpoints="$(module_field control_checkpoints)"
  control_len="$("$cli_bin" -p "$port" xlen events:#control 2>/dev/null || true)"
  control_memory="$("$cli_bin" -p "$port" memory usage events:#control 2>/dev/null || true)"
  aof_size="$(redis_field persistence aof_current_size)"
  stop_server
  printf '%s %s %s %s\n' \
    "${checkpoints:-0}" "${control_len:-0}" "${control_memory:-0}" "${aof_size:-0}"
}

rows_json="$workdir/results.jsonl"
table="$workdir/table.md"
: >"$rows_json"
: >"$table"
baseline_rps=""
baseline_aof=""

for cadence in $cadences; do
  read -r ops p50 p99 foreground_checkpoints <<<"$(foreground "$cadence")"
  read -r idle_checkpoints control_len control_memory aof_size \
    <<<"$(idle_persistence "$cadence")"
  if [[ -z "$baseline_rps" ]]; then
    baseline_rps="$ops"
    baseline_aof="$aof_size"
  fi
  rps_delta="$(
    awk -v value="$ops" -v baseline="$baseline_rps" \
      'BEGIN { if (baseline == 0) print "NA"; else printf "%+.2f", (value - baseline) / baseline * 100 }'
  )"
  aof_delta="$((aof_size - baseline_aof))"
  printf '| %s | %s | %s%% | %s | %s | %s | %s | %s | %s |\n' \
    "$cadence" "$ops" "$rps_delta" "$p50" "$p99" "$idle_checkpoints" \
    "$control_len" "$control_memory" "$aof_delta" >>"$table"
  jq -cn \
    --argjson cadence_ms "$cadence" \
    --argjson foreground_ops_per_sec "$ops" \
    --argjson foreground_p50_ms "$p50" \
    --argjson foreground_p99_ms "$p99" \
    --argjson foreground_checkpoints "$foreground_checkpoints" \
    --argjson idle_seconds "$idle_seconds" \
    --argjson idle_checkpoints "$idle_checkpoints" \
    --argjson control_stream_entries "$control_len" \
    --argjson control_stream_memory_bytes "$control_memory" \
    --argjson aof_current_size_bytes "$aof_size" \
    --argjson aof_bytes_over_disabled "$aof_delta" \
    '{cadence_ms: $cadence_ms,
      foreground: {
        ops_per_sec: $foreground_ops_per_sec,
        p50_ms: $foreground_p50_ms,
        p99_ms: $foreground_p99_ms,
        checkpoints_written: $foreground_checkpoints
      },
      idle_persistence: {
        seconds: $idle_seconds,
        checkpoints_written: $idle_checkpoints,
        control_stream_entries: $control_stream_entries,
        control_stream_memory_bytes: $control_stream_memory_bytes,
        aof_current_size_bytes: $aof_current_size_bytes,
        aof_bytes_over_disabled: $aof_bytes_over_disabled
      }}' >>"$rows_json"
done

echo
echo "## Control checkpoint overhead (median of $reps foreground runs)"
echo
echo "Foreground: $requests SETs, $clients clients, $threads threads, ${payload}B payload; every SET captured."
echo "Persistence: ${idle_seconds}s idle, AOF everysec; delta is versus disabled cadence on the same host."
echo
echo '| cadence (ms) | ops/sec | vs disabled | p50 (ms) | p99 (ms) | idle checkpoints | control entries | control memory (B) | AOF delta (B) |'
echo '|---:|---:|---:|---:|---:|---:|---:|---:|---:|'
cat "$table"

if [[ -n "$json_out" ]]; then
  jq -s . "$rows_json" >"$json_out"
  echo
  echo "json: $json_out"
fi
