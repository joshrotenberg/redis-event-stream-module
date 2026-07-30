#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 8 ]]; then
  echo "usage: remote-benchmark.sh <scenario> <server-ip> <loadgen-image> <requests> <clients> <threads> <payload> <keyspace>" >&2
  exit 2
fi

scenario="$1"
server_ip="$2"
loadgen_image="$3"
requests="$4"
clients="$5"
threads="$6"
payload="$7"
keyspace="$8"

redis_cli=(
  docker run --rm --network host "$loadgen_image"
  redis-cli -h "$server_ip" -p 6379 --raw
)

if ! "${redis_cli[@]}" PING | grep -q PONG; then
  echo "Redis at $server_ip did not answer PING" >&2
  exit 1
fi

benchmark="$(
  docker run --rm --network host "$loadgen_image" \
    redis-benchmark \
    -h "$server_ip" \
    -p 6379 \
    -t set \
    -n "$requests" \
    -c "$clients" \
    --threads "$threads" \
    -d "$payload" \
    -r "$keyspace" \
    --csv
)"

row="$(
  awk -F',' '
    NR == 2 {
      gsub(/"/, "")
      print
    }
  ' <<<"$benchmark"
)"

IFS=',' read -r test_name ops_per_sec avg_ms min_ms p50_ms p95_ms p99_ms max_ms <<<"$row"
if [[ -z "${ops_per_sec:-}" || -z "${p99_ms:-}" ]]; then
  echo "could not parse redis-benchmark CSV:" >&2
  echo "$benchmark" >&2
  exit 1
fi

info="$("${redis_cli[@]}" INFO eventstream | tr -d '\r')"
module_list="$("${redis_cli[@]}" MODULE LIST)"
stream_len="$("${redis_cli[@]}" XLEN events:set)"

info_field() {
  local key="$1"
  local value
  value="$(awk -F: -v name="eventstream_${key}" '$1 == name { print $2; exit }' <<<"$info")"
  printf '%s' "${value:-0}"
}

if grep -q eventstream <<<"$module_list"; then
  module_loaded=true
else
  module_loaded=false
fi

jq -n \
  --arg scenario "$scenario" \
  --arg test "$test_name" \
  --argjson requests "$requests" \
  --argjson clients "$clients" \
  --argjson threads "$threads" \
  --argjson payload_bytes "$payload" \
  --argjson keyspace "$keyspace" \
  --argjson ops_per_sec "$ops_per_sec" \
  --argjson avg_ms "$avg_ms" \
  --argjson min_ms "$min_ms" \
  --argjson p50_ms "$p50_ms" \
  --argjson p95_ms "$p95_ms" \
  --argjson p99_ms "$p99_ms" \
  --argjson max_ms "$max_ms" \
  --argjson module_loaded "$module_loaded" \
  --argjson forwarded "$(info_field forwarded)" \
  --argjson events_lost "$(info_field events_lost)" \
  --argjson dropped "$(info_field dropped)" \
  --argjson handler_panics "$(info_field handler_panics)" \
  --argjson skipped_filtered "$(info_field skipped_filtered)" \
  --argjson stream_len "${stream_len:-0}" \
  '{
    scenario: $scenario,
    workload: {
      test: $test,
      requests: $requests,
      clients: $clients,
      threads: $threads,
      payload_bytes: $payload_bytes,
      keyspace: $keyspace
    },
    result: {
      ops_per_sec: $ops_per_sec,
      avg_ms: $avg_ms,
      min_ms: $min_ms,
      p50_ms: $p50_ms,
      p95_ms: $p95_ms,
      p99_ms: $p99_ms,
      max_ms: $max_ms
    },
    module: {
      loaded: $module_loaded,
      forwarded: $forwarded,
      events_lost: $events_lost,
      dropped: $dropped,
      handler_panics: $handler_panics,
      skipped_filtered: $skipped_filtered,
      stream_len: $stream_len
    }
  }'
