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
benchmark_container="eventstream-benchmark-${scenario}-$$"
benchmark_file="$(mktemp)"
stats_file="$(mktemp)"

cleanup() {
  rm -f "$benchmark_file" "$stats_file"
  docker rm -f "$benchmark_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

redis_cli=(
  docker run --rm --network host "$loadgen_image"
  redis-cli -h "$server_ip" -p 6379 --raw
)

if ! "${redis_cli[@]}" PING | grep -q PONG; then
  echo "Redis at $server_ip did not answer PING" >&2
  exit 1
fi

cpu_before="$("${redis_cli[@]}" INFO cpu | tr -d '\r')"
started_ns="$(date +%s%N)"
docker run --rm --name "$benchmark_container" --network host "$loadgen_image" \
    redis-benchmark \
    -h "$server_ip" \
    -p 6379 \
    -t set \
    -n "$requests" \
    -c "$clients" \
    --threads "$threads" \
    -d "$payload" \
    -r "$keyspace" \
    --csv >"$benchmark_file" &
benchmark_pid="$!"

while kill -0 "$benchmark_pid" >/dev/null 2>&1; do
  if ! docker stats \
    --no-stream \
    --format '{{.CPUPerc}}|{{.MemUsage}}' \
    "$benchmark_container" >>"$stats_file" 2>/dev/null; then
    sleep 0.25
  fi
done

if ! wait "$benchmark_pid"; then
  echo "redis-benchmark container failed" >&2
  exit 1
fi
completed_ns="$(date +%s%N)"

benchmark="$(cat "$benchmark_file")"
duration_seconds="$(
  awk -v started="$started_ns" -v completed="$completed_ns" \
    'BEGIN { printf "%.6f", (completed - started) / 1000000000 }'
)"

read -r stats_samples cpu_percent_avg cpu_percent_max memory_peak_mib < <(
  awk -F'|' '
    function memory_mib(value, number) {
      split(value, fields, " ")
      number = fields[1]
      gsub(/[[:alpha:]]/, "", number)
      if (fields[1] ~ /GiB$/) {
        return number * 1024
      }
      if (fields[1] ~ /KiB$/) {
        return number / 1024
      }
      if (fields[1] ~ /[kK]B$/) {
        return number / 1000
      }
      if (fields[1] ~ /GB$/) {
        return number * 1000
      }
      return number
    }
    {
      cpu = $1
      gsub(/%/, "", cpu)
      memory = memory_mib($2)
      samples += 1
      cpu_sum += cpu
      if (cpu > cpu_max) cpu_max = cpu
      if (memory > memory_max) memory_max = memory
    }
    END {
      if (samples == 0) {
        print "0 0 0 0"
      } else {
        printf "%d %.3f %.3f %.3f\n",
          samples, cpu_sum / samples, cpu_max, memory_max
      }
    }
  ' "$stats_file"
)

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

benchmark_duration_seconds="$(
  awk \
    -v requests="$requests" \
    -v ops_per_sec="$ops_per_sec" \
    'BEGIN { printf "%.6f", requests / ops_per_sec }'
)"

info="$("${redis_cli[@]}" INFO eventstream | tr -d '\r')"
cpu_after="$("${redis_cli[@]}" INFO cpu | tr -d '\r')"
memory_after="$("${redis_cli[@]}" INFO memory | tr -d '\r')"
module_list="$("${redis_cli[@]}" MODULE LIST)"
stream_len="$("${redis_cli[@]}" XLEN events:set)"

info_field() {
  local key="$1"
  local value
  value="$(awk -F: -v name="eventstream_${key}" '$1 == name { print $2; exit }' <<<"$info")"
  printf '%s' "${value:-0}"
}

redis_info_field() {
  local source="$1"
  local key="$2"
  local value
  value="$(awk -F: -v name="$key" '$1 == name { print $2; exit }' <<<"$source")"
  printf '%s' "${value:-0}"
}

before_main_thread_seconds="$(
  awk \
    -v sys="$(redis_info_field "$cpu_before" used_cpu_sys_main_thread)" \
    -v user="$(redis_info_field "$cpu_before" used_cpu_user_main_thread)" \
    'BEGIN { printf "%.6f", sys + user }'
)"
after_main_thread_seconds="$(
  awk \
    -v sys="$(redis_info_field "$cpu_after" used_cpu_sys_main_thread)" \
    -v user="$(redis_info_field "$cpu_after" used_cpu_user_main_thread)" \
    'BEGIN { printf "%.6f", sys + user }'
)"
main_thread_cpu_seconds="$(
  awk \
    -v before="$before_main_thread_seconds" \
    -v after="$after_main_thread_seconds" \
    'BEGIN { printf "%.6f", after - before }'
)"
main_thread_core_percent="$(
  awk \
    -v cpu="$main_thread_cpu_seconds" \
    -v duration="$benchmark_duration_seconds" \
    'BEGIN {
      if (duration > 0) {
        printf "%.3f", (cpu / duration) * 100
      } else {
        print "0"
      }
    }'
)"

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
  --argjson duration_seconds "$duration_seconds" \
  --argjson benchmark_duration_seconds "$benchmark_duration_seconds" \
  --argjson host_logical_cpus "$(getconf _NPROCESSORS_ONLN)" \
  --argjson stats_samples "$stats_samples" \
  --argjson cpu_percent_avg "$cpu_percent_avg" \
  --argjson cpu_percent_max "$cpu_percent_max" \
  --argjson memory_peak_mib "$memory_peak_mib" \
  --argjson main_thread_cpu_seconds "$main_thread_cpu_seconds" \
  --argjson main_thread_core_percent "$main_thread_core_percent" \
  --argjson used_memory_bytes \
    "$(redis_info_field "$memory_after" used_memory)" \
  --argjson used_memory_rss_bytes \
    "$(redis_info_field "$memory_after" used_memory_rss)" \
  --argjson used_memory_peak_bytes \
    "$(redis_info_field "$memory_after" used_memory_peak)" \
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
      keyspace: $keyspace,
      duration_seconds: $duration_seconds,
      benchmark_duration_seconds: $benchmark_duration_seconds
    },
    load_generator: {
      host_logical_cpus: $host_logical_cpus,
      stats_samples: $stats_samples,
      cpu_percent_avg: $cpu_percent_avg,
      cpu_percent_max: $cpu_percent_max,
      memory_peak_mib: $memory_peak_mib
    },
    server: {
      main_thread_cpu_seconds: $main_thread_cpu_seconds,
      main_thread_core_percent: $main_thread_core_percent,
      used_memory_bytes: $used_memory_bytes,
      used_memory_rss_bytes: $used_memory_rss_bytes,
      used_memory_peak_bytes: $used_memory_peak_bytes
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
