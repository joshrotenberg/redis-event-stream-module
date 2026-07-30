#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 9 ]]; then
  echo "usage: remote-soak-workload.sh <server-ip> <loadgen-image> <client-image> <seconds> <base-clients> <base-rps> <payload> <keyspace> <result-dir>" >&2
  exit 2
fi

server_ip="$1"
loadgen_image="$2"
client_image="$3"
soak_seconds="$4"
base_clients="$5"
base_rps="$6"
payload="$7"
keyspace="$8"
result_dir="$9"
cli_container="eventstream-soak-cli"
consumer_container="eventstream-soak-consumer"
current_benchmark_file="$result_dir/current-benchmark"
stop_sampler_file="$result_dir/stop-sampler"
sampler_pid=""

cleanup() {
  touch "$stop_sampler_file" 2>/dev/null || true
  if [[ -n "$sampler_pid" ]]; then
    wait "$sampler_pid" 2>/dev/null || true
  fi
  docker unpause "$consumer_container" >/dev/null 2>&1 || true
  docker rm -f "$consumer_container" "$cli_container" >/dev/null 2>&1 || true
  if [[ -f "$current_benchmark_file" ]]; then
    docker rm -f "$(cat "$current_benchmark_file")" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$result_dir/phases"
rm -f "$stop_sampler_file" "$current_benchmark_file"
docker rm -f "$consumer_container" "$cli_container" >/dev/null 2>&1 || true
docker run \
  --detach \
  --name "$cli_container" \
  --network host \
  "$loadgen_image" \
  sleep infinity >/dev/null

redis_raw() {
  docker exec "$cli_container" \
    redis-cli -h "$server_ip" -p 6379 --raw "$@"
}

info_field() {
  local info="$1"
  local key="$2"
  local value
  value="$(awk -F: -v name="$key" '$1 == name { print $2; exit }' <<<"$info")"
  printf '%s' "${value:-0}"
}

module_snapshot() {
  local info
  info="$(redis_raw INFO eventstream | tr -d '\r')"
  jq -n \
    --argjson forwarded "$(info_field "$info" eventstream_forwarded)" \
    --argjson events_lost "$(info_field "$info" eventstream_events_lost)" \
    --argjson dropped "$(info_field "$info" eventstream_dropped)" \
    --argjson handler_panics \
      "$(info_field "$info" eventstream_handler_panics)" \
    --argjson async_enqueued \
      "$(info_field "$info" eventstream_async_enqueued)" \
    --argjson async_fallbacks \
      "$(info_field "$info" eventstream_async_fallbacks)" \
    --argjson async_queue_depth \
      "$(info_field "$info" eventstream_async_queue_depth)" \
    --argjson async_queue_high_water \
      "$(info_field "$info" eventstream_async_queue_high_water)" \
    --argjson async_drains \
      "$(info_field "$info" eventstream_async_drains)" \
    --argjson async_drain_events \
      "$(info_field "$info" eventstream_async_drain_events)" \
    --argjson async_envelopes \
      "$(info_field "$info" eventstream_async_envelopes)" \
    --argjson async_envelope_events \
      "$(info_field "$info" eventstream_async_envelope_events)" \
    --argjson async_worker_errors \
      "$(info_field "$info" eventstream_async_worker_errors)" \
    '{
      forwarded: $forwarded,
      events_lost: $events_lost,
      dropped: $dropped,
      handler_panics: $handler_panics,
      async_enqueued: $async_enqueued,
      async_fallbacks: $async_fallbacks,
      async_queue_depth: $async_queue_depth,
      async_queue_high_water: $async_queue_high_water,
      async_drains: $async_drains,
      async_drain_events: $async_drain_events,
      async_envelopes: $async_envelopes,
      async_envelope_events: $async_envelope_events,
      async_worker_errors: $async_worker_errors
    }'
}

requests_for_rate() {
  local rate="$1"
  local seconds="$2"
  awk -v rate="$rate" -v seconds="$seconds" \
    'BEGIN { value = int((rate * seconds) + 0.5); print value > 0 ? value : 1 }'
}

steady_one=$((soak_seconds * 27 / 100))
burst_one=$((soak_seconds * 2 / 100))
steady_two=$((soak_seconds * 23 / 100))
consumer_pause=$((soak_seconds * 4 / 100))
consumer_catchup=$((soak_seconds * 13 / 100))
burst_two=$((soak_seconds * 2 / 100))
steady_three=$((
  soak_seconds - steady_one - burst_one - steady_two -
    consumer_pause - consumer_catchup - burst_two
))

base_threads="$base_clients"
if ((base_threads > 4)); then
  base_threads=4
fi
burst_clients=100
burst_threads=4
burst_rps="$(awk -v rate="$base_rps" 'BEGIN { printf "%.3f", rate * 2 }')"

steady_one_requests="$(requests_for_rate "$base_rps" "$steady_one")"
burst_one_requests="$(requests_for_rate "$burst_rps" "$burst_one")"
steady_two_requests="$(requests_for_rate "$base_rps" "$steady_two")"
pause_requests="$(requests_for_rate "$base_rps" "$consumer_pause")"
catchup_requests="$(requests_for_rate "$base_rps" "$consumer_catchup")"
burst_two_requests="$(requests_for_rate "$burst_rps" "$burst_two")"
steady_three_requests="$(requests_for_rate "$base_rps" "$steady_three")"

jq -n \
  --argjson base_clients "$base_clients" \
  --argjson base_threads "$base_threads" \
  --argjson burst_clients "$burst_clients" \
  --argjson burst_threads "$burst_threads" \
  --argjson steady_one "$steady_one" \
  --argjson burst_one "$burst_one" \
  --argjson steady_two "$steady_two" \
  --argjson consumer_pause "$consumer_pause" \
  --argjson consumer_catchup "$consumer_catchup" \
  --argjson burst_two "$burst_two" \
  --argjson steady_three "$steady_three" \
  --argjson steady_one_requests "$steady_one_requests" \
  --argjson burst_one_requests "$burst_one_requests" \
  --argjson steady_two_requests "$steady_two_requests" \
  --argjson pause_requests "$pause_requests" \
  --argjson catchup_requests "$catchup_requests" \
  --argjson burst_two_requests "$burst_two_requests" \
  --argjson steady_three_requests "$steady_three_requests" \
  '[
    {
      name: "steady-1",
      kind: "steady",
      target_seconds: $steady_one,
      clients: $base_clients,
      threads: $base_threads,
      requests: $steady_one_requests,
      pause_consumer: false
    },
    {
      name: "burst-1",
      kind: "burst",
      target_seconds: $burst_one,
      clients: $burst_clients,
      threads: $burst_threads,
      requests: $burst_one_requests,
      pause_consumer: false
    },
    {
      name: "steady-2",
      kind: "steady",
      target_seconds: $steady_two,
      clients: $base_clients,
      threads: $base_threads,
      requests: $steady_two_requests,
      pause_consumer: false
    },
    {
      name: "consumer-pause",
      kind: "consumer-pause",
      target_seconds: $consumer_pause,
      clients: $base_clients,
      threads: $base_threads,
      requests: $pause_requests,
      pause_consumer: true
    },
    {
      name: "consumer-catchup",
      kind: "consumer-catchup",
      target_seconds: $consumer_catchup,
      clients: $base_clients,
      threads: $base_threads,
      requests: $catchup_requests,
      pause_consumer: false
    },
    {
      name: "burst-2",
      kind: "burst",
      target_seconds: $burst_two,
      clients: $burst_clients,
      threads: $burst_threads,
      requests: $burst_two_requests,
      pause_consumer: false
    },
    {
      name: "steady-3",
      kind: "steady",
      target_seconds: $steady_three,
      clients: $base_clients,
      threads: $base_threads,
      requests: $steady_three_requests,
      pause_consumer: false
    }
  ]' >"$result_dir/plan.json"

total_requests="$(jq '[.[].requests] | add' "$result_dir/plan.json")"

redis_raw SET __eventstream_soak_seed v >/dev/null
for _ in $(seq 1 1200); do
  seed_snapshot="$(module_snapshot)"
  if [[ "$(jq -r '.async_queue_depth' <<<"$seed_snapshot")" == "0" ]]; then
    break
  fi
  sleep 0.05
done
module_snapshot >"$result_dir/module-start.json"
start_forwarded="$(jq -r '.forwarded' "$result_dir/module-start.json")"

docker run \
  --detach \
  --name "$consumer_container" \
  --network host \
  --volume "$result_dir:/results" \
  "$client_image" \
  /eventstream-client \
  --url "redis://$server_ip:6379" \
  consume \
  --events set \
  --from '$' \
  --count "$total_requests" \
  --quiet \
  --checkpoint /results/consumer.json \
  --read-count 2000 >/dev/null

for _ in $(seq 1 100); do
  if [[ -f "$result_dir/consumer.json" ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! -f "$result_dir/consumer.json" ]]; then
  docker logs "$consumer_container" >&2 || true
  echo "consumer did not create its checkpoint" >&2
  exit 1
fi

sample_loop() {
  while [[ ! -f "$stop_sampler_file" ]]; do
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    epoch_ms="$(date +%s%3N)"
    module_info="$(redis_raw INFO eventstream | tr -d '\r')"
    cpu_info="$(redis_raw INFO cpu | tr -d '\r')"
    memory_info="$(redis_raw INFO memory | tr -d '\r')"
    forwarded="$(info_field "$module_info" eventstream_forwarded)"
    consumer_seen="$(
      jq -r '.logical_events // 0' "$result_dir/consumer.json" 2>/dev/null ||
        printf '0'
    )"
    stream_len="$(redis_raw XLEN events:set 2>/dev/null || printf '0')"
    stream_memory="$(
      redis_raw MEMORY USAGE events:set 2>/dev/null |
        awk 'NF { print; found=1 } END { if (!found) print 0 }'
    )"
    benchmark_cpu=0
    benchmark_memory=0
    if [[ -f "$current_benchmark_file" ]]; then
      benchmark_container="$(cat "$current_benchmark_file")"
      stats="$(
        docker stats \
          --no-stream \
          --format '{{.CPUPerc}}|{{.MemUsage}}' \
          "$benchmark_container" 2>/dev/null ||
          true
      )"
      benchmark_cpu="$(
        awk -F'|' '{
          value = $1
          gsub(/%/, "", value)
          print value + 0
        }' <<<"$stats"
      )"
      benchmark_memory="$(
        awk -F'|' '{
          split($2, fields, " ")
          value = fields[1]
          unit = value
          gsub(/[[:alpha:]]/, "", value)
          if (unit ~ /GiB$/) value *= 1024
          else if (unit ~ /KiB$/) value /= 1024
          print value + 0
        }' <<<"$stats"
      )"
    fi
    jq -nc \
      --arg timestamp "$timestamp" \
      --argjson epoch_ms "$epoch_ms" \
      --argjson forwarded "$forwarded" \
      --argjson start_forwarded "$start_forwarded" \
      --argjson consumer_seen "$consumer_seen" \
      --argjson queue_depth \
        "$(info_field "$module_info" eventstream_async_queue_depth)" \
      --argjson queue_high_water \
        "$(info_field "$module_info" eventstream_async_queue_high_water)" \
      --argjson stream_len "$stream_len" \
      --argjson stream_memory_bytes "$stream_memory" \
      --argjson used_memory_bytes \
        "$(info_field "$memory_info" used_memory)" \
      --argjson used_memory_rss_bytes \
        "$(info_field "$memory_info" used_memory_rss)" \
      --argjson used_cpu_main_seconds "$(
        awk \
          -v sys="$(info_field "$cpu_info" used_cpu_sys_main_thread)" \
          -v user="$(info_field "$cpu_info" used_cpu_user_main_thread)" \
          'BEGIN { printf "%.6f", sys + user }'
      )" \
      --argjson used_cpu_total_seconds "$(
        awk \
          -v sys="$(info_field "$cpu_info" used_cpu_sys)" \
          -v user="$(info_field "$cpu_info" used_cpu_user)" \
          'BEGIN { printf "%.6f", sys + user }'
      )" \
      --argjson benchmark_cpu_percent "${benchmark_cpu:-0}" \
      --argjson benchmark_memory_mib "${benchmark_memory:-0}" \
      '{
        timestamp: $timestamp,
        epoch_ms: $epoch_ms,
        forwarded: ($forwarded - $start_forwarded),
        consumer_seen: $consumer_seen,
        consumer_lag: (($forwarded - $start_forwarded) - $consumer_seen),
        queue_depth: $queue_depth,
        queue_high_water: $queue_high_water,
        stream_len: $stream_len,
        stream_memory_bytes: $stream_memory_bytes,
        used_memory_bytes: $used_memory_bytes,
        used_memory_rss_bytes: $used_memory_rss_bytes,
        used_cpu_main_seconds: $used_cpu_main_seconds,
        used_cpu_total_seconds: $used_cpu_total_seconds,
        benchmark_cpu_percent: $benchmark_cpu_percent,
        benchmark_memory_mib: $benchmark_memory_mib
      }' >>"$result_dir/telemetry.jsonl"
    sleep 5
  done
}
sample_loop &
sampler_pid="$!"

while IFS= read -r phase; do
  name="$(jq -r '.name' <<<"$phase")"
  kind="$(jq -r '.kind' <<<"$phase")"
  target_seconds="$(jq -r '.target_seconds' <<<"$phase")"
  clients="$(jq -r '.clients' <<<"$phase")"
  threads="$(jq -r '.threads' <<<"$phase")"
  requests="$(jq -r '.requests' <<<"$phase")"
  pause_consumer="$(jq -r '.pause_consumer' <<<"$phase")"
  benchmark_container="eventstream-soak-$name"
  output="$result_dir/phases/$name.csv"
  consumer_before="$(
    jq -r '.logical_events // 0' "$result_dir/consumer.json"
  )"

  if [[ "$pause_consumer" == "true" ]]; then
    docker pause "$consumer_container" >/dev/null
  fi

  printf '%s' "$benchmark_container" >"$current_benchmark_file"
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  started_ns="$(date +%s%N)"
  docker run \
    --rm \
    --name "$benchmark_container" \
    --network host \
    "$loadgen_image" \
    redis-benchmark \
    -h "$server_ip" \
    -p 6379 \
    -t set \
    -n "$requests" \
    -c "$clients" \
    --threads "$threads" \
    -d "$payload" \
    -r "$keyspace" \
    --csv >"$output"
  completed_ns="$(date +%s%N)"
  completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  rm -f "$current_benchmark_file"

  if [[ "$pause_consumer" == "true" ]]; then
    docker unpause "$consumer_container" >/dev/null
  fi

  consumer_after="$(
    jq -r '.logical_events // 0' "$result_dir/consumer.json"
  )"
  row="$(
    awk -F',' '
      NR == 2 {
        gsub(/"/, "")
        print
      }
    ' "$output"
  )"
  IFS=',' read -r _ ops_per_sec avg_ms min_ms p50_ms p95_ms p99_ms max_ms <<<"$row"
  duration_seconds="$(
    awk -v started="$started_ns" -v completed="$completed_ns" \
      'BEGIN { printf "%.6f", (completed - started) / 1000000000 }'
  )"
  jq -n \
    --arg name "$name" \
    --arg kind "$kind" \
    --arg started_at "$started_at" \
    --arg completed_at "$completed_at" \
    --argjson target_seconds "$target_seconds" \
    --argjson duration_seconds "$duration_seconds" \
    --argjson clients "$clients" \
    --argjson threads "$threads" \
    --argjson requests "$requests" \
    --argjson pause_consumer "$pause_consumer" \
    --argjson consumer_before "$consumer_before" \
    --argjson consumer_after "$consumer_after" \
    --argjson ops_per_sec "$ops_per_sec" \
    --argjson avg_ms "$avg_ms" \
    --argjson min_ms "$min_ms" \
    --argjson p50_ms "$p50_ms" \
    --argjson p95_ms "$p95_ms" \
    --argjson p99_ms "$p99_ms" \
    --argjson max_ms "$max_ms" \
    '{
      name: $name,
      kind: $kind,
      started_at: $started_at,
      completed_at: $completed_at,
      target_seconds: $target_seconds,
      duration_seconds: $duration_seconds,
      clients: $clients,
      threads: $threads,
      requests: $requests,
      pause_consumer: $pause_consumer,
      consumer_before: $consumer_before,
      consumer_after: $consumer_after,
      ops_per_sec: $ops_per_sec,
      avg_ms: $avg_ms,
      min_ms: $min_ms,
      p50_ms: $p50_ms,
      p95_ms: $p95_ms,
      p99_ms: $p99_ms,
      max_ms: $max_ms
    }' >"$result_dir/phases/$name.json"
done < <(jq -c '.[]' "$result_dir/plan.json")

expected_forwarded=$((start_forwarded + total_requests))
settled=false
for _ in $(seq 1 3000); do
  snapshot="$(module_snapshot)"
  forwarded="$(jq -r '.forwarded' <<<"$snapshot")"
  queue_depth="$(jq -r '.async_queue_depth' <<<"$snapshot")"
  if [[ "$forwarded" == "$expected_forwarded" && "$queue_depth" == "0" ]]; then
    settled=true
    break
  fi
  sleep 0.1
done

consumer_completed=false
for _ in $(seq 1 3000); do
  if ! docker inspect \
    --format '{{.State.Running}}' \
    "$consumer_container" 2>/dev/null |
    grep -q true; then
    consumer_completed=true
    break
  fi
  sleep 0.1
done

# Give the sampler one interval to record the settled queue and caught-up
# consumer rather than ending on a transient pre-settle sample.
sleep 5
touch "$stop_sampler_file"
wait "$sampler_pid" || true
sampler_pid=""
docker logs "$consumer_container" >"$result_dir/consumer.stdout" 2>"$result_dir/consumer.stderr" ||
  true

module_snapshot >"$result_dir/module-end.json"
jq -s '.' "$result_dir"/phases/*.json >"$result_dir/phases.json"
if [[ -s "$result_dir/telemetry.jsonl" ]]; then
  jq -s '.' "$result_dir/telemetry.jsonl" >"$result_dir/telemetry.json"
else
  printf '[]\n' >"$result_dir/telemetry.json"
fi

stream_len="$(redis_raw XLEN events:set)"
stream_memory="$(
  redis_raw MEMORY USAGE events:set |
    awk 'NF { print; found=1 } END { if (!found) print 0 }'
)"

jq -n \
  --arg schema_version "1" \
  --argjson requested_seconds "$soak_seconds" \
  --argjson target_rps "$base_rps" \
  --argjson base_clients "$base_clients" \
  --argjson total_requests "$total_requests" \
  --argjson settled "$settled" \
  --argjson consumer_completed "$consumer_completed" \
  --argjson stream_len "$stream_len" \
  --argjson stream_memory_bytes "$stream_memory" \
  --slurpfile plan "$result_dir/plan.json" \
  --slurpfile phases "$result_dir/phases.json" \
  --slurpfile telemetry "$result_dir/telemetry.json" \
  --slurpfile module_start "$result_dir/module-start.json" \
  --slurpfile module_end "$result_dir/module-end.json" \
  --slurpfile consumer "$result_dir/consumer.json" \
  '{
    schema_version: ($schema_version | tonumber),
    requested_seconds: $requested_seconds,
    selected_load: {
      target_rps: $target_rps,
      clients: $base_clients
    },
    planned_logical_events: $total_requests,
    plan: $plan[0],
    phases: $phases[0],
    telemetry: $telemetry[0],
    module_start: $module_start[0],
    module_end: $module_end[0],
    consumer: $consumer[0],
    retained_stream: {
      physical_entries: $stream_len,
      memory_bytes: $stream_memory_bytes
    },
    correctness: {
      capture_settled: $settled,
      consumer_completed: $consumer_completed,
      forwarded:
        ($module_end[0].forwarded - $module_start[0].forwarded),
      consumed_logical_events: $consumer[0].logical_events,
      source_to_forwarded_exact:
        (($module_end[0].forwarded - $module_start[0].forwarded) ==
         $total_requests),
      source_to_consumer_exact:
        ($consumer[0].logical_events == $total_requests),
      events_lost:
        ($module_end[0].events_lost - $module_start[0].events_lost),
      dropped:
        ($module_end[0].dropped - $module_start[0].dropped),
      handler_panics:
        ($module_end[0].handler_panics - $module_start[0].handler_panics),
      async_worker_errors:
        ($module_end[0].async_worker_errors -
         $module_start[0].async_worker_errors)
    }
  }' >"$result_dir/result.json"

jq '{
  planned_logical_events,
  selected_load,
  correctness,
  retained_stream,
  samples: (.telemetry | length)
}' "$result_dir/result.json"
