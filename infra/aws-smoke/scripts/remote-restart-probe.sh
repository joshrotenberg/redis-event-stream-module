#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 10 ]]; then
  echo "usage: remote-restart-probe.sh <graceful|abrupt> <module-image> <module-so|-> <queue-capacity> <batch-size> <max-wait-ms> <depth-threshold> <events-per-batch> <max-batches> <result-dir>" >&2
  exit 2
fi

mode="$1"
module_image="$2"
module_override="$3"
queue_capacity="$4"
batch_size="$5"
max_wait_ms="$6"
depth_threshold="$7"
events_per_batch="$8"
max_batches="$9"
result_dir="${10}"
container="eventstream-server"
module_path="/usr/local/lib/redis/modules/libredis_event_stream_module.so"
data_dir="/var/lib/eventstream-smoke/restart-probe-aof"

case "$mode" in
  graceful | abrupt) ;;
  *)
    echo "mode must be graceful or abrupt" >&2
    exit 2
    ;;
esac

mkdir -p "$result_dir"

info_field() {
  local info="$1"
  local key="$2"
  local value
  value="$(awk -F: -v name="$key" '$1 == name { print $2; exit }' <<<"$info")"
  printf '%s' "${value:-0}"
}

redis_raw() {
  docker exec "$container" redis-cli --raw "$@"
}

wait_for_server() {
  for _ in $(seq 1 600); do
    if redis_raw PING 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.1
  done
  docker logs "$container" >&2 || true
  echo "probe server did not become ready" >&2
  return 1
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

start_server() {
  local persistent="$1"
  local -a docker_args server_args

  docker rm -f "$container" >/dev/null 2>&1 || true
  docker_args=(
    --detach
    --name "$container"
    --network host
  )
  server_args=(
    redis-server
    --bind 0.0.0.0
    --protected-mode no
    --save ""
  )
  if [[ "$persistent" == "yes" ]]; then
    docker_args+=(--volume "$data_dir:/data")
    server_args+=(
      --dir /data
      --appendonly yes
      --appendfsync everysec
    )
  else
    server_args+=(--appendonly no)
  fi
  if [[ "$module_override" != "-" ]]; then
    test -f "$module_override"
    docker_args+=(--volume "$module_override:$module_path:ro")
  fi
  server_args+=(
    --loadmodule "$module_path"
    events set
    maxlen 0
    write-mode envelope
    async-queue-capacity "$queue_capacity"
    async-batch-size "$batch_size"
    async-max-wait-ms "$max_wait_ms"
  )

  docker run \
    "${docker_args[@]}" \
    "$module_image" \
    "${server_args[@]}" >/dev/null
  wait_for_server
}

produce_batch() {
  local prefix="$1"
  local offset="$2"
  local count="$3"
  awk -v prefix="$prefix" -v offset="$offset" -v count="$count" '
    BEGIN {
      for (i = 1; i <= count; i++) {
        key = prefix (offset + i)
        printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n",
          length(key), key
      }
    }
  ' |
    docker exec -i "$container" redis-cli --pipe
}

source_count() {
  local prefix="$1"
  redis_raw --scan --pattern "${prefix}*" |
    awk 'NF { count += 1 } END { print count + 0 }'
}

if [[ "$mode" == "abrupt" ]]; then
  rm -rf "$data_dir"
  mkdir -p "$data_dir"
  start_server yes
else
  start_server no
fi

prefix="probe:${mode}:"
attempted=$((events_per_batch * max_batches))
output_file="$result_dir/producer.stdout"
error_file="$result_dir/producer.stderr"
threshold_reached=false
module_snapshot >"$result_dir/module-before.json"
best_depth=0

set +e
produce_batch "$prefix" 0 "$attempted" \
  >"$output_file" 2>"$error_file" &
producer_pid="$!"
set -e

while kill -0 "$producer_pid" 2>/dev/null; do
  snapshot="$(module_snapshot)"
  depth="$(jq -r '.async_queue_depth' <<<"$snapshot")"
  if ((depth > best_depth)); then
    best_depth="$depth"
    printf '%s\n' "$snapshot" >"$result_dir/module-before.json"
  fi
  if ((depth >= depth_threshold)); then
    threshold_reached=true
    break
  fi
  sleep 0.001
done

if [[ "$threshold_reached" == "true" ]]; then
  if [[ "$mode" == "graceful" ]]; then
    docker exec "$container" pkill -TERM redis-cli >/dev/null 2>&1 || true
  else
    docker kill --signal KILL "$container" >/dev/null
  fi
fi

set +e
wait "$producer_pid"
pipe_status="$?"
set -e

replies="$(
  sed -n 's/.*replies: \([0-9][0-9]*\).*/\1/p' "$output_file" |
    tail -n 1
)"
errors="$(
  sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p' "$output_file" |
    tail -n 1
)"
replies="${replies:-0}"
errors="${errors:-0}"

if [[ "$threshold_reached" != "true" ]]; then
  module_snapshot >"$result_dir/module-before.json"
fi
depth_before="$(jq -r '.async_queue_depth' "$result_dir/module-before.json")"
high_water_before="$(jq -r '.async_queue_high_water' "$result_dir/module-before.json")"
forwarded_before="$(jq -r '.forwarded' "$result_dir/module-before.json")"

if [[ "$mode" == "graceful" ]]; then
  unload_started_ns="$(date +%s%N)"
  redis_raw MODULE UNLOAD eventstream >"$result_dir/unload.stdout"
  unload_completed_ns="$(date +%s%N)"
  unload_seconds="$(
    awk -v started="$unload_started_ns" -v completed="$unload_completed_ns" \
      'BEGIN { printf "%.6f", (completed - started) / 1000000000 }'
  )"
  durable_source_keys="$(source_count "$prefix")"
  physical_entries="$(redis_raw XLEN events:set)"
  redis_raw \
    MODULE LOAD "$module_path" \
    events set \
    maxlen 0 \
    write-mode envelope \
    async-queue-capacity "$queue_capacity" \
    async-batch-size "$batch_size" \
    async-max-wait-ms "$max_wait_ms" >"$result_dir/reload.stdout"
  process_restarted=false
  persistence="none"
else
  if [[ "$threshold_reached" != "true" ]]; then
    docker kill --signal KILL "$container" >/dev/null
  fi
  docker rm "$container" >/dev/null
  start_server yes
  durable_source_keys="$(source_count "$prefix")"
  physical_entries="$(redis_raw XLEN events:set)"
  unload_seconds="0"
  process_restarted=true
  persistence="aof-everysec"
fi

acknowledged="$forwarded_before"
if ((durable_source_keys > acknowledged)); then
  acknowledged="$durable_source_keys"
fi
if ((replies > acknowledged)); then
  acknowledged="$replies"
fi

jq -n \
  --argjson attempted "$attempted" \
  --argjson replies "$replies" \
  --argjson errors "$errors" \
  --argjson pipe_status "$pipe_status" \
  --slurpfile module "$result_dir/module-before.json" \
  '[{
    batch: 1,
    offset: 0,
    attempted: $attempted,
    replies: $replies,
    errors: $errors,
    pipe_status: $pipe_status,
    module: $module[0]
  }]' >"$result_dir/batches.json"

stream_memory="$(
  redis_raw MEMORY USAGE events:set 2>/dev/null |
    awk 'NF { print; found=1 } END { if (!found) print 0 }'
)"
aof_bytes=0
if [[ "$mode" == "abrupt" ]]; then
  aof_bytes="$(du -sb "$data_dir" | awk '{ print $1 }')"
fi

jq -n \
  --arg schema_version "1" \
  --arg mode "$mode" \
  --arg persistence "$persistence" \
  --arg prefix "$prefix" \
  --argjson threshold_reached "$threshold_reached" \
  --argjson depth_threshold "$depth_threshold" \
  --argjson attempted "$attempted" \
  --argjson acknowledged "$acknowledged" \
  --argjson durable_source_keys "$durable_source_keys" \
  --argjson physical_entries "$physical_entries" \
  --argjson stream_memory_bytes "$stream_memory" \
  --argjson depth_before "$depth_before" \
  --argjson high_water_before "$high_water_before" \
  --argjson unload_seconds "$unload_seconds" \
  --argjson process_restarted "$process_restarted" \
  --argjson aof_bytes "$aof_bytes" \
  --slurpfile batches "$result_dir/batches.json" \
  --slurpfile module_before "$result_dir/module-before.json" \
  '{
    schema_version: ($schema_version | tonumber),
    mode: $mode,
    persistence: $persistence,
    source_key_prefix: $prefix,
    configured_depth_threshold: $depth_threshold,
    threshold_reached: $threshold_reached,
    attempted_commands: $attempted,
    acknowledged_commands: $acknowledged,
    durable_source_keys_after_boundary: $durable_source_keys,
    retained_physical_entries: $physical_entries,
    retained_stream_memory_bytes: $stream_memory_bytes,
    queue_depth_before_boundary: $depth_before,
    queue_high_water_before_boundary: $high_water_before,
    unload_seconds: $unload_seconds,
    process_restarted: $process_restarted,
    aof_directory_bytes: $aof_bytes,
    batches: $batches[0],
    module_before_boundary: $module_before[0]
  }' >"$result_dir/result.json"

cat "$result_dir/result.json"
