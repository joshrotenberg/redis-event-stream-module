#!/usr/bin/env bash
# Deterministic Redis maxmemory behavior probe for issue #259.
set -euo pipefail

host="${PRESSURE_HOST:-127.0.0.1}"
port="${PRESSURE_PORT:-6379}"
uri="${PRESSURE_URI:-}"
cli_bin="${PRESSURE_CLI_BIN:-redis-cli}"
module_path="${PRESSURE_SERVER_MODULE_PATH:-}"
result_path="${PRESSURE_RESULT_PATH:-}"
prefill_keys="${PRESSURE_PREFILL_KEYS:-50000}"
payload_bytes="${PRESSURE_PAYLOAD_BYTES:-1024}"
delete_events="${PRESSURE_DELETE_EVENTS:-15000}"
write_events="${PRESSURE_WRITE_EVENTS:-15000}"
write_payload_bytes="${PRESSURE_WRITE_PAYLOAD_BYTES:-64}"
maxmemory_percent="${PRESSURE_MAXMEMORY_PERCENT:-90}"
churn_keys="${PRESSURE_CHURN_KEYS:-50000}"
churn_rounds="${PRESSURE_CHURN_ROUNDS:-6}"
maxlen="${PRESSURE_MAXLEN:-50000}"

positive_integer() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }

for value in \
  "$prefill_keys" "$payload_bytes" "$delete_events" "$write_events" \
  "$write_payload_bytes" "$maxmemory_percent" "$churn_keys" \
  "$churn_rounds" "$maxlen"; do
  if ! positive_integer "$value"; then
    echo "pressure controls must be positive integers: $value" >&2
    exit 2
  fi
done
if ((maxmemory_percent >= 100)); then
  echo "PRESSURE_MAXMEMORY_PERCENT must be less than 100" >&2
  exit 2
fi
if ((delete_events >= prefill_keys)); then
  echo "PRESSURE_DELETE_EVENTS must be smaller than PRESSURE_PREFILL_KEYS" >&2
  exit 2
fi
if ((delete_events > maxlen || write_events > maxlen)); then
  echo "pressure event counts must not exceed PRESSURE_MAXLEN" >&2
  exit 2
fi
if [[ -z "$module_path" ]]; then
  echo "PRESSURE_SERVER_MODULE_PATH is required" >&2
  exit 2
fi

for tool in "$cli_bin" awk jq sed; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

if [[ -n "$uri" ]]; then
  cli=("$cli_bin" -u "$uri")
else
  cli=("$cli_bin" -h "$host" -p "$port")
fi

tmp_dir="$(mktemp -d)"

redis_raw() { "${cli[@]}" --raw "$@"; }

module_loaded() {
  redis_raw MODULE LIST 2>/dev/null | grep -q eventstream
}

cleanup() {
  local status="$?"
  trap - EXIT
  set +e
  redis_raw CONFIG SET maxmemory 0 >/dev/null 2>&1
  redis_raw CONFIG SET maxmemory-policy noeviction >/dev/null 2>&1
  if module_loaded; then
    redis_raw CONFIG SET eventstream.enabled no >/dev/null 2>&1
    redis_raw MODULE UNLOAD eventstream >/dev/null 2>&1
  fi
  redis_raw FLUSHALL >/dev/null 2>&1
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT

epoch_ms() {
  local now seconds micros
  now="$(redis_raw TIME)"
  seconds="$(sed -n '1p' <<<"$now")"
  micros="$(sed -n '2p' <<<"$now")"
  echo $((seconds * 1000 + micros / 1000))
}

info_value() {
  local input="$1"
  local name="$2"
  local fallback="${3:-0}"
  awk -F: -v name="$name" -v fallback="$fallback" '
    $1 == name { gsub(/\r/, "", $2); print $2; found=1; exit }
    END { if (!found) print fallback }
  ' <<<"$input"
}

config_value() {
  redis_raw CONFIG GET "$1" | sed -n '2p'
}

module_snapshot() {
  local info
  info="$(redis_raw INFO eventstream)"
  jq -n \
    --argjson enabled "$(info_value "$info" eventstream_enabled)" \
    --argjson eviction_risk "$(info_value "$info" eventstream_eviction_risk)" \
    --argjson forwarded "$(info_value "$info" eventstream_forwarded)" \
    --argjson events_lost "$(info_value "$info" eventstream_events_lost)" \
    --argjson dropped "$(info_value "$info" eventstream_dropped)" \
    --argjson dropped_oom "$(info_value "$info" eventstream_dropped_oom)" \
    --argjson dropped_xadd_error "$(info_value "$info" eventstream_dropped_xadd_error)" \
    --argjson registry_errors "$(info_value "$info" eventstream_registry_errors)" \
    --argjson active_streams "$(info_value "$info" eventstream_active_streams)" \
    --argjson control_markers "$(info_value "$info" eventstream_control_markers)" \
    --argjson handler_panics "$(info_value "$info" eventstream_handler_panics)" \
    --argjson last_error_time "$(info_value "$info" eventstream_last_error_time)" '
      {enabled: $enabled, eviction_risk: $eviction_risk,
       forwarded: $forwarded, events_lost: $events_lost,
       dropped: $dropped, dropped_oom: $dropped_oom,
       dropped_xadd_error: $dropped_xadd_error,
       registry_errors: $registry_errors, active_streams: $active_streams,
       control_markers: $control_markers, handler_panics: $handler_panics,
       last_error_time: $last_error_time}'
}

memory_snapshot() {
  local memory stats errorstats policy oom_count
  memory="$(redis_raw INFO memory)"
  stats="$(redis_raw INFO stats)"
  errorstats="$(redis_raw INFO errorstats)"
  policy="$(config_value maxmemory-policy)"
  oom_count="$(
    awk -F'[=,]' '
      $1 == "errorstat_OOM:count" { gsub(/\r/, "", $2); print $2; found=1; exit }
      END { if (!found) print 0 }
    ' <<<"$errorstats"
  )"
  jq -n \
    --arg policy "$policy" \
    --argjson maxmemory "$(config_value maxmemory)" \
    --argjson used_memory "$(info_value "$memory" used_memory)" \
    --argjson used_memory_rss "$(info_value "$memory" used_memory_rss)" \
    --argjson used_memory_peak "$(info_value "$memory" used_memory_peak)" \
    --argjson used_memory_overhead "$(info_value "$memory" used_memory_overhead)" \
    --argjson used_memory_dataset "$(info_value "$memory" used_memory_dataset)" \
    --argjson allocator_active "$(info_value "$memory" allocator_active)" \
    --argjson allocator_resident "$(info_value "$memory" allocator_resident)" \
    --argjson fragmentation_ratio "$(info_value "$memory" mem_fragmentation_ratio 0)" \
    --argjson allocator_frag_ratio "$(info_value "$memory" allocator_frag_ratio 0)" \
    --argjson evicted_keys "$(info_value "$stats" evicted_keys)" \
    --argjson expired_keys "$(info_value "$stats" expired_keys)" \
    --argjson oom_errors "$oom_count" '
      {maxmemory_policy: $policy, maxmemory_bytes: $maxmemory,
       used_memory_bytes: $used_memory, used_memory_rss_bytes: $used_memory_rss,
       used_memory_peak_bytes: $used_memory_peak,
       used_memory_overhead_bytes: $used_memory_overhead,
       used_memory_dataset_bytes: $used_memory_dataset,
       allocator_active_bytes: $allocator_active,
       allocator_resident_bytes: $allocator_resident,
       fragmentation_ratio: $fragmentation_ratio,
       allocator_frag_ratio: $allocator_frag_ratio,
       evicted_keys: $evicted_keys, expired_keys: $expired_keys,
       oom_errors: $oom_errors}'
}

source_count() {
  local pattern="$1"
  redis_raw --scan --pattern "$pattern" |
    awk 'NF { count += 1 } END { print count + 0 }'
}

stream_exists() {
  redis_raw EXISTS events:set
}

stream_entries() {
  if [[ "$(stream_exists)" == 1 ]]; then
    redis_raw XLEN events:set
  else
    echo 0
  fi
}

wait_for_eviction_risk() {
  local expected="$1"
  local current
  for _ in $(seq 1 100); do
    current="$(module_snapshot | jq -r '.eviction_risk')"
    [[ "$current" == "$expected" ]] && return 0
    sleep 0.05
  done
  echo "eviction_risk did not reach $expected" >&2
  return 1
}

prepare_case() {
  local verify_oom="$1"
  redis_raw CONFIG SET maxmemory 0 >/dev/null
  redis_raw CONFIG SET maxmemory-policy noeviction >/dev/null
  if module_loaded; then
    redis_raw MODULE UNLOAD eventstream >/dev/null
  fi
  redis_raw FLUSHALL >/dev/null
  redis_raw CONFIG RESETSTAT >/dev/null
  redis_raw MODULE LOAD "$module_path" \
    events del maxlen "$maxlen" verify-oom "$verify_oom" >/dev/null
}

set_pressure_limit() {
  local policy="$1"
  local used_memory="$2"
  pressure_limit=$((used_memory * maxmemory_percent / 100))
  redis_raw CONFIG SET maxmemory-policy "$policy" >/dev/null
  redis_raw CONFIG SET maxmemory "$pressure_limit" >/dev/null
  if [[ "$policy" == allkeys-* ]]; then
    wait_for_eviction_risk 1
  else
    wait_for_eviction_risk 0
  fi
}

emit_del_commands() {
  local prefix="$1"
  local count="$2"
  awk -v prefix="$prefix" -v count="$count" '
    BEGIN {
      for (i = 1; i <= count; i++) {
        key = prefix i
        printf "*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", length(key), key
      }
    }
  '
}

emit_set_commands() {
  local prefix="$1"
  local count="$2"
  local bytes="$3"
  local ttl_seconds="$4"
  local offset="${5:-0}"
  awk -v prefix="$prefix" -v count="$count" -v bytes="$bytes" \
    -v ttl="$ttl_seconds" -v offset="$offset" '
    BEGIN {
      value = sprintf("%*s", bytes, "")
      gsub(/ /, "x", value)
      for (i = 1; i <= count; i++) {
        key = prefix (offset + i)
        if (ttl > 0) {
          printf "*5\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$2\r\nEX\r\n$%d\r\n%d\r\n", \
            length(key), key, length(value), value, length(ttl), ttl
        } else {
          printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", \
            length(key), key, length(value), value
        }
      }
    }
  '
}

run_pipe() {
  local label="$1"
  local kind="$2"
  local prefix="$3"
  local count="$4"
  local bytes="${5:-0}"
  local ttl_seconds="${6:-0}"
  local offset="${7:-0}"
  local output="$tmp_dir/$label.pipe"
  local stderr="$tmp_dir/$label.stderr"
  local started_ms finished_ms errors replies
  local statuses

  started_ms="$(epoch_ms)"
  set +e
  {
    case "$kind" in
      del) emit_del_commands "$prefix" "$count" ;;
      set) emit_set_commands "$prefix" "$count" "$bytes" "$ttl_seconds" "$offset" ;;
      *) exit 2 ;;
    esac
  } | "${cli[@]}" --pipe >"$output" 2>"$stderr"
  statuses=("${PIPESTATUS[@]}")
  set -e
  finished_ms="$(epoch_ms)"

  errors="$(
    sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p' "$output" | tail -n 1
  )"
  replies="$(
    sed -n 's/.*replies: \([0-9][0-9]*\).*/\1/p' "$output" | tail -n 1
  )"
  errors="${errors:-0}"
  replies="${replies:-0}"
  pipe_result="$tmp_dir/$label-producer.json"
  jq -n \
    --argjson generator_status "${statuses[0]:-1}" \
    --argjson cli_status "${statuses[1]:-1}" \
    --argjson attempted "$count" \
    --argjson replies "$replies" \
    --argjson errors "$errors" \
    --argjson started_ms "$started_ms" \
    --argjson finished_ms "$finished_ms" '
      {attempted: $attempted, replies: $replies, errors: $errors,
       generator_status: $generator_status, cli_status: $cli_status,
       started_ms: $started_ms, finished_ms: $finished_ms,
       elapsed_ms: ($finished_ms - $started_ms),
       operations_per_second:
         (if $finished_ms > $started_ms then
            ($replies * 1000 / ($finished_ms - $started_ms))
          else null end)}' >"$pipe_result"
}

prefill() {
  local label="$1"
  local prefix="$2"
  local ttl_seconds="$3"
  run_pipe "$label" set "$prefix" "$prefill_keys" "$payload_bytes" "$ttl_seconds"
  jq -e --argjson expected "$prefill_keys" '
    .generator_status == 0 and .cli_status == 0 and
    .errors == 0 and .replies == $expected
  ' "$pipe_result" >/dev/null
  if [[ "$(source_count "${prefix}*")" -ne "$prefill_keys" ]]; then
    echo "prefill did not reconcile for $label" >&2
    return 1
  fi
}

run_noeviction_case() {
  local verify_oom="$1"
  local case_name="noeviction-verify-$verify_oom"
  local prefix="eventstream:pressure:${case_name}:"
  local before_source after_source before_stream after_stream
  local expected_risk=0

  prepare_case "$verify_oom"
  prefill "$case_name-prefill" "$prefix" 0
  memory_snapshot >"$tmp_dir/$case_name-memory-before.json"
  module_snapshot >"$tmp_dir/$case_name-module-before.json"
  before_source="$(source_count "${prefix}*")"
  before_stream="$(redis_raw XLEN events:del)"
  set_pressure_limit noeviction \
    "$(jq -r '.used_memory_bytes' "$tmp_dir/$case_name-memory-before.json")"
  memory_snapshot >"$tmp_dir/$case_name-memory-pressured.json"

  run_pipe "$case_name-delete" del "$prefix" "$delete_events"
  memory_snapshot >"$tmp_dir/$case_name-memory-after.json"
  module_snapshot >"$tmp_dir/$case_name-module-after.json"
  after_source="$(source_count "${prefix}*")"
  after_stream="$(redis_raw XLEN events:del)"

  jq -n \
    --arg name "$case_name" \
    --arg policy noeviction \
    --arg verify_oom "$verify_oom" \
    --argjson maxmemory_percent "$maxmemory_percent" \
    --argjson expected_events "$delete_events" \
    --argjson before_source "$before_source" \
    --argjson after_source "$after_source" \
    --argjson before_stream "$before_stream" \
    --argjson after_stream "$after_stream" \
    --argjson expected_risk "$expected_risk" \
    --slurpfile producer "$pipe_result" \
    --slurpfile memory_before "$tmp_dir/$case_name-memory-before.json" \
    --slurpfile memory_pressured "$tmp_dir/$case_name-memory-pressured.json" \
    --slurpfile memory_after "$tmp_dir/$case_name-memory-after.json" \
    --slurpfile module_before "$tmp_dir/$case_name-module-before.json" \
    --slurpfile module_after "$tmp_dir/$case_name-module-after.json" '
      ($module_after[0].forwarded - $module_before[0].forwarded) as $forwarded |
      ($module_after[0].events_lost - $module_before[0].events_lost) as $lost |
      ($module_after[0].dropped - $module_before[0].dropped) as $dropped |
      ($module_after[0].dropped_oom - $module_before[0].dropped_oom) as $dropped_oom |
      {
        name: $name, policy: $policy, verify_oom: ($verify_oom == "yes"),
        maxmemory_percent: $maxmemory_percent,
        expected_events: $expected_events,
        producer: $producer[0],
        source_keys: {before: $before_source, after: $after_source},
        stream_entries: {before: $before_stream, after: $after_stream},
        module: {before: $module_before[0], after: $module_after[0],
          delta: {forwarded: $forwarded, events_lost: $lost,
            dropped: $dropped, dropped_oom: $dropped_oom,
            registry_errors:
              ($module_after[0].registry_errors - $module_before[0].registry_errors)}},
        memory: {before_limit: $memory_before[0],
          pressured: $memory_pressured[0], after: $memory_after[0]},
        passed:
          ($producer[0].generator_status == 0) and
          ($producer[0].cli_status == 0) and
          ($producer[0].errors == 0) and
          ($producer[0].replies == $expected_events) and
          ($before_source - $after_source == $expected_events) and
          ($before_stream == 0) and
          ($after_stream == $forwarded) and
          ($forwarded + $lost == $expected_events) and
          ($dropped == $lost) and ($dropped_oom == $lost) and
          ($module_after[0].eviction_risk == $expected_risk) and
          ($module_after[0].handler_panics == 0) and
          (if $verify_oom == "yes" then
             ($lost > 0) and ($forwarded > 0)
           else
             ($lost == 0) and ($forwarded == $expected_events)
           end)
      }
    ' >"$tmp_dir/$case_name.json"
}

run_volatile_case() {
  local case_name=volatile-lru
  local prefill_prefix="eventstream:pressure:${case_name}:prefill:"
  local write_prefix="eventstream:pressure:${case_name}:write:"
  local source_after stream_after

  prepare_case yes
  prefill "$case_name-prefill" "$prefill_prefix" 3600
  memory_snapshot >"$tmp_dir/$case_name-memory-before.json"
  set_pressure_limit volatile-lru \
    "$(jq -r '.used_memory_bytes' "$tmp_dir/$case_name-memory-before.json")"
  redis_raw CONFIG SET eventstream.events set >/dev/null
  redis_raw CONFIG RESETSTAT >/dev/null
  memory_snapshot >"$tmp_dir/$case_name-memory-pressured.json"
  module_snapshot >"$tmp_dir/$case_name-module-before.json"

  run_pipe "$case_name-write" set "$write_prefix" "$write_events" \
    "$write_payload_bytes" 3600
  source_after="$(source_count 'eventstream:pressure:volatile-lru:*')"
  stream_after="$(stream_entries)"
  memory_snapshot >"$tmp_dir/$case_name-memory-after.json"
  module_snapshot >"$tmp_dir/$case_name-module-after.json"

  jq -n \
    --arg name "$case_name" \
    --arg policy volatile-lru \
    --argjson expected_events "$write_events" \
    --argjson source_after "$source_after" \
    --argjson stream_after "$stream_after" \
    --slurpfile producer "$pipe_result" \
    --slurpfile memory_before "$tmp_dir/$case_name-memory-before.json" \
    --slurpfile memory_pressured "$tmp_dir/$case_name-memory-pressured.json" \
    --slurpfile memory_after "$tmp_dir/$case_name-memory-after.json" \
    --slurpfile module_before "$tmp_dir/$case_name-module-before.json" \
    --slurpfile module_after "$tmp_dir/$case_name-module-after.json" '
      ($module_after[0].forwarded - $module_before[0].forwarded) as $forwarded |
      ($module_after[0].events_lost - $module_before[0].events_lost) as $lost |
      ($module_after[0].dropped - $module_before[0].dropped) as $dropped |
      ($memory_after[0].evicted_keys - $memory_pressured[0].evicted_keys) as $evicted |
      {
        name: $name, policy: $policy, verify_oom: true,
        expected_events: $expected_events,
        producer: $producer[0], source_keys_after: $source_after,
        stream_entries_after: $stream_after,
        module: {before: $module_before[0], after: $module_after[0],
          delta: {forwarded: $forwarded, events_lost: $lost,
            dropped: $dropped, dropped_oom:
              ($module_after[0].dropped_oom - $module_before[0].dropped_oom)}},
        memory: {before_limit: $memory_before[0],
          pressured: $memory_pressured[0], after: $memory_after[0],
          evicted_keys_delta: $evicted},
        passed:
          ($producer[0].generator_status == 0) and
          ($producer[0].cli_status == 0) and
          ($producer[0].errors == 0) and
          ($producer[0].replies == $expected_events) and
          ($forwarded == $expected_events) and
          ($lost == 0) and ($dropped == 0) and
          ($stream_after == $expected_events) and
          ($evicted > 0) and
          ($module_after[0].eviction_risk == 0) and
          ($module_after[0].registry_errors == 0) and
          ($module_after[0].handler_panics == 0)
      }
    ' >"$tmp_dir/$case_name.json"
}

run_allkeys_case() {
  local case_name=allkeys-lru
  local prefill_prefix="eventstream:pressure:${case_name}:prefill:"
  local write_prefix="eventstream:pressure:${case_name}:write:"
  local churn_prefix="eventstream:pressure:${case_name}:churn:"
  local captured_stream captured_exists after_stream after_exists
  local rounds_run=0 churn_replies=0 churn_errors=0 churn_elapsed_ms=0
  local round offset round_replies round_errors round_elapsed

  prepare_case yes
  prefill "$case_name-prefill" "$prefill_prefix" 0
  memory_snapshot >"$tmp_dir/$case_name-memory-before.json"
  set_pressure_limit allkeys-lru \
    "$(jq -r '.used_memory_bytes' "$tmp_dir/$case_name-memory-before.json")"
  redis_raw CONFIG SET eventstream.events set >/dev/null
  redis_raw CONFIG RESETSTAT >/dev/null
  memory_snapshot >"$tmp_dir/$case_name-memory-pressured.json"
  module_snapshot >"$tmp_dir/$case_name-module-before.json"

  run_pipe "$case_name-write" set "$write_prefix" "$write_events" \
    "$write_payload_bytes" 0
  cp "$pipe_result" "$tmp_dir/$case_name-capture-producer.json"
  captured_exists="$(stream_exists)"
  captured_stream="$(stream_entries)"
  memory_snapshot >"$tmp_dir/$case_name-memory-captured.json"
  module_snapshot >"$tmp_dir/$case_name-module-captured.json"

  redis_raw CONFIG SET eventstream.enabled no >/dev/null
  sleep 1
  if [[ "$captured_stream" -eq "$write_events" ]]; then
    for round in $(seq 1 "$churn_rounds"); do
      rounds_run="$round"
      offset=$(((round - 1) * churn_keys))
      run_pipe "$case_name-churn-$round" set "$churn_prefix" "$churn_keys" \
        "$payload_bytes" 0 "$offset"
      round_replies="$(jq -r '.replies' "$pipe_result")"
      round_errors="$(jq -r '.errors' "$pipe_result")"
      round_elapsed="$(jq -r '.elapsed_ms' "$pipe_result")"
      churn_replies=$((churn_replies + round_replies))
      churn_errors=$((churn_errors + round_errors))
      churn_elapsed_ms=$((churn_elapsed_ms + round_elapsed))
      if [[ "$(stream_entries)" -lt "$captured_stream" ]]; then
        break
      fi
    done
  fi

  after_exists="$(stream_exists)"
  after_stream="$(stream_entries)"
  memory_snapshot >"$tmp_dir/$case_name-memory-after.json"
  module_snapshot >"$tmp_dir/$case_name-module-after.json"

  jq -n \
    --arg name "$case_name" \
    --arg policy allkeys-lru \
    --argjson expected_events "$write_events" \
    --argjson captured_exists "$captured_exists" \
    --argjson captured_stream "$captured_stream" \
    --argjson after_exists "$after_exists" \
    --argjson after_stream "$after_stream" \
    --argjson rounds_run "$rounds_run" \
    --argjson churn_replies "$churn_replies" \
    --argjson churn_errors "$churn_errors" \
    --argjson churn_elapsed_ms "$churn_elapsed_ms" \
    --slurpfile producer "$tmp_dir/$case_name-capture-producer.json" \
    --slurpfile memory_before "$tmp_dir/$case_name-memory-before.json" \
    --slurpfile memory_pressured "$tmp_dir/$case_name-memory-pressured.json" \
    --slurpfile memory_captured "$tmp_dir/$case_name-memory-captured.json" \
    --slurpfile memory_after "$tmp_dir/$case_name-memory-after.json" \
    --slurpfile module_before "$tmp_dir/$case_name-module-before.json" \
    --slurpfile module_captured "$tmp_dir/$case_name-module-captured.json" \
    --slurpfile module_after "$tmp_dir/$case_name-module-after.json" '
      ($module_captured[0].forwarded - $module_before[0].forwarded) as $forwarded |
      ($module_captured[0].events_lost - $module_before[0].events_lost) as $capture_lost |
      ($module_captured[0].dropped - $module_before[0].dropped) as $capture_dropped |
      ($memory_after[0].evicted_keys - $memory_pressured[0].evicted_keys) as $evicted |
      {
        name: $name, policy: $policy, verify_oom: true,
        expected_events: $expected_events,
        producer: $producer[0],
        captured_stream: {exists: ($captured_exists == 1), entries: $captured_stream},
        churn: {rounds: $rounds_run, replies: $churn_replies,
          errors: $churn_errors, elapsed_ms: $churn_elapsed_ms},
        retained_stream: {exists: ($after_exists == 1), entries: $after_stream},
        silent_history_loss: ($after_stream < $forwarded),
        module: {before: $module_before[0], captured: $module_captured[0],
          after_churn: $module_after[0],
          capture_delta: {forwarded: $forwarded, events_lost: $capture_lost,
            dropped: $capture_dropped},
          churn_delta: {
            forwarded: ($module_after[0].forwarded - $module_captured[0].forwarded),
            events_lost: ($module_after[0].events_lost - $module_captured[0].events_lost),
            dropped: ($module_after[0].dropped - $module_captured[0].dropped)}},
        memory: {before_limit: $memory_before[0],
          pressured: $memory_pressured[0], captured: $memory_captured[0],
          after_churn: $memory_after[0], evicted_keys_delta: $evicted},
        passed:
          ($producer[0].generator_status == 0) and
          ($producer[0].cli_status == 0) and
          ($producer[0].errors == 0) and
          ($producer[0].replies == $expected_events) and
          ($forwarded == $expected_events) and
          ($capture_lost == 0) and ($capture_dropped == 0) and
          ($captured_exists == 1) and ($captured_stream > 0) and
          ($churn_errors == 0) and ($evicted > 0) and
          ($after_stream < $forwarded) and
          ($module_after[0].eviction_risk == 1) and
          ($module_after[0].forwarded == $module_captured[0].forwarded) and
          ($module_after[0].events_lost == $module_captured[0].events_lost) and
          ($module_after[0].dropped == $module_captured[0].dropped) and
          ($module_after[0].handler_panics == 0)
      }
    ' >"$tmp_dir/$case_name.json"
}

run_noeviction_case yes
run_noeviction_case no
run_volatile_case
run_allkeys_case

result_tmp="$tmp_dir/result.json"
jq -s \
  --argjson schema_version 1 \
  --argjson prefill_keys "$prefill_keys" \
  --argjson payload_bytes "$payload_bytes" \
  --argjson delete_events "$delete_events" \
  --argjson write_events "$write_events" \
  --argjson write_payload_bytes "$write_payload_bytes" \
  --argjson maxmemory_percent "$maxmemory_percent" \
  --argjson churn_keys "$churn_keys" \
  --argjson churn_rounds "$churn_rounds" \
  --argjson maxlen "$maxlen" '
    {
      schema_version: $schema_version,
      configuration: {
        prefill_keys: $prefill_keys, payload_bytes: $payload_bytes,
        delete_events: $delete_events, write_events: $write_events,
        write_payload_bytes: $write_payload_bytes,
        maxmemory_percent: $maxmemory_percent,
        churn_keys_per_round: $churn_keys, churn_rounds: $churn_rounds,
        maxlen: $maxlen
      },
      cases: .,
      totals: {
        expected_events: (map(.expected_events) | add),
        forwarded: (map(.module.delta.forwarded // .module.capture_delta.forwarded) | add),
        events_lost: (map(.module.delta.events_lost // .module.capture_delta.events_lost) | add),
        dropped: (map(.module.delta.dropped // .module.capture_delta.dropped) | add),
        producer_errors: (map(.producer.errors) | add),
        evicted_keys: (map(.memory.evicted_keys_delta // 0) | add)
      },
      passed: all(.[]; .passed == true)
    }
  ' \
  "$tmp_dir/noeviction-verify-yes.json" \
  "$tmp_dir/noeviction-verify-no.json" \
  "$tmp_dir/volatile-lru.json" \
  "$tmp_dir/allkeys-lru.json" >"$result_tmp"

jq -e '.passed == true' "$result_tmp" >/dev/null
if [[ -n "$result_path" ]]; then
  mkdir -p "$(dirname "$result_path")"
  install -m 0644 "$result_tmp" "$result_path"
fi
cat "$result_tmp"
