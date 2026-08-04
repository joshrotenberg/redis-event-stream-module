#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: remote-persistence-probe.sh <off|aof-everysec|aof-always> <module-image> <module-so|-> <maxlen> <event-count>" >&2
  exit 2
fi

persistence_mode="$1"
module_image="$2"
module_override="$3"
maxlen="$4"
event_count="$5"
container="eventstream-server"
prefix="persistence-probe:"

case "$persistence_mode" in
  off | aof-everysec | aof-always) ;;
  *)
    echo "persistence mode must be off, aof-everysec, or aof-always" >&2
    exit 2
    ;;
esac
if ! [[ "$maxlen" =~ ^[1-9][0-9]*$ && "$event_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "maxlen and event count must be positive integers" >&2
  exit 2
fi
if ((event_count > maxlen)); then
  echo "event count must not exceed maxlen" >&2
  exit 2
fi
if [[ ! -x /tmp/eventstream-server.sh ]]; then
  echo "/tmp/eventstream-server.sh is missing or not executable" >&2
  exit 1
fi

redis_raw() {
  docker exec "$container" redis-cli --raw "$@"
}

info_field() {
  local section="$1"
  local name="$2"
  redis_raw INFO "$section" |
    awk -F: -v name="$name" '
      $1 == name { gsub(/\r/, "", $2); print $2; found=1; exit }
      END { if (!found) print 0 }
    '
}

source_count() {
  redis_raw --scan --pattern "${prefix}*" |
    awk 'NF { count += 1 } END { print count + 0 }'
}

module_snapshot() {
  jq -n \
    --argjson forwarded "$(info_field eventstream eventstream_forwarded)" \
    --argjson events_lost "$(info_field eventstream eventstream_events_lost)" \
    --argjson dropped "$(info_field eventstream eventstream_dropped)" \
    --argjson handler_panics "$(info_field eventstream eventstream_handler_panics)" \
    --argjson async_worker_errors "$(info_field eventstream eventstream_async_worker_errors)" \
    '{forwarded: $forwarded, events_lost: $events_lost, dropped: $dropped,
      handler_panics: $handler_panics, async_worker_errors: $async_worker_errors}'
}

persistence_snapshot() {
  local last_write_status
  last_write_status="$(info_field persistence aof_last_write_status)"
  [[ "$last_write_status" == 0 ]] && last_write_status=unknown
  jq -n \
    --argjson aof_enabled "$(info_field persistence aof_enabled)" \
    --argjson aof_current_size_bytes "$(info_field persistence aof_current_size)" \
    --argjson aof_base_size_bytes "$(info_field persistence aof_base_size)" \
    --argjson aof_delayed_fsync "$(info_field persistence aof_delayed_fsync)" \
    --argjson aof_pending_bio_fsync "$(info_field persistence aof_pending_bio_fsync)" \
    --arg aof_last_write_status "$last_write_status" \
    '{aof_enabled: $aof_enabled,
      aof_current_size_bytes: $aof_current_size_bytes,
      aof_base_size_bytes: $aof_base_size_bytes,
      aof_delayed_fsync: $aof_delayed_fsync,
      aof_pending_bio_fsync: $aof_pending_bio_fsync,
      aof_last_write_status: $aof_last_write_status}'
}

start_server() {
  local data_action="$1"
  /tmp/eventstream-server.sh \
    s2 "$module_image" "$maxlen" "$module_override" 65536 64 1 \
    "$persistence_mode" "$data_action" >/dev/null
}

start_server reset

producer_output="$(
  awk -v prefix="$prefix" -v count="$event_count" '
    BEGIN {
      for (i = 1; i <= count; i++) {
        key = prefix i
        printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", length(key), key
      }
    }
  ' | docker exec -i "$container" redis-cli --pipe
)"
producer_errors="$(
  sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p' <<<"$producer_output" |
    tail -n 1
)"
producer_errors="${producer_errors:-0}"

if [[ "$persistence_mode" != off ]]; then
  redis_raw WAITAOF 1 0 5000 >/dev/null
fi

before_source_keys="$(source_count)"
before_stream_entries="$(redis_raw XLEN events:set)"
before_module="$(module_snapshot)"
before_persistence="$(persistence_snapshot)"

set +e
redis_raw SHUTDOWN NOSAVE >/dev/null 2>&1
shutdown_status="$?"
set -e
docker wait "$container" >/dev/null 2>&1 || true
start_server preserve

after_source_keys="$(source_count)"
after_stream_entries="$(redis_raw XLEN events:set)"
after_module="$(module_snapshot)"
after_persistence="$(persistence_snapshot)"

expected_survival=false
expected_after=0
expected_aof_enabled=0
if [[ "$persistence_mode" != off ]]; then
  expected_survival=true
  expected_after="$event_count"
  expected_aof_enabled=1
fi

jq -n \
  --argjson schema_version 1 \
  --arg mode "$persistence_mode" \
  --argjson event_count "$event_count" \
  --argjson producer_errors "$producer_errors" \
  --argjson shutdown_status "$shutdown_status" \
  --argjson expected_survival "$expected_survival" \
  --argjson expected_after "$expected_after" \
  --argjson expected_aof_enabled "$expected_aof_enabled" \
  --argjson before_source_keys "$before_source_keys" \
  --argjson before_stream_entries "$before_stream_entries" \
  --argjson after_source_keys "$after_source_keys" \
  --argjson after_stream_entries "$after_stream_entries" \
  --argjson before_module "$before_module" \
  --argjson after_module "$after_module" \
  --argjson before_persistence "$before_persistence" \
  --argjson after_persistence "$after_persistence" '
    {
      schema_version: $schema_version,
      mode: $mode,
      attempted_events: $event_count,
      expected_survival: $expected_survival,
      producer_errors: $producer_errors,
      shutdown_status: $shutdown_status,
      before_restart: {
        source_keys: $before_source_keys,
        stream_entries: $before_stream_entries,
        module: $before_module,
        persistence: $before_persistence
      },
      after_restart: {
        source_keys: $after_source_keys,
        stream_entries: $after_stream_entries,
        module: $after_module,
        persistence: $after_persistence
      },
      passed:
        ($producer_errors == 0) and
        ($before_source_keys == $event_count) and
        ($before_stream_entries == $event_count) and
        ($before_module.forwarded == $event_count) and
        ($before_module.events_lost == 0) and
        ($before_module.dropped == 0) and
        ($before_module.handler_panics == 0) and
        ($before_module.async_worker_errors == 0) and
        ($after_source_keys == $expected_after) and
        ($after_stream_entries == $expected_after) and
        ($after_module.forwarded == 0) and
        ($after_module.events_lost == 0) and
        ($after_module.dropped == 0) and
        ($after_module.handler_panics == 0) and
        ($after_module.async_worker_errors == 0) and
        ($after_persistence.aof_enabled == $expected_aof_enabled) and
        (if $expected_aof_enabled == 1 then
           ($before_persistence.aof_last_write_status == "ok") and
           ($after_persistence.aof_last_write_status == "ok")
         else true end)
    }
  '
