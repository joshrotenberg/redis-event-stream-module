#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: remote-replication-probe.sh <primary-ip> <redis-image> <maxlen> <event-count> <pause-seconds>" >&2
  exit 2
fi

primary_ip="$1"
redis_image="$2"
maxlen="$3"
event_count="$4"
pause_seconds="$5"
replica_container=eventstream-replica
cli_container=eventstream-replication-cli
module_path=/usr/local/lib/redis/modules/libredis_event_stream_module.so
prefix=replication-probe:

if ! [[ "$maxlen" =~ ^[1-9][0-9]*$ && "$event_count" =~ ^[1-9][0-9]*$ &&
  "$pause_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "maxlen, event count, and pause seconds must be positive integers" >&2
  exit 2
fi
if ((event_count > maxlen)); then
  echo "event count must not exceed maxlen" >&2
  exit 2
fi

cleanup() {
  docker unpause "$replica_container" >/dev/null 2>&1 || true
  docker rm -f "$cli_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$cli_container" >/dev/null 2>&1 || true
docker run --detach --name "$cli_container" --network host \
  "$redis_image" sleep infinity >/dev/null

primary_raw() {
  docker exec "$cli_container" redis-cli -h "$primary_ip" -p 6379 --raw "$@"
}

replica_raw() {
  docker exec "$replica_container" redis-cli --raw "$@"
}

info_field() {
  local endpoint="$1"
  local name="$2"
  "$endpoint" INFO replication |
    awk -F: -v name="$name" '
      $1 == name { gsub(/\r/, "", $2); print $2; found=1; exit }
      END { if (!found) print 0 }
    '
}

source_count() {
  local endpoint="$1"
  "$endpoint" --scan --pattern "${prefix}*" |
    awk 'NF { count += 1 } END { print count + 0 }'
}

module_snapshot() {
  local endpoint="$1"
  "$endpoint" INFO eventstream |
    awk -F: '
      /^eventstream_forwarded:/ { forwarded=$2 }
      /^eventstream_events_lost:/ { events_lost=$2 }
      /^eventstream_dropped:/ { dropped=$2 }
      /^eventstream_handler_panics:/ { handler_panics=$2 }
      /^eventstream_async_worker_errors:/ { async_worker_errors=$2 }
      END {
        gsub(/\r/, "", forwarded)
        gsub(/\r/, "", events_lost)
        gsub(/\r/, "", dropped)
        gsub(/\r/, "", handler_panics)
        gsub(/\r/, "", async_worker_errors)
        printf "{\"forwarded\":%d,\"events_lost\":%d,\"dropped\":%d,", forwarded, events_lost, dropped
        printf "\"handler_panics\":%d,\"async_worker_errors\":%d}\n", handler_panics, async_worker_errors
      }
    '
}

wait_for_replica() {
  local required_offset="$1"
  local deadline=$(( $(date +%s) + 120 ))
  while (( $(date +%s) < deadline )); do
    link="$(info_field replica_raw master_link_status)"
    sync="$(info_field replica_raw master_sync_in_progress)"
    replica_offset="$(info_field replica_raw slave_repl_offset)"
    replica_keys="$(source_count replica_raw)"
    replica_stream="$(replica_raw XLEN events:set)"
    if [[ "$link" == up && "$sync" == 0 && "$replica_offset" -ge "$required_offset" &&
      "$replica_keys" -eq "$event_count" && "$replica_stream" -eq "$event_count" ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

if ! primary_raw MODULE UNLOAD eventstream >/dev/null 2>&1; then
  true
fi
primary_raw MODULE LOAD "$module_path" events set maxlen "$maxlen" >/dev/null
primary_raw FLUSHALL >/dev/null

for _ in $(seq 1 300); do
  if [[ "$(replica_raw DBSIZE)" -eq 0 && "$(info_field replica_raw master_link_status)" == up ]]; then
    break
  fi
  sleep 0.1
done
if [[ "$(replica_raw DBSIZE)" -ne 0 || "$(info_field replica_raw master_link_status)" != up ]]; then
  echo "replica did not reach the clean synchronized starting state" >&2
  exit 1
fi

before_primary_offset="$(info_field primary_raw master_repl_offset)"
before_replica_offset="$(info_field replica_raw slave_repl_offset)"

docker pause "$replica_container" >/dev/null

producer_output="$(
  awk -v prefix="$prefix" -v count="$event_count" '
    BEGIN {
      for (i = 1; i <= count; i++) {
        key = prefix i
        printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", length(key), key
      }
    }
  ' | docker exec -i "$cli_container" redis-cli -h "$primary_ip" -p 6379 --pipe
)"
producer_errors="$(
  sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p' <<<"$producer_output" |
    tail -n 1
)"
producer_errors="${producer_errors:-0}"

for _ in $(seq 1 300); do
  if [[ "$(source_count primary_raw)" -eq "$event_count" &&
    "$(primary_raw XLEN events:set)" -eq "$event_count" ]]; then
    break
  fi
  sleep 0.1
done

sleep "$pause_seconds"
lagged_primary_offset="$(info_field primary_raw master_repl_offset)"
lag_bytes=$((lagged_primary_offset - before_replica_offset))
primary_source_keys="$(source_count primary_raw)"
primary_stream_entries="$(primary_raw XLEN events:set)"
primary_module="$(module_snapshot primary_raw)"

catchup_started_ms="$(date +%s%3N)"
docker unpause "$replica_container" >/dev/null
wait_for_replica "$lagged_primary_offset"
catchup_completed_ms="$(date +%s%3N)"

after_primary_offset="$(info_field primary_raw master_repl_offset)"
after_replica_offset="$(info_field replica_raw slave_repl_offset)"
replica_source_keys="$(source_count replica_raw)"
replica_stream_entries="$(replica_raw XLEN events:set)"
replica_module="$(module_snapshot replica_raw)"
replica_link_status="$(info_field replica_raw master_link_status)"
replica_sync_in_progress="$(info_field replica_raw master_sync_in_progress)"

jq -n \
  --argjson schema_version 1 \
  --argjson attempted_events "$event_count" \
  --argjson pause_seconds "$pause_seconds" \
  --argjson producer_errors "$producer_errors" \
  --argjson before_primary_offset "$before_primary_offset" \
  --argjson before_replica_offset "$before_replica_offset" \
  --argjson lagged_primary_offset "$lagged_primary_offset" \
  --argjson lag_bytes "$lag_bytes" \
  --argjson catchup_started_ms "$catchup_started_ms" \
  --argjson catchup_completed_ms "$catchup_completed_ms" \
  --argjson after_primary_offset "$after_primary_offset" \
  --argjson after_replica_offset "$after_replica_offset" \
  --argjson primary_source_keys "$primary_source_keys" \
  --argjson primary_stream_entries "$primary_stream_entries" \
  --argjson replica_source_keys "$replica_source_keys" \
  --argjson replica_stream_entries "$replica_stream_entries" \
  --argjson primary_module "$primary_module" \
  --argjson replica_module "$replica_module" \
  --arg replica_link_status "$replica_link_status" \
  --argjson replica_sync_in_progress "$replica_sync_in_progress" '
    {
      schema_version: $schema_version,
      attempted_events: $attempted_events,
      pause_seconds: $pause_seconds,
      producer_errors: $producer_errors,
      before_pause: {
        primary_offset: $before_primary_offset,
        replica_offset: $before_replica_offset
      },
      while_paused: {
        primary_offset: $lagged_primary_offset,
        lag_bytes: $lag_bytes
      },
      after_catchup: {
        primary_offset: $after_primary_offset,
        replica_offset: $after_replica_offset,
        link_status: $replica_link_status,
        sync_in_progress: $replica_sync_in_progress,
        catchup_ms: ($catchup_completed_ms - $catchup_started_ms)
      },
      primary: {
        source_keys: $primary_source_keys,
        stream_entries: $primary_stream_entries,
        module: $primary_module
      },
      replica: {
        source_keys: $replica_source_keys,
        stream_entries: $replica_stream_entries,
        module: $replica_module
      },
      passed:
        ($producer_errors == 0) and
        ($lag_bytes > 0) and
        ($primary_source_keys == $attempted_events) and
        ($primary_stream_entries == $attempted_events) and
        ($primary_module.forwarded == $attempted_events) and
        ($primary_module.events_lost == 0) and
        ($primary_module.dropped == 0) and
        ($primary_module.handler_panics == 0) and
        ($primary_module.async_worker_errors == 0) and
        ($replica_source_keys == $attempted_events) and
        ($replica_stream_entries == $attempted_events) and
        ($replica_module.forwarded == 0) and
        ($replica_module.events_lost == 0) and
        ($replica_module.dropped == 0) and
        ($replica_module.handler_panics == 0) and
        ($replica_module.async_worker_errors == 0) and
        ($replica_link_status == "up") and
        ($replica_sync_in_progress == 0) and
        ($after_replica_offset >= $lagged_primary_offset)
    }
  '
