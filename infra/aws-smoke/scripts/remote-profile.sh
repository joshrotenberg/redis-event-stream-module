#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: remote-profile.sh <start frequency|stop>" >&2
  exit 2
fi

command="$1"
profile_dir="/var/lib/eventstream-smoke/profile"
perf_data="$profile_dir/perf.data"
perf_pid_file="$profile_dir/perf.pid"
record_stdout="$profile_dir/perf-record.stdout"
record_stderr="$profile_dir/perf-record.stderr"
header_report="$profile_dir/perf-header.txt"
leaf_report="$profile_dir/perf-leaf.txt"
children_report="$profile_dir/perf-children.txt"

profile_file() {
  printf '%s/%s' "$profile_dir" "$1"
}

case "$command" in
  start)
    if [[ $# -ne 2 ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
      echo "usage: remote-profile.sh start <positive-frequency>" >&2
      exit 2
    fi
    frequency="$2"

    command -v perf >/dev/null || {
      echo "perf is not installed" >&2
      exit 1
    }

    install -d -m 0755 "$profile_dir"
    rm -f \
      "$perf_data" \
      "$perf_pid_file" \
      "$record_stdout" \
      "$record_stderr" \
      "$header_report" \
      "$leaf_report" \
      "$children_report" \
      "$(profile_file perf-header.txt.gz)" \
      "$(profile_file perf-leaf.txt.gz)" \
      "$(profile_file perf-children.txt.gz)" \
      "$(profile_file perf-record.stderr.gz)"

    redis_pid="$(docker inspect --format '{{.State.Pid}}' eventstream-server)"
    if ! [[ "$redis_pid" =~ ^[1-9][0-9]*$ ]]; then
      echo "could not resolve the Redis host PID" >&2
      exit 1
    fi

    sysctl -w kernel.perf_event_paranoid=1 >/dev/null

    redis_binary="/proc/$redis_pid/root/usr/local/bin/redis-server"
    module_binary="/proc/$redis_pid/root/usr/local/lib/redis/modules/libredis_event_stream_module.so"
    perf buildid-cache --add "$redis_binary" >/dev/null 2>&1 || true
    perf buildid-cache --add "$module_binary" >/dev/null 2>&1 || true

    nohup perf record \
      --freq "$frequency" \
      --event cpu-clock \
      --call-graph dwarf,8192 \
      --buildid-all \
      --pid "$redis_pid" \
      --output "$perf_data" \
      -- sleep 600 >"$record_stdout" 2>"$record_stderr" &
    perf_pid="$!"
    printf '%s\n' "$perf_pid" >"$perf_pid_file"

    sleep 2
    if ! kill -0 "$perf_pid" >/dev/null 2>&1; then
      cat "$record_stderr" >&2
      echo "perf exited before the benchmark started" >&2
      exit 1
    fi

    jq -n \
      --arg command "$command" \
      --arg perf_version "$(perf version)" \
      --arg event "cpu-clock" \
      --arg call_graph "dwarf,8192" \
      --argjson frequency "$frequency" \
      --argjson redis_pid "$redis_pid" \
      --argjson perf_pid "$perf_pid" \
      '{
        command: $command,
        perf_version: $perf_version,
        event: $event,
        call_graph: $call_graph,
        frequency: $frequency,
        redis_pid: $redis_pid,
        perf_pid: $perf_pid
      }'
    ;;
  stop)
    if [[ $# -ne 1 ]]; then
      echo "usage: remote-profile.sh stop" >&2
      exit 2
    fi
    if [[ ! -f "$perf_pid_file" ]]; then
      echo "missing perf PID file" >&2
      exit 1
    fi

    perf_pid="$(cat "$perf_pid_file")"
    kill -INT "$perf_pid" >/dev/null 2>&1 || true

    for _ in $(seq 1 100); do
      if ! kill -0 "$perf_pid" >/dev/null 2>&1; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "$perf_pid" >/dev/null 2>&1; then
      kill -TERM "$perf_pid" >/dev/null 2>&1 || true
      sleep 1
    fi

    if [[ ! -s "$perf_data" ]]; then
      cat "$record_stderr" >&2
      echo "perf did not produce profile data" >&2
      exit 1
    fi

    perf report \
      --header-only \
      --stdio \
      --input "$perf_data" >"$header_report" 2>&1
    perf report \
      --stdio \
      --input "$perf_data" \
      --no-children \
      --sort comm,dso,symbol \
      --percent-limit 0.10 >"$leaf_report" 2>&1
    perf report \
      --stdio \
      --input "$perf_data" \
      --children \
      --sort comm,dso,symbol \
      --percent-limit 0.50 >"$children_report" 2>&1

    gzip -9 -c "$header_report" >"$(profile_file perf-header.txt.gz)"
    gzip -9 -c "$leaf_report" >"$(profile_file perf-leaf.txt.gz)"
    gzip -9 -c "$children_report" >"$(profile_file perf-children.txt.gz)"
    gzip -9 -c "$record_stderr" >"$(profile_file perf-record.stderr.gz)"

    jq -n \
      --arg command "$command" \
      --arg perf_version "$(perf version)" \
      --arg event "cpu-clock" \
      --arg call_graph "dwarf,8192" \
      --arg sample_header \
        "$(grep -m1 '^# Samples:' "$leaf_report" | sed 's/^# *//' || true)" \
      --argjson perf_data_bytes "$(stat -c '%s' "$perf_data")" \
      --argjson header_report_bytes "$(stat -c '%s' "$header_report")" \
      --argjson leaf_report_bytes "$(stat -c '%s' "$leaf_report")" \
      --argjson children_report_bytes "$(stat -c '%s' "$children_report")" \
      '{
        command: $command,
        perf_version: $perf_version,
        event: $event,
        call_graph: $call_graph,
        sample_header: $sample_header,
        perf_data_bytes: $perf_data_bytes,
        reports: {
          header_bytes: $header_report_bytes,
          leaf_bytes: $leaf_report_bytes,
          children_bytes: $children_report_bytes
        }
      }'
    ;;
  *)
    echo "unknown command: $command" >&2
    exit 2
    ;;
esac
