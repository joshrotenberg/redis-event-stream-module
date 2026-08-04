#!/usr/bin/env bash
# Time-based saturation harness for issue #254.
#
# This is intentionally separate from bench/run.sh: the existing script stays a
# fast, request-counted regression check, while this runner produces a
# reproducible performance artifact from warmup and measurement windows.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

schema_version=1
scenarios="${SATURATION_SCENARIOS:-s0 s1 s2 expiry-s0 expiry-s2}"
selectivities="${SATURATION_SELECTIVITIES:-0 1 10 100}"
client_levels="${SATURATION_CLIENT_LEVELS:-50}"
thread_levels="${SATURATION_THREAD_LEVELS:-4}"
pipeline_levels="${SATURATION_PIPELINE_LEVELS:-1}"
rate_limit_levels="${SATURATION_RATE_LIMIT_LEVELS:-0}"
repetitions="${SATURATION_REPETITIONS:-5}"
warmup_seconds="${SATURATION_WARMUP_SECONDS:-10}"
measurement_seconds="${SATURATION_MEASUREMENT_SECONDS:-60}"
payload_bytes="${SATURATION_PAYLOAD_BYTES:-64}"
keyspace="${SATURATION_KEYSPACE:-100000}"
maxlen="${SATURATION_MAXLEN:-10000}"
seed="${SATURATION_SEED:-254}"
plan_only="${SATURATION_PLAN_ONLY:-no}"
run_id="${SATURATION_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
results_dir="${SATURATION_RESULTS_DIR:-$repo_dir/bench/results/$run_id}"

host="${SATURATION_HOST:-127.0.0.1}"
port="${SATURATION_PORT:-6394}"
uri="${SATURATION_URI:-}"
start_server="${SATURATION_START_SERVER:-yes}"
build_module="${SATURATION_BUILD_MODULE:-yes}"
server_bin="${SATURATION_SERVER_BIN:-redis-server}"
cli_bin="${SATURATION_CLI_BIN:-redis-cli}"
memtier_bin="${SATURATION_MEMTIER_BIN:-memtier_benchmark}"
server_module_path="${SATURATION_SERVER_MODULE_PATH:-}"
server_log="${SATURATION_SERVER_LOG:-}"
metrics_hook="${SATURATION_METRICS_HOOK:-}"
profiler_hook="${SATURATION_PROFILER_HOOK:-}"
commands_file="${SATURATION_COMMANDS_FILE:-}"
monitor_input="${SATURATION_MONITOR_INPUT:-}"
capture_events="${SATURATION_CAPTURE_EVENTS:-set}"
expected_events_override="${SATURATION_EXPECTED_EVENTS:-}"
workload_name="${SATURATION_WORKLOAD_NAME:-builtin-write-only}"
p99_budget_ms="${SATURATION_P99_BUDGET_MS:-2}"
achievement_ratio="${SATURATION_ACHIEVEMENT_RATIO:-0.98}"

expiry_keys="${SATURATION_EXPIRY_KEYS:-100000}"
expiry_ttl_min_ms="${SATURATION_EXPIRY_TTL_MIN_MS:-5000}"
expiry_ttl_spread_ms="${SATURATION_EXPIRY_TTL_SPREAD_MS:-5000}"
expiry_clients="${SATURATION_EXPIRY_CLIENTS:-8}"
expiry_timeout_seconds="${SATURATION_EXPIRY_TIMEOUT_SECONDS:-120}"
expiry_poll_seconds="${SATURATION_EXPIRY_POLL_SECONDS:-0.02}"

usage() {
  cat <<'USAGE'
usage: bench/saturation.sh

The runner is configured with SATURATION_* environment variables. Common
examples:

  SATURATION_PLAN_ONLY=yes bench/saturation.sh
  SATURATION_REPETITIONS=1 SATURATION_WARMUP_SECONDS=1 \
    SATURATION_MEASUREMENT_SECONDS=5 bench/saturation.sh
  SATURATION_START_SERVER=no SATURATION_HOST=10.0.0.10 \
    SATURATION_PORT=6379 SATURATION_SERVER_MODULE_PATH=/path/on/server/module.so \
    bench/saturation.sh

See bench/saturation/README.md for the complete contract.
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

positive_integer() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
non_negative_integer() { [[ "$1" =~ ^[0-9]+$ ]]; }
yes_or_no() { [[ "$1" == yes || "$1" == no ]]; }

for value in \
  "$repetitions" "$measurement_seconds" "$payload_bytes" "$keyspace" \
  "$maxlen" "$expiry_keys" "$expiry_ttl_min_ms" "$expiry_clients" \
  "$expiry_timeout_seconds"; do
  if ! positive_integer "$value"; then
    echo "expected a positive integer, got: $value" >&2
    exit 2
  fi
done
for value in "$warmup_seconds" "$expiry_ttl_spread_ms" "$seed"; do
  if ! non_negative_integer "$value"; then
    echo "expected a non-negative integer, got: $value" >&2
    exit 2
  fi
done
for value in "$plan_only" "$start_server" "$build_module"; do
  if ! yes_or_no "$value"; then
    echo "expected yes or no, got: $value" >&2
    exit 2
  fi
done
if [[ "$plan_only" == no && "$start_server" == no && -z "$server_module_path" ]]; then
  echo "SATURATION_SERVER_MODULE_PATH is required for a remote server" >&2
  exit 2
fi

valid_scenario() {
  case "$1" in
    s0 | s1 | s2 | expiry-s0 | expiry-s2 | custom-s0 | custom-s1 | custom-s2) return 0 ;;
    *) return 1 ;;
  esac
}

for scenario in $scenarios; do
  if ! valid_scenario "$scenario"; then
    echo "unknown scenario: $scenario" >&2
    exit 2
  fi
  if [[ "$scenario" == custom-* && -z "$commands_file" && -z "$monitor_input" ]]; then
    echo "$scenario requires SATURATION_COMMANDS_FILE or SATURATION_MONITOR_INPUT" >&2
    exit 2
  fi
done
for selection in $selectivities; do
  case "$selection" in 0 | 1 | 10 | 100) ;; *)
    echo "selectivity must be one of 0, 1, 10, or 100: $selection" >&2
    exit 2
  esac
done
for value in $client_levels $thread_levels $pipeline_levels; do
  if ! positive_integer "$value"; then
    echo "sweep values must be positive integers: $value" >&2
    exit 2
  fi
done
for value in $rate_limit_levels; do
  if ! non_negative_integer "$value"; then
    echo "rate limits must be non-negative integers: $value" >&2
    exit 2
  fi
done
if ! awk -v value="$p99_budget_ms" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/ && value > 0) }'; then
  echo "SATURATION_P99_BUDGET_MS must be a positive number" >&2
  exit 2
fi
if ! awk -v value="$achievement_ratio" \
  'BEGIN { exit !(value ~ /^0([.][0-9]+)?$|^1([.]0+)?$/ && value > 0 && value <= 1) }'; then
  echo "SATURATION_ACHIEVEMENT_RATIO must be greater than 0 and at most 1" >&2
  exit 2
fi
if [[ -n "$commands_file" ]]; then
  jq -e '
    type == "array" and length > 0 and
    all(.[];
      (.command | type == "string" and length > 0) and
      (.ratio | type == "number" and . > 0 and floor == .) and
      ((.key_pattern // "R") | IN("G", "R", "Z", "S", "P")) and
      ((.expected_events_per_command // 0) |
        type == "number" and . >= 0 and floor == .))
  ' "$commands_file" >/dev/null
fi
if [[ -n "$monitor_input" && ! -f "$monitor_input" ]]; then
  echo "monitor input does not exist: $monitor_input" >&2
  exit 2
fi
if [[ -n "$expected_events_override" ]] && ! non_negative_integer "$expected_events_override"; then
  echo "SATURATION_EXPECTED_EVENTS must be a non-negative integer" >&2
  exit 2
fi

mkdir -p "$results_dir/raw"
plan="$results_dir/plan.tsv"
unsorted_plan="$results_dir/raw/plan.unsorted.tsv"
: >"$unsorted_plan"

add_trial() {
  local scenario="$1"
  local selectivity="$2"
  local clients="$3"
  local threads="$4"
  local pipeline="$5"
  local rate_limit="$6"
  local repetition="$7"
  local order_key
  order_key="$(
    printf '%s' "$seed:$scenario:$selectivity:$clients:$threads:$pipeline:$rate_limit:$repetition" |
      cksum | awk '{ print $1 }'
  )"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$order_key" "$scenario" "$selectivity" "$clients" "$threads" \
    "$pipeline" "$rate_limit" "$repetition" >>"$unsorted_plan"
}

for clients in $client_levels; do
  for threads in $thread_levels; do
    for pipeline in $pipeline_levels; do
      for rate_limit in $rate_limit_levels; do
        for repetition in $(seq 1 "$repetitions"); do
          for scenario in $scenarios; do
            case "$scenario" in
              s2)
                for selectivity in $selectivities; do
                  add_trial "$scenario" "$selectivity" "$clients" "$threads" "$pipeline" "$rate_limit" "$repetition"
                done
                ;;
              custom-*)
                add_trial "$scenario" - "$clients" "$threads" "$pipeline" "$rate_limit" "$repetition"
                ;;
              expiry-*)
                # Expiry has a separate foreground client control and no SET
                # selectivity or rate-limit dimension; avoid multiplying
                # identical drains by ordinary-load sweep settings.
                if [[ "$clients" == "${client_levels%% *}" && \
                  "$threads" == "${thread_levels%% *}" && \
                  "$pipeline" == "${pipeline_levels%% *}" && \
                  "$rate_limit" == "${rate_limit_levels%% *}" ]]; then
                  add_trial "$scenario" - "$expiry_clients" 1 1 0 "$repetition"
                fi
                ;;
              *)
                add_trial "$scenario" 100 "$clients" "$threads" "$pipeline" "$rate_limit" "$repetition"
                ;;
            esac
          done
        done
      done
    done
  done
done
LC_ALL=C sort -n -k1,1 "$unsorted_plan" >"$plan"
planned_trials="$(wc -l <"$plan" | tr -d '[:space:]')"

if [[ "$plan_only" == yes ]]; then
  echo "order scenario selectivity-percent clients-per-thread threads pipeline rate-limit-per-connection repetition"
  cat "$plan"
  echo
  echo "trial plan: $plan"
  exit 0
fi

for tool in "$cli_bin" "$memtier_bin" jq awk sed grep date; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done
if [[ "$start_server" == yes ]]; then
  command -v "$server_bin" >/dev/null || {
    echo "missing server binary: $server_bin" >&2
    exit 1
  }
  if [[ "$build_module" == yes ]]; then
    command -v cargo >/dev/null || {
      echo "cargo is required when SATURATION_BUILD_MODULE=yes" >&2
      exit 1
    }
  fi
fi

ext=so
[[ "$(uname)" == Darwin ]] && ext=dylib
local_module="$repo_dir/target/release/libredis_event_stream_module.$ext"
if [[ -z "$server_module_path" ]]; then
  server_module_path="$local_module"
fi
if [[ "$start_server" == yes && "$build_module" == yes ]]; then
  echo "building release module..." >&2
  cargo build --release >/dev/null
fi
if [[ ! -f "$server_module_path" && "$start_server" == yes ]]; then
  echo "module artifact does not exist: $server_module_path" >&2
  exit 1
fi

server_pid=""
server_dir=""
if [[ "$start_server" == yes ]]; then
  server_dir="$(mktemp -d)"
  server_log="$server_dir/redis.log"
fi

cleanup() {
  local status="$?"
  trap - EXIT INT TERM
  if [[ -n "$server_pid" ]]; then
    "$cli_bin" -h "$host" -p "$port" shutdown nosave >/dev/null 2>&1 || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_dir" ]]; then
    rm -rf "$server_dir"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

cli=("$cli_bin")
cli_json=("$cli_bin" --json)
memtier_connection=()
if [[ -n "$uri" ]]; then
  cli+=(--uri "$uri")
  cli_json+=(--uri "$uri")
  memtier_connection+=(--uri "$uri")
else
  cli+=(-h "$host" -p "$port")
  cli_json+=(-h "$host" -p "$port")
  memtier_connection+=(-h "$host" -p "$port")
fi

if [[ "$start_server" == yes ]]; then
  "$server_bin" \
    --bind "$host" \
    --port "$port" \
    --dir "$server_dir" \
    --save '' \
    --appendonly no \
    --enable-module-command yes \
    --latency-tracking yes \
    --logfile "$server_log" &
  server_pid=$!
  ready=no
  for _ in $(seq 1 100); do
    if "${cli[@]}" PING 2>/dev/null | grep -q PONG; then
      ready=yes
      break
    fi
    sleep 0.05
  done
  if [[ "$ready" != yes ]]; then
    echo "server did not become ready" >&2
    cat "$server_log" >&2
    exit 1
  fi
fi

if ! "${cli[@]}" PING 2>/dev/null | grep -q PONG; then
  echo "target did not answer PING" >&2
  exit 1
fi

epoch_ms() {
  local ns
  ns="$(date +%s%N)"
  printf '%s\n' "${ns%??????}"
}

module_loaded() {
  "${cli[@]}" --raw MODULE LIST 2>/dev/null | grep -q '^eventstream$'
}

unload_module() {
  if module_loaded; then
    "${cli[@]}" MODULE UNLOAD eventstream >/dev/null
  fi
}

configure_scenario() {
  local scenario="$1"
  unload_module
  case "$scenario" in
    s0 | expiry-s0 | custom-s0) ;;
    s1 | custom-s1)
      "${cli[@]}" MODULE LOAD "$server_module_path" >/dev/null
      ;;
    s2)
      "${cli[@]}" MODULE LOAD "$server_module_path" events set maxlen "$maxlen" >/dev/null
      ;;
    custom-s2)
      "${cli[@]}" MODULE LOAD "$server_module_path" events "$capture_events" maxlen "$maxlen" >/dev/null
      ;;
    expiry-s2)
      "${cli[@]}" MODULE LOAD "$server_module_path" events expired maxlen "$maxlen" >/dev/null
      ;;
  esac
  "${cli[@]}" FLUSHALL >/dev/null
  "${cli[@]}" LATENCY RESET >/dev/null 2>&1 || true
}

module_stats_json() {
  if ! module_loaded; then
    echo null
    return
  fi
  "${cli_json[@]}" EVENTSTREAM.STATS |
    jq 'if type == "object" then .
        else . as $items |
          reduce range(0; length; 2) as $i
            ({}; .[$items[$i]] = $items[$i + 1])
        end'
}

collect_checkpoint() {
  local phase="$1"
  local trial_dir="$2"
  local checkpoint_dir="$trial_dir/checkpoints/$phase"
  local stream stream_memory stream_length
  mkdir -p "$checkpoint_dir"
  epoch_ms >"$checkpoint_dir/epoch-ms.txt"
  for section in server clients memory stats cpu persistence keyspace commandstats eventstream; do
    "${cli[@]}" --raw INFO "$section" >"$checkpoint_dir/info-$section.txt" 2>"$checkpoint_dir/info-$section.stderr" || true
  done
  "${cli_json[@]}" MODULE LIST >"$checkpoint_dir/module-list.json" 2>"$checkpoint_dir/module-list.stderr" || true
  "${cli_json[@]}" CONFIG GET '*' >"$checkpoint_dir/config.json" 2>"$checkpoint_dir/config.stderr" || true
  "${cli_json[@]}" LATENCY HISTOGRAM >"$checkpoint_dir/latency-histogram.json" 2>"$checkpoint_dir/latency-histogram.stderr" || true
  "${cli_json[@]}" LATENCY LATEST >"$checkpoint_dir/latency-latest.json" 2>"$checkpoint_dir/latency-latest.stderr" || true
  if module_loaded; then
    module_stats_json >"$checkpoint_dir/eventstream-stats.json"
    "${cli_json[@]}" EVENTSTREAM.STREAMS WITHSTATS >"$checkpoint_dir/eventstream-streams-withstats.json"
    "${cli_json[@]}" EVENTSTREAM.STREAMS VERBOSE >"$checkpoint_dir/eventstream-streams-verbose.json"
    : >"$checkpoint_dir/eventstream-stream-memory.tsv"
    while IFS= read -r stream; do
      stream_memory="$("${cli[@]}" --raw MEMORY USAGE "$stream" 2>/dev/null || true)"
      stream_length="$("${cli[@]}" --raw XLEN "$stream" 2>/dev/null || true)"
      printf '%s\t%s\t%s\n' "$stream" "${stream_length:-0}" "${stream_memory:-0}" \
        >>"$checkpoint_dir/eventstream-stream-memory.tsv"
    done < <(jq -r '.[] | .[0]' "$checkpoint_dir/eventstream-streams-withstats.json")
  else
    printf 'null\n' >"$checkpoint_dir/eventstream-stats.json"
    printf 'null\n' >"$checkpoint_dir/eventstream-streams-withstats.json"
    printf 'null\n' >"$checkpoint_dir/eventstream-streams-verbose.json"
    : >"$checkpoint_dir/eventstream-stream-memory.tsv"
  fi
  if [[ -n "$metrics_hook" ]]; then
    SATURATION_HOOK_PHASE="$phase" SATURATION_HOOK_DIR="$checkpoint_dir" \
      "$metrics_hook" >"$checkpoint_dir/metrics-hook.stdout" 2>"$checkpoint_dir/metrics-hook.stderr"
  fi
}

append_command_specs() {
  local selectivity="$1"
  local mode="$2"
  if [[ "$mode" == custom ]]; then
    if [[ -n "$commands_file" ]]; then
      while IFS=$'\t' read -r command ratio pattern; do
        memtier_args+=(--command "$command" --command-ratio "$ratio" --command-key-pattern "$pattern")
      done < <(jq -r '.[] | [.command, (.ratio | tostring), (.key_pattern // "R")] | @tsv' "$commands_file")
    else
      memtier_args+=(--monitor-input "$monitor_input" --monitor-pattern S --command '__monitor_line@__')
    fi
    return
  fi

  case "$selectivity" in
    0)
      memtier_args+=(--command 'HSET sat:hash:__key__ field __data__' --command-ratio 1 --command-key-pattern R)
      ;;
    1)
      memtier_args+=(
        --command 'SET sat:string:__key__ __data__' --command-ratio 1 --command-key-pattern R
        --command 'HSET sat:hash:__key__ field __data__' --command-ratio 99 --command-key-pattern R
      )
      ;;
    10)
      memtier_args+=(
        --command 'SET sat:string:__key__ __data__' --command-ratio 1 --command-key-pattern R
        --command 'HSET sat:hash:__key__ field __data__' --command-ratio 9 --command-key-pattern R
      )
      ;;
    100)
      memtier_args+=(--command 'SET sat:string:__key__ __data__' --command-ratio 1 --command-key-pattern R)
      ;;
  esac
}

build_memtier_args() {
  local trial_dir="$1"
  local duration="$2"
  local clients="$3"
  local threads="$4"
  local pipeline="$5"
  local selectivity="$6"
  local mode="$7"
  local prefix="$8"
  local rate_limit="$9"
  memtier_args=(
    "$memtier_bin" "${memtier_connection[@]}"
    --protocol redis
    --run-count 1
    --test-time "$duration"
    --clients "$clients"
    --threads "$threads"
    --pipeline "$pipeline"
    --data-size "$payload_bytes"
    --key-minimum 1
    --key-maximum "$keyspace"
    --key-prefix "$prefix"
    --command-stats-breakdown line
    --print-percentiles '50,95,99,99.9,100'
    --json-out-file "$trial_dir/memtier.json"
    --out-file "$trial_dir/memtier.txt"
    --hdr-file-prefix "$trial_dir/memtier-hdr"
    --show-config
  )
  if ((rate_limit > 0)); then
    memtier_args+=(--rate-limiting "$rate_limit")
  fi
  append_command_specs "$selectivity" "$mode"
}

command_error_count() {
  local stderr_file="$1"
  grep -c 'handle error response:' "$stderr_file" 2>/dev/null || true
}

connection_error_count() {
  local json_file="$1"
  jq '(."ALL STATS".Totals."Connection Errors" // 0)' "$json_file"
}

expected_set_count() {
  local json_file="$1"
  jq '[."ALL STATS" | to_entries[] |
        select(.key != "Totals") |
        select((.key | ascii_downcase) == "sets" or
               (.key | ascii_downcase | startswith("set "))) |
        (.value.Count // 0)] | add // 0' "$json_file"
}

expected_custom_count() {
  local json_file="$1"
  if [[ -n "$expected_events_override" ]]; then
    echo "$expected_events_override"
    return
  fi
  if [[ -z "$commands_file" ]]; then
    echo "SATURATION_EXPECTED_EVENTS is required for monitor replay" >&2
    return 1
  fi
  jq --slurpfile specs "$commands_file" '
    ."ALL STATS" as $stats |
    [$specs[0][] |
      ((.command | split(" ")[0] | ascii_downcase)) as $command |
      (.expected_events_per_command // 0) as $factor |
      if $factor == 0 then 0
      else
        ([$stats | to_entries[] |
          select(.key != "Totals") |
          select((.value | type) == "object" and (.value.Count | type) == "number") |
          select(
            ((.key | ascii_downcase) == $command) or
            ((.key | ascii_downcase) == ($command + "s"))) |
          .value.Count] | add // 0) * $factor
      end] | add // 0
  ' "$json_file"
}

run_memtier() {
  local trial_dir="$1"
  rm -f "$trial_dir/memtier.json"
  printf '%s\0' "${memtier_args[@]}" |
    jq -Rs '
      split("\u0000")[:-1] |
      map(if test("^rediss?://[^/@]+@") then
            sub("^(?<scheme>rediss?://)[^/@]+@"; "\(.scheme)***@")
          else . end)
    ' >"$trial_dir/memtier-command.json"
  set +e
  "${memtier_args[@]}" >"$trial_dir/memtier.stdout" 2>"$trial_dir/memtier.stderr"
  local status="$?"
  set -e
  if [[ "$status" -ne 0 || ! -s "$trial_dir/memtier.json" ]]; then
    echo "memtier failed (status $status): $trial_dir" >&2
    cat "$trial_dir/memtier.stderr" >&2
    return 1
  fi
  local command_errors connection_errors
  command_errors="$(command_error_count "$trial_dir/memtier.stderr")"
  connection_errors="$(connection_error_count "$trial_dir/memtier.json")"
  if [[ "$command_errors" -ne 0 || "$connection_errors" -ne 0 ]]; then
    echo "memtier reported command=$command_errors connection=$connection_errors errors: $trial_dir" >&2
    return 1
  fi
}

run_hook() {
  local action="$1"
  local trial_id="$2"
  local trial_dir="$3"
  if [[ -n "$profiler_hook" ]]; then
    SATURATION_HOOK_ACTION="$action" SATURATION_TRIAL_ID="$trial_id" \
      SATURATION_HOOK_DIR="$trial_dir" "$profiler_hook" \
      >"$trial_dir/profiler-$action.stdout" 2>"$trial_dir/profiler-$action.stderr"
  fi
}

normalize_trial() {
  local trial_id="$1"
  local scenario="$2"
  local selectivity="$3"
  local clients="$4"
  local threads="$5"
  local pipeline="$6"
  local rate_limit="$7"
  local repetition="$8"
  local trial_dir="$9"
  local expiry_json="${10:-null}"
  local observed_duration_seconds="${11:-$measurement_seconds}"
  local command_errors connection_errors expected module_stats module_loaded_json
  local before_main after_main before_total after_total main_cpu total_cpu
  local used_memory used_memory_rss used_memory_peak
  local retained_stream_entries stream_memory_bytes
  local load_generator_metrics
  command_errors="$(command_error_count "$trial_dir/memtier.stderr")"
  connection_errors="$(connection_error_count "$trial_dir/memtier.json")"
  if [[ "$scenario" == custom-* ]]; then
    expected="$(expected_custom_count "$trial_dir/memtier.json")"
  else
    expected="$(expected_set_count "$trial_dir/memtier.json")"
  fi
  if [[ "$scenario" == expiry-* ]]; then
    expected="$expiry_keys"
  fi
  module_stats="$(
    jq -n \
      --slurpfile before "$trial_dir/checkpoints/pre/eventstream-stats.json" \
      --slurpfile after "$trial_dir/checkpoints/post/eventstream-stats.json" '
        if $after[0] == null then null
        else
          reduce ($after[0] | keys_unsorted[]) as $key
            ({};
              .[$key] =
                (if ($key | IN("enabled", "eviction_risk", "async_queue_depth",
                               "cluster_per_node", "cluster_pinned_tag",
                               "last_error_time")) then
                   $after[0][$key]
                 elif ($after[0][$key] | type) == "number" then
                   $after[0][$key] - ($before[0][$key] // 0)
                 else $after[0][$key]
                 end)) +
          {totals_before: $before[0], totals_after: $after[0]}
        end
      '
  )"
  if [[ "$module_stats" == null ]]; then module_loaded_json=false; else module_loaded_json=true; fi

  if grep -q '^used_cpu_sys_main_thread:' "$trial_dir/checkpoints/pre/info-cpu.txt" &&
    grep -q '^used_cpu_sys_main_thread:' "$trial_dir/checkpoints/post/info-cpu.txt"; then
    before_main="$(
      awk -F: '
        $1 == "used_cpu_sys_main_thread" { sys=$2 }
        $1 == "used_cpu_user_main_thread" { user=$2 }
        END { printf "%.6f", sys + user }
      ' "$trial_dir/checkpoints/pre/info-cpu.txt"
    )"
    after_main="$(
      awk -F: '
        $1 == "used_cpu_sys_main_thread" { sys=$2 }
        $1 == "used_cpu_user_main_thread" { user=$2 }
        END { printf "%.6f", sys + user }
      ' "$trial_dir/checkpoints/post/info-cpu.txt"
    )"
    main_cpu="$(awk -v before="$before_main" -v after="$after_main" 'BEGIN { printf "%.6f", after - before }')"
  else
    main_cpu=null
  fi
  before_total="$(
    awk -F: '
      $1 == "used_cpu_sys" { sys=$2 }
      $1 == "used_cpu_user" { user=$2 }
      END { printf "%.6f", sys + user }
    ' "$trial_dir/checkpoints/pre/info-cpu.txt"
  )"
  after_total="$(
    awk -F: '
      $1 == "used_cpu_sys" { sys=$2 }
      $1 == "used_cpu_user" { user=$2 }
      END { printf "%.6f", sys + user }
    ' "$trial_dir/checkpoints/post/info-cpu.txt"
  )"
  total_cpu="$(awk -v before="$before_total" -v after="$after_total" 'BEGIN { printf "%.6f", after - before }')"
  used_memory="$(awk -F: '$1 == "used_memory" { gsub(/\r/, "", $2); print $2; exit }' "$trial_dir/checkpoints/post/info-memory.txt")"
  used_memory_rss="$(awk -F: '$1 == "used_memory_rss" { gsub(/\r/, "", $2); print $2; exit }' "$trial_dir/checkpoints/post/info-memory.txt")"
  used_memory_peak="$(awk -F: '$1 == "used_memory_peak" { gsub(/\r/, "", $2); print $2; exit }' "$trial_dir/checkpoints/post/info-memory.txt")"
  retained_stream_entries="$(
    awk -F'\t' '{ total += $2 } END { print total + 0 }' \
      "$trial_dir/checkpoints/post/eventstream-stream-memory.tsv"
  )"
  stream_memory_bytes="$(
    awk -F'\t' '{ total += $3 } END { print total + 0 }' \
      "$trial_dir/checkpoints/post/eventstream-stream-memory.tsv"
  )"
  load_generator_metrics=null
  if [[ -s "$trial_dir/checkpoints/pre/metrics-hook.stdout" &&
    -s "$trial_dir/checkpoints/post/metrics-hook.stdout" ]] &&
    jq -e '.schema == "linux-proc-stat-v1"' \
      "$trial_dir/checkpoints/pre/metrics-hook.stdout" >/dev/null 2>&1 &&
    jq -e '.schema == "linux-proc-stat-v1"' \
      "$trial_dir/checkpoints/post/metrics-hook.stdout" >/dev/null 2>&1; then
    load_generator_metrics="$(
      jq -n \
        --slurpfile before "$trial_dir/checkpoints/pre/metrics-hook.stdout" \
        --slurpfile after "$trial_dir/checkpoints/post/metrics-hook.stdout" '
          def total_ticks:
            .cpu_ticks | .user + .nice + .system + .idle + .iowait + .irq + .softirq + .steal;
          def idle_ticks: .cpu_ticks | .idle + .iowait;
          ($before[0] | total_ticks) as $before_total |
          ($after[0] | total_ticks) as $after_total |
          ($before[0] | idle_ticks) as $before_idle |
          ($after[0] | idle_ticks) as $after_idle |
          ($after_total - $before_total) as $total_delta |
          ($after_idle - $before_idle) as $idle_delta |
          (if $total_delta > 0 then
             (($total_delta - $idle_delta) / $total_delta) * 100
           else 0 end) as $host_cpu_percent |
          {
            schema: "linux-proc-stat-v1",
            logical_cpus: $after[0].logical_cpus,
            observed_duration_seconds:
              (($after[0].epoch_ms - $before[0].epoch_ms) / 1000),
            host_cpu_percent: $host_cpu_percent,
            core_percent: $host_cpu_percent * $after[0].logical_cpus,
            headroom_percent: 100 - $host_cpu_percent,
            total_tick_delta: $total_delta
          }
        '
    )"
  fi

  jq -n \
    --argjson schema_version "$schema_version" \
    --arg trial_id "$trial_id" \
    --arg scenario "$scenario" \
    --arg selectivity "$selectivity" \
    --argjson clients "$clients" \
    --argjson threads "$threads" \
    --argjson pipeline "$pipeline" \
    --argjson rate_limit "$rate_limit" \
    --argjson repetition "$repetition" \
    --argjson warmup_seconds "$warmup_seconds" \
    --argjson measurement_seconds "$measurement_seconds" \
    --argjson expected "$expected" \
    --arg workload_name "$workload_name" \
    --argjson observed_duration_seconds "$observed_duration_seconds" \
    --argjson main_cpu_seconds "$main_cpu" \
    --argjson total_cpu_seconds "$total_cpu" \
    --argjson used_memory_bytes "${used_memory:-0}" \
    --argjson used_memory_rss_bytes "${used_memory_rss:-0}" \
    --argjson used_memory_peak_bytes "${used_memory_peak:-0}" \
    --argjson retained_stream_entries "$retained_stream_entries" \
    --argjson stream_memory_bytes "$stream_memory_bytes" \
    --argjson load_generator_metrics "$load_generator_metrics" \
    --argjson command_errors "$command_errors" \
    --argjson connection_errors "$connection_errors" \
    --argjson module_loaded "$module_loaded_json" \
    --argjson module_stats "$module_stats" \
    --argjson expiry "$expiry_json" \
    --slurpfile command "$trial_dir/memtier-command.json" \
    --slurpfile memtier "$trial_dir/memtier.json" \
    '
      ($memtier[0]."ALL STATS".Totals) as $totals |
      ($totals."Percentile Latencies") as $latency |
      {
        schema_version: $schema_version,
        trial_id: $trial_id,
        scenario: $scenario,
        selectivity_percent: (if $selectivity == "-" then null else ($selectivity | tonumber) end),
        repetition: $repetition,
        workload: {
          name: $workload_name,
          clients_per_thread: $clients,
          threads: $threads,
          pipeline: $pipeline,
          rate_limit_per_connection: (if $rate_limit == 0 then null else $rate_limit end),
          target_ops_per_sec:
            (if $rate_limit == 0 then null else $rate_limit * $clients * $threads end),
          warmup_seconds: $warmup_seconds,
          measurement_seconds: $measurement_seconds,
          command: $command[0],
          operations: ($totals.Count // 0),
          expected_selected_events: $expected,
          observed_selectivity_percent:
            (if ($totals.Count // 0) > 0 then ($expected / $totals.Count) * 100
             else null end)
        },
        result: {
          ops_per_sec: ($totals."Ops/sec" // 0),
          target_achievement_ratio:
            (if $rate_limit == 0 then null
             else ($totals."Ops/sec" // 0) / ($rate_limit * $clients * $threads)
             end),
          errors: {connection: $connection_errors, command: $command_errors},
          latency_ms: {
            p50: ($latency."p50.00" // null),
            p95: ($latency."p95.00" // null),
            p99: ($latency."p99.00" // null),
            p99_9: ($latency."p99.90" // null),
            max: ($latency."p100.0" // $totals.Max // null)
          }
        },
        server: {
          observed_duration_seconds: $observed_duration_seconds,
          main_thread_cpu_seconds: $main_cpu_seconds,
          main_thread_core_percent:
            (if $main_cpu_seconds != null and $observed_duration_seconds > 0 then
               ($main_cpu_seconds / $observed_duration_seconds) * 100
             else null end),
          total_cpu_seconds: $total_cpu_seconds,
          total_core_percent:
            (if $observed_duration_seconds > 0 then
               ($total_cpu_seconds / $observed_duration_seconds) * 100
             else 0 end),
          cpu_us_per_operation:
            (if ($totals.Count // 0) > 0 then
               ($total_cpu_seconds * 1000000) / $totals.Count
             else null end),
          used_memory_bytes: $used_memory_bytes,
          used_memory_rss_bytes: $used_memory_rss_bytes,
          used_memory_peak_bytes: $used_memory_peak_bytes,
          retained_stream_entries: $retained_stream_entries,
          stream_memory_bytes: $stream_memory_bytes,
          memory_bytes_per_retained_entry:
            (if $retained_stream_entries > 0 then
               $stream_memory_bytes / $retained_stream_entries
             else null end)
        },
        load_generator: $load_generator_metrics,
        module: (
          if $module_loaded then
            $module_stats + {loaded: true}
          else
            {loaded: false}
          end
        ),
        expiry: $expiry
      }
    ' >"$trial_dir/trial.json"
}

reconcile_trial() {
  local trial_file="$1"
  jq -e '
    (.result.errors.connection == 0) and
    (.result.errors.command == 0) and
    (if .scenario == "s0" or .scenario == "custom-s0" or .scenario == "expiry-s0" then
      .module.loaded == false
    elif .scenario == "s1" or .scenario == "custom-s1" then
      (.module.loaded == true) and (.module.forwarded == 0) and
      (.module.events_lost == 0) and (.module.dropped == 0) and (.module.handler_panics == 0)
    else
      (.module.loaded == true) and
      (.module.forwarded == .workload.expected_selected_events) and
      (.module.events_lost == 0) and (.module.dropped == 0) and
      (.module.handler_panics == 0) and (.module.async_worker_errors == 0)
    end) and
    (if (.scenario | startswith("expiry-")) then
      .expiry.overlap_proven == true and .expiry.drained == true
    else true end)
  ' "$trial_file" >/dev/null
}

run_normal_trial() {
  local trial_id="$1"
  local scenario="$2"
  local selectivity="$3"
  local clients="$4"
  local threads="$5"
  local pipeline="$6"
  local rate_limit="$7"
  local repetition="$8"
  local trial_dir="$results_dir/raw/$trial_id"
  local mode=builtin
  [[ "$scenario" == custom-* ]] && mode=custom
  mkdir -p "$trial_dir"

  configure_scenario "$scenario"
  if ((warmup_seconds > 0)); then
    mkdir -p "$trial_dir/warmup"
    build_memtier_args "$trial_dir/warmup" "$warmup_seconds" "$clients" "$threads" "$pipeline" "$selectivity" "$mode" "warm:$trial_id:" "$rate_limit"
    run_memtier "$trial_dir/warmup"
  fi

  # Reload after warmup because the module deliberately has no mutable counter
  # reset command. A reload makes measured counters exact without changing the
  # measured configuration.
  configure_scenario "$scenario"
  collect_checkpoint pre "$trial_dir"
  run_hook start "$trial_id" "$trial_dir"
  build_memtier_args "$trial_dir" "$measurement_seconds" "$clients" "$threads" "$pipeline" "$selectivity" "$mode" "measure:$trial_id:" "$rate_limit"
  run_memtier "$trial_dir"
  run_hook stop "$trial_id" "$trial_dir"
  collect_checkpoint post "$trial_dir"
  normalize_trial "$trial_id" "$scenario" "$selectivity" "$clients" "$threads" "$pipeline" "$rate_limit" "$repetition" "$trial_dir"
  reconcile_trial "$trial_dir/trial.json"
}

expires_left() {
  "${cli[@]}" --raw INFO keyspace |
    awk -F'[=,]' '/^db0:/ { print $4; found=1 } END { if (!found) print 0 }' |
    tr -d '\r'
}

preload_expiring() {
  awk -v n="$expiry_keys" -v min="$expiry_ttl_min_ms" -v spread="$expiry_ttl_spread_ms" -v seed="$seed" '
    BEGIN {
      srand(seed)
      for (i = 0; i < n; i++) {
        ttl = min + (spread > 0 ? int(rand() * spread) : 0)
        printf "SET expiry:%d value PX %d\r\n", i, ttl
      }
    }
  ' | "${cli[@]}" --pipe >/dev/null
}

run_expiry_trial() {
  local trial_id="$1"
  local scenario="$2"
  local clients="$3"
  local repetition="$4"
  local trial_dir="$results_dir/raw/$trial_id"
  local started_ms finished_ms drain_start_ms=0 drain_end_ms=0
  local observed_duration_seconds
  local initial left deadline memtier_pid memtier_status overlap=false drained=false
  mkdir -p "$trial_dir"
  configure_scenario "$scenario"
  preload_expiring
  initial="$(expires_left)"
  if [[ "$initial" -ne "$expiry_keys" ]]; then
    echo "expiry preload already drained ($initial/$expiry_keys); increase SATURATION_EXPIRY_TTL_MIN_MS" >&2
    return 1
  fi
  collect_checkpoint pre "$trial_dir"
  run_hook start "$trial_id" "$trial_dir"
  memtier_args=(
    "$memtier_bin" "${memtier_connection[@]}"
    --protocol redis --run-count 1 --test-time "$expiry_timeout_seconds"
    --clients "$clients" --threads 1 --pipeline 1 --ratio 0:1
    --data-size "$payload_bytes" --key-minimum 1 --key-maximum "$keyspace"
    --key-prefix "foreground:$trial_id:" --key-pattern R:R
    --print-percentiles '50,95,99,99.9,100'
    --json-out-file "$trial_dir/memtier.json"
    --out-file "$trial_dir/memtier.txt"
    --hdr-file-prefix "$trial_dir/memtier-hdr"
    --show-config
  )
  printf '%s\0' "${memtier_args[@]}" |
    jq -Rs '
      split("\u0000")[:-1] |
      map(if test("^rediss?://[^/@]+@") then
            sub("^(?<scheme>rediss?://)[^/@]+@"; "\(.scheme)***@")
          else . end)
    ' >"$trial_dir/memtier-command.json"
  started_ms="$(epoch_ms)"
  "${memtier_args[@]}" >"$trial_dir/memtier.stdout" 2>"$trial_dir/memtier.stderr" &
  memtier_pid=$!
  deadline=$(( $(date +%s) + expiry_timeout_seconds ))
  while kill -0 "$memtier_pid" 2>/dev/null; do
    left="$(expires_left)"
    now_ms="$(epoch_ms)"
    printf '%s\t%s\n' "$now_ms" "$left" >>"$trial_dir/expiry-drain.tsv"
    if [[ "$drain_start_ms" -eq 0 && "$left" -lt "$initial" ]]; then
      drain_start_ms="$now_ms"
    fi
    if [[ "$left" -eq 0 ]]; then
      drain_end_ms="$now_ms"
      drained=true
      kill -INT "$memtier_pid" 2>/dev/null || true
      break
    fi
    if (( $(date +%s) >= deadline )); then
      break
    fi
    sleep "$expiry_poll_seconds"
  done
  set +e
  wait "$memtier_pid"
  memtier_status="$?"
  set -e
  finished_ms="$(epoch_ms)"
  run_hook stop "$trial_id" "$trial_dir"
  if [[ "$drained" != true ]]; then
    echo "expiry backlog did not drain inside ${expiry_timeout_seconds}s" >&2
    return 1
  fi
  if [[ "$memtier_status" -ne 0 || ! -s "$trial_dir/memtier.json" ]]; then
    echo "expiry foreground failed (status $memtier_status)" >&2
    return 1
  fi
  if [[ "$(command_error_count "$trial_dir/memtier.stderr")" -ne 0 || \
    "$(connection_error_count "$trial_dir/memtier.json")" -ne 0 ]]; then
    echo "expiry foreground reported errors" >&2
    return 1
  fi
  if [[ "$started_ms" -le "$drain_start_ms" && "$finished_ms" -ge "$drain_end_ms" ]]; then
    overlap=true
  fi
  collect_checkpoint post "$trial_dir"
  expiry_json="$(
    jq -n \
      --argjson keys "$expiry_keys" \
      --argjson initial_keys "$initial" \
      --argjson foreground_started_ms "$started_ms" \
      --argjson drain_started_ms "$drain_start_ms" \
      --argjson drain_completed_ms "$drain_end_ms" \
      --argjson foreground_finished_ms "$finished_ms" \
      --argjson overlap_proven "$overlap" \
      --argjson drained "$drained" \
      '{keys: $keys, initial_keys: $initial_keys, drained: $drained,
        foreground_started_ms: $foreground_started_ms,
        drain_started_ms: $drain_started_ms,
        drain_completed_ms: $drain_completed_ms,
        foreground_finished_ms: $foreground_finished_ms,
        overlap_proven: $overlap_proven,
        drain_seconds: (($drain_completed_ms - $drain_started_ms) / 1000)}'
  )"
  observed_duration_seconds="$(
    awk -v started="$started_ms" -v finished="$finished_ms" \
      'BEGIN { printf "%.6f", (finished - started) / 1000 }'
  )"
  normalize_trial "$trial_id" "$scenario" - "$clients" 1 1 0 "$repetition" \
    "$trial_dir" "$expiry_json" "$observed_duration_seconds"
  reconcile_trial "$trial_dir/trial.json"
}

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
git_commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
module_sha=null
if [[ -f "$local_module" ]]; then
  module_sha="$(shasum -a 256 "$local_module" | awk '{ print $1 }')"
fi
server_version="$("${cli[@]}" --raw INFO server | awk -F: '$1 == "redis_version" { gsub(/\r/, "", $2); print $2; exit }')"
memtier_version="$("$memtier_bin" --version 2>&1 | sed -n '1p')"
jq -n \
  --argjson schema_version "$schema_version" \
  --arg run_id "$run_id" \
  --arg started_at "$started_at" \
  --arg git_commit "$git_commit" \
  --arg module_path "$server_module_path" \
  --arg module_sha256 "$module_sha" \
  --arg memtier_version "$memtier_version" \
  --arg server_version "$server_version" \
  --arg host "$host" \
  --argjson port "$port" \
  --argjson repetitions "$repetitions" \
  --argjson warmup_seconds "$warmup_seconds" \
  --argjson measurement_seconds "$measurement_seconds" \
  --arg scenarios "$scenarios" \
  --arg selectivities "$selectivities" \
  --arg clients "$client_levels" \
  --arg threads "$thread_levels" \
  --arg pipelines "$pipeline_levels" \
  --arg rate_limits "$rate_limit_levels" \
  --arg workload_name "$workload_name" \
  --argjson p99_budget_ms "$p99_budget_ms" \
  --argjson achievement_ratio "$achievement_ratio" \
  --arg capture_events "$capture_events" \
  --arg commands_file "$commands_file" \
  --arg monitor_input "$monitor_input" \
  '{schema_version: $schema_version, run_id: $run_id, started_at: $started_at,
    source: {git_commit: $git_commit, module_path: $module_path,
      module_sha256: (if $module_sha256 == "null" then null else $module_sha256 end)},
    target: {host: $host, port: $port, server_version: $server_version},
    generator: {name: "memtier_benchmark", version: $memtier_version},
    plan: {scenarios: ($scenarios | split(" ") | map(select(length > 0))),
      selectivity_percent: ($selectivities | split(" ") | map(tonumber)),
      clients_per_thread: ($clients | split(" ") | map(tonumber)),
      threads: ($threads | split(" ") | map(tonumber)),
      pipelines: ($pipelines | split(" ") | map(tonumber)),
      rate_limit_per_connection: ($rate_limits | split(" ") | map(tonumber)),
      repetitions: $repetitions, warmup_seconds: $warmup_seconds,
      measurement_seconds: $measurement_seconds,
      knee_criterion: {p99_budget_ms: $p99_budget_ms,
        minimum_target_achievement_ratio: $achievement_ratio},
      custom: {name: $workload_name, capture_events: $capture_events,
        commands_file: (if $commands_file == "" then null else $commands_file end),
        monitor_input: (if $monitor_input == "" then null else $monitor_input end)}}}' >"$results_dir/manifest.json"

trial_files=""
trial_number=0
while IFS=$'\t' read -r _ scenario selectivity clients threads pipeline rate_limit repetition; do
  trial_number=$((trial_number + 1))
  selection_id="$selectivity"
  [[ "$selection_id" == - ]] && selection_id=na
  trial_id="${scenario}-sel${selection_id}-c${clients}-t${threads}-p${pipeline}-rl${rate_limit}-r${repetition}"
  echo "[$trial_number] $trial_id" >&2
  if [[ "$scenario" == expiry-* ]]; then
    run_expiry_trial "$trial_id" "$scenario" "$clients" "$repetition"
  else
    run_normal_trial "$trial_id" "$scenario" "$selectivity" "$clients" "$threads" "$pipeline" "$rate_limit" "$repetition"
  fi
  trial_files="$trial_files $results_dir/raw/$trial_id/trial.json"
done <"$plan"

if [[ "$trial_number" -ne "$planned_trials" ]]; then
  echo "executed $trial_number trials but the plan contains $planned_trials" >&2
  exit 1
fi

# shellcheck disable=SC2086
jq -s '.' $trial_files >"$results_dir/trials.json"
jq '
  def distribution:
    map(select(. != null)) | sort |
    if length == 0 then {min: null, median: null, max: null}
    else . as $v |
      {min: $v[0],
       median:
         (if ($v | length) % 2 == 1 then
            $v[(($v | length) / 2 | floor)]
          else
            (($v[(($v | length) / 2) - 1] + $v[($v | length) / 2]) / 2)
          end),
       max: $v[-1]}
    end;
  sort_by([.scenario, (.selectivity_percent // -1), .workload.clients_per_thread,
    .workload.threads, .workload.pipeline, (.workload.rate_limit_per_connection // 0)]) |
  group_by([.scenario, (.selectivity_percent // -1), .workload.clients_per_thread,
    .workload.threads, .workload.pipeline, (.workload.rate_limit_per_connection // 0)]) |
  map(. as $trials | {
    scenario: $trials[0].scenario,
    selectivity_percent: $trials[0].selectivity_percent,
    clients_per_thread: $trials[0].workload.clients_per_thread,
    threads: $trials[0].workload.threads,
    pipeline: $trials[0].workload.pipeline,
    rate_limit_per_connection: $trials[0].workload.rate_limit_per_connection,
    target_ops_per_sec: $trials[0].workload.target_ops_per_sec,
    repetitions: ($trials | length),
    ops_per_sec: ($trials | map(.result.ops_per_sec) | distribution),
    target_achievement_ratio:
      ($trials | map(.result.target_achievement_ratio) | distribution),
    p50_ms: ($trials | map(.result.latency_ms.p50) | distribution),
    p95_ms: ($trials | map(.result.latency_ms.p95) | distribution),
    p99_ms: ($trials | map(.result.latency_ms.p99) | distribution),
    p99_9_ms: ($trials | map(.result.latency_ms.p99_9) | distribution),
    max_ms: ($trials | map(.result.latency_ms.max) | distribution),
    main_thread_core_percent:
      ($trials | map(.server.main_thread_core_percent) | distribution),
    total_core_percent:
      ($trials | map(.server.total_core_percent) | distribution),
    cpu_us_per_operation:
      ($trials | map(.server.cpu_us_per_operation) | distribution),
    used_memory_bytes:
      ($trials | map(.server.used_memory_bytes) | distribution),
    used_memory_rss_bytes:
      ($trials | map(.server.used_memory_rss_bytes) | distribution),
    retained_stream_entries:
      ($trials | map(.server.retained_stream_entries) | distribution),
    stream_memory_bytes:
      ($trials | map(.server.stream_memory_bytes) | distribution),
    memory_bytes_per_retained_entry:
      ($trials | map(.server.memory_bytes_per_retained_entry) | distribution),
    load_generator_host_cpu_percent:
      ($trials | map(.load_generator.host_cpu_percent) | distribution),
    load_generator_core_percent:
      ($trials | map(.load_generator.core_percent) | distribution),
    load_generator_headroom_percent:
      ($trials | map(.load_generator.headroom_percent) | distribution),
    expiry_drain_seconds: ($trials | map(.expiry.drain_seconds) | distribution)
  })
' "$results_dir/trials.json" >"$results_dir/summary.json"

jq \
  --argjson p99_budget_ms "$p99_budget_ms" \
  --argjson achievement_ratio "$achievement_ratio" '
    def classified:
      . + {
        healthy:
          ((.target_achievement_ratio.median // 0) >= $achievement_ratio and
           .p99_ms.median != null and .p99_ms.median <= $p99_budget_ms)
      };
    [ .[] | select(.target_ops_per_sec != null) ] |
    sort_by([.scenario, (.selectivity_percent // -1), .clients_per_thread,
      .threads, .pipeline, .target_ops_per_sec]) |
    group_by([.scenario, (.selectivity_percent // -1), .clients_per_thread,
      .threads, .pipeline]) |
    map(
      map(classified) as $levels |
      ([$levels[] | select(.healthy)] |
        if length == 0 then null else max_by(.target_ops_per_sec) end) as $at |
      {
        scenario: $levels[0].scenario,
        selectivity_percent: $levels[0].selectivity_percent,
        clients_per_thread: $levels[0].clients_per_thread,
        threads: $levels[0].threads,
        pipeline: $levels[0].pipeline,
        criterion: {
          p99_budget_ms: $p99_budget_ms,
          minimum_target_achievement_ratio: $achievement_ratio
        },
        status:
          (if $at == null then "no-healthy-point"
           elif ([$levels[] | select(.target_ops_per_sec > $at.target_ops_per_sec)] | length) == 0
             then "ceiling-not-reached"
           else "bracketed"
           end),
        below:
          (if $at == null then null
           else ([$levels[] | select(.target_ops_per_sec < $at.target_ops_per_sec)] |
             if length == 0 then null else max_by(.target_ops_per_sec) end)
           end),
        at: $at,
        above:
          (if $at == null then ($levels | min_by(.target_ops_per_sec))
           else ([$levels[] | select(.target_ops_per_sec > $at.target_ops_per_sec)] |
             if length == 0 then null else min_by(.target_ops_per_sec) end)
           end),
        all_levels: $levels
      }
    )
  ' "$results_dir/summary.json" >"$results_dir/knee.json"

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --argjson schema_version "$schema_version" \
  --arg run_id "$run_id" \
  --arg completed_at "$completed_at" \
  --slurpfile trials "$results_dir/trials.json" \
  '{schema_version: $schema_version, run_id: $run_id, completed_at: $completed_at,
    passed: true, trial_count: ($trials[0] | length)}' >"$results_dir/result.json"

if [[ -n "$server_log" && -f "$server_log" ]]; then
  cp "$server_log" "$results_dir/raw/server.log"
fi
echo "results: $results_dir"
