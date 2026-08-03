#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$root_dir/../.." && pwd)"

requests="${BENCH_REQUESTS:-1000000}"
clients="${BENCH_CLIENTS:-50}"
client_levels="${BENCH_CLIENT_LEVELS:-$clients}"
threads="${BENCH_THREADS:-2}"
payload="${BENCH_PAYLOAD:-64}"
keyspace="${BENCH_KEYSPACE:-100000}"
maxlen="${BENCH_MAXLEN:-10000}"
repetitions="${BENCH_REPETITIONS:-1}"
baseline_repetitions="${BENCH_BASELINE_REPETITIONS:-$repetitions}"
filtered_repetitions="${BENCH_FILTERED_REPETITIONS:-$repetitions}"
capture_scenarios="${BENCH_CAPTURE_SCENARIOS:-s2}"
order_seed="${BENCH_ORDER_SEED:-260}"
profile_scenarios="${BENCH_PROFILE_SCENARIO:-}"
profile_frequency="${BENCH_PROFILE_FREQUENCY:-99}"
async_queue_capacity="${BENCH_ASYNC_QUEUE_CAPACITY:-65536}"
async_batch_size="${BENCH_ASYNC_BATCH_SIZE:-64}"
async_max_wait_ms="${BENCH_ASYNC_MAX_WAIT_MS:-1}"
async_configs="${BENCH_ASYNC_CONFIGS:-}"
module_source_commit="${BENCH_MODULE_SOURCE_COMMIT:-HEAD}"
module_source_repo="${BENCH_MODULE_SOURCE_REPO:-https://github.com/joshrotenberg/redis-event-stream-module}"
environment_network_samples="${BENCH_ENVIRONMENT_NETWORK_SAMPLES:-10000}"
plan_only="${BENCH_PLAN_ONLY:-no}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
results_dir="${RESULTS_DIR:-$root_dir/results/$run_id}"

required_tools=(cksum git jq sort)
if [[ "$plan_only" == "no" ]]; then
  required_tools+=(aws base64 gzip shasum terraform)
elif [[ "$plan_only" != "yes" ]]; then
  echo "BENCH_PLAN_ONLY must be yes or no" >&2
  exit 2
fi

for tool in "${required_tools[@]}"; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

non_negative_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

for value in \
  "$requests" "$threads" "$payload" "$keyspace" "$maxlen" \
  "$repetitions" "$async_queue_capacity" \
  "$async_batch_size" "$async_max_wait_ms" "$environment_network_samples"; do
  if ! positive_integer "$value"; then
    echo "benchmark values must be positive integers: $value" >&2
    exit 2
  fi
done

for value in "$baseline_repetitions" "$filtered_repetitions"; do
  if ! non_negative_integer "$value"; then
    echo "baseline and filtered repetitions must be non-negative integers: $value" >&2
    exit 2
  fi
done

valid_scenario() {
  case "$1" in
    s0 | s1 | s2 | s2-sync | s2-individual | s2-envelope) return 0 ;;
    *) return 1 ;;
  esac
}

for capture_scenario in $capture_scenarios; do
  if ! valid_scenario "$capture_scenario" || [[ "$capture_scenario" != s2* ]]; then
    echo "BENCH_CAPTURE_SCENARIOS must contain only s2 capture variants" >&2
    exit 2
  fi
done

seen_async_configs=" "
for async_config in $async_configs; do
  if ! [[ "$async_config" =~ ^[1-9][0-9]*:[1-9][0-9]*$ ]]; then
    echo "BENCH_ASYNC_CONFIGS entries must be <batch-size>:<max-wait-ms>: $async_config" >&2
    exit 2
  fi
  if [[ "$seen_async_configs" == *" $async_config "* ]]; then
    echo "BENCH_ASYNC_CONFIGS contains a duplicate entry: $async_config" >&2
    exit 2
  fi
  seen_async_configs+="$async_config "
done

if [[ -n "$async_configs" ]] && [[ " $capture_scenarios " != *" s2-envelope "* ]]; then
  echo "BENCH_ASYNC_CONFIGS requires s2-envelope in BENCH_CAPTURE_SCENARIOS" >&2
  exit 2
fi

for level in $client_levels; do
  if ! positive_integer "$level"; then
    echo "client levels must be positive integers: $level" >&2
    exit 2
  fi
done

if ! [[ "$order_seed" =~ ^[0-9]+$ ]]; then
  echo "BENCH_ORDER_SEED must be a non-negative integer" >&2
  exit 2
fi

if [[ -n "$profile_scenarios" ]]; then
  for profile_scenario in ${profile_scenarios//,/ }; do
    if ! valid_scenario "$profile_scenario"; then
      echo "BENCH_PROFILE_SCENARIO contains an unknown scenario" >&2
      exit 2
    fi
  done
  if ! positive_integer "$profile_frequency"; then
    echo "BENCH_PROFILE_FREQUENCY must be a positive integer" >&2
    exit 2
  fi
fi

if [[ -n "$module_source_commit" ]] &&
  [[ "$module_source_commit" != "HEAD" ]] &&
  ! [[ "$module_source_commit" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "BENCH_MODULE_SOURCE_COMMIT must be HEAD or a hexadecimal Git commit" >&2
  exit 2
fi

profile_requested() {
  local scenario="$1"
  local requested

  for requested in ${profile_scenarios//,/ }; do
    if [[ "$scenario" == "$requested" ]]; then
      return 0
    fi
  done
  return 1
}

mkdir -p "$results_dir/raw"

unsorted_plan="$results_dir/raw/trial-plan.unsorted.tsv"
trial_plan="$results_dir/raw/trial-plan.tsv"
: >"$unsorted_plan"

add_trial() {
  local scenario="$1"
  local trial_clients="$2"
  local repetition="$3"
  local trial_batch_size="$4"
  local trial_max_wait_ms="$5"
  local order_key

  order_key="$(
    printf '%s' \
      "$order_seed:$scenario:$trial_clients:$repetition:$trial_batch_size:$trial_max_wait_ms" |
      cksum |
      awk '{ print $1 }'
  )"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$order_key" "$scenario" "$trial_clients" "$repetition" \
    "$trial_batch_size" "$trial_max_wait_ms" >>"$unsorted_plan"
}

for trial_clients in $client_levels; do
  if ((baseline_repetitions > 0)); then
    for repetition in $(seq 1 "$baseline_repetitions"); do
      add_trial \
        s0 "$trial_clients" "$repetition" \
        "$async_batch_size" "$async_max_wait_ms"
    done
  fi

  for repetition in $(seq 1 "$repetitions"); do
    for capture_scenario in $capture_scenarios; do
      if [[ "$capture_scenario" == "s2-envelope" ]] &&
        [[ -n "$async_configs" ]]; then
        for async_config in $async_configs; do
          trial_batch_size="${async_config%%:*}"
          trial_max_wait_ms="${async_config##*:}"
          add_trial \
            "$capture_scenario" "$trial_clients" "$repetition" \
            "$trial_batch_size" "$trial_max_wait_ms"
        done
      else
        add_trial \
          "$capture_scenario" "$trial_clients" "$repetition" \
          "$async_batch_size" "$async_max_wait_ms"
      fi
    done
  done

  if ((filtered_repetitions > 0)); then
    for repetition in $(seq 1 "$filtered_repetitions"); do
      add_trial \
        s1 "$trial_clients" "$repetition" \
        "$async_batch_size" "$async_max_wait_ms"
    done
  fi
done

LC_ALL=C sort -n -k1,1 "$unsorted_plan" >"$trial_plan"

if [[ "$plan_only" == "yes" ]]; then
  echo "order scenario clients repetition batch-size max-wait-ms"
  cat "$trial_plan"
  echo
  echo "trial plan: $trial_plan"
  exit 0
fi

terraform_output="$(terraform -chdir="$root_dir" output -json)"
printf '%s\n' "$terraform_output" >"$results_dir/raw/terraform-output.json"

region="$(jq -r '.region.value' <<<"$terraform_output")"
server_id="$(jq -r '.server_instance_id.value' <<<"$terraform_output")"
server_ip="$(jq -r '.server_private_ip.value' <<<"$terraform_output")"
loadgen_id="$(jq -r '.loadgen_instance_id.value' <<<"$terraform_output")"
module_image="$(jq -r '.module_image.value' <<<"$terraform_output")"
loadgen_image="$(jq -r '.loadgen_image.value' <<<"$terraform_output")"

aws_args=(--region "$region")
if [[ -n "${AWS_PROFILE:-}" ]]; then
  aws_args+=(--profile "$AWS_PROFILE")
fi

aws_cli() {
  aws "${aws_args[@]}" "$@"
}

wait_for_ssm() {
  local instance_id="$1"
  local count
  for _ in $(seq 1 90); do
    count="$(
      aws_cli ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$instance_id" \
        --query 'length(InstanceInformationList)' \
        --output text
    )"
    if [[ "$count" == "1" ]]; then
      return 0
    fi
    sleep 5
  done
  echo "instance did not register with SSM: $instance_id" >&2
  return 1
}

run_remote() {
  local instance_id="$1"
  local label="$2"
  local command="$3"
  local output_base="$4"
  local parameters_file command_id status

  parameters_file="$(mktemp)"
  jq -n --arg command "$command" '{commands: [$command]}' >"$parameters_file"

  command_id="$(
    aws_cli ssm send-command \
      --instance-ids "$instance_id" \
      --document-name AWS-RunShellScript \
      --comment "redis-event-stream $label" \
      --parameters "file://$parameters_file" \
      --timeout-seconds 1200 \
      --query 'Command.CommandId' \
      --output text
  )"
  rm -f "$parameters_file"

  for _ in $(seq 1 240); do
    status="$(
      aws_cli ssm get-command-invocation \
        --command-id "$command_id" \
        --instance-id "$instance_id" \
        --query Status \
        --output text 2>/dev/null || true
    )"
    case "$status" in
      Success | Failed | Cancelled | TimedOut)
        break
        ;;
    esac
    sleep 5
  done

  aws_cli ssm get-command-invocation \
    --command-id "$command_id" \
    --instance-id "$instance_id" \
    --output json >"${output_base}.invocation.json"

  jq -r '.StandardOutputContent' "${output_base}.invocation.json" >"${output_base}.stdout"
  jq -r '.StandardErrorContent' "${output_base}.invocation.json" >"${output_base}.stderr"

  if [[ "$status" != "Success" ]]; then
    echo "remote command failed ($label): $status" >&2
    cat "${output_base}.stderr" >&2
    return 1
  fi
}

encode_file() {
  base64 <"$1" | tr -d '\n'
}

decode_base64() {
  if base64 --decode </dev/null >/dev/null 2>&1; then
    base64 --decode
  else
    base64 -D
  fi
}

fetch_remote_file() {
  local instance_id="$1"
  local label="$2"
  local remote_path="$3"
  local local_path="$4"
  local metadata_base metadata size expected_sha offset chunk_size chunk_base
  local actual_sha

  metadata_base="$results_dir/raw/fetch-$label-metadata"
  run_remote \
    "$instance_id" \
    "fetch-$label-metadata" \
    "stat -c '%s' '$remote_path'
sha256sum '$remote_path' | awk '{ print \$1 }'" \
    "$metadata_base"

  metadata="$(cat "$metadata_base.stdout")"
  size="$(sed -n '1p' <<<"$metadata")"
  expected_sha="$(sed -n '2p' <<<"$metadata")"
  if ! [[ "$size" =~ ^[0-9]+$ ]] ||
    ! [[ "$expected_sha" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid remote file metadata for $remote_path" >&2
    exit 1
  fi

  : >"$local_path"
  offset=0
  chunk_size=15000
  while ((offset < size)); do
    chunk_base="$results_dir/raw/fetch-$label-$offset"
    run_remote \
      "$instance_id" \
      "fetch-$label-$offset" \
      "dd if='$remote_path' iflag=skip_bytes,count_bytes skip='$offset' count='$chunk_size' 2>/dev/null | base64 -w0" \
      "$chunk_base"
    decode_base64 <"$chunk_base.stdout" >>"$local_path"
    offset=$((offset + chunk_size))
  done

  actual_sha="$(shasum -a 256 "$local_path" | awk '{ print $1 }')"
  if [[ "$actual_sha" != "$expected_sha" ]]; then
    echo "checksum mismatch fetching $remote_path" >&2
    exit 1
  fi
}

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
git_commit="$(git -C "$repo_dir" rev-parse HEAD)"

echo "waiting for EC2 instance health..."
aws_cli ec2 wait instance-status-ok --instance-ids "$server_id" "$loadgen_id"

echo "waiting for SSM registration..."
wait_for_ssm "$server_id"
wait_for_ssm "$loadgen_id"

echo "waiting for cloud-init and image pulls..."
bootstrap_command="cloud-init status --wait && test -f /var/lib/eventstream-smoke/ready"
run_remote "$server_id" bootstrap-server "$bootstrap_command" "$results_dir/raw/bootstrap-server"
run_remote "$loadgen_id" bootstrap-loadgen "$bootstrap_command" "$results_dir/raw/bootstrap-loadgen"

module_override="-"
module_artifact_json="null"
if [[ -n "$module_source_commit" ]]; then
  if [[ "$module_source_commit" == "HEAD" ]]; then
    module_source_commit="$git_commit"
  fi
  module_archive="${module_source_repo%/}/archive/${module_source_commit}.tar.gz"
  module_build_script_b64="$(encode_file "$root_dir/scripts/remote-module-build.sh")"
  module_override="/var/lib/eventstream-smoke/libredis_event_stream_module.so"
  module_build_command="printf '%s' '$module_build_script_b64' | base64 --decode > /tmp/eventstream-module-build.sh
chmod 0700 /tmp/eventstream-module-build.sh
/tmp/eventstream-module-build.sh '$module_archive' '$module_override'"
  echo "building branch module artifact from $module_source_commit..."
  run_remote \
    "$server_id" \
    branch-module-build \
    "$module_build_command" \
    "$results_dir/raw/branch-module-build"
  module_artifact_json="$(cat "$results_dir/raw/branch-module-build.stdout")"
  jq -e \
    '.sha256 | test("^[0-9a-f]{64}$")' \
    <<<"$module_artifact_json" >/dev/null
  module_artifact_json="$(
    jq --arg commit "$module_source_commit" '. + {git_commit: $commit}' \
      <<<"$module_artifact_json"
  )"
fi

server_script_b64="$(encode_file "$root_dir/scripts/remote-server.sh")"
benchmark_script_b64="$(encode_file "$root_dir/scripts/remote-benchmark.sh")"
profile_script_b64="$(encode_file "$root_dir/scripts/remote-profile.sh")"
environment_script_b64="$(encode_file "$root_dir/scripts/remote-environment.sh")"

# Capture the physical and software environment once per campaign. A clean s0
# server supplies the exact Redis binary while avoiding module/workload noise
# in the intrinsic and private-network latency probes.
environment_server_start="printf '%s' '$server_script_b64' | base64 --decode > /tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh s0 '$module_image' '$maxlen' '$module_override' '$async_queue_capacity' '$async_batch_size' '$async_max_wait_ms'"
run_remote \
  "$server_id" \
  environment-server-start \
  "$environment_server_start" \
  "$results_dir/raw/environment-server-start"

environment_server_command="printf '%s' '$environment_script_b64' | base64 --decode > /tmp/eventstream-environment.sh
chmod 0700 /tmp/eventstream-environment.sh
/tmp/eventstream-environment.sh server '$server_ip' '$loadgen_image' '$environment_network_samples'"
run_remote \
  "$server_id" \
  environment-server \
  "$environment_server_command" \
  "$results_dir/raw/environment-server"

environment_loadgen_command="printf '%s' '$environment_script_b64' | base64 --decode > /tmp/eventstream-environment.sh
chmod 0700 /tmp/eventstream-environment.sh
/tmp/eventstream-environment.sh loadgen '$server_ip' '$loadgen_image' '$environment_network_samples'"
run_remote \
  "$loadgen_id" \
  environment-loadgen \
  "$environment_loadgen_command" \
  "$results_dir/raw/environment-loadgen"

jq -e . "$results_dir/raw/environment-server.stdout" >/dev/null
jq -e . "$results_dir/raw/environment-loadgen.stdout" >/dev/null

aws_cli ec2 describe-instances \
  --instance-ids "$server_id" "$loadgen_id" \
  --output json >"$results_dir/raw/ec2-instances.json"
aws_cli ec2 describe-volumes \
  --filters "Name=attachment.instance-id,Values=$server_id,$loadgen_id" \
  --output json >"$results_dir/raw/ec2-volumes.json"

environment_collected_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg schema_version "1" \
  --arg collected_at "$environment_collected_at" \
  --arg server_id "$server_id" \
  --arg loadgen_id "$loadgen_id" \
  --arg region "$region" \
  --arg availability_zone "$(jq -r '.availability_zone.value' <<<"$terraform_output")" \
  --arg vpc_id "$(jq -r '.vpc_id.value' <<<"$terraform_output")" \
  --arg subnet_id "$(jq -r '.subnet_id.value' <<<"$terraform_output")" \
  --arg server_private_ip "$server_ip" \
  --arg expiry_stop_schedule_arn \
    "$(jq -r '.expiry_stop_schedule_arn.value' <<<"$terraform_output")" \
  --arg expires_at "$(jq -r '.expires_at.value' <<<"$terraform_output")" \
  --arg root_volume_type "$(jq -r '.root_volume_type.value' <<<"$terraform_output")" \
  --argjson root_volume_gib \
    "$(jq -r '.root_volume_gib.value' <<<"$terraform_output")" \
  --slurpfile server_host "$results_dir/raw/environment-server.stdout" \
  --slurpfile loadgen_host "$results_dir/raw/environment-loadgen.stdout" \
  --slurpfile instances "$results_dir/raw/ec2-instances.json" \
  --slurpfile volumes "$results_dir/raw/ec2-volumes.json" \
  -f "$root_dir/scripts/assemble-environment.jq" \
  >"$results_dir/environment.json"
jq -e . "$results_dir/environment.json" >/dev/null

trial_files=()
trial_number=0
while IFS=$'\t' read -r _ scenario trial_clients repetition \
  trial_batch_size trial_max_wait_ms; do
  trial_number=$((trial_number + 1))
  if [[ "$scenario" == "s2-individual" || "$scenario" == "s2-envelope" ]]; then
    trial_id="${scenario}-b${trial_batch_size}-w${trial_max_wait_ms}-c${trial_clients}-r${repetition}"
  else
    trial_id="${scenario}-c${trial_clients}-r${repetition}"
  fi
  echo "running trial $trial_number: $trial_id..."

  server_command="printf '%s' '$server_script_b64' | base64 --decode > /tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh '$scenario' '$module_image' '$maxlen' '$module_override' '$async_queue_capacity' '$trial_batch_size' '$trial_max_wait_ms'"
  run_remote \
    "$server_id" \
    "server-$trial_id" \
    "$server_command" \
    "$results_dir/raw/server-$trial_id"

  profiled=false
  if profile_requested "$scenario"; then
    profiled=true
    profile_start_command="printf '%s' '$profile_script_b64' | base64 --decode > /tmp/eventstream-profile.sh
chmod 0700 /tmp/eventstream-profile.sh
/tmp/eventstream-profile.sh start '$profile_frequency'"
    run_remote \
      "$server_id" \
      "profile-start-$trial_id" \
      "$profile_start_command" \
      "$results_dir/raw/profile-start-$trial_id"
  fi

  benchmark_command="printf '%s' '$benchmark_script_b64' | base64 --decode > /tmp/eventstream-benchmark.sh
chmod 0700 /tmp/eventstream-benchmark.sh
/tmp/eventstream-benchmark.sh '$scenario' '$server_ip' '$loadgen_image' '$requests' '$trial_clients' '$threads' '$payload' '$keyspace'"
  run_remote \
    "$loadgen_id" \
    "benchmark-$trial_id" \
    "$benchmark_command" \
    "$results_dir/raw/benchmark-$trial_id"

  benchmark_file="$results_dir/raw/benchmark-$trial_id.stdout"
  jq -e . "$benchmark_file" >/dev/null

  if [[ "$profiled" == true ]]; then
    profile_stop_command="printf '%s' '$profile_script_b64' | base64 --decode > /tmp/eventstream-profile.sh
chmod 0700 /tmp/eventstream-profile.sh
/tmp/eventstream-profile.sh stop"
    run_remote \
      "$server_id" \
      "profile-stop-$trial_id" \
      "$profile_stop_command" \
      "$results_dir/raw/profile-stop-$trial_id"

    profile_results_dir="$results_dir/profile/$trial_id"
    mkdir -p "$profile_results_dir"
    for artifact in \
      perf-header.txt.gz \
      perf-leaf.txt.gz \
      perf-children.txt.gz \
      perf-record.stderr.gz; do
      fetch_remote_file \
        "$server_id" \
        "$trial_id-${artifact%.gz}" \
        "/var/lib/eventstream-smoke/profile/$artifact" \
        "$profile_results_dir/$artifact"
      gzip -dc "$profile_results_dir/$artifact" \
        >"$profile_results_dir/${artifact%.gz}"
    done
  fi

  trial_file="$results_dir/$trial_id.json"
  if [[ "$profiled" == true ]]; then
    jq \
      --arg trial_id "$trial_id" \
      --argjson repetition "$repetition" \
      --argjson order "$trial_number" \
      --argjson async_queue_capacity "$async_queue_capacity" \
      --argjson async_batch_size "$trial_batch_size" \
      --argjson async_max_wait_ms "$trial_max_wait_ms" \
      --slurpfile profile_start "$results_dir/raw/profile-start-$trial_id.stdout" \
      --slurpfile profile_stop "$results_dir/raw/profile-stop-$trial_id.stdout" \
      '. + {
        trial_id: $trial_id,
        repetition: $repetition,
        order: $order,
        async_configuration:
          (if (.scenario == "s2-individual" or .scenario == "s2-envelope")
           then {
             queue_capacity: $async_queue_capacity,
             batch_size: $async_batch_size,
             max_wait_ms: $async_max_wait_ms
           }
           else null
           end),
        profile: {
          start: $profile_start[0],
          stop: $profile_stop[0]
        }
      }' \
      "$benchmark_file" >"$trial_file"
  else
    jq \
      --arg trial_id "$trial_id" \
      --argjson repetition "$repetition" \
      --argjson order "$trial_number" \
      --argjson async_queue_capacity "$async_queue_capacity" \
      --argjson async_batch_size "$trial_batch_size" \
      --argjson async_max_wait_ms "$trial_max_wait_ms" \
      '. + {
        trial_id: $trial_id,
        repetition: $repetition,
        order: $order,
        async_configuration:
          (if (.scenario == "s2-individual" or .scenario == "s2-envelope")
           then {
             queue_capacity: $async_queue_capacity,
             batch_size: $async_batch_size,
             max_wait_ms: $async_max_wait_ms
           }
           else null
           end)
      }' \
      "$benchmark_file" >"$trial_file"
  fi
  jq -e . "$trial_file" >/dev/null
  trial_files+=("$trial_file")
done <"$trial_plan"

jq -s '.' "${trial_files[@]}" >"$results_dir/trials.json"

jq -e '
  all(.[];
    (.module.events_lost == 0) and
    (.module.dropped == 0) and
    (.module.handler_panics == 0) and
    (.module.async_worker_errors == 0) and
    (if .scenario == "s0"
     then .module.loaded == false
     elif .scenario == "s1"
     then
       (.module.loaded == true) and
       (.module.forwarded == 0) and
       (.module.skipped_filtered == .workload.requests)
     elif (.scenario | startswith("s2"))
     then
       (.module.loaded == true) and
       (.module.forwarded == .workload.requests) and
       (if (.scenario == "s2-individual" or .scenario == "s2-envelope")
        then
          ((.module.async_enqueued + .module.async_fallbacks) == .workload.requests) and
          (.module.async_drain_events == .module.async_enqueued)
        else true end) and
       (if .scenario == "s2-envelope"
        then .module.async_envelopes > 0
        else true end)
     else false
     end))
' "$results_dir/trials.json" >/dev/null

jq '
  def distribution:
    map(select(. != null)) | sort |
    if length == 0 then
      {min: null, median: null, max: null}
    else
      . as $values |
      {
        min: $values[0],
        median:
          (if ($values | length) % 2 == 1
           then $values[(($values | length) / 2 | floor)]
           else
             (($values[(($values | length) / 2) - 1] +
               $values[($values | length) / 2]) / 2)
           end),
        max: $values[-1]
      }
    end;

  sort_by([
    .scenario,
    .workload.clients,
    (.async_configuration.batch_size // 0),
    (.async_configuration.max_wait_ms // 0)
  ]) |
  group_by([
    .scenario,
    .workload.clients,
    (.async_configuration.batch_size // 0),
    (.async_configuration.max_wait_ms // 0)
  ]) |
  map(
    . as $trials |
    {
      scenario: $trials[0].scenario,
      clients: $trials[0].workload.clients,
      async_configuration: $trials[0].async_configuration,
      repetitions: ($trials | length),
      ops_per_sec:
        ($trials | map(.result.ops_per_sec) | distribution),
      end_to_end_ops_per_sec:
        ($trials | map(.result.end_to_end_ops_per_sec) | distribution),
      p99_ms:
        ($trials | map(.result.p99_ms) | distribution),
      max_ms:
        ($trials | map(.result.max_ms) | distribution),
      server_main_thread_core_percent:
        ($trials | map(.server.main_thread_core_percent) | distribution),
      server_total_core_percent:
        ($trials | map(.server.total_core_percent) | distribution),
      capture_settle_seconds:
        ($trials | map(.workload.capture_settle_seconds) | distribution),
      load_generator_cpu_percent_avg:
        ($trials | map(.load_generator.cpu_percent_avg) | distribution),
      load_generator_cpu_percent_max:
        ($trials | map(.load_generator.cpu_percent_max) | distribution),
      async_queue_high_water:
        ($trials | map(
          if .async_configuration == null
          then null
          else .module.async_queue_high_water
          end
        ) | distribution),
      achieved_envelope_size:
        ($trials | map(
          if .module.async_envelopes > 0
          then .module.async_envelope_events / .module.async_envelopes
          else null
          end
        ) | distribution),
      envelope_event_percent:
        ($trials | map(
          if .module.async_envelope_events > 0
          then (.module.async_envelope_events / .workload.requests) * 100
          else null
          end
        ) | distribution),
      fallback_percent:
        ($trials | map(
          if .async_configuration == null
          then null
          else (.module.async_fallbacks / .workload.requests) * 100
          end
        ) | distribution),
      events_lost: ($trials | map(.module.events_lost) | add),
      dropped: ($trials | map(.module.dropped) | add),
      handler_panics: ($trials | map(.module.handler_panics) | add),
      async_worker_errors: ($trials | map(.module.async_worker_errors) | add)
    }
  )
' "$results_dir/trials.json" >"$results_dir/summary.json"

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -n \
  --arg schema_version "5" \
  --arg run_id "$run_id" \
  --arg started_at "$started_at" \
  --arg completed_at "$completed_at" \
  --arg git_commit "$git_commit" \
  --arg region "$region" \
  --arg availability_zone "$(jq -r '.availability_zone.value' <<<"$terraform_output")" \
  --arg ami_id "$(jq -r '.ami_id.value' <<<"$terraform_output")" \
  --arg server_instance_type "$(jq -r '.server_instance_type.value' <<<"$terraform_output")" \
  --arg loadgen_instance_type "$(jq -r '.loadgen_instance_type.value' <<<"$terraform_output")" \
  --arg module_image "$module_image" \
  --arg loadgen_image "$loadgen_image" \
  --arg expires_at "$(jq -r '.expires_at.value' <<<"$terraform_output")" \
  --argjson module_artifact "$module_artifact_json" \
  --slurpfile lab_environment "$results_dir/environment.json" \
  --arg capture_scenarios "$capture_scenarios" \
  --argjson order_seed "$order_seed" \
  --argjson async_queue_capacity "$async_queue_capacity" \
  --argjson async_batch_size "$async_batch_size" \
  --argjson async_max_wait_ms "$async_max_wait_ms" \
  --arg async_configs "$async_configs" \
  --slurpfile trials "$results_dir/trials.json" \
  --slurpfile summary "$results_dir/summary.json" \
  '{
    schema_version: ($schema_version | tonumber),
    run_id: $run_id,
    started_at: $started_at,
    completed_at: $completed_at,
    git_commit: $git_commit,
    harness_git_commit: $git_commit,
    order_seed: $order_seed,
    environment: {
      region: $region,
      availability_zone: $availability_zone,
      ami_id: $ami_id,
      server_instance_type: $server_instance_type,
      loadgen_instance_type: $loadgen_instance_type,
      module_image: $module_image,
      module_build_profile: "release",
      module_artifact: $module_artifact,
      module_git_commit: $module_artifact.git_commit,
      loadgen_image: $loadgen_image,
      expires_at: $expires_at,
      lab: $lab_environment[0]
    },
    capture_configuration: {
      scenarios: ($capture_scenarios | split(" ")),
      async_queue_capacity: $async_queue_capacity,
      async_batch_size: $async_batch_size,
      async_max_wait_ms: $async_max_wait_ms,
      async_configs:
        ($async_configs
         | split(" ")
         | map(select(length > 0)
           | split(":")
           | {
             batch_size: (.[0] | tonumber),
             max_wait_ms: (.[1] | tonumber)
           }))
    },
    trials: $trials[0],
    summary: $summary[0]
  }' >"$results_dir/result.json"

echo
jq -r '
  ["scenario", "batch", "wait", "clients", "reps", "client ops/s", "end-to-end ops/s", "p99", "total core %", "settle s", "envelope", "fallback %"],
  (.summary[] | [
    .scenario,
    ((.async_configuration.batch_size // "-") | tostring),
    ((.async_configuration.max_wait_ms // "-") | tostring),
    (.clients | tostring),
    (.repetitions | tostring),
    (.ops_per_sec.median | tostring),
    (.end_to_end_ops_per_sec.median | tostring),
    (.p99_ms.median | tostring),
    (.server_total_core_percent.median | tostring),
    (.capture_settle_seconds.median | tostring),
    ((.achieved_envelope_size.median // "-") | tostring),
    ((.fallback_percent.median // "-") | tostring)
  ]) | @tsv
' "$results_dir/result.json" | column -t -s $'\t'

echo
echo "result: $results_dir/result.json"
