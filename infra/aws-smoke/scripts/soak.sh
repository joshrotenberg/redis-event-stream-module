#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$root_dir/../.." && pwd)"

soak_seconds="${SOAK_SECONDS:-1800}"
target_rps="${SOAK_TARGET_RPS:-80000}"
calibration_requests="${SOAK_CALIBRATION_REQUESTS:-500000}"
payload="${SOAK_PAYLOAD:-64}"
keyspace="${SOAK_KEYSPACE:-100000}"
maxlen="${SOAK_MAXLEN:-750000}"
queue_capacity="${SOAK_QUEUE_CAPACITY:-1048576}"
batch_size="${SOAK_BATCH_SIZE:-128}"
max_wait_ms="${SOAK_MAX_WAIT_MS:-1}"
probe_depth="${SOAK_PROBE_DEPTH:-10000}"
probe_batch_events="${SOAK_PROBE_BATCH_EVENTS:-100000}"
probe_max_batches="${SOAK_PROBE_MAX_BATCHES:-10}"
audit_idle_ms="${SOAK_AUDIT_IDLE_MS:-2000}"
module_source_commit="${SOAK_MODULE_SOURCE_COMMIT:-HEAD}"
module_source_repo="${SOAK_MODULE_SOURCE_REPO:-https://github.com/joshrotenberg/redis-event-stream-module}"
plan_only="${SOAK_PLAN_ONLY:-no}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-soak}"
results_dir="${RESULTS_DIR:-$root_dir/results/$run_id}"
ssm_timeout_seconds=$((soak_seconds + 3600))

positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

for value in \
  "$soak_seconds" "$target_rps" "$calibration_requests" "$payload" \
  "$keyspace" "$maxlen" "$queue_capacity" "$batch_size" "$max_wait_ms" \
  "$probe_depth" "$probe_batch_events" "$probe_max_batches" \
  "$audit_idle_ms"; do
  if ! positive_integer "$value"; then
    echo "soak values must be positive integers: $value" >&2
    exit 2
  fi
done

if ((soak_seconds < 70)); then
  echo "SOAK_SECONDS must be at least 70 so every planned phase is nonzero" >&2
  exit 2
fi
if ((probe_depth >= queue_capacity)); then
  echo "SOAK_PROBE_DEPTH must be smaller than SOAK_QUEUE_CAPACITY" >&2
  exit 2
fi
if [[ "$plan_only" != "yes" && "$plan_only" != "no" ]]; then
  echo "SOAK_PLAN_ONLY must be yes or no" >&2
  exit 2
fi
if [[ "$module_source_commit" != "HEAD" ]] &&
  ! [[ "$module_source_commit" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "SOAK_MODULE_SOURCE_COMMIT must be HEAD or a hexadecimal Git commit" >&2
  exit 2
fi

jq -n \
  --arg run_id "$run_id" \
  --argjson soak_seconds "$soak_seconds" \
  --argjson target_rps "$target_rps" \
  --argjson calibration_requests "$calibration_requests" \
  --argjson payload "$payload" \
  --argjson keyspace "$keyspace" \
  --argjson maxlen "$maxlen" \
  --argjson queue_capacity "$queue_capacity" \
  --argjson batch_size "$batch_size" \
  --argjson max_wait_ms "$max_wait_ms" \
  --argjson probe_depth "$probe_depth" \
  --argjson probe_batch_events "$probe_batch_events" \
  --argjson probe_max_batches "$probe_max_batches" \
  '{
    run_id: $run_id,
    soak_seconds: $soak_seconds,
    target_rps: $target_rps,
    calibration_requests_per_candidate: $calibration_requests,
    payload_bytes: $payload,
    keyspace: $keyspace,
    maxlen: $maxlen,
    async_configuration: {
      queue_capacity: $queue_capacity,
      batch_size: $batch_size,
      max_wait_ms: $max_wait_ms
    },
    phase_percentages: [
      {name: "steady-1", percent: 27},
      {name: "burst-1", percent: 2, target_multiplier: 2},
      {name: "steady-2", percent: 23},
      {name: "consumer-pause", percent: 4},
      {name: "consumer-catchup", percent: 13},
      {name: "burst-2", percent: 2, target_multiplier: 2},
      {name: "steady-3", percent: 29}
    ],
    restart_probe: {
      queue_depth_threshold: $probe_depth,
      unique_events_per_batch: $probe_batch_events,
      maximum_batches: $probe_max_batches,
      modes: ["graceful-module-unload", "abrupt-process-kill-aof-everysec"]
    }
  }' >"/tmp/eventstream-soak-plan.json"

if [[ "$plan_only" == "yes" ]]; then
  cat /tmp/eventstream-soak-plan.json
  exit 0
fi

required_tools=(aws base64 git gzip jq shasum tar terraform)
for tool in "${required_tools[@]}"; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

mkdir -p "$results_dir/raw"
cp /tmp/eventstream-soak-plan.json "$results_dir/plan.json"

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
  local parameters_file command_id status poll_count

  parameters_file="$(mktemp)"
  jq -n --arg command "$command" '{commands: [$command]}' >"$parameters_file"
  command_id="$(
    aws_cli ssm send-command \
      --instance-ids "$instance_id" \
      --document-name AWS-RunShellScript \
      --comment "redis-event-stream $label" \
      --parameters "file://$parameters_file" \
      --timeout-seconds "$ssm_timeout_seconds" \
      --query 'Command.CommandId' \
      --output text
  )"
  rm -f "$parameters_file"

  poll_count=$((ssm_timeout_seconds / 5 + 60))
  for _ in $(seq 1 "$poll_count"); do
    status="$(
      aws_cli ssm get-command-invocation \
        --command-id "$command_id" \
        --instance-id "$instance_id" \
        --query Status \
        --output text 2>/dev/null ||
        true
    )"
    case "$status" in
      Success | Failed | Cancelled | TimedOut) break ;;
    esac
    sleep 5
  done

  aws_cli ssm get-command-invocation \
    --command-id "$command_id" \
    --instance-id "$instance_id" \
    --output json >"${output_base}.invocation.json"
  jq -r '.StandardOutputContent' "${output_base}.invocation.json" \
    >"${output_base}.stdout"
  jq -r '.StandardErrorContent' "${output_base}.invocation.json" \
    >"${output_base}.stderr"
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
if [[ "$module_source_commit" == "HEAD" ]]; then
  module_source_commit="$git_commit"
fi
module_archive="${module_source_repo%/}/archive/${module_source_commit}.tar.gz"

echo "waiting for EC2 instance health..."
aws_cli ec2 wait instance-status-ok --instance-ids "$server_id" "$loadgen_id"
echo "waiting for SSM registration..."
wait_for_ssm "$server_id"
wait_for_ssm "$loadgen_id"
echo "waiting for cloud-init and image pulls..."
bootstrap_command="cloud-init status --wait && test -f /var/lib/eventstream-smoke/ready"
run_remote \
  "$server_id" \
  bootstrap-server \
  "$bootstrap_command" \
  "$results_dir/raw/bootstrap-server"
run_remote \
  "$loadgen_id" \
  bootstrap-loadgen \
  "$bootstrap_command" \
  "$results_dir/raw/bootstrap-loadgen"

module_build_b64="$(encode_file "$root_dir/scripts/remote-module-build.sh")"
module_override="/var/lib/eventstream-smoke/libredis_event_stream_module.so"
module_build_command="printf '%s' '$module_build_b64' | base64 --decode > /tmp/eventstream-module-build.sh
chmod 0700 /tmp/eventstream-module-build.sh
/tmp/eventstream-module-build.sh '$module_archive' '$module_override'"
echo "building module from $module_source_commit..."
run_remote \
  "$server_id" \
  branch-module-build \
  "$module_build_command" \
  "$results_dir/raw/branch-module-build"
module_artifact="$(cat "$results_dir/raw/branch-module-build.stdout")"

client_build_b64="$(encode_file "$root_dir/scripts/remote-client-build.sh")"
client_image="eventstream-branch-client:soak-${module_source_commit:0:12}"
client_build_command="printf '%s' '$client_build_b64' | base64 --decode > /tmp/eventstream-client-build.sh
chmod 0700 /tmp/eventstream-client-build.sh
/tmp/eventstream-client-build.sh '$module_archive' '$client_image'"
echo "building decoder/consumer from $module_source_commit..."
run_remote \
  "$loadgen_id" \
  branch-client-build \
  "$client_build_command" \
  "$results_dir/raw/branch-client-build"
client_artifact="$(cat "$results_dir/raw/branch-client-build.stdout")"

server_script_b64="$(encode_file "$root_dir/scripts/remote-server.sh")"
calibration_script_b64="$(encode_file "$root_dir/scripts/remote-calibrate.sh")"
workload_script_b64="$(encode_file "$root_dir/scripts/remote-soak-workload.sh")"
probe_script_b64="$(encode_file "$root_dir/scripts/remote-restart-probe.sh")"
audit_script_b64="$(encode_file "$root_dir/scripts/remote-consumer-audit.sh")"

start_server() {
  local label="$1"
  local retention_maxlen="$2"
  local command
  command="printf '%s' '$server_script_b64' | base64 --decode > /tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh s2-envelope '$module_image' '$retention_maxlen' '$module_override' '$queue_capacity' '$batch_size' '$max_wait_ms'"
  run_remote \
    "$server_id" \
    "$label" \
    "$command" \
    "$results_dir/raw/$label"
}

echo "calibrating client concurrency around $target_rps requests/s..."
start_server server-calibration 10000
calibration_command="printf '%s' '$calibration_script_b64' | base64 --decode > /tmp/eventstream-calibrate.sh
chmod 0700 /tmp/eventstream-calibrate.sh
/tmp/eventstream-calibrate.sh '$server_ip' '$loadgen_image' '$target_rps' '$calibration_requests'"
run_remote \
  "$loadgen_id" \
  calibration \
  "$calibration_command" \
  "$results_dir/raw/calibration"
cp "$results_dir/raw/calibration.stdout" "$results_dir/calibration.json"
jq -e '.selected.clients and .selected.ops_per_sec' \
  "$results_dir/calibration.json" >/dev/null
base_clients="$(jq -r '.selected.clients' "$results_dir/calibration.json")"
base_rps="$(jq -r '.selected.ops_per_sec' "$results_dir/calibration.json")"

echo "running ${soak_seconds}s main soak with $base_clients clients..."
start_server server-main "$maxlen"
main_remote_dir="/var/lib/eventstream-smoke/main-$run_id"
main_remote_archive="/var/lib/eventstream-smoke/main-$run_id.tar.gz"
workload_command="printf '%s' '$workload_script_b64' | base64 --decode > /tmp/eventstream-soak-workload.sh
chmod 0700 /tmp/eventstream-soak-workload.sh
/tmp/eventstream-soak-workload.sh '$server_ip' '$loadgen_image' '$client_image' '$soak_seconds' '$base_clients' '$base_rps' '$payload' '$keyspace' '$main_remote_dir'
workload_status=\$?
if [ \"\$workload_status\" -ne 0 ]; then
  exit \"\$workload_status\"
fi
tar -C '$main_remote_dir' -czf '$main_remote_archive' ."
run_remote \
  "$loadgen_id" \
  main-soak \
  "$workload_command" \
  "$results_dir/raw/main-soak"
fetch_remote_file \
  "$loadgen_id" \
  main-soak \
  "$main_remote_archive" \
  "$results_dir/main.tar.gz"
mkdir -p "$results_dir/main"
tar -xzf "$results_dir/main.tar.gz" -C "$results_dir/main"

run_probe() {
  local mode="$1"
  local remote_dir="/var/lib/eventstream-smoke/${mode}-$run_id"
  local probe_command audit_command
  local audit_remote_dir="/var/lib/eventstream-smoke/audit-${mode}-$run_id"

  echo "running $mode restart probe..."
  probe_command="printf '%s' '$probe_script_b64' | base64 --decode > /tmp/eventstream-restart-probe.sh
chmod 0700 /tmp/eventstream-restart-probe.sh
/tmp/eventstream-restart-probe.sh '$mode' '$module_image' '$module_override' '$queue_capacity' '$batch_size' '$max_wait_ms' '$probe_depth' '$probe_batch_events' '$probe_max_batches' '$remote_dir'"
  run_remote \
    "$server_id" \
    "probe-$mode" \
    "$probe_command" \
    "$results_dir/raw/probe-$mode"
  fetch_remote_file \
    "$server_id" \
    "probe-$mode" \
    "$remote_dir/result.json" \
    "$results_dir/$mode-probe.json"

  audit_command="printf '%s' '$audit_script_b64' | base64 --decode > /tmp/eventstream-consumer-audit.sh
chmod 0700 /tmp/eventstream-consumer-audit.sh
/tmp/eventstream-consumer-audit.sh '$server_ip' '$client_image' '$mode' '$audit_idle_ms' '$audit_remote_dir'"
  run_remote \
    "$loadgen_id" \
    "audit-$mode" \
    "$audit_command" \
    "$results_dir/raw/audit-$mode"
  fetch_remote_file \
    "$loadgen_id" \
    "audit-$mode" \
    "$audit_remote_dir/$mode.json" \
    "$results_dir/$mode-audit.json"
}

run_probe graceful
run_probe abrupt

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg schema_version "1" \
  --arg run_id "$run_id" \
  --arg started_at "$started_at" \
  --arg completed_at "$completed_at" \
  --arg git_commit "$git_commit" \
  --arg region "$region" \
  --arg availability_zone \
    "$(jq -r '.availability_zone.value' <<<"$terraform_output")" \
  --arg server_instance_type \
    "$(jq -r '.server_instance_type.value' <<<"$terraform_output")" \
  --arg loadgen_instance_type \
    "$(jq -r '.loadgen_instance_type.value' <<<"$terraform_output")" \
  --arg module_image "$module_image" \
  --arg loadgen_image "$loadgen_image" \
  --arg expires_at "$(jq -r '.expires_at.value' <<<"$terraform_output")" \
  --argjson module_artifact "$module_artifact" \
  --argjson client_artifact "$client_artifact" \
  --slurpfile plan "$results_dir/plan.json" \
  --slurpfile calibration "$results_dir/calibration.json" \
  --slurpfile main "$results_dir/main/result.json" \
  --slurpfile graceful_probe "$results_dir/graceful-probe.json" \
  --slurpfile graceful_audit "$results_dir/graceful-audit.json" \
  --slurpfile abrupt_probe "$results_dir/abrupt-probe.json" \
  --slurpfile abrupt_audit "$results_dir/abrupt-audit.json" \
  -f "$root_dir/scripts/assemble-soak.jq" >"$results_dir/result.json"

echo
jq '{
  run_id,
  main: {
    planned_logical_events: .main.planned_logical_events,
    correctness: .main.correctness,
    analysis: .main.analysis
  },
  graceful: {
    queue_depth_before_boundary:
      .graceful.probe.queue_depth_before_boundary,
    source_events: .graceful.probe.durable_source_keys_after_boundary,
    decoded_events: .graceful.audit.logical_events,
    missing: .graceful.missing_logical_events
  },
  abrupt: {
    queue_depth_before_boundary: .abrupt.probe.queue_depth_before_boundary,
    acknowledged: .abrupt.probe.acknowledged_commands,
    durable_source_events:
      .abrupt.probe.durable_source_keys_after_boundary,
    decoded_events: .abrupt.audit.logical_events,
    missing: .abrupt.missing_logical_events
  }
}' "$results_dir/result.json"
echo
echo "result: $results_dir/result.json"

jq -e '
  .main.correctness.capture_settled and
  .main.correctness.consumer_completed and
  .main.correctness.source_to_forwarded_exact and
  .main.correctness.source_to_consumer_exact and
  (.main.correctness.events_lost == 0) and
  (.main.correctness.dropped == 0) and
  (.main.correctness.handler_panics == 0) and
  (.main.correctness.async_worker_errors == 0) and
  .graceful.exact and
  .abrupt.accounting_valid
' "$results_dir/result.json" >/dev/null
