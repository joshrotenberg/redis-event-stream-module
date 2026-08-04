#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$root_dir/../.." && pwd)"
plan_only="${SATURATION_PLAN_ONLY:-no}"
run_id="${SATURATION_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-saturation}"
results_dir="${SATURATION_RESULTS_DIR:-$root_dir/results/$run_id}"
module_source_commit="${SATURATION_MODULE_SOURCE_COMMIT:-HEAD}"
module_source_repo="${SATURATION_MODULE_SOURCE_REPO:-https://github.com/joshrotenberg/redis-event-stream-module}"
environment_network_samples="${SATURATION_ENVIRONMENT_NETWORK_SAMPLES:-10000}"
persistence_mode="${SATURATION_PERSISTENCE_MODE:-off}"
background_action="${SATURATION_BACKGROUND_ACTION:-none}"
background_delay_seconds="${SATURATION_BACKGROUND_DELAY_SECONDS:-5}"
background_timeout_seconds="${SATURATION_BACKGROUND_TIMEOUT_SECONDS:-120}"
prefill_keys="${SATURATION_PREFILL_KEYS:-0}"
prefill_payload_bytes="${SATURATION_PREFILL_PAYLOAD_BYTES:-1024}"
replication_mode="${SATURATION_REPLICATION_MODE:-off}"
maxlen="${SATURATION_MAXLEN:-10000}"
persistence_probe_events="${SATURATION_PERSISTENCE_PROBE_EVENTS:-5000}"
replication_probe_events="${SATURATION_REPLICATION_PROBE_EVENTS:-5000}"
replication_pause_seconds="${SATURATION_REPLICATION_PAUSE_SECONDS:-2}"
maxmemory_probe="${SATURATION_MAXMEMORY_PROBE:-off}"
maxmemory_prefill_keys="${SATURATION_MAXMEMORY_PREFILL_KEYS:-50000}"
maxmemory_payload_bytes="${SATURATION_MAXMEMORY_PAYLOAD_BYTES:-1024}"
maxmemory_delete_events="${SATURATION_MAXMEMORY_DELETE_EVENTS:-15000}"
maxmemory_write_events="${SATURATION_MAXMEMORY_WRITE_EVENTS:-15000}"
maxmemory_write_payload_bytes="${SATURATION_MAXMEMORY_WRITE_PAYLOAD_BYTES:-64}"
maxmemory_percent="${SATURATION_MAXMEMORY_PERCENT:-90}"
maxmemory_churn_keys="${SATURATION_MAXMEMORY_CHURN_KEYS:-50000}"
maxmemory_churn_rounds="${SATURATION_MAXMEMORY_CHURN_ROUNDS:-6}"

case "$persistence_mode" in
  off | rdb | aof-everysec | aof-always) ;;
  *)
    echo "SATURATION_PERSISTENCE_MODE must be off, rdb, aof-everysec, or aof-always" >&2
    exit 2
    ;;
esac
case "$background_action" in
  none) ;;
  bgsave)
    [[ "$persistence_mode" == rdb ]] || {
      echo "bgsave requires SATURATION_PERSISTENCE_MODE=rdb" >&2
      exit 2
    }
    ;;
  bgrewriteaof)
    [[ "$persistence_mode" == aof-everysec || "$persistence_mode" == aof-always ]] || {
      echo "bgrewriteaof requires an AOF persistence mode" >&2
      exit 2
    }
    ;;
  *)
    echo "SATURATION_BACKGROUND_ACTION must be none, bgsave, or bgrewriteaof" >&2
    exit 2
    ;;
esac
case "$replication_mode" in
  off | replica) ;;
  *)
    echo "SATURATION_REPLICATION_MODE must be off or replica" >&2
    exit 2
    ;;
esac
case "$maxmemory_probe" in
  off | on) ;;
  *)
    echo "SATURATION_MAXMEMORY_PROBE must be off or on" >&2
    exit 2
    ;;
esac
if ! [[ "$maxlen" =~ ^[1-9][0-9]*$ && "$persistence_probe_events" =~ ^[1-9][0-9]*$ &&
  "$replication_probe_events" =~ ^[1-9][0-9]*$ && "$replication_pause_seconds" =~ ^[1-9][0-9]*$ &&
  "$background_delay_seconds" =~ ^[0-9]+$ && "$background_timeout_seconds" =~ ^[1-9][0-9]*$ &&
  "$prefill_keys" =~ ^[0-9]+$ && "$prefill_payload_bytes" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_prefill_keys" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_payload_bytes" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_delete_events" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_write_events" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_write_payload_bytes" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_percent" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_churn_keys" =~ ^[1-9][0-9]*$ &&
  "$maxmemory_churn_rounds" =~ ^[1-9][0-9]*$ ]]; then
  echo "MAXLEN, persistence, replication, background, prefill, or maxmemory controls are invalid" >&2
  exit 2
fi
if ((persistence_probe_events > maxlen || replication_probe_events > maxlen)); then
  echo "probe event counts must not exceed SATURATION_MAXLEN" >&2
  exit 2
fi
if [[ "$maxmemory_probe" == on ]]; then
  if ((maxmemory_percent >= 100)); then
    echo "SATURATION_MAXMEMORY_PERCENT must be less than 100" >&2
    exit 2
  fi
  if ((maxmemory_delete_events >= maxmemory_prefill_keys)); then
    echo "SATURATION_MAXMEMORY_DELETE_EVENTS must be smaller than the maxmemory prefill" >&2
    exit 2
  fi
  if ((maxmemory_delete_events > maxlen || maxmemory_write_events > maxlen)); then
    echo "maxmemory event counts must not exceed SATURATION_MAXLEN" >&2
    exit 2
  fi
  if [[ "$persistence_mode" != off || "$replication_mode" != off ]]; then
    echo "SATURATION_MAXMEMORY_PROBE=on requires persistence and replication off" >&2
    exit 2
  fi
fi

if [[ "$plan_only" == yes ]]; then
  SATURATION_PLAN_ONLY=yes \
  SATURATION_RESULTS_DIR="$results_dir" \
    "$repo_dir/bench/saturation.sh"
  exit 0
elif [[ "$plan_only" != no ]]; then
  echo "SATURATION_PLAN_ONLY must be yes or no" >&2
  exit 2
fi

for tool in aws base64 git gzip jq shasum tar terraform; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

mkdir -p "$results_dir/raw"
terraform_output="$(terraform -chdir="$root_dir" output -json)"
printf '%s\n' "$terraform_output" >"$results_dir/raw/terraform-output.json"
region="$(jq -r '.region.value' <<<"$terraform_output")"
server_id="$(jq -r '.server_instance_id.value' <<<"$terraform_output")"
server_ip="$(jq -r '.server_private_ip.value' <<<"$terraform_output")"
loadgen_id="$(jq -r '.loadgen_instance_id.value' <<<"$terraform_output")"
replica_enabled="$(jq -r '.replica_enabled.value' <<<"$terraform_output")"
replica_id="$(jq -r '.replica_instance_id.value // empty' <<<"$terraform_output")"
replica_ip="$(jq -r '.replica_private_ip.value // empty' <<<"$terraform_output")"
module_image="$(jq -r '.module_image.value' <<<"$terraform_output")"
redis_image="$(jq -r '.loadgen_image.value' <<<"$terraform_output")"
memtier_image="$(jq -r '.memtier_image.value' <<<"$terraform_output")"

if [[ "$replication_mode" == replica && "$replica_enabled" != true ]]; then
  echo "SATURATION_REPLICATION_MODE=replica requires TF_VAR_replica_enabled=true" >&2
  exit 2
fi
if [[ "$replication_mode" == off && "$replica_enabled" != false ]]; then
  echo "SATURATION_REPLICATION_MODE=off requires TF_VAR_replica_enabled=false" >&2
  exit 2
fi

instance_ids=("$server_id" "$loadgen_id")
if [[ "$replication_mode" == replica ]]; then
  instance_ids+=("$replica_id")
fi
instance_ids_csv="$(IFS=,; printf '%s' "${instance_ids[*]}")"

aws_args=(--region "$region")
if [[ -n "${AWS_PROFILE:-}" ]]; then aws_args+=(--profile "$AWS_PROFILE"); fi
aws_cli() { aws "${aws_args[@]}" "$@"; }

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
      --comment "eventstream saturation $label" \
      --parameters "file://$parameters_file" \
      --timeout-seconds 21600 \
      --query 'Command.CommandId' \
      --output text
  )"
  rm -f "$parameters_file"
  for _ in $(seq 1 4320); do
    status="$(
      aws_cli ssm get-command-invocation \
        --command-id "$command_id" --instance-id "$instance_id" \
        --query Status --output text 2>/dev/null || true
    )"
    case "$status" in Success | Failed | Cancelled | TimedOut) break ;; esac
    sleep 5
  done
  aws_cli ssm get-command-invocation \
    --command-id "$command_id" --instance-id "$instance_id" \
    --output json >"${output_base}.invocation.json"
  jq -r '.StandardOutputContent' "${output_base}.invocation.json" >"${output_base}.stdout"
  jq -r '.StandardErrorContent' "${output_base}.invocation.json" >"${output_base}.stderr"
  if [[ "$status" != Success ]]; then
    echo "remote command failed ($label): $status" >&2
    cat "${output_base}.stderr" >&2
    return 1
  fi
}

wait_for_ssm() {
  local instance_id="$1"
  local count
  for _ in $(seq 1 90); do
    count="$(
      aws_cli ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$instance_id" \
        --query 'length(InstanceInformationList)' --output text
    )"
    [[ "$count" == 1 ]] && return 0
    sleep 5
  done
  echo "instance did not register with SSM: $instance_id" >&2
  return 1
}

encode_file() { base64 <"$1" | tr -d '\n'; }
decode_base64() {
  if base64 --decode </dev/null >/dev/null 2>&1; then base64 --decode; else base64 -D; fi
}

fetch_remote_file() {
  local instance_id="$1" label="$2" remote_path="$3" local_path="$4"
  local metadata size expected_sha actual_sha offset=0 chunk_size=15000 chunk_base
  run_remote_for_fetch "$instance_id" "fetch-$label-metadata" \
    "stat -c '%s' '$remote_path'
sha256sum '$remote_path' | awk '{ print \$1 }'" \
    "$results_dir/raw/fetch-$label-metadata"
  metadata="$(cat "$results_dir/raw/fetch-$label-metadata.stdout")"
  size="$(sed -n '1p' <<<"$metadata")"
  expected_sha="$(sed -n '2p' <<<"$metadata")"
  : >"$local_path"
  while ((offset < size)); do
    chunk_base="$results_dir/raw/fetch-$label-$offset"
    run_remote_for_fetch "$instance_id" "fetch-$label-$offset" \
      "dd if='$remote_path' iflag=skip_bytes,count_bytes skip='$offset' count='$chunk_size' 2>/dev/null | base64 -w0" \
      "$chunk_base"
    decode_base64 <"$chunk_base.stdout" >>"$local_path"
    offset=$((offset + chunk_size))
  done
  actual_sha="$(shasum -a 256 "$local_path" | awk '{ print $1 }')"
  [[ "$actual_sha" == "$expected_sha" ]] || {
    echo "checksum mismatch fetching $remote_path" >&2
    return 1
  }
}

run_remote_for_fetch() {
  local instance_id="$1" label="$2" command="$3" output_base="$4"
  local attempt
  for attempt in 1 2 3 4 5; do
    if run_remote "$instance_id" "$label" "$command" "$output_base"; then
      return 0
    fi
    if ((attempt < 5)); then
      echo "retrying remote fetch ($label), attempt $((attempt + 1)) of 5" >&2
      sleep $((attempt * 2))
    fi
  done
  echo "remote fetch exhausted retries ($label)" >&2
  return 1
}

echo "waiting for EC2 health and SSM..." >&2
aws_cli ec2 wait instance-status-ok --instance-ids "${instance_ids[@]}"
wait_for_ssm "$server_id"
wait_for_ssm "$loadgen_id"
if [[ "$replication_mode" == replica ]]; then
  wait_for_ssm "$replica_id"
fi
bootstrap="cloud-init status --wait && test -f /var/lib/eventstream-smoke/ready"
run_remote "$server_id" bootstrap-server "$bootstrap" "$results_dir/raw/bootstrap-server"
run_remote "$loadgen_id" bootstrap-loadgen "$bootstrap" "$results_dir/raw/bootstrap-loadgen"
if [[ "$replication_mode" == replica ]]; then
  run_remote "$replica_id" bootstrap-replica "$bootstrap" "$results_dir/raw/bootstrap-replica"
fi

git_commit="$(git -C "$repo_dir" rev-parse HEAD)"
if [[ "$module_source_commit" == HEAD ]]; then module_source_commit="$git_commit"; fi
module_archive="${module_source_repo%/}/archive/${module_source_commit}.tar.gz"
module_override=/var/lib/eventstream-smoke/libredis_event_stream_module.so
module_build_b64="$(encode_file "$root_dir/scripts/remote-module-build.sh")"
run_remote "$server_id" module-build \
  "printf '%s' '$module_build_b64' | base64 --decode >/tmp/eventstream-module-build.sh
chmod 0700 /tmp/eventstream-module-build.sh
/tmp/eventstream-module-build.sh '$module_archive' '$module_override'" \
  "$results_dir/raw/module-build"
module_artifact_json="$(cat "$results_dir/raw/module-build.stdout")"
jq -e '.sha256 | test("^[0-9a-f]{64}$")' <<<"$module_artifact_json" >/dev/null

replica_module_artifact_json=null
if [[ "$replication_mode" == replica ]]; then
  replica_module_override=/var/lib/eventstream-smoke/libredis_event_stream_module.so
  run_remote "$replica_id" module-build-replica \
    "printf '%s' '$module_build_b64' | base64 --decode >/tmp/eventstream-module-build.sh
chmod 0700 /tmp/eventstream-module-build.sh
/tmp/eventstream-module-build.sh '$module_archive' '$replica_module_override'" \
    "$results_dir/raw/module-build-replica"
  replica_module_artifact_json="$(cat "$results_dir/raw/module-build-replica.stdout")"
  jq -e --arg primary_sha "$(jq -r '.sha256' <<<"$module_artifact_json")" \
    '.sha256 == $primary_sha' <<<"$replica_module_artifact_json" >/dev/null
fi

server_b64="$(encode_file "$root_dir/scripts/remote-server.sh")"
run_remote "$server_id" server-start \
  "printf '%s' '$server_b64' | base64 --decode >/tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh s0 '$module_image' '$maxlen' '$module_override' 65536 64 1 '$persistence_mode' reset" \
  "$results_dir/raw/server-start"

if [[ "$replication_mode" == replica ]]; then
  replica_server_b64="$(encode_file "$root_dir/scripts/remote-replica-server.sh")"
  run_remote "$replica_id" replica-start \
    "printf '%s' '$replica_server_b64' | base64 --decode >/tmp/eventstream-replica-server.sh
chmod 0700 /tmp/eventstream-replica-server.sh
/tmp/eventstream-replica-server.sh '$server_ip' '$module_image' '$replica_module_override'" \
    "$results_dir/raw/replica-start"
fi

# Capture the same versioned physical environment manifest as the request-counted
# and soak runners before introducing benchmark load.
environment_b64="$(encode_file "$root_dir/scripts/remote-environment.sh")"
run_remote "$server_id" environment-server \
  "printf '%s' '$environment_b64' | base64 --decode >/tmp/eventstream-environment.sh
chmod 0700 /tmp/eventstream-environment.sh
/tmp/eventstream-environment.sh server '$server_ip' '$redis_image' '$environment_network_samples'" \
  "$results_dir/raw/environment-server"
run_remote "$loadgen_id" environment-loadgen \
  "printf '%s' '$environment_b64' | base64 --decode >/tmp/eventstream-environment.sh
chmod 0700 /tmp/eventstream-environment.sh
/tmp/eventstream-environment.sh loadgen '$server_ip' '$redis_image' '$environment_network_samples'" \
  "$results_dir/raw/environment-loadgen"
printf 'null\n' >"$results_dir/raw/environment-replica.stdout"
if [[ "$replication_mode" == replica ]]; then
  run_remote "$replica_id" environment-replica \
    "printf '%s' '$environment_b64' | base64 --decode >/tmp/eventstream-environment.sh
chmod 0700 /tmp/eventstream-environment.sh
/tmp/eventstream-environment.sh replica '$server_ip' '$redis_image' '$environment_network_samples'" \
    "$results_dir/raw/environment-replica"
fi
aws_cli ec2 describe-instances --instance-ids "${instance_ids[@]}" \
  --output json >"$results_dir/raw/ec2-instances.json"
aws_cli ec2 describe-volumes \
  --filters "Name=attachment.instance-id,Values=$instance_ids_csv" \
  --output json >"$results_dir/raw/ec2-volumes.json"
jq -n \
  --arg schema_version 1 \
  --arg collected_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg server_id "$server_id" --arg loadgen_id "$loadgen_id" \
  --arg replica_id "$replica_id" \
  --arg region "$region" \
  --arg availability_zone "$(jq -r '.availability_zone.value' <<<"$terraform_output")" \
  --arg vpc_id "$(jq -r '.vpc_id.value' <<<"$terraform_output")" \
  --arg subnet_id "$(jq -r '.subnet_id.value' <<<"$terraform_output")" \
  --arg server_private_ip "$server_ip" \
  --arg replica_private_ip "$replica_ip" \
  --arg expiry_stop_schedule_arn "$(jq -r '.expiry_stop_schedule_arn.value' <<<"$terraform_output")" \
  --arg expires_at "$(jq -r '.expires_at.value' <<<"$terraform_output")" \
  --arg root_volume_type "$(jq -r '.root_volume_type.value' <<<"$terraform_output")" \
  --argjson root_volume_gib "$(jq -r '.root_volume_gib.value' <<<"$terraform_output")" \
  --slurpfile server_host "$results_dir/raw/environment-server.stdout" \
  --slurpfile loadgen_host "$results_dir/raw/environment-loadgen.stdout" \
  --slurpfile replica_host "$results_dir/raw/environment-replica.stdout" \
  --slurpfile instances "$results_dir/raw/ec2-instances.json" \
  --slurpfile volumes "$results_dir/raw/ec2-volumes.json" \
  -f "$root_dir/scripts/assemble-environment.jq" >"$results_dir/environment.json"

harness_url="${module_source_repo%/}/raw/${module_source_commit}/bench/saturation.sh"
remote_saturation_b64="$(encode_file "$root_dir/scripts/remote-saturation.sh")"
remote_result_dir=/var/lib/eventstream-smoke/saturation
remote_replica_ip=-
if [[ "$replication_mode" == replica ]]; then
  remote_replica_ip="$replica_ip"
fi

# Forward only documented workload controls. Connection, binaries, module path,
# and output location are owned by remote-saturation.sh.
remote_exports=""
for name in \
  SATURATION_RUN_ID SATURATION_SCENARIOS SATURATION_SELECTIVITIES SATURATION_CLIENT_LEVELS \
  SATURATION_THREAD_LEVELS SATURATION_PIPELINE_LEVELS SATURATION_REPETITIONS \
  SATURATION_RATE_LIMIT_LEVELS SATURATION_P99_BUDGET_MS \
  SATURATION_ACHIEVEMENT_RATIO SATURATION_WORKLOAD_NAME SATURATION_PRECISE_TIMER \
  SATURATION_WARMUP_SECONDS SATURATION_MEASUREMENT_SECONDS \
  SATURATION_PAYLOAD_BYTES SATURATION_KEYSPACE SATURATION_MAXLEN SATURATION_SEED \
  SATURATION_PERSISTENCE_MODE SATURATION_BACKGROUND_ACTION \
  SATURATION_BACKGROUND_DELAY_SECONDS SATURATION_BACKGROUND_TIMEOUT_SECONDS \
  SATURATION_PREFILL_KEYS SATURATION_PREFILL_PAYLOAD_BYTES \
  SATURATION_EXPIRY_KEYS SATURATION_EXPIRY_TTL_MIN_MS \
  SATURATION_EXPIRY_TTL_SPREAD_MS SATURATION_EXPIRY_CLIENTS \
  SATURATION_EXPIRY_TIMEOUT_SECONDS SATURATION_EXPIRY_POLL_SECONDS \
  SATURATION_CAPTURE_EVENTS SATURATION_EXPECTED_EVENTS; do
  if [[ -n "${!name:-}" ]]; then
    printf -v quoted '%q' "${!name}"
    remote_exports+="export $name=$quoted
"
  fi
done

remote_workload_setup=""
if [[ -n "${SATURATION_COMMANDS_FILE:-}" ]]; then
  if [[ ! -f "$SATURATION_COMMANDS_FILE" ]]; then
    echo "SATURATION_COMMANDS_FILE does not exist: $SATURATION_COMMANDS_FILE" >&2
    exit 2
  fi
  if [[ "$(wc -c <"$SATURATION_COMMANDS_FILE")" -gt 8192 ]]; then
    echo "SATURATION_COMMANDS_FILE must be at most 8192 bytes for SSM transport" >&2
    exit 2
  fi
  commands_b64="$(encode_file "$SATURATION_COMMANDS_FILE")"
  remote_workload_setup="printf '%s' '$commands_b64' | base64 --decode >/tmp/eventstream-saturation-commands.json
export SATURATION_COMMANDS_FILE=/tmp/eventstream-saturation-commands.json
"
fi

run_remote "$loadgen_id" saturation \
  "$remote_exports
$remote_workload_setup
printf '%s' '$remote_saturation_b64' | base64 --decode >/tmp/eventstream-remote-saturation.sh
chmod 0700 /tmp/eventstream-remote-saturation.sh
/tmp/eventstream-remote-saturation.sh '$server_ip' '$redis_image' '$memtier_image' '/usr/local/lib/redis/modules/libredis_event_stream_module.so' '$harness_url' '$remote_result_dir' '$remote_replica_ip'" \
  "$results_dir/raw/saturation"

printf 'null\n' >"$results_dir/maxmemory-probe.json"
if [[ "$maxmemory_probe" == on ]]; then
  maxmemory_harness_url="${module_source_repo%/}/raw/${module_source_commit}/bench/maxmemory-pressure.sh"
  maxmemory_remote_b64="$(encode_file "$root_dir/scripts/remote-maxmemory-probe.sh")"
  run_remote "$server_id" maxmemory-pressure-probe \
    "printf '%s' '$maxmemory_remote_b64' | base64 --decode >/tmp/eventstream-remote-maxmemory-probe.sh
chmod 0700 /tmp/eventstream-remote-maxmemory-probe.sh
export PRESSURE_PREFILL_KEYS='$maxmemory_prefill_keys'
export PRESSURE_PAYLOAD_BYTES='$maxmemory_payload_bytes'
export PRESSURE_DELETE_EVENTS='$maxmemory_delete_events'
export PRESSURE_WRITE_EVENTS='$maxmemory_write_events'
export PRESSURE_WRITE_PAYLOAD_BYTES='$maxmemory_write_payload_bytes'
export PRESSURE_MAXMEMORY_PERCENT='$maxmemory_percent'
export PRESSURE_CHURN_KEYS='$maxmemory_churn_keys'
export PRESSURE_CHURN_ROUNDS='$maxmemory_churn_rounds'
export PRESSURE_MAXLEN='$maxlen'
/tmp/eventstream-remote-maxmemory-probe.sh \
  '$maxmemory_harness_url' \
  /usr/local/lib/redis/modules/libredis_event_stream_module.so \
  /var/lib/eventstream-smoke/maxmemory-pressure.json" \
    "$results_dir/raw/maxmemory-pressure-probe"
  fetch_remote_file "$server_id" maxmemory-pressure \
    /var/lib/eventstream-smoke/maxmemory-pressure.json \
    "$results_dir/maxmemory-probe.json"
  jq -e '.passed == true and (.cases | length) == 4' \
    "$results_dir/maxmemory-probe.json" >/dev/null
fi

printf 'null\n' >"$results_dir/replication-probe.json"
if [[ "$replication_mode" == replica ]]; then
  replication_probe_b64="$(encode_file "$root_dir/scripts/remote-replication-probe.sh")"
  run_remote "$replica_id" replication-pause-probe \
    "printf '%s' '$replication_probe_b64' | base64 --decode >/tmp/eventstream-replication-probe.sh
chmod 0700 /tmp/eventstream-replication-probe.sh
/tmp/eventstream-replication-probe.sh '$server_ip' '$redis_image' '$maxlen' '$replication_probe_events' '$replication_pause_seconds'" \
    "$results_dir/raw/replication-pause-probe"
  jq -e '.passed == true' \
    "$results_dir/raw/replication-pause-probe.stdout" >/dev/null
  cp "$results_dir/raw/replication-pause-probe.stdout" \
    "$results_dir/replication-probe.json"
fi

persistence_probe_b64="$(encode_file "$root_dir/scripts/remote-persistence-probe.sh")"
run_remote "$server_id" persistence-restart-probe \
  "printf '%s' '$persistence_probe_b64' | base64 --decode >/tmp/eventstream-persistence-probe.sh
chmod 0700 /tmp/eventstream-persistence-probe.sh
/tmp/eventstream-persistence-probe.sh '$persistence_mode' '$module_image' '$module_override' '$maxlen' '$persistence_probe_events'" \
  "$results_dir/raw/persistence-restart-probe"
jq -e --arg mode "$persistence_mode" \
  '.passed == true and .mode == $mode' \
  "$results_dir/raw/persistence-restart-probe.stdout" >/dev/null
cp "$results_dir/raw/persistence-restart-probe.stdout" \
  "$results_dir/restart-probe.json"

fetch_remote_file "$loadgen_id" saturation-summary \
  /var/lib/eventstream-smoke/saturation-summary.tar.gz \
  "$results_dir/raw/saturation-summary.tar.gz"
tar -xzf "$results_dir/raw/saturation-summary.tar.gz" -C "$results_dir"

fetch_remote_file "$loadgen_id" saturation-results \
  /var/lib/eventstream-smoke/saturation-results.tar.gz \
  "$results_dir/raw/saturation-results.tar.gz"
tar -xzf "$results_dir/raw/saturation-results.tar.gz" -C "$results_dir"

module_sha="$(jq -r '.sha256' <<<"$module_artifact_json")"
replica_module_sha=""
if [[ "$replication_mode" == replica ]]; then
  replica_module_sha="$(jq -r '.sha256' <<<"$replica_module_artifact_json")"
fi
jq \
  --arg commit "$module_source_commit" \
  --arg module_sha "$module_sha" \
  --arg replica_module_sha "$replica_module_sha" \
  --arg replication_mode "$replication_mode" \
  --arg image "$memtier_image" \
  --slurpfile environment "$results_dir/environment.json" \
  --slurpfile restart_probe "$results_dir/restart-probe.json" \
  --slurpfile replication_probe "$results_dir/replication-probe.json" \
  --slurpfile maxmemory_probe "$results_dir/maxmemory-probe.json" \
  '.source.git_commit = $commit |
   .source.module_sha256 = $module_sha |
   .source.replica_module_sha256 =
     (if $replica_module_sha == "" then null else $replica_module_sha end) |
   .generator.image = $image |
   .topology = {replication_mode: $replication_mode} |
   .environment = $environment[0] |
   .validation.persistence_restart = $restart_probe[0] |
   .validation.replication_pause = $replication_probe[0] |
   .validation.maxmemory_pressure = $maxmemory_probe[0]' \
  "$results_dir/manifest.json" >"$results_dir/manifest.enriched.json"
mv "$results_dir/manifest.enriched.json" "$results_dir/manifest.json"

echo "results: $results_dir"
