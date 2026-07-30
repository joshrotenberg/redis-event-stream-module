#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$root_dir/../.." && pwd)"

requests="${BENCH_REQUESTS:-1000000}"
clients="${BENCH_CLIENTS:-50}"
threads="${BENCH_THREADS:-2}"
payload="${BENCH_PAYLOAD:-64}"
keyspace="${BENCH_KEYSPACE:-100000}"
maxlen="${BENCH_MAXLEN:-10000}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
results_dir="${RESULTS_DIR:-$root_dir/results/$run_id}"

for tool in aws base64 git jq terraform; do
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

server_script_b64="$(encode_file "$root_dir/scripts/remote-server.sh")"
benchmark_script_b64="$(encode_file "$root_dir/scripts/remote-benchmark.sh")"

scenario_files=()
for scenario in s0 s1 s2; do
  echo "running $scenario..."

  server_command="printf '%s' '$server_script_b64' | base64 --decode > /tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh '$scenario' '$module_image' '$maxlen'"
  run_remote \
    "$server_id" \
    "server-$scenario" \
    "$server_command" \
    "$results_dir/raw/server-$scenario"

  benchmark_command="printf '%s' '$benchmark_script_b64' | base64 --decode > /tmp/eventstream-benchmark.sh
chmod 0700 /tmp/eventstream-benchmark.sh
/tmp/eventstream-benchmark.sh '$scenario' '$server_ip' '$loadgen_image' '$requests' '$clients' '$threads' '$payload' '$keyspace'"
  run_remote \
    "$loadgen_id" \
    "benchmark-$scenario" \
    "$benchmark_command" \
    "$results_dir/raw/benchmark-$scenario"

  scenario_file="$results_dir/$scenario.json"
  cp "$results_dir/raw/benchmark-$scenario.stdout" "$scenario_file"
  jq -e . "$scenario_file" >/dev/null
  scenario_files+=("$scenario_file")
done

jq -s '.' "${scenario_files[@]}" >"$results_dir/scenarios.json"

jq -e '
  (map(select(.scenario == "s0"))[0].module.loaded == false) and
  (map(select(.scenario == "s1"))[0].module.loaded == true) and
  (map(select(.scenario == "s1"))[0].module.forwarded == 0) and
  (map(select(.scenario == "s2"))[0].module.loaded == true) and
  (map(select(.scenario == "s2"))[0].module.forwarded == map(select(.scenario == "s2"))[0].workload.requests) and
  (map(select(.scenario == "s2"))[0].module.events_lost == 0) and
  (map(select(.scenario == "s2"))[0].module.dropped == 0) and
  (map(select(.scenario == "s2"))[0].module.handler_panics == 0)
' "$results_dir/scenarios.json" >/dev/null

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -n \
  --arg schema_version "1" \
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
  --slurpfile scenarios "$results_dir/scenarios.json" \
  '{
    schema_version: ($schema_version | tonumber),
    run_id: $run_id,
    started_at: $started_at,
    completed_at: $completed_at,
    git_commit: $git_commit,
    environment: {
      region: $region,
      availability_zone: $availability_zone,
      ami_id: $ami_id,
      server_instance_type: $server_instance_type,
      loadgen_instance_type: $loadgen_instance_type,
      module_image: $module_image,
      loadgen_image: $loadgen_image,
      expires_at: $expires_at
    },
    scenarios: $scenarios[0]
  }' >"$results_dir/result.json"

echo
jq -r '
  ["scenario", "ops/sec", "p50 ms", "p99 ms", "forwarded", "lost", "dropped"],
  (.scenarios[] | [
    .scenario,
    (.result.ops_per_sec | tostring),
    (.result.p50_ms | tostring),
    (.result.p99_ms | tostring),
    (.module.forwarded | tostring),
    (.module.events_lost | tostring),
    (.module.dropped | tostring)
  ]) | @tsv
' "$results_dir/result.json" | column -t -s $'\t'

echo
echo "result: $results_dir/result.json"
