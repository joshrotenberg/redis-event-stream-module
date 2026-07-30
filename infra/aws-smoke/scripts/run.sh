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
filtered_repetitions="${BENCH_FILTERED_REPETITIONS:-$repetitions}"
order_seed="${BENCH_ORDER_SEED:-260}"
run_id="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
results_dir="${RESULTS_DIR:-$root_dir/results/$run_id}"

for tool in aws base64 cksum git jq sort terraform; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

for value in \
  "$requests" "$threads" "$payload" "$keyspace" "$maxlen" \
  "$repetitions" "$filtered_repetitions"; do
  if ! positive_integer "$value"; then
    echo "benchmark values must be positive integers: $value" >&2
    exit 2
  fi
done

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

unsorted_plan="$results_dir/raw/trial-plan.unsorted.tsv"
trial_plan="$results_dir/raw/trial-plan.tsv"
: >"$unsorted_plan"

add_trial() {
  local scenario="$1"
  local trial_clients="$2"
  local repetition="$3"
  local order_key

  order_key="$(
    printf '%s' "$order_seed:$scenario:$trial_clients:$repetition" |
      cksum |
      awk '{ print $1 }'
  )"
  printf '%s\t%s\t%s\t%s\n' \
    "$order_key" "$scenario" "$trial_clients" "$repetition" >>"$unsorted_plan"
}

for trial_clients in $client_levels; do
  for repetition in $(seq 1 "$repetitions"); do
    add_trial s0 "$trial_clients" "$repetition"
    add_trial s2 "$trial_clients" "$repetition"
  done
  for repetition in $(seq 1 "$filtered_repetitions"); do
    add_trial s1 "$trial_clients" "$repetition"
  done
done

LC_ALL=C sort -n -k1,1 "$unsorted_plan" >"$trial_plan"

trial_files=()
trial_number=0
while IFS=$'\t' read -r _ scenario trial_clients repetition; do
  trial_number=$((trial_number + 1))
  trial_id="${scenario}-c${trial_clients}-r${repetition}"
  echo "running trial $trial_number: $trial_id..."

  server_command="printf '%s' '$server_script_b64' | base64 --decode > /tmp/eventstream-server.sh
chmod 0700 /tmp/eventstream-server.sh
/tmp/eventstream-server.sh '$scenario' '$module_image' '$maxlen'"
  run_remote \
    "$server_id" \
    "server-$trial_id" \
    "$server_command" \
    "$results_dir/raw/server-$trial_id"

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

  trial_file="$results_dir/$trial_id.json"
  jq \
    --arg trial_id "$trial_id" \
    --argjson repetition "$repetition" \
    --argjson order "$trial_number" \
    '. + {trial_id: $trial_id, repetition: $repetition, order: $order}' \
    "$benchmark_file" >"$trial_file"
  jq -e . "$trial_file" >/dev/null
  trial_files+=("$trial_file")
done <"$trial_plan"

jq -s '.' "${trial_files[@]}" >"$results_dir/trials.json"

jq -e '
  all(.[];
    (.module.events_lost == 0) and
    (.module.dropped == 0) and
    (.module.handler_panics == 0) and
    (if .scenario == "s0"
     then .module.loaded == false
     elif .scenario == "s1"
     then
       (.module.loaded == true) and
       (.module.forwarded == 0) and
       (.module.skipped_filtered == .workload.requests)
     else
       (.module.loaded == true) and
       (.module.forwarded == .workload.requests)
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

  sort_by([.scenario, .workload.clients]) |
  group_by([.scenario, .workload.clients]) |
  map(
    . as $trials |
    {
      scenario: $trials[0].scenario,
      clients: $trials[0].workload.clients,
      repetitions: ($trials | length),
      ops_per_sec:
        ($trials | map(.result.ops_per_sec) | distribution),
      p99_ms:
        ($trials | map(.result.p99_ms) | distribution),
      max_ms:
        ($trials | map(.result.max_ms) | distribution),
      server_main_thread_core_percent:
        ($trials | map(.server.main_thread_core_percent) | distribution),
      load_generator_cpu_percent_avg:
        ($trials | map(.load_generator.cpu_percent_avg) | distribution),
      load_generator_cpu_percent_max:
        ($trials | map(.load_generator.cpu_percent_max) | distribution),
      events_lost: ($trials | map(.module.events_lost) | add),
      dropped: ($trials | map(.module.dropped) | add),
      handler_panics: ($trials | map(.module.handler_panics) | add)
    }
  )
' "$results_dir/trials.json" >"$results_dir/summary.json"

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -n \
  --arg schema_version "2" \
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
  --slurpfile trials "$results_dir/trials.json" \
  --slurpfile summary "$results_dir/summary.json" \
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
    trials: $trials[0],
    summary: $summary[0]
  }' >"$results_dir/result.json"

echo
jq -r '
  ["scenario", "clients", "reps", "ops/s median", "ops/s range", "p99 median", "server core %", "loadgen CPU %"],
  (.summary[] | [
    .scenario,
    (.clients | tostring),
    (.repetitions | tostring),
    (.ops_per_sec.median | tostring),
    "\(.ops_per_sec.min)-\(.ops_per_sec.max)",
    (.p99_ms.median | tostring),
    (.server_main_thread_core_percent.median | tostring),
    (.load_generator_cpu_percent_avg.median | tostring)
  ]) | @tsv
' "$results_dir/result.json" | column -t -s $'\t'

echo
echo "result: $results_dir/result.json"
