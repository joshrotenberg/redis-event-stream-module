#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="$root_dir/tests/fixtures"
test_dir="$(mktemp -d)"

cleanup() {
  rm -rf "$test_dir"
}
trap cleanup EXIT

BENCH_PLAN_ONLY=yes \
RESULTS_DIR="$test_dir/benchmark" \
RUN_ID=contract-test \
  "$root_dir/lab.sh" run >"$test_dir/benchmark-plan.txt"
awk '
  NR == 1 && $1 == "order" { header = 1 }
  NR > 1 && $2 ~ /^s[012]/ { trials += 1 }
  END { exit !(header && trials == 3) }
' "$test_dir/benchmark-plan.txt"

SATURATION_PLAN_ONLY=yes \
SATURATION_REPETITIONS=1 \
SATURATION_RESULTS_DIR="$test_dir/saturation" \
  "$root_dir/lab.sh" saturation >"$test_dir/saturation-plan.txt"
awk '
  NR == 1 && $1 == "order" { header = 1 }
  NR > 1 && $2 == "s0" { s0 += 1 }
  NR > 1 && $2 == "s1" { s1 += 1 }
  NR > 1 && $2 == "s2" { s2 += 1 }
  NR > 1 && $2 == "expiry-s0" { expiry_s0 += 1 }
  NR > 1 && $2 == "expiry-s2" { expiry_s2 += 1 }
  END { exit !(header && s0 == 1 && s1 == 1 && s2 == 4 && expiry_s0 == 1 && expiry_s2 == 1) }
' "$test_dir/saturation-plan.txt"
# These are literal wrapper source assertions; expansion would invalidate them.
# shellcheck disable=SC2016
grep -Fq 'if [[ "$arg" == --pipe ]]; then' \
  "$root_dir/scripts/remote-saturation.sh"
grep -Fq 'exec docker exec -i eventstream-saturation-cli redis-cli "$@"' \
  "$root_dir/scripts/remote-saturation.sh"
grep -Fq 'exec docker exec eventstream-saturation-cli redis-cli "$@"' \
  "$root_dir/scripts/remote-saturation.sh"

SOAK_PLAN_ONLY=yes RUN_ID=contract-soak-test \
  "$root_dir/lab.sh" soak >"$test_dir/soak-plan.json"
jq -e '
  .soak_seconds == 1800 and
  (.phase_percentages | map(.percent) | add) == 100 and
  (.restart_probe.modes | length) == 2
' "$test_dir/soak-plan.json" >/dev/null

jq -n \
  --arg schema_version 1 \
  --arg collected_at 2026-08-03T12:00:00Z \
  --arg server_id i-server \
  --arg loadgen_id i-loadgen \
  --arg region us-west-2 \
  --arg availability_zone us-west-2a \
  --arg vpc_id vpc-test \
  --arg subnet_id subnet-test \
  --arg server_private_ip 10.87.0.10 \
  --arg expiry_stop_schedule_arn arn:aws:scheduler:test \
  --arg expires_at 2026-08-03T16:00:00Z \
  --arg root_volume_type gp3 \
  --argjson root_volume_gib 16 \
  --slurpfile server_host "$fixture_dir/server-host.json" \
  --slurpfile loadgen_host "$fixture_dir/loadgen-host.json" \
  --slurpfile instances "$fixture_dir/instances.json" \
  --slurpfile volumes "$fixture_dir/volumes.json" \
  -f "$root_dir/scripts/assemble-environment.jq" \
  >"$test_dir/environment.json"

jq -e '
  .schema_version == 1 and
  .topology.kind == "standalone" and
  .hosts.server.intrinsic_latency.max_us == 4 and
  .hosts.load_generator.private_network_ping.p99_ms == 0.04 and
  (.instances | length) == 2 and
  (.volumes | length) == 2 and
  .hard_stop.scheduler_arn == "arn:aws:scheduler:test"
' "$test_dir/environment.json" >/dev/null
