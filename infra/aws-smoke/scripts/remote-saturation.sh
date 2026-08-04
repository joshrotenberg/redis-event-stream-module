#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 || $# -gt 7 ]]; then
  echo "usage: remote-saturation.sh <server-ip> <redis-image> <memtier-image> <module-path-in-server> <harness-url> <result-dir> [replica-ip|-]" >&2
  exit 2
fi

server_ip="$1"
redis_image="$2"
memtier_image="$3"
module_path="$4"
harness_url="$5"
result_dir="$6"
replica_ip="${7:--}"
cli_container="eventstream-saturation-cli"

if [[ "$result_dir" != /var/lib/eventstream-smoke/saturation ]]; then
  echo "result directory must be /var/lib/eventstream-smoke/saturation" >&2
  exit 2
fi

cleanup() {
  docker rm -f "$cli_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

rm -rf "$result_dir"
install -d -m 0755 "$result_dir" /tmp/eventstream-saturation-bin
curl --fail --location --silent --show-error \
  "$harness_url" >/tmp/eventstream-saturation.sh
chmod 0700 /tmp/eventstream-saturation.sh
classifier_url="${harness_url%/saturation.sh}/saturation/classify-knee.jq"
curl --fail --location --silent --show-error \
  "$classifier_url" >/tmp/eventstream-classify-knee.jq

docker rm -f "$cli_container" >/dev/null 2>&1 || true
docker run --detach --name "$cli_container" --network host \
  "$redis_image" sleep infinity >/dev/null

cat >/tmp/eventstream-saturation-bin/redis-cli <<'WRAPPER'
#!/usr/bin/env bash
# Keep stdin attached only for redis-cli --pipe during the mass-expiry preload.
# Attaching it to ordinary commands would consume the harness plan redirected
# into the outer trial loop.
for arg in "$@"; do
  if [[ "$arg" == --pipe ]]; then
    exec docker exec -i eventstream-saturation-cli redis-cli "$@"
  fi
done
exec docker exec eventstream-saturation-cli redis-cli "$@"
WRAPPER
cat >/tmp/eventstream-saturation-bin/memtier_benchmark <<'WRAPPER'
#!/usr/bin/env bash
exec docker run --rm --network host \
  --env EVENT_PRECISE_TIMER \
  --volume /var/lib/eventstream-smoke/saturation:/var/lib/eventstream-smoke/saturation \
  "$SATURATION_MEMTIER_IMAGE" "$@"
WRAPPER
cat >/tmp/eventstream-saturation-metrics.sh <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail

read -r _ user nice system idle iowait irq softirq steal _ </proc/stat
replica=null
if [[ "${SATURATION_REPLICA_IP:-}" != "-" ]]; then
  replica_cpu="$(docker exec eventstream-saturation-cli redis-cli -h "$SATURATION_REPLICA_IP" --raw INFO cpu)"
  replica_memory="$(docker exec eventstream-saturation-cli redis-cli -h "$SATURATION_REPLICA_IP" --raw INFO memory)"
  replica_replication="$(docker exec eventstream-saturation-cli redis-cli -h "$SATURATION_REPLICA_IP" --raw INFO replication)"
  replica="$(
    jq -n \
      --argjson used_cpu_sys "$(awk -F: '$1 == "used_cpu_sys" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_cpu")" \
      --argjson used_cpu_user "$(awk -F: '$1 == "used_cpu_user" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_cpu")" \
      --argjson used_memory "$(awk -F: '$1 == "used_memory" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_memory")" \
      --argjson used_memory_rss "$(awk -F: '$1 == "used_memory_rss" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_memory")" \
      --arg role "$(awk -F: '$1 == "role" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_replication")" \
      --arg link_status "$(awk -F: '$1 == "master_link_status" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_replication")" \
      --argjson sync_in_progress "$(awk -F: '$1 == "master_sync_in_progress" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_replication")" \
      --argjson master_offset "$(awk -F: '$1 == "master_repl_offset" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_replication")" \
      --argjson replica_offset "$(awk -F: '$1 == "slave_repl_offset" { gsub(/\r/, "", $2); print $2; exit }' <<<"$replica_replication")" \
      '{used_cpu_sys: $used_cpu_sys, used_cpu_user: $used_cpu_user,
        used_memory_bytes: $used_memory, used_memory_rss_bytes: $used_memory_rss,
        role: $role, link_status: $link_status, sync_in_progress: $sync_in_progress,
        master_offset: $master_offset, replica_offset: $replica_offset}'
  )"
fi
jq -n \
  --arg schema linux-proc-stat-v1 \
  --argjson epoch_ms "$(date +%s%3N)" \
  --argjson logical_cpus "$(getconf _NPROCESSORS_ONLN)" \
  --argjson user "$user" \
  --argjson nice "$nice" \
  --argjson system "$system" \
  --argjson idle "$idle" \
  --argjson iowait "$iowait" \
  --argjson irq "$irq" \
  --argjson softirq "$softirq" \
  --argjson steal "$steal" \
  --argjson replica "$replica" \
  '{schema: $schema, epoch_ms: $epoch_ms, logical_cpus: $logical_cpus,
    cpu_ticks: {user: $user, nice: $nice, system: $system, idle: $idle,
      iowait: $iowait, irq: $irq, softirq: $softirq, steal: $steal},
    replica: $replica}'
WRAPPER
chmod 0700 \
  /tmp/eventstream-saturation-bin/redis-cli \
  /tmp/eventstream-saturation-bin/memtier_benchmark \
  /tmp/eventstream-saturation-metrics.sh

export SATURATION_MEMTIER_IMAGE="$memtier_image"
export SATURATION_REPLICA_IP="$replica_ip"
export SATURATION_START_SERVER=no
export SATURATION_BUILD_MODULE=no
export SATURATION_HOST="$server_ip"
export SATURATION_PORT=6379
export SATURATION_SERVER_MODULE_PATH="$module_path"
export SATURATION_CLI_BIN=/tmp/eventstream-saturation-bin/redis-cli
export SATURATION_MEMTIER_BIN=/tmp/eventstream-saturation-bin/memtier_benchmark
export SATURATION_METRICS_HOOK=/tmp/eventstream-saturation-metrics.sh
export SATURATION_KNEE_CLASSIFIER=/tmp/eventstream-classify-knee.jq
export SATURATION_RESULTS_DIR="$result_dir"

/tmp/eventstream-saturation.sh
tar -C "$result_dir" -czf /var/lib/eventstream-smoke/saturation-summary.tar.gz \
  manifest.json trials.json summary.json knee.json result.json
tar -C "$result_dir" -czf /var/lib/eventstream-smoke/saturation-results.tar.gz .
