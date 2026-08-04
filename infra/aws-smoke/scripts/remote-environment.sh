#!/usr/bin/env bash
# Emit one host's reproducibility metadata. Server roles measure intrinsic
# scheduler latency inside their Redis container; the load-generator role
# records private-network Redis PING latency to the primary.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: remote-environment.sh <server|replica|loadgen> <server-ip> <loadgen-image> <samples>" >&2
  exit 2
fi

role="$1"
server_ip="$2"
loadgen_image="$3"
samples="$4"

if [[ "$role" != server && "$role" != replica && "$role" != loadgen ]]; then
  echo "role must be server, replica, or loadgen" >&2
  exit 2
fi
if ! [[ "$samples" =~ ^[1-9][0-9]*$ ]]; then
  echo "samples must be a positive integer" >&2
  exit 2
fi

os_id="$(awk -F= '$1 == "ID" { gsub(/\"/, "", $2); print $2; exit }' /etc/os-release)"
os_version="$(awk -F= '$1 == "VERSION_ID" { gsub(/\"/, "", $2); print $2; exit }' /etc/os-release)"
kernel="$(uname -r)"
architecture="$(uname -m)"
logical_cpus="$(getconf _NPROCESSORS_ONLN)"
cpu_model="$(lscpu | awk -F: '/^Model name:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }')"
docker_version="$(docker version --format '{{.Server.Version}}')"
loadgen_version=""

intrinsic_raw=""
intrinsic_max_us="null"
network_raw=""
network_ops_per_sec="null"
network_p50_ms="null"
network_p95_ms="null"
network_p99_ms="null"
network_max_ms="null"
network_command="null"

if [[ "$role" == server || "$role" == replica ]]; then
  redis_container=eventstream-server
  [[ "$role" == replica ]] && redis_container=eventstream-replica
  intrinsic_raw="$(
    docker exec "$redis_container" redis-cli --intrinsic-latency 1 2>&1
  )"
  intrinsic_max_us="$(
    awk '/Max latency so far:/ { value = $(NF - 1) } END { print value + 0 }' \
      <<<"$intrinsic_raw"
  )"
else
  loadgen_version="$(
    docker run --rm --network host "$loadgen_image" redis-benchmark --version
  )"
  network_command="$(
    jq -cn --args '$ARGS.positional' -- \
      redis-benchmark -h "$server_ip" -p 6379 -t ping_inline \
      -n "$samples" -c 1 --threads 1 --csv
  )"
  network_raw="$(
    docker run --rm --network host "$loadgen_image" \
      redis-benchmark -h "$server_ip" -p 6379 -t ping_inline \
      -n "$samples" -c 1 --threads 1 --csv
  )"
  read -r network_ops_per_sec network_p50_ms network_p95_ms \
    network_p99_ms network_max_ms < <(
      awk -F, 'NR == 2 { gsub(/"/, ""); print $2, $5, $6, $7, $8 }' \
        <<<"$network_raw"
    )
fi

jq -n \
  --arg role "$role" \
  --arg os_id "$os_id" \
  --arg os_version "$os_version" \
  --arg kernel "$kernel" \
  --arg architecture "$architecture" \
  --argjson logical_cpus "$logical_cpus" \
  --arg cpu_model "$cpu_model" \
  --arg docker_version "$docker_version" \
  --arg loadgen_image "$loadgen_image" \
  --arg loadgen_version "$loadgen_version" \
  --arg intrinsic_raw "$intrinsic_raw" \
  --argjson intrinsic_max_us "$intrinsic_max_us" \
  --arg network_raw "$network_raw" \
  --argjson network_samples "$samples" \
  --argjson network_ops_per_sec "$network_ops_per_sec" \
  --argjson network_p50_ms "$network_p50_ms" \
  --argjson network_p95_ms "$network_p95_ms" \
  --argjson network_p99_ms "$network_p99_ms" \
  --argjson network_max_ms "$network_max_ms" \
  --argjson network_command "$network_command" \
  '{
    role: $role,
    os: {id: $os_id, version: $os_version},
    kernel: $kernel,
    architecture: $architecture,
    cpu: {logical_cpus: $logical_cpus, model: $cpu_model},
    docker_version: $docker_version,
    load_generator: {
      image: (if $role == "loadgen" then $loadgen_image else null end),
      version: (if $role == "loadgen" then $loadgen_version else null end)
    },
    intrinsic_latency: {
      duration_seconds: (if $role == "loadgen" then null else 1 end),
      max_us: $intrinsic_max_us,
      raw: (if $role == "loadgen" then null else $intrinsic_raw end)
    },
    private_network_ping: {
      command: $network_command,
      samples: (if $role == "loadgen" then $network_samples else null end),
      ops_per_sec: $network_ops_per_sec,
      p50_ms: $network_p50_ms,
      p95_ms: $network_p95_ms,
      p99_ms: $network_p99_ms,
      max_ms: $network_max_ms,
      raw: (if $role == "loadgen" then $network_raw else null end)
    }
  }'
