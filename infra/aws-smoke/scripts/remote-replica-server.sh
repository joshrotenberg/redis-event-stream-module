#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: remote-replica-server.sh <primary-ip> <module-image> <module-so|->" >&2
  exit 2
fi

primary_ip="$1"
module_image="$2"
module_override="$3"
container=eventstream-replica
module_path=/usr/local/lib/redis/modules/libredis_event_stream_module.so

docker rm -f "$container" >/dev/null 2>&1 || true

docker_args=(
  --detach
  --name "$container"
  --network host
)
if [[ "$module_override" != "-" ]]; then
  test -f "$module_override"
  docker_args+=(--volume "$module_override:$module_path:ro")
fi

docker run \
  "${docker_args[@]}" \
  "$module_image" \
  redis-server \
  --bind 0.0.0.0 \
  --protected-mode no \
  --save "" \
  --appendonly no \
  --enable-module-command yes \
  --replicaof "$primary_ip" 6379 \
  --loadmodule "$module_path" >/dev/null

for _ in $(seq 1 120); do
  role="$(
    docker exec "$container" redis-cli --raw INFO replication 2>/dev/null |
      awk -F: '$1 == "role" { gsub(/\r/, "", $2); print $2; exit }'
  )"
  link="$(
    docker exec "$container" redis-cli --raw INFO replication 2>/dev/null |
      awk -F: '$1 == "master_link_status" { gsub(/\r/, "", $2); print $2; exit }'
  )"
  if [[ "$role" == slave && "$link" == up ]]; then
    break
  fi
  sleep 0.25
done

if [[ "${role:-}" != slave || "${link:-}" != up ]]; then
  docker logs "$container" >&2
  echo "replica did not connect to primary $primary_ip" >&2
  exit 1
fi

if ! docker exec "$container" redis-cli --raw MODULE LIST | grep -q eventstream; then
  docker logs "$container" >&2
  echo "eventstream did not load on replica" >&2
  exit 1
fi

echo "replica ready: $primary_ip:6379"
