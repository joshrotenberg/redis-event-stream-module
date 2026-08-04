#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: remote-saturation.sh <server-ip> <redis-image> <memtier-image> <module-path-in-server> <harness-url> <result-dir>" >&2
  exit 2
fi

server_ip="$1"
redis_image="$2"
memtier_image="$3"
module_path="$4"
harness_url="$5"
result_dir="$6"
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

docker rm -f "$cli_container" >/dev/null 2>&1 || true
docker run --detach --name "$cli_container" --network host \
  "$redis_image" sleep infinity >/dev/null

cat >/tmp/eventstream-saturation-bin/redis-cli <<'WRAPPER'
#!/usr/bin/env bash
# Keep stdin attached for redis-cli --pipe during the mass-expiry preload.
exec docker exec -i eventstream-saturation-cli redis-cli "$@"
WRAPPER
cat >/tmp/eventstream-saturation-bin/memtier_benchmark <<'WRAPPER'
#!/usr/bin/env bash
exec docker run --rm --network host \
  --volume /var/lib/eventstream-smoke/saturation:/var/lib/eventstream-smoke/saturation \
  "$SATURATION_MEMTIER_IMAGE" "$@"
WRAPPER
chmod 0700 \
  /tmp/eventstream-saturation-bin/redis-cli \
  /tmp/eventstream-saturation-bin/memtier_benchmark

export SATURATION_MEMTIER_IMAGE="$memtier_image"
export SATURATION_START_SERVER=no
export SATURATION_BUILD_MODULE=no
export SATURATION_HOST="$server_ip"
export SATURATION_PORT=6379
export SATURATION_SERVER_MODULE_PATH="$module_path"
export SATURATION_CLI_BIN=/tmp/eventstream-saturation-bin/redis-cli
export SATURATION_MEMTIER_BIN=/tmp/eventstream-saturation-bin/memtier_benchmark
export SATURATION_RESULTS_DIR="$result_dir"

/tmp/eventstream-saturation.sh
tar -C "$result_dir" -czf /var/lib/eventstream-smoke/saturation-results.tar.gz .
