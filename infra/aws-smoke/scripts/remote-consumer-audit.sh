#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: remote-consumer-audit.sh <server-ip> <client-image> <label> <idle-exit-ms> <result-dir>" >&2
  exit 2
fi

server_ip="$1"
client_image="$2"
label="$3"
idle_exit_ms="$4"
result_dir="$5"
container="eventstream-audit-$label"

mkdir -p "$result_dir"
docker rm -f "$container" >/dev/null 2>&1 || true
docker run \
  --name "$container" \
  --network host \
  --volume "$result_dir:/results" \
  "$client_image" \
  /eventstream-client \
  --url "redis://$server_ip:6379" \
  consume \
  --events set \
  --from 0 \
  --quiet \
  --idle-exit-ms "$idle_exit_ms" \
  --checkpoint "/results/$label.json" \
  --read-count 2000 >"$result_dir/$label.stdout" \
  2>"$result_dir/$label.stderr"
docker rm "$container" >/dev/null

cat "$result_dir/$label.json"
