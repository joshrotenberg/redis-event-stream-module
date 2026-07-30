#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: remote-server.sh <s0|s1|s2> <module-image> <maxlen>" >&2
  exit 2
fi

scenario="$1"
module_image="$2"
maxlen="$3"
module_path="/usr/local/lib/redis/modules/libredis_event_stream_module.so"

docker rm -f eventstream-server >/dev/null 2>&1 || true

server_args=(
  redis-server
  --bind 0.0.0.0
  --protected-mode no
  --save ""
  --appendonly no
)

case "$scenario" in
  s0)
    ;;
  s1)
    server_args+=(--loadmodule "$module_path")
    ;;
  s2)
    server_args+=(
      --loadmodule "$module_path"
      events set
      maxlen "$maxlen"
    )
    ;;
  *)
    echo "unknown scenario: $scenario" >&2
    exit 2
    ;;
esac

docker run \
  --detach \
  --name eventstream-server \
  --network host \
  "$module_image" \
  "${server_args[@]}" >/dev/null

for _ in $(seq 1 60); do
  if docker exec eventstream-server redis-cli PING 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.25
done

if ! docker exec eventstream-server redis-cli PING 2>/dev/null | grep -q PONG; then
  docker logs eventstream-server >&2
  exit 1
fi

module_list="$(docker exec eventstream-server redis-cli --raw MODULE LIST)"
if [[ "$scenario" == "s0" ]]; then
  if grep -q eventstream <<<"$module_list"; then
    echo "eventstream unexpectedly loaded in baseline scenario" >&2
    exit 1
  fi
else
  if ! grep -q eventstream <<<"$module_list"; then
    docker logs eventstream-server >&2
    echo "eventstream did not load for $scenario" >&2
    exit 1
  fi
fi

echo "server ready: $scenario"
