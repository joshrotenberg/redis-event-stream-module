#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 7 || $# -gt 9 ]]; then
  echo "usage: remote-server.sh <scenario> <module-image> <maxlen> <module-so|-> <queue-capacity> <batch-size> <max-wait-ms> [off|rdb|aof-everysec|aof-always] [reset|preserve]" >&2
  exit 2
fi

scenario="$1"
module_image="$2"
maxlen="$3"
module_override="$4"
queue_capacity="$5"
batch_size="$6"
max_wait_ms="$7"
persistence_mode="${8:-off}"
data_action="${9:-reset}"
module_path="/usr/local/lib/redis/modules/libredis_event_stream_module.so"
data_dir="/var/lib/eventstream-smoke/persistence-$persistence_mode"

case "$persistence_mode" in
  off | rdb | aof-everysec | aof-always) ;;
  *)
    echo "persistence mode must be off, rdb, aof-everysec, or aof-always" >&2
    exit 2
    ;;
esac
case "$data_action" in
  reset | preserve) ;;
  *)
    echo "data action must be reset or preserve" >&2
    exit 2
    ;;
esac

docker rm -f eventstream-server >/dev/null 2>&1 || true

if [[ "$persistence_mode" != off ]]; then
  if [[ "$data_action" == reset ]]; then
    rm -rf "$data_dir"
  fi
  install -d -m 0777 "$data_dir"
fi

server_args=(
  redis-server
  --bind 0.0.0.0
  --protected-mode no
  --save ""
  --enable-module-command yes
)

case "$persistence_mode" in
  off)
    server_args+=(--appendonly no)
    ;;
  rdb)
    server_args+=(--dir /data --appendonly no)
    ;;
  aof-everysec)
    server_args+=(
      --dir /data
      --appendonly yes
      --appendfsync everysec
      --auto-aof-rewrite-percentage 0
    )
    ;;
  aof-always)
    server_args+=(
      --dir /data
      --appendonly yes
      --appendfsync always
      --auto-aof-rewrite-percentage 0
    )
    ;;
esac

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
  s2-sync)
    server_args+=(
      --loadmodule "$module_path"
      events set
      maxlen "$maxlen"
      write-mode sync
    )
    ;;
  s2-individual | s2-envelope)
    write_mode="${scenario#s2-}"
    server_args+=(
      --loadmodule "$module_path"
      events set
      maxlen "$maxlen"
      write-mode "$write_mode"
      async-queue-capacity "$queue_capacity"
      async-batch-size "$batch_size"
      async-max-wait-ms "$max_wait_ms"
    )
    ;;
  *)
    echo "unknown scenario: $scenario" >&2
    exit 2
    ;;
esac

docker_args=(
  --detach
  --name eventstream-server
  --network host
)
if [[ "$module_override" != "-" ]]; then
  test -f "$module_override"
  docker_args+=(--volume "$module_override:$module_path:ro")
fi
if [[ "$persistence_mode" != off ]]; then
  docker_args+=(--volume "$data_dir:/data")
fi

docker run \
  "${docker_args[@]}" \
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

echo "server ready: $scenario ($persistence_mode, $data_action)"
