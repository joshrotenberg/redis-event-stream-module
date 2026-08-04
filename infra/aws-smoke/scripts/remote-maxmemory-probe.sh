#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: remote-maxmemory-probe.sh <core-script-url> <module-path-in-server> <result-path>" >&2
  exit 2
fi

core_script_url="$1"
module_path="$2"
result_path="$3"
container=eventstream-server
wrapper_dir=/tmp/eventstream-pressure-bin
core_script=/tmp/eventstream-maxmemory-pressure.sh

if [[ "$result_path" != /var/lib/eventstream-smoke/maxmemory-pressure.json ]]; then
  echo "result path must be /var/lib/eventstream-smoke/maxmemory-pressure.json" >&2
  exit 2
fi
if ! docker exec "$container" redis-cli PING 2>/dev/null | grep -q PONG; then
  echo "eventstream-server is not ready" >&2
  exit 1
fi

curl --fail --location --silent --show-error \
  "$core_script_url" >"$core_script"
chmod 0700 "$core_script"
install -d -m 0755 "$wrapper_dir"
cat >"$wrapper_dir/redis-cli" <<'WRAPPER'
#!/usr/bin/env bash
for arg in "$@"; do
  if [[ "$arg" == --pipe ]]; then
    exec docker exec -i eventstream-server redis-cli "$@"
  fi
done
exec docker exec eventstream-server redis-cli "$@"
WRAPPER
chmod 0700 "$wrapper_dir/redis-cli"

export PRESSURE_HOST=127.0.0.1
export PRESSURE_PORT=6379
export PRESSURE_CLI_BIN="$wrapper_dir/redis-cli"
export PRESSURE_SERVER_MODULE_PATH="$module_path"
export PRESSURE_RESULT_PATH="$result_path"

"$core_script" >/dev/null
jq -e '.passed == true' "$result_path" >/dev/null
jq '{passed, totals, cases: [.cases[] | {name, passed}]}' "$result_path"
