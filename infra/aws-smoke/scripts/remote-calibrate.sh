#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: remote-calibrate.sh <server-ip> <loadgen-image> <target-rps> <requests>" >&2
  exit 2
fi

server_ip="$1"
loadgen_image="$2"
target_rps="$3"
requests="$4"
work_dir="$(mktemp -d)"

cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

for clients in 1 2 4 8; do
  threads="$clients"
  if ((threads > 4)); then
    threads=4
  fi
  output="$work_dir/c${clients}.csv"
  docker run --rm --network host "$loadgen_image" \
    redis-benchmark \
    -h "$server_ip" \
    -p 6379 \
    -t set \
    -n "$requests" \
    -c "$clients" \
    --threads "$threads" \
    -d 64 \
    -r 100000 \
    --csv >"$output"

  row="$(
    awk -F',' '
      NR == 2 {
        gsub(/"/, "")
        print
      }
    ' "$output"
  )"
  IFS=',' read -r _ ops_per_sec avg_ms min_ms p50_ms p95_ms p99_ms max_ms <<<"$row"
  jq -n \
    --argjson clients "$clients" \
    --argjson threads "$threads" \
    --argjson requests "$requests" \
    --argjson ops_per_sec "$ops_per_sec" \
    --argjson avg_ms "$avg_ms" \
    --argjson min_ms "$min_ms" \
    --argjson p50_ms "$p50_ms" \
    --argjson p95_ms "$p95_ms" \
    --argjson p99_ms "$p99_ms" \
    --argjson max_ms "$max_ms" \
    '{
      clients: $clients,
      threads: $threads,
      requests: $requests,
      ops_per_sec: $ops_per_sec,
      avg_ms: $avg_ms,
      min_ms: $min_ms,
      p50_ms: $p50_ms,
      p95_ms: $p95_ms,
      p99_ms: $p99_ms,
      max_ms: $max_ms
    }' >"$work_dir/c${clients}.json"
done

for _ in $(seq 1 1200); do
  depth="$(
    docker run --rm --network host "$loadgen_image" \
      redis-cli -h "$server_ip" -p 6379 --raw INFO eventstream |
      tr -d '\r' |
      awk -F: '$1 == "eventstream_async_queue_depth" { print $2; exit }'
  )"
  if [[ "${depth:-0}" == "0" ]]; then
    break
  fi
  sleep 0.05
done

jq -s \
  --argjson target_rps "$target_rps" \
  '
    def distance:
      if .ops_per_sec > $target_rps
      then .ops_per_sec - $target_rps
      else $target_rps - .ops_per_sec
      end;
    . as $candidates |
    {
      target_rps: $target_rps,
      candidates: $candidates,
      selected: ($candidates | min_by(distance))
    }
  ' "$work_dir"/c*.json
