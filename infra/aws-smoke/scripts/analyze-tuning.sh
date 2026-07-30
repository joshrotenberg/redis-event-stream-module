#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: analyze-tuning.sh <output-directory> <result.json>..." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_dir="$1"
shift

for result in "$@"; do
  jq -e '
    (.schema_version >= 4) and
    (.trials | type == "array") and
    all(.trials[];
      has("async_configuration") and
      (.module | has("async_worker_errors")))
  ' "$result" >/dev/null
done

mkdir -p "$output_dir"
analysis="$output_dir/analysis.json"
summary="$output_dir/summary.md"

jq -s -f "$script_dir/analyze-tuning.jq" "$@" >"$analysis"
jq -e '.all_healthy == true' "$analysis" >/dev/null

{
  echo "# Preview envelope tuning summary"
  echo
  echo "Trials: $(jq -r '.trial_count' "$analysis")"
  echo
  echo "The Pareto test maximizes end-to-end throughput while minimizing p99"
  echo "latency and total Redis-process core utilization. Comparisons are made"
  echo "only within the same client level."
  echo
  echo "## All configurations"
  echo
  echo "| Configuration | Repetitions | End-to-end ops/s | p99 ms | Total core % | Envelope size | Envelope events % | Fallback % |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|"
  jq -r '
    def round3:
      if type == "number" then (. * 1000 | round) / 1000 else . end;
    .summaries[] |
    "| \(.id) | \(.repetitions) | \(.end_to_end_ops_per_sec.median | floor) | \(.p99_ms.median | round3) | \(.server_total_core_percent.median | round3) | \((.achieved_envelope_size.median // "-") | round3) | \((.envelope_event_percent.median // "-") | round3) | \((.fallback_percent.median // "-") | round3) |"
  ' "$analysis"
  echo
  echo "## Pareto frontier"
  echo
  jq -r '
    def round3:
      if type == "number" then (. * 1000 | round) / 1000 else . end;
    .pareto_frontier[] |
    "- `\(.id)`: \(.end_to_end_ops_per_sec.median | floor) end-to-end ops/s, p99 \(.p99_ms.median | round3) ms, \(.server_total_core_percent.median | round3)% total core"
  ' "$analysis"
} >"$summary"

echo "analysis: $analysis"
echo "summary:  $summary"
