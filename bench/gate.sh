#!/usr/bin/env bash
# CI regression gate over the JSON that `BENCH_JSON=... bench/run.sh` emits.
#
# Hosted runners are noisy, so every check is RELATIVE within one run (ratios
# are self-normalizing) or an exact counter, never absolute ops/sec:
#
#   S1/S0 ops ratio >= 0.90    SPEC.md section 11 budgets the non-capturing
#                              gate tax at "within a few percent"; the 10%
#                              headroom absorbs runner noise while still
#                              catching a regression in the ~100 ns guarded
#                              path, which would show up as tens of percent.
#   S2/S0 ops ratio >= 0.40    section 11 budgets full capture at "can
#                              approach half of baseline"; 0.40 leaves noise
#                              headroom below the 0.50 budget.
#   S4/S0 ops ratio >= 0.40    every maxlen value runs the same full-capture
#                              workload, so the S2 budget applies; a trim-cost
#                              regression at any maxlen breaches it.
#   S3 dropped == 0            mass expiry must lose nothing outside OOM and
#                              cluster migration windows (SPEC.md section 9).
#   S3 forwarded == expected   every staggered expiration reached the stream.
#   S3 fg p99 ceiling          module fg p99 <= 10x the no-module fg p99, with
#                              the no-module side floored at 2 ms so one lucky
#                              baseline rep cannot make the ratio flake; the
#                              median-of-reps in run.sh does the smoothing.
#                              This only catches pathological drain
#                              regressions by design.
#
# Thresholds are deliberately loose starting points (issue #70 says start
# loose and tighten with observed variance); tightening one is a reviewed
# change to this file.
#
# Usage: bench/gate.sh results.json
# Markdown report on stdout (numbers reported even when passing); exit 1 on
# any breach.
set -euo pipefail

JSON="${1:?usage: bench/gate.sh results.json}"
command -v jq >/dev/null || { echo "gate needs jq"; exit 1; }
jq -e 'type == "array"' "$JSON" >/dev/null || { echo "not a results array: $JSON"; exit 1; }

val() { # scenario field -> value, or empty when absent/null
  jq -r --arg s "$1" --arg f "$2" 'first(.[] | select(.scenario==$s))[$f] // empty' "$JSON"
}

FAIL=0

ratio_check() { # label numerator denominator min-ratio
  local r ok=FAIL
  r="$(awk -v n="${2:-0}" -v d="${3:-0}" 'BEGIN{ if (d+0<=0 || n+0<=0) print "NA"; else printf "%.3f", n/d }')"
  if [[ "$r" != NA ]]; then
    if awk -v r="$r" -v m="$4" 'BEGIN{ exit !(r+0 >= m+0) }'; then ok=pass; fi
  fi
  if [[ "$ok" == FAIL ]]; then FAIL=1; fi
  printf '| %s | %s | >= %s | %s |\n' "$1" "$r" "$4" "$ok"
}

eq_check() { # label value expected
  local ok=FAIL
  if [[ -n "${2:-}" && "$2" == "$3" ]]; then ok=pass; fi
  if [[ "$ok" == FAIL ]]; then FAIL=1; fi
  printf '| %s | %s | == %s | %s |\n' "$1" "${2:-missing}" "$3" "$ok"
}

ceiling_check() { # label value ceiling
  local ok=FAIL
  if awk -v v="${2:-0}" -v c="$3" 'BEGIN{ exit !(v+0 > 0 && v+0 <= c+0) }'; then ok=pass; fi
  if [[ "$ok" == FAIL ]]; then FAIL=1; fi
  printf '| %s | %s | <= %s | %s |\n' "$1" "${2:-missing}" "$3" "$ok"
}

echo "## Bench results"
echo
echo "| Scenario | ops/sec | p50 (ms) | p99 (ms) | drain (s) | forwarded | dropped |"
echo "|---|---|---|---|---|---|---|"
jq -r '.[] | "| \(.scenario) | \(.ops_per_sec) | \(.p50_ms) | \(.p99_ms) | \(.drain_s // "-") | \(.forwarded // "-") | \(.dropped // "-") |"' "$JSON"
echo
echo "## Gate checks"
echo
echo "| Check | Value | Threshold | Result |"
echo "|---|---|---|---|"

S0_OPS="$(val s0 ops_per_sec)"
ratio_check "S1/S0 ops (non-capturing gate tax)" "$(val s1 ops_per_sec)" "$S0_OPS" 0.90
ratio_check "S2/S0 ops (full-capture budget)" "$(val s2 ops_per_sec)" "$S0_OPS" 0.40

# S4 scenario ids carry the maxlen value; discover them rather than hardcode,
# so a BENCH_MAXLENS override upstream stays gated.
while read -r sc ops; do
  ratio_check "$sc/S0 ops (full-capture budget)" "$ops" "$S0_OPS" 0.40
done < <(jq -r '.[] | select(.scenario | startswith("s4_maxlen_")) | "\(.scenario) \(.ops_per_sec)"' "$JSON")

eq_check "S3 dropped" "$(val s3_module dropped)" 0
eq_check "S3 forwarded" "$(val s3_module forwarded)" "$(val s3_module expected_forwarded)"
S3_CEIL="$(awk -v b="$(val s3_nomodule p99_ms)" 'BEGIN{ f=(b+0>2.0)?b:2.0; printf "%.3f", 10*f }')"
ceiling_check "S3 module fg p99 (ms)" "$(val s3_module p99_ms)" "$S3_CEIL"

echo
if [[ "$FAIL" -ne 0 ]]; then
  echo "**GATE: FAIL** - a threshold above was breached; see bench/gate.sh for rationale."
  exit 1
fi
echo "**GATE: pass** - all relative thresholds held."
