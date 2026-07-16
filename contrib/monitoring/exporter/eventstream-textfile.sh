#!/bin/sh
# eventstream-textfile.sh — emit the module's INFO counters as Prometheus
# exposition-format text.
#
# Why this exists (and why not a stock redis_exporter scrape): module INFO
# sections do NOT appear in `INFO` or `INFO all` — only `INFO everything`,
# `INFO eventstream`, `INFO eventstream_stats`, or `INFO modules` (SPEC.md
# section 13). redis_exporter's `--include-modules-metrics` does run
# `INFO MODULES`, so it sees this section, but it then filters every field
# through an allow-list of metric names it already knows; the `eventstream_*`
# fields are not in that list, so it exports only the `module_info` gauge
# (module presence and version), never the counters. There is no flag to add
# arbitrary INFO field names to that allow-list. See contrib/monitoring/README.md.
#
# This script closes that gap: it runs `INFO eventstream` directly and writes
# every numeric field as a metric named exactly as in the INFO section
# (`eventstream_forwarded`, `eventstream_dropped`, ...), so an operator can map
# each metric 1:1 to the SPEC.md section 13 counter table. Counter fields are
# left without a `_total` suffix on purpose, to keep that mapping literal.
#
# Usage:
#   Write once to a node_exporter textfile-collector directory (atomic rename):
#     eventstream-textfile.sh > "$TEXTFILE_DIR/eventstream.prom.$$" \
#       && mv "$TEXTFILE_DIR/eventstream.prom.$$" "$TEXTFILE_DIR/eventstream.prom"
#   Or loop as a sidecar (see docker-compose.yml).
#
# Environment:
#   REDIS_HOST      (default 127.0.0.1)
#   REDIS_PORT      (default 6379)
#   REDIS_PASSWORD  (optional; sent with -a, warnings silenced)
#   REDIS_CLI       (default redis-cli; set to an absolute path if needed)
#   REDIS_CLI_ARGS  (optional; extra args appended verbatim, e.g. --tls)
#
# Counters reset to 0 when the module reloads (process-lifetime, SPEC.md
# section 13); this is expected — Prometheus rate()/increase() handle counter
# resets, which is why the rules never read absolute counter values.

set -eu

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"

set -- -h "$REDIS_HOST" -p "$REDIS_PORT"
if [ -n "${REDIS_PASSWORD:-}" ]; then
	set -- "$@" --no-auth-warning -a "$REDIS_PASSWORD"
fi
# shellcheck disable=SC2086 # REDIS_CLI_ARGS is intentionally word-split.
set -- "$@" ${REDIS_CLI_ARGS:-}

# `INFO eventstream` returns only this module's section; a missing module or an
# unreachable server yields empty output or an error, which we surface as
# eventstream_up 0 rather than emitting stale or partial counters.
if info="$("$REDIS_CLI" "$@" INFO eventstream 2>/dev/null)" \
	&& printf '%s' "$info" | grep -q '^eventstream_enabled:'; then
	up=1
else
	up=0
	info=""
fi

printf '# HELP eventstream_up Whether INFO eventstream was scraped successfully (1) or not (0).\n'
printf '# TYPE eventstream_up gauge\n'
printf 'eventstream_up %s\n' "$up"

[ "$up" -eq 1 ] || exit 0

# Fields that are point-in-time state, not monotonic event tallies. Everything
# else numeric is a counter. `cluster_pinned_tag` is a string (the hash tag, or
# empty) and has no numeric representation, so it is skipped — its presence is
# still visible via `INFO eventstream` and the log.
printf '%s\n' "$info" | awk -F: '
BEGIN {
	gauge["eventstream_enabled"] = 1
	gauge["eventstream_eviction_risk"] = 1
	gauge["eventstream_active_streams"] = 1
	gauge["eventstream_cluster_per_node"] = 1
	gauge["eventstream_last_error_time"] = 1
}
/^eventstream_/ {
	key = $1
	# Value is the remainder after the first colon, CR stripped.
	val = substr($0, index($0, ":") + 1)
	sub(/\r$/, "", val)
	# Numeric only: integer or float, optionally signed. Skips the empty
	# cluster_pinned_tag and any future string-valued field.
	if (val !~ /^-?[0-9]+(\.[0-9]+)?$/)
		next
	type = (key in gauge) ? "gauge" : "counter"
	printf "# TYPE %s %s\n", key, type
	printf "%s %s\n", key, val
}
'
