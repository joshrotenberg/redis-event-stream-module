#!/usr/bin/env bash
# Chaos and load scenarios for the cluster capture paths (SPEC.md section 10,
# issues #45/#46/#47). Each scenario stands up real servers, drives load through
# the example client, injects a topology change or failure, and asserts the
# data-safety property that must hold. These are heavier than the integration
# suite (tens of thousands of events, real reshards and failovers), so they live
# here rather than in `cargo test`.
#
# Scenarios:
#   reshard    40k events through a live slot migration: zero loss, one re-pin.
#   failover   kill a master; the promoted replica keeps the same tag, so the
#              destination stream name is stable and nothing double-captures.
#   massexpiry 50k expirations (the heaviest capture path): zero loss.
#   repeated   migrate a node's pinned slot several times in a row: one clean
#              re-pin per migration, capture continues throughout.
#
# Usage:
#   chaos/run.sh                 # all scenarios
#   chaos/run.sh reshard         # one scenario
#   REDIS_SERVER_BIN=/path/to/redis-server REDIS_CLI_BIN=/path/to/redis-cli \
#     chaos/run.sh               # pin to a specific build (defaults: PATH)
#
# CI knobs (default off, so local runs are unchanged): CHAOS_WORK_DIR pins the
# scratch dir to a known path instead of a fresh mktemp, and CHAOS_KEEP_WORK
# leaves it in place on exit so a failed scheduled run can upload the per-node
# redis.log / soak.log as an artifact (the .github/workflows/chaos.yml suite).
#
# `set -e` is deliberately omitted: scenarios kill nodes on purpose, so many
# commands return nonzero by design. Failures are detected by explicit checks.

# Scenarios are dispatched indirectly via "scenario_$s" at the bottom, so the
# linter sees the whole call graph as unreachable/never-invoked. The reported
# code differs by version (SC2317 unreachable, SC2329 never invoked), so both
# are disabled file-wide below.
# shellcheck disable=SC2317,SC2329

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

RS="${REDIS_SERVER_BIN:-redis-server}"
RC="${REDIS_CLI_BIN:-redis-cli}"
EXT=so
[[ "$(uname)" == "Darwin" ]] && EXT=dylib
MODULE="$PWD/target/release/libredis_event_stream_module.${EXT}"
EX="$PWD/target/release/examples/eventstream_client"
WORK="${CHAOS_WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK"
PORTS=()   # every server port we start, for cleanup

cleanup() {
    for p in "${PORTS[@]:-}"; do "$RC" -p "$p" shutdown nosave >/dev/null 2>&1; done
    pkill -9 -f "redis-server .*${WORK}" >/dev/null 2>&1
    [[ -n "${CHAOS_KEEP_WORK:-}" ]] || rm -rf "$WORK"
}
trap cleanup EXIT

fail_count=0
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; fail_count=$((fail_count + 1)); }

# Start one module-loaded server. Args: port, dir, extra redis-server args...
start_node() {
    local port="$1" dir="$2"; shift 2
    mkdir -p "$dir"
    "$RS" --port "$port" --dir "$dir" --save '' --enable-module-command yes \
        --daemonize yes --logfile "$dir/redis.log" "$@"
    PORTS+=("$port")
}

# Poll every given node until cluster_state:ok, up to ~12s.
wait_cluster_ok() {
    local _
    for _ in $(seq 1 30); do
        local ok=1 p
        for p in "$@"; do
            [[ "$("$RC" -p "$p" cluster info 2>/dev/null | grep -o cluster_state:ok)" == cluster_state:ok ]] || ok=0
        done
        [[ $ok -eq 1 ]] && return 0
        sleep 0.4
    done
    return 1
}

es_field() { "$RC" -p "$1" info eventstream 2>/dev/null | grep "eventstream_$2:" | cut -d: -f2 | tr -d '\r'; }

# Migrate a slot from one node to another via the SETSLOT dance.
# Args: slot, from_port, to_port, all_ports...
migrate_slot() {
    local slot="$1" from="$2" to="$3"; shift 3
    local fromid toid
    fromid="$("$RC" -p "$from" cluster myid)"
    toid="$("$RC" -p "$to" cluster myid)"
    "$RC" -p "$to" cluster setslot "$slot" importing "$fromid" >/dev/null
    "$RC" -p "$from" cluster setslot "$slot" migrating "$toid" >/dev/null
    local keys
    while :; do
        keys="$("$RC" -p "$from" cluster getkeysinslot "$slot" 100)"
        [[ -z "$keys" ]] && break
        echo "$keys" | xargs -r "$RC" -p "$from" migrate 127.0.0.1 "$to" "" 0 5000 keys >/dev/null 2>&1 || break
    done
    local p
    for p in "$to" "$from" "$@"; do "$RC" -p "$p" cluster setslot "$slot" node "$toid" >/dev/null; done
}

form_cluster() {  # replicas, port... ; uses redis-cli --cluster create
    local replicas="$1"; shift
    local addrs=() p
    for p in "$@"; do addrs+=("127.0.0.1:$p"); done
    "$RC" --cluster create "${addrs[@]}" --cluster-replicas "$replicas" --cluster-yes >/dev/null 2>&1
}

# ---------------------------------------------------------------------------

scenario_reshard() {
    echo "[reshard] 40k events through a live slot migration"
    local dir="$WORK/reshard" ports=(7601 7602 7603) p
    for p in "${ports[@]}"; do
        start_node "$p" "$dir/$p" --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout 2000 --loadmodule "$MODULE" events set cluster-streams per-node
    done
    sleep 1
    form_cluster 0 "${ports[@]}"
    wait_cluster_ok "${ports[@]}" || { fail "cluster did not form"; return; }

    "$EX" soak --url "redis://127.0.0.1:7601" --count 40000 --rate 3000 >"$dir/soak.log" 2>&1 &
    local soakpid=$!
    sleep 5
    local tag slot
    tag="$(es_field 7601 cluster_pinned_tag)"
    slot="$("$RC" -p 7601 cluster keyslot "{$tag}")"
    echo "  migrating node 7601 tag=$tag slot=$slot -> 7602 under load"
    migrate_slot "$slot" 7601 7602 7603
    wait "$soakpid"

    local repins
    repins=$(( $(es_field 7601 repins) + $(es_field 7602 repins) + $(es_field 7603 repins) ))
    if grep -q "every produced event was captured" "$dir/soak.log"; then
        pass "zero loss under a live reshard"
    else
        fail "events lost: $(grep -E 'captured|result' "$dir/soak.log" | tr '\n' ' ')"
    fi
    if [[ "$repins" -ge 1 ]]; then pass "re-pinned after the slot moved (repins=$repins)"; else fail "expected a re-pin, got repins=$repins"; fi
}

scenario_failover() {
    echo "[failover] kill a master, promoted replica keeps the tag"
    local dir="$WORK/failover" ports=(7611 7612 7613 7614 7615 7616) p
    for p in "${ports[@]}"; do
        start_node "$p" "$dir/$p" --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout 2000 --loadmodule "$MODULE" events set cluster-streams per-node
    done
    sleep 1
    form_cluster 1 "${ports[@]}"
    wait_cluster_ok 7612 || { fail "cluster did not form"; return; }

    "$EX" produce --url "redis://127.0.0.1:7611" --sets 300 >/dev/null 2>&1
    local tag deadid replica
    tag="$(es_field 7611 cluster_pinned_tag)"
    deadid="$("$RC" -p 7611 cluster myid)"
    replica="$("$RC" -p 7612 cluster nodes | awk -v m="$deadid" '$3 ~ /slave/ && $4==m {split($2,a,"@"); split(a[1],b,":"); print b[2]}')"
    if [[ -z "$tag" || -z "$replica" ]]; then fail "no tag or replica found (tag=$tag replica=$replica)"; return; fi
    echo "  master 7611 tag=$tag, replica=$replica; killing 7611"
    "$RC" -p 7611 shutdown nosave >/dev/null 2>&1

    local promoted=0 _
    for _ in $(seq 1 60); do
        local failed st
        failed="$("$RC" -p 7612 cluster nodes 2>/dev/null | awk -v id="$deadid" '$1==id && $3 ~ /fail/{print "y"}')"
        st="$("$RC" -p 7612 cluster info 2>/dev/null | grep -o cluster_state:ok)"
        [[ "$failed" == y && "$st" == cluster_state:ok ]] && { promoted=1; break; }
        sleep 0.4
    done
    [[ $promoted -eq 1 ]] || { fail "replica did not promote"; return; }

    "$EX" produce --url "redis://127.0.0.1:7612" --sets 300 >/dev/null 2>&1
    local newtag
    newtag="$(es_field "$replica" cluster_pinned_tag)"
    if [[ "$newtag" == "$tag" ]]; then
        pass "promoted replica re-derived the same tag ($newtag): stream name stable"
    else
        fail "tag changed across failover: old=$tag new=$newtag"
    fi
}

scenario_massexpiry() {
    echo "[massexpiry] 50k expirations (heaviest capture path)"
    local dir="$WORK/massexpiry" port=7620
    start_node "$port" "$dir/$port" --loadmodule "$MODULE" events expired maxlen 0
    sleep 1
    "$EX" produce --url "redis://127.0.0.1:$port" --burst 50000 --ttl-ms 800 >/dev/null 2>&1
    local fwd _
    for _ in $(seq 1 40); do fwd="$(es_field "$port" forwarded)"; [[ "$fwd" -ge 50000 ]] && break; sleep 0.3; done
    local dropped
    dropped="$(es_field "$port" dropped)"
    if [[ "$fwd" -eq 50000 && "$dropped" -eq 0 ]]; then
        pass "captured all 50000 expirations with zero drops"
    else
        fail "forwarded=$fwd dropped=$dropped (expected 50000, 0)"
    fi
}

scenario_repeated() {
    echo "[repeated] migrate a node's pinned slot several times in a row"
    local dir="$WORK/repeated" ports=(7631 7632 7633) p
    for p in "${ports[@]}"; do
        start_node "$p" "$dir/$p" --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout 2000 --loadmodule "$MODULE" events set cluster-streams per-node
    done
    sleep 1
    form_cluster 0 "${ports[@]}"
    wait_cluster_ok "${ports[@]}" || { fail "cluster did not form"; return; }

    local rounds=3 r ok=1 produced=0
    "$EX" produce --url "redis://127.0.0.1:7631" --sets 200 >/dev/null 2>&1
    produced=$((produced + 200))
    for r in $(seq 1 "$rounds"); do
        local tag slot
        tag="$(es_field 7631 cluster_pinned_tag)"
        [[ -z "$tag" ]] && { ok=0; break; }
        slot="$("$RC" -p 7631 cluster keyslot "{$tag}")"
        migrate_slot "$slot" 7631 7632 7633
        "$EX" produce --url "redis://127.0.0.1:7632" --sets 200 >/dev/null 2>&1
        produced=$((produced + 200))
        local newtag
        newtag="$(es_field 7631 cluster_pinned_tag)"
        echo "  round $r: tag $tag -> $newtag"
        [[ "$newtag" != "$tag" && -n "$newtag" ]] || ok=0
    done

    local repins captured
    repins="$(es_field 7631 repins)"
    captured=$(( $(es_field 7631 forwarded) + $(es_field 7632 forwarded) + $(es_field 7633 forwarded) ))
    if [[ $ok -eq 1 && "$repins" -ge "$rounds" ]]; then
        pass "$repins clean re-pins over $rounds migrations, capture continued"
    else
        fail "re-pin sequence broke (ok=$ok repins=$repins want>=$rounds)"
    fi
    if [[ "$captured" -eq "$produced" ]]; then
        pass "every event captured across the churn ($captured/$produced)"
    else
        fail "captured $captured of $produced across the churn"
    fi
}

# ---------------------------------------------------------------------------

echo "building module and example client (release)..."
cargo build --release >/dev/null 2>&1 || { echo "module build failed"; exit 1; }
cargo build --release --example eventstream_client >/dev/null 2>&1 || { echo "example build failed"; exit 1; }

want="${1:-all}"
for s in reshard failover massexpiry repeated; do
    if [[ "$want" == all || "$want" == "$s" ]]; then
        pkill -9 -f "redis-server .*${WORK}" >/dev/null 2>&1; sleep 1
        "scenario_$s"
    fi
done

echo ""
if [[ $fail_count -eq 0 ]]; then
    echo "all scenarios passed"
else
    echo "$fail_count check(s) failed"
fi
exit "$fail_count"
