#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
usage: lab.sh <init|plan|up|run|saturation|soak|campaign|down|orphans> [terraform arguments]

Environment:
  AWS_PROFILE   optional AWS CLI/profile selection
  TF_VAR_owner  required owner tag for Terraform operations
  AWS_REGION    orphan-check region (default: us-west-2)

Examples:
  ./lab.sh init
  ./lab.sh plan
  ./lab.sh up
  ./lab.sh run
  ./lab.sh saturation
  ./lab.sh soak
  ./lab.sh campaign -auto-approve
  ./lab.sh down
  ./lab.sh orphans
USAGE
}

command="${1:-}"
if [[ -z "$command" ]]; then
  usage
  exit 2
fi
shift

case "$command" in
  init)
    terraform -chdir="$root_dir" init "$@"
    ;;
  plan)
    terraform -chdir="$root_dir" init
    terraform -chdir="$root_dir" plan "$@"
    ;;
  up)
    terraform -chdir="$root_dir" init
    terraform -chdir="$root_dir" apply "$@"
    ;;
  run)
    "$root_dir/scripts/run.sh" "$@"
    ;;
  saturation)
    "$root_dir/scripts/saturation.sh" "$@"
    ;;
  soak)
    "$root_dir/scripts/soak.sh" "$@"
    ;;
  campaign)
    campaign_args=("$@")
    cleanup_needed=true
    cleanup_campaign() {
      local status="$?"
      trap - EXIT
      if [[ "$cleanup_needed" == true ]]; then
        echo "cleaning up campaign resources..." >&2
        terraform -chdir="$root_dir" destroy "${campaign_args[@]}" || true
        "$root_dir/scripts/orphans.sh" || true
      fi
      exit "$status"
    }
    trap cleanup_campaign EXIT

    terraform -chdir="$root_dir" init
    terraform -chdir="$root_dir" apply "${campaign_args[@]}"
    "$root_dir/scripts/run.sh"
    terraform -chdir="$root_dir" destroy "${campaign_args[@]}"
    cleanup_needed=false
    "$root_dir/scripts/orphans.sh"
    trap - EXIT
    ;;
  down)
    terraform -chdir="$root_dir" destroy "$@"
    ;;
  orphans)
    "$root_dir/scripts/orphans.sh" "$@"
    ;;
  *)
    usage
    exit 2
    ;;
esac
