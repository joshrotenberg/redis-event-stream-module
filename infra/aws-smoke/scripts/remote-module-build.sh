#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: remote-module-build.sh <source-archive-url> <destination-so>" >&2
  exit 2
fi

source_archive="$1"
destination="$2"
image="eventstream-branch-module:build"
container=""
source_dir="$(mktemp -d)"

cleanup() {
  if [[ -n "$container" ]]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$source_dir"
}
trap cleanup EXIT

curl -fsSL "$source_archive" |
  tar xz --strip-components=1 -C "$source_dir"
docker build \
  --target module-build \
  --tag "$image" \
  "$source_dir" >&2

container="$(docker create "$image")"
install -d -m 0755 "$(dirname "$destination")"
docker cp "$container:/module.so" "$destination"
chmod 0755 "$destination"

sha="$(sha256sum "$destination" | awk '{ print $1 }')"
size="$(stat -c '%s' "$destination")"
jq -n \
  --arg source_archive "$source_archive" \
  --arg path "$destination" \
  --arg sha256 "$sha" \
  --argjson size_bytes "$size" \
  '{
    source_archive: $source_archive,
    path: $path,
    sha256: $sha256,
    size_bytes: $size_bytes
  }'
