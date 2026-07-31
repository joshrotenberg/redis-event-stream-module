#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: remote-client-build.sh <source-archive-url> <image-tag>" >&2
  exit 2
fi

source_archive="$1"
image="$2"
source_dir="$(mktemp -d)"

cleanup() {
  rm -rf "$source_dir"
}
trap cleanup EXIT

curl -fsSL "$source_archive" |
  tar xz --strip-components=1 -C "$source_dir"
docker build \
  --target client-build \
  --tag "$image" \
  "$source_dir" >&2

image_id="$(docker image inspect "$image" --format '{{.Id}}')"
binary_sha="$(
  docker run --rm "$image" sha256sum /eventstream-client |
    awk '{ print $1 }'
)"
binary_size="$(
  docker run --rm "$image" stat -c '%s' /eventstream-client
)"

jq -n \
  --arg source_archive "$source_archive" \
  --arg image "$image" \
  --arg image_id "$image_id" \
  --arg binary_sha256 "$binary_sha" \
  --argjson binary_size_bytes "$binary_size" \
  '{
    source_archive: $source_archive,
    image: $image,
    image_id: $image_id,
    binary_sha256: $binary_sha256,
    binary_size_bytes: $binary_size_bytes
  }'
