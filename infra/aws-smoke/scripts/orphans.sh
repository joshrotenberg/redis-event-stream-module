#!/usr/bin/env bash
set -euo pipefail

region="${AWS_REGION:-us-west-2}"
if [[ $# -gt 0 ]]; then
  region="$1"
fi

aws_args=(--region "$region")
if [[ -n "${AWS_PROFILE:-}" ]]; then
  aws_args+=(--profile "$AWS_PROFILE")
fi

payload="$(
  aws "${aws_args[@]}" resourcegroupstaggingapi get-resources \
    --tag-filters \
      Key=Project,Values=redis-event-stream-module \
      Key=Environment,Values=aws-smoke \
    --output json
)"

count="$(jq '.ResourceTagMappingList | length' <<<"$payload")"
if [[ "$count" == "0" ]]; then
  echo "no tagged aws-smoke resources found in $region"
  exit 0
fi

echo "tagged aws-smoke resources still present in $region:"
jq -r '
  .ResourceTagMappingList[]
  | [
      .ResourceARN,
      ((.Tags[] | select(.Key == "Owner") | .Value) // "-"),
      ((.Tags[] | select(.Key == "ExpiresAt") | .Value) // "-")
    ]
  | @tsv
' <<<"$payload" | column -t -s $'\t'
