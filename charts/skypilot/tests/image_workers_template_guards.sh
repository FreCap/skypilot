#!/usr/bin/env bash
set -euo pipefail

chart_dir=${1:-charts/skypilot}
temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT
cp -R "$chart_dir" "$temporary_dir/chart"
rm -f "$temporary_dir/chart/values.schema.json"

common_args=(
  managed-image-workers
  "$temporary_dir/chart"
  --set apiService.dbConnectionSecretName=external-db
  --set imageCanaryWorker.enabled=true
  --set imageCanaryWorker.serviceAccount.create=false
  --set imageCanaryWorker.serviceAccount.name=canary-worker
)

expect_failure() {
  local expected=$1
  shift
  local output
  if output=$(helm template "${common_args[@]}" "$@" 2>&1); then
    printf 'Expected helm template to fail for arguments: %s\n' "$*" >&2
    exit 1
  fi
  if [[ $output != *"$expected"* ]]; then
    printf 'Expected error containing %q, got:\n%s\n' "$expected" "$output" >&2
    exit 1
  fi
}

expect_failure \
  'imageCanaryWorker.terminationGracePeriodSeconds must be at least 600 seconds' \
  --set imageCanaryWorker.terminationGracePeriodSeconds=0
expect_failure \
  'imageCanaryWorker.terminationGracePeriodSeconds must be at least 600 seconds' \
  --set imageCanaryWorker.terminationGracePeriodSeconds=599
expect_failure \
  'imageCanaryWorker.terminationGracePeriodSeconds must be an integer' \
  --set imageCanaryWorker.terminationGracePeriodSeconds=600.5
expect_failure \
  'imageCanaryWorker.terminationGracePeriodSeconds must be an integer' \
  --set-string imageCanaryWorker.terminationGracePeriodSeconds=600
