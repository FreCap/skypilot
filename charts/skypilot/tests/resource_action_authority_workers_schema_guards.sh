#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "${1:-charts/skypilot}" && pwd)
fixture="$chart_dir/tests/fixtures/resource-action-authority-values.yaml"

python3 - "$chart_dir/values.schema.json" <<'PY'
import json
from pathlib import Path
import sys

schema = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
authority = schema['properties']['resourceActions']['properties'][
    'authorityWorker']
cohorts = authority['properties']['cohorts']
manifest_contract = cohorts['items']['properties']['manifestContract']
assert manifest_contract['allOf'] == [
    {'type': 'string'},
    {'const': 'provider_authority_worker_cohort_v2'},
]

closed_items = cohorts['allOf'][0]['items']
assert closed_items['additionalProperties'] is False
assert 'manifestContract' in closed_items['properties']
assert 'manifestVersion' not in closed_items['properties']
# The shipped V1 --reuse-values shape is intentionally admitted by schema so
# the phase-aware template can preserve it only during explicit deselect.
assert 'manifestContract' not in closed_items['required']
PY

expect_lint_failure() {
  local expected=$1
  shift
  local output
  if output=$(helm lint "$chart_dir" --values "$fixture" "$@" 2>&1); then
    printf 'Expected helm lint to fail for arguments: %s\n' "$*" >&2
    exit 1
  fi
  if [[ $output != *"$expected"* ]]; then
    printf 'Expected lint error containing %q, got:\n%s\n' \
      "$expected" "$output" >&2
    exit 1
  fi
}

expect_lint_failure \
  'manifestContract must equal provider_authority_worker_cohort_v2'
expect_lint_failure \
  'Invalid type. Expected: string, given: null' \
  --set-json resourceActions.authorityWorker.cohorts[0].manifestContract=null
expect_lint_failure \
  'Invalid type. Expected: string, given: integer' \
  --set-json resourceActions.authorityWorker.cohorts[0].manifestContract=1
expect_lint_failure \
  'Invalid type. Expected: string, given: integer' \
  --set-json resourceActions.authorityWorker.cohorts[0].manifestContract=2
expect_lint_failure \
  'Invalid type. Expected: string, given: integer' \
  --set-json resourceActions.authorityWorker.cohorts[0].manifestContract=2.0
expect_lint_failure \
  'does not match: "provider_authority_worker_cohort_v2"' \
  --set-string resourceActions.authorityWorker.cohorts[0].manifestContract=2
expect_lint_failure \
  'does not match: "provider_authority_worker_cohort_v2"' \
  --set-string resourceActions.authorityWorker.cohorts[0].manifestContract=arbitrary
expect_lint_failure \
  'Additional property manifestVersion is not allowed' \
  --set resourceActions.authorityWorker.cohorts[0].manifestVersion=2
expect_lint_failure \
  'qualification artifact is not packaged by this chart' \
  --set databaseMigration.authorityV1RetirementPhase=deselect \
  --set resourceActions.authorityWorker.activeCohort=
expect_lint_failure \
  'qualification artifact is not packaged by this chart' \
  --set-string resourceActions.authorityWorker.cohorts[0].manifestContract=provider_authority_worker_cohort_v2
expect_lint_failure \
  'manifestContract must be absent during authority V1 deselect' \
  --set databaseMigration.authorityV1RetirementPhase=deselect \
  --set resourceActions.authorityWorker.activeCohort= \
  --set-string resourceActions.authorityWorker.cohorts[0].manifestContract=provider_authority_worker_cohort_v2
