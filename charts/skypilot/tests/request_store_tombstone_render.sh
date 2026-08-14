#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart_dir="$(cd "${script_dir}/.." && pwd)"
prior_values="${script_dir}/fixtures/request-store-tombstone-prior.yaml"
generated_values="${script_dir}/fixtures/request-store-tombstone-generated.yaml"
final_values="${script_dir}/fixtures/request-store-tombstone-final.yaml"
probe_chart="${script_dir}/request-store-values-probe"

helm_version="$(helm version --template '{{.Version}}')"
if [[ "${helm_version}" != "v3.19.1" ]]; then
  echo "request-store tombstone test requires Helm v3.19.1, got ${helm_version}" >&2
  exit 1
fi

test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

helm template request-store-tombstone "${chart_dir}" \
  --namespace skypilot \
  --values "${prior_values}" \
  --values "${generated_values}" \
  --values "${final_values}" \
  >"${test_dir}/rendered.yaml"

assert_env_count() {
  local expected="$1"
  local name="$2"
  local value="$3"
  local actual
  actual="$(awk -v expected_name="${name}" -v expected_value="${value}" '
    $0 ~ "name: " expected_name "$" {
      if (getline > 0) {
        sub(/^[[:space:]]*value:[[:space:]]*/, "")
        gsub(/^"|"$/, "")
        if ($0 == expected_value) {
          count++
        }
      }
    }
    END { print count + 0 }
  ' "${test_dir}/rendered.yaml")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "expected ${expected} rendered ${name}=${value} entries, got ${actual}" >&2
    exit 1
  fi
}

# The API, executor, controller, and migration hook all stay on PostgreSQL;
# every long-running role also retains the quiescence and durable-gate fence.
assert_env_count 4 SKYPILOT_API_REQUEST_BACKEND postgres
assert_env_count 3 SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS true
assert_env_count 3 SKYPILOT_API_REQUEST_CUTOVER_GATE_PATH /root/.sky/api-request-cutover.json

# Helm 3.19.1 parent-null coalescing removes the key seen by templates. During
# an upgrade with --reuse-values, Helm's reuseValues path deletes the same null
# destination before assigning the new release Config; `helm get values` must
# therefore omit requestStore after the live revision, not retain a null map.
helm template request-store-values-probe "${probe_chart}" \
  --values "${prior_values}" \
  --values "${generated_values}" \
  --values "${final_values}" \
  >"${test_dir}/values-probe.yaml"
grep -q 'request-store-present: "false"' "${test_dir}/values-probe.yaml"
