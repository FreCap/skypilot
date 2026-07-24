#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_file="terraform-tests/external_permissions_boundary.tftest.hcl"
output="$(mktemp)"
trap 'rm -f "${output}"' EXIT

terraform -chdir="${module_dir}" init \
  -backend=false \
  -test-directory="terraform-tests" \
  -input=false \
  -no-color >/dev/null

terraform -chdir="${module_dir}" test \
  -test-directory="terraform-tests" \
  -filter="${test_file}" \
  -verbose \
  -no-color >"${output}" 2>&1

required_lines=(
  'run "null_preserves_module_managed_boundaries_and_fingerprints"... pass'
  'run "null_default_is_a_state_noop"... pass'
  'run "external_boundary_is_attached_and_its_document_is_fingerprinted"... pass'
  'No changes. Your infrastructure matches the configuration.'
  '# aws_iam_role.copy_target will be updated in-place'
  '# aws_iam_role.lifecycle_target will be updated in-place'
  'Plan: 0 to add, 2 to change, 0 to destroy.'
)

for required_line in "${required_lines[@]}"; do
  if [[ "$(grep -Fc "${required_line}" "${output}")" -ne 1 ]]; then
    echo "Terraform did not produce the exact in-place boundary rollout evidence: ${required_line}" >&2
    cat "${output}" >&2
    exit 1
  fi
done

for forbidden_line in "must be replaced" "will be destroyed"; do
  if grep -Fq "${forbidden_line}" "${output}"; then
    echo "The external boundary rollout planned a destructive role action: ${forbidden_line}" >&2
    cat "${output}" >&2
    exit 1
  fi
done

echo "PASS: both target roles switch to the external boundary in place with zero creates or destroys."
