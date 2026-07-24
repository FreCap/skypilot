#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="terraform-tests/prevent-destroy-fixture"
output="$(mktemp)"
trap 'rm -f "${output}"' EXIT

terraform -chdir="${module_dir}" init \
  -backend=false \
  -test-directory="${fixture_dir}" \
  -input=false \
  -no-color >/dev/null

set +e
terraform -chdir="${module_dir}" test \
  -test-directory="${fixture_dir}" \
  -filter="${fixture_dir}/removal.tftest.hcl" \
  -no-color >"${output}" 2>&1
status=$?
set -e

if [[ ${status} -eq 0 ]]; then
  echo "Expected generation removal to be rejected, but terraform test passed." >&2
  cat "${output}" >&2
  exit 1
fi

required_lines=(
  'run "apply_generation_zero"... pass'
  'run "add_and_activate_generation_one"... pass'
  'run "attempt_to_remove_generation_one"... fail'
  'Resource aws_ecr_repository.qualification_generation["g01"] has'
  'lifecycle.prevent_destroy set, but the plan calls for this resource to be'
  'destroyed.'
)

for required_line in "${required_lines[@]}"; do
  if ! grep -Fq "${required_line}" "${output}"; then
    echo "Terraform failed without the expected prevent_destroy evidence: ${required_line}" >&2
    cat "${output}" >&2
    exit 1
  fi
done

echo "PASS: generation 1 removal was rejected by lifecycle.prevent_destroy."
