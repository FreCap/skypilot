#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "${1:-charts/skypilot}" && pwd)

python3 - "$chart_dir/values.schema.json" <<'PY'
import json
from pathlib import Path
import sys

schema = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
policy = schema['properties']['resourceActions']['properties'][
    'qualificationPolicy']
assert policy['additionalProperties'] is False
assert set(policy['properties']) == {'repoPath', 'byteSize', 'sha256'}
assert policy['oneOf'] == [{
    'required': ['repoPath', 'byteSize', 'sha256'],
    'properties': {
        'repoPath': {'const': ''},
        'byteSize': {'const': 0},
        'sha256': {'const': ''},
    },
}, {
    'required': ['repoPath', 'byteSize', 'sha256'],
    'properties': {
        'repoPath': {'minLength': 1},
        'byteSize': {'minimum': 1},
        'sha256': {'minLength': 64},
    },
}]
assert policy['properties']['byteSize'] == {
    'type': 'integer',
    'minimum': 0,
    'maximum': 65536,
}
assert policy['properties']['sha256']['pattern'] == '^$|^[0-9a-f]{64}$'
assert policy['properties']['repoPath']['pattern'].startswith(
    '^$|^charts/skypilot/files/resource-action-qualification-policies/')
PY

temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT
cp -R "$chart_dir" "$temporary_dir/chart"
rm -f "$temporary_dir/chart/values.schema.json"

policy_relative_path='files/resource-action-qualification-policies/projection-test.json'
policy_repo_path="charts/skypilot/$policy_relative_path"
mkdir -p "$(dirname "$temporary_dir/chart/$policy_relative_path")"
printf '%s' '{"fixture":"qualification-projection-only"}' \
  > "$temporary_dir/chart/$policy_relative_path"
policy_size=$(wc -c < "$temporary_dir/chart/$policy_relative_path" | tr -d ' ')
policy_sha256=$(sha256sum "$temporary_dir/chart/$policy_relative_path" | cut -d ' ' -f 1)

common_args=(
  qualification-policy-test
  "$temporary_dir/chart"
)
configured_args=(
  --set-string resourceActions.qualificationPolicy.repoPath="$policy_repo_path"
  --set resourceActions.qualificationPolicy.byteSize="$policy_size"
  --set-string resourceActions.qualificationPolicy.sha256="$policy_sha256"
)

helm template "${common_args[@]}" > "$temporary_dir/default.yaml"
if grep -q 'resource-action-qualification-policy' "$temporary_dir/default.yaml"; then
  printf 'Default render unexpectedly projected a qualification policy.\n' >&2
  exit 1
fi

# `helm template` always merges the current chart defaults, while an upgrade
# with `--reuse-values` can present templates with the old stored value tree.
# Remove the newly introduced defaults from copies of the chart to exercise
# both old shapes exactly as the upgrade renderer sees them.
old_without_resource_actions="$temporary_dir/old-without-resource-actions"
old_without_policy="$temporary_dir/old-without-policy"
old_with_null_policy="$temporary_dir/old-with-null-policy"
cp -R "$temporary_dir/chart" "$old_without_resource_actions"
cp -R "$temporary_dir/chart" "$old_without_policy"
cp -R "$temporary_dir/chart" "$old_with_null_policy"
python3 - "$old_without_resource_actions/values.yaml" \
  "$old_without_policy/values.yaml" \
  "$old_with_null_policy/values.yaml" <<'PY'
from pathlib import Path
import sys


def remove_block(path: Path, start: str, end: str) -> None:
    lines = path.read_text(encoding='utf-8').splitlines(keepends=True)
    start_index = next(index for index, line in enumerate(lines)
                       if line == start)
    end_index = next(index for index, line in enumerate(lines[start_index + 1:],
                                                        start=start_index + 1)
                     if line == end)
    path.write_text(''.join(lines[:start_index] + lines[end_index:]),
                    encoding='utf-8')


remove_block(Path(sys.argv[1]), 'resourceActions:\n', 'databaseConnection:\n')
remove_block(Path(sys.argv[2]), '  qualificationPolicy:\n',
             '  authorityWorker:\n')
null_policy_path = Path(sys.argv[3])
remove_block(null_policy_path, '  qualificationPolicy:\n',
             '  authorityWorker:\n')
null_policy_values = null_policy_path.read_text(encoding='utf-8')
null_policy_path.write_text(
    null_policy_values.replace('resourceActions:\n',
                               'resourceActions:\n  qualificationPolicy: null\n',
                               1),
    encoding='utf-8')
PY

for old_chart in "$old_without_resource_actions" "$old_without_policy"; do
  old_name=$(basename "$old_chart")
  helm template "$old_name" "$old_chart" \
    > "$temporary_dir/$old_name.yaml"
  if grep -q 'resource-action-qualification-policy' \
      "$temporary_dir/$old_name.yaml"; then
    printf 'Old values shape %s unexpectedly projected a policy.\n' \
      "$old_name" >&2
    exit 1
  fi
done

helm template "${common_args[@]}" "${configured_args[@]}" \
  > "$temporary_dir/configured.yaml"
python3 - "$temporary_dir/configured.yaml" \
  "$temporary_dir/chart/$policy_relative_path" "$policy_sha256" \
  "$policy_size" <<'PY'
import base64
from pathlib import Path
import re
import sys

rendered = Path(sys.argv[1]).read_text(encoding='utf-8')
documents = rendered.split('\n---\n')
policy_bytes = Path(sys.argv[2]).read_bytes()
expected_sha256 = sys.argv[3]
expected_size = sys.argv[4]

config_maps = [doc for doc in documents if re.search(
    r'(?m)^kind: ConfigMap$.*?^  name: skypilot-ra-policy-', doc, re.DOTALL)]
assert len(config_maps) == 1
config_map = config_maps[0]
config_map_name = re.search(
    r'(?m)^  name: (skypilot-ra-policy-[0-9a-f]{40})$',
    config_map).group(1)
assert 'immutable: true' in config_map
encoded = re.search(
    r'(?m)^  qualification-policy\.json: "([A-Za-z0-9+/=]+)"$',
    config_map).group(1)
assert base64.b64decode(encoded, validate=True) == policy_bytes
assert (f'skypilot.co/resource-action-qualification-policy-byte-size: '
        f'"{expected_size}"') in config_map
assert (f'skypilot.co/resource-action-qualification-policy-sha256: '
        f'"{expected_sha256}"') in config_map

api = next(doc for doc in documents if re.search(
    r'(?m)^kind: Deployment$.*?^  name: '
    r'qualification-policy-test-api-server$', doc, re.DOTALL))
assert (f'skypilot.co/resource-action-qualification-policy-config-map: '
        f'"{config_map_name}"') in api
assert (f'skypilot.co/resource-action-qualification-policy-byte-size: '
        f'"{expected_size}"') in api
assert (f'skypilot.co/resource-action-qualification-policy-sha256: '
        f'"{expected_sha256}"') in api
assert '''        - name: skypilot-resource-action-qualification-policy
          mountPath: /etc/skypilot/resource-actions/qualification-policy.json
          subPath: qualification-policy.json
          readOnly: true''' in api
assert f'''      - name: skypilot-resource-action-qualification-policy
        configMap:
          name: "{config_map_name}"
          defaultMode: 0444
          items:
          - key: qualification-policy.json
            path: qualification-policy.json''' in api
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED' not in rendered
assert not any(re.search(r'(?m)^kind: Deployment$.*?^  name: .*authority-',
                         doc, re.DOTALL) for doc in documents)
PY

expect_chart_failure() {
  local expected=$1
  local chart=$2
  shift 2
  local output
  if output=$(helm template qualification-policy-test "$chart" "$@" 2>&1); then
    printf 'Expected helm template to fail for arguments: %s\n' "$*" >&2
    exit 1
  fi
  if [[ $output != *"$expected"* ]]; then
    printf 'Expected error containing %q, got:\n%s\n' \
      "$expected" "$output" >&2
    exit 1
  fi
}

expect_failure() {
  local expected=$1
  shift
  expect_chart_failure "$expected" "$temporary_dir/chart" "$@"
}

expect_failure \
  'qualificationPolicy.byteSize must be between 1 and 65536' \
  --set-string resourceActions.qualificationPolicy.repoPath="$policy_repo_path"
expect_chart_failure \
  'resourceActions.qualificationPolicy must be an object' \
  "$old_with_null_policy"
expect_failure \
  'resourceActions.qualificationPolicy must be an object' \
  --set-string resourceActions.qualificationPolicy=invalid
expect_chart_failure \
  'qualificationPolicy must contain exactly repoPath, byteSize, and sha256' \
  "$old_without_policy" \
  --set-json 'resourceActions.qualificationPolicy={"repoPath":""}'
expect_failure \
  'qualificationPolicy must contain exactly repoPath, byteSize, and sha256' \
  --set resourceActions.qualificationPolicy.extra=true
expect_failure \
  'qualificationPolicy.byteSize must be an integer' \
  --set-string resourceActions.qualificationPolicy.byteSize=1
expect_failure \
  'qualificationPolicy.repoPath must name a normalized JSON file' \
  --set-string resourceActions.qualificationPolicy.repoPath=../policy.json \
  --set resourceActions.qualificationPolicy.byteSize=1 \
  --set-string resourceActions.qualificationPolicy.sha256="$(printf '0%.0s' {1..64})"
expect_failure \
  'qualificationPolicy is not packaged by this chart' \
  --set-string resourceActions.qualificationPolicy.repoPath=charts/skypilot/files/resource-action-qualification-policies/missing.json \
  --set resourceActions.qualificationPolicy.byteSize=1 \
  --set-string resourceActions.qualificationPolicy.sha256="$(printf '0%.0s' {1..64})"
expect_failure \
  'qualificationPolicy.byteSize does not match packaged bytes' \
  "${configured_args[@]}" \
  --set resourceActions.qualificationPolicy.byteSize="$((policy_size + 1))"
expect_failure \
  'qualificationPolicy.sha256 does not match packaged bytes' \
  "${configured_args[@]}" \
  --set-string resourceActions.qualificationPolicy.sha256="$(printf '0%.0s' {1..64})"
expect_failure \
  'volume name "skypilot-resource-action-qualification-policy" is duplicated or reserved' \
  --set-json 'apiService.extraVolumes=[{"name":"skypilot-resource-action-qualification-policy","emptyDir":{}}]'
expect_failure \
  'volume mount path "/etc/skypilot/resource-actions/qualification-policy.json" is duplicated or reserved' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy","mountPath":"/etc/skypilot/resource-actions/qualification-policy.json"}]'
expect_failure \
  'volume mount path "/etc/skypilot/resource-actions" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-parent","mountPath":"/etc/skypilot/resource-actions"}]'
expect_failure \
  'volume mount path "/etc/skypilot/resource-actions/qualification-policy.json/child" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-child","mountPath":"/etc/skypilot/resource-actions/qualification-policy.json/child"}]'
expect_failure \
  'volume mount path "/etc/skypilot/resource-actions/" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-trailing-slash","mountPath":"/etc/skypilot/resource-actions/"}]'
expect_failure \
  'volume mount path "/etc//skypilot/resource-actions" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-repeated-slash","mountPath":"/etc//skypilot/resource-actions"}]'
expect_failure \
  'volume mount path "/etc/skypilot/./resource-actions" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-dot-segment","mountPath":"/etc/skypilot/./resource-actions"}]'
expect_failure \
  'volume mount path "/etc/skypilot/resource-actions/qualification-policy.json/../qualification-policy.json" overlaps the reserved resource-action qualification policy path' \
  --set-json 'apiService.extraVolumeMounts=[{"name":"spoofed-policy-parent-segment","mountPath":"/etc/skypilot/resource-actions/qualification-policy.json/../qualification-policy.json"}]'
expect_failure \
  'API Pod annotation "skypilot.co/resource-action-qualification-policy-sha256" is reserved' \
  --set-json 'apiService.annotations={"skypilot.co/resource-action-qualification-policy-sha256":"spoofed"}'
