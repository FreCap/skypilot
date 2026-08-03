#!/usr/bin/env bash
set -euo pipefail

chart_dir=$(cd "${1:-charts/skypilot}" && pwd)
repository_root=$(cd "$chart_dir/../.." && pwd)
temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT
cp -R "$chart_dir" "$temporary_dir/chart"
mkdir -p "$temporary_dir/chart/files/resource-action-qualifications"

manifest_one=1111111111111111111111111111111111111111111111111111111111111111
config_one=2222222222222222222222222222222222222222222222222222222222222222
manifest_two=6666666666666666666666666666666666666666666666666666666666666666
config_two=7777777777777777777777777777777777777777777777777777777777777777
qualification_one=$(printf '{"oci_config_digest":"sha256:%s","oci_manifest_digest":"sha256:%s","platform":"linux/amd64","requested_reference":"registry.example/authority@sha256:%s","source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","version":1}' "$config_one" "$manifest_one" "$manifest_one")
qualification_two=$(printf '{"oci_config_digest":"sha256:%s","oci_manifest_digest":"sha256:%s","platform":"linux/amd64","requested_reference":"registry.example/authority@sha256:%s","source_commit":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","version":1}' "$config_two" "$manifest_two" "$manifest_two")
printf '%s\n' "$qualification_one" > "$temporary_dir/chart/files/resource-action-qualifications/p2a-v1.json"
printf '%s\n' "$qualification_two" > "$temporary_dir/chart/files/resource-action-qualifications/p2a-v2.json"
qualification_one_size=$(wc -c < "$temporary_dir/chart/files/resource-action-qualifications/p2a-v1.json")
qualification_two_size=$(wc -c < "$temporary_dir/chart/files/resource-action-qualifications/p2a-v2.json")
qualification_one_sha=$(sha256sum "$temporary_dir/chart/files/resource-action-qualifications/p2a-v1.json" | cut -d' ' -f1)
qualification_two_sha=$(sha256sum "$temporary_dir/chart/files/resource-action-qualifications/p2a-v2.json" | cut -d' ' -f1)
projector_path=sky/serve/resource_action_provider_preflight.py
artifact_inventory_path=sky/serve/resource_action_artifacts/provider_authority_v1/renderer_artifact_inventory.json
callable_inventory_path=sky/serve/resource_action_artifacts/provider_authority_v1/callable_inventory.json
projector_size=$(wc -c < "$repository_root/$projector_path")
projector_sha=$(sha256sum "$repository_root/$projector_path" | cut -d' ' -f1)
artifact_inventory_size=$(wc -c < "$repository_root/$artifact_inventory_path")
artifact_inventory_sha=$(sha256sum "$repository_root/$artifact_inventory_path" | cut -d' ' -f1)
callable_inventory_size=$(wc -c < "$repository_root/$callable_inventory_path")
callable_inventory_sha=$(sha256sum "$repository_root/$callable_inventory_path" | cut -d' ' -f1)

common_args=(
  ra-test
  "$temporary_dir/chart"
  --namespace skypilot-system
  --set apiService.highAvailability.enabled=true
  --set apiService.replicas=2
  --set apiService.upgradeStrategy=RollingUpdate
  --set apiService.dbConnectionSecretName=external-db
  --set requestStore.backend=postgres
  --set kubernetesCredentials.useApiServerCluster=false
  --set serve.externalLoadBalancer.enabled=false
  --set storage.enabled=true
  --set storage.accessMode=ReadWriteMany
  --set resourceActions.authorityWorker.enabled=true
  --set-string resourceActions.authorityWorker.installationId=01234567-89ab-cdef-0123-456789abcdef
  --set resourceActions.authorityWorker.auth.existingSecret=authority-auth
  --set resourceActions.authorityWorker.tls.existingSecret=authority-tls
  --set resourceActions.authorityWorker.retirementTombstones[0]=retired-v1
  --set resourceActions.authorityWorker.cohorts[0].id=p2a-v1
  --set resourceActions.authorityWorker.cohorts[0].replicas=2
  --set-string resourceActions.authorityWorker.cohorts[0].image=registry.example/authority@sha256:${manifest_one}
  --set resourceActions.authorityWorker.cohorts[0].imagePullPolicy=Always
  --set-string resourceActions.authorityWorker.cohorts[0].ociConfigDigest=sha256:${config_one}
  --set-string resourceActions.authorityWorker.cohorts[0].qualificationArtifact.repoPath=charts/skypilot/files/resource-action-qualifications/p2a-v1.json
  --set resourceActions.authorityWorker.cohorts[0].qualificationArtifact.byteSize="${qualification_one_size}"
  --set-string resourceActions.authorityWorker.cohorts[0].qualificationArtifact.sha256="${qualification_one_sha}"
  --set-string resourceActions.authorityWorker.cohorts[0].podTemplateContract.repoPath="$projector_path"
  --set resourceActions.authorityWorker.cohorts[0].podTemplateContract.byteSize="$projector_size"
  --set-string resourceActions.authorityWorker.cohorts[0].podTemplateContract.sha256="$projector_sha"
  --set-string resourceActions.authorityWorker.cohorts[0].artifactInventory.repoPath="$artifact_inventory_path"
  --set resourceActions.authorityWorker.cohorts[0].artifactInventory.byteSize="$artifact_inventory_size"
  --set-string resourceActions.authorityWorker.cohorts[0].artifactInventory.sha256="$artifact_inventory_sha"
  --set-string resourceActions.authorityWorker.cohorts[0].callableInventory.repoPath="$callable_inventory_path"
  --set resourceActions.authorityWorker.cohorts[0].callableInventory.byteSize="$callable_inventory_size"
  --set-string resourceActions.authorityWorker.cohorts[0].callableInventory.sha256="$callable_inventory_sha"
  --set resourceActions.authorityWorker.cohorts[1].id=p2a-v2
  --set resourceActions.authorityWorker.cohorts[1].replicas=2
  --set-string resourceActions.authorityWorker.cohorts[1].image=registry.example/authority@sha256:${manifest_two}
  --set resourceActions.authorityWorker.cohorts[1].imagePullPolicy=Always
  --set-string resourceActions.authorityWorker.cohorts[1].ociConfigDigest=sha256:${config_two}
  --set-string resourceActions.authorityWorker.cohorts[1].qualificationArtifact.repoPath=charts/skypilot/files/resource-action-qualifications/p2a-v2.json
  --set resourceActions.authorityWorker.cohorts[1].qualificationArtifact.byteSize="${qualification_two_size}"
  --set-string resourceActions.authorityWorker.cohorts[1].qualificationArtifact.sha256="${qualification_two_sha}"
  --set-string resourceActions.authorityWorker.cohorts[1].podTemplateContract.repoPath="$projector_path"
  --set resourceActions.authorityWorker.cohorts[1].podTemplateContract.byteSize="$projector_size"
  --set-string resourceActions.authorityWorker.cohorts[1].podTemplateContract.sha256="$projector_sha"
  --set-string resourceActions.authorityWorker.cohorts[1].artifactInventory.repoPath="$artifact_inventory_path"
  --set resourceActions.authorityWorker.cohorts[1].artifactInventory.byteSize="$artifact_inventory_size"
  --set-string resourceActions.authorityWorker.cohorts[1].artifactInventory.sha256="$artifact_inventory_sha"
  --set-string resourceActions.authorityWorker.cohorts[1].callableInventory.repoPath="$callable_inventory_path"
  --set resourceActions.authorityWorker.cohorts[1].callableInventory.byteSize="$callable_inventory_size"
  --set-string resourceActions.authorityWorker.cohorts[1].callableInventory.sha256="$callable_inventory_sha"
)

authority_base_args=(
  ra-test
  "$temporary_dir/chart"
  --namespace skypilot-system
  --set apiService.highAvailability.enabled=true
  --set apiService.replicas=2
  --set apiService.upgradeStrategy=RollingUpdate
  --set apiService.dbConnectionSecretName=external-db
  --set requestStore.backend=postgres
  --set kubernetesCredentials.useApiServerCluster=false
  --set serve.externalLoadBalancer.enabled=false
  --set storage.enabled=true
  --set storage.accessMode=ReadWriteMany
  --set resourceActions.authorityWorker.enabled=true
  --set-string resourceActions.authorityWorker.installationId=01234567-89ab-cdef-0123-456789abcdef
)

tombstone_args=(
  "${authority_base_args[@]}"
  --set resourceActions.authorityWorker.retirementTombstones[0]=retired-v1
)

render() {
  local active_cohort=$1
  local output=$2
  helm template "${common_args[@]}" \
    --show-only templates/resource-action-authority-workers.yaml \
    --set resourceActions.authorityWorker.activeCohort="$active_cohort" \
    > "$output"
}

render_controller() {
  local active_cohort=$1
  local output=$2
  helm template "${common_args[@]}" \
    --show-only templates/controller-deployment.yaml \
    --set resourceActions.authorityWorker.activeCohort="$active_cohort" \
    > "$output"
}

render_api() {
  local active_cohort=$1
  local output=$2
  helm template "${common_args[@]}" \
    --show-only templates/api-deployment.yaml \
    --set resourceActions.authorityWorker.activeCohort="$active_cohort" \
    > "$output"
}

render_migration() {
  local output=$1
  helm template "${common_args[@]}" \
    --show-only templates/database-migration-job.yaml \
    --set resourceActions.authorityWorker.activeCohort=p2a-v1 \
    > "$output"
}

render_disabled_migration() {
  local full_name=$1
  local output=$2
  local full_name_args=()
  if [[ -n $full_name ]]; then
    full_name_args=(--set fullnameOverride="$full_name")
  fi
  helm template ra-test "$temporary_dir/chart" \
    --namespace skypilot-system \
    --show-only templates/database-migration-job.yaml \
    --set apiService.highAvailability.enabled=false \
    --set apiService.dbConnectionSecretName=external-db \
    --set requestStore.backend=postgres \
    --set resourceActions.authorityWorker.enabled=false \
    "${full_name_args[@]}" \
    > "$output"
}

render_chart_managed_migration() {
  local output=$1
  helm template ra-test "$temporary_dir/chart" \
    --namespace skypilot-system \
    --show-only templates/database-migration-job.yaml \
    --set-string apiService.dbConnectionString=postgresql://user:pass@db.example/skypilot \
    --set requestStore.backend=postgres \
    > "$output"
}

render_disabled_api_compatibility() {
  local output=$1
  helm template ra-test "$temporary_dir/chart" \
    --namespace skypilot-system \
    --show-only templates/api-deployment.yaml \
    --set resourceActions.authorityWorker.enabled=false \
    --set apiService.extraEnvs[0].name=SKYPILOT_RESOURCE_ACTION_AUTHORITY_INSTALLATION_ID \
    --set apiService.extraEnvs[0].value=compat-installation \
    --set apiService.extraEnvs[1].name=SKYPILOT_RESOURCE_ACTION_AUTHORITY_COHORT_SUFFIXES_JSON \
    --set-string apiService.extraEnvs[1].value='[]' \
    --set apiService.extraEnvs[2].name=SKYPILOT_RESOURCE_ACTION_AUTHORITY_RETIREMENT_TOMBSTONES_JSON \
    --set-string apiService.extraEnvs[2].value='[]' \
    > "$output"
}

render_disabled_controller_compatibility() {
  local output=$1
  helm template "${authority_base_args[@]}" \
    --show-only templates/controller-deployment.yaml \
    --set resourceActions.authorityWorker.enabled=false \
    --set controllerService.extraEnvs[0].name=SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE \
    --set controllerService.extraEnvs[0].value=/compat/tokens \
    --set controllerService.extraVolumes[0].name=skypilot-resource-action-authority-auth \
    --set controllerService.extraVolumes[0].emptyDir={} \
    --set controllerService.extraVolumes[1].name=skypilot-resource-action-authority-ca \
    --set controllerService.extraVolumes[1].emptyDir={} \
    --set controllerService.extraVolumes[2].name=skypilot-resource-action-authority-manifest \
    --set controllerService.extraVolumes[2].emptyDir={} \
    --set controllerService.extraVolumeMounts[0].name=skypilot-resource-action-authority-auth \
    --set controllerService.extraVolumeMounts[0].mountPath=/etc/skypilot/resource-action-authority/auth \
    --set controllerService.extraVolumeMounts[1].name=skypilot-resource-action-authority-ca \
    --set controllerService.extraVolumeMounts[1].mountPath=/etc/skypilot/resource-action-authority/tls \
    --set controllerService.extraVolumeMounts[2].name=skypilot-resource-action-authority-manifest \
    --set controllerService.extraVolumeMounts[2].mountPath=/etc/skypilot/resource-action-authority/manifest.json \
    > "$output"
}

render_other_release() {
  local output=$1
  helm template ra-other "${common_args[@]:1}" \
    --show-only templates/resource-action-authority-workers.yaml \
    --set resourceActions.authorityWorker.activeCohort=p2a-v1 \
    > "$output"
}

render_renamed_full_name() {
  local output=$1
  helm template "${common_args[@]}" \
    --show-only templates/resource-action-authority-workers.yaml \
    --set fullnameOverride=ra-proposed \
    --set resourceActions.authorityWorker.activeCohort=p2a-v1 \
    > "$output"
}

render_tombstone_only() {
  local template=$1
  local output=$2
  helm template "${tombstone_args[@]}" \
    --show-only "$template" \
    > "$output"
}

expect_failure() {
  local expected=$1
  shift
  local output
  if output=$(helm template "${common_args[@]}" \
      --show-only templates/resource-action-authority-workers.yaml \
      --set resourceActions.authorityWorker.activeCohort=p2a-v1 \
      "$@" 2>&1); then
    printf 'Expected helm template to fail for arguments: %s\n' "$*" >&2
    exit 1
  fi
  if [[ $output != *"$expected"* ]]; then
    printf 'Expected error containing %q, got:\n%s\n' "$expected" "$output" >&2
    exit 1
  fi
}

render p2a-v1 "$temporary_dir/active-v1.yaml"
render p2a-v2 "$temporary_dir/active-v2.yaml"
render_controller p2a-v1 "$temporary_dir/controller-v1.yaml"
render_controller p2a-v2 "$temporary_dir/controller-v2.yaml"
render_api p2a-v1 "$temporary_dir/active-api.yaml"
render_migration "$temporary_dir/active-migration.yaml"
render_disabled_migration "" "$temporary_dir/disabled-migration.yaml"
render_disabled_migration proposed-full-name \
  "$temporary_dir/disabled-renamed-migration.yaml"
render_chart_managed_migration "$temporary_dir/chart-managed-migration.yaml"
render_disabled_api_compatibility "$temporary_dir/disabled-api-compat.yaml"
render_disabled_controller_compatibility \
  "$temporary_dir/disabled-controller-compat.yaml"
render_other_release "$temporary_dir/other-release.yaml"
render_renamed_full_name "$temporary_dir/renamed-full-name.yaml"
render_tombstone_only templates/resource-action-authority-workers.yaml \
  "$temporary_dir/tombstone-authority.yaml"
render_tombstone_only templates/controller-deployment.yaml \
  "$temporary_dir/tombstone-controller.yaml"
render_tombstone_only templates/api-deployment.yaml \
  "$temporary_dir/tombstone-api.yaml"

expect_failure \
  'resourceActions.authorityWorker.activeCohort must name exactly one cohort' \
  --set resourceActions.authorityWorker.activeCohort=missing
expect_failure \
  'qualification artifact sha256 does not match packaged bytes' \
  --set-string resourceActions.authorityWorker.cohorts[0].qualificationArtifact.sha256=0000000000000000000000000000000000000000000000000000000000000000
expect_failure \
  'qualification artifact byteSize does not match packaged bytes' \
  --set resourceActions.authorityWorker.cohorts[0].qualificationArtifact.byteSize="$((qualification_one_size + 1))"
expect_failure \
  'executorService.tolerations[0] collides with a fixed authority-worker NoExecute toleration' \
  --set executorService.tolerations[0].key=node.kubernetes.io/not-ready \
  --set executorService.tolerations[0].operator=Exists \
  --set executorService.tolerations[0].effect=NoExecute
expect_failure \
  'executorService.tolerations[0] collides with a fixed authority-worker NoExecute toleration' \
  --set executorService.tolerations[0].key=node.kubernetes.io/unreachable \
  --set executorService.tolerations[0].operator=Exists
if empty_output=$(helm template "${authority_base_args[@]}" \
    --show-only templates/resource-action-authority-workers.yaml 2>&1); then
  printf 'Expected enabled authority without cohorts/tombstones to fail\n' >&2
  exit 1
fi
if [[ $empty_output != *'resourceActions.authorityWorker.enabled=true requires at least one live cohort or retirement tombstone'* ]]; then
  printf 'Unexpected empty authority error:\n%s\n' "$empty_output" >&2
  exit 1
fi

python_bin=python3
if [[ -x .venv/bin/python ]]; then
  python_bin=.venv/bin/python
fi
"$python_bin" - "$temporary_dir/active-v1.yaml" "$temporary_dir/active-v2.yaml" \
  "$temporary_dir/chart/files/resource-action-qualifications/p2a-v1.json" \
  "$temporary_dir/chart/files/resource-action-qualifications/p2a-v2.json" \
  "$temporary_dir/controller-v1.yaml" "$temporary_dir/controller-v2.yaml" \
  "$temporary_dir/tombstone-authority.yaml" \
  "$temporary_dir/tombstone-controller.yaml" \
  "$temporary_dir/tombstone-api.yaml" \
  "$temporary_dir/active-api.yaml" \
  "$temporary_dir/other-release.yaml" \
  "$temporary_dir/active-migration.yaml" \
  "$temporary_dir/disabled-migration.yaml" \
  "$temporary_dir/disabled-renamed-migration.yaml" \
  "$temporary_dir/chart-managed-migration.yaml" \
  "$temporary_dir/disabled-api-compat.yaml" \
  "$temporary_dir/disabled-controller-compat.yaml" \
  "$temporary_dir/renamed-full-name.yaml" <<'PY'
import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import sys

try:
    from sky.serve import resource_action_provider_preflight
    from sky.serve import resource_actions
except ModuleNotFoundError:
    # The chart-only workflow installs Helm but not SkyPilot's Python
    # dependencies.  Developer environments with SkyPilot installed also run
    # the typed contract and canonical projector below.
    resource_action_provider_preflight = None
    resource_actions = None


def documents(path):
    text = Path(path).read_text(encoding='utf-8')
    result = {}
    for part in re.split(r'(?m)^---\s*\n', text):
        kind_match = re.search(r'(?m)^kind: ([^\n]+)$', part)
        name_match = re.search(
            r'(?m)^metadata:\n(?:  [^\n]*\n)*?  name: ([^\n]+)$', part)
        if kind_match is None or name_match is None:
            continue
        key = (kind_match.group(1), name_match.group(1).strip('"'))
        if key in result:
            raise AssertionError(f'duplicate rendered object {key}')
        result[key] = part
    return result


active_one = documents(sys.argv[1])
active_two = documents(sys.argv[2])
controller_one = documents(sys.argv[5])
controller_two = documents(sys.argv[6])
tombstone_authority = documents(sys.argv[7])
tombstone_controller = documents(sys.argv[8])
tombstone_api = documents(sys.argv[9])
active_api = documents(sys.argv[10])
other_release = documents(sys.argv[11])
active_migration = documents(sys.argv[12])
disabled_migration = documents(sys.argv[13])
disabled_renamed_migration = documents(sys.argv[14])
chart_managed_migration = documents(sys.argv[15])
disabled_api_compat = documents(sys.argv[16])
disabled_controller_compat = documents(sys.argv[17])
renamed_full_name = documents(sys.argv[18])
qualification_paths = {
    'p2a-v1': Path(sys.argv[3]),
    'p2a-v2': Path(sys.argv[4]),
}


def preflight_manifest_name(release_name, suffix):
    identity = f'skypilot-system\n{release_name}\n{suffix}'.encode()
    return f'skypilot-ra-preflight-{hashlib.sha256(identity).hexdigest()[:40]}'


expected_names = {
    ('ConfigMap', f'ra-test-authority-{suffix}-{artifact}')
    for suffix in ('p2a-v1', 'p2a-v2')
    for artifact in ('manifest', 'qualification')
}
expected_names |= {
    ('ConfigMap', preflight_manifest_name('ra-test', suffix))
    for suffix in ('p2a-v1', 'p2a-v2')
}
expected_names |= {
    ('ServiceAccount', f'ra-test-authority-{suffix}')
    for suffix in ('p2a-v1', 'p2a-v2')
}
expected_names |= {
    ('Deployment', f'ra-test-authority-{suffix}')
    for suffix in ('p2a-v1', 'p2a-v2')
}
expected_names |= {
    ('Service', 'ra-test-authority-preflight'),
    ('NetworkPolicy', 'ra-test-authority-preflight'),
    ('Role', 'ra-test-authority-self-attestation'),
    ('RoleBinding', 'ra-test-authority-self-attestation'),
    ('Role', 'ra-test-authority-retirement-verifier'),
    ('RoleBinding', 'ra-test-authority-retirement-verifier'),
}
assert set(active_one) == expected_names, set(active_one) ^ expected_names
assert set(active_two) == expected_names, set(active_two) ^ expected_names
for key in expected_names - {('Service', 'ra-test-authority-preflight')}:
    assert active_one[key] == active_two[key], f'active selection changed {key}'
assert active_one[('Service', 'ra-test-authority-preflight')] != active_two[
    ('Service', 'ra-test-authority-preflight')]

other_expected_names = {
    (kind, name.replace('ra-test-', 'ra-other-', 1))
    for kind, name in expected_names
    if name.startswith('ra-test-')
}
other_expected_names |= {
    ('ConfigMap', preflight_manifest_name('ra-other', suffix))
    for suffix in ('p2a-v1', 'p2a-v2')
}
assert set(other_release) == other_expected_names, (
    set(other_release) ^ other_expected_names)
renamed_hook_names = {
    key for key in renamed_full_name
    if key[0] == 'ConfigMap' and key[1].startswith('skypilot-ra-preflight-')
}
active_hook_names = {
    key for key in active_one
    if key[0] == 'ConfigMap' and key[1].startswith('skypilot-ra-preflight-')
}
assert renamed_hook_names == active_hook_names
for key in active_hook_names:
    assert renamed_full_name[key] != active_one[key]


def release_scope_values(document):
    return set(
        re.findall(
            r'skypilot\.co/authority-release-scope: "?([0-9a-f]{63})"?',
            document))


test_scope = hashlib.sha256(b'skypilot-system\nra-test').hexdigest()[:63]
other_scope = hashlib.sha256(b'skypilot-system\nra-other').hexdigest()[:63]
assert test_scope != other_scope
for documents_by_release, release_name, scope in (
    (active_one, 'ra-test', test_scope),
    (other_release, 'ra-other', other_scope),
):
    deployment = documents_by_release[
        ('Deployment', f'{release_name}-authority-p2a-v1')]
    service = documents_by_release[
        ('Service', f'{release_name}-authority-preflight')]
    network_policy = documents_by_release[
        ('NetworkPolicy', f'{release_name}-authority-preflight')]
    assert release_scope_values(deployment) == {scope}
    assert release_scope_values(service) == {scope}
    assert release_scope_values(network_policy) == {scope}
    assert f'app: {release_name}-controller' in network_policy
    other_name = 'ra-other' if release_name == 'ra-test' else 'ra-test'
    assert f'app: {other_name}-controller' not in network_policy

assert set(tombstone_authority) == {
    ('Role', 'ra-test-authority-retirement-verifier'),
    ('RoleBinding', 'ra-test-authority-retirement-verifier'),
}
tombstone_role = tombstone_authority[
    ('Role', 'ra-test-authority-retirement-verifier')]
assert tombstone_role.count('verbs: ["get"]') == 2
assert tombstone_role.count('ra-test-authority-retired-v1') == 2
assert set(tombstone_controller) == {('Deployment', 'ra-test-controller')}
tombstone_controller_text = tombstone_controller[
    ('Deployment', 'ra-test-controller')]
assert 'skypilot-resource-action-authority' not in tombstone_controller_text
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED' not in (
    tombstone_controller_text)
assert 'SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE' not in (
    tombstone_controller_text)
assert set(tombstone_api) == {('Deployment', 'ra-test-api-server')}
tombstone_api_text = tombstone_api[('Deployment', 'ra-test-api-server')]
assert 'automountServiceAccountToken: true' in tombstone_api_text
assert tombstone_api_text.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED') == 1
assert tombstone_api_text.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_INSTALLATION_ID') == 1
assert 'value: "01234567-89ab-cdef-0123-456789abcdef"' in tombstone_api_text
assert tombstone_api_text.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_COHORT_SUFFIXES_JSON') == 1
assert tombstone_api_text.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_RETIREMENT_TOMBSTONES_JSON') == 1
assert 'value: "[]"' in tombstone_api_text
assert 'value: "[\\"retired-v1\\"]"' in tombstone_api_text
assert set(active_api) == {('Deployment', 'ra-test-api-server')}
active_api_text = active_api[('Deployment', 'ra-test-api-server')]
assert 'automountServiceAccountToken: true' in active_api_text
assert active_api_text.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED') == 1
assert 'value: "[\\"p2a-v1\\",\\"p2a-v2\\"]"' in active_api_text
assert 'value: "[\\"retired-v1\\"]"' in active_api_text

controller_key = ('Deployment', 'ra-test-controller')
assert set(controller_one) == {controller_key}
assert set(controller_two) == {controller_key}
controller_v1 = controller_one[controller_key]
controller_v2 = controller_two[controller_key]
assert controller_v1.replace(
    'ra-test-authority-p2a-v1-manifest',
    'ra-test-authority-$ACTIVE-manifest') == controller_v2.replace(
        'ra-test-authority-p2a-v2-manifest',
        'ra-test-authority-$ACTIVE-manifest')
assert controller_v1.count(
    'name: SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED') == 1
assert controller_v1.count(
    'name: SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE') == 1
assert ('value: /etc/skypilot/resource-action-authority/auth/tokens' in
        controller_v1)
assert 'mountPath: /etc/skypilot/resource-action-authority/manifest.json' in (
    controller_v1)
assert 'subPath: manifest.json' in controller_v1
assert 'mountPath: /etc/skypilot/resource-action-authority/auth' in controller_v1
assert 'mountPath: /etc/skypilot/resource-action-authority/tls' in controller_v1
assert 'secretName: "authority-auth"' in controller_v1
assert 'secretName: "authority-tls"' in controller_v1
assert 'key: "tokens"' in controller_v1
assert 'key: "ca.crt"' in controller_v1
assert 'path: ca.crt' in controller_v1
assert 'qualification' not in controller_v1
assert 'key: "tls.crt"' not in controller_v1
assert 'key: "tls.key"' not in controller_v1
assert 'path: tls.crt' not in controller_v1
assert 'path: tls.key' not in controller_v1
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ACTIVE_COHORT' not in controller_v1
assert 'kind: NetworkPolicy' not in controller_v1

literal_env = [
    {'name': 'SKYPILOT_API_REQUEST_BACKEND', 'value': 'postgres'},
    {'name': 'SKYPILOT_API_SERVER_ROLE', 'value': 'authority-worker'},
    {'name': 'SKYPILOT_RELEASE_NAME', 'value': 'ra-test'},
    {
        'name': 'SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE',
        'value': '/etc/skypilot/resource-action-authority/auth/tokens',
    },
    {'name': 'SKYPILOT_STATE_DB_MIGRATION_MODE', 'value': 'verify'},
]
downward = [
    {'env': 'SKYPILOT_POD_NAME', 'field_path': 'metadata.name'},
    {'env': 'SKYPILOT_POD_NAMESPACE', 'field_path': 'metadata.namespace'},
    {'env': 'SKYPILOT_POD_UID', 'field_path': 'metadata.uid'},
]
release_input_keys = {
    'version', 'namespace', 'helm_full_name', 'cohort_suffix', 'cohort_id',
    'deployment_name', 'service_account_name', 'container_name', 'image',
    'image_pull_policy', 'command', 'args', 'health_port', 'preflight_port',
    'manifest_config_map', 'qualification_config_map', 'auth_secret',
    'tls_secret', 'database_secret', 'downward_api_fields', 'literal_env',
    'secret_env', 'resources', 'image_pull_secrets', 'pod_labels',
    'pod_annotations_without_manifest_hash', 'pod_security_context',
    'container_security_context', 'node_selector', 'affinity', 'tolerations',
    'topology_spread_constraints', 'priority_class_name',
    'runtime_class_name', 'scheduler_name',
    'termination_grace_period_seconds',
}
binding_keys = {
    'version', 'contract', 'projector_artifact_sha256', 'release_inputs',
    'expected_template_sha256', 'manifest_hash_annotation_json_pointer',
    'manifest_hash_placeholder',
}


def canonical_bytes(value):
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False).encode()


def projected_template(inputs):
    labels = {entry['key']: entry['value'] for entry in inputs['pod_labels']}
    env = copy.deepcopy(inputs['literal_env'])
    env.extend({
        'name': entry['name'],
        'valueFrom': {
            'secretKeyRef': {
                'name': entry['secret_name'],
                'key': entry['key'],
            }
        },
    } for entry in inputs['secret_env'])
    env.extend({
        'name': entry['env'],
        'valueFrom': {
            'fieldRef': {
                'apiVersion': 'v1',
                'fieldPath': entry['field_path'],
            }
        },
    } for entry in inputs['downward_api_fields'])
    manifest_ref = inputs['manifest_config_map']
    qualification_ref = inputs['qualification_config_map']
    auth_ref = inputs['auth_secret']
    tls_ref = inputs['tls_secret']
    mounts = [
        {
            'name': 'authority-manifest',
            'mountPath': manifest_ref['mount_path'],
            'subPath': manifest_ref['key'],
            'readOnly': True,
        },
        {
            'name': 'authority-qualification',
            'mountPath': qualification_ref['mount_path'],
            'subPath': qualification_ref['key'],
            'readOnly': True,
        },
        {
            'name': 'authority-auth',
            'mountPath': '/etc/skypilot/resource-action-authority/auth',
            'readOnly': True,
        },
        {
            'name': 'authority-tls',
            'mountPath': '/etc/skypilot/resource-action-authority/tls',
            'readOnly': True,
        },
        {'name': 'skypilot-role-runtime', 'mountPath': '/var/run/skypilot'},
        {
            'name': 'kube-api-access',
            'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount',
            'readOnly': True,
        },
    ]
    volumes = [
        {
            'name': 'authority-manifest',
            'configMap': {
                'name': manifest_ref['name'],
                'defaultMode': 292,
                'items': [{'key': manifest_ref['key'], 'path': manifest_ref['key']}],
            },
        },
        {
            'name': 'authority-qualification',
            'configMap': {
                'name': qualification_ref['name'],
                'defaultMode': 292,
                'items': [{
                    'key': qualification_ref['key'],
                    'path': qualification_ref['key'],
                }],
            },
        },
        {
            'name': 'authority-auth',
            'secret': {
                'secretName': auth_ref['name'],
                'defaultMode': 256,
                'items': [{'key': auth_ref['key'], 'path': 'tokens'}],
            },
        },
        {
            'name': 'authority-tls',
            'secret': {
                'secretName': tls_ref['name'],
                'defaultMode': 256,
                'items': [
                    {'key': tls_ref['cert_key'], 'path': 'tls.crt'},
                    {'key': tls_ref['private_key_key'], 'path': 'tls.key'},
                    {'key': tls_ref['ca_key'], 'path': 'ca.crt'},
                ],
            },
        },
        {'name': 'skypilot-role-runtime', 'emptyDir': {}},
        {
            'name': 'kube-api-access',
            'projected': {
                'defaultMode': 420,
                'sources': [
                    {
                        'serviceAccountToken': {
                            'expirationSeconds': 3607,
                            'path': 'token',
                        },
                    },
                    {
                        'configMap': {
                            'name': 'kube-root-ca.crt',
                            'items': [{'key': 'ca.crt', 'path': 'ca.crt'}],
                        },
                    },
                    {
                        'downwardAPI': {
                            'items': [{
                                'path': 'namespace',
                                'fieldRef': {
                                    'apiVersion': 'v1',
                                    'fieldPath': 'metadata.namespace',
                                },
                            }],
                        },
                    },
                ],
            },
        },
    ]
    container = {
        'name': inputs['container_name'],
        'image': inputs['image'],
        'imagePullPolicy': inputs['image_pull_policy'],
        'command': inputs['command'],
        'args': inputs['args'],
        'env': env,
        'ports': [
            {'name': 'health', 'containerPort': int(inputs['health_port']), 'protocol': 'TCP'},
            {'name': 'preflight', 'containerPort': int(inputs['preflight_port']), 'protocol': 'TCP'},
        ],
        'resources': inputs['resources'],
        'securityContext': inputs['container_security_context'],
        'terminationMessagePath': '/dev/termination-log',
        'terminationMessagePolicy': 'File',
        'lifecycle': {
            'preStop': {
                'exec': {
                    'command': ['/bin/sh', '-c', 'touch /var/run/skypilot/draining']
                }
            }
        },
        'startupProbe': {
            'httpGet': {'path': '/bootstrapz', 'port': 'health', 'scheme': 'HTTP'},
            'failureThreshold': 60,
            'periodSeconds': 10,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'livenessProbe': {
            'httpGet': {'path': '/livez', 'port': 'health', 'scheme': 'HTTP'},
            'failureThreshold': 3,
            'periodSeconds': 10,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'readinessProbe': {
            'httpGet': {'path': '/bootstrapz', 'port': 'health', 'scheme': 'HTTP'},
            'failureThreshold': 3,
            'periodSeconds': 5,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'volumeMounts': mounts,
    }
    spec = {
        'automountServiceAccountToken': False,
        'serviceAccount': inputs['service_account_name'],
        'serviceAccountName': inputs['service_account_name'],
        'terminationGracePeriodSeconds':
            inputs['termination_grace_period_seconds'],
        'restartPolicy': 'Always',
        'dnsPolicy': 'ClusterFirst',
        'enableServiceLinks': False,
        'hostNetwork': False,
        'hostPID': False,
        'hostIPC': False,
        'schedulerName': inputs['scheduler_name'] or 'default-scheduler',
        'priority': 0,
        'preemptionPolicy': 'PreemptLowerPriority',
        'securityContext': inputs['pod_security_context'],
        'containers': [container],
        'volumes': volumes,
        'tolerations': list(inputs['tolerations']) + [
            {
                'effect': 'NoExecute',
                'key': 'node.kubernetes.io/not-ready',
                'operator': 'Exists',
                'tolerationSeconds': 300,
            },
            {
                'effect': 'NoExecute',
                'key': 'node.kubernetes.io/unreachable',
                'operator': 'Exists',
                'tolerationSeconds': 300,
            },
        ],
    }
    if inputs['image_pull_secrets']:
        spec['imagePullSecrets'] = [
            {'name': name} for name in inputs['image_pull_secrets']
        ]
    if inputs['node_selector']:
        spec['nodeSelector'] = {
            entry['key']: entry['value'] for entry in inputs['node_selector']
        }
    if inputs['affinity'] is not None:
        spec['affinity'] = inputs['affinity']
    if inputs['topology_spread_constraints']:
        spec['topologySpreadConstraints'] = inputs[
            'topology_spread_constraints']
    if inputs['runtime_class_name'] is not None:
        spec['runtimeClassName'] = inputs['runtime_class_name']
    if inputs['scheduler_name'] is not None:
        spec['schedulerName'] = inputs['scheduler_name']
    return {
        'metadata': {
            'labels': labels,
            'annotations': {
                'skypilot.co/resource-action-manifest-sha256':
                    '$MANIFEST_SHA256'
            },
        },
        'spec': spec,
    }


for suffix in ('p2a-v1', 'p2a-v2'):
    manifest_document = active_one[
        ('ConfigMap', f'ra-test-authority-{suffix}-manifest')]
    preflight_manifest_document = active_one[
        ('ConfigMap', preflight_manifest_name('ra-test', suffix))]
    qualification_document = active_one[
        ('ConfigMap', f'ra-test-authority-{suffix}-qualification')]
    manifest_encoded = re.search(
        r'(?m)^  manifest\.json: "([A-Za-z0-9+/=]+)"$',
        manifest_document).group(1)
    qualification_encoded = re.search(
        r'(?m)^  qualification\.json: "([A-Za-z0-9+/=]+)"$',
        qualification_document).group(1)
    manifest_bytes = base64.b64decode(manifest_encoded, validate=True)
    preflight_manifest_encoded = re.search(
        r'(?m)^  manifest\.json: "([A-Za-z0-9+/=]+)"$',
        preflight_manifest_document).group(1)
    preflight_manifest_bytes = base64.b64decode(preflight_manifest_encoded,
                                                validate=True)
    assert preflight_manifest_bytes == manifest_bytes
    assert len(preflight_manifest_name('ra-test', suffix)) <= 63
    assert 'helm.sh/hook: pre-install,pre-upgrade' in (
        preflight_manifest_document)
    assert 'helm.sh/hook-weight: "-20"' in preflight_manifest_document
    assert ('helm.sh/hook-delete-policy: before-hook-creation' in
            preflight_manifest_document)
    assert 'app.kubernetes.io/instance: ra-test' in (
        preflight_manifest_document)
    other_preflight = other_release[
        ('ConfigMap', preflight_manifest_name('ra-other', suffix))]
    assert preflight_manifest_name('ra-test', suffix) != (
        preflight_manifest_name('ra-other', suffix))
    assert base64.b64decode(
        re.search(r'(?m)^  manifest\.json: "([A-Za-z0-9+/=]+)"$',
                  other_preflight).group(1), validate=True) != b''
    qualification_bytes = base64.b64decode(qualification_encoded,
                                           validate=True)
    manifest = json.loads(manifest_bytes)
    qualification = json.loads(qualification_bytes)
    assert canonical_bytes(manifest) == manifest_bytes
    assert qualification_bytes.endswith(b'\n')
    assert not qualification_bytes.endswith(b'\n\n')
    assert canonical_bytes(qualification) + b'\n' == qualification_bytes
    assert qualification_bytes == qualification_paths[suffix].read_bytes()
    assert set(qualification) == {
        'version', 'requested_reference', 'oci_manifest_digest',
        'oci_config_digest', 'source_commit', 'platform'
    }
    assert qualification['version'] == 1
    assert qualification['platform'] == 'linux/amd64'
    assert re.fullmatch('[0-9a-f]{40}', qualification['source_commit'])
    expected_scope = hashlib.sha256(
        f'skypilot-system\nra-test\n{suffix}'.encode()).hexdigest()
    assert manifest['cohort_id'] == (
        f'ra:01234567-89ab-cdef-0123-456789abcdef:'
        f'{expected_scope}:{suffix}')
    assert manifest['image']['requested_reference'] == qualification[
        'requested_reference']
    assert manifest['image']['oci_manifest_digest'] == qualification[
        'oci_manifest_digest']
    assert manifest['image']['oci_config_digest'] == qualification[
        'oci_config_digest']
    artifact_ref = manifest['image']['qualification_artifact']
    assert artifact_ref['byte_size'] == len(qualification_bytes)
    assert artifact_ref['sha256'] == hashlib.sha256(
        qualification_bytes).hexdigest()
    assert artifact_ref['source'] == 'helm_chart_configmap_v1'
    binding = manifest['pod_template_binding']
    assert set(binding) == binding_keys
    assert binding['contract'] == 'authority_worker_pod_template_v1'
    assert binding['projector_artifact_sha256'] == manifest[
        'pod_template_contract']['sha256']
    assert manifest['pod_template_contract']['repo_path'] == (
        'sky/serve/resource_action_provider_preflight.py')
    assert binding['manifest_hash_annotation_json_pointer'] == (
        '/metadata/annotations/skypilot.co~1resource-action-manifest-sha256')
    assert binding['manifest_hash_placeholder'] == '$MANIFEST_SHA256'
    assert manifest_bytes.count(b'$MANIFEST_SHA256') == 1
    inputs = binding['release_inputs']
    assert set(inputs) == release_input_keys
    assert inputs['literal_env'] == literal_env
    assert inputs['secret_env'] == [{
        'name': 'SKYPILOT_DB_CONNECTION_URI',
        'secret_name': 'external-db',
        'key': 'connection_string',
    }]
    assert inputs['downward_api_fields'] == downward
    assert inputs['cohort_suffix'] == suffix
    assert inputs['cohort_id'] == manifest['cohort_id']
    assert inputs['affinity'] is None
    assert inputs['priority_class_name'] is None
    assert inputs['runtime_class_name'] is None
    assert inputs['scheduler_name'] is None
    expected_template = projected_template(inputs)
    assert hashlib.sha256(canonical_bytes(expected_template)).hexdigest() == (
        binding['expected_template_sha256'])
    if resource_actions is not None:
        typed_manifest = (
            resource_actions.ProviderAuthorityWorkerCohortManifestV1.
            from_value(manifest))
        assert typed_manifest.canonical_bytes == manifest_bytes
        actual_projection = (
            resource_action_provider_preflight.
            project_provider_authority_worker_pod_template_v1(
                typed_manifest.pod_template_binding.release_inputs))
        assert actual_projection.canonical_bytes == canonical_bytes(
            expected_template)
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    deployment = active_one[('Deployment', f'ra-test-authority-{suffix}')]
    service_account = active_one[
        ('ServiceAccount', f'ra-test-authority-{suffix}')]
    assert re.search(
        r'skypilot\.co/resource-action-manifest-sha256: '
        + manifest_sha + r'$', deployment, re.MULTILINE)
    assert 'type: Recreate' in deployment
    assert 'path: /livez' in deployment
    assert deployment.count('path: /bootstrapz') == 2
    assert '/readyz' not in deployment
    env_names = re.findall(r'(?m)^        - name: (SKYPILOT_[A-Z0-9_]+)$',
                           deployment)
    assert env_names == [entry['name'] for entry in literal_env] + [
        'SKYPILOT_DB_CONNECTION_URI', 'SKYPILOT_POD_NAME',
        'SKYPILOT_POD_NAMESPACE', 'SKYPILOT_POD_UID'
    ]
    assert 'SKYPILOT_API_SERVER_INSTANCE_ID' not in deployment
    assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ACTIVE_COHORT' not in deployment
    assert 'subPath: manifest.json' in deployment
    assert 'subPath: qualification.json' in deployment
    assert deployment.count('automountServiceAccountToken: false') == 1
    assert 'automountServiceAccountToken: true' in service_account
    assert f'serviceAccount: ra-test-authority-{suffix}' in deployment
    assert f'serviceAccountName: ra-test-authority-{suffix}' in deployment
    assert 'priority: 0' in deployment
    assert 'preemptionPolicy: PreemptLowerPriority' in deployment
    assert deployment.count('name: kube-api-access') == 2
    assert ('mountPath: /var/run/secrets/kubernetes.io/serviceaccount' in
            deployment)
    assert 'expirationSeconds: 3607' in deployment
    assert 'name: kube-root-ca.crt' in deployment
    assert 'fieldPath: metadata.namespace' in deployment
    assert deployment.count('tolerationSeconds: 300') == 2
    assert 'key: node.kubernetes.io/not-ready' in deployment
    assert 'key: node.kubernetes.io/unreachable' in deployment

service_one = active_one[('Service', 'ra-test-authority-preflight')]
service_two = active_two[('Service', 'ra-test-authority-preflight')]
assert 'skypilot.co/authority-cohort: "p2a-v1"' in service_one
assert 'skypilot.co/authority-cohort: "p2a-v2"' in service_two
assert 'type: ClusterIP' in service_one
assert 'port: 46583' in service_one
policy = active_one[('NetworkPolicy', 'ra-test-authority-preflight')]
assert 'skypilot.co/role: controller' in policy
assert 'app: ra-test-controller' in policy
assert 'port: 46583' in policy
assert 'policyTypes:\n  - Ingress' in policy
assert '\negress:' not in policy
assert '\n  - Egress' not in policy
self_role = active_one[('Role', 'ra-test-authority-self-attestation')]
assert self_role.count('verbs: ["get"]') == 4
assert 'resources: ["pods"]' in self_role
assert 'resources: ["replicasets"]' in self_role
assert 'resources: ["deployments"]' in self_role
assert 'resources: ["serviceaccounts"]' in self_role
for forbidden in ('list', 'watch', 'create', 'update', 'patch', 'delete'):
    assert f'verbs: ["{forbidden}"]' not in self_role
for forbidden in ('secrets', 'configmaps'):
    assert f'resources: ["{forbidden}"]' not in self_role
assert 'kind: ClusterRole' not in Path(sys.argv[1]).read_text(encoding='utf-8')
self_binding = active_one[
    ('RoleBinding', 'ra-test-authority-self-attestation')]
assert self_binding.count('kind: ServiceAccount') == 2
assert 'name: ra-test-authority-p2a-v1' in self_binding
assert 'name: ra-test-authority-p2a-v2' in self_binding
retirement_role = active_one[
    ('Role', 'ra-test-authority-retirement-verifier')]
assert retirement_role.count('verbs: ["get"]') == 2
assert 'ra-test-authority-retired-v1' in retirement_role
retirement_binding = active_one[
    ('RoleBinding', 'ra-test-authority-retirement-verifier')]
assert 'name: ra-test-api-sa' in retirement_binding


def migration_job(rendered):
    jobs = {
        key: value for key, value in rendered.items() if key[0] == 'Job'
    }
    assert len(jobs) == 1, jobs.keys()
    return next(iter(jobs.values()))


def release_preflight(document):
    match = re.search(
        r'(?m)^        - name: '
        r'SKYPILOT_RESOURCE_ACTION_AUTHORITY_RELEASE_PREFLIGHT_JSON\n'
        r'          value: (".*")$', document)
    assert match is not None
    raw = json.loads(match.group(1))
    value = json.loads(raw)
    assert raw == json.dumps(value,
                             sort_keys=True,
                             separators=(',', ':'),
                             ensure_ascii=False)
    assert set(value) == {
        'version', 'namespace', 'helm_release_name', 'helm_full_name',
        'installation_id', 'enabled', 'live_manifest_files',
        'tombstone_suffixes'
    }
    return value


active_migration_job = migration_job(active_migration)
assert 'helm.sh/hook: pre-install,pre-upgrade' in active_migration_job
assert 'helm.sh/hook-weight: "-10"' in active_migration_job
assert 'helm.sh/hook-delete-policy: before-hook-creation' in (
    active_migration_job)
assert -20 < -10
active_proposal = release_preflight(active_migration_job)
assert active_proposal == {
    'version': 1,
    'namespace': 'skypilot-system',
    'helm_release_name': 'ra-test',
    'helm_full_name': 'ra-test',
    'installation_id': '01234567-89ab-cdef-0123-456789abcdef',
    'enabled': True,
    'live_manifest_files': [
        {
            'cohort_suffix': suffix,
            'path': ('/etc/skypilot/resource-action-authority/'
                     f'release-preflight/{suffix}/manifest.json'),
        } for suffix in ('p2a-v1', 'p2a-v2')
    ],
    'tombstone_suffixes': ['retired-v1'],
}
for index, manifest_file in enumerate(active_proposal['live_manifest_files']):
    suffix = manifest_file['cohort_suffix']
    assert active_migration_job.count(
        f'name: authority-preflight-{index}') == 2
    assert active_migration_job.count(
        f'mountPath: "{manifest_file["path"]}"') == 1
    assert active_migration_job.count('subPath: manifest.json') == 2
    assert active_migration_job.count('readOnly: true') >= 2
    assert (f'name: {preflight_manifest_name("ra-test", suffix)}' in
            active_migration_job)

disabled_migration_job = migration_job(disabled_migration)
disabled_proposal = release_preflight(disabled_migration_job)
assert disabled_proposal == {
    'version': 1,
    'namespace': 'skypilot-system',
    'helm_release_name': 'ra-test',
    'helm_full_name': 'ra-test',
    'installation_id': '',
    'enabled': False,
    'live_manifest_files': [],
    'tombstone_suffixes': [],
}
assert 'helm.sh/hook: pre-install,pre-upgrade' in disabled_migration_job
assert 'authority-preflight-' not in disabled_migration_job

renamed_migration_job = migration_job(disabled_renamed_migration)
renamed_proposal = release_preflight(renamed_migration_job)
assert renamed_proposal['namespace'] == disabled_proposal['namespace']
assert renamed_proposal['helm_release_name'] == disabled_proposal[
    'helm_release_name']
assert renamed_proposal['helm_full_name'] == 'proposed-full-name'
assert renamed_proposal['installation_id'] == ''
assert renamed_proposal['live_manifest_files'] == []
assert renamed_proposal['tombstone_suffixes'] == []

chart_managed_migration_job = migration_job(chart_managed_migration)
assert 'helm.sh/hook:' not in chart_managed_migration_job
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_RELEASE_PREFLIGHT_JSON' not in (
    chart_managed_migration_job)
assert 'name: ra-test-db-connection' in chart_managed_migration_job

disabled_api = disabled_api_compat[('Deployment', 'ra-test-api-server')]
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED' not in disabled_api
for env_name in (
        'SKYPILOT_RESOURCE_ACTION_AUTHORITY_INSTALLATION_ID',
        'SKYPILOT_RESOURCE_ACTION_AUTHORITY_COHORT_SUFFIXES_JSON',
        'SKYPILOT_RESOURCE_ACTION_AUTHORITY_RETIREMENT_TOMBSTONES_JSON'):
    assert disabled_api.count(f'name: {env_name}') == 1

disabled_controller = disabled_controller_compat[
    ('Deployment', 'ra-test-controller')]
assert 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED' not in disabled_controller
assert disabled_controller.count(
    'name: SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE') == 1
for volume_name in (
        'skypilot-resource-action-authority-auth',
        'skypilot-resource-action-authority-ca',
        'skypilot-resource-action-authority-manifest'):
    assert disabled_controller.count(f'name: {volume_name}') == 2
for mount_path in (
        '/etc/skypilot/resource-action-authority/auth',
        '/etc/skypilot/resource-action-authority/tls',
        '/etc/skypilot/resource-action-authority/manifest.json'):
    assert disabled_controller.count(f'mountPath: {mount_path}') == 1
PY
