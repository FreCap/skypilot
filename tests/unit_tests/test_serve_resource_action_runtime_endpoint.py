"""Pure prebooted-runtime, Skylet-job, and endpoint contract tests."""

import builtins
from collections.abc import Callable
import copy
import dataclasses
import pathlib

import pytest

from sky.serve import resource_actions as actions

_ARTIFACT_ROLES = (
    'ray_runtime',
    'skylet_runtime',
    'skylet_job_protocol',
    'skylet_state_schema',
    'startup_probe',
    'serve_canary_entrypoint',
)


def _artifact(path: str, marker: str = 'a') -> dict:
    return {'repo_path': path, 'byte_size': 17, 'sha256': marker * 64}


def _source(*, workspace: str = 'workspace-a') -> dict:
    return {
        'store': 'serve_version_specs',
        'service_name': 'service-a',
        'service_incarnation': '33333333-3333-4333-8333-333333333333',
        'service_version': 1,
        'yaml_content_sha256': 'b' * 64,
        'workspace': workspace,
    }


def _binding(role: str, index: int = 0) -> dict:
    marker = format(index + 1, 'x')
    return {
        'role': role,
        'workload_image_digest': 'sha256:' + marker * 64,
        'installed_root': f'artifacts/{role}',
        'source_manifest': _artifact(f'manifests/{role}.json', marker),
        'image_build_attestation': _artifact(f'attestations/{role}.json',
                                             marker),
        'measurement_contract': 'canonical_regular_file_tree_v1',
    }


def _job_contract() -> dict:
    return {
        'schema_id': 'skypilot.serve.prebooted-canary-job.v1',
        'schema_artifact': _artifact('skylet/job-schema.json', '1'),
        'renderer_artifact': _artifact('skylet/job-renderer.py', '2'),
        'state_store_schema_artifact': _artifact('skylet/job-state-schema.sql',
                                                 '3'),
        'protocol_artifact_role': 'skylet_job_protocol',
    }


def _durability() -> dict:
    return {
        'volume_name': 'skylet-state',
        'volume_kind': 'emptyDir',
        'store': 'sqlite_wal_synchronous_full_v1',
        'schema_artifact': _artifact('skylet/durability-schema.sql', '4'),
        'transaction_contract': 'job_and_start_outbox_same_transaction_v1',
        'drain_order': 'job_id_ascending',
        'launcher_contract': 'durable_run_token_and_post_exec_handshake_v1',
    }


def _runtime_metadata() -> dict:
    return {
        'runtime_setup_done': True,
        'has_ray': True,
        'has_skylet': True,
        'has_job_queue': True,
        'workdir_synced': False,
        'file_mounts_synced': False,
        'setup_done': True,
        'run_started': False,
    }


def _job_submission() -> dict:
    return {
        'protocol': 'skylet_idempotent_submit_v1',
        'submission_key_source': 'launch_action_id',
        'run_source': _source(),
        'contract': _job_contract(),
        'durability': _durability(),
        'job_spec_profile': 'ProviderSkyletJobSpecV1',
    }


def _post_provision() -> dict:
    return {
        'runtime_mode': 'prebooted_ray_skylet_v1',
        'runtime_artifacts': [
            _binding(role, index) for index, role in enumerate(_ARTIFACT_ROLES)
        ],
        'provision_runtime_metadata': _runtime_metadata(),
        'sync_workdir': 'assert_absent_skip',
        'sync_file_mounts': 'assert_absent_skip',
        'user_setup': 'assert_null_skip',
        'pre_exec_hooks_autostop': 'assert_absent_skip',
        'management_transport': 'skylet_grpc_only',
        'management_port': '46590',
        'ssh_fallback': False,
        'job_submission': _job_submission(),
    }


def _network_policy_prerequisite(*, uid: str = 'uid-network-policy') -> dict:
    spec = {
        'kind': 'NetworkPolicy',
        'contract': 'serve_action_network_policy_v1',
        'manifest': _artifact('prerequisites/network-policy.json', 'c'),
    }
    return {
        'role': 'endpoint_network_policy',
        'api_version': 'networking.k8s.io/v1',
        'kind': 'NetworkPolicy',
        'namespace': 'serve-canary',
        'name': 'serve-network',
        'uid': uid,
        'resource_version': '17',
        'deletion_timestamp': None,
        'spec': spec,
        'spec_sha256': actions.canonical_sha256(spec),
    }


def _namespace_prerequisite(slot: int) -> dict:
    spec = {'kind': 'Namespace', 'labels': [], 'annotations': []}
    return {
        'role': f'serve_lb_slot_{slot}_namespace',
        'api_version': 'v1',
        'kind': 'Namespace',
        'namespace': None,
        'name': 'skypilot-ha',
        'uid': 'uid-skypilot-ha',
        'resource_version': '18',
        'deletion_timestamp': None,
        'spec': spec,
        'spec_sha256': actions.canonical_sha256(spec),
    }


def _service_account_prerequisite(slot: int) -> dict:
    projection = {
        'namespace': 'skypilot-ha',
        'name': f'serve-lb-{slot}',
        'uid': f'uid-serve-lb-{slot}',
        'resource_version': str(20 + slot),
        'labels': [],
        'annotations': [],
        'automount_service_account_token': False,
        'image_pull_secrets': [],
        'legacy_secret_refs': [],
    }
    spec = {'kind': 'ServiceAccount', 'projection': projection}
    return {
        'role': f'serve_lb_slot_{slot}_service_account',
        'api_version': 'v1',
        'kind': 'ServiceAccount',
        'namespace': projection['namespace'],
        'name': projection['name'],
        'uid': projection['uid'],
        'resource_version': projection['resource_version'],
        'deletion_timestamp': None,
        'spec': spec,
        'spec_sha256': actions.canonical_sha256(spec),
    }


def _selector(slot: int) -> list[dict]:
    return [{
        'key': 'app',
        'value': 'serve-lb'
    }, {
        'key': 'slot',
        'value': str(slot)
    }]


def _caller_workload(slot: int) -> dict:
    return {
        'api_version': 'apps/v1',
        'kind': 'Deployment',
        'namespace': 'skypilot-ha',
        'name': f'serve-lb-{slot}',
        'uid': f'uid-serve-lb-deployment-{slot}',
        'resource_version': str(30 + slot),
        'generation': 7,
        'observed_generation': 7,
        'deletion_timestamp': None,
        'selector': _selector(slot),
        'pod_template_labels': [{
            'key': 'app',
            'value': 'serve-lb'
        }, {
            'key': 'component',
            'value': 'load-balancer'
        }, {
            'key': 'slot',
            'value': str(slot)
        }],
        'service_account_name': f'serve-lb-{slot}',
        'automount_service_account_token': False,
    }


def _caller(slot: int,
            *,
            namespace: str | None = None,
            empty_selector: bool = False) -> dict:
    selector = [] if empty_selector else _selector(slot)
    namespace = namespace or 'skypilot-ha'
    return {
        'role': f'serve_lb_slot_{slot}',
        'namespace': namespace,
        'namespace_uid': 'uid-skypilot-ha',
        'pod_selector': selector,
        'service_account_name': f'serve-lb-{slot}',
        'service_account_uid': f'uid-serve-lb-{slot}',
        'workload': _caller_workload(slot),
    }


def _endpoint() -> dict:
    return {
        'mode': 'podip',
        'application_port': '8080',
        'ambient_fallback': False,
        'prerequisite_projection': [
            _network_policy_prerequisite(),
            _namespace_prerequisite(0),
            _service_account_prerequisite(0),
            _namespace_prerequisite(1),
            _service_account_prerequisite(1),
        ],
        'required_callers': [_caller(0), _caller(1)],
    }


def test_workload_artifact_role_order_and_bindings_roundtrip() -> None:
    assert tuple(
        role.value for role in actions.ProviderWorkloadArtifactRoleV1) == (
            _ARTIFACT_ROLES)

    for index, role in enumerate(_ARTIFACT_ROLES):
        raw = _binding(role, index)
        parsed = actions.ProviderWorkloadArtifactBindingV1.from_value(raw)
        assert parsed.role.value == role
        assert parsed.canonical_value() == raw
        assert parsed.sha256 == actions.canonical_sha256(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('role', 'unknown'),
    ('workload_image_digest', '1' * 64),
    ('installed_root', ''),
    ('source_manifest', 'hash-only'),
    ('image_build_attestation', 'hash-only'),
    ('measurement_contract', 'other'),
])
def test_workload_artifact_binding_rejects_invalid_fields(
        field: str, value: object) -> None:
    raw = _binding('ray_runtime')
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderWorkloadArtifactBindingV1.from_value(raw)


def test_job_and_durability_contracts_roundtrip() -> None:
    job_raw = _job_contract()
    job = actions.ProviderSkyletJobContractV1.from_value(job_raw)
    assert job.canonical_value() == job_raw
    assert job.protocol_artifact_role is (
        actions.ProviderWorkloadArtifactRoleV1.SKYLET_JOB_PROTOCOL)

    durability_raw = _durability()
    durability = actions.ProviderSkyletDurabilityContractV1.from_value(
        durability_raw)
    assert durability.canonical_value() == durability_raw
    assert actions.ProviderSkyletDurabilityContractV1.from_value(
        durability.canonical_value(
        )).canonical_bytes == durability.canonical_bytes


@pytest.mark.parametrize(('field', 'value'), [
    ('schema_id', 'other'),
    ('schema_artifact', 'hash-only'),
    ('renderer_artifact', 'hash-only'),
    ('state_store_schema_artifact', 'hash-only'),
    ('protocol_artifact_role', 'ray_runtime'),
])
def test_job_contract_rejects_wrong_literals_or_children(
        field: str, value: object) -> None:
    raw = _job_contract()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderSkyletJobContractV1.from_value(raw)


@pytest.mark.parametrize('field', [
    'volume_name', 'volume_kind', 'store', 'transaction_contract',
    'drain_order', 'launcher_contract'
])
def test_durability_rejects_every_wrong_literal(field: str) -> None:
    raw = _durability()
    raw[field] = 'other'
    with pytest.raises(ValueError, match=field):
        actions.ProviderSkyletDurabilityContractV1.from_value(raw)


@pytest.mark.parametrize('field', list(_runtime_metadata()))
def test_runtime_metadata_requires_exact_strict_boolean(field: str) -> None:
    raw = _runtime_metadata()
    raw[field] = not raw[field]
    with pytest.raises(ValueError, match=field):
        actions.ProviderKubernetesProvisionRuntimeMetadataV1.from_value(raw)

    raw = _runtime_metadata()
    raw[field] = int(raw[field])
    with pytest.raises(TypeError, match=field):
        actions.ProviderKubernetesProvisionRuntimeMetadataV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('protocol', 'other'),
    ('submission_key_source', 'request_id'),
    ('run_source', {
        'store': 'serve_version_specs'
    }),
    ('contract', 'hash-only'),
    ('durability', 'hash-only'),
    ('job_spec_profile', 'OtherSpec'),
])
def test_job_submission_rejects_wrong_literals_or_children(
        field: str, value: object) -> None:
    raw = _job_submission()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesJobSubmissionV1.from_value(raw)


def test_post_provision_roundtrip_exact_six_role_order() -> None:
    raw = _post_provision()
    parsed = actions.ProviderKubernetesPostProvisionV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert tuple(item.role.value for item in parsed.runtime_artifacts) == (
        _ARTIFACT_ROLES)
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesPostProvisionV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize('mutation',
                         ['reverse', 'duplicate', 'missing', 'extra'])
def test_post_provision_rejects_runtime_artifact_role_drift(
        mutation: str) -> None:
    raw = _post_provision()
    artifacts = raw['runtime_artifacts']
    if mutation == 'reverse':
        artifacts.reverse()
    elif mutation == 'duplicate':
        artifacts[1] = copy.deepcopy(artifacts[0])
    elif mutation == 'missing':
        artifacts.pop()
    else:
        artifacts.append(copy.deepcopy(artifacts[-1]))
    with pytest.raises(ValueError, match='post-provision'):
        actions.ProviderKubernetesPostProvisionV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('runtime_mode', 'ordinary_bootstrap'),
    ('sync_workdir', 'sync'),
    ('sync_file_mounts', 'sync'),
    ('user_setup', 'run'),
    ('pre_exec_hooks_autostop', 'run'),
    ('management_transport', 'ssh'),
    ('management_port', '46591'),
    ('management_port', '046590'),
    ('management_port', 46590),
    ('ssh_fallback', True),
    ('ssh_fallback', 0),
    ('job_submission', 'untyped'),
])
def test_post_provision_rejects_wrong_literal_or_child(field: str,
                                                       value: object) -> None:
    raw = _post_provision()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesPostProvisionV1.from_value(raw)


def test_post_provision_requires_wire_list_and_typed_direct_tuple() -> None:
    raw = _post_provision()
    raw['runtime_artifacts'] = tuple(raw['runtime_artifacts'])
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesPostProvisionV1.from_value(raw)

    parsed = actions.ProviderKubernetesPostProvisionV1.from_value(
        _post_provision())
    with pytest.raises(ValueError, match='six typed'):
        dataclasses.replace(parsed,
                            runtime_artifacts=list(parsed.runtime_artifacts))


def test_endpoint_caller_workload_roundtrip_and_structural_boundary() -> None:
    raw = _caller_workload(0)
    parsed = actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw)
    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes

    # Generation freshness is endpoint authority, not standalone leaf shape.
    raw['observed_generation'] = 6
    structurally_valid = (
        actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw))
    assert structurally_valid.observed_generation != structurally_valid.generation


@pytest.mark.parametrize('field', ['selector', 'pod_template_labels'])
def test_endpoint_caller_workload_rejects_noncanonical_label_sets(
        field: str) -> None:
    raw = _caller_workload(0)
    raw[field].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw)

    raw = _caller_workload(0)
    raw[field].append(copy.deepcopy(raw[field][0]))
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('api_version', 'v1'),
    ('kind', 'Pod'),
    ('namespace', ''),
    ('name', ''),
    ('uid', ''),
    ('resource_version', ''),
    ('generation', 0),
    ('generation', True),
    ('observed_generation', 0),
    ('deletion_timestamp', '2026-08-01T00:00:00Z'),
    ('selector', []),
    ('selector', {}),
    ('pod_template_labels', []),
    ('pod_template_labels', {}),
    ('service_account_name', ''),
    ('automount_service_account_token', True),
    ('automount_service_account_token', 0),
])
def test_endpoint_caller_workload_rejects_invalid_shape(field: str,
                                                        value: object) -> None:
    raw = _caller_workload(0)
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw)


def test_endpoint_caller_roundtrip_and_selector_canonicality() -> None:
    raw = _caller(0)
    parsed = actions.ProviderKubernetesEndpointCallerV1.from_value(raw)
    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)

    raw['pod_selector'].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointCallerV1.from_value(raw)

    raw = _caller(0)
    raw['pod_selector'].append(copy.deepcopy(raw['pod_selector'][0]))
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointCallerV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('role', 'serve_lb_slot_2'),
    ('namespace', ''),
    ('namespace', 'x' * 254),
    ('namespace_uid', ''),
    ('pod_selector', {}),
    ('service_account_name', ''),
    ('service_account_uid', ''),
    ('workload', {}),
])
def test_endpoint_caller_rejects_invalid_shape(field: str,
                                               value: object) -> None:
    raw = _caller(0)
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesEndpointCallerV1.from_value(raw)


def test_endpoint_roundtrip_exact_prerequisite_and_caller_order() -> None:
    raw = _endpoint()
    parsed = actions.ProviderKubernetesEndpointContractV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert tuple(caller.role.value for caller in parsed.required_callers) == (
        'serve_lb_slot_0', 'serve_lb_slot_1')
    assert tuple(prerequisite.role.value
                 for prerequisite in parsed.prerequisite_projection) == (
                     'endpoint_network_policy', 'serve_lb_slot_0_namespace',
                     'serve_lb_slot_0_service_account',
                     'serve_lb_slot_1_namespace',
                     'serve_lb_slot_1_service_account')
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesEndpointContractV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize('mutation',
                         ['reverse', 'duplicate', 'missing', 'extra'])
def test_endpoint_rejects_caller_role_drift(mutation: str) -> None:
    raw = _endpoint()
    callers = raw['required_callers']
    if mutation == 'reverse':
        callers.reverse()
    elif mutation == 'duplicate':
        callers[1] = copy.deepcopy(callers[0])
    elif mutation == 'missing':
        callers.pop()
    else:
        callers.append(_caller(1))
    with pytest.raises(ValueError, match='caller'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize('mutation',
                         ['reverse', 'duplicate', 'missing', 'extra', 'empty'])
def test_endpoint_rejects_prerequisite_projection_role_drift(
        mutation: str) -> None:
    raw = _endpoint()
    projection = raw['prerequisite_projection']
    if mutation == 'reverse':
        projection.reverse()
    elif mutation == 'duplicate':
        projection[1] = copy.deepcopy(projection[0])
    elif mutation == 'missing':
        projection.pop()
    elif mutation == 'extra':
        projection.append(copy.deepcopy(projection[-1]))
    else:
        projection.clear()
    with pytest.raises(ValueError, match='projection|roles'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


def test_endpoint_rejects_legacy_generic_network_prerequisites() -> None:
    raw = _endpoint()
    raw['network_prerequisites'] = raw.pop('prerequisite_projection')
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)

    raw = _endpoint()
    raw['prerequisite_projection'] = [
        _network_policy_prerequisite(),
        _namespace_prerequisite(0),
    ]
    with pytest.raises(ValueError, match='exact five'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize('field', ['name', 'uid', 'resource_version', 'spec'])
def test_endpoint_namespace_aliases_differ_only_by_role(field: str) -> None:
    raw = _endpoint()
    namespace_one = raw['prerequisite_projection'][3]
    if field == 'spec':
        namespace_one['spec']['labels'] = [{'key': 'slot', 'value': 'one'}]
        namespace_one['spec_sha256'] = actions.canonical_sha256(
            namespace_one['spec'])
    else:
        namespace_one[field] = f'different-{field}'
    with pytest.raises(ValueError, match='aliases'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize('mutation', [
    'caller_namespace',
    'caller_namespace_uid',
    'caller_service_account_name',
    'caller_service_account_uid',
    'service_account_namespace',
    'service_account_automount',
    'service_account_image_pull_secret',
    'service_account_legacy_secret',
    'workload_namespace',
    'workload_service_account',
    'workload_selector',
    'workload_template_labels',
    'workload_observed_generation',
    'shared_service_account_key',
    'shared_service_account_uid',
    'shared_deployment_name',
    'shared_deployment_uid',
])
def test_endpoint_rejects_every_internal_projection_mismatch(
        mutation: str) -> None:
    raw = _endpoint()
    caller_zero, caller_one = raw['required_callers']
    service_account_zero = raw['prerequisite_projection'][2]
    service_account_one = raw['prerequisite_projection'][4]
    if mutation == 'caller_namespace':
        caller_one['namespace'] = 'other-namespace'
    elif mutation == 'caller_namespace_uid':
        caller_one['namespace_uid'] = 'other-namespace-uid'
    elif mutation == 'caller_service_account_name':
        caller_one['service_account_name'] = 'other-service-account'
    elif mutation == 'caller_service_account_uid':
        caller_one['service_account_uid'] = 'other-service-account-uid'
    elif mutation == 'service_account_namespace':
        service_account_one['namespace'] = 'other-namespace'
        service_account_one['spec']['projection'][
            'namespace'] = 'other-namespace'
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
    elif mutation == 'service_account_automount':
        service_account_one['spec']['projection'][
            'automount_service_account_token'] = True
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
    elif mutation == 'service_account_image_pull_secret':
        service_account_one['spec']['projection']['image_pull_secrets'] = [
            'pull-secret'
        ]
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
    elif mutation == 'service_account_legacy_secret':
        service_account_one['spec']['projection']['legacy_secret_refs'] = [
            'legacy-secret'
        ]
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
    elif mutation == 'workload_namespace':
        caller_one['workload']['namespace'] = 'other-namespace'
    elif mutation == 'workload_service_account':
        caller_one['workload']['service_account_name'] = 'other-service-account'
    elif mutation == 'workload_selector':
        caller_one['workload']['selector'][1]['value'] = 'different'
    elif mutation == 'workload_template_labels':
        caller_one['workload']['pod_template_labels'].pop()
    elif mutation == 'workload_observed_generation':
        caller_one['workload']['observed_generation'] = 6
    elif mutation == 'shared_service_account_key':
        service_account_one['name'] = service_account_zero['name']
        service_account_one['spec']['projection'][
            'name'] = service_account_zero['name']
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
        caller_one['service_account_name'] = service_account_zero['name']
        caller_one['workload']['service_account_name'] = service_account_zero[
            'name']
    elif mutation == 'shared_service_account_uid':
        service_account_one['uid'] = service_account_zero['uid']
        service_account_one['spec']['projection']['uid'] = service_account_zero[
            'uid']
        service_account_one['spec_sha256'] = actions.canonical_sha256(
            service_account_one['spec'])
        caller_one['service_account_uid'] = service_account_zero['uid']
    elif mutation == 'shared_deployment_name':
        caller_one['workload']['name'] = caller_zero['workload']['name']
    else:
        caller_one['workload']['uid'] = caller_zero['workload']['uid']
    expected_message = None
    if mutation.startswith('shared_service_account'):
        expected_message = 'endpoint ServiceAccounts must be distinct'
    elif mutation.startswith('shared_deployment'):
        expected_message = 'endpoint caller Deployments must be distinct'
    with pytest.raises(ValueError, match=expected_message):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('mode', 'service'),
    ('application_port', '0'),
    ('application_port', '08080'),
    ('application_port', '65536'),
    ('application_port', 8080),
    ('ambient_fallback', True),
    ('ambient_fallback', 0),
    ('prerequisite_projection', {}),
    ('required_callers', {}),
])
def test_endpoint_rejects_wrong_literals_or_wire_collections(
        field: str, value: object) -> None:
    raw = _endpoint()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


def test_endpoint_requires_typed_direct_tuples() -> None:
    parsed = actions.ProviderKubernetesEndpointContractV1.from_value(
        _endpoint())
    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(parsed,
                            prerequisite_projection=list(
                                parsed.prerequisite_projection))
    with pytest.raises(ValueError, match='two typed'):
        dataclasses.replace(parsed,
                            required_callers=list(parsed.required_callers))

    caller = parsed.required_callers[0]
    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(caller, pod_selector=list(caller.pod_selector))
    with pytest.raises(TypeError, match='invalid type'):
        dataclasses.replace(caller, workload={})

    workload = caller.workload
    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(workload, selector=list(workload.selector))


@pytest.mark.parametrize('field',
                         ['prerequisite_projection', 'required_callers'])
def test_endpoint_requires_wire_lists(field: str) -> None:
    raw = _endpoint()
    raw[field] = tuple(raw[field])
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize('field', ['selector', 'pod_template_labels'])
def test_endpoint_caller_workload_requires_wire_lists(field: str) -> None:
    raw = _caller_workload(0)
    raw[field] = tuple(raw[field])
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(raw)


def test_leafs_accept_structural_values_without_cross_capsule_authority(
) -> None:
    post_raw = _post_provision()
    post_raw['job_submission']['run_source'] = _source(
        workspace='different-but-structurally-valid')
    # The fixture also deliberately uses six different workload image digests
    # and distinct job/durability state-schema artifact references.
    post = actions.ProviderKubernetesPostProvisionV1.from_value(post_raw)
    assert len({item.workload_image_digest for item in post.runtime_artifacts
               }) == 6
    assert (post.job_submission.contract.state_store_schema_artifact
            != post.job_submission.durability.schema_artifact)

    caller_raw = _caller(0,
                         namespace='structurally-valid-other-namespace',
                         empty_selector=True)
    caller = actions.ProviderKubernetesEndpointCallerV1.from_value(caller_raw)
    assert not caller.pod_selector
    assert caller.namespace != caller.workload.namespace

    workload_raw = _caller_workload(0)
    workload_raw['observed_generation'] = 6
    workload_raw['service_account_name'] = 'structurally-valid-other-sa'
    workload = actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value(
        workload_raw)
    assert workload.observed_generation != workload.generation

    service_account_raw = _service_account_prerequisite(0)
    service_account_raw['spec']['projection'][
        'automount_service_account_token'] = True
    service_account_raw['spec_sha256'] = actions.canonical_sha256(
        service_account_raw['spec'])
    service_account = actions.ProviderKubernetesPrerequisiteV1.from_value(
        service_account_raw)
    assert service_account.spec.projection.automount_service_account_token

    endpoint_raw = _endpoint()
    endpoint_raw['required_callers'][0] = caller_raw
    with pytest.raises(ValueError):
        actions.ProviderKubernetesEndpointContractV1.from_value(endpoint_raw)


def test_pure_runtime_endpoint_leaves_never_open_artifacts_or_live_state(
        monkeypatch: pytest.MonkeyPatch) -> None:

    def _forbidden(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError(
            'pure runtime/endpoint leaves must not perform I/O')

    monkeypatch.setattr(builtins, 'open', _forbidden)
    monkeypatch.setattr(pathlib.Path, 'open', _forbidden)
    monkeypatch.setattr(pathlib.Path, 'read_bytes', _forbidden)
    actions.ProviderKubernetesPostProvisionV1.from_value(_post_provision())
    actions.ProviderKubernetesEndpointContractV1.from_value(_endpoint())


@pytest.mark.parametrize(('factory', 'parser'), [
    (lambda: _binding('ray_runtime'),
     actions.ProviderWorkloadArtifactBindingV1.from_value),
    (_job_contract, actions.ProviderSkyletJobContractV1.from_value),
    (_durability, actions.ProviderSkyletDurabilityContractV1.from_value),
    (_runtime_metadata,
     actions.ProviderKubernetesProvisionRuntimeMetadataV1.from_value),
    (_job_submission, actions.ProviderKubernetesJobSubmissionV1.from_value),
    (_post_provision, actions.ProviderKubernetesPostProvisionV1.from_value),
    (lambda: _caller_workload(0),
     actions.ProviderKubernetesEndpointCallerWorkloadV1.from_value),
    (lambda: _caller(0), actions.ProviderKubernetesEndpointCallerV1.from_value),
    (_endpoint, actions.ProviderKubernetesEndpointContractV1.from_value),
])
def test_runtime_endpoint_contracts_are_closed(
        factory: Callable[[], dict], parser: Callable[[object],
                                                      object]) -> None:
    raw = factory()
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        parser(raw)
