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


def _namespace_prerequisite() -> dict:
    spec = {'kind': 'Namespace', 'labels': [], 'annotations': []}
    return {
        'api_version': 'v1',
        'kind': 'Namespace',
        'namespace': None,
        'name': 'lb-a',
        'uid': 'uid-lb-a',
        'resource_version': '18',
        'deletion_timestamp': None,
        'spec': spec,
        'spec_sha256': actions.canonical_sha256(spec),
    }


def _caller(slot: int,
            *,
            namespace: str | None = None,
            empty_selector: bool = False) -> dict:
    selector = [] if empty_selector else [{
        'key': 'app',
        'value': 'serve-lb'
    }, {
        'key': 'slot',
        'value': str(slot)
    }]
    namespace = namespace or f'lb-{slot}'
    return {
        'role': f'serve_lb_slot_{slot}',
        'namespace': namespace,
        'namespace_uid': f'uid-{namespace}',
        'pod_selector': selector,
        'service_account_name': f'serve-lb-{slot}',
        'service_account_uid': f'uid-serve-lb-{slot}',
    }


def _endpoint() -> dict:
    # networking.k8s.io sorts before v1 under the exact protocol key.
    prerequisites = [_network_policy_prerequisite(), _namespace_prerequisite()]
    return {
        'mode': 'podip',
        'application_port': '8080',
        'ambient_fallback': False,
        'network_prerequisites': prerequisites,
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


def test_endpoint_rejects_unsorted_or_duplicate_prerequisite_key() -> None:
    raw = _endpoint()
    raw['network_prerequisites'].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)

    raw = _endpoint()
    raw['network_prerequisites'].insert(
        1, _network_policy_prerequisite(uid='different-uid'))
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesEndpointContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('mode', 'service'),
    ('application_port', '0'),
    ('application_port', '08080'),
    ('application_port', '65536'),
    ('application_port', 8080),
    ('ambient_fallback', True),
    ('ambient_fallback', 0),
    ('network_prerequisites', {}),
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
                            network_prerequisites=list(
                                parsed.network_prerequisites))
    with pytest.raises(ValueError, match='two typed'):
        dataclasses.replace(parsed,
                            required_callers=list(parsed.required_callers))


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

    caller_zero = _caller(0, namespace='LB Namespace', empty_selector=True)
    caller_one = _caller(1, namespace='LB Namespace', empty_selector=True)
    # Caller identity equality, empty selectors, and an empty prerequisite
    # collection remain structural here; preflight/capsule owns live equality.
    for field in ('namespace_uid', 'service_account_name',
                  'service_account_uid'):
        caller_one[field] = caller_zero[field]
    endpoint_raw = {
        'mode': 'podip',
        'application_port': '9999',
        'ambient_fallback': False,
        'network_prerequisites': [],
        'required_callers': [caller_zero, caller_one],
    }
    endpoint = actions.ProviderKubernetesEndpointContractV1.from_value(
        endpoint_raw)
    assert not endpoint.network_prerequisites
    assert all(not caller.pod_selector for caller in endpoint.required_callers)


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
