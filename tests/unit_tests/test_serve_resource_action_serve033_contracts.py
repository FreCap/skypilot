"""Pure Serve033 coverage and authority-worker contract tests."""
# pylint: disable=protected-access

import dataclasses
import datetime
import uuid

import pytest

from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions
from tests.unit_tests import (test_serve_resource_action_launch_execution_config
                              as launch_config_fixtures)

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'
_DECISION_TIME = '2026-08-01T01:00:00.000000Z'
_OBSERVED_TIME = '2026-08-01T01:01:00.000000Z'
_REGISTERED_TIME = '2026-08-01T01:02:00.000000Z'


def _coverage_identity(action_type: str = 'launch') -> dict:
    return {
        'version': 1,
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 3,
        'action_type': action_type,
    }


def _coverage(
    *,
    action_type: str = 'launch',
    outcome: str = 'REPRESENTABLE',
    reason: str | None = None,
    cohort_ref: bool = True,
) -> dict:
    identity = actions.CoverageDecisionIdentityV1.from_value(
        _coverage_identity(action_type))
    return {
        'decision_id': str(identity.decision_id),
        'service_name': 'svc',
        'service_hash': identity.service_hash,
        'service_incarnation': str(identity.service_incarnation),
        'replica_id': identity.replica_id,
        'replica_incarnation': str(identity.replica_incarnation),
        'desired_generation': identity.desired_generation,
        'action_type': identity.action_type.value,
        'normalizer_contract_version': 1,
        'normalization_outcome': outcome,
        'not_representable_reason': reason,
        'worker_cohort_ref_id':
            (str(identity.decision_id) if cohort_ref else None),
        'admitted_at': _DECISION_TIME,
    }


def _artifact(path: str, digest_character: str) -> dict:
    return {
        'repo_path': path,
        'byte_size': 17,
        'sha256': digest_character * 64,
    }


def _qualification() -> dict:
    return {
        'requested_reference':
            ('registry.example/authority@sha256:' + '1' * 64),
        'oci_manifest_digest': 'sha256:' + '1' * 64,
        'oci_config_digest': 'sha256:' + '2' * 64,
        'qualification_artifact': _artifact('images/authority.json', '3'),
    }


def _cohort() -> dict:
    manifest = {
        'version': 1,
        'cohort_id': 'authority-v1',
        'namespace': 'skypilot-system',
        'deployment_name': 'skypilot-authority-v1',
        'service_account_name': 'skypilot-authority-v1',
        'container_name': 'skypilot-authority-worker',
        'image': _qualification(),
        'pod_template_contract': _artifact('charts/worker.yaml', '4'),
        'artifact_inventory': _artifact('inventories/artifacts.json', '5'),
        'callable_inventory': _artifact('inventories/callables.json', '6'),
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1),
    }
    return {
        'version': 1,
        'manifest': manifest,
        'manifest_sha256': actions.canonical_sha256(manifest),
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'service-account-uid-v1',
    }


def _worker(pod_uid: str) -> dict:
    cohort = _cohort()
    manifest = cohort['manifest']
    qualification = _qualification()
    runtime = {
        'raw_image_id': 'containerd://sha256:' + '2' * 64,
        'runtime_image_id_scheme': 'containerd',
        'runtime_image_id_digest': 'sha256:' + '2' * 64,
        'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
        'qualified_oci_config_digest': 'sha256:' + '2' * 64,
        'qualification_artifact_sha256': qualification['qualification_artifact']
                                         ['sha256'],
        'runtime_id_contract': 'qualified_oci_config_digest_v1',
    }
    return {
        'namespace': manifest['namespace'],
        'pod_name': f'worker-{pod_uid}',
        'pod_uid': pod_uid,
        'pod_resource_version': '101',
        'pod_service_account_name': manifest['service_account_name'],
        'pod_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'ReplicaSet',
            'name': 'skypilot-authority-v1-abc',
            'uid': 'replicaset-uid-v1',
        },
        'replica_set_name': 'skypilot-authority-v1-abc',
        'replica_set_uid': 'replicaset-uid-v1',
        'replica_set_resource_version': '102',
        'replica_set_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'Deployment',
            'name': manifest['deployment_name'],
            'uid': cohort['deployment_uid'],
        },
        'deployment_name': manifest['deployment_name'],
        'deployment_uid': cohort['deployment_uid'],
        'deployment_resource_version': '103',
        'deployment_generation': 5,
        'deployment_observed_generation': 5,
        'pod_template_contract_sha256': manifest['pod_template_contract']
                                        ['sha256'],
        'image': {
            'qualification': qualification,
            'runtime': runtime,
        },
        'service_account_uid': cohort['service_account_uid'],
        'artifact_inventory_sha256': manifest['artifact_inventory']['sha256'],
        'callable_inventory_sha256': manifest['callable_inventory']['sha256'],
        'handler_allowlist_sha256': actions.canonical_sha256(
            manifest['handler_allowlist']),
        'observed_at': _OBSERVED_TIME,
    }


def _registration(pod_uid: str) -> dict:
    return {
        'worker': _worker(pod_uid),
        'pod_ready': True,
        'deployment_spec_replicas': 2,
        'deployment_status_observed_generation': 5,
        'deployment_ready_replicas': 2,
        'deployment_available_replicas': 2,
        'registered_at': _REGISTERED_TIME,
    }


def _registration_set(*pod_uids: str) -> dict:
    cohort = _cohort()
    return {
        'version': 1,
        'cohort_identity_sha256': actions.canonical_sha256(cohort),
        'workers': [_registration(pod_uid) for pod_uid in pod_uids],
    }


def _reference() -> dict:
    coverage = _coverage()
    return {
        'version': 1,
        'decision_id': coverage['decision_id'],
        'cohort_id': 'authority-v1',
        'service_hash': _SERVICE_UUID,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 3,
        'action_type': 'launch',
        'controller_owner_fence': 'owner-fence-7',
        'lifecycle_epoch': 4,
        'preparation_capability_sha256': 'd' * 64,
    }


def _attempt(phase: str) -> dict:
    value = {
        'decision_id': _coverage()['decision_id'],
        'request_sequence': 1,
        'logical_attempt': 1,
        'request_role': 'PRIMARY_LAUNCH',
        'phase': phase,
        'legacy_request_id': None,
        'terminal_request_status': None,
        'retry_disposition': None,
        'admitted_at': '2026-08-01T01:03:00.000000Z',
        'request_bound_at': None,
        'completed_at': None,
        'updated_at': '2026-08-01T01:03:00.000000Z',
    }
    if phase == 'REQUEST_BOUND':
        value.update({
            'legacy_request_id': 'request-1',
            'request_bound_at': '2026-08-01T01:03:01.000000Z',
            'updated_at': '2026-08-01T01:03:01.000000Z',
        })
    elif phase == 'COMPLETE':
        value.update({
            'legacy_request_id': 'request-1',
            'terminal_request_status': 'SUCCEEDED',
            'retry_disposition': 'TERMINAL',
            'request_bound_at': '2026-08-01T01:03:01.000000Z',
            'completed_at': '2026-08-01T01:03:02.000000Z',
            'updated_at': '2026-08-01T01:03:02.000000Z',
        })
    elif phase in ('ABANDONED_PRE_SUBMIT', 'REQUEST_ASSOCIATION_UNKNOWN'):
        value.update({
            'completed_at': '2026-08-01T01:03:02.000000Z',
            'updated_at': '2026-08-01T01:03:02.000000Z',
        })
    return value


def _identity(generation: int = 1) -> dict:
    return {
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': generation,
    }


def _target() -> dict:
    return launch_config_fixtures._target()


def _resources() -> dict:
    return launch_config_fixtures._resource_snapshot()


def _launch_invocation() -> dict:
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'launch',
        'resource_identity': _identity(),
        'requested_target': _target(),
        'launch': launch_config_fixtures.launch_payload(_identity(),
                                                        _target(),
                                                        _resources(),
                                                        workspace='workspace'),
        'down': None,
    }


def _down_invocation() -> dict:
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'down',
        'resource_identity': _identity(2),
        'requested_target': _target(),
        'launch': None,
        'down': {
            'cluster_name': 'svc-7',
            'expected_cluster_record_uuid': _CLUSTER_UUID,
            'workspace': 'workspace',
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        },
    }


def test_normalization_enums_are_closed_and_precedence_is_exact() -> None:
    launch_values = tuple(
        reason.value for reason in
        actions.PROVIDER_LAUNCH_NOT_REPRESENTABLE_REASON_PRECEDENCE)
    assert launch_values == (
        'request_contract', 'secret_or_tls_material', 'source_mismatch',
        'policy_configured_or_mutated', 'managed_secrets', 'multi_task',
        'multi_node', 'multi_resource', 'mount_or_storage', 'non_kubernetes',
        'spot', 'non_direct_pod_topology', 'port_contract',
        'reserved_label_collision', 'mutable_image',
        'custom_provider_implementation', 'preflight_unavailable_or_invalid',
        'authority_worker_attestation', 'authorization_or_principal_drift',
        'prerequisite_or_network_drift', 'admitted_object_contract',
        'runtime_or_job_contract', 'unrepresented_execution_config',
        'unrepresented_resource', 'unfrozen_placement', 'unfrozen_identity',
        'unfrozen_kubernetes_scope', 'target_mismatch')
    assert tuple(
        reason.precedence
        for reason in actions.ProviderLaunchNotRepresentableReasonV1) == tuple(
            range(len(launch_values)))
    assert tuple(
        reason.value
        for reason in actions.PROVIDER_DOWN_NOT_REPRESENTABLE_REASON_PRECEDENCE
    ) == ('request_contract', 'prior_launch_basis', 'target_mismatch',
          'preflight_unavailable_or_invalid', 'authority_worker_attestation',
          'authorization_or_principal_drift', 'prerequisite_or_network_drift',
          'policy_configured_or_mutated', 'unrepresented_execution_config',
          'unfrozen_kubernetes_scope')
    with pytest.raises(ValueError):
        actions.NormalizationOutcome('representable')
    with pytest.raises(ValueError):
        actions.ProviderLaunchNotRepresentableReasonV1('new_reason')


def test_serve033_storage_enums_are_exact() -> None:
    assert tuple(value.value for value in actions.NormalizationOutcome) == (
        'REPRESENTABLE', 'NOT_REPRESENTABLE')
    assert tuple(
        value.value for value in actions.WorkerCohortLifecycleState) == (
            'REGISTERING', 'ACCEPTING', 'DRAINING', 'REMOVAL_AUTHORIZED',
            'RETIRED')
    assert tuple(
        value.value for value in actions.WorkerCohortReferenceState) == (
            'PREPARING', 'SHADOW_ACTIVE', 'ACTION_ACTIVE', 'RELEASED')
    assert tuple(
        value.value for value in actions.CoverageAttemptTerminalStatus) == (
            'SUCCEEDED', 'FAILED', 'CANCELLED')
    assert tuple(
        value.value for value in actions.CoverageAttemptRetryDisposition) == (
            'RETRY_SAME_DECISION', 'TERMINAL', 'REPLAN_NEW_GENERATION', 'BLOCK')
    assert tuple(value.value for value in actions.ShadowRequestRole) == (
        'PRIMARY_LAUNCH', 'PRIMARY_DOWN', 'LAUNCH_CLEANUP_DOWN')
    assert tuple(value.value for value in actions.CoverageAttemptPhase) == (
        'PRE_SUBMIT', 'REQUEST_BOUND', 'COMPLETE', 'ABANDONED_PRE_SUBMIT',
        'REQUEST_ASSOCIATION_UNKNOWN')
    assert actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1 == (
        'serve_shadow_candidate_launch', 'serve_shadow_candidate_down',
        'serve_resource_action_launch', 'serve_resource_action_down')


def test_qualified_image_binds_requested_reference_to_manifest() -> None:
    qualification = _qualification()
    qualification['oci_manifest_digest'] = 'sha256:' + 'f' * 64
    with pytest.raises(ValueError, match='requested reference digest'):
        actions.ProviderOCIImageQualificationV1.from_value(qualification)


@pytest.mark.parametrize('raw_image_id,scheme', [
    ('cri-o://sha256:' + '2' * 64, 'containerd'),
    ('containerd://sha256:' + '4' * 64, 'containerd'),
    ('containerd://unrelated-raw-value', 'containerd'),
    ('docker-pullable://sha256:' + '2' * 64, 'docker-pullable'),
    ('docker-pullable://@sha256:' + '2' * 64, 'docker-pullable'),
    ('docker-pullable://repo name@sha256:' + '2' * 64, 'docker-pullable'),
    ('docker-pullable://user@repo@sha256:' + '2' * 64, 'docker-pullable'),
])
def test_runtime_image_binds_raw_id_scheme_and_digest(raw_image_id: str,
                                                      scheme: str) -> None:
    runtime = _worker('pod-uid')['image']['runtime']
    runtime['raw_image_id'] = raw_image_id
    runtime['runtime_image_id_scheme'] = scheme
    with pytest.raises(ValueError, match='raw runtime image ID'):
        actions.ProviderRuntimeImageIdentityV1.from_value(runtime)


def test_runtime_image_scheme_union_is_closed() -> None:
    assert actions.ProviderRuntimeImageIdentityV1._SCHEMES == frozenset(
        {'containerd', 'cri-o', 'docker-pullable'})
    runtime = _worker('pod-uid')['image']['runtime']
    runtime['raw_image_id'] = 'docker://sha256:' + '2' * 64
    runtime['runtime_image_id_scheme'] = 'docker'
    with pytest.raises(ValueError, match='scheme is unsupported'):
        actions.ProviderRuntimeImageIdentityV1.from_value(runtime)


def test_docker_pullable_runtime_image_parses_terminal_digest() -> None:
    runtime = _worker('pod-uid')['image']['runtime']
    runtime['raw_image_id'] = ('docker-pullable://registry.example/authority@'
                               'sha256:' + '2' * 64)
    runtime['runtime_image_id_scheme'] = 'docker-pullable'
    assert actions.ProviderRuntimeImageIdentityV1.from_value(
        runtime).runtime_image_id_digest == 'sha256:' + '2' * 64


@pytest.mark.parametrize('requested_reference', [
    'https://registry.example/repo@sha256:' + '1' * 64,
    'registry.example/repo name@sha256:' + '1' * 64,
    'user@registry.example/repo@sha256:' + '1' * 64,
])
def test_qualified_image_rejects_noncanonical_oci_reference(
        requested_reference: str) -> None:
    qualification = _qualification()
    qualification['requested_reference'] = requested_reference
    with pytest.raises(ValueError, match='canonical secret-free OCI'):
        actions.ProviderOCIImageQualificationV1.from_value(qualification)


@pytest.mark.parametrize(
    'cohort_id',
    ['not/a-dns-label', 'A_UPPER', 'a' * 64, '-leading', 'trailing-'])
def test_cohort_manifest_requires_dns_label_id(cohort_id: str) -> None:
    cohort = _cohort()
    cohort['manifest']['cohort_id'] = cohort_id
    cohort['manifest_sha256'] = actions.canonical_sha256(cohort['manifest'])
    with pytest.raises(ValueError, match='DNS label'):
        actions.ProviderAuthorityWorkerCohortV1.from_value(cohort)


def test_coverage_identity_uses_kernel_uuidv5_and_is_immutable() -> None:
    identity = actions.CoverageDecisionIdentityV1.from_value(
        _coverage_identity())
    expected = kernel_actions.ResourceActionIdentity(
        service_hash=_SERVICE_UUID,
        service_incarnation=uuid.UUID(_SERVICE_UUID),
        replica_id=7,
        replica_incarnation=uuid.UUID(_REPLICA_UUID),
        desired_generation=3,
        action_kind=kernel_actions.ActionKind.LAUNCH)
    assert identity.decision_id == expected.action_id
    assert actions.CoverageDecisionIdentityV1.from_value(
        identity.canonical_value()) == identity
    with pytest.raises(dataclasses.FrozenInstanceError):
        identity.replica_id = 8


@pytest.mark.parametrize('field,value', [
    ('replica_id', 2**63),
    ('desired_generation', 2**63),
])
def test_coverage_identity_rejects_postgres_bigint_overflow(
        field: str, value: int) -> None:
    identity = _coverage_identity()
    identity[field] = value
    with pytest.raises(ValueError, match='no greater than'):
        actions.CoverageDecisionIdentityV1.from_value(identity)


@pytest.mark.parametrize('service_hash', [
    '00000000-0000-0000-0000-000000000000',
    '11111111-1111-1111-1111-111111111111',
])
def test_coverage_identity_requires_serve033_schema_uuid(
        service_hash: str) -> None:
    identity = _coverage_identity()
    identity['service_hash'] = service_hash
    identity['service_incarnation'] = service_hash
    with pytest.raises(ValueError, match='RFC 4122 version 1..5'):
        actions.CoverageDecisionIdentityV1.from_value(identity)


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'extra': None}), 'unknown or missing'),
    (lambda value: value.pop('service_name'), 'unknown or missing'),
    (lambda value: value.update({'decision_id': _CLUSTER_UUID}), 'UUIDv5'),
    (lambda value: value.update({'service_incarnation': _CLUSTER_UUID}),
     'service_hash'),
    (lambda value: value.update({'service_name': 's' * 257}), '1..256'),
    (lambda value: value.update({'normalizer_contract_version': 2}),
     'integer 1'),
    (lambda value: value.update({'normalization_outcome': 'MAYBE'}),
     'unsupported'),
    (lambda value: value.update({'worker_cohort_ref_id': _CLUSTER_UUID}),
     'decision ID'),
])
def test_coverage_rejects_unknown_missing_uuid_hash_enum_and_bounds(
        mutate, match: str) -> None:
    value = _coverage()
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.CoverageDecisionV1.from_value(value)


def test_postgres_backed_text_rejects_nul() -> None:
    coverage = _coverage()
    coverage['service_name'] = 'svc\x00hidden'
    with pytest.raises(ValueError, match=r'U\+0000'):
        actions.CoverageDecisionV1.from_value(coverage)


def test_coverage_enforces_outcome_pair_and_kind_refined_reason() -> None:
    represented = actions.CoverageDecisionV1.from_value(_coverage())
    assert represented.identity.decision_id == represented.decision_id
    assert represented.not_representable_reason is None

    launch = actions.CoverageDecisionV1.from_value(
        _coverage(outcome='NOT_REPRESENTABLE', reason='mount_or_storage'))
    assert launch.not_representable_reason is (
        actions.ProviderLaunchNotRepresentableReasonV1.MOUNT_OR_STORAGE)
    down = actions.CoverageDecisionV1.from_value(
        _coverage(action_type='down',
                  outcome='NOT_REPRESENTABLE',
                  reason='prior_launch_basis'))
    assert down.not_representable_reason is (
        actions.ProviderDownNotRepresentableReasonV1.PRIOR_LAUNCH_BASIS)

    with pytest.raises(ValueError, match='null reason'):
        actions.CoverageDecisionV1.from_value(
            _coverage(reason='request_contract'))
    with pytest.raises(ValueError, match='requires a closed reason'):
        actions.CoverageDecisionV1.from_value(
            _coverage(outcome='NOT_REPRESENTABLE'))
    value = _coverage(outcome='NOT_REPRESENTABLE', reason='request_contract')
    value['not_representable_reason'] = (
        actions.ProviderDownNotRepresentableReasonV1.REQUEST_CONTRACT)
    with pytest.raises(ValueError, match='wrong action kind'):
        actions.CoverageDecisionV1(**value)
    with pytest.raises(ValueError, match='unsupported'):
        actions.CoverageDecisionV1.from_value(
            _coverage(outcome='NOT_REPRESENTABLE', reason='new_reason'))


def test_cohort_identity_is_closed_bounded_and_hash_linked() -> None:
    cohort = actions.WorkerCohortIdentityV1.from_value(_cohort())
    assert cohort.cohort_id == 'authority-v1'
    assert cohort.deployment_uid == 'deployment-uid-v1'
    assert actions.WorkerCohortIdentityV1.from_value(
        cohort.canonical_value()).canonical_bytes == cohort.canonical_bytes

    for mutate, match in (
        (lambda value: value.update({'extra': None}), 'unknown or missing'),
        (lambda value: value.pop('deployment_uid'), 'unknown or missing'),
        (lambda value: value.update({'manifest_sha256': '0' * 64}),
         'manifest hash'),
        (lambda value: value['manifest'].update({'cohort_id': 'c' * 254}),
         'DNS label'),
        (lambda value: value['manifest']['image'].update(
            {'oci_manifest_digest': 'SHA256:' + '1' * 64}), 'sha256:<64'),
        (lambda value: value['manifest'].update(
            {'handler_allowlist': ['serve_shadow_candidate_launch']}),
         'ordered v1 allowlist'),
    ):
        value = _cohort()
        mutate(value)
        if 'manifest_sha256' in value:
            value['manifest_sha256'] = (
                value['manifest_sha256'] if match == 'manifest hash' else
                actions.canonical_sha256(value['manifest']))
        with pytest.raises((TypeError, ValueError), match=match):
            actions.WorkerCohortIdentityV1.from_value(value)


@pytest.mark.parametrize('field,value,match', [
    ('qualified_oci_manifest_digest', 'sha256:' + 'f' * 64,
     'differs from its qualified OCI image'),
    ('qualified_oci_config_digest', 'sha256:' + 'f' * 64,
     'runtime image ID must equal'),
    ('qualification_artifact_sha256', 'f' * 64,
     'differs from its qualified OCI image'),
])
def test_worker_runtime_image_requires_all_qualification_links(
        field: str, value: str, match: str) -> None:
    image = _worker('pod-a')['image']
    image['runtime'][field] = value
    with pytest.raises(ValueError, match=match):
        actions.ProviderAuthorityWorkerImageV1.from_value(image)


@pytest.mark.parametrize('field,value', [
    ('pod_template_contract_sha256', 'f' * 64),
    ('artifact_inventory_sha256', 'f' * 64),
    ('callable_inventory_sha256', 'f' * 64),
    ('handler_allowlist_sha256', 'f' * 64),
])
def test_worker_identity_requires_all_cohort_manifest_links(
        field: str, value: str) -> None:
    cohort = actions.WorkerCohortIdentityV1.from_value(_cohort())
    worker = _worker('pod-a')
    worker[field] = value
    parsed = actions.ProviderAuthorityWorkerIdentityV1.from_value(worker)
    with pytest.raises(ValueError):
        parsed.validate_for_cohort(cohort)


def test_registration_set_relation_sorting_readiness_hash_and_freshness(
) -> None:
    cohort = actions.WorkerCohortIdentityV1.from_value(_cohort())
    registrations = actions.WorkerCohortRegistrationSetV1.from_value(
        _registration_set('pod-a', 'pod-b'))
    assert registrations.registrations == registrations.workers
    assert registrations.count == 2
    database_now = datetime.datetime(2026,
                                     8,
                                     1,
                                     1,
                                     5,
                                     tzinfo=datetime.timezone.utc)
    registrations.validate_for_cohort(cohort,
                                      require_two=True,
                                      database_now=database_now)
    assert actions.WorkerCohortRegistrationSetV1.from_value(
        registrations.canonical_value()).sha256 == registrations.sha256

    one = actions.WorkerCohortRegistrationSetV1.from_value(
        _registration_set('pod-a'))
    with pytest.raises(ValueError, match='requires two'):
        one.validate_for_cohort(cohort, require_two=True)
    with pytest.raises(ValueError, match='sorted by distinct'):
        actions.WorkerCohortRegistrationSetV1.from_value(
            _registration_set('pod-b', 'pod-a'))
    with pytest.raises(ValueError, match='sorted by distinct'):
        actions.WorkerCohortRegistrationSetV1.from_value(
            _registration_set('pod-a', 'pod-a'))

    wrong_hash = _registration_set('pod-a')
    wrong_hash['cohort_identity_sha256'] = '0' * 64
    parsed = actions.WorkerCohortRegistrationSetV1.from_value(wrong_hash)
    with pytest.raises(ValueError, match='cohort hash'):
        parsed.validate_for_cohort(cohort)
    wrong_worker = _registration_set('pod-a')
    wrong_worker['workers'][0]['worker']['deployment_uid'] = 'other'
    wrong_worker['workers'][0]['worker']['replica_set_controller_owner'][
        'uid'] = 'other'
    parsed = actions.WorkerCohortRegistrationSetV1.from_value(wrong_worker)
    with pytest.raises(ValueError, match='does not match its cohort'):
        parsed.validate_for_cohort(cohort)

    with pytest.raises(ValueError, match='database future'):
        registrations.validate_freshness(
            datetime.datetime(2026, 8, 1, 1, 0, tzinfo=datetime.timezone.utc))
    with pytest.raises(ValueError, match='older than five minutes'):
        registrations.validate_freshness(
            datetime.datetime(2026,
                              8,
                              1,
                              1,
                              7,
                              0,
                              1,
                              tzinfo=datetime.timezone.utc))
    with pytest.raises(TypeError, match='timezone-aware'):
        registrations.validate_freshness(datetime.datetime(2026, 8, 1, 1, 5))


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'extra': None}), 'unknown or missing'),
    (lambda value: value.pop('cohort_id'), 'unknown or missing'),
    (lambda value: value.pop('preparation_capability_sha256'),
     'unknown or missing'),
    (lambda value: value.update({'decision_id': 'not-a-uuid'}), 'UUID'),
    (lambda value: value.update(
        {'service_hash': 'AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA'}),
     'lowercase hyphenated'),
    (lambda value: value.update({'action_type': 'restart'}), 'unsupported'),
    (lambda value: value.update({'controller_owner_fence': 'x' * 1025}),
     '1..1024'),
    (lambda value: value.update({'preparation_capability_sha256': 'D' * 64}),
     'lowercase SHA-256'),
    (lambda value: value.update({'preparation_capability_sha256': 'd' * 63}),
     'lowercase SHA-256'),
    (lambda value: value.update({'preparation_capability_sha256': None}),
     'lowercase SHA-256'),
    (lambda value: value.update({
        'preparation_capability_sha256': type('HashSubclass', (str,), {})
                                         ('d' * 64)
    }), 'lowercase SHA-256'),
])
def test_cohort_reference_is_closed_and_bounded(mutate, match: str) -> None:
    value = _reference()
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.WorkerCohortReferenceInputV1.from_value(value)


def test_cohort_reference_requires_exact_coverage_identity() -> None:
    reference = actions.WorkerCohortReferenceInputV1.from_value(_reference())
    assert reference.canonical_value() == _reference()
    changed_capability = dataclasses.replace(reference,
                                             preparation_capability_sha256='e' *
                                             64)
    assert changed_capability.canonical_bytes != reference.canonical_bytes
    assert changed_capability.sha256 != reference.sha256
    coverage = actions.CoverageDecisionV1.from_value(_coverage())
    reference.validate_coverage(coverage)
    changed = _reference()
    changed['desired_generation'] += 1
    with pytest.raises(ValueError, match='does not match coverage'):
        actions.WorkerCohortReferenceInputV1.from_value(
            changed).validate_coverage(coverage)
    unlinked = actions.CoverageDecisionV1.from_value(
        _coverage(cohort_ref=False))
    with pytest.raises(ValueError, match='does not match coverage'):
        reference.validate_coverage(unlinked)


@pytest.mark.parametrize('field', ['desired_generation', 'lifecycle_epoch'])
def test_cohort_reference_rejects_postgres_bigint_overflow(field: str) -> None:
    reference = _reference()
    reference[field] = 2**63
    with pytest.raises(ValueError, match='no greater than'):
        actions.WorkerCohortReferenceInputV1.from_value(reference)


@pytest.mark.parametrize(
    'cohort_id',
    ['not/a-dns-label', 'A_UPPER', 'a' * 64, '-leading', 'trailing-'])
def test_cohort_reference_requires_dns_label_id(cohort_id: str) -> None:
    reference = _reference()
    reference['cohort_id'] = cohort_id
    with pytest.raises(ValueError, match='DNS label'):
        actions.WorkerCohortReferenceInputV1.from_value(reference)


@pytest.mark.parametrize('phase', [
    'PRE_SUBMIT', 'REQUEST_BOUND', 'COMPLETE', 'ABANDONED_PRE_SUBMIT',
    'REQUEST_ASSOCIATION_UNKNOWN'
])
def test_coverage_attempt_round_trips_every_closed_phase(phase: str) -> None:
    attempt = actions.CoverageAttemptV1.from_value(_attempt(phase))
    assert attempt.phase.value == phase
    assert actions.CoverageAttemptV1.from_value(
        attempt.canonical_value()) == attempt


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'extra': None}), 'unknown or missing'),
    (lambda value: value.pop('phase'), 'unknown or missing'),
    (lambda value: value.update({'decision_id': 'bad'}), 'UUID'),
    (lambda value: value.update({'request_sequence': 0}), 'positive'),
    (lambda value: value.update({'phase': 'WAITING'}), 'unsupported'),
    (lambda value: value.update({'request_role': 'PRIMARY_RESTART'}),
     'unsupported'),
    (lambda value: value.update({'legacy_request_id': 'r' * 129}), '1..128'),
    (lambda value: value.update({'terminal_request_status': 'TIMED_OUT'}),
     'unsupported'),
    (lambda value: value.update({'retry_disposition': 'OBSERVE'}),
     'unsupported'),
])
def test_coverage_attempt_rejects_unknown_missing_uuid_enum_and_bounds(
        mutate, match: str) -> None:
    value = _attempt('COMPLETE')
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.CoverageAttemptV1.from_value(value)


@pytest.mark.parametrize('field', ['request_sequence', 'logical_attempt'])
def test_coverage_attempt_rejects_postgres_integer_overflow(field: str) -> None:
    attempt = _attempt('COMPLETE')
    attempt[field] = 2**31
    with pytest.raises(ValueError, match='no greater than'):
        actions.CoverageAttemptV1.from_value(attempt)


@pytest.mark.parametrize('phase,field,value', [
    ('PRE_SUBMIT', 'legacy_request_id', 'request-1'),
    ('REQUEST_BOUND', 'completed_at', '2026-08-01T01:03:02.000000Z'),
    ('COMPLETE', 'retry_disposition', None),
    ('ABANDONED_PRE_SUBMIT', 'request_bound_at', '2026-08-01T01:03:01.000000Z'),
    ('REQUEST_ASSOCIATION_UNKNOWN', 'terminal_request_status', 'FAILED'),
])
def test_coverage_attempt_rejects_wrong_phase_shape(phase: str, field: str,
                                                    value) -> None:
    attempt = _attempt(phase)
    attempt[field] = value
    with pytest.raises(ValueError):
        actions.CoverageAttemptV1.from_value(attempt)


def test_lifecycle_invocation_refinement_preserves_canonical_bytes() -> None:
    launch = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    refined_launch = launch.as_launch()
    assert isinstance(refined_launch,
                      actions.ProviderLaunchLifecycleInvocationV1)
    assert refined_launch.require_launch() is refined_launch.launch
    assert refined_launch.canonical_bytes == launch.canonical_bytes
    assert refined_launch.sha256 == launch.sha256
    assert refined_launch.action_id == launch.action_id
    assert launch.refined().canonical_bytes == launch.canonical_bytes
    with pytest.raises(ValueError, match='not down'):
        launch.require_down()
    with pytest.raises(ValueError, match='not down'):
        launch.as_down()

    down = actions.ProviderLifecycleInvocationV1.from_value(_down_invocation())
    refined_down = down.as_down()
    assert isinstance(refined_down, actions.ProviderDownLifecycleInvocationV1)
    assert refined_down.require_down() is refined_down.down
    assert refined_down.canonical_bytes == down.canonical_bytes
    assert refined_down.sha256 == down.sha256
    with pytest.raises(ValueError, match='not launch'):
        down.require_launch()
    with pytest.raises(ValueError, match='not launch'):
        down.as_launch()


def test_refined_invocation_constructors_reject_wrong_kind() -> None:
    with pytest.raises(ValueError, match='not launch'):
        actions.ProviderLaunchLifecycleInvocationV1.from_value(
            _down_invocation())
    with pytest.raises(ValueError, match='not down'):
        actions.ProviderDownLifecycleInvocationV1.from_value(
            _launch_invocation())
