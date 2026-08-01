"""Pure contract tests for durable SkyServe resource actions."""

import copy
import dataclasses
import uuid

import pytest

from sky.serve import resource_actions as actions

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'


def _identity(generation: int = 1) -> dict:
    return {
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': generation,
    }


def _target() -> dict:
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'cloud': 'kubernetes',
        'region': None,
        'zone': None,
        'sky_cluster_name': 'svc-7',
        'sky_cluster_record_uuid': _CLUSTER_UUID,
        'kubernetes': {
            'cluster_fingerprint_sha256': 'a' * 64,
            'namespace': 'serve-prod',
            'workload_kind': 'Pod',
            'workload_name': 'svc-7',
            'cluster_record_uuid_label': _CLUSTER_UUID,
            'replica_incarnation_label': _REPLICA_UUID,
        },
    }


def _resources() -> dict:
    return {
        'version': 1,
        'cloud': 'kubernetes',
        'cluster_fingerprint_sha256': 'a' * 64,
        'namespace': 'serve-prod',
        'instance_type': None,
        'accelerator': {
            'name': 'H100',
            'count': 1,
        },
        'cpus': '8+',
        'memory': '32+',
        'image_id': 'docker:example/image@sha256:' + '9' * 64,
        'disk_size_gb': 100,
        'disk_tier': None,
        'ports': ['8000', '8001'],
        'labels': [{
            'key': 'app',
            'value': 'serve'
        }, {
            'key': 'replica',
            'value': '7'
        }],
        'use_spot': False,
    }


def _launch_invocation() -> dict:
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'launch',
        'resource_identity': _identity(),
        'requested_target': _target(),
        'launch': {
            'source': {
                'store': 'serve_version_specs',
                'service_name': 'svc',
                'service_incarnation': _SERVICE_UUID,
                'service_version': 3,
                'yaml_content_sha256': 'b' * 64,
                'workspace': 'boltz-test',
            },
            'resources': _resources(),
            'replica_env': {
                'SKYPILOT_SERVE_REPLICA_ID': '7'
            },
            'security_group_scope': 'serve-svc',
            'admin_policy_input_sha256': 'c' * 64,
            'admin_policy_output_sha256': 'd' * 64,
            'retry_until_up': True,
            'exact_resources_override': True,
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
            'dryrun': False,
            'no_setup': False,
            'clone_disk_from': None,
            'fast': False,
            'file_mounts_blob_id': None,
            'tls_material_ref': None,
        },
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
            'workspace': 'boltz-test',
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        },
    }


def _launch_plan() -> dict:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'action_kind': 'launch',
        'resource_identity': _identity(),
        'placement_decision_sha256': 'e' * 64,
        'resources_snapshot_sha256': invocation.launch.resources.sha256,
        'workspace_identity_sha256': 'f' * 64,
        'requested_target': _target(),
        'prior_resolved_target': None,
        'request_payload_sha256': invocation.sha256,
        'redaction_profile': 'provider_lifecycle_redaction_v1',
    }


def _resolved_target() -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    return {
        'version': 1,
        'requested_target_sha256': target.sha256,
        'provider_resource_id': 'pod/svc-7',
        'workload_uid': 'uid-7',
        'provider_operation_id': None,
        'resolved_at': '2026-08-01T01:02:03.000004Z',
    }


def _observation() -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    return {
        'version': 1,
        'target_sha256': target.sha256,
        'state': 'present',
        'certainty': 'authoritative',
        'observed_provider_operation_id': None,
        'observed_provider_resource_id': 'pod/svc-7',
        'observed_cluster_record_uuid': _CLUSTER_UUID,
        'observed_workload_uid': 'uid-7',
        'observed_replica_incarnation_label': _REPLICA_UUID,
        'resolved_target': _resolved_target(),
        'ready': True,
        'evidence_sha256': '4' * 64,
        'observed_at': '2026-08-01T01:02:04.000005Z',
    }


def test_launch_invocation_literal_golden_bytes_hash_and_action_id() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.canonical_bytes == (
        b'{"action_kind":"launch","down":null,"launch":{'
        b'"admin_policy_input_sha256":"cccccccccccccccccccccccccccccccccccc'
        b'cccccccccccccccccccccccccccc","admin_policy_output_sha256":"dddddddd'
        b'dddddddddddddddddddddddddddddddddddddddddddddddddddddddd",'
        b'"backend":"cloud_vm_ray","clone_disk_from":null,"dryrun":false,'
        b'"exact_resources_override":true,"fast":false,"file_mounts_blob_id":'
        b'null,"no_setup":false,"optimize_target":"cost","replica_env":{'
        b'"SKYPILOT_SERVE_REPLICA_ID":"7"},"resources":{"accelerator":{'
        b'"count":1,"name":"H100"},"cloud":"kubernetes",'
        b'"cluster_fingerprint_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaa","cpus":"8+","disk_size_gb":100,'
        b'"disk_tier":null,"image_id":"docker:example/image@sha256:'
        b'9999999999999999999999999999999999999999999999999999999999999999",'
        b'"instance_type":null,"labels":[{"key":"app","value":"serve"},{'
        b'"key":"replica","value":"7"}],"memory":"32+","namespace":'
        b'"serve-prod","ports":["8000","8001"],"use_spot":false,'
        b'"version":1},"retry_until_up":true,"security_group_scope":'
        b'"serve-svc","source":{"service_incarnation":"11111111-1111-4111-'
        b'8111-111111111111","service_name":"svc","service_version":3,'
        b'"store":"serve_version_specs","workspace":"boltz-test",'
        b'"yaml_content_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        b'bbbbbbbbbbbbbbbbbbbbbbbb"},"tls_material_ref":null},"profile":'
        b'"pod_cluster_v1","redaction_profile":'
        b'"provider_lifecycle_redaction_v1","requested_target":{"cloud":'
        b'"kubernetes","kubernetes":{"cluster_fingerprint_sha256":'
        b'"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"cluster_record_uuid_label":"33333333-3333-4333-8333-333333333333",'
        b'"namespace":"serve-prod","replica_incarnation_label":'
        b'"22222222-2222-4222-8222-222222222222","workload_kind":"Pod",'
        b'"workload_name":"svc-7"},"profile":"pod_cluster_v1","region":null,'
        b'"sky_cluster_name":"svc-7","sky_cluster_record_uuid":'
        b'"33333333-3333-4333-8333-333333333333","version":1,"zone":null},'
        b'"resource_identity":{"desired_generation":1,"replica_id":7,'
        b'"replica_incarnation":"22222222-2222-4222-8222-222222222222",'
        b'"service_hash":"11111111-1111-4111-8111-111111111111",'
        b'"service_incarnation":"11111111-1111-4111-8111-111111111111"},'
        b'"version":1}')
    assert invocation.sha256 == (
        '43ef241087744463c109d23df2d89eec653862a614fa5b2b4f159ad72b8dbe3c')
    assert invocation.action_id == uuid.UUID(
        'a1fa64dd-eea2-59db-b7b6-733d8001a086')
    assert invocation.launch is not None
    assert invocation.launch.resources.sha256 == (
        '3ef165259e42862cffe0b9ca275e2711e76bd20bb05f8cf846aed0cfb9eea4c6')


def test_down_invocation_literal_golden_bytes_hash_and_action_id() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _down_invocation())
    assert invocation.canonical_bytes == (
        b'{"action_kind":"down","down":{"cluster_name":"svc-7",'
        b'"expected_cluster_record_uuid":"33333333-3333-4333-8333-'
        b'333333333333","graceful":false,"graceful_timeout":null,"purge":'
        b'false,"workspace":"boltz-test"},"launch":null,"profile":'
        b'"pod_cluster_v1","redaction_profile":'
        b'"provider_lifecycle_redaction_v1","requested_target":{"cloud":'
        b'"kubernetes","kubernetes":{"cluster_fingerprint_sha256":'
        b'"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"cluster_record_uuid_label":"33333333-3333-4333-8333-333333333333",'
        b'"namespace":"serve-prod","replica_incarnation_label":'
        b'"22222222-2222-4222-8222-222222222222","workload_kind":"Pod",'
        b'"workload_name":"svc-7"},"profile":"pod_cluster_v1","region":null,'
        b'"sky_cluster_name":"svc-7","sky_cluster_record_uuid":'
        b'"33333333-3333-4333-8333-333333333333","version":1,"zone":null},'
        b'"resource_identity":{"desired_generation":2,"replica_id":7,'
        b'"replica_incarnation":"22222222-2222-4222-8222-222222222222",'
        b'"service_hash":"11111111-1111-4111-8111-111111111111",'
        b'"service_incarnation":"11111111-1111-4111-8111-111111111111"},'
        b'"version":1}')
    assert invocation.sha256 == (
        '53b16b443ae4caaffd8ab9539d3017dd9853f411db4bad29faa6d0c6b75b351d')
    assert invocation.action_id == uuid.UUID(
        '324a4cdd-4640-57ae-aea8-b3f65851f735')


def test_provider_plan_commits_invocation_resource_and_target() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    value = _launch_plan()
    plan = actions.ProviderLifecyclePlanV1.from_value(value)
    plan.validate_invocation(invocation)
    assert plan.action_id == invocation.action_id
    assert len(plan.canonical_bytes) < 65_536

    forged = copy.deepcopy(value)
    forged['request_payload_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='payload hash'):
        actions.ProviderLifecyclePlanV1.from_value(forged).validate_invocation(
            invocation)


def test_locator_and_plan_literal_golden_bytes_and_hashes() -> None:
    locator = actions.ProviderLocatorV1.from_value(_target())
    assert locator.canonical_bytes == (
        b'{"cloud":"kubernetes","kubernetes":{"cluster_fingerprint_sha256":"aa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","clu'
        b'ster_record_uuid_label":"33333333-3333-4333-8333-333333333333","name'
        b'space":"serve-prod","replica_incarnation_label":"22222222-2222-4222-'
        b'8222-222222222222","workload_kind":"Pod","workload_name":"svc-7"},"p'
        b'rofile":"pod_cluster_v1","region":null,"sky_cluster_name":"svc-7","s'
        b'ky_cluster_record_uuid":"33333333-3333-4333-8333-333333333333","vers'
        b'ion":1,"zone":null}')
    assert locator.sha256 == (
        'e2c114a700e6c83517fbb39bf037b887cdde766cf25cefa91f0e5d3b273daa2e')

    plan = actions.ProviderLifecyclePlanV1.from_value(_launch_plan())
    assert plan.canonical_bytes == (
        b'{"action_kind":"launch","placement_decision_sha256":"eeeeeeeeeeeeeee'
        b'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","prior_resolved_t'
        b'arget":null,"profile":"pod_cluster_v1","redaction_profile":"provider'
        b'_lifecycle_redaction_v1","request_payload_sha256":"43ef241087744463c'
        b'109d23df2d89eec653862a614fa5b2b4f159ad72b8dbe3c","requested_target":'
        b'{"cloud":"kubernetes","kubernetes":{"cluster_fingerprint_sha256":"aa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","clu'
        b'ster_record_uuid_label":"33333333-3333-4333-8333-333333333333","name'
        b'space":"serve-prod","replica_incarnation_label":"22222222-2222-4222-'
        b'8222-222222222222","workload_kind":"Pod","workload_name":"svc-7"},"p'
        b'rofile":"pod_cluster_v1","region":null,"sky_cluster_name":"svc-7","s'
        b'ky_cluster_record_uuid":"33333333-3333-4333-8333-333333333333","vers'
        b'ion":1,"zone":null},"resource_identity":{"desired_generation":1,"rep'
        b'lica_id":7,"replica_incarnation":"22222222-2222-4222-8222-2222222222'
        b'22","service_hash":"11111111-1111-4111-8111-111111111111","service_i'
        b'ncarnation":"11111111-1111-4111-8111-111111111111"},"resources_snaps'
        b'hot_sha256":"3ef165259e42862cffe0b9ca275e2711e76bd20bb05f8cf846aed0c'
        b'fb9eea4c6","version":1,"workspace_identity_sha256":"ffffffffffffffff'
        b'ffffffffffffffffffffffffffffffffffffffffffffffff"}')
    assert plan.sha256 == (
        '030916d7936d8d18f6790417afba8b4f13c2687d82e540a5fb29b21ac0f6186f')


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'credentials': 'secret'}),
     'unknown or missing'),
    (lambda value: value['launch'].update(
        {'private_key': '-----BEGIN PRIVATE KEY-----'}), 'unknown or missing'),
    (lambda value: value['launch']['replica_env'].update(
        {'API_TOKEN': 'secret'}), 'unknown or missing'),
    (lambda value: value['launch']['resources'].update({'kubeconfig': 'secret'}
                                                      ), 'unknown or missing'),
    (lambda value: value.update({'down': _down_invocation()['down']}),
     'requires only launch'),
    (lambda value: value['resource_identity'].update(
        {'service_incarnation': _CLUSTER_UUID}), 'service_hash'),
    (lambda value: value['requested_target']['kubernetes'].update(
        {'replica_incarnation_label': _CLUSTER_UUID}), 'replica label'),
    (lambda value: value['requested_target'].update(
        {'sky_cluster_record_uuid': _CLUSTER_UUID.replace('-', '')}),
     'lowercase hyphenated'),
    (lambda value: value['launch']['resources'].update({'disk_size_gb': 100.0}),
     'forbids floats'),
    (lambda value: value['launch']['resources'].update(
        {'ports': ['8001', '8000']}), 'sorted and unique'),
    (lambda value: value['launch']['resources'].update({
        'labels': [{
            'key': 'replica',
            'value': '7'
        }, {
            'key': 'app',
            'value': 'serve'
        }]
    }), 'sorted by unique key'),
    (lambda value: value['launch']['source'].update(
        {'service_name': 'x' * 1025}), '1..1024'),
    (lambda value: value['launch']['replica_env'].update(
        {'SKYPILOT_SERVE_REPLICA_ID': '1' * 1025}), '1..1024'),
    (lambda value: value['launch']['source'].update(
        {'service_name': 'e\u0301'}), 'not canonical|NFC-normalized'),
    (lambda value: value['requested_target'].update({'version': 2}),
     'integer 1'),
])
def test_invocation_rejects_closed_shape_identity_bounds_and_secrets(
        mutate, match: str) -> None:
    value = _launch_invocation()
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.ProviderLifecycleInvocationV1.from_value(value)


def test_redaction_profile_marks_opaque_material_outside_first_cohort() -> None:
    value = _launch_invocation()
    value['launch']['tls_material_ref'] = 'secret/tls-material-v1'
    invocation = actions.ProviderLifecycleInvocationV1.from_value(value)
    assert invocation.launch is not None
    assert not invocation.launch.first_authority_cohort_redacted


def test_provider_evidence_contracts_are_closed_and_cross_checked() -> None:
    error = actions.ProviderErrorV1.from_value({
        'category': 'quota',
        'provider_code': 'TooManyPods',
        'retry_after_seconds': 3,
        'normalized_message': 'quota unavailable',
    })
    submission = actions.ProviderSubmissionV1.from_value({
        'disposition': 'ambiguous',
        'provider_operation_id': None,
        'normalized_response_sha256': None,
        'normalized_error': error.canonical_value(),
    })
    assert submission.normalized_error == error

    target = actions.ProviderLocatorV1.from_value(_target())
    resolved = actions.ResolvedProviderTargetV1.from_value(_resolved_target())
    resolved.validate_requested_target(target)
    observation = actions.ProviderLifecycleObservationV1.from_value(
        _observation())
    observation.validate_target(target)
    assert observation.resolved_target == resolved

    bad = _observation()
    bad['target_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='resolved target hash'):
        actions.ProviderLifecycleObservationV1.from_value(bad)
    bad = _observation()
    bad['observed_at'] = '2026-08-01T01:02:04Z'
    with pytest.raises(ValueError, match='six fractional'):
        actions.ProviderLifecycleObservationV1.from_value(bad)
    with pytest.raises(ValueError, match='submission evidence'):
        actions.ProviderSubmissionV1.from_value({
            'disposition': 'not_submitted',
            'provider_operation_id': 'operation-1',
            'normalized_response_sha256': None,
            'normalized_error': None,
        })


def test_authoritative_present_observation_requires_exact_identity() -> None:
    target = actions.ProviderLocatorV1.from_value(_target())
    missing = _observation()
    for field in ('observed_cluster_record_uuid',
                  'observed_replica_incarnation_label', 'observed_workload_uid',
                  'resolved_target'):
        missing[field] = None
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(missing)

    mismatched_cluster = _observation()
    mismatched_cluster['observed_cluster_record_uuid'] = _SERVICE_UUID
    with pytest.raises(ValueError, match='frozen target identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            mismatched_cluster).validate_target(target)

    mismatched_label = _observation()
    mismatched_label['observed_replica_incarnation_label'] = _SERVICE_UUID
    with pytest.raises(ValueError, match='frozen target identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            mismatched_label).validate_target(target)

    mismatched_native = _observation()
    mismatched_native['observed_workload_uid'] = 'replacement-uid'
    with pytest.raises(ValueError, match='resolved identity conflicts'):
        actions.ProviderLifecycleObservationV1.from_value(mismatched_native)

    missing_resolved_native = _observation()
    missing_resolved_native['resolved_target']['provider_resource_id'] = None
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            missing_resolved_native)

    missing_resolved_operation = _observation()
    missing_resolved_operation['observed_provider_operation_id'] = 'op-new'
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            missing_resolved_operation)

    conflict = mismatched_native
    conflict['state'] = 'conflict'
    conflict['certainty'] = 'authoritative'
    conflict['ready'] = None
    actions.ProviderLifecycleObservationV1.from_value(conflict).validate_target(
        target)


def test_direct_construction_rejects_float_versions_and_is_immutable() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    objects = [
        invocation.requested_target,
        actions.ResolvedProviderTargetV1.from_value(_resolved_target()),
        invocation.launch.resources,
        invocation,
        actions.ProviderLifecyclePlanV1.from_value(_launch_plan()),
        actions.ProviderLifecycleObservationV1.from_value(_observation()),
        actions.ServeShadowProjectionV1.from_value({
            'version': 1,
            'action_kind': 'launch',
            'row_disposition': 'retained',
            'replica_status': 'READY',
            'capacity_outcome': 'success',
            'action_disposition': 'succeeded',
            'resolved_target': _resolved_target(),
        }),
        actions.ServeShadowRetryDecisionV1.from_value({
            'version': 1,
            'decision': 'observe',
            'retry_class': 'observation_required',
            'delay_seconds': 5,
            'logical_attempt': 2,
        }),
    ]
    for value in objects:
        with pytest.raises(ValueError, match='integer 1'):
            dataclasses.replace(value, version=1.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        invocation.profile = actions.ProviderProfile.POD_CLUSTER_V1


def test_collection_and_complete_object_bounds() -> None:
    too_many_ports = _launch_invocation()
    too_many_ports['launch']['resources']['ports'] = [
        f'{port:03d}' for port in range(257)
    ]
    with pytest.raises(ValueError, match='at most 256'):
        actions.ProviderLifecycleInvocationV1.from_value(too_many_ports)

    oversized = _launch_invocation()
    oversized['launch']['resources']['labels'] = [{
        'key': f'{index:03d}' + 'k' * 250,
        'value': 'v' * 253,
    } for index in range(256)]
    with pytest.raises(ValueError, match='exceeds 65536'):
        actions.ProviderLifecycleInvocationV1.from_value(oversized)


def test_outcome_projection_and_retry_contract_combinations() -> None:
    outcome = actions.ServeReplicaActionOutcomeV1.from_value({
        'disposition': 'succeeded',
        'certainty': 'observed',
        'provider_operation_id': None,
        'provider_code': None,
        'retry_class': None,
        'retry_after_seconds': None,
        'observation': _observation(),
        'normalized_message': None,
    })
    projection = actions.ServeShadowProjectionV1.from_value({
        'version': 1,
        'action_kind': 'launch',
        'row_disposition': 'retained',
        'replica_status': 'READY',
        'capacity_outcome': 'success',
        'action_disposition': 'succeeded',
        'resolved_target': _resolved_target(),
    })
    retry = actions.ServeShadowRetryDecisionV1.from_value({
        'version': 1,
        'decision': 'observe',
        'retry_class': 'observation_required',
        'delay_seconds': 5,
        'logical_attempt': 2,
    })
    assert outcome.disposition is actions.ServeActionDisposition.SUCCEEDED
    assert projection.replica_status is actions.ReplicaStatusValue.READY
    assert retry.decision is actions.ShadowRetryDecision.OBSERVE

    bad_outcome = outcome.canonical_value()
    bad_outcome['retry_class'] = 'transient'
    with pytest.raises(ValueError, match='terminal outcome'):
        actions.ServeReplicaActionOutcomeV1.from_value(bad_outcome)
    with pytest.raises(ValueError, match='capacity_outcome'):
        actions.ServeShadowProjectionV1.from_value({
            **projection.canonical_value(),
            'action_kind': 'down',
        })
    with pytest.raises(ValueError, match='cannot contain retry'):
        actions.ServeShadowRetryDecisionV1.from_value({
            'version': 1,
            'decision': 'terminal',
            'retry_class': 'transient',
            'delay_seconds': 1,
            'logical_attempt': 1,
        })


def test_shadow_state_role_eligibility_parity_and_divergence_vocabularies(
) -> None:
    assert {value.value for value in actions.ShadowParentPhase} == {
        'PENDING', 'RUNNING', 'COMPLETE', 'ABANDONED_PRE_SUBMIT', 'AMBIGUOUS'
    }
    assert {value.value for value in actions.ShadowAttemptPhase} == {
        'PRE_SUBMIT', 'REQUEST_BOUND', 'COMPLETE', 'ABANDONED_PRE_SUBMIT',
        'REQUEST_ASSOCIATION_UNKNOWN'
    }
    assert {value.value for value in actions.ShadowRequestRole
           } == {'PRIMARY_LAUNCH', 'PRIMARY_DOWN', 'LAUNCH_CLEANUP_DOWN'}
    assert {value.value for value in actions.ProfileEligibility
           } == {'ELIGIBLE', 'UNSUPPORTED'}
    assert actions.ShadowDivergenceClass.IDENTITY_MISMATCH.parity_class is (
        actions.ShadowParityClass.IDENTITY_MISMATCH)
    assert actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN.value == (
        'legacy_direct_down')
