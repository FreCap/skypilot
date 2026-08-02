"""Focused pure tests for the Serve-owned API006 provider cursor."""

# pylint: disable=protected-access

import copy
import dataclasses
import datetime
import uuid

import pytest
import test_serve_resource_action_down_execution_config
import test_serve_resource_action_execution_foundation
import test_serve_resource_action_skylet_policy

from sky.serve import resource_action_progress as progress
from sky.serve import resource_actions as values
from sky.server.requests import requests as requests_lib
from sky.server.requests import resource_actions as kernel

down_config_fixtures = test_serve_resource_action_down_execution_config
foundation = test_serve_resource_action_execution_foundation
skylet_fixtures = test_serve_resource_action_skylet_policy

_ACTION_ID = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_STATE_STORE_UUID = '44444444-4444-4444-8444-444444444444'
_TIME = '2026-08-01T01:02:03.000004Z'


def _artifact(path: str, marker: str) -> dict:
    return {
        'repo_path': path,
        'byte_size': 17,
        'sha256': marker * 64,
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
        'deployment_name': 'authority-v1',
        'service_account_name': 'authority-v1',
        'container_name': 'skypilot-authority-worker',
        'image': _qualification(),
        'pod_template_contract': _artifact('charts/worker.yaml', '4'),
        'artifact_inventory': _artifact('inventories/artifacts.json', '5'),
        'callable_inventory': _artifact('inventories/callables.json', '6'),
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            values.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1),
    }
    return {
        'version': 1,
        'manifest': manifest,
        'manifest_sha256': values.canonical_sha256(manifest),
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'service-account-uid-v1',
    }


def _worker(pod_uid: str = 'worker-pod-uid', observed_at: str = _TIME) -> dict:
    qualification = _qualification()
    return {
        'namespace': 'skypilot-system',
        'pod_name': 'worker-0',
        'pod_uid': pod_uid,
        'pod_resource_version': '101',
        'pod_service_account_name': 'authority-v1',
        'pod_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'ReplicaSet',
            'name': 'authority-v1-abc',
            'uid': 'replicaset-uid-v1',
        },
        'replica_set_name': 'authority-v1-abc',
        'replica_set_uid': 'replicaset-uid-v1',
        'replica_set_resource_version': '102',
        'replica_set_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'Deployment',
            'name': 'authority-v1',
            'uid': 'deployment-uid-v1',
        },
        'deployment_name': 'authority-v1',
        'deployment_uid': 'deployment-uid-v1',
        'deployment_resource_version': '103',
        'deployment_generation': 5,
        'deployment_observed_generation': 5,
        'pod_template_contract_sha256': '4' * 64,
        'image': {
            'qualification': qualification,
            'runtime': {
                'raw_image_id': 'containerd://sha256:' + '2' * 64,
                'runtime_image_id_scheme': 'containerd',
                'runtime_image_id_digest': 'sha256:' + '2' * 64,
                'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
                'qualified_oci_config_digest': 'sha256:' + '2' * 64,
                'qualification_artifact_sha256':
                    qualification['qualification_artifact']['sha256'],
                'runtime_id_contract': 'qualified_oci_config_digest_v1',
            },
        },
        'service_account_uid': 'service-account-uid-v1',
        'artifact_inventory_sha256': '5' * 64,
        'callable_inventory_sha256': '6' * 64,
        'handler_allowlist_sha256': values.canonical_sha256(
            _cohort()['manifest']['handler_allowlist']),
        'observed_at': observed_at,
    }


def _attestation(
    action_id: uuid.UUID = _ACTION_ID,
    attempt: int = 1,
    generation: int = 1,
    worker_id: uuid.UUID = uuid.UUID('55555555-5555-4555-8555-555555555555'),
    claimed_cursor_sha256: str | None = None,
    after: bool = False,
) -> dict:
    before = _worker()
    after_value = None
    if after:
        after_value = _worker(observed_at='2026-08-01T01:02:04.000005Z')
    return {
        'request_id': kernel.request_id_for_attempt(action_id, attempt),
        'request_execution_generation': generation,
        'request_worker_id': str(worker_id),
        'claimed_cursor_sha256': claimed_cursor_sha256,
        'before': before,
        'after': after_value,
    }


def _claim(
    action_id: uuid.UUID = _ACTION_ID,
    attempt: int = 1,
    generation: int = 1,
    worker_id: uuid.UUID = uuid.UUID('55555555-5555-4555-8555-555555555555'),
    claimed_cursor_sha256: str | None = None,
) -> dict:
    attestation = _attestation(action_id, attempt, generation, worker_id,
                               claimed_cursor_sha256)
    return {
        'version': 1,
        'launch_attempt': attempt,
        'request_id': kernel.request_id_for_attempt(action_id, attempt),
        'request_execution_generation': generation,
        'worker_attestation': attestation,
        'worker_attestation_sha256': values.canonical_sha256(attestation),
    }


def _claim_from_attestation(attestation: dict, attempt: int = 1) -> dict:
    return {
        'version': 1,
        'launch_attempt': attempt,
        'request_id': attestation['request_id'],
        'request_execution_generation':
            attestation['request_execution_generation'],
        'worker_attestation': attestation,
        'worker_attestation_sha256': values.canonical_sha256(attestation),
    }


def _foreign_attestation(attestation: dict) -> dict:
    crossed = copy.deepcopy(attestation)
    crossed['before']['pod_template_contract_sha256'] = '7' * 64
    if crossed['after'] is not None:
        crossed['after']['pod_template_contract_sha256'] = '7' * 64
    return crossed


def _target() -> dict:
    scope = _scope()
    scope_sha256 = values.ProviderKubernetesScopeV1.from_value(scope).sha256
    basis = {
        'version': 1,
        'display_name': 'svc-7',
        'frozen_user_hash': 'user-hash',
        'max_length': 42,
        'cluster_name_hash_length': 8,
    }
    topology = foundation._topology()
    workload_name = 'svc-7-user-hash-head'
    names = (f'{workload_name}-ssh', workload_name, workload_name)
    for item, name in zip(topology['mutable_objects'], names):
        item['name'] = name
        labels = {label['key']: label['value'] for label in item['labels']}
        labels['skypilot-cluster-name'] = 'svc-7-user-hash'
        labels['skypilot.co/cluster-record-uuid'] = _CLUSTER_UUID
        labels['skypilot.co/serve-replica-incarnation'] = _REPLICA_UUID
        item['labels'] = [{
            'key': key,
            'value': value,
        } for key, value in sorted(labels.items())]
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'cloud': 'kubernetes',
        'region': None,
        'zone': None,
        'sky_cluster_name': 'svc-7',
        'sky_cluster_record_uuid': _CLUSTER_UUID,
        'kubernetes': {
            'scope': scope,
            'cluster_fingerprint_sha256': scope_sha256,
            'namespace': 'serve-canary',
            'name_basis': basis,
            'provider_cluster_name': 'svc-7-user-hash',
            'workload_kind': 'Pod',
            'workload_name': workload_name,
            'cluster_record_uuid_label': _CLUSTER_UUID,
            'replica_incarnation_label': _REPLICA_UUID,
            'topology': topology,
        },
    }


def _target_sha256() -> str:
    return values.ProviderLocatorV1.from_value(_target()).sha256


def _scope() -> dict:
    return {
        'version': 1,
        'context_name': 'kubernetes',
        'context_identity': ['skypilot-in-cluster-identity-kubernetes'],
        'in_cluster': True,
        'namespace': 'serve-canary',
        'transport': {
            'version': 1,
            'server_origin': {
                'scheme': 'https',
                'host': '10.0.0.1',
                'port': 443,
                'path': '/',
            },
            'tls_server_name': 'kubernetes.default.svc',
            'ca_cert_der_base64': ['MAMCAQE='],
        },
        'kube_system_namespace_uid': 'uid-kube-system',
        'target_namespace_uid': 'uid-serve-canary',
        'api_server_git_version': 'v1.33.1',
        'caller_service_account_namespace': 'skypilot-system',
        'caller_service_account_name': 'authority-worker',
        'caller_service_account_uid': 'uid-authority-worker',
        'workload_service_account_namespace': 'serve-canary',
        'workload_service_account_name': 'serve-workload',
        'workload_service_account_uid': 'uid-serve-workload',
    }


def _scope_read() -> dict:
    return {
        'disposition': 'complete',
        'scope': _scope(),
        'observed_at': _TIME,
    }


_ROLE_VALUES = (
    ('head_ssh_service', 'Service', 'svc-7-user-hash-head-ssh'),
    ('head_service', 'Service', 'svc-7-user-hash-head'),
    ('head_pod', 'Pod', 'svc-7-user-hash-head'),
)


def _semantic(sequence: int) -> dict:
    role, kind, _ = _ROLE_VALUES[sequence]
    return {'kind': kind, 'role': role, 'sequence': sequence}


def _semantic_sha256(sequence: int) -> str:
    return values.canonical_sha256(_semantic(sequence))


def _object_observation(sequence: int,
                        disposition: str,
                        *,
                        pod_node: bool = True) -> dict:
    role, kind, name = _ROLE_VALUES[sequence]
    value = {
        'role': role,
        'api_version': 'v1',
        'kind': kind,
        'namespace': 'serve-canary',
        'name': name,
        'query_mode': 'exact_name_get_then_validate_labels',
        'read_disposition': disposition,
        'uid': None,
        'cluster_name_label': None,
        'cluster_record_uuid_label': None,
        'replica_incarnation_label': None,
        'requested_semantic_sha256': _semantic_sha256(sequence),
        'normalized_observed_semantic': None,
        'observed_semantic_sha256': None,
        'spec_match': None,
        'server_allocations': [],
        'deletion_timestamp': None,
        'pod_phase': None,
        'ready': None,
    }
    if disposition == 'present':
        resolved = _resolved_object(
            sequence, with_node=pod_node if sequence == 2 else True)
        value.update({
            'uid': resolved['uid'],
            'cluster_name_label': 'svc-7-user-hash',
            'cluster_record_uuid_label': _CLUSTER_UUID,
            'replica_incarnation_label': _REPLICA_UUID,
            'normalized_observed_semantic': _semantic(sequence),
            'observed_semantic_sha256': _semantic_sha256(sequence),
            'spec_match': True,
            'server_allocations': resolved['server_allocations'],
            'pod_phase': 'Running' if sequence == 2 else None,
            'ready': True if sequence == 2 else None,
        })
    return value


def _observation(
    *,
    absent: bool = False,
    dispositions: tuple[str, str, str] | None = None,
    pod_node: bool = True,
    state: str | None = None,
) -> dict:
    if dispositions is None:
        disposition = 'not_found' if absent else 'uncertain'
        dispositions = (disposition,) * 3
    evidence = {
        'version': 1,
        'source': 'core_v1_exact_get_same_live_client',
        'frozen_scope': _scope(),
        'observed_scope_before': _scope_read(),
        'observed_scope_after': _scope_read(),
        'objects': [
            _object_observation(sequence, disposition, pod_node=pod_node)
            for sequence, disposition in enumerate(dispositions)
        ],
    }
    all_present = dispositions == ('present',) * 3
    all_absent = dispositions == ('not_found',) * 3
    if state is None:
        state = ('present'
                 if all_present else 'absent' if all_absent else 'uncertain')
    resolved = (_resolved_target(
        with_node=pod_node) if all_present and state == 'present' else None)
    return {
        'version': 1,
        'target_sha256': _target_sha256(),
        'state': state,
        'certainty':
            ('authoritative' if 'uncertain' not in dispositions else 'unknown'),
        'observed_provider_operation_id': None,
        'observed_provider_resource_id': None if resolved is None else
                                         resolved['provider_resource_id'],
        'observed_cluster_record_uuid': _CLUSTER_UUID
                                        if resolved is not None else None,
        'observed_workload_uid': None if resolved is None else
                                 resolved['workload_uid'],
        'observed_replica_incarnation_label': _REPLICA_UUID
                                              if resolved is not None else None,
        'resolved_target': resolved,
        'ready': True if resolved is not None else None,
        'evidence': evidence,
        'evidence_sha256': values.canonical_sha256(evidence),
        'observed_at': _TIME,
    }


def _prefix_observation(count: int, *, complete: bool = False) -> dict:
    dispositions = ('present',) * count + ('not_found',) * (3 - count)
    return _observation(
        dispositions=dispositions,
        pod_node=complete,
        state=('present' if complete else 'uncertain' if count == 3 else None))


def _allocation(pointer: str, allocator: str, value: object) -> dict:
    return {
        'json_pointer': pointer,
        'allocator': allocator,
        'value': value,
    }


def _resolved_object(sequence: int, *, with_node: bool = True) -> dict:
    role, kind, name = _ROLE_VALUES[sequence]
    if sequence == 0:
        allocations = [
            _allocation('/spec/clusterIP', 'api_server', '10.0.0.2'),
            _allocation('/spec/clusterIPs', 'api_server', ['10.0.0.2']),
            _allocation('/spec/ipFamilies', 'api_server', ['IPv4']),
            _allocation('/spec/ipFamilyPolicy', 'api_server', 'SingleStack'),
        ]
    elif sequence == 1:
        allocations = [
            _allocation('/spec/clusterIP', 'api_server', 'None'),
            _allocation('/spec/clusterIPs', 'api_server', ['None']),
            _allocation('/spec/ipFamilies', 'api_server', ['IPv4']),
            _allocation('/spec/ipFamilyPolicy', 'api_server', 'SingleStack'),
        ]
    else:
        allocations = ([
            _allocation('/spec/nodeName', 'scheduler', 'worker-node-0')
        ] if with_node else [])
    return {
        'role': role,
        'kind': kind,
        'namespace': 'serve-canary',
        'name': name,
        'uid': f'uid-{role}',
        'observed_semantic_sha256': _semantic_sha256(sequence),
        'server_allocations': allocations,
    }


def _partial_target(count: int, *, pod_node: bool = False) -> dict:
    objects = []
    for sequence, (role, _, _) in enumerate(_ROLE_VALUES):
        committed = sequence < count
        objects.append({
            'sequence': sequence,
            'role': role,
            'disposition': 'committed' if committed else 'unknown',
            'object': (_resolved_object(sequence, with_node=pod_node)
                       if committed else None),
        })
    return {
        'version': 1,
        'requested_target_sha256': _target_sha256(),
        'kubernetes_objects': objects,
    }


def _resolved_target(*, with_node: bool = True) -> dict:
    objects = [
        _resolved_object(sequence,
                         with_node=with_node if sequence == 2 else True)
        for sequence in range(3)
    ]
    return {
        'version': 1,
        'requested_target_sha256': _target_sha256(),
        'provider_resource_id': 'pod/svc-7-user-hash-head',
        'workload_uid': objects[2]['uid'],
        'kubernetes_objects': objects,
        'provider_operation_id': None,
        'resolved_at': _TIME,
    }


def _handle() -> dict:
    config = {
        'context_mode': 'in_cluster',
        'scope_sha256': values.ProviderKubernetesScopeV1.from_value(_scope()
                                                                   ).sha256,
        'namespace': 'serve-canary',
        'port_mode': 'podip',
        'use_internal_ips': True,
        'application_port': '8080',
        'pod_name': 'svc-7-user-hash-head',
        'pod_uid': 'uid-head_pod',
        'node_name': 'worker-node-0',
        'pod_ip': '10.1.2.3',
        'head_service_uid': 'uid-head_service',
        'head_ssh_service_uid': 'uid-head_ssh_service',
        'ambient_fallback': False,
    }
    return {
        'version': 1,
        'cluster_record_uuid': _CLUSTER_UUID,
        'cluster_name': 'svc-7',
        'cluster_name_on_cloud': 'svc-7-user-hash',
        'requested_target_sha256': _target_sha256(),
        'launched_resources_sha256': 'c' * 64,
        'provider_config': config,
        'provider_config_sha256': values.canonical_sha256(config),
    }


def _create_effect(sequence: int, claim: dict | None = None) -> dict:
    if claim is None:
        claim = _claim()
    return {
        'version': 1,
        'evidence_kind': 'core_v1_create_committed',
        'effect_sequence': sequence,
        'effect_kind': 'core_v1_create',
        'role': _ROLE_VALUES[sequence][0],
        'intent_phase': 'CREATE_INTENT',
        'intent_origin': claim,
        'evidence_commit_origin': claim,
        'commit_disposition': 'created',
        'request_body_sha256': chr(ord('d') + sequence) * 64,
        'requested_semantic_sha256': _semantic_sha256(sequence),
        'object_at_commit': _resolved_object(sequence),
    }


def _handle_effect(claim: dict) -> dict:
    handle = _handle()
    return {
        'version': 1,
        'evidence_kind': 'cluster_record_insert_committed',
        'effect_sequence': 3,
        'effect_kind': 'cluster_record_insert',
        'role': None,
        'intent_phase': 'HANDLE_INTENT',
        'intent_origin': claim,
        'evidence_commit_origin': claim,
        'write_disposition': 'inserted',
        'intended_handle': handle,
        'intended_handle_sha256': values.canonical_sha256(handle),
    }


def _runtime_evidence(*, observed_at: str = _TIME) -> dict:
    return {
        'version': 1,
        'pod_uid': 'uid-head_pod',
        'container_name': 'ray-node',
        'requested_image': _qualification(),
        'observed_runtime_image': _worker()['image']['runtime'],
        'container_started': True,
        'startup_probe_succeeded': True,
        'runtime_contract_sha256': 'a' * 64,
        'artifact_measurements': [{
            'role': role.value,
            'binding_sha256': chr(ord('a') + index) * 64,
            'observed_tree_sha256': chr(ord('1') + index) * 64,
            'matches_expected_manifest': True,
        } for index, role in enumerate(values.ProviderWorkloadArtifactRoleV1)],
        'ray_health': 'ready',
        'skylet_health': 'ready',
        'skylet_state_store_uuid': _STATE_STORE_UUID,
        'observed_at': observed_at,
    }


def _endpoint_evidence(*, observed_at: str = _TIME) -> dict:
    return {
        'version': 1,
        'pod_uid': 'uid-head_pod',
        'pod_ip': '10.1.2.3',
        'application_port': '8080',
        'provider_config_sha256': values.canonical_sha256(
            _handle()['provider_config']),
        'resolution': 'exact_handle_podip',
        'observed_at': observed_at,
    }


def _submit_request() -> dict:
    return skylet_fixtures._submit_request(submission_key=str(_ACTION_ID),
                                           replica_id='7')


def _job_evidence(
        disposition: str = 'present',
        *,
        durable_state: str = 'RUNNING',
        record_revision: int = 3,
        run_epoch: int = 0,
        retained_submit_request: dict | None = None,
        state_store_uuid: str = _STATE_STORE_UUID,
        submission_key: str = str(_ACTION_ID),
) -> dict:
    expected_request = _submit_request()
    if retained_submit_request is None:
        retained_submit_request = expected_request
    has_record = disposition in ('present', 'conflict')
    return {
        'protocol': 'skylet_idempotent_submit_v1',
        'submission_key': submission_key,
        'job_contract_sha256': expected_request['job_contract_sha256'],
        'job_spec_sha256': expected_request['job_spec_sha256'],
        'retained_submit_request':
            (retained_submit_request if has_record else None),
        'state_store_uuid': state_store_uuid,
        'read_disposition': disposition,
        'durable_state': durable_state if has_record else None,
        'job_id': 19 if has_record else None,
        'run_epoch': run_epoch if has_record else None,
        'record_revision': record_revision if has_record else None,
        'observed_at': _TIME,
    }


def _job_effect(claim: dict,
                *,
                retained_submit_request: dict | None = None) -> dict:
    if retained_submit_request is None:
        retained_submit_request = _submit_request()
    job = _job_evidence(durable_state='COMMITTED_PENDING_START',
                        record_revision=2,
                        retained_submit_request=retained_submit_request)
    return {
        'version': 1,
        'evidence_kind': 'skylet_job_submit_committed',
        'effect_sequence': 4,
        'effect_kind': 'skylet_job_submit',
        'role': None,
        'intent_phase': 'JOB_INTENT',
        'intent_origin': claim,
        'evidence_commit_origin': claim,
        'commit_disposition': 'submitted',
        'submit_request_sha256':
            values.canonical_sha256(retained_submit_request),
        'job_at_commit': job,
    }


def _launch_cursor(phase: str, *, claim: dict | None = None) -> dict:
    if claim is None:
        claim = _claim(claimed_cursor_sha256='e' * 64)
    effects = [_create_effect(sequence) for sequence in range(3)]
    base = {
        'version': 1,
        'action_kind': 'launch',
        'phase': phase,
        'committed_effects': effects,
    }
    if phase == 'OBJECTS_EXACT':
        base.update({
            'resolved_target': _resolved_target(),
            'post_observation': _prefix_observation(3, complete=True),
        })
    elif phase == 'HANDLE_INTENT':
        base.update({
            'intent_origin': claim,
            'resolved_target': _resolved_target(),
            'intended_handle': _handle(),
        })
    elif phase == 'HANDLE_COMMITTED':
        base['committed_effects'].append(_handle_effect(claim))
        base.update({
            'resolved_target': _resolved_target(),
            'handle': _handle(),
        })
    else:
        raise AssertionError(f'unsupported test phase: {phase}')
    return base


def _advanced_launch_cursor(
    phase: str,
    *,
    claim: dict | None = None,
    submit_request: dict | None = None,
    committed_submit_request: dict | None = None,
    job_state: str = 'RUNNING',
    job_revision: int = 3,
    job_run_epoch: int = 0,
) -> dict:
    if claim is None:
        claim = _claim(claimed_cursor_sha256='e' * 64)
    if submit_request is None:
        submit_request = _submit_request()
    if committed_submit_request is None:
        committed_submit_request = submit_request
    cursor = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    cursor['phase'] = phase
    cursor['runtime_evidence'] = _runtime_evidence()
    if phase == 'RUNTIME_READY':
        return cursor
    if phase == 'JOB_INTENT':
        cursor.update({
            'intent_origin': claim,
            'submit_request': submit_request,
        })
        return cursor
    cursor['committed_effects'].append(
        _job_effect(claim, retained_submit_request=committed_submit_request))
    if phase == 'JOB_COMMITTED':
        cursor['job'] = copy.deepcopy(
            cursor['committed_effects'][-1]['job_at_commit'])
        return cursor
    cursor['job'] = _job_evidence(
        durable_state=job_state,
        record_revision=job_revision,
        run_epoch=job_run_epoch,
        retained_submit_request=committed_submit_request)
    if phase == 'JOB_RUNNING':
        return cursor
    cursor['endpoint'] = _endpoint_evidence()
    if phase == 'ENDPOINT_RESOLVED':
        return cursor
    if phase == 'SUCCEEDED':
        cursor['success_observation'] = _observation(dispositions=('present',
                                                                   'present',
                                                                   'present'))
        return cursor
    raise AssertionError(f'unsupported advanced test phase: {phase}')


def _envelope(cursor: dict, attestation: dict | None = None) -> dict:
    return {
        'version': 1,
        'cursor': cursor,
        'worker_attestation': attestation,
    }


def _delete_target(states: tuple[str, str, str]) -> dict:
    dispositions = tuple('present' if state == 'present_exact' else 'not_found'
                         for state in states)
    return {
        'version': 1,
        'requested_target_sha256': _target_sha256(),
        'prior_launch_basis_sha256': 'f' * 64,
        'objects': [{
            'plan_sequence': sequence,
            'role': role,
            'expected_uid': f'uid-{role}',
            'state': states[sequence],
            'requested_semantic_sha256': _semantic_sha256(sequence),
        } for sequence, (role, _, _) in enumerate(_ROLE_VALUES)],
        'observation': _observation(dispositions=dispositions),
    }


def _down_cursor(phase: str,
                 states: tuple[str, str, str],
                 role: str | None = None) -> dict:
    value = {
        'version': 1,
        'action_kind': 'down',
        'phase': phase,
        'delete_target': _delete_target(states),
    }
    if role is not None:
        value['role'] = role
    if phase in ('ABSENCE_EXACT', 'HANDLE_REMOVE_INTENT', 'HANDLE_REMOVED',
                 'SUCCEEDED'):
        value['absence_observation'] = _observation(absent=True)
    if phase in ('HANDLE_REMOVED', 'SUCCEEDED'):
        value['handle_removal'] = {
            'version': 1,
            'cluster_name': 'svc-7',
            'expected_cluster_record_uuid': _CLUSTER_UUID,
            'disposition': 'already_absent',
            'removed_handle': None,
            'removed_handle_sha256': None,
            'observed_at': _TIME,
        }
    return value


def _context(action_kind: kernel.ActionKind) -> progress._ActionContext:
    submit_request = values.ProviderSkyletSubmitRequestV1.from_value(
        _submit_request())
    return progress._ActionContext(
        action_id=_ACTION_ID,
        action_kind=action_kind,
        requested_target=values.ProviderLocatorV1.from_value(_target()),
        resources_sha256='c' * 64,
        launch_object_commitments=tuple(
            (chr(ord('d') + sequence) * 64, _semantic_sha256(sequence),
             'serve-canary', name)
            for sequence, (_, _, name) in enumerate(_ROLE_VALUES)),
        launch_skylet_binding=(submit_request.job_contract_sha256,
                               submit_request.job_spec.source.canonical_bytes,
                               submit_request.job_spec.replica_id),
        executor_cohort=values.ProviderAuthorityWorkerCohortV1.from_value(
            _cohort()),
        launch_workspace_identity=values.ProviderWorkspaceIdentityV1(
            version=1,
            workspace='workspace-a',
            kubernetes_scope=values.ProviderKubernetesScopeV1.from_value(
                _scope())),
        launch_application_port='8080',
        launch_image_qualification=(
            values.ProviderOCIImageQualificationV1.from_value(
                _qualification())),
        launch_runtime_contract_sha256='a' * 64,
        launch_artifact_bindings=tuple(
            (role.value, chr(ord('a') + index) * 64, chr(ord('1') + index) * 64)
            for index, role in enumerate(
                values.ProviderWorkloadArtifactRoleV1)))


def _action_record(action_kind: kernel.ActionKind) -> kernel.ActionRecord:
    now = datetime.datetime.now(datetime.timezone.utc)
    return kernel.ActionRecord(action_id=_ACTION_ID,
                               domain='serve',
                               resource_type='replica',
                               resource_identity='test',
                               desired_generation=1,
                               action_type=action_kind.value,
                               immutable_spec={},
                               immutable_spec_sha256='0' * 64,
                               kernel_state=kernel.KernelState.QUEUED,
                               current_attempt=1,
                               next_attempt_at=None,
                               last_result=None,
                               last_result_sha256=None,
                               terminal_disposition=None,
                               revision=1,
                               created_at=now,
                               updated_at=now,
                               terminal_at=None)


def _attempt_record(
    progress_value: dict | None,
    *,
    attempt: int = 1,
    mutation_boundary: kernel.MutationBoundary | None = None,
    provider_io_boundary: kernel.ProviderIOBoundary | None = None,
    revision: int | None = None,
) -> kernel.AttemptRecord:
    now = datetime.datetime.now(datetime.timezone.utc)
    if revision is None:
        revision = 0 if progress_value is None else 1
    if provider_io_boundary is None:
        if progress_value is None:
            provider_io_boundary = kernel.ProviderIOBoundary.NOT_STARTED
        else:
            cursor = progress_value['cursor']
            first = (cursor['phase'] == 'TARGET_RESOLVED' or
                     (cursor['phase'] == 'CREATE_INTENT' and
                      cursor['role'] == 'head_ssh_service'))
            provider_io_boundary = (
                kernel.ProviderIOBoundary.INTENT_COMMITTED
                if first else kernel.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS)
    if mutation_boundary is None:
        mutation_boundary = kernel.MutationBoundary(provider_io_boundary.value)
    return kernel.AttemptRecord(
        action_id=_ACTION_ID,
        attempt=attempt,
        request_id=kernel.request_id_for_attempt(_ACTION_ID, attempt),
        request_input_sha256='1' * 64,
        provider_operation_id=None,
        mutation_boundary=mutation_boundary,
        provider_io_boundary=provider_io_boundary,
        provider_progress=progress_value,
        provider_progress_sha256=(None if progress_value is None else
                                  values.canonical_sha256(progress_value)),
        provider_progress_revision=revision,
        typed_outcome={'version': 1},
        typed_outcome_sha256=values.canonical_sha256({'version': 1}),
        request_terminal_state='SUCCEEDED'
        if mutation_boundary is kernel.MutationBoundary.SETTLED else None,
        admitted_at=now,
        updated_at=now,
        settled_at=now
        if mutation_boundary is kernel.MutationBoundary.SETTLED else None)


def _provider_result(
    disposition: str,
    certainty: str,
    *,
    provider_operation_id: str | None = None,
    provider_code: str | None = None,
    retry_class: str | None = None,
    retry_after_seconds: int | None = None,
    observation: dict | None = None,
    normalized_message: str | None = None,
) -> dict:
    return {
        'disposition': disposition,
        'certainty': certainty,
        'provider_operation_id': provider_operation_id,
        'provider_code': provider_code,
        'retry_class': retry_class,
        'retry_after_seconds': retry_after_seconds,
        'observation': observation,
        'normalized_message': normalized_message,
    }


def test_action_context_loads_exact_completed_down_capsule() -> None:
    basis = values.CompletedLaunchBasisV1.from_value(
        down_config_fixtures.completed_basis_payload())
    cleanup = values.ProviderKubernetesCleanupTargetV1.from_value(
        down_config_fixtures._cleanup_target())
    spec = values.ServeReplicaActionSpecV1.from_value(
        down_config_fixtures._down_spec_payload_for_basis(basis, cleanup))
    plan = spec.provider_plan
    now = datetime.datetime.now(datetime.timezone.utc)
    action = kernel.ActionRecord(
        action_id=spec.action_id,
        domain='serve',
        resource_type='replica',
        resource_identity=plan.resource_identity.action_identity(
            plan.action_kind).resource_identity,
        desired_generation=plan.resource_identity.desired_generation,
        action_type='down',
        immutable_spec=spec.canonical_value(),
        immutable_spec_sha256=spec.sha256,
        kernel_state=kernel.KernelState.QUEUED,
        current_attempt=1,
        next_attempt_at=None,
        last_result=None,
        last_result_sha256=None,
        terminal_disposition=None,
        revision=1,
        created_at=now,
        updated_at=now,
        terminal_at=None)
    context = progress._ActionContext.from_record(action)
    capsule = spec.invocation.require_down().execution_config.capsule
    assert context.executor_cohort == capsule.executor_cohort
    assert context.down_cleanup_target == cleanup
    assert context.down_prior_launch_basis_sha256 == basis.sha256
    assert context.launch_object_commitments == tuple(
        (item.plan.request_body_sha256, item.plan.requested_semantic_sha256,
         item.plan.namespace, item.plan.name) for item in cleanup.objects)


def _provider_error(
    category: str,
    *,
    provider_code: str = 'ProviderError',
    retry_after_seconds: int | None = None,
    normalized_message: str = 'normalized provider error',
) -> dict:
    return {
        'category': category,
        'provider_code': provider_code,
        'retry_after_seconds': retry_after_seconds,
        'normalized_message': normalized_message,
    }


def _terminal_return(
    action_kind: kernel.ActionKind,
    attempt: kernel.AttemptRecord,
    provider_result: dict,
    *,
    reduction_kind: str = 'domain',
    normalized_provider_error: dict | None = None,
    launch_no_effect_resolution: dict | None = None,
    terminal_attestation: dict | None = None,
) -> dict:
    if terminal_attestation is None:
        if attempt.provider_progress is None:
            terminal_attestation = _attestation(attempt=attempt.attempt)
        else:
            terminal_attestation = copy.deepcopy(
                attempt.provider_progress['worker_attestation'])
            assert terminal_attestation is not None
    terminal = {
        'version': 1,
        'result_kind': 'serve_resource_action_handler_terminal_v1',
        'action_id': str(attempt.action_id),
        'action_kind': action_kind.value,
        'attempt': attempt.attempt,
        'request_id': attempt.request_id,
        'request_execution_generation':
            terminal_attestation['request_execution_generation'],
        'handler_name': f'serve_resource_action_{action_kind.value}',
        'reduction_kind': reduction_kind,
        'request_input_sha256': attempt.request_input_sha256,
        'final_provider_progress_sha256': attempt.provider_progress_sha256,
        'worker_attestation': terminal_attestation,
        'worker_attestation_sha256':
            values.canonical_sha256(terminal_attestation),
        'provider_result': provider_result,
        'normalized_provider_error': normalized_provider_error,
        'launch_no_effect_resolution': launch_no_effect_resolution,
    }
    return {
        'version': 1,
        'return_type': 'serve_replica_action_handler_terminal_result_v1',
        'terminal_result': terminal,
        'terminal_result_sha256': values.canonical_sha256(terminal),
    }


def _reduction_context(
    action_kind: kernel.ActionKind,
    request_return: dict,
    *,
    finished_at: datetime.datetime,
    database_now: datetime.datetime,
    worker_instance_id: str | None = None,
) -> kernel.ReductionContext:
    terminal = request_return['terminal_result']
    if worker_instance_id is None:
        worker_instance_id = terminal['worker_attestation']['request_worker_id']
    request = requests_lib.Request(
        request_id=terminal['request_id'],
        name='serve-resource-action-test',
        entrypoint=lambda: None,
        request_body=None,
        status=requests_lib.RequestStatus.SUCCEEDED,
        created_at=finished_at.timestamp() - 1,
        user_id='test-user',
        return_value=request_return,
        finished_at=finished_at.timestamp(),
        handler_name=f'serve_resource_action_{action_kind.value}',
        execution_generation=terminal['request_execution_generation'],
        worker_instance_id=worker_instance_id)
    return kernel.ReductionContext(request, database_now)


def _fallback_context(
    action_kind: kernel.ActionKind,
    attempt: kernel.AttemptRecord,
    status: requests_lib.RequestStatus,
    *,
    return_value: object = None,
) -> kernel.ReductionContext:
    finished_at = datetime.datetime(2026,
                                    8,
                                    1,
                                    1,
                                    2,
                                    3,
                                    4,
                                    tzinfo=datetime.timezone.utc)
    request = requests_lib.Request(
        request_id=attempt.request_id,
        name='serve-resource-action-test',
        entrypoint=lambda: None,
        request_body=None,
        status=status,
        created_at=finished_at.timestamp() - 1,
        user_id='test-user',
        return_value=return_value,
        finished_at=finished_at.timestamp(),
        handler_name=f'serve_resource_action_{action_kind.value}')
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    return kernel.ReductionContext(request, database_now)


def _settled_attempt(
    attempt: kernel.AttemptRecord,
    reduction: kernel.ActionReduction,
    settled_at: datetime.datetime,
    request_terminal_state: str = 'SUCCEEDED',
) -> kernel.AttemptRecord:
    return dataclasses.replace(
        attempt,
        mutation_boundary=kernel.MutationBoundary.SETTLED,
        typed_outcome=reduction.typed_outcome,
        typed_outcome_sha256=values.canonical_sha256(reduction.typed_outcome),
        request_terminal_state=request_terminal_state,
        updated_at=settled_at,
        settled_at=settled_at)


def _settled_handler_attempt(
    attempt: kernel.AttemptRecord,
    provider_result: dict,
) -> kernel.AttemptRecord:
    outcome = progress.ServeReplicaActionHandlerOutcomeV1(
        handler_terminal_result_sha256='a' * 64,
        provider_result=(progress.ServeReplicaActionProviderResultV1.from_value(
            provider_result)),
        supersession_quiescence=None,
        launch_no_io_prefix=None).canonical_value()
    settled_at = datetime.datetime(2026,
                                   8,
                                   2,
                                   4,
                                   5,
                                   6,
                                   7,
                                   tzinfo=datetime.timezone.utc)
    return dataclasses.replace(
        attempt,
        mutation_boundary=kernel.MutationBoundary.SETTLED,
        typed_outcome=outcome,
        typed_outcome_sha256=values.canonical_sha256(outcome),
        request_terminal_state='SUCCEEDED',
        updated_at=settled_at,
        settled_at=settled_at)


def test_progress_local_attestation_runtime_and_endpoint_are_closed() -> None:
    attestation = progress.ProviderAuthorityWorkerAttemptAttestationV1.from_value(
        _attestation(after=True))
    assert attestation.sha256 == values.canonical_sha256(
        attestation.canonical_value())

    runtime = {
        'version': 1,
        'pod_uid': 'uid-head_pod',
        'container_name': 'ray-node',
        'requested_image': _qualification(),
        'observed_runtime_image': _worker()['image']['runtime'],
        'container_started': True,
        'startup_probe_succeeded': True,
        'runtime_contract_sha256': 'a' * 64,
        'artifact_measurements': [{
            'role': role.value,
            'binding_sha256': chr(ord('a') + index) * 64,
            'observed_tree_sha256': chr(ord('1') + index) * 64,
            'matches_expected_manifest': True,
        } for index, role in enumerate(values.ProviderWorkloadArtifactRoleV1)],
        'ray_health': 'ready',
        'skylet_health': 'ready',
        'skylet_state_store_uuid': _STATE_STORE_UUID,
        'observed_at': _TIME,
    }
    assert progress.ProviderKubernetesRuntimeEvidenceV1.from_value(
        runtime).pod_uid == 'uid-head_pod'

    endpoint = {
        'version': 1,
        'pod_uid': 'uid-head_pod',
        'pod_ip': '10.1.2.3',
        'application_port': '8080',
        'provider_config_sha256': values.canonical_sha256(
            _handle()['provider_config']),
        'resolution': 'exact_handle_podip',
        'observed_at': _TIME,
    }
    assert progress.ProviderKubernetesEndpointEvidenceV1.from_value(
        endpoint).pod_ip == '10.1.2.3'
    unknown = copy.deepcopy(endpoint)
    unknown['extra'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        progress.ProviderKubernetesEndpointEvidenceV1.from_value(unknown)


def test_progress_uses_shared_api006_resolved_target_contract() -> None:
    assert (progress.ProviderResolvedTargetV1
            is values.ResolvedProviderTargetV1)
    parsed = progress.ProviderResolvedTargetV1.from_value(_resolved_target())
    assert parsed.canonical_value() == _resolved_target()


def test_authoritative_observation_vectors_and_projection_are_closed() -> None:
    present = _observation(dispositions=('present', 'present', 'present'))
    parsed = progress.ProviderLifecycleObservationV1.from_value(present)
    assert parsed.state is progress.ProviderObservationStateV1.PRESENT

    crossed_ready = copy.deepcopy(present)
    crossed_ready['ready'] = False
    with pytest.raises(ValueError, match='top-level projection differs'):
        progress.ProviderLifecycleObservationV1.from_value(crossed_ready)

    mixed = _observation(dispositions=('present', 'not_found', 'not_found'))
    assert progress.ProviderLifecycleObservationV1.from_value(
        mixed).state is progress.ProviderObservationStateV1.UNCERTAIN
    crossed_mixed = copy.deepcopy(mixed)
    crossed_mixed['observed_workload_uid'] = 'uid-head_pod'
    with pytest.raises(ValueError, match='top-level identity/readiness'):
        progress.ProviderLifecycleObservationV1.from_value(crossed_mixed)
    wrong_mixed_state = copy.deepcopy(mixed)
    wrong_mixed_state['state'] = 'present'
    with pytest.raises(ValueError, match='mixed present/NotFound'):
        progress.ProviderLifecycleObservationV1.from_value(wrong_mixed_state)

    absent = _observation(absent=True)
    wrong_absent_state = copy.deepcopy(absent)
    wrong_absent_state['state'] = 'uncertain'
    with pytest.raises(ValueError, match='all-NotFound'):
        progress.ProviderLifecycleObservationV1.from_value(wrong_absent_state)

    uncertain_read = _observation()
    uncertain_read['certainty'] = 'authoritative'
    with pytest.raises(ValueError, match='uncertain object read'):
        progress.ProviderLifecycleObservationV1.from_value(uncertain_read)


def test_observation_context_binds_scope_plans_and_identity_labels() -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    observation = progress.ProviderLifecycleObservationV1.from_value(
        _observation())
    observation.validate_action_context(context)

    crossed_scope = _observation()
    crossed_scope['evidence']['frozen_scope'][
        'api_server_git_version'] = 'v1.34.0'
    crossed_scope['evidence_sha256'] = values.canonical_sha256(
        crossed_scope['evidence'])
    with pytest.raises(ValueError, match='frozen Kubernetes action scope'):
        progress.ProviderLifecycleObservationV1.from_value(
            crossed_scope).validate_action_context(context)

    crossed_name = _observation()
    crossed_name['evidence']['objects'][0]['name'] = 'crossed-head-ssh'
    crossed_name['evidence_sha256'] = values.canonical_sha256(
        crossed_name['evidence'])
    with pytest.raises(ValueError, match='immutable action plan'):
        progress.ProviderLifecycleObservationV1.from_value(
            crossed_name).validate_action_context(context)

    crossed_hash = _observation()
    crossed_hash['evidence']['objects'][1][
        'requested_semantic_sha256'] = 'f' * 64
    crossed_hash['evidence_sha256'] = values.canonical_sha256(
        crossed_hash['evidence'])
    with pytest.raises(ValueError, match='immutable action plan'):
        progress.ProviderLifecycleObservationV1.from_value(
            crossed_hash).validate_action_context(context)

    crossed_label = _observation(dispositions=('present', 'not_found',
                                               'not_found'))
    crossed_label['evidence']['objects'][0]['cluster_record_uuid_label'] = (
        '77777777-7777-4777-8777-777777777777')
    crossed_label['evidence_sha256'] = values.canonical_sha256(
        crossed_label['evidence'])
    with pytest.raises(ValueError, match='identity labels differ'):
        progress.ProviderLifecycleObservationV1.from_value(
            crossed_label).validate_action_context(context)

    without_object_plans = dataclasses.replace(context,
                                               launch_object_commitments=None)
    with pytest.raises(ValueError, match='PriorLaunchBasisV1 is absent'):
        observation.validate_action_context(without_object_plans)


def test_launch_literal_prefix_committed_hashes_and_handle_edge() -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    intent_value = _launch_cursor('HANDLE_INTENT', claim=claim)
    committed_value = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    intent = progress.ProviderLaunchProgressV1.from_value(intent_value)
    committed = progress.ProviderLaunchProgressV1.from_value(committed_value)

    assert [effect.effect_sequence for effect in committed.committed_effects
           ] == [0, 1, 2, 3]
    intent.validate_successor(committed)

    bad_hash = copy.deepcopy(committed_value)
    bad_hash['committed_effects'][3]['intended_handle_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='handle hash'):
        progress.ProviderLaunchProgressV1.from_value(bad_hash)

    sparse = copy.deepcopy(committed_value)
    sparse['committed_effects'][2]['effect_sequence'] = 1
    with pytest.raises(ValueError, match='sequence and role|contiguous'):
        progress.ProviderLaunchProgressV1.from_value(sparse)

    crossed_successor = copy.deepcopy(committed_value)
    crossed_successor['handle']['launched_resources_sha256'] = 'b' * 64
    crossed_successor['committed_effects'][3][
        'intended_handle'] = copy.deepcopy(crossed_successor['handle'])
    crossed_successor['committed_effects'][3][
        'intended_handle_sha256'] = values.canonical_sha256(
            crossed_successor['handle'])
    with pytest.raises(ValueError, match='exact intended handle'):
        intent.validate_successor(
            progress.ProviderLaunchProgressV1.from_value(crossed_successor))

    adopted = copy.deepcopy(committed_value)
    effect = adopted['committed_effects'][3]
    effect['write_disposition'] = 'adopted_exact'
    origin = copy.deepcopy(effect['evidence_commit_origin'])
    origin['worker_attestation']['before']['pod_uid'] = 'other-worker-pod'
    origin['worker_attestation_sha256'] = values.canonical_sha256(
        origin['worker_attestation'])
    effect['evidence_commit_origin'] = origin
    with pytest.raises(ValueError, match='execution provenance'):
        progress.ProviderLaunchProgressV1.from_value(adopted)


def test_launch_phase_observations_bind_exact_committed_prefixes() -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    create = _create_intent_cursor(0, claim)
    partial = {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'OBJECTS_PARTIAL',
        'committed_effects': [
            _create_effect(sequence, claim) for sequence in range(2)
        ],
        'known_objects': _partial_target(2),
        'post_observation': _prefix_observation(2),
    }
    exact = _launch_cursor('OBJECTS_EXACT', claim=claim)
    for cursor in (create, partial, exact):
        progress.ProviderLaunchProgressV1.from_value(cursor)

    crossed_create = copy.deepcopy(create)
    crossed_create['pre_observation'] = _prefix_observation(1)
    with pytest.raises(ValueError, match='committed-prefix matrix'):
        progress.ProviderLaunchProgressV1.from_value(crossed_create)

    crossed_partial = copy.deepcopy(partial)
    crossed_partial['post_observation'] = _prefix_observation(1)
    with pytest.raises(ValueError, match='committed-prefix matrix'):
        progress.ProviderLaunchProgressV1.from_value(crossed_partial)

    crossed_exact = copy.deepcopy(exact)
    crossed_exact['post_observation']['resolved_target'][
        'resolved_at'] = '2026-08-01T01:02:04.000005Z'
    with pytest.raises(ValueError, match='not byte-equal'):
        progress.ProviderLaunchProgressV1.from_value(crossed_exact)


def test_launch_successor_carries_exact_durable_evidence() -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)

    objects_exact = progress.ProviderLaunchProgressV1.from_value(
        _launch_cursor('OBJECTS_EXACT', claim=claim))
    crossed_target = _launch_cursor('HANDLE_INTENT', claim=claim)
    crossed_target['resolved_target'][
        'resolved_at'] = '2026-08-01T01:02:04.000005Z'
    with pytest.raises(ValueError, match='resolved_target'):
        objects_exact.validate_successor(
            progress.ProviderLaunchProgressV1.from_value(crossed_target))

    handle_committed = progress.ProviderLaunchProgressV1.from_value(
        _launch_cursor('HANDLE_COMMITTED', claim=claim))
    runtime_ready = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('RUNTIME_READY', claim=claim))
    crossed_handle = copy.deepcopy(_handle())
    crossed_handle['provider_config']['pod_ip'] = '10.1.2.4'
    crossed_handle['provider_config_sha256'] = values.canonical_sha256(
        crossed_handle['provider_config'])
    crossed_runtime_ready = dataclasses.replace(
        runtime_ready,
        handle=values.ProviderKubernetesHandleV1.from_value(crossed_handle))
    with pytest.raises(ValueError, match='handle'):
        handle_committed.validate_successor(crossed_runtime_ready)

    crossed_runtime = _advanced_launch_cursor('JOB_INTENT', claim=claim)
    crossed_runtime['runtime_evidence'][
        'observed_at'] = '2026-08-01T01:02:04.000005Z'
    with pytest.raises(ValueError, match='runtime_evidence'):
        runtime_ready.validate_successor(
            progress.ProviderLaunchProgressV1.from_value(crossed_runtime))

    endpoint_resolved = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('ENDPOINT_RESOLVED', claim=claim))
    crossed_endpoint = _advanced_launch_cursor('SUCCEEDED', claim=claim)
    crossed_endpoint['endpoint']['observed_at'] = '2026-08-01T01:02:04.000005Z'
    with pytest.raises(ValueError, match='endpoint'):
        endpoint_resolved.validate_successor(
            progress.ProviderLaunchProgressV1.from_value(crossed_endpoint))

    running_revision_four = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_RUNNING', claim=claim, job_revision=4))
    endpoint_revision_three = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('ENDPOINT_RESOLVED',
                                claim=claim,
                                job_revision=3))
    with pytest.raises(ValueError, match='regressed revision'):
        running_revision_four.validate_successor(endpoint_revision_three)

    with pytest.raises(ValueError, match='strictly newer record revision'):
        progress.ProviderLaunchProgressV1.from_value(
            _advanced_launch_cursor('JOB_RUNNING', claim=claim, job_revision=2))


def test_launch_runtime_handle_and_endpoint_bind_full_capsule_context() -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    cursor = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('ENDPOINT_RESOLVED'))
    cursor.validate_action_context(context)

    wrong_port = dataclasses.replace(context, launch_application_port='8081')
    with pytest.raises(ValueError, match='application port'):
        cursor.validate_action_context(wrong_port)

    wrong_runtime = dataclasses.replace(context,
                                        launch_runtime_contract_sha256='b' * 64)
    with pytest.raises(ValueError, match='runtime capsule'):
        cursor.validate_action_context(wrong_runtime)

    assert context.launch_workspace_identity is not None
    workspace_value = context.launch_workspace_identity.canonical_value()
    workspace_value['kubernetes_scope']['api_server_git_version'] = 'v1.34.0'
    wrong_workspace = dataclasses.replace(
        context,
        launch_workspace_identity=values.ProviderWorkspaceIdentityV1.from_value(
            workspace_value))
    with pytest.raises(ValueError, match='workspace scope'):
        cursor.validate_action_context(wrong_workspace)


def test_job_intent_c4_retains_exact_submit_request_bytes(monkeypatch) -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    intent = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_INTENT', claim=claim))
    committed = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_COMMITTED', claim=claim))
    intent.validate_successor(committed)

    byte_different = _submit_request()
    byte_different['job_spec']['source']['workspace'] = 'workspace-crossed'
    byte_different['job_spec_sha256'] = values.canonical_sha256(
        byte_different['job_spec'])
    crossed = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_COMMITTED',
                                claim=claim,
                                committed_submit_request=byte_different))
    crossed_retained = crossed.committed_effects[-1].job_at_commit
    assert crossed_retained is not None
    assert crossed_retained.retained_submit_request is not None
    monkeypatch.setattr(values, 'canonical_sha256', lambda unused: '6' * 64)
    assert (intent.submit_request is not None and intent.submit_request.sha256
            == crossed_retained.retained_submit_request.sha256)
    with pytest.raises(ValueError, match='exact current submit request'):
        intent.validate_successor(crossed)


@pytest.mark.parametrize('phase',
                         ['JOB_RUNNING', 'ENDPOINT_RESOLVED', 'SUCCEEDED'])
def test_running_and_later_phases_require_exact_running_job(phase: str) -> None:
    progress.ProviderLaunchProgressV1.from_value(_advanced_launch_cursor(phase))
    crossed = _advanced_launch_cursor(phase, job_state='RECOVERY_PENDING')
    with pytest.raises(ValueError, match='exact present RUNNING'):
        progress.ProviderLaunchProgressV1.from_value(crossed)


def test_skylet_recovery_cycle_and_same_phase_read_refresh_are_monotonic(
) -> None:
    recovery_job = _job_evidence(durable_state='RECOVERY_PENDING',
                                 record_revision=4,
                                 run_epoch=1)
    restart_job = _job_evidence(durable_state='START_INTENT',
                                record_revision=5,
                                run_epoch=2)
    recovery = values.ProviderSkyletJobEvidenceV1.from_value(recovery_job)
    restart = values.ProviderSkyletJobEvidenceV1.from_value(restart_job)
    progress._stable_job_evidence(recovery, restart)

    same_epoch_restart = values.ProviderSkyletJobEvidenceV1.from_value(
        _job_evidence(durable_state='START_INTENT',
                      record_revision=5,
                      run_epoch=1))
    with pytest.raises(ValueError, match='strictly newer run epoch'):
        progress._stable_job_evidence(recovery, same_epoch_restart)

    terminal = values.ProviderSkyletJobEvidenceV1.from_value(
        _job_evidence(durable_state='SUCCEEDED', record_revision=6,
                      run_epoch=2))
    restart_after_terminal = values.ProviderSkyletJobEvidenceV1.from_value(
        _job_evidence(durable_state='START_INTENT',
                      record_revision=7,
                      run_epoch=3))
    with pytest.raises(ValueError, match='terminal durable state'):
        progress._stable_job_evidence(terminal, restart_after_terminal)

    running_before = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_RUNNING', job_revision=3, job_run_epoch=1))
    running_after = progress.ProviderLaunchProgressV1.from_value(
        _advanced_launch_cursor('JOB_RUNNING', job_revision=6, job_run_epoch=2))
    running_before.validate_successor(running_after)

    committed_recovery_value = _advanced_launch_cursor('JOB_COMMITTED')
    committed_recovery_value['committed_effects'][-1][
        'job_at_commit'] = recovery_job
    committed_recovery_value['job'] = copy.deepcopy(recovery_job)
    committed_restart_value = copy.deepcopy(committed_recovery_value)
    committed_restart_value['job'] = restart_job
    committed_recovery = progress.ProviderLaunchProgressV1.from_value(
        committed_recovery_value)
    committed_restart = progress.ProviderLaunchProgressV1.from_value(
        committed_restart_value)
    committed_recovery.validate_successor(committed_restart)


def test_launch_succeeded_requires_ready_true_observation() -> None:
    crossed = _advanced_launch_cursor('SUCCEEDED')
    observation = crossed['success_observation']
    observation['ready'] = False
    observation['evidence']['objects'][2]['ready'] = False
    observation['evidence_sha256'] = values.canonical_sha256(
        observation['evidence'])
    with pytest.raises(ValueError, match='ready present observation'):
        progress.ProviderLaunchProgressV1.from_value(crossed)


def test_launch_origin_order_is_attempt_then_generation() -> None:
    first = _claim(attempt=1, generation=9)
    second = _claim(attempt=2, generation=1)
    cursor = {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'OBJECTS_PARTIAL',
        'committed_effects': [
            _create_effect(0, first),
            _create_effect(1, second),
        ],
        'known_objects': _partial_target(2),
        'post_observation': _prefix_observation(2),
    }
    parsed = progress.ProviderLaunchProgressV1.from_value(cursor)
    assert parsed.committed_effects[1].intent_origin.origin_key == (2, 1)

    crossed = copy.deepcopy(cursor)
    crossed['committed_effects'][1] = _create_effect(
        1, _claim(attempt=1, generation=8))
    with pytest.raises(ValueError, match='precedes'):
        progress.ProviderLaunchProgressV1.from_value(crossed)


def test_down_literal_delete_order_and_monotonic_checkpoint() -> None:
    present = ('present_exact', 'present_exact', 'present_exact')
    target = progress.ProviderDownProgressV1.from_value(
        _down_cursor('TARGET_RESOLVED', present))
    intent = progress.ProviderDownProgressV1.from_value(
        _down_cursor('DELETE_INTENT', present, role='head_service'))
    after_head_service = ('present_exact', 'absent_exact', 'present_exact')
    partial = progress.ProviderDownProgressV1.from_value(
        _down_cursor('DELETE_PARTIAL', after_head_service))

    target.validate_successor(intent)
    intent.validate_successor(partial)
    next_intent = progress.ProviderDownProgressV1.from_value(
        _down_cursor('DELETE_INTENT',
                     after_head_service,
                     role='head_ssh_service'))
    partial.validate_successor(next_intent)

    wrong_role = _down_cursor('DELETE_INTENT', present, role='head_ssh_service')
    with pytest.raises(ValueError, match='first present role'):
        progress.ProviderDownProgressV1.from_value(wrong_role)

    erased_uid = _down_cursor('DELETE_PARTIAL', after_head_service)
    erased_uid['delete_target']['objects'][1]['expected_uid'] = None
    erased = progress.ProviderDownProgressV1.from_value(erased_uid)
    with pytest.raises(ValueError, match='immutable object commitment'):
        intent.validate_successor(erased)

    already_absent = ('absent_exact', 'absent_exact', 'absent_exact')
    no_delete_target = progress.ProviderDownProgressV1.from_value(
        _down_cursor('TARGET_RESOLVED', already_absent))
    direct_absence = progress.ProviderDownProgressV1.from_value(
        _down_cursor('ABSENCE_EXACT', already_absent))
    no_delete_target.validate_successor(direct_absence)


def test_delete_target_binds_observation_state_uid_and_hash() -> None:
    states = ('present_exact', 'absent_exact', 'present_exact')
    valid = _delete_target(states)
    parsed = progress.ProviderKubernetesDeleteTargetV1.from_value(valid)
    assert parsed.present_roles == (
        values.ProviderObjectRoleV1.HEAD_SSH_SERVICE,
        values.ProviderObjectRoleV1.HEAD_POD,
    )

    crossed_state = copy.deepcopy(valid)
    crossed_state['objects'][0]['state'] = 'absent_exact'
    with pytest.raises(ValueError, match='absent_exact.*exact NotFound'):
        progress.ProviderKubernetesDeleteTargetV1.from_value(crossed_state)

    crossed_uid = copy.deepcopy(valid)
    crossed_uid['objects'][2]['expected_uid'] = 'replacement-pod-uid'
    with pytest.raises(ValueError, match='present_exact.*exact present'):
        progress.ProviderKubernetesDeleteTargetV1.from_value(crossed_uid)

    crossed_hash = copy.deepcopy(valid)
    crossed_hash['objects'][0]['requested_semantic_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='role/hash differs'):
        progress.ProviderKubernetesDeleteTargetV1.from_value(crossed_hash)


def test_down_present_target_rejects_uncommitted_delete_uid() -> None:
    cursor = _down_cursor('TARGET_RESOLVED',
                          ('present_exact', 'present_exact', 'present_exact'))
    cursor['delete_target']['objects'][0]['expected_uid'] = None
    with pytest.raises(ValueError, match='present_exact delete target'):
        progress.ProviderDownProgressV1.from_value(cursor)


def test_down_partial_cleanup_commits_exact_read_uid_before_delete() -> None:
    cleanup = values.ProviderKubernetesCleanupTargetV1.from_value(
        down_config_fixtures._cleanup_target(
            basis_kind='partial_launch_cleanup',
            committed_count=0,
            exact_handle=False))
    target = values.ProviderLocatorV1.from_value(down_config_fixtures._target())
    observation = down_config_fixtures._prefix_observation_for_plans(
        3, complete=True)
    assert target.kubernetes is not None
    frozen_scope = target.kubernetes.scope.canonical_value()
    observation['evidence']['frozen_scope'] = copy.deepcopy(frozen_scope)
    observation['evidence']['observed_scope_before']['scope'] = copy.deepcopy(
        frozen_scope)
    observation['evidence']['observed_scope_after']['scope'] = copy.deepcopy(
        frozen_scope)
    observation['evidence_sha256'] = values.canonical_sha256(
        observation['evidence'])
    objects = []
    for sequence, (cleanup_object, observed) in enumerate(
            zip(cleanup.objects, observation['evidence']['objects'])):
        assert observed['uid'] is not None
        objects.append({
            'plan_sequence': sequence,
            'role': cleanup_object.role.value,
            'expected_uid': observed['uid'],
            'state': 'present_exact',
            'requested_semantic_sha256':
                cleanup_object.plan.requested_semantic_sha256,
        })
    cursor_value = {
        'version': 1,
        'action_kind': 'down',
        'phase': 'TARGET_RESOLVED',
        'delete_target': {
            'version': 1,
            'requested_target_sha256': target.sha256,
            'prior_launch_basis_sha256': 'f' * 64,
            'objects': objects,
            'observation': observation,
        },
    }
    cursor = progress.ProviderDownProgressV1.from_value(cursor_value)
    context = dataclasses.replace(_context(kernel.ActionKind.DOWN),
                                  requested_target=target,
                                  launch_object_commitments=tuple(
                                      (item.plan.request_body_sha256,
                                       item.plan.requested_semantic_sha256,
                                       item.plan.namespace, item.plan.name)
                                      for item in cleanup.objects),
                                  down_prior_launch_basis_sha256='f' * 64,
                                  down_cleanup_target=cleanup)
    cursor.validate_action_context(context)
    assert all(
        item.expected_uid is not None for item in cursor.delete_target.objects)

    intent_value = copy.deepcopy(cursor_value)
    intent_value['phase'] = 'DELETE_INTENT'
    intent_value['role'] = 'head_service'
    intent = progress.ProviderDownProgressV1.from_value(intent_value)
    intent.validate_action_context(context)
    cursor.validate_successor(intent)

    partial_value = copy.deepcopy(intent_value)
    partial_value['phase'] = 'DELETE_PARTIAL'
    partial_value.pop('role')
    partial_value['delete_target']['objects'][1]['state'] = 'absent_exact'
    after_delete = down_config_fixtures._prefix_observation_for_plans(
        0, complete=False)
    after_delete['state'] = 'uncertain'
    after_delete['evidence']['frozen_scope'] = copy.deepcopy(frozen_scope)
    after_delete['evidence']['observed_scope_before']['scope'] = copy.deepcopy(
        frozen_scope)
    after_delete['evidence']['observed_scope_after']['scope'] = copy.deepcopy(
        frozen_scope)
    after_delete['evidence']['objects'][0] = copy.deepcopy(
        observation['evidence']['objects'][0])
    after_delete['evidence']['objects'][2] = copy.deepcopy(
        observation['evidence']['objects'][2])
    after_delete['evidence_sha256'] = values.canonical_sha256(
        after_delete['evidence'])
    partial_value['delete_target']['observation'] = after_delete
    partial = progress.ProviderDownProgressV1.from_value(partial_value)
    partial.validate_action_context(context)
    intent.validate_successor(partial)
    assert partial.delete_target.objects[1].expected_uid == (
        cursor.delete_target.objects[1].expected_uid)

    missing_uid = copy.deepcopy(cursor_value)
    missing_uid['delete_target']['objects'][0]['expected_uid'] = None
    with pytest.raises(ValueError, match='present_exact delete target'):
        progress.ProviderDownProgressV1.from_value(missing_uid)

    known_cleanup_value = down_config_fixtures._cleanup_target(
        basis_kind='partial_launch_cleanup',
        committed_count=1,
        exact_handle=False)
    known_cleanup_value['objects'][0][
        'committed_uid'] = 'different-committed-uid'
    known_cleanup = values.ProviderKubernetesCleanupTargetV1.from_value(
        known_cleanup_value)
    crossed_context = dataclasses.replace(context,
                                          down_cleanup_target=known_cleanup)
    with pytest.raises(ValueError, match='committed or exact-read'):
        cursor.validate_action_context(crossed_context)


def test_api006_fresh_transition_binds_exact_claim_and_fence(
        monkeypatch) -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    attempt = _attempt_record(
        None,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED)
    worker_id = uuid.UUID('55555555-5555-4555-8555-555555555555')
    attestation = _attestation(worker_id=worker_id)
    claim = _claim(worker_id=worker_id)
    cursor = {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'CREATE_INTENT',
        'role': 'head_ssh_service',
        'intent_origin': claim,
        'committed_effects': [],
        'known_objects': _partial_target(0),
        'pre_observation': _prefix_observation(0),
    }
    proposed = _envelope(cursor, attestation)
    fence = kernel.AttemptExecutionFence(
        request_id=attempt.request_id,
        execution_generation=1,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=worker_id,
        controller_generation=1)

    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, attempt, fence, proposed)

    wrong_claim = copy.deepcopy(proposed)
    wrong_claim['cursor']['intent_origin'] = _claim(generation=2)
    with pytest.raises(ValueError, match='intent origin|envelope attestation'):
        progress.ServeProviderProgressContractV1.validate_progress_transition(
            action, None, attempt, fence, wrong_claim)


def test_retry_seed_is_byte_exact_and_crossed_predecessor_rejects(
        monkeypatch) -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    final = _envelope(_launch_cursor('HANDLE_COMMITTED', claim=claim),
                      _attestation(claimed_cursor_sha256='e' * 64))
    predecessor = _settled_handler_attempt(
        _attempt_record(
            final,
            mutation_boundary=kernel.MutationBoundary.SUBMITTED_OR_AMBIGUOUS,
            provider_io_boundary=kernel.ProviderIOBoundary.
            SUBMITTED_OR_AMBIGUOUS,
            revision=7),
        _provider_result('retryable',
                         'unknown',
                         retry_class='transient',
                         retry_after_seconds=60))
    seed = progress.ServeProviderProgressContractV1.retry_seed(
        action, None, predecessor)
    assert seed is not None
    assert seed['cursor'] == final['cursor']
    assert seed['worker_attestation'] is None

    successor = _attempt_record(
        seed,
        attempt=2,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED,
        revision=1)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, predecessor, successor, None)

    cursor_sha256 = values.canonical_sha256(seed['cursor'])
    inherited_bound = _envelope(
        copy.deepcopy(seed['cursor']),
        _attestation(attempt=2, claimed_cursor_sha256=cursor_sha256))
    inherited_attempt = _attempt_record(
        inherited_bound,
        attempt=2,
        mutation_boundary=kernel.MutationBoundary.INTENT_COMMITTED,
        provider_io_boundary=kernel.ProviderIOBoundary.INTENT_COMMITTED,
        revision=2)
    inherited_settled = _settled_handler_attempt(
        inherited_attempt,
        _provider_result('retryable',
                         'unknown',
                         retry_class='transient',
                         retry_after_seconds=60))
    third_seed = progress.ServeProviderProgressContractV1.retry_seed(
        action, predecessor, inherited_settled)
    assert third_seed is not None
    assert third_seed['cursor'] == seed['cursor']
    assert third_seed['worker_attestation'] is None

    crossed_watermark = dataclasses.replace(
        predecessor,
        provider_io_boundary=kernel.ProviderIOBoundary.INTENT_COMMITTED)
    with pytest.raises(ValueError, match='not admitted'):
        progress.ServeProviderProgressContractV1.retry_seed(
            action, None, crossed_watermark)

    crossed = copy.deepcopy(seed)
    crossed['cursor'] = _launch_cursor('HANDLE_INTENT', claim=claim)
    crossed_attempt = _attempt_record(
        crossed,
        attempt=2,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED,
        revision=1)
    with pytest.raises(ValueError, match='inherited progress differs'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, predecessor, crossed_attempt, None)


def test_inherited_intent_rebinds_envelope_without_relabeling_origin(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = dataclasses.replace(_action_record(kernel.ActionKind.LAUNCH),
                                 current_attempt=2)
    origin_attestation = _attestation(attempt=1, claimed_cursor_sha256='e' * 64)
    origin = _claim_from_attestation(origin_attestation, attempt=1)
    cursor = _launch_cursor('HANDLE_INTENT', claim=origin)
    predecessor = _attempt_record(
        _envelope(cursor, origin_attestation),
        mutation_boundary=kernel.MutationBoundary.SETTLED,
        provider_io_boundary=kernel.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS,
        revision=7)
    seed = _envelope(copy.deepcopy(cursor), None)
    attempt = _attempt_record(
        seed,
        attempt=2,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED,
        revision=1)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, predecessor, attempt, None)

    cursor_sha256 = values.canonical_sha256(cursor)
    current_attestation = _attestation(attempt=2,
                                       claimed_cursor_sha256=cursor_sha256)
    proposed = _envelope(copy.deepcopy(cursor), current_attestation)
    worker_id = uuid.UUID('55555555-5555-4555-8555-555555555555')
    fence = kernel.AttemptExecutionFence(
        request_id=attempt.request_id,
        execution_generation=1,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=worker_id,
        controller_generation=1)
    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, predecessor, attempt, fence, proposed)
    assert proposed['cursor']['intent_origin'] == origin


def test_retry_seed_requires_r_u_p0_or_o_and_exact_journal(monkeypatch) -> None:
    launch_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: launch_context))
    launch_action = _action_record(kernel.ActionKind.LAUNCH)

    p0_attempt = _attempt_record(None)
    p0_context = _fallback_context(kernel.ActionKind.LAUNCH, p0_attempt,
                                   requests_lib.RequestStatus.FAILED)
    p0_reduction = progress.reduce_request_terminal_fallback_v1(
        launch_action, None, p0_attempt, p0_context)
    p0_settled = _settled_attempt(p0_attempt,
                                  p0_reduction,
                                  p0_context.database_now,
                                  request_terminal_state='FAILED')
    assert progress.ServeProviderProgressContractV1.retry_seed(
        launch_action, None, p0_settled) is None

    claim = _claim(claimed_cursor_sha256='e' * 64)
    intent_progress = _envelope(_launch_cursor('HANDLE_INTENT', claim=claim),
                                claim['worker_attestation'])
    intent_attempt = _attempt_record(intent_progress, revision=7)
    u_settled = _settled_handler_attempt(
        intent_attempt,
        _provider_result('uncertain',
                         'unknown',
                         retry_class='observation_required',
                         retry_after_seconds=60))
    u_seed = progress.ServeProviderProgressContractV1.retry_seed(
        launch_action, None, u_settled)
    assert u_seed is not None and u_seed['worker_attestation'] is None

    for unauthorized_result, match in ((_provider_result(
            'terminal_error',
            'unknown'), 'does not authorize'), (_provider_result(
                'cancelled', 'observed'), 'quiescence')):
        unauthorized = _settled_handler_attempt(intent_attempt,
                                                unauthorized_result)
        with pytest.raises(ValueError, match=match):
            progress.ServeProviderProgressContractV1.retry_seed(
                launch_action, None, unauthorized)

    malformed = dataclasses.replace(
        u_settled,
        typed_outcome={'version': 1},
        typed_outcome_sha256=values.canonical_sha256({'version': 1}))
    with pytest.raises(ValueError, match='unknown or missing|closed basis'):
        progress.ServeProviderProgressContractV1.retry_seed(
            launch_action, None, malformed)

    crossed_revision = dataclasses.replace(u_settled,
                                           provider_progress_revision=0)
    with pytest.raises(ValueError, match='hash/revision'):
        progress.ServeProviderProgressContractV1.retry_seed(
            launch_action, None, crossed_revision)

    down_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: down_context))
    down_action = _action_record(kernel.ActionKind.DOWN)
    states = ('present_exact', 'present_exact', 'present_exact')
    o_attempt = _attempt_record(_envelope(
        _down_cursor('TARGET_RESOLVED', states), _attestation()),
                                revision=3)
    o_context = _fallback_context(kernel.ActionKind.DOWN, o_attempt,
                                  requests_lib.RequestStatus.CANCELLED)
    o_reduction = progress.reduce_request_terminal_fallback_v1(
        down_action, None, o_attempt, o_context)
    o_settled = _settled_attempt(o_attempt,
                                 o_reduction,
                                 o_context.database_now,
                                 request_terminal_state='CANCELLED')
    o_seed = progress.ServeProviderProgressContractV1.retry_seed(
        down_action, None, o_settled)
    assert o_seed is not None
    assert o_seed['cursor'] == o_attempt.provider_progress['cursor']
    assert o_seed['worker_attestation'] is None


def test_intent_envelope_allows_newer_execution_but_rejects_after_erasure(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    initial_attestation = _attestation(claimed_cursor_sha256='e' * 64)
    origin = _claim_from_attestation(initial_attestation)
    cursor = _launch_cursor('HANDLE_INTENT', claim=origin)
    current = _envelope(cursor, initial_attestation)
    attempt = _attempt_record(current, revision=7)
    cursor_sha256 = values.canonical_sha256(cursor)
    replacement_attestation = _attestation(generation=2,
                                           claimed_cursor_sha256=cursor_sha256)
    fence = kernel.AttemptExecutionFence(
        request_id=attempt.request_id,
        execution_generation=2,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=uuid.UUID('55555555-5555-4555-8555-555555555555'),
        controller_generation=1)
    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, attempt, fence,
        _envelope(copy.deepcopy(cursor), replacement_attestation))

    completed_attestation = _attestation(after=True,
                                         claimed_cursor_sha256='e' * 64)
    completed_origin = _claim_from_attestation(completed_attestation)
    completed_cursor = _launch_cursor('HANDLE_INTENT', claim=completed_origin)
    erased = copy.deepcopy(completed_attestation)
    erased['after'] = None
    with pytest.raises(ValueError, match='envelope attestation'):
        progress.ProviderLifecycleProgressV1.from_value(
            _envelope(completed_cursor, erased))

    completed_current = _envelope(completed_cursor, completed_attestation)
    completed_attempt = _attempt_record(completed_current, revision=8)
    completed_cursor_sha256 = values.canonical_sha256(completed_cursor)
    replacement_after_completion = _attestation(
        generation=2, claimed_cursor_sha256=completed_cursor_sha256)
    replacement_fence = kernel.AttemptExecutionFence(
        request_id=completed_attempt.request_id,
        execution_generation=2,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=uuid.UUID('55555555-5555-4555-8555-555555555555'),
        controller_generation=1)
    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, completed_attempt, replacement_fence,
        _envelope(copy.deepcopy(completed_cursor),
                  replacement_after_completion))


def test_api006_replacement_execution_generation_must_increase(
        monkeypatch) -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(generation=2, claimed_cursor_sha256='e' * 64)
    cursor = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    current = _envelope(
        cursor, _attestation(generation=2, claimed_cursor_sha256='e' * 64))
    attempt = _attempt_record(current, revision=8)
    proposed = _envelope(
        cursor,
        _attestation(generation=1,
                     claimed_cursor_sha256=values.canonical_sha256(cursor)))
    fence = kernel.AttemptExecutionFence(
        request_id=attempt.request_id,
        execution_generation=1,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=uuid.UUID('55555555-5555-4555-8555-555555555555'),
        controller_generation=1)

    with pytest.raises(ValueError, match='strictly newer'):
        progress.ServeProviderProgressContractV1.validate_progress_transition(
            action, None, attempt, fence, proposed)


def test_api006_watermark_cursor_phase_matrix_rejects_crosses(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    cursor = _launch_cursor('HANDLE_INTENT')
    envelope = _envelope(cursor, cursor['intent_origin']['worker_attestation'])
    crossed = _attempt_record(
        envelope,
        provider_io_boundary=kernel.ProviderIOBoundary.INTENT_COMMITTED,
        mutation_boundary=kernel.MutationBoundary.INTENT_COMMITTED,
        revision=7)
    with pytest.raises(ValueError, match='not admitted'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed, None)

    submitted = dataclasses.replace(
        crossed,
        provider_io_boundary=(kernel.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS),
        mutation_boundary=kernel.MutationBoundary.SUBMITTED_OR_AMBIGUOUS)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, submitted, None)
    crossed_mutation = dataclasses.replace(
        submitted, mutation_boundary=kernel.MutationBoundary.INTENT_COMMITTED)
    with pytest.raises(ValueError, match='boundaries are crossed'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_mutation, None)


def test_api006_down_checkpoints_pre_submission_intents_before_io(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.DOWN)
    worker_id = uuid.UUID('55555555-5555-4555-8555-555555555555')
    attestation = _attestation(worker_id=worker_id)
    fence = kernel.AttemptExecutionFence(
        request_id=kernel.request_id_for_attempt(_ACTION_ID, 1),
        execution_generation=1,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=worker_id,
        controller_generation=1)
    present = ('present_exact', 'present_exact', 'present_exact')
    target = _envelope(_down_cursor('TARGET_RESOLVED', present), attestation)
    attempt = _attempt_record(
        target,
        mutation_boundary=kernel.MutationBoundary.INTENT_COMMITTED,
        provider_io_boundary=kernel.ProviderIOBoundary.INTENT_COMMITTED,
        revision=1)
    delete_intent = _envelope(
        _down_cursor('DELETE_INTENT', present, role='head_service'),
        attestation)

    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, attempt, fence, delete_intent)
    intent_attempt = dataclasses.replace(
        attempt,
        provider_progress=delete_intent,
        provider_progress_sha256=values.canonical_sha256(delete_intent),
        provider_progress_revision=2)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, intent_attempt, fence)

    post_delete = _envelope(
        _down_cursor('DELETE_PARTIAL',
                     ('present_exact', 'absent_exact', 'present_exact')),
        attestation)
    with pytest.raises(ValueError, match='SUBMITTED_OR_AMBIGUOUS'):
        progress.ServeProviderProgressContractV1.validate_progress_transition(
            action, None, intent_attempt, fence, post_delete)

    absent = ('absent_exact', 'absent_exact', 'absent_exact')
    absent_target = _envelope(_down_cursor('TARGET_RESOLVED', absent),
                              attestation)
    absent_attempt = dataclasses.replace(
        attempt,
        provider_progress=absent_target,
        provider_progress_sha256=values.canonical_sha256(absent_target))
    absence_exact = _envelope(_down_cursor('ABSENCE_EXACT', absent),
                              attestation)
    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, absent_attempt, fence, absence_exact)
    absence_attempt = dataclasses.replace(
        absent_attempt,
        provider_progress=absence_exact,
        provider_progress_sha256=values.canonical_sha256(absence_exact),
        provider_progress_revision=2)
    handle_intent_cursor = _down_cursor('HANDLE_REMOVE_INTENT', absent)
    handle_intent_cursor['expected_handle'] = None
    progress.ServeProviderProgressContractV1.validate_progress_transition(
        action, None, absence_attempt, fence,
        _envelope(handle_intent_cursor, attestation))


def test_quiescence_effect_sequence_rejects_boolean() -> None:
    raw = progress.ProviderLaunchEffectQuiescenceV1.from_committed(
        progress.ProviderLaunchCommittedEffectEvidenceV1.from_value(
            _create_effect(0))).canonical_value()
    raw['effect_sequence'] = True
    with pytest.raises(ValueError, match='nonnegative'):
        progress.ProviderLaunchEffectQuiescenceV1.from_value(raw)


def _call_not_entered(cursor: dict, claim: dict, sequence: int) -> dict:
    role = _ROLE_VALUES[sequence][0] if sequence < 3 else None
    effect_kind = ('core_v1_create' if sequence < 3 else 'cluster_record_insert'
                   if sequence == 3 else 'skylet_job_submit')
    intent_phase = ('CREATE_INTENT' if sequence < 3 else
                    'HANDLE_INTENT' if sequence == 3 else 'JOB_INTENT')
    return {
        'version': 1,
        'effect_sequence': sequence,
        'effect_kind': effect_kind,
        'role': role,
        'intent_phase': intent_phase,
        'intent_cursor_sha256': values.canonical_sha256(cursor),
        'intent_origin': claim,
        'resolution_origin': claim,
        'resolution': 'call_not_entered',
        'evidence_sha256': None,
        'definitive_no_effect': None,
    }


def _create_intent_cursor(sequence: int, claim: dict) -> dict:
    return {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'CREATE_INTENT',
        'role': _ROLE_VALUES[sequence][0],
        'intent_origin': claim,
        'committed_effects': [
            _create_effect(item, claim) for item in range(sequence)
        ],
        'known_objects': _partial_target(sequence),
        'pre_observation': _prefix_observation(sequence),
    }


def _core_v1_no_effect(cursor: dict, claim: dict, sequence: int,
                       observation: dict) -> dict:
    proof = {
        'version': 1,
        'proof_kind': 'core_v1_422_no_create',
        'request_body_sha256': chr(ord('d') + sequence) * 64,
        'response_status': 422,
        'post_observation': observation,
    }
    resolution = _call_not_entered(cursor, claim, sequence)
    resolution.update({
        'resolution': 'definitive_no_effect',
        'evidence_sha256': values.canonical_sha256(proof),
        'definitive_no_effect': proof,
    })
    return resolution


def _definitive_no_effect(cursor: dict, claim: dict) -> dict:
    intended_handle = cursor['intended_handle']
    proof = {
        'version': 1,
        'proof_kind': 'cluster_record_no_commit',
        'intended_handle_sha256': values.canonical_sha256(intended_handle),
        'transaction_result': 'rolled_back',
        'cluster_name': intended_handle['cluster_name'],
        'expected_cluster_record_uuid': intended_handle['cluster_record_uuid'],
        'post_read_disposition': 'not_found',
        'observed_cluster_record_uuid': None,
        'observed_handle': None,
        'observed_at': _TIME,
    }
    resolution = _call_not_entered(cursor, claim, 3)
    resolution.update({
        'resolution': 'definitive_no_effect',
        'evidence_sha256': values.canonical_sha256(proof),
        'definitive_no_effect': proof,
    })
    return resolution


def _skylet_no_effect(cursor: dict, claim: dict, post_job: dict,
                      rejection: str) -> dict:
    submit_request = cursor['submit_request']
    proof = {
        'version': 1,
        'proof_kind': 'skylet_rejected_before_job_commit',
        'submit_request_sha256': values.canonical_sha256(submit_request),
        'rejection': rejection,
        'post_job': post_job,
        'pending_start_outbox': False,
        'active_run_token': False,
    }
    resolution = _call_not_entered(cursor, claim, 4)
    resolution.update({
        'resolution': 'definitive_no_effect',
        'evidence_sha256': values.canonical_sha256(proof),
        'definitive_no_effect': proof,
    })
    return resolution


@pytest.mark.parametrize('sequence', [0, 1, 2])
def test_core_v1_422_no_effect_accepts_only_literal_prefix_matrix(
        sequence: int) -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor_value = _create_intent_cursor(sequence, claim)
    cursor = progress.ProviderLaunchProgressV1.from_value(cursor_value)
    resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _core_v1_no_effect(cursor_value, claim, sequence,
                           _prefix_observation(sequence)))
    resolution.validate_cursor(cursor, _ACTION_ID, 1,
                               _context(kernel.ActionKind.LAUNCH))


def test_core_v1_422_no_effect_rejects_crossed_observation_evidence() -> None:
    sequence = 1
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor_value = _create_intent_cursor(sequence, claim)
    cursor = progress.ProviderLaunchProgressV1.from_value(cursor_value)
    context = _context(kernel.ActionKind.LAUNCH)

    wrong_vector = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _core_v1_no_effect(cursor_value, claim, sequence,
                           _prefix_observation(0)))
    with pytest.raises(ValueError, match='literal.*observation matrix'):
        wrong_vector.validate_cursor(cursor, _ACTION_ID, 1, context)

    wrong_state_observation = _prefix_observation(sequence)
    wrong_state_observation['state'] = 'conflict'
    wrong_state = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _core_v1_no_effect(cursor_value, claim, sequence,
                           wrong_state_observation))
    with pytest.raises(ValueError, match='literal.*observation matrix'):
        wrong_state.validate_cursor(cursor, _ACTION_ID, 1, context)

    conflict_observation = _prefix_observation(sequence)
    conflict_observation['state'] = 'conflict'
    conflict_observation['evidence']['objects'][0]['spec_match'] = False
    conflict_observation['evidence_sha256'] = values.canonical_sha256(
        conflict_observation['evidence'])
    conflict = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _core_v1_no_effect(cursor_value, claim, sequence, conflict_observation))
    with pytest.raises(ValueError, match='literal.*observation matrix'):
        conflict.validate_cursor(cursor, _ACTION_ID, 1, context)

    uncertain_observation = _observation(dispositions=('present', 'uncertain',
                                                       'not_found'))
    uncertain = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _core_v1_no_effect(cursor_value, claim, sequence,
                           uncertain_observation))
    with pytest.raises(ValueError, match='literal.*observation matrix'):
        uncertain.validate_cursor(cursor, _ACTION_ID, 1, context)

    malformed_not_found = _prefix_observation(sequence)
    malformed_not_found['evidence']['objects'][1]['uid'] = 'unexpected-uid'
    malformed_not_found['evidence_sha256'] = values.canonical_sha256(
        malformed_not_found['evidence'])
    with pytest.raises(ValueError, match='not-found.*response-derived'):
        progress.ProviderLaunchNoEffectResolutionV1.from_value(
            _core_v1_no_effect(cursor_value, claim, sequence,
                               malformed_not_found))


def test_core_v1_422_no_effect_rejects_prior_present_drift() -> None:
    sequence = 2
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor_value = _create_intent_cursor(sequence, claim)
    cursor = progress.ProviderLaunchProgressV1.from_value(cursor_value)
    context = _context(kernel.ActionKind.LAUNCH)
    base = _prefix_observation(sequence)

    crossed_uid = copy.deepcopy(base)
    crossed_uid['evidence']['objects'][0]['uid'] = 'replacement-service-uid'
    crossed_uid['evidence_sha256'] = values.canonical_sha256(
        crossed_uid['evidence'])
    crossed_allocation = copy.deepcopy(base)
    crossed_allocation['evidence']['objects'][0]['server_allocations'][0][
        'value'] = '10.0.0.9'
    crossed_allocation['evidence_sha256'] = values.canonical_sha256(
        crossed_allocation['evidence'])

    for observation in (crossed_uid, crossed_allocation):
        resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
            _core_v1_no_effect(cursor_value, claim, sequence, observation))
        with pytest.raises(ValueError, match='prior present observation'):
            resolution.validate_cursor(cursor, _ACTION_ID, 1, context)


def test_skylet_n4_no_effect_binds_exact_request_action_and_runtime() -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor_value = _advanced_launch_cursor('JOB_INTENT', claim=claim)
    cursor = progress.ProviderLaunchProgressV1.from_value(cursor_value)
    context = _context(kernel.ActionKind.LAUNCH)

    schema_post_job = _job_evidence('not_found')
    schema_resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _skylet_no_effect(cursor_value, claim, schema_post_job,
                          'schema_rejected'))
    schema_resolution.validate_cursor(cursor, _ACTION_ID, 1, context)

    retained = _submit_request()
    retained['job_contract_sha256'] = 'c' * 64
    conflict_post_job = _job_evidence('conflict',
                                      durable_state='SUCCEEDED',
                                      retained_submit_request=retained)
    conflict_resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _skylet_no_effect(cursor_value, claim, conflict_post_job,
                          'same_key_different_spec'))
    conflict_resolution.validate_cursor(cursor, _ACTION_ID, 1, context)

    mutators = (
        lambda job: job.update(
            {'submission_key': '77777777-7777-4777-8777-777777777777'}),
        lambda job: job.update({'job_contract_sha256': 'd' * 64}),
        lambda job: job.update({'job_spec_sha256': 'e' * 64}),
        lambda job: job.update(
            {'state_store_uuid': '88888888-8888-4888-8888-888888888888'}),
    )
    for mutate in mutators:
        crossed_job = copy.deepcopy(schema_post_job)
        mutate(crossed_job)
        crossed = progress.ProviderLaunchNoEffectResolutionV1.from_value(
            _skylet_no_effect(cursor_value, claim, crossed_job,
                              'schema_rejected'))
        with pytest.raises(ValueError, match='request/action/runtime binding'):
            crossed.validate_cursor(cursor, _ACTION_ID, 1, context)


def test_skylet_n4_same_key_conflict_requires_byte_different_request() -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor_value = _advanced_launch_cursor('JOB_INTENT', claim=claim)
    cursor = progress.ProviderLaunchProgressV1.from_value(cursor_value)
    equal_post_job = _job_evidence('conflict', durable_state='SUCCEEDED')
    resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _skylet_no_effect(cursor_value, claim, equal_post_job,
                          'same_key_different_spec'))
    with pytest.raises(ValueError, match='exact expected submit request'):
        resolution.validate_cursor(cursor, _ACTION_ID, 1,
                                   _context(kernel.ActionKind.LAUNCH))


def test_reducer_builds_exact_e_only_and_e_plus_n_quiescence(
        monkeypatch) -> None:
    context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    attestation = _attestation(claimed_cursor_sha256='e' * 64)

    committed_cursor = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    committed_attempt = _attempt_record(_envelope(committed_cursor,
                                                  attestation),
                                        revision=8)
    quiescence = progress.build_launch_supersession_quiescence_v1(
        action,
        committed_attempt,
        request_terminal_state='SUCCEEDED',
        active_claim=False,
        handler_terminal_result_sha256='a' * 64,
        request_settled_at=_TIME,
        launch_no_effect_resolution=None)
    assert [
        entry.canonical_value()['resolution'] for entry in quiescence.effects
    ] == ['evidence_committed'] * 4
    assert quiescence.canonical_value()['launch_provider_cursor_sha256'] == (
        values.canonical_sha256(committed_cursor))

    intent_cursor = _launch_cursor('HANDLE_INTENT', claim=claim)
    intent_attempt = _attempt_record(_envelope(intent_cursor, attestation),
                                     revision=7)
    resolution = _call_not_entered(intent_cursor, claim, 3)
    intent_quiescence = progress.build_launch_supersession_quiescence_v1(
        action,
        intent_attempt,
        request_terminal_state='SUCCEEDED',
        active_claim=False,
        handler_terminal_result_sha256='b' * 64,
        request_settled_at=_TIME,
        launch_no_effect_resolution=resolution)
    assert [
        entry.canonical_value()['resolution']
        for entry in intent_quiescence.effects
    ] == [
        'evidence_committed', 'evidence_committed', 'evidence_committed',
        'call_not_entered'
    ]
    with pytest.raises(ValueError, match='requires its exact'):
        progress.build_launch_supersession_quiescence_v1(
            action,
            intent_attempt,
            request_terminal_state='SUCCEEDED',
            active_claim=False,
            handler_terminal_result_sha256='b' * 64,
            request_settled_at=_TIME,
            launch_no_effect_resolution=None)
    with pytest.raises(ValueError, match='E-only'):
        progress.build_launch_supersession_quiescence_v1(
            action,
            committed_attempt,
            request_terminal_state='SUCCEEDED',
            active_claim=False,
            handler_terminal_result_sha256='b' * 64,
            request_settled_at=_TIME,
            launch_no_effect_resolution=resolution)


def test_persisted_timestamp_boundaries_reject_subclasses(monkeypatch) -> None:

    class _AdversarialDatetime(datetime.datetime):
        method_called = False

        def utcoffset(self):
            type(self).method_called = True
            raise AssertionError('subclass method must not be called')

        def astimezone(self, tz=None):
            type(self).method_called = True
            raise AssertionError('subclass method must not be called')

    class _TimestampTextSubclass(str):
        pass

    adversarial = _AdversarialDatetime(2026, 8, 2, tzinfo=datetime.timezone.utc)
    with pytest.raises(TypeError, match='exact datetime'):
        progress._timestamp_from_datetime(adversarial,
                                          name='adversarial timestamp')
    assert not _AdversarialDatetime.method_called

    context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    attempt = _attempt_record(_envelope(
        cursor, _attestation(claimed_cursor_sha256='e' * 64)),
                              revision=8)
    for invalid in (adversarial, _TimestampTextSubclass(_TIME), object()):
        with pytest.raises(TypeError, match='exact datetime or str'):
            progress.build_launch_supersession_quiescence_v1(
                action,
                attempt,
                request_terminal_state='SUCCEEDED',
                active_claim=False,
                handler_terminal_result_sha256='a' * 64,
                request_settled_at=invalid,
                launch_no_effect_resolution=None)
    assert not _AdversarialDatetime.method_called


def test_no_effect_resolution_rejects_wrong_cursor_hash_and_retry_claim(
) -> None:
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor = _launch_cursor('HANDLE_INTENT', claim=claim)
    resolution = _call_not_entered(cursor, claim, 3)
    resolution['intent_cursor_sha256'] = '0' * 64
    parsed = progress.ProviderLaunchNoEffectResolutionV1.from_value(resolution)
    parsed_cursor = progress.ProviderLaunchProgressV1.from_value(cursor)
    with pytest.raises(ValueError, match='exact current intent'):
        parsed.validate_cursor(parsed_cursor, _ACTION_ID, 1)

    inherited_claim = _claim(attempt=1,
                             generation=2,
                             claimed_cursor_sha256='e' * 64)
    inherited_cursor = _launch_cursor('HANDLE_INTENT', claim=inherited_claim)
    inherited_resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        _call_not_entered(inherited_cursor, inherited_claim, 3))
    with pytest.raises(ValueError, match='retry-local'):
        inherited_resolution.validate_cursor(
            progress.ProviderLaunchProgressV1.from_value(inherited_cursor),
            _ACTION_ID, 2)


def test_progress_rejects_unknown_keys_noncanonical_hashes_and_oversize(
) -> None:
    cursor = _launch_cursor('HANDLE_COMMITTED')
    envelope = _envelope(cursor, _attestation(claimed_cursor_sha256='e' * 64))
    envelope['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        progress.ProviderLifecycleProgressV1.from_value(envelope)

    bad_origin = _claim()
    bad_origin['worker_attestation_sha256'] = 'A' * 64
    with pytest.raises(ValueError, match='lowercase SHA-256'):
        progress.ProviderLaunchEffectClaimV1.from_value(bad_origin)

    oversize = _attestation()
    oversize['request_worker_id'] = 'x' * 65_536
    with pytest.raises(ValueError, match='exceeds 65536 bytes|1..1024'):
        progress.ProviderAuthorityWorkerAttemptAttestationV1.from_value(
            oversize)


def test_revision_zero_r_reduction_uses_database_clock_and_replays_projection(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    attempt = _attempt_record(
        None,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED)
    error = _provider_error(
        'transient',
        provider_code='TemporarilyUnavailable',
        retry_after_seconds=17,
        normalized_message='provider temporarily unavailable')
    expected_result = _provider_result(
        'retryable',
        'unknown',
        provider_code='TemporarilyUnavailable',
        retry_class='transient',
        retry_after_seconds=17,
        normalized_message='provider temporarily unavailable')
    request_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                      attempt,
                                      expected_result,
                                      normalized_provider_error=error)
    request_finished_at = datetime.datetime(2026,
                                            8,
                                            1,
                                            1,
                                            2,
                                            3,
                                            4,
                                            tzinfo=datetime.timezone.utc)
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    reduction_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                           request_return,
                                           finished_at=request_finished_at,
                                           database_now=database_now)

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is kernel.KernelState.READY
    assert reduction.retry_after_seconds == 17
    assert reduction.terminal_disposition is None
    assert reduction.typed_outcome == reduction.result
    assert reduction.typed_outcome['provider_result'] == expected_result
    current = reduction.typed_outcome['launch_no_io_prefix']['current_attempt']
    assert current['settled_at'] == '2026-08-02T04:05:06.000007Z'
    assert current['settled_at'] != _TIME
    progress.ServeProviderProgressContractV1.validate_reduction(
        action, None, attempt, reduction, reduction_context)
    with pytest.raises(ValueError, match='exact handler-result reduction'):
        progress.ServeProviderProgressContractV1.validate_reduction(
            action, None, attempt,
            dataclasses.replace(reduction, retry_after_seconds=18),
            reduction_context)

    # After the first transaction the retained projection is authority: replay
    # needs no request row or duplicate terminal worker-attestation preimage.
    settled = _settled_attempt(attempt, reduction, database_now)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, settled, None)

    wrong_operation = copy.deepcopy(settled.typed_outcome)
    wrong_operation['provider_result']['provider_operation_id'] = 'foreign-op'
    crossed_operation = dataclasses.replace(
        settled,
        typed_outcome=wrong_operation,
        typed_outcome_sha256=values.canonical_sha256(wrong_operation))
    with pytest.raises(ValueError, match='operation ID differs'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_operation, None)

    wrong_prefix = copy.deepcopy(settled.typed_outcome)
    prefix = wrong_prefix['launch_no_io_prefix']
    prefix['current_attempt']['request_input_sha256'] = '2' * 64
    prefix['prefix_sha256'] = values.canonical_sha256({
        'previous_prefix_sha256': prefix['previous_prefix_sha256'],
        'current_attempt': prefix['current_attempt'],
    })
    crossed_prefix = dataclasses.replace(
        settled,
        typed_outcome=wrong_prefix,
        typed_outcome_sha256=values.canonical_sha256(wrong_prefix))
    with pytest.raises(ValueError, match='projection differs'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_prefix, None)

    forged_cancelled = copy.deepcopy(settled.typed_outcome)
    forged_cancelled['provider_result'] = _provider_result(
        'cancelled', 'observed')
    crossed_cancelled = dataclasses.replace(
        settled,
        typed_outcome=forged_cancelled,
        typed_outcome_sha256=values.canonical_sha256(forged_cancelled))
    with pytest.raises(ValueError, match='cancelled outcome lacks'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_cancelled, None)


@pytest.mark.parametrize(
    ('category', 'expected_state', 'expected_disposition',
     'expected_retry_class', 'expected_delay'), [
         ('transient', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('capacity', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('quota', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('rate_limited', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('unknown', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('invalid_request', kernel.KernelState.BLOCKED, 'terminal_error', None,
          None),
         ('permission', kernel.KernelState.BLOCKED, 'terminal_error', None,
          None),
         ('conflict', kernel.KernelState.BLOCKED, 'terminal_error', None, None),
     ])
def test_current_launch_intent_reduces_to_exact_u_or_b(
        monkeypatch, category: str, expected_state: kernel.KernelState,
        expected_disposition: str, expected_retry_class: str | None,
        expected_delay: int | None) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    attestation = copy.deepcopy(claim['worker_attestation'])
    cursor = _launch_cursor('HANDLE_INTENT', claim=claim)
    attempt = _attempt_record(_envelope(cursor, attestation), revision=7)
    error = _provider_error(category,
                            provider_code='ProviderRejected',
                            retry_after_seconds=7,
                            normalized_message='normalized rejection')
    expected_result = _provider_result(
        expected_disposition,
        'unknown',
        provider_code='ProviderRejected',
        retry_class=expected_retry_class,
        retry_after_seconds=expected_delay,
        normalized_message='normalized rejection')
    request_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                      attempt,
                                      expected_result,
                                      normalized_provider_error=error)
    reduction_context = _reduction_context(
        kernel.ActionKind.LAUNCH,
        request_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is expected_state
    assert reduction.retry_after_seconds == expected_delay
    assert reduction.typed_outcome['provider_result'] == expected_result
    assert reduction.typed_outcome['launch_no_io_prefix'] is None
    assert reduction.typed_outcome['supersession_quiescence'] is None


@pytest.mark.parametrize(
    ('category', 'expected_state', 'expected_disposition',
     'expected_retry_class', 'expected_delay'), [
         ('transient', kernel.KernelState.READY, 'retryable', 'transient', 7),
         ('capacity', kernel.KernelState.READY, 'retryable', 'capacity', 7),
         ('quota', kernel.KernelState.READY, 'retryable', 'quota', 7),
         ('rate_limited', kernel.KernelState.READY, 'retryable', 'rate_limited',
          7),
         ('unknown', kernel.KernelState.READY, 'uncertain',
          'observation_required', 60),
         ('invalid_request', kernel.KernelState.BLOCKED, 'terminal_error', None,
          None),
         ('permission', kernel.KernelState.BLOCKED, 'terminal_error', None,
          None),
         ('conflict', kernel.KernelState.BLOCKED, 'terminal_error', None, None),
     ])
def test_nonintent_down_reduces_every_error_category_to_exact_r_u_or_b(
        monkeypatch, category: str, expected_state: kernel.KernelState,
        expected_disposition: str, expected_retry_class: str | None,
        expected_delay: int | None) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.DOWN)
    states = ('present_exact', 'present_exact', 'present_exact')
    attempt = _attempt_record(_envelope(_down_cursor('TARGET_RESOLVED', states),
                                        _attestation()),
                              revision=3)
    error = _provider_error(category,
                            provider_code='ProviderRejected',
                            retry_after_seconds=7,
                            normalized_message='normalized rejection')
    expected_result = _provider_result(
        expected_disposition,
        'unknown',
        provider_code='ProviderRejected',
        retry_class=expected_retry_class,
        retry_after_seconds=expected_delay,
        normalized_message='normalized rejection')
    request_return = _terminal_return(kernel.ActionKind.DOWN,
                                      attempt,
                                      expected_result,
                                      normalized_provider_error=error)
    reduction_context = _reduction_context(
        kernel.ActionKind.DOWN,
        request_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is expected_state
    assert reduction.retry_after_seconds == expected_delay
    assert reduction.typed_outcome['provider_result'] == expected_result


@pytest.mark.parametrize(
    ('phase', 'role', 'expected_disposition', 'expected_retry_class',
     'expected_delay'), [
         ('TARGET_RESOLVED', None, 'retryable', 'transient', 7),
         ('DELETE_INTENT', 'head_service', 'uncertain', 'observation_required',
          60),
     ])
def test_max_attempt_blocks_r_and_u_without_changing_provider_tuple(
        monkeypatch, phase: str, role: str | None, expected_disposition: str,
        expected_retry_class: str, expected_delay: int) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    maximum_attempt = 2_147_483_647
    action = dataclasses.replace(_action_record(kernel.ActionKind.DOWN),
                                 current_attempt=maximum_attempt)
    states = ('present_exact', 'present_exact', 'present_exact')
    envelope = _envelope(_down_cursor(phase, states, role=role),
                         _attestation(attempt=maximum_attempt))
    attempt = _attempt_record(envelope, attempt=maximum_attempt, revision=3)
    error = _provider_error('transient',
                            provider_code='TemporaryFailure',
                            retry_after_seconds=7,
                            normalized_message='temporary provider failure')
    expected_result = _provider_result(
        expected_disposition,
        'unknown',
        provider_code='TemporaryFailure',
        retry_class=expected_retry_class,
        retry_after_seconds=expected_delay,
        normalized_message='temporary provider failure')
    request_return = _terminal_return(kernel.ActionKind.DOWN,
                                      attempt,
                                      expected_result,
                                      normalized_provider_error=error)
    reduction_context = _reduction_context(
        kernel.ActionKind.DOWN,
        request_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is kernel.KernelState.BLOCKED
    assert reduction.retry_after_seconds is None
    assert reduction.typed_outcome['provider_result'] == expected_result


def test_down_s_requires_exact_succeeded_cursor_and_replays_exact_observation(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.DOWN)
    states = ('absent_exact', 'absent_exact', 'absent_exact')
    terminal_attestation = _attestation(after=True)
    cursor = _down_cursor('SUCCEEDED', states)
    attempt = _attempt_record(_envelope(cursor, terminal_attestation),
                              revision=9)
    expected_result = _provider_result(
        'succeeded', 'observed', observation=cursor['absence_observation'])
    request_return = _terminal_return(kernel.ActionKind.DOWN, attempt,
                                      expected_result)
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    reduction_context = _reduction_context(kernel.ActionKind.DOWN,
                                           request_return,
                                           finished_at=datetime.datetime(
                                               2026,
                                               8,
                                               1,
                                               tzinfo=datetime.timezone.utc),
                                           database_now=database_now)

    missing_after_attempt = dataclasses.replace(
        attempt,
        provider_progress=_envelope(cursor, _attestation(after=False)),
        provider_progress_sha256=values.canonical_sha256(
            _envelope(cursor, _attestation(after=False))))
    missing_after_return = _terminal_return(kernel.ActionKind.DOWN,
                                            missing_after_attempt,
                                            expected_result)
    with pytest.raises(ValueError, match='post-execution'):
        progress.reduce_handler_terminal_result_v1(
            action, None, missing_after_attempt,
            _reduction_context(kernel.ActionKind.DOWN,
                               missing_after_return,
                               finished_at=datetime.datetime(
                                   2026, 8, 1, tzinfo=datetime.timezone.utc),
                               database_now=database_now))

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is kernel.KernelState.TERMINAL
    assert reduction.terminal_disposition == 'succeeded'
    assert reduction.typed_outcome['provider_result'] == expected_result
    settled = _settled_attempt(attempt, reduction, database_now)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, settled, None)

    earlier_cursor = _down_cursor('HANDLE_REMOVED', states)
    earlier_attempt = dataclasses.replace(
        attempt,
        provider_progress=_envelope(earlier_cursor, terminal_attestation),
        provider_progress_sha256=values.canonical_sha256(
            _envelope(earlier_cursor, terminal_attestation)))
    earlier_return = _terminal_return(kernel.ActionKind.DOWN, earlier_attempt,
                                      expected_result)
    earlier_context = _reduction_context(kernel.ActionKind.DOWN,
                                         earlier_return,
                                         finished_at=datetime.datetime(
                                             2026,
                                             8,
                                             1,
                                             tzinfo=datetime.timezone.utc),
                                         database_now=database_now)
    with pytest.raises(ValueError, match='nonsuccessful domain result'):
        progress.reduce_handler_terminal_result_v1(action, None,
                                                   earlier_attempt,
                                                   earlier_context)

    crossed_cursor = dataclasses.replace(
        settled,
        provider_progress=earlier_attempt.provider_progress,
        provider_progress_sha256=earlier_attempt.provider_progress_sha256)
    with pytest.raises(ValueError, match='lacks a SUCCEEDED cursor'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_cursor, None)


def test_provider_operation_id_is_bound_through_progress_and_outcome(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.DOWN)
    states = ('present_exact', 'present_exact', 'present_exact')
    cursor = _down_cursor('TARGET_RESOLVED', states)
    observation = cursor['delete_target']['observation']
    observation['observed_provider_operation_id'] = 'operation-1'
    observation['resolved_target']['provider_operation_id'] = 'operation-1'
    attempt = dataclasses.replace(_attempt_record(_envelope(
        cursor, _attestation()),
                                                  revision=3),
                                  provider_operation_id='operation-1')
    error = _provider_error('transient',
                            provider_code='TemporaryFailure',
                            retry_after_seconds=9,
                            normalized_message='temporary provider failure')
    expected_handler_result = _provider_result(
        'retryable',
        'unknown',
        provider_code='TemporaryFailure',
        retry_class='transient',
        retry_after_seconds=9,
        normalized_message='temporary provider failure')
    request_return = _terminal_return(kernel.ActionKind.DOWN,
                                      attempt,
                                      expected_handler_result,
                                      normalized_provider_error=error)
    reduction_context = _reduction_context(
        kernel.ActionKind.DOWN,
        request_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert request_return['terminal_result']['provider_result'][
        'provider_operation_id'] is None
    assert reduction.typed_outcome['provider_result'][
        'provider_operation_id'] == 'operation-1'

    mismatched_result = copy.deepcopy(expected_handler_result)
    mismatched_result['provider_operation_id'] = 'operation-2'
    mismatched_return = _terminal_return(kernel.ActionKind.DOWN,
                                         attempt,
                                         mismatched_result,
                                         normalized_provider_error=error)
    mismatched_context = _reduction_context(
        kernel.ActionKind.DOWN,
        mismatched_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))
    with pytest.raises(ValueError, match='operation ID conflicts'):
        progress.reduce_handler_terminal_result_v1(action, None, attempt,
                                                   mismatched_context)


def test_operation_id_binding_preserves_point_in_time_null_projections(
        monkeypatch) -> None:
    launch_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: launch_context))
    launch_action = _action_record(kernel.ActionKind.LAUNCH)
    attestation = _attestation()
    claim = _claim_from_attestation(attestation)
    intent = _create_intent_cursor(0, claim)
    assert intent['pre_observation']['observed_provider_operation_id'] is None
    launch_attempt = dataclasses.replace(
        _attempt_record(
            _envelope(intent, attestation),
            mutation_boundary=kernel.MutationBoundary.SUBMITTED_OR_AMBIGUOUS,
            provider_io_boundary=kernel.ProviderIOBoundary.
            SUBMITTED_OR_AMBIGUOUS,
            revision=2),
        provider_operation_id='operation-learned-after-intent')
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        launch_action, None, launch_attempt, None)

    down_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: down_context))
    down_action = _action_record(kernel.ActionKind.DOWN)
    absent = ('absent_exact', 'absent_exact', 'absent_exact')
    success_attempt = dataclasses.replace(
        _attempt_record(
            _envelope(_down_cursor('SUCCEEDED', absent),
                      _attestation(after=True)),
            mutation_boundary=kernel.MutationBoundary.SUBMITTED_OR_AMBIGUOUS,
            provider_io_boundary=kernel.ProviderIOBoundary.
            SUBMITTED_OR_AMBIGUOUS,
            revision=9),
        provider_operation_id='delete-operation')
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        down_action, None, success_attempt, None)

    present = ('present_exact', 'present_exact', 'present_exact')
    crossed_cursor = _down_cursor('TARGET_RESOLVED', present)
    crossed_cursor['delete_target']['observation'][
        'observed_provider_operation_id'] = 'other-operation'
    crossed_cursor['delete_target']['observation']['resolved_target'][
        'provider_operation_id'] = 'other-operation'
    crossed_attempt = dataclasses.replace(
        _attempt_record(_envelope(crossed_cursor, _attestation()), revision=3),
        provider_operation_id='journal-operation')
    with pytest.raises(ValueError, match='operation ID differs'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            down_action, None, crossed_attempt, None)


def test_q_call_not_entered_uses_request_clock_and_exact_null_after(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    attestation = copy.deepcopy(claim['worker_attestation'])
    cursor = _launch_cursor('HANDLE_INTENT', claim=claim)
    attempt = _attempt_record(_envelope(cursor, attestation), revision=7)
    resolution = _call_not_entered(cursor, claim, 3)
    expected_result = _provider_result('cancelled', 'observed')
    request_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                      attempt,
                                      expected_result,
                                      reduction_kind='supersede_to_down',
                                      launch_no_effect_resolution=resolution)
    request_finished_at = datetime.datetime(2026,
                                            8,
                                            1,
                                            1,
                                            2,
                                            3,
                                            4,
                                            tzinfo=datetime.timezone.utc)
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    reduction_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                           request_return,
                                           finished_at=request_finished_at,
                                           database_now=database_now)

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is kernel.KernelState.TERMINAL
    assert reduction.terminal_disposition == 'SUPERSEDED_TO_DOWN'
    quiescence = reduction.typed_outcome['supersession_quiescence']
    assert quiescence['settled_at'] == _TIME
    assert quiescence['settled_at'] != '2026-08-02T04:05:06.000007Z'
    assert [effect['resolution'] for effect in quiescence['effects']] == [
        'evidence_committed', 'evidence_committed', 'evidence_committed',
        'call_not_entered'
    ]

    settled = _settled_attempt(attempt, reduction, database_now)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, settled, None)
    wrong_quiescence = copy.deepcopy(settled.typed_outcome)
    wrong_quiescence['supersession_quiescence'][
        'launch_provider_cursor_sha256'] = '0' * 64
    crossed_quiescence = dataclasses.replace(
        settled,
        typed_outcome=wrong_quiescence,
        typed_outcome_sha256=values.canonical_sha256(wrong_quiescence))
    with pytest.raises(ValueError, match='differs from the retained'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_quiescence, None)

    completed_attestation = _attestation(claimed_cursor_sha256='e' * 64,
                                         after=True)
    crossed_return = _terminal_return(
        kernel.ActionKind.LAUNCH,
        attempt,
        expected_result,
        reduction_kind='supersede_to_down',
        launch_no_effect_resolution=resolution,
        terminal_attestation=completed_attestation)
    crossed_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                         crossed_return,
                                         finished_at=request_finished_at,
                                         database_now=database_now)
    with pytest.raises(ValueError, match='byte-equal terminal'):
        progress.reduce_handler_terminal_result_v1(action, None, attempt,
                                                   crossed_context)

    missing_worker_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                                request_return,
                                                finished_at=request_finished_at,
                                                database_now=database_now)
    missing_worker_context.terminal_request.worker_instance_id = None
    with pytest.raises(ValueError, match='locked request worker'):
        progress.reduce_handler_terminal_result_v1(action, None, attempt,
                                                   missing_worker_context)


def test_q_nonintent_launch_builds_exact_e_only_quiescence(monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    cursor = _launch_cursor('HANDLE_COMMITTED', claim=claim)
    attempt = _attempt_record(_envelope(
        cursor, _attestation(claimed_cursor_sha256='e' * 64)),
                              revision=8)
    expected_result = _provider_result('cancelled', 'observed')
    request_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                      attempt,
                                      expected_result,
                                      reduction_kind='supersede_to_down')
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    reduction_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                           request_return,
                                           finished_at=datetime.datetime(
                                               2026,
                                               8,
                                               1,
                                               1,
                                               2,
                                               3,
                                               4,
                                               tzinfo=datetime.timezone.utc),
                                           database_now=database_now)

    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, reduction_context)
    assert reduction.kernel_state is kernel.KernelState.TERMINAL
    assert reduction.terminal_disposition == 'SUPERSEDED_TO_DOWN'
    assert reduction.typed_outcome['launch_no_io_prefix'] is None
    assert [
        effect['resolution'] for effect in
        reduction.typed_outcome['supersession_quiescence']['effects']
    ] == ['evidence_committed'] * 4
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, _settled_attempt(attempt, reduction, database_now), None)


def test_q_definitive_no_effect_requires_completed_worker_attestation(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    claim = _claim(claimed_cursor_sha256='e' * 64)
    attestation = copy.deepcopy(claim['worker_attestation'])
    cursor = _launch_cursor('HANDLE_INTENT', claim=claim)
    attempt = _attempt_record(_envelope(cursor, attestation), revision=7)
    resolution = _definitive_no_effect(cursor, claim)
    expected_result = _provider_result('cancelled', 'observed')
    request_finished_at = datetime.datetime(2026,
                                            8,
                                            1,
                                            1,
                                            2,
                                            3,
                                            4,
                                            tzinfo=datetime.timezone.utc)
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)

    incomplete_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                         attempt,
                                         expected_result,
                                         reduction_kind='supersede_to_down',
                                         launch_no_effect_resolution=resolution)
    incomplete_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                            incomplete_return,
                                            finished_at=request_finished_at,
                                            database_now=database_now)
    with pytest.raises(ValueError, match='completion'):
        progress.reduce_handler_terminal_result_v1(action, None, attempt,
                                                   incomplete_context)

    completed_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                        attempt,
                                        expected_result,
                                        reduction_kind='supersede_to_down',
                                        launch_no_effect_resolution=resolution,
                                        terminal_attestation=_attestation(
                                            claimed_cursor_sha256='e' * 64,
                                            after=True))
    completed_context = _reduction_context(kernel.ActionKind.LAUNCH,
                                           completed_return,
                                           finished_at=request_finished_at,
                                           database_now=database_now)
    reduction = progress.reduce_handler_terminal_result_v1(
        action, None, attempt, completed_context)
    assert reduction.kernel_state is kernel.KernelState.TERMINAL
    assert reduction.typed_outcome['supersession_quiescence']['effects'][-1][
        'resolution'] == 'definitive_no_effect'

    settled = _settled_attempt(attempt, reduction, database_now)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, settled, None)
    crossed_outcome = copy.deepcopy(settled.typed_outcome)
    final_effect = crossed_outcome['supersession_quiescence']['effects'][-1]
    final_effect['definitive_no_effect']['cluster_name'] = 'crossed-cluster'
    final_effect['evidence_sha256'] = values.canonical_sha256(
        final_effect['definitive_no_effect'])
    crossed_settled = dataclasses.replace(
        settled,
        typed_outcome=crossed_outcome,
        typed_outcome_sha256=values.canonical_sha256(crossed_outcome))
    with pytest.raises(ValueError, match='exact current intended handle'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, crossed_settled, None)


def test_foreign_worker_cohort_rejects_fresh_replacement_terminal_and_replay(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    worker_id = uuid.UUID('55555555-5555-4555-8555-555555555555')

    fresh_attempt = _attempt_record(
        None,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED)
    foreign_fresh_attestation = _foreign_attestation(
        _attestation(worker_id=worker_id))
    foreign_claim = _claim_from_attestation(foreign_fresh_attestation)
    fresh_cursor = {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'CREATE_INTENT',
        'role': 'head_ssh_service',
        'intent_origin': foreign_claim,
        'committed_effects': [],
        'known_objects': _partial_target(0),
        'pre_observation': _prefix_observation(0),
    }
    fresh_fence = kernel.AttemptExecutionFence(
        request_id=fresh_attempt.request_id,
        execution_generation=1,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=worker_id,
        controller_generation=1)
    with pytest.raises(ValueError, match='does not match its cohort'):
        progress.ServeProviderProgressContractV1.validate_progress_transition(
            action, None, fresh_attempt, fresh_fence,
            _envelope(fresh_cursor, foreign_fresh_attestation))

    valid_claim = _claim(claimed_cursor_sha256='e' * 64)
    carried_cursor = _launch_cursor('HANDLE_COMMITTED', claim=valid_claim)
    valid_progress = _envelope(carried_cursor,
                               _attestation(claimed_cursor_sha256='e' * 64))
    crossed_attempt = _attempt_record(valid_progress, revision=8)
    foreign_replacement = _foreign_attestation(
        _attestation(
            generation=2,
            worker_id=worker_id,
            claimed_cursor_sha256=values.canonical_sha256(carried_cursor)))
    replacement_fence = kernel.AttemptExecutionFence(
        request_id=crossed_attempt.request_id,
        execution_generation=2,
        claim_token=uuid.UUID('66666666-6666-4666-8666-666666666666'),
        worker_instance_id=worker_id,
        controller_generation=1)
    with pytest.raises(ValueError, match='does not match its cohort'):
        progress.ServeProviderProgressContractV1.validate_progress_transition(
            action, None, crossed_attempt, replacement_fence,
            _envelope(carried_cursor, foreign_replacement))

    error = _provider_error('transient',
                            provider_code='TemporaryFailure',
                            retry_after_seconds=11,
                            normalized_message='temporary provider failure')
    expected_result = _provider_result(
        'retryable',
        'unknown',
        provider_code='TemporaryFailure',
        retry_class='transient',
        retry_after_seconds=11,
        normalized_message='temporary provider failure')
    foreign_terminal_attestation = _foreign_attestation(_attestation())
    foreign_terminal_return = _terminal_return(
        kernel.ActionKind.LAUNCH,
        fresh_attempt,
        expected_result,
        normalized_provider_error=error,
        terminal_attestation=foreign_terminal_attestation)
    foreign_terminal_context = _reduction_context(
        kernel.ActionKind.LAUNCH,
        foreign_terminal_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=datetime.datetime(2026, 8, 2,
                                       tzinfo=datetime.timezone.utc))
    with pytest.raises(ValueError, match='does not match its cohort'):
        progress.reduce_handler_terminal_result_v1(action, None, fresh_attempt,
                                                   foreign_terminal_context)

    valid_terminal_return = _terminal_return(kernel.ActionKind.LAUNCH,
                                             crossed_attempt,
                                             expected_result,
                                             normalized_provider_error=error)
    database_now = datetime.datetime(2026, 8, 2, tzinfo=datetime.timezone.utc)
    valid_terminal_context = _reduction_context(
        kernel.ActionKind.LAUNCH,
        valid_terminal_return,
        finished_at=datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
        database_now=database_now)
    valid_reduction = progress.reduce_handler_terminal_result_v1(
        action, None, crossed_attempt, valid_terminal_context)
    settled = _settled_attempt(crossed_attempt, valid_reduction, database_now)
    foreign_replay_progress = _envelope(
        carried_cursor,
        _foreign_attestation(_attestation(claimed_cursor_sha256='e' * 64)))
    foreign_replay = dataclasses.replace(
        settled,
        provider_progress=foreign_replay_progress,
        provider_progress_sha256=values.canonical_sha256(
            foreign_replay_progress))
    with pytest.raises(ValueError, match='does not match its cohort'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, foreign_replay, None)


def test_exact_scalar_types_and_attempt_origin_bindings(monkeypatch) -> None:
    no_io_projection = {
        'attempt': 1,
        'request_id': kernel.request_id_for_attempt(_ACTION_ID, 1),
        'request_input_sha256': '1' * 64,
        'mutation_boundary': 'SETTLED',
        'provider_io_boundary': 'NOT_STARTED',
        'provider_progress_revision': False,
        'provider_progress_sha256': None,
        'provider_operation_id': None,
        'request_terminal_state': 'SUCCEEDED',
        'settled_at': _TIME,
    }
    with pytest.raises(ValueError, match='nonnegative signed-64-bit'):
        progress.ServeLaunchNoIoAttemptProjectionV1.from_value(no_io_projection)

    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    future_claim = _claim(attempt=2,
                          generation=1,
                          claimed_cursor_sha256='e' * 64)
    future_cursor = _launch_cursor('HANDLE_COMMITTED', claim=future_claim)
    future_attempt = _attempt_record(_envelope(
        future_cursor, _attestation(attempt=1, claimed_cursor_sha256='e' * 64)),
                                     revision=8)
    with pytest.raises(ValueError, match='future action attempt'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, future_attempt, None)

    intent = _claim(attempt=2)
    earlier_evidence = _claim(attempt=1)
    crossed_effect = _create_effect(0, intent)
    crossed_effect['commit_disposition'] = 'adopted_exact'
    crossed_effect['evidence_commit_origin'] = earlier_evidence
    with pytest.raises(ValueError, match='precedes its intent'):
        progress.ProviderLaunchCommittedEffectEvidenceV1.from_value(
            crossed_effect)

    down_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: down_context))
    down_action = _action_record(kernel.ActionKind.DOWN)
    states = ('present_exact', 'present_exact', 'present_exact')
    crossed_envelope_attempt = _attempt_record(_envelope(
        _down_cursor('TARGET_RESOLVED', states), _attestation(attempt=2)),
                                               revision=1)
    with pytest.raises(ValueError, match='another action attempt request'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            down_action, None, crossed_envelope_attempt, None)


def test_no_io_prefix_chains_exact_immediate_predecessor_projection(
        monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.LAUNCH)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.LAUNCH)
    error = _provider_error('transient',
                            provider_code='TemporaryFailure',
                            retry_after_seconds=5,
                            normalized_message='temporary provider failure')
    expected_result = _provider_result(
        'retryable',
        'unknown',
        provider_code='TemporaryFailure',
        retry_class='transient',
        retry_after_seconds=5,
        normalized_message='temporary provider failure')

    attempt_one = _attempt_record(
        None,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED)
    return_one = _terminal_return(kernel.ActionKind.LAUNCH,
                                  attempt_one,
                                  expected_result,
                                  normalized_provider_error=error)
    time_one = datetime.datetime(2026,
                                 8,
                                 2,
                                 1,
                                 2,
                                 3,
                                 4,
                                 tzinfo=datetime.timezone.utc)
    reduction_one = progress.reduce_handler_terminal_result_v1(
        action, None, attempt_one,
        _reduction_context(kernel.ActionKind.LAUNCH,
                           return_one,
                           finished_at=datetime.datetime(
                               2026, 8, 1, tzinfo=datetime.timezone.utc),
                           database_now=time_one))
    settled_one = _settled_attempt(attempt_one, reduction_one, time_one)

    attempt_two = _attempt_record(
        None,
        attempt=2,
        mutation_boundary=kernel.MutationBoundary.NOT_STARTED,
        provider_io_boundary=kernel.ProviderIOBoundary.NOT_STARTED)
    return_two = _terminal_return(kernel.ActionKind.LAUNCH,
                                  attempt_two,
                                  expected_result,
                                  normalized_provider_error=error)
    time_two = datetime.datetime(2026,
                                 8,
                                 3,
                                 1,
                                 2,
                                 3,
                                 4,
                                 tzinfo=datetime.timezone.utc)
    reduction_two = progress.reduce_handler_terminal_result_v1(
        action, settled_one, attempt_two,
        _reduction_context(kernel.ActionKind.LAUNCH,
                           return_two,
                           finished_at=datetime.datetime(
                               2026, 8, 2, tzinfo=datetime.timezone.utc),
                           database_now=time_two))
    prefix_one = reduction_one.typed_outcome['launch_no_io_prefix']
    prefix_two = reduction_two.typed_outcome['launch_no_io_prefix']
    assert prefix_two['count'] == 2
    assert prefix_two['previous_prefix_sha256'] == prefix_one['prefix_sha256']
    settled_two = _settled_attempt(attempt_two, reduction_two, time_two)
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, settled_one, settled_two, None)
    assert progress.ServeProviderProgressContractV1.retry_seed(
        action, settled_one, settled_two) is None

    tampered_outcome = copy.deepcopy(settled_two.typed_outcome)
    tampered_prefix = tampered_outcome['launch_no_io_prefix']
    tampered_prefix['previous_prefix_sha256'] = '0' * 64
    tampered_prefix['prefix_sha256'] = values.canonical_sha256({
        'previous_prefix_sha256': tampered_prefix['previous_prefix_sha256'],
        'current_attempt': tampered_prefix['current_attempt'],
    })
    tampered_predecessor = dataclasses.replace(
        settled_two,
        typed_outcome=tampered_outcome,
        typed_outcome_sha256=values.canonical_sha256(tampered_outcome))
    with pytest.raises(ValueError, match='immediate predecessor link'):
        progress.ServeProviderProgressContractV1.retry_seed(
            action, settled_one, tampered_predecessor)

    wrong_predecessor_outcome = copy.deepcopy(settled_one.typed_outcome)
    wrong_prefix = wrong_predecessor_outcome['launch_no_io_prefix']
    wrong_prefix['current_attempt']['request_input_sha256'] = '2' * 64
    wrong_prefix['prefix_sha256'] = values.canonical_sha256({
        'previous_prefix_sha256': wrong_prefix['previous_prefix_sha256'],
        'current_attempt': wrong_prefix['current_attempt'],
    })
    wrong_predecessor = dataclasses.replace(
        settled_one,
        typed_outcome=wrong_predecessor_outcome,
        typed_outcome_sha256=values.canonical_sha256(wrong_predecessor_outcome))
    with pytest.raises(ValueError, match='exact retained attempt projection'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, wrong_predecessor, settled_two, None)

    bool_basis = copy.deepcopy(settled_two.typed_outcome)
    bool_basis['basis']['version'] = True
    crossed_basis = dataclasses.replace(
        settled_two,
        typed_outcome=bool_basis,
        typed_outcome_sha256=values.canonical_sha256(bool_basis))
    with pytest.raises(ValueError, match='integer 1'):
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, settled_one, crossed_basis, None)


def test_request_terminal_fallback_p0_o_s_x_and_goldens(monkeypatch) -> None:
    states = ('present_exact', 'present_exact', 'present_exact')
    absent = ('absent_exact', 'absent_exact', 'absent_exact')
    p0_attempt = _attempt_record(None)
    o_attempt = _attempt_record(_envelope(
        _down_cursor('TARGET_RESOLVED', states), _attestation()),
                                revision=3)
    s_attempt = _attempt_record(_envelope(_down_cursor('SUCCEEDED', absent),
                                          _attestation(after=True)),
                                revision=9)
    invalid_progress = _envelope(_down_cursor('TARGET_RESOLVED', states),
                                 _attestation())
    x_attempt = dataclasses.replace(_attempt_record(invalid_progress,
                                                    revision=3),
                                    provider_progress_sha256='0' * 64)
    cases = (
        ('P0', kernel.ActionKind.LAUNCH, p0_attempt,
         requests_lib.RequestStatus.FAILED, None, 'not_started_empty',
         kernel.KernelState.READY),
        ('O', kernel.ActionKind.DOWN, o_attempt,
         requests_lib.RequestStatus.CANCELLED, None, 'valid_nonterminal',
         kernel.KernelState.READY),
        ('S', kernel.ActionKind.DOWN, s_attempt,
         requests_lib.RequestStatus.FAILED, None, 'valid_succeeded',
         kernel.KernelState.TERMINAL),
        ('X', kernel.ActionKind.DOWN, x_attempt,
         requests_lib.RequestStatus.SUCCEEDED, {
             'malformed': True
         }, 'invalid', kernel.KernelState.BLOCKED),
    )
    database_now = datetime.datetime(2026,
                                     8,
                                     2,
                                     4,
                                     5,
                                     6,
                                     7,
                                     tzinfo=datetime.timezone.utc)
    observed = {}
    for (symbol, action_kind, attempt, request_status, return_value,
         expected_class, expected_state) in cases:
        action_context = _context(action_kind)
        monkeypatch.setattr(
            progress._ActionContext, 'from_record',
            classmethod(lambda cls, action, value=action_context: value))
        action = _action_record(action_kind)
        reduction = progress.reduce_request_terminal_fallback_v1(
            action, None, attempt,
            _fallback_context(action_kind,
                              attempt,
                              request_status,
                              return_value=return_value))
        assert reduction.kernel_state is expected_state
        assert reduction.typed_outcome == reduction.result
        evidence = reduction.typed_outcome['basis']['request_fallback_evidence']
        assert evidence['journal_class'] == expected_class
        assert evidence['active_claim'] is False
        assert reduction.typed_outcome['supersession_quiescence'] is None
        if symbol == 'P0':
            assert reduction.retry_after_seconds == 60
            assert reduction.typed_outcome['launch_no_io_prefix'][
                'current_attempt']['request_terminal_state'] == 'FAILED'
        elif symbol == 'O':
            assert reduction.retry_after_seconds == 60
            assert reduction.typed_outcome['launch_no_io_prefix'] is None
        elif symbol == 'S':
            assert reduction.terminal_disposition == 'succeeded'
        else:
            assert 'provider_progress' not in evidence
        parsed = progress.ServeReplicaActionRequestFallbackOutcomeV1.from_value(
            reduction.typed_outcome)
        observed[symbol] = (len(parsed.canonical_bytes), parsed.sha256)
        settled = dataclasses.replace(
            attempt,
            mutation_boundary=kernel.MutationBoundary.SETTLED,
            typed_outcome=reduction.typed_outcome,
            typed_outcome_sha256=values.canonical_sha256(
                reduction.typed_outcome),
            request_terminal_state=request_status.value,
            updated_at=database_now,
            settled_at=database_now)
        progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, None, settled, None)
    assert observed == {
        'P0':
            (1413,
             '107745520de2f879c863ec8f545815b4f1a5036e323922ad96db851d60170c6d'
            ),
        'O': (957,
              '6bb45637d871e085952bad8130d7f35333e6d7876259743b55da34c172e69187'
             ),
        'S': (5687,
              'd4117c231a5eb1cce99c3a839da6e05fadcbf9a1c862ae91e9d29f21ac2d0382'
             ),
        'X': (941,
              '8e13d9e0fc07b33ff17a3e91babf5b74dd0a83eb0b269c0152002ee7fbcac49d'
             ),
    }


@pytest.mark.parametrize(('phase', 'states', 'role'), [
    ('DELETE_INTENT',
     ('present_exact', 'present_exact', 'present_exact'), 'head_service'),
    ('ABSENCE_EXACT', ('absent_exact', 'absent_exact', 'absent_exact'), None),
])
def test_request_fallback_preserves_down_pre_submission_intent_as_o(
        monkeypatch, phase: str, states: tuple[str, str, str],
        role: str | None) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    action = _action_record(kernel.ActionKind.DOWN)
    attempt = _attempt_record(
        _envelope(_down_cursor(phase, states, role=role), _attestation()),
        mutation_boundary=kernel.MutationBoundary.INTENT_COMMITTED,
        provider_io_boundary=kernel.ProviderIOBoundary.INTENT_COMMITTED,
        revision=2)
    reduction = progress.reduce_request_terminal_fallback_v1(
        action, None, attempt,
        _fallback_context(kernel.ActionKind.DOWN, attempt,
                          requests_lib.RequestStatus.CANCELLED))

    assert reduction.kernel_state is kernel.KernelState.READY
    assert reduction.retry_after_seconds == 60
    evidence = reduction.typed_outcome['basis']['request_fallback_evidence']
    assert evidence['journal_class'] == 'valid_nonterminal'
    assert reduction.typed_outcome['provider_result'] == _provider_result(
        'uncertain',
        'unknown',
        retry_class='observation_required',
        retry_after_seconds=60)

    settled = _settled_attempt(attempt,
                               reduction,
                               datetime.datetime(2026,
                                                 8,
                                                 2,
                                                 tzinfo=datetime.timezone.utc),
                               request_terminal_state='CANCELLED')
    progress.ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, None, settled, None)


def test_fallback_max_attempt_blocks_without_rewriting_o(monkeypatch) -> None:
    action_context = _context(kernel.ActionKind.DOWN)
    monkeypatch.setattr(progress._ActionContext, 'from_record',
                        classmethod(lambda cls, action: action_context))
    maximum = 2_147_483_647
    action = dataclasses.replace(_action_record(kernel.ActionKind.DOWN),
                                 current_attempt=maximum)
    states = ('present_exact', 'present_exact', 'present_exact')
    attempt = _attempt_record(_envelope(_down_cursor('TARGET_RESOLVED', states),
                                        _attestation(attempt=maximum)),
                              attempt=maximum,
                              revision=3)
    reduction = progress.reduce_request_terminal_fallback_v1(
        action, None, attempt,
        _fallback_context(kernel.ActionKind.DOWN, attempt,
                          requests_lib.RequestStatus.CANCELLED))
    assert reduction.kernel_state is kernel.KernelState.BLOCKED
    assert reduction.retry_after_seconds is None
    assert reduction.typed_outcome['provider_result'] == _provider_result(
        'uncertain',
        'unknown',
        retry_class='observation_required',
        retry_after_seconds=60)
    assert reduction.typed_outcome['launch_no_io_prefix'] is None
