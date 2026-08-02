"""Prior-launch basis and exact Kubernetes down execution-config tests."""

# pylint: disable=protected-access

import copy
import dataclasses
import uuid

import pytest

from sky.serve import resource_action_progress as progress
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions
from tests.unit_tests import test_serve_resource_action_capsule_leaves as leaves
from tests.unit_tests import test_serve_resource_action_launch_execution_config
from tests.unit_tests import test_serve_resource_action_progress

launch_fixtures = test_serve_resource_action_launch_execution_config
progress_fixtures = test_serve_resource_action_progress
_OBSERVED_AT = '2026-08-01T05:06:07.123456Z'
_FIXTURE_MEMBERS = ('realistic', 'candidate_maximal')
_MAX_RESOURCE_ACTION_ATTEMPT = 2**31 - 1
_MAX_POSTGRES_BIGINT = 2**63 - 1


def _resource_identity(generation: int = 3) -> dict:
    return launch_fixtures._resource_identity(generation)


def _target() -> dict:
    return launch_fixtures._target()


def _resources() -> dict:
    return launch_fixtures._resource_snapshot()


def _workspace_identity(*, workspace: str = 'workspace-a') -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    assert target.kubernetes is not None
    return {
        'version': 1,
        'workspace': workspace,
        'kubernetes_scope': target.kubernetes.scope.canonical_value(),
    }


def _allocations(sequence: int) -> list[dict]:
    if sequence == 0:
        return [{
            'json_pointer': '/spec/clusterIP',
            'allocator': 'api_server',
            'value': '10.0.0.7',
        }, {
            'json_pointer': '/spec/clusterIPs',
            'allocator': 'api_server',
            'value': ['10.0.0.7'],
        }, {
            'json_pointer': '/spec/ipFamilies',
            'allocator': 'api_server',
            'value': ['IPv4'],
        }, {
            'json_pointer': '/spec/ipFamilyPolicy',
            'allocator': 'api_server',
            'value': 'SingleStack',
        }]
    if sequence == 1:
        return [{
            'json_pointer': '/spec/clusterIP',
            'allocator': 'api_server',
            'value': 'None',
        }, {
            'json_pointer': '/spec/clusterIPs',
            'allocator': 'api_server',
            'value': ['None'],
        }, {
            'json_pointer': '/spec/ipFamilies',
            'allocator': 'api_server',
            'value': ['IPv4'],
        }, {
            'json_pointer': '/spec/ipFamilyPolicy',
            'allocator': 'api_server',
            'value': 'SingleStack',
        }]
    return [{
        'json_pointer': '/spec/nodeName',
        'allocator': 'scheduler',
        'value': 'worker-node-0',
    }]


def _uid(sequence: int, fixture_member: str = 'realistic') -> str:
    if fixture_member not in _FIXTURE_MEMBERS:
        raise ValueError('unknown down size fixture member.')
    if fixture_member == 'realistic':
        return ('uid-head_ssh_service', 'uid-head_service',
                'uid-head_pod')[sequence]
    prefix = f'candidate-maximal-{sequence}-'
    return prefix + 'u' * (1_024 - len(prefix))


def _handle(fixture_member: str = 'realistic') -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    resources = actions.ProviderPodResourceSnapshotV1.from_value(_resources())
    assert target.kubernetes is not None
    config = {
        'context_mode': 'in_cluster',
        'scope_sha256': target.kubernetes.scope.sha256,
        'namespace': target.kubernetes.namespace,
        'port_mode': 'podip',
        'use_internal_ips': True,
        'application_port': '8080',
        'pod_name': target.kubernetes.workload_name,
        'pod_uid': _uid(2, fixture_member),
        'node_name': 'worker-node-0',
        'pod_ip': '10.1.2.3',
        'head_service_uid': _uid(1, fixture_member),
        'head_ssh_service_uid': _uid(0, fixture_member),
        'ambient_fallback': False,
    }
    return {
        'version': 1,
        'cluster_record_uuid': str(target.sky_cluster_record_uuid),
        'cluster_name': target.sky_cluster_name,
        'cluster_name_on_cloud': target.kubernetes.provider_cluster_name,
        'requested_target_sha256': target.sha256,
        'launched_resources_sha256': resources.sha256,
        'provider_config': config,
        'provider_config_sha256': actions.canonical_sha256(config),
    }


def _cleanup_target(*,
                    basis_kind: str = 'completed_launch',
                    committed_count: int = 3,
                    exact_handle: bool = True,
                    pod_node: bool = True,
                    fixture_member: str = 'realistic') -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    capsule = launch_fixtures._capsule_raw()
    handle = _handle(fixture_member)
    return {
        'version': 1,
        'basis_kind': basis_kind,
        'requested_target_sha256': target.sha256,
        'cluster_name': target.sky_cluster_name,
        'cluster_record_uuid': str(target.sky_cluster_record_uuid),
        'objects': [{
            'sequence': sequence,
            'role': plan['role'],
            'plan': copy.deepcopy(plan),
            'committed_uid': _uid(sequence, fixture_member)
                             if sequence < committed_count else None,
            'committed_server_allocations':
                (_allocations(sequence) if sequence < committed_count and
                 (sequence != 2 or pod_node) else []),
        } for sequence, plan in enumerate(capsule['objects'])],
        'cluster_row_disposition': 'exact_handle'
                                   if exact_handle else 'not_found',
        'handle': handle if exact_handle else None,
        'observed_at': _OBSERVED_AT,
    }


def _resolved_target() -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    assert target.kubernetes is not None
    return {
        'version': 1,
        'requested_target_sha256': target.sha256,
        'provider_resource_id': f'pod/{target.kubernetes.workload_name}',
        'workload_uid': _uid(2),
        'kubernetes_objects': [
            _resolved_object_for_plan(sequence, pod_node=True)
            for sequence in range(3)
        ],
        'provider_operation_id': None,
        'resolved_at': _OBSERVED_AT,
    }


def completed_basis_payload(
    *,
    source_store: str = 'api_resource_actions',
    generation: int = 3,
) -> dict:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    resolved = actions.ResolvedProviderTargetV1.from_value(_resolved_target())
    handle = actions.ProviderKubernetesHandleV1.from_value(_handle())
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target())
    return {
        'version': 1,
        'basis_kind': 'completed_launch',
        'source_store': source_store,
        'launch_action_id': str(
            identity.action_identity(kernel_actions.ActionKind.LAUNCH).action_id
        ),
        'launch_resource_identity': identity.canonical_value(),
        'launch_requested_target': _target(),
        'launch_resources': _resources(),
        'launch_workspace_identity': _workspace_identity(),
        'launch_resolved_target': resolved.canonical_value(),
        'launch_resolved_target_sha256': resolved.sha256,
        'launch_handle': handle.canonical_value(),
        'launch_handle_sha256': handle.sha256,
        'launch_cleanup_target_sha256': cleanup.sha256,
        'launch_immutable_spec_sha256': 'a' * 64,
        'exact_resources_override': True,
    }


def _partial_cursor(generation: int = 3) -> progress.ProviderLaunchProgressV1:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    action_id = identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    target = actions.ProviderLocatorV1.from_value(_target())
    claim = progress_fixtures._claim(action_id)
    known_objects = progress_fixtures._partial_target(0)
    known_objects['requested_target_sha256'] = target.sha256
    pre_observation = progress_fixtures._prefix_observation(0)
    pre_observation['target_sha256'] = target.sha256
    raw = {
        'version': 1,
        'action_kind': 'launch',
        'phase': 'CREATE_INTENT',
        'role': 'head_ssh_service',
        'intent_origin': claim,
        'committed_effects': [],
        'known_objects': known_objects,
        'pre_observation': pre_observation,
    }
    return progress.ProviderLaunchProgressV1.from_value(raw)


def partial_basis_payload(generation: int = 3) -> dict:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    action_id = identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    cursor = _partial_cursor(generation)
    cursor_value = cursor.canonical_value()
    assert cursor.intent_origin is not None
    resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
        progress_fixtures._call_not_entered(
            cursor_value, cursor.intent_origin.canonical_value(), 0))
    effects = (
        progress.ProviderLaunchEffectQuiescenceV1.from_resolution(resolution),)
    quiescence = progress.ProviderLaunchSupersessionQuiescenceV1(
        launch_action_id=action_id,
        launch_attempt=1,
        request_id=uuid.UUID(kernel_actions.request_id_for_attempt(
            action_id, 1)),
        handler_terminal_result_sha256='b' * 64,
        launch_provider_cursor_sha256=cursor.sha256,
        effects=effects,
        settled_at=_OBSERVED_AT)
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target(basis_kind='partial_launch_cleanup',
                        committed_count=0,
                        exact_handle=False))
    return {
        'version': 1,
        'basis_kind': 'partial_launch_cleanup',
        'source_store': 'api_resource_actions',
        'launch_action_id': str(action_id),
        'launch_attempt': 1,
        'launch_resource_identity': identity.canonical_value(),
        'launch_requested_target': _target(),
        'launch_resources': _resources(),
        'launch_workspace_identity': _workspace_identity(),
        'launch_provider_cursor_sha256': cursor.sha256,
        'launch_provider_progress_revision': 8,
        'launch_quiescence_sha256': quiescence.sha256,
        'launch_cleanup_target_sha256': cleanup.sha256,
        'launch_immutable_spec_sha256': 'a' * 64,
        'exact_resources_override': True,
    }


_PARTIAL_DOWN_CASES = (
    actions.enumerate_provider_partial_launch_cleanup_legal_shapes_v1())

_PARTIAL_DOWN_SPEC_GOLDEN_MANIFEST_V1 = {
    'realistic': {
        'create_intent_0_not_found':
            (38_159,
             '53d870a98149bb10845d2a80e8acf7801e38f83255daf6de1dbb44ac104fc82e'
            ),
        'objects_partial_1_not_found':
            (38_502,
             '90f020c67bd719e6c06a1ca662c129ad6b9861ee8d8aa6e495bea8d56da0763e'
            ),
        'create_intent_1_not_found':
            (38_502,
             '58f1db9b91a72a0be2589effdce1610fe0237c5649bd21ab212f2dca858d8125'
            ),
        'objects_partial_2_not_found':
            (38_833,
             'd7cce0b9d02807d438e3d56dbdb9af4bce40dea15acb6dda5271a38668a82ec9'
            ),
        'create_intent_2_not_found':
            (38_833,
             'faefad015cd6ca2c4d377795a44d2295af4bb290571f86152a124f56de30a732'
            ),
        'objects_partial_3_unscheduled_not_found':
            (38_843,
             'cc2077bdbc6983f907449e6dd6d683e657533932f409b6c67732896e05a2254b'
            ),
        'objects_exact_not_found':
            (38_924,
             '9f7680af1130ff2a5a8b668d5991aa44ee05483ed2323a6abb0c25ff5fbc501c'
            ),
        'handle_intent_not_found':
            (38_924,
             'f206304a6d811edc04ab754e950dbdc5aa16c1f99b58b3781862c04095de0b38'
            ),
        'handle_committed_not_found':
            (38_924,
             'f71d453dcd185e239403e67f094ffda12bfaa44c1ccc3d87dc7faa257dc1cebf'
            ),
        'handle_committed_exact_handle':
            (39_785,
             '1061c2f9313c78f43080863ad76f6137608a1fcee79b2645464d75b39a024af6'
            ),
        'runtime_ready_not_found':
            (38_924,
             'e1a84b0e0958af4b181ce6370a1adac86575df3106326a89c970c52bd5180622'
            ),
        'runtime_ready_exact_handle':
            (39_785,
             '4f604cd19132ca88dce338b5b2e146a43f0f3f0a8f823d2626c93fe3763f6cd0'
            ),
        'job_intent_not_found':
            (38_924,
             'fa3df8dd580cb3a5585bb9702e8faaae558ab23caaa20d74971ecc926e13bd1b'
            ),
        'job_intent_exact_handle':
            (39_785,
             '3c9335f0592468bacc7bb8cf14507f35831de782a7e51784372e9b8c93d863c4'
            ),
        'job_committed_not_found':
            (38_924,
             '56b7f2c95d4b7e0085c6738920d6b273ea70342085fcc7ce23d8e82b4467310a'
            ),
        'job_committed_exact_handle':
            (39_785,
             'eb75274a1554817e36f05c1f5297125e792af1061a80d9816076087442ed3c9b'
            ),
        'job_running_not_found':
            (38_924,
             '2502b4f4dfc1648a5005e425b2a71e998a7488ecf6d8e97046a99a0bac6c08fe'
            ),
        'job_running_exact_handle':
            (39_785,
             'eb17f0c1abfdb9323d3520b7c339961e8a260050c2867b4bc1d73f6e8d3428c6'
            ),
        'endpoint_resolved_not_found':
            (38_924,
             'aa66b9ed8e3478e0c2d68b4a789c1db49eec3b8680e6472227020a12b7a03e29'
            ),
        'endpoint_resolved_exact_handle':
            (39_785,
             '54d00be552ce4e8e9b46930c953260c900df2d1a40d04db7651339cd0e283785'
            ),
    },
    'candidate_maximal': {
        'create_intent_0_not_found':
            (38_186,
             '5373e1153019f85501d065d605495431630f7462e9fcc8a8cd4bfb7a03041db4'
            ),
        'objects_partial_1_not_found':
            (39_533,
             '64faf65cbd3e8be8b4a838225cf0b86c6669bcc7169e461835a2d914b37b44b9'
            ),
        'create_intent_1_not_found':
            (39_533,
             '78027302e4623f1ba6e24660a7602cd6c56d00fc7fabeafcaaf42c0ab6224b41'
            ),
        'objects_partial_2_not_found':
            (40_872,
             'b44b8be5b23ee80ef3f9f0a4b164671b1a4941964ec21066386cb1850d5d7507'
            ),
        'create_intent_2_not_found':
            (40_872,
             '55fff0cb6349acf9020536c6621dc06283120f981489dd64f01c949016c988b4'
            ),
        'objects_partial_3_unscheduled_not_found':
            (41_894,
             '003523fdb2be5eff3fb001808c5d4cb922ef3d89d1caa245ef8f4ac81348ac87'
            ),
        'objects_exact_not_found':
            (41_975,
             '5df0e2b08717b6ebc757a20aa364ddac25f42048e445ec0024cc71bd2a36cd28'
            ),
        'handle_intent_not_found':
            (41_975,
             '1838bc532bd6d3808abe5be94374949a8cef236b3984666f64fc3b78d3c0660b'
            ),
        'handle_committed_not_found':
            (41_975,
             '735e72d009629e8243a6d7e19f533b62185d330a027de6ff11d2d5269caf8b7a'
            ),
        'handle_committed_exact_handle':
            (45_860,
             '11717621914678cd385888b51e8219143dfe957847def58ffa2c9c0212d47b0d'
            ),
        'runtime_ready_not_found':
            (41_975,
             '30ea1c700eafbb4c56c76227cbb1ab4a0cf8c27f3259090046e915d8564cb6f6'
            ),
        'runtime_ready_exact_handle':
            (45_860,
             '8c7e1f217aae227237fd5d2e0429e9584a0d66d371339fd9d569c674b4a2be80'
            ),
        'job_intent_not_found':
            (41_975,
             'a93b433e4c7aea349b3bf2ee7480f68963172cbae8f684d91f7e50d21e042046'
            ),
        'job_intent_exact_handle':
            (45_860,
             'ccf4686cd0e4a11c9b6cbe3e9d4c76e4773434795e3af4c541e89018c0a96bd3'
            ),
        'job_committed_not_found':
            (41_975,
             'e91cb94928c49f60665ccf879596ded610686749dddc072a5585abc60017b807'
            ),
        'job_committed_exact_handle':
            (45_860,
             'efef123fc038344f9c7c23b43c6a27494c47c09fdec40bfc20719ab29bfb16bc'
            ),
        'job_running_not_found':
            (41_975,
             '9b46f952dab8637d8331bea4ba2cfae6e07dfff78f0caf2aca5e9a6960b51795'
            ),
        'job_running_exact_handle':
            (45_860,
             'd9f39110365fc070a7877f709a554a5b6fd3c73f164fe6158eca9569b9106b32'
            ),
        'endpoint_resolved_not_found':
            (41_975,
             '5c317e760ff2364b1b1721445fea2ea2673b500bbc622bd66a96cde49ac6c54d'
            ),
        'endpoint_resolved_exact_handle':
            (45_860,
             '42c3ab325e41ef90603c4a9dc2444b6a4730e7e39b4901153b76216164ec542b'
            ),
    },
}


def _resolved_object_for_plan(sequence: int,
                              *,
                              pod_node: bool,
                              fixture_member: str = 'realistic') -> dict:
    plan = launch_fixtures._capsule_raw()['objects'][sequence]
    return {
        'role': plan['role'],
        'kind': plan['kind'],
        'namespace': plan['namespace'],
        'name': plan['name'],
        'uid': _uid(sequence, fixture_member),
        'observed_semantic_sha256': plan['requested_semantic_sha256'],
        'server_allocations':
            (_allocations(sequence) if sequence != 2 or pod_node else []),
    }


def _partial_target_for_plans(count: int,
                              *,
                              pod_node: bool,
                              fixture_member: str = 'realistic') -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    plans = launch_fixtures._capsule_raw()['objects']
    return {
        'version': 1,
        'requested_target_sha256': target.sha256,
        'kubernetes_objects': [{
            'sequence': sequence,
            'role': plan['role'],
            'disposition': 'committed' if sequence < count else 'unknown',
            'object': (_resolved_object_for_plan(
                sequence, pod_node=pod_node, fixture_member=fixture_member)
                       if sequence < count else None),
        } for sequence, plan in enumerate(plans)],
    }


def _progress_resolved_target(fixture_member: str = 'realistic') -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    assert target.kubernetes is not None
    objects = [
        _resolved_object_for_plan(sequence,
                                  pod_node=True,
                                  fixture_member=fixture_member)
        for sequence in range(3)
    ]
    return {
        'version': 1,
        'requested_target_sha256': target.sha256,
        'provider_resource_id': f'pod/{target.kubernetes.workload_name}',
        'workload_uid': _uid(2, fixture_member),
        'kubernetes_objects': objects,
        'provider_operation_id': None,
        'resolved_at': progress_fixtures._TIME,
    }


def _prefix_observation_for_plans(count: int,
                                  *,
                                  complete: bool,
                                  fixture_member: str = 'realistic') -> dict:
    raw = progress_fixtures._prefix_observation(count, complete=complete)
    target = actions.ProviderLocatorV1.from_value(_target())
    assert target.kubernetes is not None
    plans = launch_fixtures._capsule_raw()['objects']
    raw['target_sha256'] = target.sha256
    for sequence, (observed,
                   plan) in enumerate(zip(raw['evidence']['objects'], plans)):
        observed['requested_semantic_sha256'] = plan[
            'requested_semantic_sha256']
        if sequence >= count:
            continue
        labels = {
            item['key']: item['value']
            for item in plan['required_identity_labels']
        }
        resolved = _resolved_object_for_plan(sequence,
                                             pod_node=complete or sequence != 2,
                                             fixture_member=fixture_member)
        observed.update({
            'uid': resolved['uid'],
            'cluster_name_label': labels['skypilot-cluster-name'],
            'cluster_record_uuid_label':
                labels['skypilot.co/cluster-record-uuid'],
            'replica_incarnation_label':
                labels['skypilot.co/serve-replica-incarnation'],
            'normalized_observed_semantic': copy.deepcopy(
                plan['requested_semantic']),
            'observed_semantic_sha256': plan['requested_semantic_sha256'],
            'server_allocations': resolved['server_allocations'],
        })
    raw['evidence_sha256'] = actions.canonical_sha256(raw['evidence'])
    if complete:
        resolved_target = _progress_resolved_target(fixture_member)
        raw.update({
            'observed_provider_resource_id':
                resolved_target['provider_resource_id'],
            'observed_cluster_record_uuid': str(target.sky_cluster_record_uuid),
            'observed_workload_uid': resolved_target['workload_uid'],
            'observed_replica_incarnation_label': str(
                target.kubernetes.replica_incarnation_label),
            'resolved_target': resolved_target,
        })
    return raw


def _normalized_create_effect(sequence: int,
                              claim: dict,
                              *,
                              pod_node: bool = True,
                              fixture_member: str = 'realistic') -> dict:
    plan = launch_fixtures._capsule_raw()['objects'][sequence]
    effect = progress_fixtures._create_effect(sequence, claim)
    effect.update({
        'request_body_sha256': plan['request_body_sha256'],
        'requested_semantic_sha256': plan['requested_semantic_sha256'],
        'object_at_commit': _resolved_object_for_plan(
            sequence, pod_node=pod_node, fixture_member=fixture_member),
    })
    return effect


def _partial_cursor_for_case(
        case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
        generation: int = 3,
        fixture_member: str = 'realistic') -> progress.ProviderLaunchProgressV1:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    action_id = identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    launch_attempt = (_MAX_RESOURCE_ACTION_ATTEMPT
                      if fixture_member == 'candidate_maximal' else 1)
    execution_generation = (_MAX_POSTGRES_BIGINT
                            if fixture_member == 'candidate_maximal' else 1)
    claim = progress_fixtures._claim(action_id,
                                     attempt=launch_attempt,
                                     generation=execution_generation,
                                     claimed_cursor_sha256='e' * 64)
    effects = [
        _normalized_create_effect(sequence,
                                  claim,
                                  pod_node=case.pod_node_allocation,
                                  fixture_member=fixture_member)
        for sequence in range(case.committed_object_count)
    ]
    if case.launch_phase == 'CREATE_INTENT':
        plan = launch_fixtures._capsule_raw()['objects'][
            case.committed_object_count]
        raw = {
            'version': 1,
            'action_kind': 'launch',
            'phase': case.launch_phase,
            'role': plan['role'],
            'intent_origin': claim,
            'committed_effects': effects,
            'known_objects': _partial_target_for_plans(
                case.committed_object_count,
                pod_node=False,
                fixture_member=fixture_member),
            'pre_observation': _prefix_observation_for_plans(
                case.committed_object_count,
                complete=False,
                fixture_member=fixture_member),
        }
    elif case.launch_phase == 'OBJECTS_PARTIAL':
        raw = {
            'version': 1,
            'action_kind': 'launch',
            'phase': case.launch_phase,
            'committed_effects': effects,
            'known_objects': _partial_target_for_plans(
                case.committed_object_count,
                pod_node=case.pod_node_allocation,
                fixture_member=fixture_member),
            'post_observation': _prefix_observation_for_plans(
                case.committed_object_count,
                complete=False,
                fixture_member=fixture_member),
        }
    elif case.launch_phase in ('OBJECTS_EXACT', 'HANDLE_INTENT',
                               'HANDLE_COMMITTED'):
        raw = progress_fixtures._launch_cursor(case.launch_phase, claim=claim)
    else:
        raw = progress_fixtures._advanced_launch_cursor(case.launch_phase,
                                                        claim=claim)
    if case.launch_phase not in ('CREATE_INTENT', 'OBJECTS_PARTIAL'):
        raw['committed_effects'][:3] = [
            _normalized_create_effect(sequence,
                                      claim,
                                      fixture_member=fixture_member)
            for sequence in range(3)
        ]
        raw['resolved_target'] = _progress_resolved_target(fixture_member)
        if case.launch_phase == 'OBJECTS_EXACT':
            raw['post_observation'] = _prefix_observation_for_plans(
                3, complete=True, fixture_member=fixture_member)
        if 'intended_handle' in raw:
            raw['intended_handle'] = _handle(fixture_member)
        if 'handle' in raw:
            raw['handle'] = _handle(fixture_member)
        if len(raw['committed_effects']) >= 4:
            handle_effect = progress_fixtures._handle_effect(claim)
            handle_effect['intended_handle'] = _handle(fixture_member)
            handle_effect['intended_handle_sha256'] = actions.canonical_sha256(
                handle_effect['intended_handle'])
            raw['committed_effects'][3] = handle_effect
        if len(raw['committed_effects']) == 5:
            raw['committed_effects'][4]['intent_origin'] = copy.deepcopy(claim)
            raw['committed_effects'][4][
                'evidence_commit_origin'] = copy.deepcopy(claim)
            if fixture_member == 'candidate_maximal':
                committed_job = raw['committed_effects'][4]['job_at_commit']
                committed_job['job_id'] = _MAX_POSTGRES_BIGINT
                if case.launch_phase == 'JOB_COMMITTED':
                    committed_job['run_epoch'] = _MAX_POSTGRES_BIGINT
                    committed_job['record_revision'] = _MAX_POSTGRES_BIGINT
                    raw['job'] = copy.deepcopy(committed_job)
        if 'runtime_evidence' in raw:
            raw['runtime_evidence']['pod_uid'] = _uid(2, fixture_member)
        if fixture_member == 'candidate_maximal' and 'job' in raw:
            raw['job']['job_id'] = _MAX_POSTGRES_BIGINT
            raw['job']['run_epoch'] = _MAX_POSTGRES_BIGINT
            raw['job']['record_revision'] = _MAX_POSTGRES_BIGINT
        if 'endpoint' in raw:
            raw['endpoint']['pod_uid'] = _uid(2, fixture_member)
            raw['endpoint']['provider_config_sha256'] = (
                actions.ProviderKubernetesHandleV1.from_value(
                    _handle(fixture_member)).provider_config_sha256)
    return progress.ProviderLaunchProgressV1.from_value(raw)


def _partial_source_for_case(
    case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
    generation: int = 3,
    fixture_member: str = 'realistic',
) -> tuple[actions.PartialLaunchCleanupBasisV1, actions.
           ProviderKubernetesCleanupTargetV1, progress.ProviderLaunchProgressV1,
           progress.ProviderLaunchSupersessionQuiescenceV1]:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    action_id = identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    cursor = _partial_cursor_for_case(case, generation, fixture_member)
    effects = [
        progress.ProviderLaunchEffectQuiescenceV1.from_committed(effect)
        for effect in cursor.committed_effects
    ]
    if cursor.is_intent:
        assert cursor.intent_origin is not None
        intent_sequence = cursor.current_intent_sequence
        assert intent_sequence is not None
        resolution = progress.ProviderLaunchNoEffectResolutionV1.from_value(
            progress_fixtures._call_not_entered(
                cursor.canonical_value(),
                cursor.intent_origin.canonical_value(), intent_sequence))
        effects.append(
            progress.ProviderLaunchEffectQuiescenceV1.from_resolution(
                resolution))
    launch_attempt = (_MAX_RESOURCE_ACTION_ATTEMPT
                      if fixture_member == 'candidate_maximal' else 1)
    quiescence = progress.ProviderLaunchSupersessionQuiescenceV1(
        launch_action_id=action_id,
        launch_attempt=launch_attempt,
        request_id=uuid.UUID(
            kernel_actions.request_id_for_attempt(action_id, launch_attempt)),
        handler_terminal_result_sha256='b' * 64,
        launch_provider_cursor_sha256=cursor.sha256,
        effects=tuple(effects),
        settled_at=_OBSERVED_AT)
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target(
            basis_kind='partial_launch_cleanup',
            committed_count=case.committed_object_count,
            exact_handle=(case.cluster_row_disposition
                          is actions.ProviderKubernetesClusterRowDispositionV1.
                          EXACT_HANDLE),
            pod_node=case.pod_node_allocation,
            fixture_member=fixture_member))
    basis = actions.PartialLaunchCleanupBasisV1.from_value({
        'version': 1,
        'basis_kind': 'partial_launch_cleanup',
        'source_store': 'api_resource_actions',
        'launch_action_id': str(action_id),
        'launch_attempt': launch_attempt,
        'launch_resource_identity': identity.canonical_value(),
        'launch_requested_target': _target(),
        'launch_resources': _resources(),
        'launch_workspace_identity': _workspace_identity(),
        'launch_provider_cursor_sha256': cursor.sha256,
        'launch_provider_progress_revision': (
            _MAX_POSTGRES_BIGINT if fixture_member == 'candidate_maximal' else 8
        ),
        'launch_quiescence_sha256': quiescence.sha256,
        'launch_cleanup_target_sha256': cleanup.sha256,
        'launch_immutable_spec_sha256': 'a' * 64,
        'exact_resources_override': True,
    })
    return basis, cleanup, cursor, quiescence


def _down_capsule_for_cleanup(
    cleanup: actions.ProviderKubernetesCleanupTargetV1,
) -> actions.ProviderKubernetesDownExecutionCapsuleV1:
    launch_capsule = launch_fixtures._capsule_raw()
    raw = {
        'version': 1,
        'implementation_contract': 'kubernetes_serve_exact_cleanup_v1',
        'executor_cohort': copy.deepcopy(launch_capsule['executor_cohort']),
        'config_projection': copy.deepcopy(launch_capsule['config_projection']),
        'config_projection_sha256': launch_capsule['config_projection_sha256'],
        'scope': copy.deepcopy(launch_capsule['scope']),
        'principals': copy.deepcopy(launch_capsule['principals']),
        'prerequisites': copy.deepcopy(launch_capsule['prerequisites']),
        'cleanup_target': cleanup.canonical_value(),
        'cleanup_target_sha256': cleanup.sha256,
        'mutation_contract': leaves._down_mutation(),
    }
    return actions.ProviderKubernetesDownExecutionCapsuleV1.from_value(raw)


def _down_capsule(
    basis: actions.PriorLaunchBasisV1,
    cleanup: actions.ProviderKubernetesCleanupTargetV1 | None = None,
) -> actions.ProviderKubernetesDownExecutionCapsuleV1:
    if cleanup is None:
        cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
            _cleanup_target(
                basis_kind=basis.basis_kind.value,
                committed_count=(3 if type(basis)
                                 is actions.CompletedLaunchBasisV1 else 0),
                exact_handle=type(basis) is actions.CompletedLaunchBasisV1))
    if cleanup.sha256 != basis.launch_cleanup_target_sha256:
        raise ValueError('fixture cleanup target does not match basis hash.')
    return _down_capsule_for_cleanup(cleanup)


def _policy_proof(
    boundary: str,
    capsule: actions.ProviderKubernetesDownExecutionCapsuleV1,
    subject: actions.ProviderDownPolicySubjectV1,
) -> actions.ProviderPolicyBoundaryProofV1:
    return actions.ProviderPolicyBoundaryProofV1(
        version=1,
        boundary=boundary,
        config_projection_sha256=capsule.config_projection_sha256,
        modes=actions.ProviderPolicyModeEvidenceV1(
            admin_policy_entrypoint=None,
            admin_policy_applied=False,
            managed_secrets_provider=None,
            managed_secret_reference_count=0),
        policy_subject_sha256=subject.sha256,
        projection_before_sha256=subject.sha256,
        projection_after_sha256=subject.sha256,
        projections_equal=True)


def down_execution_config(
    basis: actions.PriorLaunchBasisV1 | None = None,
    cleanup: actions.ProviderKubernetesCleanupTargetV1 | None = None,
) -> actions.ProviderKubernetesDownExecutionConfigV1:
    if basis is None:
        basis = actions.CompletedLaunchBasisV1.from_value(
            completed_basis_payload())
    capsule = _down_capsule(basis, cleanup)
    workspace = basis.launch_workspace_identity.workspace
    subject = actions.project_provider_down_policy_subject_v1(
        basis.launch_requested_target, workspace, basis, capsule)
    return actions.ProviderKubernetesDownExecutionConfigV1(
        version=1,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=_policy_proof('serve_controller_prepare', capsule, subject),
        executor=_policy_proof('api_executor_pre_io', capsule, subject))


def _down_invocation_payload_for_basis(
    basis: actions.PriorLaunchBasisV1,
    cleanup: actions.ProviderKubernetesCleanupTargetV1,
    *,
    generation: int,
) -> dict:
    config = down_execution_config(basis, cleanup)
    target = basis.launch_requested_target
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'down',
        'resource_identity': _resource_identity(generation),
        'requested_target': target.canonical_value(),
        'launch': None,
        'down': {
            'cluster_name': target.sky_cluster_name,
            'expected_cluster_record_uuid': str(target.sky_cluster_record_uuid),
            'workspace': basis.launch_workspace_identity.workspace,
            'prior_launch_basis': basis.canonical_value(),
            'execution_config': config.canonical_value(),
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        },
    }


def down_invocation_payload(*,
                            generation: int = 4,
                            partial: bool = False) -> dict:
    if generation <= 1:
        raise ValueError('down fixture generation must be greater than one.')
    basis = actions.prior_launch_basis_from_value_v1(
        partial_basis_payload(generation -
                              1) if partial else completed_basis_payload(
                                  generation=generation - 1))
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target(basis_kind=basis.basis_kind.value,
                        committed_count=0 if partial else 3,
                        exact_handle=not partial))
    return _down_invocation_payload_for_basis(basis,
                                              cleanup,
                                              generation=generation)


def down_plan_payload(*, generation: int = 4, partial: bool = False) -> dict:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        down_invocation_payload(generation=generation, partial=partial))
    basis = invocation.require_down().prior_launch_basis
    cleanup = invocation.require_down().execution_config.capsule.cleanup_target
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'action_kind': 'down',
        'resource_identity': invocation.resource_identity.canonical_value(),
        'placement_decision_sha256': 'e' * 64,
        'resources_snapshot_sha256': basis.launch_resources.sha256,
        'workspace_identity_sha256': basis.launch_workspace_identity.sha256,
        'requested_target': basis.launch_requested_target.canonical_value(),
        'prior_launch_basis_sha256': basis.sha256,
        'prior_cleanup_target_sha256': cleanup.sha256,
        'request_payload_sha256': invocation.sha256,
        'redaction_profile': 'provider_lifecycle_redaction_v1',
    }


def _down_spec_payload_for_basis(
    basis: actions.PriorLaunchBasisV1,
    cleanup: actions.ProviderKubernetesCleanupTargetV1,
    *,
    generation: int = 4,
) -> dict:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _down_invocation_payload_for_basis(basis,
                                           cleanup,
                                           generation=generation))
    plan = {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'action_kind': 'down',
        'resource_identity': invocation.resource_identity.canonical_value(),
        'placement_decision_sha256': 'e' * 64,
        'resources_snapshot_sha256': basis.launch_resources.sha256,
        'workspace_identity_sha256': basis.launch_workspace_identity.sha256,
        'requested_target': basis.launch_requested_target.canonical_value(),
        'prior_launch_basis_sha256': basis.sha256,
        'prior_cleanup_target_sha256': cleanup.sha256,
        'request_payload_sha256': invocation.sha256,
        'redaction_profile': 'provider_lifecycle_redaction_v1',
    }
    return {
        'version': 1,
        'provider_plan': plan,
        'invocation': invocation.canonical_value(),
    }


def test_completed_basis_and_down_config_roundtrip_freeze_exact_graph() -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    config = down_execution_config(basis)
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        down_invocation_payload())

    assert actions.prior_launch_basis_from_value_v1(
        basis.canonical_value()).canonical_bytes == basis.canonical_bytes
    assert actions.ProviderKubernetesDownExecutionConfigV1.from_value(
        config.canonical_value()).canonical_bytes == config.canonical_bytes
    assert invocation.require_down().prior_launch_basis == basis
    assert invocation.sha256 == actions.canonical_sha256(
        invocation.canonical_value())


@pytest.mark.parametrize(('mutate', 'match'), [
    (lambda target: target['kubernetes_objects'].pop(), 'exactly three'),
    (lambda target: target['kubernetes_objects'].reverse(),
     'canonical role order'),
    (lambda target: target['kubernetes_objects'][0].update(
        {'server_allocations': []}), 'complete Service allocation quartet'),
])
def test_completed_resolved_target_requires_complete_three_object_api006_shape(
        mutate, match: str) -> None:
    value = _resolved_target()
    mutate(value)

    with pytest.raises(ValueError, match=match):
        actions.ResolvedProviderTargetV1.from_value(value)


@pytest.mark.parametrize(('mutate', 'match'), [
    (lambda target: target['kubernetes_objects'][0].update(
        {'uid': 'crossed-head-ssh-service-uid'}), 'resolved object identity'),
    (lambda target: target['kubernetes_objects'][2]['server_allocations'][0].
     update({'value': 'crossed-worker-node'}), 'resolved Pod allocation'),
])
def test_completed_basis_binds_resolved_target_to_exact_launch_handle(
        mutate, match: str) -> None:
    value = completed_basis_payload()
    resolved = value['launch_resolved_target']
    mutate(resolved)
    value['launch_resolved_target_sha256'] = actions.canonical_sha256(resolved)

    with pytest.raises(ValueError, match=match):
        actions.CompletedLaunchBasisV1.from_value(value)


def test_completed_down_binds_resolved_target_to_cleanup_source_evidence(
) -> None:
    value = completed_basis_payload()
    resolved = value['launch_resolved_target']
    resolved['kubernetes_objects'][0]['observed_semantic_sha256'] = 'b' * 64
    value['launch_resolved_target_sha256'] = actions.canonical_sha256(resolved)
    basis = actions.CompletedLaunchBasisV1.from_value(value)

    with pytest.raises(ValueError, match='object evidence'):
        down_execution_config(basis)


def test_partial_basis_roundtrip_preserves_retained_source_hashes() -> None:
    basis = actions.PartialLaunchCleanupBasisV1.from_value(
        partial_basis_payload())
    config = down_execution_config(basis)
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        down_invocation_payload(partial=True))

    assert basis.launch_provider_progress_revision == 8
    assert basis.launch_provider_cursor_sha256 == (
        partial_basis_payload()['launch_provider_cursor_sha256'])
    assert basis.launch_quiescence_sha256 == (
        partial_basis_payload()['launch_quiescence_sha256'])
    assert config.policy_subject.prior_launch_basis_sha256 == basis.sha256
    assert invocation.require_down().prior_launch_basis.canonical_bytes == (
        basis.canonical_bytes)


@pytest.mark.parametrize(
    'removed_preimage',
    ['launch_provider_cursor', 'launch_quiescence', 'launch_cleanup_target'])
def test_partial_basis_forbids_retained_source_preimages(
        removed_preimage: str) -> None:
    value = partial_basis_payload()
    value[removed_preimage] = {}

    with pytest.raises(ValueError, match='unknown or missing'):
        actions.PartialLaunchCleanupBasisV1.from_value(value)


def test_partial_basis_locally_accepts_hash_then_invocation_binds_target(
) -> None:
    value = partial_basis_payload()
    value['launch_cleanup_target_sha256'] = '0' * 64
    basis = actions.PartialLaunchCleanupBasisV1.from_value(value)

    assert basis.launch_cleanup_target_sha256 == '0' * 64
    invocation = down_invocation_payload(partial=True)
    invocation['down']['prior_launch_basis'] = basis.canonical_value()
    with pytest.raises(ValueError, match='cleanup target hash'):
        actions.ProviderLifecycleInvocationV1.from_value(invocation)


def test_completed_down_spec_literal_size_and_hash_goldens() -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target())
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))
    config = spec.invocation.require_down().execution_config

    assert (len(basis.canonical_bytes), basis.sha256) == (
        7_705,
        '3b2db54341018cc4b3739580bf84679a31058c032bbaec1161db81eb04f83a44')
    assert (len(cleanup.canonical_bytes), cleanup.sha256) == (
        8_517,
        '9dc1c6a25a3a8257f3117a258e9d0772f3e6d5124eaf4a449bfbbcfe901fd091')
    assert (len(config.capsule.canonical_bytes), config.capsule.sha256) == (
        22_324,
        '3f5f36e80500059c40c1bc8039782384a12087bbd1751e874a63521e5ab9eb89')
    assert (len(config.canonical_bytes), config.sha256) == (
        27_170,
        '68a130f1d86147131da9faa90847389f5b9db67c4c7cb8ce61ad5b37e90e8476')
    assert (len(spec.invocation.canonical_bytes), spec.invocation.sha256) == (
        38_423,
        '109c7977907a5bae42071488ae7aca264c8f3d58b3597d8010144c20124a6a12')
    assert (len(
        spec.provider_plan.canonical_bytes), spec.provider_plan.sha256) == (
            3_878,
            'ba7d780fe40336fd40f1acfb288fabb7f08c5ea6ad3929d85a285b3146e12bcf')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        42_345,
        '6d5ea9549eb0e9679f3ba4719a27c022e6799ea941420205d051063521a98d2b')
    assert len(spec.canonical_bytes) <= 60_000


def test_partial_down_legal_shape_manifest_is_production_owned_and_frozen(
) -> None:
    manifest = [case.canonical_value() for case in _PARTIAL_DOWN_CASES]

    assert _PARTIAL_DOWN_CASES is (
        actions.PROVIDER_PARTIAL_LAUNCH_CLEANUP_LEGAL_SHAPE_MANIFEST_V1)
    assert len(_PARTIAL_DOWN_CASES) == 20
    assert (
        len(actions.canonical_json_bytes(manifest)),
        actions.canonical_sha256(manifest)) == (
            3_307,
            '2e939c18fdc72acd06f8ded8cc338d77cc59f24a61ec23ddf3afd6de4575c29a')
    case_ids = {case.case_id for case in _PARTIAL_DOWN_CASES}
    assert set(_PARTIAL_DOWN_SPEC_GOLDEN_MANIFEST_V1) == set(_FIXTURE_MEMBERS)
    assert all(
        set(member) == case_ids
        for member in _PARTIAL_DOWN_SPEC_GOLDEN_MANIFEST_V1.values())


@pytest.mark.parametrize('fixture_member', _FIXTURE_MEMBERS)
@pytest.mark.parametrize('case',
                         _PARTIAL_DOWN_CASES,
                         ids=lambda case: case.case_id)
def test_every_legal_partial_down_spec_has_exact_golden(
        case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
        fixture_member: str) -> None:
    basis, cleanup, cursor, quiescence = _partial_source_for_case(
        case, fixture_member=fixture_member)
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))
    expected_size, expected_sha256 = (
        _PARTIAL_DOWN_SPEC_GOLDEN_MANIFEST_V1[fixture_member][case.case_id])

    assert basis.launch_provider_cursor_sha256 == cursor.sha256
    assert basis.launch_quiescence_sha256 == quiescence.sha256
    assert basis.launch_cleanup_target_sha256 == cleanup.sha256
    assert spec.provider_plan.prior_launch_basis_sha256 == basis.sha256
    assert spec.provider_plan.prior_cleanup_target_sha256 == cleanup.sha256
    assert (len(spec.canonical_bytes), spec.sha256) == (expected_size,
                                                        expected_sha256)
    assert len(cursor.canonical_bytes) <= 65_536
    assert len(quiescence.canonical_bytes) <= 65_536
    assert len(spec.canonical_bytes) <= 60_000
    assert len(spec.canonical_bytes) <= 65_536


def test_realistic_handle_committed_partial_down_component_goldens() -> None:
    case = next(case for case in _PARTIAL_DOWN_CASES
                if case.case_id == 'handle_committed_exact_handle')
    basis, cleanup, cursor, quiescence = _partial_source_for_case(case)
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))
    config = spec.invocation.require_down().execution_config

    assert (len(cursor.canonical_bytes), cursor.sha256) == (
        28_716,
        'a80e68912a6ea6954b597ec941e4b37cfbdf9aee67621d3b944fe0735a38f619')
    assert (len(quiescence.canonical_bytes), quiescence.sha256) == (
        27_607,
        '1e986d87a79095206339bbc995bb14d26f43bb4cee2e5aa82906dc0c0782d2ea')
    assert (len(basis.canonical_bytes), basis.sha256) == (
        5_139,
        '779e1bcd43c78def8c237e6f437359a5929f590cd673d762c4e1302c794f7091')
    assert (len(cleanup.canonical_bytes), cleanup.sha256) == (
        8_523,
        'b567cad6a75392e1ab1936548c210b5e7efc951f9683a942db7c475ed907958c')
    assert (len(config.capsule.canonical_bytes), config.capsule.sha256) == (
        22_330,
        '39348bc394ba7ad76694ad417bb88871c36ec3c6c24ef5f7a1b70f78303a8c10')
    assert (len(config.canonical_bytes), config.sha256) == (
        27_176,
        '0fbb89d9d2932d6472709de084e3bdd9a8d4604fda0353399ea2e145483f7b9b')
    assert (len(spec.invocation.canonical_bytes), spec.invocation.sha256) == (
        35_863,
        'd849bbd46651017b7e374bd04e4016b2071cab3948f2fcb6d12e5fae93e1d652')
    assert (len(
        spec.provider_plan.canonical_bytes), spec.provider_plan.sha256) == (
            3_878,
            '6aa1b27d5a8226e92968780dba37a193c8fcd86f4c61bffdffd241e228edecbf')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        39_785,
        '1061c2f9313c78f43080863ad76f6137608a1fcee79b2645464d75b39a024af6')


def test_candidate_maximal_partial_down_component_goldens() -> None:
    case = next(case for case in _PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    basis, cleanup, cursor, quiescence = _partial_source_for_case(
        case, fixture_member='candidate_maximal')
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))

    assert basis.launch_attempt == _MAX_RESOURCE_ACTION_ATTEMPT
    assert basis.launch_provider_progress_revision == _MAX_POSTGRES_BIGINT
    assert all(item.committed_uid is not None and
               len(item.committed_uid.encode('utf-8')) == 1_024
               for item in cleanup.objects)
    assert cursor.job is not None
    assert cursor.job.job_id == _MAX_POSTGRES_BIGINT
    assert cursor.job.run_epoch == _MAX_POSTGRES_BIGINT
    assert cursor.job.record_revision == _MAX_POSTGRES_BIGINT
    assert cursor.endpoint is not None
    assert len(cursor.endpoint.pod_uid.encode('utf-8')) == 1_024
    assert (len(cursor.canonical_bytes), cursor.sha256) == (
        56_319,
        '091f900c8441e30ee25e55136721e29606e4481c25207091c5fca3f1bea3049e')
    assert (len(quiescence.canonical_bytes), quiescence.sha256) == (
        41_704,
        '0c1b3e94ae7606aa694c3b888322a9d0cb5a6df9b2ab006612bfe5907d30d2d0')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        45_860,
        '42c3ab325e41ef90603c4a9dc2444b6a4730e7e39b4901153b76216164ec542b')


def test_serve_spec_parser_keeps_absolute_65536_byte_bound() -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target())
    oversized = _down_spec_payload_for_basis(basis, cleanup)
    oversized['invocation']['down']['workspace'] = 'w' * 30_000

    assert len(actions.canonical_json_bytes(oversized)) > 65_536
    with pytest.raises(ValueError, match='exceeds 65536'):
        actions.ServeReplicaActionSpecV1.from_value(oversized)


@pytest.mark.parametrize('field,match', [
    ('prior_launch_basis_sha256', 'prior launch basis hash'),
    ('prior_cleanup_target_sha256', 'cleanup target hash'),
    ('resources_snapshot_sha256', 'resources hash'),
    ('workspace_identity_sha256', 'workspace hash'),
])
def test_down_plan_recomputes_every_hash_only_commitment(
        field: str, match: str) -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target())
    value = _down_spec_payload_for_basis(basis, cleanup)
    value['provider_plan'][field] = '0' * 64

    with pytest.raises(ValueError, match=match):
        actions.ServeReplicaActionSpecV1.from_value(value)


def test_down_spec_contains_only_one_basis_and_cleanup_target_preimage(
) -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target())
    value = _down_spec_payload_for_basis(basis, cleanup)

    def count_key(item: object, key: str) -> int:
        if isinstance(item, dict):
            return int(key in item) + sum(
                count_key(child, key) for child in item.values())
        if isinstance(item, list):
            return sum(count_key(child, key) for child in item)
        return 0

    assert count_key(value, 'prior_launch_basis') == 1
    assert count_key(value, 'cleanup_target') == 1
    assert count_key(value, 'prior_cleanup_target') == 0
    plan = value['provider_plan']
    assert plan['prior_launch_basis_sha256'] == basis.sha256
    assert plan['prior_cleanup_target_sha256'] == cleanup.sha256
    assert 'launch_cleanup_target' not in value['invocation']['down'][
        'prior_launch_basis']


@pytest.mark.parametrize('forbidden', [
    'renderer', 'request_identity', 'endpoint', 'objects', 'resources',
    'post_provision', 'scheduling', 'storage', 'metadata', 'security',
    'topology'
])
def test_down_capsule_rejects_launch_only_or_runtime_fields(
        forbidden: str) -> None:
    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())
    raw = _down_capsule(basis).canonical_value()
    raw[forbidden] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesDownExecutionCapsuleV1.from_value(raw)


def test_down_invocation_rejects_crossed_outer_cleanup_copy() -> None:
    value = down_invocation_payload()
    crossed_basis = completed_basis_payload()
    crossed_basis['launch_cleanup_target_sha256'] = '0' * 64
    value['down']['prior_launch_basis'] = crossed_basis
    with pytest.raises(ValueError, match='cleanup target hash'):
        actions.ProviderLifecycleInvocationV1.from_value(value)


def test_down_invocation_and_plan_require_next_generation() -> None:
    invocation = down_invocation_payload()
    invocation['resource_identity']['desired_generation'] = 5
    with pytest.raises(ValueError, match='immediately follow'):
        actions.ProviderLifecycleInvocationV1.from_value(invocation)

    spec = _down_spec_payload_for_basis(
        actions.CompletedLaunchBasisV1.from_value(completed_basis_payload()),
        actions.ProviderKubernetesCleanupTargetV1.from_value(_cleanup_target()))
    spec['provider_plan']['resource_identity']['desired_generation'] = 5
    with pytest.raises(ValueError, match='action IDs differ'):
        actions.ServeReplicaActionSpecV1.from_value(spec)


@pytest.mark.parametrize('field',
                         ['service_identity', 'replica_id', 'workspace'])
def test_down_invocation_binds_stable_identity_and_workspace(
        field: str) -> None:
    value = down_invocation_payload()
    if field == 'service_identity':
        replacement = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
        value['resource_identity']['service_hash'] = replacement
        value['resource_identity']['service_incarnation'] = replacement
        match = 'prior launch identity'
    elif field == 'replica_id':
        value['resource_identity']['replica_id'] = 8
        match = 'prior launch identity'
    else:
        value['down']['workspace'] = 'crossed-workspace'
        match = 'workspace'

    with pytest.raises(ValueError, match=match):
        actions.ProviderLifecycleInvocationV1.from_value(value)


def test_prior_basis_parsers_reject_wrong_variant_and_nested_subclasses(
) -> None:
    completed = completed_basis_payload()
    completed['basis_kind'] = 'partial_launch_cleanup'
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.prior_launch_basis_from_value_v1(completed)

    basis = actions.CompletedLaunchBasisV1.from_value(completed_basis_payload())

    class EvilHandle(actions.ProviderKubernetesHandleV1):
        pass

    handle = basis.launch_handle
    evil = EvilHandle(
        **{
            field.name: getattr(handle, field.name)
            for field in dataclasses.fields(handle)
        })
    with pytest.raises(TypeError, match='launch_handle'):
        dataclasses.replace(basis, launch_handle=evil)
