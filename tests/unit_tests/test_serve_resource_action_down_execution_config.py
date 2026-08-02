"""Prior-launch basis and exact Kubernetes down execution-config tests."""

# pylint: disable=protected-access

import copy
import dataclasses
import uuid

import pytest
import test_serve_resource_action_capsule_leaves as leaves
import test_serve_resource_action_launch_execution_config
import test_serve_resource_action_progress

from sky.serve import resource_action_progress as progress
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

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
            (40902,
             'c3ca60be204c5cdaae0858615ce0651c30ce36725305b4a3f725d991a738e1ac'
            ),
        'objects_partial_1_not_found':
            (41245,
             '90770c254f142f1c0fcd1ad1ca4e9c3b03a7eed682400d109ede0545c7b8a7d9'
            ),
        'create_intent_1_not_found':
            (41245,
             '4f0843ce69d8b58c33dd72a11e20e8ba04c960ef5839fd8263d6e914c4658d3a'
            ),
        'objects_partial_2_not_found':
            (41576,
             'f65522552fcc3d9f75e068c727cfa842e6547c798f26cd7a5b17d5bd457ec4b2'
            ),
        'create_intent_2_not_found':
            (41576,
             '6ef439f6129f354e11c49040931650a451b879d917dbfd37eff519fb38efe28e'
            ),
        'objects_partial_3_unscheduled_not_found':
            (41586,
             '451c21ae311a277b175f6c83642483f287c602ced07a3a534e00c754087717d6'
            ),
        'objects_exact_not_found':
            (41667,
             '974d64610580aef4fdfcc51d2f6ab23f9e181409009dd2808d78325d73997d15'
            ),
        'handle_intent_not_found':
            (41667,
             '091a086a0c4bb40f646fbea98088c5321cac26b98bf0ea730af7f9278dc640c3'
            ),
        'handle_committed_not_found':
            (41667,
             '3899af5c151589a461fa079947b86982874785d1ef4ebc8aa0fea04b19ae8a77'
            ),
        'handle_committed_exact_handle':
            (42528,
             'e5101e78369455c8c88919674561cbbe229c84cb10f53d40bb78aec47db1fd1c'
            ),
        'runtime_ready_not_found':
            (41667,
             '3979e4cd73fd7ae758986bba041d0bfebd8bf3945c76100bd57e56afeb746186'
            ),
        'runtime_ready_exact_handle':
            (42528,
             '83636bcbd2c51669a5f9dc2c899f5ee3e88c7c6bb8d472aae85e56790b98fb3e'
            ),
        'job_intent_not_found':
            (41667,
             '65e6520fcef3b71cb8128ad9b1ab1c76caddffb55b38abd21efc2d7dd3f9327b'
            ),
        'job_intent_exact_handle':
            (42528,
             '157829fe6fcb74e66504df3ffc1cf64ac3dc3f9aa7589de26f57db014fab589a'
            ),
        'job_committed_not_found':
            (41667,
             'a49450742e8ad05a130ccf8dbba7f1ed9850b4495ae6cf103127d6d89da12be8'
            ),
        'job_committed_exact_handle':
            (42528,
             'cf1450c232d09ac7f954ae3832f05293ae7d9bd0811e148b82ff0aa87197a727'
            ),
        'job_running_not_found':
            (41667,
             '4346bba59c134692a0826d3e9978478dacdf996bc79bd35ecf74e6ff23d3aa24'
            ),
        'job_running_exact_handle':
            (42528,
             '74ba0982994e3f5961c5b31d82193d276d67299d459eb9bc50d74ba26bb325d2'
            ),
        'endpoint_resolved_not_found':
            (41667,
             '1223070789777fbc695a87732e7dc02afd7a025dc7950bd68f2b2eabf73320c4'
            ),
        'endpoint_resolved_exact_handle':
            (42528,
             '9a4c2a674a4ad0758ad30f7b0013af59f7c8868ccd5a2ed11dace135b15d2115'
            ),
    },
    'candidate_maximal': {
        'create_intent_0_not_found':
            (40929,
             '943fdb0767d09b148c5686bae959c1975b26c11edb528108f17d1266bdba6c67'
            ),
        'objects_partial_1_not_found':
            (42276,
             'a6804d63efcd8e95ba9c673077fdb29a087d61c432dd120a32316534d5d16f52'
            ),
        'create_intent_1_not_found':
            (42276,
             '6bbbd6058d241fb6e757f0da35492147d1a3ecea018ff83941b3e3bdabebebb1'
            ),
        'objects_partial_2_not_found':
            (43615,
             'c171ed9be5b68a7112df0c064c136527229dfa98fb2ab883c2f8bc3ed50b167a'
            ),
        'create_intent_2_not_found':
            (43615,
             'a17449a9fede611f5a50b8940f4a72b68e7960b0b07d12cb89e8ec4a454eadbb'
            ),
        'objects_partial_3_unscheduled_not_found':
            (44637,
             'ed97ac6a68f6bc39470806aa51d5b5b89a22a2e3875e6257e8d6477189a27484'
            ),
        'objects_exact_not_found':
            (44718,
             'abda7acfa130801ff6c2218edaa561570cdaee9163836a5dd3cf6f06ad0ff02f'
            ),
        'handle_intent_not_found':
            (44718,
             '09ffb85c73b79282c56d96a3226fd09f2c5dc35b2fa3683b49ead0f4dbaf867b'
            ),
        'handle_committed_not_found':
            (44718,
             '6b5342f2954c59b688a9176d873ece8b778b765c92153a4994e01bb02b301a8d'
            ),
        'handle_committed_exact_handle':
            (48603,
             '03b507aede0721a5189687a2315de1e08f97549000aac47df36bdb65578ba2cc'
            ),
        'runtime_ready_not_found':
            (44718,
             '5951215d10dcfe7ab9fb9513f99f786d9ec80c95730b2963aef442587da3c16c'
            ),
        'runtime_ready_exact_handle':
            (48603,
             '5acd2c4edfa4efc109bea49ff91fc3709ecbd46f7d769cfb9306fd1c02051b1c'
            ),
        'job_intent_not_found':
            (44718,
             '9a9636903a9653dd06ab4e2c59971edf520a4e8abd2060e109fe62935a17b250'
            ),
        'job_intent_exact_handle':
            (48603,
             '73193bb83ddf96ed096e76789196ad1def97c8f18652ec57fef804498f4db98b'
            ),
        'job_committed_not_found':
            (44718,
             '7126ac1d0bfec4bbac7f1f70564484a4ff324f57efff409d458bfbe645eeb993'
            ),
        'job_committed_exact_handle':
            (48603,
             '4036710b64d408b349edf5c00d06abb7852a9d87c36a153a5b7c0a35aaaecad5'
            ),
        'job_running_not_found':
            (44718,
             'b1264bae9df8c2829bf81a218a08166087893fcfe075bc3f58491ed4f53aa49a'
            ),
        'job_running_exact_handle':
            (48603,
             '8905715e2acf2722e94e8f4bc95aa8c8fc7435541e59ebc8b5f8e8a61dd9f7cd'
            ),
        'endpoint_resolved_not_found':
            (44718,
             '7c578cf5cfb7a9d8bbc3829a8125be8ee500eb2edde0ab736d49d03b97093b45'
            ),
        'endpoint_resolved_exact_handle':
            (48603,
             '198a9edd1a8f7cdde06668257ef9968248ee8861d17b6f7c900a69332e68a7d0'
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
        'b2c1a2521aed918a3a226072bad4b6b1a598b8dd8e9da906087e20f36be45838')
    assert (len(cleanup.canonical_bytes), cleanup.sha256) == (
        11_190,
        '62f5070ee0db6df4742bfeb2e8fcc4c52ea0b98f803aed6cd1f5c2c405aa74d8')
    assert (len(config.capsule.canonical_bytes), config.capsule.sha256) == (
        25_067,
        '598eac1ff12dc289389bcb5707a23e0107668570ffa6df57dbae8b0894a66c1e')
    assert (len(config.canonical_bytes), config.sha256) == (
        29_913,
        'fab84be9a0d31b8cc94220a8ffe531e134766eb7f15bd9d6d174bdd6d4b27dbb')
    assert (len(spec.invocation.canonical_bytes), spec.invocation.sha256) == (
        41_166,
        '9c1977d999d30373862677056855355e905a0c9fa61eee308e60d8bd6dbcf980')
    assert (len(
        spec.provider_plan.canonical_bytes), spec.provider_plan.sha256) == (
            3_878,
            'b15b8be415ddac7b730f6844d534ffce3b990c66c77d23fff18cf6acd3e69c5a')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        45_088,
        'a04de4329e36779997b4c99845f7eac68424b5d19c8792120717f73ec90d2dd7')
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
        '3ecec5f09161a670cf079d69b698ec697f096e746d62d306404c4f4414e5d1d2')
    assert (len(quiescence.canonical_bytes), quiescence.sha256) == (
        27_607,
        '9ace94abed0fd8031f67781c1b0f048363039a90a5b83624ae7fc83d88dd2567')
    assert (len(basis.canonical_bytes), basis.sha256) == (
        5_139,
        '8e84f10e712170f4b6807f14d2d973d35b2948edcccbd77b0636290bd218f2f9')
    assert (len(cleanup.canonical_bytes), cleanup.sha256) == (
        11_196,
        'c5db037b65c5373b5927d90454743bf1ca10513653b4b9cc19f8fb3d89309e38')
    assert (len(config.capsule.canonical_bytes), config.capsule.sha256) == (
        25_073,
        '99900a7e0cb9fd5cc8055d81427ae0b93912b1c904ba97b10aba540d63c5daf2')
    assert (len(config.canonical_bytes), config.sha256) == (
        29_919,
        '672274d16176bd3c01f73f2fae3efd809f03215cec8fc16899713052c0a74e90')
    assert (len(spec.invocation.canonical_bytes), spec.invocation.sha256) == (
        38_606,
        '1e322001b96a17156dfe69b2f3538473dfb4dee8142e52d20a42d0cd7801d71e')
    assert (len(
        spec.provider_plan.canonical_bytes), spec.provider_plan.sha256) == (
            3_878,
            '476edbe55250fa5445969f089e07471424256b124de4267fe5d194bafe646390')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        42_528,
        'e5101e78369455c8c88919674561cbbe229c84cb10f53d40bb78aec47db1fd1c')


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
        'e5a8458b8a4781b2f4c833b2dc499407883399c6195c3d0edf18087ba230f1af')
    assert (len(quiescence.canonical_bytes), quiescence.sha256) == (
        41_704,
        '94babbb7a9f587e5b4fa22329a0405fdeadc1c8d1dc407819f5711d3445bde8c')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        48_603,
        '198a9edd1a8f7cdde06668257ef9968248ee8861d17b6f7c900a69332e68a7d0')


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
