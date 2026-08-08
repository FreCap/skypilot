"""Prior-launch basis and exact Kubernetes down execution-config tests."""

# pylint: disable=protected-access

import copy
import dataclasses

import pytest
import test_serve_resource_action_capsule_leaves as leaves
import test_serve_resource_action_launch_execution_config

from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

launch_fixtures = test_serve_resource_action_launch_execution_config
_OBSERVED_AT = '2026-08-01T05:06:07.123456Z'
_FIXTURE_MEMBERS = ('realistic', 'candidate_maximal')
_MAX_RESOURCE_ACTION_ATTEMPT = 2**31 - 1
_MAX_POSTGRES_BIGINT = 2**63 - 1
_FROZEN_PARTIAL_CURSOR_SHA256 = (
    'b677c6730805518d50b4d5e03cb1036d93a9143467763bdd78592338e20d2ccf')
_FROZEN_PARTIAL_QUIESCENCE_SHA256 = (
    'e29abbe8c150422ee597ade7e63a65617d381f5026704c30fce19b777d9b108a')


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


def partial_basis_payload(generation: int = 3) -> dict:
    identity = actions.ProviderResourceIdentityV1.from_value(
        _resource_identity(generation))
    action_id = identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
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
        # The retained shared schema stores only these frozen commitments.
        # Their retired provider-progress preimages are deliberately not
        # reconstructed as fixtures.
        'launch_provider_cursor_sha256': _FROZEN_PARTIAL_CURSOR_SHA256,
        'launch_provider_progress_revision': 8,
        'launch_quiescence_sha256': _FROZEN_PARTIAL_QUIESCENCE_SHA256,
        'launch_cleanup_target_sha256': cleanup.sha256,
        'launch_immutable_spec_sha256': 'a' * 64,
        'exact_resources_override': True,
    }


_PARTIAL_DOWN_CASES = (
    actions.enumerate_provider_partial_launch_cleanup_legal_shapes_v1())


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
        'resolved_at': '2026-08-01T01:02:03.000004Z',
    }


def _partial_source_for_case(
    case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
    *,
    generation: int = 3,
    fixture_member: str = 'realistic',
) -> tuple[actions.PartialLaunchCleanupBasisV1,
           actions.ProviderKubernetesCleanupTargetV1]:
    """Build retained partial-down inputs without retired progress preimages."""
    launch_attempt = (_MAX_RESOURCE_ACTION_ATTEMPT
                      if fixture_member == 'candidate_maximal' else 1)
    progress_revision = (_MAX_POSTGRES_BIGINT
                         if fixture_member == 'candidate_maximal' else 8)
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        _cleanup_target(
            basis_kind='partial_launch_cleanup',
            committed_count=case.committed_object_count,
            exact_handle=(case.cluster_row_disposition
                          is actions.ProviderKubernetesClusterRowDispositionV1.
                          EXACT_HANDLE),
            pod_node=case.pod_node_allocation,
            fixture_member=fixture_member))
    raw = partial_basis_payload(generation)
    raw.update({
        'launch_attempt': launch_attempt,
        'launch_provider_progress_revision': progress_revision,
        'launch_cleanup_target_sha256': cleanup.sha256,
    })
    basis = actions.PartialLaunchCleanupBasisV1.from_value(raw)
    return basis, cleanup


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
        7_727,
        'fd7c9d45f9cd1145d32a11ad74b7570d71d4fe687376c398f9ff13a6980e2322')
    assert (len(cleanup.canonical_bytes), cleanup.sha256) == (
        11_190,
        '185b254247d8149b3b042cfa7d8d20a3e18b182039198779a57aaa26e502f33e')
    assert (len(config.capsule.canonical_bytes), config.capsule.sha256) == (
        28_843,
        'a3c87b4ea539ace88eca79c896658900169695ef71201be64a18b44f6ebccc08')
    assert (len(config.canonical_bytes), config.sha256) == (
        33_700,
        '511718d8f4402f7803752361c93881312f6d19eb5b11ab338b75df1702785243')
    assert (len(spec.invocation.canonical_bytes), spec.invocation.sha256) == (
        44_986,
        'ec71a0291f865694addeb307ec0e1184d3864b895c0a59af9bd5bb389ee5a2d8')
    assert (len(
        spec.provider_plan.canonical_bytes), spec.provider_plan.sha256) == (
            3_889,
            'e8e78370fea6c50c030e3e693430aafc59548197ab812eeb36557f4ac157f9f9')
    assert (len(spec.canonical_bytes), spec.sha256) == (
        48_919,
        'e51205dd1e6d7d44e2c92eb148290bc183deef25cadd97bc39c1d3716c7a5a68')
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


@pytest.mark.parametrize('fixture_member', _FIXTURE_MEMBERS)
@pytest.mark.parametrize('case',
                         _PARTIAL_DOWN_CASES,
                         ids=lambda case: case.case_id)
def test_every_legal_partial_down_shape_roundtrips_without_progress_preimages(
        case: actions.ProviderPartialLaunchCleanupLegalShapeV1,
        fixture_member: str) -> None:
    basis, cleanup = _partial_source_for_case(case,
                                              fixture_member=fixture_member)
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))
    parsed_basis = spec.invocation.require_down().prior_launch_basis
    parsed_cleanup = spec.invocation.require_down(
    ).execution_config.capsule.cleanup_target

    assert type(parsed_basis) is actions.PartialLaunchCleanupBasisV1
    assert parsed_basis.canonical_bytes == basis.canonical_bytes
    assert parsed_cleanup.canonical_bytes == cleanup.canonical_bytes
    assert basis.launch_provider_cursor_sha256 == (
        _FROZEN_PARTIAL_CURSOR_SHA256)
    assert basis.launch_quiescence_sha256 == (_FROZEN_PARTIAL_QUIESCENCE_SHA256)
    assert basis.launch_cleanup_target_sha256 == cleanup.sha256
    assert spec.provider_plan.prior_launch_basis_sha256 == basis.sha256
    assert spec.provider_plan.prior_cleanup_target_sha256 == cleanup.sha256
    assert spec.canonical_bytes == actions.canonical_json_bytes(
        spec.canonical_value())
    assert len(spec.canonical_bytes) <= 60_000
    assert len(spec.canonical_bytes) <= 65_536


def test_candidate_maximal_partial_down_stays_within_wire_bounds() -> None:
    case = next(case for case in _PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    basis, cleanup = _partial_source_for_case(
        case, fixture_member='candidate_maximal')
    spec = actions.ServeReplicaActionSpecV1.from_value(
        _down_spec_payload_for_basis(basis, cleanup))

    assert basis.launch_attempt == _MAX_RESOURCE_ACTION_ATTEMPT
    assert basis.launch_provider_progress_revision == _MAX_POSTGRES_BIGINT
    assert all(item.committed_uid is not None and
               len(item.committed_uid.encode('utf-8')) == 1_024
               for item in cleanup.objects)
    assert len(spec.canonical_bytes) <= 60_000
    assert len(spec.canonical_bytes) <= 65_536


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
