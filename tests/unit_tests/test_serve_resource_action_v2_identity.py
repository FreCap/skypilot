"""Pure M4 service-version identity and V2 action-envelope tests."""
# pylint: disable=protected-access

import hashlib
import inspect
import pathlib
import uuid

import pytest
import serve_resource_action_test_fixtures as action_fixtures
import test_serve_resource_actions as v1_fixtures

import sky
from sky import task as task_lib
from sky.serve import resource_action_authority as authority_contracts
from sky.serve import resource_action_identity as identity_projector
from sky.serve import resource_actions as actions

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_OTHER_SERVICE_UUID = '44444444-4444-4444-8444-444444444444'
_SHADOW_EPOCH = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
_POLICY_EPOCH = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
_YAML_OMITTED_DEFAULTS = '''\
service:
  readiness_probe: /health
  replicas: 1
resources:
  infra: kubernetes
run: python app.py
'''
_YAML_EXPLICIT_DEFAULTS = '''\
service:
  readiness_probe:
    path: /health
    initial_delay_seconds: 1200
    timeout_seconds: 15
    endpoint_probe_interval_seconds: 10
  load_balancer:
    stream_timeout_seconds: 120
  replica_policy:
    min_replicas: 1
resources:
  infra: kubernetes
  disk_size: 256
num_nodes: 1
run: python app.py
'''


def _capacity_profile() -> dict:
    return {
        'version': 1,
        'profile': 'ordinary_ondemand_physical_width1_v1',
        'pool': False,
        'replica_unit': 'physical_backend',
        'planned_capacity': 1,
        'node_count': 1,
        'use_spot': False,
        'accelerator': None,
        'spot_placer': None,
        'reserved_capacity_fill': False,
        'cost_rebalance': False,
        'dynamic_ondemand_fallback': False,
        'base_ondemand_fallback_replicas': 0,
    }


def _service_version_identity(*,
                              service_incarnation: str = _SERVICE_UUID,
                              service_version: int = 3) -> dict:
    return {
        'version': 1,
        'service_name': 'svc',
        'service_incarnation': service_incarnation,
        'service_version': service_version,
        'effective_service_config_sha256': 'a' * 64,
        'effective_task_config_sha256': 'b' * 64,
        'capacity_profile': _capacity_profile(),
        'provider_profile': 'pod_cluster_v1',
    }


def _shadow_binding() -> dict:
    return {
        'version': 1,
        'binding_kind': 'shadow_candidate',
        'candidate_epoch': _SHADOW_EPOCH,
        'qualification_policy_sha256': 'c' * 64,
        'qualification_binding_sha256': 'd' * 64,
    }


def _authority_binding() -> dict:
    return {
        'version': 1,
        'binding_kind': 'authoritative_action',
        'policy_epoch': _POLICY_EPOCH,
        'policy_sha256': 'e' * 64,
        'authority_binding_sha256': 'f' * 64,
    }


def _rebound_proof(
    proof: actions.ProviderPolicyBoundaryProofV1,
    subject_sha256: str,
) -> actions.ProviderPolicyBoundaryProofV1:
    value = proof.canonical_value()
    value['policy_subject_sha256'] = subject_sha256
    value['projection_before_sha256'] = subject_sha256
    value['projection_after_sha256'] = subject_sha256
    return actions.ProviderPolicyBoundaryProofV1.from_value(value)


def _v2_authority_cohort(
) -> authority_contracts.ProviderAuthorityWorkerCohortV2:
    manifest_value = action_fixtures.authority_manifest_value()
    manifest_value['version'] = 2
    manifest_value['claim_contract'] = 'frozen_action_cohort_join_v2'
    manifest = (authority_contracts.ProviderAuthorityWorkerCohortManifestV2.
                from_value(manifest_value))
    return authority_contracts.ProviderAuthorityWorkerCohortV2(
        version=2,
        manifest=manifest,
        manifest_sha256=manifest.sha256,
        deployment_uid='deployment-uid-v2',
        service_account_uid='service-account-uid-v2')


def _v2_cohort_reference() -> actions.ProviderAuthorityWorkerCohortReferenceV1:
    cohort = _v2_authority_cohort()
    return actions.ProviderAuthorityWorkerCohortReferenceV1(
        version=1,
        cohort_id=cohort.cohort_id,
        cohort_identity_sha256=cohort.sha256)


def _v2_launch_capsule_from_frozen_v1(
    old: actions.ProviderKubernetesExecutionCapsuleV1,
) -> actions.ProviderKubernetesExecutionCapsuleV2:
    value = old.canonical_value()
    value['version'] = 2
    value['executor_cohort'] = _v2_cohort_reference().canonical_value()
    return actions.ProviderKubernetesExecutionCapsuleV2.from_value(value)


def _v2_down_capsule_from_frozen_v1(
    old: actions.ProviderKubernetesDownExecutionCapsuleV1,
) -> actions.ProviderKubernetesDownExecutionCapsuleV2:
    value = old.canonical_value()
    value['version'] = 2
    value['executor_cohort'] = _v2_cohort_reference().canonical_value()
    return actions.ProviderKubernetesDownExecutionCapsuleV2.from_value(value)


def _v2_launch_spec(*,
                    authoritative: bool = False,
                    service_name: str = 'svc',
                    service_version: int = 3,
                    workspace: str = 'boltz-test') -> dict:
    if workspace == 'boltz-test':
        old_value = v1_fixtures._launch_spec()
        old_invocation = actions.ProviderLifecycleInvocationV1.from_value(
            old_value['invocation'])
        old_plan = actions.ProviderLifecyclePlanV1.from_value(
            old_value['provider_plan'])
    else:
        old_invocation_value = v1_fixtures._launch_invocation(
            workspace=workspace)
        old_invocation = actions.ProviderLifecycleInvocationV1.from_value(
            old_invocation_value)
        old_plan_value = v1_fixtures._launch_plan()
        old_plan_value['request_payload_sha256'] = old_invocation.sha256
        old_plan_value['resources_snapshot_sha256'] = (
            old_invocation.require_launch().resources.sha256)
        old_plan = actions.ProviderLifecyclePlanV1.from_value(old_plan_value)
    old_plan.validate_invocation(old_invocation)
    old_launch = old_invocation.require_launch()
    old_config = old_launch.execution_config
    capsule = _v2_launch_capsule_from_frozen_v1(old_config.capsule)
    subject = actions.project_provider_launch_policy_subject_v2(
        old_invocation.resource_identity, old_launch.source,
        old_invocation.requested_target, old_launch.resources,
        old_launch.topology, old_invocation.resource_identity.replica_id,
        old_launch.retry_until_up, capsule)
    config = actions.ProviderKubernetesExecutionConfigV2(
        version=2,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=_rebound_proof(old_config.controller, subject.sha256),
        executor=_rebound_proof(old_config.executor, subject.sha256))
    launch = actions.ProviderLaunchInvocationV2(
        source=old_launch.source,
        resources=old_launch.resources,
        topology=old_launch.topology,
        execution_config=config,
        replica_id_text=old_launch.replica_id_text,
        security_group_scope=old_launch.security_group_scope,
        admin_policy_mode=old_launch.admin_policy_mode,
        managed_secrets_mode=old_launch.managed_secrets_mode,
        retry_until_up=old_launch.retry_until_up,
        exact_resources_override=old_launch.exact_resources_override,
        backend=old_launch.backend,
        optimize_target=old_launch.optimize_target,
        dryrun=old_launch.dryrun,
        no_setup=old_launch.no_setup,
        clone_disk_from=None,
        fast=old_launch.fast,
        file_mounts_blob_id=None,
        tls_material_ref=None)
    invocation = actions.ProviderLifecycleInvocationV2(
        version=2,
        profile=old_invocation.profile,
        redaction_profile=old_invocation.redaction_profile,
        action_kind=old_invocation.action_kind,
        resource_identity=old_invocation.resource_identity,
        requested_target=old_invocation.requested_target,
        launch=launch,
        down=None)
    plan = actions.ProviderLifecyclePlanV2(
        version=2,
        profile=old_plan.profile,
        action_kind=old_plan.action_kind,
        resource_identity=old_plan.resource_identity,
        placement_decision_sha256=old_plan.placement_decision_sha256,
        resources_snapshot_sha256=old_plan.resources_snapshot_sha256,
        workspace_identity_sha256=old_plan.workspace_identity_sha256,
        requested_target=old_plan.requested_target,
        prior_launch_basis_sha256=None,
        prior_cleanup_target_sha256=None,
        request_payload_sha256=invocation.sha256,
        redaction_profile=old_plan.redaction_profile)
    plan.validate_invocation(invocation)
    identity = _service_version_identity()
    identity['service_name'] = service_name
    identity['service_version'] = service_version
    return {
        'version': 2,
        'service_version_spec_identity': identity,
        'service_version_spec_identity_sha256':
            actions.canonical_sha256(identity),
        'admission_binding':
            (_authority_binding() if authoritative else _shadow_binding()),
        'provider_plan': plan.canonical_value(),
        'invocation': invocation.canonical_value(),
    }


def _v2_down_from_v1(
    old: actions.ServeReplicaActionSpecV1,
    *,
    service_version: int = 3,
) -> dict:
    old_invocation = old.invocation
    old_down = old_invocation.require_down()
    old_config = old_down.execution_config
    capsule = _v2_down_capsule_from_frozen_v1(old_config.capsule)
    subject = actions.project_provider_down_policy_subject_v2(
        old_invocation.requested_target, old_down.workspace,
        old_down.prior_launch_basis, capsule)
    config = actions.ProviderKubernetesDownExecutionConfigV2(
        version=2,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=_rebound_proof(old_config.controller, subject.sha256),
        executor=_rebound_proof(old_config.executor, subject.sha256))
    down = actions.ProviderDownInvocationV2(
        cluster_name=old_down.cluster_name,
        expected_cluster_record_uuid=old_down.expected_cluster_record_uuid,
        workspace=old_down.workspace,
        prior_launch_basis=old_down.prior_launch_basis,
        execution_config=config,
        purge=False,
        graceful=False,
        graceful_timeout=None)
    invocation = actions.ProviderLifecycleInvocationV2(
        version=2,
        profile=old_invocation.profile,
        redaction_profile=old_invocation.redaction_profile,
        action_kind=old_invocation.action_kind,
        resource_identity=old_invocation.resource_identity,
        requested_target=old_invocation.requested_target,
        launch=None,
        down=down)
    old_plan = old.provider_plan
    plan = actions.ProviderLifecyclePlanV2(
        version=2,
        profile=old_plan.profile,
        action_kind=old_plan.action_kind,
        resource_identity=old_plan.resource_identity,
        placement_decision_sha256=old_plan.placement_decision_sha256,
        resources_snapshot_sha256=old_plan.resources_snapshot_sha256,
        workspace_identity_sha256=old_plan.workspace_identity_sha256,
        requested_target=old_plan.requested_target,
        prior_launch_basis_sha256=old_plan.prior_launch_basis_sha256,
        prior_cleanup_target_sha256=old_plan.prior_cleanup_target_sha256,
        request_payload_sha256=invocation.sha256,
        redaction_profile=old_plan.redaction_profile)
    plan.validate_invocation(invocation)
    identity = _service_version_identity(service_version=service_version)
    return {
        'version': 2,
        'service_version_spec_identity': identity,
        'service_version_spec_identity_sha256':
            actions.canonical_sha256(identity),
        'admission_binding': _authority_binding(),
        'provider_plan': plan.canonical_value(),
        'invocation': invocation.canonical_value(),
    }


def _v2_down_spec(
    prior_launch_spec: actions.ServeReplicaActionSpecV2,
    *,
    service_version: int = 3,
    prior_spec_sha256: str | None = None,
) -> dict:
    down_fixtures = v1_fixtures.down_config_fixtures
    basis_value = down_fixtures.completed_basis_payload(generation=1)
    basis_value['launch_immutable_spec_sha256'] = (prior_launch_spec.sha256
                                                   if prior_spec_sha256 is None
                                                   else prior_spec_sha256)
    basis = actions.CompletedLaunchBasisV1.from_value(basis_value)
    cleanup = actions.ProviderKubernetesCleanupTargetV1.from_value(
        down_fixtures._cleanup_target())
    old = actions.ServeReplicaActionSpecV1.from_value(
        down_fixtures._down_spec_payload_for_basis(basis, cleanup,
                                                   generation=2))
    return _v2_down_from_v1(old, service_version=service_version)


def _project(
        yaml_content: str = _YAML_OMITTED_DEFAULTS,
        *,
        service_incarnation: str = _SERVICE_UUID,
        service_version: int = 3) -> actions.ServeServiceVersionSpecIdentityV1:
    return (identity_projector.
            project_potential_serve_service_version_spec_identity_v1(
                yaml_content=yaml_content,
                service_name='svc',
                service_incarnation=service_incarnation,
                service_version=service_version))


def test_v1_action_spec_golden_bytes_and_parser_remain_unchanged() -> None:
    spec = actions.ServeReplicaActionSpecV1.from_value(
        v1_fixtures._launch_spec())

    assert len(spec.canonical_bytes) == 60_851
    assert spec.sha256 == (
        '81a770595947e61f0c2095a84ab746e72aed08b554f5acf5e458debd3264b0a7')


def test_v2_realistic_and_candidate_maximal_exact_size_hash_goldens() -> None:
    realistic_launch = actions.serve_replica_action_spec_from_value_v2(
        _v2_launch_spec())
    completed_down = actions.serve_replica_action_spec_from_value_v2(
        _v2_down_spec(realistic_launch))
    # Candidate-maximal keeps the admitted frozen launch inputs byte-exact.
    # Runtime-derived leaves do not occur in a launch action spec; the
    # authoritative binding is the other admitted envelope shape.
    candidate_launch = actions.serve_replica_action_spec_from_value_v2(
        _v2_launch_spec(authoritative=True))

    down_fixtures = v1_fixtures.down_config_fixtures
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    basis, cleanup, _, _ = down_fixtures._partial_source_for_case(
        case, fixture_member='candidate_maximal')
    old_candidate_down = actions.ServeReplicaActionSpecV1.from_value(
        down_fixtures._down_spec_payload_for_basis(basis, cleanup))
    candidate_down = actions.serve_replica_action_spec_from_value_v2(
        _v2_down_from_v1(old_candidate_down))

    assert (len(realistic_launch.canonical_bytes), realistic_launch.sha256) == (
        56_994,
        '7d680f846c37326330903064bc210fb73a67e6b7625b1614b17ce9df6feea733')
    assert (len(completed_down.canonical_bytes), completed_down.sha256) == (
        45_045,
        'f638480d05f9283a52c7b1075ab2df9a1a3a8280890f9e01fa10053d3277c82d')
    assert (len(candidate_launch.canonical_bytes), candidate_launch.sha256) == (
        56_977,
        '7392f6792ec560ce4a99884b9bc2dd6ac83a4a5925a936ace27de8fcf458891e')
    assert (len(candidate_down.canonical_bytes), candidate_down.sha256) == (
        48_560,
        'b66dabb27ec6f8cb7fff670bf8a1975228741ea80b8aea2c87cd822dd901c796')
    assert all(
        len(spec.canonical_bytes) <= 60_000
        for spec in (realistic_launch, completed_down, candidate_launch,
                     candidate_down))

    # Every nested V2 object remains valid at the generic 1,024-byte Text
    # ceiling; the independent full-spec qualification gate rejects the
    # genuinely oversized combination.
    oversized_nested = _v2_launch_spec(authoritative=True,
                                       workspace='w' * 1_024)
    actions.ProviderLifecyclePlanV2.from_value(
        oversized_nested['provider_plan'])
    actions.ProviderLifecycleInvocationV2.from_value(
        oversized_nested['invocation'])
    with pytest.raises(ValueError, match='60000-byte qualification'):
        actions.serve_replica_action_spec_from_value_v2(oversized_nested)


def test_v2_graph_is_additive_compact_and_resolves_only_typed_v2_cohort(
) -> None:
    old = actions.ServeReplicaActionSpecV1.from_value(
        v1_fixtures._launch_spec())
    spec = actions.serve_replica_action_spec_from_value_v2(_v2_launch_spec())

    assert type(spec.provider_plan) is actions.ProviderLifecyclePlanV2
    assert type(spec.invocation) is actions.ProviderLifecycleInvocationV2
    assert type(spec.invocation.require_launch()) is (
        actions.ProviderLaunchInvocationV2)
    assert type(spec.invocation.require_launch().execution_config) is (
        actions.ProviderKubernetesExecutionConfigV2)
    assert type(spec.invocation.require_launch().execution_config.capsule) is (
        actions.ProviderKubernetesExecutionCapsuleV2)
    reference = spec.invocation.executor_cohort_reference
    assert type(reference) is actions.ProviderAuthorityWorkerCohortReferenceV1
    assert set(reference.canonical_value()) == {
        'version', 'cohort_id', 'cohort_identity_sha256'
    }
    assert len(reference.canonical_bytes) == 231
    cohort = _v2_authority_cohort()
    authority_contracts.validate_locked_action_spec_cohort_v2(spec, cohort)
    authority_contracts.validate_locked_action_spec_cohort_v2(reference, cohort)

    with pytest.raises(TypeError, match='parsed locked V2 worker cohort'):
        authority_contracts.validate_locked_action_spec_cohort_v2(
            reference, old.invocation.executor_cohort)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match='live V2 action spec'):
        authority_contracts.validate_locked_action_spec_cohort_v2(
            old, cohort)  # type: ignore[arg-type]

    crossed = actions.ProviderAuthorityWorkerCohortReferenceV1(
        version=1,
        cohort_id=reference.cohort_id,
        cohort_identity_sha256='0' * 64)
    with pytest.raises(ValueError, match='parsed locked V2 worker cohort'):
        authority_contracts.validate_locked_action_spec_cohort_v2(
            crossed, cohort)


def test_v1_and_v2_parsers_reject_cross_version_nested_graphs() -> None:
    v2 = _v2_launch_spec()
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ServeReplicaActionSpecV1.from_value(v2)

    crossed = _v2_launch_spec()
    crossed['provider_plan'] = v1_fixtures._launch_plan()
    crossed['invocation'] = v1_fixtures._launch_invocation()
    with pytest.raises((TypeError, ValueError),
                       match='reference|integer 2|unknown or missing'):
        actions.serve_replica_action_spec_from_value_v2(crossed)

    for path in (('provider_plan',), ('invocation',), ('invocation', 'launch',
                                                       'execution_config'),
                 ('invocation', 'launch', 'execution_config', 'capsule')):
        crossed = _v2_launch_spec()
        target = crossed
        for key in path:
            target = target[key]
        target['version'] = 1
        with pytest.raises(ValueError, match='integer 2'):
            actions.serve_replica_action_spec_from_value_v2(crossed)


def test_capacity_identity_and_bindings_are_closed_canonical_values() -> None:
    capacity = actions.ServeActionCapacityProfileV1.from_value(
        _capacity_profile())
    identity = actions.ServeServiceVersionSpecIdentityV1.from_value(
        _service_version_identity())
    shadow = actions.serve_replica_action_admission_binding_from_value_v1(
        _shadow_binding())
    authority = actions.serve_replica_action_admission_binding_from_value_v1(
        _authority_binding())

    assert capacity == (actions.ServeActionCapacityProfileV1.
                        ordinary_ondemand_physical_width1())
    assert type(identity.service_incarnation) is uuid.UUID
    assert type(shadow.candidate_epoch) is uuid.UUID
    assert type(authority.policy_epoch) is uuid.UUID
    assert shadow.canonical_value()['candidate_epoch'] == _SHADOW_EPOCH
    assert authority.canonical_value()['policy_epoch'] == _POLICY_EPOCH
    assert identity.sha256 == (
        '5980a129f7a40cfa5825cbffcd2e992deda3581be543566e69775135431a82a5')

    for value, field in ((_capacity_profile(), 'profile'),
                         (_service_version_identity(), 'provider_profile')):
        value[field] = 'future'
        parser = (actions.ServeActionCapacityProfileV1.from_value
                  if field == 'profile' else
                  actions.ServeServiceVersionSpecIdentityV1.from_value)
        with pytest.raises(ValueError, match='unsupported'):
            parser(value)


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'unknown': None}), 'unknown or missing'),
    (lambda value: value.pop('admission_binding'), 'unknown or missing'),
    (lambda value: value.update({'version': 1}), 'integer 2'),
    (lambda value: value.update(
        {'service_version_spec_identity_sha256': '0' * 64}), 'hash does not'),
])
def test_v2_parser_rejects_unknown_missing_version_and_hash(mutate,
                                                            match: str) -> None:
    value = _v2_launch_spec()
    mutate(value)

    with pytest.raises((TypeError, ValueError), match=match):
        actions.serve_replica_action_spec_from_value_v2(value)


def test_v2_parser_binds_launch_source_action_id_kind_and_target() -> None:
    crossed_source = _v2_launch_spec()
    crossed_source['service_version_spec_identity']['service_version'] = 4
    crossed_source['service_version_spec_identity_sha256'] = (
        actions.canonical_sha256(
            crossed_source['service_version_spec_identity']))
    with pytest.raises(ValueError, match='retained-source tuple'):
        actions.serve_replica_action_spec_from_value_v2(crossed_source)

    crossed_action = _v2_launch_spec()
    crossed_action['provider_plan']['resource_identity'][
        'desired_generation'] = 2
    with pytest.raises(ValueError, match='action IDs differ'):
        actions.serve_replica_action_spec_from_value_v2(crossed_action)

    crossed_target = _v2_launch_spec()
    crossed_target['provider_plan']['requested_target'][
        'sky_cluster_record_uuid'] = ('77777777-7777-4777-8777-777777777777')
    with pytest.raises(ValueError,
                       match='identities differ|cluster label does not match'):
        actions.serve_replica_action_spec_from_value_v2(crossed_target)

    crossed_kind = _v2_launch_spec()
    crossed_kind['provider_plan']['action_kind'] = 'down'
    with pytest.raises(ValueError, match='prior hashes|action IDs differ'):
        actions.serve_replica_action_spec_from_value_v2(crossed_kind)


def test_binding_union_rejects_crossed_discriminator_and_requires_mode(
) -> None:
    crossed_shadow = _v2_launch_spec()
    crossed_shadow['admission_binding']['binding_kind'] = (
        'authoritative_action')
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.serve_replica_action_spec_from_value_v2(crossed_shadow)

    crossed_authority = _v2_launch_spec(authoritative=True)
    crossed_authority['admission_binding']['binding_kind'] = 'shadow_candidate'
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.serve_replica_action_spec_from_value_v2(crossed_authority)

    shadow = actions.serve_replica_action_spec_from_value_v2(_v2_launch_spec())
    authority = actions.serve_replica_action_spec_from_value_v2(
        _v2_launch_spec(authoritative=True))
    assert shadow.require_shadow_candidate_binding().candidate_epoch == (
        uuid.UUID(_SHADOW_EPOCH))
    assert authority.require_authoritative_action_binding().policy_epoch == (
        uuid.UUID(_POLICY_EPOCH))
    with pytest.raises(ValueError, match='does not carry'):
        shadow.require_authoritative_action_binding()
    with pytest.raises(ValueError, match='does not carry'):
        authority.require_shadow_candidate_binding()
    with pytest.raises(ValueError, match='not byte-equal'):
        shadow.validate_admission_binding(
            authority.require_authoritative_action_binding())


@pytest.mark.parametrize('binding_factory,field,invalid', [
    (_shadow_binding, 'candidate_epoch', 7),
    (_shadow_binding, 'candidate_epoch', '7'),
    (_shadow_binding, 'candidate_epoch', _SHADOW_EPOCH.upper()),
    (_authority_binding, 'policy_epoch', 7),
    (_authority_binding, 'policy_epoch', '7'),
    (_authority_binding, 'policy_epoch', _POLICY_EPOCH.replace('-', '')),
])
def test_binding_uuid_fields_reject_integers_numeric_and_noncanonical_text(
        binding_factory, field: str, invalid: object) -> None:
    value = binding_factory()
    value[field] = invalid

    with pytest.raises((TypeError, ValueError), match='UUID|canonical'):
        actions.serve_replica_action_admission_binding_from_value_v1(value)


def test_v2_cleanup_down_preserves_outer_version_identity_and_binding() -> None:
    spec = actions.serve_replica_action_spec_from_value_v2(_v2_launch_spec())
    identity_bytes = spec.service_version_spec_identity.canonical_bytes
    binding_bytes = spec.admission_binding.canonical_bytes
    cleanup = spec.launch_cleanup_down_invocation()

    spec.validate_shadow_child_invocation(
        actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN, cleanup)
    assert spec.service_version_spec_identity.canonical_bytes == identity_bytes
    assert spec.admission_binding.canonical_bytes == binding_bytes
    assert spec.canonical_value()['service_version_spec_identity_sha256'] == (
        spec.service_version_spec_identity.sha256)
    assert cleanup.parent_launch_action_id == spec.action_id
    assert cleanup.parent_launch_request_payload_sha256 == spec.invocation.sha256


def test_down_store_boundary_requires_exact_retained_v2_launch_identity(
) -> None:
    launch = actions.serve_replica_action_spec_from_value_v2(_v2_launch_spec())
    down = actions.serve_replica_action_spec_from_value_v2(
        _v2_down_spec(launch))

    down.validate_down_prior_launch_spec(launch)
    assert down.service_version_spec_identity.canonical_bytes == (
        launch.service_version_spec_identity.canonical_bytes)
    assert down.service_version_spec_identity_sha256 == (
        launch.service_version_spec_identity_sha256)

    crossed_version = actions.serve_replica_action_spec_from_value_v2(
        _v2_down_spec(launch, service_version=4))
    with pytest.raises(ValueError, match='service-version identity'):
        crossed_version.validate_down_prior_launch_spec(launch)

    crossed_hash = actions.serve_replica_action_spec_from_value_v2(
        _v2_down_spec(launch, prior_spec_sha256='0' * 64))
    with pytest.raises(ValueError, match='immutable-spec hash'):
        crossed_hash.validate_down_prior_launch_spec(launch)

    retained_v1 = actions.ServeReplicaActionSpecV1.from_value(
        v1_fixtures._launch_spec())
    with pytest.raises(TypeError, match='prior_launch_spec'):
        down.validate_down_prior_launch_spec(
            retained_v1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='requires a down'):
        launch.validate_down_prior_launch_spec(launch)


def test_v2_module_exposes_only_the_explicit_live_spec_parser() -> None:
    parser_names = sorted(
        name for name in vars(actions)
        if name.startswith('serve_replica_action_spec_from_value'))

    assert parser_names == ['serve_replica_action_spec_from_value_v2']


def test_projector_materializes_defaults_and_pins_canonical_hashes() -> None:
    omitted = _project(_YAML_OMITTED_DEFAULTS)
    explicit = _project(_YAML_EXPLICIT_DEFAULTS)

    assert omitted == explicit
    assert omitted.effective_service_config_sha256 == (
        '363779d09201c3ae64c099072df8e74a79039d3ec413b1952ddf9e88818ebc78')
    assert omitted.effective_task_config_sha256 == (
        '74acab14c5a210522c914a9b1966e44e85a3cb0330d9725ce23798f95857fffe')
    assert omitted.capacity_profile.canonical_value() == _capacity_profile()
    assert omitted.provider_profile == 'pod_cluster_v1'


def test_projector_rejects_duplicate_yaml_and_secret_material_value_free(
) -> None:
    duplicate = _YAML_OMITTED_DEFAULTS.replace('  replicas: 1',
                                               '  replicas: 1\n  replicas: 2')
    with pytest.raises(
            identity_projector.ServeServiceVersionIdentityProjectionError,
            match='Duplicate key'):
        _project(duplicate)

    secret = 'raw-projector-secret-value'
    secret_yaml = _YAML_OMITTED_DEFAULTS + f'secrets:\n  TOKEN: {secret}\n'
    with pytest.raises(identity_projector.
                       ServeServiceVersionIdentityProjectionError) as error:
        _project(secret_yaml)
    assert secret not in str(error.value)

    tls_yaml = _YAML_OMITTED_DEFAULTS.replace(
        '  replicas: 1', '  replicas: 1\n  tls:\n    keyfile: /secret/key\n'
        '    certfile: /secret/cert')
    with pytest.raises(
            identity_projector.ServeServiceVersionIdentityProjectionError,
            match='TLS-bearing'):
        _project(tls_yaml)


@pytest.mark.parametrize(('fragment', 'match'), [
    ('num_nodes: 2\n', 'exactly one physical node'),
    ('resources:\n  infra: kubernetes\n  use_spot: true\n', 'Spot resources'),
    ('resources:\n  infra: kubernetes\n  accelerators: A100:1\n',
     'Accelerator resources'),
    ('resources:\n  infra: kubernetes\n  _internal: value\n',
     'internal or provenance'),
    ('resources:\n  any_of:\n    - infra: kubernetes\n'
     '      _internal: value\n', 'internal or provenance'),
    ('_metadata:\n  injected: value\n', 'internal or provenance'),
])
def test_projector_rejects_ineligible_or_internal_content(
        fragment: str, match: str) -> None:
    if fragment.startswith('resources:'):
        yaml_content = _YAML_OMITTED_DEFAULTS.replace(
            'resources:\n  infra: kubernetes\n', fragment)
    else:
        yaml_content = _YAML_OMITTED_DEFAULTS + fragment

    with pytest.raises(
            identity_projector.ServeServiceVersionIdentityProjectionError,
            match=match):
        _project(yaml_content)


def test_projector_preserves_underscore_prefixed_user_mapping_keys() -> None:
    yaml_content = _YAML_OMITTED_DEFAULTS + 'envs:\n  _VISIBLE: value\n'

    projected = _project(yaml_content)
    baseline = _project()

    assert (projected.effective_task_config_sha256
            != baseline.effective_task_config_sha256)


def test_projector_requires_exact_constructor_provenance(monkeypatch) -> None:
    task = task_lib.Task.from_yaml_str(_YAML_OMITTED_DEFAULTS)
    task._user_specified_yaml = 'different immutable bytes'  # pylint: disable=protected-access
    monkeypatch.setattr(task_lib.Task, 'from_yaml_str', lambda _: task)

    with pytest.raises(
            identity_projector.ServeServiceVersionIdentityProjectionError,
            match='provenance does not match'):
        _project(_YAML_OMITTED_DEFAULTS)


def test_projector_separates_incarnation_version_from_effective_subhashes(
) -> None:
    original = _project()
    next_version = _project(service_version=4)
    next_incarnation = _project(service_incarnation=_OTHER_SERVICE_UUID)
    reformatted_yaml = '\n' + _YAML_OMITTED_DEFAULTS
    reformatted = _project(reformatted_yaml)

    assert original.effective_service_config_sha256 == (
        next_version.effective_service_config_sha256)
    assert original.effective_task_config_sha256 == (
        next_incarnation.effective_task_config_sha256)
    assert len({original.sha256, next_version.sha256,
                next_incarnation.sha256}) == 3
    assert original == reformatted
    assert hashlib.sha256(
        _YAML_OMITTED_DEFAULTS.encode()).digest() != (hashlib.sha256(
            reformatted_yaml.encode()).digest())


def test_projector_has_no_pickle_dependency() -> None:
    source = inspect.getsource(identity_projector)

    assert 'import pickle' not in source
    assert 'from pickle' not in source


def test_potential_identity_projector_has_no_durable_writer_inventory() -> None:
    symbol = 'project_potential_serve_service_version_spec_identity_v1'
    package_root = pathlib.Path(sky.__file__).resolve().parent
    callers = []
    for path in package_root.rglob('*.py'):
        if path.name == 'resource_action_identity.py':
            continue
        if symbol in path.read_text(encoding='utf-8'):
            callers.append(str(path.relative_to(package_root)))

    assert not callers


def test_locked_identity_verifier_has_one_reviewed_state_boundary() -> None:
    symbol = 'verify_locked_serve_service_version_spec_identity_v1'
    package_root = pathlib.Path(sky.__file__).resolve().parent
    callers = []
    for path in package_root.rglob('*.py'):
        if path.name == 'resource_action_identity.py':
            continue
        if symbol in path.read_text(encoding='utf-8'):
            callers.append(str(path.relative_to(package_root)))

    assert callers == ['serve/resource_action_identity_state.py']
