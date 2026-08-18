"""Focused non-PostgreSQL contracts for ordinary Serve launch binding."""
# pylint: disable=protected-access

import copy
import dataclasses
import pathlib
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy

from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding as binding
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import system_oom_recovery
from sky.serve import system_recovery_state
from sky.server import constants as server_constants
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import payloads
from sky.utils import common_utils
from sky.utils.db import migration_utils

_SUBMISSION_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CONTROLLER_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')


def _profile_info() -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(replica_id=3,
                                        cluster_name='svc-3',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=2,
                                        resources_override=None)
    info.replica_record_id = str(_RECORD_ID)
    return info


def _system_recovery_intent(
) -> system_recovery_state.SystemRecoveryLaunchIntent:
    digest = 'a' * 64
    return system_recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=(
            system_recovery_state.SYSTEM_RECOVERY_CAPABILITY),
        service_hash='svc-hash',
        replica_id=3,
        launch_generation=9,
        launch_nonce='b' * 64,
        workspace='workspace-a',
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)


def _active_system_recovery() -> system_recovery_state.ReplicaSystemRecovery:
    return system_recovery_state.ReplicaSystemRecovery(
        state=system_recovery_state.ControllerRecoveryState.ARMED,
        job_id=9,
        capability=system_recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        original_attempt_id='44444444-4444-4444-8444-444444444444',
        replacement_attempt_id=None,
        node_boot_id='boot-id',
        remote_phase=system_recovery_state.RemoteRecoveryPhase.ARMED,
        occurrence_count=0,
        armed_at=10.0)


def test_non_pool_profile_envelope_is_closed_and_canonical() -> None:
    profile = binding.NonPoolLaunchProfile.create(
        binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference='allocation:pool-a:41',
        authorization_generation=41,
        authorization_payload={
            'card': 'L4',
            'physical_uid': 'cluster-uid',
            'zero_cost': True,
        })
    profile.validate()
    assert profile.authorization_kind == (
        binding.NonPoolLaunchAuthorizationKind.RESERVED_FILL_ALLOCATION)
    assert len(profile.authorization_digest) == 64
    assert len(profile.digest) == 64

    with pytest.raises(ValueError, match='does not match'):
        binding.canonical_non_pool_profile_digest(
            binding.NonPoolLaunchProfileKind.RESERVED_FILL,
            profile_version=binding.NON_POOL_PROFILE_VERSION,
            authorization_kind=(
                binding.NonPoolLaunchAuthorizationKind.PAID_CAPACITY_CLAIM),
            authorization_reference='allocation:pool-a:41',
            authorization_generation=41,
            authorization_digest=profile.authorization_digest)
    with pytest.raises(ValueError, match='not canonical'):
        dataclasses.replace(profile, digest='0' * 64).validate()


def test_supported_non_pool_profile_set_digest_is_stable_and_complete() -> None:
    digest = binding.supported_non_pool_profile_set_digest()
    assert len(digest) == 64
    assert digest == binding.supported_non_pool_profile_set_digest()
    assert set(binding._PROFILE_AUTHORIZATION_KIND) == set(  # pylint: disable=protected-access
        binding.NonPoolLaunchProfileKind)


def test_generic_capability_requires_the_complete_exact_tuple() -> None:
    authority = binding.ControllerBindingAuthority(
        service_name='svc',
        service_hash='svc-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_CONTROLLER_ID,
        controller_owner_epoch=7,
        capable=True,
        binding_mode=binding.BindingMode.BOUND,
        binding_epoch=3,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))

    assert authority.generic_launches_required
    assert not dataclasses.replace(
        authority,
        non_pool_profile_set_digest='0' * 64).generic_launches_required
    assert not dataclasses.replace(
        authority,
        non_pool_receipt_protocol_version=None).generic_launches_required


def test_replacement_planner_authorization_is_exact_and_canonical() -> None:
    authority = binding.ControllerBindingAuthority(
        service_name='svc',
        service_hash='svc-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_CONTROLLER_ID,
        controller_owner_epoch=7,
        capable=True,
        binding_mode=binding.BindingMode.BOUND,
        binding_epoch=3)
    predecessor = uuid.UUID('44444444-4444-4444-8444-444444444444')
    authorization = binding.build_replacement_planner_authorization(
        binding.NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT,
        authority,
        predecessor_replica_id=9,
        predecessor_record_id=str(predecessor),
        predecessor_service_version=2,
        observation_generation=41,
        observation_service_version=2,
        target_capacity=8,
        target_capacity_by_accelerator={
            'L4': 4,
            'A100': 4,
        },
        accelerator_shapes={
            'L4': 1,
            'A100': 8,
        })

    assert authorization['predecessor'] == {
        'replica_id': 9,
        'replica_record_id': str(predecessor),
        'service_version': 2,
    }
    assert {
        key: authorization[key]
        for key in ('service_hash', 'service_lifecycle_epoch',
                    'service_binding_epoch')
    } == {
        'service_hash': 'svc-hash',
        'service_lifecycle_epoch': 4,
        'service_binding_epoch': 3,
    }
    assert authorization['observation'] == {
        'accelerator_shapes': [['A100', 8], ['L4', 1]],
        'classification': 'UNKNOWN',
        'reconcile_generation': 41,
        'service_version': 2,
        'target_capacity': 8,
        'target_capacity_by_accelerator': [['A100', 4], ['L4', 4]],
    }
    assert binding.build_replacement_planner_authorization(
        binding.NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT,
        dataclasses.replace(authority,
                            controller_incarnation=uuid.uuid4(),
                            controller_owner_epoch=8),
        predecessor_replica_id=9,
        predecessor_record_id=str(predecessor),
        predecessor_service_version=2,
        observation_generation=41,
        observation_service_version=2,
        target_capacity=8,
        target_capacity_by_accelerator={
            'L4': 4,
            'A100': 4,
        },
        accelerator_shapes={
            'L4': 1,
            'A100': 8,
        }) == authorization

    with pytest.raises(ValueError, match='cannot carry'):
        binding.build_replacement_planner_authorization(
            binding.NonPoolLaunchProfileKind.COST_REBALANCE,
            authority,
            predecessor_replica_id=9,
            predecessor_record_id=str(predecessor),
            predecessor_service_version=2,
            observation_generation=41)


def test_service_generic_capability_tuple_is_all_or_none_and_supported(
) -> None:
    digest = binding.supported_non_pool_profile_set_digest()
    assert binding._non_pool_capability_from_service({
        'non_pool_launch_binding_capable': True,
        'controller_incarnation': _CONTROLLER_ID,
        'non_pool_launch_controller_incarnation': _CONTROLLER_ID,
        'non_pool_launch_binding_protocol_version':
            binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        'non_pool_launch_capability_profile_set_digest': digest,
        'non_pool_launch_capability_cohort_epoch':
            binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
        'non_pool_launch_receipt_protocol_version':
            binding.NON_POOL_RECEIPT_PROTOCOL_VERSION,
    }) == (True, binding.NON_POOL_BINDING_PROTOCOL_VERSION, digest,
           binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
           binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='malformed'):
        binding._non_pool_capability_from_service({
            'non_pool_launch_binding_capable': False,
            'non_pool_launch_binding_protocol_version':
                binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        })
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='unsupported'):
        binding._non_pool_capability_from_service({
            'non_pool_launch_binding_capable': True,
            'controller_incarnation': _CONTROLLER_ID,
            'non_pool_launch_controller_incarnation': _CONTROLLER_ID,
            'non_pool_launch_binding_protocol_version':
                binding.NON_POOL_BINDING_PROTOCOL_VERSION,
            'non_pool_launch_capability_profile_set_digest': '0' * 64,
            'non_pool_launch_capability_cohort_epoch':
                binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
            'non_pool_launch_receipt_protocol_version':
                binding.NON_POOL_RECEIPT_PROTOCOL_VERSION,
        })


def test_classify_all_non_pool_launch_profiles() -> None:
    ordinary = _profile_info()
    assert binding.classify_non_pool_launch_profile(ordinary) == (
        binding.NonPoolLaunchProfileKind.ORDINARY_PAID)

    zero_cost = _profile_info()
    zero_cost.is_zero_cost = True
    zero_cost.zero_cost_admission_sequence = 5
    assert binding.classify_non_pool_launch_profile(zero_cost) == (
        binding.NonPoolLaunchProfileKind.ORDINARY_ZERO_COST)

    reserved = _profile_info()
    reserved.reserved_fill = True
    reserved.is_zero_cost = True
    reserved.zero_cost_admission_sequence = 6
    reserved_values = {
        'reserved_fill_pool_key': 'pool-a',
        'reserved_fill_service_generation': 7,
        'reserved_fill_physical_cluster_uid': 'cluster-uid',
        'reserved_fill_kubernetes_context': 'context-a',
        'reserved_fill_allocation_generation': 8,
        'reserved_fill_allocation_input_sha256': 'a' * 64,
        'reserved_fill_allocation_claim_generation': 7,
        'reserved_fill_reconciliation_gate_generation': 9,
        'reserved_fill_reclaim_fleet_bundle_sha256': 'b' * 64,
        'reserved_fill_reclaim_policy_revision': 'policy-v1',
        'reserved_fill_reclaim_provider_inventory_sha256': 'c' * 64,
        'reserved_fill_worker_projection_sha256': 'd' * 64,
        'reserved_fill_observation_generation': 10,
        'reserved_fill_observation_sequence': 0,
        'reserved_fill_intent_idempotency_key': 'e' * 64,
    }
    for field, value in reserved_values.items():
        setattr(reserved, field, value)
    assert binding.classify_non_pool_launch_profile(reserved) == (
        binding.NonPoolLaunchProfileKind.RESERVED_FILL)

    replacement = _profile_info()
    replacement.unknown_capacity_replacement = True
    assert binding.classify_non_pool_launch_profile(replacement) == (
        binding.NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT)

    rebalance = _profile_info()
    rebalance.cost_rebalance_for_replica_id = 2
    assert binding.classify_non_pool_launch_profile(rebalance) == (
        binding.NonPoolLaunchProfileKind.COST_REBALANCE)

    recovery = _profile_info()
    recovery.system_recovery_launch_intent = _system_recovery_intent()
    recovery.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
    recovery.system_recovery_revision = 1
    assert binding.classify_non_pool_launch_profile(recovery) == (
        binding.NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY)


def test_non_pool_profile_classification_rejects_partial_authority() -> None:
    zero_cost = _profile_info()
    zero_cost.is_zero_cost = True
    assert binding.classify_non_pool_launch_profile(zero_cost) is None

    reserved = _profile_info()
    reserved.reserved_fill = True
    reserved.is_zero_cost = True
    reserved.zero_cost_admission_sequence = 1
    reserved.reserved_fill_pool_key = 'pool-a'
    assert binding.classify_non_pool_launch_profile(reserved) is None

    recovery = _profile_info()
    recovery.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
    assert binding.classify_non_pool_launch_profile(recovery) is None


def _excluded_owner(fields: dict[str, object],
                    status: str = 'PROVISIONING') -> dict[str, object]:
    info = replica_managers.ReplicaInfo(replica_id=3,
                                        cluster_name='svc-3',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=2,
                                        resources_override=None)
    info.replica_record_id = str(_RECORD_ID)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.RUNNING
    state = info.to_storage_dict()
    for field, value in fields.items():
        if dataclasses.is_dataclass(value):
            value = dataclasses.asdict(value)
        elif isinstance(value, system_recovery_state.SystemRecoveryDisposition):
            value = value.value
        state[field] = value
    return {
        'binding_excluded_replica_status': status,
        'binding_excluded_replica_state_version': 1,
        'binding_excluded_replica_version': 2,
        'binding_excluded_replica_state': state,
    }


def test_excluded_profile_discriminator_is_closed_and_canonical() -> None:
    context = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    assert serve_state.normalize_binding_excluded_launch_context(
        context) == context

    partial = dict(context)
    partial.pop(
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY)
    with pytest.raises(ValueError, match='incomplete'):
        serve_state.normalize_binding_excluded_launch_context(partial)

    context[serve_constants.
            ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY] = (
                'AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA')
    with pytest.raises(ValueError, match='canonical'):
        serve_state.normalize_binding_excluded_launch_context(context)

    context = {
        f'{serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}unknown': True,
    }
    with pytest.raises(ValueError, match='Unknown'):
        serve_state.normalize_binding_excluded_launch_context(context)


@pytest.mark.parametrize('excluded_state', [
    {
        'reserved_fill': True
    },
    {
        'is_zero_cost': True
    },
    {
        'unknown_capacity_replacement': True
    },
    {
        'cost_rebalance_for_replica_id': 1
    },
    {
        'system_recovery_launch_intent': _system_recovery_intent(),
        'system_recovery_disposition': 'ORDINARY',
        'system_recovery': None,
        'system_recovery_quarantine': None,
        'launch_request_id': None,
        'service_job_id': None,
    },
])
def test_each_persisted_exclusion_matches_only_exact_pending_record(
        excluded_state) -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    owner = _excluded_owner(excluded_state)
    assert serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)
    owner['binding_excluded_replica_state']['replica_record_id'] = str(
        uuid.uuid4())
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)


@pytest.mark.parametrize('recovery_state', [
    {
        'system_recovery_launch_intent': _system_recovery_intent(),
        'system_recovery_disposition': 'CANDIDATE',
    },
    {
        'system_recovery': {
            'phase': 'ARMED'
        },
        'system_recovery_disposition': 'CAPABLE',
    },
    {
        'system_recovery_quarantine': {
            'reason': 'malformed'
        },
        'system_recovery_disposition': 'ORDINARY',
    },
])
def test_persisted_exclusion_cannot_downgrade_system_recovery_contract(
        recovery_state) -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    owner = _excluded_owner(recovery_state)
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)


def test_persisted_special_marker_cannot_override_zero_cost_candidate() -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    owner = _excluded_owner({
        'is_zero_cost': True,
        'system_recovery_launch_intent': _system_recovery_intent(),
        'system_recovery_disposition': 'CANDIDATE',
    })
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)


@pytest.mark.parametrize(('field', 'value'), [
    ('binding_excluded_replica_state_version', 99),
    ('binding_excluded_replica_version', 3),
    ('binding_excluded_replica_state', {
        'replica_id': 3
    }),
])
def test_persisted_exclusion_rejects_unsupported_or_malformed_storage(
        field, value) -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    owner = _excluded_owner({'reserved_fill': True})
    owner[field] = value
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)


def test_persisted_exclusion_requires_exact_positive_service_version() -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    }
    owner = _excluded_owner({'reserved_fill': True})
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, None)
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 3)


def test_system_recovery_exclusion_matches_exact_bound_request() -> None:
    normalized = {
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.
            ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY: 'request-id',
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY: 9,
    }
    owner = _excluded_owner({
        'system_recovery_launch_intent': _system_recovery_intent(),
        'launch_request_id': 'request-id',
        'system_recovery_disposition': 'CANDIDATE',
        'system_recovery_quarantine': None,
    })
    assert serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)
    owner['binding_excluded_replica_state']['launch_request_id'] = (
        'different-request')
    assert not serve_state._binding_excluded_replica_matches(  # pylint: disable=protected-access
        owner, normalized, 2)


def _context() -> dict[str, object]:
    return {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'svc-hash',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.2',
        binding.REPLICA_ID_KEY: 3,
        binding.REPLICA_RECORD_ID_KEY: str(_RECORD_ID),
        binding.LIFECYCLE_EPOCH_KEY: 4,
        binding.BINDING_EPOCH_KEY: 5,
        binding.CONTROLLER_INCARNATION_KEY: str(_CONTROLLER_ID),
        binding.CONTROLLER_OWNER_EPOCH_KEY: 6,
    }


def _body() -> payloads.LaunchBody:
    return payloads.LaunchBody(
        task='name: task\nresources:\n  cpus: 2\n',
        cluster_name='svc-3',
        is_launched_by_sky_serve_controller=True,
        extra_launch_context=_context(),
        env_vars={'SKYPILOT_USER_ID': 'owner'},
    )


def _identity(body: payloads.LaunchBody) -> binding.BindingIdentity:
    intent = binding.parse_unbound_launch_context(body.extra_launch_context)
    return binding.build_binding_identity(
        intent,
        submission_id=_SUBMISSION_ID,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='svc-3',
        input_digest=binding.canonical_launch_digest(body),
    )


def _non_pool_identity(
        body: payloads.LaunchBody) -> binding.NonPoolBindingIdentity:
    intent = binding.parse_unbound_launch_context(body.extra_launch_context)
    profile = binding.NonPoolLaunchProfile.create(
        binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference='paid-capacity:svc:3',
        authorization_generation=5,
        authorization_payload={'pool_key': 'pool-a'})
    return binding.build_non_pool_binding_identity(
        intent,
        submission_id=_SUBMISSION_ID,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='svc-3',
        input_digest=binding.canonical_launch_digest(body),
        profile=profile,
        capability_cohort_epoch=9,
        capability_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)


def _system_recovery_body_and_identity(
) -> tuple[payloads.LaunchBody, binding.NonPoolBindingIdentity]:
    intent = _system_recovery_intent()
    recovery_context = system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name='svc',
        service_version=2,
        controller_pid=123,
        controller_ip='10.0.0.2')
    recovery_context.update({
        binding.REPLICA_ID_KEY: 3,
        binding.REPLICA_RECORD_ID_KEY: str(_RECORD_ID),
        binding.LIFECYCLE_EPOCH_KEY: 4,
        binding.BINDING_EPOCH_KEY: 5,
        binding.CONTROLLER_INCARNATION_KEY: str(_CONTROLLER_ID),
        binding.CONTROLLER_OWNER_EPOCH_KEY: 6,
    })
    body = _body()
    body.extra_launch_context = recovery_context
    profile = binding.NonPoolLaunchProfile.create(
        binding.NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY,
        authorization_reference=f'system-oom:{intent.launch_nonce}',
        authorization_generation=intent.launch_generation,
        authorization_payload={
            'intent': intent.to_dict(),
            'placement': 'test',
        })
    parsed_intent = binding.parse_unbound_launch_context(recovery_context)
    identity = binding.build_non_pool_binding_identity(
        parsed_intent,
        submission_id=_SUBMISSION_ID,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='svc-3',
        input_digest=binding.canonical_launch_digest(body),
        profile=profile,
        capability_cohort_epoch=9,
        capability_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)
    return body, identity


def test_unbound_parser_accepts_launch_facts_but_no_server_owned_ids() -> None:
    intent = binding.parse_unbound_launch_context(_context())
    assert intent.replica_record_id == _RECORD_ID
    assert intent.controller_incarnation == _CONTROLLER_ID
    assert intent.lifecycle_epoch == 4
    assert intent.binding_epoch == 5

    for key, value in ((binding.SUBMISSION_ID_KEY, str(_SUBMISSION_ID)),
                       (binding.ASSOCIATION_ID_KEY, str(uuid.uuid4())),
                       (binding.BOUND_REQUEST_ID_KEY,
                        str(uuid.uuid4())), (binding.LAUNCH_GENERATION_KEY, 1)):
        context = _context()
        context[key] = value
        try:
            binding.parse_unbound_launch_context(context)
        except ValueError as error:
            assert 'server-owned' in str(error)
        else:
            raise AssertionError(f'Caller-owned {key} was accepted.')


def test_deterministic_ids_include_tenant_workspace_and_submission() -> None:
    first = binding.derive_binding_ids('tenant-a', 'workspace-a',
                                       _SUBMISSION_ID)
    assert first == binding.derive_binding_ids('tenant-a', 'workspace-a',
                                               _SUBMISSION_ID)
    assert first != binding.derive_binding_ids('tenant-b', 'workspace-a',
                                               _SUBMISSION_ID)
    assert first != binding.derive_binding_ids('tenant-a', 'workspace-b',
                                               _SUBMISSION_ID)
    assert first != binding.derive_binding_ids('tenant-a', 'workspace-a',
                                               uuid.uuid4())
    assert all(uuid.UUID(str(value)) for value in first)


def test_digest_excludes_mutable_owner_and_server_binding_fields() -> None:
    body = _body()
    digest = binding.canonical_launch_digest(body)
    changed_owner = copy.deepcopy(body)
    context = changed_owner.extra_launch_context
    context[serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY] = 999
    context[serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY] = 'new-ip'
    context[binding.CONTROLLER_INCARNATION_KEY] = str(uuid.uuid4())
    context[binding.CONTROLLER_OWNER_EPOCH_KEY] = 99
    context[binding.ASSOCIATION_ID_KEY] = str(uuid.uuid4())
    context[binding.BOUND_REQUEST_ID_KEY] = str(uuid.uuid4())
    context[binding.LAUNCH_GENERATION_KEY] = 8
    context[binding.INPUT_DIGEST_KEY] = 'f' * 64
    assert binding.canonical_launch_digest(changed_owner) == digest

    changed_input = copy.deepcopy(body)
    changed_input.task += 'run: echo changed\n'
    assert binding.canonical_launch_digest(changed_input) != digest
    changed_version = copy.deepcopy(body)
    changed_version.extra_launch_context[
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY] = 3
    assert binding.canonical_launch_digest(changed_version) != digest


def test_install_bound_context_contains_only_immutable_authority() -> None:
    body = _body()
    identity = _identity(body)
    binding.install_bound_context(body, identity, 7)
    context = body.extra_launch_context

    assert context[binding.ASSOCIATION_ID_KEY] == str(identity.association_id)
    assert context[binding.BOUND_REQUEST_ID_KEY] == identity.request_id
    assert context[binding.LAUNCH_GENERATION_KEY] == 7
    assert context[binding.INPUT_DIGEST_KEY] == identity.input_digest
    assert binding.CONTROLLER_INCARNATION_KEY not in context
    assert binding.CONTROLLER_OWNER_EPOCH_KEY not in context
    assert binding.OWNER_REVISION_KEY not in context
    assert context[
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY] == -1
    assert context[serve_constants.
                   REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY].endswith('.invalid')
    parsed = binding.parse_bound_launch_context(context)
    assert parsed.association_id == identity.association_id
    assert parsed.replica_record_id == _RECORD_ID
    assert not hasattr(parsed, 'owner_revision')


def test_non_pool_identity_and_context_are_structurally_distinct() -> None:
    body = _body()
    identity = _non_pool_identity(body)
    assert isinstance(identity, binding.NonPoolBindingIdentity)

    with pytest.raises(ValueError, match='install_bound_non_pool_context'):
        binding.install_bound_context(body, identity, 7)
    binding.install_bound_non_pool_context(body, identity, 7)
    context = body.extra_launch_context
    assert context[binding.PROFILE_KIND_KEY] == 'ORDINARY_PAID'
    assert context[binding.CAPABILITY_COHORT_EPOCH_KEY] == 9
    parsed = binding.parse_bound_non_pool_launch_context(context)
    assert isinstance(parsed, binding.BoundNonPoolLaunchContext)
    assert parsed.profile == identity.profile
    assert parsed.capability_profile_set_digest == (
        binding.supported_non_pool_profile_set_digest())
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='cannot enter'):
        binding.parse_bound_launch_context(context)


def test_non_pool_context_rejects_partial_profile() -> None:
    body = _body()
    identity = _non_pool_identity(body)
    binding.install_bound_non_pool_context(body, identity, 7)
    body.extra_launch_context.pop(binding.AUTHORIZATION_DIGEST_KEY)
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='complete profile'):
        binding.parse_bound_non_pool_launch_context(body.extra_launch_context)


def test_system_recovery_profile_binds_complete_execution_envelope() -> None:
    body, identity = _system_recovery_body_and_identity()
    binding.install_bound_non_pool_context(body, identity, 7)

    context = body.extra_launch_context
    parsed = binding.parse_bound_non_pool_launch_context(context)
    recovery = system_oom_recovery.extract_bound_launch_context(context)
    assert parsed.profile.kind == (
        binding.NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY)
    assert recovery[
        serve_constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY] == (
            identity.request_id)
    assert serve_constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY not in context
    assert recovery[
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY] == -1
    assert recovery[serve_constants.
                    REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY].endswith('.invalid')


def test_non_pool_profile_and_recovery_envelope_must_correspond() -> None:
    recovery_body, _ = _system_recovery_body_and_identity()
    ordinary_identity = _non_pool_identity(_body())
    with pytest.raises(ValueError, match='non-recovery profile'):
        binding.install_bound_non_pool_context(recovery_body, ordinary_identity,
                                               7)

    ordinary_body = _body()
    _, recovery_identity = _system_recovery_body_and_identity()
    with pytest.raises(ValueError, match='no recovery execution envelope'):
        binding.install_bound_non_pool_context(ordinary_body, recovery_identity,
                                               7)


def test_partial_context_fails_closed_and_unbound_effect_helpers_noop() -> None:
    assert not binding.has_bound_launch_context({})
    assert binding.has_bound_launch_context(
        {binding.SUBMISSION_ID_KEY: str(_SUBMISSION_ID)})
    partial = {binding.ASSOCIATION_ID_KEY: str(uuid.uuid4())}
    try:
        binding.parse_bound_launch_context(partial)
    except (ValueError, binding.OrdinaryLaunchBindingConflict):
        pass
    else:
        raise AssertionError('Partial bound context was accepted.')

    generic_partial = {
        binding.PROFILE_KIND_KEY:
            binding.NonPoolLaunchProfileKind.ORDINARY_PAID.value,
    }
    assert ordinary_launch_request._has_bound_context_fields(generic_partial)
    with pytest.raises((ValueError, binding.OrdinaryLaunchBindingConflict)):
        ordinary_launch_request._validate_bound_entrypoint_context(
            generic_partial)

    with binding.provider_effect_guard(
        {}, None, claim_validator=lambda *_args: False) as authorization:
        assert authorization is None
    assert binding.begin_service_job_io({}) is None
    assert binding.record_service_job({}, 1) is None


def test_startup_classification_never_infers_effect_absence() -> None:
    association = {
        'resolution': binding.Resolution.BOUND.value,
        'effect_phase': binding.EffectPhase.NOT_STARTED.value,
    }
    assert binding.classify_startup(
        association,
        binding.RequestStartupFacts(
            True, 'PENDING', False, 0, False,
            False)) == (binding.StartupClassification.PRE_EFFECT_TERMINALIZE)
    association['effect_phase'] = binding.EffectPhase.PROVIDER_IO.value
    assert binding.classify_startup(
        association,
        binding.RequestStartupFacts(
            True, 'PENDING', False, 0, False,
            False)) == binding.StartupClassification.AMBIGUOUS


def _effect_rows() -> tuple[dict[str, object], dict[str, object], dict[
    str, object], dict[str, object], binding.BoundLaunchContext]:
    association_id = uuid.uuid4()
    request_id = str(uuid.uuid4())
    info = replica_managers.ReplicaInfo(replica_id=3,
                                        cluster_name='svc-3',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=2,
                                        resources_override=None)
    info.replica_record_id = str(_RECORD_ID)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.RUNNING
    association = {
        'association_id': association_id,
        'request_id': request_id,
        'service_name': 'svc',
        'service_hash': 'svc-hash',
        'service_workspace': 'workspace-a',
        'service_lifecycle_epoch': 4,
        'service_binding_epoch': 5,
        'service_version': 2,
        'replica_id': 3,
        'replica_record_id': _RECORD_ID,
        'launch_generation': 1,
        'cluster_name': 'svc-3',
        'input_digest': 'a' * 64,
        'owner_controller_incarnation': _CONTROLLER_ID,
        'owner_controller_epoch': 6,
        'paid_capacity_pool_key': None,
        'resolution': binding.Resolution.BOUND.value,
    }
    replica = {
        'replica_id': 3,
        'replica_state_version': 1,
        'replica_state': info.to_storage_dict(),
        'status': info.status.value,
        'version': 2,
        'cluster_name': 'svc-3',
        'paid_capacity_pool_key': None,
        'ordinary_launch_association_id': association_id,
    }
    lifecycle = {'epoch': 4}
    service = {
        'lifecycle_epoch': 4,
        'hash': 'svc-hash',
        'workspace': 'workspace-a',
        'ordinary_launch_binding_mode': 'bound',
        'ordinary_launch_binding_epoch': 5,
        'ordinary_launch_binding_capable': True,
        'controller_incarnation': _CONTROLLER_ID,
        'controller_owner_epoch': 6,
        'status': 'READY',
    }
    context = binding.BoundLaunchContext(association_id=association_id,
                                         request_id=request_id,
                                         service_name='svc',
                                         replica_id=3,
                                         replica_record_id=_RECORD_ID,
                                         launch_generation=1,
                                         input_digest='a' * 64)
    return lifecycle, service, replica, association, context


def test_effect_boundary_rejects_durable_teardown_status() -> None:
    lifecycle, service, replica, association, context = _effect_rows()
    info = serve_state.decode_replica_state_for_authority(
        replica['replica_state_version'], replica['replica_state'])
    info.status_property.sky_launch_status = common_utils.ProcessStatus.INTERRUPTED
    replica['replica_state'] = info.to_storage_dict()
    replica['status'] = info.status.value

    with pytest.raises(binding.OrdinaryLaunchBindingConflict, match='status'):
        binding._validate_effect_rows(lifecycle,
                                      service,
                                      replica,
                                      association,
                                      context,
                                      require_launch_authorized=True)
    # Cancellation/reduction must still be able to settle the exact row after
    # teardown persisted INTERRUPTED.
    binding._validate_effect_rows(lifecycle, service, replica, association,
                                  context)


@pytest.mark.parametrize(('field', 'value'), [('version', 3),
                                              ('cluster_name', 'other')])
def test_effect_reduction_rejects_mutated_replica_identity(field,
                                                           value) -> None:
    lifecycle, service, replica, association, context = _effect_rows()
    replica[field] = value
    with pytest.raises(binding.OrdinaryLaunchBindingConflict, match='identity'):
        binding._validate_effect_rows(lifecycle, service, replica, association,
                                      context)


@pytest.mark.parametrize('decoded_key,scalar_key', [('pool-a', None),
                                                    (None, 'pool-a')])
def test_effect_rejects_paid_capacity_scalar_payload_drift(
        decoded_key: str | None, scalar_key: str | None) -> None:
    lifecycle, service, replica, association, context = _effect_rows()
    info = serve_state.decode_replica_state_for_authority(
        replica['replica_state_version'], replica['replica_state'])
    info.paid_capacity_pool_key = decoded_key
    replica['replica_state'] = info.to_storage_dict()
    replica['paid_capacity_pool_key'] = scalar_key
    association['paid_capacity_pool_key'] = scalar_key

    with pytest.raises(binding.OrdinaryLaunchBindingConflict, match='identity'):
        binding._validate_effect_rows(lifecycle,
                                      service,
                                      replica,
                                      association,
                                      context,
                                      require_launch_authorized=True)


@pytest.mark.parametrize('special_state', [
    pytest.param({'reserved_fill': True}, id='reserved-fill'),
    pytest.param({'is_zero_cost': True}, id='zero-cost'),
    pytest.param({'unknown_capacity_replacement': True},
                 id='unknown-capacity-replacement'),
    pytest.param({'cost_rebalance_for_replica_id': 1}, id='cost-rebalance'),
    pytest.param(
        {
            'system_recovery_launch_intent':
                _system_recovery_intent().to_dict(),
            'system_recovery_disposition':
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE.value,
        },
        id='system-recovery-candidate'),
    pytest.param(
        {
            'system_recovery_launch_intent':
                _system_recovery_intent().to_dict(),
            'system_recovery_disposition':
                system_recovery_state.SystemRecoveryDisposition.CAPABLE.value,
            'launch_request_id': 'system-recovery-request',
            'service_job_id': 9,
            'system_recovery': _active_system_recovery().to_dict(),
        },
        id='system-recovery-active'),
    pytest.param(
        {
            'system_recovery_quarantine': {
                'reason': system_recovery_state.RecoveryQuarantineReason.
                          MALFORMED_V13_BUNDLE.value,
            },
        },
        id='system-recovery-quarantine'),
    pytest.param(
        {
            'is_zero_cost': True,
            'system_recovery_launch_intent':
                _system_recovery_intent().to_dict(),
            'system_recovery_disposition':
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE.value,
        },
        id='system-recovery-candidate-zero-cost'),
])
def test_decoded_replica_authority_rejects_every_special_profile(
        special_state: dict[str, object]) -> None:
    lifecycle, service, replica, association, context = _effect_rows()
    replica_state = replica['replica_state']
    assert isinstance(replica_state, dict)
    replica_state.update(copy.deepcopy(special_state))

    # Prove that rejection is due to the special-profile scope check, rather
    # than an invalid serialized recovery record failing to decode first.
    serve_state.decode_replica_state_for_authority(
        replica['replica_state_version'], replica_state)
    assert not binding._replica_snapshot_matches_association(
        replica, association, require_launch_authorized=True)
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='identity|status|cluster'):
        binding._validate_effect_rows(lifecycle,
                                      service,
                                      replica,
                                      association,
                                      context,
                                      require_launch_authorized=True)


def test_api014_serve051_lineage_and_sqlite_stays_at_serve037(
        tmp_path: pathlib.Path) -> None:
    sqlite = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "serve.db"}')
    api_config = migration_utils.get_alembic_config(
        sqlite, migration_utils.API_REQUESTS_DB_NAME)
    serve_config = migration_utils.get_alembic_config(
        sqlite, migration_utils.SERVE_DB_NAME)
    api_scripts = alembic_script.ScriptDirectory.from_config(api_config)
    serve_scripts = alembic_script.ScriptDirectory.from_config(serve_config)

    assert api_scripts.get_heads() == ['015']
    assert api_scripts.get_revision('015').down_revision == '014'
    assert api_scripts.get_revision('013').down_revision == '012'
    assert api_scripts.get_revision('012').down_revision == '011'
    assert api_scripts.get_revision('011').down_revision == '010'
    assert serve_scripts.get_heads() == ['052']
    assert serve_scripts.get_revision('052').down_revision == '051'
    assert serve_scripts.get_revision('050').down_revision == '049'
    assert serve_scripts.get_revision('049').down_revision == '048'
    assert serve_scripts.get_revision('047').down_revision == '046'
    assert serve_scripts.get_revision('046').down_revision == '045'
    assert serve_scripts.get_revision('045').down_revision == '044'
    assert serve_scripts.get_revision('044').down_revision == '043'
    assert serve_scripts.get_revision('043').down_revision == '042'
    assert serve_scripts.get_revision('042').down_revision == '041'
    assert migration_utils.serve_target_version(sqlite) == '037'
    assert server_constants.MIN_ORDINARY_LAUNCH_BINDING_API_VERSION == 74
    assert server_constants.MIN_SERVE_PLACEMENT_PROJECTION_API_VERSION == 77
    assert (server_constants.
            MIN_SERVE_RESERVED_FILL_RECONCILIATION_STATUS_API_VERSION == 76)
    assert (server_constants.
            MIN_KUBERNETES_PREEMPTIBLE_SERVICE_BREAKDOWN_API_VERSION == 78)
    assert server_constants.MIN_NON_POOL_LAUNCH_BINDING_API_VERSION == 80
    assert (server_constants.
            MIN_KUBERNETES_OPERATIONAL_PRIORITY_BREAKDOWN_API_VERSION == 81)
    assert (server_constants.
            MIN_KUBERNETES_OPERATIONAL_WORKLOAD_BREAKDOWN_API_VERSION == 84)
    assert server_constants.MIN_SERVE_DURABLE_DEMAND_API_VERSION == 82
    assert server_constants.MIN_SERVE_ROUTE_PROJECTION_API_VERSION == 83
    assert (
        server_constants.MIN_SERVE_ORDERED_CAPACITY_ADMISSION_API_VERSION == 85)
    assert (server_constants.MIN_SERVE_PARTIAL_IN_FLIGHT_TELEMETRY_API_VERSION
            == 86)
    assert (
        server_constants.MIN_EXECUTOR_TERMINATION_EVIDENCE_API_VERSION == 87)
    assert (
        server_constants.MIN_SERVE_INCREMENTAL_ROUTE_LEASES_API_VERSION == 88)
    assert server_constants.API_VERSION == 90

    alembic_command.upgrade(serve_config, '037')
    inspector = sqlalchemy.inspect(sqlite)
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    replica_columns = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    assert 'controller_incarnation' not in service_columns
    assert 'ordinary_launch_binding_mode' not in service_columns
    assert 'non_pool_launch_binding_capable' not in service_columns
    assert 'non_pool_launch_controller_incarnation' not in service_columns
    assert 'ordinary_launch_association_id' not in replica_columns
    assert not inspector.has_table(
        binding.ordinary_launch_associations_table.name)


def test_runtime_metadata_keeps_serve043_columns() -> None:
    assert 'controller_incarnation' in serve_state_schema.services_table.c
    assert 'ordinary_launch_binding_mode' in serve_state_schema.services_table.c
    assert ('ordinary_launch_association_id'
            in serve_state_schema.replicas_table.c)
    assert list(binding.ordinary_launch_associations_table.c.keys())[:3] == [
        'association_id', 'submission_id', 'tenant_scope'
    ]
    assert {
        'controller_job_projection',
        'controller_work_cache',
        'worker_placement_projections',
    }.issubset(serve_state_schema.version_specs_table.c.keys())
    assert 'storage_broker' not in serve_state_schema.version_specs_table.c


def test_controller_authority_stays_legacy_on_sqlite(tmp_path: pathlib.Path,
                                                     monkeypatch) -> None:
    sqlite = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "legacy.db"}')
    serve_config = migration_utils.get_alembic_config(
        sqlite, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '037')
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine', sqlite)

    assert binding.claim_controller_incarnation(
        'svc',
        'svc-hash', (123, '10.0.0.2'),
        uuid.uuid4(),
        new_parent_owner=(456, '10.0.0.9'),
        expected_lifecycle_epoch=4,
        expected_recovery_version=2) is None
    assert binding.validate_controller_authority(
        None,
        service_name='svc',
        service_hash='svc-hash',
        controller_pid=456,
        controller_ip='10.0.0.9') is None
