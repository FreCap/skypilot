"""Real-PostgreSQL tests for the dark V2 PREPARING trust fence."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,too-many-locals,unused-import

import dataclasses
import datetime
import time
import uuid

import pytest
import sqlalchemy
from test_serve_resource_action_authority_activation_pg import (
    _expire_first_registration_lease)
from test_serve_resource_action_authority_activation_pg import (
    _insert_api_instances)
from test_serve_resource_action_authority_activation_pg import _register_pair
from test_serve_resource_action_authority_activation_pg import _snapshot
from test_serve_resource_action_authority_state_pg import _approved_cohort
from test_serve_resource_action_authority_state_pg import _cohort
from test_serve_resource_action_authority_state_pg import _database_now
from test_serve_resource_action_authority_state_pg import _empty_inventory
from test_serve_resource_action_authority_state_pg import _hashed
from test_serve_resource_action_authority_state_pg import _install_release
from test_serve_resource_action_authority_state_pg import _timestamp
from test_serve_resource_action_authority_state_pg import _worker
from test_serve_resource_action_qualification_policy import _crash_inventory
from test_serve_resource_action_qualification_policy import (
    _deployment_inventory)
import test_serve_resource_action_qualification_policy as qualification_policy_fixtures
from test_serve_resource_action_schema_038_pg import postgres_engine

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.server.requests import requests as requests_lib
from sky.server.requests import resource_actions as kernel_actions
from sky.utils.db import migration_utils

_FIRST_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_SECOND_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_SERVICE_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')
_POLICY_EPOCH = uuid.UUID('018f0f5e-7b8a-7abc-8def-0123456789ab')
_SUCCESSOR_POLICY_EPOCH = uuid.UUID('118f0f5e-7b8a-7abc-8def-0123456789ab')
_SECOND_SUCCESSOR_POLICY_EPOCH = uuid.UUID(
    '218f0f5e-7b8a-7abc-8def-0123456789ab')
_CAPABILITY_SHA256 = 'f' * 64
_UTC = datetime.timezone.utc


@dataclasses.dataclass(frozen=True)
class _TrustState:
    engine: sqlalchemy.engine.Engine
    store: authority_state.ServeResourceActionAuthorityStore
    fence: authority.AuthorityServiceFenceV1
    policy: authority.ResourceActionQualificationPolicyV1
    candidate: authority.ResourceActionCandidateBindingV1
    resource_identity: resource_actions.ProviderResourceIdentityV1


@pytest.fixture(scope='module')
def reference_database(postgres_engine):  # noqa: F811
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.SERVE_DB_NAME, '038')
    return postgres_engine


def _policy() -> authority.ResourceActionQualificationPolicyV1:
    return dataclasses.replace(qualification_policy_fixtures._policy(),
                               approved_cohorts=(_approved_cohort(),))


def _policy_with_additional_cohort(
    policy: authority.ResourceActionQualificationPolicyV1,
    suffix: str,
) -> authority.ResourceActionQualificationPolicyV1:
    """Change policy bytes while retaining the live cohort as selection one."""

    selected = policy.approved_cohorts[0]
    additional = dataclasses.replace(selected,
                                     cohort_id=f'{selected.cohort_id}-{suffix}')
    return dataclasses.replace(policy, approved_cohorts=(selected, additional))


def _candidate_binding(
    policy: authority.ResourceActionQualificationPolicyV1,
    fence: authority.AuthorityServiceFenceV1,
) -> authority.ResourceActionCandidateBindingV1:
    deployments = _deployment_inventory()
    crash_inventory = _crash_inventory()
    profile = (resource_actions.ServeActionCapacityProfileV1.
               ordinary_ondemand_physical_width1())
    elected = resource_actions.ServeServiceVersionSpecIdentityV1(
        version=1,
        service_name=fence.service_name,
        service_incarnation=uuid.UUID(fence.service_hash),
        service_version=1,
        effective_service_config_sha256='a' * 64,
        effective_task_config_sha256='b' * 64,
        capacity_profile=profile,
        provider_profile='pod_cluster_v1')
    binding = authority.ResourceActionCandidateBindingV1(
        version=1,
        qualification_policy_sha256=policy.sha256,
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'),
        deployment_inventory=deployments,
        deployment_inventory_sha256=deployments.sha256,
        selected_cohort=policy.approved_cohorts[0],
        selected_cohort_sha256=policy.approved_cohorts[0].sha256,
        capacity_profile=profile,
        capacity_profile_sha256=profile.sha256,
        elected_version_identity=elected,
        elected_version_identity_sha256=elected.sha256,
        live_replica_identity_inventory=authority.HashedCanonicalObjectV1.
        from_object({
            'contract': 'serve_live_replica_identity_inventory_v1',
            'replicas': [],
            'version': 1,
        }),
        required_crash_canary_inventory=crash_inventory,
        required_crash_canary_inventory_sha256=crash_inventory.sha256)
    binding.validate_for_policy(policy)
    return binding


def _promotion_proof(
    *,
    fence: authority.AuthorityServiceFenceV1,
    policy: authority.ResourceActionQualificationPolicyV1,
    candidate: authority.ResourceActionCandidateBindingV1,
    verified_at: datetime.datetime,
) -> authority.AuthoritativePromotionProofV1:
    return authority.AuthoritativePromotionProofV1(
        version=1,
        service_fence=fence,
        candidate_epoch=_POLICY_EPOCH,
        candidate_since=_timestamp(verified_at - datetime.timedelta(hours=25)),
        verified_at=_timestamp(verified_at),
        candidate_duration_seconds=90_000,
        qualification_policy_sha256=policy.sha256,
        qualification_binding_sha256=candidate.sha256,
        coverage_inventory_sha256='c' * 64,
        clean_launches=100,
        clean_downs=100,
        blocker_count=0,
        crash_canary_inventory=_hashed('crash'),
        referenced_cohort_inventory=_hashed('cohort'),
        deployment_inventory=_hashed('deployment'),
        elected_version_identity=_hashed('version'),
        live_replica_identity_inventory=_hashed('replicas'),
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'))


def _install_preflight_api_capabilities(
    state: _TrustState,
    accepted: authority_state.WorkerCohortV2Record,
) -> None:
    now = _database_now(state.engine)
    workers = {
        item.worker_instance_id: item.worker
        for item in accepted.registration_set.workers
    }
    expected_versions = {
        requests_lib.DURABLE_PAYLOAD_FORMAT: {
            'minimum': requests_lib.DURABLE_PAYLOAD_VERSION,
            'maximum': requests_lib.DURABLE_PAYLOAD_VERSION,
        }
    }
    with state.engine.begin() as connection:
        for worker_id, worker in workers.items():
            connection.execute(
                sqlalchemy.update(
                    request_postgres_schema.SERVER_INSTANCES).where(
                        request_postgres_schema.SERVER_INSTANCES.c.instance_id
                        == worker_id).values(
                            pod_name=worker.pod_name,
                            pod_uid=str(worker.pod_uid),
                            heartbeat_at=now,
                            ready=False,
                            draining_at=None,
                            health_detail={'phase': 'preflight-only'},
                            supported_handlers=sorted(
                                _cohort().manifest.handler_allowlist),
                            supported_payload_versions=expected_versions))


@pytest.fixture
def trust_state(reference_database) -> _TrustState:
    engine = reference_database
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'TRUNCATE TABLE services, service_lifecycle_fences, '
            'serve_resource_action_authority_policy_epochs, '
            'serve_resource_action_worker_cohort_refs, '
            'serve_resource_action_worker_registration_handoffs, '
            'serve_resource_action_worker_registration_leases, '
            'serve_resource_action_worker_cohorts, '
            'serve_resource_action_authority_release_cohorts, '
            'serve_resource_action_authority_releases, '
            'api_server_instances CASCADE')
    _install_release(engine)
    store = authority_state.ServeResourceActionAuthorityStore(engine)
    registering = _register_pair(engine, store)
    _insert_api_instances(engine)
    accepted = store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                             expected_cohort_revision=2,
                                             deployment_snapshot=_snapshot(
                                                 _database_now(engine))).cohort

    fence = authority.AuthorityServiceFenceV1(
        service_name='v2-reference-service',
        service_hash=str(_SERVICE_ID),
        controller_owner_fence='123:10.0.0.1',
        lifecycle_epoch=7)
    policy = _policy()
    candidate = _candidate_binding(policy, fence)
    activated_at = _database_now(engine)
    proof = _promotion_proof(fence=fence,
                             policy=policy,
                             candidate=candidate,
                             verified_at=activated_at)
    inventory = _empty_inventory()
    with engine.begin() as connection:
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.insert().values(
                name=fence.service_name, epoch=fence.lifecycle_epoch))
        connection.execute(serve_state_schema.services_table.insert().values(
            name=fence.service_name,
            hash=fence.service_hash,
            status='READY',
            pool=0,
            controller_pid=123,
            controller_ip='10.0.0.1',
            lifecycle_epoch=fence.lifecycle_epoch,
            resource_action_mode='authoritative',
            resource_action_mode_changed_at=activated_at,
            resource_action_candidate_epoch=_POLICY_EPOCH,
            resource_action_candidate_policy_sha256=policy.sha256,
            resource_action_candidate_binding_sha256=candidate.sha256))
        connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.insert().values(
            service_hash=fence.service_hash,
            policy_epoch=_POLICY_EPOCH,
            predecessor_policy_epoch=None,
            policy=policy.canonical_value(),
            policy_sha256=policy.sha256,
            authority_binding_sha256=candidate.sha256,
            rotation_proof=proof.canonical_value(),
            rotation_proof_sha256=proof.sha256,
            nonterminal_inventory=inventory.canonical_value(),
            nonterminal_inventory_sha256=inventory.sha256,
            reason='INITIAL_PROMOTION',
            policy_state=authority_state.AuthorityPolicyState.ACTIVE.value,
            admission_state=(
                authority_state.AuthorityPolicyAdmissionState.OPEN.value),
            admission_revision=1,
            last_operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb1'),
            last_operation_kind=(
                authority_state.AuthorityPolicyOperation.ACTIVATE.value),
            created_at=activated_at,
            admission_changed_at=activated_at,
            activated_at=activated_at,
            superseded_at=None))
    resource_identity = resource_actions.ProviderResourceIdentityV1(
        service_hash=fence.service_hash,
        service_incarnation=_SERVICE_ID,
        replica_id=4,
        replica_incarnation=uuid.UUID('44444444-4444-4444-8444-444444444444'),
        desired_generation=3)
    state = _TrustState(engine, store, fence, policy, candidate,
                        resource_identity)
    _install_preflight_api_capabilities(state, accepted)
    return state


def _prepare(
    state: _TrustState,
    *,
    capability_sha256: str = _CAPABILITY_SHA256,
    candidate_binding: authority.ResourceActionCandidateBindingV1 | None = None,
) -> authority_state.WorkerCohortReferencePreparationV2:
    if candidate_binding is None:
        candidate_binding = state.candidate
    return state.store.prepare_worker_cohort_reference(
        service_fence=state.fence,
        resource_identity=state.resource_identity,
        action_kind=kernel_actions.ActionKind.LAUNCH,
        expected_manifest=_cohort().manifest,
        preparation_capability_sha256=capability_sha256,
        candidate_binding=candidate_binding)


def _launch_identity_context(
    state: _TrustState,
    *,
    capability_sha256: str = _CAPABILITY_SHA256,
) -> resource_actions.ProviderLaunchIdentityCanonicalizationContextV1:
    canonicalization_input = (
        resource_actions.ProviderLaunchIdentityCanonicalizationInputV1(
            version=1,
            contract='api_server_effective_launch_identity_v1',
            service_name=state.fence.service_name,
            resource_identity=state.resource_identity,
            prepared_original_user='test-user',
            prepared_user_hash='test-user-hash'))
    return resource_actions.ProviderLaunchIdentityCanonicalizationContextV1(
        version=1,
        decision_id=state.resource_identity.action_identity(
            kernel_actions.ActionKind.LAUNCH).action_id,
        cohort_id=_cohort().manifest.cohort_id,
        action_type=kernel_actions.ActionKind.LAUNCH,
        controller_owner_fence=state.fence.controller_owner_fence,
        lifecycle_epoch=state.fence.lifecycle_epoch,
        preparation_reference_revision=1,
        reference_state=resource_actions.WorkerCohortReferenceState.PREPARING,
        preparation_capability_sha256=capability_sha256,
        input=canonicalization_input,
        input_sha256=canonicalization_input.sha256)


def _install_successor_policy(
    state: _TrustState,
    *,
    predecessor_policy_epoch: uuid.UUID = _POLICY_EPOCH,
    predecessor_policy: authority.ResourceActionQualificationPolicyV1 |
    None = None,
    successor_policy_epoch: uuid.UUID = _SUCCESSOR_POLICY_EPOCH,
    successor_suffix: str = 'rotation-successor',
    predecessor_policy_sha256: str | None = None,
) -> tuple[authority.ResourceActionQualificationPolicyV1,
           authority.ResourceActionCandidateBindingV1]:
    """Install a distinct rotated policy over the retained promotion root."""

    now = _database_now(state.engine)
    inventory = _empty_inventory()
    if predecessor_policy is None:
        predecessor_policy = state.policy
    successor_policy = _policy_with_additional_cohort(predecessor_policy,
                                                      successor_suffix)
    successor_candidate = _candidate_binding(successor_policy, state.fence)
    rotation = authority.ServeAuthorityPolicyRotationProofV1(
        version=1,
        service_fence=state.fence,
        predecessor_policy_epoch=predecessor_policy_epoch,
        predecessor_policy_sha256=(predecessor_policy.sha256
                                   if predecessor_policy_sha256 is None else
                                   predecessor_policy_sha256),
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'),
        successor_policy=successor_policy,
        successor_policy_sha256=successor_policy.sha256,
        successor_authority_binding_sha256=successor_candidate.sha256,
        staged_artifact_inventory=_hashed('staged-artifacts'),
        rollback_artifact_inventory=_hashed('rollback-artifacts'),
        service_version_inventory=_hashed('service-versions'),
        cohort_inventory=_hashed('cohort-inventory'),
        nonterminal_inventory=inventory,
        started_at=_timestamp(now),
        completed_at=_timestamp(now),
        reason='COMPATIBLE_IMAGE_ROTATION')
    with state.engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                predecessor_policy_epoch).values(
                    policy_state=authority_state.AuthorityPolicyState.
                    SUPERSEDED.value,
                    admission_state=authority_state.
                    AuthorityPolicyAdmissionState.CLOSED.value,
                    # INITIAL/OPEN -> DRAINING -> CLOSED -> SUPERSEDED.
                    admission_revision=4,
                    last_operation_id=uuid.UUID(
                        'aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb8'),
                    last_operation_kind=authority_state.
                    AuthorityPolicyOperation.SUPERSEDE.value,
                    admission_changed_at=now,
                    superseded_at=now))
        connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.insert().values(
            service_hash=state.fence.service_hash,
            policy_epoch=successor_policy_epoch,
            predecessor_policy_epoch=predecessor_policy_epoch,
            policy=successor_policy.canonical_value(),
            policy_sha256=successor_policy.sha256,
            authority_binding_sha256=successor_candidate.sha256,
            rotation_proof=rotation.canonical_value(),
            rotation_proof_sha256=rotation.sha256,
            nonterminal_inventory=inventory.canonical_value(),
            nonterminal_inventory_sha256=inventory.sha256,
            reason='COMPATIBLE_IMAGE_ROTATION',
            policy_state=authority_state.AuthorityPolicyState.ACTIVE.value,
            admission_state=authority_state.AuthorityPolicyAdmissionState.OPEN.
            value,
            admission_revision=1,
            last_operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb9'),
            last_operation_kind=authority_state.AuthorityPolicyOperation.
            ACTIVATE.value,
            created_at=now,
            admission_changed_at=now,
            activated_at=now,
            superseded_at=None))
    return successor_policy, successor_candidate


def _trust_snapshot(engine: sqlalchemy.engine.Engine) -> tuple:
    tables = (
        serve_state_schema.service_lifecycle_fences_table,
        serve_state_schema.services_table,
        m4_schema.AUTHORITY_POLICY_EPOCHS,
        m4_schema.WORKER_COHORTS_V2,
        m4_schema.WORKER_REGISTRATION_HANDOFFS,
        m4_schema.WORKER_REGISTRATION_LEASES,
        m4_schema.WORKER_COHORT_REFS_V2,
        request_postgres_schema.SERVER_INSTANCES,
    )
    with engine.connect() as connection:
        return tuple(
            tuple(
                dict(row)
                for row in connection.execute(
                    sqlalchemy.select(table).order_by(
                        *tuple(table.primary_key.columns))).mappings())
            for table in tables)


def test_prepare_locks_complete_order_and_exactly_adopts(trust_state) -> None:
    state = trust_state
    statements: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context,
                _executemany) -> None:
        if ' FOR UPDATE' in statement.upper():
            statements.append(statement.lower())

    sqlalchemy.event.listen(state.engine, 'before_cursor_execute', capture)
    try:
        with state.engine.begin() as connection:
            inserted = state.store.prepare_worker_cohort_reference_in_transaction(
                connection,
                service_fence=state.fence,
                resource_identity=state.resource_identity,
                action_kind=kernel_actions.ActionKind.LAUNCH,
                expected_manifest=_cohort().manifest,
                preparation_capability_sha256=_CAPABILITY_SHA256,
                candidate_binding=state.candidate)
    finally:
        sqlalchemy.event.remove(state.engine, 'before_cursor_execute', capture)

    assert not inserted.adopted
    assert inserted.record.reference_state is (
        resource_actions.WorkerCohortReferenceState.PREPARING)
    assert inserted.record.revision == 1
    assert inserted.record.authority_policy_epoch is None
    assert inserted.record.authority_policy_sha256 is None
    assert inserted.record.authority_binding_sha256 is None
    assert inserted.cohort.lifecycle_state is (
        resource_actions.WorkerCohortLifecycleState.ACCEPTING)
    assert len(inserted.accepted_memberships) == 2
    assert inserted.initial_candidate_binding.qualification_binding_sha256 == (
        state.candidate.sha256)
    assert inserted.current_authority_binding.canonical_bytes == (
        state.candidate.canonical_bytes)
    lock_relations = (
        'service_lifecycle_fences',
        'services',
        m4_schema.AUTHORITY_POLICY_EPOCHS.name,
        m4_schema.WORKER_COHORTS_V2.name,
        m4_schema.WORKER_REGISTRATION_HANDOFFS.name,
        m4_schema.WORKER_REGISTRATION_LEASES.name,
        m4_schema.WORKER_COHORT_REFS_V2.name,
    )
    assert len(statements) == len(lock_relations)
    assert all(relation in statement
               for relation, statement in zip(lock_relations, statements))

    adopted = _prepare(state)
    assert adopted.adopted
    assert adopted.record == inserted.record
    with state.engine.begin() as connection:
        read = state.store.read_worker_cohort_reference_in_transaction(
            connection, inserted.record.decision_id)
        assert read == inserted.record
        assert state.store.validate_preparing_worker_cohort_reference_in_transaction(
            connection, inserted.record.reference) == inserted.record
    with state.engine.connect() as connection:
        raw = connection.execute(
            sqlalchemy.select(
                m4_schema.WORKER_COHORT_REFS_V2)).mappings().one()
    assert raw['authority_policy_epoch'] is None
    assert raw['authority_policy_sha256'] is None
    assert raw['authority_binding_sha256'] is None


def test_full_preflight_validation_is_mutation_free_and_accepts_exact_worker(
        trust_state) -> None:
    state = trust_state
    prepared = _prepare(state)
    statements: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context,
                _executemany) -> None:
        if ' FOR UPDATE' in statement.upper():
            statements.append(statement.lower())

    before = _trust_snapshot(state.engine)
    sqlalchemy.event.listen(state.engine, 'before_cursor_execute', capture)
    try:
        record = state.store.validate_preparing_reference_for_preflight(
            worker_instance_id=str(_FIRST_ID),
            expected_manifest=_cohort().manifest,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            launch_identity_context=_launch_identity_context(state))
    finally:
        sqlalchemy.event.remove(state.engine, 'before_cursor_execute', capture)
    assert record == prepared.record
    lock_relations = (
        'service_lifecycle_fences',
        'services',
        m4_schema.AUTHORITY_POLICY_EPOCHS.name,
        m4_schema.WORKER_COHORTS_V2.name,
        m4_schema.WORKER_REGISTRATION_HANDOFFS.name,
        m4_schema.WORKER_REGISTRATION_LEASES.name,
        m4_schema.WORKER_COHORT_REFS_V2.name,
        request_postgres_schema.SERVER_INSTANCES.name,
    )
    assert len(statements) == len(lock_relations)
    assert all(relation in statement
               for relation, statement in zip(lock_relations, statements))
    assert _trust_snapshot(state.engine) == before


@pytest.mark.parametrize('drift',
                         ('capability', 'owner', 'lifecycle', 'service_name'))
def test_preflight_rejects_crossed_launch_canonicalization_context(
        trust_state, drift: str) -> None:
    state = trust_state
    _prepare(state)
    context = _launch_identity_context(state)
    if drift == 'capability':
        context = dataclasses.replace(context,
                                      preparation_capability_sha256='e' * 64)
    elif drift == 'owner':
        context = dataclasses.replace(context,
                                      controller_owner_fence='999:10.0.0.9')
    elif drift == 'lifecycle':
        context = dataclasses.replace(
            context, lifecycle_epoch=state.fence.lifecycle_epoch + 1)
    else:
        crossed_input = dataclasses.replace(context.input,
                                            service_name='crossed-service')
        context = dataclasses.replace(context,
                                      input=crossed_input,
                                      input_sha256=crossed_input.sha256)

    assert context.input.resource_identity.canonical_bytes == (
        state.resource_identity.canonical_bytes)
    assert context.cohort_id == _cohort().manifest.cohort_id
    before = _trust_snapshot(state.engine)
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='canonicalization context'):
        state.store.validate_preparing_reference_for_preflight(
            worker_instance_id=_FIRST_ID,
            expected_manifest=_cohort().manifest,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            launch_identity_context=context)
    assert _trust_snapshot(state.engine) == before


def test_preflight_action_kind_context_boundary_is_closed(trust_state) -> None:
    state = trust_state
    _prepare(state)
    with pytest.raises(TypeError, match='Launch preflight requires'):
        state.store.validate_preparing_reference_for_preflight(
            worker_instance_id=_FIRST_ID,
            expected_manifest=_cohort().manifest,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            launch_identity_context=None)

    down_prepared = state.store.prepare_worker_cohort_reference(
        service_fence=state.fence,
        resource_identity=state.resource_identity,
        action_kind=kernel_actions.ActionKind.DOWN,
        expected_manifest=_cohort().manifest,
        preparation_capability_sha256='d' * 64,
        candidate_binding=state.candidate)
    down_locked = state.store.validate_preparing_reference_for_preflight(
        worker_instance_id=_FIRST_ID,
        expected_manifest=_cohort().manifest,
        resource_identity=state.resource_identity,
        action_kind=kernel_actions.ActionKind.DOWN,
        launch_identity_context=None)
    assert down_locked == down_prepared.record
    with pytest.raises(TypeError, match='Down preflight'):
        state.store.validate_preparing_reference_for_preflight(
            worker_instance_id=_FIRST_ID,
            expected_manifest=_cohort().manifest,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.DOWN,
            launch_identity_context=_launch_identity_context(state))


def test_preparation_uses_rotated_active_policy_without_rewriting_service_root(
        trust_state) -> None:
    """Compatible rotation advances authority, not the promotion anchor."""

    state = trust_state
    successor_policy, successor_candidate = _install_successor_policy(state)
    assert successor_policy.sha256 != state.policy.sha256
    assert successor_candidate.sha256 != state.candidate.sha256
    assert successor_candidate.selected_cohort.canonical_bytes == (
        _approved_cohort().canonical_bytes)

    prepared = _prepare(state, candidate_binding=successor_candidate)
    assert prepared.initial_candidate_binding.candidate_epoch == _POLICY_EPOCH
    assert prepared.initial_candidate_binding.qualification_policy_sha256 == (
        state.policy.sha256)
    assert prepared.authority_policy.policy_epoch == _SUCCESSOR_POLICY_EPOCH
    assert prepared.authority_policy.policy.canonical_bytes == (
        successor_policy.canonical_bytes)
    assert prepared.current_authority_binding.canonical_bytes == (
        successor_candidate.canonical_bytes)
    state.store.validate_preparing_reference_for_preflight(
        worker_instance_id=_FIRST_ID,
        expected_manifest=_cohort().manifest,
        resource_identity=state.resource_identity,
        action_kind=kernel_actions.ActionKind.LAUNCH,
        launch_identity_context=_launch_identity_context(state))


def test_rotated_policy_rejects_crossed_predecessor_hash(trust_state) -> None:
    state = trust_state
    _, successor_candidate = _install_successor_policy(
        state, predecessor_policy_sha256='e' * 64)

    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='locked predecessor'):
        _prepare(state, candidate_binding=successor_candidate)


def test_rotated_policy_rejects_drifted_predecessor_bytes(trust_state) -> None:
    state = trust_state
    _, successor_candidate = _install_successor_policy(state)
    drifted_root_policy = _policy_with_additional_cohort(
        state.policy, 'drifted-root')
    drifted_root_candidate = _candidate_binding(drifted_root_policy,
                                                state.fence)
    with state.engine.connect() as connection:
        root_activated_at = connection.execute(
            sqlalchemy.select(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.activated_at).where(
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                    _POLICY_EPOCH)).scalar_one()
    drifted_root_proof = _promotion_proof(fence=state.fence,
                                          policy=drifted_root_policy,
                                          candidate=drifted_root_candidate,
                                          verified_at=root_activated_at)
    with state.engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                state.fence.service_name).values(
                    resource_action_candidate_policy_sha256=(
                        drifted_root_policy.sha256),
                    resource_action_candidate_binding_sha256=(
                        drifted_root_candidate.sha256)))
        connection.execute(
            sqlalchemy.update(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                _POLICY_EPOCH).values(
                    policy=drifted_root_policy.canonical_value(),
                    policy_sha256=drifted_root_policy.sha256,
                    authority_binding_sha256=drifted_root_candidate.sha256,
                    rotation_proof=drifted_root_proof.canonical_value(),
                    rotation_proof_sha256=drifted_root_proof.sha256))

    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='locked predecessor'):
        _prepare(state, candidate_binding=successor_candidate)


def test_rotated_policy_rejects_impossible_predecessor_history(
        trust_state) -> None:
    state = trust_state
    _, successor_candidate = _install_successor_policy(state)
    # The physical CHECK permits any revision > 1 for SUPERSEDE.  Revision two
    # would skip the mandatory DRAIN and CLOSE edges, so the typed reader must
    # reject it independently of catalog constraints.
    with state.engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                _POLICY_EPOCH).values(admission_revision=2))

    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='closed shape'):
        _prepare(state, candidate_binding=successor_candidate)


def test_three_epoch_lineage_validates_every_predecessor(trust_state) -> None:
    state = trust_state
    first_policy, _ = _install_successor_policy(state)
    second_policy, second_candidate = _install_successor_policy(
        state,
        predecessor_policy_epoch=_SUCCESSOR_POLICY_EPOCH,
        predecessor_policy=first_policy,
        successor_policy_epoch=_SECOND_SUCCESSOR_POLICY_EPOCH,
        successor_suffix='second-rotation-successor')

    prepared = _prepare(state, candidate_binding=second_candidate)
    assert prepared.authority_policy.policy_epoch == (
        _SECOND_SUCCESSOR_POLICY_EPOCH)
    assert prepared.authority_policy.policy.canonical_bytes == (
        second_policy.canonical_bytes)

    # Re-hash a typed intermediate proof whose own row remains internally
    # consistent and whose service hash still matches, but whose owner fence
    # crosses the exact service fence shared by both lineage endpoints.
    with state.engine.connect() as connection:
        raw_proof = connection.execute(
            sqlalchemy.select(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.rotation_proof).where(
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                    _SUCCESSOR_POLICY_EPOCH)).scalar_one()
    crossed_proof = authority.ServeAuthorityPolicyRotationProofV1.from_value(
        raw_proof)
    crossed_fence = dataclasses.replace(state.fence,
                                        controller_owner_fence='999:10.0.0.9')
    crossed_proof = dataclasses.replace(crossed_proof,
                                        service_fence=crossed_fence)
    with state.engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                _SUCCESSOR_POLICY_EPOCH).values(
                    rotation_proof=crossed_proof.canonical_value(),
                    rotation_proof_sha256=crossed_proof.sha256))

    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='crosses the locked service fence'):
        _prepare(state, candidate_binding=second_candidate)


def test_preflight_lock_contention_fails_closed_inside_database_budget(
        trust_state) -> None:
    state = trust_state
    _prepare(state)
    statements: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context,
                _executemany) -> None:
        normalized = ' '.join(statement.lower().split())
        if normalized.startswith('set local') or ' for update' in normalized:
            statements.append(normalized)

    with state.engine.connect() as blocker:
        blocker_transaction = blocker.begin()
        try:
            blocker.execute(
                sqlalchemy.select(
                    serve_state_schema.service_lifecycle_fences_table).where(
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == state.fence.service_name).with_for_update()).one()
            sqlalchemy.event.listen(state.engine, 'before_cursor_execute',
                                    capture)
            started_at = time.monotonic()
            try:
                with pytest.raises(authority_state.AuthorityStateConflict,
                                   match='database transaction'):
                    state.store.validate_preparing_reference_for_preflight(
                        worker_instance_id=_FIRST_ID,
                        expected_manifest=_cohort().manifest,
                        resource_identity=state.resource_identity,
                        action_kind=kernel_actions.ActionKind.LAUNCH,
                        launch_identity_context=_launch_identity_context(state))
            finally:
                elapsed_seconds = time.monotonic() - started_at
                sqlalchemy.event.remove(state.engine, 'before_cursor_execute',
                                        capture)
        finally:
            blocker_transaction.rollback()

    assert elapsed_seconds < 4.5
    assert (0 < authority_state._PREFLIGHT_TRUST_LOCK_TIMEOUT_MILLISECONDS <
            authority_state._PREFLIGHT_TRUST_STATEMENT_TIMEOUT_MILLISECONDS <
            5_000)
    assert statements[:2] == [
        "set local statement_timeout = '3500ms'",
        "set local lock_timeout = '750ms'",
    ]
    assert statements[2] == (
        "set local idle_in_transaction_session_timeout = '4000ms'")
    assert any(
        'service_lifecycle_fences' in statement and ' for update' in statement
        for statement in statements[3:])


def test_preflight_transaction_installs_all_server_side_limits(
        trust_state) -> None:
    state = trust_state
    with state.engine.begin() as connection:
        state.store._bound_preflight_trust_read(connection)
        settings = connection.execute(
            sqlalchemy.text(
                "SELECT current_setting('statement_timeout'), "
                "current_setting('lock_timeout'), "
                "current_setting('idle_in_transaction_session_timeout')")).one(
                )
    assert tuple(settings) == ('3500ms', '750ms', '4s')


def test_cumulative_preflight_deadline_starts_no_late_statement(
        trust_state, monkeypatch: pytest.MonkeyPatch) -> None:
    state = trust_state
    executed: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context,
                _executemany) -> None:
        executed.append(' '.join(statement.lower().split()))

    ticks = iter((1.0, 2.0, 3.0, 11.0))
    monkeypatch.setattr(authority_state.time, 'monotonic', lambda: next(ticks))
    sqlalchemy.event.listen(state.engine, 'after_cursor_execute', capture)
    try:
        with state.engine.begin() as connection:
            with pytest.raises(authority_state.AuthorityStateConflict,
                               match='cumulative budget'):
                state.store._validate_preparing_reference_with_deadline(
                    connection,
                    deadline_monotonic=10.0,
                    worker_instance_id=_FIRST_ID,
                    expected_manifest=_cohort().manifest,
                    resource_identity=state.resource_identity,
                    action_kind=kernel_actions.ActionKind.LAUNCH,
                    launch_identity_context=_launch_identity_context(state))
    finally:
        sqlalchemy.event.remove(state.engine, 'after_cursor_execute', capture)
    assert executed == [
        "set local statement_timeout = '3500ms'",
        "set local lock_timeout = '750ms'",
        "set local idle_in_transaction_session_timeout = '4000ms'",
    ]


def test_caller_owned_helpers_reject_connection_without_transaction(
        trust_state) -> None:
    state = trust_state
    decision_id = state.resource_identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    with state.engine.connect() as connection:
        with pytest.raises(RuntimeError, match='caller-owned transaction'):
            state.store.read_worker_cohort_reference_in_transaction(
                connection, decision_id)


@pytest.mark.parametrize(
    'drift', ('lifecycle', 'policy', 'cohort', 'lease', 'lease_future'))
def test_prepare_rejects_locked_trust_drift(trust_state, drift: str) -> None:
    state = trust_state
    if drift == 'lifecycle':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    serve_state_schema.service_lifecycle_fences_table).values(
                        epoch=state.fence.lifecycle_epoch + 1))
    elif drift == 'policy':
        state.store.drain_policy(
            service_fence=state.fence,
            policy_epoch=_POLICY_EPOCH,
            expected_revision=1,
            operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb2'))
    elif drift == 'cohort':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORTS_V2).values(
                    lifecycle_state=(resource_actions.
                                     WorkerCohortLifecycleState.DRAINING.value),
                    state_changed_at=sqlalchemy.func.clock_timestamp()))
    elif drift == 'lease':
        _expire_first_registration_lease(state.engine)
    else:
        with state.engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    _FIRST_ID)).mappings().one()
        lease = state.store._lease_record(row)
        future = _database_now(state.engine) + datetime.timedelta(minutes=1)
        worker = dataclasses.replace(lease.renewal_registration.worker,
                                     observed_at=_timestamp(future))
        registration = authority.ProviderAuthorityWorkerRegistrationV2(
            version=2,
            worker_instance_id=_FIRST_ID,
            worker=worker,
            pod_ready=True,
            registered_at=_timestamp(future))
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    _FIRST_ID).values(
                        renewal_registration=registration.canonical_value(),
                        renewal_registration_sha256=registration.sha256,
                        renewed_at=future,
                        expires_at=future + datetime.timedelta(seconds=60)))

    with pytest.raises(authority_state.AuthorityStateError):
        _prepare(state)
    with state.engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                m4_schema.WORKER_COHORT_REFS_V2)).scalar_one() == 0


def test_prepare_rejects_nonterminal_handoff(trust_state) -> None:
    state = trust_state
    now = _database_now(state.engine)
    cohort = state.store.read_worker_bootstrap_state(_cohort().cohort_id,
                                                     _FIRST_ID)
    assert cohort is not None
    registration_set = cohort.cohort.registration_set
    first, second = registration_set.workers
    candidate_id = uuid.UUID('55555555-5555-4555-8555-555555555555')
    candidate_worker = _worker(candidate_id, now)
    candidate_registration = state.store._registration_at(candidate_worker, now)
    with state.engine.begin() as connection:
        connection.execute(m4_schema.WORKER_REGISTRATION_LEASES.insert().values(
            **state.store._lease_values(
                _cohort().cohort_id, candidate_registration,
                uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa8'), now)))
        connection.execute(
            m4_schema.WORKER_REGISTRATION_HANDOFFS.insert().values(
                cohort_id=_cohort().cohort_id,
                handoff_id=uuid.UUID('66666666-6666-4666-8666-666666666666'),
                predecessor_handoff_id=None,
                chain_sequence=1,
                stale_fence_disposition='NEWLY_REVOKED',
                source_cohort_revision=registration_set.revision,
                source_cohort_state='ACCEPTING',
                source_registration_set_revision=registration_set.revision,
                source_registration_set=registration_set.canonical_value(),
                source_registration_set_sha256=registration_set.sha256,
                stale_worker_instance_id=first.worker_instance_id,
                stale_pod_name=first.worker.pod_name,
                stale_pod_uid=first.worker_instance_id,
                survivor_worker_instance_id=second.worker_instance_id,
                survivor_pod_uid=second.worker_instance_id,
                candidate_worker_instance_id=candidate_id,
                candidate_pod_name=candidate_worker.pod_name,
                candidate_pod_uid=candidate_id,
                stale_authority_fence={'version': 1},
                stale_authority_fence_sha256=authority.canonical_sha256(
                    {'version': 1}),
                stale_uid_absence_proof={'version': 1},
                stale_uid_absence_proof_sha256=authority.canonical_sha256(
                    {'version': 1}),
                candidate_registration=candidate_registration.canonical_value(),
                candidate_registration_sha256=candidate_registration.sha256,
                handoff_state='OPEN',
                revision=1,
                opened_at=now,
                fenced_at=now))
    with pytest.raises(authority_state.AuthorityStateConflict, match='handoff'):
        _prepare(state)


def test_prepare_rejects_candidate_selection_or_lost_ack_drift(
        trust_state) -> None:
    state = trust_state
    drifted_candidate = dataclasses.replace(
        state.candidate,
        live_replica_identity_inventory=authority.HashedCanonicalObjectV1.
        from_object({
            'contract': 'serve_live_replica_identity_inventory_v1',
            'replicas': [{
                'drift': True
            }],
            'version': 1,
        }))
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='selection'):
        state.store.prepare_worker_cohort_reference(
            service_fence=state.fence,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            expected_manifest=_cohort().manifest,
            preparation_capability_sha256=_CAPABILITY_SHA256,
            candidate_binding=drifted_candidate)

    committed = _prepare(state)
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='another reference'):
        _prepare(state, capability_sha256='e' * 64)
    assert _prepare(state).record == committed.record


def test_strict_decoder_rejects_revision_or_policy_triple_corruption(
        trust_state) -> None:
    state = trust_state
    _prepare(state)
    with state.engine.connect() as connection:
        raw = dict(
            connection.execute(
                sqlalchemy.select(
                    m4_schema.WORKER_COHORT_REFS_V2)).mappings().one())
    revision_drift = dict(raw, revision=2)
    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='reference row'):
        authority_state.decode_worker_cohort_reference_v2_row(revision_drift)
    policy_drift = dict(raw,
                        authority_policy_epoch=_POLICY_EPOCH,
                        authority_policy_sha256='a' * 64,
                        authority_binding_sha256='b' * 64)
    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='reference row'):
        authority_state.decode_worker_cohort_reference_v2_row(policy_drift)

    # The physical schema deliberately permits any positive revision.  The
    # typed V2 PREPARING decoder is the stricter revision-one trust boundary.
    with state.engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                m4_schema.WORKER_COHORT_REFS_V2).values(revision=2))
    with state.engine.begin() as connection:
        with pytest.raises(authority_state.AuthorityStateCorruption):
            state.store.read_worker_cohort_reference_in_transaction(
                connection, raw['decision_id'])


@pytest.mark.parametrize('drift',
                         ('ready', 'heartbeat', 'handlers', 'nonmember',
                          'reference', 'reference_future', 'binding', 'cohort'))
def test_preflight_validator_rejects_crossed_or_stale_trust(
        trust_state, drift: str) -> None:
    state = trust_state
    prepared = _prepare(state)
    worker_id: uuid.UUID = _FIRST_ID
    if drift == 'ready':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres_schema.SERVER_INSTANCES).where(
                        request_postgres_schema.SERVER_INSTANCES.c.instance_id
                        == _FIRST_ID).values(ready=True))
    elif drift == 'heartbeat':
        stale = _database_now(state.engine) - datetime.timedelta(seconds=21)
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres_schema.SERVER_INSTANCES).where(
                        request_postgres_schema.SERVER_INSTANCES.c.instance_id
                        == _FIRST_ID).values(started_at=stale,
                                             heartbeat_at=stale))
    elif drift == 'handlers':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres_schema.SERVER_INSTANCES).where(
                        request_postgres_schema.SERVER_INSTANCES.c.instance_id
                        == _FIRST_ID).values(supported_handlers=[]))
    elif drift == 'nonmember':
        worker_id = uuid.UUID('77777777-7777-4777-8777-777777777777')
    elif drift == 'reference':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORT_REFS_V2).values(
                    reference_state=(resource_actions.WorkerCohortReferenceState
                                     .SHADOW_ACTIVE.value),
                    revision=2,
                    bound_at=sqlalchemy.func.clock_timestamp()))
    elif drift == 'reference_future':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORT_REFS_V2).values(
                    created_at=(sqlalchemy.func.clock_timestamp() +
                                sqlalchemy.text("INTERVAL '1 minute'"))))
    elif drift == 'binding':
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).values(
                    resource_action_candidate_binding_sha256='e' * 64))
    else:
        with state.engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORTS_V2).values(
                    lifecycle_state=(resource_actions.
                                     WorkerCohortLifecycleState.DRAINING.value),
                    state_changed_at=sqlalchemy.func.clock_timestamp()))

    with pytest.raises(authority_state.AuthorityStateError):
        state.store.validate_preparing_reference_for_preflight(
            worker_instance_id=worker_id,
            expected_manifest=_cohort().manifest,
            resource_identity=state.resource_identity,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            launch_identity_context=_launch_identity_context(state))
    assert prepared.record.decision_id == state.resource_identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
