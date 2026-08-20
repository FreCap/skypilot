"""PostgreSQL state-machine contracts for ordinary Serve launch binding."""
# pylint: disable=not-callable,protected-access,redefined-outer-name
# pylint: disable=unused-argument,unused-import

import copy
import dataclasses
import datetime
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding as binding
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.serve import system_recovery_state
from sky.server.requests import payloads
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_ordinary_launch_binding_schema_050_pg')

_SUBMISSION_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CONTROLLER_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')


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


def _stored_replica_state(
        fields: dict[str, object] | None = None) -> dict[str, object]:
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
    for field, value in (fields or {}).items():
        if dataclasses.is_dataclass(value):
            value = dataclasses.asdict(value)
        elif isinstance(value, system_recovery_state.SystemRecoveryDisposition):
            value = value.value
        state[field] = value
    return state


@pytest.fixture
def binding_database(empty_postgres, monkeypatch):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)

    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc', epoch=4))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                workspace='workspace-a',
                status='READY',
                hash='svc-hash',
                current_version=2,
                active_versions='[2]',
                pool=0,
                controller_pid=123,
                controller_ip='10.0.0.2',
                lifecycle_epoch=4,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=5))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=2,
                yaml_content='service:\n  min_replicas: 0\n',
                controller_applied_at=1.0))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=3,
                replica_state_version=1,
                status='PROVISIONING',
                version=2,
                cluster_name='svc-3',
                is_spot=False,
                replica_state=_stored_replica_state()))
    return empty_postgres


def _unbound_context() -> dict[str, object]:
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
    return payloads.LaunchBody(task='name: task\nresources:\n  cpus: 2\n',
                               cluster_name='svc-3',
                               is_launched_by_sky_serve_controller=True,
                               extra_launch_context=copy.deepcopy(
                                   _unbound_context()),
                               env_vars={'SKYPILOT_USER_ID': 'owner'})


def _identity(*,
              submission_id: uuid.UUID = _SUBMISSION_ID,
              digest: str | None = None) -> binding.BindingIdentity:
    body = _body()
    intent = binding.parse_unbound_launch_context(body.extra_launch_context)
    return binding.build_binding_identity(
        intent,
        submission_id=submission_id,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='svc-3',
        input_digest=(binding.canonical_launch_digest(body)
                      if digest is None else digest))


def _admit(database, identity: binding.BindingIdentity | None = None):
    identity = _identity() if identity is None else identity
    with database.begin() as connection:
        admission = binding.insert_or_get_locked(connection, identity)
    return identity, admission


def _bound_context(identity: binding.BindingIdentity,
                   generation: int) -> binding.BoundLaunchContext:
    body = _body()
    binding.install_bound_context(body, identity, generation)
    return binding.parse_bound_launch_context(body.extra_launch_context)


def _bound_launch_context(identity: binding.BindingIdentity,
                          generation: int) -> dict[str, object]:
    body = _body()
    binding.install_bound_context(body, identity, generation)
    return body.extra_launch_context


def _pre_effect_settle(connection, context: binding.BoundLaunchContext) -> None:
    evidence = binding.TerminalEvidence(
        status=binding.TerminalStatus.FAILED,
        cause='request never acquired an execution claim',
        execution_generation=0,
        quiescence_required=True,
        quiesced_generation=0,
        quiesced_at=datetime.datetime.now(datetime.timezone.utc))
    assert binding.record_terminal_in_connection(
        connection, context,
        evidence) == binding.StartupClassification.PRE_EFFECT_TERMINALIZE
    assert binding.project_in_connection(connection,
                                         context,
                                         pre_effect_terminal=True,
                                         service_job_id=None)


def _controller_authority() -> binding.ControllerBindingAuthority:
    return binding.ControllerBindingAuthority(
        service_name='svc',
        service_hash='svc-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_CONTROLLER_ID,
        controller_owner_epoch=6,
        capable=True,
        binding_mode=binding.BindingMode.BOUND,
        binding_epoch=5)


def _legacy_identity() -> binding.LegacyLaunchIdentity:
    return binding.LegacyLaunchIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=4,
        replica_id=3,
        replica_record_id=_RECORD_ID,
        replica_version=2,
        cluster_name='svc-3',
        request_id='legacy-request-3',
        provider_context='kubernetes-context-a',
        provider_physical_resource_uid='cluster-uid-a')


def _legacy_evidence(
    *,
    executor_terminated_at: datetime.datetime | None = None,
    provider_evidence: binding.ProviderEvidence = binding.ProviderEvidence.
    NOT_QUERIED,
    provider_evidence_observed_at: datetime.datetime | None = None,
) -> binding.LegacyReconciliationEvidence:
    return binding.LegacyReconciliationEvidence(
        observed_request_status='CANCELLED',
        observed_request_execution_generation=0,
        observed_request_queue_present=False,
        observed_request_claim_present=False,
        observed_request_result_digest=None,
        observed_request_at=datetime.datetime(2026,
                                              8,
                                              16,
                                              1,
                                              tzinfo=datetime.timezone.utc),
        observed_request_evidence={
            'request_id': 'legacy-request-3',
            'source': 'api_requests',
        },
        executor_terminated_at=executor_terminated_at,
        executor_termination_evidence=(None
                                       if executor_terminated_at is None else {
                                           'pod_uid': 'old-api-pod-uid',
                                           'termination': 'observed',
                                       }),
        provider_evidence=provider_evidence,
        provider_evidence_observed_at=provider_evidence_observed_at,
        provider_evidence_payload=(None if provider_evidence
                                   == binding.ProviderEvidence.NOT_QUERIED else
                                   {
                                       'context': 'kubernetes-context-a',
                                       'physical_cluster_uid': 'cluster-uid-a',
                                       'resource': 'svc-3',
                                   }))


def test_serve047_legacy_scope_requires_monotonic_exact_evidence_and_projects(
        binding_database) -> None:
    identity = _legacy_identity()
    terminated_at = datetime.datetime(2026,
                                      8,
                                      16,
                                      2,
                                      tzinfo=datetime.timezone.utc)
    provider_at = terminated_at + datetime.timedelta(minutes=1)
    with binding_database.begin() as connection:
        scope_id = binding.create_legacy_reconciliation_scope_in_connection(
            connection, [identity],
            reviewed_by='operator@example.com',
            review_reason='Mixed-version executor left an unbound row.')
        ambiguous = binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS,
            _legacy_evidence(),
            actor='reconciler',
            reason='No current-protocol receipt exists.')
        replay = binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS,
            _legacy_evidence(),
            actor='reconciler',
            reason='No current-protocol receipt exists.')
        assert replay['event_id'] == ambiguous['event_id']
        authorized = binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            binding.LegacyReconciliationResolution.CLEANUP_AUTHORIZED,
            _legacy_evidence(executor_terminated_at=terminated_at,
                             provider_evidence=binding.ProviderEvidence.ABSENT,
                             provider_evidence_observed_at=provider_at),
            actor='reconciler',
            reason='Exact provider UID is absent after executor termination.')
        assert authorized['reconciliation_sequence'] == 2
        assert binding.project_legacy_replica_cleanup_in_connection(
            connection,
            scope_id,
            identity,
            actor='reconciler',
            reason='Delete the exact phantom replica row.',
            cleanup_completion_evidence={
                'deleted_replica_record_id': str(_RECORD_ID),
                'operation': 'database-projection',
            })

    with binding_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id ==
                    3)).scalar_one() == 0
        events = connection.execute(
            sqlalchemy.select(binding.legacy_reconciliations_table).order_by(
                binding.legacy_reconciliations_table.c.reconciliation_sequence)
        ).mappings().all()
    assert [event['resolution'] for event in events] == [
        binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS.value,
        binding.LegacyReconciliationResolution.CLEANUP_AUTHORIZED.value,
        binding.LegacyReconciliationResolution.PROJECTED.value,
    ]
    assert events[-1]['cleanup_completion_evidence'] == {
        'deleted_replica_record_id': str(_RECORD_ID),
        'operation': 'database-projection',
    }


def test_serve047_legacy_cleanup_rejects_missing_absence_authority(
        binding_database) -> None:
    identity = _legacy_identity()
    with binding_database.begin() as connection:
        scope_id = binding.create_legacy_reconciliation_scope_in_connection(
            connection, [identity],
            reviewed_by='operator@example.com',
            review_reason='Bounded legacy review scope.')
        binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS,
            _legacy_evidence(),
            actor='reconciler',
            reason='No current-protocol receipt exists.')
        with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                           match='not authorized'):
            binding.project_legacy_replica_cleanup_in_connection(
                connection,
                scope_id,
                identity,
                actor='reconciler',
                reason='Must remain quarantined.',
                cleanup_completion_evidence={
                    'operation': 'forbidden',
                })


def test_serve047_legacy_scope_and_events_are_append_only(
        binding_database) -> None:
    identity = _legacy_identity()
    with binding_database.begin() as connection:
        scope_id = binding.create_legacy_reconciliation_scope_in_connection(
            connection, [identity],
            reviewed_by='operator@example.com',
            review_reason='Bounded legacy review scope.')
        binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS,
            _legacy_evidence(),
            actor='reconciler',
            reason='No current-protocol receipt exists.')

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    binding.legacy_reconciliation_scopes_table).where(
                        binding.legacy_reconciliation_scopes_table.c.scope_id ==
                        scope_id).values(review_reason='mutated'))
    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(binding.legacy_reconciliations_table).where(
                    binding.legacy_reconciliations_table.c.scope_id ==
                    scope_id))


def _generic_controller_authority() -> binding.ControllerBindingAuthority:
    return dataclasses.replace(
        _controller_authority(),
        binding_epoch=6,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))


def _admit_generic_paid(
    database,
) -> tuple[binding.NonPoolBindingIdentity, binding.BoundNonPoolLaunchContext]:
    with database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        state = _stored_replica_state({'paid_capacity_pool_key': 'pool-a'})
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING',
                    paid_capacity_pool_key='pool-a',
                    replica_state=state))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_pools_table).values(
                    pool_key='pool-a',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=time.time()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_claims_table).values(
                    service_name='svc',
                    service_hash='svc-hash',
                    replica_id=3,
                    pool_key='pool-a',
                    priority=1,
                    claimed_at=time.time()))

    profile = binding.resolve_non_pool_launch_profile('svc', 3, _RECORD_ID)
    launch_body = _body()
    launch_body.extra_launch_context[binding.BINDING_EPOCH_KEY] = 6
    intent = binding.parse_unbound_launch_context(
        launch_body.extra_launch_context)
    identity = binding.build_non_pool_binding_identity(
        intent,
        submission_id=_SUBMISSION_ID,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='svc-3',
        input_digest=binding.canonical_launch_digest(launch_body),
        profile=profile,
        capability_cohort_epoch=binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
        capability_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)
    with database.begin() as connection:
        admission = binding.insert_or_get_locked(connection, identity)
    binding.install_bound_non_pool_context(launch_body, identity,
                                           admission.launch_generation)
    return identity, binding.parse_bound_non_pool_launch_context(
        launch_body.extra_launch_context)


def test_serve047_provider_evidence_is_owner_fenced_and_monotonic(
        binding_database) -> None:
    identity, context = _admit_generic_paid(binding_database)
    with binding_database.begin() as connection:
        assert binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')

    terminal = binding.TerminalEvidence(status=binding.TerminalStatus.CANCELLED,
                                        cause='execution_lease_expired',
                                        execution_generation=0,
                                        quiescence_required=True,
                                        quiesced_generation=0,
                                        quiesced_at=datetime.datetime.now(
                                            datetime.timezone.utc))

    def _record(evidence, result, *, quiescent=True):
        with binding_database.begin() as connection:
            return binding.record_non_pool_provider_evidence(
                connection, context, _generic_controller_authority(), evidence,
                {
                    'cluster_name': 'svc-3',
                    'probe_contract': 'test-provider-v1',
                    'result': result,
                }, lambda _connection, _context: terminal
                if quiescent else None)

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='exact request quiescence'):
        _record(binding.ProviderEvidence.ABSENT, 'ABSENT', quiescent=False)
    assert _record(binding.ProviderEvidence.PRESENT, 'PRESENT')
    # A later unreadable provider must not erase stronger presence evidence.
    assert not _record(binding.ProviderEvidence.UNKNOWN, 'UNKNOWN')
    assert _record(binding.ProviderEvidence.ABSENT, 'ABSENT')
    assert not _record(binding.ProviderEvidence.ABSENT, 'ABSENT')
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='terminal classification'):
        _record(binding.ProviderEvidence.PRESENT, 'PRESENT')

    with binding_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(binding.ordinary_launch_associations_table).where(
                binding.ordinary_launch_associations_table.c.association_id ==
                identity.association_id)).mappings().one()
    assert association['resolution'] == binding.Resolution.AMBIGUOUS.value
    assert association['reconciliation_outcome'] == (
        binding.ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value)
    assert association['provider_evidence'] == (
        binding.ProviderEvidence.ABSENT.value)
    assert association['provider_evidence_observed_at'] is not None
    assert association['provider_evidence_payload'] == {
        'cluster_name': 'svc-3',
        'probe_contract': 'test-provider-v1',
        'result': 'ABSENT',
    }
    assert len(association['provider_evidence_digest']) == 64


def test_pre_admission_generic_intent_retirement_is_effect_free_and_exact(
        binding_database) -> None:
    """A pointerless post-cutover row releases its planner debit only."""
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc'))
        promoted_epoch = binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        assert promoted_epoch == 6
        state = _stored_replica_state({'paid_capacity_pool_key': 'pool-a'})
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=3,
                replica_state_version=1,
                status='PROVISIONING',
                version=2,
                cluster_name='svc-3',
                is_spot=False,
                paid_capacity_pool_key='pool-a',
                replica_state=state))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_pools_table).values(
                    pool_key='pool-a',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=time.time()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_claims_table).values(
                    service_name='svc',
                    service_hash='svc-hash',
                    replica_id=3,
                    pool_key='pool-a',
                    priority=1,
                    claimed_at=time.time()))

    retired = binding.retire_pre_admission_non_pool_launch_intent(
        _generic_controller_authority(), 3, _RECORD_ID)

    assert retired == binding.PreAdmissionRetirement(
        binding.PreAdmissionRetirementDisposition.RETIRED,
        binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    with binding_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.paid_capacity_claims_table)).scalar_one(
                ) == 0

    assert binding.retire_pre_admission_non_pool_launch_intent(
        _generic_controller_authority(), 3, _RECORD_ID).disposition == (
            binding.PreAdmissionRetirementDisposition.ABSENT)


def test_pre_admission_retirement_quarantines_incomplete_generic_profile(
        binding_database) -> None:
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc'))
        binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=3,
                replica_state_version=1,
                status='PROVISIONING',
                version=2,
                cluster_name='svc-3',
                is_spot=False,
                replica_state=_stored_replica_state({
                    'reserved_fill': True,
                })))

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='incomplete generic planner profile'):
        binding.retire_pre_admission_non_pool_launch_intent(
            _generic_controller_authority(), 3, _RECORD_ID)

    with binding_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 1


def test_serve047_generic_capability_transition_is_adjacent_and_reversible(
        binding_database) -> None:
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        promoted_epoch = binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()

    assert promoted_epoch == 6
    assert service['ordinary_launch_binding_mode'] == 'bound'
    assert service['ordinary_launch_binding_epoch'] == 6
    assert service['non_pool_launch_binding_capable'] is True
    assert service['non_pool_launch_controller_incarnation'] == _CONTROLLER_ID
    assert service['non_pool_launch_binding_protocol_version'] == 2
    assert service['non_pool_launch_capability_profile_set_digest'] == (
        binding.supported_non_pool_profile_set_digest())
    assert service['non_pool_launch_capability_cohort_epoch'] == 1
    assert service['non_pool_launch_receipt_protocol_version'] == 1

    with binding_database.begin() as connection:
        demoted_epoch = binding.demote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=6,
            request_barrier_clear=lambda _connection: True)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()

    assert demoted_epoch == 7
    assert service['ordinary_launch_binding_epoch'] == 7
    assert service['non_pool_launch_binding_capable'] is False
    assert service['non_pool_launch_controller_incarnation'] is None
    assert service['non_pool_launch_binding_protocol_version'] is None
    assert service['non_pool_launch_capability_profile_set_digest'] is None
    assert service['non_pool_launch_capability_cohort_epoch'] is None
    assert service['non_pool_launch_receipt_protocol_version'] is None


def test_serve047_rejects_capability_change_without_binding_epoch_cas(
        binding_database) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='capability change requires'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name == 'svc').values(
                        non_pool_launch_binding_capable=True,
                        non_pool_launch_controller_incarnation=_CONTROLLER_ID,
                        non_pool_launch_binding_protocol_version=2,
                        non_pool_launch_capability_profile_set_digest=(
                            binding.supported_non_pool_profile_set_digest()),
                        non_pool_launch_capability_cohort_epoch=1,
                        non_pool_launch_receipt_protocol_version=1))


def test_serve047_rejects_bound_epoch_advance_without_capability_change(
        binding_database) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='mode or non-pool capability transition'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name == 'svc').values(
                        ordinary_launch_binding_epoch=6))


def test_serve047_replica_planner_authorization_is_initial_insert_only(
        binding_database) -> None:
    authorization = {'authorization_version': 1, 'profile_kind': 'test'}
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=4,
                replica_state_version=1,
                status='READY',
                version=2,
                cluster_name='svc-4',
                is_spot=False,
                replica_state=_stored_replica_state(),
                non_pool_launch_authorization=authorization))

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='initial-insert-only'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id == 4).values(
                        non_pool_launch_authorization={
                            **authorization, 'profile_kind': 'changed'
                        }))

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='initial-insert-only'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id == 3).values(
                        non_pool_launch_authorization=authorization))


def test_serve047_rejects_incapable_controller_takeover(
        binding_database) -> None:
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)

    with pytest.raises(sqlalchemy.exc.DBAPIError):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name == 'svc').values(
                        controller_incarnation=uuid.uuid4(),
                        controller_owner_epoch=7))


def _insert_legacy_service(database, service_name: str) -> None:
    with database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=service_name, epoch=1))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=service_name,
                workspace='workspace-a',
                status='READY',
                hash=f'{service_name}-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                controller_pid=123,
                controller_ip='10.0.0.2',
                lifecycle_epoch=1,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='legacy',
                ordinary_launch_binding_epoch=0))


def test_atomic_legacy_teardown_blocks_later_promotion(
        binding_database) -> None:
    service_name = 'svc-teardown-first'
    _insert_legacy_service(binding_database, service_name)

    result = binding.begin_service_teardown_if_owner(service_name,
                                                     f'{service_name}-hash',
                                                     (123, '10.0.0.2'))

    assert result == binding.ServiceTeardownResult(
        binding.ServiceTeardownDisposition.MARKED_LEGACY, None)
    with binding_database.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.status, serve_state_schema.
                services_table.c.ordinary_launch_binding_mode,
                serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch).where(
                    serve_state_schema.services_table.c.name ==
                    service_name)).one()
    assert tuple(row) == ('SHUTTING_DOWN', 'legacy', 0)

    def _unexpected_barrier(_connection):
        raise AssertionError('terminal promotion must not run barriers')

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='terminal service status'):
        with binding_database.begin() as connection:
            binding.promote_service_in_connection(
                connection,
                service_name=service_name,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                expected_binding_epoch=0,
                participant_barrier_passed=_unexpected_barrier,
                legacy_requests_drained=_unexpected_barrier)


def test_promotion_before_atomic_teardown_returns_bound_authority(
        binding_database) -> None:
    service_name = 'svc-promotion-first'
    _insert_legacy_service(binding_database, service_name)
    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name=service_name,
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=0,
            participant_barrier_passed=lambda _: True,
            legacy_requests_drained=lambda _: True) == 1

    result = binding.begin_service_teardown_if_owner(service_name,
                                                     f'{service_name}-hash',
                                                     (123, '10.0.0.2'))

    assert result.disposition == (
        binding.ServiceTeardownDisposition.MARKED_BOUND)
    assert result.authority is not None
    assert result.authority.service_name == service_name
    assert result.authority.binding_mode == binding.BindingMode.BOUND
    assert result.authority.binding_epoch == 1
    with binding_database.connect() as connection:
        status = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table.c.status).where(
                serve_state_schema.services_table.c.name ==
                service_name)).scalar_one()
    assert status == 'SHUTTING_DOWN'


def test_controller_authority_refresh_allows_only_binding_advance_and_fences_legacy(
        binding_database) -> None:
    authority = _controller_authority()
    assert not serve_state.service_replica_launch_fence_holds(
        _unbound_context())
    with binding.refresh_controller_authority(authority) as current:
        assert current == authority

    with binding_database.begin() as connection:
        assert binding.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            request_barrier_clear=lambda _: True) == 6

    with binding.refresh_controller_authority(authority) as current:
        assert current.binding_mode == binding.BindingMode.LEGACY
        assert current.binding_epoch == 6
    assert serve_state.service_replica_launch_fence_holds(_unbound_context())

    wrong_workspace = dataclasses.replace(current,
                                          service_workspace='workspace-b')
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='outside a binding transition'):
        with binding.refresh_controller_authority(wrong_workspace):
            pass


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
def test_bound_service_allows_only_exact_persisted_excluded_profile(
        binding_database, excluded_state) -> None:
    replica_state = _stored_replica_state(excluded_state)
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING', replica_state=replica_state))

    context = _unbound_context()
    assert not serve_state.service_replica_launch_fence_holds(context)
    context.update({
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    })
    assert serve_state.service_replica_launch_fence_holds(context)
    context[serve_constants.
            ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY] = str(
                uuid.uuid4())
    assert not serve_state.service_replica_launch_fence_holds(context)


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
            'reason': 'MALFORMED_V13_BUNDLE'
        },
        'system_recovery_disposition': 'ORDINARY',
    },
])
def test_bound_service_rejects_persistent_marker_for_recovery_contract(
        binding_database, recovery_state) -> None:
    replica_state = _stored_replica_state(recovery_state)
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING', replica_state=replica_state))

    context = _unbound_context()
    context.update({
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    })
    assert not serve_state.service_replica_launch_fence_holds(context)


@pytest.mark.parametrize('special_state', [
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
        'system_recovery_disposition': 'CANDIDATE',
    },
    {
        'is_zero_cost': True,
        'system_recovery_launch_intent': _system_recovery_intent(),
        'system_recovery_disposition': 'CANDIDATE',
    },
])
def test_bound_admission_rejects_every_persisted_special_profile(
        binding_database, special_state) -> None:
    replica_state = _stored_replica_state(special_state)
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING', replica_state=replica_state))

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='Replica identity, version, state, or cluster'):
        with binding_database.begin() as connection:
            binding.insert_or_get_locked(connection, _identity())

    with binding_database.connect() as connection:
        association_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                binding.ordinary_launch_associations_table)).scalar_one()
        pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id ==
                    3)).scalar_one()
    assert association_count == 0
    assert pointer is None


def test_paid_replica_without_exact_claim_fails_before_admission(
        binding_database) -> None:
    pool_key = 'aws|us-east-1|paid'
    replica_state = _stored_replica_state({'paid_capacity_pool_key': pool_key})
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    paid_capacity_pool_key=pool_key,
                    replica_state=replica_state))

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='no exact capacity claim'):
        with binding_database.begin() as connection:
            binding.insert_or_get_locked(connection, _identity())

    with binding_database.connect() as connection:
        association_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                binding.ordinary_launch_associations_table)).scalar_one()
    assert association_count == 0


def test_bound_service_allows_only_exact_bound_system_recovery(
        binding_database) -> None:
    request_id = 'system-recovery-request'
    generation = 9
    replica_state = _stored_replica_state({
        'system_recovery_launch_intent': _system_recovery_intent(),
        'system_recovery_disposition': 'CANDIDATE',
        'system_recovery_quarantine': None,
        'launch_request_id': request_id,
    })
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING', replica_state=replica_state))

    context = _unbound_context()
    context.update({
        serve_constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY: 3,
        serve_constants.SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY: generation,
        serve_constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY: request_id,
    })
    assert serve_state.service_replica_launch_fence_holds(context)
    context[serve_constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY] = (
        'different-request')
    assert not serve_state.service_replica_launch_fence_holds(context)


def test_schema_042_catalog_is_complete_and_self_contained(
        binding_database) -> None:
    inspector = sqlalchemy.inspect(binding_database)
    assert [
        column['name'] for column in inspector.get_columns(
            binding.ordinary_launch_associations_table.name)
    ] == list(binding.ordinary_launch_associations_table.c.keys())
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    replica_columns = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    assert {
        'controller_incarnation', 'controller_owner_epoch',
        'ordinary_launch_binding_capable', 'ordinary_launch_binding_mode',
        'ordinary_launch_binding_epoch'
    } <= service_columns
    assert 'ordinary_launch_association_id' in replica_columns
    foreign_keys = {
        item['name']: item for item in inspector.get_foreign_keys('replicas')
    }
    assert foreign_keys['fk_replicas_ordinary_launch_association'][
        'referred_table'] == binding.ordinary_launch_associations_table.name
    with binding_database.connect() as connection:
        triggers = set(
            connection.execute(
                sqlalchemy.text("""
                    SELECT tgname FROM pg_catalog.pg_trigger
                    WHERE NOT tgisinternal AND tgname LIKE 'skyserve042_%'
                """)).scalars())
    assert {
        'skyserve042_service_binding_guard',
        'skyserve042_association_guard',
        'skyserve042_replica_binding_guard',
        'skyserve042_association_consistency',
        'skyserve042_replica_consistency',
        'skyserve042_service_consistency',
    } <= triggers


def test_serve042_is_the_only_owner_of_binding_columns(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '041')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '041'

    inspector = sqlalchemy.inspect(empty_postgres)
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    replica_columns = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    binding_service_columns = {
        'controller_incarnation', 'controller_owner_epoch',
        'ordinary_launch_binding_capable', 'ordinary_launch_binding_mode',
        'ordinary_launch_binding_epoch'
    }
    assert binding_service_columns.isdisjoint(service_columns)
    assert 'ordinary_launch_association_id' not in replica_columns

    alembic_command.upgrade(config, '042')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '042'
    inspector = sqlalchemy.inspect(empty_postgres)
    assert binding_service_columns <= {
        column['name'] for column in inspector.get_columns('services')
    }
    assert 'ordinary_launch_association_id' in {
        column['name'] for column in inspector.get_columns('replicas')
    }


def test_serve043_adds_nullable_jsonb_projections_and_retains_on_rollback(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '043')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '043'

    projection_names = {
        'controller_job_projection',
        'controller_work_cache',
        'worker_placement_projections',
    }
    projection_columns = {
        column['name']: column
        for column in sqlalchemy.inspect(empty_postgres).get_columns(
            'version_specs')
        if column['name'] in projection_names
    }
    assert set(projection_columns) == projection_names
    assert all(column['nullable'] for column in projection_columns.values())
    assert all(
        isinstance(column['type'], postgresql.JSONB)
        for column in projection_columns.values())
    historical_broker_column = next(column for column in sqlalchemy.inspect(
        empty_postgres).get_columns('version_specs')
                                    if column['name'] == 'storage_broker')
    assert historical_broker_column['nullable']
    assert isinstance(historical_broker_column['type'], postgresql.JSONB)

    alembic_command.downgrade(config, '042')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '042'
    retained_columns = {
        column['name'] for column in sqlalchemy.inspect(
            empty_postgres).get_columns('version_specs')
    }
    assert projection_names <= retained_columns
    assert 'storage_broker' in retained_columns


@pytest.mark.parametrize('early_column_ddl', [
    ('ALTER TABLE services ADD COLUMN controller_incarnation '
     'UUID NOT NULL DEFAULT gen_random_uuid()',),
    (
        'ALTER TABLE services ADD COLUMN controller_incarnation '
        'UUID NOT NULL DEFAULT gen_random_uuid()',
        'ALTER TABLE services ADD COLUMN controller_owner_epoch '
        'BIGINT NOT NULL DEFAULT 1',
        'ALTER TABLE services ADD COLUMN ordinary_launch_binding_capable '
        'BOOLEAN NOT NULL DEFAULT false',
        'ALTER TABLE services ADD COLUMN ordinary_launch_binding_mode '
        "TEXT NOT NULL DEFAULT 'legacy'",
        'ALTER TABLE services ADD COLUMN ordinary_launch_binding_epoch '
        'BIGINT NOT NULL DEFAULT 0',
        'ALTER TABLE replicas ADD COLUMN ordinary_launch_association_id UUID',
    ),
])
def test_serve038_rejects_partial_and_malformed_complete_serve042_catalog(
        empty_postgres, early_column_ddl) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '037')
    with empty_postgres.begin() as connection:
        for statement in early_column_ddl:
            connection.exec_driver_sql(statement)

    with pytest.raises(RuntimeError, match='incompatible column inventory'):
        alembic_command.upgrade(config, '038')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '037'
    assert not sqlalchemy.inspect(empty_postgres).has_table(
        'serve_resource_actions')


def test_serve047_downgrade_is_forward_only(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '047')
    with pytest.raises(RuntimeError, match='Serve047 is forward-only'):
        alembic_command.downgrade(config, '046')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '047'
    assert binding.ordinary_launch_associations_table.name in sqlalchemy.inspect(
        empty_postgres).get_table_names()


def test_admission_is_idempotent_and_conflicting_retry_fails_closed(
        binding_database) -> None:
    identity, first = _admit(binding_database)
    assert first.disposition == binding.AdmissionDisposition.CREATE
    assert first.launch_generation == 1
    with binding_database.begin() as connection:
        retry = binding.insert_or_get_locked(connection, identity)
    assert retry.disposition == binding.AdmissionDisposition.EXISTING_EXACT
    assert retry.request_id == first.request_id
    assert binding.binding_allows_request(str(identity.association_id),
                                          identity.request_id)

    conflict = dataclasses.replace(identity, input_digest='f' * 64)
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='different launch intent'):
        with binding_database.begin() as connection:
            binding.insert_or_get_locked(connection, conflict)
    with binding_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                binding.ordinary_launch_associations_table)).scalar_one() == 1


def test_successor_generation_requires_pre_effect_terminal_settlement(
        binding_database) -> None:
    identity, admission = _admit(binding_database)
    context = _bound_context(identity, admission.launch_generation)
    with binding_database.begin() as connection:
        _pre_effect_settle(connection, context)

    successor = _identity(submission_id=uuid.uuid4())
    with binding_database.begin() as connection:
        admitted = binding.insert_or_get_locked(connection, successor)
    assert admitted.launch_generation == 2


def test_cancelled_pre_effect_predecessor_cannot_admit_successor(
        binding_database) -> None:
    identity, admission = _admit(binding_database)
    context = _bound_context(identity, admission.launch_generation)
    with binding_database.begin() as connection:
        assert binding.request_cancel_in_connection(connection, context,
                                                    'replica-teardown') == 2
        _pre_effect_settle(connection, context)

    successor = _identity(submission_id=uuid.uuid4())
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='cancelled pre-effect predecessor'):
        with binding_database.begin() as connection:
            binding.insert_or_get_locked(connection, successor)

    with binding_database.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                binding.ordinary_launch_associations_table).order_by(
                    binding.ordinary_launch_associations_table.c.
                    launch_generation)).mappings().all()
    assert len(rows) == 1
    assert rows[0]['association_id'] == identity.association_id
    assert rows[0]['launch_generation'] == 1
    assert rows[0]['cancel_reason'] == 'replica-teardown'


@pytest.mark.parametrize('mutation', [
    {
        'cancel_reason': None,
        'cancel_requested_at': None,
    },
    {
        'cancel_reason': 'different-reason',
    },
    {
        'cancel_requested_at': None,
    },
    {
        'cancel_requested_at': datetime.datetime(
            2030, 1, 1, tzinfo=datetime.timezone.utc),
    },
])
def test_serve042_cancel_intent_is_immutable_after_commit(
        binding_database, mutation) -> None:
    identity, admission = _admit(binding_database)
    context = _bound_context(identity, admission.launch_generation)
    with binding_database.begin() as connection:
        committed_revision = binding.request_cancel_in_connection(
            connection, context, 'replica-teardown')

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='cancellation intent is immutable'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    binding.ordinary_launch_associations_table).where(
                        binding.ordinary_launch_associations_table.c.
                        association_id == identity.association_id).values(
                            **mutation,
                            owner_revision=committed_revision + 1,
                            updated_at=sqlalchemy.func.clock_timestamp()))

    with binding_database.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                binding.ordinary_launch_associations_table.c.cancel_reason,
                binding.ordinary_launch_associations_table.c.
                cancel_requested_at,
                binding.ordinary_launch_associations_table.c.owner_revision).
            where(binding.ordinary_launch_associations_table.c.association_id ==
                  identity.association_id)).one()
    assert row.cancel_reason == 'replica-teardown'
    assert row.cancel_requested_at is not None
    assert row.owner_revision == committed_revision


def test_quarantine_aware_election_fences_stale_admission_and_provider_effect(
        binding_database) -> None:
    # A newer quarantined generation must keep the last applied viable
    # generation elected for both admission and provider authorization.
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=3,
                yaml_content='service:\n  min_replicas: 1\n',
                quarantined_at=time.time(),
                quarantine_reason='invalid controller config'))

    identity, admission = _admit(binding_database)
    claim = _Claim(identity.request_id, 1, str(uuid.uuid4()), str(uuid.uuid4()))
    with binding.provider_effect_guard(
            _bound_launch_context(identity, admission.launch_generation),
            claim,
            claim_validator=lambda _connection, _association_id, _claim: True):
        pass

    # A later viable generation supersedes version 2.  The same stale replica
    # can neither admit a retry nor cross another provider-effect boundary.
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=4,
                yaml_content='service:\n  min_replicas: 2\n'))

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='version, state, or cluster changed'):
        with binding_database.begin() as connection:
            binding.insert_or_get_locked(connection, identity)
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='service version is no longer elected'):
        with binding.provider_effect_guard(_bound_launch_context(
                identity, admission.launch_generation),
                                           claim,
                                           claim_validator=lambda _connection,
                                           _association_id, _claim: True):
            pass

    with binding_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                binding.ordinary_launch_associations_table)).scalar_one() == 1


def test_database_blocks_identity_rewrite_effect_skip_and_old_replica_writer(
        binding_database) -> None:
    identity, admission = _admit(binding_database)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='identity is immutable'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    binding.ordinary_launch_associations_table).where(
                        binding.ordinary_launch_associations_table.c.
                        association_id == identity.association_id).values(
                            service_version=3, owner_revision=2))

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='effect phase transition is illegal'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    binding.ordinary_launch_associations_table).where(
                        binding.ordinary_launch_associations_table.c.
                        association_id == identity.association_id).
                values(
                    effect_phase='SERVICE_JOB_IO',
                    owner_revision=2,
                    effect_phase_changed_at=sqlalchemy.func.clock_timestamp()))

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='pointer clear lacks terminal evidence'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id == 3).values(
                        ordinary_launch_association_id=None))

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='bound ordinary-launch replica cannot be deleted'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id == 3))

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='cannot be deleted'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(
                    binding.ordinary_launch_associations_table).where(
                        binding.ordinary_launch_associations_table.c.
                        association_id == identity.association_id))
    assert admission.effect_phase == binding.EffectPhase.NOT_STARTED


def test_owner_transfer_updates_service_and_every_unresolved_association(
        binding_database) -> None:
    identity, admission = _admit(binding_database)
    new_incarnation = uuid.uuid4()
    with binding_database.begin() as connection:
        authority = binding.transfer_service_owner_in_connection(
            connection,
            service_name='svc',
            expected_incarnation=_CONTROLLER_ID,
            expected_owner_epoch=6,
            new_incarnation=new_incarnation,
            new_controller_pid=123,
            new_controller_ip='10.0.0.2',
            capable=True)
    assert authority.controller_incarnation == new_incarnation
    assert authority.controller_owner_epoch == 7
    with binding_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(binding.ordinary_launch_associations_table).where(
                binding.ordinary_launch_associations_table.c.association_id ==
                identity.association_id)).mappings().one()
    assert association['owner_controller_incarnation'] == new_incarnation
    assert association['owner_controller_epoch'] == 7
    assert association['owner_revision'] == admission.owner_revision + 1
    assert binding.binding_allows_request(str(identity.association_id),
                                          identity.request_id)


def test_claim_controller_incarnation_is_fresh_even_when_pid_ip_reused(
        binding_database) -> None:
    incarnation = uuid.uuid4()
    authority = binding.claim_controller_incarnation('svc', 'svc-hash',
                                                     (123, '10.0.0.2'),
                                                     incarnation)
    assert authority is not None
    assert authority.controller_incarnation == incarnation
    assert authority.controller_owner_epoch == 7
    assert authority.capable
    assert authority.binding_mode == binding.BindingMode.BOUND
    assert binding.binding_mode('svc') == binding.BindingMode.BOUND

    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='parent-owned'):
        binding.claim_controller_incarnation('svc', 'svc-hash',
                                             (999, '10.0.0.2'), uuid.uuid4())


def test_nonblocking_controller_claim_defers_behind_provider_guard(
        binding_database) -> None:
    started = time.monotonic()
    with serve_state.service_replica_launch_authority_guard('svc'):
        with pytest.raises(binding.OrdinaryLaunchBindingBusy,
                           match='active provider work'):
            binding.claim_controller_incarnation('svc',
                                                 'svc-hash', (123, '10.0.0.2'),
                                                 uuid.uuid4(),
                                                 wait_for_authority=False)
    assert time.monotonic() - started < 2


def test_changed_parent_claim_is_atomic_and_ready_port_keeps_incarnation(
        binding_database) -> None:
    identity, admission = _admit(binding_database)
    incarnation = uuid.uuid4()
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='no claimed incarnation'):
        binding.validate_controller_authority(None,
                                              service_name='svc',
                                              service_hash='svc-hash',
                                              controller_pid=456,
                                              controller_ip='10.0.0.9')
    authority = binding.claim_controller_incarnation(
        'svc',
        'svc-hash', (123, '10.0.0.2'),
        incarnation,
        new_parent_owner=(456, '10.0.0.9'),
        expected_lifecycle_epoch=4,
        expected_status=serve_statuses.ServiceStatus.READY,
        expected_recovery_version=2)
    assert authority is not None
    assert authority.controller_pid == 456
    assert authority.controller_ip == '10.0.0.9'
    assert authority.controller_incarnation == incarnation
    assert authority.controller_owner_epoch == 7

    assert binding.validate_controller_authority(
        authority,
        service_name='svc',
        service_hash='svc-hash',
        controller_pid=456,
        controller_ip='10.0.0.9') is authority
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='no longer current'):
        binding.validate_controller_authority(dataclasses.replace(
            authority, controller_owner_epoch=6),
                                              service_name='svc',
                                              service_hash='svc-hash',
                                              controller_pid=456,
                                              controller_ip='10.0.0.9')
    assert binding.publish_controller_port_if_authority(authority, 20017)
    with binding_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
        association = connection.execute(
            sqlalchemy.select(binding.ordinary_launch_associations_table).where(
                binding.ordinary_launch_associations_table.c.association_id ==
                identity.association_id)).mappings().one()
    assert service['controller_pid'] == 456
    assert service['controller_ip'] == '10.0.0.9'
    assert service['controller_port'] == 20017
    assert service['controller_incarnation'] == incarnation
    assert service['controller_owner_epoch'] == 7
    assert association['owner_controller_incarnation'] == incarnation
    assert association['owner_controller_epoch'] == 7
    assert association['owner_revision'] == admission.owner_revision + 1


def test_controller_claim_fences_status_lifecycle_and_recovery_version(
        binding_database) -> None:
    cases = ({
        'expected_lifecycle_epoch': 5,
    }, {
        'expected_status': serve_statuses.ServiceStatus.REPLICA_INIT,
    }, {
        'expected_recovery_version': 3,
    })
    for fences in cases:
        with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                           match='fence changed'):
            binding.claim_controller_incarnation('svc',
                                                 'svc-hash', (123, '10.0.0.2'),
                                                 uuid.uuid4(),
                                                 new_parent_owner=(456,
                                                                   '10.0.0.9'),
                                                 **fences)
    with binding_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.controller_pid,
                serve_state_schema.services_table.c.controller_ip,
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name == 'svc')).one()
    assert tuple(service) == (123, '10.0.0.2', _CONTROLLER_ID, 6)


def _assert_transition_rows_locked(database) -> bool:
    selectors = (
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                'svc'),
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == 'svc'),
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name == 'svc'),
        sqlalchemy.select(binding.ordinary_launch_associations_table).where(
            binding.ordinary_launch_associations_table.c.service_name == 'svc'),
    )
    for selector in selectors:
        contender = database.connect()
        transaction = contender.begin()
        try:
            with pytest.raises(sqlalchemy.exc.DBAPIError):
                contender.execute(selector.with_for_update(nowait=True)).all()
        finally:
            transaction.rollback()
            contender.close()
    return True


def test_transition_barriers_run_after_all_canonical_serve_locks(
        binding_database) -> None:
    with pytest.raises(binding.OrdinaryLaunchBindingUnavailable,
                       match='precomputed passing'):
        with binding_database.begin() as connection:
            binding.demote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=(_CONTROLLER_ID),
                controller_owner_epoch=6,
                expected_binding_epoch=5,
                request_barrier_clear=True)

    identity, admission = _admit(binding_database)
    context = _bound_context(identity, admission.launch_generation)
    with binding_database.begin() as connection:
        _pre_effect_settle(connection, context)
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))

    callbacks: list[str] = []

    def _barrier(_connection, name):
        callbacks.append(name)
        return _assert_transition_rows_locked(binding_database)

    with binding_database.begin() as connection:
        assert binding.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            request_barrier_clear=lambda connection: _barrier(
                connection, 'demote')) == 6

    def _unexpected_demote_barrier(_connection):
        raise AssertionError('an exact legacy retry must not rerun barriers')

    with binding_database.begin() as connection:
        assert binding.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            request_barrier_clear=_unexpected_demote_barrier) == 6
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='different controller authority'):
        with binding_database.begin() as connection:
            binding.demote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=uuid.uuid4(),
                controller_owner_epoch=6,
                expected_binding_epoch=5,
                request_barrier_clear=_unexpected_demote_barrier)
    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=6,
            participant_barrier_passed=lambda connection: _barrier(
                connection, 'participants'),
            legacy_requests_drained=lambda connection: _barrier(
                connection, 'legacy')) == 7
    assert callbacks == ['demote', 'participants', 'legacy']

    def _unexpected_barrier(_connection):
        raise AssertionError('an exact bound retry must not rerun barriers')

    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=6,
            participant_barrier_passed=_unexpected_barrier,
            legacy_requests_drained=_unexpected_barrier) == 7
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='different controller authority'):
        with binding_database.begin() as connection:
            binding.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=uuid.uuid4(),
                controller_owner_epoch=6,
                expected_binding_epoch=6,
                participant_barrier_passed=_unexpected_barrier,
                legacy_requests_drained=_unexpected_barrier)
    with binding_database.begin() as connection:
        assert binding.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=7,
            request_barrier_clear=lambda _: True) == 8
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='different binding epoch'):
        with binding_database.begin() as connection:
            binding.demote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                expected_binding_epoch=5,
                request_barrier_clear=_unexpected_barrier)

    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=8,
            participant_barrier_passed=lambda _: True,
            legacy_requests_drained=lambda _: True) == 9
    with pytest.raises(binding.OrdinaryLaunchBindingConflict,
                       match='authority or binding epoch'):
        with binding_database.begin() as connection:
            binding.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                expected_binding_epoch=6,
                participant_barrier_passed=_unexpected_barrier,
                legacy_requests_drained=_unexpected_barrier)


def test_first_promotion_accepts_migrated_epoch_zero_and_exact_retry(
        binding_database) -> None:
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc-zero', epoch=1))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc-zero',
                workspace='workspace-a',
                status='READY',
                hash='svc-zero-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                controller_pid=123,
                controller_ip='10.0.0.2',
                lifecycle_epoch=1,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='legacy',
                ordinary_launch_binding_epoch=0))

    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name='svc-zero',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=0,
            participant_barrier_passed=lambda _: True,
            legacy_requests_drained=lambda _: True) == 1

    def _unexpected_barrier(_connection):
        raise AssertionError('an exact epoch-zero retry must skip barriers')

    with binding_database.begin() as connection:
        assert binding.promote_service_in_connection(
            connection,
            service_name='svc-zero',
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=0,
            participant_barrier_passed=_unexpected_barrier,
            legacy_requests_drained=_unexpected_barrier) == 1


@dataclasses.dataclass(frozen=True)
class _Claim:
    request_id: str
    execution_generation: int
    claim_token: str
    worker_instance_id: str


def test_effect_guard_service_job_and_projection_are_monotonic(
        binding_database) -> None:
    identity, admission = _admit(binding_database)
    body = _body()
    binding.install_bound_context(body, identity, admission.launch_generation)
    context = binding.parse_bound_launch_context(body.extra_launch_context)
    claim = _Claim(identity.request_id, 1, str(uuid.uuid4()), str(uuid.uuid4()))
    validations: list[uuid.UUID] = []

    def _validate(_connection, association_id, observed_claim):
        assert observed_claim is claim
        validations.append(association_id)
        return True

    with binding.provider_effect_guard(
            body.extra_launch_context, claim,
            claim_validator=_validate) as authorization:
        assert authorization is not None
        assert (authorization.durable_replica_info.replica_record_id == str(
            identity.replica_record_id))
        binding.begin_service_job_io(body.extra_launch_context)
        binding.record_service_job(body.extra_launch_context, 17)
    assert validations == [identity.association_id] * 3

    evidence = binding.TerminalEvidence(status=binding.TerminalStatus.SUCCEEDED,
                                        cause='service job completed',
                                        execution_generation=1,
                                        quiescence_required=True,
                                        quiesced_generation=1,
                                        quiesced_at=datetime.datetime.now(
                                            datetime.timezone.utc))
    released: list[tuple[str, uuid.UUID]] = []
    with binding_database.begin() as connection:
        assert binding.record_terminal_in_connection(
            connection, context,
            evidence) == binding.StartupClassification.REDUCE_TERMINAL
        assert binding.project_from_request(
            connection,
            context,
            pre_effect_terminal=False,
            service_job_id=17,
            release_pin=lambda _connection, request_id, association_id:
            not released.append((request_id, association_id)))
    assert released == [(identity.request_id, identity.association_id)]
    with binding_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(binding.ordinary_launch_associations_table).where(
                binding.ordinary_launch_associations_table.c.association_id ==
                identity.association_id)).mappings().one()
        pointer = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table.c.
                              ordinary_launch_association_id)).scalar_one()
    assert association['resolution'] == binding.Resolution.PROJECTED.value
    assert association['service_job_id'] == 17
    assert association['pin_released_at'] is not None
    assert pointer is None

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='tombstone retention'):
        with binding_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(
                    binding.ordinary_launch_associations_table).where(
                        binding.ordinary_launch_associations_table.c.
                        association_id == identity.association_id))
