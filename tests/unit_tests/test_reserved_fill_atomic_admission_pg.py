"""PostgreSQL contracts for atomic reserved-fill request admission."""

# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import concurrent.futures
import contextlib
import copy
import dataclasses
import datetime
import functools
import os
import threading
import time
from unittest import mock
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_reserved_fill_allocation_pg import _commit_evidence
from test_reserved_fill_allocation_pg import _CONTEXT
from test_reserved_fill_allocation_pg import _CREATOR_ID
from test_reserved_fill_allocation_pg import _CREATOR_NAME
from test_reserved_fill_allocation_pg import _OWNER
from test_reserved_fill_allocation_pg import _POOL_KEY
from test_reserved_fill_allocation_pg import _publish_current_allocation
from test_reserved_fill_allocation_pg import _SERVICE
from test_reserved_fill_allocation_pg import _SERVICE_HASH
from test_reserved_fill_allocation_pg import _typed_fill_replica
from test_reserved_fill_allocation_pg import _UID
from test_reserved_fill_allocation_pg import _WORKSPACE
from test_reserved_fill_allocation_pg import allocation_engine  # noqa: F401
from test_reserved_fill_allocation_pg import observation_engine  # noqa: F401
from test_reserved_fill_allocation_pg import pg_server  # noqa: F401

from sky import global_user_state
from sky import global_user_state_schema
from sky.client import sdk
from sky.events import api_models as event_api_models
from sky.serve import capacity_admission
from sky.serve import constants as serve_constants
from sky.serve import controller_transport
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_lineage_schema
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import reserved_fill_reclaim_proof_schema
from sky.serve import reserved_fill_reclaim_proofs
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema
from sky.server import constants as server_constants
from sky.server.requests import executor
from sky.server.requests import non_pool_launch as non_pool_launch_request
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests as api_requests
from sky.server.requests import reserved_fill_admission
from sky.server.requests import storage as request_storage
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import locks
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='reserved_fill_atomic_admission_pg')

_CONTROLLER_PORT = 8123
_CONTROLLER_INCARNATION = uuid.UUID('11111111-1111-4111-8111-111111111111')
_CONTROLLER_OWNER_EPOCH = 2
_BINDING_EPOCH = 1
_REQUEST_RUNTIME = request_postgres.NonPoolLaunchBindingRuntime(
    handler_name=non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME,
    storage_backend_type=(
        request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
    queue_backend_type=request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE)


class _InjectedAdmissionFault(BaseException):
    pass


class _InjectedSuffixFault(RuntimeError):
    pass


@pytest.fixture
def atomic_database(allocation_engine, monkeypatch):
    global_user_state_schema.user_table.create(allocation_engine,
                                               checkfirst=True)
    request_postgres._initialize_schema(allocation_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine',
                        allocation_engine)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        allocation_engine)
    monkeypatch.setattr(
        request_postgres, '_resolved_request_backend_capability', lambda:
        (request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE,
         request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE, True))
    profile_digest = ordinary_launch_binding.supported_non_pool_profile_set_digest(
    )
    with allocation_engine.begin() as connection:
        # The allocation fixture creates the permanent Serve058 owner tuple.
        # Keep it immutable while specializing the controller/fill authority
        # used by this suite.
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).
            where(serve_state_schema.services_table.c.name == _SERVICE).values(
                workspace=_WORKSPACE,
                lifecycle_epoch=4,
                controller_port=_CONTROLLER_PORT,
                controller_incarnation=_CONTROLLER_INCARNATION,
                controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=_BINDING_EPOCH,
                non_pool_launch_binding_capable=True,
                non_pool_launch_controller_incarnation=(
                    _CONTROLLER_INCARNATION),
                non_pool_launch_binding_protocol_version=(
                    ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
                non_pool_launch_capability_profile_set_digest=(profile_digest),
                non_pool_launch_capability_cohort_epoch=(
                    ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
                non_pool_launch_receipt_protocol_version=(
                    ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION),
                demand_source_mode=(
                    capacity_admission.DemandSourceMode.DURABLE_FEED.value),
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=(
                    _CONTROLLER_INCARNATION),
                demand_authority_protocol_version=(
                    capacity_admission.PROTOCOL_VERSION),
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=(
                    _CONTROLLER_INCARNATION),
                reserved_fill_actuation_protocol_version=1))
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    _SERVICE).values(epoch=4))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name ==
                _SERVICE,
                serve_state_schema.version_specs_table.c.version == 1).values(
                    created_by=_CREATOR_NAME))
    _publish_fresh_provider_proof(allocation_engine)
    return allocation_engine


def _publish_fresh_provider_proof(engine) -> None:
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='test-policy-v1',
        provider_inventory_sha256='b' * 64)
    repository = reserved_fill_reclaim_proofs.ReclaimProviderProofRepository(
        engine)
    try:
        repository.renew(
            identity=identity,
            gate_generation=1,
            kubernetes_context=_CONTEXT,
            deadline_monotonic=(time.monotonic() +
                                reserved_fill_reclaim_attestation.
                                PROVIDER_PROOF_REFRESH_TIMEOUT_SECONDS),
            prove=lambda: reserved_fill_reclaim_proofs.
            ReclaimProviderProofCandidate(proof_payload={
                'aws': {},
                'kubernetes': {
                    'physical_cluster_uid': _UID,
                },
            },
                                          oldest_completed_monotonic=time.
                                          monotonic()),
            validate=lambda _payload: True,
            minimum_remaining_seconds=(
                reserved_fill_reclaim_attestation.
                PROVIDER_PROOF_RENEW_MIN_REMAINING_SECONDS))
    finally:
        repository._proof_engine.dispose()


def _age_provider_proof(engine, seconds: float) -> None:
    table = (reserved_fill_reclaim_proof_schema.
             serve_reserved_fill_reclaim_provider_proofs_table)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(table).values(
                completed_at=(sqlalchemy.func.clock_timestamp() -
                              datetime.timedelta(seconds=seconds))))


def _authority():
    return ordinary_launch_binding.ControllerBindingAuthority(
        service_name=_SERVICE,
        service_hash=_SERVICE_HASH,
        service_workspace=_WORKSPACE,
        service_lifecycle_epoch=4,
        controller_pid=_OWNER[0],
        controller_ip=_OWNER[1],
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        binding_epoch=_BINDING_EPOCH,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))


def _atomic_specs(engine, count=1, *, image_id=None, authority=None):
    if authority is None:
        authority = _authority()
    serve_state.attest_service_owner_user_id(authority, _CREATOR_ID,
                                             _CREATOR_NAME)
    _publish_fresh_provider_proof(engine)
    _, snapshot = _commit_evidence(engine)
    if image_id is not None:
        snapshot = dataclasses.replace(
            snapshot,
            locations=tuple(
                dataclasses.replace(location, image_id=(('docker', image_id),))
                for location in snapshot.locations))
    allocation = _publish_current_allocation(engine, snapshot)
    owner_fingerprint = controller_transport.make_controller_owner_fingerprint(
        _SERVICE_HASH, _OWNER[0], _OWNER[1], _CONTROLLER_PORT)
    plan = reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=2,
        reconcile_generation=3,
        allocation_map=allocation,
        service_incarnation=_SERVICE_HASH,
        service_version=1,
        controller_owner=owner_fingerprint,
        max_replicas=count,
        planned_replicas=0,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    receipt = repository.grant_plan(
        _SERVICE,
        plan,
        max_capacity=count,
        expected_controller_incarnation=_CONTROLLER_INCARNATION,
        expected_controller_owner_epoch=_CONTROLLER_OWNER_EPOCH)
    assert len(receipt.accepted) == count
    specs = []
    for replica_id in range(1, count + 1):
        lease = repository.lease_next(service_name=_SERVICE,
                                      pool_key=plan.intents[0].pool_key,
                                      owner=uuid.uuid4(),
                                      lease_seconds=30)
        assert lease is not None
        intent = lease.intent
        info = _typed_fill_replica(_SERVICE,
                                   replica_id,
                                   snapshot,
                                   allocation,
                                   card=intent.accelerator,
                                   intent_key=intent.idempotency_key)
        selected_location = intent.allowed_locations[0].to_location()
        info.location = selected_location.to_pickleable()
        info.resources_override = selected_location.to_dict()
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        launch_context = {
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: _SERVICE,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _SERVICE_HASH,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 1,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: _OWNER[0],
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: _OWNER[1],
            ordinary_launch_binding.REPLICA_ID_KEY: replica_id,
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY:
                info.replica_record_id,
            ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: 4,
            ordinary_launch_binding.BINDING_EPOCH_KEY: authority.binding_epoch,
            ordinary_launch_binding.CONTROLLER_INCARNATION_KEY:
                str(_CONTROLLER_INCARNATION),
            ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY: _CONTROLLER_OWNER_EPOCH,
        }
        body = payloads.LaunchBody(
            task=('name: atomic-fill\nresources:\n  accelerators: '
                  'A100-80GB:1\n'),
            cluster_name=f'{_SERVICE}-{replica_id}',
            is_launched_by_sky_serve_controller=True,
            client_api_version=server_constants.API_VERSION,
            extra_launch_context=launch_context,
            env_vars={
                skylet_constants.USER_ID_ENV_VAR: 'controller-tenant',
                skylet_constants.USER_ENV_VAR: 'controller-system',
            },
            override_skypilot_config={'active_workspace': _WORKSPACE})
        prepared = sdk.PreparedLaunchRequest(
            sdk._canonical_launch_body_bytes(body))
        specs.append(
            reserved_fill_admission.AdmissionSpec(
                prepared_request=prepared,
                submission_id=uuid.uuid5(
                    uuid.UUID('22222222-2222-4222-8222-222222222222'),
                    str(replica_id)),
                authority=authority,
                replica_info=info,
                actuation_lease=lease,
                launch_limit=max(1, count)))
    return tuple(specs)


def _atomic_spec(engine, **kwargs):
    return _atomic_specs(engine, **kwargs)[0]


def _active_reserved_fill_effect_claim(engine):
    """Commit one fill graph and install its exact live executor claim."""
    spec = _atomic_spec(engine)
    _, receipt = reserved_fill_admission._transaction(spec,
                                                      7,
                                                      require_existing=False)
    claim_token = uuid.uuid4()
    worker_instance_id = uuid.uuid4()
    with engine.begin() as connection:
        updated_request = connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id).values(
                    status=api_requests.RequestStatus.RUNNING.value,
                    execution_generation=1,
                    claim_token=claim_token,
                    worker_instance_id=worker_instance_id,
                    lease_expires_at=sqlalchemy.text(
                        "clock_timestamp() + INTERVAL '1 hour'"),
                    execution_quiescence_required=True,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        assert updated_request.rowcount == 1
        updated_delivery = connection.execute(
            sqlalchemy.update(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                receipt.request_id).values(
                    delivery_state='claimed',
                    claim_generation=1,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        assert updated_delivery.rowcount == 1
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
    request = request_postgres.request_from_mapping(row)
    launch_context = request.request_body.extra_launch_context
    context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        launch_context)
    claim = request_storage.ExecutionClaim(receipt.request_id, 1,
                                           str(claim_token),
                                           str(worker_instance_id))
    return context, launch_context, claim


def _suffix_counts(connection):
    tables = (
        serve_state_schema.replicas_table,
        ordinary_launch_binding.ordinary_launch_associations_table,
        request_postgres.REQUESTS,
        request_postgres.QUEUE,
        request_postgres.REQUEST_RETENTION_PINS,
    )
    return tuple(
        connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                table)).scalar_one() for table in tables)


def _commit_intent_and_build_replica_row(connection, spec):
    info = spec.replica_info
    zero_cost_actuation.commit_lease_in_connection(connection,
                                                   spec.actuation_lease,
                                                   service_name=_SERVICE,
                                                   replica_id=info.replica_id,
                                                   replica_record_id=uuid.UUID(
                                                       info.replica_record_id),
                                                   replica_info=info)
    values = serve_state._reserved_fill_replica_row_values(
        _SERVICE,
        info.replica_id,
        info,
        pool_key=info.reserved_fill_pool_key,
        expected_protocol_version=2)
    assert values is not None
    return values


def _committed_launch_context(spec):
    intent = spec.actuation_lease.intent
    context = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: _SERVICE,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _SERVICE_HASH,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY:
            intent.service_version,
    }
    context.update(
        reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=intent.pool_key,
            service_generation=intent.service_generation,
            service_version=intent.service_version,
            physical_cluster_uid=intent.physical_cluster_uid,
            kubernetes_context=intent.allowed_locations[0].region,
            accelerator=intent.accelerator,
            accelerator_count=intent.accelerator_count,
            reconciliation_gate_generation=(
                intent.reconciliation_gate_generation),
            reclaim_fleet_bundle_sha256=intent.reclaim_fleet_bundle_sha256,
            reclaim_policy_revision=intent.reclaim_policy_revision,
            reclaim_provider_inventory_sha256=(
                intent.reclaim_provider_inventory_sha256),
            worker_projection_sha256=intent.worker_projection_sha256))
    return context


def _committed_launch_authorization(engine, spec):
    intent = spec.actuation_lease.intent
    launch_context = _committed_launch_context(spec)
    fence = reserved_capacity.parse_protocol_v2_launch_fence(launch_context)
    assert fence is not None
    with engine.connect() as connection:
        projections = connection.execute(
            sqlalchemy.select(
                serve_state_schema.version_specs_table.c.
                worker_placement_projections).where(
                    serve_state_schema.version_specs_table.c.service_name ==
                    _SERVICE, serve_state_schema.version_specs_table.c.version
                    == intent.service_version)).scalar_one()
    _, projected_admission = reserved_capacity.require_reclaim_worker_projection(
        fence, projections)
    scope = reserved_fill_reclaim_attestation.ReclaimLaunchScope(
        service_name=_SERVICE,
        service_version=intent.service_version,
        pool_key=intent.pool_key,
        service_generation=intent.service_generation,
        physical_cluster_uid=intent.physical_cluster_uid,
        kubernetes_context=intent.allowed_locations[0].region,
        accelerator=intent.accelerator,
        accelerator_count=intent.accelerator_count,
        projected_admission=projected_admission)
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256=intent.reclaim_fleet_bundle_sha256,
        policy_revision=intent.reclaim_policy_revision,
        provider_inventory_sha256=intent.reclaim_provider_inventory_sha256)
    completed = time.monotonic()
    reference = reserved_fill_reclaim_attestation.ReclaimProviderProofReference(
        receipt_nonce='a' * 64,
        proof_sha256='b' * 64,
        identity=identity,
        gate_generation=intent.reconciliation_gate_generation,
        kubernetes_context=scope.kubernetes_context,
        completed_monotonic=completed)
    authorization = reserved_fill_reclaim_attestation.ReclaimLaunchAuthorization(
        identity=identity,
        gate_generation=intent.reconciliation_gate_generation,
        scope=scope,
        provider_proof_reference=reference,
        completed_monotonic=completed)
    return scope, authorization


def _owner_tuple(engine):
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.owner_user_id,
                serve_state_schema.services_table.c.owner_user_name).
            where(serve_state_schema.services_table.c.name == _SERVICE)).one()


def _failed_teardown_reserved_fill_ambiguity(engine):
    """Create one exact terminal+quiesced ambiguous fill association."""
    spec = _atomic_spec(engine)
    staged, receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=False)
    with engine.connect() as connection:
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
    request = request_postgres.request_from_mapping(request_row)
    context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        request.request_body.extra_launch_context)
    with engine.begin() as connection:
        associations = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == context.association_id).values(
                    effect_phase=ordinary_launch_binding.EffectPhase.
                    PROVIDER_IO.value,
                    effect_phase_changed_at=sqlalchemy.func.clock_timestamp(),
                    owner_revision=associations.c.owner_revision + 1,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == context.request_id))
        now = sqlalchemy.func.clock_timestamp()
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                context.request_id).values(
                    status=api_requests.RequestStatus.CANCELLED.value,
                    terminal_cause=event_api_models.EventCause.
                    EXECUTION_LEASE_EXPIRED.value,
                    execution_generation=1,
                    execution_quiescence_required=True,
                    execution_quiesced_generation=1,
                    execution_quiesced_at=now,
                    finished_at=now,
                    updated_at=now))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    status=serve_state.ServiceStatus.CONTROLLER_FAILED.value))
    teardown = ordinary_launch_binding.begin_service_teardown_if_owner(
        _SERVICE, _SERVICE_HASH, _OWNER)
    assert teardown.disposition is (
        ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND)
    assert teardown.authority is not None
    info = serve_state.get_replica_info_from_id(
        _SERVICE, staged.persisted_info.replica_id)
    assert info is not None
    return context, info, teardown.authority


def _install_failed_teardown_provider_observation(monkeypatch, engine, info,
                                                  evidence):
    """Install one real-lock probe and closed provider classification."""
    provider_reads = []

    def _get_physical_uid(kubernetes_context, *, force_refresh):
        assert kubernetes_context == info.reserved_fill_kubernetes_context
        assert force_refresh
        # This NOWAIT lock is the regression witness that provider observation
        # runs after the request/state transactions release their row locks.
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.select(
                    serve_state_schema.services_table.c.name).where(
                        serve_state_schema.services_table.c.name ==
                        _SERVICE).with_for_update(nowait=True)).scalar_one()
        provider_reads.append('physical-cluster-uid')
        if evidence == ordinary_launch_binding.ProviderEvidence.REPLACED:
            return 'replacement-physical-cluster-uid'
        return info.reserved_fill_physical_cluster_uid

    def _probe_presence(_fence, cluster_name, *, observed_after):
        assert cluster_name == info.cluster_name
        assert isinstance(observed_after, float)
        provider_reads.append('replica-presence')
        return {
            ordinary_launch_binding.ProviderEvidence.PRESENT:
                reserved_capacity.PhysicalReplicaPresence.PRESENT,
            ordinary_launch_binding.ProviderEvidence.ABSENT:
                reserved_capacity.PhysicalReplicaPresence.ABSENT,
            ordinary_launch_binding.ProviderEvidence.UNKNOWN:
                reserved_capacity.PhysicalReplicaPresence.UNPROVEN,
        }[evidence]

    monkeypatch.setattr(reserved_capacity,
                        'get_kubernetes_physical_cluster_uid',
                        _get_physical_uid)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        _probe_presence)
    return provider_reads


def _post_teardown_absence_receipt(info):
    """Build the exact receipt a fenced protocol-v2 down returns (#1685)."""
    fence = reserved_capacity.parse_protocol_v2_cleanup_fence(info)
    assert fence is not None
    return reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
        cleanup_fence=fence, cluster_name=info.cluster_name)


def test_failed_teardown_present_ambiguity_authorizes_cleanup_marker(
        atomic_database, monkeypatch) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    provider_reads = _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info,
        ordinary_launch_binding.ProviderEvidence.PRESENT)
    monkeypatch.setattr(
        service.request_postgres, 'request_bound_ordinary_launch_cancel',
        lambda *_args, **_kwargs: pytest.fail(
            'an ambiguous reserved-fill request must not be cancelled'))

    settlement = (service._settle_bound_ordinary_launches_for_teardown(
        authority, [info]))
    cleanup_contexts = settlement.provider_present_cleanup_contexts

    assert provider_reads == ['physical-cluster-uid', 'replica-presence']
    assert not settlement.provider_reconciliation_failures
    assert cleanup_contexts == {
        (info.replica_id, info.replica_record_id): context
    }
    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                info.replica_id)).mappings().one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    context.request_id)).scalar_one()
    persisted = replica_managers.ReplicaInfo.from_storage_dict(
        replica['replica_state'])
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.AMBIGUOUS.value)
    assert association['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.PRESENT.value)
    assert association['cancel_reason'] is None
    assert replica['ordinary_launch_association_id'] == context.association_id
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        persisted, require_scheduled=True)
    assert pin_count == 1

    # The cleanup owner must use the existing exact PRESENT path, not generic
    # cancellation or a name-only down. Since #1685 the fenced down itself
    # yields the exact post-teardown ABSENT receipt, and the projection reuses
    # that receipt without a second provider read before releasing the
    # association and retention pin.
    cleanup_reads = _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, persisted,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    terminated = []

    def _terminate(*args, **kwargs):
        terminated.append((args, kwargs))
        return _post_teardown_absence_receipt(persisted)

    monkeypatch.setattr(replica_managers, 'terminate_cluster', _terminate)
    replica_managers.terminate_bound_non_pool_provider_present_cluster(
        context, persisted, authority,
        functools.partial(service._project_bound_ordinary_launch_for_teardown,
                          authority), persisted.cluster_name)

    assert len(terminated) == 1
    assert terminated[0][0] == (persisted.cluster_name, 0)
    assert cleanup_reads == []
    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
        replica_pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    info.replica_id)).scalar_one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    context.request_id)).scalar_one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.PROJECTED.value)
    assert association['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.ABSENT.value)
    assert replica_pointer is None
    assert pin_count == 0


def test_failed_teardown_present_marker_without_cluster_record_reconciles(
        atomic_database, monkeypatch) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info,
        ordinary_launch_binding.ProviderEvidence.PRESENT)
    settlement = (service._settle_bound_ordinary_launches_for_teardown(
        authority, [info]))
    cleanup_contexts = settlement.provider_present_cleanup_contexts
    assert not settlement.provider_reconciliation_failures
    persisted = serve_state.get_replica_info_from_id(_SERVICE, info.replica_id)
    assert persisted is not None
    cleanup_reads = _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, persisted,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    monkeypatch.setattr(
        replica_managers, 'terminate_cluster', lambda *_args, **_kwargs: pytest.
        fail('a missing cluster record must not use generic teardown'))

    preparation = service._prepare_provider_present_cleanup(
        _SERVICE, authority, [persisted], set(), cleanup_contexts)

    assert not preparation.contexts
    assert preparation.projected_absence_keys == frozenset({
        (persisted.replica_id, persisted.replica_record_id)
    })
    assert not preparation.failures
    assert cleanup_reads == ['physical-cluster-uid', 'replica-presence']
    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
        replica_pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    info.replica_id)).scalar_one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    context.request_id)).scalar_one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.PROJECTED.value)
    assert association['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.ABSENT.value)
    assert replica_pointer is None
    assert pin_count == 0


def test_failed_teardown_present_marker_rejects_stale_authority(
        atomic_database, monkeypatch) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info,
        ordinary_launch_binding.ProviderEvidence.PRESENT)
    settlement = (service._settle_bound_ordinary_launches_for_teardown(
        authority, [info]))
    cleanup_contexts = settlement.provider_present_cleanup_contexts
    assert not settlement.provider_reconciliation_failures
    persisted = serve_state.get_replica_info_from_id(_SERVICE, info.replica_id)
    assert persisted is not None
    stale_authority = dataclasses.replace(
        authority, controller_owner_epoch=authority.controller_owner_epoch - 1)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='lost its exact bound association authority'):
        service._provider_present_cleanup_context(persisted, stale_authority,
                                                  cleanup_contexts)

    assert request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
        context, authority)


def test_failed_teardown_absent_ambiguity_projects_exact_result(
        atomic_database, monkeypatch) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    provider_reads = _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    monkeypatch.setattr(
        service.request_postgres, 'request_bound_ordinary_launch_cancel',
        lambda *_args, **_kwargs: pytest.fail(
            'an ambiguous reserved-fill request must not be cancelled'))

    service._settle_bound_ordinary_launches_for_teardown(authority, [info])

    assert provider_reads == ['physical-cluster-uid', 'replica-presence']
    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                info.replica_id)).mappings().one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    context.request_id)).scalar_one()
    persisted = replica_managers.ReplicaInfo.from_storage_dict(
        replica['replica_state'])
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.PROJECTED.value)
    assert association['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.ABSENT.value)
    assert association['cancel_reason'] is None
    assert replica['ordinary_launch_association_id'] is None
    # Exact post-quiescence reserved ABSENT evidence normalizes the replica to
    # the immediate-cleanup INTERRUPTED marker (#1748), not FAILED.
    assert persisted.status_property.sky_launch_status == (
        common_utils.ProcessStatus.INTERRUPTED)
    assert pin_count == 0


@pytest.mark.parametrize(
    'evidence',
    (ordinary_launch_binding.ProviderEvidence.UNKNOWN,
     ordinary_launch_binding.ProviderEvidence.REPLACED),
)
def test_failed_teardown_uncertain_provider_ambiguity_stays_fail_closed(
        atomic_database, monkeypatch, evidence) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    provider_reads = _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info, evidence)
    monkeypatch.setattr(
        service.request_postgres, 'request_bound_ordinary_launch_cancel',
        lambda *_args, **_kwargs: pytest.fail(
            'an ambiguous reserved-fill request must not be cancelled'))

    settlement = service._settle_bound_ordinary_launches_for_teardown(
        authority, [info])
    assert not settlement.provider_present_cleanup_contexts
    assert settlement.provider_reconciliation_failures.keys() == {
        (info.replica_id, info.replica_record_id)
    }
    assert (f'returned {evidence.value}'
            in next(iter(settlement.provider_reconciliation_failures.values())))

    expected_reads = ['physical-cluster-uid']
    if evidence == ordinary_launch_binding.ProviderEvidence.UNKNOWN:
        expected_reads.append('replica-presence')
    assert provider_reads == expected_reads
    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                info.replica_id)).mappings().one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    context.request_id)).scalar_one()
    persisted = replica_managers.ReplicaInfo.from_storage_dict(
        replica['replica_state'])
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.AMBIGUOUS.value)
    assert association['provider_evidence'] == evidence.value
    assert association['cancel_reason'] is None
    assert replica['ordinary_launch_association_id'] == context.association_id
    assert not ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        persisted)
    assert pin_count == 1


def _use_real_broker(monkeypatch, engine):
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable', lambda: True)
    monkeypatch.setattr(request_postgres,
                        'prepare_non_pool_launch_binding_runtime',
                        lambda: _REQUEST_RUNTIME)

    def get_postgres_lock(lock_id,
                          timeout=None,
                          lock_type=None,
                          poll_interval=None,
                          shared_lock=False):
        assert lock_type in (None, 'postgres')
        if poll_interval is None:
            poll_interval = 1
        return locks.PostgresLock(lock_id,
                                  timeout=timeout,
                                  poll_interval=poll_interval,
                                  shared_lock=shared_lock,
                                  engine=engine)

    monkeypatch.setattr(reserved_capacity_broker.locks, 'get_lock',
                        get_postgres_lock)


def test_serve059_lineage_and_sqlite_ceiling() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ['067']
    assert scripts.get_revision('060').down_revision == '059'
    assert scripts.get_revision('059').down_revision == '058'
    assert scripts.get_revision('058').down_revision == '057'
    assert scripts.get_revision('056').down_revision == '055'
    assert scripts.get_revision('055').down_revision == '054'
    assert migration_utils.SERVE_VERSION == '067'
    assert migration_utils.serve_target_version(sqlite) == '037'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '056')


def test_retained_serve054_row_migrates_null_and_055_is_forward_only(
        observation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        observation_engine)
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '054')
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO services
                    (name, hash, resource_scope, controller_pid,
                     controller_ip, status, current_version,
                     logical_replica_semantics)
                VALUES ('retained-svc', 'retained-hash', 'retained-hash',
                        41, '10.0.0.41', 'READY', 1, 0)
            """))
    assert {'owner_user_id', 'owner_user_name'}.isdisjoint({
        column['name'] for column in sqlalchemy.inspect(
            observation_engine).get_columns('services')
    })
    global_user_state_schema.user_table.drop(observation_engine,
                                             checkfirst=True)
    with pytest.raises(
            RuntimeError,
            match='global user-state users\\(id\\).*before the Serve'):
        alembic_command.upgrade(config, '055')
    assert migration_utils.get_current_alembic_revision(
        observation_engine, migration_utils.SERVE_DB_NAME) == '054'
    failed_inspector = sqlalchemy.inspect(observation_engine)
    assert {'owner_user_id', 'owner_user_name'}.isdisjoint(
        {column['name'] for column in failed_inspector.get_columns('services')})
    with observation_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text(
                "SELECT count(*) FROM pg_proc WHERE proname = "
                "'skyserve055_guard_service_owner'")).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.text(
                "SELECT count(*) FROM pg_trigger WHERE tgname = "
                "'skyserve055_service_owner_guard'")).scalar_one() == 0

    global_user_state_schema.user_table.create(observation_engine,
                                               checkfirst=True)
    alembic_command.upgrade(config, '055')
    with observation_engine.connect() as connection:
        owner = connection.execute(
            sqlalchemy.text(
                "SELECT owner_user_id, owner_user_name FROM services "
                "WHERE name = 'retained-svc'")).one()
    assert owner == (None, None)
    inspector = sqlalchemy.inspect(observation_engine)
    assert {'owner_user_id', 'owner_user_name'} <= {
        column['name'] for column in inspector.get_columns('services')
    }
    assert 'serve055_owner_user_id_nonempty' in {
        constraint['name']
        for constraint in inspector.get_check_constraints('services')
    }
    owner_fks = {
        constraint['name']: constraint
        for constraint in inspector.get_foreign_keys('services')
    }
    assert owner_fks['serve055_service_owner_user_fk']['referred_table'] == (
        'users')
    assert owner_fks['serve055_service_owner_user_fk'][
        'constrained_columns'] == (['owner_user_id'])
    assert owner_fks['serve055_service_owner_user_fk']['referred_columns'] == ([
        'id'
    ])

    with pytest.raises(RuntimeError, match='forward-only'):
        alembic_command.downgrade(config, '054')
    assert migration_utils.get_current_alembic_revision(
        observation_engine, migration_utils.SERVE_DB_NAME) == '055'
    assert {'owner_user_id', 'owner_user_name'} <= {
        column['name'] for column in sqlalchemy.inspect(
            observation_engine).get_columns('services')
    }


def test_serve056_retains_json_only_rows_but_rejects_new_old_writer_rows(
        observation_engine) -> None:
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    global_user_state_schema.user_table.create(observation_engine,
                                               checkfirst=True)
    alembic_command.upgrade(config, '055')
    legacy_state = {
        'replica_id': 1,
        'cluster_name': 'retained-fill-1',
        'version': 1,
        'replica_record_id': str(uuid.uuid4()),
        'reserved_fill': True,
        'reserved_fill_service_generation': 3,
        'reserved_fill_intent_idempotency_key': 'a' * 64,
    }
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='retained-fill',
                workspace=_WORKSPACE,
                status='READY',
                hash='retained-fill-hash',
                resource_scope='retained-fill-hash',
                current_version=1,
                active_versions='[1]',
                pool=0))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='retained-fill',
                replica_id=1,
                replica_state_version=1,
                status='PROVISIONING',
                version=1,
                cluster_name='retained-fill-1',
                is_spot=False,
                replica_state=legacy_state))

    alembic_command.upgrade(config, '056')
    with observation_engine.begin() as connection:
        retained = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'retained-fill')).mappings().one()
        assert retained['reserved_fill_intent_idempotency_key'] is None
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'retained-fill').values(status='FAILED'))

    new_state = dict(legacy_state,
                     replica_id=2,
                     cluster_name='retained-fill-2',
                     replica_record_id=str(uuid.uuid4()))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='requires a committed intent link'):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state_schema.replicas_table).values(
                    service_name='retained-fill',
                    replica_id=2,
                    replica_state_version=1,
                    status='PROVISIONING',
                    version=1,
                    cluster_name='retained-fill-2',
                    is_spot=False,
                    replica_state=new_state))


def test_serve059_exposes_only_owner_attestation_symbol(
        atomic_database) -> None:
    del atomic_database
    assert hasattr(serve_state, 'attest_service_owner_user_id')
    assert not hasattr(serve_state, 'verify_service_owner_user_id')
    # The Serve055 transition predicate is not an owner-transition leftover:
    # sky/users/server.py consumes it as the fail-closed user-deletion gate.
    assert hasattr(serve_state, 'service_owner_attestation_transition_active')


def test_service_owner_attestation_is_idempotent_and_restart_safe(
        atomic_database) -> None:
    authority = _authority()
    before = _owner_tuple(atomic_database)
    assert before == (_CREATOR_ID, _CREATOR_NAME)
    serve_state.attest_service_owner_user_id(authority, _CREATOR_ID,
                                             _CREATOR_NAME)
    serve_state.attest_service_owner_user_id(authority, _CREATOR_ID,
                                             _CREATOR_NAME)
    assert _owner_tuple(atomic_database) == before
    assert serve_state.get_service_names_owned_by_user_id(_CREATOR_ID) == [
        _SERVICE
    ]

    with atomic_database.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE).values(owner_user_id='replacement',
                                     owner_user_name='replacement@example.com'))
        transaction.rollback()
    assert _owner_tuple(atomic_database) == (_CREATOR_ID, _CREATOR_NAME)


def test_service_owner_fk_blocks_delete_until_service_teardown(
        atomic_database) -> None:
    owner_id = 'owner-for-delete'
    service_name = 'owner-delete-service'
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id=owner_id,
                name='owner-for-delete@example.com',
                created_at=int(time.time())))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=service_name,
                owner_user_id=owner_id,
                owner_user_name='owner-for-delete@example.com'))

    with atomic_database.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            connection.execute(
                sqlalchemy.delete(global_user_state_schema.user_table).where(
                    global_user_state_schema.user_table.c.id == owner_id))
        transaction.rollback()

    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name))
        deleted = connection.execute(
            sqlalchemy.delete(global_user_state_schema.user_table).where(
                global_user_state_schema.user_table.c.id == owner_id))
        assert deleted.rowcount == 1


def test_service_owner_fk_serializes_concurrent_delete_and_service_create(
        atomic_database) -> None:
    owner_id = 'owner-create-race'
    service_name = 'owner-create-race-service'
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id=owner_id,
                name='owner-create-race@example.com',
                created_at=int(time.time())))

    creator_connection = atomic_database.connect()
    creator_transaction = creator_connection.begin()
    creator_connection.execute(
        sqlalchemy.insert(serve_state_schema.services_table).values(
            name=service_name,
            owner_user_id=owner_id,
            owner_user_name='owner-create-race@example.com'))

    def _delete_owner() -> None:
        with atomic_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(global_user_state_schema.user_table).where(
                    global_user_state_schema.user_table.c.id == owner_id))

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            deletion = executor.submit(_delete_owner)
            time.sleep(0.1)
            assert not deletion.done()
            creator_transaction.commit()
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                deletion.result(timeout=5)
    finally:
        if creator_transaction.is_active:
            creator_transaction.rollback()
        creator_connection.close()

    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name))
        connection.execute(
            sqlalchemy.delete(global_user_state_schema.user_table).where(
                global_user_state_schema.user_table.c.id == owner_id))


@pytest.mark.parametrize('failure',
                         ['stale_controller', 'wrong_owner', 'deleted_owner'])
def test_service_owner_verification_fails_closed(atomic_database,
                                                 failure) -> None:
    authority = _authority()
    owner_id, owner_name = _CREATOR_ID, _CREATOR_NAME
    expected_owner = (_CREATOR_ID, _CREATOR_NAME)
    if failure == 'stale_controller':
        authority = dataclasses.replace(
            authority,
            controller_owner_epoch=authority.controller_owner_epoch + 1)
    elif failure == 'wrong_owner':
        owner_id, owner_name = 'wrong-owner', 'wrong@example.com'
        with atomic_database.begin() as connection:
            connection.execute(
                sqlalchemy.insert(global_user_state_schema.user_table).values(
                    id=owner_id, name=owner_name, created_at=int(time.time())))
    else:
        with atomic_database.begin() as connection:
            connection.execute(
                sqlalchemy.text('ALTER TABLE services DROP CONSTRAINT '
                                'serve055_service_owner_user_fk'))
            connection.execute(
                sqlalchemy.delete(global_user_state_schema.user_table).where(
                    global_user_state_schema.user_table.c.id == _CREATOR_ID))
    with pytest.raises(serve_state.ServiceOwnerAuthorityError):
        serve_state.attest_service_owner_user_id(authority, owner_id,
                                                 owner_name)
    assert _owner_tuple(atomic_database) == expected_owner


def test_missing_owner_tuple_is_attested_under_controller_fence(
        atomic_database) -> None:
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.text('DROP TRIGGER skyserve055_service_owner_guard '
                            'ON services'))
        connection.execute(
            sqlalchemy.text('ALTER TABLE services DROP CONSTRAINT '
                            'serve055_owner_user_id_nonempty'))
        connection.execute(
            sqlalchemy.text('ALTER TABLE services ALTER COLUMN '
                            'owner_user_id DROP NOT NULL'))
        connection.execute(
            sqlalchemy.text('ALTER TABLE services ALTER COLUMN '
                            'owner_user_name DROP NOT NULL'))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    owner_user_id=None, owner_user_name=None))

    serve_state.attest_service_owner_user_id(_authority(), _CREATOR_ID,
                                             _CREATOR_NAME)
    assert _owner_tuple(atomic_database) == (_CREATOR_ID, _CREATOR_NAME)


@pytest.mark.parametrize('rejection', [
    'lease_token',
    'owner',
    'lease_sequence',
    'lease_round_epoch',
    'service_generation',
    'physical_uid',
])
def test_rejected_savepoint_leaves_outer_transaction_usable_and_empty(
        atomic_database, rejection) -> None:
    spec = _atomic_spec(atomic_database)
    lease_token = 7
    if rejection == 'lease_token':
        lease_token += 1
    elif rejection == 'owner':
        spec = dataclasses.replace(spec,
                                   authority=dataclasses.replace(
                                       spec.authority,
                                       service_hash='stale-service'))
    elif rejection == 'lease_sequence':
        object.__setattr__(
            spec.actuation_lease.intent,
            'ordinary_zero_cost_admission_sequence',
            spec.actuation_lease.intent.ordinary_zero_cost_admission_sequence +
            1)
    elif rejection == 'lease_round_epoch':
        object.__setattr__(spec.actuation_lease.intent, 'pool_epoch',
                           spec.actuation_lease.intent.pool_epoch + 1)
    elif rejection == 'service_generation':
        spec.replica_info.reserved_fill_service_generation += 1
    else:
        spec.replica_info.reserved_fill_physical_cluster_uid = 'stale-uid'

    with atomic_database.connect() as connection:
        outer = connection.begin()
        with pytest.raises(reserved_fill_admission._Rejected):
            reserved_fill_admission._stage_and_bind(connection,
                                                    spec,
                                                    lease_token,
                                                    runtime=_REQUEST_RUNTIME,
                                                    require_existing=False)
        assert connection.execute(sqlalchemy.select(1)).scalar_one() == 1
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        outer.commit()

    with atomic_database.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
        assert state == zero_cost_actuation.IntentState.ACTUATING.value
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
    assert spec.replica_info.zero_cost_admission_sequence is None
    assert spec.replica_info.zero_cost_materialization_sequence is None


@pytest.mark.parametrize('field', (
    'zero_cost_admission_sequence',
    'zero_cost_materialization_sequence',
))
def test_atomic_fill_rejects_caller_assigned_event_sequence(
        atomic_database, field) -> None:
    spec = _atomic_spec(atomic_database)
    setattr(spec.replica_info, field, 7)

    with atomic_database.connect() as connection:
        outer = connection.begin()
        with pytest.raises(ValueError, match='assigned by PostgreSQL'):
            reserved_fill_admission._stage_and_bind(connection,
                                                    spec,
                                                    7,
                                                    runtime=_REQUEST_RUNTIME,
                                                    require_existing=False)
        assert connection.execute(sqlalchemy.select(1)).scalar_one() == 1
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        outer.commit()

    with atomic_database.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
        assert state == zero_cost_actuation.IntentState.ACTUATING.value
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)


def test_committed_intent_and_linked_replica_cannot_commit_without_association(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='lacks its exact (replica|association) handoff'):
        with atomic_database.begin() as connection:
            values = _commit_intent_and_build_replica_row(connection, spec)
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.replicas_table).values(**values))

    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        assert connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.state)
        ).scalar_one() == zero_cost_actuation.IntentState.ACTUATING.value


@pytest.mark.parametrize('tamper', ('card', 'count', 'context', 'zone'))
def test_database_rejects_committed_replica_location_tamper(
        atomic_database, tamper) -> None:
    spec = _atomic_spec(atomic_database)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='does not exactly match an allowed committed'):
        with atomic_database.begin() as connection:
            values = _commit_intent_and_build_replica_row(connection, spec)
            state = copy.deepcopy(values['replica_state'])
            location = state['location']
            override = state['resources_override']
            if tamper == 'card':
                location['accelerators'] = {'tampered-card': 1}
                override['accelerators'] = {'tampered-card': 1}
            elif tamper == 'count':
                card = next(iter(location['accelerators']))
                location['accelerators'] = {card: 2}
                override['accelerators'] = {card: 2}
            elif tamper == 'context':
                location['region'] = 'retargeted-context'
                override['region'] = 'retargeted-context'
            else:
                location['zone'] = 'retargeted-zone'
                override['zone'] = 'retargeted-zone'
            values['replica_state'] = state
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.replicas_table).values(**values))

    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        assert connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.state)
        ).scalar_one() == zero_cost_actuation.IntentState.ACTUATING.value


def test_database_rejects_null_current_capability_tuple(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='lost its committed service owner'):
        with atomic_database.begin() as connection:
            values = _commit_intent_and_build_replica_row(connection, spec)
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE).values(
                        ordinary_launch_binding_epoch=_BINDING_EPOCH + 1,
                        non_pool_launch_binding_capable=False,
                        non_pool_launch_controller_incarnation=None,
                        non_pool_launch_binding_protocol_version=None,
                        non_pool_launch_capability_profile_set_digest=None,
                        non_pool_launch_capability_cohort_epoch=None,
                        non_pool_launch_receipt_protocol_version=None))
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.replicas_table).values(**values))

    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        assert connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.state)
        ).scalar_one() == zero_cost_actuation.IntentState.ACTUATING.value


def test_inner_commit_is_savepoint_and_outer_rollback_removes_full_suffix(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    with atomic_database.connect() as connection:
        outer = connection.begin()
        staged, receipt = reserved_fill_admission._stage_and_bind(
            connection,
            spec,
            7,
            runtime=_REQUEST_RUNTIME,
            require_existing=False)
        assert not staged.already_committed
        assert receipt.replica_id == 1
        assert isinstance(receipt.context,
                          ordinary_launch_binding.BoundNonPoolLaunchContext)
        assert str(receipt.context.association_id) == receipt.association_id
        assert receipt.context.request_id == receipt.request_id
        assert receipt.context.service_name == _SERVICE
        assert receipt.context.replica_id == receipt.replica_id
        assert (str(
            receipt.context.replica_record_id) == receipt.replica_record_id)
        assert (receipt.context.launch_generation == receipt.launch_generation)
        assert receipt.context.profile.kind is (
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)
        assert connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.state)
        ).scalar_one() == zero_cost_actuation.IntentState.COMMITTED.value
        assert spec.replica_info.zero_cost_admission_sequence is None
        assert spec.replica_info.zero_cost_materialization_sequence is None
        outer.rollback()

    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        assert connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.state)
        ).scalar_one() == zero_cost_actuation.IntentState.ACTUATING.value
    assert spec.replica_info.zero_cost_admission_sequence is None
    assert spec.replica_info.zero_cost_materialization_sequence is None


@pytest.mark.parametrize('table', (
    serve_state_schema.replicas_table,
    ordinary_launch_binding.ordinary_launch_associations_table,
    request_postgres.REQUESTS,
    request_postgres.QUEUE,
    request_postgres.REQUEST_RETENTION_PINS,
))
def test_every_suffix_insert_fault_rolls_back_to_usable_savepoint(
        atomic_database, table) -> None:
    spec = _atomic_spec(atomic_database)
    target = f'insert into {table.name}'.casefold()
    injected = False

    def fail_after_insert(_connection, _cursor, statement, _parameters,
                          _context, _executemany):
        nonlocal injected
        if not injected and target in ' '.join(statement.casefold().split()):
            injected = True
            raise _InjectedSuffixFault()

    sqlalchemy.event.listen(atomic_database, 'after_cursor_execute',
                            fail_after_insert)
    try:
        with atomic_database.connect() as connection:
            outer = connection.begin()
            with pytest.raises(_InjectedSuffixFault):
                reserved_fill_admission._stage_and_bind(
                    connection,
                    spec,
                    7,
                    runtime=_REQUEST_RUNTIME,
                    require_existing=False)
            assert injected
            assert connection.execute(sqlalchemy.select(1)).scalar_one() == 1
            assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
            outer.commit()
    finally:
        sqlalchemy.event.remove(atomic_database, 'after_cursor_execute',
                                fail_after_insert)

    with atomic_database.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
        assert state == zero_cost_actuation.IntentState.ACTUATING.value
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)


def test_savepoint_rollback_interrupt_preserves_original_operator_signal(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    original_interrupt = _InjectedAdmissionFault()
    rollback_interrupt = _InjectedAdmissionFault()
    original_rollback = sqlalchemy.engine.NestedTransaction.rollback

    def fail_stage(*_args, **_kwargs):
        raise original_interrupt

    def lose_rollback_ack(transaction):
        original_rollback(transaction)
        raise rollback_interrupt

    monkeypatch.setattr(reserved_fill_admission, '_stage_and_bind_in_savepoint',
                        fail_stage)
    monkeypatch.setattr(sqlalchemy.engine.NestedTransaction, 'rollback',
                        lose_rollback_ack)

    with atomic_database.connect() as connection:
        outer = connection.begin()
        with pytest.raises(_InjectedAdmissionFault) as raised:
            reserved_fill_admission._stage_and_bind(connection,
                                                    spec,
                                                    7,
                                                    runtime=_REQUEST_RUNTIME,
                                                    require_existing=False)
        assert raised.value is original_interrupt
        assert raised.value.__cause__ is rollback_interrupt
        outer.rollback()

    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)


def test_root_rollback_interrupt_preserves_original_operator_signal(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    original_interrupt = _InjectedAdmissionFault()
    rollback_interrupt = _InjectedAdmissionFault()
    original_rollback = sqlalchemy.engine.RootTransaction.rollback

    def fail_stage(*_args, **_kwargs):
        raise original_interrupt

    def lose_rollback_ack(transaction):
        original_rollback(transaction)
        raise rollback_interrupt

    monkeypatch.setattr(reserved_fill_admission, '_stage_and_bind', fail_stage)
    monkeypatch.setattr(sqlalchemy.engine.RootTransaction, 'rollback',
                        lose_rollback_ack)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission._transaction(spec, 7, require_existing=False)

    assert raised.value is original_interrupt
    assert raised.value.__cause__ is rollback_interrupt
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)


@pytest.mark.parametrize('cleanup_seam', ('rollback', 'connection_close'))
def test_protocol_v1_wrapper_cleanup_preserves_original_operator_signal(
        atomic_database, monkeypatch, cleanup_seam) -> None:
    spec = _atomic_spec(atomic_database)
    original_interrupt = _InjectedAdmissionFault()
    cleanup_interrupt = _InjectedAdmissionFault()
    cleanup_faulted = False

    def fail_stage(*_args, **_kwargs):
        raise original_interrupt

    monkeypatch.setattr(serve_state, '_stage_postgres_replica_if_round_epoch',
                        fail_stage)
    if cleanup_seam == 'rollback':
        original_cleanup = sqlalchemy.engine.RootTransaction.rollback

        def lose_cleanup_ack(transaction):
            nonlocal cleanup_faulted
            original_cleanup(transaction)
            if (transaction.connection.engine is atomic_database and
                    not cleanup_faulted):
                cleanup_faulted = True
                raise cleanup_interrupt

        monkeypatch.setattr(sqlalchemy.engine.RootTransaction, 'rollback',
                            lose_cleanup_ack)
    else:
        original_cleanup = sqlalchemy.engine.Connection.close

        def lose_cleanup_ack(connection):
            nonlocal cleanup_faulted
            original_cleanup(connection)
            if connection.engine is atomic_database and not cleanup_faulted:
                cleanup_faulted = True
                raise cleanup_interrupt

        monkeypatch.setattr(sqlalchemy.engine.Connection, 'close',
                            lose_cleanup_ack)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        serve_state.add_replica_if_round_epoch(
            _SERVICE,
            7,
            spec.replica_info,
            pool_key=spec.replica_info.reserved_fill_pool_key,
            expected_epoch=spec.actuation_lease.intent.pool_epoch,
            expected_lease_token=7)

    assert cleanup_faulted
    assert raised.value is original_interrupt
    assert raised.value is not cleanup_interrupt
    assert raised.value.__cause__ is cleanup_interrupt


def test_atomic_suffix_uses_canonical_lock_order(atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    statements = []

    def observe(_connection, _cursor, statement, _parameters, _context,
                _executemany):
        statements.append(' '.join(statement.casefold().split()))

    sqlalchemy.event.listen(atomic_database, 'before_cursor_execute', observe)
    try:
        with atomic_database.connect() as connection:
            outer = connection.begin()
            reserved_fill_admission._stage_and_bind(connection,
                                                    spec,
                                                    7,
                                                    runtime=_REQUEST_RUNTIME,
                                                    require_existing=False)
            outer.rollback()
    finally:
        sqlalchemy.event.remove(atomic_database, 'before_cursor_execute',
                                observe)

    def first(fragment, *, after=-1):
        return next(index for index, statement in enumerate(statements)
                    if index > after and fragment in statement)

    first_protocol = first('from reserved_fill_protocol_state')
    second_protocol = first('from reserved_fill_protocol_state',
                            after=first_protocol)
    lifecycle = first('from service_lifecycle_fences', after=second_protocol)
    service = first('from services', after=lifecycle)
    intent = first('from serve_zero_cost_actuation_intents', after=service)
    replica_insert = first('insert into replicas', after=intent)
    association_insert = first('insert into serve_ordinary_launch_associations',
                               after=replica_insert)
    request_insert = first('insert into api_requests', after=association_insert)
    assert (first_protocol < second_protocol < lifecycle < service < intent <
            replica_insert < association_insert < request_insert)


def test_outer_commit_publishes_sequences_and_hydrates_exact_request(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    staged, receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=False)
    assert spec.replica_info.zero_cost_admission_sequence is None
    assert spec.replica_info.zero_cost_materialization_sequence is None

    # A lost commit ACK retries before any process-local publication.
    # Hydration is recovery of an already-committed graph and therefore must
    # remain independent of a newer provider-proof blackout.
    _age_provider_proof(atomic_database, 11)
    replay, replay_receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=True)
    assert replay.already_committed
    assert replay_receipt == receipt

    staged.publish_after_commit()
    assert spec.replica_info.zero_cost_admission_sequence is not None
    # Provider materialization has not happened yet.
    assert spec.replica_info.zero_cost_materialization_sequence is None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                receipt.replica_id)).mappings().one()
        assert (replica['reserved_fill_intent_idempotency_key'] ==
                spec.actuation_lease.intent.idempotency_key)
        assert (replica['replica_state']['reserved_fill_intent_idempotency_key']
                == replica['reserved_fill_intent_idempotency_key'])
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id ==
                  replica['ordinary_launch_association_id'])).mappings().one()
        assert association['authorization_reference'] == (
            'reserved-fill:' + spec.actuation_lease.intent.idempotency_key)
        assert association['authorization_generation'] == (
            spec.actuation_lease.intent.allocation_generation)
        assert zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name=_SERVICE,
            service_hash=_SERVICE_HASH,
            replica_info=staged.persisted_info) == spec.actuation_lease.intent
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
        assert stored['user_id'] == _CREATOR_ID
        body = request_postgres.request_from_mapping(stored).request_body
        assert isinstance(body, payloads.LaunchBody)
        assert body.client_api_version == server_constants.API_VERSION
        assert body.env_vars[skylet_constants.USER_ID_ENV_VAR] == _CREATOR_ID
        assert body.env_vars[skylet_constants.USER_ENV_VAR] == _CREATOR_NAME
        association_digest = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                input_digest)).scalar_one()
        assert ordinary_launch_binding.canonical_launch_digest(
            body) == association_digest
    assert ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
        receipt.request_id, _CREATOR_ID, _WORKSPACE)
    assert not ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
        receipt.request_id, 'different-owner', _WORKSPACE)
    assert not ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
        receipt.request_id, _CREATOR_ID, 'different-workspace')


def test_atomic_admission_parks_before_any_suffix_when_proof_needs_renewal(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _age_provider_proof(atomic_database, 11)
    _use_real_broker(monkeypatch, atomic_database)

    result = reserved_fill_admission.admit(spec)

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.REJECTED)
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
        intent_state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
        lane = connection.execute(
            sqlalchemy.select(kueue_lane_lineage_schema.
                              serve_kueue_admissions_table)).mappings().one()
    assert intent_state == zero_cost_actuation.IntentState.ACTUATING.value
    assert lane['state'] == 'INTENT_PENDING'
    assert lane['replica_id'] is None
    assert lane['association_id'] is None

    _publish_fresh_provider_proof(atomic_database)
    resumed = reserved_fill_admission.admit(spec)
    assert resumed.disposition is (
        reserved_fill_admission.AdmissionDisposition.COMMITTED)
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_reserved_fill_provider_io_holds_no_postgres_authority_session(
        atomic_database) -> None:
    """A parked K8s effect has durable authority, not a retained PG lock."""
    engine = atomic_database
    context, launch_context, claim = _active_reserved_fill_effect_claim(engine)
    lock_key = locks.postgres_lock_key(
        serve_state._replica_launch_authority_lock_id(_SERVICE, engine))

    def transition_service() -> bool:
        return serve_state.set_service_status_and_active_versions_if_owner(
            _SERVICE,
            _SERVICE_HASH,
            _OWNER[0],
            _OWNER[1],
            serve_state.ServiceStatus.SHUTTING_DOWN,
            expected_lifecycle_epoch=4)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='no longer authorizes provider effects'):
            with ordinary_launch_binding.non_pool_provider_effect_guard(
                    launch_context,
                    claim,
                    claim_validator=(
                        request_postgres.
                        validate_bound_non_pool_launch_claim_in_transaction
                    )) as authorization:
                assert authorization.guard is None
                with engine.connect() as connection:
                    advisory_locks = connection.execute(
                        sqlalchemy.text(
                            'SELECT count(*) FROM pg_locks '
                            "WHERE locktype = 'advisory' "
                            'AND database = ('
                            '  SELECT oid FROM pg_database '
                            '  WHERE datname = current_database()'
                            ') '
                            'AND ((classid::bigint << 32) | objid::bigint) = '
                            ':lock_key'), {
                                'lock_key': lock_key,
                            }).scalar_one()
                    all_advisory_locks = connection.execute(
                        sqlalchemy.text('SELECT count(*) FROM pg_locks '
                                        "WHERE locktype = 'advisory' "
                                        'AND database = ('
                                        '  SELECT oid FROM pg_database '
                                        '  WHERE datname = current_database()'
                                        ')')).scalar_one()
                    idle_transactions = connection.execute(
                        sqlalchemy.text(
                            'SELECT count(*) FROM pg_stat_activity '
                            'WHERE datname = current_database() '
                            'AND pid <> pg_backend_pid() '
                            "AND state = 'idle in transaction'")) \
                        .scalar_one()
                assert advisory_locks == 0
                assert all_advisory_locks == 0
                assert idle_transactions == 0

                # This body stands in for blocked Kubernetes I/O. The same-
                # service exclusive writer must complete instead of waiting
                # for the parked provider call.
                transition = pool.submit(transition_service)
                assert transition.result(timeout=5) is True

    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                effect_phase, ordinary_launch_binding.
                ordinary_launch_associations_table.c.resolution).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == context.association_id)).one()
        request = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.status,
                request_postgres.REQUESTS.c.claim_token,
                request_postgres.REQUESTS.c.worker_instance_id).where(
                    request_postgres.REQUESTS.c.request_id ==
                    context.request_id)).one()
    assert association.effect_phase == (
        ordinary_launch_binding.EffectPhase.PROVIDER_IO.value)
    assert association.resolution == (
        ordinary_launch_binding.Resolution.BOUND.value)
    assert request.status == api_requests.RequestStatus.RUNNING.value
    assert request.claim_token == uuid.UUID(claim.claim_token)
    assert request.worker_instance_id == uuid.UUID(claim.worker_instance_id)


def test_reserved_fill_owner_transfer_fences_lock_free_provider_effect(
        atomic_database) -> None:
    """Takeover commits during provider I/O and the stale effect cannot win."""
    engine = atomic_database
    context, launch_context, claim = _active_reserved_fill_effect_claim(engine)
    new_incarnation = uuid.UUID('33333333-3333-4333-8333-333333333333')
    initial_revision = None

    def transfer_owner():
        with engine.begin() as connection:
            return ordinary_launch_binding.transfer_service_owner_in_connection(
                connection,
                service_name=_SERVICE,
                expected_incarnation=_CONTROLLER_INCARNATION,
                expected_owner_epoch=_CONTROLLER_OWNER_EPOCH,
                new_incarnation=new_incarnation,
                new_controller_pid=31337,
                new_controller_ip='10.0.0.33',
                capable=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='owner revision changed'):
            with ordinary_launch_binding.non_pool_provider_effect_guard(
                    launch_context,
                    claim,
                    claim_validator=(
                        request_postgres.
                        validate_bound_non_pool_launch_claim_in_transaction
                    )) as authorization:
                assert authorization.guard is None
                initial_revision = authorization.owner_revision
                # This body stands in for an effectful provider call.  A
                # takeover must commit without waiting for it, while the
                # post-effect read rejects this now-stale authorization.
                transferred = pool.submit(transfer_owner).result(timeout=5)
                assert transferred.controller_incarnation == new_incarnation
                assert transferred.controller_owner_epoch == (
                    _CONTROLLER_OWNER_EPOCH + 1)

    assert initial_revision is not None
    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                effect_phase, ordinary_launch_binding.
                ordinary_launch_associations_table.c.resolution,
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                owner_controller_incarnation, ordinary_launch_binding.
                ordinary_launch_associations_table.c.owner_controller_epoch,
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                owner_revision).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
    assert association.effect_phase == (
        ordinary_launch_binding.EffectPhase.PROVIDER_IO.value)
    assert association.resolution == (
        ordinary_launch_binding.Resolution.BOUND.value)
    assert association.owner_controller_incarnation == new_incarnation
    assert association.owner_controller_epoch == _CONTROLLER_OWNER_EPOCH + 1
    assert association.owner_revision == initial_revision + 1

    # The active queue delivery is adopted, never resubmitted.  If that exact
    # worker later terminates, provider-effect state waits for its guardian
    # quiescence receipt before reduction/reconciliation.
    assert ordinary_launch_binding.classify_startup(
        association,
        ordinary_launch_binding.RequestStartupFacts(
            True, api_requests.RequestStatus.RUNNING.value, True, 1, True, False
        )) == ordinary_launch_binding.StartupClassification.ADOPT_ACTIVE
    assert ordinary_launch_binding.classify_startup(
        association,
        ordinary_launch_binding.RequestStartupFacts(
            True, api_requests.RequestStatus.CANCELLED.value, False, 1, False,
            False)) == (
                ordinary_launch_binding.StartupClassification.WAIT_QUIESCENCE)

    # A different executor generation has no durable claim and cannot replay
    # the already-effectful request after takeover.
    replay_claim = request_storage.ExecutionClaim(context.request_id, 2,
                                                  str(uuid.uuid4()),
                                                  str(uuid.uuid4()))
    entered = False
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='exact API request execution claim'):
        with ordinary_launch_binding.non_pool_provider_effect_guard(
                launch_context,
                replay_claim,
                claim_validator=(
                    request_postgres.
                    validate_bound_non_pool_launch_claim_in_transaction)):
            entered = True
    assert not entered


def test_reserved_fill_lock_free_effect_tracks_latest_phase_revision(
        atomic_database) -> None:
    """Legitimate job-phase CASes remain valid inside one effect epoch."""
    engine = atomic_database
    context, launch_context, claim = _active_reserved_fill_effect_claim(engine)

    with ordinary_launch_binding.non_pool_provider_effect_guard(
            launch_context,
            claim,
            claim_validator=(request_postgres.
                             validate_bound_non_pool_launch_claim_in_transaction
                            )) as authorization:
        assert authorization.guard is None
        assert ordinary_launch_binding.begin_service_job_io(
            launch_context) == authorization.owner_revision + 1
        recorded_revision = ordinary_launch_binding.record_service_job(
            launch_context, 19)
        assert recorded_revision == authorization.owner_revision + 2

    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                effect_phase, ordinary_launch_binding.
                ordinary_launch_associations_table.c.owner_revision,
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                service_job_id).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == context.association_id)).one()
    assert association.effect_phase == (
        ordinary_launch_binding.EffectPhase.SERVICE_JOB_RECORDED.value)
    assert association.owner_revision == recorded_revision
    assert association.service_job_id == 19


def test_remove_service_completely_removes_intent_linked_replica_graph(
        atomic_database, monkeypatch) -> None:
    context, info, authority = _failed_teardown_reserved_fill_ambiguity(
        atomic_database)
    _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, info,
        ordinary_launch_binding.ProviderEvidence.PRESENT)
    settlement = service._settle_bound_ordinary_launches_for_teardown(
        authority, [info])
    cleanup_contexts = settlement.provider_present_cleanup_contexts
    assert not settlement.provider_reconciliation_failures
    assert cleanup_contexts == {
        (info.replica_id, info.replica_record_id): context
    }
    persisted = serve_state.get_replica_info_from_id(_SERVICE, info.replica_id)
    assert persisted is not None
    _install_failed_teardown_provider_observation(
        monkeypatch, atomic_database, persisted,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    monkeypatch.setattr(
        replica_managers, 'terminate_cluster',
        lambda *_args, **_kwargs: _post_teardown_absence_receipt(persisted))
    replica_managers.terminate_bound_non_pool_provider_present_cluster(
        context, persisted, authority,
        functools.partial(service._project_bound_ordinary_launch_for_teardown,
                          authority), persisted.cluster_name)

    assert serve_state.remove_service_completely(
        _SERVICE,
        _SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_lifecycle_epoch=4)

    with atomic_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one() == 0


def test_remove_service_completely_rejects_unsettled_intent_graph(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    reserved_fill_admission._transaction(spec, 7, require_existing=False)
    teardown = ordinary_launch_binding.begin_service_teardown_if_owner(
        _SERVICE, _SERVICE_HASH, _OWNER)
    assert teardown.disposition is (
        ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND)

    with pytest.raises(
            kueue_lane_lineage.KueueAdmissionConflict,
            match='retirement lacks canonical provider-absence authority'):
        serve_state.remove_service_completely(_SERVICE,
                                              _SERVICE_HASH,
                                              expected_controller_owner=_OWNER,
                                              expected_lifecycle_epoch=4)

    with atomic_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one() == 1


def test_serve056_accepts_live_intent_and_replica_image_id_encodings(
        atomic_database) -> None:
    image = 'docker:registry.example/boltz@sha256:' + 'e' * 64
    spec = _atomic_spec(atomic_database, image_id=image)

    _, receipt = reserved_fill_admission._transaction(spec,
                                                      7,
                                                      require_existing=False)

    with atomic_database.connect() as connection:
        intent_locations = connection.execute(
            sqlalchemy.select(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table.c.
                              allowed_locations)).scalar_one()
        replica_state = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    receipt.replica_id)).scalar_one()
    assert intent_locations[0]['image_id'] == {'docker': image}
    assert replica_state['location']['image_id'] == [['docker', image]]
    assert replica_state['resources_override']['image_id'] == [[
        'docker', image
    ]]


def test_provider_cleanup_uses_committed_intent_gate_after_gate_advances(
        atomic_database) -> None:
    """Post-effect teardown validates frozen admission, not today's plan."""
    spec = _atomic_spec(atomic_database)
    _, receipt = reserved_fill_admission._transaction(spec,
                                                      7,
                                                      require_existing=False)
    repository = pool_capacity_observation.PoolCapacityObservationRepository(
        atomic_database)
    before = repository.read_reconciliation_gate()
    assert before.reclaim_policy_identity is not None
    rotated_identity = dataclasses.replace(
        before.reclaim_policy_identity,
        policy_revision=before.reclaim_policy_identity.policy_revision +
        '-rotated')
    evidence = reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence(
        contract=(reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256=rotated_identity.fleet_bundle_sha256,
        policy_revision=rotated_identity.policy_revision,
        provider_inventory_sha256=(rotated_identity.provider_inventory_sha256),
        claimed_contexts=repository.read_activation_claim_scope(),
        completed_monotonic=time.monotonic())
    rotated_receipt = reserved_fill_reclaim_attestation.activation_receipt(
        evidence,
        writer_image_digest='sha256:' + 'f' * 64,
        writer_deployment_generation='rotated',
        writer_deployment_uid='rotated-deployment-uid',
        writer_pod_inventory_count=1,
        writer_pod_inventory_sha256='9' * 64)
    rotated = repository.authorize_sequenced_reconciliation(
        expected_generation=before.generation, receipt=rotated_receipt)
    assert rotated.changed
    assert rotated.gate.generation == before.generation + 1

    with atomic_database.begin() as connection:
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
        request = request_postgres.request_from_mapping(request_row)
        context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
            request.request_body.extra_launch_context)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                receipt.replica_id)).mappings().one()
        ordinary_launch_binding._validate_reserved_fill_cleanup_profile_in_connection(
            connection, service, replica, context.profile)
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='planner authorization changed'):
            ordinary_launch_binding._validate_profile_authority_in_connection(
                connection, service, replica, context.profile)
        with pytest.raises(
                sqlalchemy.exc.IntegrityError), connection.begin_nested():
            connection.execute(
                sqlalchemy.delete(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table).where(
                        zero_cost_actuation_schema.
                        serve_zero_cost_actuation_intents_table.c.
                        intent_idempotency_key ==
                        spec.replica_info.reserved_fill_intent_idempotency_key))
        ordinary_launch_binding._validate_reserved_fill_cleanup_profile_in_connection(
            connection, service, replica, context.profile)


def _reconstruct_serve055_replica_handoff_boundary(engine) -> None:
    """Reconstruct Serve055 in an isolated PostgreSQL test database.

    The fixture's current writer is needed to produce a fully canonical
    intent/replica/association/request graph.  Remove the additive Serve057
    relation before its Serve056 parent key, then remove the Serve056 boundary.
    This leaves the exact deployed Serve055 representation: the immutable
    intent key exists only in ReplicaInfo JSON.  No application writes occur
    until the real 055 -> 056 migration is replayed.
    """
    statements = (
        # The fixture starts at the current schema.  Serve057 references the
        # composite intent key introduced by Serve056, so reverse the additive
        # test-only DDL in dependency order.  Avoid CASCADE: a newly introduced
        # dependent object must make this historical reconstruction fail until
        # the fixture names it explicitly.
        'DROP TABLE IF EXISTS serve_kueue_admissions',
        'DROP FUNCTION IF EXISTS skyserve057_guard_kueue_admission()',
        'DROP TRIGGER IF EXISTS '
        'skyserve056_fill_replica_handoff_consistency ON replicas',
        'DROP TRIGGER IF EXISTS '
        'skyserve056_committed_fill_intent_consistency ON '
        'serve_zero_cost_actuation_intents',
        'DROP TRIGGER IF EXISTS skyserve056_committed_fill_intent_guard ON '
        'serve_zero_cost_actuation_intents',
        'DROP TRIGGER IF EXISTS '
        'skyserve047_replica_non_pool_authorization_guard ON replicas',
        'DROP FUNCTION IF EXISTS '
        'skyserve056_check_fill_replica_handoff()',
        'DROP FUNCTION IF EXISTS '
        'skyserve056_check_committed_fill_intent()',
        'DROP FUNCTION IF EXISTS skyserve056_guard_committed_fill_intent()',
        'DROP FUNCTION IF EXISTS '
        'skyserve047_guard_replica_non_pool_authorization()',
        'ALTER TABLE replicas DROP CONSTRAINT IF EXISTS '
        'serve056_replica_intent_fk',
        'ALTER TABLE replicas DROP CONSTRAINT IF EXISTS '
        'serve056_replica_intent_uq',
        'ALTER TABLE replicas DROP CONSTRAINT IF EXISTS '
        'serve056_replica_intent_key_ck',
        'ALTER TABLE serve_zero_cost_actuation_intents DROP CONSTRAINT IF '
        'EXISTS serve056_intent_service_key_uq',
        'ALTER TABLE replicas DROP COLUMN '
        'reserved_fill_intent_idempotency_key',
        "UPDATE alembic_version_serve_state_db SET version_num = '055'",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME) == '055'
    assert 'reserved_fill_intent_idempotency_key' not in {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('replicas')
    }
    assert not sqlalchemy.inspect(engine).has_table('serve_kueue_admissions')


def test_serve056_retained_provider_present_cleanup_reaches_manager_down_shape(
        atomic_database, monkeypatch) -> None:
    """A real Serve055 JSON-only launch remains exactly cleanup-authorized."""
    current_cohort = ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH
    previous_cohort = current_cohort - 1
    assert previous_cohort > 0
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    ordinary_launch_binding_epoch=(
                        serve_state_schema.services_table.c.
                        ordinary_launch_binding_epoch + 1),
                    non_pool_launch_capability_cohort_epoch=previous_cohort))
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).mappings().one()
    previous_authority = ordinary_launch_binding._authority_from_service(
        service,
        controller_pid=service['controller_pid'],
        controller_ip=service['controller_ip'],
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=service['controller_owner_epoch'],
        capable=True)
    # Produce the retained graph exactly as the previous binary cohort did.
    # The provider-effect phase below is a durable historical fact; no current
    # provider admission or provider-effect start is exercised by this test.
    with monkeypatch.context() as previous_binary:
        previous_binary.setattr(ordinary_launch_binding,
                                'NON_POOL_CAPABILITY_COHORT_EPOCH',
                                previous_cohort)
        spec = _atomic_spec(atomic_database, authority=previous_authority)
        staged, receipt = reserved_fill_admission._transaction(
            spec, 7, require_existing=False)
    with atomic_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).mappings().one()
    authority = ordinary_launch_binding._authority_from_service(
        service,
        controller_pid=service['controller_pid'],
        controller_ip=service['controller_ip'],
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=service['controller_owner_epoch'],
        capable=True)
    assert not authority.generic_launches_required
    assert authority.retained_non_pool_settlement_allowed
    with atomic_database.connect() as connection:
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
        request = request_postgres.request_from_mapping(request_row)
    context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        request.request_body.extra_launch_context)
    assert context.profile.kind is (
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    assert context.capability_cohort_epoch == previous_cohort

    with atomic_database.begin() as connection:
        association = ordinary_launch_binding.ordinary_launch_associations_table
        connection.execute(
            sqlalchemy.update(association).where(
                association.c.association_id == context.association_id).values(
                    effect_phase=(
                        ordinary_launch_binding.EffectPhase.PROVIDER_IO.value),
                    effect_phase_changed_at=sqlalchemy.func.clock_timestamp(),
                    owner_revision=association.c.owner_revision + 1,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'retained-serve055-provider-result-uncertain')
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == context.request_id))
        now = sqlalchemy.func.clock_timestamp()
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                context.request_id).values(
                    status=api_requests.RequestStatus.CANCELLED.value,
                    terminal_cause=event_api_models.EventCause.
                    EXECUTION_LEASE_EXPIRED.value,
                    execution_generation=1,
                    execution_quiescence_required=True,
                    execution_quiesced_generation=1,
                    execution_quiesced_at=now,
                    finished_at=now,
                    updated_at=now))

    info = staged.persisted_info
    provider_payload = {
        'association_id': str(context.association_id),
        'cluster_name': info.cluster_name,
        'kubernetes_context': info.reserved_fill_kubernetes_context,
        'physical_cluster_uid': info.reserved_fill_physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': info.replica_record_id,
        'result': ordinary_launch_binding.ProviderEvidence.PRESENT.value,
    }
    current_launch_authority = dataclasses.replace(
        authority, non_pool_capability_cohort_epoch=current_cohort)
    assert current_launch_authority.generic_launches_required
    assert (current_launch_authority.non_pool_capability_cohort_epoch ==
            current_cohort)
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='no longer owns this association'):
        request_postgres.record_bound_non_pool_provider_evidence(
            context, current_launch_authority,
            ordinary_launch_binding.ProviderEvidence.PRESENT, provider_payload)
    assert request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, ordinary_launch_binding.ProviderEvidence.PRESENT,
        provider_payload)
    with atomic_database.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
    assert retained['reconciliation_outcome'] == (
        ordinary_launch_binding.ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.
        value)
    assert retained['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.PRESENT.value)
    assert retained['provider_evidence_observed_at'] >= (
        retained['execution_quiesced_at'])

    _reconstruct_serve055_replica_handoff_boundary(atomic_database)
    config = migration_utils.get_alembic_config(atomic_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '056')

    with atomic_database.connect() as connection:
        migrated_service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).mappings().one()
        migrated = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == _SERVICE,
                serve_state_schema.replicas_table.c.replica_id ==
                info.replica_id)).mappings().one()
    assert migrated['reserved_fill_intent_idempotency_key'] is None
    assert (migrated['replica_state']['reserved_fill_intent_idempotency_key'] ==
            spec.actuation_lease.intent.idempotency_key)
    authority = ordinary_launch_binding._authority_from_service(
        migrated_service,
        controller_pid=migrated_service['controller_pid'],
        controller_ip=migrated_service['controller_ip'],
        controller_incarnation=migrated_service['controller_incarnation'],
        controller_owner_epoch=migrated_service['controller_owner_epoch'],
        capable=True)
    assert not authority.generic_launches_required
    assert authority.retained_non_pool_settlement_allowed
    assert authority.non_pool_capability_cohort_epoch == previous_cohort

    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = _SERVICE
    manager._ordinary_launch_binding_authority = authority

    def _project_manager_cleanup(connection, projection):
        return manager._project_bound_ordinary_launch(None, connection,
                                                      projection)

    assert request_postgres.authorize_bound_non_pool_provider_present_cleanup(
        context, authority, project_replica_result=_project_manager_cleanup)
    with atomic_database.connect() as connection:
        persisted_row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    info.replica_id)).scalar_one()
    persisted = replica_managers.ReplicaInfo.from_storage_dict(persisted_row)
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        persisted, require_scheduled=True)
    assert manager._provider_present_cleanup_marker_shape(persisted)
    assert manager._bound_non_pool_provider_present_cleanup_context(
        persisted) == context


def test_postcommit_launch_authority_ignores_mutable_successor_state(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    staged, _ = reserved_fill_admission._transaction(spec,
                                                     7,
                                                     require_existing=False)
    launch_context = _committed_launch_context(spec)
    scope, authorization = _committed_launch_authorization(
        atomic_database, spec)
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(
        staged.persisted_info)
    monkeypatch.setattr(reserved_fill_reclaim_proofs,
                        'provider_proof_reference_holds_in_connection',
                        lambda *_args, **_kwargs: True)
    assert serve_state.reserved_fill_committed_launch_authority_holds(
        scope, authorization, launch_context, snapshot)

    # Publish a newer observation and policy/gate generation, then withdraw
    # the current claim/allocation projection entirely. These are precommit
    # planner authorities; none may revoke an already committed provider
    # handoff.
    repository = pool_capacity_observation.PoolCapacityObservationRepository(
        atomic_database)
    observation_lease = repository.begin_observation(
        pool_key=_POOL_KEY,
        physical_cluster_uid=_UID,
        accelerator_names=('a100-80gb', 'h200'),
        access_context=_CONTEXT,
        lease_duration_seconds=60,
        authority_horizon_seconds=600)
    repository.complete_success(
        observation_lease,
        pool_capacity_observation.PoolCapacitySuccess.from_counts(
            2, {
                'a100-80gb': 1,
                'h200': 1,
            }),
        access_context=observation_lease.access_context)
    before = repository.read_reconciliation_gate()
    assert before.reclaim_policy_identity is not None
    successor_identity = dataclasses.replace(
        before.reclaim_policy_identity,
        policy_revision=before.reclaim_policy_identity.policy_revision +
        '-successor')
    evidence = reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence(
        contract=(reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256=successor_identity.fleet_bundle_sha256,
        policy_revision=successor_identity.policy_revision,
        provider_inventory_sha256=(
            successor_identity.provider_inventory_sha256),
        claimed_contexts=repository.read_activation_claim_scope(),
        completed_monotonic=time.monotonic())
    successor_receipt = reserved_fill_reclaim_attestation.activation_receipt(
        evidence,
        writer_image_digest='sha256:' + 'f' * 64,
        writer_deployment_generation='successor',
        writer_deployment_uid='successor-deployment-uid',
        writer_pod_inventory_count=1,
        writer_pod_inventory_sha256='9' * 64)
    assert repository.authorize_sequenced_reconciliation(
        expected_generation=before.generation,
        receipt=successor_receipt).changed
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE))
        connection.execute(
            sqlalchemy.delete(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE))

    assert serve_state.reserved_fill_committed_launch_authority_holds(
        scope, authorization, launch_context, snapshot)


def test_postcommit_launch_authority_requires_fresh_provider_proof(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    staged, _ = reserved_fill_admission._transaction(spec,
                                                     7,
                                                     require_existing=False)
    launch_context = _committed_launch_context(spec)
    scope, authorization = _committed_launch_authorization(
        atomic_database, spec)
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(
        staged.persisted_info)
    proof = mock.Mock(return_value=False)
    monkeypatch.setattr(reserved_fill_reclaim_proofs,
                        'provider_proof_reference_holds_in_connection', proof)

    assert not serve_state.reserved_fill_committed_launch_authority_holds(
        scope, authorization, launch_context, snapshot)
    proof.assert_called_once()
    assert proof.call_args.args[1] == authorization.provider_proof_reference
    assert proof.call_args.kwargs == {
        'expected_physical_cluster_uid':
            spec.actuation_lease.intent.physical_cluster_uid
    }


@pytest.mark.parametrize('revocation', ('lifecycle', 'version', 'capability'))
def test_postcommit_launch_authority_still_honors_service_owner_revocation(
        atomic_database, monkeypatch, revocation) -> None:
    spec = _atomic_spec(atomic_database)
    staged, _ = reserved_fill_admission._transaction(spec,
                                                     7,
                                                     require_existing=False)
    launch_context = _committed_launch_context(spec)
    scope, authorization = _committed_launch_authorization(
        atomic_database, spec)
    snapshot = serve_state.ServiceReplicaLaunchFenceSnapshot(
        staged.persisted_info)
    monkeypatch.setattr(reserved_fill_reclaim_proofs,
                        'provider_proof_reference_holds_in_connection',
                        lambda *_args, **_kwargs: True)
    assert serve_state.reserved_fill_committed_launch_authority_holds(
        scope, authorization, launch_context, snapshot)

    if revocation == 'lifecycle':
        table = serve_state_schema.service_lifecycle_fences_table
        where = table.c.name == _SERVICE
        values = {'epoch': 5}
    elif revocation == 'version':
        table = serve_state_schema.services_table
        where = table.c.name == _SERVICE
        values = {'current_version': 2}
    else:
        table = serve_state_schema.services_table
        where = table.c.name == _SERVICE
        values = {
            'reserved_fill_actuation_mode':
                zero_cost_actuation.ActuationMode.DIRECT_REPLICA.value
        }
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(table).where(where).values(**values))

    assert not serve_state.reserved_fill_committed_launch_authority_holds(
        scope, authorization, launch_context, snapshot)


def test_workspace_authority_requires_durable_intent(atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    _, receipt = reserved_fill_admission._transaction(spec,
                                                      7,
                                                      require_existing=False)
    assert ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
        receipt.request_id, _CREATOR_ID, _WORKSPACE)

    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    reserved_fill_actuation_mode=(
                        zero_cost_actuation.ActuationMode.DIRECT_REPLICA.value
                    )))

    assert not ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
        receipt.request_id, _CREATOR_ID, _WORKSPACE)


def test_public_admit_uses_real_broker_and_postgres_transaction(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)

    result = reserved_fill_admission.admit(spec)

    assert result.disposition is reserved_fill_admission.AdmissionDisposition.COMMITTED
    assert result.receipt is not None
    assert spec.replica_info.zero_cost_admission_sequence is not None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_commit_ack_interrupt_hydrates_then_reraises_original(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    original_commit = sqlalchemy.engine.RootTransaction.commit
    faulted = False
    interrupt = _InjectedAdmissionFault()

    def lose_first_commit_ack(transaction):
        nonlocal faulted
        original_commit(transaction)
        if transaction.connection.engine is atomic_database and not faulted:
            faulted = True
            raise interrupt

    monkeypatch.setattr(sqlalchemy.engine.RootTransaction, 'commit',
                        lose_first_commit_ack)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission.admit(spec)

    assert faulted
    assert raised.value is interrupt
    # Exact hydration completed before the original interrupt was restored,
    # so post-commit publication is safe and visible to the caller object.
    assert spec.replica_info.zero_cost_admission_sequence is not None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_commit_interrupt_survives_second_hydration_interrupt(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    original_commit = sqlalchemy.engine.RootTransaction.commit
    commit_faulted = False
    hydration_faulted = False
    commit_interrupt = _InjectedAdmissionFault()
    hydration_interrupt = _InjectedAdmissionFault()

    def lose_first_commit_ack(transaction):
        nonlocal commit_faulted
        original_commit(transaction)
        if (transaction.connection.engine is atomic_database and
                not commit_faulted):
            commit_faulted = True
            raise commit_interrupt

    def fail_hydration_read(*_args):
        nonlocal hydration_faulted
        if commit_faulted and not hydration_faulted:
            hydration_faulted = True
            raise hydration_interrupt

    monkeypatch.setattr(sqlalchemy.engine.RootTransaction, 'commit',
                        lose_first_commit_ack)
    sqlalchemy.event.listen(atomic_database, 'before_cursor_execute',
                            fail_hydration_read)
    try:
        with pytest.raises(_InjectedAdmissionFault) as raised:
            reserved_fill_admission.admit(spec)
    finally:
        sqlalchemy.event.remove(atomic_database, 'before_cursor_execute',
                                fail_hydration_read)

    assert commit_faulted and hydration_faulted
    assert raised.value is commit_interrupt
    assert raised.value is not hydration_interrupt
    assert spec.replica_info.zero_cost_admission_sequence is None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_commit_interrupt_survives_publication_interrupt(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    original_commit = sqlalchemy.engine.RootTransaction.commit
    commit_faulted = False
    commit_interrupt = _InjectedAdmissionFault()
    publication_interrupt = _InjectedAdmissionFault()

    def lose_first_commit_ack(transaction):
        nonlocal commit_faulted
        original_commit(transaction)
        if (transaction.connection.engine is atomic_database and
                not commit_faulted):
            commit_faulted = True
            raise commit_interrupt

    def fail_publication(_staged):
        raise publication_interrupt

    monkeypatch.setattr(sqlalchemy.engine.RootTransaction, 'commit',
                        lose_first_commit_ack)
    monkeypatch.setattr(serve_state.StagedReservedFillReplica,
                        'publish_after_commit', fail_publication)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission.admit(spec)

    assert commit_faulted
    assert raised.value is commit_interrupt
    assert raised.value is not publication_interrupt
    assert raised.value.__cause__ is publication_interrupt
    assert spec.replica_info.zero_cost_admission_sequence is None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_connection_close_interrupt_hydrates_then_reraises_original(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    original_close = sqlalchemy.engine.Connection.close
    faulted = False
    interrupt = _InjectedAdmissionFault()

    def lose_first_close_ack(connection):
        nonlocal faulted
        original_close(connection)
        if connection.engine is atomic_database and not faulted:
            faulted = True
            raise interrupt

    monkeypatch.setattr(sqlalchemy.engine.Connection, 'close',
                        lose_first_close_ack)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission.admit(spec)

    assert faulted
    assert raised.value is interrupt
    assert spec.replica_info.zero_cost_admission_sequence is not None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_broker_exit_interrupt_hydrates_then_reraises_original(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable', lambda: True)
    exit_faulted = False
    interrupt = _InjectedAdmissionFault()

    class ExitFaultLock(locks.PostgresLock):

        @contextlib.contextmanager
        def acquire(self, blocking=True):
            nonlocal exit_faulted
            with super().acquire(blocking=blocking) as acquired:
                yield acquired
            if not exit_faulted:
                exit_faulted = True
                raise interrupt

    monkeypatch.setattr(
        reserved_capacity_broker.locks, 'get_lock', lambda lock_id:
        ExitFaultLock(lock_id, timeout=0, engine=atomic_database))

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission.admit(spec)

    assert exit_faulted
    assert raised.value is interrupt
    assert spec.replica_info.zero_cost_admission_sequence is not None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_public_postcommit_publication_interrupt_is_never_swallowed(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    interrupt = _InjectedAdmissionFault()

    def fail_publication(_staged):
        raise interrupt

    monkeypatch.setattr(serve_state.StagedReservedFillReplica,
                        'publish_after_commit', fail_publication)

    with pytest.raises(_InjectedAdmissionFault) as raised:
        reserved_fill_admission.admit(spec)

    assert raised.value is interrupt
    assert spec.replica_info.zero_cost_admission_sequence is None
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_competing_distinct_lineages_finish_without_deadlock(
        atomic_database) -> None:
    specs = _atomic_specs(atomic_database, 2)

    def transact(spec):
        try:
            return reserved_fill_admission._transaction(spec,
                                                        7,
                                                        require_existing=False)
        except reserved_fill_admission._Rejected:
            # The transaction-scoped global mutation gate is deliberately
            # nonblocking. A simultaneous distinct lineage may defer, then
            # succeed on the next controller tick/retry.
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(transact, spec) for spec in specs]
        outcomes = [future.result(timeout=15) for future in futures]

    committed = {
        receipt.replica_id for outcome in outcomes if outcome is not None
        for _, receipt in [outcome]
    }
    assert committed
    for spec in specs:
        if spec.replica_info.replica_id in committed:
            continue
        _, receipt = reserved_fill_admission._transaction(
            spec, 7, require_existing=False)
        committed.add(receipt.replica_id)
    assert committed == {1, 2}
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (2, 2, 2, 2, 2)


def test_cleanup_profile_survives_independent_materialization_sequence(
        atomic_database) -> None:
    """Authenticate a real two-admission graph after only #2 materializes."""
    specs = _atomic_specs(atomic_database, 2)
    outcomes = [
        reserved_fill_admission._transaction(spec, 7, require_existing=False)
        for spec in specs
    ]
    _, receipt = outcomes[1]
    association_id = uuid.UUID(receipt.association_id)
    intent_key = specs[1].actuation_lease.intent.idempotency_key
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    replicas = serve_state_schema.replicas_table
    services = serve_state_schema.services_table
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table

    with atomic_database.connect() as connection:
        intent = connection.execute(
            sqlalchemy.select(intents).where(intents.c.intent_idempotency_key ==
                                             intent_key)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 2)).mappings().one()
        service_row = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == _SERVICE)).mappings().one()
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        info = serve_state.decode_replica_state_for_authority(
            replica['replica_state_version'], replica['replica_state'])
        identity = (zero_cost_actuation.
                    kueue_admission_identity_for_locked_intent_in_connection(
                        connection, intent))
        assert identity is not None
        assert (intent['observation_sequence'],
                intent['ordinary_zero_cost_admission_sequence'],
                info.zero_cost_admission_sequence,
                info.zero_cost_materialization_sequence) == (0, 0, 2, None)
        assert kueue_lane_lineage._replica_matches_reserved_fill_intent(
            info, intent, identity, intent_key)
        info.zero_cost_admission_sequence = intent['observation_sequence']
        assert not kueue_lane_lineage._replica_matches_reserved_fill_intent(
            info, intent, identity, intent_key)
        info.zero_cost_admission_sequence = 2
        context = ordinary_launch_binding.bound_context_from_association(
            association)
        assert isinstance(context,
                          ordinary_launch_binding.BoundNonPoolLaunchContext)
        frozen_profile = context.profile
        assert ordinary_launch_binding.validate_reserved_fill_cleanup_association_in_connection(
            connection, service_row, replica, association) == frozen_profile

    with atomic_database.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_projection(
            connection)
        ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        locked_info = (
            serve_state.read_replica_for_bound_ordinary_launch_in_transaction(
                connection, _SERVICE, 2, receipt.replica_record_id,
                association_id))
        locked_info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            _SERVICE,
            _SERVICE_HASH,
            2,
            receipt.replica_record_id,
            association_id,
            locked_info,
            provider_launch_succeeded=True,
            paid_capacity_pool_key=None,
            paid_capacity_outcome=None)

    with atomic_database.connect() as connection:
        replica = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 2)).mappings().one()
        service_row = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == _SERVICE)).mappings().one()
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        materialized = serve_state.decode_replica_state_for_authority(
            replica['replica_state_version'], replica['replica_state'])
        assert (materialized.zero_cost_admission_sequence,
                materialized.zero_cost_materialization_sequence) == (2, 1)
        assert ordinary_launch_binding.validate_reserved_fill_cleanup_association_in_connection(
            connection, service_row, replica, association) == frozen_profile
        materialized.zero_cost_admission_sequence = 3
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='sequencer authority'):
            ordinary_launch_binding._zero_cost_sequence_payload(  # pylint: disable=protected-access
                connection, materialized)
        materialized.zero_cost_admission_sequence = 2
        materialized.zero_cost_materialization_sequence = 2
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='sequencer authority'):
            ordinary_launch_binding._zero_cost_sequence_payload(  # pylint: disable=protected-access
                connection, materialized)


def test_grant_writer_and_atomic_admission_forced_overlap_has_no_deadlock(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    intent = spec.actuation_lease.intent
    plan = reserved_fill_planner.FillPlan(
        policy_revision=intent.policy_revision,
        reconcile_generation=intent.reconcile_generation,
        allocation_generation=intent.allocation_generation,
        allocation_input_sha256=intent.allocation_input_sha256,
        allocation_claim_generation=intent.allocation_claim_generation,
        reconciliation_gate_generation=(intent.reconciliation_gate_generation),
        reclaim_fleet_bundle_sha256=intent.reclaim_fleet_bundle_sha256,
        reclaim_policy_revision=intent.reclaim_policy_revision,
        reclaim_provider_inventory_sha256=(
            intent.reclaim_provider_inventory_sha256),
        capacity_unit=intent.capacity_unit,
        intents=(intent,))
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        atomic_database)
    grant_holds_service = threading.Event()
    release_grant = threading.Event()
    atomic_has_session = threading.Event()
    atomic_pid: list[int] = []
    role = threading.local()

    def before_cursor(_connection, cursor, _statement, _parameters, _context,
                      _executemany):
        if getattr(role, 'name', None) == 'atomic' and not atomic_pid:
            atomic_pid.append(cursor.connection.get_backend_pid())
            atomic_has_session.set()

    def after_cursor(_connection, _cursor, statement, _parameters, _context,
                     _executemany):
        normalized = ' '.join(statement.lower().split())
        if (getattr(role, 'name', None) == 'grant' and
                'from services' in normalized and 'for update' in normalized and
                not grant_holds_service.is_set()):
            grant_holds_service.set()
            assert release_grant.wait(timeout=10)

    def run_grant():
        role.name = 'grant'
        return repository.grant_plan(
            _SERVICE,
            plan,
            max_capacity=1,
            expected_controller_incarnation=_CONTROLLER_INCARNATION,
            expected_controller_owner_epoch=_CONTROLLER_OWNER_EPOCH)

    def run_admission():
        role.name = 'atomic'
        return reserved_fill_admission._transaction(spec,
                                                    7,
                                                    require_existing=False)

    sqlalchemy.event.listen(atomic_database, 'before_cursor_execute',
                            before_cursor)
    sqlalchemy.event.listen(atomic_database, 'after_cursor_execute',
                            after_cursor)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            grant_future = executor.submit(run_grant)
            assert grant_holds_service.wait(timeout=10)
            admission_future = executor.submit(run_admission)
            assert atomic_has_session.wait(timeout=10)
            deadline = time.monotonic() + 10
            blocked = False
            with atomic_database.connect() as observer:
                while time.monotonic() < deadline:
                    blocked = bool(
                        observer.execute(
                            sqlalchemy.text(
                                'SELECT cardinality(pg_blocking_pids(:pid)) '
                                '> 0'), {
                                    'pid': atomic_pid[0]
                                }).scalar_one())
                    if blocked:
                        break
                    time.sleep(0.02)
            assert blocked, 'atomic admission never overlapped the grant lock'
            release_grant.set()
            grant_receipt = grant_future.result(timeout=15)
            _, admission_receipt = admission_future.result(timeout=15)
    finally:
        release_grant.set()
        sqlalchemy.event.remove(atomic_database, 'before_cursor_execute',
                                before_cursor)
        sqlalchemy.event.remove(atomic_database, 'after_cursor_execute',
                                after_cursor)

    assert grant_receipt.accepted[0].replica_id is None
    assert admission_receipt.replica_id == spec.replica_info.replica_id
    with atomic_database.connect() as connection:
        assert _suffix_counts(connection) == (1, 1, 1, 1, 1)


def test_killed_persist_lock_session_is_fenced_before_atomic_suffix(
        atomic_database, monkeypatch) -> None:
    """A successor round's lease token fences the complete atomic suffix."""
    spec = _atomic_spec(atomic_database)
    _use_real_broker(monkeypatch, atomic_database)
    pool_key = spec.actuation_lease.intent.pool_key
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_rounds_table).where(
                    serve_state_schema.reserved_fill_rounds_table.c.pool_key ==
                    pool_key).values(snapshot_time=0.0))

    persist_entered = threading.Event()
    resume_persist = threading.Event()
    stale_token = {}
    persist_errors = []

    def transaction(token):
        stale_token['value'] = token
        persist_entered.set()
        assert resume_persist.wait(timeout=60)
        return reserved_fill_admission._transaction(spec,
                                                    token,
                                                    require_existing=False)

    def run_persist():
        try:
            reserved_capacity_broker.run_fill_persist_transaction(transaction)
        except BaseException as error:  # pylint: disable=broad-except
            persist_errors.append(error)

    persist_thread = threading.Thread(target=run_persist)
    persist_thread.start()
    assert persist_entered.wait(timeout=60)
    assert isinstance(stale_token['value'], int)

    lock_key = locks.postgres_lock_key(
        serve_constants.RESERVED_FILL_BROKER_LOCK_ID)
    with atomic_database.begin() as observer:
        holder_pid = observer.execute(
            sqlalchemy.text(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' "
                'AND granted AND '
                '((classid::bigint << 32) | objid::bigint) = :lock_key'), {
                    'lock_key': lock_key
                }).scalar_one()
        assert observer.execute(
            sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                'pid': holder_pid
            }).scalar_one()

    real_occupying_debit = reserved_capacity_broker._occupying_debit
    successor_scanned = threading.Event()
    resume_successor = threading.Event()

    def paused_after_replica_scan(*args, **kwargs):
        scanned = real_occupying_debit(*args, **kwargs)
        successor_scanned.set()
        assert resume_successor.wait(timeout=60)
        return scanned

    monkeypatch.setattr(reserved_capacity_broker, '_occupying_debit',
                        paused_after_replica_scan)

    # The imported allocation fixture deliberately uses ancient timestamps.
    # Refresh only the unchanged claim-set and edge heartbeats so the
    # successor exercises the real takeover path without replacing the
    # generation whose atomic suffix is under test.
    claim_generation = spec.actuation_lease.intent.service_generation
    with atomic_database.begin() as connection:
        heartbeat_ts = float(
            connection.execute(
                sqlalchemy.text('SELECT EXTRACT(EPOCH FROM clock_timestamp())')
            ).scalar_one())
        claim_set_update = connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(
                serve_state_schema.reserved_fill_service_claim_sets_table.c.
                service_name == _SERVICE,
                serve_state_schema.reserved_fill_service_claim_sets_table.c.
                generation == claim_generation).values(
                    heartbeat_ts=heartbeat_ts))
        claim_edge_update = connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE,
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_generation == claim_generation,
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    pool_key == pool_key).values(heartbeat_ts=heartbeat_ts))
        assert claim_set_update.rowcount == 1
        assert claim_edge_update.rowcount == 1
    committed_observation = (
        pool_capacity_observation.PoolCapacityObservationRepository(
            atomic_database).read_latest_authoritative(pool_key))
    assert committed_observation is not None
    successor = {}
    successor_errors = []
    successor_done = threading.Event()

    def run_successor():
        try:
            successor['allocation'] = (
                reserved_capacity_broker.run_round_from_committed_observation(
                    _SERVICE,
                    pool_key,
                    committed_observation,
                    0.001,
                    expected_service_generation=(
                        spec.actuation_lease.intent.service_generation),
                    publish_round=(
                        reserved_capacity_broker.publish_committed_round)))
        except BaseException as error:  # pylint: disable=broad-except
            successor_errors.append(error)
        finally:
            successor_done.set()

    successor_thread = threading.Thread(target=run_successor)
    successor_thread.start()
    try:
        deadline = time.monotonic() + 60
        while not successor_scanned.wait(timeout=0.1):
            if successor_done.is_set():
                assert not successor_errors, successor_errors
                pytest.fail('successor round exited before its replica scan')
            if time.monotonic() >= deadline:
                pytest.fail('successor round did not reach its replica scan')
        with atomic_database.connect() as connection:
            successor_token = connection.execute(
                sqlalchemy.select(
                    serve_state_schema.reserved_fill_lease_table.c.epoch).where(
                        serve_state_schema.reserved_fill_lease_table.c.id ==
                        1)).scalar_one()
        assert int(successor_token) > int(stale_token['value'])
        resume_persist.set()
        persist_thread.join(timeout=60)
        assert not persist_thread.is_alive(), 'stale admission thread hung'
        assert len(persist_errors) == 1
        assert isinstance(persist_errors[0], reserved_fill_admission._Rejected)
        with atomic_database.connect() as connection:
            assert _suffix_counts(connection) == (0, 0, 0, 0, 0)
            assert connection.execute(
                sqlalchemy.select(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table.c.state)
            ).scalar_one() == zero_cost_actuation.IntentState.ACTUATING.value
    finally:
        resume_persist.set()
        resume_successor.set()
    persist_thread.join(timeout=60)
    successor_thread.join(timeout=60)
    assert not persist_thread.is_alive(), 'stale admission thread hung'
    assert not successor_thread.is_alive(), 'successor round thread hung'
    assert not successor_errors, successor_errors
    assert successor.get('allocation') is not None


def test_user_rename_between_commit_and_hydration_keeps_exact_digest(
        atomic_database) -> None:
    spec = _atomic_spec(atomic_database)
    staged, receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=False)
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state_schema.user_table).where(
                global_user_state_schema.user_table.c.id == _CREATOR_ID).values(
                    name='renamed@example.com'))
    replay, replay_receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=True)
    assert replay.already_committed
    assert replay_receipt == receipt
    staged.publish_after_commit()
    with atomic_database.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
        body = request_postgres.request_from_mapping(stored).request_body
        assert body.env_vars[skylet_constants.USER_ENV_VAR] == _CREATOR_NAME
        digest = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                input_digest)).scalar_one()
        assert ordinary_launch_binding.canonical_launch_digest(body) == digest


def test_user_rename_after_commit_executes_as_current_without_digest_rewrite(
        atomic_database, monkeypatch) -> None:
    spec = _atomic_spec(atomic_database)
    _, receipt = reserved_fill_admission._transaction(spec,
                                                      7,
                                                      require_existing=False)
    renamed = 'renamed-before-execution@example.com'
    with atomic_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state_schema.user_table).where(
                global_user_state_schema.user_table.c.id == _CREATOR_ID).values(
                    name=renamed))
    with atomic_database.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
        body = request_postgres.request_from_mapping(stored).request_body
        digest = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                input_digest)).scalar_one()

    reload_request = mock.Mock()
    monkeypatch.setattr(global_user_state._db_manager, '_engine',
                        atomic_database)
    monkeypatch.setattr(executor.server_common, 'reload_for_new_request',
                        reload_request)
    monkeypatch.setattr(
        executor.skypilot_config, 'override_skypilot_config',
        lambda *_args, **_kwargs: executor.skypilot_config.
        local_active_workspace_ctx(_WORKSPACE))
    monkeypatch.setattr(executor, '_should_apply_workspace_resolver',
                        lambda _version: False)
    permission_check = mock.Mock(
        side_effect=AssertionError('membership check must be bypassed'))
    set_event_workspace = mock.Mock(return_value=True)
    monkeypatch.setattr(executor.workspaces_core,
                        'reject_request_for_unauthorized_workspace',
                        permission_check)
    monkeypatch.setattr(executor.api_requests, 'set_event_workspace',
                        set_event_workspace)

    assert isinstance(body, payloads.LaunchBody)
    assert body.env_vars[skylet_constants.USER_ENV_VAR] == _CREATOR_NAME
    with executor.override_request_env_and_config(body,
                                                  receipt.request_id,
                                                  'sky.launch',
                                                  require_existing_user=True):
        assert os.environ[skylet_constants.USER_ID_ENV_VAR] == _CREATOR_ID
        assert os.environ[skylet_constants.USER_ENV_VAR] == renamed
        effective_user = reload_request.call_args.kwargs['user']
        assert effective_user.id == _CREATOR_ID
        assert effective_user.name == renamed

    permission_check.assert_not_called()
    set_event_workspace.assert_called_once_with(receipt.request_id, _WORKSPACE)

    with atomic_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(global_user_state_schema.user_table.c.name).where(
                global_user_state_schema.user_table.c.id ==
                _CREATOR_ID)).scalar_one() == renamed
    assert body.env_vars[skylet_constants.USER_ENV_VAR] == _CREATOR_NAME
    assert ordinary_launch_binding.canonical_launch_digest(body) == digest
