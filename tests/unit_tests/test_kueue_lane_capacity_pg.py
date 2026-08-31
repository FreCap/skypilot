"""PostgreSQL capacity contracts for three-state Kueue admissions."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import dataclasses
import uuid

import pytest
import sqlalchemy
from test_concurrency_autoscaler import _make_autoscaler
from test_concurrency_autoscaler import _replica
from test_kueue_lane_lineage_pg import _assert_retired_graph
from test_kueue_lane_lineage_pg import _canonical_intent_key
from test_kueue_lane_lineage_pg import (
    _configure_serve_state_for_kueue_retirement)
from test_kueue_lane_lineage_pg import _EAST_PROJECTION
from test_kueue_lane_lineage_pg import _identity
from test_kueue_lane_lineage_pg import _insert_intent
from test_kueue_lane_lineage_pg import (
    _install_canonical_cleanup_profile_authority)
from test_kueue_lane_lineage_pg import _install_historical_v5_worker_projections
from test_kueue_lane_lineage_pg import _install_retirable_materialized_graph
from test_kueue_lane_lineage_pg import _materialize
from test_kueue_lane_lineage_pg import _receipt
from test_kueue_lane_lineage_pg import _reserved_location_state
from test_kueue_lane_lineage_pg import _set_physical_provider_evidence
from test_kueue_lane_lineage_pg import admission_database  # noqa: F401
from test_kueue_lane_lineage_pg import postgres_engine  # noqa: F401
from test_serve_resource_actions_pg import empty_postgres  # noqa: F401

from sky import clouds
from sky.serve import capacity_admission
from sky.serve import kueue_lane_capacity
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_lineage_schema
from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import spot_placer
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema

pytestmark = pytest.mark.xdist_group(name='serve_kueue_capacity_057_pg')

_SERVICE = 'svc'
_SERVICE_HASH = 'service-incarnation'
_SERVICE_VERSION = 19
_LIFECYCLE_EPOCH = 3
_INTENTS = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
_REPLICAS = serve_state_schema.replicas_table
_ADMISSIONS = kueue_lane_lineage_schema.serve_kueue_admissions_table
_COPIED_ADMISSION_MUTATIONS = (
    ('intent_idempotency_key', '0' * 64),
    ('unresolved_domain_sha256', '0' * 64),
    ('service_name', 'other-service'),
    ('service_hash', 'other-incarnation'),
    ('service_lifecycle_epoch', 4),
    ('service_version', 18),
    ('pool_key', 'other-pool'),
    ('pool_epoch', 8),
    ('physical_cluster_uid', 'other-cluster'),
    ('kubernetes_context', 'other-context'),
    ('accelerator', 'l4'),
    ('accelerator_count', 2),
    ('worker_projection_sha256', '0' * 64),
    ('capacity_unit', 'logical'),
    ('planned_capacity', 2),
)


def _normalize_replica_state(
    connection: sqlalchemy.engine.Connection,
    *,
    replica_id: int,
    record_id: uuid.UUID,
    accelerator: str = 'H200',
    accelerator_count: int = 1,
) -> None:
    """Install the normalized ReplicaInfo v18 accounting projection."""
    identity = _identity(accelerator=accelerator.casefold(),
                         accelerator_count=accelerator_count)
    location = spot_placer.Location(
        cloud=clouds.Kubernetes(),
        region=identity.kubernetes_context,
        zone=None,
        accelerators={accelerator: accelerator_count},
        use_spot=False)
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'{_SERVICE}-{replica_id}',
                                        replica_port='8000',
                                        is_spot=False,
                                        location=location,
                                        version=identity.service_version,
                                        resources_override=location.to_dict(),
                                        planned_capacity=1)
    info.replica_record_id = str(record_id)
    info.reserved_fill = True
    info.is_zero_cost = True
    info.reserved_fill_pool_key = identity.pool_key
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = identity.physical_cluster_uid
    info.reserved_fill_kubernetes_context = identity.kubernetes_context
    info.reserved_fill_allocation_generation = 1
    info.reserved_fill_allocation_input_sha256 = 'a' * 64
    info.reserved_fill_allocation_claim_generation = 1
    info.reserved_fill_reconciliation_gate_generation = 1
    info.reserved_fill_reclaim_fleet_bundle_sha256 = 'b' * 64
    info.reserved_fill_reclaim_policy_revision = 'reclaim-v1'
    info.reserved_fill_reclaim_provider_inventory_sha256 = 'c' * 64
    info.reserved_fill_worker_projection_sha256 = (
        identity.worker_projection_sha256)
    info.reserved_fill_observation_generation = 1
    info.reserved_fill_observation_sequence = 1
    info.reserved_fill_intent_idempotency_key = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.reserved_fill_intent_idempotency_key).where(
                _REPLICAS.c.service_name == _SERVICE,
                _REPLICAS.c.replica_id == replica_id)).scalar_one()
    connection.execute(
        sqlalchemy.update(_REPLICAS).where(
            _REPLICAS.c.service_name == _SERVICE,
            _REPLICAS.c.replica_id == replica_id).values(
                replica_state_version=1, replica_state=info.to_storage_dict()))


def _install_materialized_admission(
    engine: sqlalchemy.engine.Engine,
    *,
    intent_key: str,
    state: kueue_lane_lineage.KueueAdmissionState,
    replacement_surge: bool = False,
) -> tuple[kueue_lane_lineage.KueueAdmissionRepository, uuid.UUID]:
    identity = _identity()
    _insert_intent(engine, intent_key)
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    digest = None
    if replacement_surge:
        digest = kueue_lane_capacity.replacement_compatibility_sha256(
            service_hash=_SERVICE_HASH,
            service_lifecycle_epoch=_LIFECYCLE_EPOCH,
            service_version=_SERVICE_VERSION,
            capacity_unit='physical',
            accelerator='h200',
            accelerator_count=1,
            worker_projection_sha256=identity.worker_projection_sha256)
    with engine.begin() as connection:
        repository.insert_intent_pending_in_connection(
            connection,
            identity,
            intent_key,
            replacement_surge_units=(1 if replacement_surge else 0),
            replacement_compatibility_sha256=digest)
    record_id, association_id = _materialize(engine, repository, identity,
                                             intent_key)
    with engine.begin() as connection:
        _normalize_replica_state(connection, replica_id=1, record_id=record_id)
        receipt = _receipt(state, intent_key, record_id, identity=identity)
        kwargs = dict(intent_idempotency_key=intent_key,
                      replica_id=1,
                      replica_record_id=record_id,
                      provider_cluster_generation=9,
                      association_id=association_id,
                      pod_namespace='skypilot',
                      pod_name='worker-1',
                      pod_uid='pod-uid-1',
                      pod_receipt=receipt,
                      provider_read_started_at=connection.execute(
                          sqlalchemy.select(
                              sqlalchemy.func.clock_timestamp())).scalar_one())
        if state is kueue_lane_lineage.KueueAdmissionState.POD_WAITING:
            repository.observe_pod_waiting_in_connection(
                connection, identity, **kwargs)
        elif state is kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED:
            repository.observe_policy_admitted_in_connection(
                connection, identity, **kwargs)
        else:
            raise AssertionError(
                'The materialized fixture needs an observation.')
    return repository, record_id


def _locked_projection(
    connection: sqlalchemy.engine.Connection,
    *,
    accounting_cards: set[str] | None = None,
    capacity_unit: reserved_fill_planner.
    FillCapacityUnit = reserved_fill_planner.FillCapacityUnit.PHYSICAL,
) -> tuple[kueue_lane_capacity.KueueAdmissionCapacityProjection, tuple[dict[
        str, int], dict[str, int], dict[str, int], int]]:
    cards = {'h200'} if accounting_cards is None else accounting_cards
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    locked = capacity_admission._lock_capacity_rows(connection,
                                                    service_name=_SERVICE,
                                                    service_hash=_SERVICE_HASH,
                                                    now=now)
    projection = capacity_admission._lock_kueue_projection(
        connection,
        service_name=_SERVICE,
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=_LIFECYCLE_EPOCH,
        service_version=_SERVICE_VERSION,
        accounting_cards=cards,
        locked=locked)
    inventory = capacity_admission._project_capacity_inventory(
        locked,
        service_version=_SERVICE_VERSION,
        capacity_unit=capacity_unit,
        accounting_cards=cards,
        now=now,
        lane_projection=projection)
    return projection, inventory


def _stored_replica_info(
        engine: sqlalchemy.engine.Engine) -> replica_managers.ReplicaInfo:
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(_REPLICAS.c.replica_state).where(
                _REPLICAS.c.service_name == _SERVICE,
                _REPLICAS.c.replica_id == 1)).scalar_one()
    return replica_managers.ReplicaInfo.from_storage_dict(dict(state))


def _prepare_real_retirement_snapshot(
    engine: sqlalchemy.engine.Engine,
    monkeypatch,
    reserved: replica_managers.ReplicaInfo | None,
) -> tuple[object, object, object]:
    """Cross the PostgreSQL projection-to-autoscaler integration seam."""
    paid = _replica(2, card='H200', version=_SERVICE_VERSION)
    autoscaler = _make_autoscaler()
    autoscaler.latest_version = _SERVICE_VERSION
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: engine)
    replicas = [paid] if reserved is None else [paid, reserved]
    inputs = autoscaler._prepare_scaling_decision_inputs(replicas)
    autoscaler._kueue_capacity_by_replica_id_for_tick = dict(
        inputs.kueue_capacity_by_replica_id)
    autoscaler._kueue_blocked_retirement_shapes_for_tick = (
        inputs.kueue_blocked_retirement_shapes)
    return autoscaler, paid, inputs


def test_fresh_waiting_is_neither_demand_supply_nor_assigned_capacity(
        admission_database) -> None:
    key = '1' * 64
    _, record_id = _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING)

    with admission_database.begin() as connection:
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is False
    assert projection.assigned_gpu_for_intent(key) is False
    assert projection.fresh_waiting_replica_record_ids == {(1, record_id)}
    assert inventory == ({'h200': 0}, {'h200': 0}, {'h200': 0}, 0)


def test_policy_admitted_is_supply_and_assigned_before_replica_ready(
        admission_database) -> None:
    key = '2' * 64
    _, record_id = _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)

    with admission_database.begin() as connection:
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is True
    assert projection.assigned_gpu_for_intent(key) is True
    assert projection.admitted_replica_record_ids == {(1, record_id)}
    assert inventory == ({'h200': 1}, {'h200': 0}, {'h200': 0}, 0)


def test_missing_kueue_admission_is_exact_shape_unknown(
        admission_database) -> None:
    key = '6' * 64
    _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key == key))
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is False
    assert projection.assigned_gpu_for_intent(key) is True
    assert projection.unknown_intent_keys == {key}
    assert projection.unknown_shapes == {('h200', 1)}
    assert not projection.unbounded_unknown
    assert inventory == ({'h200': 1}, {'h200': 0}, {'h200': 0}, 0)


def test_historical_v5_live_intent_is_exact_shape_unknown(
        admission_database) -> None:
    """Fresh accounting cannot reinterpret cleanup-only v5 as admission."""
    projection_sha256 = _install_historical_v5_worker_projections(
        admission_database)
    key = _canonical_intent_key(worker_projection_sha256=projection_sha256)
    _insert_intent(admission_database,
                   key,
                   worker_projection_sha256=projection_sha256)

    with admission_database.begin() as connection:
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is False
    assert projection.assigned_gpu_for_intent(key) is True
    assert projection.unknown_intent_keys == {key}
    assert projection.unknown_shapes == {('h200', 1)}
    assert not projection.unbounded_unknown
    assert inventory == ({'h200': 0}, {'h200': 0}, {'h200': 1}, 0)


@pytest.mark.parametrize(('field', 'value'), _COPIED_ADMISSION_MUTATIONS)
def test_each_copied_admission_identity_mismatch_is_exact_shape_unknown(
        admission_database, monkeypatch, field, value) -> None:
    key = '7' * 64
    _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
    original = (kueue_lane_lineage.KueueAdmissionRepository.
                lock_service_admissions_in_connection)

    def _corrupt(self, connection, service_name, service_hash, **kwargs):
        rows = original(self, connection, service_name, service_hash, **kwargs)
        return tuple(
            dataclasses.replace(row, **{field: value}) if row.
            intent_idempotency_key == key else row for row in rows)

    monkeypatch.setattr(kueue_lane_lineage.KueueAdmissionRepository,
                        'lock_service_admissions_in_connection', _corrupt)
    with admission_database.begin() as connection:
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is False
    assert projection.assigned_gpu_for_intent(key) is True
    assert key in projection.unknown_intent_keys
    assert projection.unknown_shapes == {('h200', 1)}
    assert not projection.unbounded_unknown
    # A corrupted copied key leaves both the original materialized graph and
    # the now-unmatched admission row as distinct unresolved authorities.
    # Debit both exact shapes until their identity collision is adjudicated.
    expected_pending = 1 if field == 'intent_idempotency_key' else 0
    assert inventory == ({
        'h200': 1
    }, {
        'h200': 0
    }, {
        'h200': expected_pending
    }, 0)


def test_proven_east_intent_without_admission_retains_legacy_accounting(
        admission_database) -> None:
    key = '8' * 64
    _insert_intent(admission_database,
                   key,
                   pool_key=reserved_capacity_broker.make_pool_key(
                       'east',
                       'h200',
                       protocol_version=reserved_capacity_broker.PROTOCOL_V2,
                       physical_cluster_uid='cluster-east'),
                   physical_cluster_uid='cluster-east',
                   kubernetes_context='east',
                   worker_projection_sha256=_EAST_PROJECTION,
                   allowed_locations=[{
                       'cloud': 'Kubernetes',
                       'region': 'east',
                       'zone': None,
                       'accelerators': {
                           'h200': 1,
                       },
                       'use_spot': False,
                       'image_id': None,
                       'container_image': None,
                       'disk_tier': None,
                       'ephemeral_storage': None,
                       'instance_type': None,
                   }])

    with admission_database.begin() as connection:
        intent = connection.execute(
            sqlalchemy.select(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key ==
                key).with_for_update()).mappings().one()
        assert (zero_cost_actuation.
                kueue_admission_identity_for_locked_intent_in_connection(
                    connection, intent) is None)
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is None
    assert projection.assigned_gpu_for_intent(key) is None
    assert not projection.has_unknown
    assert inventory == ({'h200': 0}, {'h200': 0}, {'h200': 1}, 0)


def test_missing_admission_real_projection_blocks_autoscaler_retirement(
        admission_database, monkeypatch) -> None:
    """A missing row reaches retirement as DB-derived exact-shape UNKNOWN."""
    key = '9' * 64
    _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
    reserved = _stored_replica_info(admission_database)
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(_ADMISSIONS).where(
                _ADMISSIONS.c.intent_idempotency_key == key))

    autoscaler, paid, inputs = _prepare_real_retirement_snapshot(
        admission_database, monkeypatch, reserved)

    assert inputs.kueue_capacity_by_replica_id == {
        1: kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN,
    }
    assert inputs.kueue_blocked_retirement_shapes == {('h200', 1)}
    assert not autoscaler._kueue_ordinary_victim_eligible(paid)
    assert not autoscaler._kueue_ordinary_victim_eligible(reserved)


def test_unmaterialized_live_missing_admission_blocks_paid_retirement(
        admission_database, monkeypatch) -> None:
    """Locked PostgreSQL intent liveness does not depend on ReplicaInfo."""
    key = 'c' * 64
    _insert_intent(admission_database, key)

    autoscaler, paid, inputs = _prepare_real_retirement_snapshot(
        admission_database, monkeypatch, None)

    assert not inputs.kueue_capacity_by_replica_id
    assert inputs.kueue_blocked_retirement_shapes == {('h200', 1)}
    assert not autoscaler._kueue_ordinary_victim_eligible(paid)


def test_provider_clean_retirement_deletes_live_intent_before_accounting(
        admission_database, monkeypatch) -> None:
    """Retained association history cannot become permanent UNKNOWN."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database, repository, intent_key=key, replica_id=1)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    assert serve_state.remove_replica(_SERVICE,
                                      1,
                                      expected_service_hash=_SERVICE_HASH,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                      expected_replica_record_id=str(record_id))
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))

    autoscaler, paid, inputs = _prepare_real_retirement_snapshot(
        admission_database, monkeypatch, None)
    assert not inputs.kueue_capacity_by_replica_id
    assert inputs.kueue_blocked_retirement_shapes == set()
    assert autoscaler._kueue_ordinary_victim_eligible(paid)


@pytest.mark.parametrize(('field', 'value'), _COPIED_ADMISSION_MUTATIONS)
def test_each_real_copied_mismatch_blocks_autoscaler_retirement(
        admission_database, monkeypatch, field, value) -> None:
    """Every copied-field corruption crosses DB projection as UNKNOWN."""
    key = 'a' * 64
    _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
    reserved = _stored_replica_info(admission_database)
    original = (kueue_lane_lineage.KueueAdmissionRepository.
                lock_service_admissions_in_connection)

    def _corrupt(self, connection, service_name, service_hash, **kwargs):
        rows = original(self, connection, service_name, service_hash, **kwargs)
        return tuple(
            dataclasses.replace(row, **{field: value}) if row.
            intent_idempotency_key == key else row for row in rows)

    monkeypatch.setattr(kueue_lane_lineage.KueueAdmissionRepository,
                        'lock_service_admissions_in_connection', _corrupt)
    autoscaler, paid, inputs = _prepare_real_retirement_snapshot(
        admission_database, monkeypatch, reserved)

    assert inputs.kueue_capacity_by_replica_id == {
        1: kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN,
    }
    assert inputs.kueue_blocked_retirement_shapes == {('h200', 1)}
    assert not autoscaler._kueue_ordinary_victim_eligible(paid)
    assert not autoscaler._kueue_ordinary_victim_eligible(reserved)


def test_real_east_no_admission_keeps_ordinary_autoscaler_retirement(
        admission_database, monkeypatch) -> None:
    """An exact East projection is the sole no-admission legacy control."""
    key = 'b' * 64
    _insert_intent(admission_database,
                   key,
                   pool_key=reserved_capacity_broker.make_pool_key(
                       'east',
                       'h200',
                       protocol_version=reserved_capacity_broker.PROTOCOL_V2,
                       physical_cluster_uid='cluster-east'),
                   physical_cluster_uid='cluster-east',
                   kubernetes_context='east',
                   worker_projection_sha256=_EAST_PROJECTION,
                   allowed_locations=[{
                       'cloud': 'Kubernetes',
                       'region': 'east',
                       'zone': None,
                       'accelerators': {
                           'h200': 1,
                       },
                       'use_spot': False,
                       'image_id': None,
                       'container_image': None,
                       'disk_tier': None,
                       'ephemeral_storage': None,
                       'instance_type': None,
                   }])
    reserved = _replica(1,
                        card='H200',
                        version=_SERVICE_VERSION,
                        reserved_fill=True)
    reserved.is_zero_cost = True
    reserved.reserved_fill_intent_idempotency_key = key
    reserved.replica_record_id = str(uuid.uuid4())

    autoscaler, paid, inputs = _prepare_real_retirement_snapshot(
        admission_database, monkeypatch, reserved)

    assert not inputs.kueue_capacity_by_replica_id
    assert inputs.kueue_blocked_retirement_shapes == set()
    assert autoscaler._kueue_ordinary_victim_eligible(paid)
    assert autoscaler._kueue_ordinary_victim_eligible(reserved)


def test_terminal_never_materialized_admission_is_conservative_debit(
        admission_database) -> None:
    key = '3' * 64
    accelerator_count = 8
    identity = _identity(accelerator_count=accelerator_count)
    _insert_intent(admission_database,
                   key,
                   accelerator_count=accelerator_count,
                   allowed_locations=[{
                       **_reserved_location_state(),
                       'accelerators': {
                           'h200': accelerator_count,
                       },
                   }])
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       key)
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.update(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key == key).values(
                    state='TERMINAL',
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error='grant_expired',
                    terminal_at=now,
                    updated_at=now))

    with admission_database.begin() as connection:
        projection, inventory = _locked_projection(connection)

    assert projection.demand_supply_for_intent(key) is False
    assert projection.assigned_gpu_for_intent(key) is True
    assert projection.unknown_shapes == {('h200', accelerator_count)}
    assert not projection.unbounded_unknown
    assert inventory == ({'h200': 0}, {'h200': 0}, {'h200': 1}, 0)

    # A retained physical one-machine debit must never under-debit a current
    # logical service whose same machine represents eight GPU slots.
    with admission_database.begin() as connection:
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='capacity debit is malformed'):
            _locked_projection(
                connection,
                capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)


def test_bounded_unknown_shape_does_not_block_unrelated_paid_card(
        admission_database) -> None:
    key = '4' * 64
    _insert_intent(admission_database, key)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, _identity(),
                                                       key)
        projection, inventory = _locked_projection(connection,
                                                   accounting_cards={'l4'})

    assert projection.unknown_shapes == {('h200', 1)}
    assert not projection.unbounded_unknown
    assert inventory == ({'l4': 0}, {'l4': 0}, {'l4': 0}, 0)


def test_surge_lease_survives_shutting_down_until_provider_cleanup(
        admission_database) -> None:
    key = '5' * 64
    _, record_id = _install_materialized_admission(
        admission_database,
        intent_key=key,
        state=kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
        replacement_surge=True)
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(_REPLICAS).where(
                _REPLICAS.c.service_name == _SERVICE,
                _REPLICAS.c.replica_id == 1).values(status='SHUTTING_DOWN'))
        projection, inventory = _locked_projection(connection)

    assert projection.replacement_surge_intent_keys == {key}
    assert projection.replacement_surge_shapes == {('h200', 1)}
    assert projection.replacement_surge_replica_record_ids == {(1, record_id)}
    assert projection.assigned_gpu_for_intent(key) is True
    assert inventory == ({'h200': 0}, {'h200': 0}, {'h200': 1}, 0)
