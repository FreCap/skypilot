"""Real-PostgreSQL tests for the temporary v18 record normalizer."""

import contextlib
import copy
import json
import logging
import pickle
import sys
import types

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_pool_capacity_observation_pg import _isolated_engine
from test_pool_capacity_observation_pg import pg_server as _pool_pg_server

from sky import clouds
from sky.serve import replica_info
from sky.serve import replica_managers
from sky.serve import replica_record_normalization as normalization
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils.db import migration_utils

_LEGACY_TOP_BASE = frozenset((
    'replica_info_version',
    'replica_id',
    'cluster_name',
    'version',
    'replica_port',
    'created_at',
    'first_not_ready_time',
    'first_consecutive_failure_time',
    'status_property',
    'is_spot',
    'location',
    'resources_override',
    'reserved_fill',
    'cost_rebalance_for_replica_id',
))
_LEGACY_TOP_CAPACITY = frozenset((
    'planned_capacity',
    'unknown_capacity_replacement',
    'logical_bridge_capacity_verified',
))
_LEGACY_TOP_ECONOMIC = frozenset((
    'is_zero_cost',
    'paid_capacity_pool_key',
))
_LEGACY_TOP_RECOVERY = frozenset((
    'replica_record_id',
    'system_recovery_launch_intent',
    'system_recovery_disposition',
    'launch_request_id',
    'service_job_id',
    'candidate_ready_observed_at',
    'ordinary_release_not_before',
    'system_recovery_revision',
    'system_recovery',
    'system_recovery_quarantine',
))
_LEGACY_TOP_FILL_IDENTITY_V13 = frozenset((
    'reserved_fill_pool_key',
    'reserved_fill_service_generation',
    'reserved_fill_physical_cluster_uid',
))
_LEGACY_TOP_FILL_IDENTITY_V14 = frozenset((
    *_LEGACY_TOP_FILL_IDENTITY_V13,
    'reserved_fill_kubernetes_context',
))
_LEGACY_STATUS_BASE = frozenset((
    'sky_launch_status',
    'user_app_failed',
    'service_ready_now',
    'first_ready_time',
    'sky_down_status',
    'is_scale_down',
    'preempted',
    'purged',
    'failed_spot_availability',
    'drain_cap_seconds',
    'wait_for_idle_before_termination',
))
_LEGACY_STATUS_CURRENT = frozenset((
    *_LEGACY_STATUS_BASE,
    'drain_started_at',
    'logical_retirement_version',
    'logical_retirement_controller_epoch',
    'logical_retirement_generation',
    'logical_retirement_target_capacity',
    'logical_retirement_confirmed_generation',
    'logical_retirement_bounded_deadline',
    'logical_retirement_committed',
))
_LEGACY_TOP_CAPACITY_SHAPE = _LEGACY_TOP_BASE | _LEGACY_TOP_CAPACITY
_LEGACY_TOP_ECONOMIC_SHAPE = (_LEGACY_TOP_CAPACITY_SHAPE | _LEGACY_TOP_ECONOMIC)
_LEGACY_TOP_RECOVERY_SHAPE = (_LEGACY_TOP_ECONOMIC_SHAPE | _LEGACY_TOP_RECOVERY)
_LEGACY_CENSUS_CASES = (
    (3, _LEGACY_TOP_BASE, _LEGACY_STATUS_BASE),
    (6, _LEGACY_TOP_CAPACITY_SHAPE, _LEGACY_STATUS_CURRENT),
    (7, _LEGACY_TOP_BASE, _LEGACY_STATUS_BASE),
    (12, _LEGACY_TOP_ECONOMIC_SHAPE, _LEGACY_STATUS_CURRENT),
    (13, _LEGACY_TOP_RECOVERY_SHAPE, _LEGACY_STATUS_CURRENT),
    (13, _LEGACY_TOP_RECOVERY_SHAPE | _LEGACY_TOP_FILL_IDENTITY_V13,
     _LEGACY_STATUS_CURRENT),
    (13, _LEGACY_TOP_RECOVERY_SHAPE | _LEGACY_TOP_FILL_IDENTITY_V14,
     _LEGACY_STATUS_CURRENT),
    (14, _LEGACY_TOP_RECOVERY_SHAPE | _LEGACY_TOP_FILL_IDENTITY_V14,
     _LEGACY_STATUS_CURRENT),
)
_LEGACY_ATTRIBUTION_FIELDS = frozenset((
    'reserved_fill_allocation_generation',
    'reserved_fill_allocation_input_sha256',
    'reserved_fill_allocation_claim_generation',
    'reserved_fill_reconciliation_gate_generation',
    'reserved_fill_reclaim_fleet_bundle_sha256',
    'reserved_fill_reclaim_policy_revision',
    'reserved_fill_reclaim_provider_inventory_sha256',
    'reserved_fill_worker_projection_sha256',
    'reserved_fill_observation_generation',
    'reserved_fill_observation_sequence',
    'reserved_fill_intent_idempotency_key',
    'zero_cost_admission_sequence',
    'zero_cost_materialization_sequence',
))
_CURRENT_TOP_LEVEL_FIELDS = (_LEGACY_TOP_RECOVERY_SHAPE |
                             _LEGACY_TOP_FILL_IDENTITY_V14 |
                             _LEGACY_ATTRIBUTION_FIELDS)
_LEGACY_TOP_LEVEL_DEFAULTS = {
    'planned_capacity': 1,
    'unknown_capacity_replacement': False,
    'logical_bridge_capacity_verified': False,
    'reserved_fill_pool_key': None,
    'reserved_fill_service_generation': None,
    'reserved_fill_physical_cluster_uid': None,
    'reserved_fill_kubernetes_context': None,
    'reserved_fill_allocation_generation': None,
    'reserved_fill_allocation_input_sha256': None,
    'reserved_fill_allocation_claim_generation': None,
    'reserved_fill_reconciliation_gate_generation': None,
    'reserved_fill_reclaim_fleet_bundle_sha256': None,
    'reserved_fill_reclaim_policy_revision': None,
    'reserved_fill_reclaim_provider_inventory_sha256': None,
    'reserved_fill_worker_projection_sha256': None,
    'reserved_fill_observation_generation': None,
    'reserved_fill_observation_sequence': None,
    'reserved_fill_intent_idempotency_key': None,
    'zero_cost_admission_sequence': None,
    'zero_cost_materialization_sequence': None,
    'is_zero_cost': False,
    'paid_capacity_pool_key': None,
}
_LEGACY_STATUS_DEFAULTS = {
    'drain_started_at': None,
    'logical_retirement_version': None,
    'logical_retirement_controller_epoch': None,
    'logical_retirement_generation': None,
    'logical_retirement_target_capacity': None,
    'logical_retirement_confirmed_generation': None,
    'logical_retirement_bounded_deadline': False,
    'logical_retirement_committed': None,
}
_LEGACY_SYSTEM_RECOVERY_DEFAULTS = {
    'system_recovery_launch_intent': None,
    'system_recovery_disposition': 'ORDINARY',
    'launch_request_id': None,
    'service_job_id': None,
    'candidate_ready_observed_at': None,
    'ordinary_release_not_before': None,
    'system_recovery_revision': 0,
    'system_recovery': None,
    'system_recovery_quarantine': None,
}
_EXPECTED_PRE_V13_RECORD_IDS = {
    100: '3a8f71cf-df76-5780-a16c-9776220a1dde',
    101: 'b84a4e3f-455d-52e0-b1b0-87357586e1d1',
    102: 'bce08e9d-b91d-5420-ba8b-1b6002a23296',
    103: '8c14e31e-2b79-53c1-a8fd-22634c96dc78',
}


@pytest.fixture(scope='session')
def pg_server():
    """Expose the PostgreSQL fixture when this file runs standalone."""
    yield from _pool_pg_server.__wrapped__()


def _split_rollout(*,
                   pod_count_per_role: int = 2,
                   writer_count_per_role: int = 2):
    deployments = tuple(
        types.SimpleNamespace(role=role,
                              pod_cohort=tuple(
                                  (f'{role}-{index}', f'{role}-uid-{index}',
                                   f'{index}')
                                  for index in range(pod_count_per_role)))
        for role in ('api', 'controller', 'executor'))
    writer_instances = tuple(
        types.SimpleNamespace(role=role)
        for role in ('api', 'controller', 'executor')
        for _ in range(writer_count_per_role))
    return types.SimpleNamespace(
        image_digest='sha256:' + 'a' * 64,
        deployment_generation='generation',
        deployment_uid='uid',
        deployments=deployments,
        writer_instances=writer_instances,
        pod_inventory_count=3 * pod_count_per_role,
        pod_inventory_sha256='b' * 64,
    )


@pytest.fixture(name='normalization_engine')
def _normalization_engine(request, monkeypatch):
    engine = _isolated_engine(request, 'replica_v18_normalization')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '046')
    monkeypatch.setattr(
        serve_state._db_manager,  # pylint: disable=protected-access
        '_engine',
        engine)

    class _Lock:

        @contextlib.contextmanager
        def acquire(self, blocking):
            assert blocking is True
            yield

    rollout = _split_rollout()
    monkeypatch.setattr(normalization.locks, 'get_lock', lambda _: _Lock())
    monkeypatch.setattr(
        normalization.reserved_capacity_broker,
        '_read_stable_writer_rollout',  # pylint: disable=protected-access
        lambda: rollout)
    return engine


def _replica(replica_id: int) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'service-{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=spot_placer.Location(
                                            cloud=clouds.AWS(),
                                            region='us-east-1',
                                            zone='us-east-1a'),
                                        version=1,
                                        resources_override=None)


def _insert_state(engine: sqlalchemy.engine.Engine,
                  state: dict[str, object],
                  *,
                  replica_state_version: int | None = 1,
                  service_name: str = 'service') -> None:
    replica_id = int(state['replica_id'])
    info = _replica(replica_id)
    values = serve_state._replica_row_values(  # pylint: disable=protected-access
        'service', replica_id, info)
    # Reproduce the pre-precursor dual-write column; live precursor writers no
    # longer emit it and the normalizer clears every retained value.
    values['replica_info'] = pickle.dumps(info)
    values['replica_state_version'] = replica_state_version
    values['replica_state'] = state
    values['service_name'] = service_name
    # The pre-precursor dual writer derived these query columns from the same
    # JSON object. Keep the fixture internally consistent unless a test
    # deliberately denormalizes one scalar afterward.
    for field in ('version', 'cluster_name', 'created_at', 'is_spot',
                  'paid_capacity_pool_key'):
        if field in state:
            values[field] = state[field]
    status_property = state.get('status_property')
    if isinstance(status_property, dict):
        values['sky_down_status'] = status_property.get('sky_down_status')
    with engine.begin() as connection:
        connection.execute(serve_state.replicas_table.insert().values(**values))


def _v17_collision_state(replica_id: int = 7) -> dict[str, object]:
    state = _replica(replica_id).to_storage_dict()
    state['replica_info_version'] = 17
    for field in normalization._ATTRIBUTION_FIELDS:  # pylint: disable=protected-access
        state.pop(field)
    return state


def _legacy_state(replica_id: int, version: int, top_fields: frozenset[str],
                  status_fields: frozenset[str]) -> dict[str, object]:
    state = _replica(replica_id).to_storage_dict()
    state['created_at'] = float(1000 + replica_id)
    state['location'] = {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'zone': None,
        'accelerators': {
            'H200': 1,
        },
        'use_spot': False,
        'image_id': None,
        'container_image': None,
        'disk_tier': None,
        'ephemeral_storage': None,
        'instance_type': None,
    }
    state['resources_override'] = {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'accelerators': {
            'H200': 1,
        },
        'image_id': [[None, 'global-image'], ['phx-context', 'regional-image']],
    }
    state['planned_capacity'] = 4
    state['unknown_capacity_replacement'] = True
    state['logical_bridge_capacity_verified'] = True
    state['reserved_fill'] = True
    state['is_zero_cost'] = True
    state['cost_rebalance_for_replica_id'] = 91
    state['paid_capacity_pool_key'] = 'paid-capacity-pool'
    state['reserved_fill_pool_key'] = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-uid')
    state['reserved_fill_service_generation'] = 7
    state['reserved_fill_physical_cluster_uid'] = 'physical-uid'
    state['reserved_fill_kubernetes_context'] = 'phx-context'
    state['replica_info_version'] = version
    status = state['status_property']
    assert isinstance(status, dict)
    status['drain_started_at'] = 123.5
    status['logical_retirement_version'] = 3
    status['logical_retirement_controller_epoch'] = 'controller-epoch'
    status['logical_retirement_generation'] = 11
    status['logical_retirement_target_capacity'] = 5
    status['logical_retirement_confirmed_generation'] = 10
    status['logical_retirement_bounded_deadline'] = True
    status['logical_retirement_committed'] = True
    state['status_property'] = {
        field: copy.deepcopy(status[field]) for field in status_fields
    }
    return {field: copy.deepcopy(state[field]) for field in top_fields}


def _json_exact(left, right) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return (set(left) == set(right) and
                all(_json_exact(left[key], right[key]) for key in left))
    if isinstance(left, list):
        return (len(left) == len(right) and all(
            _json_exact(left_item, right_item)
            for left_item, right_item in zip(left, right)))
    return left == right


def test_normalizer_rewrites_exact_v17_collision_and_fences_old_writer(
        normalization_engine) -> None:
    collision = _v17_collision_state()
    _insert_state(normalization_engine, collision)

    receipt = normalization.normalize_retained_replica_records()
    assert receipt == {
        'already_current_records': 0,
        'constraint': 'ck_replicas_replica_info_version_18',
        'contract': 'skyserve.replica-info-v18-normalization/v1',
        'invalid_records': 0,
        'remaining_legacy_pickle_records': 0,
        'remaining_noncurrent_records': 0,
        'rewritten_records': 1,
        'scanned_records': 1,
        'scanned_services': 1,
        'schema_version': 18,
        'serve_database_revision': '046',
        'writer_deployment_roles': ['api', 'controller', 'executor'],
        'writer_image_digest': 'sha256:' + 'a' * 64,
        'writer_pod_inventory_count': 6,
        'writer_pod_inventory_sha256': 'b' * 64,
        'writer_process_count': 6,
    }
    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info)).one()
    state = row.replica_state
    assert row.replica_info is None
    expected = copy.deepcopy(collision)
    expected['replica_info_version'] = 18
    for field in normalization._ATTRIBUTION_FIELDS:  # pylint: disable=protected-access
        expected[field] = None
    assert state == expected
    assert replica_info.ReplicaInfo.from_storage_dict(
        copy.deepcopy(state)).to_storage_dict() == state
    grouped = serve_state.get_replica_infos_grouped()
    assert [info.replica_id for info in grouped['service']] == [7]

    second = normalization.normalize_retained_replica_records()
    assert second['already_current_records'] == 1
    assert second['rewritten_records'] == 0

    old_state = copy.deepcopy(state)
    old_state['replica_info_version'] = 17
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with normalization_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    serve_state.replicas_table).values(replica_state=old_state))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with normalization_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state.replicas_table).values(
                    replica_state_version=None))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with normalization_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state.replicas_table).values(
                    replica_info=pickle.dumps(_replica(7))))


def test_normalizer_rewrites_all_eight_observed_legacy_shapes(
        normalization_engine) -> None:
    originals = []
    for offset, (version, top_fields,
                 status_fields) in enumerate(_LEGACY_CENSUS_CASES):
        state = _legacy_state(100 + offset, version, top_fields, status_fields)
        originals.append(state)
        _insert_state(normalization_engine, state)

    receipt = normalization.normalize_retained_replica_records()

    assert receipt['scanned_records'] == 8
    assert receipt['rewritten_records'] == 8
    assert receipt['already_current_records'] == 0
    assert receipt['invalid_records'] == 0
    assert receipt['remaining_noncurrent_records'] == 0
    assert receipt['remaining_legacy_pickle_records'] == 0
    with normalization_engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_id,
                serve_state.replicas_table.c.replica_state,
                serve_state.replicas_table.c.replica_info).order_by(
                    serve_state.replicas_table.c.replica_id)).all()
    assert [row.replica_id for row in rows] == list(range(100, 108))
    for original, row in zip(originals, rows):
        state = row.replica_state
        assert row.replica_info is None
        assert state['replica_info_version'] == 18
        assert set(state) == _CURRENT_TOP_LEVEL_FIELDS
        assert set(state['status_property']) == _LEGACY_STATUS_CURRENT

        for field in set(original) - {
                'replica_info_version', 'status_property'
        }:
            assert _json_exact(state[field], original[field])
        original_status = original['status_property']
        assert isinstance(original_status, dict)
        for field, value in original_status.items():
            assert _json_exact(state['status_property'][field], value)

        expected_additions = {
            field: copy.deepcopy(default)
            for field, default in _LEGACY_TOP_LEVEL_DEFAULTS.items()
            if field not in original
        }
        version = original['replica_info_version']
        assert isinstance(version, int)
        if version < 13:
            expected_additions.update(_LEGACY_SYSTEM_RECOVERY_DEFAULTS)
            expected_additions['replica_record_id'] = (
                _EXPECTED_PRE_V13_RECORD_IDS[row.replica_id])
        actual_additions = {
            field: state[field]
            for field in _CURRENT_TOP_LEVEL_FIELDS - set(original)
        }
        assert _json_exact(actual_additions, expected_additions)
        expected_status_additions = {
            field: default
            for field, default in _LEGACY_STATUS_DEFAULTS.items()
            if field not in original_status
        }
        actual_status_additions = {
            field: state['status_property'][field]
            for field in _LEGACY_STATUS_CURRENT - set(original_status)
        }
        assert _json_exact(actual_status_additions, expected_status_additions)

        restored = replica_info.ReplicaInfo.from_storage_dict(
            copy.deepcopy(state))
        if row.replica_id in (105, 106, 107):
            assert reserved_capacity.parse_protocol_v2_cleanup_fence(
                restored) == reserved_capacity.ProtocolV2CleanupFence(
                    kubernetes_context='phx-context',
                    physical_cluster_uid='physical-uid')

    second = normalization.normalize_retained_replica_records()
    assert second['already_current_records'] == 8
    assert second['rewritten_records'] == 0


def test_normalizer_rejects_legacy_present_field_type_coercion(
        normalization_engine) -> None:
    state = _legacy_state(108, 12, _LEGACY_TOP_ECONOMIC_SHAPE,
                          _LEGACY_STATUS_CURRENT)
    status = state['status_property']
    assert isinstance(status, dict)
    status['service_ready_now'] = 1
    _insert_state(normalization_engine, state)

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='bounded legacy-to-v18 materialization'):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info)).one()
    assert _json_exact(row.replica_state, state)
    assert row.replica_info is not None


def test_normalizer_sanitizes_legacy_delta_derivation_failure(
        normalization_engine) -> None:
    state = _legacy_state(112, 7, _LEGACY_TOP_BASE, _LEGACY_STATUS_BASE)
    _insert_state(normalization_engine, state)
    malformed = copy.deepcopy(state)
    malformed['cluster_name'] = 987654321
    with normalization_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state.replicas_table).values(replica_state=malformed))

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='cannot verify its canonical delta') as exc:
        normalization.normalize_retained_replica_records()

    assert '987654321' not in str(exc.value)
    assert exc.value.__cause__ is None
    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info)).one()
    assert _json_exact(row.replica_state, malformed)
    assert row.replica_info is not None
    assert 'ck_replicas_replica_info_version_18' not in {
        item['name'] for item in sqlalchemy.inspect(
            normalization_engine).get_check_constraints('replicas')
    }


@pytest.mark.parametrize('version', [13, 14])
def test_normalizer_rejects_legacy_recovery_quarantine_delta_atomically(
        normalization_engine, version: int) -> None:
    state = _legacy_state(
        109, version,
        _LEGACY_TOP_RECOVERY_SHAPE | _LEGACY_TOP_FILL_IDENTITY_V14,
        _LEGACY_STATUS_CURRENT)
    state['system_recovery_disposition'] = 'not-a-disposition'
    _insert_state(normalization_engine, state)

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='bounded legacy-to-v18 materialization'):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info)).one()
    assert _json_exact(row.replica_state, state)
    assert row.replica_info is not None


def test_normalizer_preserves_complete_v17_attribution(
        normalization_engine) -> None:
    state = _legacy_state(
        110, 14, _LEGACY_TOP_RECOVERY_SHAPE | _LEGACY_TOP_FILL_IDENTITY_V14,
        _LEGACY_STATUS_CURRENT)
    canonical = replica_info.ReplicaInfo.from_storage_dict(
        state).to_storage_dict()
    canonical.update({
        'replica_info_version': 17,
        'reserved_fill_allocation_generation': 5,
        'reserved_fill_allocation_input_sha256': 'a' * 64,
        'reserved_fill_allocation_claim_generation': 7,
        'reserved_fill_reconciliation_gate_generation': 29,
        'reserved_fill_reclaim_fleet_bundle_sha256': 'c' * 64,
        'reserved_fill_reclaim_policy_revision': 'reclaim-v1',
        'reserved_fill_reclaim_provider_inventory_sha256': 'd' * 64,
        'reserved_fill_worker_projection_sha256': 'e' * 64,
        'reserved_fill_observation_generation': 13,
        'reserved_fill_observation_sequence': 17,
        'reserved_fill_intent_idempotency_key': 'b' * 64,
        'zero_cost_admission_sequence': 19,
        'zero_cost_materialization_sequence': 23,
    })
    _insert_state(normalization_engine, canonical)

    receipt = normalization.normalize_retained_replica_records()

    assert receipt['rewritten_records'] == 1
    with normalization_engine.connect() as connection:
        persisted = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_state)).scalar_one()
    expected = copy.deepcopy(canonical)
    expected['replica_info_version'] = 18
    assert _json_exact(persisted, expected)


def test_normalizer_clears_stale_pickle_from_exact_v18_then_is_idempotent(
        normalization_engine) -> None:
    state = _replica(111).to_storage_dict()
    _insert_state(normalization_engine, state)

    first = normalization.normalize_retained_replica_records()

    assert first['rewritten_records'] == 1
    assert first['already_current_records'] == 0
    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info)).one()
    assert _json_exact(row.replica_state, state)
    assert row.replica_info is None

    second = normalization.normalize_retained_replica_records()
    assert second['rewritten_records'] == 0
    assert second['already_current_records'] == 1


def test_normalizer_rolls_back_unknown_record_version(
        normalization_engine) -> None:
    state = _replica(8).to_storage_dict()
    state['replica_info_version'] = 19
    _insert_state(normalization_engine, state)

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='unsupported ReplicaInfo version'):
        normalization.normalize_retained_replica_records()
    with normalization_engine.connect() as connection:
        persisted = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_state)).scalar_one()
    assert persisted['replica_info_version'] == 19
    constraints = {
        item['name'] for item in sqlalchemy.inspect(
            normalization_engine).get_check_constraints('replicas')
    }
    assert 'ck_replicas_replica_info_version_18' not in constraints


@pytest.mark.parametrize('case', [
    'outer-version',
    'outer-version-null',
    'extra-top-level',
    'missing-top-level',
    'extra-status',
    'missing-status',
    'partial-v17-attribution',
    'coercible-value',
])
def test_normalizer_rolls_back_all_rows_for_every_invalid_shape(
        normalization_engine, case: str) -> None:
    valid = _v17_collision_state(7)
    invalid = _v17_collision_state(8)
    outer_version = 1
    if case == 'outer-version':
        outer_version = 0
    elif case == 'outer-version-null':
        outer_version = None
    elif case == 'extra-top-level':
        invalid['unknown_field'] = 'not-owned'
    elif case == 'missing-top-level':
        invalid.pop('planned_capacity')
    elif case == 'extra-status':
        status = invalid['status_property']
        assert isinstance(status, dict)
        status['unknown_field'] = 'not-owned'
    elif case == 'missing-status':
        status = invalid['status_property']
        assert isinstance(status, dict)
        status.pop('service_ready_now')
    elif case == 'partial-v17-attribution':
        invalid['reserved_fill_allocation_generation'] = None
    else:
        invalid['is_spot'] = 1
    _insert_state(normalization_engine, valid)
    _insert_state(normalization_engine,
                  invalid,
                  replica_state_version=outer_version)

    with pytest.raises(normalization.ReplicaRecordNormalizationError):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_id,
                serve_state.replicas_table.c.replica_state,
                serve_state.replicas_table.c.replica_info).order_by(
                    serve_state.replicas_table.c.replica_id)).all()
    assert [row.replica_state for row in rows] == [valid, invalid]
    assert all(row.replica_info is not None for row in rows)
    constraints = {
        item['name'] for item in sqlalchemy.inspect(
            normalization_engine).get_check_constraints('replicas')
    }
    assert 'ck_replicas_replica_info_version_18' not in constraints


def test_normalizer_requires_postgresql(monkeypatch) -> None:
    engine = sqlalchemy.create_engine('sqlite:///:memory:')

    class _Lock:

        @contextlib.contextmanager
        def acquire(self, blocking):
            assert blocking is True
            yield

    rollout = _split_rollout()
    monkeypatch.setattr(normalization.locks, 'get_lock', lambda _: _Lock())
    monkeypatch.setattr(
        normalization.reserved_capacity_broker,
        '_read_stable_writer_rollout',  # pylint: disable=protected-access
        lambda: rollout)
    monkeypatch.setattr(serve_state, 'get_database_engine', lambda: engine)

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='requires PostgreSQL'):
        normalization.normalize_retained_replica_records()


@pytest.mark.parametrize('column,value', [
    ('status', 'READY'),
    ('paid_capacity_pool_key', 'pool-denormalized'),
])
def test_normalizer_rejects_denormalized_scalar_columns_without_rewriting(
        normalization_engine, column: str, value: str) -> None:
    state = _v17_collision_state()
    _insert_state(normalization_engine, state)
    with normalization_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state.replicas_table).values(**{column: value}))

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match=f'denormalized scalar columns: {column}'):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_state,
                              serve_state.replicas_table.c.replica_info,
                              getattr(serve_state.replicas_table.c,
                                      column))).one()
    assert row.replica_state == state
    assert row.replica_info is not None
    assert row[2] == value


@pytest.mark.parametrize('rollout', [
    types.SimpleNamespace(
        deployments=(types.SimpleNamespace(
            role='api',
            pod_cohort=(('api-0', 'uid-0', '0'), ('api-1', 'uid-1', '1'))),),
        writer_instances=(types.SimpleNamespace(role='all'),),
    ),
    _split_rollout(pod_count_per_role=1),
    _split_rollout(writer_count_per_role=1),
])
def test_normalizer_rejects_non_2x_split_topology_before_database_mutation(
        normalization_engine, monkeypatch, rollout) -> None:
    state = _v17_collision_state()
    _insert_state(normalization_engine, state)
    monkeypatch.setattr(
        normalization.reserved_capacity_broker,
        '_read_stable_writer_rollout',  # pylint: disable=protected-access
        lambda: rollout)

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='exact split API/controller/executor'):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_info,
                serve_state.replicas_table.c.replica_state)).one()
    assert row.replica_info is not None
    assert row.replica_state == state
    assert 'ck_replicas_replica_info_version_18' not in {
        item['name'] for item in sqlalchemy.inspect(
            normalization_engine).get_check_constraints('replicas')
    }


def test_normalizer_requires_exact_database_revision_before_mutation(
        normalization_engine, monkeypatch) -> None:
    state = _v17_collision_state()
    _insert_state(normalization_engine, state)
    monkeypatch.setattr(normalization.migration_utils,
                        'get_current_alembic_revision', lambda *_: '045')

    with pytest.raises(normalization.ReplicaRecordNormalizationError,
                       match='exact Serve database revision 046'):
        normalization.normalize_retained_replica_records()

    with normalization_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state.replicas_table.c.replica_info,
                serve_state.replicas_table.c.replica_state)).one()
    assert row.replica_info is not None
    assert row.replica_state == state
    assert 'ck_replicas_replica_info_version_18' not in {
        item['name'] for item in sqlalchemy.inspect(
            normalization_engine).get_check_constraints('replicas')
    }


def test_normalizer_failure_never_exposes_retained_identifiers_or_payload(
        normalization_engine, capsys) -> None:
    sentinel = 'postgresql://operator:credential-secret@internal/db'
    state = _v17_collision_state()
    resources_override = state['resources_override']
    if resources_override is None:
        resources_override = {}
        state['resources_override'] = resources_override
    assert isinstance(resources_override, dict)
    resources_override['credential'] = sentinel
    state[sentinel] = 'invalid-top-level-field'
    _insert_state(normalization_engine,
                  state,
                  service_name=f'service-{sentinel}')

    with pytest.raises(normalization.ReplicaRecordNormalizationError) as exc:
        normalization.normalize_retained_replica_records()
    captured = capsys.readouterr()
    combined = str(exc.value) + captured.out + captured.err
    assert sentinel not in combined


def test_normalizer_sanitizes_unexpected_database_failure(
        normalization_engine, caplog, capsys) -> None:
    sentinel = 'normalizer-database-failure-identifier-sentinel'
    state = _v17_collision_state()
    state['cluster_name'] = sentinel
    _insert_state(normalization_engine, state)
    with normalization_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'ALTER TABLE replicas ADD CONSTRAINT audit_v17_only '
                "CHECK ((replica_state->>'replica_info_version')::int = 17)"))

    with caplog.at_level(logging.ERROR):
        with pytest.raises(normalization.ReplicaRecordNormalizationError,
                           match='failed unexpectedly') as exc:
            normalization.normalize_retained_replica_records()
    captured = capsys.readouterr()
    combined = str(exc.value) + caplog.text + captured.out + captured.err
    assert sentinel not in combined
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True


@pytest.mark.parametrize('case', ['malformed', 'inconsistent'])
def test_normalizer_recovery_quarantine_never_logs_row_identity_or_payload(
        normalization_engine, caplog, capsys, case: str) -> None:
    sentinel_id = 987654321
    sentinel_payload = 'normalizer-recovery-payload-sentinel'
    state = _v17_collision_state(sentinel_id)
    if case == 'malformed':
        state['system_recovery_launch_intent'] = {
            'raw-secret': sentinel_payload,
        }
    else:
        state['system_recovery_disposition'] = 'CAPABLE'
        state['launch_request_id'] = sentinel_payload
    _insert_state(normalization_engine, state)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(
                normalization.ReplicaRecordNormalizationError) as exc:
            normalization.normalize_retained_replica_records()
    captured = capsys.readouterr()
    combined = str(exc.value) + caplog.text + captured.out + captured.err
    assert str(sentinel_id) not in combined
    assert sentinel_payload not in combined


def test_normalizer_unknown_status_never_logs_row_identity_or_payload(
        normalization_engine, caplog, capsys) -> None:
    sentinel_id = 987654321
    sentinel_payload = 'normalizer-unknown-status-payload-sentinel'
    state = _v17_collision_state(sentinel_id)
    state['cluster_name'] = sentinel_payload
    status = state['status_property']
    assert isinstance(status, dict)
    status['sky_launch_status'] = common_utils.ProcessStatus.RUNNING.value
    status['sky_down_status'] = common_utils.ProcessStatus.SUCCEEDED.value
    _insert_state(normalization_engine, state)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(normalization.ReplicaRecordNormalizationError,
                           match='denormalized scalar columns: status') as exc:
            normalization.normalize_retained_replica_records()
    captured = capsys.readouterr()
    combined = str(exc.value) + caplog.text + captured.out + captured.err
    assert str(sentinel_id) not in combined
    assert sentinel_payload not in combined


def test_main_prints_one_deterministic_receipt_line(monkeypatch,
                                                    capsys) -> None:
    receipt = {
        'contract': 'skyserve.replica-info-v18-normalization/v1',
        'schema_version': 18,
    }
    monkeypatch.setattr(normalization, 'normalize_retained_replica_records',
                        lambda: receipt)
    monkeypatch.setattr(sys, 'argv', ['replica-record-normalization', '--json'])

    normalization.main()

    assert capsys.readouterr().out == (
        json.dumps(receipt, sort_keys=True, separators=(',', ':')) + '\n')
