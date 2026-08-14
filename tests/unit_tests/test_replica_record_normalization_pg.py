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
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils.db import migration_utils


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
