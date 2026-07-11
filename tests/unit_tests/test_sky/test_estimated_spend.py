"""Tests for the eventually consistent estimated-spend rollup."""

# pylint: disable=protected-access

import importlib
import pickle
from unittest import mock

from alembic import migration
from alembic import operations
import fastapi
import pytest
import sqlalchemy

from sky import estimated_spend
from sky import global_user_state
from sky import models
from sky.backends import cloud_vm_ray_backend
from sky.serve import constants as serve_constants
from sky.server import server
from sky.skylet import constants
from sky.utils.db import db_utils


class _FakeResources:

    def __init__(self, hourly_cost: float, cloud: str = 'AWS'):
        self.hourly_cost = hourly_cost
        self.cloud = cloud
        self.use_spot = False

    def get_cost(self, seconds: int) -> float:
        return self.hourly_cost * seconds / estimated_spend.SECONDS_PER_HOUR


class _MinimalHandle:
    """Just enough cluster state for attribution persistence tests."""

    launched_resources = None
    launched_nodes = 1


def _source(*,
            start: int,
            end,
            hourly_cost: float = 2.0,
            cloud: str = 'AWS',
            num_nodes: int = 1,
            cluster_hash: str = 'hash-1'):
    return {
        'cluster_hash': cluster_hash,
        'name': f'job-cluster-{cluster_hash}',
        'num_nodes': num_nodes,
        'launched_resources': pickle.dumps(
            _FakeResources(hourly_cost, cloud=cloud)),
        'usage_intervals': pickle.dumps([(start, end)]),
        'user_hash': 'user-1',
        'workspace': 'default',
        'cloud': cloud,
        'region': 'us-east-1',
        'is_managed': 1,
        'workload_type': 'managed_job',
        'workload_id': '42',
        'workload_task_id': 0,
    }


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )
    return global_user_state.initialize_and_get_db()


def _insert_source(connection, source, *, usage_updated_at: int):
    connection.execute(
        sqlalchemy.insert(global_user_state.cluster_history_table).values(
            **source,
            requested_resources=pickle.dumps(set()),
            last_activity_time=usage_updated_at,
            launched_at=usage_updated_at - 3600,
            zone='us-east-1a',
            node_names=None,
            usage_updated_at=usage_updated_at,
        ))


def test_split_interval_at_utc_midnight():
    day = 1_700_006_400  # UTC midnight.
    overlaps = estimated_spend._split_interval_by_utc_day(
        day + 23 * 3600, day + 25 * 3600)
    assert overlaps == {
        day: 3600,
        day + estimated_spend.SECONDS_PER_DAY: 3600,
    }


def test_build_rows_uses_machine_uptime_and_node_count():
    day = 1_700_006_400
    rows = estimated_spend._build_daily_rows(_source(start=day + 22 * 3600,
                                                     end=day + 26 * 3600,
                                                     hourly_cost=3.0,
                                                     num_nodes=2),
                                             as_of=day + 30 * 3600,
                                             recompute_start=day,
                                             rate_cache={})

    assert [row['machine_seconds'] for row in rows] == [4 * 3600, 4 * 3600]
    assert sum(row['estimated_cost'] for row in rows) == 24.0
    assert all(row['workload_id'] == '42' for row in rows)


def test_kubernetes_is_excluded_not_zero_priced():
    day = 1_700_006_400
    rows = estimated_spend._build_daily_rows(_source(start=day,
                                                     end=day + 3600,
                                                     hourly_cost=99.0,
                                                     cloud='Kubernetes'),
                                             as_of=day + 3600,
                                             recompute_start=day,
                                             rate_cache={})

    assert len(rows) == 1
    assert rows[0]['estimated_cost'] is None
    assert rows[0]['exclusion_reason'] == 'kubernetes'
    assert rows[0]['machine_seconds'] == 3600


def test_workload_attribution_is_persisted(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_cluster(
        cluster_name='job-cluster-42',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
        is_managed=True,
        workload_type='managed_job',
        workload_id='42',
        workload_task_id=3,
    )

    with engine.connect() as connection:
        active = connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_table)).mappings().one()
        history = connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_history_table)).mappings().one()

    for row in (active, history):
        assert row['workload_type'] == 'managed_job'
        assert row['workload_id'] == '42'
        assert row['workload_task_id'] == 3


def test_managed_job_attribution_uses_launch_env_without_lookup():
    task = mock.MagicMock()
    task.envs = {
        constants.MANAGED_JOB_ID_ENV_VAR: '42',
        constants.TASK_ID_ENV_VAR: '2026-07-10-job-name-42-3',
    }
    attribution = cloud_vm_ray_backend._get_workload_attribution(
        task, 'job-cluster-fallback', 'managed_job')
    assert attribution == ('42', 3)

    regular = cloud_vm_ray_backend._get_workload_attribution(
        task, 'regular-cluster', 'cluster')
    assert regular == ('regular-cluster', None)

    task.envs = {serve_constants.REPLICA_ID_ENV_VAR: '7'}
    pool = cloud_vm_ray_backend._get_workload_attribution(
        task, 'training-pool-7', 'pool')
    assert pool == ('training-pool', None)


def test_changed_rows_with_same_timestamp_make_keyset_progress(
        tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(estimated_spend, 'CHANGED_BATCH_SIZE', 2)
    day = 1_700_006_400
    as_of = day + 2 * estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        for suffix in ('a', 'b', 'c'):
            _insert_source(connection,
                           _source(start=day,
                                   end=day + 3600,
                                   cluster_hash=f'hash-{suffix}'),
                           usage_updated_at=as_of - 1)
        connection.execute(
            sqlalchemy.insert(
                global_user_state.estimated_spend_state_table).values(
                    singleton_id=1, backfill_complete=True))

    assert estimated_spend.run_rollup_once(now=as_of)['rows_processed'] == 2
    assert estimated_spend.run_rollup_once(now=as_of)['rows_processed'] == 1
    assert estimated_spend.run_rollup_once(now=as_of)['rows_processed'] == 0

    with engine.connect() as connection:
        count = connection.scalar(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 global_user_state.estimated_spend_daily_table))
    assert count == 3


def test_active_rows_rotate_through_bounded_batches(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(estimated_spend, 'ACTIVE_ROW_LIMIT', 2)
    day = 1_700_006_400
    as_of = day + estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        for suffix in ('a', 'b', 'c'):
            cluster_hash = f'hash-{suffix}'
            _insert_source(connection,
                           _source(start=day,
                                   end=None,
                                   cluster_hash=cluster_hash),
                           usage_updated_at=0)
            connection.execute(
                sqlalchemy.insert(global_user_state.cluster_table).values(
                    name=f'active-{suffix}',
                    cluster_hash=cluster_hash,
                    status='UP'))
        connection.execute(
            sqlalchemy.insert(
                global_user_state.estimated_spend_state_table).values(
                    singleton_id=1,
                    source_watermark=as_of - 1,
                    source_watermark_hash='\uffff',
                    backfill_complete=True))

    assert estimated_spend.run_rollup_once(now=as_of)['rows_processed'] == 2
    assert estimated_spend.run_rollup_once(now=as_of)['rows_processed'] == 1

    with engine.connect() as connection:
        cluster_hashes = set(
            connection.scalars(
                sqlalchemy.select(global_user_state.estimated_spend_daily_table.
                                  c.cluster_hash)))
    assert cluster_hashes == {'hash-a', 'hash-b', 'hash-c'}


def test_old_rollup_rows_are_pruned_in_bounded_batches(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(estimated_spend, 'PRUNE_BATCH_SIZE', 1)
    daily = global_user_state.estimated_spend_daily_table
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(daily), [{
            'day_start_utc': 100,
            'cluster_hash': 'old-a',
            'cluster_name': 'old-a',
            'workload_type': 'cluster',
            'machine_seconds': 10,
            'updated_at': 100,
        }, {
            'day_start_utc': 100,
            'cluster_hash': 'old-b',
            'cluster_name': 'old-b',
            'workload_type': 'cluster',
            'machine_seconds': 10,
            'updated_at': 100,
        }])

    assert estimated_spend._prune_old_rows(engine, cutoff=200) == 1
    assert estimated_spend._prune_old_rows(engine, cutoff=200) == 1
    assert estimated_spend._prune_old_rows(engine, cutoff=200) == 0


def test_estimated_spend_endpoint_rejects_non_admin():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='user-1', name='User')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['user']), mock.patch.object(
                               server.estimated_spend_lib,
                               'get_estimated_spend') as get_estimate:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server.estimated_spend(request, days=30)

    assert exc_info.value.status_code == 403
    get_estimate.assert_not_called()


def test_estimated_spend_endpoint_serves_admin_snapshot():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='admin-1', name='Admin')
    expected = {'as_of': 123, 'days': []}
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['admin']), mock.patch.object(
                               server.estimated_spend_lib,
                               'get_estimated_spend',
                               return_value=expected) as get_estimate:
        response = server.estimated_spend(request, days=7)

    assert response == expected
    get_estimate.assert_called_once_with(days=7)


def test_schema_021_upgrades_existing_sqlite_database(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "old.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'clusters', old_metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True))
    sqlalchemy.Table(
        'cluster_history', old_metadata,
        sqlalchemy.Column('cluster_hash', sqlalchemy.Text, primary_key=True))
    old_metadata.create_all(engine)

    schema_021 = importlib.import_module(
        'sky.schemas.db.global_user_state.021_estimated_spend')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            schema_021.upgrade()

    inspector = sqlalchemy.inspect(engine)
    cluster_columns = {
        column['name'] for column in inspector.get_columns('clusters')
    }
    history_columns = {
        column['name'] for column in inspector.get_columns('cluster_history')
    }
    history_indexes = {
        index['name'] for index in inspector.get_indexes('cluster_history')
    }
    assert {'workload_type', 'workload_id', 'workload_task_id'
           } <= cluster_columns
    assert {
        'workload_type', 'workload_id', 'workload_task_id', 'usage_updated_at'
    } <= history_columns
    assert 'ix_cluster_history_usage_updated_at' in history_indexes
    assert 'estimated_spend_daily' in inspector.get_table_names()
    assert 'estimated_spend_state' in inspector.get_table_names()


def test_rollup_is_idempotent_and_query_returns_daily_breakdown(
        tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    as_of = day + 2 * estimated_spend.SECONDS_PER_DAY
    source = _source(start=day + 22 * 3600,
                     end=day + 26 * 3600,
                     hourly_cost=3.0,
                     num_nodes=2)
    history = global_user_state.cluster_history_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(history).values(
                **source,
                requested_resources=pickle.dumps(set()),
                last_activity_time=day + 26 * 3600,
                launched_at=day + 22 * 3600,
                zone='us-east-1a',
                node_names=None,
                usage_updated_at=as_of - 1,
            ))

    first = estimated_spend.run_rollup_once(now=as_of)
    second = estimated_spend.run_rollup_once(now=as_of)
    assert first['rows_processed'] == 1
    assert second['rows_processed'] == 0

    monkeypatch.setattr(estimated_spend.time, 'time', lambda: as_of)
    response = estimated_spend.get_estimated_spend(days=3)
    assert response['stale'] is False
    assert response['backfill_complete'] is True
    assert response['totals']['estimated_cost'] == 24.0
    assert response['totals']['priced_machine_seconds'] == 8 * 3600
    assert [row['estimated_cost'] for row in response['days']
           ] == [12.0, 12.0, 0.0]
    assert response['workloads'][0]['workload_type'] == 'managed_job'
    assert response['workloads'][0]['workload_id'] == '42'
