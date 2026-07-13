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
from sky.serve import serve_utils
from sky.server import server
from sky.skylet import constants
from sky.utils.db import db_utils


class _FakeResources:
    """Minimal priced resource used by rollup tests."""

    def __init__(self,
                 hourly_cost: float,
                 cloud: str = 'AWS',
                 use_spot: bool = False):
        self.hourly_cost = hourly_cost
        self.cloud = cloud
        self.use_spot = use_spot

    def get_cost(self, seconds: int) -> float:
        return self.hourly_cost * seconds / estimated_spend.SECONDS_PER_HOUR

    def __repr__(self) -> str:
        return (f'_FakeResources(hourly_cost={self.hourly_cost!r}, '
                f'cloud={self.cloud!r}, use_spot={self.use_spot!r})')


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
            cluster_hash: str = 'hash-1',
            use_spot: bool = False,
            user_hash: str = 'user-1',
            workload_type: str = 'managed_job',
            workload_id: str = '42'):
    return {
        'cluster_hash': cluster_hash,
        'name': f'job-cluster-{cluster_hash}',
        'num_nodes': num_nodes,
        'launched_resources': pickle.dumps(
            _FakeResources(hourly_cost, cloud=cloud, use_spot=use_spot)),
        'usage_intervals': pickle.dumps([(start, end)]),
        'user_hash': user_hash,
        'workspace': 'default',
        'cloud': cloud,
        'region': 'us-east-1',
        'is_managed': 1,
        'workload_type': workload_type,
        'workload_id': workload_id,
        'workload_task_id': 0,
    }


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    manager = db_utils.DatabaseManager(
        'state',
        global_user_state.create_table,
        post_init_fn=lambda _: global_user_state._sqlite_supports_returning(),
    )
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        manager.get_engine)
    return manager.get_engine()


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


def _insert_daily(connection,
                  *,
                  day: int,
                  cluster_hash: str,
                  cost: float,
                  use_spot: bool,
                  user_hash=None,
                  workload_id: str = '42'):
    connection.execute(
        sqlalchemy.insert(global_user_state.estimated_spend_daily_table).values(
            day_start_utc=day,
            cluster_hash=cluster_hash,
            cluster_name=f'cluster-{cluster_hash}',
            workload_type='managed_job',
            workload_id=workload_id,
            user_hash=user_hash,
            cloud='AWS',
            use_spot=use_spot,
            machine_seconds=3600,
            catalog_hourly_rate=cost,
            estimated_cost=cost,
            updated_at=day + 3600,
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


def test_corrupt_resource_pickle_does_not_block_rollup(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    as_of = day + estimated_spend.SECONDS_PER_DAY
    corrupt = _source(start=day, end=day + 3600, cluster_hash='corrupt')
    corrupt['launched_resources'] = b'cmissing_resource_module\nResources\n.'
    healthy = _source(start=day,
                      end=day + 3600,
                      hourly_cost=3.0,
                      cluster_hash='healthy')
    with engine.begin() as connection:
        _insert_source(connection, corrupt, usage_updated_at=as_of - 1)
        _insert_source(connection, healthy, usage_updated_at=as_of - 1)

    result = estimated_spend.run_rollup_once(now=as_of)

    assert result['rows_processed'] == 2
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(global_user_state.estimated_spend_daily_table)
        ).mappings().all()
        state = connection.execute(
            sqlalchemy.select(global_user_state.estimated_spend_state_table)
        ).mappings().one()
    rows_by_hash = {row['cluster_hash']: row for row in rows}
    assert rows_by_hash['corrupt']['exclusion_reason'] == 'unknown_price'
    assert rows_by_hash['corrupt']['estimated_cost'] is None
    assert rows_by_hash['healthy']['estimated_cost'] == 3.0
    assert state['last_success_at'] == as_of


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


def test_workload_attribution_uses_launch_metadata_without_lookup():
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

    service_name = 'inference-service-with-a-name-that-can-be-truncated'
    scoped_cluster_name = serve_utils.generate_replica_cluster_name(
        service_name, 7, resource_scope='service-incarnation-hash')
    assert scoped_cluster_name != f'{service_name}-7'
    scoped_service = cloud_vm_ray_backend._get_workload_attribution(
        task, scoped_cluster_name, 'service', {
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: service_name,
        })
    assert scoped_service == (service_name, None)


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
        response = server.estimated_spend(request,
                                          days=7,
                                          group_by=estimated_spend.GroupBy.USER)

    assert response == expected
    get_estimate.assert_called_once_with(days=7,
                                         group_by=estimated_spend.GroupBy.USER)


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
    assert {'workload_type', 'workload_id',
            'workload_task_id'} <= cluster_columns
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


def test_job_breakdown_splits_spot_and_on_demand_cost(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    as_of = day + 2 * estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        _insert_source(connection,
                       _source(start=day,
                               end=day + 3600,
                               hourly_cost=2.0,
                               cluster_hash='spot-recovery',
                               use_spot=True),
                       usage_updated_at=as_of - 1)
        _insert_source(connection,
                       _source(start=day,
                               end=day + 3600,
                               hourly_cost=3.0,
                               cluster_hash='on-demand-recovery'),
                       usage_updated_at=as_of - 1)

    estimated_spend.run_rollup_once(now=as_of)
    monkeypatch.setattr(estimated_spend.time, 'time', lambda: as_of)
    response = estimated_spend.get_estimated_spend(days=3, group_by='job')

    group = response['groups'][0]
    assert group['workload_id'] == '42'
    assert group['estimated_cost'] == 5.0
    assert group['spot_estimated_cost'] == 2.0
    assert group['on_demand_estimated_cost'] == 3.0
    assert response['series'][0]['estimated_cost_by_day'] == [5.0, 0.0, 0.0]


def test_service_breakdown_combines_spot_and_on_demand_replicas(
        tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    as_of = day + 2 * estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        _insert_source(connection,
                       _source(start=day,
                               end=day + 3600,
                               hourly_cost=1.5,
                               cluster_hash='service-spot-replica',
                               use_spot=True,
                               workload_type='service',
                               workload_id='inference-service'),
                       usage_updated_at=as_of - 1)
        _insert_source(connection,
                       _source(start=day,
                               end=day + 3600,
                               hourly_cost=4.0,
                               cluster_hash='service-demand-replica',
                               workload_type='service',
                               workload_id='inference-service'),
                       usage_updated_at=as_of - 1)

    estimated_spend.run_rollup_once(now=as_of)
    monkeypatch.setattr(estimated_spend.time, 'time', lambda: as_of)
    response = estimated_spend.get_estimated_spend(days=3, group_by='job')

    group = response['groups'][0]
    assert group['workload_type'] == 'service'
    assert group['workload_id'] == 'inference-service'
    assert group['estimated_cost'] == 5.5
    assert group['spot_estimated_cost'] == 1.5
    assert group['on_demand_estimated_cost'] == 4.0
    assert response['series'][0] == {
        'workload_type': 'service',
        'workload_id': 'inference-service',
        'estimated_cost_by_day': [5.5, 0.0, 0.0],
    }


def test_user_breakdown_resolves_names_and_keeps_unknown_owner(
        tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state.user_table).values(
                id='user-1', name='Alice'))
        _insert_daily(connection,
                      day=day,
                      cluster_hash='alice-spot',
                      cost=4.0,
                      use_spot=True,
                      user_hash='user-1')
        _insert_daily(connection,
                      day=day,
                      cluster_hash='unknown-demand',
                      cost=6.0,
                      use_spot=False)

    monkeypatch.setattr(estimated_spend.time, 'time', lambda: day + 3600)
    response = estimated_spend.get_estimated_spend(days=1, group_by='user')
    groups = {group['user_hash']: group for group in response['groups']}

    assert groups['user-1']['user_name'] == 'Alice'
    assert groups['user-1']['spot_estimated_cost'] == 4.0
    assert groups[None]['user_name'] is None
    assert groups[None]['on_demand_estimated_cost'] == 6.0


def test_purchase_option_breakdown_returns_stacked_series(
        tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    with engine.begin() as connection:
        _insert_daily(connection,
                      day=day,
                      cluster_hash='spot',
                      cost=2.5,
                      use_spot=True)
        _insert_daily(connection,
                      day=day,
                      cluster_hash='demand',
                      cost=7.5,
                      use_spot=False)

    monkeypatch.setattr(estimated_spend.time, 'time', lambda: day + 3600)
    response = estimated_spend.get_estimated_spend(
        days=1, group_by=estimated_spend.GroupBy.PURCHASE_OPTION)
    groups = {group['purchase_option']: group for group in response['groups']}
    series = {
        row['purchase_option']: row['estimated_cost_by_day']
        for row in response['series']
    }

    assert groups['spot']['estimated_cost'] == 2.5
    assert groups['on_demand']['estimated_cost'] == 7.5
    assert series == {'on_demand': [7.5], 'spot': [2.5]}


def test_job_chart_bounds_series_and_combines_other(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    with engine.begin() as connection:
        for index in range(10):
            _insert_daily(connection,
                          day=day,
                          cluster_hash=f'cluster-{index}',
                          cost=float(10 - index),
                          use_spot=index % 2 == 0,
                          workload_id=str(index))

    monkeypatch.setattr(estimated_spend.time, 'time', lambda: day + 3600)
    response = estimated_spend.get_estimated_spend(days=1, group_by='job')

    assert len(response['groups']) == 10
    assert len(response['series']) == estimated_spend.GROUP_CHART_LIMIT + 1
    assert response['series'][-1] == {
        'is_other': True,
        'estimated_cost_by_day': [3.0],
    }


def test_invalid_group_by_is_rejected_before_querying():
    with pytest.raises(ValueError, match='group_by must be one of'):
        estimated_spend.get_estimated_spend(group_by='cloud')
