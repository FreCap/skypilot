"""Tests for the eventually consistent estimated-spend rollup."""

# pylint: disable=protected-access

import datetime
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


class _ZonalFakeResources(_FakeResources):
    """Priced resource whose display repr intentionally omits placement."""

    def __init__(self, hourly_cost: float, region: str, zone: str | None):
        super().__init__(hourly_cost, use_spot=True)
        self.region = region
        self.zone = zone

    def __repr__(self) -> str:
        return '_ZonalFakeResources(g6.4xlarge[Spot])'


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


def _insert_drilldown_daily(
    connection,
    *,
    day: int,
    cluster_hash: str,
    cluster_name: str,
    cost: float,
    user_hash: str | None,
    workload_type: str,
    workload_id: str | None,
    workload_task_id: int | None = None,
    use_spot: bool = False,
    workspace: str = 'default',
):
    connection.execute(
        sqlalchemy.insert(global_user_state.estimated_spend_daily_table).values(
            day_start_utc=day,
            cluster_hash=cluster_hash,
            cluster_name=cluster_name,
            workload_type=workload_type,
            workload_id=workload_id,
            workload_task_id=workload_task_id,
            user_hash=user_hash,
            workspace=workspace,
            cloud='AWS',
            use_spot=use_spot,
            machine_seconds=3600,
            catalog_hourly_rate=cost,
            estimated_cost=cost,
            updated_at=day + 3600,
        ))


def _utc_date(day_start: int) -> datetime.date:
    return datetime.datetime.fromtimestamp(day_start,
                                           tz=datetime.timezone.utc).date()


def test_split_interval_at_utc_midnight():
    day = 1_700_006_400  # UTC midnight.
    overlaps = estimated_spend._split_interval_by_utc_day(
        day + 23 * 3600, day + 25 * 3600)
    assert overlaps == {
        day: 3600,
        day + estimated_spend.SECONDS_PER_DAY: 3600,
    }


def test_cost_projection_facade_and_row_shape():
    assert estimated_spend.estimate_hourly_cost.__module__ == (
        'sky.estimated_spend')
    assert pickle.loads(pickle.dumps(estimated_spend.estimate_hourly_cost)) is (
        estimated_spend.estimate_hourly_cost)

    day = 1_700_006_400
    as_of = day + 30 * 3600
    rows = estimated_spend._build_daily_rows(_source(start=day + 22 * 3600,
                                                     end=day + 26 * 3600,
                                                     hourly_cost=3.0,
                                                     num_nodes=2,
                                                     use_spot=True),
                                             as_of=as_of,
                                             recompute_start=day,
                                             rate_cache={})

    assert rows == [{
        'day_start_utc': day,
        'cluster_hash': 'hash-1',
        'cluster_name': 'job-cluster-hash-1',
        'workload_type': 'managed_job',
        'workload_id': '42',
        'workload_task_id': 0,
        'user_hash': 'user-1',
        'workspace': 'default',
        'cloud': 'AWS',
        'region': 'us-east-1',
        'use_spot': True,
        'num_nodes': 2,
        'machine_seconds': 4 * 3600,
        'catalog_hourly_rate': 6.0,
        'estimated_cost': 12.0,
        'exclusion_reason': None,
        'priced_at': as_of,
        'updated_at': as_of,
    }, {
        'day_start_utc': day + estimated_spend.SECONDS_PER_DAY,
        'cluster_hash': 'hash-1',
        'cluster_name': 'job-cluster-hash-1',
        'workload_type': 'managed_job',
        'workload_id': '42',
        'workload_task_id': 0,
        'user_hash': 'user-1',
        'workspace': 'default',
        'cloud': 'AWS',
        'region': 'us-east-1',
        'use_spot': True,
        'num_nodes': 2,
        'machine_seconds': 4 * 3600,
        'catalog_hourly_rate': 6.0,
        'estimated_cost': 12.0,
        'exclusion_reason': None,
        'priced_at': as_of,
        'updated_at': as_of,
    }]


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
    expected_cost = _FakeResources(3.0).get_cost(4 * 3600) * 2
    assert sum(row['estimated_cost'] for row in rows) == expected_cost == 24.0
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


def test_estimate_hourly_cost_uses_actual_purchase_option_and_nodes():
    spot_resources = _FakeResources(1.25, use_spot=True)
    assert estimated_spend.estimate_hourly_cost(spot_resources,
                                                2) == (2.5, None)

    kubernetes_resources = _FakeResources(99.0, cloud='Kubernetes')
    assert estimated_spend.estimate_hourly_cost(kubernetes_resources) == (
        None, 'kubernetes')


def test_estimate_hourly_cost_cache_distinguishes_region_and_zone():
    rate_cache = {}
    resources = [
        _ZonalFakeResources(0.3, 'ap-south-1', 'ap-south-1a'),
        _ZonalFakeResources(0.3189, 'ap-south-1', 'ap-south-1b'),
        _ZonalFakeResources(0.6899, 'eu-west-2', None),
        _ZonalFakeResources(0.5161, 'ap-northeast-2', None),
    ]

    hourly_costs = [
        estimated_spend.estimate_hourly_cost(resource, rate_cache=rate_cache)[0]
        for resource in resources
    ]

    assert hourly_costs == [0.3, 0.3189, 0.6899, 0.5161]
    assert len(rate_cache) == 4


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
    start_date = datetime.date(2026, 7, 12)
    end_date = datetime.date(2026, 7, 13)
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['admin']), mock.patch.object(
                               server.estimated_spend_lib,
                               'get_estimated_spend',
                               return_value=expected) as get_estimate:
        response = server.estimated_spend(request,
                                          days=7,
                                          group_by=estimated_spend.GroupBy.USER,
                                          start_date=start_date,
                                          end_date=end_date)

    assert response == expected
    get_estimate.assert_called_once_with(days=7,
                                         group_by=estimated_spend.GroupBy.USER,
                                         start_date=start_date,
                                         end_date=end_date)


def test_estimated_spend_endpoint_rejects_invalid_date_range():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='admin-1', name='Admin')
    with mock.patch.object(
            server.permission.permission_service,
            'get_user_roles',
            return_value=['admin']), mock.patch.object(
                server.estimated_spend_lib,
                'get_estimated_spend',
                side_effect=estimated_spend.InvalidDateRangeError(
                    'invalid UTC range')):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server.estimated_spend(request,
                                   start_date=datetime.date(2026, 7, 13),
                                   end_date=datetime.date(2026, 7, 12))

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == 'invalid UTC range'


def test_estimated_spend_drilldown_endpoint_serves_admin_page():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='admin-1', name='Admin')
    expected = {'level': 'owner', 'rows': [], 'total': 0}
    start_date = datetime.date(2026, 7, 12)
    end_date = datetime.date(2026, 7, 13)
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['admin']), mock.patch.object(
                               server.estimated_spend_lib,
                               'get_estimated_spend_drilldown',
                               return_value=expected) as get_drilldown:
        response = server.estimated_spend_drilldown(
            request,
            level=estimated_spend.SpendDrilldownLevel.OWNER,
            days=7,
            start_date=start_date,
            end_date=end_date,
            offset=2,
            limit=25)

    assert response == expected
    get_drilldown.assert_called_once_with(
        level=estimated_spend.SpendDrilldownLevel.OWNER,
        days=7,
        start_date=start_date,
        end_date=end_date,
        owner_user_hash=None,
        owner_unknown=False,
        workload_type=None,
        workload_id=None,
        workload_task_id=None,
        offset=2,
        limit=25)


def test_estimated_spend_drilldown_endpoint_rejects_non_admin():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='user-1', name='User')
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['user']), mock.patch.object(
                               server.estimated_spend_lib,
                               'get_estimated_spend_drilldown') as get_page:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server.estimated_spend_drilldown(
                request, level=estimated_spend.SpendDrilldownLevel.OWNER)

    assert exc_info.value.status_code == 403
    get_page.assert_not_called()


def test_estimated_spend_drilldown_endpoint_rejects_invalid_scope():
    request = mock.MagicMock()
    request.state.auth_user = models.User(id='admin-1', name='Admin')
    with mock.patch.object(
            server.permission.permission_service,
            'get_user_roles',
            return_value=['admin']), mock.patch.object(
                server.estimated_spend_lib,
                'get_estimated_spend_drilldown',
                side_effect=estimated_spend.InvalidDrilldownScopeError(
                    'invalid hierarchy scope')):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server.estimated_spend_drilldown(
                request, level=estimated_spend.SpendDrilldownLevel.WORKLOAD)

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == 'invalid hierarchy scope'


def test_spend_drilldown_hierarchy_and_pagination(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    as_of = day + estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(global_user_state.user_table), [{
            'id': 'alice',
            'name': 'Alice',
        }, {
            'id': 'bob',
            'name': 'Bob',
        }])
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='job-42-task-0',
                                cluster_name='job-42-recovery-a',
                                cost=4,
                                user_hash='alice',
                                workload_type='managed_job',
                                workload_id='42',
                                workload_task_id=0,
                                use_spot=True)
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='job-42-task-1',
                                cluster_name='job-42-recovery-b',
                                cost=6,
                                user_hash='alice',
                                workload_type='managed_job',
                                workload_id='42',
                                workload_task_id=1)
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='legacy-a',
                                cluster_name='legacy-managed-a',
                                cost=3,
                                user_hash='alice',
                                workload_type='managed',
                                workload_id='legacy-managed-a')
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='legacy-b',
                                cluster_name='legacy-managed-b',
                                cost=2,
                                user_hash='alice',
                                workload_type='managed',
                                workload_id='legacy-managed-b')
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='standalone',
                                cluster_name='standalone',
                                cost=1,
                                user_hash='alice',
                                workload_type='cluster',
                                workload_id='standalone')
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='unknown-owner',
                                cluster_name='unknown-owner',
                                cost=7,
                                user_hash=None,
                                workload_type='cluster',
                                workload_id='unknown-owner')
        _insert_drilldown_daily(connection,
                                day=day,
                                cluster_hash='bob-cluster',
                                cluster_name='bob-cluster',
                                cost=2,
                                user_hash='bob',
                                workload_type='cluster',
                                workload_id='bob-cluster')
        for task_id, cluster_hash in ((0, 'bob-job-task-0'), (1,
                                                              'bob-job-task-1'),
                                      (None, 'bob-job-task-unknown')):
            _insert_drilldown_daily(connection,
                                    day=day,
                                    cluster_hash=cluster_hash,
                                    cluster_name=cluster_hash,
                                    cost=1,
                                    user_hash='bob',
                                    workload_type='managed_job',
                                    workload_id='99',
                                    workload_task_id=task_id)
        _insert_drilldown_daily(connection,
                                day=day - estimated_spend.SECONDS_PER_DAY,
                                cluster_hash='outside-range',
                                cluster_name='outside-range',
                                cost=100,
                                user_hash='alice',
                                workload_type='cluster',
                                workload_id='outside-range')

    monkeypatch.setattr(estimated_spend.time, 'time', lambda: as_of)
    selected_date = _utc_date(day)
    owner_page = estimated_spend.get_estimated_spend_drilldown(
        'owner', start_date=selected_date, end_date=selected_date, limit=1)
    assert owner_page['total'] == 3
    assert owner_page['has_more'] is True
    assert owner_page['rows'] == [{
        'user_hash': 'alice',
        'user_name': 'Alice',
        'estimated_cost': 16.0,
        'spot_estimated_cost': 4.0,
        'on_demand_estimated_cost': 12.0,
        'priced_machine_seconds': 5 * 3600,
        'excluded_machine_seconds': 0,
        'workload_count': 3,
        'cluster_count': 5,
        'owner_unknown': False,
    }]
    unknown_owner = estimated_spend.get_estimated_spend_drilldown(
        'owner',
        start_date=selected_date,
        end_date=selected_date,
        offset=1,
        limit=1)['rows'][0]
    assert unknown_owner['owner_unknown'] is True
    assert unknown_owner['estimated_cost'] == 7
    unknown_workloads = estimated_spend.get_estimated_spend_drilldown(
        'workload',
        start_date=selected_date,
        end_date=selected_date,
        owner_unknown=True)
    assert unknown_workloads['total'] == 1
    assert unknown_workloads['rows'][0]['workload_id'] == 'unknown-owner'
    assert unknown_workloads['rows'][0]['estimated_cost'] == 7

    workloads = estimated_spend.get_estimated_spend_drilldown(
        'workload',
        start_date=selected_date,
        end_date=selected_date,
        owner_user_hash='alice',
        limit=100)
    assert workloads['total'] == 3
    assert sum(row['estimated_cost'] for row in workloads['rows']) == 16
    by_workload_type = {row['workload_type']: row for row in workloads['rows']}
    managed_job = by_workload_type['managed_job']
    assert managed_job['workload_id'] == '42'
    assert managed_job['estimated_cost'] == 10
    assert managed_job['task_count'] == 2
    assert managed_job['cluster_count'] == 2
    legacy = by_workload_type['managed_unattributed']
    assert legacy['workload_id'] is None
    assert legacy['estimated_cost'] == 5
    assert legacy['cluster_count'] == 2
    assert legacy['unknown_task_cluster_count'] == 2
    flat_workloads = estimated_spend.get_estimated_spend(
        start_date=selected_date, end_date=selected_date,
        group_by='job')['groups']
    flat_legacy = [
        row for row in flat_workloads
        if row['workload_type'] == 'managed_unattributed'
    ]
    assert len(flat_legacy) == 1
    assert flat_legacy[0]['workload_id'] is None
    assert flat_legacy[0]['estimated_cost'] == 5

    bob_workloads = estimated_spend.get_estimated_spend_drilldown(
        'workload',
        start_date=selected_date,
        end_date=selected_date,
        owner_user_hash='bob',
        limit=100)['rows']
    bob_managed_job = next(
        row for row in bob_workloads if row['workload_type'] == 'managed_job')
    assert bob_managed_job['task_count'] == 2
    assert bob_managed_job['unknown_task_cluster_count'] == 1
    assert bob_managed_job['cluster_count'] == 3

    tasks = estimated_spend.get_estimated_spend_drilldown(
        'task',
        start_date=selected_date,
        end_date=selected_date,
        owner_user_hash='alice',
        workload_type='managed_job',
        workload_id='42')
    assert [row['workload_task_id'] for row in tasks['rows']] == [0, 1]
    assert [row['estimated_cost'] for row in tasks['rows']] == [4, 6]
    assert sum(row['estimated_cost'] for row in tasks['rows']) == (
        managed_job['estimated_cost'])

    task_attempts = estimated_spend.get_estimated_spend_drilldown(
        'cluster',
        start_date=selected_date,
        end_date=selected_date,
        owner_user_hash='alice',
        workload_type='managed_job',
        workload_id='42',
        workload_task_id=1)
    assert task_attempts['rows'][0]['cluster_name'] == 'job-42-recovery-b'
    assert task_attempts['rows'][0]['estimated_cost'] == 6

    legacy_attempts = estimated_spend.get_estimated_spend_drilldown(
        'cluster',
        start_date=selected_date,
        end_date=selected_date,
        owner_user_hash='alice',
        workload_type='managed_unattributed',
        limit=100)
    assert {row['cluster_name'] for row in legacy_attempts['rows']
           } == {'legacy-managed-a', 'legacy-managed-b'}
    assert sum(row['estimated_cost']
               for row in legacy_attempts['rows']) == legacy['estimated_cost']


@pytest.mark.parametrize(('kwargs', 'message'), [
    ({
        'level': 'workload',
    }, 'require owner'),
    ({
        'level': 'owner',
        'owner_unknown': True,
    }, 'does not accept'),
    ({
        'level': 'workload',
        'owner_unknown': True,
        'owner_user_hash': 'alice',
    }, 'not both'),
    ({
        'level': 'cluster',
        'owner_user_hash': 'alice',
        'workload_type': 'managed_job',
    }, 'require workload_id'),
    ({
        'level': 'cluster',
        'owner_user_hash': 'alice',
        'workload_type': 'managed_unattributed',
        'workload_id': 'invented',
    }, 'must not include workload_id'),
    ({
        'level': 'owner',
        'limit': 101,
    }, 'limit must be'),
])
def test_spend_drilldown_rejects_invalid_scopes(tmp_path, monkeypatch, kwargs,
                                                message):
    _fresh_db(tmp_path, monkeypatch)
    with pytest.raises(estimated_spend.InvalidDrilldownScopeError,
                       match=message):
        estimated_spend.get_estimated_spend_drilldown(**kwargs)


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
    assert response['service_requests'] == {
        'available': False,
        'definition': 'admitted_inbound_requests',
        'coverage_start_utc': None,
        'total_request_count': 0,
        'services': [],
        'series': [],
    }
    assert response['stale'] is False
    assert response['backfill_complete'] is True
    assert response['totals']['estimated_cost'] == 24.0
    assert response['totals']['priced_machine_seconds'] == 8 * 3600
    assert [row['estimated_cost'] for row in response['days']
           ] == [12.0, 12.0, 0.0]
    assert response['workloads'][0]['workload_type'] == 'managed_job'
    assert response['workloads'][0]['workload_id'] == '42'


def test_exact_date_range_filters_all_aggregates(tmp_path, monkeypatch):
    engine = _fresh_db(tmp_path, monkeypatch)
    day = 1_700_006_400
    current_day = day + 2 * estimated_spend.SECONDS_PER_DAY
    with engine.begin() as connection:
        _insert_daily(connection,
                      day=day,
                      cluster_hash='before',
                      cost=1.0,
                      use_spot=True,
                      workload_id='before')
        _insert_daily(connection,
                      day=day + estimated_spend.SECONDS_PER_DAY,
                      cluster_hash='selected',
                      cost=2.5,
                      use_spot=False,
                      workload_id='selected')
        _insert_daily(connection,
                      day=current_day,
                      cluster_hash='after',
                      cost=4.0,
                      use_spot=True,
                      workload_id='after')

    monkeypatch.setattr(estimated_spend.time, 'time',
                        lambda: current_day + 3600)
    selected_date = _utc_date(day + estimated_spend.SECONDS_PER_DAY)
    response = estimated_spend.get_estimated_spend(days=30,
                                                   group_by='job',
                                                   start_date=selected_date,
                                                   end_date=selected_date)

    assert response['start_date'] == selected_date.isoformat()
    assert response['end_date'] == selected_date.isoformat()
    assert response['requested_days'] == 1
    assert response['totals']['estimated_cost'] == 2.5
    assert response['totals']['priced_machine_seconds'] == 3600
    assert [row['estimated_cost'] for row in response['days']] == [2.5]
    assert [row['workload_id'] for row in response['workloads']] == ['selected']
    assert response['clouds'][0]['estimated_cost'] == 2.5
    assert [row['workload_id'] for row in response['groups']] == ['selected']
    assert response['series'] == [{
        'workload_type': 'managed_job',
        'workload_id': 'selected',
        'estimated_cost_by_day': [2.5],
    }]

    inclusive_response = estimated_spend.get_estimated_spend(
        start_date=_utc_date(day), end_date=selected_date)
    assert inclusive_response['requested_days'] == 2
    assert inclusive_response['totals']['estimated_cost'] == 3.5
    assert [row['estimated_cost'] for row in inclusive_response['days']
           ] == [1.0, 2.5]


def test_service_cost_per_request_aligns_complete_request_days():
    day = 1_700_006_400
    days = [{
        'day_start_utc': day
    }, {
        'day_start_utc': day + estimated_spend.SECONDS_PER_DAY
    }, {
        'day_start_utc': day + 2 * estimated_spend.SECONDS_PER_DAY
    }]
    service_requests = {
        'available': True,
        'coverage_start_utc': day + 60,
        'services': [
            {
                'service_name': 'complete',
                'request_count': 21,
            },
            {
                'service_name': 'partial',
                'request_count': 3,
            },
            {
                'service_name': 'unavailable',
                'request_count': 2,
            },
            {
                'service_name': 'zero-cost',
                'request_count': 5,
            },
        ],
        'series': [{
            'service_name': 'complete',
            'request_count_by_day': [1, 4, 16],
        }, {
            'service_name': 'partial',
            'request_count_by_day': [0, 1, 2],
        }, {
            'service_name': 'unavailable',
            'request_count_by_day': [0, 1, 1],
        }, {
            'service_name': 'zero-cost',
            'request_count_by_day': [0, 2, 3],
        }, {
            'is_other': True,
            'request_count_by_day': [3, 0, 0],
        }],
    }
    request_rows = [
        mock.Mock(service_name='complete',
                  day_start=datetime.datetime.fromtimestamp(
                      day + offset * estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=count)
        for offset, count in enumerate((1, 4, 16))
    ]
    request_rows.extend([
        mock.Mock(service_name='partial',
                  day_start=datetime.datetime.fromtimestamp(
                      day + estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=1),
        mock.Mock(service_name='partial',
                  day_start=datetime.datetime.fromtimestamp(
                      day + 2 * estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=2),
        mock.Mock(service_name='unavailable',
                  day_start=datetime.datetime.fromtimestamp(
                      day + estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=1),
        mock.Mock(service_name='unavailable',
                  day_start=datetime.datetime.fromtimestamp(
                      day + 2 * estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=1),
        mock.Mock(service_name='zero-cost',
                  day_start=datetime.datetime.fromtimestamp(
                      day + estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=2),
        mock.Mock(service_name='zero-cost',
                  day_start=datetime.datetime.fromtimestamp(
                      day + 2 * estimated_spend.SECONDS_PER_DAY,
                      datetime.timezone.utc),
                  request_count=3),
    ])
    cost_rows = [
        mock.Mock(service_name='complete',
                  day_start_utc=day + offset * estimated_spend.SECONDS_PER_DAY,
                  estimated_cost=cost,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=0)
        for offset, cost in enumerate((10.0, 8.0, 12.0))
    ]
    cost_rows.extend([
        mock.Mock(service_name='partial',
                  day_start_utc=day + estimated_spend.SECONDS_PER_DAY,
                  estimated_cost=2.0,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=0),
        mock.Mock(service_name='partial',
                  day_start_utc=day + 2 * estimated_spend.SECONDS_PER_DAY,
                  estimated_cost=3.0,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=1800),
        mock.Mock(service_name='zero-cost',
                  day_start_utc=day + estimated_spend.SECONDS_PER_DAY,
                  estimated_cost=0.0,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=0),
        mock.Mock(service_name='zero-cost',
                  day_start_utc=day + 2 * estimated_spend.SECONDS_PER_DAY,
                  estimated_cost=0.0,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=0),
    ])

    estimated_spend._enrich_service_requests_with_costs(service_requests, days,
                                                        request_rows, cost_rows,
                                                        day)

    services = {
        service['service_name']: service
        for service in service_requests['services']
    }
    complete = services['complete']
    assert complete['ratio_coverage_start_utc'] == (
        day + estimated_spend.SECONDS_PER_DAY)
    assert complete['ratio_request_count'] == 20
    assert complete['estimated_cost'] == 20.0
    assert complete['estimated_cost_per_request'] == 1.0
    assert complete['cost_coverage'] == 'complete'

    partial = services['partial']
    assert partial['ratio_request_count'] == 3
    assert partial['estimated_cost'] == 5.0
    assert partial['estimated_cost_per_request'] is None
    assert partial['cost_coverage'] == 'partial'
    assert partial['excluded_machine_seconds'] == 1800

    unavailable = services['unavailable']
    assert unavailable['ratio_request_count'] == 2
    assert unavailable['estimated_cost_per_request'] is None
    assert unavailable['cost_coverage'] == 'unavailable'

    zero_cost = services['zero-cost']
    assert zero_cost['ratio_request_count'] == 5
    assert zero_cost['estimated_cost'] == 0
    assert zero_cost['estimated_cost_per_request'] == 0
    assert zero_cost['cost_coverage'] == 'complete'

    complete_series = service_requests['series'][0]
    assert complete_series['estimated_cost_by_day'] == [10.0, 8.0, 12.0]
    assert complete_series['estimated_cost_per_request_by_day'] == [
        None, 2.0, 0.75
    ]
    zero_cost_series = service_requests['series'][3]
    assert zero_cost_series['estimated_cost_per_request_by_day'] == [
        None, 0.0, 0.0
    ]
    assert 'estimated_cost_by_day' not in service_requests['series'][-1]


def test_service_cost_per_request_includes_midnight_coverage_day():
    day = 1_700_006_400
    assert estimated_spend._first_complete_coverage_day(day) == day
    assert estimated_spend._first_complete_coverage_day(day + 1) == (
        day + estimated_spend.SECONDS_PER_DAY)
    assert estimated_spend._first_complete_coverage_day(None) is None


def test_service_cost_per_request_waits_for_spend_backfill():
    day = 1_700_006_400
    service_requests = {
        'available': True,
        'coverage_start_utc': day,
        'services': [{
            'service_name': 'service',
            'request_count': 2,
        }],
        'series': [{
            'service_name': 'service',
            'request_count_by_day': [2],
        }],
    }
    request_rows = [
        mock.Mock(service_name='service',
                  day_start=datetime.datetime.fromtimestamp(
                      day, datetime.timezone.utc),
                  request_count=2)
    ]
    cost_rows = [
        mock.Mock(service_name='service',
                  day_start_utc=day,
                  estimated_cost=4.0,
                  priced_machine_seconds=3600,
                  excluded_machine_seconds=0)
    ]

    estimated_spend._enrich_service_requests_with_costs(
        service_requests, [{
            'day_start_utc': day
        }],
        request_rows,
        cost_rows,
        spend_coverage_start_utc=None)

    service = service_requests['services'][0]
    assert service['ratio_coverage_start_utc'] is None
    assert service['ratio_request_count'] == 0
    assert service['estimated_cost_per_request'] is None
    assert service['cost_coverage'] == 'unavailable'
    assert service_requests['series'][0][
        'estimated_cost_per_request_by_day'] == [None]


@pytest.mark.parametrize(('start_offset', 'end_offset', 'message'), [
    (None, 0, 'provided together'),
    (0, None, 'provided together'),
    (0, -1, 'on or before'),
    (-90, -89, 'within the last 90'),
    (0, 1, 'cannot be after'),
])
def test_exact_date_range_validation(start_offset, end_offset, message):
    current_day = 1_700_006_400
    current_date = _utc_date(current_day)
    start_date = (None if start_offset is None else current_date +
                  datetime.timedelta(days=start_offset))
    end_date = (None if end_offset is None else current_date +
                datetime.timedelta(days=end_offset))

    with pytest.raises(estimated_spend.InvalidDateRangeError, match=message):
        estimated_spend._resolve_query_range(days=30,
                                             start_date=start_date,
                                             end_date=end_date,
                                             now=current_day + 3600)


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
