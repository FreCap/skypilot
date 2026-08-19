"""PostgreSQL contracts for durable provider-free SkyServe routes."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import concurrent.futures
import datetime
import threading
import time
import types
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import demand_state_schema
from sky.serve import lb_cutover_state
from sky.serve import lb_ha
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_route_projection_schema_051_pg')


@pytest.fixture
def route_database(empty_postgres):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '051')
    incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                status='READY',
                hash='svc-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                lifecycle_epoch=3,
                controller_incarnation=incarnation,
                controller_owner_epoch=4,
                controller_pid=123,
                controller_ip='10.0.0.5'))
    return empty_postgres, incarnation


def _wait_until_blocked_by(engine, blocked_pid, blocker_pid):
    deadline = time.monotonic() + 10
    while True:
        with engine.connect() as connection:
            blocking_pids = connection.execute(
                sqlalchemy.select(sqlalchemy.func.pg_blocking_pids(
                    blocked_pid))).scalar_one()
        if blocker_pid in blocking_pids:
            return
        if time.monotonic() >= deadline:
            pytest.fail(f'backend {blocked_pid} did not block on '
                        f'backend {blocker_pid}')
        threading.Event().wait(timeout=0.01)


def _identity(incarnation):
    return route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        controller_incarnation=incarnation,
        controller_owner_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.5')


def test_route_promotion_owner_reads_exact_postgres_state(
        route_database, monkeypatch):
    engine, incarnation = route_database
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)

    owner = serve_state.get_service_controller_owner(
        'svc', include_lb_state=True, include_route_owner_state=True)

    assert owner is not None
    assert owner['current_version'] == 1
    assert owner['controller_incarnation'] == incarnation
    assert owner['controller_owner_epoch'] == 4
    assert owner['route_source_mode'] == 'LEGACY_PROXY'
    assert owner['route_source_epoch'] == 0
    assert owner['route_projection_capable'] is False
    assert owner['route_projection_controller_incarnation'] is None
    assert owner['route_projection_protocol_version'] is None


def _insert_replica(engine,
                    record_id,
                    *,
                    replica_id=1,
                    version=1,
                    status='READY'):
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=replica_id,
                replica_state_version=18,
                replica_state={'replica_record_id': record_id},
                status=status,
                version=version,
                cluster_name=f'svc-{replica_id}'))


def _material(url='http://10.0.0.1:8000'):
    return route_projection.RouteLeaseMaterial(
        route=route_projection.ResolvedRouteMaterial(url, 'L4', 1),
        readiness_path='/health',
        probe_timeout_seconds=15,
        post_data=None,
        headers={'X-Probe': 'serve'},
        async_occupancy=True,
        uses_logical_replicas=False,
        is_zero_cost=False,
        planned_capacity=1,
        route_allowed=True,
        requires_route_marker=False)


def _response(url='http://10.0.0.1:8000'):
    return {
        'replica_info': {
            url: {
                'gpu_type': 'L4',
                'gpu_count': '1',
            }
        },
        'num_ready_replicas': 1,
        'routing_spec': {
            'load_balancing_policy_name': 'round_robin'
        },
        'capacity_hint': {
            'replica_unit': 'physical_backend'
        },
        'request_history_accepted': False,
        'request_classification_history_accepted': False,
        'response_time_history_accepted': False,
        'prediction_time_history_accepted': False,
        'queued_compatibility_demand_supported': True,
        'service_version': 1,
    }


def _identity_payload(record_id, url='http://10.0.0.1:8000'):
    return {
        url: {
            'replica_id': 1,
            'replica_record_id': record_id,
            'gpu_type': 'L4',
            'gpu_count': 1,
            'advertised': True,
            'alias_expires_at': None,
        }
    }


def _insert_report(engine, session_id='pod-a'):
    now = datetime.datetime.now(datetime.timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                demand_state_schema.serve_lb_demand_reports_table).values(
                    service_name='svc',
                    service_hash='svc-hash',
                    reporter_session_id='process-a',
                    lb_session_id=session_id,
                    lb_slot='a',
                    protocol_version=1,
                    sequence=1,
                    routing_version=1,
                    reporter_observed_at=now,
                    received_at=now,
                    valid_until=now + datetime.timedelta(seconds=15),
                    payload_sha256='a' * 64,
                    complete=True,
                    payload={}))


def _compose_empty_incremental(repository, incarnation):
    return repository.compose_incremental_snapshot(
        _identity(incarnation),
        1, {'load_balancing_policy_name': 'round_robin'},
        lambda _version, _state: None,
        lambda _infos, _translation, _logical_versions:
        {'replica_unit': 'physical_backend'},
        ttl_seconds=60)


def _enable_projected_contract(route_database, *, response=None):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    projected_response = response or _response()
    record_id = str(uuid.uuid4())
    route_url = next(iter(projected_response['replica_info']))
    receipt = repository.publish(_identity(incarnation),
                                 1,
                                 projected_response,
                                 _identity_payload(record_id, route_url),
                                 {record_id},
                                 ttl_seconds=60)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_source_mode='DURABLE_PROJECTED',
                    route_source_epoch=1,
                    route_projection_capable=True,
                    route_projection_controller_incarnation=incarnation,
                    route_projection_protocol_version=1))
    return repository, incarnation, projected_response, receipt


def test_revocation_hooks_noop_before_serve051(empty_postgres):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '050')
    with sqlalchemy.orm.Session(empty_postgres) as session, session.begin():
        assert route_projection.revoke_replica_lease_in_session(
            session, 'svc', 1, str(uuid.uuid4()), 'historical_schema') == 0
        assert route_projection.revoke_service_leases_in_session(
            session, 'svc', 'historical_schema') == 0


def test_serve049_schema_is_postgresql_only_and_complete(route_database):
    engine, _ = route_database
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table(
        route_projection_schema.serve_route_snapshots_table.name)
    assert inspector.has_table(
        route_projection_schema.serve_route_heads_table.name)
    head_fks = inspector.get_foreign_keys('serve_route_heads')
    assert any(foreign_key['referred_table'] == 'serve_route_snapshots'
               for foreign_key in head_fks)
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    assert {
        'route_source_mode', 'route_source_epoch', 'route_projection_capable',
        'route_projection_controller_incarnation',
        'route_projection_protocol_version'
    } <= service_columns
    assert inspector.has_table(
        route_projection_schema.serve_route_replica_leases_table.name)
    assert inspector.has_table('serve_paid_replica_retirements')


def test_route_producer_selection_reads_exact_current_owner(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    identity = _identity(incarnation)

    # A legacy-proxy service must start the protocol-2 producer dark.  These
    # fields are deliberately absent from get_service_from_name()'s Serve037
    # compatibility projection, so bootstrap must read this canonical owner.
    assert repository.current_owner_uses_incremental_producer(identity)

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_source_mode='DURABLE_PROJECTED',
                    route_source_epoch=1,
                    route_projection_capable=True,
                    route_projection_controller_incarnation=incarnation,
                    route_projection_protocol_version=1))
    assert not repository.current_owner_uses_incremental_producer(identity)

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_projection_protocol_version=2))
    assert repository.current_owner_uses_incremental_producer(identity)

    stale = route_projection.RoutePublisherIdentity(
        service_name=identity.service_name,
        service_hash=identity.service_hash,
        service_lifecycle_epoch=identity.service_lifecycle_epoch,
        controller_incarnation=identity.controller_incarnation,
        controller_owner_epoch=identity.controller_owner_epoch + 1,
        controller_pid=identity.controller_pid,
        controller_ip=identity.controller_ip)
    with pytest.raises(route_projection.RouteProjectionConflict,
                       match='no longer owns'):
        repository.current_owner_uses_incremental_producer(stale)


def test_incremental_material_is_idempotent_and_probe_is_generation_fenced(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    _insert_replica(engine, record_id)
    info = type('Info', (), {
        'replica_id': 1,
        'replica_record_id': record_id,
        'version': 1,
    })()

    first = repository.upsert_replica_material(_identity(incarnation), info,
                                               _material())
    duplicate = repository.upsert_replica_material(_identity(incarnation), info,
                                                   _material())
    old_target = repository.list_probe_targets(_identity(incarnation))[0]
    changed = repository.upsert_replica_material(
        _identity(incarnation), info, _material('http://10.0.0.2:8000'))
    new_target = repository.list_probe_targets(_identity(incarnation))[0]

    assert first.material_generation == duplicate.material_generation == 1
    assert duplicate.duplicate is True
    assert changed.material_generation == 2
    assert repository.record_probe_result(old_target, True,
                                          ttl_seconds=60).accepted is False
    accepted = repository.record_probe_result(new_target, True, ttl_seconds=60)
    assert accepted.accepted is True
    assert accepted.readiness_generation == 1
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
    assert row['route_url'] == 'http://10.0.0.2:8000'
    assert row['ready'] is True


def test_probe_result_batch_isolates_stale_sibling(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    first_record_id = str(uuid.uuid4())
    second_record_id = str(uuid.uuid4())
    _insert_replica(engine, first_record_id, replica_id=1)
    _insert_replica(engine, second_record_id, replica_id=2)
    first_info = types.SimpleNamespace(replica_id=1,
                                       replica_record_id=first_record_id,
                                       version=1)
    second_info = types.SimpleNamespace(replica_id=2,
                                        replica_record_id=second_record_id,
                                        version=1)
    identity = _identity(incarnation)
    repository.upsert_replica_material(identity, first_info, _material())
    repository.upsert_replica_material(identity, second_info,
                                       _material('http://10.0.0.2:8000'))
    targets = repository.list_probe_targets(identity)
    stale_first = targets[0]
    current_second = targets[1]
    repository.upsert_replica_material(identity, first_info,
                                       _material('http://10.0.0.3:8000'))

    receipts = repository.record_probe_results([
        route_projection.RouteLeaseProbeResult(stale_first, True),
        route_projection.RouteLeaseProbeResult(current_second, True),
    ],
                                               ttl_seconds=60)

    assert [receipt.accepted for receipt in receipts] == [False, True]
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table.c.
                replica_id,
                route_projection_schema.serve_route_replica_leases_table.c.ready
            ).order_by(route_projection_schema.serve_route_replica_leases_table.
                       c.replica_id)).all()
    assert rows == [(1, False), (2, True)]


def test_material_batch_has_fixed_statement_count_and_joined_target_read(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    identity = _identity(incarnation)
    entries = []
    for replica_id in range(1, 4):
        record_id = str(uuid.uuid4())
        _insert_replica(engine, record_id, replica_id=replica_id)
        entries.append((
            types.SimpleNamespace(replica_id=replica_id,
                                  replica_record_id=record_id,
                                  version=1),
            _material(f'http://10.0.0.{replica_id}:8000'),
        ))

    statements = []

    def _record_statement(*args):
        del args
        statements.append(None)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _record_statement)
    try:
        receipts = repository.upsert_replica_materials(identity, entries)
        material_statement_count = len(statements)
        statements.clear()
        targets = repository.list_probe_targets(identity)
        target_statement_count = len(statements)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _record_statement)

    assert len(receipts) == 3
    assert [receipt.material_generation for receipt in receipts] == [1, 1, 1]
    # The material transaction is owner, replicas, histories, sibling revoke,
    # bulk upsert, and ranked history prune regardless of fleet batch size.
    assert material_statement_count == 6
    # Probe targets are one owner fence and one lease/current-replica join.
    assert target_statement_count == 2
    assert [target.replica_id for target in targets] == [1, 2, 3]


def test_material_batch_isolates_revoked_and_stale_siblings(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    identity = _identity(incarnation)
    record_ids = [str(uuid.uuid4()) for _ in range(3)]
    infos = []
    for replica_id, record_id in enumerate(record_ids, start=1):
        _insert_replica(engine, record_id, replica_id=replica_id)
        infos.append(
            types.SimpleNamespace(replica_id=replica_id,
                                  replica_record_id=record_id,
                                  version=1))
    repository.upsert_replica_materials(
        identity, [(info, _material(f'http://10.0.0.{info.replica_id}:8000'))
                   for info in infos])
    assert repository.revoke_replica(identity, 1, record_ids[0],
                                     'scale_down_admitted') == 1
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 2).values(
                    status='SHUTTING_DOWN'))

    receipts = repository.upsert_replica_materials(identity, [
        (infos[0], _material('http://10.0.1.1:8000')),
        (infos[1], _material('http://10.0.1.2:8000')),
        (infos[2], _material('http://10.0.1.3:8000')),
    ])

    assert len(receipts) == 1
    assert receipts[0].material_generation == 2
    targets = repository.list_probe_targets(identity)
    assert [target.replica_id for target in targets] == [3]
    assert targets[0].route_url == 'http://10.0.1.3:8000'


def test_material_batch_serializes_replica_replacement(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    old_record_id = str(uuid.uuid4())
    new_record_id = str(uuid.uuid4())
    _insert_replica(engine, old_record_id)
    old_info = types.SimpleNamespace(replica_id=1,
                                     replica_record_id=old_record_id,
                                     version=1)

    blocker = engine.connect()
    transaction = blocker.begin()
    replica_lock_attempted = threading.Event()

    def _observe_lock_attempt(_connection, _cursor, statement, *_args):
        if 'FROM replicas' in statement and 'FOR UPDATE' in statement:
            replica_lock_attempted.set()

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _observe_lock_attempt)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        blocker.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 1).values(
                    replica_state={'replica_record_id': new_record_id}))
        future = executor.submit(repository.upsert_replica_materials,
                                 _identity(incarnation),
                                 [(old_info, _material())])
        assert replica_lock_attempted.wait(timeout=5)
        assert not future.done()
        transaction.commit()
        assert future.result(timeout=5) == []
    finally:
        if transaction.is_active:
            transaction.rollback()
        executor.shutdown(wait=True)
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _observe_lock_attempt)
        blocker.close()

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                route_projection_schema.serve_route_replica_leases_table)
        ).scalar_one() == 0


def test_revocation_rejects_delayed_probe_and_implicit_revival(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    _insert_replica(engine, record_id)
    info = type('Info', (), {
        'replica_id': 1,
        'replica_record_id': record_id,
        'version': 1,
    })()
    repository.upsert_replica_material(_identity(incarnation), info,
                                       _material())
    target = repository.list_probe_targets(_identity(incarnation))[0]

    assert repository.revoke_replica(_identity(incarnation), 1, record_id,
                                     'scale_down_admitted') == 1
    assert repository.record_probe_result(target, True,
                                          ttl_seconds=60).accepted is False
    assert not repository.list_probe_targets(_identity(incarnation))
    with pytest.raises(route_projection.RouteProjectionConflict,
                       match='cannot be implicitly revived'):
        repository.upsert_replica_material(_identity(incarnation), info,
                                           _material())


def test_delayed_ready_probe_cannot_reactivate_off_route_replica(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    _insert_replica(engine, record_id)
    info = type('Info', (), {
        'replica_id': 1,
        'replica_record_id': record_id,
        'version': 1,
    })()
    repository.upsert_replica_material(_identity(incarnation), info,
                                       _material())
    target = repository.list_probe_targets(_identity(incarnation))[0]
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 1).values(
                    status='SHUTTING_DOWN'))

    assert repository.record_probe_result(target, True,
                                          ttl_seconds=60).accepted is False
    with engine.connect() as connection:
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
    assert lease['ready'] is False
    assert lease['valid_until'] is None


def test_reused_numeric_id_revokes_old_record_and_isolates_late_result(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    old_record_id = str(uuid.uuid4())
    new_record_id = str(uuid.uuid4())
    _insert_replica(engine, old_record_id)
    old_info = type('Info', (), {
        'replica_id': 1,
        'replica_record_id': old_record_id,
        'version': 1,
    })()
    repository.upsert_replica_material(_identity(incarnation), old_info,
                                       _material())
    old_target = repository.list_probe_targets(_identity(incarnation))[0]
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 1).values(
                    replica_state={'replica_record_id': new_record_id}))
    new_info = type('Info', (), {
        'replica_id': 1,
        'replica_record_id': new_record_id,
        'version': 1,
    })()
    repository.upsert_replica_material(_identity(incarnation), new_info,
                                       _material())

    assert repository.record_probe_result(old_target, True,
                                          ttl_seconds=60).accepted is False
    targets = repository.list_probe_targets(_identity(incarnation))
    assert [target.replica_record_id for target in targets] == [new_record_id]
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table.c.
                replica_record_id, route_projection_schema.
                serve_route_replica_leases_table.c.revocation_reason)).all()
    reasons = {str(row_record_id): reason for row_record_id, reason in rows}
    assert reasons[old_record_id] == 'replica_record_replaced'
    assert reasons[new_record_id] is None


def test_publish_refresh_promote_and_provider_free_read(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    identity_payload = _identity_payload(record_id)

    first = repository.publish(_identity(incarnation),
                               1,
                               _response(),
                               identity_payload, {record_id},
                               ttl_seconds=60)
    duplicate = repository.publish(_identity(incarnation),
                                   1,
                                   _response(),
                                   identity_payload, {record_id},
                                   ttl_seconds=60)

    assert first.generation == duplicate.generation == 1
    assert first.duplicate is False
    assert duplicate.duplicate is True
    legacy = repository.resolve_sync('svc', 'svc-hash', None)
    assert legacy.mode == route_projection.RouteSourceMode.LEGACY_PROXY
    with pytest.raises(route_projection.RouteProjectionUnavailable,
                       match='capability'):
        repository.promote('svc', 'svc-hash')

    _insert_replica(engine, record_id)
    info = types.SimpleNamespace(replica_id=1,
                                 replica_record_id=record_id,
                                 version=1,
                                 status=types.SimpleNamespace(name='READY'))
    repository.upsert_replica_material(_identity(incarnation), info,
                                       _material())
    target = repository.list_probe_targets(_identity(incarnation))[0]
    assert repository.record_probe_result(target, True, ttl_seconds=60).accepted

    def _decode(_, state):
        return types.SimpleNamespace(
            replica_id=1,
            replica_record_id=state['replica_record_id'],
            version=1)

    composed = repository.compose_incremental_snapshot(
        _identity(incarnation),
        1, {'load_balancing_policy_name': 'round_robin'},
        _decode,
        lambda infos, translation, logical_versions: {
            'replica_unit': 'physical_backend',
            'decoded': len(infos),
            'translated': len(translation),
            'logical_versions': len(logical_versions),
        },
        ttl_seconds=60)

    _insert_report(engine)
    assert repository.promote('svc', 'svc-hash') == 1
    projected = repository.resolve_sync('svc', 'svc-hash', 'pod-a')
    assert projected.mode == route_projection.RouteSourceMode.DURABLE_PROJECTED
    assert projected.response is not None
    assert composed.generation == 2
    assert (projected.response['route_projection_generation'] ==
            composed.generation)
    assert projected.response['route_projection_sha256'] != first.content_sha256
    assert projected.response['route_source_epoch'] == 1
    assert set(projected.response['replica_info']) == {'http://10.0.0.1:8000'}
    assert projected.response['capacity_hint']['decoded'] == 1


def test_route_promotion_waits_for_and_rejects_nonstable_ha_cutover(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    _compose_empty_incremental(repository, incarnation)
    blocker = engine.connect()
    transaction = blocker.begin()
    promotion_thread_ids = set()
    promotion_lock_attempted = threading.Event()
    promotion_backend_pid = {}

    def _record_promotion_statement(_connection, cursor, statement, *_args):
        if (threading.get_ident() in promotion_thread_ids and
                'FROM services' in statement and 'FOR UPDATE' in statement):
            promotion_backend_pid['value'] = cursor.connection.get_backend_pid()
            promotion_lock_attempted.set()

    def _promote():
        promotion_thread_ids.add(threading.get_ident())
        return repository.promote('svc', 'svc-hash')

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _record_promotion_statement)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        blocker_pid = blocker.execute(
            sqlalchemy.select(sqlalchemy.func.pg_backend_pid())).scalar_one()
        blocker.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot=lb_ha.LbSlot.A.value,
                    lb_cutover_generation=2,
                    lb_pending_slot=lb_ha.LbSlot.B.value,
                    lb_cutover_phase=lb_ha.LbCutoverPhase.PREPARING.value))
        promotion = executor.submit(_promote)
        assert promotion_lock_attempted.wait(timeout=5)
        _wait_until_blocked_by(engine, promotion_backend_pid['value'],
                               blocker_pid)
        assert not promotion.done()

        transaction.commit()
        with pytest.raises(route_projection.RouteProjectionUnavailable,
                           match='stable HA cutover'):
            promotion.result(timeout=5)
    finally:
        if transaction.is_active:
            transaction.rollback()
        executor.shutdown(wait=True)
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _record_promotion_statement)
        blocker.close()

    with engine.connect() as connection:
        mode = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.route_source_mode).where(
                    serve_state_schema.services_table.c.name ==
                    'svc')).scalar_one()
    assert mode == route_projection.RouteSourceMode.LEGACY_PROXY.value


def test_route_promotion_rejects_malformed_ha_enablement(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    _compose_empty_incremental(repository, incarnation)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=2))

    with pytest.raises(route_projection.RouteProjectionCorruption,
                       match='HA enablement'):
        repository.promote('svc', 'svc-hash')


def test_promotion_contract_reads_exact_fresh_snapshot(route_database):
    response = _response()
    route_url = next(iter(response['replica_info']))
    response['replica_info'][route_url]['async_occupancy'] = 'true'
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database, response=response)

    statements = []

    def _record_statement(_connection, _cursor, statement, *_args):
        statements.append(statement)

    sqlalchemy.event.listen(route_database[0], 'before_cursor_execute',
                            _record_statement)
    try:
        decision = repository.resolve_promotion_contract(
            _identity(incarnation), 1, 1, receipt.generation,
            receipt.content_sha256)
    finally:
        sqlalchemy.event.remove(route_database[0], 'before_cursor_execute',
                                _record_statement)

    assert decision.mode == route_projection.RouteSourceMode.DURABLE_PROJECTED
    assert decision.contract == route_projection.RoutePromotionContract(
        service_version=1,
        route_source_epoch=1,
        generation=receipt.generation,
        content_sha256=receipt.content_sha256,
        routing_urls=frozenset({route_url}),
        async_occupancy_urls=frozenset({route_url}))
    assert len(statements) == 2
    assert 'JOIN serve_route_snapshots' in statements[1]


def test_legacy_promotion_allows_committed_version_ahead_of_applied(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    current_version=2))

    statements = []

    def _record_statement(_connection, _cursor, statement, *_args):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _record_statement)
    try:
        decision = repository.resolve_promotion_contract(
            _identity(incarnation), 1, None, None, None)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _record_statement)

    assert decision == route_projection.RoutePromotionDecision(
        mode=route_projection.RouteSourceMode.LEGACY_PROXY)
    assert len(statements) == 1


def test_promotion_contract_shared_lock_is_scoped_to_multi_read(route_database):
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database)
    engine, _ = route_database
    blocker = engine.connect()
    transaction = blocker.begin()
    resolver_thread_ids = set()
    publisher_thread_ids = set()
    reader_reached_head = threading.Event()
    release_reader = threading.Event()
    publisher_lock_attempted = threading.Event()
    resolver_backend_pid = {}
    publisher_backend_pid = {}

    def _coordinate_statements(_connection, cursor, statement, *_args):
        thread_id = threading.get_ident()
        if (thread_id in resolver_thread_ids and
                'FROM serve_route_heads' in statement and
                not reader_reached_head.is_set()):
            resolver_backend_pid['value'] = cursor.connection.get_backend_pid()
            reader_reached_head.set()
            assert release_reader.wait(timeout=10)
        if (thread_id in publisher_thread_ids and
                'FROM services' in statement and 'FOR UPDATE' in statement):
            publisher_backend_pid['value'] = cursor.connection.get_backend_pid()
            publisher_lock_attempted.set()

    def _resolve():
        resolver_thread_ids.add(threading.get_ident())
        return repository.resolve_promotion_contract(_identity(incarnation), 1,
                                                     1, receipt.generation,
                                                     receipt.content_sha256)

    next_url = 'http://10.0.0.2:8000'
    next_record_id = str(uuid.uuid4())

    def _publish():
        publisher_thread_ids.add(threading.get_ident())
        return repository.publish(_identity(incarnation),
                                  1,
                                  _response(next_url),
                                  _identity_payload(next_record_id, next_url),
                                  {next_record_id},
                                  ttl_seconds=60)

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _coordinate_statements)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    try:
        blocker.execute(
            sqlalchemy.select(serve_state_schema.services_table.c.name).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).scalar_one()

        # Producer selection is only an owner read, so an unrelated writer
        # must not block it under PostgreSQL MVCC.
        producer = executor.submit(
            repository.current_owner_uses_incremental_producer,
            _identity(incarnation))
        assert producer.result(timeout=5) is False
        transaction.commit()

        resolution = executor.submit(_resolve)
        assert reader_reached_head.wait(timeout=5)
        publication = executor.submit(_publish)
        assert publisher_lock_attempted.wait(timeout=5)
        _wait_until_blocked_by(engine, publisher_backend_pid['value'],
                               resolver_backend_pid['value'])
        assert not publication.done()

        release_reader.set()
        decision = resolution.result(timeout=5)
        assert decision.mode == route_projection.RouteSourceMode.DURABLE_PROJECTED
        assert decision.contract is not None
        assert decision.contract.generation == receipt.generation
        assert decision.contract.content_sha256 == receipt.content_sha256

        published = publication.result(timeout=5)
        assert published.generation == receipt.generation + 1
        next_decision = repository.resolve_promotion_contract(
            _identity(incarnation), 1, 1, published.generation,
            published.content_sha256)
        assert next_decision.contract is not None
        assert next_decision.contract.generation == published.generation
        assert next_decision.contract.content_sha256 == published.content_sha256
        assert next_decision.contract.routing_urls == frozenset({next_url})
    finally:
        release_reader.set()
        if transaction.is_active:
            transaction.rollback()
        executor.shutdown(wait=True)
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _coordinate_statements)
        blocker.close()


def test_promotion_contract_fails_fast_on_owner_lock_contention(route_database):
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database)
    engine, _ = route_database
    blocker = engine.connect()
    transaction = blocker.begin()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        blocker.execute(
            sqlalchemy.select(serve_state_schema.services_table.c.name).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).scalar_one()
        resolution = executor.submit(repository.resolve_promotion_contract,
                                     _identity(incarnation), 1, 1,
                                     receipt.generation, receipt.content_sha256)
        with pytest.raises(route_projection.RouteProjectionUnavailable,
                           match='promotion-contract read'):
            resolution.result(timeout=1)
    finally:
        if transaction.is_active:
            transaction.rollback()
        executor.shutdown(wait=True)
        blocker.close()


@pytest.mark.parametrize(
    ('version', 'epoch', 'generation', 'digest', 'error'),
    [
        (2, 1, 1, 'a' * 64, route_projection.RouteProjectionConflict),
        (1, 2, 1, 'a' * 64, route_projection.RouteProjectionConflict),
        (1, 1, 2, 'a' * 64, route_projection.RouteProjectionConflict),
        (1, 1, 1, 'b' * 64, route_projection.RouteProjectionConflict),
        (1, 1, 1, None, route_projection.RouteProjectionValidationError),
        # A report that omits the fence entirely (every field absent) must be
        # rejected as malformed rather than read as a wildcard match.
        (1, None, 1, 'a' * 64, route_projection.RouteProjectionValidationError),
        (1, 1, None, 'a' * 64, route_projection.RouteProjectionValidationError),
        (1, None, None, None, route_projection.RouteProjectionValidationError),
    ])
def test_promotion_contract_rejects_every_reported_fence_mismatch(
        route_database, version, epoch, generation, digest, error):
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database)
    generation = receipt.generation if generation == 1 else generation
    digest = receipt.content_sha256 if digest == 'a' * 64 else digest

    with pytest.raises(error):
        repository.resolve_promotion_contract(_identity(incarnation), version,
                                              epoch, generation, digest)


def test_promotion_contract_rejects_invalid_async_marker(route_database):
    response = _response()
    route_url = next(iter(response['replica_info']))
    response['replica_info'][route_url]['async_occupancy'] = 'TRUE'
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database, response=response)

    with pytest.raises(route_projection.RouteProjectionCorruption,
                       match='async-occupancy'):
        repository.resolve_promotion_contract(_identity(incarnation), 1, 1,
                                              receipt.generation,
                                              receipt.content_sha256)


@pytest.mark.parametrize('missing', [True, False])
def test_promotion_contract_rejects_missing_or_expired_head(
        route_database, missing):
    repository, incarnation, _, receipt = _enable_projected_contract(
        route_database)
    engine, _ = route_database
    head = route_projection_schema.serve_route_heads_table
    with engine.begin() as connection:
        if missing:
            connection.execute(
                sqlalchemy.delete(head).where(head.c.service_name == 'svc'))
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            connection.execute(
                sqlalchemy.update(head).where(
                    head.c.service_name == 'svc').values(
                        refreshed_at=now - datetime.timedelta(seconds=2),
                        valid_until=now - datetime.timedelta(seconds=1)))

    with pytest.raises(route_projection.RouteProjectionUnavailable,
                       match='missing or stale'):
        repository.resolve_promotion_contract(_identity(incarnation), 1, 1,
                                              receipt.generation,
                                              receipt.content_sha256)


def test_cutover_cas_revalidates_current_route_head(route_database,
                                                    monkeypatch):
    repository, incarnation, _, first = _enable_projected_contract(
        route_database)
    engine, _ = route_database
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine', engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot=lb_ha.LbSlot.A.value,
                    lb_cutover_generation=1,
                    lb_pending_slot=None,
                    lb_cutover_phase=lb_ha.LbCutoverPhase.STABLE.value))
    first_decision = repository.resolve_promotion_contract(
        _identity(incarnation), 1, 1, first.generation, first.content_sha256)
    assert first_decision.contract is not None

    next_url = 'http://10.0.0.2:8000'
    next_record_id = str(uuid.uuid4())
    second = repository.publish(_identity(incarnation),
                                1,
                                _response(next_url),
                                _identity_payload(next_record_id, next_url),
                                {next_record_id},
                                ttl_seconds=60)
    assert second.generation == first.generation + 1
    owner = (123, '10.0.0.5')
    assert lb_cutover_state.begin_lb_cutover(
        'svc',
        'svc-hash',
        owner,
        3,
        lb_ha.LbSlot.A,
        1,
        lb_ha.LbSlot.B,
        route_promotion_contract=first_decision.contract) is None
    assert lb_cutover_state.get_lb_cutover_state(
        'svc').phase == lb_ha.LbCutoverPhase.STABLE

    second_decision = repository.resolve_promotion_contract(
        _identity(incarnation), 1, 1, second.generation, second.content_sha256)
    preparing = lb_cutover_state.begin_lb_cutover(
        'svc',
        'svc-hash',
        owner,
        3,
        lb_ha.LbSlot.A,
        1,
        lb_ha.LbSlot.B,
        route_promotion_contract=second_decision.contract)
    assert preparing is not None
    assert preparing.phase == lb_ha.LbCutoverPhase.PREPARING


def _prepare_incremental_replica(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    _insert_replica(engine, record_id)
    info = types.SimpleNamespace(replica_id=1,
                                 replica_record_id=record_id,
                                 version=1)
    repository.upsert_replica_material(_identity(incarnation), info,
                                       _material())
    target = repository.list_probe_targets(_identity(incarnation))[0]
    assert repository.record_probe_result(target, True, ttl_seconds=60).accepted
    return engine, incarnation, repository, record_id, target


def _decode_incremental_replica(_state_version, state):
    return types.SimpleNamespace(replica_id=1,
                                 replica_record_id=state['replica_record_id'],
                                 version=1)


def test_incremental_composition_callbacks_hold_no_database_locks(
        route_database):
    engine, incarnation, repository, _, _ = _prepare_incremental_replica(
        route_database)
    callback_entered = threading.Event()
    release_callback = threading.Event()

    def _capacity_hint(_infos, _translation, _logical_versions):
        callback_entered.set()
        assert release_callback.wait(timeout=10)
        return {'replica_unit': 'physical_backend'}

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(repository.compose_incremental_snapshot,
                                 _identity(incarnation),
                                 1,
                                 {'load_balancing_policy_name': 'round_robin'},
                                 _decode_incremental_replica,
                                 _capacity_hint,
                                 ttl_seconds=60)
        try:
            assert callback_entered.wait(timeout=10)
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.select(
                        serve_state_schema.services_table.c.name).where(
                            serve_state_schema.services_table.c.name ==
                            'svc').with_for_update(nowait=True)).one()
                connection.execute(
                    sqlalchemy.select(
                        serve_state_schema.replicas_table.c.replica_id).where(
                            serve_state_schema.replicas_table.c.service_name ==
                            'svc').with_for_update(nowait=True)).one()
        finally:
            release_callback.set()
        assert future.result(timeout=10).generation == 1


def test_incremental_composition_rejects_replica_change(route_database):
    engine, incarnation, repository, _, _ = _prepare_incremental_replica(
        route_database)
    callback_entered = threading.Event()
    release_callback = threading.Event()

    def _capacity_hint(_infos, _translation, _logical_versions):
        callback_entered.set()
        assert release_callback.wait(timeout=10)
        return {'replica_unit': 'physical_backend'}

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(repository.compose_incremental_snapshot,
                                 _identity(incarnation),
                                 1,
                                 {'load_balancing_policy_name': 'round_robin'},
                                 _decode_incremental_replica,
                                 _capacity_hint,
                                 ttl_seconds=60)
        try:
            assert callback_entered.wait(timeout=10)
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.update(serve_state_schema.replicas_table).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        'svc',
                        serve_state_schema.replicas_table.c.replica_id == 1).
                    values(
                        replica_state={'replica_record_id': str(uuid.uuid4())}))
        finally:
            release_callback.set()
        with pytest.raises(route_projection.RouteProjectionConflict,
                           match='Replica state changed'):
            future.result(timeout=10)

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                route_projection_schema.serve_route_heads_table)).scalar_one(
                ) == 0


def test_incremental_composition_accepts_monotonic_readiness_refresh(
        route_database):
    _, incarnation, repository, _, target = _prepare_incremental_replica(
        route_database)

    def _capacity_hint(_infos, _translation, _logical_versions):
        receipt = repository.record_probe_result(target, True, ttl_seconds=60)
        assert receipt.accepted
        return {'replica_unit': 'physical_backend'}

    receipt = repository.compose_incremental_snapshot(
        _identity(incarnation),
        1, {'load_balancing_policy_name': 'round_robin'},
        _decode_incremental_replica,
        _capacity_hint,
        ttl_seconds=60)

    assert receipt.generation == 1


def test_incremental_composition_rejects_changed_temporal_eligibility(
        route_database):
    engine, incarnation, repository, _, target = _prepare_incremental_replica(
        route_database)
    expired_at = (datetime.datetime.now(datetime.timezone.utc) -
                  datetime.timedelta(seconds=1))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_replica_leases_table).
            values(observed_at=expired_at - datetime.timedelta(seconds=1),
                   valid_until=expired_at))

    def _capacity_hint(_infos, _translation, _logical_versions):
        receipt = repository.record_probe_result(target, True, ttl_seconds=60)
        assert receipt.accepted
        return {'replica_unit': 'physical_backend'}

    with pytest.raises(route_projection.RouteProjectionConflict,
                       match='freshness changed'):
        repository.compose_incremental_snapshot(
            _identity(incarnation),
            1, {'load_balancing_policy_name': 'round_robin'},
            _decode_incremental_replica,
            _capacity_hint,
            ttl_seconds=60)


def test_incremental_composition_excludes_terminal_replica_history(
        route_database):
    _, incarnation, repository, _, _ = _prepare_incremental_replica(
        route_database)
    _insert_replica(route_database[0],
                    str(uuid.uuid4()),
                    replica_id=2,
                    status='FAILED_PROVISION')
    decoded_replica_ids = []

    def _decode(_state_version, state):
        decoded_replica_ids.append(state['replica_record_id'])
        return types.SimpleNamespace(
            replica_id=1,
            replica_record_id=state['replica_record_id'],
            version=1)

    def _capacity_hint(infos, _translation, _logical_versions):
        assert len(infos) == 1
        return {'replica_unit': 'physical_backend'}

    receipt = repository.compose_incremental_snapshot(
        _identity(incarnation),
        1, {'load_balancing_policy_name': 'round_robin'},
        _decode,
        _capacity_hint,
        ttl_seconds=60)

    assert receipt.generation == 1
    assert len(decoded_replica_ids) == 1


def test_semantic_change_retains_bounded_exact_url_alias(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    first_url = 'http://10.0.0.1:8000'
    second_url = 'http://10.0.0.2:8000'
    repository.publish(_identity(incarnation),
                       1,
                       _response(first_url),
                       _identity_payload(record_id, first_url), {record_id},
                       ttl_seconds=60)
    second = repository.publish(_identity(incarnation),
                                1,
                                _response(second_url),
                                _identity_payload(record_id, second_url),
                                {record_id},
                                ttl_seconds=60)

    assert second.generation == 2
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_snapshots_table.c.
                identity_payload).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc',
                    route_projection_schema.serve_route_snapshots_table.c.
                    generation == 2)).scalar_one()
    assert row[second_url]['alias_expires_at'] is None
    assert row[first_url]['advertised'] is False
    assert row[first_url]['alias_expires_at'] > time.time()


def test_projected_mode_never_falls_back_when_head_or_membership_is_stale(
        route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    repository.publish(_identity(incarnation),
                       1,
                       _response(),
                       _identity_payload(record_id), {record_id},
                       ttl_seconds=60)
    _compose_empty_incremental(repository, incarnation)
    repository.promote('svc', 'svc-hash')

    with pytest.raises(route_projection.RouteProjectionUnavailable,
                       match='membership'):
        repository.resolve_sync('svc', 'svc-hash', 'pod-a')

    _insert_report(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_heads_table).values(
                    refreshed_at=sqlalchemy.func.clock_timestamp() -
                    sqlalchemy.text("INTERVAL '20 seconds'"),
                    valid_until=sqlalchemy.func.clock_timestamp() -
                    sqlalchemy.text("INTERVAL '5 seconds'")))
    with pytest.raises(route_projection.RouteProjectionUnavailable,
                       match='missing or stale'):
        repository.resolve_sync('svc', 'svc-hash', 'pod-a')


def test_owner_takeover_invalidates_old_projection(route_database):
    engine, incarnation = route_database
    repository = route_projection.RouteProjectionRepository(engine)
    record_id = str(uuid.uuid4())
    repository.publish(_identity(incarnation),
                       1,
                       _response(),
                       _identity_payload(record_id), {record_id},
                       ttl_seconds=60)
    _compose_empty_incremental(repository, incarnation)
    _insert_report(engine)
    repository.promote('svc', 'svc-hash')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    controller_incarnation=uuid.uuid4(),
                    controller_owner_epoch=5,
                    controller_pid=456))

    with pytest.raises(route_projection.RouteProjectionUnavailable,
                       match='capability'):
        repository.resolve_sync('svc', 'svc-hash', 'pod-a')
