"""PostgreSQL contracts for durable provider-free SkyServe routes."""
# pylint: disable=not-callable,redefined-outer-name,unused-import

import datetime
import time
import types
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import demand_state_schema
from sky.serve import route_projection
from sky.serve import route_projection_schema
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


def _identity(incarnation):
    return route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        controller_incarnation=incarnation,
        controller_owner_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.5')


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
