"""PostgreSQL contracts for durable provider-free SkyServe routes."""
# pylint: disable=not-callable,redefined-outer-name,unused-import

import datetime
import time
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
    name='serve_route_projection_schema_049_pg')


@pytest.fixture
def route_database(empty_postgres):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '049')
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

    _insert_report(engine)
    assert repository.promote('svc', 'svc-hash') == 1
    projected = repository.resolve_sync('svc', 'svc-hash', 'pod-a')
    assert projected.mode == route_projection.RouteSourceMode.DURABLE_PROJECTED
    assert projected.response is not None
    assert projected.response['route_projection_generation'] == 1
    assert projected.response['route_projection_sha256'] == first.content_sha256
    assert projected.response['route_source_epoch'] == 1
    assert projected.response['replica_info'] == _response()['replica_info']


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
