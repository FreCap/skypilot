"""PostgreSQL contracts for exact-idle paid replica retirement."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import datetime
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission_schema
from sky.serve import demand_state_schema
from sky.serve import paid_retirement
from sky.serve import replica_managers
from sky.serve import route_projection_schema
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_paid_retirement_schema_052_pg')

_OWNER = (123, '10.0.0.5')
_ROUTE_URL = 'http://replica:8000'


def _authority() -> paid_retirement.FreshZeroAuthority:
    return paid_retirement.FreshZeroAuthority(service_hash='svc-hash',
                                              demand_source_epoch=1,
                                              demand_feed_generation=1,
                                              capacity_plan_generation=1,
                                              capacity_plan_sha256='b' * 64,
                                              route_generation=1)


@pytest.fixture
def retirement_database(empty_postgres, monkeypatch):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    now = datetime.datetime.now(datetime.timezone.utc)
    incarnation = uuid.uuid4()
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8000',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    info.status_property.first_ready_time = time.time()
    replica_values = serve_state._replica_row_values('svc', 1, info)
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc', epoch=3))
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
                controller_pid=_OWNER[0],
                controller_ip=_OWNER[1],
                route_source_mode='DURABLE_PROJECTED',
                route_source_epoch=1,
                route_projection_capable=True,
                route_projection_controller_incarnation=incarnation,
                route_projection_protocol_version=2,
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica_values))
        connection.execute(
            sqlalchemy.insert(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    service_name='svc',
                    service_hash='svc-hash',
                    generation=1,
                    updated_at=now))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_snapshots_table).values(
                    service_name='svc',
                    generation=1,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    controller_incarnation=incarnation,
                    controller_owner_epoch=4,
                    controller_pid=_OWNER[0],
                    controller_ip=_OWNER[1],
                    service_version=1,
                    protocol_version=1,
                    producer_protocol_version=2,
                    content_sha256='a' * 64,
                    response_payload={},
                    identity_payload={},
                    created_at=now))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_heads_table).values(
                    service_name='svc',
                    generation=1,
                    refreshed_at=now,
                    valid_until=now + datetime.timedelta(seconds=60)))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_replica_leases_table).
            values(service_name='svc',
                   service_hash='svc-hash',
                   replica_id=1,
                   replica_record_id=uuid.UUID(info.replica_record_id),
                   service_lifecycle_epoch=3,
                   controller_incarnation=incarnation,
                   controller_owner_epoch=4,
                   controller_pid=_OWNER[0],
                   controller_ip=_OWNER[1],
                   service_version=1,
                   route_url=_ROUTE_URL,
                   gpu_type='L4',
                   gpu_count=1,
                   probe_method='GET',
                   readiness_path='/health',
                   probe_timeout_seconds=15,
                   probe_post_data=None,
                   probe_headers=None,
                   async_occupancy=True,
                   uses_logical_replicas=False,
                   is_zero_cost=False,
                   planned_capacity=1,
                   route_allowed=True,
                   requires_route_marker=False,
                   route_marker_payload=None,
                   material_sha256='c' * 64,
                   material_generation=1,
                   readiness_generation=1,
                   ready=True,
                   created_at=now,
                   resolved_at=now,
                   observed_at=now,
                   valid_until=now + datetime.timedelta(seconds=60),
                   revocation_generation=0,
                   revoked_at=None,
                   revocation_reason=None))
        payload = {
            'normalized_demand': {
                'fresh_aggregate_zero': True,
            },
            'capacity_target_by_accelerator': {
                'l4': 0,
            },
            'paid_residual_by_accelerator': {},
        }
        connection.execute(
            sqlalchemy.insert(
                capacity_admission_schema.serve_capacity_plans_table).values(
                    service_name='svc',
                    generation=1,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    service_version=1,
                    demand_source_epoch=1,
                    demand_feed_generation=1,
                    route_generation=1,
                    route_sha256='a' * 64,
                    route_source_epoch=1,
                    protocol_version=1,
                    content_sha256='b' * 64,
                    payload=payload,
                    created_at=now))
        connection.execute(
            sqlalchemy.insert(
                capacity_admission_schema.serve_capacity_plan_heads_table).
            values(service_name='svc',
                   generation=1,
                   demand_feed_generation=1,
                   receipt_watermark_sha256='d' * 64,
                   refreshed_at=now,
                   valid_until=now + datetime.timedelta(seconds=60)))
    return empty_postgres, info


def test_delete_cleanup_noops_before_serve051(empty_postgres):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '050')
    with sqlalchemy.orm.Session(empty_postgres) as session, session.begin():
        paid_retirement.delete_in_session(session, 'svc', [1])


def _mark_retiring(info):
    info.status_property.is_scale_down = True
    info.status_property.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    info.status_property.wait_for_idle_before_termination = True
    info.status_property.drain_cap_seconds = None
    info.status_property.drain_started_at = None


def test_admission_atomically_revokes_route_and_persists_exact_intent(
        retirement_database):
    engine, info = retirement_database
    _mark_retiring(info)

    record = serve_state.admit_paid_retirement('svc',
                                               1,
                                               info,
                                               _authority(),
                                               requires_idle_proof=True,
                                               expected_service_hash='svc-hash',
                                               expected_controller_owner=_OWNER)

    assert record is not None
    assert record['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
    assert record['route_url'] == _ROUTE_URL
    with engine.connect() as connection:
        intent = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table)).mappings(
                ).one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.status)).scalar_one()
    assert intent['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
    assert lease['revocation_reason'] == 'replica_became_route_ineligible'
    assert replica == 'SHUTTING_DOWN'


def test_idle_commit_is_generation_fenced_and_irreversible(retirement_database):
    engine, info = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        _authority(),
        requires_idle_proof=True,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.wait_for_idle_before_termination = False

    assert not serve_state.commit_paid_retirement(
        'svc',
        1,
        info,
        _authority(),
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.ACTIVE.value


def test_exact_idle_commit_cannot_be_cancelled(retirement_database):
    engine, info = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        _authority(),
        requires_idle_proof=True,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.commit_paid_retirement('svc',
                                              1,
                                              info,
                                              _authority(),
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    assert not serve_state.cancel_paid_retirement(
        'svc',
        1,
        info,
        2,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.COMMITTED.value


def test_newer_positive_demand_cancels_only_uncommitted_intent(
        retirement_database):
    engine, info = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        _authority(),
        requires_idle_proof=True,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.cancel_paid_retirement('svc',
                                              1,
                                              info,
                                              2,
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
    assert state == paid_retirement.PaidRetirementState.CANCELLED.value
    assert lease['revoked_at'] is not None
