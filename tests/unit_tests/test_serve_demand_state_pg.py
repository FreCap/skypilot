"""PostgreSQL contracts for controller-independent Serve demand."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import copy
import time

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import constants
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_demand_state_schema_048_pg')


def _report(now: float, *, sequence: int = 1) -> dict:
    bucket_seconds = constants.LB_DEMAND_WINDOW_BUCKET_SECONDS
    bucket_start = int(now // bucket_seconds) * bucket_seconds
    return {
        'protocol_version': constants.LB_DEMAND_REPORT_PROTOCOL_VERSION,
        'sequence': sequence,
        'reporter_session_id': 'process-a',
        'reporter_observed_at': now,
        'lb_session_id': 'pod-a',
        'lb_slot': 'a',
        'routing_version': 3,
        'armed_generation': None,
        'applied_role': 'ACTIVE',
        'applied_generation': 2,
        'local_in_flight': 1,
        'http_in_flight': {
            'http://replica': 1
        },
        'async_occupancy': {
            'http://replica': 2
        },
        'occupancy_sample_generation': {
            'http://replica': 4
        },
        'occupancy_sample_age_seconds': {
            'http://replica': 0.1
        },
        'routing_urls': ['http://replica'],
        'unknown_in_flight_urls': [],
        'draining_urls': [],
        'demand_window': {
            'bucket_seconds': bucket_seconds,
            'window_seconds': constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 1,
                'compatibility_profiles': [{
                    'priority': 50,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
            }],
            'compatibility_complete': True,
            'saturated': False,
        },
        'request_history': {
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': [{
                'bucket_start': int(now // 60) * 60,
                'request_count': 1,
                'rejected_count': 0,
            }],
        },
        'request_classification_history': {
            'classification_version': 1,
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': [],
        },
        'prediction_time_history': None,
        'configured_accelerators': ['L4'],
        'request_accelerator_compatibility_version': 1,
        'queue_depth': 0,
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
        'queue_depth_by_priority': {},
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'rejected_in_window_by_priority': {},
        'rejected_in_recent_window_by_priority': {},
        'unique_job_arrivals_60s': 1,
        'unique_job_arrivals_300s': 1,
        'headerless_arrivals_60s': 0,
        'headerless_arrivals_300s': 0,
        'offered_arrival_tracking_saturated': False,
    }


@pytest.fixture
def demand_database(empty_postgres, monkeypatch):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '048')
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                status='READY',
                hash='svc-hash',
                current_version=1,
                active_versions='[1]',
                pool=0))
    return empty_postgres


def test_serve048_schema_is_postgresql_only_and_complete(demand_database):
    inspector = sqlalchemy.inspect(demand_database)
    assert inspector.has_table(
        demand_state_schema.serve_lb_demand_reports_table.name)
    assert inspector.has_table(
        demand_state_schema.serve_demand_feed_generations_table.name)
    for table_name in (
            demand_state_schema.serve_lb_demand_reports_table.name,
            demand_state_schema.serve_demand_feed_generations_table.name):
        foreign_keys = inspector.get_foreign_keys(table_name)
        assert any(
            foreign_key['referred_table'] == 'services' and
            foreign_key['constrained_columns'] == ['service_name'] and
            (foreign_key.get('options') or {}).get('ondelete') == 'CASCADE'
            for foreign_key in foreign_keys)


def test_report_sequence_idempotency_freshness_and_summary(demand_database):
    now = time.time()
    report = _report(now)

    first = demand_state.ingest_report('svc', 'svc-hash', report)
    duplicate = demand_state.ingest_report('svc', 'svc-hash', report)

    assert first.generation == 1
    assert first.duplicate is False
    assert duplicate.generation == 1
    assert duplicate.duplicate is True
    summary = demand_state.get_request_summary('svc', 'svc-hash')
    assert summary['request_telemetry_state'] == 'fresh'
    assert summary['request_telemetry_compatibility_complete'] is True
    assert summary['recent_request_count'] == 1
    assert summary['in_flight_requests'] == 3

    conflict = copy.deepcopy(report)
    conflict['queue_depth'] = 1
    with pytest.raises(demand_state.DemandReportConflict,
                       match='conflicts with prior payload'):
        demand_state.ingest_report('svc', 'svc-hash', conflict)

    with demand_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_lb_demand_reports_table).values(
                    valid_until=sqlalchemy.func.clock_timestamp() -
                    sqlalchemy.text("INTERVAL '1 second'")))
    stale = demand_state.get_request_summary('svc', 'svc-hash')
    assert stale['request_telemetry_state'] == 'stale'
    assert stale['recent_request_count'] is None


def test_report_rejects_wrong_service_hash_and_reporter_clock(demand_database):
    assert demand_database is not None
    now = time.time()
    with pytest.raises(demand_state.DemandReportConflict,
                       match='incarnation mismatch'):
        demand_state.ingest_report('svc', 'wrong-hash', _report(now))
    with pytest.raises(demand_state.DemandReportError, match='database clock'):
        demand_state.ingest_report('svc', 'svc-hash', _report(now + 3600))


def test_corrupt_durable_payload_fails_closed(demand_database):
    demand_state.ingest_report('svc', 'svc-hash', _report(time.time()))
    with demand_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_lb_demand_reports_table).values(
                    payload={
                        'applied_role': 'ACTIVE',
                        'applied_generation': 'corrupt',
                    }))

    summary = demand_state.get_request_summary('svc', 'svc-hash')

    assert summary['request_telemetry_state'] == 'unavailable'
    assert summary['request_telemetry_reason'] == 'invalid_durable_payload'
    assert summary['recent_request_count'] is None


def test_unknown_occupancy_never_displays_processing_zero(demand_database):
    assert demand_database is not None
    report = _report(time.time())
    report['async_occupancy'] = {}
    report['occupancy_sample_generation'] = {}
    report['occupancy_sample_age_seconds'] = {}
    report['unknown_in_flight_urls'] = ['http://replica']
    demand_state.ingest_report('svc', 'svc-hash', report)

    summary = demand_state.get_request_summary('svc', 'svc-hash')

    assert summary['request_telemetry_state'] == 'fresh'
    assert summary['request_telemetry_reason'] == 'in_flight_incomplete'
    assert summary['in_flight_requests'] is None
    assert summary['recent_request_count'] == 1


def test_reporter_identity_and_count_are_bounded(demand_database, monkeypatch):
    assert demand_database is not None
    now = time.time()
    demand_state.ingest_report('svc', 'svc-hash', _report(now))

    changed_identity = _report(now, sequence=2)
    changed_identity['lb_session_id'] = 'pod-b'
    with pytest.raises(demand_state.DemandReportConflict,
                       match='identity changed'):
        demand_state.ingest_report('svc', 'svc-hash', changed_identity)

    monkeypatch.setattr(constants, 'LB_DEMAND_REPORT_MAX_REPORTERS', 1)
    second_reporter = _report(now)
    second_reporter['reporter_session_id'] = 'process-b'
    with pytest.raises(demand_state.DemandReportConflict,
                       match='reporter limit'):
        demand_state.ingest_report('svc', 'svc-hash', second_reporter)


def test_service_delete_cascades_demand_state(demand_database):
    demand_state.ingest_report('svc', 'svc-hash', _report(time.time()))

    with demand_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc'))
        report_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                demand_state_schema.serve_lb_demand_reports_table)).scalar_one(
                )
        generation_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                demand_state_schema.serve_demand_feed_generations_table)
        ).scalar_one()
    assert report_count == 0
    assert generation_count == 0
