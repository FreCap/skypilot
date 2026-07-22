"""Unit coverage for aggregate SkyServe status history."""
# pylint: disable=protected-access

import datetime
from unittest import mock

import pytest
from sqlalchemy.dialects import postgresql

from sky.serve import constants
from sky.serve import serve_history
from sky.serve import serve_state


def test_status_bucket_mapping_is_exhaustive():
    expected = {
        serve_state.ReplicaStatus.PENDING: 'provisioning_count',
        serve_state.ReplicaStatus.PROVISIONING: 'provisioning_count',
        serve_state.ReplicaStatus.STARTING: 'provisioning_count',
        serve_state.ReplicaStatus.READY: 'ready_count',
        serve_state.ReplicaStatus.NOT_READY: 'not_ready_count',
        serve_state.ReplicaStatus.SHUTTING_DOWN: 'stopping_count',
        serve_state.ReplicaStatus.PREEMPTED: 'preempted_count',
        serve_state.ReplicaStatus.FAILED: 'errored_count',
        serve_state.ReplicaStatus.FAILED_INITIAL_DELAY: 'errored_count',
        serve_state.ReplicaStatus.FAILED_PROBING: 'errored_count',
        serve_state.ReplicaStatus.FAILED_PROVISION: 'errored_count',
        serve_state.ReplicaStatus.FAILED_CLEANUP: 'errored_count',
        serve_state.ReplicaStatus.UNKNOWN: 'errored_count',
    }
    assert set(expected) == set(serve_state.ReplicaStatus)
    for status, bucket in expected.items():
        assert serve_history._status_bucket(status.value) == bucket


def test_build_history_rows_groups_capacity_modes_and_reserved_ready():
    observed_at = datetime.datetime(2026,
                                    7,
                                    16,
                                    13,
                                    5,
                                    20,
                                    tzinfo=datetime.timezone.utc)
    bucket_start = observed_at.replace(second=0)
    rows = [
        ('svc', 'hash', 1, 'READY', 3, 12, 2, 8),
        ('svc', 'hash', 1, 'FAILED_PROBING', 2, 5, 1, 3),
        ('svc', 'hash', 2, 'PROVISIONING', 4, 16, 0, 0),
        ('empty', 'empty-hash', 7, None, 0, 0, 0, 0),
    ]

    result = serve_history._build_history_rows(rows, observed_at, bucket_start)

    by_key = {(row['service_name'], row['version']): row for row in result}
    assert by_key[('svc', 1)]['ready_count'] == 3
    assert by_key[('svc', 1)]['errored_count'] == 2
    assert by_key[('svc', 1)]['total_count'] == 5
    assert by_key[('svc', 1)]['ready_reserved_count'] == 2
    assert by_key[('svc', 1)]['logical_ready_count'] == 12
    assert by_key[('svc', 1)]['logical_ready_reserved_count'] == 8
    assert by_key[('svc', 1)]['logical_errored_count'] == 5
    assert by_key[('svc', 1)]['logical_total_count'] == 17
    assert by_key[('svc', 2)]['provisioning_count'] == 4
    assert by_key[('svc', 2)]['total_count'] == 4
    assert by_key[('svc', 2)]['logical_provisioning_count'] == 16
    assert by_key[('svc', 2)]['logical_total_count'] == 16
    assert by_key[('empty', 7)]['total_count'] == 0
    assert by_key[('empty', 7)]['logical_total_count'] == 0
    assert by_key[('empty', 7)]['bucket_start'] == bucket_start


def test_snapshot_query_uses_normalized_columns_and_durable_capacity_state():
    sql = str(serve_history._snapshot_query().compile(
        dialect=postgresql.dialect(),
        compile_kwargs={'literal_binds': True})).lower()
    assert 'replicas.status' in sql
    assert 'replicas.version' in sql
    assert 'replicas.sky_down_status is distinct from' in sql
    assert "'succeeded'" in sql
    assert 'replica_state' in sql
    assert 'planned_capacity' in sql
    assert 'reserved_fill' in sql
    assert 'replica_info' not in sql


@pytest.mark.parametrize('hours', [0, 73, True, 1.5])
def test_history_hours_are_bounded_before_database_access(hours):
    with pytest.raises(ValueError, match='hours must be an integer'):
        serve_history.get_status_history('svc', hours=hours)


def test_missing_central_service_is_unavailable(monkeypatch):
    engine = mock_engine = mock.MagicMock()
    engine.dialect.name = 'postgresql'
    monkeypatch.setattr(serve_history, '_postgres_engine', lambda: mock_engine)
    session = mock.MagicMock()
    session.__enter__.return_value = session
    session.execute.return_value.scalar_one_or_none.return_value = None
    monkeypatch.setattr(serve_history.orm, 'Session',
                        lambda unused_engine: session)

    history = serve_history.get_status_history('missing', timestamp=120)

    assert history == {
        'available': False,
        'bucket_seconds': 60,
        'retention_hours': 72,
        'samples': [],
        'request_samples': [],
        'prediction_time_samples': [],
        'autoscaler_samples': [],
        'prediction_time_histogram_version': 1,
        'prediction_time_bucket_upper_bounds_seconds': list(
            constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS),
        'rejection_history_available': False,
        'request_window_seconds': 3600,
        'requests_last_hour': 0,
    }


def test_request_history_rows_validate_and_normalize_recent_buckets():
    observed_at = datetime.datetime(2026,
                                    7,
                                    16,
                                    13,
                                    5,
                                    20,
                                    tzinfo=datetime.timezone.utc)

    rows = serve_history._request_history_rows(
        'svc', 'hash', 'pod:process', {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': int(observed_at.timestamp()) // 60 * 60,
                'request_count': 7,
            }],
        }, observed_at)

    assert rows == [{
        'service_name': 'svc',
        'service_hash': 'hash',
        'reporter_session_id': 'pod:process',
        'bucket_start': observed_at.replace(second=0, microsecond=0),
        'observed_at': observed_at,
        'request_count': 7,
        'rejected_count': 0,
        'rejection_count_available': False,
    }]

    rejection_only = serve_history._request_history_rows(
        'svc', 'hash', 'pod:process', {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': int(observed_at.timestamp()) // 60 * 60,
                'request_count': 0,
                'rejected_count': 1,
            }],
        }, observed_at)
    assert rejection_only[0]['request_count'] == 0
    assert rejection_only[0]['rejected_count'] == 1
    assert rejection_only[0]['rejection_count_available'] is True


@pytest.mark.parametrize(
    'request_history',
    [
        {
            'bucket_seconds': 30,
            'buckets': [],
        },
        {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 61,
                'request_count': 1,
            }],
        },
        {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 60,
                'request_count': 0,
            }],
        },
        {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 60,
                'request_count': 1,
                'rejected_count': -1,
            }],
        },
    ],
)
def test_request_history_rows_reject_malformed_reports(request_history):
    observed_at = datetime.datetime.fromtimestamp(60, datetime.timezone.utc)
    with pytest.raises(ValueError):
        serve_history._request_history_rows('svc', 'hash', 'pod:process',
                                            request_history, observed_at)
