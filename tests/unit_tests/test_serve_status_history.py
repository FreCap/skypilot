"""Unit coverage for aggregate SkyServe status history."""
# pylint: disable=protected-access

import datetime

import pytest
from sqlalchemy.dialects import postgresql

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


def test_build_history_rows_groups_versions_and_preserves_zero_capacity():
    observed_at = datetime.datetime(2026,
                                    7,
                                    16,
                                    13,
                                    5,
                                    20,
                                    tzinfo=datetime.timezone.utc)
    bucket_start = observed_at.replace(second=0)
    rows = [
        ('svc', 'hash', 1, 'READY', 3),
        ('svc', 'hash', 1, 'FAILED_PROBING', 2),
        ('svc', 'hash', 2, 'PROVISIONING', 4),
        ('empty', 'empty-hash', 7, None, 0),
    ]

    result = serve_history._build_history_rows(rows, observed_at, bucket_start)

    by_key = {(row['service_name'], row['version']): row for row in result}
    assert by_key[('svc', 1)]['ready_count'] == 3
    assert by_key[('svc', 1)]['errored_count'] == 2
    assert by_key[('svc', 1)]['total_count'] == 5
    assert by_key[('svc', 2)]['provisioning_count'] == 4
    assert by_key[('svc', 2)]['total_count'] == 4
    assert by_key[('empty', 7)]['total_count'] == 0
    assert by_key[('empty', 7)]['bucket_start'] == bucket_start


def test_snapshot_query_uses_normalized_columns_only():
    sql = str(serve_history._snapshot_query().compile(
        dialect=postgresql.dialect(),
        compile_kwargs={'literal_binds': True})).lower()
    assert 'replicas.status' in sql
    assert 'replicas.version' in sql
    assert 'replica_state' not in sql
    assert 'replica_info' not in sql


@pytest.mark.parametrize('hours', [0, 73, True, 1.5])
def test_history_hours_are_bounded_before_database_access(hours):
    with pytest.raises(ValueError, match='hours must be an integer'):
        serve_history.get_status_history('svc', hours=hours)
