"""Tests for bounded direct SkyServe dashboard reads."""

# pylint: disable=protected-access
import base64
import contextlib
import json
from unittest import mock

import pytest
from sqlalchemy.dialects import postgresql

from sky.serve import serve_dashboard


def _compiled(statement) -> str:
    return str(
        statement.compile(dialect=postgresql.dialect(),
                          compile_kwargs={'literal_binds': True}))


def _replica_row(replica_id: int, status: str | None = 'READY') -> dict:
    return {
        'replica_id': replica_id,
        'status': status,
        'version': 3,
        'created_at': 100.0,
        'is_spot': True,
        'replica_state_version': 1,
        'replica_state': {
            'planned_capacity': 4,
            'location': {
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': 'us-east-1a',
                'instance_type': 'g6.4xlarge',
                'accelerators': {
                    'L4': 1
                },
            },
            'status_property': {
                'first_ready_time': 130.0,
            },
        },
    }


class _Result:
    """Minimal SQLAlchemy result stub."""

    def __init__(self, *, row=None, scalar=None, rows=None):
        self._row = row
        self._scalar = scalar
        self._rows = [] if rows is None else rows

    def fetchone(self):
        return self._row

    def scalar_one(self):
        return self._scalar

    def fetchall(self):
        return self._rows


class _Connection:
    """Ordered-result connection stub that records statements."""

    def __init__(self, results):
        self.results = list(results)
        self.statements = []

    def begin(self):
        return contextlib.nullcontext()

    def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0)


def _cursor_with(payload: dict) -> str:
    raw = json.dumps(payload, separators=(',', ':')).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip('=')
    return 'v1.' + encoded


def test_summary_query_is_one_compact_grouped_scan():
    sql = _compiled(serve_dashboard._replica_summary_query(['svc-a', 'svc-b']))

    assert 'LEFT OUTER JOIN replicas' in sql
    assert 'GROUP BY services.name' in sql
    assert "services.name IN ('svc-a', 'svc-b')" in sql
    assert 'replicas.replica_state' in sql
    assert 'replicas.replica_info' not in sql


def test_summary_counts_physical_attempts_separately_from_capacity():
    rows = [{
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': 'READY',
        'physical_count': 2,
        'capacity_count': 8,
    }, {
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': 'FAILED_PROVISION',
        'physical_count': 3,
        'capacity_count': 12,
    }, {
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': None,
        'physical_count': 1,
        'capacity_count': 4,
    }]

    assert serve_dashboard._build_replica_summaries(rows) == [{
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'replica_unit': 'logical_slot',
        'replica_status_counts': {
            'READY': 2,
            'FAILED_PROVISION': 3,
            'UNKNOWN': 1,
        },
        'replica_capacity_counts': {
            'READY': 8,
            'FAILED_PROVISION': 12,
            'UNKNOWN': 4,
        },
        'current_or_uncertain_count': 3,
        'past_attempt_count': 3,
    }]


def test_replica_summaries_execute_one_batched_query(monkeypatch):
    connection = _Connection([
        _Result(rows=[{
            'name': 'svc',
            'hash': 'hash-a',
            'logical_replica_semantics': 0,
            'status': 'READY',
            'physical_count': 1,
            'capacity_count': 1,
        }])
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))
    monkeypatch.setattr(serve_dashboard.time, 'time', lambda: 42.0)

    result = serve_dashboard.get_replica_summaries(['svc'])

    assert len(connection.statements) == 1
    assert result['observed_at'] == 42.0
    assert result['summaries'][0]['service_name'] == 'svc'


def test_scope_predicates_keep_unknown_and_future_states_current():
    current_sql = _compiled(
        serve_dashboard._scope_predicate(
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE))
    past_sql = _compiled(
        serve_dashboard._scope_predicate(serve_dashboard.PAST_ATTEMPTS_SCOPE))

    assert 'replicas.status IS NULL' in current_sql
    assert 'NOT IN' in current_sql
    for status in serve_dashboard.PAST_ATTEMPT_STATUSES:
        assert status in current_sql
        assert status in past_sql
    assert 'FAILED_CLEANUP' not in past_sql
    assert 'UNKNOWN' not in past_sql
    assert 'PREEMPTED' not in past_sql


def test_page_query_filters_and_bounds_before_compact_state_decode():
    query = serve_dashboard._replica_page_query(
        'svc', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 50, 120, 75)
    sql = _compiled(query)

    assert 'replicas.replica_id <= 120' in sql
    assert 'replicas.replica_id < 75' in sql
    assert 'replicas.status IS NULL' in sql
    assert 'ORDER BY replicas.replica_id DESC' in sql
    assert 'LIMIT 51' in sql
    assert 'replicas.replica_state' in sql
    assert 'replicas.replica_info' not in sql


def test_cursor_round_trip_allows_limit_one_first_page():
    cursor = serve_dashboard._encode_cursor(
        'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 10, 10)

    assert serve_dashboard._decode_cursor(
        cursor, 'hash-a',
        serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE) == (10, 10)


@pytest.mark.parametrize('payload', [{
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 2,
}, {
    'hash': '',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 11,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': True,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': [],
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'invalid',
    'version': 1,
}])
def test_cursor_rejects_malformed_payload(payload):
    with pytest.raises(serve_dashboard.InvalidReplicaCursorError):
        serve_dashboard._decode_cursor(
            _cursor_with(payload), 'hash-a',
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE)


def test_cursor_rejects_excessively_nested_json():
    raw = ('{"hash":"hash-a","last":1,"max":1,"scope":' + '[' * 1000 + '0' +
           ']' * 1000 + ',"version":1}').encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip('=')

    with pytest.raises(serve_dashboard.InvalidReplicaCursorError):
        serve_dashboard._decode_cursor(
            f'v1.{encoded}', 'hash-a',
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE)


@pytest.mark.parametrize(('service_hash', 'scope'), [
    ('hash-b', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE),
    ('hash-a', serve_dashboard.PAST_ATTEMPTS_SCOPE),
])
def test_cursor_rejects_other_incarnation_or_scope(service_hash, scope):
    cursor = serve_dashboard._encode_cursor(
        'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 10, 8)

    with pytest.raises(serve_dashboard.ReplicaCursorMismatchError):
        serve_dashboard._decode_cursor(cursor, service_hash, scope)


def test_replica_serialization_is_lightweight_and_handle_free():
    replica = serve_dashboard._serialize_replica_row(_replica_row(7))

    assert replica == {
        'replica_id': 7,
        'status': 'READY',
        'version': 3,
        'planned_capacity': 4,
        'is_spot': True,
        'created_at': 100.0,
        'launched_at': None,
        'ready_at': 130.0,
        'time_to_ready_seconds': 30.0,
        'cloud': 'AWS',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'infra': 'AWS (us-east-1)',
        'resources_str': 'g6.4xlarge; L4:1',
        'resources_str_full': 'g6.4xlarge; L4:1',
        'instance_type': 'g6.4xlarge',
        'accelerators': {
            'L4': 1
        },
    }
    assert not ({'handle', 'endpoint', 'hourly_cost'} & replica.keys())


def test_replica_page_uses_snapshot_max_in_followup_cursor(monkeypatch):
    first_connection = _Connection([
        _Result(row=('hash-a', 1)),
        _Result(scalar=105),
        _Result(scalar=3),
        _Result(rows=[
            _replica_row(105),
            _replica_row(100, 'FAILED_CLEANUP'),
            _replica_row(90, 'UNKNOWN'),
        ]),
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(
        serve_dashboard, '_repeatable_read_connection',
        lambda _engine: contextlib.nullcontext(first_connection))

    first = serve_dashboard.get_replica_page(
        'svc', 'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 2, None)

    assert [row['replica_id'] for row in first['replicas']] == [105, 100]
    assert first['total'] == 3
    assert first['replica_unit'] == 'logical_slot'
    assert first['next_cursor'] is not None
    assert serve_dashboard._decode_cursor(
        first['next_cursor'], 'hash-a',
        serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE) == (105, 100)

    next_connection = _Connection([
        _Result(row=('hash-a', 1)),
        _Result(scalar=3),
        _Result(rows=[_replica_row(90, 'UNKNOWN')]),
    ])
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(next_connection))

    second = serve_dashboard.get_replica_page(
        'svc', 'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 2,
        first['next_cursor'])

    assert [row['replica_id'] for row in second['replicas']] == [90]
    assert second['next_cursor'] is None
    assert len(next_connection.statements) == 3
    page_sql = _compiled(next_connection.statements[-1])
    assert 'replicas.replica_id <= 105' in page_sql
    assert 'replicas.replica_id < 100' in page_sql


@pytest.mark.parametrize(('identity', 'error'), [
    (None, serve_dashboard.ServiceNotFoundError),
    (('hash-b', 0), serve_dashboard.ServiceHashMismatchError),
])
def test_replica_page_fences_service_identity(monkeypatch, identity, error):
    connection = _Connection([_Result(row=identity)])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))

    with pytest.raises(error):
        serve_dashboard.get_replica_page('svc', 'hash-a',
                                         serve_dashboard.PAST_ATTEMPTS_SCOPE,
                                         50, None)

    assert len(connection.statements) == 1
