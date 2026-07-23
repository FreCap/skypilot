"""Tests for bounded, fail-open SkyServe placement history."""
# pylint: disable=protected-access
import base64
import datetime
import json

import pytest

from sky.serve import placement_history


@pytest.fixture(autouse=True)
def _reset_buffer():
    placement_history._drain_request_buffer()
    placement_history.reset_request_buffer()
    yield
    placement_history._drain_request_buffer()
    placement_history.reset_request_buffer()


def test_event_fields_match_table_columns():
    """EVENT_FIELDS must track the table schema (minus service_hash)."""
    column_names = [
        str(column.name)
        for column in placement_history.serve_placement_events_table.columns
        if str(column.name) != 'service_hash'
    ]
    assert list(placement_history.EVENT_FIELDS) == column_names
    # History payloads are persisted with orjson, which only accepts exact
    # str dict keys; the literals guarantee that.
    for field in placement_history.EVENT_FIELDS:
        assert type(field) is str  # pylint: disable=unidiomatic-typecheck


def _record(**overrides):
    values = {
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'request_id': 'request-a',
        'cluster_name': 'svc-1',
        'outcome': 'capacity_failed',
    }
    values.update(overrides)
    return placement_history.record_event(**values)


def test_record_event_is_bounded_and_sanitized(monkeypatch):
    monkeypatch.setattr(placement_history, 'MAX_EVENTS_PER_REQUEST', 2)

    assert _record(error_summary='\x1b[31m capacity\n  unavailable \x1b[0m')
    assert _record(hourly_price=float('inf'))
    assert not _record()

    events, truncated = placement_history._drain_request_buffer()
    assert truncated is True
    assert [event['attempt_ordinal'] for event in events] == [0, 1]
    assert events[0]['error_summary'] == 'capacity unavailable'
    assert events[1]['hourly_price'] is None
    assert events[1]['price_source'] is None


def test_flush_without_postgres_drains_without_writing(monkeypatch):
    assert _record(hourly_price=1.25)
    monkeypatch.setattr(placement_history, '_postgres_engine', lambda: None)

    assert placement_history.flush_request_buffer() == 0
    assert placement_history._drain_request_buffer() == ([], False)


def test_flush_failure_preserves_events_for_retry(monkeypatch):
    assert _record()

    class _FailingEngine:

        def begin(self):
            raise RuntimeError('database unavailable')

    monkeypatch.setattr(placement_history, '_postgres_engine', _FailingEngine)
    with pytest.raises(RuntimeError):
        placement_history.flush_request_buffer()

    events, truncated = placement_history._drain_request_buffer()
    assert truncated is False
    assert len(events) == 1
    assert events[0]['request_id'] == 'request-a'


def test_reset_preserves_unflushed_events_and_restarts_ordinals():
    assert _record()
    placement_history.reset_request_buffer()
    assert _record(request_id='request-b')

    events, _ = placement_history._drain_request_buffer()
    assert [event['request_id'] for event in events
           ] == ['request-a', 'request-b']
    # Attempt ordinals restart per request.
    assert events[1]['attempt_ordinal'] == 0


def test_discard_only_removes_snapshotted_events():
    assert _record()
    events, _ = placement_history._snapshot_request_buffer()
    assert _record(request_id='request-b')

    placement_history._discard_events(events)

    remaining, _ = placement_history._drain_request_buffer()
    assert [event['request_id'] for event in remaining] == ['request-b']


def test_cursor_round_trip_and_validation():
    observed_at = datetime.datetime(2026,
                                    7,
                                    19,
                                    12,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    cursor = placement_history._encode_cursor(observed_at, 'event-a')

    decoded_time, decoded_id = placement_history._decode_cursor(cursor)
    assert decoded_time == observed_at
    assert decoded_id == 'event-a'
    with pytest.raises(ValueError, match='Invalid placement-history cursor'):
        placement_history._decode_cursor('not-json')
    non_finite = base64.urlsafe_b64encode(
        json.dumps([float('inf'), 'event-a']).encode()).decode()
    with pytest.raises(ValueError, match='Invalid placement-history cursor'):
        placement_history._decode_cursor(non_finite)


@pytest.mark.parametrize('kwargs', [{
    'hours': 0
}, {
    'hours': 25
}, {
    'limit': 0
}, {
    'limit': 101
}])
def test_history_bounds_are_validated_before_database_access(
        monkeypatch, kwargs):
    monkeypatch.setattr(
        placement_history, '_postgres_engine',
        lambda: pytest.fail('invalid inputs must not access the database'))

    with pytest.raises(ValueError):
        placement_history.get_history('svc', 'hash-a', **kwargs)


def test_non_postgres_history_is_explicitly_unavailable(monkeypatch):
    monkeypatch.setattr(placement_history, '_postgres_engine', lambda: None)

    history = placement_history.get_history('svc', 'hash-a')

    assert history == {
        'available': False,
        'retention_hours': 24,
        'outcome_counts': {},
        'events': [],
        'next_cursor': None,
    }
