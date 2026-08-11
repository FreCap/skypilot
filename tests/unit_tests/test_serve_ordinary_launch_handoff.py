"""Unit tests for diagnostic ordinary-launch handoff telemetry."""
# pylint: disable=protected-access

import queue
import uuid

import pytest
import sqlalchemy

from sky.serve import ordinary_launch_handoff

_RECORD_ID = '11111111-1111-4111-8111-111111111111'
_ROUTE_EPOCH = '22222222-2222-4222-8222-222222222222'


def _event_kwargs(**overrides):
    values = {
        'event_kind': ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
        'service_name': 'svc',
        'service_version': 2,
        'replica_id': 7,
        'replica_record_id': _RECORD_ID,
        'controller_route_epoch': _ROUTE_EPOCH,
        'ordinary_request_id': 'request-a',
        'service_job_id': None,
        'terminal_status': None,
        'input_digest': 'a' * 64,
    }
    values.update(overrides)
    return values


def test_closed_event_kinds_and_table_contract():
    assert ordinary_launch_handoff.EVENT_KIND_VALUES == (
        'request_published',
        'controller_start_nonterminal',
        'restart_redrive',
        'owner_loss_cancelled',
        'api_terminal',
        'serve_result_projected',
        'service_job_observed',
        'cleanup_retry_after_route_epoch_change',
    )
    table = ordinary_launch_handoff.serve_ordinary_launch_handoff_events_table
    assert tuple(table.c.keys()) == (
        'event_id',
        'observed_at',
        'event_kind',
        'service_name',
        'service_version',
        'replica_id',
        'replica_record_id',
        'controller_route_epoch',
        'ordinary_request_id',
        'service_job_id',
        'terminal_status',
        'input_digest',
    )
    assert table.c.observed_at.server_default is not None
    assert ordinary_launch_handoff.RETENTION_DAYS == 60


def test_redacted_digest_is_deterministic_and_order_independent():
    left = ordinary_launch_handoff.redacted_input_digest(
        'resources:\n  cpus: 4\n', {
            'region': 'us-east-1',
            'cpus': 4,
        })
    right = ordinary_launch_handoff.redacted_input_digest(
        'resources:\n  cpus: 4\n', {
            'cpus': 4,
            'region': 'us-east-1',
        })
    changed = ordinary_launch_handoff.redacted_input_digest(
        'resources:\n  cpus: 8\n', {
            'cpus': 4,
            'region': 'us-east-1',
        })

    assert left == right
    assert left != changed
    assert len(left) == 64
    assert 'us-east-1' not in left


def test_emit_enqueues_validated_uuid_event(monkeypatch):
    events = []
    monkeypatch.setattr(ordinary_launch_handoff, '_enqueue_event',
                        lambda event: events.append(event) is None)

    assert ordinary_launch_handoff.emit_event(**_event_kwargs())

    assert len(events) == 1
    event = events[0]
    assert isinstance(event.event_id, uuid.UUID)
    assert event.replica_record_id == uuid.UUID(_RECORD_ID)
    assert event.controller_route_epoch == uuid.UUID(_ROUTE_EPOCH)
    assert event.event_kind == 'request_published'
    assert 'observed_at' not in event.insert_values()


@pytest.mark.parametrize('terminal_status',
                         list(ordinary_launch_handoff.TerminalStatus))
def test_api_terminal_requires_and_retains_closed_status(
        monkeypatch, terminal_status):
    events = []
    monkeypatch.setattr(ordinary_launch_handoff, '_enqueue_event',
                        lambda event: events.append(event) is None)

    assert ordinary_launch_handoff.emit_event(**_event_kwargs(
        event_kind=ordinary_launch_handoff.EventKind.API_TERMINAL,
        terminal_status=terminal_status))

    assert events[0].terminal_status == terminal_status.value


@pytest.mark.parametrize('override', [
    {
        'event_kind': 'request_published'
    },
    {
        'replica_record_id': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'.upper()
    },
    {
        'service_version': True
    },
    {
        'ordinary_request_id': ''
    },
    {
        'service_job_id': 0
    },
    {
        'input_digest': 'not-a-digest'
    },
    {
        'event_kind': ordinary_launch_handoff.EventKind.API_TERMINAL,
        'terminal_status': None,
    },
    {
        'terminal_status': ordinary_launch_handoff.TerminalStatus.FAILED,
    },
])
def test_invalid_diagnostic_event_fails_open(monkeypatch, override):
    monkeypatch.setattr(
        ordinary_launch_handoff, '_enqueue_event',
        lambda unused_event: pytest.fail('invalid event must not be queued'))

    assert not ordinary_launch_handoff.emit_event(**_event_kwargs(**override))


def test_non_postgres_write_and_summary_are_explicitly_unavailable(monkeypatch):
    sqlite = sqlalchemy.create_engine('sqlite://')
    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'get_database_engine', lambda: sqlite)
    event = ordinary_launch_handoff._event(**_event_kwargs())

    assert not ordinary_launch_handoff._write_event(event)
    assert ordinary_launch_handoff.get_summary() == {
        'available': False,
        'retention_days': 60,
        'process_local_delivery': {
            'scope': 'current_process_since_module_import',
            'queue_drops':
                (ordinary_launch_handoff._event_queue_drop_count +
                 ordinary_launch_handoff._terminal_observation_queue_drop_count
                ),
            'event_queue_drops':
                ordinary_launch_handoff._event_queue_drop_count,
            'terminal_observation_queue_drops':
                ordinary_launch_handoff._terminal_observation_queue_drop_count,
            'writer_failures': ordinary_launch_handoff._write_failure_count,
            'terminal_lookup_failures':
                (ordinary_launch_handoff._terminal_lookup_failure_count),
            'pending_events': ordinary_launch_handoff._pending_events.qsize(),
            'pending_terminal_observations':
                ordinary_launch_handoff._pending_terminal_observations.qsize(),
        },
    }


def test_process_local_delivery_failures_are_counted_and_scoped(monkeypatch):
    pending_events = queue.Queue(maxsize=1)
    pending_events.put_nowait(object())
    monkeypatch.setattr(ordinary_launch_handoff, '_pending_events',
                        pending_events)
    monkeypatch.setattr(ordinary_launch_handoff, '_event_queue_drop_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_terminal_observation_queue_drop_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_write_failure_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_terminal_lookup_failure_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_ensure_writer', lambda: None)
    sqlite = sqlalchemy.create_engine('sqlite://')
    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'get_database_engine', lambda: sqlite)

    event = ordinary_launch_handoff._event(**_event_kwargs())
    assert not ordinary_launch_handoff._enqueue_event(event)
    ordinary_launch_handoff._log_write_failure(RuntimeError('write failed'))
    ordinary_launch_handoff._log_terminal_lookup_failure(
        RuntimeError('lookup failed'))

    delivery = ordinary_launch_handoff.get_summary()['process_local_delivery']
    assert delivery == {
        'scope': 'current_process_since_module_import',
        'queue_drops': 1,
        'event_queue_drops': 1,
        'terminal_observation_queue_drops': 0,
        'writer_failures': 1,
        'terminal_lookup_failures': 1,
        'pending_events': 1,
        'pending_terminal_observations':
            ordinary_launch_handoff._pending_terminal_observations.qsize(),
    }


def test_terminal_observation_queue_drop_is_counted(monkeypatch):
    pending_observations = queue.Queue(maxsize=1)
    pending_observations.put_nowait(object())
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_pending_terminal_observations', pending_observations)
    monkeypatch.setattr(ordinary_launch_handoff, '_event_queue_drop_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_terminal_observation_queue_drop_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_ensure_terminal_observer',
                        lambda: None)

    assert not ordinary_launch_handoff.observe_terminal_nonblocking(
        'request-a',
        lookup=lambda unused_request_id: 'SUCCEEDED',
        emit=lambda unused_status: None)

    delivery = ordinary_launch_handoff._process_local_delivery_summary()
    assert delivery['queue_drops'] == 1
    assert delivery['event_queue_drops'] == 0
    assert delivery['terminal_observation_queue_drops'] == 1


@pytest.mark.parametrize('status', [None, 'PENDING', 'WAITING', 'RUNNING'])
def test_terminal_observer_ignores_missing_and_nonterminal_status(status):
    emitted = []
    observation = ordinary_launch_handoff._TerminalObservation(
        request_id='request-a',
        lookup=lambda unused_request_id: status,
        emit=emitted.append)

    ordinary_launch_handoff._process_terminal_observation(observation)

    assert not emitted


def test_terminal_observer_lookup_failure_is_fail_open(monkeypatch):
    emitted = []
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_terminal_lookup_failure_count', 0)

    def _failed_lookup(unused_request_id):
        raise RuntimeError('transport failed')

    observation = ordinary_launch_handoff._TerminalObservation(
        request_id='request-a', lookup=_failed_lookup, emit=emitted.append)

    ordinary_launch_handoff._process_terminal_observation(observation)

    assert not emitted
    assert ordinary_launch_handoff._terminal_lookup_failure_count == 1


@pytest.mark.parametrize('days', [0, 61, True, 1.5])
def test_summary_bounds_fail_before_database_access(monkeypatch, days):
    monkeypatch.setattr(
        ordinary_launch_handoff, '_postgres_engine',
        lambda: pytest.fail('invalid query must not access PostgreSQL'))

    with pytest.raises(ValueError, match='days must be an integer'):
        ordinary_launch_handoff.get_summary(days=days)
