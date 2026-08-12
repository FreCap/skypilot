"""Unit tests for diagnostic ordinary-launch handoff telemetry."""
# pylint: disable=protected-access

import asyncio
import queue
import threading
import uuid

import pytest
import sqlalchemy

from sky.serve import ordinary_launch_handoff

_RECORD_ID = '11111111-1111-4111-8111-111111111111'
_ROUTE_EPOCH = '22222222-2222-4222-8222-222222222222'


def _launch_fence() -> dict[str, object]:
    return {
        'sky_serve_service_name': 'svc',
        'sky_serve_service_hash': 'incarnation-a',
        'sky_serve_service_version': 2,
        'sky_serve_controller_pid': 123,
        'sky_serve_controller_ip': '10.0.0.1',
    }


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
        'owner_loss_cancel_requested',
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
    assert ordinary_launch_handoff.RETENTION_PRUNE_INTERVAL_SECONDS == 300
    assert ordinary_launch_handoff.RETENTION_PRUNE_BATCH_SIZE == 1000
    assert ordinary_launch_handoff.DELIVERY_METRIC_NAME == (
        'sky_serve_ordinary_launch_handoff_delivery_total')


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


def test_redacted_digest_failure_is_fail_open():

    class _BadRepr:

        def __repr__(self):
            raise RuntimeError('repr unavailable')

    class _BadEncode(str):

        def encode(self, *args, **kwargs):
            del args, kwargs
            raise RuntimeError('encoding unavailable')

    assert ordinary_launch_handoff.redacted_input_digest(
        'resources: {}', {'typed': _BadRepr()}) is None
    assert ordinary_launch_handoff.redacted_input_digest(
        _BadEncode('resources: {}'), {}) is None


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


def test_verified_publication_carries_only_closed_launch_fence(monkeypatch):
    pending = []

    def _enqueue(event, *, required_launch_fence=None):
        pending.append((event, required_launch_fence))
        return True

    monkeypatch.setattr(ordinary_launch_handoff, '_enqueue_event', _enqueue)
    fence = {
        **_launch_fence(),
        'sky_serve_ordinary_launch_handoff': {
            'untrusted': 'nested-value'
        },
    }

    assert ordinary_launch_handoff.emit_verified_request_publication(
        service_name='svc',
        service_version=2,
        replica_id=7,
        replica_record_id=_RECORD_ID,
        controller_route_epoch=_ROUTE_EPOCH,
        ordinary_request_id='request-a',
        input_digest='a' * 64,
        launch_fence=fence)

    event, required_fence = pending[0]
    assert event.event_kind == 'request_published'
    assert required_fence == _launch_fence()
    assert 'sky_serve_ordinary_launch_handoff' not in required_fence


@pytest.mark.parametrize('fence', [
    {
        **_launch_fence(), 'sky_serve_service_name': 'other'
    },
    {
        key: value
        for key, value in _launch_fence().items()
        if key != 'sky_serve_controller_ip'
    },
])
def test_verified_publication_rejects_mismatched_or_incomplete_fence(
        monkeypatch, fence):
    monkeypatch.setattr(
        ordinary_launch_handoff, '_enqueue_event',
        lambda *args, **kwargs: pytest.fail('invalid provenance was queued'))

    assert not ordinary_launch_handoff.emit_verified_request_publication(
        service_name='svc',
        service_version=2,
        replica_id=7,
        replica_record_id=_RECORD_ID,
        controller_route_epoch=_ROUTE_EPOCH,
        ordinary_request_id='request-a',
        input_digest='a' * 64,
        launch_fence=fence)


def test_pending_publication_requires_fresh_durable_fence(monkeypatch):
    event = ordinary_launch_handoff._event(**_event_kwargs())
    pending = ordinary_launch_handoff._PendingEvent(
        event=event, required_launch_fence=_launch_fence())
    outcomes = []
    monkeypatch.setattr(ordinary_launch_handoff, '_provenance_rejected_count',
                        0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_provenance_check_failure_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_record_delivery_outcome',
                        outcomes.append)
    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'service_replica_launch_fence_holds',
                        lambda unused_fence: False)

    assert not ordinary_launch_handoff._pending_event_provenance_holds(pending)
    assert ordinary_launch_handoff._provenance_rejected_count == 1
    assert outcomes == ['event_provenance_rejected']

    def _fail_check(unused_fence):
        raise RuntimeError('database unavailable')

    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'service_replica_launch_fence_holds', _fail_check)
    assert not ordinary_launch_handoff._pending_event_provenance_holds(pending)
    assert ordinary_launch_handoff._provenance_check_failure_count == 1
    assert outcomes[-1] == 'event_provenance_check_failed'


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
        'evidence_is_lower_bound': True,
        'fleet_delivery_metric':
            ('sky_serve_ordinary_launch_handoff_delivery_total'),
        'fleet_delivery_outcomes': list(
            ordinary_launch_handoff.DELIVERY_OUTCOMES),
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
            'backend_unavailable':
                ordinary_launch_handoff._backend_unavailable_count,
            'retention_prune_failures':
                ordinary_launch_handoff._retention_prune_failure_count,
            'provenance_rejections':
                ordinary_launch_handoff._provenance_rejected_count,
            'provenance_check_failures':
                ordinary_launch_handoff._provenance_check_failure_count,
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
    monkeypatch.setattr(ordinary_launch_handoff, '_backend_unavailable_count',
                        0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_retention_prune_failure_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_provenance_rejected_count',
                        0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_provenance_check_failure_count', 0)
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
        'backend_unavailable': 0,
        'retention_prune_failures': 0,
        'provenance_rejections': 0,
        'provenance_check_failures': 0,
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


@pytest.mark.asyncio
async def test_retention_pruning_is_cadenced_and_fail_open(monkeypatch):
    calls = []
    monkeypatch.setattr(ordinary_launch_handoff, '_prune_expired_events',
                        lambda: calls.append('pruned'))
    sleeps = []

    async def _sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(ordinary_launch_handoff.asyncio, 'sleep', _sleep)
    with pytest.raises(asyncio.CancelledError):
        await ordinary_launch_handoff.retention_daemon()

    assert sleeps == [300, 300]
    assert calls == ['pruned']

    outcomes = []
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_retention_prune_failure_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff, '_record_delivery_outcome',
                        outcomes.append)

    def _fail_prune():
        raise RuntimeError('maintenance unavailable')

    monkeypatch.setattr(ordinary_launch_handoff, '_prune_expired_events',
                        _fail_prune)
    sleeps.clear()
    with pytest.raises(asyncio.CancelledError):
        await ordinary_launch_handoff.retention_daemon()
    assert ordinary_launch_handoff._retention_prune_failure_count == 1
    assert outcomes == ['retention_prune_failed']


def test_delivery_metric_outcomes_cover_success_and_loss(monkeypatch):
    outcomes = []
    monkeypatch.setattr(ordinary_launch_handoff, '_record_delivery_outcome',
                        outcomes.append)
    pending_events = queue.Queue(maxsize=1)
    monkeypatch.setattr(ordinary_launch_handoff, '_pending_events',
                        pending_events)
    monkeypatch.setattr(ordinary_launch_handoff, '_ensure_writer', lambda: None)
    monkeypatch.setattr(ordinary_launch_handoff, '_event_queue_drop_count', 0)
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_terminal_observation_queue_drop_count', 0)

    event = ordinary_launch_handoff._event(**_event_kwargs())
    assert ordinary_launch_handoff._enqueue_event(event)
    assert not ordinary_launch_handoff._enqueue_event(event)

    assert outcomes == ['event_enqueued', 'event_queue_dropped']


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


def test_terminal_observer_pool_keeps_queue_live_after_one_hung_lookup(
        monkeypatch):
    pending = queue.Queue()
    monkeypatch.setattr(ordinary_launch_handoff,
                        '_pending_terminal_observations', pending)
    first_started = threading.Event()
    release_first = threading.Event()
    first_finished = threading.Event()
    second_emitted = threading.Event()
    stop = threading.Event()

    def _hung_lookup(unused_request_id):
        first_started.set()
        assert release_first.wait(timeout=5)
        first_finished.set()
        return None

    workers = [
        threading.Thread(target=ordinary_launch_handoff._terminal_observer_loop,
                         args=(stop,),
                         daemon=True)
        for _ in range(ordinary_launch_handoff.TERMINAL_OBSERVER_WORKERS)
    ]
    for worker in workers:
        worker.start()
    try:
        pending.put(
            ordinary_launch_handoff._TerminalObservation(
                request_id='request-hung',
                lookup=_hung_lookup,
                emit=lambda unused_status: None))
        assert first_started.wait(timeout=2)
        pending.put(
            ordinary_launch_handoff._TerminalObservation(
                request_id='request-live',
                lookup=lambda unused_request_id: 'SUCCEEDED',
                emit=lambda unused_status: second_emitted.set()))
        assert second_emitted.wait(timeout=2)
    finally:
        release_first.set()
        assert first_finished.wait(timeout=2)
        stop.set()
        for worker in workers:
            worker.join(timeout=2)
            assert not worker.is_alive()
    assert pending.unfinished_tasks == 0


@pytest.mark.parametrize('days', [0, 61, True, 1.5])
def test_summary_bounds_fail_before_database_access(monkeypatch, days):
    monkeypatch.setattr(
        ordinary_launch_handoff, '_postgres_engine',
        lambda: pytest.fail('invalid query must not access PostgreSQL'))

    with pytest.raises(ValueError, match='days must be an integer'):
        ordinary_launch_handoff.get_summary(days=days)
