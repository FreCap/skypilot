"""Unit tests for the async PostgreSQL singleton session lifecycle."""
# pylint: disable=protected-access

import asyncio
import types
from typing import Any

import pytest

from sky.server.requests import postgres as request_postgres


class _ScalarResult:
    """Minimal async SQLAlchemy scalar-result fake."""

    def __init__(self, value: bool):
        self._value = value

    def scalar_one(self) -> bool:
        return self._value


class _Connection:
    """Event-recording async connection fake."""

    def __init__(
        self,
        events: list[str],
        *,
        acquired: bool,
        liveness_committed: asyncio.Event | None = None,
        acquire_failure: str | None = None,
    ):
        self._events = events
        self._acquired = acquired
        self._liveness_committed = liveness_committed
        self._acquire_failure = acquire_failure
        self._last_operation: str | None = None

    async def __aenter__(self) -> '_Connection':
        self._events.append('connection_enter')
        return self

    async def __aexit__(self, *unused_args: Any) -> None:
        self._events.append('connection_close')

    async def execute(self,
                      statement: Any,
                      unused_parameters: Any = None) -> _ScalarResult:
        sql = str(statement)
        if 'pg_try_advisory_lock' in sql:
            self._last_operation = 'acquire'
            self._events.append('acquire_query')
            if self._acquire_failure == 'query':
                raise RuntimeError('acquisition query failed')
            return _ScalarResult(self._acquired)
        if 'pg_advisory_unlock' in sql:
            self._last_operation = 'unlock'
            self._events.append('unlock_query')
            return _ScalarResult(True)
        assert sql == 'SELECT 1'
        self._last_operation = 'liveness'
        self._events.append('liveness_query')
        return _ScalarResult(True)

    async def commit(self) -> None:
        operation = self._last_operation
        assert operation is not None
        self._events.append(f'{operation}_commit')
        if operation == 'acquire' and self._acquire_failure == 'commit':
            raise RuntimeError('acquisition commit failed')
        if operation == 'liveness' and self._liveness_committed is not None:
            self._liveness_committed.set()


class _Engine:
    """Event-recording async engine fake."""

    def __init__(self, events: list[str], connection: _Connection):
        self._events = events
        self._connection = connection

    def connect(self) -> _Connection:
        self._events.append('connect')
        return self._connection


def _cancel_on_sleep_asyncio(events: list[str]) -> types.SimpleNamespace:

    async def sleep(unused_delay: float) -> None:
        events.append('retry_sleep')
        raise asyncio.CancelledError

    return types.SimpleNamespace(
        CancelledError=asyncio.CancelledError,
        Task=asyncio.Task,
        create_task=asyncio.create_task,
        sleep=sleep,
        wait=asyncio.wait,
    )


def _install_engine(monkeypatch: pytest.MonkeyPatch, engine: _Engine) -> None:

    async def get_engine() -> _Engine:
        return engine

    monkeypatch.setattr(request_postgres, '_get_async_engine', get_engine)


def _assert_before(events: list[str], first: str, second: str) -> None:
    assert events.index(first) < events.index(second), events


def test_distributed_singleton_commits_each_session_operation_in_order(
        monkeypatch: pytest.MonkeyPatch) -> None:

    async def exercise() -> list[str]:
        events: list[str] = []
        liveness_committed = asyncio.Event()
        owned_started = asyncio.Event()
        owned_release = asyncio.Event()
        connection = _Connection(events,
                                 acquired=True,
                                 liveness_committed=liveness_committed)
        _install_engine(monkeypatch, _Engine(events, connection))

        async def owned() -> None:
            events.append('owned_start')
            owned_started.set()
            try:
                await owned_release.wait()
            except asyncio.CancelledError:
                events.append('owned_cancelled')
                raise

        def factory() -> Any:
            events.append('task_factory')
            return owned()

        singleton = asyncio.create_task(
            request_postgres.run_distributed_singleton(
                'ordered-singleton',
                factory,
                connection_check_interval_seconds=0.001))
        await asyncio.wait_for(owned_started.wait(), timeout=1)
        await asyncio.wait_for(liveness_committed.wait(), timeout=1)
        singleton.cancel()
        with pytest.raises(asyncio.CancelledError):
            await singleton
        return events

    events = asyncio.run(exercise())
    _assert_before(events, 'acquire_query', 'acquire_commit')
    _assert_before(events, 'acquire_commit', 'task_factory')
    _assert_before(events, 'liveness_query', 'liveness_commit')
    _assert_before(events, 'owned_cancelled', 'unlock_query')
    _assert_before(events, 'unlock_query', 'unlock_commit')
    _assert_before(events, 'unlock_commit', 'connection_close')
    assert events.count('liveness_query') == events.count('liveness_commit')


def test_distributed_singleton_closes_nonwinner_before_retry_sleep(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    connection = _Connection(events, acquired=False)
    _install_engine(monkeypatch, _Engine(events, connection))
    monkeypatch.setattr(request_postgres, 'asyncio',
                        _cancel_on_sleep_asyncio(events))

    def factory() -> Any:
        events.append('task_factory')
        raise AssertionError('a nonwinner must not create owned work')

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            request_postgres.run_distributed_singleton('losing-singleton',
                                                       factory))

    _assert_before(events, 'acquire_query', 'acquire_commit')
    _assert_before(events, 'acquire_commit', 'connection_close')
    _assert_before(events, 'connection_close', 'retry_sleep')
    assert 'task_factory' not in events


@pytest.mark.parametrize('failure', ['query', 'commit'])
def test_distributed_singleton_acquisition_failure_cannot_start_work(
        monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    events: list[str] = []
    connection = _Connection(events, acquired=True, acquire_failure=failure)
    _install_engine(monkeypatch, _Engine(events, connection))
    monkeypatch.setattr(request_postgres, 'asyncio',
                        _cancel_on_sleep_asyncio(events))

    def factory() -> Any:
        events.append('task_factory')
        raise AssertionError('failed acquisition must not create owned work')

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            request_postgres.run_distributed_singleton('failed-singleton',
                                                       factory))

    _assert_before(events, 'connection_close', 'retry_sleep')
    assert 'task_factory' not in events
    if failure == 'commit':
        _assert_before(events, 'acquire_query', 'acquire_commit')
