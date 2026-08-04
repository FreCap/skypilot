"""Unit contracts for lock-session-bound reserved-fill persistence."""
# pylint: disable=super-init-not-called

import contextlib
from unittest import mock

import psycopg2
import pytest

from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_state
from sky.utils import locks


class _PostgresLockDouble(locks.PostgresLock):
    """PostgresLock-shaped double without opening a database connection."""

    def __init__(
        self,
        connection=None,
    ) -> None:
        self.connection = object() if connection is None else connection
        self.held = False

    @contextlib.contextmanager
    def acquire(self, blocking: bool = True):
        assert not blocking
        self.held = True
        try:
            yield self
        finally:
            self.held = False

    def run_in_lock_session(self, operation):
        assert self.held
        return operation(self.connection)


def test_persist_mints_token_on_lock_session_and_carries_it(monkeypatch):
    lock = _PostgresLockDouble()
    monkeypatch.setattr(broker.locks, 'get_lock', mock.Mock(return_value=lock))

    def advance(connection):
        assert lock.held
        assert connection is lock.connection
        return 17

    monkeypatch.setattr(serve_state, 'advance_reserved_fill_persist_token',
                        advance)

    def persist(*args, **kwargs):
        assert lock.held
        assert args == ('svc', 3, 'replica-info')
        assert kwargs['expected_lease_token'] == 17
        return True

    monkeypatch.setattr(serve_state, 'add_replica_if_round_epoch', persist)

    assert broker.persist_fill_replica('svc',
                                       3,
                                       'replica-info',
                                       pool_key='pool',
                                       expected_epoch=5)
    assert not lock.held


def test_persist_fails_closed_when_token_cannot_advance(monkeypatch):
    lock = _PostgresLockDouble()
    monkeypatch.setattr(broker.locks, 'get_lock', mock.Mock(return_value=lock))
    monkeypatch.setattr(serve_state, 'advance_reserved_fill_persist_token',
                        mock.Mock(return_value=None))
    persist = mock.Mock()
    monkeypatch.setattr(serve_state, 'add_replica_if_round_epoch', persist)

    assert not broker.persist_fill_replica(
        'svc', 3, 'replica-info', pool_key='pool', expected_epoch=5)
    persist.assert_not_called()


@pytest.mark.parametrize('failure_point', ('cursor', 'execute', 'rollback'))
def test_persist_contains_dead_lock_session_token_failure(
        monkeypatch, failure_point):

    class FailingCursor:

        def execute(self, *_args, **_kwargs):
            raise psycopg2.OperationalError('lock session died')

        def close(self):
            pass

    class FailingConnection:

        def cursor(self):
            if failure_point == 'cursor':
                raise psycopg2.InterfaceError('connection already closed')
            return FailingCursor()

        def rollback(self):
            if failure_point == 'rollback':
                raise psycopg2.InterfaceError('rollback on closed connection')

    lock = _PostgresLockDouble(FailingConnection())
    monkeypatch.setattr(broker.locks, 'get_lock', mock.Mock(return_value=lock))
    persist = mock.Mock()
    monkeypatch.setattr(serve_state, 'add_replica_if_round_epoch', persist)

    assert not broker.persist_fill_replica(
        'svc', 3, 'replica-info', pool_key='pool', expected_epoch=5)
    persist.assert_not_called()


def test_file_lock_persist_keeps_historical_tokenless_path(monkeypatch):
    lock = mock.Mock()
    lock.acquire.return_value = contextlib.nullcontext()
    monkeypatch.setattr(broker.locks, 'get_lock', mock.Mock(return_value=lock))
    advance = mock.Mock()
    monkeypatch.setattr(serve_state, 'advance_reserved_fill_persist_token',
                        advance)

    def persist(*_args, **kwargs):
        assert kwargs['expected_lease_token'] is None
        return True

    monkeypatch.setattr(serve_state, 'add_replica_if_round_epoch', persist)

    assert broker.persist_fill_replica('svc',
                                       3,
                                       'replica-info',
                                       pool_key='pool',
                                       expected_epoch=5)
    advance.assert_not_called()
