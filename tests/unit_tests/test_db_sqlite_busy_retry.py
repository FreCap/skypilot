"""Tests for retrying SQLite writes refused due to write contention."""

import asyncio
import sqlite3
from unittest import mock

import pytest

from sky.utils.db import db_utils


def test_identifies_only_contention_errors():
    assert db_utils.is_sqlite_busy_error(
        sqlite3.OperationalError('database is locked'))
    assert db_utils.is_sqlite_busy_error(
        sqlite3.OperationalError('database is busy'))
    assert not db_utils.is_sqlite_busy_error(
        sqlite3.OperationalError('no such table: requests'))
    assert not db_utils.is_sqlite_busy_error(ValueError('database is locked'))


def test_retries_until_the_write_succeeds():
    attempts = []

    @db_utils.retry_on_sqlite_busy
    def write():
        attempts.append(1)
        if len(attempts) < 3:
            raise sqlite3.OperationalError('database is locked')
        return 'committed'

    with mock.patch.object(db_utils.time, 'sleep') as sleep:
        assert write() == 'committed'

    assert len(attempts) == 3
    # Backoff grows between attempts rather than hammering the lock.
    waits = [call.args[0] for call in sleep.call_args_list]
    assert waits == sorted(waits) and len(waits) == 2


def test_gives_up_and_surfaces_the_original_error():

    @db_utils.retry_on_sqlite_busy
    def write():
        raise sqlite3.OperationalError('database is locked')

    with mock.patch.object(db_utils.time, 'sleep'):
        with pytest.raises(sqlite3.OperationalError,
                           match='database is locked'):
            write()


def test_does_not_retry_a_real_error():
    attempts = []

    @db_utils.retry_on_sqlite_busy
    def write():
        attempts.append(1)
        raise sqlite3.OperationalError('no such table: requests')

    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        write()
    # A schema error will never resolve itself; retrying only delays it.
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_async_retries_until_the_write_succeeds():
    attempts = []

    @db_utils.retry_on_sqlite_busy_async
    async def write():
        attempts.append(1)
        if len(attempts) < 3:
            raise sqlite3.OperationalError('database is locked')
        return 'committed'

    with mock.patch.object(asyncio, 'sleep', new=mock.AsyncMock()):
        assert await write() == 'committed'

    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_async_does_not_retry_a_real_error():
    attempts = []

    @db_utils.retry_on_sqlite_busy_async
    async def write():
        attempts.append(1)
        raise sqlite3.OperationalError('no such table: requests')

    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        await write()
    assert len(attempts) == 1
