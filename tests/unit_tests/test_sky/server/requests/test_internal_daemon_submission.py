"""SQLite coverage for one-way legacy internal-daemon row retirement."""

# pylint: disable=protected-access,redefined-outer-name,unused-argument
import unittest.mock as mock

import pytest
import pytest_asyncio

from sky.server import daemons
from sky.server.requests import payloads
from sky.server.requests import requests
from sky.server.requests.requests import RequestStatus
from sky.server.requests.requests import ScheduleType


def _dummy_event():
    return None


@pytest_asyncio.fixture()
async def isolated_database(tmp_path):
    temp_db_path = tmp_path / 'requests.db'
    temp_log_path = tmp_path / 'logs'
    temp_log_path.mkdir()
    with mock.patch('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                    str(temp_db_path)), \
         mock.patch('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                    str(temp_log_path)):
        await requests.close_db_async()
        yield tmp_path
        await requests.close_db_async()


def _make_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='sky.test',
        entrypoint=_dummy_event,
        request_body=payloads.RequestBody(),
        status=RequestStatus.PENDING,
        created_at=0.0,
        schedule_type=ScheduleType.SHORT,
        user_id='test-user',
    )


@pytest.mark.asyncio
async def test_sqlite_retires_only_explicit_legacy_inventory(isolated_database):
    legacy_id = next(iter(daemons.LEGACY_REQUEST_DAEMON_IDS))
    user_suffix_id = 'user-selected-daemon'
    normal_id = 'normal-request'
    for request_id in (legacy_id, user_suffix_id, normal_id):
        assert await requests.create_if_not_exists_async(
            _make_request(request_id))

    backend = requests.request_storage.get_request_backend()
    assert backend.retire_legacy_internal_daemon_rows() == 1

    assert await requests.get_request_async(legacy_id) is None
    assert await requests.get_request_async(user_suffix_id) is not None
    assert await requests.get_request_async(normal_id) is not None


@pytest.mark.asyncio
async def test_sqlite_retirement_is_idempotent(isolated_database):
    legacy_id = next(iter(daemons.LEGACY_REQUEST_DAEMON_IDS))
    assert await requests.create_if_not_exists_async(_make_request(legacy_id))
    backend = requests.request_storage.get_request_backend()

    assert backend.retire_legacy_internal_daemon_rows() == 1
    assert backend.retire_legacy_internal_daemon_rows() == 0


@pytest.mark.asyncio
async def test_close_db_async_is_idempotent(isolated_database):
    del isolated_database
    await requests.get_request_async('missing-request')

    await requests.close_db_async()
    await requests.close_db_async()
