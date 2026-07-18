"""Tests for the serving-loop lag-debug configuration in sky.server.uvicorn.

The lag threshold must be applied to the loop created by asyncio.run() (the
loop that actually serves), from inside the serving coroutine. Configuring a
loop before asyncio.run() would target a bystander loop and, on Python 3.14+,
raise RuntimeError outright.
"""
# pylint: disable=protected-access
import asyncio
import multiprocessing
import sqlite3

import fastapi
import uvicorn

from sky.server import constants as server_constants
from sky.server import uvicorn as sky_uvicorn
from sky.server.requests import requests
from sky.server.requests import storage as request_storage


def _run_server_with_failed_lifespan(request_db_path: str) -> None:
    """Run a worker whose lifespan fails after opening the async request DB."""
    server_constants.API_SERVER_REQUEST_DB_PATH = request_db_path
    requests._DB = None  # pylint: disable=protected-access
    # The spawned process must select the SQLite backend independently of the
    # parent pytest process.
    request_storage._storage_backend = None  # pylint: disable=protected-access

    class FailedLifespan:

        async def __aenter__(self):
            backend = request_storage.get_request_backend()
            await backend.get_request_async('missing-request')
            raise sqlite3.OperationalError('database is locked')

        async def __aexit__(self, exc_type, exc, traceback):
            del exc_type, exc, traceback
            return False

    def failed_lifespan(_app):
        del _app
        return FailedLifespan()

    app = fastapi.FastAPI(lifespan=failed_lifespan)
    config = uvicorn.Config(app,
                            host='127.0.0.1',
                            port=0,
                            workers=1,
                            log_config=None)
    sky_uvicorn.Server(config).run()


def test_lag_config_applies_to_running_loop():
    state = {}

    async def probe():
        sky_uvicorn._configure_running_loop_lag_debug(0.25)
        loop = asyncio.get_running_loop()
        state['debug'] = loop.get_debug()
        state['slow'] = loop.slow_callback_duration

    asyncio.run(probe())
    assert state['debug'] is True
    assert state['slow'] == 0.25


def test_lag_config_noop_when_threshold_unset():
    state = {}

    async def probe():
        sky_uvicorn._configure_running_loop_lag_debug(None)
        loop = asyncio.get_running_loop()
        state['debug'] = loop.get_debug()

    asyncio.run(probe())
    assert state['debug'] is False


def test_lag_config_requires_running_loop():
    # Outside a running loop this must raise instead of silently configuring
    # a bystander loop (the pre-fix behavior on Python <= 3.13).
    try:
        sky_uvicorn._configure_running_loop_lag_debug(0.25)
    except RuntimeError:
        pass
    else:
        raise AssertionError('expected RuntimeError with no running loop')


def test_startup_failure_does_not_leave_worker_process_alive(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=_run_server_with_failed_lifespan,
                          args=(str(tmp_path / 'requests.db'),))
    process.start()
    process.join(timeout=15)
    still_alive = process.is_alive()
    if still_alive:
        process.kill()
        process.join(timeout=5)

    assert not still_alive
    assert process.exitcode == 0
