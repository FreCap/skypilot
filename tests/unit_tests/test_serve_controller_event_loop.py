"""Event-loop responsiveness tests for the SkyServe controller API."""

# pylint: disable=protected-access

import asyncio
import threading
from unittest import mock

import fastapi
import httpx
import pytest

from sky.serve import constants
from sky.serve import controller


def _register_controller_routes(monkeypatch, autoscaler) -> fastapi.FastAPI:
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._app = fastapi.FastAPI()
    ctrl._service_name = 'test-service'
    ctrl._is_pool = True
    ctrl._controller_owner_fingerprint = 'owner-a'
    ctrl._autoscaler = autoscaler
    ctrl._replica_manager = mock.Mock()
    ctrl._replica_manager.spot_placer = None
    ctrl._replica_counts_snapshot = None
    ctrl._get_update_status = mock.Mock(return_value={})
    ctrl._update_lock = threading.Lock()
    ctrl._reserved_capacity_fill_enabled = False
    ctrl._host = '127.0.0.1'
    ctrl._port = 0

    for env_var in (constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                    constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                    constants.CONTROLLER_AUTH_TOKEN_ENV_VAR):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setattr(controller.thread_utils, 'start_supervised_thread',
                        mock.Mock())
    monkeypatch.setattr(controller.uvicorn, 'run', mock.Mock())
    monkeypatch.setattr(controller.os, '_exit', mock.Mock())
    ctrl.run()
    return ctrl._app


@pytest.mark.asyncio
async def test_autoscaler_info_does_not_block_controller_event_loop(
        monkeypatch):
    info_started = threading.Event()
    allow_info = threading.Event()
    info_finished = threading.Event()
    test_finished = threading.Event()

    def blocking_info():
        info_started.set()
        allow_info.wait(timeout=2)
        info_finished.set()
        return {}

    autoscaler = mock.Mock()
    autoscaler.info.side_effect = blocking_info
    app = _register_controller_routes(monkeypatch, autoscaler)

    # Prevent a regression from deadlocking the test. With an async route, the
    # blocking info call monopolizes the event loop until this fallback fires.
    def release_if_event_loop_is_blocked():
        if info_started.wait(timeout=1) and not test_finished.wait(timeout=0.5):
            allow_info.set()

    fallback_thread = threading.Thread(target=release_if_event_loop_is_blocked)
    fallback_thread.start()
    transport = httpx.ASGITransport(app=app)
    headers = {constants.CONTROLLER_OWNER_HEADER: 'owner-a'}
    info_task = None
    try:
        async with httpx.AsyncClient(transport=transport,
                                     base_url='http://test') as client:
            info_task = asyncio.create_task(
                client.get('/autoscaler/info', headers=headers))
            started = await asyncio.wait_for(asyncio.to_thread(
                info_started.wait, 1),
                                             timeout=2)
            assert started

            health_response = await client.get(
                constants.CONTROLLER_HEALTH_ENDPOINT_PATH, headers=headers)
            assert health_response.status_code == 200
            assert not info_finished.is_set()
    finally:
        test_finished.set()
        allow_info.set()
        if info_task is not None:
            await asyncio.gather(info_task, return_exceptions=True)
        fallback_thread.join(timeout=1)

    assert not fallback_thread.is_alive()
