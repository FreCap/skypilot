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


def _register_controller_routes(monkeypatch,
                                autoscaler,
                                replica_manager=None) -> fastapi.FastAPI:
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._app = fastapi.FastAPI()
    ctrl._service_name = 'test-service'
    ctrl._is_pool = True
    ctrl._controller_owner_fingerprint = 'owner-a'
    ctrl._autoscaler = autoscaler
    ctrl._replica_manager = replica_manager or mock.Mock()
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


@pytest.mark.asyncio
async def test_terminate_replica_does_not_block_controller_event_loop(
        monkeypatch):
    terminate_started = threading.Event()
    allow_terminate = threading.Event()
    terminate_finished = threading.Event()
    test_finished = threading.Event()

    def blocking_scale_down(replica_id, purge):
        assert replica_id == 17
        assert purge is False
        terminate_started.set()
        allow_terminate.wait(timeout=2)
        terminate_finished.set()

    autoscaler = mock.Mock()
    replica_manager = mock.Mock()
    replica_manager.spot_placer = None
    replica_manager.scale_down.side_effect = blocking_scale_down
    replica_info = mock.Mock(status=controller.serve_state.ReplicaStatus.READY)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=replica_info))
    app = _register_controller_routes(monkeypatch, autoscaler, replica_manager)

    # Release a regressed event-loop-blocking handler so the test fails instead
    # of hanging forever.
    def release_if_event_loop_is_blocked():
        if (terminate_started.wait(timeout=1) and
                not test_finished.wait(timeout=0.5)):
            allow_terminate.set()

    fallback_thread = threading.Thread(target=release_if_event_loop_is_blocked)
    fallback_thread.start()
    transport = httpx.ASGITransport(app=app)
    headers = {constants.CONTROLLER_OWNER_HEADER: 'owner-a'}
    terminate_task = None
    try:
        async with httpx.AsyncClient(transport=transport,
                                     base_url='http://test') as client:
            terminate_task = asyncio.create_task(
                client.post('/controller/terminate_replica',
                            headers=headers,
                            json={
                                'replica_id': 17,
                                'purge': False,
                            }))
            started = await asyncio.wait_for(asyncio.to_thread(
                terminate_started.wait, 1),
                                             timeout=2)
            assert started

            health_response = await client.get(
                constants.CONTROLLER_HEALTH_ENDPOINT_PATH, headers=headers)
            assert health_response.status_code == 200
            assert not terminate_finished.is_set()
            assert not terminate_task.done()

            allow_terminate.set()
            terminate_response = await asyncio.wait_for(terminate_task,
                                                        timeout=2)
            assert terminate_response.status_code == 200
    finally:
        test_finished.set()
        allow_terminate.set()
        if terminate_task is not None:
            await asyncio.gather(terminate_task, return_exceptions=True)
        fallback_thread.join(timeout=1)

    assert terminate_finished.is_set()
    assert not fallback_thread.is_alive()
    replica_manager.scale_down.assert_called_once_with(17, purge=False)


@pytest.mark.asyncio
async def test_terminate_replica_serializes_duplicate_admission(monkeypatch):
    terminate_started = threading.Event()
    allow_terminate = threading.Event()

    def blocking_scale_down(replica_id, purge):
        assert replica_id == 17
        assert purge is False
        terminate_started.set()
        allow_terminate.wait(timeout=2)

    autoscaler = mock.Mock()
    replica_manager = mock.Mock()
    replica_manager.spot_placer = None
    replica_manager.scale_down.side_effect = blocking_scale_down
    ready = mock.Mock(status=controller.serve_state.ReplicaStatus.READY)
    shutting_down = mock.Mock(
        status=controller.serve_state.ReplicaStatus.SHUTTING_DOWN)
    get_info = mock.Mock(side_effect=[ready, shutting_down])
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        get_info)
    app = _register_controller_routes(monkeypatch, autoscaler, replica_manager)

    transport = httpx.ASGITransport(app=app)
    headers = {constants.CONTROLLER_OWNER_HEADER: 'owner-a'}
    async with httpx.AsyncClient(transport=transport,
                                 base_url='http://test') as client:
        first = asyncio.create_task(
            client.post('/controller/terminate_replica',
                        headers=headers,
                        json={
                            'replica_id': 17,
                            'purge': False,
                        }))
        started = await asyncio.wait_for(asyncio.to_thread(
            terminate_started.wait, 1),
                                         timeout=2)
        assert started
        second = asyncio.create_task(
            client.post('/controller/terminate_replica',
                        headers=headers,
                        json={
                            'replica_id': 17,
                            'purge': False,
                        }))
        await asyncio.sleep(0)
        assert get_info.call_count == 1
        assert not second.done()

        allow_terminate.set()
        first_response, second_response = await asyncio.wait_for(asyncio.gather(
            first, second),
                                                                 timeout=2)

    assert first_response.status_code == 200
    assert second_response.status_code == 409
    assert get_info.call_count == 2
    replica_manager.scale_down.assert_called_once_with(17, purge=False)


@pytest.mark.asyncio
async def test_terminate_replica_persistence_failure_is_not_success(
        monkeypatch):
    autoscaler = mock.Mock()
    replica_manager = mock.Mock()
    replica_manager.spot_placer = None
    replica_manager.scale_down.side_effect = RuntimeError('ownership lost')
    replica_info = mock.Mock(status=controller.serve_state.ReplicaStatus.READY)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=replica_info))
    app = _register_controller_routes(monkeypatch, autoscaler, replica_manager)

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {constants.CONTROLLER_OWNER_HEADER: 'owner-a'}
    async with httpx.AsyncClient(transport=transport,
                                 base_url='http://test') as client:
        response = await client.post('/controller/terminate_replica',
                                     headers=headers,
                                     json={
                                         'replica_id': 17,
                                         'purge': False,
                                     })

    assert response.status_code == 500
    assert 'Failed method POST' in response.json()['message']


@pytest.mark.asyncio
async def test_cancelled_termination_keeps_duplicate_admission_serialized(
        monkeypatch):
    terminate_started = threading.Event()
    allow_terminate = threading.Event()

    def blocking_scale_down(replica_id, purge):
        assert replica_id == 17
        assert purge is False
        terminate_started.set()
        allow_terminate.wait(timeout=2)

    autoscaler = mock.Mock()
    replica_manager = mock.Mock()
    replica_manager.spot_placer = None
    replica_manager.scale_down.side_effect = blocking_scale_down
    ready = mock.Mock(status=controller.serve_state.ReplicaStatus.READY)
    shutting_down = mock.Mock(
        status=controller.serve_state.ReplicaStatus.SHUTTING_DOWN)
    get_info = mock.Mock(side_effect=[ready, shutting_down])
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        get_info)
    app = _register_controller_routes(monkeypatch, autoscaler, replica_manager)

    transport = httpx.ASGITransport(app=app)
    headers = {constants.CONTROLLER_OWNER_HEADER: 'owner-a'}
    async with httpx.AsyncClient(transport=transport,
                                 base_url='http://test') as client:
        first = asyncio.create_task(
            client.post('/controller/terminate_replica',
                        headers=headers,
                        json={
                            'replica_id': 17,
                            'purge': False,
                        }))
        started = await asyncio.wait_for(asyncio.to_thread(
            terminate_started.wait, 1),
                                         timeout=2)
        assert started
        first.cancel()
        second = asyncio.create_task(
            client.post('/controller/terminate_replica',
                        headers=headers,
                        json={
                            'replica_id': 17,
                            'purge': False,
                        }))
        await asyncio.sleep(0)
        assert get_info.call_count == 1
        assert not first.done()
        assert not second.done()

        allow_terminate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(first, timeout=2)
        second_response = await asyncio.wait_for(second, timeout=2)

    assert second_response.status_code == 409
    assert get_info.call_count == 2
    replica_manager.scale_down.assert_called_once_with(17, purge=False)
