"""Tests for external LB rollout safety (W5): readiness + graceful drain.

The LB must not report ready until it has synced at least once (never route to
a cold LB), and on drain it must fail readiness so k8s pulls it from the
Service before in-flight requests finish.
"""
# pylint: disable=invalid-name,protected-access
import asyncio
import signal as _signal
from unittest import mock

import fastapi
import pytest
import uvicorn

from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import load_balancer


def _make_lb():
    return load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001', load_balancer_port=30001)


def test_not_ready_before_first_sync():
    lb = _make_lb()
    assert lb._is_ready_to_serve() is False


def test_ready_after_first_sync():
    lb = _make_lb()
    lb._ready = True  # what a successful _sync_with_controller_once sets.
    assert lb._is_ready_to_serve() is True


def test_draining_fails_readiness_even_when_ready():
    lb = _make_lb()
    lb._ready = True
    lb._begin_draining()
    assert lb._draining is True
    assert lb._is_ready_to_serve() is False


def test_begin_draining_is_idempotent():
    lb = _make_lb()
    lb._begin_draining()
    lb._begin_draining()
    assert lb._draining is True


def test_begin_draining_flushes_pending_request_history_once():

    async def _scenario():
        lb = _make_lb()
        lb._request_aggregator.add(None)
        flushed = asyncio.Event()

        async def _flush():
            flushed.set()

        with mock.patch.object(lb,
                               '_flush_request_history_on_drain',
                               side_effect=_flush) as flush:
            lb._begin_draining()
            await asyncio.wait_for(flushed.wait(), timeout=1)
            lb._begin_draining()
            await asyncio.sleep(0)
        flush.assert_awaited_once_with()
        assert not lb._background_tasks

    asyncio.run(_scenario())


def test_drain_history_flush_reschedules_for_late_classifications():

    async def _scenario():
        lb = _make_lb()
        first_flush_started = asyncio.Event()
        release_first_flush = asyncio.Event()
        flush_count = 0

        async def _flush():
            nonlocal flush_count
            flush_count += 1
            if flush_count == 1:
                first_flush_started.set()
                await release_first_flush.wait()

        def _classify() -> None:
            request = mock.MagicMock()
            lb._mark_request_classification_eligible(request)
            lb._record_request_classification_once(request, rejected=False)

        with mock.patch.object(lb,
                               '_flush_request_history_on_drain',
                               side_effect=_flush):
            lb._begin_draining()
            await asyncio.wait_for(first_flush_started.wait(), timeout=1)

            # A classification during the first send advances the generation,
            # so the existing task must perform another pass.
            _classify()
            release_first_flush.set()
            while flush_count < 2:
                await asyncio.sleep(0)
            while lb._drain_history_flush_task is not None:
                await asyncio.sleep(0)

            # A classification after the coalesced task cleared must install a
            # successor instead of being stranded during process drain.
            _classify()
            while flush_count < 3:
                await asyncio.sleep(0)
            while lb._background_tasks:
                await asyncio.sleep(0)

        assert flush_count == 3
        assert lb._drain_history_flush_task is None

    asyncio.run(_scenario())


def test_background_loops_are_owned_until_completion():

    async def _scenario():
        lb = _make_lb()
        release = asyncio.Event()

        async def _loop():
            await release.wait()

        with mock.patch.object(lb,
                               '_sync_with_controller',
                               side_effect=_loop) as sync, \
             mock.patch.object(lb,
                               '_sync_role_with_controller',
                               side_effect=_loop) as role, \
             mock.patch.object(lb,
                               '_probe_occupancy_loop',
                               side_effect=_loop) as occupancy:
            lb._start_background_loops()
            await asyncio.sleep(0)
            assert len(lb._background_tasks) == 3
            release.set()
            await asyncio.gather(*tuple(lb._background_tasks))
            await asyncio.sleep(0)

        assert not lb._background_tasks
        sync.assert_awaited_once_with()
        role.assert_awaited_once_with()
        occupancy.assert_awaited_once_with()

    asyncio.run(_scenario())


def test_background_loop_failure_is_reported_and_released():

    async def _scenario():
        lb = _make_lb()
        reported = asyncio.Event()
        contexts = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def _handle_exception(_loop, context):
            contexts.append(context)
            reported.set()

        async def _fail():
            raise RuntimeError('controller sync loop stopped')

        async def _finish():
            return

        loop.set_exception_handler(_handle_exception)
        try:
            with mock.patch.object(lb,
                                   '_sync_with_controller',
                                   side_effect=_fail), \
                 mock.patch.object(lb,
                                   '_sync_role_with_controller',
                                   side_effect=_finish), \
                 mock.patch.object(lb,
                                   '_probe_occupancy_loop',
                                   side_effect=_finish):
                lb._start_background_loops()
                await asyncio.wait_for(reported.wait(), timeout=1)
                await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert not lb._background_tasks
        assert len(contexts) == 1
        assert contexts[0]['message'] == (
            'SkyServe load balancer background task failed')
        assert isinstance(contexts[0]['exception'], RuntimeError)
        assert str(contexts[0]['exception']) == ('controller sync loop stopped')

    asyncio.run(_scenario())


def test_background_loop_cancellation_is_quiet_and_released():

    async def _scenario():
        lb = _make_lb()
        started = asyncio.Event()
        reported = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def _handle_exception(_loop, context):
            reported.append(context)

        async def _loop():
            started.set()
            await asyncio.Event().wait()

        loop.set_exception_handler(_handle_exception)
        try:
            with mock.patch.object(lb,
                                   '_sync_with_controller',
                                   side_effect=_loop), \
                 mock.patch.object(lb,
                                   '_sync_role_with_controller',
                                   side_effect=_loop), \
                 mock.patch.object(lb,
                                   '_probe_occupancy_loop',
                                   side_effect=_loop):
                lb._start_background_loops()
                await asyncio.wait_for(started.wait(), timeout=1)
                tasks = tuple(lb._background_tasks)
                assert len(tasks) == 3
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert not lb._background_tasks
        assert not reported

    asyncio.run(_scenario())


def test_draining_rejects_new_inference_requests():
    lb = _make_lb()
    lb._begin_draining()
    with pytest.raises(fastapi.HTTPException) as exc_info:
        asyncio.run(lb._proxy_with_retries(mock.MagicMock()))
    assert exc_info.value.status_code == 503
    assert exc_info.value.headers['Retry-After']
    assert exc_info.value.headers['Connection'] == 'close'


def test_ha_armed_slot_can_serve_immediately_after_selector_patch():
    lb = load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        lb_slot='b')
    lb._lb_role = lb_ha.LbRole.ARMED
    assert lb._accepts_new_requests()


def test_ha_standby_and_draining_slots_reject_new_requests():
    lb = load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        lb_slot='b')
    assert not lb._accepts_new_requests()
    lb._lb_role = lb_ha.LbRole.DRAINING
    assert not lb._accepts_new_requests()


def test_role_draining_closes_rejected_connection():
    lb = load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        lb_slot='b')
    standby_error = lb._inactive_role_request_error()
    assert 'Connection' not in standby_error.headers
    lb._lb_role = lb_ha.LbRole.DRAINING
    draining_error = lb._inactive_role_request_error()
    assert draining_error.headers['Connection'] == 'close'


def test_drain_during_admission_rejects_before_recording_request():

    async def _scenario():
        lb = _make_lb()
        lb._queued_compatibility_demand_supported = True
        lb._apply_routing_spec({
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['A100'],
        })
        request = mock.MagicMock()

        async def _admit(_request):
            lb._begin_draining()
            return True

        with mock.patch.object(lb,
                               '_acquire_request_slot',
                               side_effect=_admit), \
             mock.patch.object(
                 lb,
                 '_release_request_slot',
                 new=mock.AsyncMock()):
            with pytest.raises(fastapi.HTTPException) as exc_info:
                await lb._proxy_with_retries(request)
        assert exc_info.value.status_code == 503
        assert lb._request_aggregator.request_history_snapshot() is None

    asyncio.run(_scenario())


def test_old_controller_mode_records_before_admission_for_safe_rollout():

    async def _scenario():
        lb = _make_lb()
        request = mock.MagicMock()

        async def _admit(_request):
            lb._begin_draining()
            return True

        with mock.patch.object(lb,
                               '_acquire_request_slot',
                               side_effect=_admit), \
             mock.patch.object(
                 lb,
                 '_release_request_slot',
                 new=mock.AsyncMock()):
            with pytest.raises(fastapi.HTTPException) as exc_info:
                await lb._proxy_with_retries(request)
        assert exc_info.value.status_code == 503
        history = lb._request_aggregator.request_history_snapshot()
        assert history is not None
        assert history['buckets'][0]['request_count'] == 1

    asyncio.run(_scenario())


def test_queue_gauge_capability_keeps_aggregate_service_arrivals():

    async def _scenario():
        lb = _make_lb()
        lb._queued_compatibility_demand_supported = True
        request = mock.MagicMock()

        async def _admit(_request):
            lb._begin_draining()
            return True

        with mock.patch.object(lb,
                               '_acquire_request_slot',
                               side_effect=_admit), \
             mock.patch.object(
                 lb,
                 '_release_request_slot',
                 new=mock.AsyncMock()):
            with pytest.raises(fastapi.HTTPException):
                await lb._proxy_with_retries(request)
        history = lb._request_aggregator.request_history_snapshot()
        assert history is not None
        assert history['buckets'][0]['request_count'] == 1

    asyncio.run(_scenario())


def test_session_id_is_pod_uid(monkeypatch):
    lb = _make_lb()
    monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'pod-uid-123')
    assert lb._get_lb_session_id() == 'pod-uid-123'


def test_session_id_missing_fails_closed(monkeypatch):
    lb = _make_lb()
    monkeypatch.delenv(constants.LB_POD_UID_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match=constants.LB_POD_UID_ENV_VAR):
        lb._get_lb_session_id()


def test_health_endpoint_status_codes():
    lb = _make_lb()
    # Cold (not yet synced) -> 503 so k8s readiness holds traffic off.
    response = asyncio.run(lb._health(None))
    assert response.status_code == 503
    assert 'connection' not in response.headers
    lb._ready = True
    response = asyncio.run(lb._health(None))
    assert response.status_code == 200
    assert 'connection' not in response.headers
    # Draining -> 503 so k8s pulls it from the Service endpoints.
    lb._begin_draining()
    response = asyncio.run(lb._health(None))
    assert response.status_code == 503
    assert response.headers['connection'] == 'close'


# --- H1 fix: _DrainableServer must suppress uvicorn's own signal handlers when
# we manage them, so the graceful-drain sequence actually runs on SIGTERM
# (uvicorn's default handler would set should_exit immediately). ---


def _make_server(own_signals):
    lb = _make_lb()
    server = load_balancer._DrainableServer(uvicorn.Config(lb._app),
                                            on_drain=lambda: None)
    server._own_signals = own_signals
    return server


def test_drainable_server_suppresses_uvicorn_signals_when_owned():
    server = _make_server(own_signals=True)
    before = _signal.getsignal(_signal.SIGTERM)
    with server.capture_signals():
        during = _signal.getsignal(_signal.SIGTERM)
    # uvicorn's handler must NOT have been installed -- we own signals.
    assert during == before


def test_drainable_server_delegates_to_uvicorn_when_not_owned():
    server = _make_server(own_signals=False)
    before = _signal.getsignal(_signal.SIGTERM)
    with server.capture_signals():
        during = _signal.getsignal(_signal.SIGTERM)
    after = _signal.getsignal(_signal.SIGTERM)
    # Fallback path: uvicorn installs its handler inside the context and
    # restores it on exit.
    assert during != before
    assert after == before


def test_first_sigterm_drains_and_second_forces_exit():
    drained = mock.Mock()
    loop = mock.Mock()
    server = load_balancer._DrainableServer(uvicorn.Config(_make_lb()._app),
                                            on_drain=drained)

    server._handle_sigterm(loop)
    drained.assert_called_once_with()
    loop.call_later.assert_called_once()
    assert server.should_exit is False
    assert server.force_exit is False

    server._handle_sigterm(loop)
    drained.assert_called_once_with()
    loop.call_later.assert_called_once()
    assert server.should_exit is True
    assert server.force_exit is True


def test_sigint_force_exit_path():
    server = _make_server(own_signals=True)
    server._force_exit()
    assert server.should_exit is True
    assert server.force_exit is True
