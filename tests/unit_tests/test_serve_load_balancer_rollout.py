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


def test_draining_rejects_new_inference_requests():
    lb = _make_lb()
    lb._begin_draining()
    with pytest.raises(fastapi.HTTPException) as exc_info:
        asyncio.run(lb._proxy_with_retries(mock.MagicMock()))
    assert exc_info.value.status_code == 503
    assert exc_info.value.headers['Retry-After']


def test_drain_during_admission_rejects_before_recording_request():

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
        assert lb._request_aggregator.request_history_snapshot() is None

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
    assert asyncio.run(lb._health(None)).status_code == 503
    lb._ready = True
    assert asyncio.run(lb._health(None)).status_code == 200
    # Draining -> 503 so k8s pulls it from the Service endpoints.
    lb._begin_draining()
    assert asyncio.run(lb._health(None)).status_code == 503


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
