"""Lifecycle regressions for the legacy system-recovery route prober."""

import asyncio
import threading
import time
import types
from unittest import mock

import pytest

from sky.serve import replica_managers


def _manager():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._update_recovery_required = False
    manager._ownership_lost = threading.Event()
    manager._manager_daemon_stop = threading.Event()
    return manager


def _target(name: str):
    return types.SimpleNamespace(method='GET',
                                 probe_url=f'https://{name}/ready',
                                 headers=None,
                                 post_data=None,
                                 name=name)


def test_route_prober_owns_one_tls_setting_for_its_session(monkeypatch):
    """Reject per-target TLS context construction and connection splitting."""
    manager = _manager()
    targets = (_target('first'), _target('second'))
    results = []

    class _Registry:

        def probe_targets(self):
            return targets

        def record_probe_result(self, target, *, request_started_at, succeeded):
            del request_started_at
            results.append((target.name, succeeded))
            if len(results) == len(targets):
                manager._ownership_lost.set()

    manager._route_lease_registry = lambda: _Registry()
    ssl_setting = object()
    ssl_lookup = mock.Mock(side_effect=[
        ssl_setting,
        AssertionError('TLS setting was rebuilt for a route target'),
    ])
    monkeypatch.setattr(replica_managers.replica_tls, 'aiohttp_ssl_setting',
                        ssl_lookup)
    connector_kwargs = []
    request_kwargs = []

    class _Connector:

        def __init__(self, **kwargs):
            connector_kwargs.append(kwargs)

    class _Response:

        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class _Session:

        def __init__(self, *, connector):
            self.connector = connector

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def request(self, *_args, **kwargs):
            request_kwargs.append(kwargs)
            return _Response()

    monkeypatch.setattr(replica_managers.aiohttp, 'TCPConnector', _Connector)
    monkeypatch.setattr(replica_managers.aiohttp, 'ClientSession', _Session)

    asyncio.run(manager._system_recovery_route_probe_loop())

    assert ssl_lookup.call_count == 1
    assert connector_kwargs == [{
        'limit':
            replica_managers.serve_constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS,
        'ssl': ssl_setting,
    }]
    assert len(request_kwargs) == len(targets)
    assert all('ssl' not in kwargs for kwargs in request_kwargs)
    assert results == [('first', True), ('second', True)]


def test_cancelled_route_probe_publishes_no_negative_evidence():
    """Cancellation is UNKNOWN, not a failed readiness observation."""
    manager = _manager()
    entered = asyncio.Event()
    never = asyncio.Event()
    registry = mock.Mock()
    manager._route_lease_registry = lambda: registry

    class _Response:

        async def __aenter__(self):
            entered.set()
            await never.wait()

        async def __aexit__(self, *_args):
            return None

    session = mock.Mock()
    session.request.return_value = _Response()

    async def _run():
        task = asyncio.create_task(
            manager._probe_system_recovery_route_target(session,
                                                        _target('cancelled')))
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())

    registry.record_probe_result.assert_not_called()


def test_route_target_failure_does_not_cancel_peers_or_kill_next_round(
        monkeypatch):
    """One malformed result sink cannot terminate the persistent poller."""
    manager = _manager()
    first_targets = (_target('bad-sink'), _target('slow-peer'))
    second_target = _target('next-round')
    probe_round = 0
    recorded = []
    slow_peer_started = asyncio.Event()
    release_slow_peer = asyncio.Event()

    class _Registry:

        def probe_targets(self):
            nonlocal probe_round
            probe_round += 1
            if probe_round == 1:
                return first_targets
            return (second_target,)

        def record_probe_result(self, target, *, request_started_at, succeeded):
            del request_started_at
            if target.name == 'bad-sink':
                asyncio.get_running_loop().call_later(0.01,
                                                      release_slow_peer.set)
                raise RuntimeError('malformed target-local result')
            recorded.append((target.name, succeeded))
            if target.name == 'next-round':
                manager._ownership_lost.set()

    manager._route_lease_registry = lambda: _Registry()

    class _Response:

        status = 200

        def __init__(self, name):
            self._name = name

        async def __aenter__(self):
            if self._name == 'slow-peer':
                slow_peer_started.set()
                await release_slow_peer.wait()
            elif self._name == 'bad-sink':
                await slow_peer_started.wait()
            return self

        async def __aexit__(self, *_args):
            return None

    class _Connector:

        def __init__(self, **_kwargs):
            pass

    class _Session:

        def __init__(self, *, connector):
            self.connector = connector

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def request(self, _method, url, **_kwargs):
            name = url.removeprefix('https://').removesuffix('/ready')
            return _Response(name)

    monkeypatch.setattr(replica_managers.replica_tls, 'aiohttp_ssl_setting',
                        lambda: None)
    monkeypatch.setattr(replica_managers.aiohttp, 'TCPConnector', _Connector)
    monkeypatch.setattr(replica_managers.aiohttp, 'ClientSession', _Session)
    monkeypatch.setattr(replica_managers.serve_constants,
                        'SYSTEM_RECOVERY_ROUTE_PROBE_INTERVAL_SECONDS', 0.001)

    asyncio.run(manager._system_recovery_route_probe_loop())

    assert recorded == [('slow-peer', True), ('next-round', True)]
    assert probe_round == 2


def test_route_prober_skips_slow_slots_in_its_own_clock_domain(monkeypatch):
    """Slow rounds never overlap or turn a shifted probe clock into a sleep."""
    manager = _manager()
    target = _target('slow')
    starts = []
    request_stamps = []
    active = 0
    peak_active = 0
    completed = 0

    class _Registry:

        def probe_targets(self):
            return (target,)

        def record_probe_result(self, _target, *, request_started_at,
                                succeeded):
            nonlocal completed
            assert succeeded
            request_stamps.append(request_started_at)
            completed += 1
            if completed == 2:
                manager._ownership_lost.set()

    manager._route_lease_registry = lambda: _Registry()

    class _Response:

        status = 200

        def __init__(self, ordinal):
            self._ordinal = ordinal

        async def __aenter__(self):
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            if self._ordinal == 1:
                # Longer than two poll slots: the next round must skip both,
                # never overlap this request, then rejoin the fixed grid.
                await asyncio.sleep(0.055)
            return self

        async def __aexit__(self, *_args):
            nonlocal active
            active -= 1

    class _Connector:

        def __init__(self, **_kwargs):
            pass

    class _Session:

        def __init__(self, *, connector):
            self.connector = connector

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def request(self, _method, _url, **_kwargs):
            starts.append(asyncio.get_running_loop().time())
            return _Response(len(starts))

    # The request-evidence clock deliberately has an unrelated origin. The
    # scheduling grid must use the event loop's own monotonic clock.
    monkeypatch.setattr(replica_managers, 'time',
                        types.SimpleNamespace(monotonic=lambda: 10_000_000.0))
    monkeypatch.setattr(replica_managers.replica_tls, 'aiohttp_ssl_setting',
                        lambda: None)
    monkeypatch.setattr(replica_managers.aiohttp, 'TCPConnector', _Connector)
    monkeypatch.setattr(replica_managers.aiohttp, 'ClientSession', _Session)
    monkeypatch.setattr(replica_managers.serve_constants,
                        'SYSTEM_RECOVERY_ROUTE_PROBE_INTERVAL_SECONDS', 0.02)

    started_at = time.monotonic()

    async def _run():
        await asyncio.wait_for(manager._system_recovery_route_probe_loop(),
                               timeout=0.5)

    asyncio.run(_run())

    assert time.monotonic() - started_at < 0.5
    assert len(starts) == 2
    assert starts[1] - starts[0] >= 0.055
    assert peak_active == 1
    assert request_stamps == [10_000_000.0, 10_000_000.0]
