"""Concurrency contracts for the provider-independent route worker."""
# pylint: disable=protected-access

import asyncio
import dataclasses
import threading
from unittest import mock
import uuid

import pytest

from sky.serve import incremental_route_worker
from sky.serve import route_projection


def _identity():
    return route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=1,
        controller_incarnation=uuid.uuid4(),
        controller_owner_epoch=1,
        controller_pid=123,
        controller_ip='127.0.0.1')


def _target(identity, replica_id=1):
    return route_projection.RouteLeaseProbeTarget(
        identity=identity,
        replica_id=replica_id,
        replica_record_id=str(uuid.uuid4()),
        service_version=1,
        route_url='http://10.0.0.1:8000',
        readiness_path='/health',
        timeout_seconds=15,
        method='GET',
        post_data=None,
        headers=None,
        material_sha256='a' * 64,
        material_generation=1,
        revocation_generation=0)


async def _wait_for(predicate, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(
                'Timed out waiting for asynchronous condition.')
        await asyncio.sleep(0.001)


async def _wait_for_call(owner):
    await _wait_for(lambda: owner._future is not None and owner._future.done())


def _close_worker(worker):
    worker._target_refresh.close()
    worker._composition.close()
    worker._receipt_writer.close()


def test_slow_probe_does_not_block_repeated_composition():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [target]
        compose = mock.Mock()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, compose, threading.Event())
        blocked = asyncio.Event()

        async def _blocked_probe(session, exact_target):
            del session, exact_target
            await blocked.wait()

        worker._probe = _blocked_probe
        tasks = {}
        await worker._run_tick(mock.Mock(), tasks)
        await _wait_for_call(worker._target_refresh)
        await _wait_for_call(worker._composition)
        await worker._run_tick(mock.Mock(), tasks)
        await _wait_for_call(worker._composition)
        await worker._run_tick(mock.Mock(), tasks)

        assert compose.call_count >= 2
        assert len(tasks) == 1
        blocked.set()
        await asyncio.gather(*tasks.values())
        _close_worker(worker)

    asyncio.run(_run())


def test_blocked_target_refresh_retains_snapshot_and_http_progress():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        refresh_started = threading.Event()
        release_refresh = threading.Event()
        read_count = 0

        def _read_targets(_identity_arg):
            nonlocal read_count
            read_count += 1
            if read_count == 1:
                return [target]
            refresh_started.set()
            assert release_refresh.wait(timeout=10)
            raise route_projection.RouteProjectionUnavailable(
                'temporary database outage')

        repository.list_probe_targets.side_effect = _read_targets
        compose = mock.Mock()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, compose, threading.Event())
        requests = []

        class _Response:

            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        class _Session:

            def request(self, method, url, **kwargs):
                requests.append((method, url, kwargs))
                return _Response()

        tasks = {}
        try:
            # First refresh supplies an exact snapshot. The next refresh blocks
            # indefinitely while HTTP work and independent composition proceed.
            assert await worker._run_tick(_Session(), tasks)
            await _wait_for_call(worker._target_refresh)
            assert await worker._run_tick(_Session(), tasks)
            assert refresh_started.wait(timeout=5)
            await asyncio.sleep(0)
            assert len(requests) == 1

            for _ in range(3):
                assert await worker._run_tick(_Session(), tasks)
                await asyncio.sleep(0)

            assert len(requests) >= 3
            assert repository.list_probe_targets.call_count == 2
            assert worker._current_targets == (target,)
            assert compose.call_count >= 1

            release_refresh.set()
            await _wait_for_call(worker._target_refresh)
            assert await worker._run_tick(_Session(), tasks)
            assert worker._current_targets == (target,)
        finally:
            release_refresh.set()
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_blocked_compose_does_not_block_refresh_or_http_progress():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [target]
        compose_started = threading.Event()
        release_compose = threading.Event()

        def _compose():
            compose_started.set()
            assert release_compose.wait(timeout=10)

        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, _compose, threading.Event())
        probe_count = 0

        async def _successful_probe(_session, exact_target):
            nonlocal probe_count
            probe_count += 1
            return route_projection.RouteLeaseProbeResult(exact_target, True)

        worker._probe = _successful_probe
        tasks = {}
        try:
            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            assert compose_started.wait(timeout=5)

            for _ in range(5):
                assert await worker._run_tick(mock.Mock(), tasks)
                await asyncio.sleep(0)

            # Event-loop yields progress probes but do not guarantee that the
            # independent target-reader thread has entered its submitted call.
            await _wait_for_call(worker._target_refresh)
            assert probe_count >= 3
            assert repository.list_probe_targets.call_count >= 2
            # Repeated ticks coalesce behind the sole running composition.
            assert worker._composition._future is not None
            assert not worker._composition._future.done()
        finally:
            release_compose.set()
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_target_validator_accepts_budget_and_rejects_max_plus_one():
    identity = _identity()
    maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
    targets = [
        _target(identity, replica_id=replica_id)
        for replica_id in range(1, maximum + 2)
    ]

    assert incremental_route_worker._validate_probe_targets(
        targets[:maximum]) == tuple(targets[:maximum])
    with pytest.raises(ValueError, match='bounded maximum'):
        incremental_route_worker._validate_probe_targets(targets)


def test_oversized_target_snapshot_drops_old_tasks_and_schedules_nothing():

    async def _run():
        identity = _identity()
        maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [
            _target(identity, replica_id=replica_id)
            for replica_id in range(1, maximum + 2)
        ]
        compose = mock.Mock()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, compose, threading.Event())
        worker._probe = mock.AsyncMock()

        old_target = _target(identity, replica_id=maximum + 2)
        old_probe_blocked = asyncio.Event()

        async def _old_probe():
            await old_probe_blocked.wait()

        old_task = asyncio.create_task(_old_probe())
        await asyncio.sleep(0)
        tasks = {worker._target_key(old_target): old_task}

        try:
            await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            await worker._run_tick(mock.Mock(), tasks)

            assert not tasks
            worker._probe.assert_not_called()
            assert compose.call_count >= 1
            await asyncio.gather(old_task, return_exceptions=True)
            assert old_task.cancelled()
        finally:
            old_probe_blocked.set()
            if not old_task.done():
                old_task.cancel()
                await asyncio.gather(old_task, return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_slow_receipt_batch_does_not_block_repeated_composition():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [target]
        writer_started = threading.Event()
        release_writer = threading.Event()

        def _blocked_write(results, *, ttl_seconds):
            del results, ttl_seconds
            writer_started.set()
            release_writer.wait(timeout=10)
            return []

        repository.record_probe_results.side_effect = _blocked_write
        compose = mock.Mock()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, compose, threading.Event())

        async def _successful_probe(session, exact_target):
            del session
            return route_projection.RouteLeaseProbeResult(exact_target, True)

        worker._probe = _successful_probe
        tasks = {}
        try:
            await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            await worker._run_tick(mock.Mock(), tasks)
            await asyncio.sleep(0)
            await worker._run_tick(mock.Mock(), tasks)
            for _ in range(1000):
                if writer_started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert writer_started.is_set()

            await worker._run_tick(mock.Mock(), tasks)
            await _wait_for(lambda: compose.call_count >= 2)

            assert compose.call_count >= 2
            assert repository.record_probe_results.call_count == 1
        finally:
            release_writer.set()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_receipt_writer_batches_and_coalesces_exact_targets():
    identity = _identity()
    first = _target(identity, replica_id=1)
    second = _target(identity, replica_id=2)
    repository = mock.Mock()
    repository.record_probe_results.return_value = []
    writer = incremental_route_worker._ProbeReceiptWriter(repository, 60)
    writer.add(route_projection.RouteLeaseProbeResult(first, False))
    writer.add(route_projection.RouteLeaseProbeResult(first, True))
    writer.add(route_projection.RouteLeaseProbeResult(second, True))

    try:
        writer.flush()
        assert writer._future is not None
        writer._future.result(timeout=10)
        writer.flush()
    finally:
        writer.close()

    repository.record_probe_results.assert_called_once()
    batch = repository.record_probe_results.call_args.args[0]
    assert [(result.target.replica_id, result.succeeded) for result in batch
           ] == [(1, True), (2, True)]


def test_receipt_writer_stays_bounded_during_exact_identity_churn():
    """A blocked writer retains only the newest result per replica ID."""
    identity = _identity()
    repository = mock.Mock()
    writer_started = threading.Event()
    release_writer = threading.Event()

    def _blocked_write(results, *, ttl_seconds):
        del results, ttl_seconds
        writer_started.set()
        assert release_writer.wait(timeout=10)
        return []

    repository.record_probe_results.side_effect = _blocked_write
    writer = incremental_route_worker._ProbeReceiptWriter(repository, 60)
    initial = _target(identity)
    writer.add(route_projection.RouteLeaseProbeResult(initial, True))
    writer.flush()
    future = writer._future
    assert future is not None
    assert writer_started.wait(timeout=5)

    try:
        maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
        latest_generation = 5
        base_targets = {
            replica_id: _target(identity, replica_id=replica_id)
            for replica_id in range(1, maximum + 1)
        }
        for generation in range(1, latest_generation + 1):
            for replica_id, base_target in base_targets.items():
                target = dataclasses.replace(
                    base_target,
                    replica_record_id=str(uuid.uuid4()),
                    material_sha256=f'{generation:064x}',
                    material_generation=generation,
                    revocation_generation=generation)
                writer.add(route_projection.RouteLeaseProbeResult(target, True))
            # A busy future must neither submit another batch nor retain one
            # entry per obsolete exact material generation.
            writer.flush()
            assert len(writer._pending) == maximum

        assert set(writer._pending) == set(range(1, maximum + 1))
        assert all(result.target.material_generation == latest_generation
                   for result in writer._pending.values())
        assert repository.record_probe_results.call_count == 1
    finally:
        release_writer.set()
        future.result(timeout=5)
        writer.close()


def test_receipt_writer_refuses_max_plus_one_but_replaces_existing_id():
    identity = _identity()
    repository = mock.Mock()
    writer = incremental_route_worker._ProbeReceiptWriter(repository, 60)
    maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
    for replica_id in range(1, maximum + 1):
        target = _target(identity, replica_id=replica_id)
        writer.add(route_projection.RouteLeaseProbeResult(target, True))

    novel_target = _target(identity, replica_id=maximum + 1)
    writer.add(route_projection.RouteLeaseProbeResult(novel_target, True))
    replacement_target = dataclasses.replace(writer._pending[1].target,
                                             replica_record_id=str(
                                                 uuid.uuid4()),
                                             material_sha256='b' * 64,
                                             material_generation=2)
    replacement = route_projection.RouteLeaseProbeResult(
        replacement_target, False)
    writer.add(replacement)

    try:
        assert len(writer._pending) == maximum
        assert novel_target.replica_id not in writer._pending
        assert writer._pending[1] is replacement
    finally:
        writer.close()


def test_receipt_writer_prunes_full_fleet_turnover_before_admitting_current_ids(
):
    identity = _identity()
    repository = mock.Mock()
    writer = incremental_route_worker._ProbeReceiptWriter(repository, 60)
    maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
    old_targets = tuple(
        _target(identity, replica_id=replica_id)
        for replica_id in range(1, maximum + 1))
    current_targets = tuple(
        _target(identity, replica_id=replica_id)
        for replica_id in range(maximum + 1, 2 * maximum + 1))
    for target in old_targets:
        writer.add(route_projection.RouteLeaseProbeResult(target, True))
    assert len(writer._pending) == maximum

    writer.retain_targets(current_targets)
    for target in current_targets:
        writer.add(route_projection.RouteLeaseProbeResult(target, True))

    try:
        assert len(writer._pending) == maximum
        assert set(writer._pending) == {
            target.replica_id for target in current_targets
        }
    finally:
        writer.close()


def test_max_fleet_generation_churn_awaits_old_tasks_before_replacement():

    async def _run():
        identity = _identity()
        maximum = incremental_route_worker.constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
        old_targets = [
            _target(identity, replica_id=replica_id)
            for replica_id in range(1, maximum + 1)
        ]
        current_targets = [
            dataclasses.replace(target,
                                replica_record_id=str(uuid.uuid4()),
                                material_sha256='b' * 64,
                                material_generation=2) for target in old_targets
        ]
        repository = mock.Mock()
        snapshots = iter((old_targets, current_targets))
        repository.list_probe_targets.side_effect = lambda _identity_arg: next(
            snapshots, current_targets)
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, mock.Mock(), threading.Event())
        release_probes = asyncio.Event()
        active = 0
        peak = 0

        async def _blocked_probe(_session, exact_target):
            del exact_target
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await release_probes.wait()
            finally:
                active -= 1

        worker._probe = _blocked_probe
        tasks = {}
        try:
            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for(lambda: active == maximum)
            await _wait_for_call(worker._target_refresh)

            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for(lambda: active == maximum)

            assert len(tasks) == maximum
            assert set(tasks) == {
                worker._target_key(target) for target in current_targets
            }
            assert peak == maximum
        finally:
            release_probes.set()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_exact_owner_conflict_clears_targets_and_terminates_worker_instance():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        read_count = 0

        def _read_targets(_identity_arg):
            nonlocal read_count
            read_count += 1
            if read_count == 1:
                return [target]
            raise route_projection.RouteProjectionConflict('owner changed')

        repository.list_probe_targets.side_effect = _read_targets
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, mock.Mock(), threading.Event())
        probe_blocked = asyncio.Event()

        async def _blocked_probe(_session, _target_arg):
            await probe_blocked.wait()

        worker._probe = _blocked_probe
        tasks = {}
        try:
            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            assert await worker._run_tick(mock.Mock(), tasks)
            worker._receipt_writer.add(
                route_projection.RouteLeaseProbeResult(target, True))
            await _wait_for_call(worker._target_refresh)

            assert not await worker._run_tick(mock.Mock(), tasks)
            assert worker._current_targets == ()
            assert worker._receipt_writer._pending == {}
            assert not tasks
            assert repository.list_probe_targets.call_count == 2
        finally:
            probe_blocked.set()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_composition_snapshot_churn_retries_without_losing_targets():

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [target]
        compose = mock.Mock(side_effect=[
            route_projection.RouteProjectionSnapshotChanged(
                'replica changed during composition'),
            None,
        ])
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, compose, threading.Event())
        probe_blocked = asyncio.Event()

        async def _blocked_probe(_session, _target_arg):
            await probe_blocked.wait()

        worker._probe = _blocked_probe
        tasks = {}
        try:
            assert await worker._run_tick(mock.Mock(), tasks)
            await _wait_for_call(worker._target_refresh)
            await _wait_for_call(worker._composition)

            # Optimistic composition churn carries no ownership evidence. The
            # same worker must retain probe progress and retry in place.
            assert await worker._run_tick(mock.Mock(), tasks)
            assert worker._current_targets == (target,)
            assert len(tasks) == 1
            await _wait_for_call(worker._composition)
            assert await worker._run_tick(mock.Mock(), tasks)
            assert compose.call_count >= 2
            assert worker._current_targets == (target,)
        finally:
            probe_blocked.set()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            _close_worker(worker)

    asyncio.run(_run())


def test_initial_target_completion_schedules_http_before_next_grid(monkeypatch):

    async def _run():
        identity = _identity()
        target = _target(identity)
        repository = mock.Mock()
        repository.list_probe_targets.return_value = [target]
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository,
            identity,
            mock.Mock(),
            threading.Event(),
            interval_seconds=60)
        request_seen = asyncio.Event()

        class _Connector:
            """Minimal fake aiohttp connector."""

            def __init__(self, **_kwargs):
                pass

        class _Response:
            """Successful fake aiohttp response."""

            status = 200

            async def __aenter__(self):
                request_seen.set()
                return self

            async def __aexit__(self, *_args):
                return None

        class _Session:
            """Minimal fake aiohttp session."""

            def __init__(self, *, connector):
                self.connector = connector

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            def request(self, *_args, **_kwargs):
                return _Response()

        monkeypatch.setattr(incremental_route_worker.replica_tls,
                            'aiohttp_ssl_setting', lambda: None)
        monkeypatch.setattr(incremental_route_worker.aiohttp, 'TCPConnector',
                            _Connector)
        monkeypatch.setattr(incremental_route_worker.aiohttp, 'ClientSession',
                            _Session)
        run_task = asyncio.create_task(worker.run_async())
        try:
            await asyncio.wait_for(request_seen.wait(), timeout=1)
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

        assert repository.list_probe_targets.call_count == 1
        assert worker._closed

    asyncio.run(_run())


def test_worker_shutdown_does_not_wait_for_running_synchronous_calls(
        monkeypatch):

    async def _run():
        identity = _identity()
        target_read_started = threading.Event()
        compose_started = threading.Event()
        release_calls = threading.Event()
        repository = mock.Mock()

        def _blocked_target_read(_identity_arg):
            target_read_started.set()
            assert release_calls.wait(timeout=10)
            return []

        def _blocked_compose():
            compose_started.set()
            assert release_calls.wait(timeout=10)

        repository.list_probe_targets.side_effect = _blocked_target_read
        stop_event = threading.Event()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, _blocked_compose, stop_event)
        worker._interval_seconds = 0.001

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

        monkeypatch.setattr(incremental_route_worker.replica_tls,
                            'aiohttp_ssl_setting', lambda: None)
        monkeypatch.setattr(incremental_route_worker.aiohttp, 'TCPConnector',
                            _Connector)
        monkeypatch.setattr(incremental_route_worker.aiohttp, 'ClientSession',
                            _Session)
        run_task = asyncio.create_task(worker.run_async())
        try:
            await _wait_for(lambda: target_read_started.is_set() and
                            compose_started.is_set())
            stop_event.set()
            await asyncio.wait_for(run_task, timeout=1)

            assert worker._closed
            assert worker._target_refresh._future is not None
            assert worker._composition._future is not None
        finally:
            release_calls.set()
            if not run_task.done():
                await asyncio.wait_for(run_task, timeout=1)

    asyncio.run(_run())


def test_run_owns_one_tls_setting_for_the_whole_aiohttp_session(monkeypatch):
    """Per-target probes cannot mint connection-key-specific SSL contexts."""

    async def _run():
        identity = _identity()
        targets = [_target(identity, replica_id=index) for index in (1, 2)]
        repository = mock.Mock()
        repository.list_probe_targets.return_value = targets
        stop_event = threading.Event()
        worker = incremental_route_worker.IncrementalRouteWorker(
            repository, identity, mock.Mock(), stop_event)
        # Keep the production loop shape while avoiding a one-second unit-test
        # wait after the first tick schedules its tasks.
        worker._interval_seconds = 0.001
        ssl_setting = object()
        ssl_lookup = mock.Mock(side_effect=[
            ssl_setting,
            AssertionError('TLS setting was rebuilt per target'),
        ])
        monkeypatch.setattr(incremental_route_worker.replica_tls,
                            'aiohttp_ssl_setting', ssl_lookup)
        connector_kwargs = []
        request_kwargs = []

        class _Connector:

            def __init__(self, **kwargs):
                connector_kwargs.append(kwargs)

        class _Response:
            """Successful fake aiohttp response."""

            status = 200

            async def __aenter__(self):
                if len(request_kwargs) == len(targets):
                    stop_event.set()
                return self

            async def __aexit__(self, *_args):
                return None

        class _Session:
            """Minimal fake aiohttp session."""

            def __init__(self, *, connector):
                self.connector = connector

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            def request(self, *_args, **kwargs):
                request_kwargs.append(kwargs)
                return _Response()

        monkeypatch.setattr(incremental_route_worker.aiohttp, 'TCPConnector',
                            _Connector)
        monkeypatch.setattr(incremental_route_worker.aiohttp, 'ClientSession',
                            _Session)

        await worker.run_async()

        assert ssl_lookup.call_count == 1
        assert connector_kwargs == [{
            'limit': incremental_route_worker.constants.
                     SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS,
            'ssl': ssl_setting,
        }]
        assert len(request_kwargs) == len(targets)
        assert all('ssl' not in kwargs for kwargs in request_kwargs)

    asyncio.run(_run())
