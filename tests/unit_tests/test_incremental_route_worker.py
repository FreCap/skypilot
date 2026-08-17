"""Concurrency contracts for the provider-independent route worker."""
# pylint: disable=protected-access

import asyncio
import threading
from unittest import mock
import uuid

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
        await worker._run_tick(mock.Mock(), tasks)

        assert compose.call_count == 2
        assert len(tasks) == 1
        blocked.set()
        await asyncio.gather(*tasks.values())
        worker._receipt_writer.close()

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
            await asyncio.sleep(0)
            await worker._run_tick(mock.Mock(), tasks)
            for _ in range(1000):
                if writer_started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert writer_started.is_set()

            await worker._run_tick(mock.Mock(), tasks)

            assert compose.call_count == 3
            assert repository.record_probe_results.call_count == 1
        finally:
            release_writer.set()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            worker._receipt_writer.close()

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
