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


def _target(identity):
    return route_projection.RouteLeaseProbeTarget(
        identity=identity,
        replica_id=1,
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

    asyncio.run(_run())
