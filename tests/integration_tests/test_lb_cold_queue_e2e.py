"""Resource-heavy production-interface component test for a 10k cold queue.

This starts the production SkyServe load balancer against a fake controller
that speaks the normal sync contract.  It replaces no load-balancer internals:
10,000 HTTP requests enter the public inference route, remain queued with zero
ready replicas, and are then cancelled through their client connections.

This is not an unpaid provider-interface E2E: it deliberately replaces the
controller and does not run PostgreSQL, planning, reconciliation, or a provider
facet.  Its contract is load-balancer event-loop responsiveness and exact
public demand accounting at maximum tested queue cardinality.

Run as the dedicated resource-heavy regression (it opens 10,000 local
sockets, so do not run it under xdist):

    pytest -n0 tests/integration_tests/test_lb_cold_queue_e2e.py
"""
import asyncio
import json
import multiprocessing
import os
import socket
import sys
import tempfile
import time

import aiohttp
from aiohttp import web
import pytest

from sky.serve import constants
from sky.serve import load_balancer

_QUEUE_SIZE = 10000
_SERVICE_HASH = 'cold-queue-e2e-incarnation'
_SYNC_TOKEN = 'cold-queue-e2e-sync-token'


def _unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        return int(listener.getsockname()[1])


def _run_fake_controller(controller_port: int) -> None:
    state: dict = {'demand_count': 0, 'last_demand': None}

    async def _sync(request: web.Request) -> web.Response:
        assert request.headers.get('Authorization') == f'Bearer {_SYNC_TOKEN}'
        assert request.headers.get(
            constants.SERVICE_HASH_HEADER) == _SERVICE_HASH
        await request.json()
        return web.json_response({
            'replica_info': {},
            'queued_compatibility_demand_supported': True,
            'routing_spec': {
                'load_balancing_policy_name': 'instance_aware_least_load',
                'request_accelerator_compatibility_version': 1,
                'configured_accelerators': ['L4'],
                'request_queue': {
                    'min_size': _QUEUE_SIZE,
                    'size_per_replica': 0,
                    'max_size': _QUEUE_SIZE,
                    'max_concurrency_per_replica': 1,
                    'max_concurrency': 128,
                    'timeout_seconds': 600,
                    'max_request_body_bytes': 1024,
                    'use_async_occupancy': False,
                },
            },
        })

    async def _demand(request: web.Request) -> web.Response:
        state['demand_count'] += 1
        state['last_demand'] = await request.json()
        return web.json_response({
            'request_history_accepted': True,
            'request_classification_history_accepted': True,
            'prediction_time_history_accepted': True,
        })

    async def _route_lease(request: web.Request) -> web.Response:
        del request
        return web.json_response({
            'version': constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION,
            'entries': [],
        })

    async def _demand_state(request: web.Request) -> web.Response:
        del request
        return web.json_response(state)

    app = web.Application()
    app.router.add_post('/controller/load_balancer_sync', _sync)
    app.router.add_post(constants.LB_DEMAND_REPORT_PATH, _demand)
    app.router.add_post(constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH,
                        _route_lease)
    app.router.add_get('/_test/demand', _demand_state)
    web.run_app(app, host='127.0.0.1', port=controller_port, print=None)


def _run_load_balancer(controller_port: int, lb_port: int) -> None:
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as token_file:
        token_file.write(f'{_SYNC_TOKEN}\n')
        token_file.flush()
        os.environ[constants.EXTERNAL_LB_ENABLED_ENV_VAR] = 'true'
        os.environ[constants.LB_POD_UID_ENV_VAR] = 'cold-queue-e2e-pod-uid'
        os.environ[constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR] = token_file.name
        os.environ[constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR] = 'false'
        os.environ.pop(constants.LB_AUTH_TOKEN_ENV_VAR, None)
        os.environ.pop(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, None)
        load_balancer.run_load_balancer(
            controller_addr=f'http://127.0.0.1:{controller_port}',
            load_balancer_port=lb_port,
            service_hash=_SERVICE_HASH,
        )


async def _read_json(session: aiohttp.ClientSession, path: str,
                     timeout_seconds: float, port: int) -> tuple[int, dict]:
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with session.get(f'http://127.0.0.1:{port}{path}',
                           timeout=timeout) as response:
        body = await response.read()
        return response.status, json.loads(body or b'{}')


async def _wait_for_depth(session: aiohttp.ClientSession, expected: int,
                          timeout_seconds: float, lb_port: int) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            status, last = await _read_json(session, '/_lb/capacity', 2,
                                            lb_port)
            if status == 200 and last.get('request_queue_depth') == expected:
                return last
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        await asyncio.sleep(0.05)
    raise AssertionError(
        f'queue depth never reached {expected}; last capacity={last}')


async def _wait_for_demand_report(session: aiohttp.ClientSession,
                                  controller_port: int, expected: int) -> dict:
    deadline = time.monotonic() + 15
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            status, last = await _read_json(session, '/_test/demand', 2,
                                            controller_port)
            payload = last.get('last_demand') or {}
            if status == 200 and payload.get('queue_depth') == expected:
                return payload
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        await asyncio.sleep(0.05)
    raise AssertionError(
        f'demand report never reached {expected}; last report={last}')


async def _queue_one(session: aiohttp.ClientSession, index: int,
                     lb_port: int) -> int:
    async with session.post(
            f'http://127.0.0.1:{lb_port}/predict',
            json={'request_id': f'cold-{index}'},
            headers={constants.LB_JOB_ID_HEADER: f'cold-{index}'}) as response:
        await response.read()
        return response.status


async def _run_test(controller_port: int, lb_port: int) -> None:
    probe_connector = aiohttp.TCPConnector(limit=32)
    queue_connector = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=None, connect=30)
    async with aiohttp.ClientSession(connector=probe_connector) as probes, \
            aiohttp.ClientSession(connector=queue_connector,
                                  timeout=timeout) as queue_client:
        deadline = time.monotonic() + 30
        while True:
            try:
                status, _ = await _read_json(probes, '/_lb/health', 1, lb_port)
                if status == 200:
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            if time.monotonic() >= deadline:
                raise AssertionError('load balancer did not become ready')
            await asyncio.sleep(0.1)

        queued = [
            asyncio.create_task(_queue_one(queue_client, index, lb_port))
            for index in range(_QUEUE_SIZE)
        ]
        try:
            capacity = await _wait_for_depth(probes, _QUEUE_SIZE, 45, lb_port)
            assert capacity['ready_replicas'] == 0
            assert capacity['request_queue_timeout_seconds'] == 600
            demand = await _wait_for_demand_report(probes, controller_port,
                                                   _QUEUE_SIZE)
            assert demand['queued_requests_by_compatibility'] == [{
                'priority': 0,
                'compatible_accelerators': ['L4'],
                'count': _QUEUE_SIZE,
            }]

            # Every waiter polls for disconnect once per second. Probe after a
            # complete poll interval, through the same public liveness route
            # Kubernetes uses, while all 10,000 requests remain resident.
            await asyncio.sleep(1.1)
            liveness_latencies = []
            for _ in range(20):
                started = time.monotonic()
                status, _ = await _read_json(probes, '/_lb/liveness', 1,
                                             lb_port)
                assert status == 200
                liveness_latencies.append(time.monotonic() - started)
                await asyncio.sleep(0.1)
            assert max(liveness_latencies) < 1
            started = time.monotonic()
            capacity = await _wait_for_depth(probes, _QUEUE_SIZE, 2, lb_port)
            capacity_latency = time.monotonic() - started
            assert capacity_latency < 1
            assert capacity['request_queue_depth'] == _QUEUE_SIZE
            print(f'PASS: {_QUEUE_SIZE} cold requests resident; '
                  'demand report published; worst liveness latency='
                  f'{max(liveness_latencies):.3f}s; capacity latency='
                  f'{capacity_latency:.3f}s')
        finally:
            for task in queued:
                task.cancel()
            await asyncio.gather(*queued, return_exceptions=True)

        await _wait_for_depth(probes, 0, 15, lb_port)
        print('PASS: all cancelled requests left the production queue')


@pytest.mark.serve_lb_10k_interface
@pytest.mark.component
def test_ten_thousand_cold_requests_keep_public_surfaces_responsive() -> None:
    context = multiprocessing.get_context('spawn')
    controller_port = _unused_local_port()
    lb_port = _unused_local_port()
    while lb_port == controller_port:
        lb_port = _unused_local_port()
    controller = context.Process(target=_run_fake_controller,
                                 args=(controller_port,),
                                 daemon=True)
    lb = context.Process(target=_run_load_balancer,
                         args=(controller_port, lb_port),
                         daemon=True)
    processes = (controller, lb)
    try:
        controller.start()
        lb.start()
        asyncio.run(_run_test(controller_port, lb_port))
    finally:
        if lb.is_alive():
            lb.terminate()
            lb.join(timeout=1)
        if lb.is_alive():
            lb.kill()
        if controller.is_alive():
            controller.terminate()
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)


if __name__ == '__main__':
    test_ten_thousand_cold_requests_keep_public_surfaces_responsive()
    sys.exit(0)
