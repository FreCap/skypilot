"""Production-interface component tests for the SkyServe request queue.

These start the production SkyServe load balancer against fake controller and
worker processes that speak the normal network contracts.  They replace no
load-balancer internals:

* 10,000 HTTP requests enter the public inference route, remain queued with
  zero ready replicas, and are then cancelled through their client connections.
* One exact async request waits behind a busy worker, reaches a short
  priority-specific queue deadline, receives a typed pre-dispatch rejection,
  and retries the byte-identical request successfully after capacity returns.

These are not unpaid provider-interface E2Es: they deliberately replace the
controller and worker and do not run PostgreSQL, planning, reconciliation, or a
provider facet.  Their contracts are the public load-balancer HTTP interface,
queue event-loop responsiveness, exact demand accounting, and typed retry wire
semantics.

Run as the dedicated resource-heavy regression (it opens 10,000 local
sockets, so do not run it under xdist):

    pytest -n0 tests/integration_tests/test_lb_cold_queue_component.py
"""
import asyncio
import hashlib
import json
import multiprocessing
import os
import socket
import sys
import tempfile
import time
from typing import Any

import aiohttp
from aiohttp import web
import pytest
import rfc8785

from sky.serve import constants
from sky.serve import load_balancer

_QUEUE_SIZE = 10000
_SERVICE_HASH = 'cold-queue-e2e-incarnation'
_SERVICE_NAME = 'cold-queue-e2e'
_SYNC_TOKEN = 'cold-queue-e2e-sync-token'
_DEADLINE_PRIORITY = 100
_DEADLINE_SECONDS = 0.2
_ROUTE_PROJECTION_SHA256 = 'b' * 64
_REJECTED_ATTEMPT_ID = '11111111-1111-4111-8111-111111111111'
_ACCEPTED_ATTEMPT_ID = '22222222-2222-4222-8222-222222222222'


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
            service_name=_SERVICE_NAME,
        )


def _run_deadline_worker(worker_port: int) -> None:
    release = asyncio.Event()
    state: dict = {
        'blocker_started': False,
        'exact_bodies': [],
        'exact_headers': [],
    }

    async def _predict(request: web.Request) -> web.Response:
        body = await request.read()
        if body == b'{"block":true}':
            state['blocker_started'] = True
            await release.wait()
            return web.json_response({'released': True})
        state['exact_bodies'].append(body.decode('utf-8'))
        state['exact_headers'].append({
            name.lower(): value
            for name, value in request.headers.items()
            if name.lower() in {
                constants.LB_JOB_ID_HEADER.lower(),
                constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER.lower(),
                constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower(),
                constants.LB_ASYNC_INTENT_SHA256_HEADER.lower(),
                constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER.lower(),
                constants.LB_ASYNC_ATTEMPT_ID_HEADER.lower(),
                constants.LB_ASYNC_ATTEMPT_NO_HEADER.lower(),
                constants.LB_ASYNC_LEDGER_REVISION_HEADER.lower(),
            }
        })
        payload = json.loads(body)
        return web.json_response(
            {
                'request_id': payload['request_id'],
                'status': 'accepted',
            },
            status=202)

    async def _state(request: web.Request) -> web.Response:
        del request
        return web.json_response(state)

    async def _release(request: web.Request) -> web.Response:
        del request
        release.set()
        return web.json_response({'released': True})

    app = web.Application()
    app.router.add_post('/predict', _predict)
    app.router.add_get('/_test/state', _state)
    app.router.add_post('/_test/release', _release)
    web.run_app(app, host='127.0.0.1', port=worker_port, print=None)


def _run_deadline_controller(controller_port: int, worker_port: int) -> None:
    state: dict[str, Any] = {
        'current_receipt': None,
        'ledger_operations': [],
    }

    def _receipt(*, request_id: str, attempt_id: str, attempt_no: int,
                 ledger_state: str, revision: int, duplicate: bool,
                 dispatch_authorized: bool) -> dict:
        return {
            'request_key_sha256': hashlib.sha256(request_id.encode('utf-8')
                                                ).hexdigest(),
            'attempt_id': attempt_id,
            'attempt_no': attempt_no,
            'state': ledger_state,
            'revision': revision,
            'duplicate': duplicate,
            'dispatch_authorized': dispatch_authorized,
        }

    async def _sync(request: web.Request) -> web.Response:
        assert request.headers.get('Authorization') == f'Bearer {_SYNC_TOKEN}'
        assert request.headers.get(
            constants.SERVICE_HASH_HEADER) == _SERVICE_HASH
        await request.json()
        return web.json_response({
            'replica_info': {
                f'http://127.0.0.1:{worker_port}': {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'is_zero_cost': 'false',
                },
            },
            'num_ready_replicas': 1,
            'queued_compatibility_demand_supported': True,
            'async_request_ledger_protocol_version':
                constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION,
            'service_version': 1,
            'route_projection_generation': 1,
            'route_projection_sha256': _ROUTE_PROJECTION_SHA256,
            'route_source_epoch': 1,
            'routing_spec': {
                'load_balancing_policy_name': 'instance_aware_least_load',
                'request_accelerator_compatibility_version': 1,
                'configured_accelerators': ['L4'],
                'request_queue': {
                    'min_size': 2,
                    'size_per_replica': 0,
                    'max_size': 2,
                    'max_concurrency_per_replica': 1,
                    'max_concurrency': 1,
                    'timeout_seconds': 600,
                    'timeout_seconds_by_priority': [{
                        'min_priority': _DEADLINE_PRIORITY,
                        'timeout_seconds': _DEADLINE_SECONDS,
                    }],
                    'max_request_body_bytes': 1024,
                    'use_async_occupancy': False,
                },
            },
        })

    async def _demand(request: web.Request) -> web.Response:
        await request.json()
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

    async def _ledger(request: web.Request) -> web.Response:
        assert request.headers.get('Authorization') == f'Bearer {_SYNC_TOKEN}'
        assert request.headers.get(
            constants.SERVICE_HASH_HEADER) == _SERVICE_HASH
        payload = await request.json()
        state['ledger_operations'].append(payload)
        operation = payload['operation']
        current = state['current_receipt']
        if operation == 'bind' and payload['allow_new_attempt'] is False:
            if current is None:
                raise web.HTTPNotFound()
            return web.json_response({
                **current,
                'duplicate': True,
                'dispatch_authorized': False,
            })
        if operation == 'reject_before_dispatch':
            current = _receipt(request_id=payload['request_id'],
                               attempt_id=_REJECTED_ATTEMPT_ID,
                               attempt_no=1,
                               ledger_state='REJECTED_PRE_DISPATCH',
                               revision=1,
                               duplicate=False,
                               dispatch_authorized=False)
        elif operation == 'bind' and payload['allow_new_attempt'] is True:
            assert isinstance(current, dict)
            prior = dict(current)
            assert prior['state'] == 'REJECTED_PRE_DISPATCH'
            current = _receipt(request_id=payload['request_id'],
                               attempt_id=_ACCEPTED_ATTEMPT_ID,
                               attempt_no=prior['attempt_no'] + 1,
                               ledger_state='DISPATCH_MAY_HAVE_OCCURRED',
                               revision=1,
                               duplicate=False,
                               dispatch_authorized=True)
        elif operation == 'accepted':
            assert isinstance(current, dict)
            prior = dict(current)
            assert payload['attempt_id'] == prior['attempt_id']
            assert payload['attempt_no'] == prior['attempt_no']
            assert payload['expected_revision'] == prior['revision']
            current = {
                **prior,
                'state': 'ACCEPTED',
                'revision': prior['revision'] + 1,
                'duplicate': False,
                'dispatch_authorized': False,
            }
        else:
            raise web.HTTPBadRequest(text=f'unsupported operation {operation}')
        state['current_receipt'] = current
        return web.json_response(current)

    async def _ledger_state(request: web.Request) -> web.Response:
        del request
        return web.json_response(state)

    app = web.Application()
    app.router.add_post('/controller/load_balancer_sync', _sync)
    app.router.add_post(constants.LB_DEMAND_REPORT_PATH, _demand)
    app.router.add_post(constants.LB_CONTROLLER_HISTORY_SYNC_PATH, _demand)
    app.router.add_post(constants.LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH,
                        _route_lease)
    app.router.add_post(constants.LB_ASYNC_REQUEST_LEDGER_PATH, _ledger)
    app.router.add_get('/_test/ledger', _ledger_state)
    web.run_app(app, host='127.0.0.1', port=controller_port, print=None)


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

            # Keep the queue resident beyond the historical one-second poll
            # boundary, then use the same public liveness route Kubernetes
            # uses. Disconnect observation must remain event-driven at 10k.
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


async def _post_bytes(
    session: aiohttp.ClientSession,
    url: str,
    body: bytes,
    *,
    headers: dict[str, str],
    timeout_seconds: float,
) -> tuple[int, bytes, dict[str, list[str]]]:
    async with session.post(
            url,
            data=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as response:
        response_body = await response.read()
        response_headers = {
            name: list(response.headers.getall(name, []))
            for name in response.headers
        }
        return response.status, response_body, response_headers


def _one_header(headers: dict[str, list[str]], name: str) -> str:
    values = next((values for candidate, values in headers.items()
                   if candidate.lower() == name.lower()), [])
    assert len(values) == 1
    return values[0]


async def _wait_for_json_value(session: aiohttp.ClientSession, url: str,
                               predicate, timeout_seconds: float) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=1)) as response:
                if response.status == 200:
                    last = await response.json()
                    if predicate(last):
                        return last
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        await asyncio.sleep(0.05)
    raise AssertionError(f'condition not reached for {url}; last={last}')


async def _run_exact_deadline_test(controller_port: int, worker_port: int,
                                   lb_port: int) -> None:
    async with aiohttp.ClientSession() as session:
        lb_base = f'http://127.0.0.1:{lb_port}'
        worker_base = f'http://127.0.0.1:{worker_port}'
        controller_base = f'http://127.0.0.1:{controller_port}'
        await _wait_for_json_value(
            session, f'{lb_base}/_lb/capacity',
            lambda value: value.get('ready_replicas') == 1 and value.get(
                'async_request_ledger_protocol_version') == constants.
            LB_ASYNC_LEDGER_PROTOCOL_VERSION, 30)

        blocker = asyncio.create_task(
            _post_bytes(session,
                        f'{lb_base}/predict',
                        b'{"block":true}',
                        headers={'Content-Type': 'application/json'},
                        timeout_seconds=30))
        await _wait_for_json_value(
            session, f'{worker_base}/_test/state',
            lambda value: value.get('blocker_started') is True, 5)

        request_id = 'deadline-request-1'
        stable_job_id = 'deadline-job-1'
        body = rfc8785.dumps({
            'action': 'async_predict',
            'payload': {
                'input': 'unpaid-component-test',
            },
            'request_id': request_id,
        })
        intent_sha256 = hashlib.sha256(body).hexdigest()
        headers = {
            constants.LB_JOB_ID_HEADER: stable_job_id,
            constants.LB_REQUEST_PRIORITY_HEADER: str(_DEADLINE_PRIORITY),
            constants.LB_REQUEST_ACCELERATORS_HEADER: 'L4',
            constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: str(
                constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION),
            constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: _SERVICE_HASH,
            constants.LB_ASYNC_INTENT_SHA256_HEADER: intent_sha256,
            constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER: request_id,
            'Content-Type': 'application/json',
        }

        started = time.monotonic()
        status, rejected_body, rejected_headers = await _post_bytes(
            session,
            f'{lb_base}/predict',
            body,
            headers=headers,
            timeout_seconds=5)
        elapsed = time.monotonic() - started
        assert status == 503
        assert _DEADLINE_SECONDS * 0.75 <= elapsed < 2
        assert _one_header(
            rejected_headers,
            constants.LB_ASYNC_LEDGER_STATE_HEADER) == 'REJECTED_PRE_DISPATCH'
        assert _one_header(
            rejected_headers,
            constants.LB_ASYNC_ATTEMPT_ID_HEADER) == _REJECTED_ATTEMPT_ID
        assert _one_header(rejected_headers,
                           constants.LB_ASYNC_ATTEMPT_NO_HEADER) == '1'
        assert _one_header(rejected_headers,
                           constants.LB_ASYNC_LEDGER_REVISION_HEADER) == '1'
        assert _one_header(rejected_headers, 'Retry-After') == str(
            constants.LB_503_RETRY_AFTER_SECONDS)
        rejected_payload = json.loads(rejected_body)
        assert rejected_payload['async_request_ledger_receipt']['state'] == (
            'REJECTED_PRE_DISPATCH')

        rejected_capacity = await _wait_for_json_value(
            session, f'{lb_base}/_lb/capacity',
            lambda value: value.get('rejected_in_window') == 1, 2)
        assert rejected_capacity['request_queue_depth'] == 0
        assert rejected_capacity['local_in_flight'] == 1
        assert rejected_capacity['rejected_in_window_by_priority'] == {
            str(_DEADLINE_PRIORITY): 1,
        }

        async with session.post(f'{worker_base}/_test/release') as response:
            assert response.status == 200
            await response.read()
        blocker_status, _, _ = await blocker
        assert blocker_status == 200

        # This is the supported retry boundary: only the typed
        # pre-dispatch receipt above permits replay, and the exact same bytes,
        # stable job identity, execution identity, and intent digest are used.
        status, accepted_body, accepted_headers = await _post_bytes(
            session,
            f'{lb_base}/predict',
            body,
            headers=headers,
            timeout_seconds=5)
        assert status == 202
        assert json.loads(accepted_body) == {
            'request_id': request_id,
            'status': 'accepted',
        }
        assert _one_header(accepted_headers,
                           constants.LB_ASYNC_LEDGER_STATE_HEADER) == 'ACCEPTED'
        assert _one_header(
            accepted_headers,
            constants.LB_ASYNC_ATTEMPT_ID_HEADER) == _ACCEPTED_ATTEMPT_ID
        assert _one_header(accepted_headers,
                           constants.LB_ASYNC_ATTEMPT_NO_HEADER) == '2'
        assert _one_header(accepted_headers,
                           constants.LB_ASYNC_LEDGER_REVISION_HEADER) == '2'

        worker_state = await _wait_for_json_value(
            session, f'{worker_base}/_test/state',
            lambda value: len(value.get('exact_bodies', [])) == 1, 2)
        assert worker_state['exact_bodies'] == [body.decode('utf-8')]
        assert worker_state['exact_headers'] == [{
            constants.LB_JOB_ID_HEADER.lower(): stable_job_id,
            constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER.lower(): str(
                constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION),
            constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower(): _SERVICE_HASH,
            constants.LB_ASYNC_INTENT_SHA256_HEADER.lower(): intent_sha256,
            constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER.lower(): request_id,
            constants.LB_ASYNC_ATTEMPT_ID_HEADER.lower(): _ACCEPTED_ATTEMPT_ID,
            constants.LB_ASYNC_ATTEMPT_NO_HEADER.lower(): '2',
            constants.LB_ASYNC_LEDGER_REVISION_HEADER.lower(): '1',
        }]

        ledger_state = await _wait_for_json_value(
            session, f'{controller_base}/_test/ledger',
            lambda value: len(value.get('ledger_operations', [])) == 4, 2)
        operations = ledger_state['ledger_operations']
        assert [operation['operation'] for operation in operations] == [
            'bind',
            'reject_before_dispatch',
            'bind',
            'accepted',
        ]
        assert operations[0]['allow_new_attempt'] is False
        assert operations[2]['allow_new_attempt'] is True
        assert all(
            operation['request_id'] == request_id for operation in operations)
        assert all(operation['intent_sha256'] == intent_sha256
                   for operation in operations)

        cleared_capacity = await _wait_for_json_value(
            session, f'{lb_base}/_lb/capacity',
            lambda value: value.get('rejected_in_window') == 0, 2)
        assert cleared_capacity['request_queue_depth'] == 0
        assert cleared_capacity['local_in_flight'] == 0


def _stop_processes(processes: tuple[multiprocessing.Process, ...]) -> None:
    for process in reversed(processes):
        if process.is_alive():
            process.terminate()
            process.join(timeout=1)
        if process.is_alive():
            process.kill()
    for process in processes:
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)


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
        _stop_processes(processes)


@pytest.mark.component
def test_exact_queue_deadline_retries_only_after_typed_rejection() -> None:
    """A public retry is byte-stable and follows a typed timeout receipt."""
    context = multiprocessing.get_context('spawn')
    controller_port = _unused_local_port()
    worker_port = _unused_local_port()
    while worker_port == controller_port:
        worker_port = _unused_local_port()
    lb_port = _unused_local_port()
    while lb_port in (controller_port, worker_port):
        lb_port = _unused_local_port()
    worker = context.Process(target=_run_deadline_worker,
                             args=(worker_port,),
                             daemon=True)
    controller = context.Process(target=_run_deadline_controller,
                                 args=(controller_port, worker_port),
                                 daemon=True)
    lb = context.Process(target=_run_load_balancer,
                         args=(controller_port, lb_port),
                         daemon=True)
    processes = (worker, controller, lb)
    try:
        for process in processes:
            process.start()
        asyncio.run(
            _run_exact_deadline_test(controller_port, worker_port, lb_port))
    finally:
        _stop_processes(processes)


if __name__ == '__main__':
    test_ten_thousand_cold_requests_keep_public_surfaces_responsive()
    test_exact_queue_deadline_retries_only_after_typed_rejection()
    sys.exit(0)
