"""Unpaid end-to-end tests for the paid-capacity synchronous worker.

The test client observes the ordinary authenticated HTTP contract used by the
paid provider qualification: the backend holds the request while it works and
authors the terminal 200 body.  No callback, 202 receipt, or async ledger is
part of this fixture.
"""

import contextlib
import http.client
import json
import os
import pathlib
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest
import yaml

_FIXTURE = (pathlib.Path(__file__).parents[1] / 'skyserve' / 'paid_capacity' /
            'service.yaml')
_PREDICT_PATH = '/v1/models/model:predict'


def _unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def _worker(*, gpu_units: int = 1):
    config = yaml.safe_load(_FIXTURE.read_text(encoding='utf-8'))
    port = _unused_port()
    process = subprocess.Popen(
        ['bash', '-c', config['run']],
        env={
            **os.environ,
            'PORT': str(port),
            'SKYPILOT_NUM_GPUS_PER_NODE': str(gpu_units),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True)
    endpoint = f'http://127.0.0.1:{port}'
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                with urllib.request.urlopen(f'{endpoint}/health',
                                            timeout=1) as response:
                    assert json.load(response) == {'status': 'ok'}
                break
            except urllib.error.URLError:
                if process.poll() is not None:
                    _, stderr = process.communicate(timeout=1)
                    pytest.fail(f'Inline worker exited early: {stderr}')
                if time.monotonic() >= deadline:
                    pytest.fail('Inline worker did not become healthy.')
                time.sleep(0.05)
        yield endpoint
    finally:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def _submit(endpoint: str, *, request_id: str,
            duration: float) -> tuple[int, object, float]:
    parsed = urllib.parse.urlsplit(endpoint)
    body = json.dumps({
        'duration_seconds': duration,
        'request_id': request_id,
    }).encode('utf-8')
    connection = http.client.HTTPConnection(parsed.hostname,
                                            parsed.port,
                                            timeout=3)
    started_at = time.monotonic()
    try:
        connection.request('POST',
                           _PREDICT_PATH,
                           body=body,
                           headers={
                               'Content-Type': 'application/json',
                               'Content-Length': str(len(body)),
                           })
        response = connection.getresponse()
        response_body = json.loads(response.read() or b'{}')
        return response.status, response_body, time.monotonic() - started_at
    finally:
        connection.close()


def test_worker_returns_backend_authored_200_only_after_work_finishes():
    duration = 0.25
    with _worker() as endpoint:
        status, response, elapsed = _submit(endpoint,
                                            request_id='backend-success',
                                            duration=duration)

    assert status == 200
    assert elapsed >= duration * 0.9
    assert response == {'request_id': 'backend-success', 'status': 'ok'}


def test_worker_uses_every_logical_gpu_slot_for_synchronous_requests():
    with _worker(gpu_units=2) as endpoint:
        outcomes = []
        threads = [
            threading.Thread(target=lambda index=index: outcomes.append(
                _submit(endpoint, request_id=f'capacity-{index}', duration=0.25)
            )) for index in range(2)
        ]
        started_at = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()

    # Both requests run concurrently; a serialized implementation would take
    # roughly twice the synthetic duration.
    assert time.monotonic() - started_at < 0.45
    assert len(outcomes) == 2
    assert all(status == 200 for status, _, _ in outcomes)
    assert {response['request_id'] for _, response, _ in outcomes
           } == {'capacity-0', 'capacity-1'}


def test_worker_rejects_async_protocol_payload_instead_of_streaming_202():
    with _worker() as endpoint:
        parsed = urllib.parse.urlsplit(endpoint)
        body = b'{"action":"async_predict","payload":{"duration_seconds":0},"request_id":"old"}'
        connection = http.client.HTTPConnection(parsed.hostname,
                                                parsed.port,
                                                timeout=3)
        try:
            connection.request('POST',
                               _PREDICT_PATH,
                               body=body,
                               headers={
                                   'Content-Type': 'application/json',
                                   'Content-Length': str(len(body)),
                               })
            response = connection.getresponse()
            payload = json.loads(response.read())
        finally:
            connection.close()

    assert response.status == 400
    assert payload == {'error': 'invalid request'}


def test_fixture_has_one_raw_synchronous_happy_path():
    config = yaml.safe_load(_FIXTURE.read_text(encoding='utf-8'))
    run = config['run']

    assert 'expected_request_duration_seconds' not in config['service'][
        'replica_policy']
    for forbidden in ('async_predict', 'async_capacity', 'ledger_protocol',
                      'X-SkyServe-Async', 'completion_url',
                      '/_lb/prediction-completed', 'callback_url'):
        assert forbidden not in run
