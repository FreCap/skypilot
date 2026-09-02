"""[boltz fork] Local process/component smoke for LB occupancy probing.

Deploys the REAL load balancer (run_load_balancer, its own process) against
a fake controller and fake replicas that mimic the async wrapper's contract
(fast-ack async_predict, PodBusy 429 when occupied, async_capacity
reporting running_count/predict_concurrency), then drives the occupancy
lifecycle through the LB's public surface:

  1. probe rounds populate /_lb/capacity (probed/busy/free_slots/age);
  2. routing sends async_predicts ONLY to idle replicas while a busy one
     exists (weighted deprioritize — zero attempts on the busy replica);
  3. an all-busy fleet sheds: LB 503 "at capacity" (the backstop);
  4. a freed replica regains traffic after the next probe round;
  5. a killed replica degrades to occupancy-unknown without breaking
     routing or the endpoint.

Run manually (not collected by pytest — this spawns servers):

    PYTHONPATH=. python tests/lb_local_component.py
"""
import json
import multiprocessing
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request

from aiohttp import web

from sky.serve import constants
from sky.serve import load_balancer

LB_PORT = 18080
CONTROLLER_PORT = 18000
REPLICA_PORTS = [18101, 18102, 18103]
PROBE_INTERVAL_SECONDS = 1
SERVICE_HASH = 'local-e2e-incarnation'
SYNC_TOKEN = 'local-e2e-sync-token'


# ----------------------------------------------------------------------
# Fake replica: the async wrapper's contract, minus the GPU.
# ----------------------------------------------------------------------
def run_fake_replica(port: int) -> None:
    state = {'running': 0, 'jobs': 0, 'attempts': 0}

    async def predict(request: web.Request) -> web.Response:
        body = await request.json()
        action = body.get('action')
        if action == 'async_capacity':
            return web.json_response({
                'status': 'READY',
                'pod_name': f'fake-{port}',
                'running_count': state['running'],
                'predict_concurrency': 1,
                'max_workers': 1,
            })
        if action == 'async_predict':
            state['attempts'] += 1
            if state['running'] >= 1:
                return web.json_response({'error': 'PodBusy'}, status=429)
            state['running'] = 1
            state['jobs'] += 1
            return web.json_response({
                'request_id': body.get('request_id'),
                'status': 'IN_PROGRESS',
            })
        return web.json_response({'echo': body})

    async def admin_occupy(request: web.Request) -> web.Response:
        del request
        state['running'] = 1
        return web.json_response({'ok': True})

    async def admin_free(request: web.Request) -> web.Response:
        del request
        state['running'] = 0
        return web.json_response({'ok': True})

    async def admin_stats(request: web.Request) -> web.Response:
        del request
        return web.json_response(state)

    app = web.Application()
    app.router.add_post('/v1/models/model:predict', predict)
    app.router.add_post('/_admin/occupy', admin_occupy)
    app.router.add_post('/_admin/free', admin_free)
    app.router.add_get('/_admin/stats', admin_stats)
    web.run_app(app, host='127.0.0.1', port=port, print=None)


# ----------------------------------------------------------------------
# Fake controller: static ready set plus the routing spec an external LB needs.
# ----------------------------------------------------------------------
def run_fake_controller(port: int, replica_ports) -> None:

    async def sync(request: web.Request) -> web.Response:
        assert request.headers.get('Authorization') == f'Bearer {SYNC_TOKEN}'
        assert request.headers.get(
            constants.SERVICE_HASH_HEADER) == SERVICE_HASH
        await request.json()  # The LB posts its request aggregator.
        return web.json_response({
            'replica_info': {
                f'http://127.0.0.1:{p}': {
                    'gpu_type': 'L4',
                    'gpu_count': '1'
                } for p in replica_ports
            },
            'routing_spec': {
                'load_balancing_policy_name': 'instance_aware_least_load',
                'target_qps_per_replica': {
                    'L4': 0.1
                },
                'retriable_status_codes': [429, 503],
            },
        })

    app = web.Application()
    app.router.add_post('/controller/load_balancer_sync', sync)
    web.run_app(app, host='127.0.0.1', port=port, print=None)


def run_lb(controller_port: int, lb_port: int) -> None:
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8') as token_file:
        token_file.write(f'{SYNC_TOKEN}\n')
        token_file.flush()
        os.environ[constants.EXTERNAL_LB_ENABLED_ENV_VAR] = 'true'
        os.environ[constants.LB_POD_UID_ENV_VAR] = 'local-e2e-pod-uid'
        os.environ[constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR] = token_file.name
        os.environ[constants.LB_OCCUPANCY_PROBE_INTERVAL_ENV_VAR] = str(
            PROBE_INTERVAL_SECONDS)
        load_balancer.run_load_balancer(
            controller_addr=f'http://127.0.0.1:{controller_port}',
            load_balancer_port=lb_port,
            service_hash=SERVICE_HASH,
        )


# ----------------------------------------------------------------------
# Driver.
# ----------------------------------------------------------------------
def _http(method: str, url: str, body=None, timeout: float = 5.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url,
                                 data=data,
                                 method=method,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b'{}')
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b'{}')


def _wait_for(predicate, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception:  # pylint: disable=broad-except
            pass
        time.sleep(0.2)
    raise AssertionError(f'Timed out waiting for: {what}')


def capacity():
    status, body = _http('GET', f'http://127.0.0.1:{LB_PORT}/_lb/capacity')
    assert status == 200, body
    return body


def replica_stats(port: int):
    return _http('GET', f'http://127.0.0.1:{port}/_admin/stats')[1]


def check(name: str, condition: bool, detail=''):
    tag = 'PASS' if condition else 'FAIL'
    print(f'  [{tag}] {name}' + (f' — {detail}' if detail else ''))
    if not condition:
        raise AssertionError(f'{name}: {detail}')


def main() -> None:
    multiprocessing.set_start_method('spawn')
    procs = []
    try:
        for port in REPLICA_PORTS:
            p = multiprocessing.Process(target=run_fake_replica,
                                        args=(port,),
                                        daemon=True)
            p.start()
            procs.append(p)
        controller = multiprocessing.Process(target=run_fake_controller,
                                             args=(CONTROLLER_PORT,
                                                   REPLICA_PORTS),
                                             daemon=True)
        controller.start()
        procs.append(controller)
        lb = multiprocessing.Process(target=run_lb,
                                     args=(CONTROLLER_PORT, LB_PORT),
                                     daemon=True)
        lb.start()
        procs.append(lb)

        r1, r2, r3 = REPLICA_PORTS

        print('== phase 0: LB becomes ready (first controller sync) ==')
        _wait_for(
            lambda: _http('GET', f'http://127.0.0.1:{LB_PORT}/_lb/health')[0] ==
            200, 30, 'LB readiness')
        print('  LB ready.')

        print('== phase 1: probe rounds populate /_lb/capacity ==')
        _wait_for(lambda: capacity()['probed_replicas'] == 3,
                  10 * PROBE_INTERVAL_SECONDS, 'all replicas probed')
        cap = capacity()
        check('ready_replicas == 3', cap['ready_replicas'] == 3, cap)
        check('busy_replicas == 0', cap['busy_replicas'] == 0, cap)
        check('free_slots == 3', cap['free_slots'] == 3, cap)
        check(
            'probe age fresh',
            cap['occupancy_probe_age_seconds'] is not None and
            cap['occupancy_probe_age_seconds'] < 5 * PROBE_INTERVAL_SECONDS,
            cap)

        print('== phase 2: busy replica is deprioritized in routing ==')
        _http('POST', f'http://127.0.0.1:{r1}/_admin/occupy')
        _wait_for(lambda: capacity()['busy_replicas'] == 1,
                  10 * PROBE_INTERVAL_SECONDS, 'busy replica probed')
        cap = capacity()
        check('free_slots == 2 with one busy', cap['free_slots'] == 2, cap)
        attempts_before = replica_stats(r1)['attempts']
        oks = []
        for i in range(2):
            status, _ = _http(
                'POST', f'http://127.0.0.1:{LB_PORT}/v1/models/model:predict', {
                    'action': 'async_predict',
                    'request_id': f'job-{i}'
                })
            oks.append(status)
        check('2 async_predicts accepted', oks == [200, 200], oks)
        check('busy replica got ZERO attempts',
              replica_stats(r1)['attempts'] == attempts_before,
              replica_stats(r1))
        check('idle replicas took one job each',
              replica_stats(r2)['jobs'] == 1 and replica_stats(r3)['jobs'] == 1,
              (replica_stats(r2), replica_stats(r3)))

        print('== phase 3: all-busy fleet sheds via the 429->503 backstop ==')
        status, body = _http(
            'POST', f'http://127.0.0.1:{LB_PORT}/v1/models/model:predict', {
                'action': 'async_predict',
                'request_id': 'job-overflow'
            })
        check('overflow request got 503', status == 503, (status, body))
        _wait_for(lambda: capacity()['busy_replicas'] == 3,
                  10 * PROBE_INTERVAL_SECONDS, 'all replicas probed busy')
        check('free_slots == 0 when saturated',
              capacity()['free_slots'] == 0, capacity())

        print('== phase 4: freed replica regains traffic ==')
        _http('POST', f'http://127.0.0.1:{r1}/_admin/free')
        _wait_for(lambda: capacity()['free_slots'] == 1,
                  10 * PROBE_INTERVAL_SECONDS, 'freed replica probed')
        jobs_before = replica_stats(r1)['jobs']
        status, _ = _http(
            'POST', f'http://127.0.0.1:{LB_PORT}/v1/models/model:predict', {
                'action': 'async_predict',
                'request_id': 'job-after-free'
            })
        check('post-free predict accepted', status == 200, status)
        check('freed replica took the job',
              replica_stats(r1)['jobs'] == jobs_before + 1, replica_stats(r1))

        print('== phase 5: killed replica degrades to occupancy-unknown ==')
        procs[2].terminate()  # replica 3's process
        procs[2].join(timeout=5)
        _wait_for(lambda: capacity()['probed_replicas'] == 2,
                  10 * PROBE_INTERVAL_SECONDS, 'dead replica ages out')
        cap = capacity()
        check('ready still 3 (controller static)', cap['ready_replicas'] == 3,
              cap)
        check('probed drops to 2', cap['probed_replicas'] == 2, cap)
        check('capacity endpoint still serving', cap['synced'] is True, cap)

        print('\nALL PHASES PASSED')
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            p.join(timeout=5)


if __name__ == '__main__':
    sys.exit(main())
