"""Packed local scale lab for SkyServe external load balancer HA.

This exercises the real LB payload builder and occupancy probe path with two
slots per service.  Logical backend URLs are packed onto a bounded number of
aiohttp origins, so the lab qualifies serialization, request count, probe
concurrency, and response handling.  It deliberately does not claim to model
distinct-Pod-IP conntrack behavior; use the cluster qualification driver or
production telemetry for that attribution.

Example:

  PYTHONPATH=. python tests/load_tests/skyserve_lb_ha_scale.py \
      --services 10 --backends-per-service 500 --emulator-origins 100 \
      --output /tmp/skyserve-lb-ha-scale.json
"""

import argparse
import asyncio
import dataclasses
import json
import math
import multiprocessing
import pathlib
import platform
import statistics
import time
from typing import Any

from aiohttp import web

from sky.serve import lb_ha
from sky.serve import load_balancer

_MAX_SERVICES = 10
_MAX_BACKENDS_PER_SERVICE = 1000
_MAX_EMULATOR_ORIGINS = 100
_MAX_EMULATOR_WORKERS = 10
_DEFAULT_JITTER_WINDOW_SECONDS = 2.0
_DARWIN_EPHEMERAL_PORTS = 16384
_DARWIN_TIME_WAIT_SECONDS = 30.0


@dataclasses.dataclass(frozen=True)
class ScaleConfig:
    """Resource-bounded packed-lab dimensions."""

    services: int = 10
    backends_per_service: int = 500
    emulator_origins: int = 100
    emulator_workers: int = 10
    jitter_window_seconds: float = _DEFAULT_JITTER_WINDOW_SECONDS
    scenario_cooldown_seconds: float | None = None

    def validate(self) -> None:
        if not 1 <= self.services <= _MAX_SERVICES:
            raise ValueError(f'services must be in [1, {_MAX_SERVICES}]')
        if not 1 <= self.backends_per_service <= _MAX_BACKENDS_PER_SERVICE:
            raise ValueError('backends_per_service must be in '
                             f'[1, {_MAX_BACKENDS_PER_SERVICE}]')
        if not 1 <= self.emulator_origins <= _MAX_EMULATOR_ORIGINS:
            raise ValueError('emulator_origins must be in '
                             f'[1, {_MAX_EMULATOR_ORIGINS}]')
        if not 1 <= self.emulator_workers <= min(_MAX_EMULATOR_WORKERS,
                                                 self.emulator_origins):
            raise ValueError('emulator_workers must be in [1, min(10, '
                             'emulator_origins)]')
        if not 0 <= self.jitter_window_seconds <= 10:
            raise ValueError('jitter_window_seconds must be in [0, 10]')
        if (self.scenario_cooldown_seconds is not None and
                not 0 <= self.scenario_cooldown_seconds <= 120):
            raise ValueError('scenario_cooldown_seconds must be in [0, 120]')

    def effective_scenario_cooldown_seconds(self) -> float:
        if self.scenario_cooldown_seconds is not None:
            return self.scenario_cooldown_seconds
        connections_across_scenarios = (self.services * 2 *
                                        self.backends_per_service * 2)
        if (platform.system() == 'Darwin' and
                connections_across_scenarios > _DARWIN_EPHEMERAL_PORTS):
            # Darwin's default ephemeral range has 16,384 ports and its
            # default 15-second MSL retains a closed tuple for 30 seconds.
            return _DARWIN_TIME_WAIT_SECONDS + 1
        return 0


async def _start_emulator() -> tuple[web.AppRunner, str]:

    async def capacity(_: web.Request) -> web.Response:
        return web.json_response({
            'status': 'READY',
            'running_count': 0,
            'predict_concurrency': 1,
        })

    app = web.Application()
    app.router.add_post('/{tail:.*}', capacity)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    server = site._server  # pylint: disable=protected-access
    assert server is not None and server.sockets
    port = server.sockets[0].getsockname()[1]
    return runner, f'http://127.0.0.1:{port}'


def _run_emulator_worker(origin_count: int, connection: Any) -> None:
    """Serve a subset of origins on an event loop outside the LB process."""

    async def run() -> None:
        runners = []
        origins = []
        try:
            for _ in range(origin_count):
                runner, origin = await _start_emulator()
                runners.append(runner)
                origins.append(origin)
            connection.send({'origins': origins})
            await asyncio.Event().wait()
        finally:
            await asyncio.gather(*[runner.cleanup() for runner in runners],
                                 return_exceptions=True)

    try:
        asyncio.run(run())
    except BaseException as exc:  # pylint: disable=broad-exception-caught
        try:
            connection.send({'error': repr(exc)})
        except (BrokenPipeError, EOFError, OSError):
            pass
        raise
    finally:
        connection.close()


async def _start_emulator_workers(
        config: ScaleConfig) -> tuple[list[multiprocessing.Process], list[str]]:
    context = multiprocessing.get_context('spawn')
    processes = []
    receivers = []
    base, remainder = divmod(config.emulator_origins, config.emulator_workers)
    for worker_index in range(config.emulator_workers):
        receive, send = context.Pipe(duplex=False)
        origin_count = base + (1 if worker_index < remainder else 0)
        process = context.Process(target=_run_emulator_worker,
                                  args=(origin_count, send),
                                  daemon=True)
        process.start()
        send.close()
        processes.append(process)
        receivers.append(receive)
    messages = await asyncio.gather(
        *[asyncio.to_thread(receiver.recv) for receiver in receivers])
    for receiver in receivers:
        receiver.close()
    errors = [message['error'] for message in messages if 'error' in message]
    if errors:
        raise RuntimeError(f'Emulator worker failed: {errors}')
    origins = [origin for message in messages for origin in message['origins']]
    return processes, origins


def _make_lb(service_index: int, slot: lb_ha.LbSlot,
             urls: list[str]) -> load_balancer.SkyServeLoadBalancer:
    lb = load_balancer.SkyServeLoadBalancer(
        'http://unused-controller',
        8890,
        service_hash=f'scale-service-{service_index}',
        lb_slot=slot)
    lb._get_lb_session_id = (  # type: ignore[method-assign]  # pylint: disable=protected-access
        lambda: f'scale-{service_index}-{slot.value}')
    lb._lb_role_generation = 1  # pylint: disable=protected-access
    lb._routing_version = 1  # pylint: disable=protected-access
    with lb._client_pool_lock:  # pylint: disable=protected-access
        lb._load_balancing_policy.set_ready_replicas(  # pylint: disable=protected-access
            urls)
    return lb


def _percentile(samples: list[float], percentile: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    index = min(
        len(ordered) - 1, max(0,
                              math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]


async def run_scale_lab(config: ScaleConfig) -> dict[str, Any]:
    config.validate()
    emulator_processes: list[multiprocessing.Process] = []
    started_at = time.time()
    try:
        emulator_processes, origins = await _start_emulator_workers(config)

        lbs: list[load_balancer.SkyServeLoadBalancer] = []
        service_lbs: list[list[load_balancer.SkyServeLoadBalancer]] = []
        for service_index in range(config.services):
            service_origins = origins[service_index::config.services] or origins
            urls = [
                f'{service_origins[index % len(service_origins)]}/'
                f'service-{service_index}/backend-{index}'
                for index in range(config.backends_per_service)
            ]
            pair = [
                _make_lb(service_index, slot, urls) for slot in lb_ha.LbSlot
            ]
            service_lbs.append(pair)
            lbs.extend(pair)

        synchronized_started_at = time.monotonic()
        synchronized_wave_seconds = []
        # Each real service has two LB Pods and therefore two independent
        # source-IP/ephemeral-port pools.  A portable localhost process has
        # only one such pool, so exercise one complete service pair at a time.
        # The cluster driver is the authority for the aggregate 20-Pod burst.
        for pair in service_lbs:
            wave_started_at = time.monotonic()
            await asyncio.gather(*[
                lb._probe_replica_occupancy_once()  # pylint: disable=protected-access
                for lb in pair
            ])
            synchronized_wave_seconds.append(time.monotonic() - wave_started_at)
        synchronized_seconds = time.monotonic() - synchronized_started_at
        scenario_cooldown_seconds = (
            config.effective_scenario_cooldown_seconds())
        if scenario_cooldown_seconds:
            await asyncio.sleep(scenario_cooldown_seconds)

        async def jittered_probe(
                delay_seconds: float,
                lb: load_balancer.SkyServeLoadBalancer) -> None:
            await asyncio.sleep(delay_seconds)
            await lb._probe_replica_occupancy_once()  # pylint: disable=protected-access

        jittered_started_at = time.monotonic()
        jittered_wave_seconds = []
        for pair in service_lbs:
            wave_started_at = time.monotonic()
            await asyncio.gather(
                jittered_probe(0, pair[0]),
                jittered_probe(config.jitter_window_seconds, pair[1]))
            jittered_wave_seconds.append(time.monotonic() - wave_started_at)
        jittered_seconds = time.monotonic() - jittered_started_at

        role_payload_sizes = []
        role_serialization_seconds = []
        per_lb = []
        for lb in lbs:
            payload_started_at = time.monotonic()
            payload = lb._ha_role_payload()  # pylint: disable=protected-access
            payload_bytes = len(json.dumps(payload).encode('utf-8'))
            role_serialization_seconds.append(time.monotonic() -
                                              payload_started_at)
            role_payload_sizes.append(payload_bytes)
            per_lb.append(lb._ha_stats().snapshot())  # pylint: disable=protected-access

        probe_rounds = [
            item['probe']['round_seconds']['last']
            for item in per_lb
            if item['probe']['round_seconds']['last'] is not None
        ]
        total_attempted = sum(item['probe']['attempted'] for item in per_lb)
        total_succeeded = sum(item['probe']['succeeded'] for item in per_lb)
        total_unknown = sum(item['probe']['unknown'] for item in per_lb)
        return {
            'schema_version': 2,
            'started_at_unix': started_at,
            'finished_at_unix': time.time(),
            'config': dataclasses.asdict(config),
            'topology': {
                'logical_lb_instances': len(lbs),
                'logical_backend_urls':
                    (config.services * config.backends_per_service),
                'emulator_origins': len(origins),
                'distinct_ip_fidelity': False,
                'aggregate_network_fidelity': False,
                'probe_concurrency_fidelity': 'per-service-pair',
                'limitation':
                    ('Packed localhost origins do not model 20 distinct Pod '
                     'source IPs, conntrack, or aggregate Kubernetes API load.'
                    ),
            },
            'probe': {
                'synchronized_wall_seconds': synchronized_seconds,
                'synchronized_wave_max_seconds': max(synchronized_wave_seconds,
                                                     default=None),
                'jittered_wall_seconds': jittered_seconds,
                'jittered_wave_max_seconds': max(jittered_wave_seconds,
                                                 default=None),
                'scenario_cooldown_seconds': scenario_cooldown_seconds,
                'round_p50_seconds': _percentile(probe_rounds, 0.50),
                'round_p99_seconds': _percentile(probe_rounds, 0.99),
                'round_max_seconds': max(probe_rounds, default=None),
                'attempted': total_attempted,
                'succeeded': total_succeeded,
                'unknown': total_unknown,
            },
            'role_payload': {
                'bytes_min': min(role_payload_sizes),
                'bytes_mean': statistics.fmean(role_payload_sizes),
                'bytes_max': max(role_payload_sizes),
                'serialization_p99_seconds': _percentile(
                    role_serialization_seconds, 0.99),
                'serialization_max_seconds': max(role_serialization_seconds),
            },
            'gates': {
                'complete_probe_samples': total_unknown == 0,
                'probe_round_within_promotion_freshness': max(probe_rounds,
                                                              default=0) <= 15,
                'role_payload_under_one_mib': max(role_payload_sizes) < 1024**2,
            },
            'per_lb': per_lb,
        }
    finally:
        for process in emulator_processes:
            if process.is_alive():
                process.terminate()
        for process in emulator_processes:
            await asyncio.to_thread(process.join, 5)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--services', type=int, default=10)
    parser.add_argument('--backends-per-service', type=int, default=500)
    parser.add_argument('--emulator-origins', type=int, default=100)
    parser.add_argument('--emulator-workers', type=int, default=10)
    parser.add_argument('--jitter-window-seconds', type=float, default=2.0)
    parser.add_argument('--scenario-cooldown-seconds', type=float)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = ScaleConfig(
        services=args.services,
        backends_per_service=args.backends_per_service,
        emulator_origins=args.emulator_origins,
        emulator_workers=args.emulator_workers,
        jitter_window_seconds=args.jitter_window_seconds,
        scenario_cooldown_seconds=(args.scenario_cooldown_seconds))
    artifact = asyncio.run(run_scale_lab(config))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) +
                           '\n',
                           encoding='utf-8')
    failed = [name for name, passed in artifact['gates'].items() if not passed]
    if failed:
        raise SystemExit(f'HA scale gates failed: {", ".join(failed)}')
    print(
        json.dumps(
            {
                'output': str(args.output),
                'gates': artifact['gates'],
                'probe': artifact['probe'],
                'role_payload': artifact['role_payload'],
            },
            indent=2,
            sort_keys=True))


if __name__ == '__main__':
    main()
