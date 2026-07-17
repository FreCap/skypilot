"""Real-cluster qualification driver for SkyServe external LB HA.

The packed scale lab covers 5,000 logical URLs cheaply. This driver covers the
parts that cannot be emulated faithfully: stable Services, EndpointSlices,
both LB slots, Pod identity, real endpoint traffic, and active-Pod loss. It
expects the services to exist already and never creates GPU replicas.

The target file is JSON and must contain exactly the requested service count:

  {"services": [{"name": "svc-a", "url": "https://.../health",
                  "method": "GET", "expected_status": 200,
                  "headers_env": {"Authorization": "SVC_A_AUTH"}}]}

Header values are read from environment variables and are never written to the
artifact. For a planned upgrade, pass the exact update command after ``--``.

  PYTHONPATH=. python tests/skyserve/high_availability/qualify_cluster.py \
      --targets /tmp/ha-targets.json --namespace skypilot \
      --mode planned --output /tmp/ha-cluster.json -- \
      sky serve update svc-a service.yaml
"""

import argparse
import asyncio
import dataclasses
import json
import math
import os
import pathlib
import re
import time
from typing import Any

import aiohttp

from sky.serve import constants as serve_constants

_LB_LABEL = 'skypilot-serve-lb'
_HASH_LABEL = 'skypilot-serve-incarnation'
_SLOT_LABEL = 'skypilot-serve-lb-slot'
_LB_PORT = serve_constants.LOAD_BALANCER_PORT_START
_LB_MEMORY_LIMIT_BYTES = 512 * 1024**2
_MAX_DURATION_SECONDS = 30 * 60
_ROLE_RECOVERY_MAX_SECONDS = 15
_EXPECTED_ROLE_OUTCOMES = {
    'planned': {
        'success', 'pod_authority_unavailable', 'pod_not_authoritative',
        'cutover_state_unavailable', 'routing_not_converged'
    },
    'active-loss': {
        'success', 'pod_not_authoritative', 'routing_not_converged'
    },
    'observe': {'success'},
}


@dataclasses.dataclass(frozen=True)
class Target:
    """One existing HA service's safe data-plane qualification target."""

    name: str
    url: str
    method: str = 'GET'
    expected_status: int = 200
    body: Any = None
    headers_env: dict[str, str] = dataclasses.field(default_factory=dict)

    def headers(self) -> dict[str, str]:
        resolved = {}
        for header, env_var in self.headers_env.items():
            value = os.environ.get(env_var)
            if value is None:
                raise ValueError(
                    f'Missing environment variable {env_var!r} for {self.name}')
            resolved[header] = value
        return resolved


class KubectlRecorder:
    """Run kubectl without a shell and retain bounded client-side evidence."""

    def __init__(self, namespace: str, context: str | None) -> None:
        self._base = ['kubectl']
        if context:
            self._base += ['--context', context]
        self._base += ['--namespace', namespace]
        self.observations: list[dict[str, Any]] = []

    async def run(self, operation: str, *args: str, timeout: float = 30) -> str:
        started_at = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            *self._base,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        outcome = 'success'
        stderr_text = ''
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(),
                                                    timeout=timeout)
            stderr_text = stderr.decode('utf-8', errors='replace')
            if process.returncode != 0:
                outcome = _kubectl_error_category(stderr_text)
                raise RuntimeError(
                    f'kubectl {operation} failed: {stderr_text[:500]}')
            return stdout.decode('utf-8')
        except asyncio.TimeoutError:
            outcome = 'timeout'
            process.kill()
            await process.wait()
            raise
        finally:
            self.observations.append({
                'operation': operation,
                'outcome': outcome,
                'duration_seconds': time.monotonic() - started_at,
            })

    async def json(self, operation: str, *args: str) -> dict[str, Any]:
        return json.loads(await self.run(operation, *args, '-o', 'json'))

    async def watch_endpoint_slices(self, stable_services: set[str],
                                    initial: dict[str,
                                                  Any], duration_seconds: float,
                                    started_at: float) -> list[dict[str, Any]]:
        """Record aggregate Ready-endpoint continuity from one API watch."""
        state: dict[str, tuple[str, bool]] = {}
        for item in initial.get('items', []):
            metadata = item.get('metadata', {})
            service_name = metadata.get('labels',
                                        {}).get('kubernetes.io/service-name')
            slice_name = metadata.get('name')
            if service_name not in stable_services or not slice_name:
                continue
            ready = any(
                endpoint.get('conditions', {}).get('ready') is True
                for endpoint in item.get('endpoints', []))
            state[slice_name] = (service_name, ready)

        def aggregate_events() -> list[dict[str, Any]]:
            return [{
                'elapsed_seconds': max(0.0,
                                       time.monotonic() - started_at),
                'service_resource': service_name,
                'ready': any(ready
                             for current_service, ready in state.values()
                             if current_service == service_name),
            }
                    for service_name in sorted(stable_services)]

        events = aggregate_events()
        columns = (
            'custom-columns=TYPE:.type,NAME:.object.metadata.name,'
            'SERVICE:.object.metadata.labels.kubernetes\\.io/service-name,'
            'READY:.object.endpoints[*].conditions.ready')
        command = self._base + [
            'get', 'endpointslices', '--watch-only', '--output-watch-events',
            '-o', columns, '--no-headers'
        ]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        watch_started_at = time.monotonic()
        outcome = 'success'
        covered_full_duration = False
        assert process.stdout is not None
        assert process.stderr is not None
        try:
            deadline = started_at + duration_seconds
            while time.monotonic() < deadline:
                timeout = max(0.01, deadline - time.monotonic())
                try:
                    raw_line = await asyncio.wait_for(process.stdout.readline(),
                                                      timeout=timeout)
                except asyncio.TimeoutError:
                    covered_full_duration = True
                    break
                if not raw_line:
                    await process.wait()
                    stderr = await process.stderr.read()
                    outcome = _kubectl_error_category(
                        stderr.decode('utf-8', errors='replace'))
                    break
                fields = raw_line.decode('utf-8', errors='replace').split()
                if len(fields) < 3:
                    continue
                event_type, slice_name, service_name = fields[:3]
                if service_name not in stable_services:
                    continue
                if event_type == 'DELETED':
                    state.pop(slice_name, None)
                else:
                    ready_text = ' '.join(fields[3:]).lower()
                    state[slice_name] = (service_name,
                                         re.search(r'\btrue\b', ready_text)
                                         is not None)
                events.extend(aggregate_events())
            if time.monotonic() >= deadline:
                covered_full_duration = True
        except Exception:  # pylint: disable=broad-except
            outcome = 'error'
            raise
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            self.observations.append({
                'operation': 'endpointslice-watch',
                'outcome': outcome,
                'duration_seconds': time.monotonic() - watch_started_at,
                'covered_full_duration': covered_full_duration,
            })
        return events


def _kubectl_error_category(stderr: str) -> str:
    lowered = stderr.lower()
    if 'too many requests' in lowered or re.search(r'\b429\b', lowered):
        return '429'
    if re.search(r'\b5\d\d\b', lowered):
        return '5xx'
    return 'error'


def _load_targets(path: pathlib.Path, expected_services: int) -> list[Target]:
    raw = json.loads(path.read_text(encoding='utf-8'))
    services = raw.get('services') if isinstance(raw, dict) else None
    if not isinstance(services, list) or len(services) != expected_services:
        raise ValueError(
            f'target file must contain exactly {expected_services} '
            'services')
    targets = []
    names = set()
    for item in services:
        if not isinstance(item, dict):
            raise ValueError('each service target must be an object')
        target = Target(**item)
        if not target.name or not target.url or target.name in names:
            raise ValueError('service names and URLs must be non-empty and '
                             'service names must be unique')
        if target.method.upper() not in {'GET', 'POST', 'PUT', 'DELETE'}:
            raise ValueError(f'unsupported method for {target.name}')
        names.add(target.name)
        targets.append(target)
    return targets


async def _discover_service(kubectl: KubectlRecorder,
                            target: Target) -> dict[str, Any]:
    selector = f'{_LB_LABEL}={target.name}'
    pods, services = await asyncio.gather(
        kubectl.json(f'{target.name}:pods', 'get', 'pods', '-l', selector),
        kubectl.json(f'{target.name}:service', 'get', 'services', '-l',
                     selector))
    pod_items = pods.get('items', [])
    service_items = services.get('items', [])
    if len(service_items) != 1:
        raise RuntimeError(f'{target.name}: expected one stable LB Service, '
                           f'found {len(service_items)}')
    service = service_items[0]
    service_hash = (service.get('metadata', {}).get('labels',
                                                    {}).get(_HASH_LABEL))
    selected_slot = service.get('spec', {}).get('selector', {}).get(_SLOT_LABEL)
    slots: dict[str, dict[str, Any]] = {}
    for pod in pod_items:
        metadata = pod.get('metadata', {})
        labels = metadata.get('labels', {})
        if labels.get(_HASH_LABEL) != service_hash:
            continue
        if metadata.get('deletionTimestamp') is not None:
            continue
        slot = labels.get(_SLOT_LABEL)
        if slot not in ('a', 'b'):
            continue
        if slot in slots:
            raise RuntimeError(f'{target.name}: multiple non-terminating Pods '
                               f'claim LB slot {slot!r}')
        ready = any(
            condition.get('type') == 'Ready' and
            condition.get('status') == 'True'
            for condition in pod.get('status', {}).get('conditions', []))
        slots[slot] = {
            'name': metadata.get('name'),
            'uid': metadata.get('uid'),
            'ready': ready,
            'deletion_timestamp': metadata.get('deletionTimestamp'),
        }
    return {
        'service_name': service.get('metadata', {}).get('name'),
        'service_hash': service_hash,
        'selected_slot': selected_slot,
        'slots': slots,
    }


def _lb_snapshot_script() -> str:
    """Build an in-container snapshot read without exposing auth material."""
    capacity_url = f'http://127.0.0.1:{_LB_PORT}/_lb/capacity'
    return (
        "import json,os,urllib.request;"
        f"configured=os.environ.get({serve_constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR!r});"
        f"path=os.environ.get({serve_constants.LB_AUTH_TOKENS_FILE_ENV_VAR!r});"
        f"fallback=os.environ.get({serve_constants.LB_AUTH_TOKEN_ENV_VAR!r});"
        "assert configured in (None,'true','false'),'invalid auth capability';"
        "enabled=bool(path or fallback) if configured is None else configured=='true';"
        "token=next((line.strip() for line in open(path,encoding='utf-8') "
        "if line.strip()),None) if path else fallback;"
        "assert not enabled or token,'LB data-plane auth token unavailable';"
        f"headers={{{serve_constants.LB_AUTHORIZATION_HEADER!r}:'Bearer '+token}} if enabled else {{}};"
        f"request=urllib.request.Request({capacity_url!r},headers=headers);"
        "d=json.load(urllib.request.urlopen(request,timeout=5));"
        "rss=0;"
        "lines=open('/proc/1/status',encoding='utf-8').read().splitlines();"
        "rss=next((int(x.split()[1])*1024 for x in lines "
        "if x.startswith('VmRSS:')),0);"
        "d['process_rss_bytes']=rss;print(json.dumps(d))")


async def _read_lb_snapshot(kubectl: KubectlRecorder, service_name: str,
                            pod_name: str) -> dict[str, Any]:
    raw = await kubectl.run(f'{service_name}:lb-snapshot', 'exec', pod_name,
                            '-c', 'load-balancer', '--', 'python', '-c',
                            _lb_snapshot_script())
    return json.loads(raw)


async def _snapshots(kubectl: KubectlRecorder,
                     topology: dict[str, dict[str, Any]]) -> dict:
    tasks = []
    keys = []
    for service_name, service in topology.items():
        for slot, pod in service['slots'].items():
            keys.append(f'{service_name}:{slot}')
            tasks.append(_read_lb_snapshot(kubectl, service_name, pod['name']))
    values = await asyncio.gather(*tasks)
    return dict(zip(keys, values))


async def _traffic_request(target: Target, session: aiohttp.ClientSession,
                           headers: dict[str, str], started_at: float,
                           samples: list[dict[str, Any]]) -> None:
    request_started_at = time.monotonic()
    status = None
    error = None
    try:
        async with session.request(
                target.method.upper(),
                target.url,
                headers=headers,
                json=target.body,
                timeout=aiohttp.ClientTimeout(total=5)) as response:
            status = response.status
            await response.read()
    except Exception as exc:  # pylint: disable=broad-except
        error = type(exc).__name__
    samples.append({
        'service': target.name,
        'elapsed_seconds': request_started_at - started_at,
        'latency_seconds': time.monotonic() - request_started_at,
        'status': status,
        'expected': status == target.expected_status and error is None,
        'error': error,
    })


async def _traffic_loop(target: Target, session: aiohttp.ClientSession,
                        duration_seconds: float, requests_per_second: float,
                        started_at: float, samples: list[dict[str,
                                                              Any]]) -> None:
    interval = 1 / requests_per_second
    deadline = started_at + duration_seconds
    next_request_at = started_at
    headers = target.headers()
    pending: set[asyncio.Task] = set()
    while next_request_at < deadline:
        await asyncio.sleep(max(0.0, next_request_at - time.monotonic()))
        task = asyncio.create_task(
            _traffic_request(target, session, headers, started_at, samples))
        pending.add(task)
        task.add_done_callback(pending.discard)
        next_request_at += interval
    if pending:
        await asyncio.gather(*pending)


async def _traffic_target(target: Target, duration_seconds: float,
                          requests_per_second: float, started_at: float,
                          samples: list[dict[str, Any]]) -> None:
    # Isolate each service's recovery measurement. A simultaneous fleet fault
    # must not queue one service behind another in the test client's connector.
    connection_limit = max(20, math.ceil(requests_per_second * 5) + 5)
    connector = aiohttp.TCPConnector(limit=connection_limit,
                                     limit_per_host=connection_limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _traffic_loop(target, session, duration_seconds,
                            requests_per_second, started_at, samples)


async def _run_fault(mode: str, command: list[str], fault_at: float,
                     started_at: float, kubectl: KubectlRecorder,
                     targets: list[Target]) -> dict[str, Any]:
    await asyncio.sleep(max(0.0, started_at + fault_at - time.monotonic()))
    if mode == 'observe':
        return {
            'triggered_at_seconds': time.monotonic() - started_at,
            'action': 'none'
        }
    if mode == 'planned':
        if not command:
            raise ValueError('planned mode requires an update command after --')
        triggered_at = time.monotonic() - started_at
        process = await asyncio.create_subprocess_exec(*command)
        returncode = await process.wait()
        if returncode != 0:
            raise RuntimeError(f'planned update command exited {returncode}')
        return {
            'triggered_at_seconds': triggered_at,
            'action': 'planned-command',
            'returncode': returncode,
        }

    topology = dict(
        zip([target.name for target in targets], await asyncio.gather(
            *[_discover_service(kubectl, target) for target in targets])))
    active_pods = []
    for service_name, service in topology.items():
        selected_slot = service['selected_slot']
        active = service['slots'].get(selected_slot)
        if active is None:
            raise RuntimeError(f'{service_name}: no Pod for selected slot '
                               f'{selected_slot!r}')
        active_pods.append(active['name'])
    await kubectl.run('delete-active-pods', 'delete', 'pods', *active_pods,
                      '--wait=false')
    return {
        # Start the recovery clock only after the API server accepts deletion.
        'triggered_at_seconds': time.monotonic() - started_at,
        'action': 'delete-active-pods',
        'pods': active_pods,
    }


def _counter_delta(before: dict, after: dict) -> dict[str, int]:
    keys = set(before) | set(after)
    if any(int(after.get(key, 0)) < int(before.get(key, 0)) for key in keys):
        # A same-Pod container restart resets every process-local counter while
        # preserving the Pod UID. Treat the post-reset counters as run samples.
        return {key: max(0, int(after.get(key, 0))) for key in sorted(keys)}
    return {
        key: max(0,
                 int(after.get(key, 0)) -
                 int(before.get(key, 0))) for key in sorted(keys)
    }


def _worst_traffic_recovery_seconds(service_samples: list[dict[str, Any]],
                                    fault_at: float) -> float | None:
    """Return the worst post-fault failure streak, or None if still failed."""
    recovery_seconds = []
    failure_started_at = None
    saw_post_fault_sample = False
    for sample in sorted(service_samples,
                         key=lambda item: item['elapsed_seconds']):
        elapsed = float(sample['elapsed_seconds'])
        if elapsed < fault_at:
            continue
        saw_post_fault_sample = True
        if not sample['expected'] and failure_started_at is None:
            failure_started_at = elapsed
        elif sample['expected'] and failure_started_at is not None:
            recovery_seconds.append(max(0.0, elapsed - failure_started_at))
            failure_started_at = None
    if failure_started_at is not None:
        return None
    if not saw_post_fault_sample:
        return None
    return max(recovery_seconds, default=0.0)


def _role_kubernetes_p99(before: dict[str, dict[str, Any]],
                         after: dict[str, dict[str, Any]]) -> float | None:
    """Estimate run-window p99 from fixed cumulative histogram deltas."""
    aggregate_counts: list[int] | None = None
    upper_bounds: list[float] | None = None
    for key, final_snapshot in after.items():
        initial_snapshot = before.get(key, {})
        same_process = (initial_snapshot.get('lb_pod_uid') is not None and
                        initial_snapshot.get('lb_pod_uid')
                        == final_snapshot.get('lb_pod_uid'))
        initial_phases = initial_snapshot.get('ha_observability', {}).get(
            'role', {}).get('controller', {}).get('phases_seconds', {})
        final_phases = final_snapshot.get('ha_observability', {}).get(
            'role', {}).get('controller', {}).get('phases_seconds', {})
        for phase, final_stats in final_phases.items():
            if not phase.startswith('kubernetes_') or not isinstance(
                    final_stats, dict):
                continue
            final_histogram = final_stats.get('histogram')
            if not isinstance(final_histogram, dict):
                continue
            bounds = final_histogram.get('upper_bounds')
            counts = final_histogram.get('counts')
            if (not isinstance(bounds, list) or not isinstance(counts, list) or
                    len(counts) != len(bounds) + 1 or not all(
                        isinstance(value, (int, float)) for value in bounds) or
                    not all(isinstance(value, int) for value in counts)):
                continue
            normalized_bounds = [float(value) for value in bounds]
            if upper_bounds is None:
                upper_bounds = normalized_bounds
                aggregate_counts = [0] * len(counts)
            if normalized_bounds != upper_bounds or aggregate_counts is None:
                continue
            initial_counts = [0] * len(counts)
            initial_stats = initial_phases.get(phase)
            if same_process and isinstance(initial_stats, dict):
                initial_histogram = initial_stats.get('histogram')
                if (isinstance(initial_histogram, dict) and
                        initial_histogram.get('upper_bounds') == bounds and
                        isinstance(initial_histogram.get('counts'), list) and
                        len(initial_histogram['counts']) == len(counts)):
                    initial_counts = initial_histogram['counts']
            if any(final < initial
                   for final, initial in zip(counts, initial_counts)):
                # Treat a process-local counter reset as all-new run samples.
                initial_counts = [0] * len(counts)
            for index, (final,
                        initial) in enumerate(zip(counts, initial_counts)):
                aggregate_counts[index] += final - initial
    if upper_bounds is None or aggregate_counts is None:
        return None
    total = sum(aggregate_counts)
    if total == 0:
        return None
    target = max(1, math.ceil(total * 0.99))
    cumulative = 0
    for index, count in enumerate(aggregate_counts):
        cumulative += count
        if cumulative >= target:
            # The final bucket is overflow. The largest finite bound still
            # fails the fixed +100 ms/25% material-regression gate.
            return upper_bounds[min(index, len(upper_bounds) - 1)]
    return None


def evaluate_gates(
        *,
        targets: list[Target],
        mode: str,
        expected_backends: int,
        fault: dict[str, Any],
        samples: list[dict[str, Any]],
        before: dict,
        after: dict,
        kubectl_observations: list[dict[str, Any]],
        endpoint_events: list[dict[str, Any]] | None = None,
        single_service_kubernetes_p99: float | None = None,
        minimum_samples_per_service: int | None = None) -> dict[str, Any]:
    failures = [sample for sample in samples if not sample['expected']]
    fault_at = float(fault['triggered_at_seconds'])
    pre_fault_failures = [
        sample for sample in failures if sample['elapsed_seconds'] < fault_at
    ]
    sample_counts = {
        target.name: sum(1
                         for sample in samples
                         if sample['service'] == target.name
                        ) for target in targets
    }
    insufficient_sample_services = [
        name for name, count in sample_counts.items()
        if minimum_samples_per_service is not None and
        count < minimum_samples_per_service
    ]
    recovery_seconds: dict[str, float | None] = {}
    for target in targets:
        recovery_seconds[target.name] = _worst_traffic_recovery_seconds(
            [sample for sample in samples if sample['service'] == target.name],
            fault_at)

    unexpected_role_outcomes = {}
    unrecovered_role_channels = {}
    rss_over_limit = []
    incomplete_backends = []
    for key, final_snapshot in after.items():
        initial_snapshot = before.get(key, {})
        before_outcomes = initial_snapshot.get('ha_observability',
                                               {}).get('role',
                                                       {}).get('outcomes', {})
        after_outcomes = final_snapshot.get('ha_observability',
                                            {}).get('role',
                                                    {}).get('outcomes', {})
        same_process = (initial_snapshot.get('lb_pod_uid') is not None and
                        initial_snapshot.get('lb_pod_uid')
                        == final_snapshot.get('lb_pod_uid'))
        delta = (_counter_delta(before_outcomes, after_outcomes)
                 if same_process else {
                     outcome: max(0, int(count))
                     for outcome, count in after_outcomes.items()
                 })
        unexpected = {
            outcome: count
            for outcome, count in delta.items()
            if count and outcome not in _EXPECTED_ROLE_OUTCOMES[mode]
        }
        if unexpected:
            unexpected_role_outcomes[key] = unexpected
        transient_count = sum(count for outcome, count in delta.items()
                              if outcome not in ('success', 'legacy_mode'))
        initial_role = initial_snapshot.get('ha_observability',
                                            {}).get('role', {})
        final_role = final_snapshot.get('ha_observability', {}).get('role', {})
        if transient_count or initial_role.get('failure_streak_active') is True:
            last_recovery = final_role.get('last_failure_recovery_seconds')
            max_recovery = final_role.get('max_failure_recovery_seconds')
            if (final_role.get('failure_streak_active') is not False or
                    final_role.get('last_outcome') not in ('success',
                                                           'legacy_mode') or
                    not isinstance(last_recovery, (int, float)) or
                    not isinstance(max_recovery, (int, float)) or
                    max_recovery > _ROLE_RECOVERY_MAX_SECONDS):
                unrecovered_role_channels[key] = {
                    'last_outcome': final_role.get('last_outcome'),
                    'failure_streak_active':
                        final_role.get('failure_streak_active'),
                    'last_failure_recovery_seconds': last_recovery,
                    'max_failure_recovery_seconds': max_recovery,
                }
        if final_snapshot.get('process_rss_bytes',
                              0) > (0.75 * _LB_MEMORY_LIMIT_BYTES):
            rss_over_limit.append(key)
        if (final_snapshot.get('routing_backend_count', 0)
                < expected_backends or final_snapshot.get(
                    'occupancy_probed_backend_count', 0) < expected_backends):
            incomplete_backends.append(key)

    bad_kubectl = [
        item for item in kubectl_observations
        if item['outcome'] in ('429', '5xx', 'timeout')
    ]
    if mode == 'planned':
        availability_pass = not failures
    elif mode == 'active-loss':
        availability_pass = (not pre_fault_failures and
                             all(recovery is not None and recovery <= 15
                                 for recovery in recovery_seconds.values()))
    else:
        availability_pass = not failures
    empty_endpoint_events = [
        event for event in (endpoint_events or []) if not event['ready']
    ]
    endpoint_watch_valid = any(
        item.get('operation') == 'endpointslice-watch' and item.get('outcome')
        == 'success' and item.get('covered_full_duration') is True
        for item in kubectl_observations)
    endpoint_continuity_pass = (endpoint_watch_valid and
                                not empty_endpoint_events if mode in ('planned',
                                                                      'observe')
                                else endpoint_watch_valid)
    role_kubernetes_p99 = _role_kubernetes_p99(before, after)
    if single_service_kubernetes_p99 is None:
        kubernetes_latency_scaling_pass = role_kubernetes_p99 is not None
    else:
        kubernetes_latency_scaling_pass = (
            role_kubernetes_p99 is not None and
            not (role_kubernetes_p99 > 1.25 * single_service_kubernetes_p99 and
                 role_kubernetes_p99 > single_service_kubernetes_p99 + 0.1))
    return {
        'availability': availability_pass,
        'traffic_sample_count': not insufficient_sample_services,
        'endpoint_ready_continuity': endpoint_continuity_pass,
        'no_pre_fault_failures': not pre_fault_failures,
        'role_outcomes_classified': not unexpected_role_outcomes,
        'role_channel_recovered': not unrecovered_role_channels,
        'kubernetes_client_clean': not bad_kubectl,
        'kubernetes_role_read_p99_scaling': kubernetes_latency_scaling_pass,
        'lb_rss_under_75_percent': not rss_over_limit,
        'complete_backend_samples': not incomplete_backends,
        'details': {
            'request_failures': len(failures),
            'sample_counts': sample_counts,
            'minimum_samples_per_service': minimum_samples_per_service,
            'insufficient_sample_services': insufficient_sample_services,
            'pre_fault_failures': len(pre_fault_failures),
            'recovery_seconds': recovery_seconds,
            'unexpected_role_outcomes': unexpected_role_outcomes,
            'unrecovered_role_channels': unrecovered_role_channels,
            'bad_kubectl_observations': bad_kubectl,
            'rss_over_limit': rss_over_limit,
            'incomplete_backends': incomplete_backends,
            'empty_endpoint_events': empty_endpoint_events,
            'endpoint_watch_valid': endpoint_watch_valid,
            'role_controller_kubernetes_p99_seconds': role_kubernetes_p99,
            'single_service_kubernetes_p99_seconds': single_service_kubernetes_p99,
        },
    }


def _topology_errors(
        topology: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        name: service
        for name, service in topology.items()
        if set(service['slots']) != {'a', 'b'} or not all(
            pod['ready'] for pod in service['slots'].values()) or
        service['selected_slot'] not in ('a', 'b')
    }


async def run(args: argparse.Namespace,
              targets: list[Target]) -> dict[str, Any]:
    kubectl = KubectlRecorder(args.namespace, args.context)
    inventory = await kubectl.json('ha-services:inventory', 'get', 'services',
                                   '-l', _LB_LABEL)
    inventory_names = {
        item.get('metadata', {}).get('labels', {}).get(_LB_LABEL)
        for item in inventory.get('items', [])
    }
    target_names = {target.name for target in targets}
    if inventory_names != target_names:
        raise RuntimeError(
            'Qualification namespace must contain exactly the target HA '
            f'services; found {sorted(str(name) for name in inventory_names)}, '
            f'expected {sorted(target_names)}')
    topology_values = await asyncio.gather(
        *[_discover_service(kubectl, target) for target in targets])
    topology = dict(zip([target.name for target in targets], topology_values))
    topology_errors = _topology_errors(topology)
    if topology_errors:
        raise RuntimeError(f'HA topology is not ready: {topology_errors}')
    before = await _snapshots(kubectl, topology)

    started_at = time.monotonic()
    stable_services = {service['service_name'] for service in topology.values()}
    initial_endpoint_slices = await kubectl.json('endpointslices:initial',
                                                 'get', 'endpointslices')
    endpoint_watch = asyncio.create_task(
        kubectl.watch_endpoint_slices(stable_services, initial_endpoint_slices,
                                      args.duration_seconds, started_at))
    samples: list[dict[str, Any]] = []
    traffic_tasks = [
        asyncio.create_task(
            _traffic_target(target, args.duration_seconds,
                            args.requests_per_second, started_at, samples))
        for target in targets
    ]
    fault_task = asyncio.create_task(
        _run_fault(args.mode, args.command, args.fault_at_seconds, started_at,
                   kubectl, targets))
    await asyncio.gather(*traffic_tasks)
    fault = await fault_task
    endpoint_events = await endpoint_watch

    final_topology_values = await asyncio.gather(
        *[_discover_service(kubectl, target) for target in targets])
    final_topology = dict(
        zip([target.name for target in targets], final_topology_values))
    final_topology_errors = _topology_errors(final_topology)
    if final_topology_errors:
        raise RuntimeError(
            f'HA topology was not restored: {final_topology_errors}')
    after = await _snapshots(kubectl, final_topology)
    single_service_kubernetes_p99 = None
    if args.single_service_baseline is not None:
        baseline_artifact = json.loads(
            args.single_service_baseline.read_text(encoding='utf-8'))
        single_service_kubernetes_p99 = baseline_artifact.get(
            'summary', {}).get('role_controller_kubernetes_p99_seconds')
        if not isinstance(single_service_kubernetes_p99, (int, float)):
            raise ValueError('single-service baseline lacks a numeric role '
                             'controller Kubernetes p99')
    gates = evaluate_gates(
        targets=targets,
        mode=args.mode,
        expected_backends=args.expected_backends,
        fault=fault,
        samples=samples,
        before=before,
        after=after,
        kubectl_observations=kubectl.observations,
        endpoint_events=endpoint_events,
        single_service_kubernetes_p99=(float(single_service_kubernetes_p99)
                                       if single_service_kubernetes_p99
                                       is not None else None),
        minimum_samples_per_service=math.floor(args.duration_seconds *
                                               args.requests_per_second))
    return {
        'schema_version': 1,
        'mode': args.mode,
        'duration_seconds': args.duration_seconds,
        'requests_per_second_per_service': args.requests_per_second,
        'service_names': [target.name for target in targets],
        'qualified_namespace_ha_services': sorted(target_names),
        'topology_before': topology,
        'topology_after': final_topology,
        'lb_snapshots_before': before,
        'lb_snapshots_after': after,
        'fault': fault,
        'traffic_samples': samples,
        'kubectl_observations': kubectl.observations,
        'endpoint_events': endpoint_events,
        'gates': gates,
        'summary': {
            'role_controller_kubernetes_p99_seconds':
                gates['details']['role_controller_kubernetes_p99_seconds'],
        },
        'required_external_evidence': [
            'API-server CPU, memory, event-loop delay, and request headroom',
            'PostgreSQL CPU, memory, connection, and I/O headroom',
        ],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--targets', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--context')
    parser.add_argument('--expected-services', type=int, default=10)
    parser.add_argument('--expected-backends', type=int, default=500)
    parser.add_argument('--single-service-baseline', type=pathlib.Path)
    parser.add_argument('--duration-seconds', type=float, default=120)
    parser.add_argument('--requests-per-second', type=float, default=10)
    parser.add_argument('--fault-at-seconds', type=float, default=30)
    parser.add_argument('--mode',
                        choices=('planned', 'active-loss', 'observe'),
                        required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == '--':
        args.command = args.command[1:]
    if not 0 < args.duration_seconds <= _MAX_DURATION_SECONDS:
        parser.error('duration must be in (0, 1800] seconds')
    if not 0 < args.requests_per_second <= 100:
        parser.error('requests per second must be in (0, 100]')
    if not 0 <= args.fault_at_seconds < args.duration_seconds:
        parser.error('fault time must fall inside the run')
    if args.expected_services > 1 and args.single_service_baseline is None:
        parser.error('multi-service qualification requires '
                     '--single-service-baseline')
    return args


def main() -> None:
    args = _parse_args()
    targets = _load_targets(args.targets, args.expected_services)
    artifact = asyncio.run(run(args, targets))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) +
                           '\n',
                           encoding='utf-8')
    required_gates = {
        key: value
        for key, value in artifact['gates'].items()
        if key != 'details'
    }
    failed = [key for key, value in required_gates.items() if not value]
    if failed:
        raise SystemExit(f'HA cluster gates failed: {", ".join(failed)}')


if __name__ == '__main__':
    main()
