"""Lifecycle/finalizer contracts for the billable SkyServe qualifier."""
# pylint: disable=protected-access

import argparse
import asyncio
import hashlib
import importlib
import json
import os
import pathlib
import shlex
import sys
import textwrap
import time

import pytest
from smoke_tests import smoke_tests_utils

_FIXTURE_DIR = pathlib.Path(__file__).parents[1] / 'skyserve' / 'paid_capacity'
sys.path.insert(0, str(_FIXTURE_DIR))
try:
    lifecycle_module = importlib.import_module('lifecycle')
finally:
    sys.path.pop(0)


class _FakeLifecycle:
    """Controllable implementation of the lifecycle boundary."""

    def __init__(self, events, *, up_error=None, down_error=None):
        self._events = events
        self._up_error = up_error
        self._down_error = down_error

    async def ensure_absent(self, service_name):
        self._events.append(('absent', service_name))

    async def up(self, service_name, service_yaml):
        self._events.append(('up', service_name, service_yaml.name))
        if self._up_error is not None:
            raise self._up_error

    async def endpoint(self, service_name):
        self._events.append(('endpoint', service_name))
        return 'https://service.example.test'

    async def down(self, service_name):
        self._events.append(('down', service_name))
        if self._down_error is not None:
            raise self._down_error


def _args(tmp_path):
    return argparse.Namespace(
        profile='provider-canary',
        provider=None,
        service_name='paid-e2e-unit',
        artifacts_dir=str(tmp_path),
        source='source.yaml',
        economic_receipt=None,
        workspace='paid-workspace',
        sky_cli='sky',
        command_timeout_seconds=60,
        endpoint_timeout_seconds=60,
        scope_timeout_seconds=60,
        down_timeout_seconds=60,
        cleanup_timeout_seconds=600,
        cleanup_zero_hold_seconds=360,
        poll_seconds=1,
        endpoint_mode=(lifecycle_module.EndpointMode.PUBLISHED),
        auth_token_env='TOKEN',
        postgres_url_env='DATABASE')


def _provider_scope(**overrides):
    values = {
        'service_hash': 'provider-service-hash',
        'resource_scope': 'authoritative-resource-scope',
        'lifecycle_epoch': 7,
        'service_version': 11,
        'max_live_paid_gpu_units': 1,
        'providers': ('gcp',),
        'project_id': 'durable-project',
        'workspace': 'workspace-a',
        'location_scope':
            lifecycle_module.qualify.GcpLocationScope.PROJECT_WIDE,
        'aws_location_scope': None,
        'aws_regions': (),
        'catalog_shapes': (lifecycle_module.qualify.CatalogShape(
            cloud='gcp',
            region='us-central1',
            zone='us-central1-a',
            instance_type='g2-standard-4',
            gpu_units_per_instance=1),),
        'placement_catalog_sha256': 'c' * 64,
        'service_yaml_sha256': 'd' * 64,
        'qualification_profile': 'provider-canary',
        'qualification_source_sha256': 'e' * 64,
        'qualification_projection_sha256':
            lifecycle_module.qualify._qualification_projection_sha256(
                source_sha256='e' * 64,
                profile=lifecycle_module.qualify.PROFILES['provider-canary'],
                providers=('gcp',)),
        'controller_config_digest': 'a' * 64,
        'controller_config_snapshot_id': 'b' * 64,
    }
    values.update(overrides)
    return lifecycle_module.qualify.ProviderScope(**values)


def _exact_zero_cleanup_payload(*, service_name, scope, qualification_receipt):

    def _sample(index, observed_at, elapsed):
        return {
            'observed_at': observed_at,
            'exact_zero': True,
            'zero_samples': index,
            'zero_hold_elapsed_seconds': elapsed,
            'cleanup_claims': 0,
            'cleanup_cluster_records': 0,
            'cleanup_debit_units': 0,
            'cleanup_effect_capable_associations': 0,
            'cleanup_blocking_requests': 0,
            'cleanup_queue_deliveries': 0,
            'cleanup_retention_pins': 0,
            'cleanup_provider_disks': 0,
            'cleanup_provider_instances': 0,
            'cleanup_provider_operations': 0,
            'cleanup_replicas': 0,
            'cleanup_service_rows': 0,
            'cleanup_waiters': 0,
            'cleanup_provider_by_cloud': {
                cloud: {
                    'cloud': cloud,
                    'instance_count': 0,
                    'running_count': 0,
                    'gpu_units': 0,
                    'running_gpu_units': 0,
                    'disk_count': 0,
                    'inflight_operation_count': 0,
                    'shapes': [],
                } for cloud in ('aws', 'gcp')
            },
        }

    hold = lifecycle_module.qualify.PROFILES[
        scope.qualification_profile].zero_hold_seconds
    return {
        'schema_version':
            lifecycle_module.qualify._CLEANUP_RECEIPT_SCHEMA_VERSION,
        'service_name': service_name,
        'service_hash': scope.service_hash,
        'lifecycle_epoch': scope.lifecycle_epoch,
        'service_version': scope.service_version,
        'controller_config_digest': scope.controller_config_digest,
        'controller_config_snapshot_id': scope.controller_config_snapshot_id,
        'expected_providers': list(scope.providers),
        'service_yaml_sha256': scope.service_yaml_sha256,
        'qualification_profile': scope.qualification_profile,
        'qualification_source_sha256': scope.qualification_source_sha256,
        'qualification_projection_sha256':
            scope.qualification_projection_sha256,
        'qualification_receipt_sha256': hashlib.sha256(
            qualification_receipt.read_bytes()).hexdigest(),
        'started_at': 1.0,
        'finished_at': hold + 2.0,
        'outcome': 'passed',
        'zero_samples': 3,
        'zero_hold_required_seconds': hold,
        'zero_hold_elapsed_seconds': hold,
        'samples': [
            _sample(1, 1.0, 0.0),
            _sample(2, 1.0 + hold / 2, hold / 2),
            _sample(3, 1.0 + hold, hold),
        ],
    }


def _install_operations(monkeypatch,
                        events,
                        *,
                        freeze_error=None,
                        cleanup_scope_error=None,
                        qualification_error=None,
                        cleanup_error=None,
                        cleanup_receipt_mutator=None):

    def render(args):
        events.append(('render', args.profile))
        pathlib.Path(args.output).write_text('rendered\n', encoding='utf-8')

    async def freeze(args):
        events.append(('freeze', args.service_name))
        if freeze_error is not None:
            raise freeze_error
        lifecycle_module.qualify.write_provider_scope(pathlib.Path(args.output),
                                                      args.service_name,
                                                      _provider_scope())

    async def qualify(args):
        events.append(('qualify', args.endpoint))
        pathlib.Path(args.receipt).write_text('{}\n', encoding='utf-8')
        if qualification_error is not None:
            raise qualification_error

    async def freeze_cleanup_scope(args):
        events.append(('freeze-cleanup-scope', args.service_name))
        if cleanup_scope_error is not None:
            raise cleanup_scope_error
        receipt_path = pathlib.Path(args.receipt)
        if not receipt_path.exists():
            receipt_path.write_text('{}\n', encoding='utf-8')

    async def cleanup(args):
        events.append(('cleanup', args.service_name))
        if not pathlib.Path(args.scope).exists():
            raise FileNotFoundError(args.scope)
        scope = lifecycle_module.qualify.read_provider_scope(
            pathlib.Path(args.scope), args.service_name)
        payload = _exact_zero_cleanup_payload(
            service_name=args.service_name,
            scope=scope,
            qualification_receipt=pathlib.Path(args.receipt))
        if cleanup_receipt_mutator is not None:
            cleanup_receipt_mutator(payload)
        pathlib.Path(args.output).write_text(json.dumps(payload) + '\n',
                                             encoding='utf-8')
        if cleanup_error is not None:
            raise cleanup_error

    monkeypatch.setattr(lifecycle_module.qualify, 'render_service', render)
    monkeypatch.setattr(lifecycle_module.qualify, 'freeze_provider_scope',
                        freeze)
    monkeypatch.setattr(lifecycle_module.qualify, 'qualify', qualify)
    monkeypatch.setattr(lifecycle_module.qualify, 'freeze_cleanup_scope',
                        freeze_cleanup_scope)
    monkeypatch.setattr(lifecycle_module.qualify, 'wait_for_cleanup', cleanup)


def _receipt(tmp_path):
    return json.loads(
        (tmp_path / 'paid-e2e-unit-lifecycle.json').read_text(encoding='utf-8'))


def test_paid_smoke_has_no_lifecycle_bypass():
    smoke_source = (pathlib.Path(__file__).parents[1] / 'smoke_tests' /
                    'test_sky_serve.py').read_text(encoding='utf-8')
    function_start = smoke_source.index(
        'def test_skyserve_paid_spot_postgres_e2e(')
    start = smoke_source.rfind('\n\n', 0, function_start) + 2
    end = smoke_source.index('\n\n@pytest.mark', start)
    paid_test = smoke_source[start:end]

    assert '@pytest.mark.gcp' not in paid_test
    assert '@pytest.mark.aws' not in paid_test
    assert 'tests/skyserve/paid_capacity/lifecycle.py' in paid_test
    assert 'tests/skyserve/paid_capacity/qualify.py' not in paid_test
    assert 'sky serve up' not in paid_test
    assert '_TEARDOWN_SERVICE' not in paid_test
    assert '--serve-paid-provider-e2e-workspace' in paid_test
    assert '--workspace' in paid_test


def test_sky_cli_lifecycle_pins_workspace_at_command_boundary(
        monkeypatch, tmp_path):
    commands = []

    class _Process:

        returncode = 0

        async def communicate(self):
            return None, None

    async def create_subprocess_exec(*command, **_kwargs):
        commands.append(command)
        return _Process()

    monkeypatch.setattr(lifecycle_module.asyncio, 'create_subprocess_exec',
                        create_subprocess_exec)
    lifecycle = lifecycle_module.SkyCliLifecycle(executable='sky',
                                                 command_timeout_seconds=60,
                                                 endpoint_timeout_seconds=60,
                                                 down_timeout_seconds=60,
                                                 poll_seconds=1,
                                                 workspace='mt_hybrid')

    asyncio.run(lifecycle.up('paid-e2e', tmp_path / 'service.yaml'))

    assert commands == [('sky', 'serve', 'up', '-n', 'paid-e2e', '-y',
                         str(tmp_path / 'service.yaml'), '--config',
                         'active_workspace=mt_hybrid')]


def test_sky_cli_down_returns_after_durable_teardown_admission():
    calls = []
    lifecycle = lifecycle_module.SkyCliLifecycle(executable='sky',
                                                 command_timeout_seconds=900,
                                                 endpoint_timeout_seconds=900,
                                                 down_timeout_seconds=300,
                                                 poll_seconds=10,
                                                 workspace=None)

    async def run(*arguments, capture, phase_deadline):
        calls.append((arguments, capture, phase_deadline.remaining()))
        return lifecycle_module.CommandResult(returncode=0, stdout='')

    lifecycle._run = run

    asyncio.run(lifecycle.down('paid-e2e'))

    assert len(calls) == 1
    assert calls[0][0] == ('serve', 'down', '-y', 'paid-e2e')
    assert calls[0][1] is False
    assert 299 < calls[0][2] <= 300


def test_sky_cli_command_communicate_uses_remaining_phase_deadline(monkeypatch):
    monkeypatch.setattr(lifecycle_module, '_LIFECYCLE_REAP_GRACE_SECONDS', 0.05)
    lifecycle = lifecycle_module.SkyCliLifecycle(executable=sys.executable,
                                                 command_timeout_seconds=60,
                                                 endpoint_timeout_seconds=60,
                                                 down_timeout_seconds=60,
                                                 poll_seconds=1,
                                                 workspace=None)

    async def exercise():
        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await lifecycle._run(
                '-c',
                'import time; time.sleep(60)',
                capture=True,
                phase_deadline=(
                    lifecycle_module.qualify.AbsoluteDeadline.after(0.05)))
        assert time.monotonic() - started < 1

    asyncio.run(exercise())


def test_sky_cli_command_spawn_uses_remaining_phase_deadline(monkeypatch):
    spawn_cancelled = False

    async def create_subprocess_exec(*_command, **_kwargs):
        nonlocal spawn_cancelled
        try:
            await asyncio.Future()
        finally:
            spawn_cancelled = True

    monkeypatch.setattr(lifecycle_module.asyncio, 'create_subprocess_exec',
                        create_subprocess_exec)
    lifecycle = lifecycle_module.SkyCliLifecycle(executable='sky',
                                                 command_timeout_seconds=60,
                                                 endpoint_timeout_seconds=60,
                                                 down_timeout_seconds=60,
                                                 poll_seconds=1,
                                                 workspace=None)

    async def exercise():
        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await lifecycle._run(
                'serve',
                'status',
                capture=True,
                phase_deadline=(
                    lifecycle_module.qualify.AbsoluteDeadline.after(0.02)))
        assert time.monotonic() - started < 1

    asyncio.run(exercise())
    assert spawn_cancelled


def test_sky_cli_success_reaps_stubborn_process_group_descendant(
        monkeypatch, tmp_path):
    monkeypatch.setattr(lifecycle_module, '_LIFECYCLE_REAP_GRACE_SECONDS', 0.05)
    pid_path = tmp_path / 'descendant.pid'
    child_source = textwrap.dedent(f'''\
        import pathlib
        import signal
        import subprocess
        import sys
        descendant = subprocess.Popen([
            sys.executable, '-c',
            'import signal, time; signal.signal(signal.SIGTERM, '
            'signal.SIG_IGN); time.sleep(60)',
        ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
           stderr=subprocess.DEVNULL, close_fds=True)
        pathlib.Path({str(pid_path)!r}).write_text(str(descendant.pid),
                                                   encoding='utf-8')
        print('complete')
    ''')
    lifecycle = lifecycle_module.SkyCliLifecycle(executable=sys.executable,
                                                 command_timeout_seconds=2,
                                                 endpoint_timeout_seconds=2,
                                                 down_timeout_seconds=2,
                                                 poll_seconds=0.1,
                                                 workspace=None)

    result = asyncio.run(lifecycle._run('-c', child_source, capture=True))

    pid = int(pid_path.read_text(encoding='utf-8'))
    assert result.stdout == 'complete'
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_in_cluster_endpoint_uses_frozen_resource_scope(tmp_path):
    service_name = 'paid-e2e-unit'
    provider_scope = tmp_path / 'scope.json'
    resource_scope = 'authoritative-resource-scope'
    lifecycle_module.qualify.write_provider_scope(
        provider_scope, service_name,
        _provider_scope(resource_scope=resource_scope))
    resolver = lifecycle_module.InClusterEndpointResolver(namespace='skypilot')

    endpoint = asyncio.run(
        resolver.resolve(
            lifecycle_module.EndpointResolutionRequest(
                service_name=service_name, provider_scope=provider_scope)))

    scoped_name = lifecycle_module.lb_k8s.lb_service_name(
        service_name, resource_scope)
    legacy_name = lifecycle_module.lb_k8s.lb_service_name(service_name)
    assert endpoint == (
        f'http://{scoped_name}.skypilot:'
        f'{lifecycle_module.serve_constants.LOAD_BALANCER_PORT_START}')
    assert scoped_name != legacy_name
    assert endpoint != (
        f'http://{legacy_name}.skypilot:'
        f'{lifecycle_module.serve_constants.LOAD_BALANCER_PORT_START}')


@pytest.mark.parametrize('resource_scope', ['', False])
def test_in_cluster_endpoint_rejects_malformed_resource_scope(
        tmp_path, resource_scope):
    service_name = 'paid-e2e-unit'
    provider_scope = tmp_path / 'scope.json'
    lifecycle_module.qualify.write_provider_scope(
        provider_scope, service_name,
        _provider_scope(resource_scope=resource_scope))
    resolver = lifecycle_module.InClusterEndpointResolver(namespace='skypilot')

    with pytest.raises(lifecycle_module.qualify.QualificationError,
                       match='Provider-scope receipt is malformed'):
        asyncio.run(
            resolver.resolve(
                lifecycle_module.EndpointResolutionRequest(
                    service_name=service_name, provider_scope=provider_scope)))


def test_in_cluster_endpoint_rejects_missing_resource_scope(tmp_path):
    service_name = 'paid-e2e-unit'
    provider_scope = tmp_path / 'scope.json'
    lifecycle_module.qualify.write_provider_scope(provider_scope, service_name,
                                                  _provider_scope())
    payload = json.loads(provider_scope.read_text(encoding='utf-8'))
    assert payload['schema_version'] == 6
    del payload['resource_scope']
    provider_scope.write_text(json.dumps(payload), encoding='utf-8')
    resolver = lifecycle_module.InClusterEndpointResolver(namespace='skypilot')

    with pytest.raises(lifecycle_module.qualify.QualificationError,
                       match='Provider-scope receipt is malformed'):
        asyncio.run(
            resolver.resolve(
                lifecycle_module.EndpointResolutionRequest(
                    service_name=service_name, provider_scope=provider_scope)))


def test_in_cluster_mode_bypasses_unreachable_published_endpoint(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)
    monkeypatch.setenv('SKYPILOT_POD_NAMESPACE', 'skypilot')
    args = _args(tmp_path)
    args.endpoint_mode = lifecycle_module.EndpointMode.IN_CLUSTER

    asyncio.run(lifecycle_module.run_lifecycle(args, _FakeLifecycle(events)))

    expected_name = lifecycle_module.lb_k8s.lb_service_name(
        args.service_name, 'authoritative-resource-scope')
    assert ('endpoint', args.service_name) not in events
    assert ('qualify',
            f'http://{expected_name}.skypilot:'
            f'{lifecycle_module.serve_constants.LOAD_BALANCER_PORT_START}') \
        in events


def test_parser_defaults_to_published_endpoint():
    args = lifecycle_module._parser().parse_args([
        '--profile', 'small', '--service-name', 'paid-e2e-unit',
        '--artifacts-dir', '/tmp/paid-e2e-unit'
    ])

    assert args.endpoint_mode is lifecycle_module.EndpointMode.PUBLISHED
    assert args.cleanup_zero_hold_seconds >= 6 * 60


def test_lifecycle_success_owns_normal_down_and_exact_cleanup(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    asyncio.run(
        lifecycle_module.run_lifecycle(_args(tmp_path), _FakeLifecycle(events)))

    assert [event[0] for event in events] == [
        'render', 'absent', 'up', 'freeze', 'endpoint', 'qualify',
        'freeze-cleanup-scope', 'down', 'cleanup'
    ]
    receipt = _receipt(tmp_path)
    assert receipt['outcome'] == 'passed'
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['serve_up_acknowledged'] is True
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['operator_escalation_required'] is False
    assert receipt['emergency_provider_cleanup'] == 'not_performed'


@pytest.mark.parametrize('corruption', [
    'malformed',
    'wrong-identity',
    'failed-outcome',
    'partial-hold',
])
def test_lifecycle_rejects_unproven_cleanup_receipt(monkeypatch, tmp_path,
                                                    corruption):
    events = []

    def corrupt(payload):
        if corruption == 'malformed':
            payload.pop('schema_version')
        elif corruption == 'wrong-identity':
            payload['service_hash'] = 'wrong-service-hash'
        elif corruption == 'failed-outcome':
            payload['outcome'] = 'failed'
        else:
            payload['zero_hold_elapsed_seconds'] = 359
            payload['samples'][-1]['zero_hold_elapsed_seconds'] = 359
            payload['samples'][-1]['observed_at'] = 360

    _install_operations(monkeypatch, events, cleanup_receipt_mutator=corrupt)

    with pytest.raises(lifecycle_module.LifecycleError,
                       match='lacks exact-zero cleanup evidence'):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    receipt = _receipt(tmp_path)
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['wait-cleanup']['outcome'] == 'passed'
    assert stages['validate-cleanup']['outcome'] == 'failed'
    assert receipt['cleanup_evidence_error_type'] == 'QualificationError'
    assert receipt['cleanup_receipt_sha256'] is None
    assert receipt['exact_cleanup_proven'] is False
    assert receipt['operator_escalation_required'] is True


def test_lifecycle_rejects_cleanup_hold_below_profile_before_mutation(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)
    args = _args(tmp_path)
    args.cleanup_zero_hold_seconds = 1

    with pytest.raises(lifecycle_module.LifecycleError,
                       match='must equal the qualification profile'):
        asyncio.run(lifecycle_module.run_lifecycle(args,
                                                   _FakeLifecycle(events)))

    assert not events


def test_pre_down_cleanup_scope_failure_cannot_publish_exact_cleanup(
        monkeypatch, tmp_path):
    events = []
    _install_operations(
        monkeypatch,
        events,
        cleanup_scope_error=RuntimeError('first database observation failed'))

    with pytest.raises(lifecycle_module.LifecycleError,
                       match='cleanup identity scope was not frozen'):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    names = [event[0] for event in events]
    assert names.index('freeze-cleanup-scope') < names.index('down')
    assert names[-2:] == ['down', 'cleanup']
    receipt = _receipt(tmp_path)
    assert receipt['cleanup_scope_freeze_error_type'] == 'RuntimeError'
    assert receipt['exact_cleanup_proven'] is False
    assert receipt['operator_escalation_required'] is True


@pytest.mark.parametrize(('error', 'expected_outcome'), [
    (RuntimeError('failed'), 'failed'),
    (KeyboardInterrupt(), 'interrupted'),
    (asyncio.CancelledError(), 'interrupted'),
])
def test_lifecycle_failure_or_interrupt_still_finalizes(monkeypatch, tmp_path,
                                                        error,
                                                        expected_outcome):
    events = []
    _install_operations(monkeypatch, events, qualification_error=error)

    with pytest.raises(type(error)):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    assert [event[0] for event in events][-2:] == ['down', 'cleanup']
    receipt = _receipt(tmp_path)
    assert receipt['outcome'] == expected_outcome
    assert receipt['cleanup_evidence_error_type'] is None
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['emergency_provider_cleanup'] == 'not_performed'
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'


def test_lifecycle_cleanup_starts_after_qualifier_terminal_drain(
        monkeypatch, tmp_path):
    events = []
    terminal_published = False
    _install_operations(monkeypatch, events)

    async def qualify_after_terminalizing(_args):
        nonlocal terminal_published
        events.append(('accepted',))
        await asyncio.sleep(0)
        terminal_published = True
        events.append(('terminal',))
        raise lifecycle_module.qualify.QualificationError('observer failed')

    class TerminalAwareLifecycle(_FakeLifecycle):

        async def down(self, service_name):
            assert terminal_published
            await super().down(service_name)

    monkeypatch.setattr(lifecycle_module.qualify, 'qualify',
                        qualify_after_terminalizing)

    with pytest.raises(lifecycle_module.qualify.QualificationError,
                       match='observer failed'):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           TerminalAwareLifecycle(events)))

    names = [event[0] for event in events]
    assert names.index('accepted') < names.index('terminal') < names.index(
        'down')
    assert names.count('down') == names.count('cleanup') == 1


def test_repeated_cancellation_cannot_interrupt_owned_finalizer(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    async def scenario():
        qualify_started = asyncio.Event()
        down_started = asyncio.Event()
        release_down = asyncio.Event()

        async def qualify(args):
            events.append(('qualify', args.endpoint))
            pathlib.Path(args.receipt).write_text('{}\n', encoding='utf-8')
            qualify_started.set()
            await asyncio.Future()

        class BlockingDownLifecycle(_FakeLifecycle):

            async def down(self, service_name):
                events.append(('down', service_name))
                down_started.set()
                await release_down.wait()

        monkeypatch.setattr(lifecycle_module.qualify, 'qualify', qualify)
        task = asyncio.create_task(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           BlockingDownLifecycle(events)))
        await qualify_started.wait()
        task.cancel()
        await down_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release_down.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    names = [event[0] for event in events]
    assert names[-2:] == ['down', 'cleanup']
    assert names.count('down') == names.count('cleanup') == 1
    stages = {stage['name']: stage for stage in _receipt(tmp_path)['stages']}
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'
    assert _receipt(tmp_path)['exact_cleanup_proven'] is True


def test_first_cancellation_during_normal_finalizer_is_deferred(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    async def scenario():
        down_started = asyncio.Event()
        release_down = asyncio.Event()

        class BlockingDownLifecycle(_FakeLifecycle):

            async def down(self, service_name):
                events.append(('down', service_name))
                down_started.set()
                await release_down.wait()

        task = asyncio.create_task(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           BlockingDownLifecycle(events)))
        await down_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release_down.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert [event[0] for event in events][-2:] == ['down', 'cleanup']
    stages = {stage['name']: stage for stage in _receipt(tmp_path)['stages']}
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'
    assert _receipt(tmp_path)['exact_cleanup_proven'] is True


def test_lost_up_acknowledgement_still_finalizes(monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    with pytest.raises(RuntimeError, match='lost acknowledgement'):
        asyncio.run(
            lifecycle_module.run_lifecycle(
                _args(tmp_path),
                _FakeLifecycle(events,
                               up_error=RuntimeError('lost acknowledgement'))))

    assert [event[0] for event in events] == [
        'render', 'absent', 'up', 'freeze', 'freeze-cleanup-scope', 'down',
        'cleanup'
    ]
    receipt = _receipt(tmp_path)
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['serve-up']['outcome'] == 'failed'
    assert stages['freeze-scope-recovery']['outcome'] == 'passed'
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['serve_up_acknowledged'] is False
    assert receipt['exact_cleanup_proven'] is False
    assert receipt['operator_escalation_required'] is True


def test_lost_up_ack_without_recoverable_scope_never_fabricates_one(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch,
                        events,
                        freeze_error=RuntimeError('service absent'))

    with pytest.raises(lifecycle_module.LifecycleFailureGroup) as group:
        asyncio.run(
            lifecycle_module.run_lifecycle(
                _args(tmp_path),
                _FakeLifecycle(events,
                               up_error=RuntimeError('lost acknowledgement'))))

    primary, *finalizer_errors = group.value.exceptions
    assert isinstance(primary, RuntimeError)
    assert str(primary) == 'lost acknowledgement'
    assert finalizer_errors
    assert any(
        isinstance(error, lifecycle_module.LifecycleError) and
        'operator escalation is required' in str(error)
        for error in finalizer_errors)

    assert [event[0] for event in events
           ] == ['render', 'absent', 'up', 'freeze', 'down', 'cleanup']
    assert not (tmp_path / 'paid-e2e-unit-scope.json').exists()
    receipt = _receipt(tmp_path)
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['freeze-scope-recovery']['outcome'] == 'failed'
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'failed'
    assert receipt['cleanup_receipt_sha256'] is None
    assert receipt['scope_recovery_error_type'] == 'RuntimeError'
    assert receipt['cleanup_evidence_error_type'] == 'FileNotFoundError'
    assert receipt['exact_cleanup_proven'] is False
    assert receipt['operator_escalation_required'] is True


def test_cleanup_failure_requires_only_explicit_operator_escalation(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch,
                        events,
                        cleanup_error=RuntimeError('cleanup incomplete'))

    with pytest.raises(lifecycle_module.LifecycleError,
                       match='lacks exact-zero cleanup evidence'):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    receipt = _receipt(tmp_path)
    assert receipt['outcome'] == 'failed'
    assert receipt['cleanup_evidence_error_type'] == 'RuntimeError'
    assert receipt['exact_cleanup_proven'] is False
    assert receipt['operator_escalation_required'] is True
    assert receipt['emergency_provider_cleanup'] == 'not_performed'
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'failed'


def test_down_failure_does_not_skip_exact_cleanup(monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    with pytest.raises(lifecycle_module.LifecycleError,
                       match='exact-zero cleanup was proven'):
        asyncio.run(
            lifecycle_module.run_lifecycle(
                _args(tmp_path),
                _FakeLifecycle(events,
                               down_error=RuntimeError('down unavailable'))))

    assert [event[0] for event in events][-2:] == ['down', 'cleanup']
    stages = {stage['name']: stage for stage in _receipt(tmp_path)['stages']}
    assert stages['serve-down']['outcome'] == 'failed'
    assert stages['wait-cleanup']['outcome'] == 'passed'
    receipt = _receipt(tmp_path)
    assert receipt['serve_down_error_type'] == 'RuntimeError'
    assert receipt['cleanup_evidence_error_type'] is None
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['operator_escalation_required'] is False


def test_primary_and_cleanup_failures_are_both_raised(monkeypatch, tmp_path):
    events = []
    primary_error = RuntimeError('qualification failed')
    cleanup_error = RuntimeError('cleanup incomplete')
    _install_operations(monkeypatch,
                        events,
                        qualification_error=primary_error,
                        cleanup_error=cleanup_error)

    with pytest.raises(lifecycle_module.LifecycleFailureGroup) as group:
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    assert group.value.exceptions[0] is primary_error
    cleanup_failures = [
        error for error in group.value.exceptions
        if isinstance(error, lifecycle_module.LifecycleError) and
        'operator escalation is required' in str(error)
    ]
    assert len(cleanup_failures) == 1
    assert cleanup_failures[0].__cause__ is cleanup_error
    assert _receipt(tmp_path)['operator_escalation_required'] is True


def test_smoke_timeout_joins_real_sigterm_finalizer_and_receipt(
        monkeypatch, tmp_path):
    receipt = tmp_path / 'sigterm-lifecycle.json'
    child_source = textwrap.dedent(f'''\
        import json
        import pathlib
        import signal
        import time

        receipt = pathlib.Path({str(receipt)!r})

        def finalize(_signal, _frame):
            time.sleep(0.15)
            receipt.write_text(json.dumps({{
                'schema_version': 1,
                'finished_at': time.time(),
                'outcome': 'failed',
                'exact_cleanup_proven': True,
                'operator_escalation_required': False,
            }}), encoding='utf-8')
            raise SystemExit(7)

        signal.signal(signal.SIGTERM, finalize)
        while True:
            time.sleep(0.01)
    ''')
    command = f'exec {shlex.quote(sys.executable)} -c {shlex.quote(child_source)}'
    test = smoke_tests_utils.Test(name='paid-lifecycle-sigterm-unit',
                                  commands=[command],
                                  timeout=1,
                                  timeout_termination_grace_seconds=2,
                                  timeout_completion_receipt=str(receipt))
    monkeypatch.setenv('LOG_TO_STDOUT', '1')
    monkeypatch.setattr(smoke_tests_utils, 'is_remote_server_test',
                        lambda: True)
    real_exists = smoke_tests_utils.os.path.exists
    monkeypatch.setattr(
        smoke_tests_utils.os.path, 'exists', lambda path: False if str(path).
        endswith('fetch_failed_job_logs.sh') else real_exists(path))

    started_at = time.monotonic()
    with pytest.raises(Exception, match='test failed'):
        smoke_tests_utils.run_one_test(test, check_sky_status=False)

    assert time.monotonic() - started_at >= 1.1
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    assert payload['exact_cleanup_proven'] is True
    assert payload['operator_escalation_required'] is False
