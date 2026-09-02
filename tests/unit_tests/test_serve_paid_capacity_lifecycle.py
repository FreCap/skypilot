"""Lifecycle/finalizer contracts for the billable SkyServe qualifier."""

import argparse
import asyncio
import importlib
import json
import pathlib
import sys

import pytest

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
    return argparse.Namespace(profile='small',
                              provider=None,
                              service_name='paid-e2e-unit',
                              artifacts_dir=str(tmp_path),
                              source='source.yaml',
                              economic_receipt=None,
                              sky_cli='sky',
                              command_timeout_seconds=60,
                              endpoint_timeout_seconds=60,
                              scope_timeout_seconds=60,
                              down_timeout_seconds=60,
                              cleanup_timeout_seconds=60,
                              poll_seconds=1,
                              auth_token_env='TOKEN',
                              postgres_url_env='DATABASE')


def _install_operations(monkeypatch,
                        events,
                        *,
                        freeze_error=None,
                        qualification_error=None,
                        cleanup_error=None):

    def render(args):
        events.append(('render', args.profile))
        pathlib.Path(args.output).write_text('rendered\n', encoding='utf-8')

    def freeze(args):
        events.append(('freeze', args.service_name))
        if freeze_error is not None:
            raise freeze_error
        pathlib.Path(args.output).write_text('{}\n', encoding='utf-8')

    async def qualify(args):
        events.append(('qualify', args.endpoint))
        pathlib.Path(args.receipt).write_text('{}\n', encoding='utf-8')
        if qualification_error is not None:
            raise qualification_error

    async def cleanup(args):
        events.append(('cleanup', args.service_name))
        if not pathlib.Path(args.scope).exists():
            raise FileNotFoundError(args.scope)
        pathlib.Path(args.output).write_text('{"outcome":"passed"}\n',
                                             encoding='utf-8')
        if cleanup_error is not None:
            raise cleanup_error

    monkeypatch.setattr(lifecycle_module.qualify, 'render_service', render)
    monkeypatch.setattr(lifecycle_module.qualify, 'freeze_provider_scope',
                        freeze)
    monkeypatch.setattr(lifecycle_module.qualify, 'qualify', qualify)
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


def test_lifecycle_success_owns_normal_down_and_exact_cleanup(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    asyncio.run(
        lifecycle_module.run_lifecycle(_args(tmp_path), _FakeLifecycle(events)))

    assert [event[0] for event in events] == [
        'render', 'absent', 'up', 'freeze', 'endpoint', 'qualify', 'down',
        'cleanup'
    ]
    receipt = _receipt(tmp_path)
    assert receipt['outcome'] == 'passed'
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['operator_escalation_required'] is False
    assert receipt['emergency_provider_cleanup'] == 'not_performed'


@pytest.mark.parametrize('error', [
    RuntimeError('failed'),
    KeyboardInterrupt(),
    asyncio.CancelledError(),
])
def test_lifecycle_failure_or_interrupt_still_finalizes(monkeypatch, tmp_path,
                                                        error):
    events = []
    _install_operations(monkeypatch, events, qualification_error=error)

    with pytest.raises(type(error)):
        asyncio.run(
            lifecycle_module.run_lifecycle(_args(tmp_path),
                                           _FakeLifecycle(events)))

    assert [event[0] for event in events][-2:] == ['down', 'cleanup']
    receipt = _receipt(tmp_path)
    assert receipt['outcome'] == 'failed'
    assert receipt['cleanup_evidence_error_type'] is None
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['exact_cleanup_proven'] is True
    assert receipt['emergency_provider_cleanup'] == 'not_performed'
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'


def test_lost_up_acknowledgement_still_finalizes(monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch, events)

    with pytest.raises(RuntimeError, match='lost acknowledgement'):
        asyncio.run(
            lifecycle_module.run_lifecycle(
                _args(tmp_path),
                _FakeLifecycle(events,
                               up_error=RuntimeError('lost acknowledgement'))))

    assert [event[0] for event in events
           ] == ['render', 'absent', 'up', 'freeze', 'down', 'cleanup']
    receipt = _receipt(tmp_path)
    stages = {stage['name']: stage for stage in receipt['stages']}
    assert stages['serve-up']['outcome'] == 'failed'
    assert stages['freeze-scope-recovery']['outcome'] == 'passed'
    assert stages['serve-down']['outcome'] == 'passed'
    assert stages['wait-cleanup']['outcome'] == 'passed'
    assert receipt['cleanup_receipt_sha256'] is not None
    assert receipt['operator_escalation_required'] is False


def test_lost_up_ack_without_recoverable_scope_never_fabricates_one(
        monkeypatch, tmp_path):
    events = []
    _install_operations(monkeypatch,
                        events,
                        freeze_error=RuntimeError('service absent'))

    with pytest.raises(RuntimeError, match='lost acknowledgement'):
        asyncio.run(
            lifecycle_module.run_lifecycle(
                _args(tmp_path),
                _FakeLifecycle(events,
                               up_error=RuntimeError('lost acknowledgement'))))

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
