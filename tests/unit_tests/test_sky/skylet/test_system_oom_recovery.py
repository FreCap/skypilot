"""Focused tests for the typed recovery-only Ray/system-OOM runtime."""

import contextlib
import dataclasses
import json
import os
import signal
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.backends import task_codegen
from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.skylet import subprocess_supervisor
from sky.skylet import system_oom_recovery

_IMAGE = 'repo/image@sha256:' + 'a' * 64


@pytest.fixture()
def v1_plan():
    return system_oom_recovery.RecoveryLaunchPlan.direct_shell()


@pytest.fixture()
def v2_plan():
    spec = system_oom_recovery.OwnedContainerSpec(
        image=_IMAGE,
        create_options=('--gpus', 'all', '--shm-size=8g'),
        argv=('python', 'serve.py'),
        inherited_environment_names=('TOKEN',))
    return system_oom_recovery.RecoveryLaunchPlan.owned_container(spec)


@pytest.fixture()
def attempt_context(tmp_path, monkeypatch, v1_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    return system_oom_recovery.new_attempt_context(7, 0, 0, v1_plan)


def _docker_identity() -> system_oom_recovery.DockerIdentity:
    return system_oom_recovery.DockerIdentity(
        host=system_oom_recovery.DOCKER_HOST,
        daemon_id='daemon-id',
        daemon_pid=42,
        daemon_pid_create_time=12.5)


def _bound(context, parent_pid=77):
    return system_oom_recovery.bind_supervisor_parent(context, parent_pid)


def _publish_markers(context, *, forced=False, owned_container_id=None):
    if (context['profile_version']
            == system_oom_recovery.PROFILE_VERSION_OWNED_CONTAINER and
            owned_container_id is None):
        owned_container_id = 'a' * 64
    identity = _docker_identity()
    supervisor = {'pid': 123, 'pid_create_time': 456.0}
    capability = {
        'schema_version': context['schema_version'],
        'kind': 'capability',
        **system_oom_recovery._attempt_fields(context),
        'armed': True,
        'reason': None,
        'supervisor': supervisor,
        'docker_identity': identity.to_dict(),
        'owned_container_id': owned_container_id,
        'written_at': context['created_at'] + 0.01,
    }
    cleanup = {
        'schema_version': context['schema_version'],
        'kind': 'cleanup',
        **system_oom_recovery._attempt_fields(context),
        'supervisor': supervisor,
        'docker_identity': identity.to_dict(),
        'owned_container_id': owned_container_id,
        'started_at': context['created_at'] + 0.02,
        'completed_at': context['created_at'] + 0.03,
        'graceful': not forced,
        'forced': forced,
        'timed_out': forced,
        'descendants_empty': True,
        'docker_empty': True,
        'enumeration_proven': True,
        'survivor_pids': [],
    }
    system_oom_recovery.atomic_write_marker(context['capability_path'],
                                            capability)
    system_oom_recovery.atomic_write_marker(context['cleanup_path'], cleanup)
    return capability, cleanup


def test_owned_spec_canonical_round_trip(v2_plan):
    spec = v2_plan.owned_container_spec
    assert spec is not None
    assert system_oom_recovery.OwnedContainerSpec.parse(spec.render()) == spec
    assert spec.render().startswith('docker run --gpus all --shm-size=8g ')


@pytest.mark.parametrize('options', [('--rm',), ('--name', 'user-name'),
                                     ('--volume', '/var/run/docker.sock:/x'),
                                     ('--stop-signal', 'SIGKILL'),
                                     ('-e', 'TOKEN=secret'),
                                     ('--unknown', 'value')])
def test_owned_spec_rejects_unsafe_or_unknown_options(options):
    with pytest.raises(ValueError):
        system_oom_recovery.OwnedContainerSpec(image=_IMAGE,
                                               create_options=options)


def test_launch_plan_round_trip_and_capability(v1_plan, v2_plan):
    for plan in (v1_plan, v2_plan):
        assert system_oom_recovery.RecoveryLaunchPlan.from_dict(
            plan.to_dict(), allow_bound=False) == plan
        assert plan.capability == (
            system_oom_recovery.CAPABILITY_BY_PROFILE_VERSION[
                plan.profile_version])


def test_launch_plan_and_context_reject_boolean_versions(v1_plan):
    with pytest.raises(ValueError, match='profile_version'):
        system_oom_recovery.RecoveryLaunchPlan(profile_version=True)
    payload = v1_plan.to_dict()
    payload['profile_version'] = True
    with pytest.raises(ValueError, match='profile_version'):
        system_oom_recovery.RecoveryLaunchPlan.from_dict(payload)
    with pytest.raises(ValueError, match='execution envelope schema'):
        system_oom_recovery.RecoveryExecutionEnvelope(
            schema_version=True,
            working_directory='/tmp',
            unset_environment_names=('RAY_RAYLET_PID',),
            postlude_script='exit $?')


def test_context_rejects_empty_boot_identity(tmp_path, monkeypatch, v1_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: '')
    with pytest.raises(system_oom_recovery.RecoveryError, match='boot ID'):
        system_oom_recovery.new_attempt_context(7, 0, 0, v1_plan)


def test_replacement_context_requires_and_binds_original_identity(
        attempt_context, v1_plan):
    with pytest.raises(ValueError, match='Docker identity'):
        system_oom_recovery.new_attempt_context(7,
                                                0,
                                                1,
                                                v1_plan,
                                                expected_boot_id='boot-id')
    replacement = system_oom_recovery.new_attempt_context(
        7,
        0,
        1,
        v1_plan,
        expected_boot_id='boot-id',
        expected_docker_identity=_docker_identity())
    assert replacement['require_armed_start'] is True
    assert replacement['node_boot_id'] == attempt_context['node_boot_id']
    assert replacement['expected_docker_identity'] == _docker_identity(
    ).to_dict()


def test_private_plan_hides_environment_from_argv(attempt_context, v2_plan):
    secret = 'super-secret-value'
    bound = v2_plan.bind_environment({'TOKEN': secret})
    supervisor_context = _bound(attempt_context)
    command = system_oom_recovery.build_supervisor_command(
        None, supervisor_context | {
            'schema_version': 2,
            'profile_version': 2,
            'capability': system_oom_recovery.CAPABILITY_V2,
            'require_armed_start': True,
        }, bound)
    assert secret not in ' '.join(command)
    plan_path = os.path.join(attempt_context['marker_dir'], 'plan.json')
    assert os.stat(plan_path).st_mode & 0o077 == 0
    assert os.stat(command[-1]).st_mode & 0o077 == 0
    with open(command[-1], encoding='utf-8') as launch_file:
        launch_script = launch_file.read()
    assert launch_script.count('source ~/.bashrc') == 1
    assert secret in launch_script
    context = supervisor_context | {
        'schema_version': 2,
        'profile_version': 2,
        'capability': system_oom_recovery.CAPABILITY_V2,
        'require_armed_start': True,
    }
    consumed = system_oom_recovery.consume_private_recovery_plan(
        plan_path, context)
    assert consumed == bound
    assert not os.path.exists(plan_path)


def test_context_and_marker_reject_boolean_schema(attempt_context):
    malformed_context = dict(attempt_context, schema_version=True)
    with pytest.raises(system_oom_recovery.RecoveryError,
                       match='schema is unsupported'):
        system_oom_recovery._validate_attempt_context(malformed_context)

    capability, _ = _publish_markers(attempt_context)
    capability['schema_version'] = True
    system_oom_recovery.atomic_write_marker(attempt_context['capability_path'],
                                            capability)
    with pytest.raises(system_oom_recovery.RecoveryError,
                       match='header is invalid'):
        system_oom_recovery.read_capability_marker(attempt_context)


@pytest.mark.parametrize('created_at', [True, float('nan'), 10**400])
def test_context_rejects_invalid_creation_time(attempt_context, created_at):
    malformed_context = dict(attempt_context, created_at=created_at)

    with pytest.raises(system_oom_recovery.RecoveryError,
                       match='creation time'):
        system_oom_recovery._validate_attempt_context(malformed_context)


def test_cleanup_marker_requires_exact_graceful_attempt(attempt_context,
                                                        monkeypatch):
    _publish_markers(attempt_context)
    stable = mock.Mock(return_value=True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        stable)
    assert system_oom_recovery.validate_cleanup_marker(attempt_context,
                                                       float('inf')) == (True,
                                                                         '')
    stable.assert_called_once_with(_docker_identity(), float('inf'))


def test_cleanup_marker_rejects_forced(attempt_context, monkeypatch):
    _publish_markers(attempt_context, forced=True)
    stable = mock.Mock(return_value=True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        stable)
    valid, reason = system_oom_recovery.validate_cleanup_marker(
        attempt_context, float('inf'))
    assert not valid and 'forced' in reason
    stable.assert_not_called()


@pytest.mark.parametrize('field,value', [('written_at', True),
                                         ('written_at', float('nan')),
                                         ('written_at', 10**400),
                                         ('completed_at', True),
                                         ('completed_at', float('nan')),
                                         ('completed_at', 10**400)])
def test_cleanup_marker_rejects_nonfinite_or_boolean_timestamps(
        attempt_context, monkeypatch, field, value):
    capability, cleanup = _publish_markers(attempt_context)
    marker = capability if field == 'written_at' else cleanup
    marker[field] = value
    path = (attempt_context['capability_path']
            if field == 'written_at' else attempt_context['cleanup_path'])
    system_oom_recovery.atomic_write_marker(path, marker)
    stable = mock.Mock(return_value=True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        stable)

    valid, _ = system_oom_recovery.validate_cleanup_marker(
        attempt_context, float('inf'))

    assert not valid
    stable.assert_not_called()


def test_v2_cleanup_marker_requires_same_full_container_id(
        tmp_path, monkeypatch, v2_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    context = system_oom_recovery.new_attempt_context(7, 0, 0, v2_plan)
    _, cleanup = _publish_markers(context, owned_container_id='a' * 64)
    cleanup['owned_container_id'] = 'b' * 64
    system_oom_recovery.atomic_write_marker(context['cleanup_path'], cleanup)
    stable = mock.Mock(return_value=True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        stable)

    valid, reason = system_oom_recovery.validate_cleanup_marker(
        context, float('inf'))

    assert not valid
    assert 'owned container ID does not match' in reason
    stable.assert_not_called()


def test_pruning_removes_only_old_terminal_attempts(tmp_path, monkeypatch,
                                                    v1_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    terminal = system_oom_recovery.new_attempt_context(7, 0, 0, v1_plan)
    incomplete = system_oom_recovery.new_attempt_context(8, 0, 0, v1_plan)
    _publish_markers(terminal)
    cleanup = json.loads(
        open(terminal['cleanup_path'], encoding='utf-8').read())
    cleanup['completed_at'] = 1.0
    system_oom_recovery.atomic_write_marker(terminal['cleanup_path'], cleanup)
    system_oom_recovery.prune_attempt_directories(now=100000.0,
                                                  retention_seconds=10.0)
    assert not os.path.exists(terminal['marker_dir'])
    assert os.path.isdir(incomplete['marker_dir'])


def _patch_v1_supervisor(monkeypatch, *, docker_error=None):
    monkeypatch.setattr(subprocess_supervisor, '_set_parent_death_signal',
                        lambda _expected_parent_pid: 77)
    monkeypatch.setattr(subprocess_supervisor, '_assert_parent_fence',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess_supervisor, '_supervisor_identity', lambda: {
        'pid': 123,
        'pid_create_time': 456.0
    })
    monkeypatch.setattr(subprocess_supervisor.os, 'getsid',
                        lambda _pid: subprocess_supervisor.os.getpid())
    monkeypatch.setattr(subprocess_supervisor, 'enable_subreaper', lambda: None)
    if docker_error is None:
        monkeypatch.setattr(system_oom_recovery, 'get_docker_identity',
                            lambda: _docker_identity())
        monkeypatch.setattr(system_oom_recovery, 'docker_container_inventory',
                            lambda: ())
    else:
        monkeypatch.setattr(system_oom_recovery, 'get_docker_identity',
                            mock.Mock(side_effect=docker_error))
    monkeypatch.setattr(subprocess_supervisor, '_write_capability',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess_supervisor, '_write_cleanup',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess_supervisor, '_cleanup',
                        lambda *_args, **_kwargs: {'graceful': False})


def test_fatal_parent_fence_never_reaches_popen(attempt_context, v1_plan,
                                                monkeypatch):
    monkeypatch.setattr(
        subprocess_supervisor, '_set_parent_death_signal',
        mock.Mock(side_effect=OSError('cannot install PDEATHSIG')))
    popen = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)
    assert subprocess_supervisor.supervise('echo service',
                                           _bound(attempt_context),
                                           v1_plan) == 1
    popen.assert_not_called()


def test_parent_change_before_pdeathsig_never_calls_prctl(monkeypatch):
    prctl = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor, '_prctl', prctl)
    monkeypatch.setattr(subprocess_supervisor.os, 'getppid', lambda: 88)

    with pytest.raises(OSError, match='parent changed'):
        subprocess_supervisor._set_parent_death_signal(77)

    prctl.assert_not_called()


def test_v1_initial_nonfatal_capability_failure_runs_unarmed(
        attempt_context, v1_plan, monkeypatch):
    _patch_v1_supervisor(
        monkeypatch,
        docker_error=system_oom_recovery.RecoveryError('no local daemon'))
    command = mock.Mock(pid=888, returncode=0)
    command.poll.return_value = 0
    popen = mock.Mock(return_value=command)
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)
    assert subprocess_supervisor.supervise('echo service',
                                           _bound(attempt_context),
                                           v1_plan) == 0
    popen.assert_called_once_with('echo service',
                                  shell=True,
                                  start_new_session=True)


def test_v1_replacement_never_starts_unarmed(attempt_context, v1_plan,
                                             monkeypatch):
    replacement = dict(attempt_context)
    replacement['attempt_number'] = 1
    replacement['require_armed_start'] = True
    replacement['expected_docker_identity'] = _docker_identity().to_dict()
    _patch_v1_supervisor(
        monkeypatch,
        docker_error=system_oom_recovery.RecoveryError('no local daemon'))
    popen = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)
    assert subprocess_supervisor.supervise('echo service', _bound(replacement),
                                           v1_plan) == 1
    popen.assert_not_called()


def test_v1_latched_final_fence_never_reaches_popen(attempt_context, v1_plan,
                                                    monkeypatch):
    _patch_v1_supervisor(monkeypatch)
    fence = mock.Mock(side_effect=[
        None,
        system_oom_recovery.RecoveryError('termination latch set'),
    ])
    monkeypatch.setattr(subprocess_supervisor, '_assert_parent_fence', fence)
    popen = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)

    assert subprocess_supervisor.supervise('echo service',
                                           _bound(attempt_context),
                                           v1_plan) == 1

    assert fence.call_count == 2
    popen.assert_not_called()


def test_runtime_validator_rejects_wrong_exact_version(monkeypatch):
    system_oom_recovery.validate_runtime_capability.cache_clear()
    monkeypatch.setattr(job_lib, 'JOB_SYSTEM_RECOVERY_API_VERSION', 2)
    with pytest.raises(system_oom_recovery.RecoveryError, match='API version'):
        system_oom_recovery.validate_runtime_capability()
    system_oom_recovery.validate_runtime_capability.cache_clear()


def test_runtime_validator_rejects_schema_drift(monkeypatch):

    @dataclasses.dataclass(frozen=True)
    class IncompleteRecoveryInfo:
        capability: str

    system_oom_recovery.validate_runtime_capability.cache_clear()
    monkeypatch.setattr(job_lib, 'JobSystemRecoveryInfo',
                        IncompleteRecoveryInfo)
    with pytest.raises(system_oom_recovery.RecoveryError, match='schema'):
        system_oom_recovery.validate_runtime_capability()
    system_oom_recovery.validate_runtime_capability.cache_clear()


def test_runtime_validator_rejects_signature_drift(monkeypatch):
    system_oom_recovery.validate_runtime_capability.cache_clear()
    monkeypatch.setattr(job_lib, 'get_status',
                        lambda: job_lib.JobStatus.RUNNING)
    with pytest.raises(system_oom_recovery.RecoveryError, match='incomplete'):
        system_oom_recovery.validate_runtime_capability()
    system_oom_recovery.validate_runtime_capability.cache_clear()


def test_recovery_log_wrapper_keeps_secret_out_of_argv(attempt_context, v2_plan,
                                                       monkeypatch, tmp_path):
    supervisor_argv = ['python', '-m', 'supervisor', '--plan-path', '/private']
    build = mock.Mock(return_value=supervisor_argv)
    monkeypatch.setattr(system_oom_recovery, 'build_supervisor_command', build)
    run = mock.Mock(return_value=0)
    monkeypatch.setattr(log_lib, 'run_with_log', run)
    result = (log_lib.
              run_bash_command_with_log_and_return_pid_with_system_oom_recovery(
                  None,
                  str(tmp_path / 'run.log'),
                  attempt_context,
                  v2_plan,
                  env_vars={'TOKEN': 'super-secret-value'},
                  stream_logs=True,
                  with_ray=True))
    assert result['return_code'] == 0
    bound_plan = build.call_args.args[2]
    assert dict(bound_plan.execution_envelope.environment)['TOKEN'] == (
        'super-secret-value')
    assert 'super-secret-value' not in ' '.join(supervisor_argv)


def test_owned_postlude_returns_original_exit_code(monkeypatch, v2_plan):
    envelope = v2_plan.execution_envelope
    assert envelope is not None
    run = mock.Mock(return_value=SimpleNamespace(returncode=17))
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'run', run)
    assert subprocess_supervisor._run_owned_postlude(17, envelope) == 17
    assert '(exit 17)' in run.call_args.args[0][-1]


def test_boltz_v1_v2_execution_envelope_differential(tmp_path, monkeypatch):
    spec = system_oom_recovery.OwnedContainerSpec(
        image='example.invalid/model@sha256:' + 'a' * 64,
        create_options=('--gpus', 'all', '--publish', '8080:8080'),
        argv=('serve', '--port', '8080'),
        inherited_environment_names=('MODEL',))
    environment = {'MODEL': 'boltz'}
    v2_plan = system_oom_recovery.RecoveryLaunchPlan.owned_container(
        spec).bind_environment(environment)
    envelope = v2_plan.execution_envelope
    assert envelope is not None
    v1_task = task_codegen.TaskCodeGen.build_task_bash_script(
        spec.render(), env_prefix='unset RAY_RAYLET_PID')
    v1_script = log_lib.make_task_bash_script(v1_task, env_vars=environment)
    v2_prelude = envelope.render_private_file_prelude()

    # The exact Boltz closure sees the same setup, environment, working
    # directory, Ray-variable removal, and byte-identical rclone postlude.
    for line in (
            'source ~/.bashrc',
            'set -a',
            'set +a',
            constants.DEACTIVATE_SKY_REMOTE_PYTHON_ENV,
            'export PYTHONUNBUFFERED=1',
            "export MODEL=boltz",
            'unset RAY_RAYLET_PID',
    ):
        assert line in v1_script
        assert line in v2_prelude
    assert f'cd {constants.SKY_REMOTE_WORKDIR}' in v1_script
    assert 'cd "$HOME"/sky_workdir' in v2_prelude
    assert spec.render() in v1_script
    assert envelope.postlude_script == (
        system_oom_recovery.build_rclone_flush_script())
    assert v1_script.endswith(envelope.postlude_script + '\n')

    # Both recovery profiles use the same log stream contract; only their
    # private supervisor command differs.
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    v1_plan = system_oom_recovery.RecoveryLaunchPlan.direct_shell()
    v1_context = system_oom_recovery.new_attempt_context(7, 0, 0, v1_plan)
    v2_context = system_oom_recovery.new_attempt_context(8, 0, 0, v2_plan)
    monkeypatch.setattr(
        system_oom_recovery, 'build_supervisor_command', lambda _command,
        _context, plan: ['supervisor', str(plan.profile_version)])
    run = mock.Mock(return_value=23)
    monkeypatch.setattr(log_lib, 'run_with_log', run)
    common_logging = {
        'stream_logs': True,
        'with_ray': True,
        'streaming_prefix': 'boltz',
    }
    log_path = str(tmp_path / 'run.log')

    v1_result = (
        log_lib.
        run_bash_command_with_log_and_return_pid_with_system_oom_recovery(
            v1_task,
            log_path,
            v1_context,
            v1_plan,
            env_vars=environment,
            **common_logging))
    v2_result = (
        log_lib.
        run_bash_command_with_log_and_return_pid_with_system_oom_recovery(
            None,
            log_path,
            v2_context,
            system_oom_recovery.RecoveryLaunchPlan.owned_container(spec),
            env_vars=environment,
            **common_logging))

    assert v1_result['return_code'] == v2_result['return_code'] == 23
    assert [call.args[1] for call in run.call_args_list] == [log_path, log_path]
    assert [call.kwargs for call in run.call_args_list] == [
        {
            **common_logging, 'shell': False
        },
        {
            **common_logging, 'shell': False
        },
    ]

    # TERM is the graceful signal in both profiles; SIGKILL can never produce
    # a positive cleanup result. The v2 attached Docker CLI has the same
    # inherited stdout/stderr stream as v1.
    docker_environment = {'PATH': '/usr/bin'}
    monkeypatch.setattr(system_oom_recovery, '_docker_environment',
                        lambda: docker_environment)
    popen = mock.Mock(return_value=mock.sentinel.attached_process)
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)
    assert subprocess_supervisor._start_owned_container(
        'a' * 64) is mock.sentinel.attached_process
    popen.assert_called_once_with(['docker', 'start', '--attach', 'a' * 64],
                                  shell=False,
                                  start_new_session=True,
                                  env=docker_environment)
    command = mock.Mock(pid=888)
    signals = []
    monkeypatch.setattr(
        subprocess_supervisor, '_signal_descendants',
        lambda requested_signal, _pid: signals.append(requested_signal) or True)
    monkeypatch.setattr(subprocess_supervisor, '_wait_for_descendants_empty',
                        lambda *_args: True)
    monkeypatch.setattr(subprocess_supervisor, '_remove_owned_container',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        lambda *_args: True)
    monkeypatch.setattr(subprocess_supervisor, '_descendants', lambda: [])
    identity = _docker_identity()

    v1_cleanup = subprocess_supervisor._cleanup(command, identity, armed=True)
    v2_cleanup = subprocess_supervisor._cleanup(command,
                                                identity,
                                                armed=True,
                                                owned_container_id='a' * 64)

    assert signals == [signal.SIGTERM, signal.SIGTERM]
    assert v1_cleanup['graceful'] is v2_cleanup['graceful'] is True
    assert v1_cleanup['forced'] is v2_cleanup['forced'] is False


def test_owned_start_uses_supported_attached_docker_argv(monkeypatch):
    environment = {'PATH': '/usr/bin'}
    monkeypatch.setattr(system_oom_recovery, '_docker_environment',
                        lambda: environment)
    process = mock.sentinel.process
    popen = mock.Mock(return_value=process)
    monkeypatch.setattr(subprocess_supervisor.subprocess, 'Popen', popen)

    assert subprocess_supervisor._start_owned_container('a' * 64) is process

    popen.assert_called_once_with(['docker', 'start', '--attach', 'a' * 64],
                                  shell=False,
                                  start_new_session=True,
                                  env=environment)


def test_v2_signal_at_final_gate_suppresses_start_and_removes_exact_id(
        tmp_path, monkeypatch, v2_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    context = system_oom_recovery.new_attempt_context(7, 0, 0, v2_plan)
    context = _bound(context)
    container_id = 'a' * 64
    identity = _docker_identity()

    monkeypatch.setattr(subprocess_supervisor, '_set_parent_death_signal',
                        lambda _expected_parent_pid: 77)
    fence = mock.Mock(side_effect=[
        None, None, None,
        system_oom_recovery.RecoveryError('termination latch set')
    ])
    monkeypatch.setattr(subprocess_supervisor, '_assert_parent_fence', fence)
    monkeypatch.setattr(subprocess_supervisor, '_supervisor_identity', lambda: {
        'pid': 123,
        'pid_create_time': 456.0
    })
    monkeypatch.setattr(subprocess_supervisor.os, 'getsid',
                        lambda _pid: subprocess_supervisor.os.getpid())
    monkeypatch.setattr(subprocess_supervisor, 'enable_subreaper', lambda: None)
    monkeypatch.setattr(system_oom_recovery, 'get_docker_identity',
                        lambda: identity)
    monkeypatch.setattr(system_oom_recovery, 'docker_container_inventory',
                        lambda: ())
    monkeypatch.setattr(subprocess_supervisor, '_create_owned_container',
                        mock.Mock(return_value=container_id))
    start = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor, '_start_owned_container', start)
    remove = mock.Mock(return_value=True)
    monkeypatch.setattr(subprocess_supervisor, '_remove_owned_container',
                        remove)
    stable = mock.Mock(return_value=True)
    monkeypatch.setattr(system_oom_recovery, 'wait_for_stable_empty_docker',
                        stable)
    write_capability = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor, '_write_capability',
                        write_capability)

    assert subprocess_supervisor.supervise(None, context, v2_plan) == 1

    start.assert_not_called()
    remove.assert_called_once_with(identity, container_id, mock.ANY, force=True)
    stable.assert_called_once_with(identity, mock.ANY)
    assert fence.call_count == 4
    assert write_capability.call_count == 1
    assert write_capability.call_args.kwargs['armed'] is False


def test_v2_positive_capability_is_published_only_after_final_gate_and_start(
        tmp_path, monkeypatch, v2_plan):
    monkeypatch.setattr(system_oom_recovery, 'RECOVERY_ROOT',
                        str(tmp_path / 'recovery'))
    monkeypatch.setattr(system_oom_recovery, 'read_boot_id', lambda: 'boot-id')
    context = _bound(system_oom_recovery.new_attempt_context(7, 0, 0, v2_plan))
    container_id = 'a' * 64
    identity = _docker_identity()
    events = []
    timeline = []

    monkeypatch.setattr(subprocess_supervisor, '_set_parent_death_signal',
                        lambda _expected_parent_pid: 77)

    def _fence(_context, _parent_pid, event, *_args):
        events.append(event)
        timeline.append('fence')

    monkeypatch.setattr(subprocess_supervisor, '_assert_parent_fence', _fence)
    monkeypatch.setattr(subprocess_supervisor, '_supervisor_identity', lambda: {
        'pid': 123,
        'pid_create_time': 456.0
    })
    monkeypatch.setattr(subprocess_supervisor.os, 'getsid',
                        lambda _pid: subprocess_supervisor.os.getpid())
    monkeypatch.setattr(subprocess_supervisor, 'enable_subreaper', lambda: None)
    monkeypatch.setattr(system_oom_recovery, 'get_docker_identity',
                        lambda: identity)
    monkeypatch.setattr(system_oom_recovery, 'docker_container_inventory',
                        lambda: ())
    monkeypatch.setattr(subprocess_supervisor, '_create_owned_container',
                        mock.Mock(return_value=container_id))
    command = mock.Mock(pid=888, returncode=1)
    command.poll.return_value = None

    def _start(_container_id):
        timeline.append('start')
        return command

    start = mock.Mock(side_effect=_start)
    monkeypatch.setattr(subprocess_supervisor, '_start_owned_container', start)
    cleanup = mock.Mock(return_value={'graceful': True})
    monkeypatch.setattr(subprocess_supervisor, '_cleanup', cleanup)
    write_cleanup = mock.Mock()
    monkeypatch.setattr(subprocess_supervisor, '_write_cleanup', write_cleanup)
    writes = []

    def _write_capability(*_args, **kwargs):
        writes.append(kwargs['armed'])
        timeline.append(f'capability:{kwargs["armed"]}')
        if kwargs['armed']:
            events[-1].set()

    monkeypatch.setattr(subprocess_supervisor, '_write_capability',
                        _write_capability)

    assert subprocess_supervisor.supervise(None, context, v2_plan) == 1

    assert len(events) == 4
    assert timeline == ['fence'] * 4 + ['start', 'capability:True']
    assert writes == [True]
    start.assert_called_once_with(container_id)
    cleanup.assert_called_once_with(command,
                                    identity,
                                    armed=True,
                                    owned_container_id=container_id)
    write_cleanup.assert_called_once()


class _FakeClock:

    def __init__(self, wall_time, monotonic_time=10.0):
        self.wall = wall_time
        self.monotonic = monotonic_time
        self.waits = []

    def wall_time(self):
        return self.wall

    def monotonic_time(self):
        return self.monotonic

    def advance(self, seconds):
        self.wall += seconds
        self.monotonic += seconds

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.advance(seconds)


def _waiting_memory_session(attempt_context,
                            v1_plan,
                            submitter,
                            *,
                            clock=None,
                            visibility_confirmed=True):
    clock_kwargs = ({
        'wall_clock': clock.wall_time,
        'monotonic_clock': clock.monotonic_time,
        'wait': clock.sleep,
    } if clock is not None else {})
    session = system_oom_recovery.RecoverySession(7, v1_plan, attempt_context,
                                                  'original-ref', submitter,
                                                  **clock_kwargs)
    session.armed_info = job_lib.JobSystemRecoveryInfo(
        capability=v1_plan.capability,
        phase=job_lib.JobSystemRecoveryPhase.ARMED,
        original_attempt_id=attempt_context['attempt_id'],
        replacement_attempt_id=None,
        task_index=0,
        node_boot_id='boot-id',
        occurrence_count=0,
        armed_at=attempt_context['created_at'],
        updated_at=attempt_context['created_at'])
    session.observe_oom()
    session.phase = job_lib.JobSystemRecoveryPhase.WAITING_MEMORY
    session.first_event_visible_monotonic = session.monotonic_clock()
    session.event_visibility_confirmed = visibility_confirmed
    return session


def _patch_session_submission(monkeypatch, transition_results):
    monkeypatch.setattr(
        system_oom_recovery, 'read_capability_marker', lambda _: {
            'armed': True,
            'docker_identity': _docker_identity().to_dict(),
        })
    monkeypatch.setattr(job_lib, 'job_status_lock',
                        lambda _job_id: contextlib.nullcontext())
    monkeypatch.setattr(job_lib, 'get_status',
                        lambda _job_id: job_lib.JobStatus.RUNNING)
    transitions = mock.Mock(side_effect=transition_results)
    monkeypatch.setattr(job_lib, 'transition_job_system_recovery_no_lock',
                        transitions)
    exhausted = mock.Mock(return_value=True)
    monkeypatch.setattr(job_lib, 'exhaust_job_system_recovery_no_lock',
                        exhausted)
    monkeypatch.setattr(job_lib, 'exhaust_job_system_recovery',
                        mock.Mock(return_value=True))
    return transitions, exhausted


@pytest.mark.parametrize('wall_jump', [10000.0, -10000.0])
def test_first_event_visibility_uses_monotonic_remainder_across_wall_jump(
        attempt_context, v1_plan, monkeypatch, wall_jump):
    clock = _FakeClock(attempt_context['created_at'] + 1)
    session = _waiting_memory_session(attempt_context,
                                      v1_plan,
                                      mock.Mock(),
                                      clock=clock,
                                      visibility_confirmed=False)
    clock.advance(12)
    clock.wall += wall_jump
    monkeypatch.setattr(job_lib, 'get_status',
                        lambda _job_id: job_lib.JobStatus.RUNNING)

    assert session.wait_for_first_event_visibility() == (True, '')

    assert clock.waits == [23.0]
    assert session.event_visibility_confirmed


def test_first_event_visibility_starts_after_waiting_cleanup_is_persisted(
        attempt_context, v1_plan, monkeypatch):
    clock = _FakeClock(attempt_context['created_at'] + 1)
    session = _waiting_memory_session(attempt_context,
                                      v1_plan,
                                      mock.Mock(),
                                      clock=clock,
                                      visibility_confirmed=False)
    session.phase = job_lib.JobSystemRecoveryPhase.ARMED
    session.first_event_visible_monotonic = None
    clock.advance(20)
    monkeypatch.setattr(job_lib, 'transition_job_system_recovery',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(job_lib, 'get_status',
                        lambda _job_id: job_lib.JobStatus.RUNNING)

    assert session.transition(job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP,
                              'persisted event')
    assert session.first_event_visible_monotonic == 30.0
    assert session.wait_for_first_event_visibility() == (True, '')

    assert clock.waits == [35.0]


def test_first_event_visibility_never_extends_local_deadline(
        attempt_context, v1_plan, monkeypatch):
    clock = _FakeClock(attempt_context['created_at'] + 1)
    session = _waiting_memory_session(attempt_context,
                                      v1_plan,
                                      mock.Mock(),
                                      clock=clock,
                                      visibility_confirmed=False)
    session.deadline_monotonic = clock.monotonic + 10
    get_status = mock.Mock(return_value=job_lib.JobStatus.RUNNING)
    monkeypatch.setattr(job_lib, 'get_status', get_status)

    valid, reason = session.wait_for_first_event_visibility()

    assert not valid
    assert 'deadline expired' in reason
    assert clock.waits == [10.0]
    get_status.assert_not_called()


def test_first_event_visibility_rechecks_cancellation_after_wait(
        attempt_context, v1_plan, monkeypatch):
    clock = _FakeClock(attempt_context['created_at'] + 1)
    submitter = mock.Mock()
    session = _waiting_memory_session(attempt_context,
                                      v1_plan,
                                      submitter,
                                      clock=clock,
                                      visibility_confirmed=False)
    get_status = mock.Mock(return_value=job_lib.JobStatus.CANCELLED)
    monkeypatch.setattr(job_lib, 'get_status', get_status)

    valid, reason = session.wait_for_first_event_visibility()

    assert not valid
    assert 'no longer running' in reason
    assert clock.waits == [35.0]
    get_status.assert_called_once_with(7)
    submitter.assert_not_called()


def test_try_arm_requires_exact_existing_record(attempt_context, v1_plan,
                                                monkeypatch):
    wall_clock = mock.Mock(return_value=attempt_context['created_at'] + 1)
    session = system_oom_recovery.RecoverySession(7,
                                                  v1_plan,
                                                  attempt_context,
                                                  'original-ref',
                                                  mock.Mock(),
                                                  wall_clock=wall_clock)
    captured = {}

    def _arm(_job_id, info):
        captured['info'] = info
        return False

    monkeypatch.setattr(job_lib, 'arm_job_system_recovery', _arm)
    monkeypatch.setattr(
        job_lib, 'get_job_system_recovery_info',
        lambda _job_id: dataclasses.replace(captured['info'], task_index=1))

    assert not session.try_arm({
        'armed': True,
        'written_at': attempt_context['created_at'],
    })
    assert session.armed_info is None


def test_recovery_session_adopts_exactly_one_replacement(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(return_value='replacement-ref')
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    transitions, _ = _patch_session_submission(monkeypatch, [True, True])
    cancel = mock.Mock()

    assert session.submit_one_retry(SimpleNamespace(cancel=cancel)) is True

    submitter.assert_called_once_with(1, session.replacement_context)
    cancel.assert_not_called()
    assert transitions.call_count == 2
    assert session.phase == job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED
    assert session.current_future == 'replacement-ref'
    replacement_ids = {
        call.args[2].replacement_attempt_id
        for call in transitions.call_args_list
    }
    assert replacement_ids == {session.replacement_context['attempt_id']}

    # A repeated call is terminal, never a second `.remote()`.
    assert session.submit_one_retry(SimpleNamespace(cancel=cancel)) is False
    submitter.assert_called_once()


def test_recovery_session_failed_adoption_cancels_and_retains_identity(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(return_value='unadopted-ref')
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    transitions, exhausted = _patch_session_submission(monkeypatch,
                                                       [True, False])
    cancel = mock.Mock()

    assert session.submit_one_retry(SimpleNamespace(cancel=cancel)) is False

    submitter.assert_called_once()
    cancel.assert_called_once_with('unadopted-ref', force=True)
    assert session.phase == job_lib.JobSystemRecoveryPhase.EXHAUSTED
    replacement_id = session.replacement_context['attempt_id']
    assert transitions.call_args_list[0].args[2].replacement_attempt_id == (
        replacement_id)
    assert transitions.call_args_list[1].args[2].replacement_attempt_id == (
        replacement_id)
    assert exhausted.call_args.args[2].replacement_attempt_id == replacement_id


def test_recovery_session_resubmitting_failure_never_calls_remote(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock()
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _, exhausted = _patch_session_submission(monkeypatch, [False])

    assert session.submit_one_retry(
        SimpleNamespace(cancel=mock.Mock())) is False

    submitter.assert_not_called()
    exhausted.assert_called_once()


def test_recovery_session_locked_transition_exception_cancels_after_unlock(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(return_value='unadopted-ref')
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _patch_session_submission(monkeypatch, [True, RuntimeError('CAS failed')])
    lock_state = {'held': False}

    @contextlib.contextmanager
    def _lock(_job_id):
        lock_state['held'] = True
        try:
            yield
        finally:
            lock_state['held'] = False

    monkeypatch.setattr(job_lib, 'job_status_lock', _lock)

    def _cancel(_future, *, force):
        assert force is True
        assert not lock_state['held']

    cancel = mock.Mock(side_effect=_cancel)

    assert not session.submit_one_retry(SimpleNamespace(cancel=cancel))

    submitter.assert_called_once()
    cancel.assert_called_once_with('unadopted-ref', force=True)


@pytest.mark.parametrize('submitted', [None, RuntimeError('submit failed')])
def test_recovery_session_submit_failure_or_missing_ref_never_adopts(
        attempt_context, v1_plan, monkeypatch, submitted):
    submitter = (mock.Mock(side_effect=submitted) if isinstance(
        submitted, Exception) else mock.Mock(return_value=submitted))
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    transitions, exhausted = _patch_session_submission(monkeypatch, [True])
    cancel = mock.Mock()

    assert not session.submit_one_retry(SimpleNamespace(cancel=cancel))

    submitter.assert_called_once()
    assert transitions.call_count == 1
    exhausted.assert_called_once()
    cancel.assert_not_called()


def test_recovery_session_long_submit_error_is_bounded_and_exhausted(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(side_effect=RuntimeError('x' * 20000))
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _, exhausted = _patch_session_submission(monkeypatch, [True])

    assert not session.submit_one_retry(SimpleNamespace(cancel=mock.Mock()))

    exhausted.assert_called_once()
    terminal_info = exhausted.call_args.args[2]
    assert terminal_info.phase == job_lib.JobSystemRecoveryPhase.EXHAUSTED
    assert len(terminal_info.summary) == (
        job_lib.JOB_SYSTEM_RECOVERY_SUMMARY_MAX_CHARS)


def test_recovery_session_post_submit_deadline_cancels_ref(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(return_value='late-ref')
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _patch_session_submission(monkeypatch, [True])
    session.deadline_monotonic = 100.0
    session.monotonic_clock = mock.Mock(side_effect=[1.0, 2.0, 101.0])
    cancel = mock.Mock()

    assert not session.submit_one_retry(SimpleNamespace(cancel=cancel))

    submitter.assert_called_once()
    cancel.assert_called_once_with('late-ref', force=True)


def test_recovery_session_lock_exit_failure_preserves_durable_adoption(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock(return_value='adopted-ref')
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _patch_session_submission(monkeypatch, [True, True])

    @contextlib.contextmanager
    def _failing_exit_lock(_job_id):
        yield
        raise RuntimeError('lock exit failed')

    monkeypatch.setattr(job_lib, 'job_status_lock', _failing_exit_lock)
    cancel = mock.Mock()

    assert session.submit_one_retry(SimpleNamespace(cancel=cancel))

    assert session.phase == job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED
    assert session.current_future == 'adopted-ref'
    submitter.assert_called_once()
    cancel.assert_not_called()


def test_recovery_session_lock_entry_failure_before_ref_returns_false(
        attempt_context, v1_plan, monkeypatch):
    submitter = mock.Mock()
    session = _waiting_memory_session(attempt_context, v1_plan, submitter)
    _patch_session_submission(monkeypatch, [])

    @contextlib.contextmanager
    def _failing_entry_lock(_job_id):
        raise RuntimeError('lock entry failed')
        yield  # pragma: no cover

    monkeypatch.setattr(job_lib, 'job_status_lock', _failing_entry_lock)

    assert not session.submit_one_retry(SimpleNamespace(cancel=mock.Mock()))

    submitter.assert_not_called()


def test_outer_finally_removes_placement_group_on_session_construction_failure(
        attempt_context, v1_plan):
    malformed_context = dict(attempt_context, schema_version=True)
    remove = mock.Mock()
    ray_util = SimpleNamespace(remove_placement_group=remove)
    placement_group = mock.sentinel.placement_group

    with pytest.raises(system_oom_recovery.RecoveryError,
                       match='schema is unsupported'):
        system_oom_recovery.get_or_fail_with_recovery(SimpleNamespace(),
                                                      ray_util, 'future',
                                                      placement_group,
                                                      mock.Mock(),
                                                      malformed_context, 7,
                                                      v1_plan)

    remove.assert_called_once_with(placement_group)


def test_supervisor_cleanup_never_marks_forced_positive(monkeypatch):
    identity = _docker_identity()
    monkeypatch.setattr(subprocess_supervisor, '_signal_descendants',
                        lambda _signal, _pid: True)
    monkeypatch.setattr(subprocess_supervisor, '_wait_for_descendants_empty',
                        mock.Mock(side_effect=[False, True]))
    monkeypatch.setattr(subprocess_supervisor,
                        '_force_remove_attempt_containers',
                        lambda _identity: None)
    monkeypatch.setattr(system_oom_recovery, 'docker_identity_matches',
                        lambda _identity: True)
    monkeypatch.setattr(system_oom_recovery, 'docker_container_inventory',
                        lambda: ())
    monkeypatch.setattr(subprocess_supervisor, '_descendants', lambda: [])
    facts = subprocess_supervisor._cleanup(None, identity, armed=True)
    assert facts['forced'] is True
    assert facts['graceful'] is False
    assert facts['timed_out'] is True
    assert signal.SIGKILL != signal.SIGTERM
