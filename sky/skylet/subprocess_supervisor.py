"""Recovery-only Linux subreaper for a single SkyServe service command.

The ordinary SkyPilot subprocess path does not invoke this module.  It is
started in a new session by ``log_lib`` and becomes the actual parent of the
user command.  Its positive cleanup marker is intentionally stricter than a
best-effort process kill: any timeout, SIGKILL, Docker identity change, or
remaining container makes same-machine replay ineligible.
"""

import argparse
import ctypes
import json
import os
import signal
import subprocess
import sys
import threading
import time
import typing

from sky.skylet import system_oom_recovery

if typing.TYPE_CHECKING:
    import psutil
else:
    from sky.adaptors import common as adaptors_common
    psutil = adaptors_common.LazyImport('psutil')

PR_SET_PDEATHSIG = 1
PR_SET_CHILD_SUBREAPER = 36
PR_GET_CHILD_SUBREAPER = 37
GRACEFUL_TERMINATION_SECONDS = 20
FORCED_REAP_SECONDS = 4
POLL_INTERVAL_SECONDS = 0.1


class ProcessEnumerationError(RuntimeError):
    """The supervisor could not prove the state of its process scope."""


def _prctl(option: int, arg2: object = 0) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.prctl(option, arg2, 0, 0, 0)
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


def enable_subreaper() -> None:
    """Enable and verify Linux child-subreaper semantics for this process."""
    if not sys.platform.startswith('linux'):
        raise OSError('child subreapers require Linux')
    _prctl(PR_SET_CHILD_SUBREAPER, 1)
    enabled = ctypes.c_int(0)
    _prctl(PR_GET_CHILD_SUBREAPER, ctypes.byref(enabled))
    if enabled.value != 1:
        raise OSError('kernel did not enable child subreaper')


def _set_parent_death_signal(expected_parent_pid: int) -> int:
    """Arrange TERM on Ray-worker death and close the setup race."""
    parent_pid = os.getppid()
    if parent_pid != expected_parent_pid:
        raise OSError('Ray worker parent changed before parent-death setup')
    _prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
    if os.getppid() != expected_parent_pid:
        raise OSError('Ray worker exited during parent-death setup')
    return expected_parent_pid


def _assert_parent_fence(
        context: dict[str, object],
        parent_pid: int,
        termination_requested: threading.Event,
        docker_identity: system_oom_recovery.DockerIdentity | None = None,
        expected_inventory: tuple[str, ...] | None = None) -> None:
    """Fail if the one-way parent/boot/daemon start fence is not intact."""
    if termination_requested.is_set():
        raise system_oom_recovery.RecoveryError(
            'termination was requested before workload start')
    if parent_pid <= 1 or os.getppid() != parent_pid:
        raise system_oom_recovery.RecoveryError(
            'Ray worker parent identity changed before workload start')
    if context['node_boot_id'] != system_oom_recovery.read_boot_id():
        raise system_oom_recovery.RecoveryError(
            'node boot identity changed before workload start')
    if docker_identity is not None:
        if not system_oom_recovery.docker_identity_matches(docker_identity):
            raise system_oom_recovery.RecoveryError(
                'Docker daemon identity changed before workload start')
        if expected_inventory is not None:
            inventory = system_oom_recovery.docker_container_inventory()
            if inventory != expected_inventory:
                raise system_oom_recovery.RecoveryError(
                    'Docker inventory changed before workload start')


def _supervisor_identity() -> dict[str, object]:
    process = psutil.Process(os.getpid())
    return {
        'pid': os.getpid(),
        'pid_create_time': float(process.create_time()),
    }


def _descendants() -> list['psutil.Process']:
    try:
        return psutil.Process(os.getpid()).children(recursive=True)
    except (psutil.Error, OSError) as e:
        raise ProcessEnumerationError(
            f'cannot enumerate supervisor descendants: {e}') from e


def _direct_children() -> list['psutil.Process']:
    try:
        return psutil.Process(os.getpid()).children(recursive=False)
    except (psutil.Error, OSError) as e:
        raise ProcessEnumerationError(
            f'cannot enumerate adopted children: {e}') from e


def _reap_adopted_children(command_pid: int | None) -> None:
    """Reap direct adopted children while leaving ``Popen``'s child alone."""
    for child in _direct_children():
        if child.pid == command_pid:
            continue
        try:
            os.waitpid(child.pid, os.WNOHANG)
        except (ChildProcessError, OSError):
            pass


def _signal_descendants(signum: int, command_pid: int | None) -> bool:
    # The command is a session/process-group leader.  Signalling its group is
    # the fast path; individually signalling the current tree also reaches a
    # descendant that called setsid()/setpgid() before becoming adopted.
    if command_pid is not None:
        try:
            os.killpg(command_pid, signum)
        except (ProcessLookupError, PermissionError):
            pass
    try:
        descendants = _descendants()
    except ProcessEnumerationError:
        return False
    for process in descendants:
        try:
            os.kill(process.pid, signum)
        except (ProcessLookupError, PermissionError):
            pass
    return True


def _wait_for_descendants_empty(command: subprocess.Popen[bytes] | None,
                                deadline: float) -> bool:
    command_pid = command.pid if command is not None else None
    while time.monotonic() <= deadline:
        if command is not None:
            command.poll()
        try:
            _reap_adopted_children(command_pid)
            descendants = _descendants()
        except ProcessEnumerationError:
            return False
        if not descendants:
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def _force_remove_attempt_containers(
        expected: system_oom_recovery.DockerIdentity) -> None:
    # Baseline emptiness is what makes every current container attempt-owned.
    # Never call this for an unarmed attempt.
    if not system_oom_recovery.docker_identity_matches(expected):
        return
    try:
        inventory = system_oom_recovery.docker_container_inventory()
        if inventory:
            system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['rm', '-f', *inventory])
    except system_oom_recovery.RecoveryError:
        pass


def _remove_owned_container(expected: system_oom_recovery.DockerIdentity,
                            container_id: str,
                            deadline: float,
                            *,
                            force: bool = False) -> bool:
    """Remove only the exact v2 container and prove that it is absent."""
    if not system_oom_recovery.docker_identity_matches(expected):
        return False
    try:
        if force:
            system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['rm', '-f', container_id])
        else:
            # TERM is explicit; unlike `docker stop`, this never silently
            # escalates to a daemon-issued SIGKILL after a timeout.
            system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['kill', '--signal', 'TERM', container_id])
            while time.monotonic() <= deadline:
                running = system_oom_recovery._run_docker(  # pylint: disable=protected-access
                    ['inspect', '--format', '{{.State.Running}}', container_id])
                if running == 'false':
                    break
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                return False
            system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['rm', container_id])
    except system_oom_recovery.RecoveryError:
        # A naturally exited container may already be stopped, so TERM can
        # report an error. Exact inspect/rm remains safe and lossless.
        if force:
            return False
        try:
            running = system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['inspect', '--format', '{{.State.Running}}', container_id])
            if running != 'false':
                return False
            system_oom_recovery._run_docker(  # pylint: disable=protected-access
                ['rm', container_id])
        except system_oom_recovery.RecoveryError:
            return False
    return (system_oom_recovery.docker_identity_matches(expected) and
            container_id
            not in system_oom_recovery.docker_container_inventory())


def _cleanup(command: subprocess.Popen[bytes] | None,
             docker_identity: system_oom_recovery.DockerIdentity | None,
             armed: bool,
             owned_container_id: str | None = None) -> dict[str, object]:
    """Terminate/reap the scope, returning facts for the cleanup marker."""
    command_pid = command.pid if command is not None else None
    started_at = time.time()
    graceful_deadline = time.monotonic() + GRACEFUL_TERMINATION_SECONDS
    enumeration_proven = _signal_descendants(signal.SIGTERM, command_pid)
    owned_removed = True
    if (armed and docker_identity is not None and
            owned_container_id is not None):
        # Docker daemon ownership outlives the attached CLI. Signal and remove
        # the exact ID before waiting for the CLI descendant to disappear.
        owned_removed = _remove_owned_container(docker_identity,
                                                owned_container_id,
                                                graceful_deadline)
    descendants_empty = (enumeration_proven and _wait_for_descendants_empty(
        command, graceful_deadline))
    docker_empty = False
    if descendants_empty and armed and docker_identity is not None:
        if owned_container_id is None:
            docker_empty = system_oom_recovery.wait_for_stable_empty_docker(
                docker_identity, graceful_deadline)
        else:
            docker_empty = (owned_removed and
                            system_oom_recovery.wait_for_stable_empty_docker(
                                docker_identity, graceful_deadline))
    elif not armed:
        # This value is diagnostic only.  An unarmed capability marker can
        # never authorize replay regardless of the cleanup marker.
        docker_empty = False

    forced = not descendants_empty or (armed and not docker_empty)
    if forced:
        enumeration_proven = (_signal_descendants(signal.SIGKILL, command_pid)
                              and enumeration_proven)
        if armed and docker_identity is not None:
            if owned_container_id is None:
                _force_remove_attempt_containers(docker_identity)
            else:
                _remove_owned_container(docker_identity,
                                        owned_container_id,
                                        time.monotonic() + FORCED_REAP_SECONDS,
                                        force=True)
        forced_deadline = time.monotonic() + FORCED_REAP_SECONDS
        descendants_empty = (enumeration_proven and _wait_for_descendants_empty(
            command, forced_deadline))
        if armed and docker_identity is not None:
            docker_empty = (
                system_oom_recovery.docker_identity_matches(docker_identity) and
                not system_oom_recovery.docker_container_inventory())

    try:
        survivor_pids = sorted(process.pid for process in _descendants())
    except ProcessEnumerationError:
        survivor_pids = []
        enumeration_proven = False
        descendants_empty = False
        forced = True
    return {
        'started_at': started_at,
        'completed_at': time.time(),
        'graceful': (armed and not forced and enumeration_proven and
                     descendants_empty and docker_empty),
        'forced': forced,
        'timed_out': forced,
        'descendants_empty': descendants_empty,
        'docker_empty': docker_empty,
        'enumeration_proven': enumeration_proven,
        'survivor_pids': survivor_pids,
    }


def _write_capability(context: dict[str, object],
                      *,
                      armed: bool,
                      reason: str | None,
                      supervisor: dict[str, object],
                      docker_identity: system_oom_recovery.DockerIdentity |
                      None,
                      owned_container_id: str | None = None) -> None:
    marker = {
        'schema_version': context['schema_version'],
        'kind': 'capability',
        **system_oom_recovery._attempt_fields(  # pylint: disable=protected-access
            context),
        'capability': context['capability'],
        'armed': armed,
        'reason': reason,
        'supervisor': supervisor,
        'docker_identity':
            (docker_identity.to_dict() if docker_identity is not None else None
            ),
        'owned_container_id': owned_container_id,
        'written_at': time.time(),
    }
    system_oom_recovery.atomic_write_marker(str(context['capability_path']),
                                            marker)


def _write_cleanup(context: dict[str, object],
                   supervisor: dict[str, object],
                   docker_identity: system_oom_recovery.DockerIdentity | None,
                   facts: dict[str, object],
                   trigger: str,
                   owned_container_id: str | None = None) -> None:
    marker = {
        'schema_version': context['schema_version'],
        'kind': 'cleanup',
        **system_oom_recovery._attempt_fields(  # pylint: disable=protected-access
            context),
        'supervisor': supervisor,
        'docker_identity':
            (docker_identity.to_dict() if docker_identity is not None else None
            ),
        'trigger': trigger,
        'owned_container_id': owned_container_id,
        **facts,
    }
    system_oom_recovery.atomic_write_marker(str(context['cleanup_path']),
                                            marker)


def _create_owned_container(spec: system_oom_recovery.OwnedContainerSpec,
                            context: dict[str, object]) -> str:
    labels = {
        'skypilot.system-oom-recovery.job-id': str(context['job_id']),
        'skypilot.system-oom-recovery.attempt-id': str(context['attempt_id']),
    }
    name = ('sky-oom-'
            f'{context["job_id"]}-{str(context["attempt_id"])[:12]}')
    arguments = spec.docker_create_argv(name, labels)
    environment = system_oom_recovery._docker_environment(  # pylint: disable=protected-access
    )
    try:
        result = subprocess.run(['docker', *arguments],
                                check=False,
                                capture_output=True,
                                text=True,
                                timeout=30,
                                env=environment)
    except (OSError, subprocess.SubprocessError) as e:
        raise system_oom_recovery.RecoveryError(
            f'owned container create failed: {e}') from e
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise system_oom_recovery.RecoveryError(
            f'owned container create exited {result.returncode}: {detail}')
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()
    container_id = result.stdout.strip()
    if system_oom_recovery._CONTAINER_ID_PATTERN.fullmatch(  # pylint: disable=protected-access
            container_id) is None:
        raise system_oom_recovery.RecoveryError(
            'Docker create returned an invalid full container ID')
    return container_id


def _start_owned_container(container_id: str) -> subprocess.Popen[bytes]:
    environment = system_oom_recovery._docker_environment(  # pylint: disable=protected-access
    )
    return subprocess.Popen(  # pylint: disable=consider-using-with
        ['docker', 'start', '--attach', container_id],
        shell=False,
        start_new_session=True,
        env=environment)


def _run_owned_postlude(
        returncode: int,
        envelope: system_oom_recovery.RecoveryExecutionEnvelope) -> int:
    # Seed `$?` with the exact container result before the byte-identical
    # ordinary-task postlude captures it.
    script = f'(exit {returncode})\n{envelope.postlude_script}'
    result = subprocess.run(['/bin/bash', '-c', script], check=False)
    return int(result.returncode)


def _wait_for_command(
        command: subprocess.Popen[bytes],
        termination_requested: threading.Event) -> tuple[int, str]:
    trigger = 'command_exit'
    while command.poll() is None:
        try:
            _reap_adopted_children(command.pid)
        except ProcessEnumerationError:
            termination_requested.set()
            return 1, 'process_enumeration_failure'
        if termination_requested.wait(POLL_INTERVAL_SECONDS):
            return 1, 'parent_or_signal_exit'
    return int(command.returncode), trigger


def supervise(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        inner_command: str | None, context: dict[str, object],
        recovery_plan: system_oom_recovery.RecoveryLaunchPlan) -> int:
    """Run one direct-shell or owned-container workload under a subreaper."""
    context = system_oom_recovery._validate_attempt_context(  # pylint: disable=protected-access
        context)
    if recovery_plan.profile_version != context['profile_version']:
        raise system_oom_recovery.RecoveryError(
            'recovery plan does not match attempt context')
    termination_requested = threading.Event()

    def _handle_termination(_signum, _frame) -> None:
        # One-way latch: no later successful check can reauthorize a start.
        termination_requested.set()

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, _handle_termination)

    try:
        expected_parent_pid = context['expected_parent_pid']
        if not isinstance(expected_parent_pid, int):
            raise system_oom_recovery.RecoveryError(
                'supervisor context has no Ray worker identity')
        parent_pid = _set_parent_death_signal(expected_parent_pid)
        _assert_parent_fence(context, parent_pid, termination_requested)
    except (OSError, system_oom_recovery.RecoveryError) as e:
        # PDEATHSIG, parent, latch, and boot failures are fatal for both
        # profiles. No workload/container Popen is reachable from here.
        print(f'SkyPilot recovery parent fence failed: {e}',
              file=sys.stderr,
              flush=True)
        return 1

    try:
        supervisor = _supervisor_identity()
    except (psutil.Error, OSError) as e:
        supervisor = {'pid': os.getpid(), 'pid_create_time': -1.0}
        failure_reason: str | None = f'supervisor identity failed: {e}'
    else:
        failure_reason = None

    docker_identity = None
    armed = False
    try:
        if os.getsid(0) != os.getpid():
            raise system_oom_recovery.RecoveryError(
                'supervisor is not a session leader')
        enable_subreaper()
        docker_identity = system_oom_recovery.get_docker_identity()
        expected_identity_value = context['expected_docker_identity']
        if expected_identity_value is not None:
            expected_identity = system_oom_recovery.DockerIdentity.from_dict(
                expected_identity_value)
            if docker_identity != expected_identity:
                raise system_oom_recovery.RecoveryError(
                    'replacement Docker daemon identity changed')
        if system_oom_recovery.docker_container_inventory():
            raise system_oom_recovery.RecoveryError(
                'local Docker inventory was nonempty before workload')
        if failure_reason is None:
            armed = True
    except (OSError, system_oom_recovery.RecoveryError) as e:
        failure_reason = str(e)

    requires_armed_start = bool(context['require_armed_start'])
    if not armed and requires_armed_start:
        _write_capability(context,
                          armed=False,
                          reason=failure_reason,
                          supervisor=supervisor,
                          docker_identity=docker_identity)
        return 1

    if (recovery_plan.profile_version ==
            system_oom_recovery.PROFILE_VERSION_DIRECT_SHELL):
        # Deprecated transition: only the original v1 command may retain its
        # ordinary no-retry behavior after a nonfatal capability failure.
        assert inner_command is not None
        _write_capability(context,
                          armed=armed,
                          reason=failure_reason,
                          supervisor=supervisor,
                          docker_identity=docker_identity)
        try:
            _assert_parent_fence(context, parent_pid, termination_requested,
                                 docker_identity if armed else None,
                                 () if armed else None)
        except system_oom_recovery.RecoveryError as e:
            print(f'SkyPilot recovery pre-Popen fence failed: {e}',
                  file=sys.stderr,
                  flush=True)
            return 1
        command = subprocess.Popen(  # pylint: disable=consider-using-with
            inner_command,
            shell=True,
            start_new_session=True)
        returncode, trigger = _wait_for_command(command, termination_requested)
        facts = _cleanup(command, docker_identity, armed)
        _write_cleanup(context, supervisor, docker_identity, facts, trigger)
        return returncode

    assert recovery_plan.owned_container_spec is not None
    assert recovery_plan.execution_envelope is not None
    if not armed or docker_identity is None:
        return 1
    container_id: str | None = None
    owned_command: subprocess.Popen[bytes] | None = None
    try:
        _assert_parent_fence(context, parent_pid, termination_requested,
                             docker_identity, ())
        container_id = _create_owned_container(
            recovery_plan.owned_container_spec, context)
        # Deterministic post-create gate. A signal here latches start off and
        # cleanup removes only the returned exact ID.
        _assert_parent_fence(context, parent_pid, termination_requested,
                             docker_identity, (container_id,))
        # The last deterministic gate precedes both start and positive marker
        # publication, so a gate failure cannot transiently arm the attempt.
        _assert_parent_fence(context, parent_pid, termination_requested,
                             docker_identity, (container_id,))
        owned_command = _start_owned_container(container_id)
        # Publish only after every pre-start gate and Docker Popen succeeded.
        # A very fast failure before this write merely makes recovery
        # unavailable and falls back to VM replacement.
        _write_capability(context,
                          armed=True,
                          reason=None,
                          supervisor=supervisor,
                          docker_identity=docker_identity,
                          owned_container_id=container_id)
        returncode, trigger = _wait_for_command(owned_command,
                                                termination_requested)
        if trigger == 'command_exit':
            returncode = _run_owned_postlude(returncode,
                                             recovery_plan.execution_envelope)
        facts = _cleanup(owned_command,
                         docker_identity,
                         armed=True,
                         owned_container_id=container_id)
        _write_cleanup(context,
                       supervisor,
                       docker_identity,
                       facts,
                       trigger,
                       owned_container_id=container_id)
        return returncode
    except (OSError, system_oom_recovery.RecoveryError) as e:
        cleanup_deadline = time.monotonic() + FORCED_REAP_SECONDS
        removed = container_id is None
        if container_id is not None:
            removed = _remove_owned_container(docker_identity,
                                              container_id,
                                              cleanup_deadline,
                                              force=True)
        cleanup_proven = (removed and
                          system_oom_recovery.wait_for_stable_empty_docker(
                              docker_identity, cleanup_deadline))
        reason = str(e)
        if not cleanup_proven:
            reason = f'{reason}; final Docker emptiness was not proven'
        _write_capability(context,
                          armed=False,
                          reason=reason,
                          supervisor=supervisor,
                          docker_identity=docker_identity,
                          owned_container_id=container_id)
        print(f'SkyPilot owned-container start suppressed: {e}',
              file=sys.stderr,
              flush=True)
        return 1


def main() -> None:
    """Run the recovery supervisor command-line entrypoint."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--context-json', required=True)
    parser.add_argument('--plan-path', required=True)
    parser.add_argument('--command')
    arguments = parser.parse_args()
    try:
        context = json.loads(arguments.context_json)
        recovery_plan = system_oom_recovery.consume_private_recovery_plan(
            arguments.plan_path, context)
        returncode = supervise(arguments.command, context, recovery_plan)
    except Exception as e:  # pylint: disable=broad-except
        print(f'SkyPilot recovery supervisor failed: {e}',
              file=sys.stderr,
              flush=True)
        returncode = 1
    sys.exit(returncode)


if __name__ == '__main__':
    main()
