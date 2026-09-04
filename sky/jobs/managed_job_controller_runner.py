"""Admission-gated Linux subreaper for one managed-job controller family.

This file is executed by path, rather than with ``python -m``, so no SkyPilot
module is imported before the runtime opens the family's admission gate.  A persistent
outer guardian and an inner warden are both Linux child subreapers.  If either
owner dies, the other adopts and drains the complete family, including a child
that double-forked or created a new session.
"""

import argparse
import ctypes
import dataclasses
import errno
import json
import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any

_PR_SET_CHILD_SUBREAPER = 36
_PR_GET_CHILD_SUBREAPER = 37
_PR_SET_DUMPABLE = 4
_PR_GET_DUMPABLE = 3
_POLL_SECONDS = 0.05
_STABLE_EMPTY_SCANS = 3
_MANAGER_READY_TIMEOUT_SECONDS = 30
_CONTROLLER_READY_FD_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_READY_FD')
_CONTROLLER_CAPABILITY_FD_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD')
_CONTROLLER_CAPABILITY_ENV_VAR = (
    'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY')
_CONTROLLER_CAPABILITY_AUTHORITY_PATH_ENV_VAR = (
    'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH')
_CAPABILITY_ENCODED_LENGTH = 43
# Keep this stdlib-only copy identical to
# ``controller_capability._TERMINAL_PROCESS_STATES``.  Importing that module
# would violate this runner's pre-admission ``python -S`` bootstrap boundary.
_TERMINAL_PROCESS_STATES = frozenset({'Z', 'X', 'x'})
_PIDFD_SYSCALLS = {
    # pidfd_send_signal, pidfd_open.  These syscall numbers are shared by the
    # Linux x86-64 and asm-generic (including aarch64) tables used by supported
    # SkyPilot API-server images.
    'x86_64': (424, 434),
    'aarch64': (424, 434),
}


class FamilyEnumerationError(RuntimeError):
    """The warden could not prove the state of its complete process family."""


@dataclasses.dataclass(frozen=True)
class ProcessIdentity:
    """Linux PID plus its non-reusable procfs birth identity."""

    pid: int
    start_time_ticks: int


def _enable_subreaper() -> None:
    if not sys.platform.startswith('linux'):
        raise OSError('managed-job controller families require Linux')
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    enabled = ctypes.c_int(0)
    if prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if prctl(_PR_GET_CHILD_SUBREAPER, ctypes.byref(enabled), 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if enabled.value != 1:
        raise OSError('kernel did not enable child-subreaper semantics')


def _make_process_non_dumpable() -> None:
    """Protect transient pipe authority from same-UID descendants."""
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if libc.prctl(_PR_GET_DUMPABLE, 0, 0, 0, 0) != 0:
        raise OSError('kernel did not disable managed-job warden dumps')


def _open_pidfd(pid: int) -> int:
    machine = os.uname().machine
    syscall_numbers = _PIDFD_SYSCALLS.get(machine)
    if syscall_numbers is None:
        raise OSError(errno.ENOTSUP,
                      f'pidfd is unsupported on architecture {machine}')
    _, pidfd_open_number = syscall_numbers
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(pidfd_open_number, pid, 0)
    if result < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    return int(result)


def _send_pidfd_signal(pidfd: int, signum: int) -> None:
    machine = os.uname().machine
    syscall_numbers = _PIDFD_SYSCALLS.get(machine)
    if syscall_numbers is None:
        raise OSError(errno.ENOTSUP,
                      f'pidfd is unsupported on architecture {machine}')
    pidfd_send_signal_number, _ = syscall_numbers
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(pidfd_send_signal_number, pidfd, signum, 0, 0)
    if result < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def _write_message(control: socket.socket, payload: dict[str, Any]) -> None:
    control.sendall(
        json.dumps(payload, separators=(',', ':')).encode('utf-8') + b'\n')


def _scrub_controller_capability_environment() -> None:
    """Remove every inheritable representation of controller authority."""
    for name in (_CONTROLLER_CAPABILITY_ENV_VAR,
                 _CONTROLLER_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
                 _CONTROLLER_CAPABILITY_FD_ENV_VAR):
        os.environ.pop(name, None)


def _read_capability_transport(file_descriptor: int) -> str:
    """Consume the supervisor's one-shot bounded pipe without logging it."""
    try:
        payload = bytearray()
        while len(payload) <= _CAPABILITY_ENCODED_LENGTH:
            chunk = os.read(file_descriptor,
                            _CAPABILITY_ENCODED_LENGTH + 1 - len(payload))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != _CAPABILITY_ENCODED_LENGTH:
            raise ValueError(
                'managed-job controller capability transport is invalid')
        try:
            return bytes(payload).decode('ascii')
        except UnicodeDecodeError as e:
            raise ValueError(
                'managed-job controller capability transport is invalid') from e
    finally:
        os.close(file_descriptor)


def _open_capability_transport(capability: str) -> int:
    """Relay authority through a fresh explicit-inheritance pipe."""
    if hasattr(os, 'pipe2'):
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
    else:
        read_fd, write_fd = os.pipe()
        os.set_inheritable(read_fd, False)
        os.set_inheritable(write_fd, False)
    keep_read_fd = False
    try:
        payload = capability.encode('ascii')
        offset = 0
        while offset < len(payload):
            written = os.write(write_fd, payload[offset:])
            if written <= 0:
                raise OSError('controller capability pipe made no progress')
            offset += written
        keep_read_fd = True
        return read_fd
    finally:
        os.close(write_fd)
        if not keep_read_fd:
            os.close(read_fd)


def _decode_command(command_json: str) -> list[str]:
    """Decode the supervisor-owned manager argv without invoking a shell."""
    command = json.loads(command_json)
    if (not isinstance(command, list) or not command or
            any(not isinstance(argument, str) or '\x00' in argument
                for argument in command)):
        raise ValueError('managed-job controller command is invalid')
    return command


def _read_message(control: socket.socket) -> dict[str, Any] | None:
    data = bytearray()
    while True:
        chunk = control.recv(1)
        if not chunk:
            return None
        if chunk == b'\n':
            break
        data.extend(chunk)
        if len(data) > 64 * 1024:
            raise ValueError('managed-job family control message is too large')
    value = json.loads(data.decode('utf-8'))
    if not isinstance(value, dict):
        raise ValueError('managed-job family control message must be an object')
    return value


def _process_identity_matches(pid: int, start_time_ticks: int) -> bool:
    try:
        return _read_process_start_time_ticks(pid) == start_time_ticks
    except (FileNotFoundError, ProcessLookupError, OSError, ValueError):
        return False


def _read_process_start_time_ticks(pid: int) -> int:
    with open(f'/proc/{pid}/stat', encoding='utf-8') as stream:
        content = stream.read()
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError(f'malformed process stat identity for PID {pid}')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ValueError(f'malformed process stat identity for PID {pid}')
    state = fields_after_comm[0]
    if len(state) != 1:
        raise ValueError(f'malformed process state for PID {pid}')
    if state in _TERMINAL_PROCESS_STATES:
        raise ProcessLookupError(f'process {pid} is no longer live')
    start_time_ticks = int(fields_after_comm[19])
    if start_time_ticks <= 0:
        raise ValueError(f'invalid process start identity for PID {pid}')
    return start_time_ticks


def _runtime_owner_identity_matches(pid: int, started_at_ticks: int) -> bool:
    try:
        return _read_process_start_time_ticks(pid) == started_at_ticks
    except (FileNotFoundError, OSError, ValueError):
        return False


def _direct_child_pids(pid: int) -> tuple[int, ...]:
    """Return every child published by every thread of one process."""
    task_directory = f'/proc/{pid}/task'
    try:
        task_names = tuple(os.listdir(task_directory))
    except FileNotFoundError:
        return ()
    except OSError as e:
        raise FamilyEnumerationError(
            f'could not enumerate tasks of PID {pid}: {e}') from e
    children: set[int] = set()
    for task_name in task_names:
        if not task_name.isdigit():
            continue
        try:
            with open(f'{task_directory}/{task_name}/children',
                      encoding='utf-8') as stream:
                raw = stream.read().strip()
        except FileNotFoundError:
            continue
        except OSError as e:
            raise FamilyEnumerationError(
                f'could not enumerate children of PID {pid} task '
                f'{task_name}: {e}') from e
        if not raw:
            continue
        try:
            children.update(int(value) for value in raw.split())
        except ValueError as e:
            raise FamilyEnumerationError(
                f'malformed child list for PID {pid} task {task_name}') from e
    return tuple(children)


def _descendants() -> dict[int, ProcessIdentity]:
    """Snapshot this subreaper's complete descendant family via procfs."""
    identities: dict[int, ProcessIdentity] = {}
    pending = list(_direct_child_pids(os.getpid()))
    while pending:
        pid = pending.pop()
        if pid in identities:
            continue
        try:
            identity = ProcessIdentity(pid, _read_process_start_time_ticks(pid))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, ValueError) as e:
            raise FamilyEnumerationError(
                f'could not identify controller descendant {pid}: {e}') from e
        identities[pid] = identity
        pending.extend(_direct_child_pids(pid))
    return identities


def _reap_adopted_children(command: subprocess.Popen[bytes] | None) -> None:
    """Reap direct adopted descendants without stealing the manager wait.

    ``Popen.poll()`` is the sole owner of the manager PID.  A generic
    ``waitpid(-1)`` can race a manager exit after ``poll()`` reports it alive,
    reap that PID itself, and make ``Popen`` lose the real exit status.  Enumerate
    the subreaper's direct children instead and exclude the still-live manager;
    every orphan adopted by this process becomes a direct child.
    """
    command_pid: int | None = None
    if command is not None and command.poll() is None:
        command_pid = command.pid
    for child_pid in _direct_child_pids(os.getpid()):
        if child_pid == command_pid:
            continue
        try:
            os.waitpid(child_pid, os.WNOHANG)
        except ChildProcessError:
            # The child exited and another owner already reaped it, or procfs
            # changed between enumeration and waitpid().
            continue
        except OSError as e:
            if e.errno in (errno.ECHILD, errno.ESRCH):
                continue
            raise FamilyEnumerationError(
                f'could not reap managed-job controller descendants: {e}'
            ) from e


def _kill_exact_process(process: ProcessIdentity) -> None:
    """SIGKILL the snapshotted process without a PID-reuse signal race."""
    try:
        pidfd = _open_pidfd(process.pid)
    except ProcessLookupError:
        return
    except OSError as e:
        raise FamilyEnumerationError(
            f'could not open pidfd for controller descendant {process.pid}: {e}'
        ) from e
    try:
        if not _process_identity_matches(process.pid, process.start_time_ticks):
            return
        _send_pidfd_signal(pidfd, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError as e:
        raise FamilyEnumerationError(
            f'could not SIGKILL controller descendant {process.pid}: {e}'
        ) from e
    finally:
        os.close(pidfd)


def _drain_family(command: subprocess.Popen[bytes] | None) -> None:
    """Freeze, repeatedly discover, kill, and reap to a stable empty point."""
    stable_empty_scans = 0
    while stable_empty_scans < _STABLE_EMPTY_SCANS:
        try:
            _reap_adopted_children(command)
            descendants = _descendants()
            if descendants:
                stable_empty_scans = 0
                # Kill parents before children.  Once the controller root is
                # dead, subreaper adoption makes every last-moment fork visible
                # in a later scan even if it called setsid().
                for process in descendants.values():
                    _kill_exact_process(process)
            else:
                stable_empty_scans += 1
        except FamilyEnumerationError as e:
            # Enumeration uncertainty is not absence.  Keep the warden alive
            # and retry so the outer runtime can retain leadership safely.
            stable_empty_scans = 0
            print(f'Managed-job controller family drain retry: {e}',
                  file=sys.stderr,
                  flush=True)
        time.sleep(_POLL_SECONDS)


def _completion_message(args: argparse.Namespace,
                        publisher: str) -> dict[str, Any]:
    return {
        'type': 'complete',
        'controller_instance_id': args.controller_instance_id,
        'controller_generation': args.controller_generation,
        'controller_slot_id': args.controller_slot_id,
        'controller_slot_attempt': args.controller_slot_attempt,
        'descendants_empty': True,
        'publisher': publisher,
    }


def _wait_for_manager_ready(command: subprocess.Popen[bytes], ready_fd: int,
                            control: socket.socket,
                            termination_requested: threading.Event,
                            guardian_pid: int,
                            guardian_start_time_ticks: int) -> None:
    """Wait for post-import ControllerManager readiness, not merely fork()."""
    deadline = time.monotonic() + _MANAGER_READY_TIMEOUT_SECONDS
    while True:
        if termination_requested.is_set():
            raise RuntimeError(
                'managed-job controller was terminated before readiness')
        _reap_adopted_children(command)
        if command.returncode is not None:
            raise RuntimeError('managed-job controller exited before readiness')
        if not _process_identity_matches(guardian_pid,
                                         guardian_start_time_ticks):
            raise RuntimeError(
                'managed-job guardian disappeared before manager readiness')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                'managed-job controller readiness handshake timed out')
        readable, _, _ = select.select([ready_fd, control], [], [],
                                       min(_POLL_SECONDS, remaining))
        if control in readable:
            message = _read_message(control)
            if message is None or message.get('type') == 'terminate':
                termination_requested.set()
                raise RuntimeError(
                    'managed-job controller was terminated before readiness')
        if ready_fd in readable:
            readiness = os.read(ready_fd, 2)
            if readiness != b'1':
                raise RuntimeError(
                    'managed-job controller returned invalid readiness proof')
            return


def _run_inner_warden(control: socket.socket, runtime_control: socket.socket,
                      args: argparse.Namespace, guardian_pid: int,
                      guardian_start_time_ticks: int) -> int:
    """Own controller effects and drain if the outer guardian disappears."""
    # This is the sole post-fork owner of the unread transport.  It inherits a
    # protected state from the guardian and reasserts it before any other call.
    _make_process_non_dumpable()
    termination_requested = threading.Event()

    def request_termination(_signum: int, _frame: Any) -> None:
        termination_requested.set()

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, request_termination)

    command: subprocess.Popen[bytes] | None = None
    manager_ready_read_fd: int | None = None
    manager_ready_write_fd: int | None = None
    manager_capability_read_fd: int | None = None
    supervisor_capability_fd: int | None = args.capability_fd
    guardian_available = True
    try:
        os.setsid()
        _enable_subreaper()
        _write_message(control, {'type': 'inner-ready'})
        admission = _read_message(control)
        if admission is None or admission.get('type') != 'admit':
            termination_requested.set()
        elif not termination_requested.is_set():
            if supervisor_capability_fd is None:
                raise RuntimeError(
                    'Controller capability transport was already consumed.')
            capability = _read_capability_transport(supervisor_capability_fd)
            supervisor_capability_fd = None
            manager_capability_read_fd = _open_capability_transport(capability)
            capability = ''
            manager_ready_read_fd, manager_ready_write_fd = os.pipe()
            command_env = dict(os.environ)
            for name in (_CONTROLLER_CAPABILITY_ENV_VAR,
                         _CONTROLLER_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
                         _CONTROLLER_CAPABILITY_FD_ENV_VAR):
                command_env.pop(name, None)
            command_env[_CONTROLLER_READY_FD_ENV_VAR] = str(
                manager_ready_write_fd)
            command_env[_CONTROLLER_CAPABILITY_FD_ENV_VAR] = str(
                manager_capability_read_fd)
            command = subprocess.Popen(  # pylint: disable=consider-using-with
                _decode_command(args.command),
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=command_env,
                pass_fds=(manager_ready_write_fd, manager_capability_read_fd),
                close_fds=True,
            )
            os.close(manager_capability_read_fd)
            manager_capability_read_fd = None
            os.close(manager_ready_write_fd)
            manager_ready_write_fd = None
            _wait_for_manager_ready(command, manager_ready_read_fd, control,
                                    termination_requested, guardian_pid,
                                    guardian_start_time_ticks)
            os.close(manager_ready_read_fd)
            manager_ready_read_fd = None
            _write_message(control, {
                'type': 'started',
                'controller_pid': command.pid,
            })
    # This process warden is synchronous and must report every pre-drain
    # failure; no asyncio cancellation can be delivered in this scope.
    except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
        try:
            _write_message(control, {
                'type': 'failed',
                'reason': f'{type(e).__name__}: {e}',
            })
        except OSError:
            pass
        termination_requested.set()
    finally:
        if supervisor_capability_fd is not None:
            os.close(supervisor_capability_fd)
        if manager_capability_read_fd is not None:
            os.close(manager_capability_read_fd)
        if manager_ready_read_fd is not None:
            os.close(manager_ready_read_fd)
        if manager_ready_write_fd is not None:
            os.close(manager_ready_write_fd)
    if command is not None:
        while not termination_requested.is_set():
            try:
                # This warden remains a subreaper for the manager's full
                # lifetime. Reap short-lived adopted descendants every
                # monitoring tick instead of retaining them as zombies until
                # the manager stops and the terminal family drain begins.
                _reap_adopted_children(command)
            except FamilyEnumerationError as e:
                # Procfs uncertainty is not a reason to kill a healthy
                # controller. Retry on the next bounded monitoring tick; the
                # terminal drain still requires stable proven absence.
                print(f'Managed-job controller live reap retry: {e}',
                      file=sys.stderr,
                      flush=True)
            if command.returncode is not None:
                break
            try:
                readable, _, _ = select.select([control], [], [], _POLL_SECONDS)
            except (OSError, ValueError):
                guardian_available = False
                termination_requested.set()
                break
            if readable:
                try:
                    message = _read_message(control)
                except (OSError, ValueError):
                    message = None
                if message is None:
                    guardian_available = False
                    termination_requested.set()
                elif message.get('type') == 'terminate':
                    termination_requested.set()
            if not _process_identity_matches(guardian_pid,
                                             guardian_start_time_ticks):
                guardian_available = False
                termination_requested.set()

    _drain_family(command)
    if guardian_available:
        try:
            _write_message(control, {'type': 'inner-drained'})
        except OSError:
            guardian_available = False
    if guardian_available:
        # Remain as the fallback subreaper/completion publisher until the outer
        # guardian durably publishes and acknowledges.  EOF means the outer
        # died in that gap, so this owner publishes before it exits.
        while True:
            try:
                message = _read_message(control)
            except (OSError, ValueError):
                message = None
            if message is None:
                guardian_available = False
                break
            if message.get('type') == 'completion-published':
                break
    control.close()
    if not guardian_available:
        try:
            _write_message(runtime_control,
                           _completion_message(args, 'inner-warden'))
        except OSError:
            pass
    runtime_control.close()
    return 0


def _run_outer_guardian(args: argparse.Namespace) -> int:
    """Gate effects, monitor runtime/inner liveness, and prove family absence."""
    # The ``-S`` runner has loaded only standard-library code.  Protect the raw
    # inherited pipe before topology setup, fork it once, then close this copy.
    _make_process_non_dumpable()
    scheduler_control = socket.socket(fileno=args.control_fd)
    scheduler_control.setblocking(True)
    termination_requested = threading.Event()

    def request_termination(_signum: int, _frame: Any) -> None:
        termination_requested.set()

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, request_termination)

    os.setsid()
    _enable_subreaper()
    guardian_pid = os.getpid()
    guardian_start_time_ticks = _read_process_start_time_ticks(guardian_pid)
    guardian_control, inner_control = socket.socketpair()
    try:
        inner_pid = os.fork()
    except BaseException:
        os.close(args.capability_fd)
        raise
    if inner_pid == 0:
        guardian_control.close()
        exit_code = 1
        try:
            exit_code = _run_inner_warden(inner_control, scheduler_control,
                                          args, guardian_pid,
                                          guardian_start_time_ticks)
        finally:
            os._exit(exit_code)  # pylint: disable=protected-access
    inner_control.close()
    os.close(args.capability_fd)

    admitted = False
    inner_drained = False
    try:
        inner_ready = _read_message(guardian_control)
        if inner_ready is None or inner_ready.get('type') != 'inner-ready':
            raise RuntimeError(
                'managed-job inner warden failed admission setup')
        _write_message(
            scheduler_control, {
                'type': 'ready',
                'pid': guardian_pid,
                'start_time_ticks': guardian_start_time_ticks,
                'controller_instance_id': args.controller_instance_id,
                'controller_generation': args.controller_generation,
                'controller_slot_id': args.controller_slot_id,
                'controller_slot_attempt': args.controller_slot_attempt,
            })
        admission = _read_message(scheduler_control)
        owner_alive = _runtime_owner_identity_matches(
            args.expected_owner_pid, args.expected_owner_start_time_ticks)
        if (admission is None or admission.get('type') != 'admit' or
                admission.get('controller_slot_attempt')
                != args.controller_slot_attempt or not owner_alive or
                termination_requested.is_set()):
            termination_requested.set()
        else:
            admitted = True
            _write_message(guardian_control, {'type': 'admit'})
            started = _read_message(guardian_control)
            if started is None or started.get('type') != 'started':
                raise RuntimeError('managed-job inner warden failed to start')
            _write_message(
                scheduler_control, {
                    'type': 'started',
                    'controller_slot_id': args.controller_slot_id,
                    'controller_slot_attempt': args.controller_slot_attempt,
                    'controller_pid': started.get('controller_pid'),
                })
    # This process guardian is synchronous and must fail closed for every
    # admission error; no asyncio cancellation can be delivered here.
    except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
        termination_requested.set()
        try:
            _write_message(
                scheduler_control, {
                    'type': 'failed',
                    'controller_slot_id': args.controller_slot_id,
                    'controller_slot_attempt': args.controller_slot_attempt,
                    'reason': f'{type(e).__name__}: {e}',
                })
        except OSError:
            pass

    terminate_sent = False
    while True:
        if not _runtime_owner_identity_matches(
                args.expected_owner_pid, args.expected_owner_start_time_ticks):
            termination_requested.set()
        if termination_requested.is_set() and not terminate_sent:
            try:
                _write_message(guardian_control, {'type': 'terminate'})
            except OSError:
                pass
            terminate_sent = True
        try:
            readable, _, _ = select.select(
                [guardian_control, scheduler_control], [], [], _POLL_SECONDS)
        except (OSError, ValueError):
            break
        if scheduler_control in readable:
            try:
                runtime_message = _read_message(scheduler_control)
            except (OSError, ValueError):
                runtime_message = None
            if (runtime_message is None or
                    runtime_message.get('type') == 'terminate'):
                termination_requested.set()
        if guardian_control in readable:
            try:
                message = _read_message(guardian_control)
            except (OSError, ValueError):
                message = None
            if message is None:
                break
            if message.get('type') == 'inner-drained':
                inner_drained = True
                break
        if not admitted and terminate_sent:
            # The inner warden will acknowledge after proving its empty family.
            continue

    # If the inner acknowledged its own stable-empty family, it remains alive as
    # the fallback publisher.  Prove the outer has no other descendants before
    # publishing.  If the inner hard-died, its family is now adopted here and
    # the same drain converges it.
    stable_empty_scans = 0
    while stable_empty_scans < _STABLE_EMPTY_SCANS:
        descendants = _descendants()
        unexpected = [
            child for child in descendants.values() if child.pid != inner_pid
        ]
        if unexpected:
            stable_empty_scans = 0
            for process in unexpected:
                _kill_exact_process(process)
        else:
            stable_empty_scans += 1
        time.sleep(_POLL_SECONDS)
    try:
        _write_message(scheduler_control,
                       _completion_message(args, 'outer-guardian'))
    except OSError:
        pass
    try:
        _write_message(guardian_control, {'type': 'completion-published'})
    except OSError:
        pass
    guardian_control.close()
    try:
        os.waitpid(inner_pid, 0)
    except ChildProcessError:
        pass
    _drain_family(None)
    scheduler_control.close()
    return 0 if inner_drained or not admitted else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('control_fd', type=int)
    parser.add_argument('capability_fd', type=int)
    parser.add_argument('expected_owner_pid', type=int)
    parser.add_argument('expected_owner_start_time_ticks', type=int)
    parser.add_argument('controller_instance_id')
    parser.add_argument('controller_generation')
    parser.add_argument('controller_slot_id', type=int)
    parser.add_argument('controller_slot_attempt')
    parser.add_argument('command')
    return parser


def main() -> int:
    if not sys.flags.no_site:
        raise RuntimeError(
            'Managed-job controller runner must run with Python -S.')
    _scrub_controller_capability_environment()
    args = _build_parser().parse_args()
    return _run_outer_guardian(args)


if __name__ == '__main__':
    raise SystemExit(main())
