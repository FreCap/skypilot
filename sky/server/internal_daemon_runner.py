"""Race-free child entrypoint for one controller runtime daemon.

This module must remain standard-library-only above ``_run_daemon``.  The
supervisor invokes this file directly so Python does not import
``sky.__init__`` before the parent-death contract is armed and revalidated.
Direct execution also leaves ``__main__.__spec__`` unset, which lets later
``multiprocessing.spawn`` children reconstruct this entrypoint by its absolute
path after the temporary bootstrap import path has been removed.
"""

import argparse
import ctypes
import importlib.util
import os
import pathlib
import select
import signal
import site
import socket
import sys
import threading
import time
import uuid

_PR_SET_PDEATHSIG = 1
_PR_SET_DUMPABLE = 4
_PR_GET_DUMPABLE = 3
_GUARDIAN_POLL_SECONDS = 0.05
_GUARDIAN_TERM_TIMEOUT_SECONDS = 10
_CONTROLLER_INSTANCE_ID_ENV_VAR = 'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID'
_CONTROLLER_GENERATION_ENV_VAR = 'SKYPILOT_SERVER_CONTROLLER_GENERATION'
_CAPABILITY_MODULE_NAME = 'sky.utils.controller_capability'


def _read_process_start_time_ticks(pid: int) -> int:
    """Read Linux procfs field 22 without importing SkyPilot."""
    if pid <= 0:
        raise ValueError('PID must be positive.')
    content = (pathlib.Path('/proc') / str(pid) /
               'stat').read_text(encoding='utf-8')
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError(f'Malformed process stat identity for PID {pid}.')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ValueError(f'Malformed process stat identity for PID {pid}.')
    start_time_ticks = int(fields_after_comm[19])
    if start_time_ticks <= 0:
        raise ValueError(f'Invalid process start identity for PID {pid}.')
    return start_time_ticks


def _arm_parent_death_contract(expected_parent_pid: int,
                               expected_parent_start_ticks: int) -> None:
    """Arm SIGKILL-on-parent-death, then close the fork/prctl race."""
    if not sys.platform.startswith('linux'):
        raise RuntimeError('Runtime daemons require Linux PR_SET_PDEATHSIG.')
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong,
        ctypes.c_ulong
    ]
    prctl.restype = ctypes.c_int
    if prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))

    # The parent may have died after fork but before prctl.  Re-read both
    # identities only after arming; PID equality alone is insufficient because
    # Linux can reuse a PID while this child is being admitted.
    if os.getppid() != expected_parent_pid:
        raise RuntimeError('Runtime daemon parent changed before admission.')
    try:
        observed_start_ticks = _read_process_start_time_ticks(
            expected_parent_pid)
    except (FileNotFoundError, OSError, ValueError) as e:
        raise RuntimeError(
            'Runtime daemon parent identity disappeared before admission.'
        ) from e
    if observed_start_ticks != expected_parent_start_ticks:
        raise RuntimeError('Runtime daemon parent was replaced before '
                           'admission.')


def _make_process_non_dumpable() -> None:
    """Protect the one-shot daemon capability before SkyPilot imports."""
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if libc.prctl(_PR_GET_DUMPABLE, 0, 0, 0, 0) != 0:
        raise OSError('Kernel did not disable runtime daemon dumps.')


def _process_identity_matches(pid: int, start_time_ticks: int) -> bool:
    """Return whether one exact Linux process identity still exists."""
    try:
        return _read_process_start_time_ticks(pid) == start_time_ticks
    except (FileNotFoundError, OSError, ValueError):
        return False


def _process_group_exists(process_group_id: int) -> bool:
    """Return whether any process remains in an exact process group."""
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Existence without signal permission is still existence.  The
        # guardian must keep ownership fail closed rather than infer absence.
        return True
    return True


def _signal_process_group(process_group_id: int, signum: int) -> None:
    try:
        os.killpg(process_group_id, signum)
    except ProcessLookupError:
        pass


def _drain_process_group(process_group_id: int) -> None:
    """Terminate the complete daemon group and wait for exact absence."""
    _signal_process_group(process_group_id, signal.SIGTERM)
    deadline = time.monotonic() + _GUARDIAN_TERM_TIMEOUT_SECONDS
    while (_process_group_exists(process_group_id) and
           time.monotonic() < deadline):
        time.sleep(_GUARDIAN_POLL_SECONDS)
    if _process_group_exists(process_group_id):
        _signal_process_group(process_group_id, signal.SIGKILL)
    while _process_group_exists(process_group_id):
        time.sleep(_GUARDIAN_POLL_SECONDS)


def _close_guardian_stdio() -> None:
    """Detach the observer from supervisor-owned log or test pipe handles."""
    null_fd = os.open(os.devnull, os.O_RDWR)
    try:
        for fd in (0, 1, 2):
            os.dup2(null_fd, fd)
    finally:
        if null_fd > 2:
            os.close(null_fd)


def _run_parent_death_group_guardian(
    control_fd: int,
    expected_parent_pid: int,
    expected_parent_start_ticks: int,
    daemon_pid: int,
    daemon_start_ticks: int,
) -> None:
    """Observe both owners from a separate session and drain on either death."""
    control = socket.socket(fileno=control_fd)
    try:
        # The guardian must stay outside the daemon group it may terminate.
        os.setsid()
        admitted = (_process_identity_matches(expected_parent_pid,
                                              expected_parent_start_ticks) and
                    _process_identity_matches(daemon_pid, daemon_start_ticks)
                    and _process_group_exists(daemon_pid))
        try:
            control.sendall(b'1' if admitted else b'0')
        except OSError:
            # The launcher may already have received PDEATHSIG.  The detached
            # guardian must still drain the group instead of treating the
            # broken admission pipe as a reason to abandon ownership.
            pass
        _close_guardian_stdio()
        if not admitted:
            _drain_process_group(daemon_pid)
            return
        while True:
            readable, _, _ = select.select([control], [], [],
                                           _GUARDIAN_POLL_SECONDS)
            if readable:
                try:
                    launcher_message = control.recv(1)
                except OSError:
                    launcher_message = b''
                if not launcher_message:
                    _drain_process_group(daemon_pid)
                    return
            parent_alive = _process_identity_matches(
                expected_parent_pid, expected_parent_start_ticks)
            daemon_alive = _process_identity_matches(daemon_pid,
                                                     daemon_start_ticks)
            group_alive = _process_group_exists(daemon_pid)
            if not group_alive:
                return
            if not parent_alive or not daemon_alive:
                _drain_process_group(daemon_pid)
                return
    finally:
        control.close()


def _close_guardian_inherited_descriptors(control_fd: int,
                                          capability_fd: int | None) -> None:
    """Leave the fork guardian only its control channel and standard I/O."""
    if capability_fd is not None:
        if capability_fd == control_fd:
            raise RuntimeError(
                'Guardian control and capability descriptors must differ.')
        # Close the raw transport first, even if unusual stdio allocation gave
        # it a descriptor below 3.  The guardian must never redeem or retain
        # controller authority.
        os.close(capability_fd)
    for raw_fd in os.listdir('/proc/self/fd'):
        try:
            file_descriptor = int(raw_fd)
        except ValueError:
            continue
        if file_descriptor <= 2 or file_descriptor == control_fd:
            continue
        try:
            os.close(file_descriptor)
        except OSError:
            # The directory descriptor used by listdir may already be gone.
            pass


def _monitor_group_guardian(control: socket.socket,
                            daemon_process_group: int) -> None:
    """Fail-stop the daemon group if its independent guardian disappears."""
    try:
        while control.recv(1):
            # No steady-state messages are currently defined.  Reserving bytes
            # keeps the channel extensible without treating one as EOF.
            pass
    except OSError:
        pass
    finally:
        control.close()
    _signal_process_group(daemon_process_group, signal.SIGKILL)


def _start_parent_death_group_guardian(expected_parent_pid: int,
                                       expected_parent_start_ticks: int,
                                       capability_fd: int | None = None) -> int:
    """Start the minimal complete-group parent-death fail-stop path."""
    daemon_pid = os.getpid()
    if os.getpgrp() != daemon_pid:
        raise RuntimeError('Runtime daemon launcher must lead its own process '
                           'group.')
    daemon_start_ticks = _read_process_start_time_ticks(daemon_pid)
    launcher_control, guardian_control = socket.socketpair()
    guardian_pid = os.fork()
    if guardian_pid == 0:
        launcher_control.close()
        control_fd = guardian_control.detach()
        try:
            _close_guardian_inherited_descriptors(control_fd, capability_fd)
            _make_process_non_dumpable()
            _run_parent_death_group_guardian(
                control_fd,
                expected_parent_pid,
                expected_parent_start_ticks,
                daemon_pid,
                daemon_start_ticks,
            )
        finally:
            os._exit(0)  # pylint: disable=protected-access

    guardian_control.close()
    try:
        admitted = launcher_control.recv(1)
    except OSError:
        admitted = b''
    if admitted != b'1':
        # No SkyPilot import or daemon effect is admitted unless the detached
        # guardian has independently observed both exact process identities.
        try:
            os.waitpid(guardian_pid, 0)
        except ChildProcessError:
            pass
        launcher_control.close()
        raise RuntimeError('Runtime daemon group guardian rejected admission.')
    monitor = threading.Thread(
        target=_monitor_group_guardian,
        args=(launcher_control, daemon_pid),
        name='runtime-daemon-group-guardian-monitor',
        daemon=True,
    )
    monitor.start()
    return guardian_pid


def _remove_bootstrap_import_path() -> None:
    """Stop the top-level bootstrap path from shadowing installed packages.

    The supervisor temporarily prepends ``sky/server`` so this file can run as
    a top-level module without importing ``sky`` before ``prctl``.  That
    directory also contains the ``requests`` package, however, and leaving it
    on ``sys.path`` would shadow the third-party HTTP library once SkyPilot is
    imported.  Remove only this bootstrap entry after admission and preserve
    every pre-existing PYTHONPATH entry.
    """
    runner_dir = pathlib.Path(__file__).resolve().parent
    sys.path[:] = [
        entry for entry in sys.path
        if pathlib.Path(entry or os.curdir).resolve() != runner_dir
    ]

    python_path = os.environ.get('PYTHONPATH')
    if python_path is None:
        return
    retained_entries = [
        entry for entry in python_path.split(os.pathsep)
        if pathlib.Path(entry or os.curdir).resolve() != runner_dir
    ]
    if retained_entries:
        os.environ['PYTHONPATH'] = os.pathsep.join(retained_entries)
    else:
        os.environ.pop('PYTHONPATH', None)


def _load_capability_primitives():
    """Load the stdlib-only authority registry without importing ``sky``."""
    module_path = (pathlib.Path(__file__).parents[1] / 'utils' /
                   'controller_capability.py')
    spec = importlib.util.spec_from_file_location(_CAPABILITY_MODULE_NAME,
                                                  module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Could not load controller capability primitives.')
    module = importlib.util.module_from_spec(spec)
    sys.modules[_CAPABILITY_MODULE_NAME] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(_CAPABILITY_MODULE_NAME, None)
        raise
    return module


def _run_daemon(daemon_id: str, max_db_connections: int,
                controller_instance_id: str,
                controller_generation: int) -> None:
    """Initialize executor-equivalent server context and run one daemon."""
    capability_primitives = sys.modules.get(_CAPABILITY_MODULE_NAME)
    if (capability_primitives is None or
            capability_primitives.get_process_local() is None):
        raise RuntimeError(
            'Runtime daemon controller capability was not installed before '
            'non-standard-library import.')
    try:
        canonical_instance_id = str(uuid.UUID(controller_instance_id))
    except ValueError as e:
        raise ValueError('Runtime daemon controller owner is invalid.') from e
    if (canonical_instance_id != controller_instance_id or
            controller_generation <= 0):
        raise ValueError('Runtime daemon controller owner is invalid.')
    # The owner pair is nonsecret, but still arrives as explicit supervisor
    # input instead of leaking through the neutral RequestWorker snapshot.
    # Install it before importing SkyPilot so every trusted daemon subsystem
    # observes one immutable outer authority.
    os.environ[_CONTROLLER_INSTANCE_ID_ENV_VAR] = canonical_instance_id
    os.environ[_CONTROLLER_GENERATION_ENV_VAR] = str(controller_generation)

    # Imports below are intentionally delayed until parent-death admission and
    # process-local capability installation have both been established.
    # pylint: disable=import-outside-toplevel
    import setproctitle
    setproctitle.setproctitle(
        f'SkyPilot:runtime-daemon:{daemon_id}:{os.getpid()}')

    from sky.utils import controller_capability
    if controller_capability.get_process_local() is None:
        raise RuntimeError(
            'Runtime daemon controller capability was not installed before '
            'SkyPilot import.')

    from sky.utils.db import db_utils
    db_utils.set_max_connections(max_db_connections)

    from sky import global_user_state
    from sky import models
    from sky.server import clean_env
    from sky.server import common as server_common
    from sky.server import daemons
    from sky.server import metrics
    from sky.server import plugins
    from sky.server import watchdog
    from sky.skylet import constants

    # pylint: enable=import-outside-toplevel

    daemon = daemons.get_runtime_daemon(daemon_id)
    watchdog.start_parent_death_watchdog()
    clean_env.restore_clean_server_env(dict(os.environ))
    plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.EXECUTOR))
    metrics.register_multiproc_cleanup_atexit()

    user = models.User(id=constants.SKYPILOT_SYSTEM_USER_ID,
                       name=constants.SKYPILOT_SYSTEM_USER_ID,
                       user_type=models.UserType.SYSTEM.value)
    _, user = global_user_state.add_or_update_user(user, return_user=True)
    server_common.reload_for_new_request(
        client_entrypoint=None,
        client_command=None,
        using_remote_api_server=False,
        user=user,
        request_id=daemon_id,
    )
    daemon.run_event()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('daemon_id')
    parser.add_argument('expected_parent_pid', type=int)
    parser.add_argument('expected_parent_start_ticks', type=int)
    parser.add_argument('max_db_connections', type=int)
    parser.add_argument('capability_fd', type=int)
    parser.add_argument('controller_instance_id')
    parser.add_argument('controller_generation', type=int)
    return parser


def main() -> None:
    if not sys.flags.no_site:
        raise RuntimeError('Runtime daemon bootstrap must run with Python -S.')
    args = _build_parser().parse_args()
    if args.max_db_connections < 0:
        raise ValueError('max_db_connections must be non-negative.')
    _arm_parent_death_contract(args.expected_parent_pid,
                               args.expected_parent_start_ticks)
    _make_process_non_dumpable()
    _start_parent_death_group_guardian(args.expected_parent_pid,
                                       args.expected_parent_start_ticks,
                                       args.capability_fd)
    capability_primitives = _load_capability_primitives()
    capability_primitives.install_process_local_from_fd(args.capability_fd)
    _remove_bootstrap_import_path()

    # Populate site-packages and run sitecustomize only after non-dumpability
    # and process-local authority are proven.  ``-S`` prevents either from
    # running during interpreter startup while the raw pipe is still readable.
    site.main()
    _run_daemon(args.daemon_id, args.max_db_connections,
                args.controller_instance_id, args.controller_generation)


if __name__ == '__main__':
    main()
