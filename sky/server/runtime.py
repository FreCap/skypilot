"""Process supervisors for explicit SkyPilot API server roles."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from collections.abc import Coroutine
import dataclasses
import functools
import multiprocessing
import os
import pathlib
import shutil
import signal
import sys
import threading
import time
from typing import Any
import uuid

import psutil
import uvloop

from sky import check as sky_check
from sky import estimated_spend as estimated_spend_lib
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.jobs import controller_fencing as managed_job_controller_fencing
from sky.jobs import controller_slots
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils
from sky.serve import ordinary_launch_handoff
from sky.serve import serve_state
from sky.server import clean_env as clean_env_module
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import daemons
from sky.server import database_migrations
from sky.server import executor_termination_observer
from sky.server import file_mount_uploads
from sky.server import metrics
from sky.server import plugins
from sky.server.blob import blob_storage as bs
from sky.server.events import store as operational_event_store
from sky.server.requests import cutover as request_cutover
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import registry as request_registry
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage
from sky.skylet import constants
from sky.usage import usage_lib
from sky.users import permission
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import controller_capability
from sky.utils import controller_utils
from sky.utils import subprocess_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

_SERVER_USER_HASH_KEY = 'server_user_hash'
_ROLE_CHOICES = ('all', 'api', 'executor', 'controller')
_SINGLETON_PREFIX = 'skypilot:api-server-runtime:v1'
_CONTROLLER_LEADERSHIP_POLL_SECONDS = 2
_CONTROLLER_LEADERSHIP_PROBE_SECONDS = 2
_METRICS_STARTUP_TIMEOUT_SECONDS = 30
_METRICS_STARTUP_POLL_SECONDS = 0.01
_CONTROLLER_CUTOVER_QUIESCENCE_ENV_VAR = (
    'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS')
_RUNTIME_DAEMON_RESTART_INITIAL_SECONDS = 1
_RUNTIME_DAEMON_RESTART_MAX_SECONDS = 30
_RUNTIME_DAEMON_TERM_TIMEOUT_SECONDS = 10
_RUNTIME_DAEMON_GROUP_POLL_SECONDS = 0.05
_BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS = 30
_OWNERSHIP_SHUTDOWN_RETRY_SECONDS = 1
_DRAIN_MARKER_POLL_SECONDS = 0.1
_DRAIN_PUBLICATION_WAIT_SECONDS = 1
_MANAGED_JOB_RUNTIME_OWNER_PID_ENV_VAR = (
    'SKYPILOT_MANAGED_JOB_RUNTIME_OWNER_PID')
_MANAGED_JOB_RUNTIME_OWNER_START_TICKS_ENV_VAR = (
    'SKYPILOT_MANAGED_JOB_RUNTIME_OWNER_START_TICKS')


class RuntimeOwnershipShutdownError(RuntimeError):
    """Owned execution effects did not quiesce before role handoff."""


class _RoleDrainMarkerMonitor:
    """Turn the Kubernetes drain marker into an immediate runtime event."""

    def __init__(
        self,
        lease: request_postgres.ServerInstanceLease,
        request_shutdown: Callable[[], bool | None],
        *,
        marker_path: str = request_storage.ROLE_DRAIN_MARKER_PATH,
        poll_seconds: float = _DRAIN_MARKER_POLL_SECONDS,
        publication_wait_seconds: float = _DRAIN_PUBLICATION_WAIT_SECONDS,
    ) -> None:
        self._lease = lease
        self._request_shutdown = request_shutdown
        self._marker_path = marker_path
        self._poll_seconds = poll_seconds
        self._publication_wait_seconds = publication_wait_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run,
                                        name='server-role-drain-marker',
                                        daemon=True)

    def _run(self) -> None:
        while not self._stop_event.wait(self._poll_seconds):
            if not os.path.exists(self._marker_path):
                continue

            def publish_drain() -> None:
                try:
                    self._lease.begin_draining()
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        'Failed to publish early role drain state.')

            publisher = threading.Thread(target=publish_drain,
                                         name='server-role-drain-publisher',
                                         daemon=True)
            publisher.start()
            publisher.join(timeout=self._publication_wait_seconds)
            if publisher.is_alive():
                # The marker already fences readiness and every dispatcher.
                # Never let a hung database publication consume the real Pod
                # execution budget before signal-driven child convergence.
                logger.warning('Early role drain publication exceeded its '
                               'bounded wait; proceeding with shutdown.')
            while not self._stop_event.is_set():
                if self._request_shutdown() is not False:
                    return
                # The role-specific signal handler may not be installed yet
                # during startup. Keep the marker fence active and retry;
                # never deliver SIGTERM to Python's default handler.
                self._stop_event.wait(self._poll_seconds)
            return

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(1, self._poll_seconds * 2))


def _open_capability_transport(capability: str) -> int:
    """Return one CLOEXEC pipe carrying canonical controller authority."""
    controller_capability.digest(capability)
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
                raise OSError('Controller capability pipe made no progress.')
            offset += written
        keep_read_fd = True
        return read_fd
    finally:
        os.close(write_fd)
        if not keep_read_fd:
            os.close(read_fd)


def _publish_managed_job_runtime_owner_identity() -> None:
    pid = os.getpid()
    start_ticks = _executor_process_start_time_ticks(pid)
    os.environ[_MANAGED_JOB_RUNTIME_OWNER_PID_ENV_VAR] = str(pid)
    os.environ[_MANAGED_JOB_RUNTIME_OWNER_START_TICKS_ENV_VAR] = str(
        start_ticks)


def _clear_managed_job_runtime_owner_identity() -> None:
    os.environ.pop(_MANAGED_JOB_RUNTIME_OWNER_PID_ENV_VAR, None)
    os.environ.pop(_MANAGED_JOB_RUNTIME_OWNER_START_TICKS_ENV_VAR, None)


def _publish_controller_origin_capability(
    capability: str,
    *,
    authority_path: str | None = None,
) -> None:
    """Install runtime authority without any inheritable environment copy."""
    # Lock down /proc before the raw bearer enters the process-local registry.
    controller_capability.make_process_non_dumpable()
    controller_capability.digest(capability)
    existing = controller_capability.get_process_local()
    if existing is not None and existing != capability:
        raise RuntimeError(
            'Another controller-origin capability is already published.')
    # Reject environment transport as a concept, including stale values from
    # an old rolling-upgrade process.  The authority path is derivable from
    # the authenticated instance UUID and contains only a hash.
    del authority_path
    os.environ.pop(server_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR, None)
    os.environ.pop(
        server_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
        None)
    controller_capability.install_process_local(capability)


def _clear_controller_origin_capability(capability: str | None) -> None:
    """Clear only the exact capability published by this runtime owner."""
    owns_publication = (capability is not None and
                        controller_capability.get_process_local() == capability)
    if not owns_publication:
        return
    controller_capability.clear_process_local()
    os.environ.pop(server_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR, None)
    os.environ.pop(
        server_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
        None)


def _request_runtime_shutdown() -> None:
    """Wake the role's ordinary signal-driven graceful shutdown path."""
    os.kill(os.getpid(), signal.SIGTERM)


def _request_runtime_shutdown_when_ready() -> bool:
    """Request shutdown only after the role owns SIGTERM handling."""
    if signal.getsignal(signal.SIGTERM) in (signal.SIG_DFL, signal.SIG_IGN):
        return False
    _request_runtime_shutdown()
    return True


async def _monitor_compat_controller_leadership(
        background: _BackgroundLoop,
        lease: request_postgres.ControllerLeaderLease) -> None:
    """Fence compatibility all-mode promptly if its outer session is lost."""
    while not background.is_stopping:
        await asyncio.sleep(_CONTROLLER_LEADERSHIP_PROBE_SECONDS)
        if background.is_stopping:
            return
        try:
            current = await asyncio.to_thread(lease.heartbeat)
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                'Compatibility managed-job leadership probe failed.')
            current = False
        if not current:
            logger.error('Compatibility managed-job leadership was lost; '
                         'fencing local work and shutting down.')
            _request_runtime_shutdown()
            return


@dataclasses.dataclass
class RuntimeState:
    """Initialized state shared by role supervisors."""

    role: str
    config: server_config.ServerConfig
    instance_lease: request_postgres.ServerInstanceLease | None
    requests_recovered: bool


def init_or_restore_server_user_hash() -> None:
    """Restore the stable API-server identity from central state."""

    def apply_user_hash(user_hash: str) -> None:
        common_utils.set_user_hash_locally(user_hash)
        common_lib.refresh_server_id()

    user_hash = global_user_state.get_system_config(_SERVER_USER_HASH_KEY)
    if user_hash is None:
        user_hash = global_user_state.get_or_set_system_config(
            _SERVER_USER_HASH_KEY, common_utils.get_user_hash())
    apply_user_hash(user_hash)


def _uses_postgres_requests() -> bool:
    return (os.environ.get(request_postgres.REQUEST_BACKEND_ENV_VAR) ==
            request_postgres.POSTGRES_REQUEST_BACKEND)


def _guard_completed_request_store_cutover() -> None:
    """Fail closed if a one-way cutover resolves back to stale storage."""
    backend = request_storage.get_request_backend()
    request_cutover.require_completed_cutover_backend(
        postgres_configured=_uses_postgres_requests(),
        postgres_backend=type(backend)
        is request_postgres.PostgresRequestBackend,
        sqlite_backend=type(backend) is requests_lib.SqliteRequestBackend)


def _guard_active_reserved_fill_protocol(role: str) -> None:
    """Require the prepared backend invariant after protocol-v2 activation."""
    if role not in ('all', 'api', 'controller', 'executor'):
        return
    state = serve_state.get_reserved_fill_protocol_state()
    if int(state['protocol_version']) != 2:
        return
    if not request_postgres.execution_quiescence_backend_guard_enabled():
        raise RuntimeError(
            'Reserved-fill protocol v2 is active, but execution-quiescence '
            'backend enforcement is disabled. Demote protocol v2 before '
            'disabling the guard.')
    request_postgres.require_builtin_execution_quiescence_backends(
        required=True)


def _controller_cutover_quiescence_seconds() -> float:
    value = os.environ.get(_CONTROLLER_CUTOVER_QUIESCENCE_ENV_VAR, '70')
    try:
        seconds = float(value)
    except ValueError as e:
        raise ValueError(
            f'{_CONTROLLER_CUTOVER_QUIESCENCE_ENV_VAR} must be numeric.') from e
    if seconds < 0:
        raise ValueError(
            f'{_CONTROLLER_CUTOVER_QUIESCENCE_ENV_VAR} must be non-negative.')
    return seconds


def _start_surface_interrupted_cluster_launches() -> None:
    try:
        scan_delay = float(
            os.environ.get(constants.EXECUTION_DRAIN_SECONDS_ENV_VAR, '60'))
    except ValueError:
        scan_delay = 60
    threading.Thread(target=requests_lib.surface_interrupted_cluster_launches,
                     args=(scan_delay,),
                     name='surface-interrupted-launches',
                     daemon=True).start()


def initialize_common_runtime(role: str, deploy: bool) -> RuntimeState:
    """Load plugins, verify schemas, identity, policy, and role resources."""
    logger.info(f'Initializing SkyPilot {role} role')
    plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))
    _guard_completed_request_store_cutover()
    usage_lib.maybe_show_privacy_policy()

    db_utils.set_max_connections(1)
    logger.info('Initializing database engines')
    database_migrations.initialize_central_databases()
    _guard_active_reserved_fill_protocol(role)
    logger.info('Database engines initialized')

    requests_recovered = False
    if role == 'all':
        request_backend = request_storage.get_request_backend()
        if not isinstance(request_backend,
                          request_postgres.PostgresRequestBackend):
            requests_recovered = requests_lib.recover_db_and_logs()
            _start_surface_interrupted_cluster_launches()

    logger.info('Initializing server user hash')
    init_or_restore_server_user_hash()
    if role in ('all', 'controller'):
        managed_job_utils.setup_consolidation_mode_on_startup(deploy)

    logger.info('Pre-loading plugin RBAC rules + viewer allowlist')
    plugins.load_plugin_rbac_rules()
    plugins.load_plugin_viewer_allowlist()
    logger.info('Initializing permission service')
    permission.permission_service.initialize()
    logger.info('Permission service initialized')

    max_db_connections = global_user_state.get_max_db_connections()
    logger.info(f'Max db connections: {max_db_connections}')
    reserved_memory_mb: float = 0
    if role in ('all', 'controller'):
        reserved_memory_mb = (
            controller_utils.compute_memory_reserved_for_controllers(
                reserve_for_controllers=os.environ.get(
                    constants.OVERRIDE_CONSOLIDATION_MODE) is not None,
                reserve_extra_for_pool=not os.environ.get(
                    constants.IS_SKYPILOT_SERVE_CONTROLLER)))
    config = server_config.compute_server_config(
        deploy, max_db_connections, reserved_memory_mb=reserved_memory_mb)
    if role in ('all', 'controller'):
        server_config.publish_serve_launch_parallelism(config)

    instance_lease = None
    if _uses_postgres_requests():
        instance_lease = request_postgres.ServerInstanceLease(role)
        instance_lease.start()
    return RuntimeState(role, config, instance_lease, requests_recovered)


async def _schedule_on_boot_check_async() -> None:
    try:
        await executor.schedule_request_async(
            request_id=server_constants.ON_BOOT_CHECK_REQUEST_ID,
            request_name=request_names.RequestName.CHECK,
            request_body=payloads.CheckBody(),
            func=sky_check.check,
            schedule_type=requests_lib.ScheduleType.SHORT,
            is_skypilot_system=True,
        )
    except exceptions.RequestAlreadyExistsError:
        logger.debug(f'Request {server_constants.ON_BOOT_CHECK_REQUEST_ID} '
                     'already exists.')


async def _initialize_normal_executor_requests() -> None:
    """Submit non-controller startup work after normal consumers exist."""
    await _schedule_on_boot_check_async()


def _cleanup_download_tmp_once() -> None:
    tmp_dir = bs.get_blob_storage().download_tmp_base_dir()
    if tmp_dir is None or not os.path.exists(tmp_dir):
        return
    cutoff = time.time() - bs.GC_GRACE_SECONDS
    with os.scandir(tmp_dir) as user_entries:
        for user_entry in user_entries:
            if not user_entry.is_dir():
                continue
            with os.scandir(user_entry.path) as entries:
                for entry in entries:
                    if not entry.is_dir():
                        continue
                    try:
                        if entry.stat().st_mtime < cutoff:
                            shutil.rmtree(entry.path, ignore_errors=True)
                    except OSError:
                        pass


async def _cleanup_download_tmp() -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            await asyncio.to_thread(_cleanup_download_tmp_once)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error in cleanup_download_tmp: {e}')


class _BackgroundLoop:
    """One owned asyncio loop with deterministic cross-thread shutdown."""

    def __init__(self) -> None:
        self.loop = uvloop.new_event_loop()
        self._tasks: list[asyncio.Task] = []
        self._graceful_shutdown_hooks: list[Callable[[], Coroutine[Any, Any,
                                                                   Any]]] = []
        self._stopping = threading.Event()
        self._stopped = threading.Event()
        self._stop_lock = threading.Lock()
        self._started = False
        self._graceful_hooks_completed = False
        self._thread = threading.Thread(target=self._run,
                                        name='server-background-loop',
                                        daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def create_task(self, coroutine: Coroutine[Any, Any,
                                               Any]) -> asyncio.Task[Any]:
        task = self.loop.create_task(coroutine)
        self._tasks.append(task)
        return task

    @property
    def is_stopping(self) -> bool:
        return self._stopping.is_set()

    def add_graceful_shutdown_hook(
            self, hook: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        self._graceful_shutdown_hooks.append(hook)

    def start(self) -> None:
        try:
            self._thread.start()
        except BaseException:
            # Thread.start() is transactional: a failed start owns no effects,
            # and stop() can close the never-run loop deterministically.
            self._started = False
            raise
        self._started = True

    def run(self,
            coroutine: Coroutine[Any, Any, Any],
            *,
            timeout: float = 60) -> Any:
        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        return future.result(timeout=timeout)

    def stop(self) -> None:
        """Join all work, or raise without claiming shutdown completed."""
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self._stopping.set()
            if not self._thread.is_alive():
                if self._started:
                    failure = RuntimeOwnershipShutdownError(
                        'Background event-loop thread exited before owned work '
                        'was joined.')
                    raise failure
                if not self.loop.is_closed():
                    self.loop.close()
                self._stopped.set()
                return

            failures: list[tuple[str, BaseException]] = []
            incomplete = False
            if not self._graceful_hooks_completed:
                hook_failures = False
                for hook in self._graceful_shutdown_hooks:
                    future = asyncio.run_coroutine_threadsafe(hook(), self.loop)
                    try:
                        future.result(
                            timeout=_BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS)
                    except Exception as e:  # pylint: disable=broad-except
                        failures.append(('graceful shutdown hook', e))
                        hook_failures = True
                        if isinstance(e, TimeoutError):
                            # The hook still owns work on this loop.  Keep the
                            # loop alive for a later authoritative retry.
                            incomplete = True
                if not hook_failures:
                    self._graceful_hooks_completed = True

            async def cancel_tasks() -> list[Any]:
                tasks = [task for task in self._tasks if not task.done()]
                for task in tasks:
                    task.cancel()
                return await asyncio.gather(*tasks, return_exceptions=True)

            future = asyncio.run_coroutine_threadsafe(cancel_tasks(), self.loop)
            try:
                results = future.result(
                    timeout=_BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS)
                for result in results:
                    if (isinstance(result, BaseException) and
                            not isinstance(result, asyncio.CancelledError)):
                        failures.append(('background task', result))
            except Exception as e:  # pylint: disable=broad-except
                failures.append(('background task join', e))
                incomplete = True

            if incomplete or failures:
                labels = ', '.join(label for label, _ in failures)
                failure = RuntimeOwnershipShutdownError(
                    'Background ownership did not quiesce: '
                    f'{labels}.')
                raise failure from failures[0][1]

            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout=_BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS)
            if self._thread.is_alive():
                failure = RuntimeOwnershipShutdownError(
                    'Background event-loop thread did not stop.')
                raise failure
            self.loop.close()
            self._stopped.set()


def _singleton_task(
    name: str,
    task_factory,
) -> Coroutine[Any, Any, None]:
    if _uses_postgres_requests():
        return request_postgres.run_distributed_singleton(
            f'{_SINGLETON_PREFIX}:{name}', task_factory)
    return task_factory()


def _runtime_daemon_process_group_exists(process_group_id: int) -> bool:
    """Return whether an exact runtime-daemon process group still exists."""
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Existence without signal permission is still existence and must fail
        # closed during leadership handoff.
        return True
    return True


async def _wait_runtime_daemon_process_group_gone(
        process_group_id: int) -> None:
    """Wait until Linux reports that no process remains in the exact group."""
    # Linux exposes no completion event for an arbitrary process group. The
    # bounded supervisor must poll until even adopted descendants are gone.
    while _runtime_daemon_process_group_exists(  # noqa: ASYNC110
            process_group_id):
        await asyncio.sleep(_RUNTIME_DAEMON_GROUP_POLL_SECONDS)


async def _terminate_runtime_daemon_process(
        process: asyncio.subprocess.Process) -> None:
    """TERM, KILL if needed, reap the leader, and drain its process group."""
    process_group_id = process.pid
    if _runtime_daemon_process_group_exists(process_group_id):
        try:
            os.killpg(process_group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass

    async def leader_and_group_stopped() -> None:
        await process.wait()
        await _wait_runtime_daemon_process_group_gone(process_group_id)

    try:
        await asyncio.wait_for(leader_and_group_stopped(),
                               timeout=_RUNTIME_DAEMON_TERM_TIMEOUT_SECONDS)
        return
    except asyncio.TimeoutError:
        logger.warning('Runtime daemon process group '
                       f'{process_group_id} did not stop after SIGTERM; '
                       'sending SIGKILL.')
    if _runtime_daemon_process_group_exists(process_group_id):
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
    await process.wait()
    await _wait_runtime_daemon_process_group_gone(process_group_id)


def _prepare_runtime_daemon_paths(
        daemon_id: str) -> tuple[pathlib.Path, pathlib.Path]:
    """Resolve and create filesystem paths used by a runtime daemon."""
    runner_dir = pathlib.Path(__file__).resolve().parent
    log_dir = pathlib.Path(
        server_constants.REQUEST_LOG_PATH_PREFIX).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    return runner_dir, log_dir / f'{daemon_id}.log'


async def _supervise_runtime_daemon(daemon_id: str, clean_env: dict[str, str],
                                    max_db_connections: int, parent_pid: int,
                                    parent_start_time_ticks: int,
                                    origin_capability: str,
                                    controller_owner: tuple[str, int]) -> None:
    """Supervise one blocking maintenance loop in its own process group."""
    runner_dir, log_path = await asyncio.to_thread(
        _prepare_runtime_daemon_paths, daemon_id)
    child_env = dict(clean_env)
    python_path = child_env.get('PYTHONPATH')
    child_env['PYTHONPATH'] = (str(runner_dir) if not python_path else
                               f'{runner_dir}{os.pathsep}{python_path}')
    restart_delay = _RUNTIME_DAEMON_RESTART_INITIAL_SECONDS
    while True:
        process: asyncio.subprocess.Process | None = None
        capability_fd: int | None = None
        try:
            capability_fd = _open_capability_transport(origin_capability)
            with log_path.open('a', encoding='utf-8') as daemon_log:
                spawn = asyncio.create_task(
                    asyncio.create_subprocess_exec(
                        sys.executable,
                        '-S',
                        '-m',
                        'internal_daemon_runner',
                        daemon_id,
                        str(parent_pid),
                        str(parent_start_time_ticks),
                        str(max_db_connections),
                        str(capability_fd),
                        controller_owner[0],
                        str(controller_owner[1]),
                        env=child_env,
                        stdout=daemon_log,
                        stderr=daemon_log,
                        start_new_session=True,
                        pass_fds=(capability_fd,),
                    ))
                try:
                    process = await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    # Creation may have crossed fork/exec before cancellation
                    # was delivered. Recover the exact process handle so the
                    # outer cancellation path can terminate and reap it.
                    process = await spawn
                    raise
                os.close(capability_fd)
                capability_fd = None
                return_code = await process.wait()
            # The entrypoint is perpetual.  Any return, including zero, is
            # unexpected and must converge through the same restart path.
            await _terminate_runtime_daemon_process(process)
            logger.error(f'Runtime daemon {daemon_id} exited unexpectedly '
                         f'with code {return_code}; restarting in '
                         f'{restart_delay} seconds.')
            await asyncio.sleep(restart_delay)
            restart_delay = min(_RUNTIME_DAEMON_RESTART_MAX_SECONDS,
                                restart_delay * 2)
        except asyncio.CancelledError:
            if capability_fd is not None:
                os.close(capability_fd)
            if process is not None:
                await _terminate_runtime_daemon_process(process)
            raise
        except Exception as e:  # pylint: disable=broad-except
            if capability_fd is not None:
                os.close(capability_fd)
            if process is not None:
                await _terminate_runtime_daemon_process(process)
            logger.exception(f'Runtime daemon {daemon_id} supervisor failed: '
                             f'{e}; restarting in {restart_delay} seconds.')
            await asyncio.sleep(restart_delay)
            restart_delay = min(_RUNTIME_DAEMON_RESTART_MAX_SECONDS,
                                restart_delay * 2)


async def _register_runtime_daemons_async(
        background: _BackgroundLoop,
        max_db_connections: int,
        controller_owner: tuple[str, int],
        pod_identity: request_postgres.ServerPodIdentity,
        *,
        observe_executor_termination: bool = False) -> tuple[str, ...]:
    """Select and register runtime daemons once for this leadership term."""
    clean_env = clean_env_module.get_clean_server_env()
    if clean_env is None:
        raise RuntimeError('Clean server environment must be captured before '
                           'runtime daemon startup.')
    parent_pid = os.getpid()
    parent_start_time_ticks = _executor_process_start_time_ticks(parent_pid)
    origin_capability = controller_capability.get_process_local()
    if origin_capability is None:
        raise RuntimeError(
            'Runtime daemon startup requires controller capability authority.')
    selected: list[str] = []
    if observe_executor_termination:
        termination_observer = executor_termination_observer.start(
            controller_owner, pod_identity)

        async def stop_termination_observer() -> None:
            await asyncio.to_thread(termination_observer.stop)

        background.add_graceful_shutdown_hook(stop_termination_observer)
    for daemon in daemons.RUNTIME_DAEMONS:
        if daemon.should_skip():
            continue
        selected.append(daemon.id)
        task_factory = functools.partial(
            _supervise_runtime_daemon,
            daemon.id,
            clean_env,
            max_db_connections,
            parent_pid,
            parent_start_time_ticks,
            origin_capability,
            controller_owner,
        )
        background.create_task(
            _singleton_task(f'internal-daemon:{daemon.id}', task_factory))
    return tuple(selected)


def _executor_process_start_time_ticks(pid: int) -> int:
    """Read current procfs birth identity for one executor PID."""
    return request_storage.read_linux_process_start_time_ticks(pid)


def _metrics_enabled() -> bool:
    return (os.environ.get(constants.ENV_VAR_SERVER_METRICS_ENABLED,
                           'false').lower() == 'true')


async def _serve_metrics_server(metrics_server: Any) -> None:
    """Keep uvicorn BaseExceptions contained in the background loop task."""
    try:
        await metrics_server.serve()
    except asyncio.CancelledError:
        raise
    except BaseException as e:
        # uvicorn raises SystemExit when its listener cannot bind. Let the
        # foreground startup barrier surface that as an ordinary role startup
        # failure instead of silently terminating only the background thread.
        raise RuntimeError('The metrics server failed.') from e


def _metrics_task_failure(task: asyncio.Task[Any]) -> BaseException:
    if task.cancelled():
        return RuntimeError(
            'The metrics server task was cancelled unexpectedly.')
    failure = task.exception()
    if failure is not None:
        return failure
    return RuntimeError('The metrics server stopped unexpectedly.')


def _start_metrics_background_loop(role: str, host: str,
                                   metrics_port: int) -> _BackgroundLoop | None:
    """Serve metrics owned by one API-server role pod."""
    if role not in ('all', 'api', 'executor', 'controller'):
        return None
    if not _metrics_enabled():
        return None
    if (role in ('executor', 'controller') and
            not os.environ.get('PROMETHEUS_MULTIPROC_DIR')):
        raise RuntimeError(
            f'The {role} metrics server requires '
            'PROMETHEUS_MULTIPROC_DIR so child-process metrics are visible.')

    background = _BackgroundLoop()
    # Initialize the optional managed-jobs shared-state collector only in its
    # historical API/all owner. metrics.metrics() applies the same role gate
    # to all built-in and plugin custom collectors. Process-local metrics,
    # including controller children, still come from every role's pod-local
    # multiprocess registry.
    if role in ('all', 'api'):
        metrics.maybe_register_managed_jobs_collector()
    metrics_server = metrics.build_metrics_server(host, metrics_port)
    serve_task = background.create_task(_serve_metrics_server(metrics_server))
    background.create_task(metrics.multiproc_reaper_daemon())

    async def stop_metrics_server() -> None:
        metrics_server.should_exit = True
        await asyncio.gather(serve_task, return_exceptions=True)

    background.add_graceful_shutdown_hook(stop_metrics_server)
    startup_complete = threading.Event()
    server_failed = threading.Event()
    failure_holder: list[BaseException] = []

    def metrics_server_done(task: asyncio.Task[Any]) -> None:
        failure = _metrics_task_failure(task)
        failure_holder.append(failure)
        server_failed.set()
        if not startup_complete.is_set() or background.is_stopping:
            return
        logger.error(
            'Metrics server stopped after startup; terminating the role.',
            exc_info=(type(failure), failure, failure.__traceback__))
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except OSError:
            logger.exception('Failed to terminate the role after metrics '
                             'server failure.')

    serve_task.add_done_callback(metrics_server_done)
    try:
        background.start()
        deadline = time.monotonic() + _METRICS_STARTUP_TIMEOUT_SECONDS
        while not metrics_server.started:
            if server_failed.wait(_METRICS_STARTUP_POLL_SECONDS):
                failure = failure_holder[0]
                raise RuntimeError(
                    f'The {role} metrics server failed to become available '
                    f'on {host}:{metrics_port}.') from failure
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f'Timed out waiting for the {role} metrics server on '
                    f'{host}:{metrics_port}.')
        startup_complete.set()
        if server_failed.is_set():
            failure = failure_holder[0]
            raise RuntimeError(
                f'The {role} metrics server failed to remain available on '
                f'{host}:{metrics_port}.') from failure
    except Exception:
        background.stop()
        raise
    return background


def _start_background_loop(
    role: str,
    *,
    compatibility_controller_lease: request_postgres.ControllerLeaderLease |
    None = None,
) -> _BackgroundLoop:
    background = _BackgroundLoop()
    if compatibility_controller_lease is not None:
        if role != 'all':
            raise ValueError('Only compatibility all-mode can register its '
                             'controller leadership monitor.')
        background.create_task(
            _monitor_compat_controller_leadership(
                background, compatibility_controller_lease))
    if role in ('all', 'controller'):
        background.create_task(
            _singleton_task('requests-gc', requests_lib.requests_gc_daemon))
        background.create_task(
            _singleton_task('cluster-event-retention',
                            global_user_state.cluster_event_retention_daemon))
        if _uses_postgres_requests():
            background.create_task(
                _singleton_task('operational-event-retention',
                                operational_event_store.retention_daemon))
            background.create_task(
                _singleton_task('serve-ordinary-launch-handoff-retention',
                                ordinary_launch_handoff.retention_daemon))
        background.create_task(
            _singleton_task('job-event-retention',
                            managed_job_state.job_event_retention_daemon))
        background.create_task(
            _singleton_task('estimated-spend-rollup',
                            estimated_spend_lib.rollup_daemon))
        background.create_task(
            _singleton_task(
                'unreferenced-file-mounts',
                file_mount_uploads.cleanup_unreferenced_file_mounts))
        background.create_task(
            _singleton_task('upload-staging-cleanup',
                            file_mount_uploads.cleanup_upload_ids))
        background.create_task(
            _singleton_task('download-staging-cleanup', _cleanup_download_tmp))
    background.start()
    return background


class _RoleHealthServer:
    """Dependency-free role-supervisor liveness and readiness endpoint."""

    def __init__(self, host: str, port: int,
                 lease: request_postgres.ServerInstanceLease) -> None:
        # Imports are deferred so API-only and local compatibility processes do
        # not pay for another HTTP-server stack.
        # pylint: disable=import-outside-toplevel
        import http.server

        self._lease = lease

        class Handler(http.server.BaseHTTPRequestHandler):
            """Serve the role supervisor's Kubernetes health contract."""

            def do_GET(self) -> None:  # pylint: disable=invalid-name
                if self.path == '/livez':
                    status = 200
                elif self.path == '/readyz':
                    status = 200 if lease.is_locally_ready() else 503
                else:
                    status = 404
                self.send_response(status)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'ok\n' if status == 200 else b'not ready\n')

            def log_message(
                    self,
                    format: str,  # pylint: disable=redefined-builtin
                    *args: Any) -> None:
                del format, args

        self._server = http.server.ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        name=f'{lease.role}-role-health',
                                        daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10)


def _run_uvicorn(state: RuntimeState, args: argparse.Namespace) -> None:
    # Imports occur only in API-bearing roles. The websocket limits are set by
    # main() before this import.
    # pylint: disable=import-outside-toplevel
    import uvicorn

    from sky.server import uvicorn as skyuvicorn

    logger.info('Starting SkyPilot API role, '
                f'workers={state.config.num_server_workers}')
    if state.instance_lease is not None:
        state.instance_lease.set_ready(True, health_detail={'phase': 'serving'})
    uvicorn_config = uvicorn.Config('sky.server.server:app',
                                    host=args.host,
                                    port=args.port,
                                    workers=state.config.num_server_workers,
                                    ws_per_message_deflate=False)
    skyuvicorn.run(
        uvicorn_config,
        max_db_connections=state.config.num_db_connections_per_worker)


def _wait_for_executor_shutdown() -> None:
    shutdown = threading.Event()

    def request_shutdown(signum, frame) -> None:
        del signum, frame
        shutdown.set()

    previous_term = signal.signal(signal.SIGTERM, request_shutdown)
    previous_int = signal.signal(signal.SIGINT, request_shutdown)
    try:
        while not shutdown.wait(1):
            pass
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


def _request_worker_shutdown(workers: list[executor.RequestWorker],
                             *,
                             terminate_children: bool = False,
                             request_stop: bool = True) -> None:
    """Stop new claims, optionally fence children, then join dispatchers."""
    if request_stop:
        for worker in workers:
            worker.request_shutdown()
    # Every finite request now owns and drains an exact per-invocation warden.
    # A broad process-tree kill would also destroy managed-controller family
    # guardians before their subreaper-owned descendants are proven absent.
    del terminate_children
    if workers:
        subprocess_utils.run_in_parallel(
            lambda worker: worker.wait_for_shutdown(),
            workers,
            num_threads=len(workers))


def _fence_execution_admission(
    workers: list[executor.RequestWorker],
    *,
    managed_job_refresh: Any | None = None,
    managed_job_slots: Any | None = None,
) -> bool:
    """Best-effort local admission fence before fallible drain publication."""
    boundaries: list[tuple[str, Callable[[], None]]] = [
        ('request-worker claim stop', worker.request_shutdown)
        for worker in workers
    ]
    if managed_job_refresh is not None:
        boundaries.append(('managed-job refresh effect fence',
                           managed_job_refresh.request_shutdown))
    if managed_job_slots is not None:
        boundaries.append(('managed-job slot claim fence',
                           managed_job_slots.request_shutdown))
    fenced = True
    for label, boundary in boundaries:
        try:
            boundary()
        except Exception:  # pylint: disable=broad-except
            # The authoritative convergence path below retries every fence and
            # retains ownership until it succeeds.  Do not let one local fence
            # prevent the remaining independent boundaries from closing.
            logger.exception(f'Early runtime shutdown step failed ({label}).')
            fenced = False
    return fenced


def _stop_queue_server(queue_server: multiprocessing.Process | None) -> None:
    if queue_server is None:
        return
    if queue_server.is_alive():
        try:
            queue_server.kill()
        except ProcessLookupError:
            pass
    queue_server.join()


def _shutdown_execution_ownership(
    *,
    background: _BackgroundLoop | None,
    workers: list[executor.RequestWorker],
    queue_server: multiprocessing.Process | None,
    terminate_children: bool,
    before_worker_join: Callable[[], None] | None = None,
    managed_job_refresh: Any | None = None,
    managed_job_slots: Any | None = None,
    admission_fenced: bool = False,
) -> None:
    """Quiesce effects while retaining the sole local PID-death proof owner."""
    failures: list[tuple[str, BaseException]] = []

    def attempt(label: str, function: Callable[[], Any]) -> None:
        try:
            function()
        except Exception as e:  # pylint: disable=broad-except
            logger.exception(f'Runtime shutdown step failed ({label}).')
            failures.append((label, e))

    # First close every finite-request claim loop.  Runtime daemon supervisors
    # and maintenance ownership then stop completely, so broad child fencing
    # cannot race a daemon supervisor that restarts what it observes dying.
    if not admission_fenced:
        for worker in workers:
            attempt('request-worker claim stop', worker.request_shutdown)
        if managed_job_refresh is not None:
            attempt('managed-job refresh effect fence',
                    managed_job_refresh.request_shutdown)
        if managed_job_slots is not None:
            attempt('managed-job slot claim fence',
                    managed_job_slots.request_shutdown)
    if background is not None:
        attempt('daemon and maintenance join', background.stop)
    if managed_job_refresh is not None:
        attempt('managed-job refresh join',
                managed_job_refresh.wait_for_shutdown)
    if workers:
        attempt(
            'request-worker child reap', lambda: _request_worker_shutdown(
                workers,
                terminate_children=terminate_children,
                request_stop=False))
    if managed_job_slots is not None:
        attempt('managed-job slot family drain',
                managed_job_slots.wait_for_shutdown)
    # Request pools are now joined and cannot spawn another detached effect.
    # Fence the durable controller inventory only after that boundary.
    if before_worker_join is not None:
        attempt('final controller child fence', before_worker_join)
    attempt('queue-server reap', lambda: _stop_queue_server(queue_server))

    # The caller retains its lease and retries this entire convergence boundary
    # while any exact request or managed-controller owner remains uncertain.
    if failures:
        labels = ', '.join(label for label, _ in failures)
        raise RuntimeOwnershipShutdownError(
            f'Runtime ownership remains held after failed shutdown: {labels}.'
        ) from failures[0][1]
    # The managed-jobs lock is retained across request-worker join and the
    # final detached-family fence, then released while the outer distributed
    # generation (PostgreSQL) or process-local authority (SQLite) is still
    # held.  That ordering is shared by split and compatibility runtimes.
    if not failures and managed_job_refresh is not None:
        attempt('managed-job refresh ownership release',
                managed_job_refresh.release_ownership)

    if failures:
        labels = ', '.join(label for label, _ in failures)
        raise RuntimeOwnershipShutdownError(
            f'Runtime ownership remains held after failed shutdown: {labels}.'
        ) from failures[0][1]


def _converge_execution_ownership_shutdown(**kwargs: Any) -> None:
    """Retain ownership and retry until every local effect is proven absent."""
    while True:
        try:
            _shutdown_execution_ownership(**kwargs)
            return
        except RuntimeOwnershipShutdownError as e:
            logger.critical('Runtime ownership shutdown has not converged; '
                            'retaining the leadership/instance session and '
                            f'retrying: {common_utils.format_exception(e)}')
            time.sleep(_OWNERSHIP_SHUTDOWN_RETRY_SECONDS)


def _run_controller_role(state: RuntimeState, args: argparse.Namespace) -> None:
    """Elect one controller leader and supervise all leader-owned work."""
    if state.instance_lease is None:
        raise RuntimeError(
            'The controller role requires PostgreSQL instance leases.')
    lease = request_postgres.ControllerLeaderLease(
        state.instance_lease.instance_id)
    health_server = _RoleHealthServer(args.host, args.role_health_port,
                                      state.instance_lease)
    background: _BackgroundLoop | None = None
    managed_job_refresh = None
    managed_job_slots = None
    queue_server: multiprocessing.Process | None = None
    workers: list[executor.RequestWorker] = []
    shutdown = threading.Event()
    became_leader = False
    leadership_lost = False
    cutover_regressed = False
    waiting_for_cutover = False
    cutover_ready = False
    generation: int | None = None
    origin_capability: str | None = None
    leadership_released = False
    cutover_quiescence_seconds = _controller_cutover_quiescence_seconds()

    def request_shutdown(signum, frame) -> None:
        del signum, frame
        shutdown.set()

    previous_term = signal.signal(signal.SIGTERM, request_shutdown)
    previous_int = signal.signal(signal.SIGINT, request_shutdown)
    health_server.start()
    try:
        state.instance_lease.set_ready(
            False, health_detail={'phase': 'checking-executor-cutover'})
        while not shutdown.is_set():
            try:
                blockers = request_postgres.recent_legacy_controller_consumers(
                    cutover_quiescence_seconds)
                if blockers:
                    if not waiting_for_cutover:
                        logger.info(
                            'Waiting for legacy controller consumers to '
                            f'quiesce: {len(blockers)} recent instance(s).')
                        state.instance_lease.set_ready(
                            False,
                            health_detail={
                                'phase': 'waiting-for-executor-cutover',
                                'legacy_consumer_count': len(blockers),
                            })
                        waiting_for_cutover = True
                        cutover_ready = False
                    shutdown.wait(_CONTROLLER_LEADERSHIP_POLL_SECONDS)
                    continue
                if not cutover_ready:
                    state.instance_lease.set_ready(
                        True, health_detail={'phase': 'standby'})
                    waiting_for_cutover = False
                    cutover_ready = True
                if lease.try_acquire():
                    try:
                        blockers = (
                            request_postgres.recent_legacy_controller_consumers(
                                cutover_quiescence_seconds))
                    except Exception:
                        lease.release()
                        state.instance_lease.set_ready(
                            False,
                            health_detail={
                                'phase': 'checking-executor-cutover'
                            })
                        cutover_ready = False
                        raise
                    if blockers:
                        logger.warning(
                            'Legacy controller consumers appeared during '
                            'promotion; releasing leadership and waiting.')
                        lease.release()
                        state.instance_lease.set_ready(
                            False,
                            health_detail={
                                'phase': 'waiting-for-executor-cutover',
                                'legacy_consumer_count': len(blockers),
                            })
                        waiting_for_cutover = True
                        cutover_ready = False
                        shutdown.wait(_CONTROLLER_LEADERSHIP_POLL_SECONDS)
                        continue
                    break
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f'Controller leadership probe failed: {e}')
            shutdown.wait(_CONTROLLER_LEADERSHIP_POLL_SECONDS)
        if shutdown.is_set():
            return

        generation = lease.generation
        assert generation is not None
        became_leader = True
        origin_capability = lease.origin_capability
        _publish_controller_origin_capability(origin_capability)
        os.environ[request_postgres.CONTROLLER_GENERATION_ENV_VAR] = str(
            generation)
        os.environ[request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR] = (
            lease.instance_id)
        managed_job_controller_fencing.publish_owner(
            (lease.instance_id, generation),
            mode=managed_job_controller_fencing.POSTGRES_OWNER_MODE)
        _publish_managed_job_runtime_owner_identity()

        request_storage.get_request_backend(
        ).retire_legacy_internal_daemon_rows()
        fenced = request_postgres.fence_stale_controller_claims(
            lease.instance_id, generation)
        logger.info('Controller generation '
                    f'{generation} fenced {fenced["replayed"]} replayable and '
                    f'{fenced["interrupted"]} ambiguous stale claim(s).')

        # The snapshot must include the immutable leader identity before any
        # request worker or controller-slot subprocess can be spawned.
        clean_env_module.capture_clean_server_env()

        # Establish the inner managed-job owner and finish its cutover before
        # admitting any controller-class RequestWorker.  submit_jobs can reach
        # controller spawning, so starting workers first creates a real
        # pre-cutover effect window even while the role is still NotReady.
        # pylint: disable=import-outside-toplevel
        from sky.jobs import managed_job_refresh_thread
        managed_job_refresh = (
            managed_job_refresh_thread.start_managed_job_refresh_daemon())
        if managed_job_refresh is not None:
            managed_job_refresh.wait_for_cutover()

        # Fixed slots are eager generation-owned runtime components.  Every
        # slot is effect-admission-gated and polling before controller-class
        # request workers can submit work to the scheduler.
        # Assign the owner before startup so a partial-admission proof failure
        # remains reachable by shutdown convergence and keeps the outer lease.
        managed_job_slots = (
            controller_slots.ManagedJobControllerSlotSupervisor(
                (lease.instance_id, generation),
                on_failure=_request_runtime_shutdown,
                origin_capability=origin_capability))
        managed_job_slots.start()

        queue_server, workers = executor.start(
            state.config,
            execution_classes=frozenset(
                {request_registry.ExecutionClass.CONTROLLER}),
            controller_generation=generation)
        background = _start_background_loop('controller')
        background.run(
            _register_runtime_daemons_async(
                background,
                state.config.num_db_connections_per_worker,
                (lease.instance_id, generation),
                state.instance_lease.pod_identity,
                observe_executor_termination=True))

        _start_surface_interrupted_cluster_launches()

        lock_backend_pid = lease.backend_pid()
        state.instance_lease.set_ready(True,
                                       health_detail={
                                           'phase': 'leading',
                                           'controller_generation': generation,
                                           'lock_backend_pid': lock_backend_pid,
                                       })
        logger.info(f'Controller generation {generation} is ready.')

        while not shutdown.wait(_CONTROLLER_LEADERSHIP_PROBE_SECONDS):
            managed_job_slots.raise_if_failed()
            blockers = request_postgres.recent_legacy_controller_consumers(
                cutover_quiescence_seconds)
            if blockers:
                cutover_regressed = True
                logger.error(
                    'A legacy controller consumer reappeared after controller '
                    f'promotion: {len(blockers)} recent instance(s).')
                try:
                    state.instance_lease.set_ready(
                        False,
                        health_detail={
                            'phase': 'legacy-consumer-detected',
                            'controller_generation': generation,
                            'legacy_consumer_count': len(blockers),
                        })
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        'Failed to publish unsafe controller cutover.')
                break
            if lease.heartbeat():
                continue
            leadership_lost = True
            logger.error('Lost API controller leadership generation '
                         f'{generation}; fencing local work and exiting.')
            try:
                state.instance_lease.set_ready(
                    False,
                    health_detail={
                        'phase': 'leadership-lost',
                        'controller_generation': generation,
                    })
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    'Failed to publish controller leadership loss.')
            break
    finally:
        try:
            if became_leader:
                assert generation is not None
                admission_fenced = _fence_execution_admission(
                    workers,
                    managed_job_refresh=managed_job_refresh,
                    managed_job_slots=managed_job_slots,
                )
                if not leadership_lost and not cutover_regressed:
                    try:
                        state.instance_lease.set_ready(
                            False,
                            health_detail={
                                'phase': 'draining',
                                'controller_generation': generation,
                            })
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            'Failed to publish controller drain state.')
                try:
                    _converge_execution_ownership_shutdown(
                        background=background,
                        workers=workers,
                        queue_server=queue_server,
                        terminate_children=True,
                        before_worker_join=None,
                        managed_job_refresh=managed_job_refresh,
                        managed_job_slots=managed_job_slots,
                        admission_fenced=admission_fenced,
                    )
                    try:
                        lease.release()
                        leadership_released = True
                    except Exception as e:
                        raise RuntimeOwnershipShutdownError(
                            'Controller leadership session did not release.'
                        ) from e
                finally:
                    if leadership_released:
                        os.environ.pop(
                            request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                            None)
                        os.environ.pop(
                            request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                            None)
                        managed_job_controller_fencing.clear_owner()
                        _clear_managed_job_runtime_owner_identity()
                        _clear_controller_origin_capability(origin_capability)
        finally:
            try:
                try:
                    state.instance_lease.set_ready(
                        False, health_detail={'phase': 'stopped'})
                except Exception:  # pylint: disable=broad-except
                    logger.warning('Failed to publish controller stop state.')
                try:
                    health_server.stop()
                except Exception:  # pylint: disable=broad-except
                    logger.exception('Failed to stop controller health server.')
            finally:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)

    if leadership_lost:
        raise RuntimeError('Controller leadership session was lost.')
    if cutover_regressed:
        raise RuntimeError('A legacy controller consumer reappeared.')


def run_role(state: RuntimeState, args: argparse.Namespace) -> None:
    """Start the selected role and unwind every owned resource on exit."""
    metrics_background: _BackgroundLoop | None = None
    background: _BackgroundLoop | None = None
    managed_job_refresh = None
    managed_job_slots = None
    queue_server = None
    workers: list[executor.RequestWorker] = []
    health_server = None
    ownership_release_safe = True
    compatibility_controller_lease = None
    compatibility_controller_owner: tuple[str, int] | None = None
    compatibility_owner_published = False
    compatibility_origin_capability: str | None = None
    compatibility_local_capability_authority = None
    try:
        try:
            metrics_background = _start_metrics_background_loop(
                state.role, args.host, args.metrics_port)
            if state.role == 'controller':
                _run_controller_role(state, args)
                return
            if state.role == 'all':
                if state.instance_lease is not None:
                    compatibility_controller_lease = (
                        request_postgres.ControllerLeaderLease(
                            state.instance_lease.instance_id))
                    if not compatibility_controller_lease.try_acquire():
                        raise RuntimeError(
                            'Compatibility API server could not acquire the '
                            'controller leadership lease.')
                    generation = compatibility_controller_lease.generation
                    assert generation is not None
                    compatibility_controller_owner = (
                        compatibility_controller_lease.instance_id, generation)
                    compatibility_origin_capability = (
                        compatibility_controller_lease.origin_capability)
                    _publish_controller_origin_capability(
                        compatibility_origin_capability)
                    owner_mode = (
                        managed_job_controller_fencing.POSTGRES_OWNER_MODE)
                else:
                    # SQLite all-mode is deliberately process-local.  Its
                    # consolidation file lock supplies exclusion; this UUID
                    # plus Linux process-birth tick supplies immutable fencing
                    # without pretending a PID is cross-host authority.
                    compatibility_controller_owner = (
                        str(uuid.uuid4()),
                        _executor_process_start_time_ticks(os.getpid()))
                    compatibility_local_capability_authority = (
                        controller_slots.
                        LocalControllerOriginCapabilityAuthority(
                            compatibility_controller_owner))
                    controller_capability.make_process_non_dumpable()
                    compatibility_local_capability_authority.publish()
                    compatibility_origin_capability = (
                        compatibility_local_capability_authority.capability)
                    owner_mode = managed_job_controller_fencing.LOCAL_OWNER_MODE
                assert compatibility_controller_owner is not None
                managed_job_controller_fencing.publish_owner(
                    compatibility_controller_owner, mode=owner_mode)
                compatibility_owner_published = True
                _publish_managed_job_runtime_owner_identity()
                if compatibility_controller_lease is not None:
                    request_backend = request_storage.get_request_backend()
                    if not isinstance(request_backend,
                                      request_postgres.PostgresRequestBackend):
                        raise RuntimeError('PostgreSQL controller leadership '
                                           'requires the PostgreSQL request '
                                           'backend.')
                    with request_postgres.legacy_daemon_transition():
                        request_backend.retire_legacy_internal_daemon_rows(
                            controller_owner=compatibility_controller_owner)
                        fenced = request_postgres.fence_stale_controller_claims(
                            *compatibility_controller_owner)
                        state.requests_recovered = (
                            request_backend.recover_on_startup(
                                controller_owner=(
                                    compatibility_controller_owner)))
                    logger.info(
                        'Compatibility controller generation '
                        f'{compatibility_controller_owner[1]} fenced '
                        f'{fenced["replayed"]} replayable and '
                        f'{fenced["interrupted"]} ambiguous stale claim(s).')
                    _start_surface_interrupted_cluster_launches()
            if state.role in ('all', 'executor'):
                clean_env_module.capture_clean_server_env()
            if compatibility_controller_lease is None:
                background = _start_background_loop(state.role)
            else:
                background = _start_background_loop(
                    state.role,
                    compatibility_controller_lease=(
                        compatibility_controller_lease))
            if state.role in ('all', 'executor'):
                if state.role == 'all':
                    # Compatibility all-mode can claim controller-class
                    # requests.  If consolidation is enabled, acquire its
                    # inner ownership and complete the one-path family
                    # inventory cutover before any RequestWorker can start.
                    # pylint: disable=import-outside-toplevel
                    from sky.jobs import managed_job_refresh_thread
                    managed_job_refresh = (managed_job_refresh_thread.
                                           start_managed_job_refresh_daemon())
                    if managed_job_refresh is not None:
                        managed_job_refresh.wait_for_cutover()
                        if compatibility_controller_owner is None:
                            raise RuntimeError(
                                'Managed-job refresh started without an exact '
                                'runtime controller owner.')
                        managed_job_slots = (
                            controller_slots.ManagedJobControllerSlotSupervisor(
                                compatibility_controller_owner,
                                on_failure=_request_runtime_shutdown,
                                origin_capability=(
                                    compatibility_origin_capability)))
                        managed_job_slots.start()
                execution_classes = None
                if state.role == 'executor':
                    execution_classes = frozenset(
                        {request_registry.ExecutionClass.NORMAL})
                start_kwargs: dict[str, Any] = {
                    'execution_classes': execution_classes,
                }
                if (state.role == 'all' and
                        compatibility_controller_lease is not None):
                    assert compatibility_controller_owner is not None
                    start_kwargs['controller_generation'] = (
                        compatibility_controller_owner[1])
                queue_server, workers = executor.start(state.config,
                                                       **start_kwargs)
                if state.requests_recovered:
                    executor.reenqueue_recovered_requests()
                if state.role == 'all':
                    # Compatibility mode owns both execution classes and
                    # retains the historical inner leader until split-role
                    # cutover.
                    if compatibility_controller_owner is None:
                        raise RuntimeError('Runtime daemons require an exact '
                                           'compatibility controller owner.')
                    if state.instance_lease is None:
                        raise RuntimeError('Runtime daemons require an exact '
                                           'server instance lease.')
                    background.run(
                        _register_runtime_daemons_async(
                            background,
                            state.config.num_db_connections_per_worker,
                            compatibility_controller_owner,
                            state.instance_lease.pod_identity))
                background.run(_initialize_normal_executor_requests())

            if state.role == 'executor':
                if state.instance_lease is None:
                    raise RuntimeError(
                        f'The {state.role} role requires PostgreSQL instance '
                        'leases.')
                health_server = _RoleHealthServer(args.host,
                                                  args.role_health_port,
                                                  state.instance_lease)
                health_server.start()
                health_detail: dict[str, Any] = {
                    'phase': 'claiming',
                    'long_workers':
                        state.config.long_worker_config.garanteed_parallelism,
                    'short_workers':
                        state.config.short_worker_config.garanteed_parallelism,
                }
                state.instance_lease.set_ready(True,
                                               health_detail=health_detail)
                _wait_for_executor_shutdown()
            else:
                _run_uvicorn(state, args)
        except RuntimeOwnershipShutdownError:
            ownership_release_safe = False
            raise
    finally:
        logger.info(f'Shutting down SkyPilot {state.role} role...')
        try:
            if state.role != 'controller':
                admission_fenced = _fence_execution_admission(
                    workers,
                    managed_job_refresh=managed_job_refresh,
                    managed_job_slots=managed_job_slots,
                )
                if health_server is not None:
                    try:
                        assert state.instance_lease is not None
                        state.instance_lease.set_ready(
                            False, health_detail={'phase': 'draining'})
                    except Exception:  # pylint: disable=broad-except
                        logger.exception('Failed to publish role drain state.')
                _converge_execution_ownership_shutdown(
                    background=background,
                    workers=workers,
                    queue_server=queue_server,
                    terminate_children=False,
                    before_worker_join=None,
                    managed_job_refresh=managed_job_refresh,
                    managed_job_slots=managed_job_slots,
                    admission_fenced=admission_fenced,
                )
                if state.role == 'all':
                    if compatibility_controller_lease is not None:
                        try:
                            compatibility_controller_lease.release()
                        except Exception as e:
                            ownership_release_safe = False
                            raise RuntimeOwnershipShutdownError(
                                'Compatibility managed-job leadership session '
                                'did not release.') from e
                    if compatibility_local_capability_authority is not None:
                        compatibility_local_capability_authority.remove()
                    else:
                        _clear_controller_origin_capability(
                            compatibility_origin_capability)
                    if compatibility_owner_published:
                        managed_job_controller_fencing.clear_owner()
                    _clear_managed_job_runtime_owner_identity()
                if health_server is not None:
                    try:
                        health_server.stop()
                    except Exception:  # pylint: disable=broad-except
                        logger.exception('Failed to stop role health server.')
            if state.instance_lease is not None and ownership_release_safe:
                try:
                    state.instance_lease.stop()
                except Exception as e:
                    ownership_release_safe = False
                    raise RuntimeOwnershipShutdownError(
                        'Server instance ownership did not release.') from e
        finally:
            # Stop collection before plugin teardown: API-owned custom
            # collectors must not race a plugin while its backing state closes.
            try:
                if metrics_background is not None:
                    metrics_background.stop()
            finally:
                for plugin in plugins.get_plugins():
                    plugin.shutdown()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default=46580, type=int)
    parser.add_argument('--deploy', action='store_true')
    parser.add_argument('--metrics-port', default=9090, type=int)
    parser.add_argument('--role',
                        choices=_ROLE_CHOICES,
                        default=os.environ.get(
                            request_postgres.SERVER_ROLE_ENV_VAR, 'all'))
    parser.add_argument('--role-health-port', default=46581, type=int)
    return parser


def main() -> None:
    """CLI entrypoint shared by local compatibility and Kubernetes roles."""
    os.environ.setdefault('WEBSOCKETS_MAX_LINE_LENGTH',
                          server_constants.WEBSOCKETS_MAX_HEADER_LINE_LENGTH)
    os.environ.setdefault('WEBSOCKETS_MAX_NUM_HEADERS',
                          server_constants.WEBSOCKETS_MAX_NUM_HEADERS)
    args = _build_parser().parse_args()
    os.environ[request_postgres.SERVER_ROLE_ENV_VAR] = args.role
    os.environ.setdefault(constants.ENV_VAR_IS_SKYPILOT_SERVER, 'true')

    if args.port == args.metrics_port and args.role in ('all', 'api'):
        raise ValueError('port and metrics-port cannot be the same')
    if (args.role in ('executor', 'controller') and _metrics_enabled() and
            args.role_health_port == args.metrics_port):
        raise ValueError('role-health-port and metrics-port cannot be the same')
    if args.role != 'all' and not _uses_postgres_requests():
        raise RuntimeError(
            f'The {args.role} role requires '
            f'{request_postgres.REQUEST_BACKEND_ENV_VAR}=postgres.')
    if args.role in ('all',
                     'api') and not common_utils.is_port_available(args.port):
        raise RuntimeError(f'Port {args.port} is not available')
    # Keep timestamped supervisor logs for every role, including executors
    # without Uvicorn.
    # pylint: disable=import-outside-toplevel
    from sky.server import uvicorn as skyuvicorn
    skyuvicorn.add_timestamp_prefix_for_server_logs()

    if _uses_postgres_requests():
        process_started_at = psutil.Process(os.getpid()).create_time()
        if request_storage.clear_stale_role_drain_marker(process_started_at):
            logger.info('Removed a drain marker retained from an earlier '
                        'container process.')

    state = initialize_common_runtime(args.role, args.deploy)
    drain_monitor = None
    if state.instance_lease is not None:
        drain_monitor = _RoleDrainMarkerMonitor(
            state.instance_lease, _request_runtime_shutdown_when_ready)
        drain_monitor.start()
    try:
        run_role(state, args)
    finally:
        if drain_monitor is not None:
            drain_monitor.stop()
