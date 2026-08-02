"""Process supervisors for explicit SkyPilot API server roles."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from collections.abc import Coroutine
import dataclasses
import multiprocessing
import os
import shutil
import signal
import threading
import time
from typing import Any

import uvloop

from sky import check as sky_check
from sky import estimated_spend as estimated_spend_lib
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils
from sky.physical_capacity import config as physical_capacity_config
from sky.physical_capacity import projector as capacity_projector_lib
from sky.serve import serve_utils
from sky.server import clean_env as clean_env_module
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import daemons
from sky.server import database_migrations
from sky.server import file_mount_uploads
from sky.server import metrics
from sky.server import plugins
from sky.server.blob import blob_storage as bs
from sky.server.events import store as operational_event_store
from sky.server.requests import authority_worker
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import registry as request_registry
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.skylet import constants
from sky.usage import usage_lib
from sky.users import permission
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import subprocess_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

_SERVER_USER_HASH_KEY = 'server_user_hash'
_ROLE_CHOICES = ('all', 'api', 'executor', 'controller', 'authority-worker')
_SINGLETON_PREFIX = 'skypilot:api-server-runtime:v1'
_CONTROLLER_LEADERSHIP_POLL_SECONDS = 2
_CONTROLLER_LEADERSHIP_PROBE_SECONDS = 2
_CONTROLLER_CUTOVER_QUIESCENCE_ENV_VAR = (
    'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS')


@dataclasses.dataclass
class RuntimeState:
    """Initialized state shared by role supervisors."""

    role: str
    config: server_config.ServerConfig
    instance_lease: request_postgres.ServerInstanceLease | None
    requests_recovered: bool
    physical_capacity_config: physical_capacity_config.CapacityConfig | None = (
        None)


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


def _ordinary_db_connections_after_capacity_reservation(
    config: physical_capacity_config.CapacityConfig,
    usable_connections: int | None,
) -> int | None:
    """Reserve exactly one isolated connection only for controller shadow."""
    if config.mode is physical_capacity_config.CapacityMode.DISABLED:
        return usable_connections
    if usable_connections is None or usable_connections < 2:
        raise RuntimeError('Physical-capacity shadow mode requires at least '
                           'two usable PostgreSQL connections.')
    return usable_connections - 1


def _start_surface_interrupted_cluster_launches() -> None:
    try:
        scan_delay = float(
            os.environ.get(constants.GRACE_PERIOD_SECONDS_ENV_VAR, '60'))
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
    usage_lib.maybe_show_privacy_policy()

    capacity_config = physical_capacity_config.load_config()
    physical_capacity_config.validate_common_runtime_environment(
        capacity_config,
        server_role=role,
        request_backend=os.environ.get(
            request_postgres.REQUEST_BACKEND_ENV_VAR))

    db_utils.set_max_connections(1)
    logger.info('Initializing database engines')
    database_migrations.initialize_central_databases()
    logger.info('Database engines initialized')

    requests_recovered = False
    if role == 'all':
        requests_recovered = requests_lib.recover_db_and_logs()
        _start_surface_interrupted_cluster_launches()

    logger.info('Initializing server user hash')
    init_or_restore_server_user_hash()
    if role in ('all', 'controller'):
        managed_job_utils.setup_consolidation_mode_on_startup(deploy)
    if capacity_config.mode is physical_capacity_config.CapacityMode.SHADOW:
        if (not managed_job_utils.is_consolidation_mode() or
                not serve_utils.is_consolidation_mode(pool=False) or
                not serve_utils.is_consolidation_mode(pool=True)):
            raise RuntimeError('Physical-capacity shadow mode requires '
                               'consolidated Serve, pool, and managed-jobs '
                               'state.')

    logger.info('Pre-loading plugin RBAC rules + viewer allowlist')
    plugins.load_plugin_rbac_rules()
    plugins.load_plugin_viewer_allowlist()
    logger.info('Initializing permission service')
    permission.permission_service.initialize()
    logger.info('Permission service initialized')

    max_db_connections = global_user_state.get_max_db_connections()
    logger.info(f'Max db connections: {max_db_connections}')
    ordinary_max_db_connections = (
        _ordinary_db_connections_after_capacity_reservation(
            capacity_config, max_db_connections))
    if capacity_config.mode is physical_capacity_config.CapacityMode.SHADOW:
        assert ordinary_max_db_connections is not None
        logger.info('Reserved one PostgreSQL connection for physical-capacity '
                    f'evidence; {ordinary_max_db_connections} remain for '
                    'ordinary server configuration.')
    reserved_memory_mb: float = 0
    if role in ('all', 'controller'):
        reserved_memory_mb = (
            controller_utils.compute_memory_reserved_for_controllers(
                reserve_for_controllers=os.environ.get(
                    constants.OVERRIDE_CONSOLIDATION_MODE) is not None,
                reserve_extra_for_pool=not os.environ.get(
                    constants.IS_SKYPILOT_SERVE_CONTROLLER)))
    config = server_config.compute_server_config(
        deploy,
        ordinary_max_db_connections,
        reserved_memory_mb=reserved_memory_mb)
    if role in ('all', 'controller'):
        server_config.publish_serve_launch_parallelism(config)

    instance_lease = None
    if _uses_postgres_requests():
        instance_lease = request_postgres.ServerInstanceLease(role)
        instance_lease.start()
    return RuntimeState(role, config, instance_lease, requests_recovered,
                        capacity_config)


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


async def _initialize_controller_requests() -> None:
    """Submit leader-owned durable daemons after controller consumers exist."""
    await requests_lib.delete_orphan_internal_daemons_async(
        daemons.INTERNAL_REQUEST_DAEMONS)
    for event in daemons.INTERNAL_REQUEST_DAEMONS:
        if event.should_skip():
            continue
        await executor.schedule_internal_daemon_async(event)


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
        self._thread = threading.Thread(target=self._run,
                                        name='server-background-loop',
                                        daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def create_task(self, coroutine: Coroutine[Any, Any, Any]) -> None:
        self._tasks.append(self.loop.create_task(coroutine))

    def start(self) -> None:
        self._thread.start()

    def run(self,
            coroutine: Coroutine[Any, Any, Any],
            *,
            timeout: float = 60) -> Any:
        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        return future.result(timeout=timeout)

    def stop(self) -> None:
        if not self._thread.is_alive():
            self.loop.close()
            return

        async def cancel_tasks() -> None:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)

        future = asyncio.run_coroutine_threadsafe(cancel_tasks(), self.loop)
        try:
            future.result(timeout=30)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Background task shutdown was incomplete: {e}')
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=30)
        self.loop.close()


def _singleton_task(
    name: str,
    task_factory,
) -> Coroutine[Any, Any, None]:
    if _uses_postgres_requests():
        return request_postgres.run_distributed_singleton(
            f'{_SINGLETON_PREFIX}:{name}', task_factory)
    return task_factory()


def _start_background_loop(role: str, host: str,
                           metrics_port: int) -> _BackgroundLoop:
    background = _BackgroundLoop()
    if role in ('all', 'api') and os.environ.get(
            constants.ENV_VAR_SERVER_METRICS_ENABLED):
        metrics.maybe_register_managed_jobs_collector()
        metrics_server = metrics.build_metrics_server(host, metrics_port)
        background.create_task(metrics_server.serve())
        background.create_task(metrics.multiproc_reaper_daemon())

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
                             terminate_children: bool = False) -> None:
    """Stop new claims, optionally fence children, then join dispatchers."""
    for worker in workers:
        worker.request_shutdown()
    if terminate_children:
        subprocess_utils.kill_children_processes()
    if workers:
        subprocess_utils.run_in_parallel(
            lambda worker: worker.wait_for_shutdown(),
            workers,
            num_threads=len(workers))


def _stop_queue_server(queue_server: multiprocessing.Process | None) -> None:
    if queue_server is None:
        return
    if queue_server.is_alive():
        try:
            queue_server.kill()
        except ProcessLookupError:
            pass
    queue_server.join()


def _kill_local_controller_children(*, fail_closed: bool = False) -> None:
    """Fail-stop detached schedulers before leader handoff."""
    # Managed job controllers use detached process sessions, so use their
    # durable local process records in addition to walking the worker tree.
    # pylint: disable=import-outside-toplevel
    from sky.jobs import scheduler as managed_job_scheduler
    try:
        managed_job_scheduler.fail_stop_local_job_controllers()
    except Exception:  # pylint: disable=broad-except
        logger.exception('Failed to fail-stop local managed-job controllers.')
        if fail_closed:
            raise


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
    queue_server: multiprocessing.Process | None = None
    workers: list[executor.RequestWorker] = []
    capacity_projector: capacity_projector_lib.EvidenceProjector | None = None
    shutdown = threading.Event()
    became_leader = False
    leadership_lost = False
    capacity_projector_failed = False
    cutover_regressed = False
    waiting_for_cutover = False
    cutover_ready = False
    generation: int | None = None
    cutover_quiescence_seconds = _controller_cutover_quiescence_seconds()
    configured_capacity = state.physical_capacity_config
    if configured_capacity is None:
        configured_capacity = physical_capacity_config.load_config()
    shadow_capacity = (configured_capacity.mode
                       is physical_capacity_config.CapacityMode.SHADOW)

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
                        if shadow_capacity:
                            acquiring_generation = lease.generation
                            assert acquiring_generation is not None
                            state.instance_lease.set_ready(
                                False,
                                health_detail={
                                    'phase': 'activating-controller',
                                    'controller_generation': acquiring_generation,
                                })
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
        os.environ[request_postgres.CONTROLLER_GENERATION_ENV_VAR] = str(
            generation)
        os.environ[request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR] = (
            lease.instance_id)

        fenced = request_postgres.fence_stale_controller_claims(
            lease.instance_id, generation)
        logger.info('Controller generation '
                    f'{generation} fenced {fenced["replayed"]} replayable and '
                    f'{fenced["interrupted"]} ambiguous stale claim(s).')

        if shadow_capacity:
            capacity_projector = (
                capacity_projector_lib.start_controller_projector(
                    configured_capacity,
                    controller_instance_id=lease.instance_id,
                    controller_generation=generation))
            assert capacity_projector is not None

        # The snapshot must include the immutable leader identity before any
        # worker or controller subprocess can be spawned.
        clean_env_module.capture_clean_server_env()
        queue_server, workers = executor.start(
            state.config,
            execution_classes=frozenset(
                {request_registry.ExecutionClass.CONTROLLER}),
            controller_generation=generation)
        background = _start_background_loop('controller', args.host,
                                            args.metrics_port)
        background.run(_initialize_controller_requests())

        # The existing managed-jobs consolidation lock and SkyServe lifecycle
        # epochs remain inner subsystem fences under this outer leader.
        # pylint: disable=import-outside-toplevel
        from sky.jobs import managed_job_refresh_thread
        managed_job_refresh_thread.start_managed_job_refresh_daemon()
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
            if (capacity_projector is not None and
                    not capacity_projector.healthy):
                capacity_projector_failed = True
                logger.error('Physical-capacity evidence projector became '
                             'unhealthy; fencing controller work and exiting.')
                try:
                    state.instance_lease.set_ready(
                        False,
                        health_detail={
                            'phase': 'capacity-projector-failed',
                            'controller_generation': generation,
                        })
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        'Failed to publish capacity projector failure.')
                break
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
                if (not leadership_lost and not cutover_regressed and
                        not capacity_projector_failed):
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
                # Stop claim loops first. Then terminate every local child
                # before releasing the leadership session so no
                # old-generation provider work survives into the replacement
                # generation.
                if capacity_projector is None:
                    # Preserve the existing disabled-mode drain and release
                    # behavior exactly. C2 strict fail-stop semantics are
                    # justified only while a shadow projector owns work.
                    try:
                        for worker in workers:
                            worker.request_shutdown()
                        _kill_local_controller_children()
                        if workers:
                            _request_worker_shutdown(workers,
                                                     terminate_children=True)
                        if background is not None:
                            background.stop()
                        _stop_queue_server(queue_server)
                    finally:
                        try:
                            lease.release()
                        finally:
                            os.environ.pop(
                                request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                                None)
                            os.environ.pop(
                                request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                                None)
                else:
                    drain_errors: list[BaseException] = []

                    def drain_step(name: str, operation: Callable[[],
                                                                  Any]) -> None:
                        try:
                            operation()
                        # This synchronous all-step barrier deliberately
                        # collects even process-control failures. The caller
                        # fail-stops before releasing leadership.
                        except BaseException as e:  # pylint: disable=broad-except  # noqa: ASYNC103
                            drain_errors.append(e)
                            logger.exception(f'Controller drain step {name} '
                                             'failed.')

                    for worker in workers:
                        drain_step('request-worker-stop-request',
                                   worker.request_shutdown)
                    drain_step(
                        'capacity-projector-stop',
                        lambda: capacity_projector_lib.
                        stop_controller_projector(capacity_projector))
                    drain_step(
                        'local-controller-child-fail-stop', lambda:
                        _kill_local_controller_children(fail_closed=True))
                    if workers:
                        drain_step(
                            'request-worker-fail-stop',
                            lambda: _request_worker_shutdown(
                                workers, terminate_children=True))
                    if background is not None:
                        drain_step('background-loop-stop', background.stop)
                    drain_step('queue-server-stop',
                               lambda: _stop_queue_server(queue_server))

                    if drain_errors:
                        # Never release the durable generation after an
                        # unproven projector/child shutdown. Immediate process
                        # exit closes the leadership session and every
                        # remaining DB socket.
                        logger.critical(
                            'Controller drain could not prove all leader-owned '
                            'work stopped; fail-stopping the process before '
                            'lease release.')
                        os._exit(1)  # pylint: disable=protected-access
                    try:
                        lease.release()
                    finally:
                        os.environ.pop(
                            request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                            None)
                        os.environ.pop(
                            request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                            None)
        finally:
            try:
                try:
                    state.instance_lease.set_ready(
                        False, health_detail={'phase': 'stopped'})
                except Exception:  # pylint: disable=broad-except
                    logger.warning('Failed to publish controller stop state.')
                health_server.stop()
            finally:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)

    if leadership_lost:
        raise RuntimeError('Controller leadership session was lost.')
    if cutover_regressed:
        raise RuntimeError('A legacy controller consumer reappeared.')
    if capacity_projector_failed:
        raise RuntimeError('Physical-capacity evidence projector failed.')


def run_role(state: RuntimeState, args: argparse.Namespace) -> None:
    """Start the selected role and unwind every owned resource on exit."""
    background: _BackgroundLoop | None = None
    queue_server = None
    workers: list[executor.RequestWorker] = []
    health_server = None
    try:
        if state.role == 'controller':
            _run_controller_role(state, args)
            return

        background = _start_background_loop(state.role, args.host,
                                            args.metrics_port)
        authority_claim_config = None
        if state.role == 'authority-worker':
            if state.instance_lease is None:
                raise RuntimeError(
                    'The authority-worker role requires PostgreSQL instance '
                    'leases.')
            authority_worker.require_private_handler_inventory()
            engine = request_postgres.initialize_and_get_db()
            with engine.connect() as connection:
                authority_claim_config = authority_worker.resolve_claim_config(
                    connection)

        if state.role in ('all', 'executor', 'authority-worker'):
            clean_env_module.capture_clean_server_env()
            execution_classes = None
            if state.role in ('executor', 'authority-worker'):
                execution_classes = frozenset(
                    {request_registry.ExecutionClass.NORMAL})
            start_kwargs: dict[str, Any] = {
                'execution_classes': execution_classes,
            }
            if authority_claim_config is not None:
                start_kwargs['authority_claim_config'] = (
                    authority_claim_config)
            queue_server, workers = executor.start(state.config, **start_kwargs)
            if state.requests_recovered:
                executor.reenqueue_recovered_requests()
            if state.role == 'all':
                # Compatibility mode owns both execution classes and retains
                # the historical inner leader until split-role cutover.
                # pylint: disable=import-outside-toplevel
                from sky.jobs import managed_job_refresh_thread
                managed_job_refresh_thread.start_managed_job_refresh_daemon()
                background.run(_initialize_controller_requests())
            if state.role != 'authority-worker':
                background.run(_initialize_normal_executor_requests())

        if state.role in ('executor', 'authority-worker'):
            if state.instance_lease is None:
                raise RuntimeError(
                    f'The {state.role} role requires PostgreSQL instance '
                    'leases.')
            health_server = _RoleHealthServer(args.host, args.role_health_port,
                                              state.instance_lease)
            health_server.start()
            health_detail: dict[str, Any] = {
                'phase': 'claiming',
                'long_workers':
                    state.config.long_worker_config.garanteed_parallelism,
                'short_workers':
                    state.config.short_worker_config.garanteed_parallelism,
            }
            if authority_claim_config is not None:
                health_detail.update({
                    'cohort_id': authority_claim_config.routing.cohort_id,
                    'active_cohort_id': authority_claim_config.active_cohort_id,
                    'cohort_lifecycle_state':
                        authority_claim_config.lifecycle_state,
                    'claim_contract': 'frozen_action_cohort_join_v1',
                })
            state.instance_lease.set_ready(True, health_detail=health_detail)
            _wait_for_executor_shutdown()
        else:
            _run_uvicorn(state, args)
    finally:
        logger.info(f'Shutting down SkyPilot {state.role} role...')
        if state.instance_lease is not None:
            state.instance_lease.stop()
        if state.role != 'controller':
            if health_server is not None:
                health_server.stop()
            if workers:
                _request_worker_shutdown(workers)
            _stop_queue_server(queue_server)
            if background is not None:
                background.stop()
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

    state = initialize_common_runtime(args.role, args.deploy)
    run_role(state, args)
