"""Executor for the requests.

We start limited number of workers for long-running requests, and
significantly more workers for short-running requests. This is to optimize the
resource usage and the latency of the requests.

* Long-running requests are those requests that can take a long time to finish
and more resources are needed, such as cluster launching, starting, job
submission, managed job submission, etc.

* Short-running requests are those requests that can be done quickly, and
require a quick response, such as status check, job status check, etc.

With more short-running workers, we can serve more short-running requests in
parallel, and reduce the latency.

The number of the workers is determined by the system resources.

See the [README.md](../README.md) for detailed architecture of the executor.
"""
import asyncio
from collections.abc import Callable
from collections.abc import Generator
import concurrent.futures
import contextlib
import multiprocessing
import os
import signal
import sys
import threading
import time
import typing
from typing import Any, Optional, ParamSpec, TextIO
import uuid

import psutil
import setproctitle

from sky import exceptions
from sky import global_user_state
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.events import api_models as event_api_models
from sky.metrics import utils as metrics_utils
from sky.serve import placement_history
from sky.server import clean_env as clean_env_module
from sky.server import common as server_common
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import metrics as metrics_lib
from sky.server import plugins
from sky.server import versions
from sky.server import watchdog
from sky.server.events import models as event_models
from sky.server.requests import payloads
from sky.server.requests import preconditions
from sky.server.requests import process
from sky.server.requests import registry as request_registry
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests
from sky.server.requests import role_filter
from sky.server.requests import storage as request_storage
from sky.server.requests import threads
from sky.server.requests.queues import base as queue_base
from sky.skylet import constants
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import context
from sky.utils import context_utils
from sky.utils import controller_capability
from sky.utils import debug_dump_helpers
from sky.utils import tempstore
from sky.utils import timeline
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.workspaces import constants as workspace_constants
from sky.workspaces import core as workspaces_core

if typing.TYPE_CHECKING:
    import types

P = ParamSpec('P')
logger = sky_logging.init_logger(__name__)

ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')

# On macOS, the default start method for multiprocessing is 'fork', which
# can cause issues with certain types of resources, including those used in
# the QueueManager in mp_queue.py.
# The 'spawn' start method is generally more compatible across different
# platforms, including macOS.
multiprocessing.set_start_method('spawn', force=True)

# An upper limit of max threads for request execution per server process that
# unlikely to be reached to allow higher concurrency while still prevent the
# server process become overloaded.
_REQUEST_THREADS_LIMIT = 128

# Max length of the retry reason in a request's backoff status message; the
# reason comes from the exception message, so truncate to keep it readable.
_RETRY_STATUS_MSG_REASON_MAX_LEN = 200
_QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS = 0.1
_QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS = 5.0

_REQUEST_THREAD_EXECUTOR_LOCK = threading.Lock()
# A dedicated thread pool executor for synced requests execution in coroutine to
# avoid:
# 1. blocking the event loop;
# 2. exhausting the default thread pool executor of event loop;
_REQUEST_THREAD_EXECUTOR: threads.OnDemandThreadExecutor | None = None


def _open_controller_capability_transport(capability: str) -> int:
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


def _request_role_shutdown_after_boundary_ambiguity(
        error: process.AmbiguousBoundaryError) -> None:
    """Enter the runtime's signal-driven not-Ready convergence path."""
    del error
    os.kill(os.getpid(), signal.SIGTERM)


def api_process_execution_enabled() -> bool:
    """Whether HTTP handlers may use the legacy in-process execution path."""
    return os.environ.get('SKYPILOT_API_SERVER_ROLE', 'all') != 'api'


def get_request_thread_executor() -> threads.OnDemandThreadExecutor:
    """Lazy init and return the request thread executor for current process."""
    global _REQUEST_THREAD_EXECUTOR
    if _REQUEST_THREAD_EXECUTOR is not None:
        return _REQUEST_THREAD_EXECUTOR
    with _REQUEST_THREAD_EXECUTOR_LOCK:
        if _REQUEST_THREAD_EXECUTOR is None:
            _REQUEST_THREAD_EXECUTOR = threads.OnDemandThreadExecutor(
                name='request_thread_executor',
                max_workers=_REQUEST_THREADS_LIMIT)
        return _REQUEST_THREAD_EXECUTOR


class RequestQueue:
    """The queue for the requests.

    Wraps a QueueBackend instance. The elements in the queue are tuples of
    (request_id, ignore_return_value, retryable).
    """

    def __init__(self, queue_backend_impl: queue_base.QueueBackend) -> None:
        self._backend = queue_backend_impl

    def put(self, request: queue_base.QueueItemLike) -> None:
        """Put a request to the queue.

        Args:
            request: A tuple of request_id, ignore_return_value, and retryable.
        """
        self._backend.put(request)

    async def put_async(self, request: queue_base.QueueItemLike) -> None:
        """Put a request to the queue, async.

        Args:
            request: A tuple of request_id, ignore_return_value, and retryable.
        """
        await self._backend.put_async(request)

    def get(self) -> queue_base.QueueItem | None:
        """Get a request from the queue.

        It is non-blocking if the queue is empty, and returns None.

        Returns:
            A tuple of request_id, ignore_return_value, and retryable.
        """
        item = self._backend.get()
        return (queue_base.normalize_queue_item(item)
                if item is not None else None)

    def peek_provider_mutation(
            self) -> queue_base.ProviderMutationCandidate | None:
        """Read a provider mutation without creating durable ownership."""
        return self._backend.peek_provider_mutation()

    def claim_provider_mutation(
        self, candidate: queue_base.ProviderMutationCandidate
    ) -> queue_base.QueueItem | None:
        """Try to claim an exact provider candidate after slot reservation."""
        return self._backend.claim_provider_mutation(candidate)

    def __len__(self) -> int:
        """Get the length of the queue."""
        return self._backend.qsize()


# The active queue factory, set during start().
_queue_factory: queue_base.QueueBackendFactory | None = None


def executor_initializer(proc_group: str,
                         clean_env: dict[str, str] | None = None):
    # The disposable boundary has already protected the handler and consumed
    # any verified managed-origin transport before invoking this initializer.
    db_utils.set_postgres_connection_metrics_process_role('executor')
    setproctitle.setproctitle(f'SkyPilot:executor:{proc_group}:'
                              f'{multiprocessing.current_process().pid}')
    # This runs in a child process of the API server. If the main process
    # dies abruptly (kill -9, OOM), exit instead of keeping executing the
    # current request as an orphan: its late terminal writes can race the
    # next server boot's startup recovery (which may have already marked
    # the request CANCELLED for retry), leading to double execution.
    if watchdog.running_in_child_process():
        watchdog.start_parent_death_watchdog()
    # The main API server process captures its env at startup and forwards
    # it via initargs (see RequestWorker.run). Adopt that snapshot directly
    # so the worker doesn't depend on its own spawn-time os.environ, which
    # for a lazy-spawned burst worker could reflect a coroutine-path
    # request mid-pollution in the main process.
    if clean_env is not None:
        clean_env_module.restore_clean_server_env(clean_env)
    # Load plugins only after adopting the clean environment. PostgreSQL
    # request-backend validation runs at the end of plugin loading, before this
    # process can execute request code.
    plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.EXECUTOR))
    # Same rationale as in sky.server.uvicorn.Server.run: reap this
    # executor's prometheus multiproc files when it exits.
    metrics_lib.register_multiproc_cleanup_atexit()
    # Executor never stops, unless the whole process is killed.
    threading.Thread(target=metrics_lib.process_monitor,
                     args=(f'worker:{proc_group}', threading.Event()),
                     daemon=True).start()


def _request_is_gone_or_cancelled(request_id: str) -> bool:
    """Cancellation check passed to ``ContinueCondition.wait()``.

    A request cancelled (or gone) while paused must not be re-queued.
    """
    request = api_requests.get_request(request_id, fields=['status'])
    return (request is None or
            request.status == api_requests.RequestStatus.CANCELLED)


class RequestWorker:
    """A worker that polls requests from the queue and runs them.

    The worker can run at least `garanteed_parallelism` requests in parallel.
    If there are more resources available, it can spin up extra workers up to
    `garanteed_parallelism + burstable_parallelism`.
    """

    # The type of queue this worker works on.
    schedule_type: api_requests.ScheduleType
    # The least number of requests that this worker can run in parallel.
    garanteed_parallelism: int
    # The extra number of requests that this worker can run in parallel
    # if there are available CPU/memory resources.
    burstable_parallelism: int = 0

    def __init__(self, schedule_type: api_requests.ScheduleType,
                 config: server_config.WorkerConfig) -> None:
        self.schedule_type = schedule_type
        self.garanteed_parallelism = config.garanteed_parallelism
        self.burstable_parallelism = config.burstable_parallelism
        self.num_db_connections_per_worker = (
            config.num_db_connections_per_worker)
        self._thread: threading.Thread | None = None
        self._cancel_event = threading.Event()
        self._monitor_condition = threading.Condition()
        self._monitor_threads: set[threading.Thread] = set()
        self._shutdown_error: BaseException | None = None
        self._boundary_ambiguity_error: (process.AmbiguousBoundaryError |
                                         None) = None

    def __str__(self) -> str:
        return f'Worker(schedule_type={self.schedule_type.value})'

    def run_in_background(self) -> None:
        # Thread dispatcher is sufficient for current scale, refer to
        # tests/load_tests/test_queue_dispatcher.py for more details.
        # Use daemon thread for automatic cleanup.
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        self._thread = thread

    def cancel(self) -> None:
        self.request_shutdown()
        self.wait_for_shutdown()

    def request_shutdown(self) -> None:
        """Stop this worker from claiming another request."""
        self._cancel_event.set()

    def wait_for_shutdown(self) -> None:
        """Wait for the worker dispatcher and its process pool to exit."""
        if self._thread is not None:
            self._thread.join()
        # Pool termination makes every accepted Future complete. Each monitor
        # remains registered until its exact outcome/receipt has durably
        # converged (or the database proves its identity obsolete).
        while True:
            with self._monitor_condition:
                monitors = tuple(self._monitor_threads)
            if not monitors:
                break
            for monitor in monitors:
                monitor.join()
        shutdown_error = (self._boundary_ambiguity_error or
                          self._shutdown_error)
        if shutdown_error is not None:
            raise RuntimeError(
                f'{self} could not prove executor child shutdown.') from (
                    shutdown_error)

    def _handle_ambiguous_boundary(
            self, error: process.AmbiguousBoundaryError) -> None:
        """Poison this role after losing exact process-family proof."""
        self._cancel_event.set()
        with self._monitor_condition:
            if self._boundary_ambiguity_error is not None:
                return
            self._boundary_ambiguity_error = error
            self._monitor_condition.notify_all()
        try:
            logger.critical(
                f'[{self}] Executor lost exact process-family quiescence proof; '
                'stopping claims and draining role readiness while retaining '
                f'ownership: {common_utils.format_exception(error)}')
            self._mark_executor_poisoned()
        finally:
            try:
                _request_role_shutdown_after_boundary_ambiguity(error)
            # Synchronous dispatcher callback: retain the boundary ambiguity
            # even if the role-level shutdown hook raises a BaseException.
            except BaseException:  # noqa: ASYNC103  # pylint: disable=broad-except
                # The worker is already locally fenced. Preserve the ambiguity
                # for wait_for_shutdown() so runtime ownership still fails
                # closed even when the role-level wakeup itself fails.
                logger.exception('Failed to request role shutdown after losing '
                                 'execution-boundary proof.')

    def _start_task_monitor(self, fut: concurrent.futures.Future,
                            request_element: queue_base.QueueItemLike) -> bool:
        """Register a result monitor before making its thread runnable.

        Returns whether the monitor thread started.  If thread creation fails,
        this method cancels a gated invocation before admission and converges it
        synchronously, so the caller must not publish RUNNING or admit effects.
        """
        monitor: threading.Thread

        def run_monitor() -> None:
            try:
                self.handle_task_result(fut, request_element)
            finally:
                with self._monitor_condition:
                    self._monitor_threads.discard(monitor)
                    self._monitor_condition.notify_all()

        monitor = threading.Thread(target=run_monitor, daemon=False)
        with self._monitor_condition:
            self._monitor_threads.add(monitor)
        try:
            monitor.start()
        # Thread start is synchronous; every failure must converge the already
        # accepted invocation before this dispatcher can return.
        except BaseException as start_error:  # noqa: ASYNC103  # pylint: disable=broad-exception-caught
            # The executor has already accepted the Future. Keep the registered
            # ownership token while the dispatcher synchronously performs the
            # exact same convergence work; dropping it here would leave an
            # effect-bearing callable with no result/receipt owner.
            logger.warning(
                f'Could not start request result monitor; retaining ownership '
                f'on the dispatcher thread: '
                f'{type(start_error).__name__}: {start_error}')
            if isinstance(fut, process.InvocationFuture):
                fut.request_cancel()
            run_monitor()
            return False  # noqa: ASYNC104 - synchronous dispatcher path
        return True

    def process_request(self, executor: process.BurstableExecutor,
                        queue: RequestQueue) -> None:
        request_id: str | None = None
        request_element: queue_base.QueueItem | None = None
        fut: process.InvocationFuture | None = None
        reservation: process.IdleWorkerReservation | None = None
        try:
            if request_storage.role_is_draining():
                # The marker is written before Kubernetes' readiness sleep.
                # Fence the dispatcher immediately instead of waiting for the
                # later SIGTERM to reach the runtime supervisor.
                self.request_shutdown()
                return
            # Capacity is reserved before *every* dequeue.  A durable claim or
            # legacy queue pop must never sit in a dispatcher-local backlog
            # waiting for a process boundary to become available.
            reservation = executor.try_reserve_idle_worker()
            if reservation is None:
                time.sleep(0.1)
                return

            # Provider-mutating PostgreSQL requests retain their distinct
            # observe-then-claim order, but both provider and generic delivery
            # now consume the same finite reservation capability.
            peek_provider = getattr(queue, 'peek_provider_mutation', None)
            provider_candidate = (peek_provider()
                                  if callable(peek_provider) else None)
            if provider_candidate is not None:
                queued = queue.claim_provider_mutation(provider_candidate)
            else:
                queued = queue.get()
            if queued is None:
                time.sleep(0.1)
                return
            request_element = queue_base.normalize_queue_item(queued)
            request_id = request_element.request_id
            if request_storage.role_is_draining():
                # The claim is still pre-effect. Return its exact generation
                # through the backend's normal atomic handoff rather than
                # abandoning it for lease expiry.
                self.request_shutdown()
                queue.put(request_element)
                return
            ignore_return_value = request_element.ignore_return_value
            request = api_requests.get_request(request_id,
                                               fields=['status', 'created_at'])
            if request is None:
                # The record can be gone, e.g. wiped by a concurrent cleanup.
                # Drop the element instead of raising: the queue element is
                # already popped and there is no row to fail.
                logger.warning(f'[{self}] Dropping queued request '
                               f'{request_id}: no request record found')
                return
            if request.status == api_requests.RequestStatus.CANCELLED:
                if request_element.claim_token is not None:
                    # This exact generic or provider claim never reached a
                    # child. Converge the generation-bound receipt with the
                    # same retry-until-definitive protocol as a completed
                    # boundary; cancellation racing a different owner makes
                    # this exact identity definitively obsolete.
                    self._converge_execution_completion(
                        request_storage.ExecutionClaim(
                            request_id, request_element.execution_generation,
                            request_element.claim_token,
                            request_element.worker_instance_id),
                        error=concurrent.futures.CancelledError(),
                        terminal_cause='dispatcher_submit_failed')
                return
            if metrics_utils.METRICS_ENABLED:
                metrics_utils.SKY_APISERVER_QUEUE_WAIT_SECONDS.labels(
                    schedule_type=self.schedule_type.value,).observe(
                        max(0,
                            time.time() - request.created_at))
            del request
            logger.info(f'[{self}] Submitting request: {request_id}')
            # Start one disposable, invocation-owned process boundary.  Its
            # outer guardian is the durable cancellation and liveness identity.
            submit_args = (_request_execution_wrapper, request_id,
                           ignore_return_value,
                           self.num_db_connections_per_worker,
                           request_element.execution_generation,
                           request_element.claim_token,
                           request_element.worker_instance_id,
                           request_element.managed_job_origin)
            capability_fd = None
            if request_element.managed_job_origin is not None:
                origin = request_element.managed_job_origin
                if request_element.worker_instance_id != origin[1]:
                    raise RuntimeError(
                        'Managed-job nested request claim and origin disagree.')
                capability = controller_capability.get_process_local()
                if capability is None:
                    raise RuntimeError(
                        'Managed-job nested request has no controller '
                        'capability authority.')
                capability_fd = _open_controller_capability_transport(
                    capability)
            # submit_reserved consumes the capability before touching the
            # selected lane, so this request can never enter a hidden backlog.
            try:
                fut = executor.submit_reserved(
                    reservation,
                    *submit_args,
                    admission_gated=True,
                    receipt_required=request_element.claim_token is not None,
                    capability_fd=capability_fd)
            finally:
                if capability_fd is not None:
                    os.close(capability_fd)
            # Decrement the free executor count when a request starts
            if metrics_utils.METRICS_ENABLED:
                if self.schedule_type == api_requests.ScheduleType.LONG:
                    metrics_utils.SKY_APISERVER_LONG_EXECUTORS.dec()
                elif self.schedule_type == api_requests.ScheduleType.SHORT:
                    metrics_utils.SKY_APISERVER_SHORT_EXECUTORS.dec()
            # Establish the parent-owned result/receipt monitor before the
            # guarded RUNNING transition or effect admission.  An accepted
            # boundary can therefore never run without a durable convergence
            # owner.
            if not self._start_task_monitor(fut, request_element):
                return

            if request_storage.role_is_draining():
                self.request_shutdown()
                fut.request_cancel()
                return

            guardian = fut.guardian_identity
            observed_start_ticks: int | None = None
            try:
                observed_start_ticks = (
                    request_storage.read_linux_process_start_time_ticks(
                        guardian.pid))
            except (OSError, ValueError) as e:
                fut.request_cancel()
                raise process.BoundaryExecutionError(
                    'Could not independently attest the invocation guardian.'
                ) from e
            if observed_start_ticks != guardian.start_time_ticks:
                fut.request_cancel()
                raise process.BoundaryExecutionError(
                    'Invocation guardian birth identity changed before '
                    'durable admission.')

            # SQLite has no durable queue claim, so PID alone used to be its
            # only cancellation identity. Persist the same exact guardian
            # birth used by distributed claims for every local invocation.
            process_start_ticks = guardian.start_time_ticks
            admission_retry_seconds = (
                _QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS)
            admission_failure_reported = False
            admitted = False
            while True:
                try:
                    admitted = api_requests.try_mark_running(
                        request_id, guardian.pid,
                        request_element.execution_generation,
                        request_element.claim_token, process_start_ticks)
                    break
                except Exception as admission_error:  # pylint: disable=broad-except
                    # An authority/DB outage is not evidence that the exact
                    # managed-job attempt is stale. Keep the guardian behind
                    # its effect gate and retry until the backend returns a
                    # definitive current/stale answer. Shutdown cancels the
                    # still-pre-effect boundary.
                    if not admission_failure_reported:
                        logger.warning(
                            f'RUNNING admission for request {request_id} is '
                            'not yet durable; retaining its pre-effect '
                            f'boundary and retrying: {common_utils.format_exception(admission_error)}'
                        )
                        admission_failure_reported = True
                    if self._cancel_event.wait(admission_retry_seconds):
                        fut.request_cancel()
                        return
                    admission_retry_seconds = min(
                        admission_retry_seconds * 2,
                        _QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS)
            if not admitted:
                logger.warning(f'Request {request_id} is already finished or '
                               'cancelled; cancelling before effect admission')
                fut.request_cancel()
                return
            if request_storage.role_is_draining():
                # Admission and the marker can race. The guarded invocation
                # has not crossed its effect gate yet, so retain its monitor
                # and converge a real cancelled/quiescent result.
                self.request_shutdown()
                fut.request_cancel()
                return
            fut.admit()

            logger.info(f'[{self}] Submitted request: {request_id}')
        except (Exception, SystemExit) as e:  # pylint: disable=broad-except
            # Catch any other exceptions to avoid crashing the worker process.
            logger.error(
                f'[{self}] Error processing request: '
                f'{request_id if request_id is not None else ""} '
                f'{common_utils.format_exception(e, use_bracket=True)}')
            if request_id is not None and fut is None:
                # The failure happened before a future was obtained, i.e. the
                # request was never handed to the executor pool. The element
                # is already popped from the queue: without terminalizing the
                # row here it would stay PENDING forever and clients polling
                # /api/get would block indefinitely.
                # If a future exists, the request was submitted successfully
                # and may already be RUNNING (or even finished); its lifecycle
                # is owned by handle_task_result, so only log here.
                self._fail_stranded_request(request_id, e, request_element)
        finally:
            if reservation is not None and fut is None:
                # No-item/CAS-loss/cancellation paths still own the capability.
                # A submit failure may already have consumed it, in which case
                # the stale-token error proves there is nothing left to release.
                try:
                    executor.release_idle_worker_reservation(reservation)
                except ValueError:
                    pass

    def _fail_stranded_request(
            self, request_id: str, e: BaseException,
            request_element: queue_base.QueueItem | None) -> None:
        """Durably fail a dequeued request that never got submitted."""
        claim_context_token = request_storage.activate_execution_claim(
            request_id, request_element.execution_generation
            if request_element is not None else 0, request_element.claim_token
            if request_element is not None else None)
        try:
            api_requests.set_exception_stacktrace(e)
            if (request_element is not None and
                    request_element.claim_token is not None):
                # No Future exists, but the exact claimed delivery remains our
                # responsibility. The same parent convergence protocol used by
                # result monitors atomically terminalizes the pre-effect row
                # and publishes its receipt, retrying transient database loss.
                self._converge_execution_completion(
                    request_storage.ExecutionClaim(
                        request_id, request_element.execution_generation,
                        request_element.claim_token,
                        request_element.worker_instance_id),
                    error=e,
                    terminal_cause='dispatcher_submit_failed')
            else:
                request_storage.get_request_backend(
                ).transition_request_terminal(request_id,
                                              api_requests.RequestStatus.FAILED,
                                              'dispatcher_submit_failed',
                                              error=e)
        except (Exception, SystemExit) as recovery_e:  # pylint: disable=broad-except
            # Never let the recovery itself crash the dispatcher thread.
            logger.error(
                f'[{self}] Failed to mark stranded request {request_id} as '
                f'failed: '
                f'{common_utils.format_exception(recovery_e, use_bracket=True)}'
            )
        finally:
            request_storage.deactivate_execution_claim(claim_context_token)

    def _mark_executor_free(self) -> None:
        """Increment the free-executor gauge for this worker's schedule type.

        Called after the exact boundary result has been settled and its receipt
        released, so the gauge never advertises capacity still retained by a
        guardian.
        """
        if not metrics_utils.METRICS_ENABLED:
            return
        if self.schedule_type == api_requests.ScheduleType.LONG:
            metrics_utils.SKY_APISERVER_LONG_EXECUTORS.inc()
        elif self.schedule_type == api_requests.ScheduleType.SHORT:
            metrics_utils.SKY_APISERVER_SHORT_EXECUTORS.inc()

    def _mark_executor_poisoned(self) -> None:
        """Stop advertising every slot in a permanently poisoned facade."""
        if not metrics_utils.METRICS_ENABLED:
            return
        try:
            if self.schedule_type == api_requests.ScheduleType.LONG:
                metrics_utils.SKY_APISERVER_LONG_EXECUTORS.set(0)
            elif self.schedule_type == api_requests.ScheduleType.SHORT:
                metrics_utils.SKY_APISERVER_SHORT_EXECUTORS.set(0)
        except Exception:  # pylint: disable=broad-except
            logger.exception('Failed to publish poisoned executor capacity.')

    def handle_task_result(self, fut: concurrent.futures.Future,
                           request_element: queue_base.QueueItemLike) -> None:
        original_request_element = request_element
        request_element = queue_base.normalize_queue_item(request_element)
        claim_context_token = request_storage.activate_execution_claim(
            request_element.request_id, request_element.execution_generation,
            request_element.claim_token)
        try:
            if request_element.claim_token is not None:
                claim = request_storage.ExecutionClaim(
                    request_element.request_id,
                    request_element.execution_generation,
                    request_element.claim_token,
                    request_element.worker_instance_id)
                retry_seconds = _QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS
                backend_failure_reported = False
                backend: request_storage.RequestBackend | None = None
                interval: float | None = None
                while True:
                    try:
                        backend = request_storage.get_request_backend()
                        interval = backend.claim_heartbeat_interval_seconds
                        break
                    except Exception as backend_error:  # pylint: disable=broad-except
                        if not backend_failure_reported:
                            logger.warning(
                                f'Cannot yet acquire the durable request '
                                f'backend for {claim.request_id}; retaining '
                                f'its monitor and retrying: '
                                f'{common_utils.format_exception(backend_error)}'
                            )
                            backend_failure_reported = True
                        time.sleep(retry_seconds)
                        retry_seconds = min(
                            retry_seconds * 2,
                            _QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS)

                assert backend is not None
                if interval is not None:
                    # One registered thread owns both lease maintenance and
                    # Future completion. This avoids a second thread-start gap
                    # after the callable has already been accepted by the pool.
                    future_done = threading.Event()
                    fut.add_done_callback(lambda _future: future_done.set())
                    revocation_reported = False
                    while not future_done.is_set():
                        try:
                            if not backend.heartbeat_claim(claim):
                                if backend.interrupt_cancelled_claim(claim):
                                    if not revocation_reported:
                                        logger.info(
                                            f'Signalled cancellation for '
                                            f'{claim.request_id} generation '
                                            f'{claim.execution_generation}; '
                                            'waiting for exact execution '
                                            'quiescence.')
                                elif not revocation_reported:
                                    logger.warning(
                                        f'Execution claim for '
                                        f'{claim.request_id} became stale; '
                                        'subsequent writes are fenced and '
                                        'exact cancellation delivery will be '
                                        'retried until the wrapper exits.')
                                # Signal delivery is not quiescence. Keep
                                # re-delivering until this exact Future proves
                                # that the wrapper returned.
                                revocation_reported = True
                        except Exception as heartbeat_error:  # pylint: disable=broad-except
                            logger.warning(
                                f'Failed to heartbeat execution claim for '
                                f'{claim.request_id}: '
                                f'{common_utils.format_exception(heartbeat_error)}'
                            )
                        future_done.wait(interval)
            self._handle_task_result(fut, original_request_element)
        finally:
            request_storage.deactivate_execution_claim(claim_context_token)

    def _converge_execution_completion(
            self,
            claim: request_storage.ExecutionClaim,
            error: BaseException | None = None,
            terminal_cause: str = 'handler_failed') -> bool:
        """Retry parent-owned outcome/receipt delivery until definitive."""
        retry_seconds = _QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS
        failure_reported = False
        while True:
            try:
                backend = request_storage.get_request_backend()
                return backend.converge_execution_completion(
                    claim, error=error, terminal_cause=terminal_cause)
            except Exception as convergence_error:  # pylint: disable=broad-except
                if not failure_reported:
                    logger.warning(
                        f'Execution completion for {claim.request_id} '
                        f'generation {claim.execution_generation} is not yet '
                        'durable; retaining its monitor and retrying: '
                        f'{common_utils.format_exception(convergence_error)}')
                    failure_reported = True
                time.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2,
                                    _QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS)

    def _converge_local_execution_quiescence(
            self, request_id: str, fut: concurrent.futures.Future) -> None:
        """Publish one SQLite receipt before releasing its exact boundary."""
        if not isinstance(fut, process.InvocationFuture):
            return
        guardian = fut.guardian_identity
        retry_seconds = _QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS
        failure_reported = False
        while True:
            try:
                backend = request_storage.get_request_backend()
                if not backend.supports_local_execution_quiescence:
                    return
                backend.acknowledge_local_execution_quiescence(
                    request_id, guardian.pid, guardian.start_time_ticks)
                return
            except Exception as convergence_error:  # pylint: disable=broad-except
                if not failure_reported:
                    logger.warning(
                        f'Local execution receipt for {request_id} is not yet '
                        'durable; retaining its boundary and retrying: '
                        f'{common_utils.format_exception(convergence_error)}')
                    failure_reported = True
                time.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2,
                                    _QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS)

    def _handoff_execution_retry(self, claim: request_storage.ExecutionClaim,
                                 status_msg: str,
                                 retry_wait_seconds: float) -> bool:
        """Retry an atomic durable handoff until its outcome is known."""
        retry_seconds = _QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS
        failure_reported = False
        while True:
            try:
                backend = request_storage.get_request_backend()
                handed_off = backend.handoff_execution_retry(
                    claim, status_msg, retry_wait_seconds)
                if handed_off is None:
                    raise RuntimeError(
                        'A durable execution claim requires an atomic retry '
                        'handoff implementation.')
                return handed_off
            except Exception as handoff_error:  # pylint: disable=broad-except
                if not failure_reported:
                    logger.warning(
                        f'Retry handoff for {claim.request_id} generation '
                        f'{claim.execution_generation} is not yet durable; '
                        f'retaining its boundary and retrying: '
                        f'{common_utils.format_exception(handoff_error)}')
                    failure_reported = True
                time.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2,
                                    _QUIESCENCE_RECEIPT_RETRY_MAX_SECONDS)

    @staticmethod
    def _acknowledge_boundary_receipt(fut: concurrent.futures.Future) -> None:
        """Release a proven boundary after its durable convergence attempt."""
        if not isinstance(fut, process.InvocationFuture):
            return
        if fut.boundary_result is None:
            raise RuntimeError('A non-ambiguous invocation Future completed '
                               'without a boundary result.')
        fut.acknowledge_receipt()

    def _handle_task_result(self, fut: concurrent.futures.Future,
                            request_element: queue_base.QueueItemLike) -> None:
        requeue_element = request_element
        request_element = queue_base.normalize_queue_item(request_element)
        claim = (request_storage.ExecutionClaim(
            request_element.request_id, request_element.execution_generation,
            request_element.claim_token, request_element.worker_instance_id)
                 if request_element.claim_token is not None else None)
        boundary_ambiguous = False
        boundary_released = False
        try:
            try:
                fut.result()
            except process.AmbiguousBoundaryError as e:
                boundary_ambiguous = True
                self._handle_ambiguous_boundary(e)
                logger.error(
                    f'Request {request_element.request_id} lost both execution '
                    'boundary owners without family-quiescence proof; retaining '
                    'its durable claim fail closed: '
                    f'{common_utils.format_exception(e, use_bracket=True)}')
                # Neither terminalize, receipt, nor requeue. Guardian absence is
                # not proof that the inner family stopped, and there is no
                # second PID-death recovery path.
                return
            except concurrent.futures.CancelledError as e:
                if claim is None:
                    self._converge_local_execution_quiescence(
                        request_element.request_id, fut)
                if claim is not None:
                    self._converge_execution_completion(
                        claim,
                        error=e,
                        terminal_cause='dispatcher_submit_failed')
                else:
                    api_requests.set_request_failed(request_element.request_id,
                                                    e)
            except exceptions.ExecutionRetryableError as e:
                if claim is None:
                    self._converge_local_execution_quiescence(
                        request_element.request_id, fut)
                request_id = request_element.request_id
                # Clamp to avoid ValueError from time.sleep() on a negative wait.
                retry_wait_seconds = max(0, e.retry_wait_seconds)
                # A pause (ExecutionPausedError) may carry a continue condition
                # that owns how to wait for the resume signal; without one, fall
                # back to a fixed backoff. Either way the wait runs in this
                # monitor thread, while the receipt-retained boundary continues
                # to own the executor slot.
                condition = getattr(e, 'continue_condition', None)
                # Surface why we are retrying, not just the wait time. status_msg
                # is a single-line field, so strip color and collapse whitespace.
                request = api_requests.get_request(
                    request_id, fields=['name', 'request_body'])
                safe_error = (api_requests.sanitize_request_error(
                    request.name, e, request.request_body)
                              if request is not None else e)
                reason = ' '.join(
                    common_utils.remove_color(str(safe_error)).split())
                if len(reason) > _RETRY_STATUS_MSG_REASON_MAX_LEN:
                    reason = reason[:_RETRY_STATUS_MSG_REASON_MAX_LEN].rstrip(
                    ) + '...'
                retry_suffix = ('waiting to resume'
                                if condition is not None and claim is None else
                                f'retrying in {retry_wait_seconds}s')
                status_msg = (f'{reason} ({retry_suffix})'
                              if reason else retry_suffix.capitalize())
                if claim is not None:
                    # The returned Future proves that the complete invocation
                    # family stopped.  Consume that proof directly into one
                    # delayed durable delivery; never retain this monitor (or
                    # its finite executor capability) for the backoff period.
                    if self._handoff_execution_retry(claim, status_msg,
                                                     retry_wait_seconds):
                        logger.info(f'Rescheduled request {request_id} for '
                                    f'retry in {retry_wait_seconds}s')
                    else:
                        # Cancellation or an owner transition may have won.
                        # Publish the same family proof to any retained exact
                        # tombstone, without reopening or requeueing it.
                        self._converge_execution_completion(claim)
                        logger.info(
                            f'Dropping retry for request {request_id}: its '
                            'exact execution identity is obsolete')
                    return
                # Set request to WAITING status for visibility. Cancellation can
                # win after the executor raises but before this monitor handles
                # the future. Only the RUNNING owner may hand the request back
                # to the retry queue; otherwise this write would resurrect
                # CANCELLED.
                with api_requests.update_request(request_id) as request_task:
                    if (request_task is None or request_task.status
                            != api_requests.RequestStatus.RUNNING):
                        logger.info(
                            f'Dropping retry for request {request_id}: request '
                            'is gone or no longer running')
                        return
                    request_task.status = api_requests.RequestStatus.WAITING
                    request_task.status_msg = status_msg
                # Local backends have no durable claim to consume atomically.
                # The Future nevertheless proves this invocation family is
                # empty, so release its disposable boundary and finite worker
                # capability before the compatibility wait.  SQLite startup
                # recovery owns a WAITING row if this monitor then disappears.
                self._acknowledge_boundary_receipt(fut)
                self._mark_executor_free()
                boundary_released = True
                try:
                    if condition is not None:
                        should_reschedule = condition.wait(
                            is_cancelled=lambda: _request_is_gone_or_cancelled(
                                request_id),
                            fallback_wait_seconds=retry_wait_seconds)
                    else:
                        time.sleep(retry_wait_seconds)
                        should_reschedule = True
                except Exception as wait_err:  # pylint: disable=broad-except
                    logger.error(
                        f'Continue-condition wait failed for {request_id}: '
                        f'{common_utils.format_exception(wait_err)}')
                    time.sleep(retry_wait_seconds)
                    should_reschedule = True
                if (should_reschedule and
                        not _request_is_gone_or_cancelled(request_id)):
                    queue = _get_queue(self.schedule_type)
                    queue.put(requeue_element)
                    logger.info(f'Rescheduled request {request_id} for retry')
            # The result monitor is a synchronous thread and transports child
            # BaseExceptions as durable request outcomes.
            except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                if claim is None:
                    self._converge_local_execution_quiescence(
                        request_element.request_id, fut)
                # A transported callable exception still proves that this exact
                # invocation's family drained. Terminalize only if the child did
                # not already persist a terminal result, then durably deliver
                # its receipt.
                if claim is not None:
                    self._converge_execution_completion(claim, error=e)
                else:
                    api_requests.set_request_failed(request_element.request_id,
                                                    e)
            else:
                if claim is not None:
                    self._converge_execution_completion(claim)
                else:
                    self._converge_local_execution_quiescence(
                        request_element.request_id, fut)
        finally:
            if not boundary_ambiguous and not boundary_released:
                try:
                    self._acknowledge_boundary_receipt(fut)
                finally:
                    self._mark_executor_free()

    def run(self) -> None:
        # Handle the SIGTERM signal to abort the executor process gracefully.
        proc_group = f'{self.schedule_type.value}'
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, _sigterm_handler)
            setproctitle.setproctitle(f'SkyPilot:worker:{proc_group}')
        queue = _get_queue(self.schedule_type)

        # Use concurrent.futures.ProcessPoolExecutor instead of
        # multiprocessing.Pool because the former is more efficient with the
        # support of lazy creation of worker processes.
        # We use executor instead of individual multiprocessing.Process to avoid
        # the overhead of forking a new process for each request, which can be
        # about 1s delay.
        executor = None
        try:
            # Pass the main process's clean env snapshot so workers (incl.
            # lazy-spawned burst workers) record the same pre-pollution env
            # regardless of when they spawn.
            executor = process.BurstableExecutor(
                garanteed_workers=self.garanteed_parallelism,
                burst_workers=self.burstable_parallelism,
                initializer=executor_initializer,
                initargs=(proc_group, clean_env_module.get_clean_server_env()),
                on_ambiguous_boundary=self._handle_ambiguous_boundary)
            # Initialize the appropriate gauge for the number of free executors
            total_executors = (self.garanteed_parallelism +
                               self.burstable_parallelism)
            if metrics_utils.METRICS_ENABLED:
                if self.schedule_type == api_requests.ScheduleType.LONG:
                    metrics_utils.SKY_APISERVER_LONG_EXECUTORS.set(
                        total_executors)
                elif self.schedule_type == api_requests.ScheduleType.SHORT:
                    metrics_utils.SKY_APISERVER_SHORT_EXECUTORS.set(
                        total_executors)
            while not self._cancel_event.is_set():
                self.process_request(executor, queue)
        # TODO(aylei): better to distinct between KeyboardInterrupt and SIGTERM.
        except KeyboardInterrupt:
            pass
        finally:
            # In most cases, here we receive either ctrl-c in foreground
            # execution or SIGTERM on server exiting. Gracefully exit the
            # worker process and the executor.
            # TODO(aylei): worker may also be killed by system daemons like
            # OOM killer, crash the API server or recreate the worker process
            # to avoid broken state in such cases.
            logger.info(f'[{self}] Worker process interrupted')
            if executor is not None:
                pending_reported = False
                while True:
                    try:
                        executor.shutdown()
                    except process.BoundaryShutdownPendingError as e:
                        # Result monitors retain receipt-required guardians while
                        # their exact database convergence is unavailable. Keep
                        # this dispatcher—and therefore its runtime ownership—
                        # alive and retry the authoritative shutdown instead of
                        # caching a transient proof gap forever.
                        if not pending_reported:
                            logger.info(
                                f'[{self}] Waiting for exact executor boundary '
                                f'shutdown: {common_utils.format_exception(e)}')
                            pending_reported = True
                        time.sleep(_QUIESCENCE_RECEIPT_RETRY_INITIAL_SECONDS)
                        continue
                    # The dispatcher thread must retain any shutdown proof
                    # failure for the runtime supervisor.
                    except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                        self._shutdown_error = e
                        logger.error(
                            f'[{self}] Executor shutdown did not prove every '
                            f'child absent: {type(e).__name__}: {e}')
                    break


@annotations.lru_cache(scope='global', maxsize=None)
def _get_queue(schedule_type: api_requests.ScheduleType) -> RequestQueue:
    factory = _queue_factory
    if factory is None:
        factory = queue_base.get_queue_backend_factory()
    assert factory is not None
    return RequestQueue(factory.create_queue(schedule_type.value))


# Request names where a non-explicit workspace pick is worth surfacing
# at INFO level (i.e. visible in the streamed CLI output, not just debug
# logs). Resource-creating commands record the resolved workspace into
# durable state (cluster.workspace / job_info.workspace) — users care
# which workspace that ended up being. Read-only commands resolve the
# same way under the hood but the log line would just be noise.
#
# To extend coverage to other resource-creating verbs (e.g. SERVE_UP),
# add the request_name here.
_RESOURCE_CREATING_REQUEST_NAMES_FOR_RESOLUTION_LOG = {
    server_constants.REQUEST_NAME_PREFIX +
    request_names.RequestName.CLUSTER_LAUNCH.value,
    server_constants.REQUEST_NAME_PREFIX +
    request_names.RequestName.JOBS_LAUNCH.value,
}

# Sources we DON'T announce, even on a resource-creating request:
#   EXPLICIT          — the user already named the workspace; repeating
#                       it in the log is noise.
#   DEFAULT_FALLBACK  — landing on 'default' is the pre-existing implicit
#                       behavior; surfacing it on every launch for every
#                       single-default user would clutter output for the
#                       common case while telling them nothing new.
# PREFERRED / SINGLE_MEMBERSHIP are the cases worth surfacing — the user
# may not realize where the resource landed.
_SILENT_WORKSPACE_RESOLUTION_SOURCES = {
    workspace_constants.WORKSPACE_SOURCE_EXPLICIT,
    workspace_constants.WORKSPACE_SOURCE_DEFAULT_FALLBACK,
}


def _should_apply_workspace_resolver(client_api_version: int | None) -> bool:
    """Returns True iff the per-user workspace resolver should run for
    this request. Two gates, in order:

      (a) skip when the client API version is below the version that
          added /users/me/workspace + WorkspaceAmbiguousError handling —
          old clients wouldn't know how to interpret the new error
          format, so preserve the legacy permission-denied path that
          they already handle. The version travels on the RequestBody
          itself (`client_api_version` field) so it is available in the
          worker process; `versions.get_remote_api_version()` returns
          None in workers because the underlying ContextVar set by
          APIVersionMiddleware does not propagate across process
          boundaries.
      (b) skip when active_workspace was explicitly set on the wire
          (anywhere in the merged config) — respect explicit user intent;
          preferred MUST be ignored when the user names a workspace.
    """
    if (client_api_version is None or client_api_version
            < server_constants.MIN_PREFERRED_WORKSPACE_API_VERSION):
        return False
    return not skypilot_config.is_active_workspace_set()


def _resolve_request_execution_user(
        request_body: payloads.RequestBody,
        *,
        require_existing_user: bool = False) -> models.User:
    """Resolve current identity without replaying stale queued metadata."""
    submitted = models.User(id=request_body.env_vars[constants.USER_ID_ENV_VAR],
                            name=request_body.env_vars[constants.USER_ENV_VAR])
    if not require_existing_user:
        _, current = global_user_state.add_or_update_user(submitted,
                                                          return_user=True)
    else:
        current = global_user_state.get_user(submitted.id)
        if current is None:
            raise RuntimeError('Bound internal request owner no longer exists.')
    if (current.id != submitted.id or not isinstance(current.name, str) or
            not current.name):
        raise RuntimeError('Request execution user identity is malformed.')
    return current


def _reserved_fill_authorized_workspace(
    request_body: payloads.RequestBody,
    request_id: str,
) -> str | None:
    """Return the exact DB-authorized workspace for internal reserved fill."""
    if (not isinstance(request_body, payloads.LaunchBody) or
            not request_body.is_launched_by_sky_serve_controller or
            request_body.is_launched_by_jobs_controller):
        return None
    owner_user_id = request_body.env_vars.get(constants.USER_ID_ENV_VAR)
    override = request_body.override_skypilot_config
    workspace = (override.get('active_workspace')
                 if isinstance(override, dict) else None)
    if (not isinstance(owner_user_id, str) or not owner_user_id or
            not isinstance(workspace, str) or not workspace or
            request_body.override_skypilot_config_path is not None):
        return None
    if not ordinary_launch_binding.reserved_fill_binding_authorizes_workspace(
            request_id, owner_user_id, workspace):
        return None
    return workspace


@contextlib.contextmanager
def override_request_env_and_config(
        request_body: payloads.RequestBody,
        request_id: str,
        request_name: str,
        *,
        require_existing_user: bool = False) -> Generator[None, None, None]:
    """Override the environment and SkyPilot config for a request."""
    original_env = os.environ.copy()
    try:
        # Unset SKYPILOT_DEBUG by default, to avoid the value set on the API
        # server affecting client requests. If set on the client side, it is
        # restored from the request body below.
        os.environ.pop('SKYPILOT_DEBUG', None)
        request_body.env_vars.pop(constants.ENV_VAR_DB_CONNECTION_URI, None)
        request_body.env_vars.pop(
            kubernetes_adaptor.IN_CLUSTER_CONTEXT_NAME_ENV_VAR, None)
        payloads.remove_server_owned_env_vars(request_body.env_vars)
        os.environ.update(request_body.env_vars)
        reserved_fill_workspace = _reserved_fill_authorized_workspace(
            request_body, request_id)
        user = _resolve_request_execution_user(
            request_body,
            require_existing_user=(require_existing_user or
                                   reserved_fill_workspace is not None))
        # Keep the persisted body immutable for exact retries/digest hydration,
        # but execute under the current display name for its immutable user ID.
        user_name = user.name
        assert isinstance(user_name, str)
        os.environ[constants.USER_ID_ENV_VAR] = user.id
        os.environ[constants.USER_ENV_VAR] = user_name
        using_remote_api_server = request_body.using_remote_api_server

        # Force color to be enabled.
        os.environ['CLICOLOR_FORCE'] = '1'
        server_common.reload_for_new_request(
            client_entrypoint=request_body.entrypoint,
            client_command=request_body.entrypoint_command,
            using_remote_api_server=using_remote_api_server,
            user=user,
            request_id=request_id)
        logger.debug(
            f'override path: {request_body.override_skypilot_config_path}')
        with skypilot_config.override_skypilot_config(
                request_body.override_skypilot_config,
                request_body.override_skypilot_config_path):
            # Skip permission check for sky.workspaces.get request
            # as it is used to determine which workspaces the user
            # has access to.
            if request_name == 'sky.workspaces.get':
                logger.debug(f'{request_id} skipping workspace check for '
                             f'{request_name}')
                yield
            else:
                # If the client did not explicitly set active_workspace,
                # resolve it from the user's memberships (preferred ->
                # default if accessible -> single-membership) instead of
                # always landing on the bare 'default' literal. Explicit
                # intent (any value, including 'default') is passed through
                # unchanged. See _should_apply_workspace_resolver for the
                # exact gate conditions (client API version and
                # explicit-intent respect).
                workspace_ctx: contextlib.AbstractContextManager = (
                    contextlib.nullcontext())
                # Read the client's API version from the request body, not
                # from versions.get_remote_api_version() — the ContextVar
                # the latter reads is set by APIVersionMiddleware in the
                # FastAPI async context but does not propagate into worker
                # processes (BurstableExecutor = ProcessPoolExecutor).
                client_api_version = getattr(request_body, 'client_api_version',
                                             None)
                if _should_apply_workspace_resolver(client_api_version):
                    resolution = workspaces_core.resolve_workspace_for_user(
                        user)
                    workspace_ctx = (skypilot_config.local_active_workspace_ctx(
                        resolution.workspace))
                    logger.debug(f'{request_id} resolved workspace '
                                 f'{resolution.workspace!r} from '
                                 f'{resolution.source} for user {user.name}')
                    # For resource-creating commands, surface the
                    # resolver's pick at INFO level so the user sees
                    # which workspace their cluster / job actually
                    # landed in. Two filters compose:
                    #   - request_name whitelist (resource-creating verbs)
                    #   - source NOT in the silent set (EXPLICIT /
                    #     DEFAULT_FALLBACK) — EXPLICIT repeats what the
                    #     user just said; DEFAULT_FALLBACK is the silent
                    #     pre-existing behavior. Only PREFERRED /
                    #     SINGLE_MEMBERSHIP are worth surfacing.
                    if (request_name in
                            _RESOURCE_CREATING_REQUEST_NAMES_FOR_RESOLUTION_LOG
                            and resolution.source
                            not in _SILENT_WORKSPACE_RESOLUTION_SOURCES):
                        logger.info(f'Using workspace {resolution.workspace!r} '
                                    f'(source: {resolution.source}).')
                with workspace_ctx:
                    active_workspace = skypilot_config.get_active_workspace()
                    if reserved_fill_workspace != active_workspace:
                        try:
                            # Reject requests that the user does not have
                            # permission to access.
                            workspaces_core.reject_request_for_unauthorized_workspace(  # pylint: disable=line-too-long
                                user)
                        except exceptions.PermissionDeniedError as e:
                            logger.debug(
                                f'{request_id} permission denied to workspace: '
                                f'{active_workspace}: {e}')
                            raise e
                    if event_models.request_kind(request_name) is not None:
                        if not api_requests.set_event_workspace(
                                request_id,
                                skypilot_config.get_active_workspace()):
                            raise RuntimeError(
                                'The request lost its execution fence before '
                                'its operational event workspace was '
                                'persisted.')
                    logger.debug(f'{request_id} permission granted to '
                                 f'{request_name} request')
                    yield
    finally:
        # We need to call the save_timeline() since atexit will not be
        # triggered as multiple requests can be sharing the same process.
        timeline.save_timeline()
        # Restore the original environment variables, so that a new request
        # won't be affected by the previous request, e.g. SKYPILOT_DEBUG
        # setting, etc. This is necessary as our executor is reusing the
        # same process for multiple requests.
        os.environ.clear()
        os.environ.update(original_env)


@contextlib.contextmanager
def _controller_execution_environment(
    controller_generation: int | None,
    controller_instance_id: str | None,
) -> Generator[None, None, None]:
    """Expose the durable controller fence to one claimed request."""
    if controller_generation is None:
        yield
        return
    if controller_instance_id is None:
        raise RuntimeError('A controller claim must have an owner instance.')

    # Runtime import avoids loading the PostgreSQL backend for the legacy
    # local and multiprocessing executors.
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import postgres
    generation_env_var = postgres.CONTROLLER_GENERATION_ENV_VAR
    instance_env_var = postgres.CONTROLLER_INSTANCE_ID_ENV_VAR
    previous_generation = os.environ.get(generation_env_var)
    previous_instance = os.environ.get(instance_env_var)
    os.environ[generation_env_var] = str(controller_generation)
    os.environ[instance_env_var] = controller_instance_id
    try:
        yield
    finally:
        if previous_generation is None:
            os.environ.pop(generation_env_var, None)
        else:
            os.environ[generation_env_var] = previous_generation
        if previous_instance is None:
            os.environ.pop(instance_env_var, None)
        else:
            os.environ[instance_env_var] = previous_instance


def _sigterm_handler(signum: int, frame: Optional['types.FrameType']) -> None:
    raise KeyboardInterrupt


# Set by _request_execution_wrapper; read by _gated_sigterm_handler.
_in_request_execution: bool = False


def _gated_sigterm_handler(signum: int,
                           frame: Optional['types.FrameType']) -> None:
    """Raise KeyboardInterrupt only while actively executing a request.

    The process is disposable, but a duplicate signal can arrive while the
    wrapper is already unwinding.  Raise only once and let the invocation
    warden own complete descendant termination and reaping.
    """
    del signum, frame
    global _in_request_execution  # pylint: disable=global-statement
    if _in_request_execution:
        _in_request_execution = False
        raise KeyboardInterrupt


def _enrich_event_target_id(request_id: str, cluster_name: str | None) -> None:
    """Best-effort capture of the cluster generation for an event."""
    if cluster_name is None:
        return
    try:
        cluster_hash = global_user_state.get_cluster_hash_for_name(cluster_name)
        if cluster_hash is not None:
            api_requests.set_event_target_id(request_id, cluster_hash)
    except Exception as e:  # pylint: disable=broad-except
        # Event target enrichment is observability. The authoritative request
        # result must never depend on this optional identity lookup.
        logger.warning(
            f'Failed to enrich operational event target for {request_id}: '
            f'{common_utils.format_exception(e)}')


@contextlib.contextmanager
def _capture_event_target(
        request_id: str, request_name: str,
        cluster_name: str | None) -> Generator[None, None, None]:
    """Capture the stable cluster generation around an opted-in operation."""
    event_kind = event_models.request_kind(request_name)
    if event_kind is None:
        yield
        return
    # A launch can replace an existing cluster generation, so only capture its
    # target after the handler has run. Other lifecycle operations capture
    # before and after: teardown removes the authoritative cluster record,
    # while a failed operation may still create or replace it.
    if event_kind != event_api_models.EventKind.CLUSTER_LAUNCH:
        _enrich_event_target_id(request_id, cluster_name)
    try:
        yield
    finally:
        _enrich_event_target_id(request_id, cluster_name)


def _durable_managed_job_origin(
    request_task: api_requests.Request,
) -> tuple[int, str, int, int, str] | None:
    """Decode a complete canonical origin from one durable request row."""
    raw = (request_task.managed_job_id,
           request_task.managed_job_controller_instance_id,
           request_task.managed_job_controller_generation,
           request_task.managed_job_controller_slot_id,
           request_task.managed_job_controller_slot_attempt)
    if all(value is None for value in raw):
        return None
    if any(value is None for value in raw):
        raise RuntimeError('Managed-job request origin is incomplete.')
    job_id, instance_id, generation, slot_id, attempt = raw
    if (isinstance(job_id, bool) or not isinstance(job_id, int) or
            not isinstance(instance_id, str) or isinstance(generation, bool) or
            not isinstance(generation, int) or isinstance(slot_id, bool) or
            not isinstance(slot_id, int) or not isinstance(attempt, str)):
        raise RuntimeError('Managed-job request origin is malformed.')
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
        canonical_attempt = str(uuid.UUID(attempt))
    except ValueError as e:
        raise RuntimeError('Managed-job request origin is malformed.') from e
    if (job_id <= 0 or generation <= 0 or slot_id < 0 or
            canonical_instance_id != instance_id or
            canonical_attempt != attempt):
        raise RuntimeError('Managed-job request origin is not canonical.')
    return job_id, instance_id, generation, slot_id, attempt


@contextlib.contextmanager
def _verified_managed_job_execution_origin(
    request_task: api_requests.Request,
    expected_origin: tuple[int, str, int, int, str] | None,
) -> Generator[None, None, None]:
    """Install only the exact queue-verified origin for this handler."""
    durable_origin = _durable_managed_job_origin(request_task)
    if durable_origin != expected_origin:
        raise RuntimeError(
            'Managed-job request origin changed after its verified claim.')
    token = versions.set_managed_job_origin(durable_origin)
    try:
        yield
    finally:
        versions.reset_managed_job_origin(token)


def _request_execution_wrapper(
        request_id: str,
        ignore_return_value: bool,
        num_db_connections_per_worker: int = 0,
        execution_generation: int = 0,
        claim_token: str | None = None,
        worker_instance_id: str | None = None,
        managed_job_origin: tuple[int, str, int, int, str] | None = None
) -> None:
    """Wrapper for a request execution.

    It wraps the execution of a request to:
    1. Deserialize the request from the request database and serialize the
       return value/exception in the request database;
    2. Update the request status based on the execution result;
    3. Redirect the stdout and stderr of the execution to log file;
    4. Handle the SIGTERM signal to abort the request gracefully.
    5. Maintain the lifecycle of the temp dir used by the request.
    """
    pid = os.getpid()
    proc = psutil.Process(pid)
    rss_begin = proc.memory_info().rss
    db_utils.set_max_connections(num_db_connections_per_worker)
    # Handle the SIGTERM signal to abort the request processing gracefully.
    # Only set up signal handlers in the main thread, as signal.signal() raises
    # ValueError if called from a non-main thread (e.g., in tests).
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _gated_sigterm_handler)

    logger.info(f'Running request {request_id} with pid {pid}')

    original_stdout = original_stderr = None

    def _save_current_output() -> None:
        """Save the current stdout and stderr file descriptors."""
        nonlocal original_stdout, original_stderr
        original_stdout = os.dup(sys.stdout.fileno())
        original_stderr = os.dup(sys.stderr.fileno())

    def _redirect_output(file: TextIO) -> None:
        """Redirect stdout and stderr to the log file."""
        # Get the file descriptor from the file object
        fd = file.fileno()
        # Copy this fd to stdout and stderr
        os.dup2(fd, sys.stdout.fileno())
        os.dup2(fd, sys.stderr.fileno())

    def _restore_output() -> None:
        """Restore stdout and stderr to their original file descriptors."""
        nonlocal original_stdout, original_stderr
        if original_stdout is not None:
            os.dup2(original_stdout, sys.stdout.fileno())
            os.close(original_stdout)
            original_stdout = None

        if original_stderr is not None:
            os.dup2(original_stderr, sys.stderr.fileno())
            os.close(original_stderr)
            original_stderr = None

    request_name = None
    request_body: payloads.RequestBody | None = None
    # Set _in_request_execution inside the try so `finally` always clears it,
    # even if a SIGTERM lands before any wrapper code runs.
    global _in_request_execution  # pylint: disable=global-statement
    execution_claim_token = request_storage.activate_execution_claim(
        request_id, execution_generation, claim_token, worker_instance_id)
    try:
        _in_request_execution = True
        placement_history.reset_request_buffer()
        # The dispatcher has already published the direct-child outer guardian
        # PID/birth identity and atomically crossed RUNNING before admitting
        # this handler.  The handler must never replace that durable identity
        # with its own PID.
        request_task = api_requests.get_request(request_id)
        assert request_task is not None, request_id
        log_path = request_task.log_path
        func = request_task.entrypoint
        request_body = request_task.request_body
        request_name = request_task.name
        request_cluster_name = request_task.cluster_name
        controller_generation = request_task.controller_generation
        controller_instance_id = request_task.worker_instance_id

        # Store copies of the original stdout and stderr file descriptors
        # We do this in two steps because we should make sure to restore the
        # original values even if we are cancelled or fail during the redirect.
        _save_current_output()

        # Append to the log file instead of overwriting it since there might be
        # logs from previous retries.
        with log_path.open('a', encoding='utf-8') as f:
            # Redirect the stdout/stderr before overriding the environment and
            # config, as there can be some logs during override that needs to be
            # captured in the log file.
            _redirect_output(f)

            with sky_logging.add_debug_log_handler(request_id), \
                _verified_managed_job_execution_origin(
                    request_task, managed_job_origin), \
                override_request_env_and_config(
                    request_body,
                    request_id,
                    request_name,
                    require_existing_user=(request_task.precondition_type ==
                                           'ordinary-launch-binding.v1')), \
                _controller_execution_environment(
                    controller_generation, controller_instance_id), \
                tempstore.tempdir():
                if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
                    config = debug_dump_helpers.redact_config(
                        dict(skypilot_config.to_dict()))
                    logger.debug(f'request config: \n'
                                 f'{yaml_utils.dump_yaml_str(dict(config))}')
                (metrics_utils.SKY_APISERVER_PROCESS_EXECUTION_START_TOTAL.
                 labels(request=request_name, pid=pid).inc())
                with metrics_utils.time_it(name=request_name,
                                           group='request_execution'), \
                        _capture_event_target(
                            request_id, request_name, request_cluster_name):
                    return_value = func(**request_body.to_kwargs())
                f.flush()
        del request_task
    except KeyboardInterrupt:
        logger.info(f'Request {request_id} cancelled by user')
        # The per-invocation warden owns descendant termination and reaping.
        # Returning here reports the handler outcome; it is not process-family
        # quiescence proof.
        return
    except exceptions.ExecutionRetryableError as e:
        safe_retry_error = api_requests.sanitize_request_error(
            request_name, e, request_body)
        logger.error(safe_retry_error)
        if safe_retry_error is e:
            logger.info(e.hint)
        should_retry = False
        with api_requests.update_request(request_id) as request_task:
            if (request_task is not None and
                    request_task.status == api_requests.RequestStatus.RUNNING):
                # PostgreSQL claims retain PID + process birth identity until
                # this wrapper has published quiescence and the parent-side
                # queue handoff atomically clears the exact claim. If this
                # process hard-dies between retry intent and ``finally``, the
                # role-local owner-death observer still has a complete address.
                # Legacy unclaimed backends keep their historical PID cleanup.
                if claim_token is None:
                    request_task.pid = None
                should_retry = True
        # Yield control to the scheduler for uniform handling of retries.
        _restore_output()
        if should_retry:
            raise
        logger.info(f'Dropping retry for request {request_id}: request is gone '
                    'or no longer running')
        return
    except (Exception, SystemExit) as e:  # pylint: disable=broad-except
        safe_failure_error = api_requests.sanitize_request_error(
            request_name, e, request_body)
        api_requests.set_request_failed(request_id, e)
        # Manually reset the original stdout and stderr file descriptors early
        # so that the "Request xxxx failed due to ..." log message will be
        # written to the original stdout and stderr file descriptors.
        _restore_output()
        logger.error(f'Request {request_id} failed due to '
                     f'{common_utils.format_exception(safe_failure_error)}')
        return
    else:
        api_requests.set_request_succeeded(
            request_id, return_value if not ignore_return_value else None)
        # Manually reset the original stdout and stderr file descriptors early
        # so that the "Request xxxx failed due to ..." log message will be
        # written to the original stdout and stderr file descriptors.
        _restore_output()
        logger.info(f'Request {request_id} finished')
    finally:
        _in_request_execution = False
        request_storage.deactivate_execution_claim(execution_claim_token)
        _restore_output()
        try:
            placement_history.flush_request_buffer()
        except Exception as e:  # pylint: disable=broad-except
            # Placement history is observability. Its PostgreSQL write runs
            # after the request result is durable and must never alter it.
            logger.warning('Failed to flush placement history: '
                           f'{common_utils.format_exception(e)}')
        try:
            # Capture the peak RSS before GC.
            peak_rss = max(proc.memory_info().rss, metrics_lib.peak_rss_bytes)
            # Clear request level cache to release all memory used by the
            # request.
            annotations.clear_request_level_cache()
            with metrics_utils.time_it(name='release_memory', group='internal'):
                common_utils.release_memory()
            if request_name is not None:
                _record_memory_metrics(request_name, proc, rss_begin, peak_rss)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Failed to record memory metrics: '
                         f'{common_utils.format_exception(e)}')


_first_request = True


def _record_memory_metrics(request_name: str, proc: psutil.Process,
                           rss_begin: int, peak_rss: int) -> None:
    """Record the memory metrics for a request."""
    # Do not record full memory delta for the first request as it
    # will loads the sky core modules and make the memory usage
    # estimation inaccurate.
    global _first_request
    if _first_request:
        _first_request = False
        return
    rss_end = proc.memory_info().rss

    # Answer "how much RSS this request contributed?"
    metrics_utils.SKY_APISERVER_REQUEST_RSS_INCR_BYTES.labels(
        name=request_name).observe(max(rss_end - rss_begin, 0))
    # Estimate the memory usage by the request by capturing the
    # peak memory delta during the request execution.
    metrics_utils.SKY_APISERVER_REQUEST_MEMORY_USAGE_BYTES.labels(
        name=request_name).observe(max(peak_rss - rss_begin, 0))


async def _join_cancelled_child(task: asyncio.Task) -> BaseException | None:
    """Join a child without completing the shielded waiter exceptionally.

    Returning failures as values prevents a cancelled ``asyncio.shield`` from
    reporting the waiter's later exception as unhandled on Python 3.14. The
    caller re-raises the failure, including KeyboardInterrupt and SystemExit.
    """
    try:
        await task
    except asyncio.CancelledError:  # noqa: ASYNC103
        return None  # noqa: ASYNC104
    except BaseException as e:  # pylint: disable=broad-exception-caught
        return e
    return None


class CoroutineTask:
    """Wrapper of a background task runs in coroutine"""

    def __init__(self, task: asyncio.Task):
        self.task = task

    async def cancel(self):
        self.task.cancel()
        # Normalize the child's expected cancellation to a successful result.
        # A CancelledError raised while shielding this waiter therefore belongs
        # to this parent cleanup task on every supported Python version.
        join_task = asyncio.create_task(_join_cancelled_child(self.task))
        parent_cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(join_task)
                break
            except asyncio.CancelledError as e:  # noqa: ASYNC103
                if parent_cancellation is None:
                    parent_cancellation = e
                if not join_task.done():
                    # Keep the child's cancellation cleanup alive if this
                    # background cleanup task is itself cancelled.
                    continue  # noqa: ASYNC104
                break  # noqa: ASYNC104
        # Consume the waiter result even if parent cancellation raced with its
        # completion. Child cleanup failures retain precedence, including
        # process-control exceptions such as KeyboardInterrupt and SystemExit.
        child_error = join_task.result()
        if child_error is not None:
            raise child_error
        if parent_cancellation is not None:
            raise parent_cancellation


def check_request_thread_executor_available() -> None:
    """Check if the request thread executor is available.

    This is a best effort check to hint the client to retry other server
    processes when there is no avaiable thread worker in current one. But
    a request may pass this check and still cannot get worker on execution
    time due to race condition. In this case, the client will see a failed
    request instead of retry.

    TODO(aylei): this can be refined with a refactor of our coroutine
    execution flow.
    """
    get_request_thread_executor().check_available()


def execute_request_in_coroutine(
        request: api_requests.Request) -> CoroutineTask:
    """Execute a request in current event loop.

    Args:
        request: The request to execute.

    Returns:
        A CoroutineTask handle to operate the background task.
    """
    task = asyncio.create_task(_execute_request_coroutine(request))
    return CoroutineTask(task)


def _execute_with_config_override(func: Callable,
                                  request_body: payloads.RequestBody,
                                  request_id: str, request_name: str,
                                  require_existing_user: bool, **kwargs) -> Any:
    """Execute a function with env and config override inside a thread."""
    # Override the environment and config within this thread's context,
    # which gets copied when we call to_thread.
    with override_request_env_and_config(
            request_body,
            request_id,
            request_name,
            require_existing_user=(require_existing_user)):
        return func(**kwargs)


async def _execute_request_coroutine(request: api_requests.Request):
    """Execute a request in current event loop.

    Similar to _request_execution_wrapper, but executed as coroutine in current
    event loop. This is designed for executing tasks that are not CPU
    intensive, e.g. sky logs.
    """
    context.initialize()
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    logger.info(f'Executing request {request.request_id} in coroutine')
    func = request.entrypoint
    request_body = request.request_body
    await api_requests.update_status_async(request.request_id,
                                           api_requests.RequestStatus.RUNNING)
    # Redirect stdout and stderr to the request log path.
    try:
        hard_free_bytes = api_requests.get_request_log_storage_usage(
        ).hard_free_bytes
        original_output = ctx.redirect_log(
            request.log_path,
            max_bytes=server_constants.STREAMING_REQUEST_LOG_MAX_BYTES,
            min_free_bytes=hard_free_bytes)
    except Exception as e:  # pylint: disable=broad-except
        await api_requests.set_request_failed_async(request.request_id, e)
        logger.error(f'Failed to open request log for {request.request_id}: '
                     f'{common_utils.format_exception(e)}')
        return
    try:
        fut: asyncio.Future = context_utils.to_thread_with_executor(
            get_request_thread_executor(), _execute_with_config_override, func,
            request_body, request.request_id, request.name,
            request.precondition_type == 'ordinary-launch-binding.v1',
            **request_body.to_kwargs())
    except Exception as e:  # pylint: disable=broad-except
        ctx.redirect_log(original_output)
        safe_submission_error = api_requests.sanitize_request_error(
            request.name, e, request_body)
        await api_requests.set_request_failed_async(request.request_id, e)
        logger.error(f'Failed to run request {request.request_id} due to '
                     f'{common_utils.format_exception(safe_submission_error)}')
        return

    async def poll_task(request_id: str) -> bool:
        req_status = await api_requests.get_request_status_async(request_id)
        if req_status is None:
            raise RuntimeError('Request not found')

        if req_status.status == api_requests.RequestStatus.CANCELLED:
            ctx.cancel()
            return True

        if fut.done():
            try:
                result = await fut
                await api_requests.set_request_succeeded_async(
                    request_id, result)
            except Exception as e:  # pylint: disable=broad-except
                ctx.redirect_log(original_output)
                safe_future_error = api_requests.sanitize_request_error(
                    request.name, e, request_body)
                await api_requests.set_request_failed_async(request_id, e)
                logger.error(
                    f'Request {request_id} failed due to '
                    f'{common_utils.format_exception(safe_future_error)}')
            return True
        return False

    try:
        while True:
            res = await poll_task(request.request_id)
            if res:
                break
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        # Current coroutine is cancelled due to client disconnect, set the
        # request status for consistency.
        await api_requests.set_request_cancelled_async(request.request_id)
        raise
    # pylint: disable=broad-except
    except (Exception, KeyboardInterrupt, SystemExit) as e:
        # Handle any other error
        ctx.redirect_log(original_output)
        safe_unhandled_error = api_requests.sanitize_request_error(
            request.name, e, request_body)
        await api_requests.set_request_failed_async(request.request_id, e)
        logger.error(f'Request {request.request_id} interrupted due to '
                     'unhandled exception: '
                     f'{common_utils.format_exception(safe_unhandled_error)}')
        if safe_unhandled_error is e:
            raise
        raise safe_unhandled_error from None
    finally:
        # Always cancel the context to kill potentially running background
        # routine.
        ctx.cancel()


def _build_request(
    request_id: str,
    request_name: request_names.RequestName,
    request_body: payloads.RequestBody,
    func: Callable[P, Any],
    request_cluster_name: str | None = None,
    schedule_type: api_requests.ScheduleType = (api_requests.ScheduleType.LONG),
    is_skypilot_system: bool = False,
    auth_user: models.User | None = None,
    ignore_return_value: bool = False,
    retryable: bool = False,
    should_enqueue: bool = False,
    precondition: preconditions.Precondition | None = None,
    managed_job_origin: tuple[int, str, int, int, str] | None = None,
    client_api_version: int | None = None,
) -> api_requests.Request:
    """Build a complete request without persisting or publishing it.

    The enqueue flags (ignore_return_value, retryable) are persisted with the
    initial INSERT so a request still queued when the server restarts can be
    re-enqueued with the same dispatch semantics
    (see reenqueue_recovered_requests).
    """
    role_filter.reject_non_admin_pod_config(auth_user, request_body)
    authenticated_managed_job_origin = versions.get_managed_job_origin()
    if (managed_job_origin is not None and
            managed_job_origin != authenticated_managed_job_origin):
        raise ValueError(
            'Managed-job request origin is server-authenticated metadata.')
    managed_job_origin = authenticated_managed_job_origin
    if auth_user is not None:
        # Authenticated requests historically did not require either submitted
        # identity field because both are replaced below.
        submitted_user_hash = request_body.env_vars.get(
            constants.USER_ID_ENV_VAR, '')
    else:
        # Preserve the legacy no-auth requirement for a submitted user hash.
        submitted_user_hash = request_body.env_vars[constants.USER_ID_ENV_VAR]
    submitted_original_user = request_body.env_vars.get(constants.USER_ENV_VAR,
                                                        submitted_user_hash)
    effective_original_user, user_id = (
        server_common.resolve_effective_request_identity(
            auth_user, submitted_original_user, submitted_user_hash))
    if auth_user is not None:
        # Use the authenticated user identity as the single source of truth
        # if present.
        # Set user identity for executors.
        request_body.env_vars[constants.USER_ID_ENV_VAR] = user_id
        request_body.env_vars[constants.USER_ENV_VAR] = effective_original_user
    actor_type: str | None
    if is_skypilot_system:
        user_id = constants.SKYPILOT_SYSTEM_USER_ID
        actor_name = user_id
        actor_type = models.UserType.SYSTEM.value
        global_user_state.add_or_update_user(
            models.User(id=user_id,
                        name=user_id,
                        user_type=models.UserType.SYSTEM.value))
    elif auth_user is not None:
        actor_name = effective_original_user or user_id
        actor_type = auth_user.user_type
    else:
        actor_name = effective_original_user
        actor_type = None
    # Capture the client's API version from the FastAPI dispatch context
    # into the request body so it survives the process boundary into the
    # worker that runs the request. APIVersionMiddleware set the
    # ContextVar from the X-SkyPilot-API-Version header; reading it here
    # (still in the async dispatch process) and stamping the body is the
    # one place where header -> body translation happens, so neither the
    # Python SDK nor the dashboard need their own stamping logic. Old
    # clients (no header) yield None, which the worker-side gate treats
    # as "skip the workspace resolver".
    request_body.client_api_version = (versions.get_remote_api_version()
                                       if client_api_version is None else
                                       client_api_version)
    durable_precondition = preconditions.serialize(precondition)
    request = api_requests.Request(
        request_id=request_id,
        name=server_constants.REQUEST_NAME_PREFIX + request_name,
        entrypoint=func,
        request_body=request_body,
        status=api_requests.RequestStatus.PENDING,
        created_at=time.time(),
        schedule_type=schedule_type,
        user_id=user_id,
        cluster_name=request_cluster_name,
        file_mounts_blob_id=getattr(request_body, 'file_mounts_blob_id', None),
        ignore_return_value=ignore_return_value,
        retryable=retryable,
        should_enqueue=should_enqueue,
        precondition_type=(durable_precondition.type_name
                           if durable_precondition is not None else None),
        precondition_payload=(durable_precondition.payload
                              if durable_precondition is not None else None),
        precondition_deadline=(durable_precondition.deadline
                               if durable_precondition is not None else None),
        event_context=event_models.initial_context(
            request_name,
            actor_name=actor_name,
            actor_type=actor_type,
            cluster_name=request_cluster_name,
        ),
        managed_job_id=(managed_job_origin[0]
                        if managed_job_origin is not None else None),
        managed_job_controller_instance_id=(
            managed_job_origin[1] if managed_job_origin is not None else None),
        managed_job_controller_generation=(
            managed_job_origin[2] if managed_job_origin is not None else None),
        managed_job_controller_slot_id=(
            managed_job_origin[3] if managed_job_origin is not None else None),
        managed_job_controller_slot_attempt=(
            managed_job_origin[4] if managed_job_origin is not None else None),
    )

    return request


async def build_request_async(
    request_id: str,
    request_name: request_names.RequestName,
    request_body: payloads.RequestBody,
    func: Callable[P, Any],
    request_cluster_name: str | None = None,
    schedule_type: api_requests.ScheduleType = (api_requests.ScheduleType.LONG),
    is_skypilot_system: bool = False,
    auth_user: models.User | None = None,
    ignore_return_value: bool = False,
    retryable: bool = False,
    should_enqueue: bool = False,
    precondition: preconditions.Precondition | None = None,
    managed_job_origin: tuple[int, str, int, int, str] | None = None,
    client_api_version: int | None = None,
) -> api_requests.Request:
    """Async compatibility wrapper around the synchronous request builder."""
    return _build_request(
        request_id=request_id,
        request_name=request_name,
        request_body=request_body,
        func=func,
        request_cluster_name=request_cluster_name,
        schedule_type=schedule_type,
        is_skypilot_system=is_skypilot_system,
        auth_user=auth_user,
        ignore_return_value=ignore_return_value,
        retryable=retryable,
        should_enqueue=should_enqueue,
        precondition=precondition,
        managed_job_origin=managed_job_origin,
        client_api_version=client_api_version,
    )


async def prepare_request_async(
    request_id: str,
    request_name: request_names.RequestName,
    request_body: payloads.RequestBody,
    func: Callable[P, Any],
    request_cluster_name: str | None = None,
    schedule_type: api_requests.ScheduleType = (api_requests.ScheduleType.LONG),
    is_skypilot_system: bool = False,
    auth_user: models.User | None = None,
    ignore_return_value: bool = False,
    retryable: bool = False,
    should_enqueue: bool = False,
    precondition: preconditions.Precondition | None = None,
    managed_job_origin: tuple[int, str, int, int, str] | None = None,
) -> api_requests.Request:
    """Build and persist one ordinary request through the selected backend."""
    request = await build_request_async(request_id,
                                        request_name,
                                        request_body,
                                        func,
                                        request_cluster_name,
                                        schedule_type,
                                        is_skypilot_system,
                                        auth_user=auth_user,
                                        ignore_return_value=ignore_return_value,
                                        retryable=retryable,
                                        should_enqueue=should_enqueue,
                                        precondition=precondition,
                                        managed_job_origin=managed_job_origin)
    if not await api_requests.create_if_not_exists_async(request):
        raise exceptions.RequestAlreadyExistsError(
            f'Request {request_id} already exists.')

    request.log_path.touch()
    return request


async def schedule_request_async(
        request_id: str,
        request_name: request_names.RequestName,
        request_body: payloads.RequestBody,
        func: Callable[P, Any],
        request_cluster_name: str | None = None,
        ignore_return_value: bool = False,
        schedule_type: api_requests.ScheduleType = (
            api_requests.ScheduleType.LONG),
        is_skypilot_system: bool = False,
        precondition: preconditions.Precondition | None = None,
        retryable: bool = False,
        auth_user: models.User | None = None,
        managed_job_origin: tuple[int, str, int, int, str] | None = None
) -> None:
    """Enqueue a request to the request queue.

    Args:
        request_id: ID of the request.
        request_name: Name of the request type, e.g. "sky.launch".
        request_body: The request body containing parameters and environment
            variables.
        func: The function to execute when the request is processed.
        request_cluster_name: The name of the cluster associated with this
            request, if any.
        ignore_return_value: If True, the return value of the function will be
            ignored.
        schedule_type: The type of scheduling to use for this request, refer to
            `api_requests.ScheduleType` for more details.
        is_skypilot_system: Denote whether the request is from SkyPilot system.
        precondition: If a precondition is provided, the request will only be
            scheduled for execution when the precondition is met (returns True).
            The precondition is waited asynchronously and does not block the
            caller.
    """
    request_task = await prepare_request_async(
        request_id,
        request_name,
        request_body,
        func,
        request_cluster_name,
        schedule_type,
        is_skypilot_system,
        auth_user=auth_user,
        ignore_return_value=ignore_return_value,
        retryable=retryable,
        should_enqueue=True,
        precondition=precondition,
        managed_job_origin=managed_job_origin)
    await schedule_prepared_request(request_task, ignore_return_value,
                                    precondition, retryable)


async def schedule_prepared_request(request_task: api_requests.Request,
                                    ignore_return_value: bool = False,
                                    precondition: preconditions.Precondition |
                                    None = None,
                                    retryable: bool = False) -> None:
    """Enqueue a request to the request queue

    Args:
        request_task: The prepared request task to schedule.
        ignore_return_value: If True, the return value of the function will be
            ignored.
        precondition: If a precondition is provided, the request will only be
            scheduled for execution when the precondition is met (returns True).
            The precondition is waited asynchronously and does not block the
            caller.
        retryable: Whether the request should be retried if it fails.
    """

    async def enqueue():
        # The non-durable queue contract is intentionally the historical
        # three-tuple. PostgreSQL inserts its queue row transactionally and
        # returns above without calling this closure; durable claim metadata is
        # attached only when that backend dequeues the row. Keeping this shape
        # also preserves external queue plugins and test fixtures during M5.
        input_tuple = (request_task.request_id, ignore_return_value, retryable)
        logger.info(f'Queuing request: {request_task.request_id}')
        await _get_queue(request_task.schedule_type).put_async(input_tuple)

    durable_queue = (request_storage.get_request_backend().uses_durable_queue
                     is True)
    if durable_queue:
        # The PostgreSQL backend inserted the request and queue delivery in one
        # transaction. An API-only process deliberately has no local queue
        # factory, and a second put would only re-read the row it just wrote.
        logger.info(f'Durably queued request: {request_task.request_id}')
        return
    if precondition is not None and not durable_queue:
        # Schedule precondition wait as a background task so the caller
        # returns immediately.  The task reference is stored in a
        # module-level set to prevent garbage collection.
        task = asyncio.create_task(
            precondition.wait_async(on_condition_met=enqueue))
        preconditions.background_tasks.add(task)
        task.add_done_callback(preconditions.background_tasks.discard)
    else:
        await enqueue()


def start(
    config: server_config.ServerConfig,
    *,
    execution_classes: frozenset[request_registry.ExecutionClass] | None = None,
    controller_generation: int | None = None,
) -> tuple[multiprocessing.Process | None, list[RequestWorker]]:
    """Start the request workers.

    Request workers run in background, schedule the requests and delegate the
    request execution to executor processes.

    Returns:
        A tuple of the queue server process and the list of request worker
        threads.
    """
    global _queue_factory
    factory = queue_base.get_registered_queue_backend_factory()
    # Explicitly registered plugin backends take precedence over config.
    if factory is not None:
        if execution_classes is not None:
            raise RuntimeError(
                'Explicit queue plugins cannot be used with role-scoped '
                'request execution because QueueBackendFactory has no closed '
                'claim-filter contract.')
        _queue_factory = factory
    elif os.environ.get('SKYPILOT_API_REQUEST_BACKEND') == 'postgres':
        # Runtime import avoids loading the PostgreSQL implementation before
        # plugins have had an opportunity to register a custom queue factory.
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import postgres
        allowed_classes = (frozenset(
            execution_class.value for execution_class in execution_classes)
                           if execution_classes is not None else None)
        _queue_factory = postgres.PostgresQueueFactory(
            execution_classes=allowed_classes,
            controller_generation=controller_generation)
    elif config.queue_backend == server_config.QueueBackend.MULTIPROCESSING:
        if execution_classes is not None:
            raise RuntimeError('Role-scoped request execution requires the '
                               'PostgreSQL request backend.')
        _queue_factory = queue_base.MultiprocessingQueueFactory()
    elif config.queue_backend == server_config.QueueBackend.LOCAL:
        if execution_classes is not None:
            raise RuntimeError('Role-scoped request execution requires the '
                               'PostgreSQL request backend.')
        _queue_factory = queue_base.LocalQueueFactory()
    else:
        raise RuntimeError(f'Invalid queue backend: {config.queue_backend}')

    _get_queue.cache_clear()
    queue_server = _queue_factory.start()
    logger.info('Request queues created')

    workers = []
    # Start a worker for long requests.
    long_worker = RequestWorker(schedule_type=api_requests.ScheduleType.LONG,
                                config=config.long_worker_config)
    long_worker.run_in_background()
    workers.append(long_worker)

    # Start a worker for short requests.
    short_worker = RequestWorker(schedule_type=api_requests.ScheduleType.SHORT,
                                 config=config.short_worker_config)
    short_worker.run_in_background()
    workers.append(short_worker)
    return queue_server, workers


def reenqueue_recovered_requests() -> None:
    """Re-enqueue queued requests recovered from the previous server run.

    Must be called after start() (the queue backend must be up). Startup
    recovery (api_requests.recover_db_and_logs) left two kinds of rows
    queued: PENDING rows, which never started executing
    (_request_execution_wrapper flips a row to RUNNING before invoking the
    entrypoint, so they are side-effect-free and replayable), and retryable
    WAITING rows, which the executor monitor had already parked for a full
    re-run. Requests originally gated on a precondition
    (schedule_prepared_request) are re-enqueued without their gate and may
    fail fast instead of waiting for it -- an accepted, client-visible
    tradeoff vs. silently losing them on restart.
    """
    reqs = api_requests.get_request_tasks(
        api_requests.RequestTaskFilter(status=[
            api_requests.RequestStatus.PENDING,
            api_requests.RequestStatus.WAITING,
        ],
                                       fields=[
                                           'request_id', 'status',
                                           'schedule_type', 'created_at',
                                           api_requests.COL_IGNORE_RETURN_VALUE,
                                           api_requests.COL_RETRYABLE
                                       ]))
    # Only replay WAITING rows explicitly marked retryable:
    # recovery flips non-retryable WAITING rows to CANCELLED+should_retry,
    # but re-check here rather than trusting that recovery ran and agreed.
    reqs = [
        req for req in reqs
        if req.status == api_requests.RequestStatus.PENDING or req.retryable
    ]
    if not reqs:
        return
    reqs.sort(key=lambda req: req.created_at)
    for req in reqs:
        _get_queue(req.schedule_type).put(
            (req.request_id, bool(req.ignore_return_value),
             bool(req.retryable)))
    logger.info(f'Re-enqueued {len(reqs)} request(s) recovered from the '
                'previous server run')
