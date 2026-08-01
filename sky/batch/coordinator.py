"""Batch coordinator — orchestrates batch processing across pool workers.

The ``BatchCoordinator`` runs inline on the jobs controller (no separate
cluster).  ``ds.map()`` passes all config via ``task._metadata`` and
``sky.jobs.launch()`` submits the task; the controller detects the
``batch_coordinator`` metadata flag and calls ``BatchCoordinator.run()``
directly via ``asyncio.to_thread()``.

Lifecycle::

    ds.map()
      └─ sky.jobs.launch(task with batch_coordinator metadata)
           └─ Jobs controller detects metadata flag
                └─ Runs BatchCoordinator.run() inline
                     ├─ Count & split dataset into batches
                     ├─ Discover pool workers (SkyServe replicas)
                     ├─ Dispatch batches to workers via sky.exec()
                     ├─ Write progress directly to DB
                     ├─ Merge results
                     └─ Return (success) or raise (failure)
"""
import asyncio
import collections
import contextvars
import json
import logging
import os
import shlex
import signal
import sys
import textwrap
import threading
import time
from typing import Any, Optional
import uuid

import sky
from sky.batch import constants
from sky.batch import io_formats
from sky.client import sdk
from sky.jobs import state as managed_job_state
from sky.server import constants as server_constants
from sky.skylet import constants as skylet_constants

logger = logging.getLogger(__name__)


class SupersededCoordinator(RuntimeError):
    """Raised when a newer coordinator owns the same managed Batch job."""


class BatchCoordinator:
    """Orchestrates batch processing across pool workers.

    Runs inline on the jobs controller.  Config is passed via
    ``task._metadata`` by ``ds.map()``.  Dispatches batches to pool
    workers via ``sky.exec()``.  Writes progress directly to the DB.
    """

    def __init__(self,
                 dataset_path: str,
                 output_path: str,
                 batch_size: int,
                 pool_name: str,
                 serialized_fn: str,
                 input_format_dict: dict[str, Any],
                 output_formats_dict: list[dict[str, Any]],
                 activate_env: str = '',
                 job_id: int | None = None,
                 is_resume: bool = False):
        self.dataset_path = dataset_path
        self.output_path = output_path
        self.batch_size = batch_size
        self.pool_name = pool_name
        self.serialized_fn = serialized_fn
        self.activate_env = activate_env
        self._is_resume = is_resume

        self._input_format_dict = input_format_dict
        self._output_formats_dict = output_formats_dict

        # Use explicit job_id if provided (inline on controller),
        # otherwise fall back to env var (backward compat).
        if job_id is not None:
            self._managed_job_id: int = job_id
        else:
            env_var = skylet_constants.MANAGED_JOB_ID_ENV_VAR
            raw = os.environ.get(env_var)
            if raw is None:
                raise RuntimeError(f'{env_var} not set. The coordinator must '
                                   'run as a managed job.')
            self._managed_job_id = int(raw)

        # Batch metadata: list of [start_idx, end_idx] tuples.
        self.batches: list[list[int]] = []
        self.pending_batches: collections.deque[int] = collections.deque()
        self._pending_ready_at: dict[int, float] = {}
        self._pending_lock = threading.Lock()

        # Worker tracking: cluster_name → worker_job_id
        self._active_workers: dict[str, int] = {}
        self._active_workers_lock = threading.Lock()

        # Cancellation flag for inline (controller) mode.
        self._cancelled = False
        self._superseded_cleanup_started = False

        # Identifies worker services started by this coordinator incarnation.
        # It prevents an older controller from feeding or shutting down a
        # replacement service after an API-server/controller restart.
        self._worker_token = uuid.uuid4().hex
        self._stale_worker_tokens: set[str] = set()
        self._stale_attempt_leases_drained = False

        # Inline coordinators share a process with unrelated managed jobs and
        # must not replace the controller's process-wide SIGTERM handler.
        # Keep the handler only for the legacy standalone entrypoint.
        if job_id is None:
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGTERM, self._handle_sigterm)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Main entry point.  Returns on success, raises on failure."""
        try:
            logger.info(f'managed_job_id={self._managed_job_id}')
            previous_token = managed_job_state.acquire_batch_coordinator(
                self._managed_job_id, self._worker_token)
            if previous_token and previous_token != self._worker_token:
                self._stale_worker_tokens.add(previous_token)
            self._refresh_stale_worker_tokens()
            self._resolve_formats()

            if self._is_resume:
                completed_count = self._resume_from_db()
            else:
                self._count_and_split()
                if not self.batches:
                    logger.info('No items in dataset — nothing to do.')
                    return
                self._save_batches_to_db()
                completed_count = 0

            if completed_count == len(self.batches):
                # Crash happened after all batches done but before merge.
                logger.info('All batches already completed, skipping '
                            'to merge.')
                self._stale_attempt_leases_drained = True
                self._cleanup_stale_worker_services()
                self._set_winding_down()
                self._reduce_results()
                return

            self._discover_workers()
            self._wait_for_stale_attempt_leases()
            self._dispatch_all()
            self._cleanup_stale_worker_services()
            self._set_winding_down()
            self._reduce_results()
            logger.info('Batch job completed successfully.')
        except SupersededCoordinator:
            raise
        except Exception:
            self._print_partial_results_instructions()
            raise

    # ------------------------------------------------------------------
    # SIGTERM handler (sky jobs cancel)
    # ------------------------------------------------------------------

    def _handle_sigterm(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """Graceful shutdown on ``sky jobs cancel``."""
        logger.info('Received SIGTERM — shutting down workers...')
        self.cancel()
        sys.exit(1)

    def cancel(self) -> None:
        """Cancel the coordinator and shut down active workers.

        Sets the ``_cancelled`` flag so the dispatch loop breaks early,
        then shuts down any active worker services.
        """
        workers_snapshot = self._begin_cleanup()
        shutdown_threads = []
        for cluster_name, worker_job_id in workers_snapshot:
            thread_ctx = contextvars.copy_context()
            shutdown_thread = threading.Thread(target=thread_ctx.run,
                                               args=(self._cancel_worker,
                                                     cluster_name,
                                                     worker_job_id))
            shutdown_thread.start()
            shutdown_threads.append(shutdown_thread)
        for shutdown_thread in shutdown_threads:
            shutdown_thread.join()

    def _cancel_worker(self, cluster_name: str, worker_job_id: int) -> None:
        """Shut down one owned worker without aborting sibling cleanup."""
        try:
            self._shutdown_worker(cluster_name, worker_job_id)
        except Exception:  # pylint: disable=broad-except
            logger.warning(f'Failed to shutdown worker on {cluster_name}')

    def _begin_cleanup(self, superseded: bool = False) -> list[tuple[str, int]]:
        """Atomically assign cleanup ownership for tracked workers."""
        with self._active_workers_lock:
            workers_snapshot = list(self._active_workers.items())
            self._cancelled = True
            if superseded:
                self._superseded_cleanup_started = True
            else:
                # Normal cancellation owns these workers synchronously.  A
                # worker finalizer can only clean a later registration.
                self._active_workers.clear()
        return workers_snapshot

    def _claim_worker_cleanup(self, cluster_name: str,
                              worker_job_id: int) -> bool:
        """Claim one worker and return whether local shutdown is allowed."""
        with self._active_workers_lock:
            if self._active_workers.get(cluster_name) != worker_job_id:
                return False
            self._active_workers.pop(cluster_name)
            return not self._superseded_cleanup_started

    async def handle_superseded(self, timeout: float = 60) -> None:
        """Bound cleanup of only this superseded incarnation's workers.

        SDK calls are synchronous and may hang.  Each cleanup segment runs in
        ``to_thread`` under the one global deadline; after it expires this
        coroutine returns without starting any additional external action.
        A timed-out in-flight call can still finish in its worker thread, but
        it targets only this incarnation's token or exact durable job ID.
        """
        workers_snapshot = self._begin_cleanup(superseded=True)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0, timeout)

        async def _run_call(label, func, *args,
                            **kwargs) -> tuple[bool, bool, Any]:
            """Return (within_deadline, succeeded, result)."""
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False, False, None
            try:
                result = await asyncio.wait_for(asyncio.to_thread(
                    func, *args, **kwargs),
                                                timeout=remaining)
                return True, True, result
            except asyncio.TimeoutError:
                logger.warning('Timed out during superseded Batch %s', label)
                return False, False, None
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Superseded Batch %s failed: %s', label, e)
                return True, False, None

        async def _cancel_exact(cluster_name: str, worker_job_id: int,
                                worker_token: str) -> bool:
            within_deadline, succeeded, request_id = await _run_call(
                'cancel request',
                sdk.cancel,
                cluster_name,
                job_ids=[worker_job_id])
            if not within_deadline:
                return False
            if not succeeded:
                return True
            within_deadline, succeeded, _ = await _run_call(
                'cancel completion', sdk.get, request_id)
            if not within_deadline:
                return False
            if succeeded:
                within_deadline, _, _ = await _run_call(
                    'worker record removal',
                    managed_job_state.remove_batch_worker_record,
                    self._managed_job_id,
                    worker_token,
                    cluster_name,
                    worker_job_id=worker_job_id)
            return within_deadline

        async def _cleanup_active_worker(cluster_name: str,
                                         worker_job_id: int) -> bool:
            shutdown_code = self._generate_shutdown_code()
            shutdown_task = sky.Task(
                name=(f'batch-shutdown-{self._managed_job_id}-'
                      f'{self._worker_token}'),
                run=shutdown_code)
            within_deadline, succeeded, request_id = await _run_call(
                'shutdown request',
                sdk.exec,
                shutdown_task,
                cluster_name=cluster_name)
            if not within_deadline:
                return False
            if succeeded:
                within_deadline, _, _ = await _run_call('shutdown completion',
                                                        sdk.get, request_id)
                if not within_deadline:
                    return False
            return await _cancel_exact(cluster_name, worker_job_id,
                                       self._worker_token)

        active_cleanup_results = await asyncio.gather(
            *(_cleanup_active_worker(cluster_name, worker_job_id)
              for cluster_name, worker_job_id in workers_snapshot))
        if not all(active_cleanup_results):
            return

        async def _resolve_durable_worker_job_id(
                record: dict[str, Any]) -> tuple[bool, int | None]:
            """Resolve one durable worker record under the shared deadline."""
            worker_job_id = record.get('worker_job_id')
            if worker_job_id is None and record.get('launch_request_id'):
                within_deadline, succeeded, result = await _run_call(
                    'launch request recovery', sdk.get,
                    record['launch_request_id'])
                if not within_deadline:
                    return False, None
                if succeeded:
                    if isinstance(result, tuple) and result:
                        worker_job_id = result[0]
                    elif isinstance(result, int):
                        worker_job_id = result

            if worker_job_id is None:
                cluster_name = record['worker_cluster']
                within_deadline, succeeded, queue_request_id = await _run_call(
                    'worker queue request',
                    sdk.queue,
                    cluster_name,
                    skip_finished=True)
                if not within_deadline:
                    return False, None
                if not succeeded:
                    return True, None
                within_deadline, succeeded, queued_jobs = await _run_call(
                    'worker queue result', sdk.get, queue_request_id)
                if not within_deadline:
                    return False, None
                if not succeeded:
                    return True, None
                if queued_jobs is None:
                    logger.warning(
                        'Superseded Batch queue snapshot for %s returned None',
                        cluster_name)
                    return True, None
                try:
                    matching_ids = self._matching_worker_job_ids(
                        record, queued_jobs)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        'Invalid superseded Batch queue snapshot for %s: %s',
                        cluster_name, e)
                    return True, None
                if len(matching_ids) > 1:
                    logger.error(
                        'Refusing ambiguous superseded Batch cleanup for %s: '
                        'exact IDs %s', record['worker_job_name'], matching_ids)
                    return True, None
                if matching_ids:
                    worker_job_id = matching_ids[0]

            if worker_job_id is None:
                return True, None
            worker_job_id = int(worker_job_id)
            within_deadline, _, _ = await _run_call(
                'worker job ID persistence',
                managed_job_state.record_batch_worker_job_id,
                self._managed_job_id, record['coordinator_token'],
                record['worker_cluster'], worker_job_id)
            if not within_deadline:
                return False, None
            return True, worker_job_id

        within_deadline, succeeded, records = await _run_call(
            'worker record read', managed_job_state.get_batch_worker_records,
            self._managed_job_id)
        if not within_deadline:
            return
        if succeeded:
            for record in records:
                if record['coordinator_token'] != self._worker_token:
                    continue
                within_deadline, worker_job_id = (
                    await _resolve_durable_worker_job_id(record))
                if not within_deadline:
                    return
                if worker_job_id is None:
                    continue
                worker_job_id = int(worker_job_id)
                if not await _cancel_exact(record['worker_cluster'],
                                           worker_job_id,
                                           record['coordinator_token']):
                    return

        while loop.time() < deadline:
            with self._active_workers_lock:
                if not self._active_workers:
                    return
            await asyncio.sleep(min(0.2, max(0, deadline - loop.time())))
        with self._active_workers_lock:
            remaining_workers = sorted(self._active_workers)
        logger.warning('Timed out waiting for superseded Batch workers: %s',
                       remaining_workers)

    def mark_succeeded(self, end_time: float) -> None:
        """Durably succeed only if this coordinator still owns the job."""
        outcome = managed_job_state.set_batch_succeeded(self._managed_job_id, 0,
                                                        self._worker_token,
                                                        end_time)
        if outcome == managed_job_state.BatchLifecycleTransition.OWNER_LOST:
            raise SupersededCoordinator(
                'Batch coordinator lost ownership before SUCCEEDED')
        if outcome == managed_job_state.BatchLifecycleTransition.INVALID_STATE:
            raise RuntimeError('Cannot mark Batch job SUCCEEDED from its '
                               'current lifecycle state')

    def mark_failed(self, failure_reason: str) -> None:
        """Durably fail only if this coordinator still owns the job."""
        outcome = managed_job_state.set_batch_failed(self._managed_job_id, 0,
                                                     self._worker_token,
                                                     failure_reason)
        if outcome == managed_job_state.BatchLifecycleTransition.OWNER_LOST:
            raise SupersededCoordinator(
                'Batch coordinator lost ownership before FAILED')
        if outcome == managed_job_state.BatchLifecycleTransition.INVALID_STATE:
            raise RuntimeError('Cannot mark Batch job FAILED from its current '
                               'lifecycle state')

    # ------------------------------------------------------------------
    # Dataset counting & splitting
    # ------------------------------------------------------------------

    def _reset_pending_batches(self) -> None:
        with self._pending_lock:
            self.pending_batches = collections.deque()
            self._pending_ready_at = {}

    def _enqueue_batch(self, batch_idx: int, ready_at: float = 0) -> None:
        """Add a batch to the local queue without creating duplicates."""
        with self._pending_lock:
            existing_ready_at = self._pending_ready_at.get(batch_idx)
            if existing_ready_at is not None:
                self._pending_ready_at[batch_idx] = max(existing_ready_at,
                                                        ready_at)
                return
            self.pending_batches.append(batch_idx)
            self._pending_ready_at[batch_idx] = ready_at

    def _pop_ready_batch(self) -> tuple[int | None, float | None]:
        """Pop an eligible batch and return a wait time for delayed work.

        ``(None, None)`` means the queue is empty.  ``(None, seconds)`` means
        work exists but is still in persisted retry backoff.
        """
        now = time.time()
        with self._pending_lock:
            if not self.pending_batches:
                return None, None
            min_wait: float | None = None
            for _ in range(len(self.pending_batches)):
                batch_idx = self.pending_batches.popleft()
                ready_at = self._pending_ready_at[batch_idx]
                if ready_at <= now:
                    self._pending_ready_at.pop(batch_idx)
                    return batch_idx, 0
                self.pending_batches.append(batch_idx)
                wait = ready_at - now
                min_wait = wait if min_wait is None else min(min_wait, wait)
            return None, min_wait

    def _pending_count(self) -> int:
        with self._pending_lock:
            return len(self._pending_ready_at)

    def _has_pending_batches(self) -> bool:
        return self._pending_count() > 0

    def _resolve_formats(self) -> None:
        """Resolve typed input/output format handlers from dicts."""
        self._input_format = io_formats.InputReader.from_dict(
            self._input_format_dict)
        self._output_formats = [
            io_formats.OutputWriter.from_dict(d)
            for d in self._output_formats_dict
        ]
        for output_format in self._output_formats:
            output_format.validate_attempt_fencing()

    def _count_and_split(self) -> None:
        """Count dataset items and create batch index ranges."""
        logger.info(f'Counting items in {self.dataset_path}')
        total_items = len(self._input_format)
        logger.info(f'Dataset contains {total_items} items')

        self.batches = []
        for i in range(0, total_items, self.batch_size):
            start_idx = i
            end_idx = min(i + self.batch_size - 1, total_items - 1)
            self.batches.append([start_idx, end_idx])

        self._reset_pending_batches()
        for batch_idx in range(len(self.batches)):
            self._enqueue_batch(batch_idx)
        logger.info(f'Created {len(self.batches)} batches '
                    f'(total_items: {total_items}, '
                    f'batch_size: {self.batch_size})')

    # ------------------------------------------------------------------
    # DB persistence for HA recovery
    # ------------------------------------------------------------------

    def _save_batches_to_db(self) -> None:
        """Write all batch records to DB with PENDING status."""
        saved = managed_job_state.save_batch_states(self._managed_job_id,
                                                    self.batches,
                                                    self._worker_token)
        if not saved:
            raise SupersededCoordinator(
                'Batch coordinator lost ownership before state initialization')
        logger.info(f'Saved {len(self.batches)} batch records to DB')

    def _resume_from_db(self) -> int:
        """Restore coordinator state from DB after a controller crash.

        Rebuilds in-memory state from persisted records.  The new coordinator
        has already fenced the previous owner; stale live attempts retain their
        leases until :meth:`_wait_for_stale_attempt_leases` reclaims them.
        """
        records = managed_job_state.get_batch_states(self._managed_job_id)
        if not records:
            raise RuntimeError(
                f'No batch records found for job {self._managed_job_id} '
                'during resume. The job may need to be re-submitted.')

        self.batches = []
        self._reset_pending_batches()
        completed_count = 0
        failed_batches = []
        leased_batches = 0

        for i, rec in enumerate(records):
            batch_idx = rec['batch_idx']
            assert batch_idx == i, (
                f'Batch records not contiguous: expected batch_idx={i}, '
                f'got {batch_idx}. DB may be corrupted.')
            self.batches.append([rec['start_idx'], rec['end_idx']])
            status = rec['status']
            attempt_owner_token = rec.get('attempt_owner_token')
            attempt_id = int(rec.get('attempt_id') or 0)
            if (not attempt_owner_token and
                (status != 'PENDING' or attempt_id > 0)):
                raise RuntimeError(
                    f'Batch {batch_idx} has pre-fence attempt state. Batch '
                    'state must be recreated with the current schema.')
            if (attempt_owner_token and
                    attempt_owner_token != self._worker_token):
                self._stale_worker_tokens.add(attempt_owner_token)
            if status == 'PENDING':
                self._enqueue_batch(batch_idx, rec.get('next_retry_at') or 0)
            elif status == 'COMPLETED':
                completed_count += 1
            elif status == 'DISPATCHED':
                leased_batches += 1
            elif status == 'FAILED':
                failed_batches.append(batch_idx)
        if failed_batches:
            raise RuntimeError('Cannot resume batch job with terminally failed '
                               f'batches: {failed_batches}')

        logger.info(f'Resumed from DB: {len(self.batches)} batches, '
                    f'{completed_count} completed, '
                    f'{self._pending_count()} pending, '
                    f'{leased_batches} leased')
        logger.info(f'BATCH_RESUME total={len(self.batches)} '
                    f'completed={completed_count} '
                    f'pending={self._pending_count()} '
                    f'leased={leased_batches}')
        return completed_count

    def _reclaim_expired_batches(self) -> int:
        reclaimed = managed_job_state.requeue_expired_batch_attempts(
            self._managed_job_id, self._worker_token)
        for batch_idx in reclaimed:
            self._enqueue_batch(batch_idx)
        if reclaimed:
            logger.warning('Reclaimed %d expired batch attempt(s): %s',
                           len(reclaimed), reclaimed)
        return len(reclaimed)

    def _assert_coordinator_owner(self) -> None:
        """Raise when a newer controller incarnation has taken ownership."""
        if not managed_job_state.is_batch_coordinator_owner(
                self._managed_job_id, self._worker_token):
            raise SupersededCoordinator(
                f'Batch coordinator {self._worker_token} no longer owns '
                f'managed job {self._managed_job_id}')

    def _set_winding_down(self) -> None:
        outcome = managed_job_state.set_batch_winding_down(
            self._managed_job_id, 0, self._worker_token)
        if outcome == managed_job_state.BatchLifecycleTransition.OWNER_LOST:
            raise SupersededCoordinator(
                'Batch coordinator lost ownership before WINDING_DOWN')
        if outcome == managed_job_state.BatchLifecycleTransition.INVALID_STATE:
            raise RuntimeError('Cannot mark Batch job WINDING_DOWN from its '
                               'current lifecycle state')

    def _wait_for_stale_attempt_leases(self) -> None:
        """Wait for fenced attempts before cleaning their worker services.

        Takeover prevents the old owner from renewing or completing an
        attempt.  We nevertheless honor the lease it acquired before takeover,
        then reclaim it.  Only after no old live lease remains may the caller
        cancel token-scoped worker jobs and start replacement services.
        """
        while True:
            self._assert_coordinator_owner()
            self._reclaim_expired_batches()
            records = managed_job_state.get_batch_states(self._managed_job_id)
            old_live_attempts = []
            for rec in records:
                attempt_owner_token = rec.get('attempt_owner_token')
                if (rec['status'] == 'DISPATCHED' and not attempt_owner_token):
                    raise RuntimeError(
                        f'Batch {rec["batch_idx"]} has a live attempt without '
                        'an owner token; recreate the unused Batch job.')
                if (attempt_owner_token and
                        attempt_owner_token != self._worker_token):
                    self._stale_worker_tokens.add(attempt_owner_token)
                if (rec['status'] == 'DISPATCHED' and
                        attempt_owner_token != self._worker_token):
                    old_live_attempts.append(rec)
            if not old_live_attempts:
                self._stale_attempt_leases_drained = True
                self._cleanup_stale_worker_services(strict=True)
                return

            now = time.time()
            expirations = [
                float(rec['lease_expires_at'])
                for rec in old_live_attempts
                if rec.get('lease_expires_at') is not None
            ]
            wait_seconds = 1.0
            if expirations:
                wait_seconds = min(10.0, max(0.1, min(expirations) - now))
            logger.info('Waiting %.1fs for %d fenced Batch attempt lease(s)',
                        wait_seconds, len(old_live_attempts))
            time.sleep(wait_seconds)

    def _sync_batch_progress_from_db(self) -> tuple[int, set[str], list[int]]:
        """Return durable progress and discover PENDING work.

        ``leased_workers`` prevents a replacement coordinator from starting a
        new worker service on a replica that still owns an unexpired attempt.
        Completion and retry progress are read exclusively from this durable
        state rather than mirrored by worker threads.
        """
        self._assert_coordinator_owner()
        records = managed_job_state.get_batch_states(self._managed_job_id)
        completed_count = 0
        leased_workers: set[str] = set()
        failed_batches = []
        for rec in records:
            batch_idx = rec['batch_idx']
            status = rec['status']
            if status == 'PENDING':
                self._enqueue_batch(batch_idx, rec.get('next_retry_at') or 0)
            elif status == 'COMPLETED':
                completed_count += 1
            elif status == 'DISPATCHED':
                worker_cluster = rec.get('worker_cluster')
                if worker_cluster:
                    leased_workers.add(worker_cluster)
            elif status == 'FAILED':
                failed_batches.append(batch_idx)
        return completed_count, leased_workers, failed_batches

    def _start_batch_lease_renewer(
        self, batch_idx: int, attempt_id: int
    ) -> tuple[threading.Event, threading.Event, threading.Thread]:
        """Keep a batch lease alive while SDK calls may be blocking."""
        stop_event = threading.Event()
        lost_event = threading.Event()

        def _renew() -> None:
            while not stop_event.wait(constants.BATCH_LEASE_RENEW_INTERVAL):
                try:
                    owned = managed_job_state.renew_batch_lease(
                        self._managed_job_id, batch_idx, attempt_id,
                        self._worker_token, constants.BATCH_LEASE_DURATION)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning('Failed to renew batch lease %d/%d: %s',
                                   batch_idx, attempt_id, e)
                    continue
                if not owned:
                    lost_event.set()
                    return

        renewer = threading.Thread(target=_renew, daemon=True)
        renewer.start()
        return stop_event, lost_event, renewer

    @staticmethod
    def _stop_batch_lease_renewer(stop_event: threading.Event,
                                  renewer: threading.Thread) -> None:
        stop_event.set()
        renewer.join(timeout=1)

    # ------------------------------------------------------------------
    # Worker discovery
    # ------------------------------------------------------------------

    def _discover_workers(self) -> None:
        """Discover all ready workers in the pool.

        Uses all available workers — no fixed ``target_num_replicas``.
        If no workers are found immediately, waits up to the discovery
        timeout for at least one to appear.  On resume the timeout is
        extended because batches are already checkpointed and the pool
        may briefly appear "not ready" while the controller pod and the
        serve-side pool status plumbing stabilize after a restart.
        """
        timeout = (constants.WORKER_DISCOVERY_RESUME_TIMEOUT
                   if self._is_resume else constants.WORKER_DISCOVERY_TIMEOUT)
        workers = self._get_ready_workers()

        if not workers:
            logger.info('No workers ready yet, waiting for at least one...')
            deadline = time.monotonic() + timeout

            while not workers and time.monotonic() < deadline:
                time.sleep(5)
                workers = self._get_ready_workers()
                if not workers:
                    remaining = int(deadline - time.monotonic())
                    logger.info(f'No workers ready yet '
                                f'(waiting up to {remaining}s more)')

        if not workers:
            raise RuntimeError(
                f'No ready workers found in pool {self.pool_name} '
                f'after waiting {timeout}s')

        self._workers = workers
        logger.info(f'Discovered {len(workers)} ready workers')

    def _fetch_pool_status(self) -> dict[str, Any] | None:
        """Fetch pool status via the SDK.

        Returns the first matching pool record dict, or None.
        """
        try:
            request_id = sky.jobs.pool_status([self.pool_name])
            pool_statuses = sdk.stream_and_get(request_id)
            if pool_statuses:
                return pool_statuses[0]
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to fetch pool status: {e}')
        return None

    def _get_ready_workers(self) -> list[str]:
        """Return cluster names for dispatchable replicas via SDK."""
        status = self._fetch_pool_status()
        if status is None:
            return []
        replica_infos = status.get('replica_info', [])
        ready = []
        unavailable_summary: list[str] = []
        for info in replica_infos:
            raw_status = info.get('status', '')
            # status may be a ReplicaStatus enum or a string; normalise.
            replica_status = (raw_status.value if hasattr(raw_status, 'value')
                              else str(raw_status))
            name = info.get('name')
            if replica_status == 'READY':
                replica_info_version = info.get('replica_info_version')
                if (replica_info_version is None or replica_info_version
                        < server_constants.MIN_BATCH_REPLICA_INFO_VERSION):
                    raise RuntimeError(
                        f'Pool replica {name!r} uses worker runtime version '
                        f'{replica_info_version}; Sky Batch requires version '
                        f'{server_constants.MIN_BATCH_REPLICA_INFO_VERSION}. '
                        f'Recreate pool {self.pool_name!r} before retrying.')
                used_by = info.get('used_by')
                # `used_by` is an advisory status snapshot, not a reservation:
                # occupancy may change after this check.  Still fail closed if
                # the server does not satisfy the current list contract, since
                # dispatching with unknown occupancy can queue behind other
                # managed jobs.
                if not isinstance(used_by, list):
                    if name:
                        unavailable_summary.append(f'{name}=USAGE_UNKNOWN')
                    continue
                other_job_ids = [
                    job_id for job_id in used_by
                    if str(job_id) != str(self._managed_job_id)
                ]
                if other_job_ids:
                    if name:
                        jobs = ','.join(str(job_id) for job_id in other_job_ids)
                        unavailable_summary.append(f'{name}=BUSY({jobs})')
                    continue
                if name:
                    ready.append(name)
            elif name:
                unavailable_summary.append(f'{name}={replica_status}')
        if not ready and unavailable_summary:
            # Help diagnose cases where the pool exists but no replica can
            # accept a Batch worker service yet.
            logger.info('Pool %s has no dispatchable replicas: %s',
                        self.pool_name, ', '.join(unavailable_summary))
        return ready

    # ------------------------------------------------------------------
    # Pool resource detection
    # ------------------------------------------------------------------

    def _get_pool_resources(self) -> Optional['sky.Resources']:
        """Return the ``sky.Resources`` for pool workers."""
        status = self._fetch_pool_status()
        if status is None:
            return None
        yaml_content = status.get('pool_yaml') or status.get('yaml_content')
        if not yaml_content:
            return None
        try:
            task = sky.Task.from_yaml_str(str(yaml_content))
            for r in task.resources:
                return r
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to parse pool resources: %s', e)
        return None

    # ------------------------------------------------------------------
    # Worker code generation
    # ------------------------------------------------------------------

    @staticmethod
    def _new_failure_marker_path() -> str:
        """Return a launch-unique failure marker path.

        The marker lives in a fixed per-node location; a stale marker from a
        crashed previous worker launch must never fail the health check of a
        fresh launch on the same node, so each launch gets its own file.
        """
        base, ext = os.path.splitext(constants.WORKER_FAILURE_MARKER_PATH)
        return f'{base}.{uuid.uuid4().hex}{ext}'

    def _generate_worker_startup_code(self,
                                      failure_marker_path: str | None = None
                                     ) -> str:
        """Generate code to start the long-running worker service."""
        job_id = str(self._managed_job_id)
        activate = self.activate_env.strip()
        activate_line = f'{activate} &&' if activate else ''
        sky_runtime = skylet_constants.SKY_REMOTE_PYTHON_ENV
        if failure_marker_path is None:
            failure_marker_path = constants.WORKER_FAILURE_MARKER_PATH
        failure_marker = shlex.quote(failure_marker_path)

        # Serialize typed format dicts as JSON env vars for workers.
        input_format_json = json.dumps(self._input_format.to_dict()).replace(
            '\'', '\'\\\'\'')
        # Pass output formats as a JSON array for multi-output support.
        output_formats_json = json.dumps(self._output_formats_dict or
                                         []).replace('\'', '\'\\\'\'')

        return textwrap.dedent(f"""\
            set -eo pipefail
            export SKY_BATCH_SERIALIZED_FN='{self.serialized_fn}'
            export SKY_BATCH_OUTPUT_PATH='{self.output_path}'
            export SKY_BATCH_JOB_ID='{job_id}'
            export SKY_BATCH_WORKER_TOKEN='{self._worker_token}'
            export SKY_BATCH_INPUT_FORMAT='{input_format_json}'
            export SKY_BATCH_OUTPUT_FORMATS='{output_formats_json}'
            export {constants.WORKER_FAILURE_MARKER_ENV_VAR}={failure_marker}

            # Make sky.batch visible to the user's python.
            SKY_SITE=$({sky_runtime}/bin/python -c \\
              "import site; print(site.getsitepackages()[0])")
            export PYTHONPATH="${{SKY_SITE}}:${{PYTHONPATH}}"

            # Ensure boto3 is available in the user env.
            {activate_line} pip install boto3 2>/dev/null

            # Start worker service in the activated environment.
            rm -f {failure_marker}
            {activate_line} python -u -c '
            import os
            from sky.batch.worker import start_worker
            start_worker(
                serialized_fn=os.environ["SKY_BATCH_SERIALIZED_FN"],
                output_path=os.environ["SKY_BATCH_OUTPUT_PATH"],
                job_id=os.environ["SKY_BATCH_JOB_ID"],
                worker_token=os.environ["SKY_BATCH_WORKER_TOKEN"],
            )
            ' 2>&1 | tee /tmp/sky_batch_worker.log
            """)

    def _generate_worker_health_check_code(self,
                                           failure_marker_path: str |
                                           None = None) -> str:
        """Generate code that waits for the worker to report healthy."""
        port = constants.WORKER_SERVICE_PORT
        timeout = constants.WORKER_SERVICE_STARTUP_TIMEOUT
        if failure_marker_path is None:
            failure_marker_path = constants.WORKER_FAILURE_MARKER_PATH
        failure_marker = shlex.quote(failure_marker_path)
        return textwrap.dedent(f"""\
            set -e
            failure_marker={failure_marker}
            health_output=$(mktemp)
            trap 'rm -f "$health_output"' EXIT
            for i in $(seq 1 {timeout}); do
                if [ -s "$failure_marker" ]; then
                    echo "ERROR: Worker service failed before becoming healthy"
                    cat "$failure_marker"
                    exit 1
                fi
                http_code=$(curl -s -o "$health_output" -w '%{{http_code}}' \
                    http://127.0.0.1:{port}/health \
                    -H 'X-Sky-Batch-Worker-Token: {self._worker_token}' || true)
                if [ "$http_code" = "200" ]; then
                    echo "Worker service ready after $i seconds"
                    exit 0
                fi
                if [ "$http_code" = "503" ]; then
                    echo "ERROR: Worker service reported a durable failure"
                    cat "$health_output"
                    exit 1
                fi
                sleep 1
            done
            echo "ERROR: Worker service did not start within {timeout}s"
            exit 1
            """)

    def _generate_notify_code(self, batch_idx: int, attempt_id: int) -> str:
        """Generate lightweight notify script for a single batch."""
        start_idx, end_idx = self.batches[batch_idx]
        port = constants.WORKER_SERVICE_PORT
        payload = json.dumps({
            'dataset_path': self.dataset_path,
            'start_idx': start_idx,
            'end_idx': end_idx,
            'batch_idx': batch_idx,
            'attempt_id': attempt_id,
        })

        return textwrap.dedent(f"""\
            set -e
            curl -sf -X POST http://127.0.0.1:{port}/feed_batch \\
                -H 'Content-Type: application/json' \\
                -H 'X-Sky-Batch-Worker-Token: {self._worker_token}' \\
                -d {shlex.quote(payload)}
            """)

    def _generate_shutdown_code(self) -> str:
        """Generate a script that shuts down the worker service."""
        port = constants.WORKER_SERVICE_PORT
        return textwrap.dedent(f"""\
            curl -sf --connect-timeout 2 --max-time 5 -X POST \\
                http://127.0.0.1:{port}/shutdown \\
                -H 'X-Sky-Batch-Worker-Token: {self._worker_token}' || true
            """)

    # ------------------------------------------------------------------
    # Worker service lifecycle
    # ------------------------------------------------------------------

    def _worker_job_name(self, worker_token: str) -> str:
        """Return the immutable job name for one coordinator incarnation."""
        return (f'batch-worker-{self._managed_job_id}-'
                f'{worker_token}')

    def _refresh_stale_worker_tokens(self) -> None:
        """Load every durable worker generation older than this owner."""
        for record in managed_job_state.get_batch_worker_records(
                self._managed_job_id):
            token = record['coordinator_token']
            if token != self._worker_token:
                self._stale_worker_tokens.add(token)

    def _cancel_stale_worker_jobs(self, cluster_name: str,
                                  worker_token: str) -> None:
        """Cancel durable exact-ID launches for one stale generation."""
        if not worker_token or worker_token == self._worker_token:
            raise ValueError('cleanup requires a non-current worker token')
        if worker_token not in self._stale_worker_tokens:
            raise ValueError('cleanup requires a durably captured stale token')
        if not self._stale_attempt_leases_drained:
            raise RuntimeError('cannot clean stale Batch workers before old '
                               'attempt leases are drained')
        self._cleanup_worker_services_for_token(worker_token, [cluster_name])

    @staticmethod
    def _matching_worker_job_ids(record: dict[str, Any],
                                 queued_jobs: list[Any]) -> list[int]:
        """Return exact queue job IDs matching one durable worker record."""
        matching_ids = []
        for queued_job in queued_jobs:
            if isinstance(queued_job, dict):
                name = queued_job.get('job_name')
                queued_job_id = queued_job.get('job_id')
            else:
                name = queued_job.job_name
                queued_job_id = queued_job.job_id
            if (name == record['worker_job_name'] and
                    queued_job_id is not None):
                matching_ids.append(int(queued_job_id))
        return sorted(set(matching_ids))

    def _resolve_worker_job_id(
        self,
        record: dict[str, Any],
        queue_jobs_by_cluster: dict[str, list[Any]] | None = None
    ) -> int | None:
        """Resolve one launch intent without guessing among duplicate names.

        ``queue_jobs_by_cluster`` memoizes one ``sdk.queue`` snapshot per
        worker cluster so repeated stale generations on the same cluster are
        judged against one queue view instead of one request per record.
        """
        worker_job_id = record.get('worker_job_id')
        if worker_job_id is not None:
            return int(worker_job_id)

        request_id = record.get('launch_request_id')
        if request_id:
            try:
                result = sdk.get(request_id)
                if isinstance(result, tuple) and result:
                    worker_job_id = result[0]
                elif isinstance(result, int):
                    worker_job_id = result
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Failed to recover Batch worker request %s: %s',
                               request_id, e)

        if worker_job_id is None:
            cluster_name = record['worker_cluster']
            queued_jobs = (None if queue_jobs_by_cluster is None else
                           queue_jobs_by_cluster.get(cluster_name))
            if queued_jobs is None:
                queue_request_id = sdk.queue(cluster_name, skip_finished=True)
                queued_jobs = sdk.get(queue_request_id)
            if queued_jobs is None:
                raise TypeError(
                    f'Queue snapshot for {cluster_name} returned None')
            matching_ids = self._matching_worker_job_ids(record, queued_jobs)
            if queue_jobs_by_cluster is not None:
                queue_jobs_by_cluster[cluster_name] = queued_jobs
            if len(matching_ids) > 1:
                logger.error(
                    'Refusing ambiguous Batch worker cleanup for %s on %s: '
                    'duplicate name maps to exact IDs %s',
                    record['worker_job_name'], record['worker_cluster'],
                    matching_ids)
                return None
            if matching_ids:
                worker_job_id = matching_ids[0]

        if worker_job_id is None:
            return None
        worker_job_id = int(worker_job_id)
        managed_job_state.record_batch_worker_job_id(
            self._managed_job_id, record['coordinator_token'],
            record['worker_cluster'], worker_job_id)
        return worker_job_id

    def _cancel_worker_record(
            self,
            record: dict[str, Any],
            queue_jobs_by_cluster: dict[str, list[Any]] | None = None) -> None:
        """Cancel one durable worker record by exactly one external job ID."""
        worker_job_id = self._resolve_worker_job_id(record,
                                                    queue_jobs_by_cluster)
        if worker_job_id is None:
            return
        self._cancel_worker_job_by_id(record['worker_cluster'], worker_job_id,
                                      record['coordinator_token'])
        logger.info('Cancelled exact Batch worker job %s for token %s on %s',
                    worker_job_id, record['coordinator_token'],
                    record['worker_cluster'])

    def _cleanup_worker_services_for_token(
            self,
            worker_token: str,
            workers: list[str] | None = None,
            queue_jobs_by_cluster: dict[str, list[Any]] | None = None) -> None:
        """Clean durable records for one token, one exact job ID at a time."""
        worker_filter = set(workers) if workers is not None else None
        for record in managed_job_state.get_batch_worker_records(
                self._managed_job_id):
            if record['coordinator_token'] != worker_token:
                continue
            if (worker_filter is not None and
                    record['worker_cluster'] not in worker_filter):
                continue
            self._cancel_worker_record(record, queue_jobs_by_cluster)

    def _cleanup_stale_worker_services(self,
                                       workers: list[str] | None = None,
                                       strict: bool = False) -> None:
        """Clean exact old-token workers after their attempt leases expire."""
        self._refresh_stale_worker_tokens()
        if not self._stale_worker_tokens:
            return
        if not self._stale_attempt_leases_drained:
            raise RuntimeError('cannot clean stale Batch workers before old '
                               'attempt leases are drained')
        queue_jobs_by_cluster: dict[str, list[Any]] = {}
        for worker_token in sorted(self._stale_worker_tokens):
            try:
                self._cleanup_worker_services_for_token(worker_token, workers,
                                                        queue_jobs_by_cluster)
            except Exception as e:  # pylint: disable=broad-except
                if strict:
                    raise
                logger.warning(
                    'Failed to clean stale Batch worker token '
                    '%s: %s', worker_token, e)

    def _launch_worker_service(self, cluster_name: str) -> int:
        """Launch worker service as a long-running SkyPilot job.

        Returns:
            The SkyPilot job ID of the worker service.
        """
        job_id = str(self._managed_job_id)
        self._assert_coordinator_owner()
        # A replica may become READY after the initial takeover cleanup.  Its
        # old token-scoped service is now safe to remove because all old
        # attempt leases were drained before dispatch began.
        self._cleanup_stale_worker_services([cluster_name], strict=True)
        # A failed thread from this same incarnation may have left an exact
        # durable worker record behind.  Retire it before reusing the
        # (job, token, cluster) launch-intent key.
        self._cleanup_worker_services_for_token(self._worker_token,
                                                [cluster_name])
        failure_marker_path = self._new_failure_marker_path()
        startup_code = self._generate_worker_startup_code(failure_marker_path)
        worker_job_name = self._worker_job_name(self._worker_token)
        task = sky.Task(name=worker_job_name, run=startup_code)
        pool_resources = self._get_pool_resources()
        if pool_resources is not None:
            task.set_resources(pool_resources)
        registered = managed_job_state.register_batch_worker_launch(
            self._managed_job_id, self._worker_token, cluster_name,
            worker_job_name)
        if not registered:
            raise SupersededCoordinator(
                'Batch coordinator lost ownership before worker launch')
        logger.info(f'Submitting exec to {cluster_name} '
                    f'with resources={pool_resources}')
        try:
            request_id = sdk.exec(task, cluster_name=cluster_name)
        except Exception as e:
            logger.error(f'sdk.exec() failed: {e}', exc_info=True)
            raise
        managed_job_state.record_batch_worker_launch_request(
            self._managed_job_id, self._worker_token, cluster_name,
            str(request_id))
        try:
            worker_job_id, _ = sdk.get(request_id)
        except Exception as e:
            logger.error(f'sdk.get() for exec failed: {e}', exc_info=True)
            raise
        assert worker_job_id is not None, 'Failed to get worker job ID'
        worker_job_id = int(worker_job_id)
        managed_job_state.record_batch_worker_job_id(self._managed_job_id,
                                                     self._worker_token,
                                                     cluster_name,
                                                     worker_job_id)

        if not managed_job_state.is_batch_coordinator_owner(
                self._managed_job_id, self._worker_token):
            # The launch crossed a takeover.  Cancel the immutable job ID we
            # just created; a replacement has a different token and name.
            self._cancel_worker_job_by_id(cluster_name, worker_job_id,
                                          self._worker_token)
            raise SupersededCoordinator(
                f'Batch coordinator lost ownership while launching worker '
                f'{worker_job_id} on {cluster_name}')

        logger.info(f'Launched worker service as job '
                    f'{worker_job_id} on {cluster_name}')

        # Wait for worker to be ready
        health_code = self._generate_worker_health_check_code(
            failure_marker_path)
        health_task = sky.Task(
            name=f'health-check-{job_id}-{self._worker_token}', run=health_code)
        try:
            req_id = sdk.exec(health_task, cluster_name=cluster_name)
            sdk.get(req_id)
            logger.info(f'Worker service ready on {cluster_name}')
            return worker_job_id
        except Exception as e:  # pylint: disable=broad-except
            try:
                self._cancel_worker_job_by_id(cluster_name, worker_job_id,
                                              self._worker_token)
            except Exception as cancel_error:  # pylint: disable=broad-except
                logger.warning(
                    'Failed to cancel unhealthy Batch worker %s '
                    'on %s: %s', worker_job_id, cluster_name, cancel_error)
            raise RuntimeError(
                f'Worker service on {cluster_name} failed to start: '
                f'{e}') from e

    def _cancel_worker_job_by_id(self, cluster_name: str, worker_job_id: int,
                                 worker_token: str) -> None:
        """Cancel exactly one worker ID and retire its durable record."""
        cancel_request_id = None
        for attempt in range(2):
            try:
                if cancel_request_id is None:
                    cancel_request_id = sdk.cancel(cluster_name,
                                                   job_ids=[worker_job_id])
                sdk.get(cancel_request_id)
                managed_job_state.remove_batch_worker_record(
                    self._managed_job_id,
                    worker_token,
                    cluster_name,
                    worker_job_id=worker_job_id)
                return
            except Exception as e:  # pylint: disable=broad-except
                if attempt == 1:
                    raise
                logger.warning(
                    'Retrying exact Batch worker cancellation for '
                    '%s on %s after: %s', worker_job_id, cluster_name, e)

    def _shutdown_worker(self,
                         cluster_name: str,
                         worker_job_id: int | None = None) -> None:
        """Send shutdown signal and cancel worker job."""
        shutdown_code = self._generate_shutdown_code()
        task = sky.Task(name=(f'batch-shutdown-{self._managed_job_id}-'
                              f'{self._worker_token}'),
                        run=shutdown_code)
        try:
            request_id = sdk.exec(task, cluster_name=cluster_name)
            sdk.get(request_id)
            logger.info('Sent shutdown to worker service on %s', cluster_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to send shutdown to %s: %s', cluster_name, e)

        if worker_job_id is not None:
            time.sleep(5)
            try:
                self._cancel_worker_job_by_id(cluster_name, worker_job_id,
                                              self._worker_token)
                logger.info(f'Cancelled worker job {worker_job_id} on '
                            f'{cluster_name}')
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f'Failed to cancel worker job '
                               f'{worker_job_id}: {e}')

    # ------------------------------------------------------------------
    # Per-worker dispatch loop (runs in its own thread)
    # ------------------------------------------------------------------

    def _worker_dispatch_loop(self, cluster_name: str) -> None:
        """Dispatch batches to *cluster_name* until the queue is empty.

        1. Launch worker service once as a separate long-running job.
        2. For each batch: submit notify job, poll status.
        3. Shutdown worker service when done.
        """
        job_id = str(self._managed_job_id)

        worker_job_id = self._launch_worker_service(cluster_name)
        with self._active_workers_lock:
            self._active_workers[cluster_name] = worker_job_id

        try:
            while not self._cancelled:
                batch_idx, wait_seconds = self._pop_ready_batch()
                if batch_idx is None:
                    if wait_seconds is not None:
                        time.sleep(min(wait_seconds, 1))
                        continue
                    return

                claim = managed_job_state.claim_batch(
                    self._managed_job_id, batch_idx, self._worker_token,
                    cluster_name, constants.BATCH_LEASE_DURATION)
                if claim is None:
                    # Another coordinator/thread won the claim, or persisted
                    # retry backoff has not elapsed yet.
                    continue
                attempt_id, retries = claim
                lease_stop, lease_lost, lease_renewer = (
                    self._start_batch_lease_renewer(batch_idx, attempt_id))

                try:
                    notify_code = self._generate_notify_code(
                        batch_idx, attempt_id)
                    task = sky.Task(name=f'batch-notify-{job_id}-{batch_idx}',
                                    run=notify_code)
                    request_id = sdk.exec(task, cluster_name=cluster_name)
                    job_id_on_cluster, _ = sdk.get(request_id)
                    assert job_id_on_cluster is not None
                    if lease_lost.is_set():
                        raise RuntimeError(
                            f'Batch {batch_idx}: attempt {attempt_id} lost '
                            'its lease during submission')

                    logger.info(f'Batch {batch_idx} running as '
                                f'job {job_id_on_cluster} on {cluster_name}')

                    # Poll until terminal.  If the cluster goes away
                    # (e.g. rolling update) we'll get repeated None
                    # statuses — treat that as a failure after a grace
                    # period so the batch can be retried.
                    none_count = 0
                    max_none = 12  # ~60s at 5s poll interval
                    while True:
                        time.sleep(constants.BATCH_POLL_INTERVAL)
                        if lease_lost.is_set():
                            raise RuntimeError(
                                f'Batch {batch_idx}: attempt {attempt_id} '
                                'lost its lease')
                        req_id = sdk.job_status(cluster_name,
                                                [job_id_on_cluster])
                        statuses = sdk.get(req_id)
                        status = statuses.get(job_id_on_cluster)
                        if status is None:
                            none_count += 1
                            if none_count >= max_none:
                                raise RuntimeError(
                                    f'Batch {batch_idx}: lost contact '
                                    f'with {cluster_name} (job status '
                                    f'unavailable for {none_count} '
                                    f'consecutive polls)')
                            continue
                        none_count = 0
                        if status.is_terminal():
                            if status != sky.JobStatus.SUCCEEDED:
                                raise RuntimeError(
                                    f'Batch {batch_idx} failed with '
                                    f'status {status.value}')
                            logger.info(f'Batch {batch_idx} SUCCEEDED '
                                        f'on {cluster_name}')
                            break

                    # Only the current attempt may publish completion.
                    self._stop_batch_lease_renewer(lease_stop, lease_renewer)
                    completed = managed_job_state.set_batch_attempt_status(
                        self._managed_job_id, batch_idx, attempt_id,
                        self._worker_token, 'COMPLETED')
                    if not completed:
                        logger.warning(
                            'Ignoring completion from stale batch attempt '
                            '%d/%d', batch_idx, attempt_id)
                        continue
                    logger.info('Batch %d durably completed on %s', batch_idx,
                                cluster_name)
                except Exception as e:  # pylint: disable=broad-except
                    self._stop_batch_lease_renewer(lease_stop, lease_renewer)
                    logger.error(f'Batch {batch_idx} failed on '
                                 f'{cluster_name}: {e}')
                    if retries < constants.MAX_RETRIES:
                        retry_count = retries + 1
                        backoff = constants.RETRY_BACKOFF_BASE**retry_count
                        ready_at = time.time() + backoff
                        requeued = managed_job_state.set_batch_attempt_status(
                            self._managed_job_id,
                            batch_idx,
                            attempt_id,
                            self._worker_token,
                            'PENDING',
                            retry_count=retry_count,
                            next_retry_at=ready_at)
                        if requeued:
                            self._enqueue_batch(batch_idx, ready_at)
                            logger.info(f'Re-queued batch {batch_idx} '
                                        f'(retry {retry_count}/'
                                        f'{constants.MAX_RETRIES}), '
                                        f'eligible in {backoff}s')
                        else:
                            logger.warning(
                                'Ignoring failure from stale batch attempt '
                                '%d/%d', batch_idx, attempt_id)
                    else:
                        failed = managed_job_state.set_batch_attempt_status(
                            self._managed_job_id,
                            batch_idx,
                            attempt_id,
                            self._worker_token,
                            'FAILED',
                            retry_count=retries)
                        if failed:
                            raise RuntimeError(
                                f'Batch {batch_idx} failed after '
                                f'{constants.MAX_RETRIES} retries: {e}') from e
                        logger.warning(
                            'Ignoring terminal failure from stale batch '
                            'attempt %d/%d', batch_idx, attempt_id)
        finally:
            # Cleanup ownership is claimed under the same lock used by cancel
            # and supersession.  Exactly one path may issue external shutdown.
            if self._claim_worker_cleanup(cluster_name, worker_job_id):
                self._shutdown_worker(cluster_name, worker_job_id=worker_job_id)

    # ------------------------------------------------------------------
    # Dispatch orchestration
    # ------------------------------------------------------------------

    def _dispatch_all(self) -> None:
        """Launch dispatch threads per worker and dynamically add new ones.

        Periodically re-discovers workers so that newly scaled-up pool
        replicas are picked up automatically.  Individual worker thread
        failures are tolerated as long as other workers can pick up the
        remaining batches.
        """
        active_threads: dict[str, threading.Thread] = {}
        errors: list[Exception] = []

        def _dispatch_wrapper(cname: str) -> None:
            try:
                self._worker_dispatch_loop(cname)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Worker thread for {cname} failed: {e}')
                errors.append(e)

        def _start_worker_thread(cluster_name: str) -> None:
            # Each thread needs its own context copy so that the log
            # redirect set up by the jobs controller is inherited.
            # contextvars.Context.run() is not re-entrant, so each
            # thread must use a separate copy.
            thread_ctx = contextvars.copy_context()
            t = threading.Thread(target=thread_ctx.run,
                                 args=(_dispatch_wrapper, cluster_name),
                                 daemon=True)
            t.start()
            active_threads[cluster_name] = t

        # Monitor until all batches complete, periodically discovering
        # new workers and spawning threads for them.  Worker startup is delayed
        # until after the durable lease snapshot so a replacement coordinator
        # cannot tear down a still-valid attempt on that replica.
        while not self._cancelled:
            self._assert_coordinator_owner()
            self._reclaim_expired_batches()
            # Retry unresolved pre-crash launch intents.  The API cannot make
            # the interval between accepting ``exec`` and returning its
            # request ID atomic with our DB, so a successor keeps polling the
            # exact persisted worker name until it can bind one unambiguous
            # external ID.  Duplicate matches are deliberately left alone.
            self._cleanup_stale_worker_services()
            completed_count, leased_workers, failed_batches = (
                self._sync_batch_progress_from_db())
            if completed_count >= len(self.batches):
                break
            if failed_batches:
                errors.append(
                    RuntimeError('Batch job has terminally failed batches: '
                                 f'{failed_batches}'))
                break

            alive = any(t.is_alive() for t in active_threads.values())
            has_pending = self._has_pending_batches()

            if not alive and not has_pending:
                if leased_workers:
                    # A previous coordinator incarnation still owns a live
                    # lease.  Wait for either its completion or expiry.
                    time.sleep(10)
                    continue
                break

            # Re-discover workers and start threads for idle ones.
            started_new = False
            worker_discovery_error: Exception | None = None
            try:
                current_workers = self._get_ready_workers()
                if not current_workers:
                    current_workers = self._workers
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Failed to rediscover Batch workers: {e}')
                worker_discovery_error = e
                current_workers = []
            for w in current_workers:
                already_active = (w in active_threads and
                                  active_threads[w].is_alive())
                if (not already_active and w not in leased_workers and
                        self._has_pending_batches()):
                    logger.info(f'Discovered new/idle worker: {w}')
                    _start_worker_thread(w)
                    started_new = True

            # If all threads are dead, work remains, and we couldn't
            # start any new threads, wait if durable leases are the only
            # blocker; otherwise there is nothing more we can do.
            if (not alive and self._has_pending_batches() and not started_new):
                if leased_workers:
                    time.sleep(10)
                    continue
                if worker_discovery_error is not None:
                    errors.append(worker_discovery_error)
                break

            time.sleep(10)

        # Wait for remaining threads to finish.
        for t in active_threads.values():
            t.join(timeout=60)

        completed_count, _, failed_batches = self._sync_batch_progress_from_db()
        if completed_count != len(self.batches):
            if failed_batches and not errors:
                errors.append(
                    RuntimeError('Batch job has terminally failed batches: '
                                 f'{failed_batches}'))
            if errors:
                raise errors[0]
            raise RuntimeError(
                f'Expected {len(self.batches)} completed batches, '
                f'got {completed_count}')

    # ------------------------------------------------------------------
    # Result merging
    # ------------------------------------------------------------------

    def _get_completed_batch_attempts(self) -> list[io_formats.BatchAttempt]:
        records = managed_job_state.get_batch_states(self._managed_job_id)
        attempts = []
        for rec in records:
            if rec['status'] != 'COMPLETED':
                raise RuntimeError(
                    f'Cannot reduce batch {rec["batch_idx"]} in state '
                    f'{rec["status"]}.')
            attempt_id = int(rec.get('attempt_id') or 0)
            if attempt_id <= 0:
                raise RuntimeError(
                    f'Completed batch {rec["batch_idx"]} has no fenced '
                    'attempt ID.')
            if not rec.get('attempt_owner_token'):
                raise RuntimeError(
                    f'Completed batch {rec["batch_idx"]} has no attempt '
                    'owner token.')
            attempts.append((rec['start_idx'], rec['end_idx'], attempt_id))
        if len(attempts) != len(self.batches):
            raise RuntimeError(f'Expected {len(self.batches)} completed batch '
                               f'attempts, found {len(attempts)}.')
        return attempts

    def _reduce_results(self) -> None:
        """Publish outputs from only the durably completed attempts."""
        self._assert_coordinator_owner()
        job_id = str(self._managed_job_id)
        batch_attempts = self._get_completed_batch_attempts()
        logger.info('Reducing results...')
        for fmt in self._output_formats:
            self._assert_coordinator_owner()
            logger.info(f'Handling output format: {type(fmt).__name__}')
            fmt.reduce_attempt_results(job_id, batch_attempts)
            self._assert_coordinator_owner()
            logger.info(f'Results written to {fmt.path}')

    def cleanup(self) -> None:
        """Best-effort cleanup after the managed job is durably SUCCEEDED."""
        self._assert_coordinator_owner()
        job_id = str(self._managed_job_id)
        for fmt in self._output_formats:
            self._assert_coordinator_owner()
            fmt.cleanup(job_id)
            logger.info(f'Cleaned up temp files for {fmt.path}')

    # ------------------------------------------------------------------
    # Partial results recovery
    # ------------------------------------------------------------------

    def _print_partial_results_instructions(self) -> None:
        """Print instructions for recovering partial results on failure."""
        output_formats = getattr(self, '_output_formats', [])
        if not output_formats:
            return
        logger.info(
            '\n'
            '============================================================\n'
            'Attempt-scoped partial results are preserved. Resume the managed\n'
            'job to publish only the attempts selected by durable batch '
            'state.\n')
        logger.info(
            '============================================================')
