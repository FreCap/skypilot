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
import base64
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
from typing import Any, Deque, Dict, List, Optional, Tuple
import uuid

import sky
from sky.batch import constants
from sky.batch import io_formats
from sky.client import sdk
from sky.jobs import state as managed_job_state
from sky.server import constants as server_constants
from sky.skylet import constants as skylet_constants

logger = logging.getLogger(__name__)

# Runs on the worker node before we start a fresh worker service: if
# port {port} (the WORKER_SERVICE_PORT) is still bound by a stale
# worker from the previous controller incarnation, SIGTERM/SIGKILL its
# holder and wait up to 30s for the port to free.  Plain-format string
# (single ``{port}`` placeholder) so it can be injected into shell code
# via ``str.format``/``shlex.quote`` without f-string/heredoc issues.
_PORT_CLEANUP_PY = """
import errno, os, signal, socket, time

PORT = {port}

def port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return False
        except OSError as e:
            return e.errno == errno.EADDRINUSE


def kill_listeners(port):
    hex_port = f"{{port:04X}}"
    listener_inodes = set()
    try:
        with open("/proc/net/tcp", "r") as f:
            next(f, None)
            for line in f:
                parts = line.split()
                if len(parts) < 10:
                    continue
                local, state, inode = parts[1], parts[3], parts[9]
                if state == "0A" and local.endswith(f":{{hex_port}}"):
                    listener_inodes.add(inode)
    except FileNotFoundError:
        return
    if not listener_inodes:
        return
    my_pid = os.getpid()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == my_pid:
            continue
        fd_dir = f"/proc/{{pid}}/fd"
        try:
            fds = os.listdir(fd_dir)
        except (FileNotFoundError, PermissionError):
            continue
        matched = False
        for fd in fds:
            try:
                target = os.readlink(os.path.join(fd_dir, fd))
            except (FileNotFoundError, PermissionError):
                continue
            if target.startswith("socket:["):
                inode = target[len("socket:["):-1]
                if inode in listener_inodes:
                    matched = True
                    break
        if matched:
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    break
                time.sleep(1)
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break


if port_in_use(PORT):
    print(f"Port {{PORT}} is in use; killing stale holder(s)...", flush=True)
    kill_listeners(PORT)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and port_in_use(PORT):
        time.sleep(1)
    if port_in_use(PORT):
        print(f"WARNING: port {{PORT}} still in use after cleanup",
              flush=True)
    else:
        print(f"Port {{PORT}} is now free", flush=True)
"""


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
                 input_format_dict: Dict[str, Any],
                 output_formats_dict: List[Dict[str, Any]],
                 activate_env: str = '',
                 job_id: Optional[int] = None,
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
        self.batches: List[List[int]] = []
        self.pending_batches: Deque[int] = collections.deque()
        self._pending_ready_at: Dict[int, float] = {}
        self._pending_lock = threading.Lock()
        self.completed_count: int = 0
        self._state_lock = threading.Lock()

        # Retry tracking: batch_idx -> retry count.  Persisted across
        # resume so that a batch cannot be retried indefinitely.
        self._retry_counts: Dict[int, int] = {}

        # Worker tracking: cluster_name → worker_job_id
        self._active_workers: Dict[str, int] = {}
        self._active_workers_lock = threading.Lock()

        # Cancellation flag for inline (controller) mode.
        self._cancelled = False

        # Identifies worker services started by this coordinator incarnation.
        # It prevents an older controller from feeding or shutting down a
        # replacement service after an API-server/controller restart.
        self._worker_token = uuid.uuid4().hex

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
            self._resolve_formats()

            if self._is_resume:
                self._resume_from_db()
            else:
                self._count_and_split()
                if not self.batches:
                    logger.info('No items in dataset — nothing to do.')
                    return
                self._save_batches_to_db()

            if self.completed_count == len(self.batches):
                # Crash happened after all batches done but before merge.
                logger.info('All batches already completed, skipping '
                            'to merge.')
                managed_job_state.set_winding_down(self._managed_job_id,
                                                   task_id=0)
                self._reduce_results()
                return

            self._discover_workers()
            self._dispatch_all()
            managed_job_state.set_winding_down(self._managed_job_id, task_id=0)
            self._reduce_results()
            logger.info('Batch job completed successfully.')
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
        self._cancelled = True
        with self._active_workers_lock:
            workers_snapshot = list(self._active_workers.items())
        for cluster_name, worker_job_id in workers_snapshot:
            try:
                self._shutdown_worker(cluster_name, worker_job_id)
            except Exception:  # pylint: disable=broad-except
                logger.warning(f'Failed to shutdown worker on {cluster_name}')

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

    def _pop_ready_batch(self) -> Tuple[Optional[int], Optional[float]]:
        """Pop an eligible batch and return a wait time for delayed work.

        ``(None, None)`` means the queue is empty.  ``(None, seconds)`` means
        work exists but is still in persisted retry backoff.
        """
        now = time.time()
        with self._pending_lock:
            if not self.pending_batches:
                return None, None
            min_wait: Optional[float] = None
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

    def _get_completed_count(self) -> int:
        with self._state_lock:
            return self.completed_count

    def _increment_completed_count(self) -> int:
        with self._state_lock:
            self.completed_count += 1
            return self.completed_count

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
        managed_job_state.save_batch_states(self._managed_job_id, self.batches)
        logger.info(f'Saved {len(self.batches)} batch records to DB')

    def _resume_from_db(self) -> None:
        """Restore coordinator state from DB after a controller crash.

        Reclaims expired attempts, then rebuilds in-memory state from the
        persisted records.  Live attempts keep their leases until they expire.
        """
        reclaimed = managed_job_state.requeue_expired_batch_attempts(
            self._managed_job_id)
        if reclaimed:
            logger.info('Reclaimed expired batch attempts: %s', reclaimed)
        records = managed_job_state.get_batch_states(self._managed_job_id)
        if not records:
            raise RuntimeError(
                f'No batch records found for job {self._managed_job_id} '
                'during resume. The job may need to be re-submitted.')

        self.batches = []
        self._reset_pending_batches()
        self.completed_count = 0
        self._retry_counts = {}
        failed_batches = []
        leased_batches = 0

        for i, rec in enumerate(records):
            batch_idx = rec['batch_idx']
            assert batch_idx == i, (
                f'Batch records not contiguous: expected batch_idx={i}, '
                f'got {batch_idx}. DB may be corrupted.')
            self.batches.append([rec['start_idx'], rec['end_idx']])
            status = rec['status']
            if status == 'PENDING':
                self._enqueue_batch(batch_idx, rec.get('next_retry_at') or 0)
            elif status == 'COMPLETED':
                self.completed_count += 1
            elif status == 'DISPATCHED':
                leased_batches += 1
            elif status == 'FAILED':
                failed_batches.append(batch_idx)
            self._retry_counts[batch_idx] = rec['retry_count']

        if failed_batches:
            raise RuntimeError('Cannot resume batch job with terminally failed '
                               f'batches: {failed_batches}')

        logger.info(f'Resumed from DB: {len(self.batches)} batches, '
                    f'{self.completed_count} completed, '
                    f'{self._pending_count()} pending, '
                    f'{leased_batches} leased')
        logger.info(f'BATCH_RESUME total={len(self.batches)} '
                    f'completed={self.completed_count} '
                    f'pending={self._pending_count()} '
                    f'leased={leased_batches}')

    def _reclaim_expired_batches(self) -> int:
        reclaimed = managed_job_state.requeue_expired_batch_attempts(
            self._managed_job_id)
        for batch_idx in reclaimed:
            self._enqueue_batch(batch_idx)
        if reclaimed:
            logger.warning('Reclaimed %d expired batch attempt(s): %s',
                           len(reclaimed), reclaimed)
        return len(reclaimed)

    def _sync_batch_progress_from_db(self) -> Tuple[int, List[int]]:
        """Refresh progress and discover PENDING work from another owner."""
        records = managed_job_state.get_batch_states(self._managed_job_id)
        completed_count = 0
        leased_count = 0
        failed_batches = []
        for rec in records:
            batch_idx = rec['batch_idx']
            status = rec['status']
            if status == 'PENDING':
                self._enqueue_batch(batch_idx, rec.get('next_retry_at') or 0)
            elif status == 'COMPLETED':
                completed_count += 1
            elif status == 'DISPATCHED':
                leased_count += 1
            elif status == 'FAILED':
                failed_batches.append(batch_idx)
            self._retry_counts[batch_idx] = rec['retry_count']
        with self._state_lock:
            self.completed_count = completed_count
        return leased_count, failed_batches

    def _start_batch_lease_renewer(
        self, batch_idx: int, attempt_id: int
    ) -> Tuple[threading.Event, threading.Event, threading.Thread]:
        """Keep a batch lease alive while SDK calls may be blocking."""
        stop_event = threading.Event()
        lost_event = threading.Event()

        def _renew() -> None:
            while not stop_event.wait(constants.BATCH_LEASE_RENEW_INTERVAL):
                try:
                    owned = managed_job_state.renew_batch_lease(
                        self._managed_job_id, batch_idx, attempt_id,
                        constants.BATCH_LEASE_DURATION)
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

    def _fetch_pool_status(self) -> Optional[Dict[str, Any]]:
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

    def _get_ready_workers(self) -> List[str]:
        """Return cluster names for ready replicas via SDK."""
        status = self._fetch_pool_status()
        if status is None:
            return []
        replica_infos = status.get('replica_info', [])
        ready = []
        unavailable_summary: List[str] = []
        for info in replica_infos:
            raw_status = info.get('status', '')
            # status may be a ReplicaStatus enum or a string; normalise.
            replica_status = (raw_status.value if hasattr(raw_status, 'value')
                              else str(raw_status))
            name = info.get('name')
            if replica_status == 'READY':
                replica_info_version = info.get('replica_info_version')
                if (replica_info_version is None or replica_info_version <
                        server_constants.MIN_BATCH_REPLICA_INFO_VERSION):
                    raise RuntimeError(
                        f'Pool replica {name!r} uses worker runtime version '
                        f'{replica_info_version}; Sky Batch requires version '
                        f'{server_constants.MIN_BATCH_REPLICA_INFO_VERSION}. '
                        f'Recreate pool {self.pool_name!r} before retrying.')
                used_by = info.get('used_by') or []
                if not isinstance(used_by, (list, tuple, set)):
                    used_by = [used_by]
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

    def _generate_worker_startup_code(self) -> str:
        """Generate code to start the long-running worker service."""
        job_id = str(self._managed_job_id)
        activate = self.activate_env.strip()
        activate_line = f'{activate} &&' if activate else ''
        sky_runtime = skylet_constants.SKY_REMOTE_PYTHON_ENV

        # Serialize typed format dicts as JSON env vars for workers.
        input_format_json = json.dumps(self._input_format.to_dict()).replace(
            '\'', '\'\\\'\'')
        # Pass output formats as a JSON array for multi-output support.
        output_formats_json = json.dumps(self._output_formats_dict or
                                         []).replace('\'', '\'\\\'\'')

        port = constants.WORKER_SERVICE_PORT
        # Python snippet that frees port {port} if a stale worker
        # service is still holding it after a controller crash.  Kept
        # as a module-level constant (``_PORT_CLEANUP_PY``) so it is
        # not reindented by the surrounding textwrap.dedent call, then
        # base64-encoded so the whole thing is a single opaque token in
        # the generated shell script (no multi-line content that would
        # confuse textwrap.dedent).
        port_cleanup_py = _PORT_CLEANUP_PY.format(port=port)
        port_cleanup_b64 = base64.b64encode(
            port_cleanup_py.encode('utf-8')).decode('ascii')
        return textwrap.dedent(f"""\
            set -e
            export SKY_BATCH_SERIALIZED_FN='{self.serialized_fn}'
            export SKY_BATCH_OUTPUT_PATH='{self.output_path}'
            export SKY_BATCH_JOB_ID='{job_id}'
            export SKY_BATCH_WORKER_TOKEN='{self._worker_token}'
            export SKY_BATCH_INPUT_FORMAT='{input_format_json}'
            export SKY_BATCH_OUTPUT_FORMATS='{output_formats_json}'

            # On HA resume the previous worker service may still hold port
            # {port}. Free only that listener before binding a replacement;
            # do not cancel unrelated jobs that share this pool replica.
            echo '{port_cleanup_b64}' | base64 -d | {sky_runtime}/bin/python -

            # Make sky.batch visible to the user's python.
            SKY_SITE=$({sky_runtime}/bin/python -c \\
              "import site; print(site.getsitepackages()[0])")
            export PYTHONPATH="${{SKY_SITE}}:${{PYTHONPATH}}"

            # Ensure boto3 is available in the user env.
            {activate_line} pip install boto3 2>/dev/null

            # Start worker service in the activated environment.
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
            curl -sf -X POST http://127.0.0.1:{port}/shutdown \\
                -H 'X-Sky-Batch-Worker-Token: {self._worker_token}' || true
            """)

    # ------------------------------------------------------------------
    # Worker service lifecycle
    # ------------------------------------------------------------------

    def _launch_worker_service(self, cluster_name: str) -> int:
        """Launch worker service as a long-running SkyPilot job.

        Returns:
            The SkyPilot job ID of the worker service.
        """
        job_id = str(self._managed_job_id)
        startup_code = self._generate_worker_startup_code()
        task = sky.Task(name=f'batch-worker-{job_id}', run=startup_code)
        pool_resources = self._get_pool_resources()
        if pool_resources is not None:
            task.set_resources(pool_resources)
        logger.info(f'Submitting exec to {cluster_name} '
                    f'with resources={pool_resources}')
        try:
            request_id = sdk.exec(task, cluster_name=cluster_name)
        except Exception as e:
            logger.error(f'sdk.exec() failed: {e}', exc_info=True)
            raise
        try:
            worker_job_id, _ = sdk.get(request_id)
        except Exception as e:
            logger.error(f'sdk.get() for exec failed: {e}', exc_info=True)
            raise
        assert worker_job_id is not None, 'Failed to get worker job ID'

        logger.info(f'Launched worker service as job '
                    f'{worker_job_id} on {cluster_name}')

        # Wait for worker to be ready
        port = constants.WORKER_SERVICE_PORT
        timeout = constants.WORKER_SERVICE_STARTUP_TIMEOUT
        health_code = textwrap.dedent(f"""\
            set -e
            for i in $(seq 1 {timeout}); do
                if curl -sf http://127.0.0.1:{port}/health \\
                    -H 'X-Sky-Batch-Worker-Token: {self._worker_token}' \\
                    > /dev/null 2>&1; then
                    echo "Worker service ready after $i seconds"
                    exit 0
                fi
                sleep 1
            done
            echo "ERROR: Worker service did not start within {timeout}s"
            exit 1
            """)
        health_task = sky.Task(name=f'health-check-{job_id}', run=health_code)
        try:
            req_id = sdk.exec(health_task, cluster_name=cluster_name)
            sdk.get(req_id)
            logger.info(f'Worker service ready on {cluster_name}')
            return worker_job_id
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(
                f'Worker service on {cluster_name} failed to start: '
                f'{e}') from e

    def _shutdown_worker(self,
                         cluster_name: str,
                         worker_job_id: Optional[int] = None) -> None:
        """Send shutdown signal and cancel worker job."""
        shutdown_code = self._generate_shutdown_code()
        task = sky.Task(name=f'batch-shutdown-{cluster_name}',
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
                cancel_req_id = sdk.cancel(cluster_name,
                                           job_ids=[worker_job_id])
                sdk.get(cancel_req_id)
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

                retries = self._retry_counts.get(batch_idx, 0)
                attempt_id = managed_job_state.claim_batch(
                    self._managed_job_id, batch_idx, cluster_name,
                    constants.BATCH_LEASE_DURATION)
                if attempt_id is None:
                    # Another coordinator/thread won the claim, or persisted
                    # retry backoff has not elapsed yet.
                    continue
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
                        'COMPLETED')
                    if not completed:
                        logger.warning(
                            'Ignoring completion from stale batch attempt '
                            '%d/%d', batch_idx, attempt_id)
                        continue
                    completed_count = self._increment_completed_count()
                    logger.info(
                        f'Batch {batch_idx} completed on {cluster_name} '
                        f'({completed_count}/{len(self.batches)})')
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
                            'PENDING',
                            retry_count=retry_count,
                            next_retry_at=ready_at)
                        if requeued:
                            self._retry_counts[batch_idx] = retry_count
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
            self._shutdown_worker(cluster_name, worker_job_id=worker_job_id)
            with self._active_workers_lock:
                self._active_workers.pop(cluster_name, None)

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
        active_threads: Dict[str, threading.Thread] = {}
        errors: List[Exception] = []

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

        # Start initial workers.
        for cluster_name in self._workers:
            _start_worker_thread(cluster_name)

        # Monitor until all batches complete, periodically discovering
        # new workers and spawning threads for them.
        while not self._cancelled:
            self._reclaim_expired_batches()
            leased_count, failed_batches = self._sync_batch_progress_from_db()
            completed_count = self._get_completed_count()
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
                if leased_count:
                    # A previous coordinator incarnation still owns a live
                    # lease.  Wait for either its completion or expiry.
                    time.sleep(10)
                    continue
                break

            # Re-discover workers and start threads for idle ones.
            started_new = False
            try:
                current_workers = self._get_ready_workers()
                for w in current_workers:
                    already_active = (w in active_threads and
                                      active_threads[w].is_alive())
                    if not already_active and self._has_pending_batches():
                        logger.info(f'Discovered new/idle worker: {w}')
                        try:
                            self._shutdown_worker(w)
                        except Exception:  # pylint: disable=broad-except
                            pass
                        _start_worker_thread(w)
                        started_new = True
            except Exception:  # pylint: disable=broad-except
                pass

            # If all threads are dead, work remains, and we couldn't
            # start any new threads, there's nothing more we can do.
            if (not alive and self._has_pending_batches() and not started_new):
                break

            time.sleep(10)

        # Wait for remaining threads to finish.
        for t in active_threads.values():
            t.join(timeout=60)

        completed_count = self._get_completed_count()
        if completed_count != len(self.batches):
            if errors:
                raise errors[0]
            raise RuntimeError(
                f'Expected {len(self.batches)} completed batches, '
                f'got {completed_count}')

    # ------------------------------------------------------------------
    # Result merging
    # ------------------------------------------------------------------

    def _get_completed_batch_attempts(self) -> List[io_formats.BatchAttempt]:
        records = managed_job_state.get_batch_states(self._managed_job_id)
        attempts = []
        for rec in records:
            if rec['status'] != 'COMPLETED':
                raise RuntimeError(
                    f'Cannot reduce batch {rec["batch_idx"]} in state '
                    f'{rec["status"]}.')
            attempts.append((rec['start_idx'], rec['end_idx'],
                             int(rec.get('attempt_id') or 0)))
        if len(attempts) != len(self.batches):
            raise RuntimeError(f'Expected {len(self.batches)} completed batch '
                               f'attempts, found {len(attempts)}.')
        return attempts

    def _reduce_results(self) -> None:
        """Publish outputs from only the durably completed attempts."""
        job_id = str(self._managed_job_id)
        batch_attempts = self._get_completed_batch_attempts()
        logger.info('Reducing results...')
        for fmt in self._output_formats:
            logger.info(f'Handling output format: {type(fmt).__name__}')
            fmt.reduce_attempt_results(job_id, batch_attempts)
            logger.info(f'Results written to {fmt.path}')

    def cleanup(self) -> None:
        """Best-effort cleanup after the managed job is durably SUCCEEDED."""
        job_id = str(self._managed_job_id)
        for fmt in self._output_formats:
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
