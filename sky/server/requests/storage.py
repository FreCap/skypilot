"""Abstract interface for request persistence."""

from __future__ import annotations

import abc
from collections.abc import AsyncGenerator
from collections.abc import Generator
import contextlib
import contextvars
import dataclasses
import os
import pathlib
import signal
import time
from typing import Any, TYPE_CHECKING

import filelock

from sky.skylet import runtime_utils

if TYPE_CHECKING:
    from sky.server.requests.requests import Request
    from sky.server.requests.requests import RequestStatus
    from sky.server.requests.requests import RequestTaskFilter
    from sky.server.requests.requests import StatusWithMsg


@dataclasses.dataclass(frozen=True)
class ExecutionClaim:
    """Fencing identity carried by one request execution."""

    request_id: str
    execution_generation: int
    claim_token: str
    # Durable claimant identity, populated for PostgreSQL queue deliveries.
    # Local/plugin backends retain None and cannot execute bound handlers.
    worker_instance_id: str | None = None


ManagedJobControllerSlotIdentity = tuple[str, int, int, str]


class ManagedJobRequestQuiescenceError(RuntimeError):
    """An exact controller attempt could not prove nested effects stopped."""


_MANAGED_JOB_REQUEST_AUTHORITY_LOCK_PATH = runtime_utils.get_runtime_dir_path(
    '.sky/locks/managed_job_request_authority.lock')


def managed_job_request_authority_lock() -> filelock.FileLock:
    """Return the cross-SQLite authority lock for job and request state.

    PostgreSQL uses row locks in one database.  The non-consolidated local
    runtime still stores managed-job and API-request rows in separate SQLite
    files, so both sides take this one file lock before validating or changing
    a controller-attempt/request relationship.
    """
    pathlib.Path(_MANAGED_JOB_REQUEST_AUTHORITY_LOCK_PATH).parent.mkdir(
        parents=True, exist_ok=True)
    return filelock.FileLock(_MANAGED_JOB_REQUEST_AUTHORITY_LOCK_PATH)


def managed_job_request_authority_lock_async() -> filelock.AsyncFileLock:
    """Async counterpart using the exact same cross-SQLite lock file."""
    pathlib.Path(_MANAGED_JOB_REQUEST_AUTHORITY_LOCK_PATH).parent.mkdir(
        parents=True, exist_ok=True)
    return filelock.AsyncFileLock(_MANAGED_JOB_REQUEST_AUTHORITY_LOCK_PATH)


def read_linux_process_start_time_ticks(pid: int) -> int:
    """Read Linux procfs birth identity for one exact PID.

    Field 22 of ``/proc/<pid>/stat`` is the process start time in clock ticks
    since boot. Parsing from the final right parenthesis is required because a
    process ``comm`` may itself contain spaces and parentheses. Absence,
    permission errors, and malformed content remain distinct exceptions so
    callers can accept only an explicit absence or identity mismatch as proof.
    """
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError('Process PID must be a positive integer.')
    stat_path = pathlib.Path('/proc') / str(pid) / 'stat'
    content = stat_path.read_text(encoding='utf-8')
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError(f'Malformed process stat identity for PID {pid}.')
    fields_after_comm = content[comm_end + 1:].split()
    # The first token after comm is field 3 (state), so field 22 is index 19.
    if len(fields_after_comm) <= 19:
        raise ValueError(f'Malformed process stat identity for PID {pid}.')
    try:
        start_time_ticks = int(fields_after_comm[19])
    except ValueError as e:
        raise ValueError(
            f'Malformed process start identity for PID {pid}.') from e
    if start_time_ticks <= 0:
        raise ValueError(f'Invalid process start identity for PID {pid}.')
    return start_time_ticks


def signal_exact_local_process(pid: int, expected_start_time_ticks: int,
                               signum: int) -> bool:
    """Signal one same-UID guardian through its PID and birth identity.

    The pidfd binds signal delivery to the process that was opened, while the
    procfs start ticks reject PID reuse before delivery.  The process title
    prevents a corrupted request row from targeting an unrelated same-UID
    process.  Absence or any unverifiable identity fails closed.
    """
    if (isinstance(expected_start_time_ticks, bool) or
            not isinstance(expected_start_time_ticks, int) or
            expected_start_time_ticks <= 0 or isinstance(signum, bool) or
            not isinstance(signum, int)):
        return False
    pidfd_open = getattr(os, 'pidfd_open', None)
    pidfd_send_signal = getattr(signal, 'pidfd_send_signal', None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        return False
    try:
        pidfd = pidfd_open(pid, 0)  # pylint: disable=not-callable
    except (OSError, ProcessLookupError, ValueError):
        return False
    try:
        process_path = pathlib.Path('/proc') / str(pid)
        if process_path.stat().st_uid != os.geteuid():
            return False
        if read_linux_process_start_time_ticks(
                pid) != expected_start_time_ticks:
            return False
        command = (process_path / 'cmdline').read_bytes().split(b'\0', 1)[0]
        if not command.startswith(b'SkyPilot:executor:guardian:'):
            return False
        pidfd_send_signal(pidfd, signum, None, 0)  # pylint: disable=not-callable
        return True
    except (OSError, ProcessLookupError, ValueError):
        return False
    finally:
        os.close(pidfd)


_EXECUTION_CLAIM: contextvars.ContextVar[ExecutionClaim |
                                         None] = (contextvars.ContextVar(
                                             'sky_request_execution_claim',
                                             default=None))


def activate_execution_claim(
    request_id: str,
    execution_generation: int,
    claim_token: str | None,
    worker_instance_id: str | None = None,
) -> contextvars.Token:
    """Activate a durable claim for fenced writes in this context."""
    claim = None
    if claim_token is not None:
        claim = ExecutionClaim(request_id, execution_generation, claim_token,
                               worker_instance_id)
    return _EXECUTION_CLAIM.set(claim)


def deactivate_execution_claim(token: contextvars.Token) -> None:
    """Restore the previous execution claim."""
    _EXECUTION_CLAIM.reset(token)


def current_execution_claim(request_id: str) -> ExecutionClaim | None:
    """Return the active claim only when it belongs to this request."""
    claim = _EXECUTION_CLAIM.get()
    if claim is None or claim.request_id != request_id:
        return None
    return claim


def active_execution_claim() -> ExecutionClaim | None:
    """Return the claim carried by the current execution context, if any."""
    return _EXECUTION_CLAIM.get()


class RequestBackend(abc.ABC):
    """Abstract interface for request persistence and lifecycle."""

    @property
    def uses_durable_queue(self) -> bool:
        """Whether creation can atomically persist queue delivery."""
        return False

    @property
    def claim_heartbeat_interval_seconds(self) -> float | None:
        """Heartbeat cadence for claimed work, or None without leases."""
        return None

    @property
    def supports_local_execution_quiescence(self) -> bool:
        """Whether unclaimed invocations publish PID/birth-bound receipts."""
        return False

    @abc.abstractmethod
    def get_request(self,
                    request_id: str,
                    fields: list[str] | None = None) -> Request | None:
        """Get a request by ID with appropriate locking."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_request_async(
            self,
            request_id: str,
            fields: list[str] | None = None) -> Request | None:
        """Async version of get_request."""
        raise NotImplementedError

    @abc.abstractmethod
    @contextlib.contextmanager
    def update_request(
            self, request_id: str) -> Generator[Request | None, None, None]:
        """Atomic read-modify-write with appropriate locking.

        Yields the request object. Caller modifies it in-place. On context
        exit, the modified request is persisted. If the request doesn't exist,
        yields None.
        """
        raise NotImplementedError

    @abc.abstractmethod
    @contextlib.asynccontextmanager
    async def update_request_async(
            self, request_id: str) -> AsyncGenerator[Request | None, None]:
        """Async version of update_request."""
        del request_id
        yield None

    @abc.abstractmethod
    async def create_if_not_exists_async(self, request: Request) -> bool:
        """Create a request if it does not exist.

        Returns:
            True if a new request was created, False if it already exists.
        """
        raise NotImplementedError

    def retire_legacy_internal_daemon_rows(self) -> int:
        """Delete explicitly known legacy daemon request/queue rows.

        The built-in SQLite and PostgreSQL backends override this synchronous
        startup primitive. Plugin backends predate the transition and have no
        built-in legacy rows, so their source-compatible default is a no-op.
        """
        return 0

    @abc.abstractmethod
    def query_requests(self, req_filter: RequestTaskFilter) -> list[Request]:
        """Query requests matching the filter."""
        raise NotImplementedError

    @abc.abstractmethod
    async def query_requests_async(
            self, req_filter: RequestTaskFilter) -> list[Request]:
        """Async version of query_requests."""
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_requests(self, request_ids: list[str]) -> None:
        """Delete requests by their IDs."""
        raise NotImplementedError

    async def gc_request_owned_tombstones(self) -> int:
        """Collect backend-owned cross-domain tombstones, if supported.

        This source-compatible default keeps local and plugin request
        backends outside the central PostgreSQL binding contract.
        """
        return 0

    @abc.abstractmethod
    async def update_status_async(self, request_id: str,
                                  status: RequestStatus) -> None:
        """Update the status of a request."""
        raise NotImplementedError

    @abc.abstractmethod
    async def update_status_msg_async(self, request_id: str,
                                      status_msg: str) -> None:
        """Update the status message of a request."""
        raise NotImplementedError

    def try_mark_running(self,
                         request_id: str,
                         pid: int | None,
                         execution_generation: int = 0,
                         claim_token: str | None = None,
                         process_start_time_ticks: int | None = None) -> bool:
        """Atomically flip a request to RUNNING if it is still executable.

        Records `pid` and clears any stale retry-backoff status_msg. Non-
        abstract with a read-modify-write default so existing plugin
        backends stay source-compatible; backends may override with a
        single guarded UPDATE.

        Returns:
            True iff the request was in an executable status and is now
            RUNNING.
        """
        del execution_generation, claim_token, process_start_time_ticks
        # Runtime import to avoid a circular import: the requests module
        # imports this module at the top level.
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as requests_lib

        with self.update_request(request_id) as request:
            if request is None:
                return False
            if (request.status
                    not in requests_lib.RequestStatus.executable_statuses()):
                return False
            request.status = requests_lib.RequestStatus.RUNNING
            request.pid = pid
            request.status_msg = None
        return True

    def heartbeat_claim(self, claim: ExecutionClaim) -> bool:
        """Extend a durable execution lease.

        Returns False when the claim is stale. Local backends have no lease
        and therefore always return True.
        """
        del claim
        return True

    def interrupt_cancelled_claim(self, claim: ExecutionClaim) -> bool:
        """Signal a cancelled or otherwise revoked exact local claim.

        Distributed backends override this so an API process can record
        cancellation intent and the remote owning executor can deliver it. A
        failed lease renewal is also a revocation: the implementation may
        interrupt a still-RUNNING claim only when durable state proves that its
        exact generation no longer has execution authority.
        Returning True means only that the matching local generation was
        signalled or its process had already exited; it does not prove that
        effect-bearing handler code has quiesced.
        """
        del claim
        return False

    def acknowledge_execution_quiescence(self, claim: ExecutionClaim) -> bool:
        """Publish execution quiescence for one exact completed invocation.

        This must be called only after the execution Future has completed (or
        by the execution wrapper after its effect-bearing cleanup). Durable
        backends override it with request-generation, claim-token, and worker-
        instance fencing. The default keeps local/plugin backends compatible.

        Returns:
            True iff the exact claim is already recorded as quiescent or this
            call recorded it.
        """
        del claim
        return False

    def acknowledge_local_execution_quiescence(
            self, request_id: str, pid: int,
            process_start_time_ticks: int) -> bool:
        """Publish local stable-empty proof for one exact guardian birth.

        Only the parent Future monitor may call this after the disposable
        boundary reports a non-ambiguous result.  Durable-claim backends use
        :meth:`acknowledge_execution_quiescence` instead.
        """
        del request_id, pid, process_start_time_ticks
        return False

    def quiesce_managed_job_slot_requests(
            self,
            identity: ManagedJobControllerSlotIdentity,
            *,
            timeout_seconds: float = 60.0,
            poll_seconds: float = 0.1) -> int:
        """Revoke and wait for every nested request from one exact slot.

        Implementations must first close new admission for ``identity`` and
        may return only after every admitted request generation has an exact
        boundary-authored quiescence receipt.  Plugin backends fail closed:
        they cannot safely participate in managed-job controller handoff.
        """
        del identity, timeout_seconds, poll_seconds
        raise ManagedJobRequestQuiescenceError(
            'The request backend cannot prove managed-job nested request '
            'quiescence.')

    def quiesce_stale_managed_job_requests(self,
                                           current_owner: tuple[str, int],
                                           *,
                                           timeout_seconds: float = 60.0,
                                           poll_seconds: float = 0.1) -> int:
        """Close and quiesce nested work owned by older outer generations."""
        del current_owner, timeout_seconds, poll_seconds
        raise ManagedJobRequestQuiescenceError(
            'The request backend cannot prove stale managed-job request '
            'quiescence.')

    def converge_execution_completion(
            self,
            claim: ExecutionClaim,
            error: BaseException | None = None,
            terminal_cause: str = 'handler_failed') -> bool:
        """Converge one parent-proven callable completion and its receipt.

        The Future monitor calls this until it returns or the exact claim is
        definitively obsolete. Durable backends override it so outcome and
        receipt mutations are one idempotent transaction. The default keeps
        plugin backends source-compatible.

        Returns:
            True iff the receipt is durable. False iff this backend cannot
            accept the exact claim (including an obsolete identity).
        """
        if error is not None:
            # Runtime import avoids a requests <-> storage import cycle.
            # pylint: disable=import-outside-toplevel
            from sky.server.requests import requests as requests_lib

            self.transition_request_terminal(claim.request_id,
                                             requests_lib.RequestStatus.FAILED,
                                             terminal_cause,
                                             error=error)
        return self.acknowledge_execution_quiescence(claim)

    def handoff_execution_retry(self, claim: ExecutionClaim, status_msg: str,
                                retry_wait_seconds: float) -> bool | None:
        """Atomically consume one completion proof into a delayed retry.

        Durable backends override this primitive so the exact execution
        receipt, request ``WAITING`` transition, claim release, and delayed
        queue delivery are one transaction.  ``False`` means that cancellation
        or another owner already made this exact claim obsolete.  ``None`` is
        the source-compatible result for local/plugin backends without durable
        claims.
        """
        del claim, status_msg, retry_wait_seconds
        return None

    def interrupt_request_for_shutdown_retry(self,
                                             request_id: str) -> bool | None:
        """Atomically interrupt a durable claim for graceful-shutdown retry.

        ``None`` means the backend does not implement this durable primitive;
        callers preserve the legacy local-backend path. PostgreSQL overrides
        it with exact claim and process-birth fencing.
        """
        del request_id
        return None

    def set_request_finished(self,
                             request_id: str,
                             status: RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> bool:
        """Persist a terminal status (SUCCEEDED/FAILED) for a request.

        Must not overwrite a status that is already terminal: a late
        terminal write racing with a CANCELLED+should_retry marker from the
        graceful-shutdown sweep (or a kill) loses, mirroring the terminal-
        status guard on the kill paths. A `result` of None leaves the
        stored return value untouched. Non-abstract with a read-modify-
        write default so existing plugin backends stay source-compatible.

        Returns:
            True iff this call persisted the terminal transition.
        """
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as requests_lib

        with self.update_request(request_id) as request:
            if request is None:
                return False
            if request.status in requests_lib.RequestStatus.finished_status():
                return False
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                try:
                    request.set_return_value(result)
                except Exception as encoding_error:  # pylint: disable=broad-except
                    request.status = requests_lib.RequestStatus.FAILED
                    request.return_value = None
                    request.set_error(encoding_error)
        return True

    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> bool:
        """Async version of set_request_finished."""
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as requests_lib

        async with self.update_request_async(request_id) as request:
            if request is None:
                return False
            if request.status in requests_lib.RequestStatus.finished_status():
                return False
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                try:
                    request.set_return_value(result)
                except Exception as encoding_error:  # pylint: disable=broad-except
                    request.status = requests_lib.RequestStatus.FAILED
                    request.return_value = None
                    request.set_error(encoding_error)
        return True

    def transition_request_terminal(self,
                                    request_id: str,
                                    status: RequestStatus,
                                    cause: str,
                                    error: BaseException | None = None,
                                    result: Any | None = None) -> bool:
        """Persist a cause-aware terminal transition.

        PostgreSQL overrides this to atomically emit an operational event.
        The default keeps existing plugin and SQLite backends compatible.
        """
        del cause
        return self.set_request_finished(request_id,
                                         status,
                                         error=error,
                                         result=result)

    async def transition_request_terminal_async(
            self,
            request_id: str,
            status: RequestStatus,
            cause: str,
            error: BaseException | None = None,
            result: Any | None = None) -> bool:
        """Async cause-aware terminal transition."""
        del cause
        return await self.set_request_finished_async(request_id,
                                                     status,
                                                     error=error,
                                                     result=result)

    def set_event_workspace(self, request_id: str, workspace: str) -> bool:
        """Persist an authoritative event workspace, if supported."""
        del request_id, workspace
        return True

    def set_event_target_id(self, request_id: str, target_id: str) -> bool:
        """Enrich an event target identity, if supported."""
        del request_id, target_id
        return True

    @abc.abstractmethod
    def kill_requests(self,
                      request_ids: list[str] | None = None,
                      user_id: str | None = None) -> list[str]:
        """Kill requests and set their status to CANCELLED.

        Returns:
            A list of request IDs that were cancelled.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def kill_request_async(self, request_id: str) -> bool:
        """Kill a single request and set its status to cancelled.

        Returns:
            True if the request was killed, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_latest_request_id_async(self) -> str | None:
        """Get the most recent request ID."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_requests_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None) -> list[Request] | None:
        """Get all requests matching an ID prefix."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_requests_async_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None) -> list[Request] | None:
        """Async version of get_requests_with_prefix."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_request_status_async(
            self,
            request_id: str,
            include_msg: bool = False) -> StatusWithMsg | None:
        """Get the status (and optionally status_msg) of a request."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_api_request_ids_start_with(self,
                                             incomplete: str) -> list[str]:
        """Get request IDs for shell completion."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_active_file_mounts_blob_ids(self) -> set[str]:
        """Get blob IDs referenced by active (PENDING/RUNNING) requests."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_shutdown_active_requests(self) -> list[tuple[str, str]]:
        """Get (request_id, name) pairs to wait for during graceful shutdown."""
        raise NotImplementedError

    def reset_on_startup(self) -> None:
        """Called on server startup for backend-specific initialization."""


_storage_backend: RequestBackend | None = None


def get_request_backend() -> RequestBackend:
    """Get the registered request backend."""
    global _storage_backend
    if _storage_backend is None:
        backend_name = os.environ.get('SKYPILOT_API_REQUEST_BACKEND', 'sqlite')
        if backend_name == 'postgres':
            # Runtime import avoids storage -> postgres -> storage while the
            # abstract interface is still being defined.
            # pylint: disable=import-outside-toplevel
            from sky.server.requests import postgres
            _storage_backend = postgres.PostgresRequestBackend()
        elif backend_name == 'sqlite':
            # Runtime import avoids storage -> requests -> storage while the
            # abstract interface is still being defined.
            # pylint: disable=import-outside-toplevel
            from sky.server.requests.requests import SqliteRequestBackend
            _storage_backend = SqliteRequestBackend()
        else:
            raise ValueError('SKYPILOT_API_REQUEST_BACKEND must be "sqlite" or '
                             f'"postgres", got {backend_name!r}.')
    return _storage_backend


def set_request_backend(backend: RequestBackend) -> None:
    """Set the request backend."""
    global _storage_backend
    _storage_backend = backend
