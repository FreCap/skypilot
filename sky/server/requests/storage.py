"""Abstract interface for request persistence."""

from __future__ import annotations

import abc
from collections.abc import AsyncGenerator
from collections.abc import Generator
import contextlib
import contextvars
import dataclasses
import hashlib
import os
import pathlib
import time
from typing import Any, TYPE_CHECKING

from sky.server.requests.serializers import encoders

if TYPE_CHECKING:
    from sky.server import daemons as daemons_lib
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


_EXECUTION_CANCELLATION_DIRECTORY = pathlib.Path(
    '/tmp/skypilot-execution-cancellation')
_EXECUTION_CANCELLATION_MARKER_MAX_AGE_SECONDS = 24 * 60 * 60


def execution_cancellation_marker_path(pid: int,
                                       claim: ExecutionClaim) -> pathlib.Path:
    """Return the unguessable-enough local marker for one exact invocation."""
    if pid <= 0:
        raise ValueError('Execution cancellation PID must be positive.')
    identity = (f'{pid}\0{claim.request_id}\0{claim.execution_generation}\0'
                f'{claim.claim_token}').encode()
    digest = hashlib.sha256(identity).hexdigest()
    return _EXECUTION_CANCELLATION_DIRECTORY / f'{pid}-{digest}'


def arm_execution_cancellation(pid: int, claim: ExecutionClaim) -> pathlib.Path:
    """Create an exact-claim marker before signalling a reusable worker PID."""
    marker = execution_cancellation_marker_path(pid, claim)
    marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cutoff = time.time() - _EXECUTION_CANCELLATION_MARKER_MAX_AGE_SECONDS
    # A worker normally consumes the marker. If it died just before delivery,
    # no handshake is possible; bounded-age pruning prevents those markers from
    # leaking inodes forever without weakening delayed-signal protection.
    try:
        for candidate in marker.parent.iterdir():
            try:
                if candidate.stat().st_mtime < cutoff:
                    candidate.unlink()
            except (FileNotFoundError, OSError):
                continue
    except OSError:
        pass
    # O_NOFOLLOW prevents replacing the final marker with a symlink in the
    # shared container filesystem.  The claim token makes paths invocation-
    # unique, so truncation is safe for duplicate cancellation delivery.
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    flags |= getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(marker, flags, 0o600)
    os.close(fd)
    return marker


def consume_execution_cancellation(marker: pathlib.Path) -> bool:
    """Atomically consume an exact-claim marker, if it is armed."""
    try:
        marker.unlink()
    except FileNotFoundError:
        return False
    return True


def clear_execution_cancellation(marker: pathlib.Path | None) -> None:
    """Best-effort removal of this invocation's marker."""
    if marker is None:
        return
    try:
        marker.unlink()
    except OSError:
        # Cleanup must never turn an otherwise completed wrapper Future into
        # an ambiguous failure: that would suppress the exact-generation
        # quiescence receipt forever.  A leftover marker is harmless because
        # the path is claim-token and generation addressed and is pruned after
        # a bounded age.
        pass


_EXECUTION_CLAIM: contextvars.ContextVar[ExecutionClaim |
                                         None] = (contextvars.ContextVar(
                                             'sky_request_execution_claim',
                                             default=None))


def activate_execution_claim(
    request_id: str,
    execution_generation: int,
    claim_token: str | None,
) -> contextvars.Token:
    """Activate a durable claim for fenced writes in this context."""
    claim = None
    if claim_token is not None:
        claim = ExecutionClaim(request_id, execution_generation, claim_token)
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

    @abc.abstractmethod
    async def create_or_refresh_internal_daemon_async(self,
                                                      request: Request) -> bool:
        """For an internal daemon request: insert a fresh PENDING row or
        refresh env-bearing columns on an existing row.

        Returns True if a new row was inserted (caller should enqueue
        the request onto the task queue), False if an existing row was
        refreshed in-place (the task_queue entry from the original
        creator stays in place; do NOT enqueue again).

        Atomic + idempotent under concurrent callers. Replaces
        `create_if_not_exists_async` on the daemon submission path:
        the dedup contract is identical (exactly one concurrent caller
        gets True), but losing callers also UPDATE `request_body`,
        `name`, and `schedule_type` on the existing row so the
        persisted `env_vars` reflect the current process's
        `os.environ` rather than whatever the original creator
        captured (which may be from a previous deployment generation
        in HA setups).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_orphan_internal_daemons_async(
        self,
        internal_daemons: list[daemons_lib.InternalRequestDaemon],
    ) -> None:
        """Delete daemon-shaped rows whose `request_id` is not in
        `internal_daemons` (daemon was renamed / removed in code),
        along with any task_queue entries (for backends with a
        persistent queue).

        Idempotent under concurrent callers.
        """
        raise NotImplementedError

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
                         claim_token: str | None = None) -> bool:
        """Atomically flip a request to RUNNING if it is still executable.

        Records `pid` and clears any stale retry-backoff status_msg. Non-
        abstract with a read-modify-write default so existing plugin
        backends stay source-compatible; backends may override with a
        single guarded UPDATE.

        Returns:
            True iff the request was in an executable status and is now
            RUNNING.
        """
        del execution_generation, claim_token
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
        """Signal a cancelled claim owned by this backend instance.

        Distributed backends override this so an API process can record
        cancellation intent and the remote owning executor can deliver it.
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
            should_encode_result = result is not None
            if status == requests_lib.RequestStatus.SUCCEEDED:
                should_encode_result = (should_encode_result or
                                        encoders.requires_strict_return_value(
                                            request.name))
            if should_encode_result:
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
            should_encode_result = result is not None
            if status == requests_lib.RequestStatus.SUCCEEDED:
                should_encode_result = (should_encode_result or
                                        encoders.requires_strict_return_value(
                                            request.name))
            if should_encode_result:
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
