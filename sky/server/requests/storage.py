"""Abstract interface for request persistence."""

from __future__ import annotations

import abc
from collections.abc import AsyncGenerator
from collections.abc import Generator
import contextlib
import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sky.server import daemons as daemons_lib
    from sky.server.requests.requests import Request
    from sky.server.requests.requests import RequestStatus
    from sky.server.requests.requests import RequestTaskFilter
    from sky.server.requests.requests import StatusWithMsg


class RequestBackend(abc.ABC):
    """Abstract interface for request persistence and lifecycle."""

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

    def try_mark_running(self, request_id: str, pid: int | None) -> bool:
        """Atomically flip a request to RUNNING if it is still executable.

        Records `pid` and clears any stale retry-backoff status_msg. Non-
        abstract with a read-modify-write default so existing plugin
        backends stay source-compatible; backends may override with a
        single guarded UPDATE.

        Returns:
            True iff the request was in an executable status and is now
            RUNNING.
        """
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

    def set_request_finished(self,
                             request_id: str,
                             status: RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> None:
        """Persist a terminal status (SUCCEEDED/FAILED) for a request.

        Must not overwrite a status that is already terminal: a late
        terminal write racing with a CANCELLED+should_retry marker from the
        graceful-shutdown sweep (or a kill) loses, mirroring the terminal-
        status guard on the kill paths. A `result` of None leaves the
        stored return value untouched. Non-abstract with a read-modify-
        write default so existing plugin backends stay source-compatible.
        """
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as requests_lib

        with self.update_request(request_id) as request:
            if request is None:
                return
            if request.status in requests_lib.RequestStatus.finished_status():
                return
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                request.set_return_value(result)

    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> None:
        """Async version of set_request_finished."""
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as requests_lib

        async with self.update_request_async(request_id) as request:
            if request is None:
                return
            if request.status in requests_lib.RequestStatus.finished_status():
                return
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                request.set_return_value(result)

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
        # pylint: disable=import-outside-toplevel
        from sky.server.requests.requests import SqliteRequestBackend

        _storage_backend = SqliteRequestBackend()
    return _storage_backend


def set_request_backend(backend: RequestBackend) -> None:
    """Set the request backend."""
    global _storage_backend
    _storage_backend = backend
