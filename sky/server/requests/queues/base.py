"""Abstract interfaces for request queue backends."""

import abc
import dataclasses
import enum
import multiprocessing
import os
import queue as queue_lib

from sky import sky_logging
from sky.server.requests import requests as api_requests
from sky.server.requests.queues import local_queue
from sky.server.requests.queues import mp_queue
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)


class ProviderMutationRequestKind(enum.Enum):
    """Closed queue classification for provider-mutating requests.

    API009 currently has one provider-mutating request handler.  Future typed
    provider handlers must be added to this enum and to the PostgreSQL
    handler-to-kind map before the generic queue is allowed to see them.
    """

    BOUND_ORDINARY_LAUNCH = 'bound_ordinary_launch'


@dataclasses.dataclass(frozen=True)
class ProviderMutationCandidate:
    """Non-authoritative hint returned before a durable queue claim."""

    request_id: str
    kind: ProviderMutationRequestKind


@dataclasses.dataclass(frozen=True)
class QueueItem:
    """One queue delivery plus an optional durable execution claim."""

    request_id: str
    ignore_return_value: bool
    retryable: bool
    execution_generation: int = 0
    claim_token: str | None = None
    worker_instance_id: str | None = None
    # Present only for authenticated nested requests from one exact managed-
    # job controller attempt.  Dispatchers use this complete tuple to decide
    # whether the disposable handler may receive controller capability.
    managed_job_origin: tuple[int, str, int, int, str] | None = None


QueueItemLike = QueueItem | tuple[str, bool, bool]


def normalize_queue_item(item: QueueItemLike) -> QueueItem:
    """Adapt legacy plugin tuples to the durable queue-item contract."""
    if isinstance(item, QueueItem):
        return item
    request_id, ignore_return_value, retryable = item
    return QueueItem(request_id, ignore_return_value, retryable)


class QueueBackend(abc.ABC):
    """Abstract queue backend."""

    @abc.abstractmethod
    def put(self, item: QueueItemLike) -> None:
        """Put a (request_id, ignore_return_value, retryable) tuple."""
        raise NotImplementedError

    async def put_async(self, item: QueueItemLike) -> None:
        """Async version of put."""
        # By default we assume put is not blocking and can be
        # called directly in event loop
        self.put(item)

    @abc.abstractmethod
    def get(self) -> QueueItemLike | None:
        """Non-blocking get. Returns None if queue is empty."""
        raise NotImplementedError

    def peek_provider_mutation(self) -> ProviderMutationCandidate | None:
        """Read a provider candidate without claiming durable ownership.

        Legacy/plugin queues return ``None`` and retain their historical
        generic delivery behavior.  A backend overriding this method must also
        override :meth:`claim_provider_mutation` and exclude every kind in
        :class:`ProviderMutationRequestKind` from :meth:`get`.
        """
        return None

    def claim_provider_mutation(
            self, candidate: ProviderMutationCandidate) -> QueueItem | None:
        """Try to claim an exact previously observed provider candidate."""
        del candidate
        raise NotImplementedError(
            'This queue backend does not support reserved provider claims.')

    @abc.abstractmethod
    def qsize(self) -> int:
        """Return approximate queue size."""
        raise NotImplementedError


class QueueBackendFactory(abc.ABC):
    """Creates queue instances and manages queue infrastructure."""

    @abc.abstractmethod
    def create_queue(self, schedule_type: str) -> QueueBackend:
        """Create a queue for the given schedule type.

        Args:
            schedule_type: The schedule type string (e.g., 'long', 'short').
        """
        raise NotImplementedError

    def start(self) -> multiprocessing.Process | None:
        """Start any required background infrastructure.

        Returns:
            A process to join on shutdown, or None if no background process
            is needed.
        """
        return None

    def stop(self, process: multiprocessing.Process | None) -> None:
        """Cleanup infrastructure."""
        if process is not None:
            process.kill()


class LocalQueueBackend(QueueBackend):
    """Process-local queue (thread-safe, no IPC)."""

    def __init__(self, queue_name: str):
        super().__init__()
        self._queue = local_queue.get_queue(queue_name)

    def put(self, item: QueueItemLike) -> None:
        normalized = normalize_queue_item(item)
        self._queue.put((normalized.request_id, normalized.ignore_return_value,
                         normalized.retryable))

    def get(self) -> QueueItemLike | None:
        try:
            return self._queue.get(block=False)
        except queue_lib.Empty:
            return None

    def qsize(self) -> int:
        return self._queue.qsize()


class MultiprocessingQueueBackend(QueueBackend):
    """Queue backed by a multiprocessing.Queue via a manager."""

    def __init__(self,
                 queue_name: str,
                 port: int = mp_queue.DEFAULT_QUEUE_MANAGER_PORT):
        super().__init__()
        self._queue = mp_queue.get_queue(queue_name, port)

    def put(self, item: QueueItemLike) -> None:
        normalized = normalize_queue_item(item)
        self._queue.put((normalized.request_id, normalized.ignore_return_value,
                         normalized.retryable))

    def get(self) -> QueueItemLike | None:
        try:
            return self._queue.get(block=False)
        except queue_lib.Empty:
            return None

    def qsize(self) -> int:
        return self._queue.qsize()


class LocalQueueFactory(QueueBackendFactory):
    """Factory for process-local queues."""

    def create_queue(self, schedule_type: str) -> QueueBackend:
        return LocalQueueBackend(schedule_type)


class MultiprocessingQueueFactory(QueueBackendFactory):
    """Factory for multiprocessing queues with a shared manager."""

    def __init__(self, port: int | None = None):
        super().__init__()
        self._port = (port if port is not None else
                      mp_queue.DEFAULT_QUEUE_MANAGER_PORT)

    def create_queue(self, schedule_type: str) -> QueueBackend:
        return MultiprocessingQueueBackend(schedule_type, self._port)

    def start(self) -> multiprocessing.Process | None:

        if not common_utils.is_port_available(self._port):
            raise RuntimeError(
                f'SkyPilot API server fails to start as port {self._port!r} '
                'is already in use by another process.')

        queue_names = self._get_queue_names()
        process = multiprocessing.Process(target=mp_queue.start_queue_manager,
                                          args=(queue_names, self._port))
        process.start()
        mp_queue.wait_for_queues_to_be_ready(queue_names,
                                             process,
                                             port=self._port)
        return process

    @staticmethod
    def _get_queue_names() -> list[str]:
        return [st.value for st in api_requests.ScheduleType]


_queue_backend_factory: QueueBackendFactory | None = None


def get_registered_queue_backend_factory() -> QueueBackendFactory | None:
    """Get the explicitly registered queue backend factory, if any."""
    return _queue_backend_factory


def get_queue_backend_factory() -> QueueBackendFactory:
    """Resolve the queue factory independently in every server process."""
    registered_factory = get_registered_queue_backend_factory()
    if registered_factory is not None:
        return registered_factory
    if os.environ.get('SKYPILOT_API_REQUEST_BACKEND') == 'postgres':
        # Uvicorn workers are spawned processes and do not inherit the
        # supervisor's module-level ``_queue_factory`` selection.  Resolve the
        # durable backend from the deployment environment so lifespan
        # submissions never fall back to the multiprocessing manager.
        # Runtime import avoids a module cycle: postgres imports this interface.
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import postgres
        return postgres.PostgresQueueFactory()
    return MultiprocessingQueueFactory()


def set_queue_backend_factory(factory: QueueBackendFactory) -> None:
    """Set the queue backend factory."""
    global _queue_backend_factory
    _queue_backend_factory = factory
