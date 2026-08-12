"""Precondition for a request to be executed.

Preconditions are introduced so that:
- Wait for precondition does not block executor process, which is expensive;
- Cross requests knowledge (e.g. waiting for other requests to be completed)
  can be handled at precondition level, instead of invading the execution
  logic of specific requests.
"""
import abc
import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
import dataclasses
import inspect
import time
from typing import Any

from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.serve import constants as serve_constants
from sky.serve import serve_state
from sky.server.requests import requests as api_requests
from sky.utils import common_utils
from sky.utils import status_lib

# The default interval seconds to check the precondition.
_PRECONDITION_CHECK_INTERVAL = 1
# The default timeout seconds to wait for the precondition to be met.
_PRECONDITION_TIMEOUT = 60 * 60

logger = sky_logging.init_logger(__name__)
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')

# Strong references to background precondition tasks to prevent GC.
# asyncio only keeps weak references to tasks, so without this set a
# long-running precondition wait (up to 1 hour) could be collected.
background_tasks: set = set()


@dataclasses.dataclass(frozen=True)
class DurablePrecondition:
    """JSON representation persisted with a queue delivery."""

    type_name: str
    payload: dict[str, Any]
    deadline: float | None


class Precondition(abc.ABC):
    """Abstract base class for a precondition for a request to be executed.

    A Precondition can be waited in either of the following ways:
    - await Precondition: wait for the precondition to be met.
    - Precondition.wait_async: wait for the precondition to be met in background
      and execute the given callback on met.
    """

    def __init__(self,
                 request_id: str,
                 check_interval: float = _PRECONDITION_CHECK_INTERVAL,
                 timeout: float = _PRECONDITION_TIMEOUT):
        self.request_id = request_id
        self.check_interval = check_interval
        self.timeout = timeout

    def __await__(self):
        """Make Precondition awaitable."""
        return self._wait().__await__()

    async def wait_async(
        self,
        on_condition_met: Callable[[], None | Awaitable[Any]] | None = None
    ) -> None:
        """Wait for the precondition and execute the callback when met.

        This coroutine blocks until the precondition is met (or times out).
        Use ``create_task(precondition.wait_async(...))`` to run it in the
        background without blocking the caller.
        """
        try:
            met = await self
            if met and on_condition_met is not None:
                result = on_condition_met()
                if inspect.isawaitable(result):
                    await result
        except (Exception, SystemExit, KeyboardInterrupt) as e:  # pylint: disable=broad-except
            await self._fail_request(e)

    async def _fail_request(
            self, error: Exception | SystemExit | KeyboardInterrupt) -> None:
        await api_requests.set_request_failed_async(self.request_id, error)
        logger.info(f'Request {self.request_id} failed due to '
                    f'{common_utils.format_exception(error)}')

    @abc.abstractmethod
    async def check(self) -> tuple[bool, str | None]:
        """Check if the precondition is met.

        Note that compared to _request_execution_wrapper, the env vars and
        skypilot config here are not overridden since the lack of process
        isolation, which may cause issues if the check accidentally depends on
        these. Make sure the check function is independent of the request
        environment.
        TODO(aylei): a new request context isolation mechanism is needed to
        enable more tasks/sub-tasks to be processed in coroutines or threads.

        Returns:
            A tuple of (bool, Optional[str]).
            The bool indicates if the precondition is met.
            The str is the current status of the precondition if any.
        """
        raise NotImplementedError

    async def _wait(self) -> bool:
        """Wait for the precondition to be met.

        Args:
            on_condition_met: Callback to execute when the precondition is met.
        """
        deadline = (time.monotonic() +
                    self.timeout if self.timeout > 0 else None)
        last_status_msg = ''
        while True:
            if deadline is not None and time.monotonic() > deadline:
                # Cancel the request on timeout.
                await api_requests.set_request_failed_async(
                    self.request_id,
                    exceptions.RequestCancelled(
                        f'Request {self.request_id} precondition wait timed '
                        f'out after {self.timeout}s'))
                return False

            # Check if the request has been cancelled
            request = await api_requests.get_request_async(self.request_id,
                                                           fields=['status'])
            if request is None:
                logger.error(f'Request {self.request_id} not found')
                return False
            if request.status == api_requests.RequestStatus.CANCELLED:
                logger.debug(f'Request {self.request_id} cancelled')
                return False
            del request

            try:
                met, status_msg = await self.check()
                if met:
                    return True
                if status_msg is not None and status_msg != last_status_msg:
                    # Update the status message if it has changed.
                    await api_requests.update_status_msg_async(
                        self.request_id, status_msg)
                    last_status_msg = status_msg
            except (Exception, SystemExit, KeyboardInterrupt) as e:  # pylint: disable=broad-except
                await self._fail_request(e)
                return False

            await asyncio.sleep(self.check_interval)


class ClusterStartCompletePrecondition(Precondition):
    """Whether the start process of a cluster is complete.

    This condition only waits the start process of a cluster to complete, e.g.
    `sky launch` or `sky start`.
    For cluster that has been started but not in UP status, bypass the waiting
    in favor of:
    - allowing the task to refresh cluster status from cloud vendor;
    - unified error message in task handlers.

    Args:
        request_id: The request ID of the task.
        cluster_name: The name of the cluster to wait for.
    """

    def __init__(self, request_id: str, cluster_name: str, **kwargs):
        super().__init__(request_id=request_id, **kwargs)
        self.cluster_name = cluster_name

    async def check(self) -> tuple[bool, str | None]:
        # Use the async DB read: this runs on the api-server event loop (the
        # precondition is polled ~once per second per pending launch), so the
        # sync variant would block the loop for every concurrent waiter. The
        # other DB read in this method (get_request_tasks_async) is already
        # async.
        cluster_status = (await
                          global_user_state.get_status_from_cluster_name_async(
                              self.cluster_name))
        if cluster_status is status_lib.ClusterStatus.UP:
            # Shortcut for started clusters, ignore cluster not found
            # since the cluster record might not yet be created by the
            # launch task.
            return True, None
        # Check if there is a task starting the cluster, we do not check
        # SUCCEEDED requests since successfully launched cluster can be
        # restarted later on.
        # Note that since the requests are not persistent yet between restarts,
        # a cluster might be started in halfway and requests are lost.
        # We unify these situations into a single state: the process of starting
        # the cluster is done (either normally or abnormally) but cluster is not
        # in UP status.
        requests = await api_requests.get_request_tasks_async(
            req_filter=api_requests.RequestTaskFilter(
                status=api_requests.RequestStatus.active_statuses(),
                include_request_names=['sky.launch', 'sky.start'],
                cluster_names=[self.cluster_name],
                # Only get the request ID to avoid fetching the whole request.
                # We're only interested in the count, not the whole request.
                fields=['request_id']))
        if len(requests) == 0:
            # No running or pending tasks, the start process is done.
            return True, None
        return False, f'Waiting for cluster {self.cluster_name} to be UP.'


class ServiceReplicaLaunchPrecondition(Precondition):
    """Fence a Serve replica request to the exact durable controller owner.

    The request row is persisted before this check runs. Therefore teardown
    either observes and cancels the active row, or removes/changes the service
    first and this precondition permanently rejects provisioning. This is the
    hard barrier that an arbitrary number of empty status snapshots cannot
    provide for an HTTP request still being accepted.
    """

    def __init__(self,
                 request_id: str,
                 service_name: str,
                 service_hash: str,
                 controller_pid: int | None,
                 controller_ip: str | None,
                 service_version: int | None = None,
                 binding_excluded_launch_context: dict[str, Any] | None = None,
                 check_interval: float = _PRECONDITION_CHECK_INTERVAL) -> None:
        super().__init__(request_id=request_id,
                         timeout=0,
                         check_interval=check_interval)
        self.service_name = service_name
        self.service_hash = service_hash
        self.controller_pid = controller_pid
        self.controller_ip = controller_ip
        self.service_version = service_version
        self.binding_excluded_launch_context = (
            None if binding_excluded_launch_context is None else
            dict(binding_excluded_launch_context))

    async def check(self) -> tuple[bool, str | None]:
        excluded = self.binding_excluded_launch_context
        if (excluded is not None and excluded.get(
                serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY)
                == serve_constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE and
                excluded.get(serve_constants.
                             ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY)
                != self.request_id):
            raise exceptions.ServeReplicaLaunchFenceError(
                'Refusing system-recovery launch whose excluded-profile '
                'request identity does not match its durable queue row.')
        launch_context = {
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY:
                self.service_name,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY:
                self.service_hash,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY:
                self.service_version,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY:
                self.controller_pid,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY:
                self.controller_ip,
        }
        authorized = await asyncio.to_thread(
            serve_state.service_replica_launch_fence_holds, launch_context,
            self.binding_excluded_launch_context)
        if not authorized:
            raise exceptions.ServeReplicaLaunchFenceError(
                f'Refusing replica launch for stale service owner '
                f'{self.service_name!r}/{self.service_hash!r}.')
        return True, None


class OrdinaryLaunchBindingPrecondition(Precondition):
    """Gate queue admission on one exact durable ordinary association."""

    def __init__(self,
                 request_id: str,
                 association_id: str,
                 check_interval: float = _PRECONDITION_CHECK_INTERVAL) -> None:
        super().__init__(request_id=request_id,
                         timeout=0,
                         check_interval=check_interval)
        self.association_id = association_id

    async def check(self) -> tuple[bool, str | None]:
        authorized = await asyncio.to_thread(
            ordinary_launch_binding.binding_allows_request, self.association_id,
            self.request_id)
        if not authorized:
            raise exceptions.ServeReplicaLaunchFenceError(
                'Refusing ordinary replica launch whose durable request '
                'association is no longer current.')
        return True, None


def serialize(precondition: Precondition | None) -> DurablePrecondition | None:
    """Serialize every supported precondition without Python pickle."""
    if precondition is None:
        return None
    deadline = (time.time() +
                precondition.timeout if precondition.timeout > 0 else None)
    common = {'check_interval': precondition.check_interval}
    if isinstance(precondition, ClusterStartCompletePrecondition):
        return DurablePrecondition(
            type_name='cluster-start-complete.v1',
            payload={
                **common,
                'cluster_name': precondition.cluster_name,
            },
            deadline=deadline)
    if isinstance(precondition, ServiceReplicaLaunchPrecondition):
        return DurablePrecondition(
            type_name='service-replica-launch.v1',
            payload={
                **common,
                'service_name': precondition.service_name,
                'service_hash': precondition.service_hash,
                'controller_pid': precondition.controller_pid,
                'controller_ip': precondition.controller_ip,
                'service_version': precondition.service_version,
                'binding_excluded_launch_context':
                    precondition.binding_excluded_launch_context,
            },
            deadline=deadline)
    if isinstance(precondition, OrdinaryLaunchBindingPrecondition):
        return DurablePrecondition(
            type_name='ordinary-launch-binding.v1',
            payload={
                **common,
                'association_id': precondition.association_id,
            },
            deadline=deadline)
    raise ValueError(
        f'Precondition {type(precondition).__module__}.'
        f'{type(precondition).__qualname__} has no durable representation.')


def deserialize(type_name: str, payload: dict[str, Any],
                request_id: str) -> Precondition:
    """Restore one supported precondition through a closed type registry."""
    if type_name == 'cluster-start-complete.v1':
        return ClusterStartCompletePrecondition(
            request_id=request_id,
            cluster_name=str(payload['cluster_name']),
            check_interval=float(payload['check_interval']),
            # The absolute queue deadline owns timeout after persistence.
            timeout=0)
    if type_name == 'service-replica-launch.v1':
        controller_pid = payload.get('controller_pid')
        if controller_pid is not None:
            controller_pid = int(controller_pid)
        controller_ip = payload.get('controller_ip')
        if controller_ip is not None:
            controller_ip = str(controller_ip)
        service_version = payload.get('service_version')
        if (service_version is not None and
            (type(service_version) is not int or service_version <= 0)):
            raise ValueError('Service replica launch version is malformed.')
        binding_excluded_launch_context = payload.get(
            'binding_excluded_launch_context')
        if binding_excluded_launch_context is not None:
            try:
                binding_excluded_launch_context = (
                    serve_state.normalize_binding_excluded_launch_context(
                        binding_excluded_launch_context))
            except (TypeError, ValueError) as error:
                raise ValueError(
                    'Service replica launch excluded-profile discriminator '
                    'is malformed.') from error
            if binding_excluded_launch_context is None:
                raise ValueError(
                    'Service replica launch excluded-profile discriminator '
                    'is malformed.')
            if (binding_excluded_launch_context.get(
                    serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY
            ) == serve_constants.
                    ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE and
                    binding_excluded_launch_context.get(
                        serve_constants.
                        ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY)
                    != request_id):
                raise ValueError(
                    'System-recovery excluded-profile request identity does '
                    'not match its durable queue row.')
        return ServiceReplicaLaunchPrecondition(
            request_id=request_id,
            service_name=str(payload['service_name']),
            service_hash=str(payload['service_hash']),
            controller_pid=controller_pid,
            controller_ip=controller_ip,
            service_version=service_version,
            binding_excluded_launch_context=binding_excluded_launch_context,
            check_interval=float(payload['check_interval']))
    if type_name == 'ordinary-launch-binding.v1':
        return OrdinaryLaunchBindingPrecondition(
            request_id=request_id,
            association_id=str(payload['association_id']),
            check_interval=float(payload['check_interval']))
    raise ValueError(f'Unknown durable precondition type {type_name!r}.')


def check_once(type_name: str, payload: dict[str, Any],
               request_id: str) -> tuple[bool, str | None]:
    """Synchronously evaluate one durable precondition from a queue thread."""
    precondition = deserialize(type_name, payload, request_id)
    return asyncio.run(precondition.check())
