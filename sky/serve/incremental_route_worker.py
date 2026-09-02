"""Provider-independent readiness and composition for SkyServe routes."""

import asyncio
from collections.abc import Callable
import concurrent.futures
import dataclasses
import threading
from typing import Any, Generic, TypeVar

import aiohttp

from sky import sky_logging
from sky.serve import constants
from sky.serve import replica_tls
from sky.serve import route_projection
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

_ResultT = TypeVar('_ResultT')
_TargetKey = tuple[int, str, int, int, str]


def _target_key(target: route_projection.RouteLeaseProbeTarget,) -> _TargetKey:
    return (target.replica_id, target.replica_record_id,
            target.material_generation, target.revocation_generation,
            target.material_sha256)


def _validate_probe_targets(
    targets: list[route_projection.RouteLeaseProbeTarget],
) -> tuple[route_projection.RouteLeaseProbeTarget, ...]:
    """Return one immutable, worker-owned snapshot within the task budget."""
    if len(targets) > constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS:
        raise ValueError('Target count exceeds the bounded maximum.')
    replica_ids = [target.replica_id for target in targets]
    if len(replica_ids) != len(set(replica_ids)):
        raise ValueError('Target snapshot contains duplicate replica IDs.')
    return tuple(targets)


@dataclasses.dataclass(frozen=True)
class _CallOutcome(Generic[_ResultT]):
    """Completed result from one bounded synchronous owner."""

    result: _ResultT | None = None
    error: Exception | None = None


class _SingleFlightCall(Generic[_ResultT]):
    """Own at most one running synchronous call and no queued calls."""

    def __init__(self, call: Callable[[], _ResultT], thread_name: str) -> None:
        self._call = call
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=thread_name)
        self._future: concurrent.futures.Future[_ResultT] | None = None
        self._closed = False

    def submit_if_idle(self, on_done: Callable[[], None] | None = None) -> bool:
        """Start one call, coalescing every request while it remains live."""
        if self._closed or self._future is not None:
            return False
        self._future = self._executor.submit(self._call)
        if on_done is not None:
            self._future.add_done_callback(lambda _future: on_done())
        return True

    def take_completed(self) -> _CallOutcome[_ResultT] | None:
        """Consume one completed call without waiting for it."""
        future = self._future
        if future is None or not future.done():
            return None
        self._future = None
        try:
            return _CallOutcome(result=future.result())
        except Exception as error:  # pylint: disable=broad-except
            return _CallOutcome(error=error)

    def close(self) -> bool:
        """Reject new work; return whether no synchronous call remains live."""
        self._closed = True
        future = self._future
        drained = future is None or future.done() or future.cancel()
        self._executor.shutdown(wait=False, cancel_futures=True)
        return drained


class _ProbeReceiptWriter:
    """One bounded PostgreSQL writer outside the composition event loop."""

    def __init__(self, repository: route_projection.RouteProjectionRepository,
                 ttl_seconds: int) -> None:
        self._repository = repository
        self._ttl_seconds = ttl_seconds
        # One IncrementalRouteWorker owns this writer for one immutable
        # publisher identity.  Within that identity a newer exact target for
        # the same numeric replica makes every older pending result stale, so
        # coalesce by replica ID instead of retaining material generations
        # while PostgreSQL is unavailable.
        self._pending: dict[int, route_projection.RouteLeaseProbeResult] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='skyserve-route-receipts')
        self._future: concurrent.futures.Future[list[
            route_projection.RouteLeaseProbeReceipt]] | None = None
        self._closed = False

    def add(self, result: route_projection.RouteLeaseProbeResult) -> None:
        """Coalesce to the newest not-yet-submitted result per replica."""
        if self._closed:
            return
        replica_id = result.target.replica_id
        if (replica_id not in self._pending and len(self._pending)
                >= constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS):
            logger.warning('Dropping incremental route probe receipt for '
                           f'replica {replica_id}: pending receipt budget is '
                           'full.')
            return
        self._pending[replica_id] = result

    def retain_targets(
        self,
        targets: tuple[route_projection.RouteLeaseProbeTarget, ...],
    ) -> None:
        """Drop pending results absent from the new exact target snapshot."""
        current_keys = {
            target.replica_id: _target_key(target) for target in targets
        }
        self._pending = {
            replica_id: result
            for replica_id, result in self._pending.items()
            if current_keys.get(replica_id) == _target_key(result.target)
        }

    def _consume_future(self) -> None:
        if self._future is None or not self._future.done():
            return
        try:
            self._future.result()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route probe receipt batch failed: '
                           f'{common_utils.format_exception(error)}')
        finally:
            self._future = None

    def flush(self) -> None:
        """Submit at most one bounded batch without waiting for PostgreSQL."""
        if self._closed:
            return
        self._consume_future()
        if self._future is not None or not self._pending:
            return
        keys = list(
            self._pending)[:constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS]
        batch = [self._pending.pop(key) for key in keys]
        self._future = self._executor.submit(
            self._repository.record_probe_results,
            batch,
            ttl_seconds=self._ttl_seconds)

    def close(self) -> bool:
        """Reject new work; return whether no persistence call remains live."""
        self._closed = True
        self._pending.clear()
        self._consume_future()
        future = self._future
        drained = future is None or future.done() or future.cancel()
        self._executor.shutdown(wait=False, cancel_futures=True)
        return drained


class IncrementalRouteWorker:
    """Renew exact URL leases without waiting for any provider operation."""

    def __init__(
        self,
        repository: route_projection.RouteProjectionRepository,
        identity: route_projection.RoutePublisherIdentity,
        compose: Callable[[], Any],
        stop_event: threading.Event,
        *,
        interval_seconds: int = (
            constants.SYSTEM_RECOVERY_ROUTE_PROBE_INTERVAL_SECONDS),
        lease_ttl_seconds: int = 3 *
        constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds < 1 or lease_ttl_seconds < interval_seconds:
            raise ValueError('Incremental route timing bounds are invalid.')
        self._repository = repository
        self._identity = identity
        self._compose = compose
        self._stop_event = stop_event
        self._interval_seconds = interval_seconds
        self._lease_ttl_seconds = lease_ttl_seconds
        self._receipt_writer = _ProbeReceiptWriter(repository,
                                                   lease_ttl_seconds)
        self._current_targets: tuple[route_projection.RouteLeaseProbeTarget,
                                     ...] = ()
        self._target_refresh = _SingleFlightCall(self._read_probe_targets,
                                                 'skyserve-route-targets')
        self._composition = _SingleFlightCall(compose, 'skyserve-route-compose')
        self._closed = False

    @staticmethod
    def _target_key(
        target: route_projection.RouteLeaseProbeTarget,) -> _TargetKey:
        return _target_key(target)

    def _read_probe_targets(
            self) -> tuple[route_projection.RouteLeaseProbeTarget, ...]:
        targets = _validate_probe_targets(
            self._repository.list_probe_targets(self._identity))
        if any(target.identity != self._identity for target in targets):
            raise ValueError(
                'Incremental route target snapshot mixes publisher '
                'identities.')
        return targets

    def _install_target_snapshot(
        self,
        targets: tuple[route_projection.RouteLeaseProbeTarget, ...],
    ) -> None:
        self._current_targets = targets
        self._receipt_writer.retain_targets(targets)

    def _consume_background_outcomes(self) -> bool:
        """Apply completed synchronous work; return false on lost ownership."""
        target_outcome = self._target_refresh.take_completed()
        compose_outcome = self._composition.take_completed()
        errors = tuple(outcome.error
                       for outcome in (target_outcome, compose_outcome)
                       if outcome is not None and outcome.error is not None)
        if any(
                isinstance(error, route_projection.RouteProjectionConflict)
                for error in errors):
            self._install_target_snapshot(())
            logger.info('Incremental route worker lost exact publication '
                        'ownership; stopping this worker instance.')
            return False

        if target_outcome is not None:
            if target_outcome.error is None:
                targets = target_outcome.result
                if targets is None:
                    self._install_target_snapshot(())
                    logger.warning(
                        'Incremental route target reader returned no '
                        'snapshot; clearing current targets.')
                else:
                    self._install_target_snapshot(targets)
            elif isinstance(target_outcome.error,
                            (route_projection.RouteProjectionValidationError,
                             route_projection.RouteProjectionCorruption,
                             ValueError, TypeError)):
                # A structurally invalid or oversized observation is not a
                # transient PostgreSQL outage. Never retain an older target set
                # as authority after observing a newer invalid set.
                self._install_target_snapshot(())
                logger.warning(
                    'Incremental route target snapshot rejected: '
                    f'{common_utils.format_exception(target_outcome.error)}')
            else:
                # Exact owner/row fences on the receipt write make continued
                # HTTP observation of the last accepted snapshot safe during a
                # transient read outage. Its durable lease still expires if
                # PostgreSQL cannot accept receipts.
                logger.warning(
                    'Incremental route target refresh failed; '
                    'retaining the last exact snapshot: '
                    f'{common_utils.format_exception(target_outcome.error)}')

        if compose_outcome is not None and compose_outcome.error is not None:
            logger.warning(
                'Incremental route composition failed: '
                f'{common_utils.format_exception(compose_outcome.error)}')
        return True

    async def _probe(
        self, session: aiohttp.ClientSession,
        target: route_projection.RouteLeaseProbeTarget
    ) -> route_projection.RouteLeaseProbeResult:
        succeeded = False
        try:
            kwargs: dict[str, Any] = {
                'headers': target.headers,
                'timeout': aiohttp.ClientTimeout(total=target.timeout_seconds),
            }
            if target.method == 'POST':
                kwargs['json'] = target.post_data
            async with session.request(target.method, target.probe_url,
                                       **kwargs) as response:
                succeeded = response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            succeeded = False
        return route_projection.RouteLeaseProbeResult(target=target,
                                                      succeeded=succeeded)

    @staticmethod
    def _consume_task_result(
        task: asyncio.Task[route_projection.RouteLeaseProbeResult]
    ) -> route_projection.RouteLeaseProbeResult | None:
        if task.cancelled():
            return None
        try:
            return task.result()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route probe task failed: '
                           f'{common_utils.format_exception(error)}')
            return None

    async def _run_tick(
        self,
        session: aiohttp.ClientSession,
        tasks: dict[_TargetKey,
                    asyncio.Task[route_projection.RouteLeaseProbeResult]],
        *,
        submit_background: bool = True,
        on_background_done: Callable[[], None] | None = None,
    ) -> bool:
        """Advance bounded owners without blocking on synchronous work."""
        if self._stop_event.is_set():
            return False
        if not self._consume_background_outcomes():
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
                tasks.clear()
            return False

        current_keys = {
            self._target_key(target) for target in self._current_targets
        }
        for key, task in list(tasks.items()):
            if task.done():
                result = self._consume_task_result(task)
                if (result is not None and key in current_keys and
                        not self._stop_event.is_set()):
                    self._receipt_writer.add(result)
                tasks.pop(key, None)

        cancelled_tasks = []
        for key, task in list(tasks.items()):
            if key not in current_keys:
                task.cancel()
                cancelled_tasks.append(task)
                tasks.pop(key, None)
        # A replacement generation is not created until every obsolete task has
        # observed cancellation. This keeps live task objects within the same
        # fleet ceiling during full-generation churn.
        if cancelled_tasks:
            await asyncio.gather(*cancelled_tasks, return_exceptions=True)
            if self._stop_event.is_set():
                return False

        for target in self._current_targets:
            key = self._target_key(target)
            if key not in tasks:
                tasks[key] = asyncio.create_task(self._probe(session, target))

        # Receipt persistence has one bounded writer and never runs on this
        # event loop.  A busy writer coalesces newer results per replica.
        self._receipt_writer.flush()

        if submit_background:
            # Each owner admits at most one synchronous call. Repeated ticks
            # while PostgreSQL is slow are coalesced instead of queueing
            # executor work.
            self._target_refresh.submit_if_idle(on_background_done)
            self._composition.submit_if_idle(on_background_done)
        return True

    async def run_async(self) -> None:
        """Probe independently and compose on a fixed monotonic cadence."""
        if self._closed:
            raise RuntimeError('Incremental route worker cannot be restarted.')
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        background_completed = asyncio.Event()

        def _notify_background_completed() -> None:
            try:
                loop.call_soon_threadsafe(background_completed.set)
            except RuntimeError:
                # A production DB call can finish after nonblocking shutdown
                # and event-loop closure. Its exact write fence still applies.
                pass

        tasks: dict[_TargetKey,
                    asyncio.Task[route_projection.RouteLeaseProbeResult]] = {}
        connector_kwargs: dict[str, Any] = {
            'limit': constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS,
        }
        ssl_setting = replica_tls.aiohttp_ssl_setting()
        if ssl_setting is not None:
            connector_kwargs['ssl'] = ssl_setting
        connector = aiohttp.TCPConnector(**connector_kwargs)
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                while not self._stop_event.is_set():
                    delay = next_tick - loop.time()
                    if delay > 0:
                        try:
                            await asyncio.wait_for(background_completed.wait(),
                                                   timeout=delay)
                        except asyncio.TimeoutError:
                            pass
                        else:
                            background_completed.clear()
                            if not await self._run_tick(
                                    session, tasks, submit_background=False):
                                return
                            # Completion wakeups apply targets and schedule HTTP
                            # immediately without advancing the fixed DB grid.
                            continue
                        if self._stop_event.is_set():
                            return

                    if not await self._run_tick(
                            session,
                            tasks,
                            on_background_done=_notify_background_completed):
                        return
                    next_tick += self._interval_seconds
                    now = loop.time()
                    while next_tick <= now:
                        next_tick += self._interval_seconds
            finally:
                for task in tasks.values():
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks.values(),
                                         return_exceptions=True)
                self._closed = True
                undrained = []
                if not self._target_refresh.close():
                    undrained.append('target refresh')
                if not self._composition.close():
                    undrained.append('composition')
                if not self._receipt_writer.close():
                    undrained.append('receipt persistence')
                if undrained:
                    # Python cannot cancel a synchronous DBAPI call already
                    # running in another thread. Central pool/connect timeouts
                    # and route-local PostgreSQL transaction deadlines bound
                    # repository I/O; the caller's pure composition callback is
                    # not forcibly interruptible. Exact owner/row fences still
                    # prevent a late operation from publishing stale state.
                    logger.warning(
                        'Incremental route worker stopped with bounded '
                        'synchronous work still in flight: %s.',
                        ', '.join(undrained))

    def run(self) -> None:
        """Supervised-thread entry point."""
        asyncio.run(self.run_async())
