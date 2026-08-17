"""Provider-independent readiness and composition for SkyServe routes."""

import asyncio
from collections.abc import Callable
import concurrent.futures
import threading
from typing import Any

import aiohttp

from sky import sky_logging
from sky.serve import constants
from sky.serve import replica_tls
from sky.serve import route_projection
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)


def _target_key(
    target: route_projection.RouteLeaseProbeTarget,
) -> tuple[int, str, int, int, str]:
    return (target.replica_id, target.replica_record_id,
            target.material_generation, target.revocation_generation,
            target.material_sha256)


class _ProbeReceiptWriter:
    """One bounded PostgreSQL writer outside the composition event loop."""

    def __init__(self, repository: route_projection.RouteProjectionRepository,
                 ttl_seconds: int) -> None:
        self._repository = repository
        self._ttl_seconds = ttl_seconds
        self._pending: dict[tuple[int, str, int, int, str],
                            route_projection.RouteLeaseProbeResult] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='skyserve-route-receipts')
        self._future: concurrent.futures.Future[list[
            route_projection.RouteLeaseProbeReceipt]] | None = None

    def add(self, result: route_projection.RouteLeaseProbeResult) -> None:
        """Coalesce to the newest not-yet-submitted result per exact target."""
        self._pending[_target_key(result.target)] = result

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

    def close(self) -> None:
        """Reject queued work and release the writer without blocking exit."""
        self._pending.clear()
        self._consume_future()
        self._executor.shutdown(wait=False, cancel_futures=True)


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

    @staticmethod
    def _target_key(
        target: route_projection.RouteLeaseProbeTarget,
    ) -> tuple[int, str, int, int, str]:
        return _target_key(target)

    async def _probe(
        self, session: aiohttp.ClientSession,
        target: route_projection.RouteLeaseProbeTarget
    ) -> route_projection.RouteLeaseProbeResult:
        succeeded = False
        try:
            kwargs: dict[str, Any] = {
                'headers': target.headers,
                'timeout': aiohttp.ClientTimeout(total=target.timeout_seconds),
                'ssl': replica_tls.aiohttp_ssl_setting(),
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
        tasks: dict[tuple[int, str, int, int, str],
                    asyncio.Task[route_projection.RouteLeaseProbeResult]],
    ) -> None:
        """Schedule independent probes and compose without awaiting them."""
        for key, task in list(tasks.items()):
            if task.done():
                result = self._consume_task_result(task)
                if result is not None and not self._stop_event.is_set():
                    self._receipt_writer.add(result)
                tasks.pop(key, None)
        try:
            targets = self._repository.list_probe_targets(self._identity)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route target read failed: '
                           f'{common_utils.format_exception(error)}')
            targets = []
        current_keys = {self._target_key(target) for target in targets}
        for key, task in list(tasks.items()):
            if key not in current_keys:
                task.cancel()
                tasks.pop(key, None)
        for target in targets:
            key = self._target_key(target)
            if key not in tasks:
                tasks[key] = asyncio.create_task(self._probe(session, target))

        # Receipt persistence has one bounded writer and never runs on this
        # event loop.  A busy writer coalesces newer results per exact target.
        self._receipt_writer.flush()

        # This invokes or awaits neither the URL tasks nor the receipt writer.
        # A slow operation can expire exact leases without joining composition.
        try:
            self._compose()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route composition failed: '
                           f'{common_utils.format_exception(error)}')

    async def run_async(self) -> None:
        """Probe independently and compose on a fixed monotonic cadence."""
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        tasks: dict[tuple[int, str, int, int, str],
                    asyncio.Task[route_projection.RouteLeaseProbeResult]] = {}
        connector = aiohttp.TCPConnector(
            limit=constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS)
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                while not self._stop_event.is_set():
                    delay = next_tick - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)
                        if self._stop_event.is_set():
                            return

                    await self._run_tick(session, tasks)
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
                self._receipt_writer.close()

    def run(self) -> None:
        """Supervised-thread entry point."""
        asyncio.run(self.run_async())
