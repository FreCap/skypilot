"""Provider-independent readiness and composition for SkyServe routes."""

import asyncio
from collections.abc import Callable
import threading
from typing import Any

import aiohttp

from sky import sky_logging
from sky.serve import constants
from sky.serve import replica_tls
from sky.serve import route_projection
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)


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

    @staticmethod
    def _target_key(
        target: route_projection.RouteLeaseProbeTarget,
    ) -> tuple[int, str, int, int, str]:
        return (target.replica_id, target.replica_record_id,
                target.material_generation, target.revocation_generation,
                target.material_sha256)

    async def _probe(self, session: aiohttp.ClientSession,
                     target: route_projection.RouteLeaseProbeTarget) -> None:
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
        if self._stop_event.is_set():
            return
        try:
            self._repository.record_probe_result(
                target, succeeded, ttl_seconds=self._lease_ttl_seconds)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Incremental route probe receipt failed for replica '
                f'{target.replica_id}: {common_utils.format_exception(error)}')

    @staticmethod
    def _consume_task_result(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route probe task failed: '
                           f'{common_utils.format_exception(error)}')

    async def _run_tick(
        self,
        session: aiohttp.ClientSession,
        tasks: dict[tuple[int, str, int, int, str], asyncio.Task[None]],
    ) -> None:
        """Schedule independent probes and compose without awaiting them."""
        for key, task in list(tasks.items()):
            if task.done():
                self._consume_task_result(task)
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
                task = asyncio.create_task(self._probe(session, target))
                task.add_done_callback(self._consume_task_result)
                tasks[key] = task

        # This never awaits the URL tasks above. A slow or hung URL can expire
        # only its lease; it cannot delay head refresh.
        try:
            self._compose()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Incremental route composition failed: '
                           f'{common_utils.format_exception(error)}')

    async def run_async(self) -> None:
        """Probe independently and compose on a fixed monotonic cadence."""
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        tasks: dict[tuple[int, str, int, int, str], asyncio.Task[None]] = {}
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

    def run(self) -> None:
        """Supervised-thread entry point."""
        asyncio.run(self.run_async())
