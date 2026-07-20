"""Shared PostgreSQL-backed provider call budgets for image workers."""

from __future__ import annotations

import dataclasses
import threading
import time

from sky.container_images import topology_state


class ProviderBudgetUnavailableError(RuntimeError):
    """No qualified provider grant became available within the bound."""


@dataclasses.dataclass
class _LocalGrant:
    tokens: int
    expires_at: int


class ProviderBudgetLimiter:
    """Spends bounded one-second grants locally instead of per API call."""

    def __init__(self,
                 worker_id: str,
                 *,
                 wait_timeout_seconds: int = 30) -> None:
        self._worker_id = worker_id
        self._wait_timeout_seconds = wait_timeout_seconds
        self._lock = threading.Lock()
        self._budgets: dict[tuple[str, str, str, str, str],
                            topology_state.ProviderBudgetRecord] = {}
        self._grants: dict[str, _LocalGrant] = {}

    def _budget(
        self,
        shard: topology_state.ShardRecord,
    ) -> topology_state.ProviderBudgetRecord:
        key = (shard.provider, shard.partition, shard.account, shard.region,
               'ecr')
        budget = self._budgets.get(key)
        if budget is None:
            budget = topology_state.get_provider_budget(
                provider=shard.provider,
                partition=shard.partition,
                account=shard.account,
                region=shard.region,
                api_family='ecr')
            if budget is None:
                raise ProviderBudgetUnavailableError(
                    'No qualified provider budget exists for this target.')
            self._budgets[key] = budget
        return budget

    def before_call(self, shard: topology_state.ShardRecord) -> None:
        budget = self._budget(shard)
        deadline = time.monotonic() + self._wait_timeout_seconds
        while time.monotonic() < deadline:
            wait_seconds = 0.05
            with self._lock:
                current = int(time.time())
                grant = self._grants.get(budget.id)
                if grant is not None and grant.expires_at > current:
                    if grant.tokens > 0:
                        grant.tokens -= 1
                        return
                    wait_seconds = min(
                        0.25, max(0.05, grant.expires_at - time.time()))
                else:
                    self._grants.pop(budget.id, None)
                    acquired = topology_state.acquire_provider_grant(
                        self._worker_id, budget.id, requested_calls=64)
                    if acquired is not None:
                        # The worker row holds only one grant at a time. Mirror
                        # that bound locally and spend one token for this call.
                        self._grants = {
                            budget.id: _LocalGrant(
                                tokens=acquired.tokens - 1,
                                expires_at=acquired.expires_at)
                        }
                        return
            time.sleep(wait_seconds)
        raise ProviderBudgetUnavailableError(
            'Provider API budget remained unavailable.')

    def record_throttle(
        self,
        shard: topology_state.ShardRecord,
    ) -> None:
        budget = self._budget(shard)
        topology_state.record_provider_throttle(budget.id)
        with self._lock:
            self._grants.pop(budget.id, None)
