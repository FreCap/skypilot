"""Bounded observability primitives for SkyServe load balancer HA.

The HA role channel is safety-sensitive, so its diagnostics must never become
another authority source.  This module only records timings, byte counts, and
bounded outcome enums that callers can expose in test artifacts or metrics.
It deliberately stores no service, Pod, replica URL, or request identifiers.
"""

import collections
from collections.abc import Callable
import concurrent.futures
import dataclasses
import enum
import math
import time
from typing import Any


class LbRoleOutcome(enum.Enum):
    """Bounded outcomes for one logical HA role heartbeat."""

    SUCCESS = 'success'
    LEGACY_MODE = 'legacy_mode'
    INVALID_REPORT = 'invalid_report'
    CONTROLLER_NOT_OWNER = 'controller_not_owner'
    POD_AUTHORITY_UNAVAILABLE = 'pod_authority_unavailable'
    POD_NOT_AUTHORITATIVE = 'pod_not_authoritative'
    CUTOVER_STATE_UNAVAILABLE = 'cutover_state_unavailable'
    REPORT_REJECTED = 'report_rejected'
    ROUTING_UNAVAILABLE = 'routing_unavailable'
    ROUTING_NOT_CONVERGED = 'routing_not_converged'
    TRANSITION_INCONSISTENT = 'transition_inconsistent'
    PROXY_OWNER_READ_FAILED = 'proxy_owner_read_failed'
    PROXY_OWNER_MISSING = 'proxy_owner_missing'
    PROXY_INCARNATION_MISMATCH = 'proxy_incarnation_mismatch'
    PROXY_AUTHENTICATION_REQUIRED = 'proxy_authentication_required'
    PROXY_CONTROLLER_CONNECTION_FAILED = ('proxy_controller_connection_failed')
    PROXY_OWNER_VERIFICATION_FAILED = 'proxy_owner_verification_failed'
    PROXY_OWNER_CHANGED = 'proxy_owner_changed'
    CLIENT_TIMEOUT = 'client_timeout'
    CLIENT_CONNECTION_ERROR = 'client_connection_error'
    HTTP_UNAUTHORIZED = 'http_unauthorized'
    HTTP_CONFLICT = 'http_conflict'
    HTTP_ERROR = 'http_error'
    INVALID_RESPONSE = 'invalid_response'


_VALID_ROLE_OUTCOMES = frozenset(outcome.value for outcome in LbRoleOutcome)


def _latency_histogram_upper_bounds() -> tuple[float, ...]:
    # Ten-percent geometric buckets are fine enough to enforce the driver's
    # combined 25% and +100 ms material-regression threshold without the
    # false positives/negatives caused by the former 2-2.5x spacing.
    bounds = []
    upper_bound = 0.001
    while upper_bound < 120:
        bounds.append(upper_bound)
        upper_bound *= 1.1
    bounds.append(120.0)
    return tuple(bounds)


_LATENCY_HISTOGRAM_UPPER_BOUNDS_SECONDS = (_latency_histogram_upper_bounds())


@dataclasses.dataclass
class _RunningStat:
    """Bounded process-local summary for one low-cardinality measurement."""

    count: int = 0
    total: float = 0.0
    maximum: float = 0.0
    last: float | None = None
    recent: collections.deque[float] = dataclasses.field(
        default_factory=lambda: collections.deque(maxlen=256))
    histogram_upper_bounds: tuple[float, ...] = ()
    histogram_counts: list[int] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.histogram_counts = [0] * (len(self.histogram_upper_bounds) + 1)

    def observe(self, value: float) -> None:
        value = max(0.0, float(value))
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)
        self.last = value
        self.recent.append(value)
        if self.histogram_upper_bounds:
            bucket = next((
                index
                for index, upper_bound in enumerate(self.histogram_upper_bounds)
                if value <= upper_bound), len(self.histogram_upper_bounds))
            self.histogram_counts[bucket] += 1

    def _percentile(self, percentile: float) -> float | None:
        if not self.recent:
            return None
        ordered = sorted(self.recent)
        index = min(
            len(ordered) - 1, max(0,
                                  math.ceil(len(ordered) * percentile) - 1))
        return ordered[index]

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            'count': self.count,
            'last': self.last,
            'mean': self.total / self.count if self.count else None,
            'max': self.maximum if self.count else None,
            'p50_recent': self._percentile(0.50),
            'p99_recent': self._percentile(0.99),
        }
        if self.histogram_upper_bounds:
            result['histogram'] = {
                'upper_bounds': list(self.histogram_upper_bounds),
                # Counts are non-cumulative; the final bucket is overflow.
                'counts': list(self.histogram_counts),
            }
        return result


def _latency_stat() -> _RunningStat:
    return _RunningStat(
        histogram_upper_bounds=_LATENCY_HISTOGRAM_UPPER_BOUNDS_SECONDS)


class RoleRequestTrace:
    """Per-request controller trace with executor and lock timing."""

    def __init__(self,
                 executor: concurrent.futures.Executor | None = None) -> None:
        self._started_at = time.monotonic()
        self._executor = executor
        self._phases: collections.defaultdict[str, float] = (
            collections.defaultdict(float))
        self._lock_wait_seconds = 0.0
        self._lock_acquired_at: float | None = None

    async def run_in_executor(self, loop: Any, phase: str, function: Callable,
                              *args: Any) -> Any:
        """Run one blocking authority read and attribute its elapsed time."""
        submitted_at = time.monotonic()
        worker_started_at: float | None = None

        def invoke() -> Any:
            nonlocal worker_started_at
            worker_started_at = time.monotonic()
            return function(*args)

        try:
            return await loop.run_in_executor(self._executor, invoke)
        finally:
            self._phases[phase] += time.monotonic() - submitted_at
            if worker_started_at is not None:
                self._phases[f'{phase}_executor_queue'] += max(
                    0.0, worker_started_at - submitted_at)

    def lock_acquired(self, wait_started_at: float) -> None:
        now = time.monotonic()
        self._lock_wait_seconds = max(0.0, now - wait_started_at)
        self._lock_acquired_at = now

    def add_phases(self, phases: dict[str, float]) -> None:
        """Merge bounded subphase timings collected inside one executor call."""
        for phase, seconds in phases.items():
            if isinstance(seconds, (int, float)):
                self._phases[phase] += max(0.0, float(seconds))

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        lock_hold_seconds = 0.0
        if self._lock_acquired_at is not None:
            lock_hold_seconds = max(0.0, now - self._lock_acquired_at)
        return {
            'total_seconds': max(0.0, now - self._started_at),
            'lock_wait_seconds': self._lock_wait_seconds,
            'lock_hold_seconds': lock_hold_seconds,
            'phases_seconds': dict(sorted(self._phases.items())),
        }


class LbHaRuntimeStats:
    """Process-local, bounded HA diagnostics for one external LB."""

    def __init__(self) -> None:
        self._role_payload_bytes = _RunningStat()
        self._role_total_seconds = _latency_stat()
        self._probe_round_seconds = _latency_stat()
        self._role_outcomes: collections.Counter[str] = collections.Counter()
        self._role_statuses: collections.Counter[str] = collections.Counter()
        self._controller_total_seconds = _latency_stat()
        self._controller_lock_wait_seconds = _latency_stat()
        self._controller_lock_hold_seconds = _latency_stat()
        self._controller_phases: dict[str, _RunningStat] = {}
        self._proxy_total_seconds = _latency_stat()
        self._proxy_phases: dict[str, _RunningStat] = {}
        self._last_role_outcome: str | None = None
        self._last_role_observed_at: float | None = None
        self._last_role_success_at: float | None = None
        self._role_failure_started_at: float | None = None
        self._last_role_failure_recovery_seconds: float | None = None
        self._max_role_failure_recovery_seconds: float | None = None
        self._probe_rounds = 0
        self._probe_urls = 0
        self._probe_successes = 0
        self._probe_unknown = 0
        self._probe_connections_created = 0
        self._last_probe: dict[str, int | float] | None = None

    def record_role(self, *, payload_bytes: int, total_seconds: float,
                    outcome: str, status_code: int | None,
                    controller_observation: dict[str, Any] | None) -> None:
        self._role_payload_bytes.observe(payload_bytes)
        self._role_total_seconds.observe(total_seconds)
        if outcome not in _VALID_ROLE_OUTCOMES:
            outcome = LbRoleOutcome.INVALID_RESPONSE.value
        now = time.monotonic()
        if outcome in (LbRoleOutcome.SUCCESS.value,
                       LbRoleOutcome.LEGACY_MODE.value):
            self._last_role_success_at = now
            if self._role_failure_started_at is not None:
                self._last_role_failure_recovery_seconds = max(
                    0.0, now - self._role_failure_started_at)
                self._max_role_failure_recovery_seconds = max(
                    self._max_role_failure_recovery_seconds or 0.0,
                    self._last_role_failure_recovery_seconds)
                self._role_failure_started_at = None
        elif self._role_failure_started_at is None:
            self._role_failure_started_at = now
            self._last_role_failure_recovery_seconds = None
        self._last_role_outcome = outcome
        self._last_role_observed_at = now
        self._role_outcomes[outcome] += 1
        status = str(status_code) if status_code is not None else 'transport'
        self._role_statuses[status] += 1
        self._record_controller_observation(controller_observation)

    @staticmethod
    def _observe_known_phases(target: dict[str, _RunningStat], raw: Any,
                              allowed: set[str]) -> None:
        if not isinstance(raw, dict):
            return
        for phase, seconds in raw.items():
            if phase not in allowed or not isinstance(seconds, (int, float)):
                continue
            target.setdefault(phase, _latency_stat()).observe(seconds)

    def _record_controller_observation(
            self, observation: dict[str, Any] | None) -> None:
        if not isinstance(observation, dict):
            return
        controller = observation.get('controller')
        if isinstance(controller, dict):
            total = controller.get('total_seconds')
            if isinstance(total, (int, float)):
                self._controller_total_seconds.observe(total)
            lock_wait = controller.get('lock_wait_seconds')
            if isinstance(lock_wait, (int, float)):
                self._controller_lock_wait_seconds.observe(lock_wait)
            lock_hold = controller.get('lock_hold_seconds')
            if isinstance(lock_hold, (int, float)):
                self._controller_lock_hold_seconds.observe(lock_hold)
            self._observe_known_phases(
                self._controller_phases, controller.get('phases_seconds'), {
                    'postgresql_owner_read',
                    'kubernetes_role_snapshot',
                    'snapshot_postgresql_owner_read',
                    'snapshot_pod_list',
                    'snapshot_service_read',
                    'snapshot_ownership_validation',
                    'snapshot_parse_routing',
                    'snapshot_parse_pods',
                    'kubernetes_pod_authority',
                    'postgresql_role_state_read',
                    'postgresql_fence_read',
                    'postgresql_cutover_state_read',
                    'kubernetes_service_routing_read',
                    'kubernetes_selector_patch',
                    'postgresql_cutover_write',
                    'kubernetes_cleanup',
                    'drain_evidence_read',
                    'drain_evidence_write',
                })
        proxy = observation.get('proxy')
        if isinstance(proxy, dict):
            total = proxy.get('total_seconds')
            if isinstance(total, (int, float)):
                self._proxy_total_seconds.observe(total)
            self._observe_known_phases(
                self._proxy_phases, proxy.get('phases_seconds'),
                {'owner_before', 'controller_forward', 'owner_after'})

    def record_probe(self, *, total_seconds: float, attempted: int,
                     succeeded: int, connections_created: int) -> None:
        attempted = max(0, int(attempted))
        succeeded = min(attempted, max(0, int(succeeded)))
        connections_created = max(0, int(connections_created))
        self._probe_rounds += 1
        self._probe_round_seconds.observe(total_seconds)
        self._probe_urls += attempted
        self._probe_successes += succeeded
        self._probe_unknown += attempted - succeeded
        self._probe_connections_created += connections_created
        self._last_probe = {
            'total_seconds': max(0.0, float(total_seconds)),
            'attempted': attempted,
            'succeeded': succeeded,
            'unknown': attempted - succeeded,
            'connections_created': connections_created,
        }

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()

        def age(observed_at: float | None) -> float | None:
            return (max(0.0, now -
                        observed_at) if observed_at is not None else None)

        return {
            'role': {
                'payload_bytes': self._role_payload_bytes.snapshot(),
                'total_seconds': self._role_total_seconds.snapshot(),
                'outcomes': dict(sorted(self._role_outcomes.items())),
                'http_statuses': dict(sorted(self._role_statuses.items())),
                'last_outcome': self._last_role_outcome,
                'last_observation_age_seconds': age(self._last_role_observed_at
                                                   ),
                'last_success_age_seconds': age(self._last_role_success_at),
                'failure_streak_active': self._role_failure_started_at is
                                         not None,
                'last_failure_recovery_seconds':
                    self._last_role_failure_recovery_seconds,
                'max_failure_recovery_seconds':
                    self._max_role_failure_recovery_seconds,
                'controller': {
                    'total_seconds': self._controller_total_seconds.snapshot(),
                    'lock_wait_seconds':
                        self._controller_lock_wait_seconds.snapshot(),
                    'lock_hold_seconds':
                        self._controller_lock_hold_seconds.snapshot(),
                    'phases_seconds': {
                        phase: stat.snapshot() for phase, stat in sorted(
                            self._controller_phases.items())
                    },
                },
                'proxy': {
                    'total_seconds': self._proxy_total_seconds.snapshot(),
                    'phases_seconds': {
                        phase: stat.snapshot()
                        for phase, stat in sorted(self._proxy_phases.items())
                    },
                },
            },
            'probe': {
                'rounds': self._probe_rounds,
                'round_seconds': self._probe_round_seconds.snapshot(),
                'attempted': self._probe_urls,
                'succeeded': self._probe_successes,
                'unknown': self._probe_unknown,
                'connections_created': self._probe_connections_created,
                'last': self._last_probe,
            },
        }
