"""Fixed-rate, provider-free observation of SkyServe physical GPU pools.

The observer owns provider *reads* only.  It does not allocate capacity,
mutate replicas, or enter a provider-mutation phase.  Every query is fenced by
an immutable :class:`PoolObservationTarget` and every result is committed
through :class:`PoolCapacityObservationRepository` before listeners are
notified.

The query callback receives an absolute monotonic deadline and must pass the
remaining budget to every provider RPC it issues.  The observer independently
publishes a timeout blackout at that deadline, so a late callback result can
never become authority.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
import concurrent.futures
import contextvars
import dataclasses
import math
import threading
import time
from typing import Protocol

from sky.serve import pool_capacity_observation

DEFAULT_OBSERVATION_INTERVAL_SECONDS = 60.0
DEFAULT_QUERY_TIMEOUT_SECONDS = 45.0
DEFAULT_COMPLETION_MARGIN_SECONDS = 10.0
DEFAULT_MAX_QUERY_WORKERS = 8
MAX_OBSERVATION_ROUTES_PER_POOL = 8


@dataclasses.dataclass(frozen=True)
class PoolObservationTarget:
    """Immutable physical-pool identity, query routes, and exact-card shape."""

    pool_key: str
    physical_cluster_uid: str
    access_contexts: tuple[str, ...]
    accelerator_names: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.pool_key, str) or not self.pool_key:
            raise ValueError('pool_key must be a nonempty string.')
        if (not isinstance(self.physical_cluster_uid, str) or
                not self.physical_cluster_uid):
            raise ValueError('physical_cluster_uid must be a nonempty string.')
        if (not isinstance(self.access_contexts, tuple) or
                not self.access_contexts or
                len(self.access_contexts) > MAX_OBSERVATION_ROUTES_PER_POOL or
                any(not isinstance(context, str) or not context
                    for context in self.access_contexts) or
                len(set(self.access_contexts)) != len(self.access_contexts)):
            raise ValueError(
                'access_contexts must contain between 1 and '
                f'{MAX_OBSERVATION_ROUTES_PER_POOL} unique nonempty strings.')
        if (not isinstance(self.accelerator_names, tuple) or
                not self.accelerator_names):
            raise ValueError(
                'accelerator_names must be a nonempty immutable tuple.')
        previous_name: str | None = None
        for name in self.accelerator_names:
            if (not isinstance(name, str) or not name or
                    name != name.casefold()):
                raise ValueError(
                    'Accelerator names must be nonempty and case-folded.')
            if previous_name is not None and name <= previous_name:
                raise ValueError('Accelerator names must be uniquely sorted.')
            previous_name = name

        key_uid, key_names = (
            pool_capacity_observation._parse_physical_pool_key(  # pylint: disable=protected-access
                self.pool_key))
        if (key_uid != self.physical_cluster_uid or
                key_names != self.accelerator_names):
            raise ValueError('Observation target does not match its pool key.')

    @property
    def initial_access_context(self) -> str:
        """Return the first route attempted by this immutable target."""
        return self.access_contexts[0]

    def rotated(self, offset: int) -> PoolObservationTarget:
        """Return the same physical target with a fair route starting point."""
        normalized = offset % len(self.access_contexts)
        if normalized == 0:
            return self
        contexts = (self.access_contexts[normalized:] +
                    self.access_contexts[:normalized])
        return dataclasses.replace(self, access_contexts=contexts)


class ObservationRepository(Protocol):
    """Narrow repository surface used by the observer."""

    def begin_observations(
        self,
        requests: tuple[
            pool_capacity_observation.PoolCapacityObservationRequest, ...],
        *,
        lease_duration_seconds: float,
        authority_horizon_seconds: float,
        minimum_refresh_interval_seconds: float,
    ) -> tuple[pool_capacity_observation.PoolCapacityObservationLease, ...]:
        ...

    def complete_success(
        self,
        lease: pool_capacity_observation.PoolCapacityObservationLease,
        payload: pool_capacity_observation.PoolCapacitySuccess,
        *,
        access_context: str,
    ) -> pool_capacity_observation.PoolCapacityObservation:
        ...

    def complete_blackout(
        self,
        lease: pool_capacity_observation.PoolCapacityObservationLease,
        payload: pool_capacity_observation.PoolCapacityBlackout,
    ) -> pool_capacity_observation.PoolCapacityObservation:
        ...


class PoolCapacityQueryFailure(RuntimeError):
    """A provider read failed with a closed blackout classification."""

    def __init__(
        self,
        reason: pool_capacity_observation.PoolCapacityBlackoutReason,
        detail: str | None = None,
    ) -> None:
        if not isinstance(reason,
                          pool_capacity_observation.PoolCapacityBlackoutReason):
            raise ValueError('reason must be a PoolCapacityBlackoutReason.')
        super().__init__(detail or reason.value)
        self.reason = reason
        self.detail = detail


@dataclasses.dataclass(frozen=True)
class PoolCapacityQuerySuccess:
    """A successful physical measurement and the exact route that proved it."""

    payload: pool_capacity_observation.PoolCapacitySuccess
    access_context: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload,
                          pool_capacity_observation.PoolCapacitySuccess):
            raise ValueError('payload must be PoolCapacitySuccess.')
        if not isinstance(self.access_context, str) or not self.access_context:
            raise ValueError('access_context must be a nonempty string.')


PoolQuery = Callable[
    [PoolObservationTarget, float],
    PoolCapacityQuerySuccess,
]
ObservationPublisher = Callable[
    [pool_capacity_observation.PoolCapacityObservation], None]
TargetsReader = Callable[[], Iterable[PoolObservationTarget]]

_CURRENT_QUERY_CANCELLATION: contextvars.ContextVar[
    threading.Event | None] = contextvars.ContextVar(
        'pool_capacity_query_cancellation', default=None)


def current_query_cancellation() -> threading.Event:
    """Return the cancellation signal owned by the current observer query."""
    cancellation = _CURRENT_QUERY_CANCELLATION.get()
    if cancellation is None:
        raise RuntimeError('Pool query is outside an observer deadline scope.')
    return cancellation


@dataclasses.dataclass(frozen=True)
class _QueryResult:
    success: PoolCapacityQuerySuccess
    completed_monotonic: float


def _positive_finite(value: float, name: str) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(float(value)) or value <= 0):
        raise ValueError(f'{name} must be a finite positive number.')
    return float(value)


def _bounded_detail(error: BaseException) -> str:
    detail = str(error)
    encoded = detail.encode('utf-8')
    if len(encoded) <= 4096:
        return detail
    return encoded[:4096].decode('utf-8', errors='ignore')


def _next_fixed_rate_deadline(previous_deadline: float, now: float,
                              interval_seconds: float) -> float:
    """Return the next deadline, coalescing all missed ticks into one now."""
    next_deadline = previous_deadline + interval_seconds
    return now if next_deadline <= now else next_deadline


class PoolCapacityObserver:
    """Query independent physical pools concurrently at fixed-rate deadlines."""

    def __init__(
        self,
        repository: ObservationRepository,
        query: PoolQuery,
        *,
        publish: ObservationPublisher | None = None,
        interval_seconds: float = DEFAULT_OBSERVATION_INTERVAL_SECONDS,
        query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS,
        authority_horizon_seconds: float = 180.0,
        completion_margin_seconds: float = DEFAULT_COMPLETION_MARGIN_SECONDS,
        max_workers: int = DEFAULT_MAX_QUERY_WORKERS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(query):
            raise TypeError('query must be callable.')
        if publish is not None and not callable(publish):
            raise TypeError('publish must be callable when present.')
        self._repository = repository
        self._query = query
        self._publish = publish
        self._interval_seconds = _positive_finite(interval_seconds,
                                                  'interval_seconds')
        self._query_timeout_seconds = _positive_finite(query_timeout_seconds,
                                                       'query_timeout_seconds')
        self._authority_horizon_seconds = _positive_finite(
            authority_horizon_seconds, 'authority_horizon_seconds')
        self._completion_margin_seconds = _positive_finite(
            completion_margin_seconds, 'completion_margin_seconds')
        if (isinstance(max_workers, bool) or not isinstance(max_workers, int) or
                max_workers <= 0):
            raise ValueError('max_workers must be a positive integer.')
        if not callable(monotonic):
            raise TypeError('monotonic must be callable.')
        self._max_workers = max_workers
        self._monotonic = monotonic
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix='serve-pool-observer')
        self._closed = False
        self._close_lock = threading.Lock()
        self._active_cancellations: set[threading.Event] = set()
        self._route_offsets: dict[str, int] = {}

    @staticmethod
    def _validate_targets(
        targets: Iterable[PoolObservationTarget],
    ) -> tuple[PoolObservationTarget, ...]:
        target_tuple = tuple(targets)
        if any(not isinstance(target, PoolObservationTarget)
               for target in target_tuple):
            raise ValueError('Every observation target must be typed.')
        pool_keys = [target.pool_key for target in target_tuple]
        if len(set(pool_keys)) != len(pool_keys):
            raise ValueError('One observation round cannot repeat a pool key.')
        if (len(target_tuple)
                > pool_capacity_observation.MAX_OBSERVATION_COHORT_POOLS):
            raise ValueError('One observation round exceeds the bounded pool '
                             'count.')
        return target_tuple

    def _begin_cohort(
        self, targets: tuple[PoolObservationTarget, ...]
    ) -> tuple[pool_capacity_observation.PoolCapacityObservationLease, ...]:
        requests = tuple(
            pool_capacity_observation.PoolCapacityObservationRequest(
                pool_key=target.pool_key,
                physical_cluster_uid=target.physical_cluster_uid,
                accelerator_names=target.accelerator_names,
                access_contexts=target.access_contexts) for target in targets)
        try:
            return self._repository.begin_observations(
                requests,
                lease_duration_seconds=(self._query_timeout_seconds +
                                        self._completion_margin_seconds),
                authority_horizon_seconds=self._authority_horizon_seconds,
                minimum_refresh_interval_seconds=self._interval_seconds,
            )
        except pool_capacity_observation.ObservationLeaseBusyError:
            # Compatibility with a repository implementation that still
            # exposes single-pool contention. The canonical repository skips
            # busy members and returns every independently acquired sibling.
            return ()

    def _complete(
        self,
        lease: pool_capacity_observation.PoolCapacityObservationLease,
        payload: pool_capacity_observation.PoolCapacityPayload,
        *,
        access_context: str | None = None,
    ) -> pool_capacity_observation.PoolCapacityObservation | None:
        try:
            if isinstance(payload,
                          pool_capacity_observation.PoolCapacitySuccess):
                if access_context is None:
                    raise ValueError(
                        'Successful observations require route provenance.')
                completed = self._repository.complete_success(
                    lease, payload, access_context=access_context)
            else:
                completed = self._repository.complete_blackout(lease, payload)
        except pool_capacity_observation.StaleObservationWriterError:
            # A successor generation won.  The stale result is intentionally
            # discarded and cannot wake readers as if it had committed.
            return None
        if self._publish is not None:
            self._publish(completed)
        return completed

    def _run_query(self, target: PoolObservationTarget, deadline: float,
                   cancellation: threading.Event) -> _QueryResult:
        token = _CURRENT_QUERY_CANCELLATION.set(cancellation)
        try:
            success = self._query(target, deadline)
            return _QueryResult(success=success,
                                completed_monotonic=self._monotonic())
        finally:
            _CURRENT_QUERY_CANCELLATION.reset(token)

    def observe_once(
        self,
        targets: Iterable[PoolObservationTarget],
    ) -> tuple[pool_capacity_observation.PoolCapacityObservation, ...]:
        """Run one bounded concurrent round and return committed results."""
        with self._close_lock:
            if self._closed:
                raise RuntimeError('PoolCapacityObserver is closed.')
        target_tuple = self._validate_targets(targets)
        if not target_tuple:
            return ()

        # A physical pool can have several authenticated kubeconfig aliases.
        # Rotate the first attempted route per acquired round so a persistently
        # bad alias cannot monopolize the fast path or hide route health.
        rotated_targets: list[PoolObservationTarget] = []
        for target in target_tuple:
            offset = self._route_offsets.get(target.pool_key, 0)
            rotated_targets.append(target.rotated(offset))
        target_tuple = tuple(rotated_targets)

        lease_tuple = self._begin_cohort(target_tuple)
        if not lease_tuple:
            return ()
        leases = {lease.pool_key: lease for lease in lease_tuple}
        targets_by_key = {target.pool_key: target for target in target_tuple}
        for pool_key in leases:
            target = targets_by_key[pool_key]
            self._route_offsets[pool_key] = (self._route_offsets.get(
                pool_key, 0) + 1) % len(target.access_contexts)

        deadline = self._monotonic() + self._query_timeout_seconds
        cancellations = {pool_key: threading.Event() for pool_key in leases}
        with self._close_lock:
            if self._closed:
                raise RuntimeError('PoolCapacityObserver is closed.')
            self._active_cancellations.update(cancellations.values())
            future_to_key = {
                self._executor.submit(self._run_query, targets_by_key[pool_key], deadline, cancellations[pool_key]): pool_key
                for pool_key in leases
            }
        timeout = max(0.0, deadline - self._monotonic())
        try:
            done, pending = concurrent.futures.wait(future_to_key,
                                                    timeout=timeout)
            completed: list[
                pool_capacity_observation.PoolCapacityObservation] = []

            for future in done:
                pool_key = future_to_key[future]
                lease = leases[pool_key]
                try:
                    query_result = future.result()
                    success = query_result.success
                    if not isinstance(success, PoolCapacityQuerySuccess):
                        raise PoolCapacityQueryFailure(
                            pool_capacity_observation.
                            PoolCapacityBlackoutReason.MALFORMED_RESPONSE,
                            'Pool query returned an untyped payload.')
                    if success.access_context not in (
                            targets_by_key[pool_key].access_contexts):
                        raise PoolCapacityQueryFailure(
                            pool_capacity_observation.
                            PoolCapacityBlackoutReason.MALFORMED_RESPONSE,
                            'Pool query returned an unauthenticated route.')
                    payload_names = tuple(
                        name
                        for name, _ in success.payload.free_gpus_by_accelerator)
                    if payload_names != lease.accelerator_names:
                        raise PoolCapacityQueryFailure(
                            pool_capacity_observation.
                            PoolCapacityBlackoutReason.MALFORMED_RESPONSE,
                            'Pool query did not cover the exact accelerator '
                            'set.')
                    payload: pool_capacity_observation.PoolCapacityPayload = (
                        success.payload)
                    winning_access_context: str | None = (
                        success.access_context)
                    if query_result.completed_monotonic >= deadline:
                        payload = pool_capacity_observation.PoolCapacityBlackout(
                            pool_capacity_observation.
                            PoolCapacityBlackoutReason.TIMEOUT)
                        winning_access_context = None
                except PoolCapacityQueryFailure as error:
                    payload = pool_capacity_observation.PoolCapacityBlackout(
                        error.reason, error.detail)
                    winning_access_context = None
                except Exception as error:  # pylint: disable=broad-except
                    payload = pool_capacity_observation.PoolCapacityBlackout(
                        pool_capacity_observation.PoolCapacityBlackoutReason.
                        PROVIDER_ERROR, _bounded_detail(error))
                    winning_access_context = None
                observation = self._complete(
                    lease, payload, access_context=winning_access_context)
                if observation is not None:
                    completed.append(observation)

            for future in pending:
                pool_key = future_to_key[future]
                # Cancellation is cooperative for arbitrary callbacks and is
                # made a hard bound by the Kubernetes deadline scope used by
                # the production query adapter. cancel() also removes work
                # that has not started from the executor queue.
                cancellations[pool_key].set()
                future.cancel()
                observation = self._complete(
                    leases[pool_key],
                    pool_capacity_observation.PoolCapacityBlackout(
                        pool_capacity_observation.PoolCapacityBlackoutReason.
                        TIMEOUT))
                if observation is not None:
                    completed.append(observation)

            completed.sort(key=lambda item: item.pool_key)
            return tuple(completed)
        finally:
            with self._close_lock:
                self._active_cancellations.difference_update(
                    cancellations.values())

    def run(self, stop_event: threading.Event,
            targets_reader: TargetsReader) -> None:
        """Run immediately, then on monotonic fixed-rate deadlines."""
        if not isinstance(stop_event, threading.Event):
            raise TypeError('stop_event must be a threading.Event.')
        if not callable(targets_reader):
            raise TypeError('targets_reader must be callable.')
        deadline = self._monotonic()
        try:
            while not stop_event.is_set():
                delay = max(0.0, deadline - self._monotonic())
                if stop_event.wait(delay):
                    return
                self.observe_once(targets_reader())
                now = self._monotonic()
                deadline = _next_fixed_rate_deadline(deadline, now,
                                                     self._interval_seconds)
        finally:
            self.close()

    def close(self) -> None:
        """Stop accepting rounds without waiting for an already-late RPC."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            for cancellation in self._active_cancellations:
                cancellation.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
