"""Tests for fixed-rate concurrent physical-pool observation."""
# pylint: disable=missing-class-docstring,protected-access

import contextlib
import json
import threading
import time
from typing import Any
import uuid

import pytest

from sky.serve import pool_capacity_observation as observation
from sky.serve import pool_capacity_observer as observer
from sky.serve import reserved_capacity


def _target(uid: str, context: str, *cards:
            str) -> observer.PoolObservationTarget:
    encoded_cards: str | list[str] = (cards[0]
                                      if len(cards) == 1 else list(cards))
    return observer.PoolObservationTarget(
        pool_key=json.dumps(['v2', uid, encoded_cards]),
        physical_cluster_uid=uid,
        access_contexts=(context,),
        accelerator_names=tuple(sorted(cards)),
    )


def _query_success(
    target: observer.PoolObservationTarget,
    payload: observation.PoolCapacitySuccess,
) -> observer.PoolCapacityQuerySuccess:
    return observer.PoolCapacityQuerySuccess(
        payload=payload, access_context=target.initial_access_context)


class _Repository:

    def __init__(self) -> None:
        self._generation: dict[str, int] = {}
        self.busy: set[str] = set()
        self.completions: list[observation.PoolCapacityObservation] = []

    def begin_observations(
            self, requests: tuple[observation.PoolCapacityObservationRequest,
                                  ...], **_kwargs: Any
    ) -> tuple[observation.PoolCapacityObservationLease, ...]:
        cohort_sequence = max(self._generation.values(), default=0) + 1
        leases = []
        for request in requests:
            pool_key = request.pool_key
            if pool_key in self.busy:
                continue
            generation = self._generation.get(pool_key, 0) + 1
            self._generation[pool_key] = generation
            leases.append(
                observation.PoolCapacityObservationLease(
                    row_key=f'row-{generation}',
                    pool_key=pool_key,
                    physical_cluster_uid=request.physical_cluster_uid,
                    accelerator_names=request.accelerator_names,
                    access_context=request.access_context,
                    access_contexts=request.access_contexts,
                    observation_generation=generation,
                    lease_token=uuid.uuid4(),
                    lease_expires_at=1000.0,
                    observation_sequence=cohort_sequence,
                    ordinary_admission_sequence=cohort_sequence,
                    materialization_sequence=cohort_sequence,
                    observed_at=100.0,
                    valid_until=280.0,
                ))
        return tuple(leases)

    def _complete(
        self,
        lease: observation.PoolCapacityObservationLease,
        payload: observation.PoolCapacityPayload,
        *,
        access_context: str | None = None,
    ) -> observation.PoolCapacityObservation:
        if access_context is None:
            access_context = lease.access_context
        completed = observation.PoolCapacityObservation(
            pool_key=lease.pool_key,
            physical_cluster_uid=lease.physical_cluster_uid,
            accelerator_names=lease.accelerator_names,
            access_context=access_context,
            observation_generation=lease.observation_generation,
            lease_token=lease.lease_token,
            lease_expires_at=lease.lease_expires_at,
            observation_sequence=lease.observation_sequence,
            ordinary_admission_sequence=lease.ordinary_admission_sequence,
            materialization_sequence=lease.materialization_sequence,
            payload=payload,
            payload_sha256='0' * 64,
            observed_at=lease.observed_at,
            completed_at=101.0,
            valid_until=lease.valid_until,
            published_at=101.0,
        )
        self.completions.append(completed)
        return completed

    def complete_success(
        self,
        lease: observation.PoolCapacityObservationLease,
        payload: observation.PoolCapacitySuccess,
        *,
        access_context: str,
    ) -> observation.PoolCapacityObservation:
        return self._complete(lease, payload, access_context=access_context)

    def complete_blackout(
        self,
        lease: observation.PoolCapacityObservationLease,
        payload: observation.PoolCapacityBlackout,
    ) -> observation.PoolCapacityObservation:
        return self._complete(lease, payload)


def test_targets_are_exact_closed_physical_identity() -> None:
    target = _target('uid-a', 'east', 'a100', 'h200')
    assert target.accelerator_names == ('a100', 'h200')
    with pytest.raises(ValueError, match='case-folded'):
        _target('uid-a', 'east', 'A100')
    with pytest.raises(ValueError, match='match its pool key'):
        observer.PoolObservationTarget(
            pool_key=target.pool_key,
            physical_cluster_uid='uid-b',
            access_contexts=('east',),
            accelerator_names=target.accelerator_names)


def test_independent_pools_query_concurrently_and_publish_independently(
) -> None:
    repository = _Repository()
    entered = 0
    entered_lock = threading.Lock()
    both_entered = threading.Event()

    def query(target: observer.PoolObservationTarget,
              deadline: float) -> observer.PoolCapacityQuerySuccess:
        del deadline
        nonlocal entered
        with entered_lock:
            entered += 1
            if entered == 2:
                both_entered.set()
        assert both_entered.wait(timeout=1)
        return _query_success(
            target,
            observation.PoolCapacitySuccess.from_counts(
                1, {target.accelerator_names[0]: 1}))

    published: list[str] = []
    worker = observer.PoolCapacityObserver(
        repository,
        query,
        publish=lambda row: published.append(row.pool_key),
        query_timeout_seconds=1,
        max_workers=2)
    try:
        completed = worker.observe_once(
            (_target('uid-a', 'east', 'a100'), _target('uid-b', 'west',
                                                       'h200')))
    finally:
        worker.close()

    assert len(completed) == 2
    assert set(published) == {row.pool_key for row in completed}
    assert all(
        isinstance(row.payload, observation.PoolCapacitySuccess)
        for row in completed)
    assert len({row.observation_sequence for row in completed}) == 1
    assert len({row.ordinary_admission_sequence for row in completed}) == 1
    assert len({row.materialization_sequence for row in completed}) == 1


def test_busy_pool_does_not_starve_cohort_sibling() -> None:
    repository = _Repository()
    busy = _target('uid-a', 'east', 'a100')
    healthy = _target('uid-b', 'west', 'h200')
    repository.busy.add(busy.pool_key)
    queried: list[str] = []

    def query(target: observer.PoolObservationTarget,
              deadline: float) -> observer.PoolCapacityQuerySuccess:
        del deadline
        queried.append(target.pool_key)
        return _query_success(
            target,
            observation.PoolCapacitySuccess.from_counts(
                2, {target.accelerator_names[0]: 2}))

    worker = observer.PoolCapacityObserver(repository,
                                           query,
                                           query_timeout_seconds=1)
    try:
        completed = worker.observe_once((busy, healthy))
    finally:
        worker.close()
    assert queried == [healthy.pool_key]
    assert [row.pool_key for row in completed] == [healthy.pool_key]


def test_observation_routes_rotate_without_changing_physical_identity() -> None:
    repository = _Repository()
    target = observer.PoolObservationTarget(
        pool_key=json.dumps(['v2', 'uid-a', 'a100']),
        physical_cluster_uid='uid-a',
        access_contexts=('east-primary', 'east-alias'),
        accelerator_names=('a100',),
    )
    attempted: list[tuple[str, ...]] = []

    def query(
        rotated_target: observer.PoolObservationTarget,
        deadline: float,
    ) -> observer.PoolCapacityQuerySuccess:
        del deadline
        attempted.append(rotated_target.access_contexts)
        return _query_success(
            rotated_target,
            observation.PoolCapacitySuccess.from_counts(1, {'a100': 1}))

    worker = observer.PoolCapacityObserver(repository,
                                           query,
                                           query_timeout_seconds=1)
    try:
        first = worker.observe_once((target,))
        second = worker.observe_once((target,))
    finally:
        worker.close()

    assert attempted == [('east-primary', 'east-alias'),
                         ('east-alias', 'east-primary')]
    assert first[0].pool_key == second[0].pool_key == target.pool_key
    assert first[0].access_context == 'east-primary'
    assert second[0].access_context == 'east-alias'


def test_deadline_publishes_blackout_and_ignores_late_success() -> None:
    repository = _Repository()
    release = threading.Event()

    def query(target: observer.PoolObservationTarget,
              deadline: float) -> observer.PoolCapacityQuerySuccess:
        del target, deadline
        release.wait(timeout=1)
        return _query_success(
            target, observation.PoolCapacitySuccess.from_counts(1, {'a100': 1}))

    worker = observer.PoolCapacityObserver(repository,
                                           query,
                                           query_timeout_seconds=0.02,
                                           completion_margin_seconds=1)
    try:
        started = time.monotonic()
        completed = worker.observe_once((_target('uid-a', 'east', 'a100'),))
        elapsed = time.monotonic() - started
        assert elapsed < 0.5
        assert len(completed) == 1
        assert isinstance(completed[0].payload,
                          observation.PoolCapacityBlackout)
        assert completed[0].payload.reason is (
            observation.PoolCapacityBlackoutReason.TIMEOUT)
        release.set()
        time.sleep(0.02)
        assert len(repository.completions) == 1
    finally:
        release.set()
        worker.close()


def test_deadline_cancels_kubernetes_work_and_releases_executor_worker(
        monkeypatch: pytest.MonkeyPatch) -> None:
    repository = _Repository()
    timed_out_query_stopped = threading.Event()

    def query_group(context: str, *_args: Any,
                    **_kwargs: Any) -> observation.PoolCapacitySuccess:
        if context == 'east':
            try:
                while True:
                    (reserved_capacity.kubernetes.
                     raise_if_api_call_deadline_exceeded())
                    time.sleep(0.001)
            except TimeoutError:
                timed_out_query_stopped.set()
                raise
        return observation.PoolCapacitySuccess.from_counts(1, {'h200': 1})

    monkeypatch.setattr(reserved_capacity, 'query_pool_group_gpu_capacity',
                        query_group)
    monkeypatch.setattr(reserved_capacity.provider_phase, 'provider_phase',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    worker = observer.PoolCapacityObserver(
        repository,
        reserved_capacity.query_pool_capacity_target,
        query_timeout_seconds=0.03,
        completion_margin_seconds=1,
        max_workers=1)
    try:
        timed_out = worker.observe_once((_target('uid-a', 'east', 'a100'),))
        assert isinstance(timed_out[0].payload,
                          observation.PoolCapacityBlackout)
        assert timed_out[0].payload.reason is (
            observation.PoolCapacityBlackoutReason.TIMEOUT)
        assert timed_out_query_stopped.wait(timeout=0.5)

        # The same one-worker executor must remain usable after cancellation;
        # a timed-out Kubernetes query cannot permanently consume its worker.
        healthy = worker.observe_once((_target('uid-b', 'west', 'h200'),))
        assert isinstance(healthy[0].payload, observation.PoolCapacitySuccess)
        assert healthy[0].payload.free_gpus == 1
    finally:
        worker.close()


def test_provider_failure_is_a_typed_pool_local_blackout() -> None:
    repository = _Repository()

    def query(target: observer.PoolObservationTarget,
              deadline: float) -> observer.PoolCapacityQuerySuccess:
        del target, deadline
        raise observer.PoolCapacityQueryFailure(
            observation.PoolCapacityBlackoutReason.PERMISSION_DENIED,
            'pods/list forbidden')

    worker = observer.PoolCapacityObserver(repository,
                                           query,
                                           query_timeout_seconds=1)
    try:
        completed = worker.observe_once((_target('uid-a', 'east', 'a100'),))
    finally:
        worker.close()
    payload = completed[0].payload
    assert isinstance(payload, observation.PoolCapacityBlackout)
    assert payload.reason is (
        observation.PoolCapacityBlackoutReason.PERMISSION_DENIED)
    assert payload.detail == 'pods/list forbidden'


def test_credential_probe_failure_publishes_blackout_not_zero(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed exact-context authorization cannot publish successful zero."""
    repository = _Repository()
    monkeypatch.setattr(reserved_capacity.provider_phase, 'provider_phase',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(reserved_capacity.kubernetes_catalog.sky_check,
                        'get_workspace_allowed_clouds',
                        lambda *_args, **_kwargs: ['Kubernetes'])
    monkeypatch.setattr(
        reserved_capacity.kubernetes_catalog.kubernetes_utils,
        'check_credentials', lambda *_args, **_kwargs:
        (False, 'test authorization failure'))

    worker = observer.PoolCapacityObserver(
        repository,
        reserved_capacity.query_pool_capacity_target,
        query_timeout_seconds=1)
    try:
        completed = worker.observe_once((_target('uid-a', 'east', 'a100'),))
    finally:
        worker.close()

    assert len(repository.completions) == 1
    assert completed == tuple(repository.completions)
    payload = completed[0].payload
    assert isinstance(payload, observation.PoolCapacityBlackout)
    assert payload.reason is (
        observation.PoolCapacityBlackoutReason.PROVIDER_ERROR)
    assert 'test authorization failure' in (payload.detail or '')


def test_round_rejects_duplicate_pool_identity() -> None:
    repository = _Repository()
    target = _target('uid-a', 'east', 'a100')
    worker = observer.PoolCapacityObserver(
        repository,
        lambda target, _deadline: _query_success(
            target, observation.PoolCapacitySuccess.from_counts(0, {'a100': 0})
        ),
        query_timeout_seconds=1)
    try:
        with pytest.raises(ValueError, match='repeat a pool key'):
            worker.observe_once((target, target))
    finally:
        worker.close()


def test_fixed_rate_misses_coalesce_to_one_immediate_round() -> None:
    assert observer._next_fixed_rate_deadline(60.0, 75.0, 60.0) == 120.0
    assert observer._next_fixed_rate_deadline(60.0, 121.0, 60.0) == 121.0
    # The immediate round establishes the next fixed-rate period; there is no
    # replay of the skipped 120-second deadline.
    assert observer._next_fixed_rate_deadline(121.0, 122.0, 60.0) == 181.0


def test_kubernetes_query_adapter_returns_exact_typed_success(
        monkeypatch: pytest.MonkeyPatch) -> None:
    target = _target('uid-a', 'east', 'a100', 'h200')
    monkeypatch.setattr(
        reserved_capacity, 'query_pool_group_gpu_capacity',
        lambda *args, **kwargs: observation.PoolCapacitySuccess.from_counts(
            3, {
                'a100': 1,
                'h200': 2,
            }))

    result = reserved_capacity.query_pool_capacity_target(
        target,
        time.monotonic() + 1)

    assert result.access_context == 'east'
    assert result.payload.free_gpus == 3
    assert result.payload.free_gpus_by_accelerator == (('a100', 1), ('h200', 2))


def test_raw_gpu_query_rejects_case_folded_provider_collisions(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reserved_capacity.kubernetes_catalog,
                        'list_accelerators_realtime', lambda **_kwargs:
                        ({}, {}, {
                            'A100': 1,
                            'a100': 2,
                        }))
    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())

    with pytest.raises(ValueError, match='case-folded duplicate'):
        reserved_capacity.query_pool_group_gpu_capacity(
            'east', ('a100',), expected_physical_cluster_uid='uid-a')


def test_raw_gpu_query_joins_a_same_context_uid_initializer(
        monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[bool] = []

    @contextlib.contextmanager
    def physical_fence(_context: str, _uid: str, *, wait_for_initializer: bool):
        waits.append(wait_for_initializer)
        yield

    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'physical_cluster_uid_fence', physical_fence)
    monkeypatch.setattr(reserved_capacity.kubernetes_catalog,
                        'list_accelerators_realtime', lambda **_kwargs:
                        ({}, {}, {
                            'A100': 1,
                        }))

    payload = reserved_capacity.query_pool_group_gpu_capacity(
        'east', ('a100',), expected_physical_cluster_uid='uid-a')

    assert payload.free_gpus == 1
    assert waits == [True]


def test_raw_gpu_query_preserves_catalog_permission_denial(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reserved_capacity.kubernetes,
                        'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(reserved_capacity.kubernetes_catalog,
                        'list_accelerators_realtime', lambda **_kwargs:
                        ({}, {}, {
                            'A100': -1,
                        }))

    with pytest.raises(observer.PoolCapacityQueryFailure) as raised:
        reserved_capacity.query_pool_group_gpu_capacity(
            'east', ('a100',), expected_physical_cluster_uid='uid-a')

    assert raised.value.reason is (
        observation.PoolCapacityBlackoutReason.PERMISSION_DENIED)


def test_kubernetes_query_adapter_fails_over_between_authenticated_aliases(
        monkeypatch: pytest.MonkeyPatch) -> None:
    target = observer.PoolObservationTarget(
        pool_key=json.dumps(['v2', 'uid-a', 'a100']),
        physical_cluster_uid='uid-a',
        access_contexts=('stale-alias', 'healthy-alias'),
        accelerator_names=('a100',),
    )
    attempted: list[tuple[str, float]] = []

    class _Forbidden(Exception):
        status = 403

    def query_group(context: str, *_args: Any,
                    **_kwargs: Any) -> observation.PoolCapacitySuccess:
        attempted.append((context, time.monotonic()))
        if context == 'stale-alias':
            raise _Forbidden('forbidden')
        return observation.PoolCapacitySuccess.from_counts(2, {'a100': 2})

    monkeypatch.setattr(reserved_capacity, 'query_pool_group_gpu_capacity',
                        query_group)
    deadline = time.monotonic() + 1
    result = reserved_capacity.query_pool_capacity_target(target, deadline)

    assert [context for context, _ in attempted
           ] == ['stale-alias', 'healthy-alias']
    assert all(attempted[index][1] < deadline for index in range(2))
    assert result.access_context == 'healthy-alias'
    assert result.payload.free_gpus == 2


def test_kubernetes_query_adapter_blackouts_only_after_every_alias_fails(
        monkeypatch: pytest.MonkeyPatch) -> None:
    target = observer.PoolObservationTarget(
        pool_key=json.dumps(['v2', 'uid-a', 'a100']),
        physical_cluster_uid='uid-a',
        access_contexts=('east-primary', 'east-alias'),
        accelerator_names=('a100',),
    )
    attempted: list[str] = []

    class _Forbidden(Exception):
        status = 403

    def forbidden(context: str, *_args: Any, **_kwargs: Any) -> None:
        attempted.append(context)
        raise _Forbidden(f'{context} forbidden')

    monkeypatch.setattr(reserved_capacity, 'query_pool_group_gpu_capacity',
                        forbidden)
    with pytest.raises(observer.PoolCapacityQueryFailure) as raised:
        reserved_capacity.query_pool_capacity_target(target,
                                                     time.monotonic() + 1)

    assert attempted == ['east-primary', 'east-alias']
    assert raised.value.reason is (
        observation.PoolCapacityBlackoutReason.PERMISSION_DENIED)
    assert 'east-primary:' in str(raised.value)
    assert 'east-alias:' in str(raised.value)


def test_kubernetes_query_adapter_rejects_partial_exact_card_response(
        monkeypatch: pytest.MonkeyPatch) -> None:
    target = _target('uid-a', 'east', 'a100', 'h200')
    monkeypatch.setattr(
        reserved_capacity, 'query_pool_group_gpu_capacity',
        lambda *args, **kwargs: observation.PoolCapacitySuccess.from_counts(
            1, {'a100': 1}))

    with pytest.raises(observer.PoolCapacityQueryFailure) as raised:
        reserved_capacity.query_pool_capacity_target(target,
                                                     time.monotonic() + 1)
    assert raised.value.reason is (
        observation.PoolCapacityBlackoutReason.MALFORMED_RESPONSE)


def test_kubernetes_query_adapter_classifies_permission_denial(
        monkeypatch: pytest.MonkeyPatch) -> None:

    class _Forbidden(Exception):
        status = 403

    def forbidden(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise _Forbidden('forbidden')

    monkeypatch.setattr(reserved_capacity, 'query_pool_group_gpu_capacity',
                        forbidden)
    with pytest.raises(observer.PoolCapacityQueryFailure) as raised:
        reserved_capacity.query_pool_capacity_target(
            _target('uid-a', 'east', 'a100'),
            time.monotonic() + 1)
    assert raised.value.reason is (
        observation.PoolCapacityBlackoutReason.PERMISSION_DENIED)
