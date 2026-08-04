"""Tests for the pure volume refresh projection."""

import ast
import dataclasses
import inspect
import typing

import pytest

from sky.utils import status_lib
from sky.volumes import refresh_projection


def _observed(**changes: object) -> refresh_projection.ObservedRefresh:
    values: dict[str, object] = {
        'current_status': status_lib.VolumeStatus.READY,
        'current_error': None,
        'current_usedby_pods': (),
        'current_usedby_clusters': (),
        'observed_error': None,
        'observed_usedby_pods': (),
        'observed_usedby_clusters': (),
    }
    values.update(changes)
    return refresh_projection.ObservedRefresh(**
                                              values)  # type: ignore[arg-type]


def _capture_observed(
    budget: refresh_projection.DeferredCaptureBudget,
    **changes: object,
) -> refresh_projection.ObservedRefresh | None:
    values: dict[str, object] = {
        'current_status': status_lib.VolumeStatus.READY,
        'current_error': None,
        'current_usedby_pods': [],
        'current_usedby_clusters': [],
        'observed_error': None,
        'observed_usedby_pods': [],
        'observed_usedby_clusters': [],
    }
    values.update(changes)
    return refresh_projection.capture_observed_refresh(
        budget, **values)  # type: ignore[arg-type]


def _budget_counts(
    budget: refresh_projection.DeferredCaptureBudget,) -> tuple[int, int, int]:
    return (budget.captured_snapshots, budget.captured_usage_references,
            budget.captured_usage_identity_bytes)


def test_snapshot_and_projection_variants_are_frozen_and_well_formed() -> None:
    failed = refresh_projection.UsedByFetchFailed()
    observed = _observed()
    write = refresh_projection.Write(status=status_lib.VolumeStatus.READY,
                                     error_message=None,
                                     usedby_pods=(),
                                     usedby_clusters=())

    assert not dataclasses.fields(failed)
    assert not dataclasses.fields(refresh_projection.Skip())
    assert not dataclasses.fields(refresh_projection.NoWrite())
    with pytest.raises(dataclasses.FrozenInstanceError):
        observed.current_error = 'changed'  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        write.error_message = 'changed'  # type: ignore[misc]
    with pytest.raises(TypeError):
        inspect.signature(refresh_projection.Write).bind(
            status=status_lib.VolumeStatus.READY,
            error_message=None,
            usedby_pods=())


def test_capture_limits_are_fixed_characterization_bounds() -> None:
    assert refresh_projection.MAX_CAPTURED_SNAPSHOTS == 128
    assert refresh_projection.MAX_CAPTURED_USAGE_REFERENCES == 4096
    assert refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES == 256 * 1024


def test_capture_usedby_fetch_failed_has_all_or_nothing_snapshot_boundary(
) -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=refresh_projection.MAX_CAPTURED_SNAPSHOTS - 1,
        captured_usage_references=(
            refresh_projection.MAX_CAPTURED_USAGE_REFERENCES),
        captured_usage_identity_bytes=(
            refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES))

    assert isinstance(refresh_projection.capture_usedby_fetch_failed(budget),
                      refresh_projection.UsedByFetchFailed)
    assert _budget_counts(budget) == (
        refresh_projection.MAX_CAPTURED_SNAPSHOTS,
        refresh_projection.MAX_CAPTURED_USAGE_REFERENCES,
        refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES)
    before_overflow = _budget_counts(budget)
    assert refresh_projection.capture_usedby_fetch_failed(budget) is None
    assert _budget_counts(budget) == before_overflow


def test_capture_observed_refresh_reaches_reference_boundary_then_rejects(
) -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_usage_references=(
            refresh_projection.MAX_CAPTURED_USAGE_REFERENCES - 4))
    current_pods = ['current-pod']
    current_clusters = ['current-cluster']
    observed_pods = ['observed-pod']
    observed_clusters = ['observed-cluster']

    snapshot = _capture_observed(budget,
                                 current_usedby_pods=current_pods,
                                 current_usedby_clusters=current_clusters,
                                 observed_usedby_pods=observed_pods,
                                 observed_usedby_clusters=observed_clusters)

    assert snapshot == refresh_projection.ObservedRefresh(
        current_status=status_lib.VolumeStatus.READY,
        current_error=None,
        current_usedby_pods=('current-pod',),
        current_usedby_clusters=('current-cluster',),
        observed_error=None,
        observed_usedby_pods=('observed-pod',),
        observed_usedby_clusters=('observed-cluster',))
    expected_bytes = sum(
        len(identity.encode('utf-8'))
        for identity in ('current-pod', 'current-cluster', 'observed-pod',
                         'observed-cluster'))
    assert _budget_counts(budget) == (
        1, refresh_projection.MAX_CAPTURED_USAGE_REFERENCES, expected_bytes)
    current_pods.append('later-mutation')
    assert snapshot.current_usedby_pods == ('current-pod',)

    before_overflow = _budget_counts(budget)
    assert _capture_observed(budget,
                             observed_usedby_pods=['one-too-many']) is None
    assert _budget_counts(budget) == before_overflow


def test_capture_observed_refresh_reaches_utf8_byte_boundary_then_rejects(
) -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_usage_identity_bytes=(
            refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES - 2))

    snapshot = _capture_observed(budget, observed_usedby_pods=['é'])

    assert snapshot is not None
    assert _budget_counts(budget) == (
        1, 1, refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES)
    before_overflow = _budget_counts(budget)
    assert _capture_observed(budget, observed_usedby_pods=['a']) is None
    assert _budget_counts(budget) == before_overflow


def test_snapshot_overflow_returns_before_usage_accounting() -> None:
    length_calls = 0

    class _TrackedList(list[str]):

        def __len__(self) -> int:
            nonlocal length_calls
            length_calls += 1
            return super().__len__()

    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=refresh_projection.MAX_CAPTURED_SNAPSHOTS)
    before = _budget_counts(budget)

    assert _capture_observed(budget,
                             observed_usedby_pods=_TrackedList(['pod'])) is None
    assert length_calls == 0
    assert _budget_counts(budget) == before


def test_reference_overflow_returns_before_utf8_encoding() -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=(
            refresh_projection.MAX_CAPTURED_USAGE_REFERENCES),
        captured_usage_identity_bytes=11)
    before = _budget_counts(budget)

    assert _capture_observed(budget, observed_usedby_pods=['pod']) is None
    assert _budget_counts(budget) == before


def test_list_subclass_cannot_underreport_retained_references() -> None:

    class _LyingList(list[str]):

        def __len__(self) -> int:
            return 0

    identities = _LyingList(
        ['pod'] * (refresh_projection.MAX_CAPTURED_USAGE_REFERENCES + 1))
    budget = refresh_projection.DeferredCaptureBudget()

    assert _capture_observed(budget, observed_usedby_pods=identities) is None
    assert _budget_counts(budget) == (0, 0, 0)


def test_string_subclass_cannot_underreport_retained_utf8_bytes() -> None:

    class _LyingIdentity(str):

        def __len__(self) -> int:
            return 0

        def encode(self,
                   encoding: str = 'utf-8',
                   errors: str = 'strict') -> bytes:
            del encoding, errors
            return b''

    identity = _LyingIdentity(
        'a' * (refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES + 1))
    budget = refresh_projection.DeferredCaptureBudget()

    assert _capture_observed(budget, observed_usedby_pods=[identity]) is None
    assert _budget_counts(budget) == (0, 0, 0)


def test_character_length_overflow_returns_before_utf8_encoding() -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=11,
        captured_usage_identity_bytes=(
            refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES - 1))
    before = _budget_counts(budget)

    assert _capture_observed(budget, observed_usedby_pods=['ab']) is None
    assert _budget_counts(budget) == before


def test_encoded_utf8_length_overflow_returns_without_partial_debit() -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=11,
        captured_usage_identity_bytes=(
            refresh_projection.MAX_CAPTURED_USAGE_IDENTITY_BYTES - 1))
    before = _budget_counts(budget)

    assert _capture_observed(budget, observed_usedby_pods=['é']) is None
    assert _budget_counts(budget) == before


def test_utf8_encoding_failure_returns_none_without_partial_debit() -> None:
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=11,
        captured_usage_identity_bytes=13)
    before = _budget_counts(budget)

    assert _capture_observed(budget, observed_usedby_pods=['\ud800']) is None
    assert _budget_counts(budget) == before


def test_custom_list_iterator_is_rejected_without_invocation() -> None:
    iterator_called = False

    class _BrokenIteratorList(list[str]):

        def __iter__(self) -> typing.Iterator[str]:
            nonlocal iterator_called
            iterator_called = True
            return iter(())

    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=11,
        captured_usage_identity_bytes=13)
    before = _budget_counts(budget)

    assert _capture_observed(budget,
                             observed_usedby_pods=_BrokenIteratorList(
                                 ['pod'])) is None
    assert not iterator_called
    assert _budget_counts(budget) == before


def test_stateful_list_subclass_is_rejected_before_second_pass() -> None:

    class _GrowsOnSecondIteration(list[str]):
        """Input that would exceed the bound if capture iterated it twice."""

        def __init__(self, values: list[str]):
            super().__init__(values)
            self.iterations = 0

        def __iter__(self) -> typing.Iterator[str]:
            self.iterations += 1
            if self.iterations == 2:
                return iter(
                    ['pod'] *
                    (refresh_projection.MAX_CAPTURED_USAGE_REFERENCES + 1))
            return super().__iter__()

    values = _GrowsOnSecondIteration(['pod'])
    budget = refresh_projection.DeferredCaptureBudget(
        captured_snapshots=7,
        captured_usage_references=11,
        captured_usage_identity_bytes=13)

    before = _budget_counts(budget)

    assert _capture_observed(budget, observed_usedby_pods=values) is None
    assert values.iterations == 0
    assert _budget_counts(budget) == before


def test_failed_usedby_fetch_projects_skip_without_current_row() -> None:
    assert refresh_projection.project_volume_refresh(
        refresh_projection.UsedByFetchFailed()) == refresh_projection.Skip()


def test_truthy_error_takes_precedence_over_pod_use() -> None:
    projection = refresh_projection.project_volume_refresh(
        _observed(current_status=status_lib.VolumeStatus.IN_USE,
                  current_usedby_pods=('old-pod',),
                  observed_error='provider pending',
                  observed_usedby_pods=('new-pod',)))

    assert projection == refresh_projection.Write(
        status=status_lib.VolumeStatus.NOT_READY,
        error_message='provider pending',
        usedby_pods=('new-pod',),
        usedby_clusters=())


@pytest.mark.parametrize('observed_error', [None, ''])
def test_falsy_error_with_pod_use_projects_in_use(
        observed_error: str | None) -> None:
    projection = refresh_projection.project_volume_refresh(
        _observed(observed_error=observed_error,
                  observed_usedby_pods=('pod-a',)))

    assert projection == refresh_projection.Write(
        status=status_lib.VolumeStatus.IN_USE,
        error_message=None,
        usedby_pods=('pod-a',),
        usedby_clusters=())


def test_falsy_error_is_normalized_to_no_error() -> None:
    projection = refresh_projection.project_volume_refresh(
        _observed(current_error='', observed_error=''))

    assert projection == refresh_projection.Write(
        status=status_lib.VolumeStatus.READY,
        error_message=None,
        usedby_pods=(),
        usedby_clusters=())


def test_no_pod_use_projects_ready_even_with_cluster_use() -> None:
    projection = refresh_projection.project_volume_refresh(
        _observed(current_status=status_lib.VolumeStatus.IN_USE,
                  observed_usedby_clusters=('cluster-a',)))

    assert projection == refresh_projection.Write(
        status=status_lib.VolumeStatus.READY,
        error_message=None,
        usedby_pods=(),
        usedby_clusters=('cluster-a',))


def test_missing_current_status_projects_write() -> None:
    assert refresh_projection.project_volume_refresh(
        _observed(current_status=None)) == refresh_projection.Write(
            status=status_lib.VolumeStatus.READY,
            error_message=None,
            usedby_pods=(),
            usedby_clusters=())


def test_identical_state_projects_no_write() -> None:
    assert isinstance(refresh_projection.project_volume_refresh(_observed()),
                      refresh_projection.NoWrite)


def test_order_only_usage_changes_project_no_write() -> None:
    projection = refresh_projection.project_volume_refresh(
        _observed(current_status=status_lib.VolumeStatus.IN_USE,
                  current_usedby_pods=('pod-a', 'pod-b'),
                  current_usedby_clusters=('cluster-a', 'cluster-b'),
                  observed_usedby_pods=('pod-b', 'pod-a'),
                  observed_usedby_clusters=('cluster-b', 'cluster-a')))

    assert isinstance(projection, refresh_projection.NoWrite)


@pytest.mark.parametrize(('changes'), [{
    'current_status': status_lib.VolumeStatus.IN_USE,
    'current_usedby_pods': ('pod-a',),
    'observed_usedby_pods': ('pod-a', 'pod-a'),
}, {
    'current_usedby_clusters': ('cluster-a',),
    'observed_usedby_clusters': ('cluster-a', 'cluster-a'),
}])
def test_duplicate_only_usage_changes_project_no_write(
        changes: dict[str, object]) -> None:
    assert isinstance(
        refresh_projection.project_volume_refresh(_observed(**changes)),
        refresh_projection.NoWrite)


@pytest.mark.parametrize(('changes'), [{
    'current_status': status_lib.VolumeStatus.READY,
    'current_usedby_pods': ('pod-a',),
    'observed_usedby_pods': ('pod-a',),
}, {
    'current_status': status_lib.VolumeStatus.NOT_READY,
    'current_error': 'old error',
    'observed_error': 'new error',
}, {
    'current_status': status_lib.VolumeStatus.IN_USE,
    'current_usedby_pods': ('pod-a',),
    'observed_usedby_pods': ('pod-b',),
}, {
    'current_usedby_clusters': ('cluster-a',),
    'observed_usedby_clusters': ('cluster-b',),
}])
def test_each_independently_changed_field_projects_write(
        changes: dict[str, object]) -> None:
    assert isinstance(
        refresh_projection.project_volume_refresh(_observed(**changes)),
        refresh_projection.Write)


def test_write_preserves_observed_order_without_mutating_tuples() -> None:
    pods = ('pod-b', 'pod-a', 'pod-b')
    clusters = ('cluster-b', 'cluster-a')
    snapshot = _observed(observed_usedby_pods=pods,
                         observed_usedby_clusters=clusters)

    projection = refresh_projection.project_volume_refresh(snapshot)

    assert isinstance(projection, refresh_projection.Write)
    assert projection.usedby_pods is pods
    assert projection.usedby_clusters is clusters
    assert snapshot.observed_usedby_pods == ('pod-b', 'pod-a', 'pod-b')
    assert snapshot.observed_usedby_clusters == ('cluster-b', 'cluster-a')


def test_compare_classifies_match_and_mismatch() -> None:
    snapshot = _observed()

    assert refresh_projection.compare_volume_refresh_projection(
        snapshot, refresh_projection.NoWrite()
    ) is refresh_projection.VolumeRefreshShadowOutcome.MATCH
    assert refresh_projection.compare_volume_refresh_projection(
        snapshot, refresh_projection.Skip()
    ) is refresh_projection.VolumeRefreshShadowOutcome.MISMATCH


def test_compare_resolves_projector_per_call_and_contains_exception(
        monkeypatch: pytest.MonkeyPatch) -> None:
    real_projector = refresh_projection.project_volume_refresh
    calls = 0

    def _raise_once(
        snapshot: refresh_projection.VolumeRefreshSnapshot
    ) -> refresh_projection.VolumeRefreshProjection:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('private provider payload')
        return real_projector(snapshot)

    monkeypatch.setattr(refresh_projection, 'project_volume_refresh',
                        _raise_once)

    first = refresh_projection.compare_volume_refresh_projection(
        _observed(), refresh_projection.NoWrite())
    second = refresh_projection.compare_volume_refresh_projection(
        _observed(), refresh_projection.NoWrite())

    assert first is refresh_projection.VolumeRefreshShadowOutcome.PROJECTOR_ERROR
    assert second is refresh_projection.VolumeRefreshShadowOutcome.MATCH
    assert 'private provider payload' not in repr(first)


def test_compare_contains_equality_exception_separately() -> None:

    class _EqualityError:

        def __eq__(self, other: object) -> bool:
            del other
            raise RuntimeError('private comparison payload')

    authoritative = typing.cast(refresh_projection.VolumeRefreshProjection,
                                _EqualityError())
    outcome = refresh_projection.compare_volume_refresh_projection(
        _observed(), authoritative)

    assert outcome is refresh_projection.VolumeRefreshShadowOutcome.COMPARISON_ERROR
    assert 'private comparison payload' not in repr(outcome)


def test_projector_base_exception_propagates(
        monkeypatch: pytest.MonkeyPatch) -> None:

    class _Cancellation(BaseException):
        pass

    def _cancel(
        snapshot: refresh_projection.VolumeRefreshSnapshot
    ) -> refresh_projection.VolumeRefreshProjection:
        del snapshot
        raise _Cancellation()

    monkeypatch.setattr(refresh_projection, 'project_volume_refresh', _cancel)

    with pytest.raises(_Cancellation):
        refresh_projection.compare_volume_refresh_projection(
            _observed(), refresh_projection.NoWrite())


def test_comparison_base_exception_propagates() -> None:

    class _Cancellation(BaseException):
        pass

    class _EqualityCancellation:

        def __eq__(self, other: object) -> bool:
            del other
            raise _Cancellation()

    authoritative = typing.cast(refresh_projection.VolumeRefreshProjection,
                                _EqualityCancellation())
    with pytest.raises(_Cancellation):
        refresh_projection.compare_volume_refresh_projection(
            _observed(), authoritative)


def test_shadow_outcome_is_closed_and_carries_no_diagnostic_payload() -> None:
    assert tuple(refresh_projection.VolumeRefreshShadowOutcome) == (
        refresh_projection.VolumeRefreshShadowOutcome.MATCH,
        refresh_projection.VolumeRefreshShadowOutcome.MISMATCH,
        refresh_projection.VolumeRefreshShadowOutcome.PROJECTOR_ERROR,
        refresh_projection.VolumeRefreshShadowOutcome.COMPARISON_ERROR,
        refresh_projection.VolumeRefreshShadowOutcome.NOT_SAMPLED_BUDGET,
    )
    for outcome in refresh_projection.VolumeRefreshShadowOutcome:
        assert outcome.value == outcome.name
        assert not hasattr(outcome, 'exception')
        assert not hasattr(outcome, 'message')
        assert not hasattr(outcome, 'payload')


def test_projection_module_has_only_pure_leaf_imports() -> None:
    tree = ast.parse(inspect.getsource(refresh_projection))
    imports: list[tuple[str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, ()) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append((node.module or
                            '', tuple(alias.name for alias in node.names)))

    assert imports == [
        ('__future__', ('annotations',)),
        ('dataclasses', ()),
        ('enum', ()),
        ('typing', ()),
        ('sky.utils', ('status_lib',)),
    ]
