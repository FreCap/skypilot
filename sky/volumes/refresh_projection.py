"""Pure projection of the existing volume refresh decision."""

from __future__ import annotations

import dataclasses
import enum
import typing

from sky.utils import status_lib

MAX_CAPTURED_SNAPSHOTS = 128
MAX_CAPTURED_USAGE_REFERENCES = 4096
MAX_CAPTURED_USAGE_IDENTITY_BYTES = 256 * 1024


@dataclasses.dataclass
class DeferredCaptureBudget:
    """Mutable all-or-nothing accounting for one deferred shadow sweep."""

    captured_snapshots: int = 0
    captured_usage_references: int = 0
    captured_usage_identity_bytes: int = 0


@dataclasses.dataclass(frozen=True)
class UsedByFetchFailed:
    """Snapshot of the existing failed-used-by early exit."""


@dataclasses.dataclass(frozen=True)
class ObservedRefresh:
    """Immutable inputs to the ordinary volume refresh decision."""

    current_status: status_lib.VolumeStatus | None
    current_error: str | None
    current_usedby_pods: tuple[str, ...]
    current_usedby_clusters: tuple[str, ...]
    observed_error: str | None
    observed_usedby_pods: tuple[str, ...]
    observed_usedby_clusters: tuple[str, ...]


VolumeRefreshSnapshot: typing.TypeAlias = (UsedByFetchFailed | ObservedRefresh)


def _admit_capture(
    budget: DeferredCaptureBudget,
    usage_groups: tuple[list[str], ...],
) -> tuple[tuple[int, int, int], list[list[str]]] | None:
    """Freeze bounded usage inputs and return their prospective accounting."""
    try:
        captured_snapshots = budget.captured_snapshots
        if type(captured_snapshots) is not int or captured_snapshots < 0:
            return None
        prospective_snapshots = captured_snapshots + 1
        if prospective_snapshots > MAX_CAPTURED_SNAPSHOTS:
            return None

        if any(type(group) is not list for group in usage_groups):
            return None

        captured_usage_references = budget.captured_usage_references
        if (type(captured_usage_references) is not int or
                captured_usage_references < 0 or
                captured_usage_references > MAX_CAPTURED_USAGE_REFERENCES):
            return None
        remaining_usage_references = (MAX_CAPTURED_USAGE_REFERENCES -
                                      captured_usage_references)

        captured_usage_identity_bytes = budget.captured_usage_identity_bytes
        if (type(captured_usage_identity_bytes) is not int or
                captured_usage_identity_bytes < 0 or
                captured_usage_identity_bytes
                > MAX_CAPTURED_USAGE_IDENTITY_BYTES):
            return None
        remaining_identity_bytes = (MAX_CAPTURED_USAGE_IDENTITY_BYTES -
                                    captured_usage_identity_bytes)

        frozen_usage_groups: list[list[str]] = []
        usage_references = 0
        usage_identity_bytes = 0
        for group in usage_groups:
            frozen_group: list[str] = []
            group_iterator = iter(group)
            while True:
                try:
                    identity = next(group_iterator)
                except StopIteration:
                    break
                if usage_references >= remaining_usage_references:
                    return None
                if type(identity) is not str:
                    return None
                if len(identity) > remaining_identity_bytes:
                    return None
                encoded_identity_bytes = len(identity.encode('utf-8'))
                if encoded_identity_bytes > remaining_identity_bytes:
                    return None
                frozen_group.append(identity)
                usage_references += 1
                usage_identity_bytes += encoded_identity_bytes
                remaining_identity_bytes -= encoded_identity_bytes
            frozen_usage_groups.append(frozen_group)
    except Exception:  # pylint: disable=broad-except
        return None
    prospective_counts = (
        prospective_snapshots,
        captured_usage_references + usage_references,
        captured_usage_identity_bytes + usage_identity_bytes,
    )
    return prospective_counts, frozen_usage_groups


def _debit_capture_budget(budget: DeferredCaptureBudget,
                          prospective_counts: tuple[int, int, int]) -> None:
    budget.captured_snapshots = prospective_counts[0]
    budget.captured_usage_references = prospective_counts[1]
    budget.captured_usage_identity_bytes = prospective_counts[2]


def capture_usedby_fetch_failed(
        budget: DeferredCaptureBudget) -> UsedByFetchFailed | None:
    """Capture the failed-fetch variant when all budgets admit it."""
    admission = _admit_capture(budget, ())
    if admission is None:
        return None
    prospective_counts, _ = admission
    try:
        snapshot = UsedByFetchFailed()
    except Exception:  # pylint: disable=broad-except
        return None
    _debit_capture_budget(budget, prospective_counts)
    return snapshot


def capture_observed_refresh(
    budget: DeferredCaptureBudget,
    *,
    current_status: status_lib.VolumeStatus | None,
    current_error: str | None,
    current_usedby_pods: list[str],
    current_usedby_clusters: list[str],
    observed_error: str | None,
    observed_usedby_pods: list[str],
    observed_usedby_clusters: list[str],
) -> ObservedRefresh | None:
    """Capture one complete ordinary snapshot within all sweep budgets."""
    usage_groups = (current_usedby_pods, current_usedby_clusters,
                    observed_usedby_pods, observed_usedby_clusters)
    admission = _admit_capture(budget, usage_groups)
    if admission is None:
        return None
    prospective_counts, frozen_usage_groups = admission
    try:
        snapshot = ObservedRefresh(
            current_status=current_status,
            current_error=current_error,
            current_usedby_pods=tuple(frozen_usage_groups[0]),
            current_usedby_clusters=tuple(frozen_usage_groups[1]),
            observed_error=observed_error,
            observed_usedby_pods=tuple(frozen_usage_groups[2]),
            observed_usedby_clusters=tuple(frozen_usage_groups[3]))
    except Exception:  # pylint: disable=broad-except
        return None
    _debit_capture_budget(budget, prospective_counts)
    return snapshot


@dataclasses.dataclass(frozen=True)
class Skip:
    """The authoritative refresh skips this snapshot."""


@dataclasses.dataclass(frozen=True)
class NoWrite:
    """The authoritative refresh observes no state change."""


@dataclasses.dataclass(frozen=True)
class Write:
    """Diagnostic payload for an authoritative refresh write."""

    status: status_lib.VolumeStatus
    error_message: str | None
    usedby_pods: tuple[str, ...]
    usedby_clusters: tuple[str, ...]


VolumeRefreshProjection: typing.TypeAlias = Skip | NoWrite | Write


class VolumeRefreshShadowOutcome(enum.Enum):
    """Closed outcomes for one diagnostic shadow comparison."""

    MATCH = 'MATCH'
    MISMATCH = 'MISMATCH'
    PROJECTOR_ERROR = 'PROJECTOR_ERROR'
    COMPARISON_ERROR = 'COMPARISON_ERROR'
    NOT_SAMPLED_BUDGET = 'NOT_SAMPLED_BUDGET'


def project_volume_refresh(
        snapshot: VolumeRefreshSnapshot) -> VolumeRefreshProjection:
    """Project the existing inline volume refresh decision without effects."""
    if isinstance(snapshot, UsedByFetchFailed):
        return Skip()

    if snapshot.observed_error:
        status = status_lib.VolumeStatus.NOT_READY
        error_message = snapshot.observed_error
    elif snapshot.observed_usedby_pods:
        status = status_lib.VolumeStatus.IN_USE
        error_message = None
    else:
        status = status_lib.VolumeStatus.READY
        error_message = None

    status_changed = snapshot.current_status != status
    error_changed = snapshot.current_error != error_message
    usedby_changed = (set(snapshot.current_usedby_pods) != set(
        snapshot.observed_usedby_pods) or set(snapshot.current_usedby_clusters)
                      != set(snapshot.observed_usedby_clusters))
    if not (status_changed or error_changed or usedby_changed):
        return NoWrite()
    return Write(status=status,
                 error_message=error_message,
                 usedby_pods=snapshot.observed_usedby_pods,
                 usedby_clusters=snapshot.observed_usedby_clusters)


def compare_volume_refresh_projection(
    snapshot: VolumeRefreshSnapshot,
    authoritative_projection: VolumeRefreshProjection,
) -> VolumeRefreshShadowOutcome:
    """Compare one candidate projection with the authoritative decision."""
    try:
        candidate_projection = project_volume_refresh(snapshot)
    except Exception:  # pylint: disable=broad-except
        return VolumeRefreshShadowOutcome.PROJECTOR_ERROR

    try:
        if candidate_projection == authoritative_projection:
            return VolumeRefreshShadowOutcome.MATCH
    except Exception:  # pylint: disable=broad-except
        return VolumeRefreshShadowOutcome.COMPARISON_ERROR
    return VolumeRefreshShadowOutcome.MISMATCH
