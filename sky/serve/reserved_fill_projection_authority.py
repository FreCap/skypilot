"""Canonical adapter from frozen worker projections to reclaim authority."""

from collections.abc import Sequence
from typing import Any

from sky.serve import kubernetes_identity
from sky.serve import reserved_fill_reclaim_attestation

_TEARDOWN_COMPATIBILITY_PREDECESSORS = 2


def projected_admission_for_candidate(
    worker_projections: Any,
    *,
    kubernetes_context: str,
    accelerator: str,
    accelerator_count: int,
    expected_sha256: str | None = None,
    require_current_protocol: bool = False,
) -> tuple[dict[str, Any],
           reserved_fill_reclaim_attestation.ReclaimProjectedAdmission]:
    """Select one strict candidate and derive its typed policy view."""
    validated = kubernetes_identity.validate_worker_placement_projections(
        worker_projections,
        allow_none=False,
        require_protocol_version=(
            kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION
            if require_current_protocol else None))
    assert validated is not None
    if not kubernetes_identity.worker_projection_has_strict_admission(
            validated[0]):
        if require_current_protocol:
            raise ValueError('Reclaim requires the exact current worker '
                             'projection protocol.')
        raise ValueError('Reclaim requires a strict worker projection '
                         'protocol.')
    projection = kubernetes_identity.worker_projection_for_context(
        validated, kubernetes_context, {accelerator: accelerator_count})
    if projection is None:
        raise ValueError('Reclaim has no exact strict worker projection.')
    projection_sha256 = kubernetes_identity.worker_projection_sha256(projection)
    if (expected_sha256 is not None and projection_sha256 != expected_sha256):
        raise ValueError('Reclaim worker projection digest changed.')
    admission = (reserved_fill_reclaim_attestation.
                 projected_admission_from_worker_projection(
                     projection, worker_projection_sha256=projection_sha256))
    return projection, admission


def projected_admission_mode_for_teardown_candidate(
    worker_projections: Any,
    *,
    kubernetes_context: str,
    accelerator: str,
    accelerator_count: int,
    expected_sha256: str,
) -> reserved_fill_reclaim_attestation.ReclaimAdmissionMode:
    """Classify exact immutable authority within the teardown-only window.

    Fresh admission and every provider-effect start require the exact current
    protocol. Normal teardown may classify the current protocol and its two
    immediate predecessors so a release cannot strand already-owned durable
    state. Older decodable projections remain evidence only and fail closed.
    """
    projection, admission = projected_admission_for_candidate(
        worker_projections,
        kubernetes_context=kubernetes_context,
        accelerator=accelerator,
        accelerator_count=accelerator_count,
        expected_sha256=expected_sha256)
    protocol_version = kubernetes_identity.worker_projection_protocol_version(
        projection)
    current_protocol = (
        kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION)
    oldest_protocol = max(
        1, current_protocol - _TEARDOWN_COMPATIBILITY_PREDECESSORS)
    if not oldest_protocol <= protocol_version <= current_protocol:
        raise ValueError(
            'Teardown requires the current worker projection protocol or one '
            f'of its two immediate predecessors ({oldest_protocol}-'
            f'{current_protocol}); found {protocol_version}.')
    return admission.admission_mode


def projected_admissions_for_edge(
    worker_projections: Any,
    *,
    access_context: str,
    accelerator_names: Sequence[str],
    accelerator_count: int,
    require_current_protocol: bool = False,
) -> tuple[reserved_fill_reclaim_attestation.ReclaimProjectedAdmission, ...]:
    """Derive the exact sorted policy candidates for one physical edge."""
    if (not isinstance(access_context, str) or not access_context or
            type(accelerator_count) is not int or accelerator_count < 1):
        raise ValueError('Reclaim edge location and width must be exact.')
    folded_names = tuple(
        sorted(name.casefold()
               for name in accelerator_names
               if isinstance(name, str) and name))
    if (not folded_names or len(folded_names) != len(accelerator_names) or
            len(set(folded_names)) != len(folded_names)):
        raise ValueError('Reclaim edge accelerators must be unique text.')
    admissions = []
    for accelerator in folded_names:
        _, admission = projected_admission_for_candidate(
            worker_projections,
            kubernetes_context=access_context,
            accelerator=accelerator,
            accelerator_count=accelerator_count,
            require_current_protocol=require_current_protocol)
        admissions.append(admission)
    return tuple(sorted(admissions))


def projection_sha256_by_accelerator(
    admissions: Sequence[
        reserved_fill_reclaim_attestation.ReclaimProjectedAdmission],
) -> dict[str, str]:
    """Return the closed persisted digest map for typed edge admissions."""
    result = {
        admission.accelerator: admission.worker_projection_sha256
        for admission in admissions
    }
    if len(result) != len(admissions):
        raise ValueError('Reclaim edge admissions contain duplicate cards.')
    return dict(sorted(result.items()))
