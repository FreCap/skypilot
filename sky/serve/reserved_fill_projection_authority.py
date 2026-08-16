"""Canonical adapter from frozen worker projections to reclaim authority."""

from collections.abc import Sequence
from typing import Any

from sky.serve import kubernetes_identity
from sky.serve import reserved_fill_reclaim_attestation


def projected_admission_for_candidate(
    worker_projections: Any,
    *,
    kubernetes_context: str,
    accelerator: str,
    accelerator_count: int,
    expected_sha256: str | None = None,
) -> tuple[dict[str, Any],
           reserved_fill_reclaim_attestation.ReclaimProjectedAdmission]:
    """Select one strict candidate and derive its typed policy view."""
    validated = kubernetes_identity.validate_worker_placement_projections(
        worker_projections, allow_none=False)
    assert validated is not None
    if not kubernetes_identity.worker_projection_has_strict_admission(
            validated[0]):
        raise ValueError('Reclaim requires a protocol-v2 or protocol-v3 '
                         'worker projection.')
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


def projected_admissions_for_edge(
    worker_projections: Any,
    *,
    access_context: str,
    accelerator_names: Sequence[str],
    accelerator_count: int,
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
            accelerator_count=accelerator_count)
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
