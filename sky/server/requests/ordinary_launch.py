"""Distinct durable handler for bound ordinary SkyServe launches.

This wrapper deliberately stays on the normal request executor topology.  Its
stable registry identity prevents a legacy executor from interpreting a bound
request as the public ``sky.execution:launch`` handler.
"""

from collections.abc import Mapping
import contextlib
from typing import Any

from sky import exceptions
from sky.adaptors import common as adaptors_common
from sky.server.requests import storage as request_storage

BOUND_ORDINARY_LAUNCH_HANDLER_NAME = (
    'sky.server.requests.ordinary_launch:launch')
execution = adaptors_common.LazyImport('sky.execution')
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')
request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')

# These are the immutable server-installed fields that distinguish bound
# execution from the legacy Serve launch path.  Keeping this tiny discriminator
# local avoids importing the full Serve state machine for every ordinary
# ``sky launch``.  Any non-empty subset is delegated to the strict parser and
# therefore fails closed if incomplete.
_BOUND_CONTEXT_FIELDS = frozenset({
    'sky_serve_ordinary_launch_submission_id',
    'sky_serve_ordinary_launch_association_id',
    'sky_serve_ordinary_launch_generation',
    'sky_serve_ordinary_launch_request_id',
    'sky_serve_ordinary_launch_input_digest',
    'sky_serve_ordinary_launch_owner_revision',
    'sky_serve_non_pool_binding_protocol_version',
    'sky_serve_non_pool_profile_kind',
    'sky_serve_non_pool_profile_version',
    'sky_serve_non_pool_profile_digest',
    'sky_serve_non_pool_capability_cohort_epoch',
    'sky_serve_non_pool_capability_profile_set_digest',
    'sky_serve_non_pool_receipt_protocol_version',
    'sky_serve_non_pool_authorization_kind',
    'sky_serve_non_pool_authorization_reference',
    'sky_serve_non_pool_authorization_generation',
    'sky_serve_non_pool_authorization_digest',
})


def _has_bound_context_fields(context: Mapping[str, Any]) -> bool:
    return any(key in context for key in _BOUND_CONTEXT_FIELDS)


def _validate_bound_entrypoint_context(
        extra_launch_context: Mapping[str, Any]) -> None:
    """Strictly parse a bound context before bypassing the legacy owner fence."""
    if ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY in (
            extra_launch_context):
        ordinary_launch_binding.parse_bound_non_pool_launch_context(
            extra_launch_context)
    else:
        ordinary_launch_binding.parse_bound_launch_context(extra_launch_context)


def _provider_effect_guard(extra_launch_context: Mapping[str, Any]) -> Any:
    """Return the provider fence for a bound request, or a legacy no-op."""
    if not _has_bound_context_fields(extra_launch_context):
        return contextlib.nullcontext()
    # Parse before claim lookup so stale, server-owned fields can never fall
    # through to legacy execution merely because the context is incomplete.
    is_non_pool = ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY in (
        extra_launch_context)
    _validate_bound_entrypoint_context(extra_launch_context)
    claim = request_storage.active_execution_claim()
    if claim is None or claim.worker_instance_id is None:
        raise exceptions.RequestCancelled(
            'Bound ordinary launch has no exact durable execution claim.')
    if is_non_pool:
        return ordinary_launch_binding.non_pool_provider_effect_guard(
            extra_launch_context,
            claim,
            claim_validator=(
                request_postgres.
                validate_bound_non_pool_launch_claim_in_transaction))
    return ordinary_launch_binding.provider_effect_guard(
        extra_launch_context,
        claim,
        claim_validator=(request_postgres.
                         validate_bound_ordinary_launch_claim_in_transaction))


def _begin_service_job_io(
        extra_launch_context: Mapping[str, Any]) -> int | None:
    """Advance a bound request before service-job I/O; legacy is a no-op."""
    if not _has_bound_context_fields(extra_launch_context):
        return None
    _validate_bound_entrypoint_context(extra_launch_context)
    return ordinary_launch_binding.begin_service_job_io(extra_launch_context)


def _record_service_job(extra_launch_context: Mapping[str, Any],
                        job_id: int) -> int | None:
    """Record a bound service job under the active provider authority."""
    if not _has_bound_context_fields(extra_launch_context):
        return None
    _validate_bound_entrypoint_context(extra_launch_context)
    return ordinary_launch_binding.record_service_job(extra_launch_context,
                                                      job_id)


def launch(*args: Any, **kwargs: Any) -> Any:
    """Delegate a bound launch while carrying its exact durable claim.

    The provider-boundary fence is intentionally not advanced here: policy
    and optimization can still fail without provider I/O.  The existing launch
    path consumes :func:`request_storage.active_execution_claim` immediately
    before its provider tail and asks Serve to atomically validate/advance the
    association under the shared launch-authority guard.
    """
    claim = request_storage.active_execution_claim()
    if claim is None or claim.worker_instance_id is None:
        raise exceptions.RequestCancelled(
            'Bound ordinary launch has no exact durable execution claim.')
    return execution.launch(*args, **kwargs)
