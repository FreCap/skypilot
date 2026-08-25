"""Failure-isolated provider evidence for bound non-pool launches.

Provider observation is deliberately separate from association reduction.
Reserved-fill profiles retain an immutable physical provider identity. An
ordinary-paid AWS Spot failure may instead carry an exact zero-effect create
receipt on its terminal request. Other profiles and incomplete receipts remain
``UNKNOWN``; a missing SkyPilot cluster record is never promoted into provider
absence.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import time
from typing import Any

from sky.adaptors import common as adaptors_common
from sky.provision import capacity_policy
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import reserved_capacity
from sky.utils import common_utils

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')
api_requests = adaptors_common.LazyImport('sky.server.requests.requests')


@dataclasses.dataclass(frozen=True)
class ProviderObservation:
    """One closed provider classification and its canonical evidence."""

    evidence: ordinary_launch_binding.ProviderEvidence
    payload: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class ProviderAbsenceReplicaProjection:
    """Validated replica and paid-capacity result for exact absence."""

    paid_capacity_pool_key: str | None
    paid_capacity_outcome: paid_capacity.LaunchOutcome | None


def decoded_request_error(error: Any) -> BaseException | None:
    """Extract the exception from the exact durable request error shape."""
    if isinstance(error, BaseException):
        return error
    if not api_requests.decoded_error_is_valid(error):
        return None
    error_object = error['object']
    assert isinstance(error_object, BaseException)
    return error_object


def apply_exact_provider_absence_replica_projection(
        projection: Any) -> ProviderAbsenceReplicaProjection | None:
    """Validate exact ABSENT evidence and update its locked replica copy.

    This is the single replica-side reducer for provider absence.  The caller
    remains responsible for committing the replica, association, retention
    pin, and paid claim in one PostgreSQL transaction.  This function performs
    no provider or database I/O.
    """
    if (getattr(projection, 'provider_evidence', None) !=
            ordinary_launch_binding.ProviderEvidence.ABSENT):
        return None
    context = getattr(projection, 'context', None)
    if (not isinstance(context,
                       ordinary_launch_binding.BoundNonPoolLaunchContext) or
            projection.pre_effect_terminal or
            projection.service_job_id is not None):
        return None

    info = projection.locked_replica_info
    pool_key = projection.paid_capacity_pool_key
    paid_outcome = None
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        if pool_key is not None:
            return None
    elif (context.profile.kind ==
          ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
        request = getattr(projection, 'request', None)
        decoded_error = decoded_request_error(getattr(request, 'error', None))
        evidence_payload = getattr(projection, 'provider_evidence_payload',
                                   None)
        expected_receipt = (evidence_payload.get('receipt') if isinstance(
            evidence_payload, Mapping) else None)
        expected_cloud_name = (expected_receipt.get('cluster_name_on_cloud') if
                               isinstance(expected_receipt, Mapping) else None)
        try:
            expected_client_token = (
                ordinary_launch_binding.ordinary_paid_aws_client_token(context))
            expected_aws_account_id = (
                ordinary_launch_binding.
                ordinary_paid_aws_account_id_from_pool_key(pool_key))
        except (TypeError, ValueError,
                ordinary_launch_binding.OrdinaryLaunchBindingConflict):
            return None
        provider_negative_ack = (
            capacity_policy.extract_provider_negative_ack(decoded_error)
            if decoded_error is not None else None)
        provider_negative_ack = capacity_policy.validate_provider_negative_ack(
            provider_negative_ack,
            cluster_name=expected_cloud_name,
            client_token=expected_client_token,
            expected_aws_account_id=expected_aws_account_id) if isinstance(
                expected_cloud_name, str) and expected_cloud_name else None
        status = getattr(getattr(projection, 'status', None), 'value', None)
        cause = getattr(getattr(projection, 'cause', None), 'value', None)
        if (provider_negative_ack is None or
                provider_negative_ack != expected_receipt or
                status != 'FAILED' or cause != 'handler_failed' or
                not isinstance(pool_key, str) or not pool_key or
                info.paid_capacity_pool_key != pool_key or
                info.is_spot is not True or info.is_zero_cost is not False or
                info.reserved_fill is not False or
                info.service_job_id is not None):
            return None
        reason = provider_negative_ack['reason']
        if reason == 'quota':
            paid_outcome = paid_capacity.LaunchOutcome.QUOTA_FAILURE
        elif reason == 'capacity':
            paid_outcome = paid_capacity.LaunchOutcome.CAPACITY_FAILURE
        else:
            return None
        info.status_property.failed_spot_availability = True
    else:
        return None

    if (info.status_property.sky_launch_status !=
            common_utils.ProcessStatus.INTERRUPTED):
        info.status_property.sky_launch_status = common_utils.ProcessStatus.FAILED
    return ProviderAbsenceReplicaProjection(paid_capacity_pool_key=pool_key,
                                            paid_capacity_outcome=paid_outcome)


def _reserved_fill_observation_payload(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    fence: reserved_capacity.ProtocolV2CleanupFence,
) -> dict[str, Any]:
    """Build the canonical exact reserved-fill provider evidence payload."""
    return {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'kubernetes_context': fence.kubernetes_context,
        'physical_cluster_uid': fence.physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }


def observe_provider(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
) -> ProviderObservation:
    """Read only the exact provider identity retained by the profile."""
    if not isinstance(context,
                      ordinary_launch_binding.BoundNonPoolLaunchContext):
        raise TypeError('context must be a bound non-pool launch context.')
    base = {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }
    if (context.profile.kind !=
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'probe_contract': 'immutable-provider-identity-v1',
                'reason': 'profile-has-no-durable-provider-uid',
            })

    try:
        fence = reserved_capacity.parse_protocol_v2_cleanup_fence(replica_info)
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'probe_contract': 'kubernetes-physical-replica-presence-v1',
                'reason': 'malformed-provider-identity',
            })
    if fence is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'probe_contract': 'kubernetes-physical-replica-presence-v1',
                'reason': 'missing-provider-identity',
            })

    base = _reserved_fill_observation_payload(context, replica_info, fence)
    current_uid = reserved_capacity.get_kubernetes_physical_cluster_uid(
        fence.kubernetes_context, force_refresh=True)
    if current_uid is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'reason': 'physical-cluster-identity-unreadable',
            })
    if current_uid != fence.physical_cluster_uid:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.REPLACED, {
                **base,
                'observed_physical_cluster_uid': current_uid,
                'reason': 'kubernetes-context-retargeted',
            })

    provider_read_boundary = time.monotonic()
    presence = reserved_capacity.probe_physical_replica_presence(
        fence, replica_info.cluster_name, observed_after=provider_read_boundary)
    classification = {
        reserved_capacity.PhysicalReplicaPresence.ABSENT:
            ordinary_launch_binding.ProviderEvidence.ABSENT,
        reserved_capacity.PhysicalReplicaPresence.PRESENT:
            ordinary_launch_binding.ProviderEvidence.PRESENT,
        reserved_capacity.PhysicalReplicaPresence.UNPROVEN:
            ordinary_launch_binding.ProviderEvidence.UNKNOWN,
    }[presence]
    return ProviderObservation(classification, {
        **base,
        'result': presence.value,
    })


def observe_post_teardown_absence_receipt(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    receipt: reserved_capacity.ProtocolV2PhysicalAbsenceReceipt,
) -> ProviderObservation:
    """Authenticate an already-observed exact post-teardown ABSENT result.

    The teardown worker obtained this receipt from the uncached provider read
    performed under the replica's immutable physical-cluster fence. Reusing it
    here prevents a second provider read from turning proven absence back into
    transient UNKNOWN.
    """
    if not isinstance(receipt,
                      reserved_capacity.ProtocolV2PhysicalAbsenceReceipt):
        raise TypeError('receipt must be a protocol-v2 absence receipt.')
    if (context.profile.kind !=
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence requires a reserved-fill profile.')
    try:
        fence = reserved_capacity.parse_protocol_v2_cleanup_fence(replica_info)
    except Exception as error:  # pylint: disable=broad-except
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence lost its durable provider '
            'identity.') from error
    if (fence is None or
            receipt.cleanup_fence != fence or receipt.cluster_name != getattr(
                replica_info, 'cluster_name', None)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence does not match the exact '
            'reserved-fill replica.')
    return ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {
            **_reserved_fill_observation_payload(context, replica_info, fence),
            'result': reserved_capacity.PhysicalReplicaPresence.ABSENT.value,
        })


def _reduce_observation(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    observation: ProviderObservation,
) -> None:
    """Persist and reduce one already-completed exact provider observation."""
    request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, observation.evidence, observation.payload)
    if observation.evidence == ordinary_launch_binding.ProviderEvidence.ABSENT:
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
    elif (observation.evidence ==
          ordinary_launch_binding.ProviderEvidence.PRESENT):
        request_postgres.authorize_bound_non_pool_provider_present_cleanup(
            context, authority, project_replica_result=project_replica_result)


def reconcile_post_teardown_absence(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    receipt: reserved_capacity.ProtocolV2PhysicalAbsenceReceipt,
) -> ProviderObservation:
    """Project one exact post-teardown receipt without provider reread."""
    if not request_postgres.bound_non_pool_provider_reconciliation_ready(
            context, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation is waiting for exact request '
            'quiescence.')
    observation = observe_post_teardown_absence_receipt(context, replica_info,
                                                        receipt)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation


def reconcile(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    *,
    force_provider_read: bool = False,
) -> ProviderObservation:
    """Observe outside locks, then reduce exact absence or authorize cleanup."""
    if not request_postgres.bound_non_pool_provider_reconciliation_ready(
            context, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation is waiting for exact request '
            'quiescence.')
    if (not force_provider_read and
            request_postgres.bound_non_pool_provider_absence_is_recorded(
                context, authority)):
        # ABSENT is immutable exact evidence. Project it before another
        # provider read: a later transient UNKNOWN observation must not strand
        # a row whose absence was already proven after executor quiescence.
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.ABSENT, {
                'result':
                    reserved_capacity.PhysicalReplicaPresence.ABSENT.value,
                'source': 'durable-provider-evidence',
            })
    observation = None
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
        paid_payload = (
            request_postgres.bound_non_pool_terminal_provider_absence_payload(
                context, authority))
        if paid_payload is not None:
            observation = ProviderObservation(
                ordinary_launch_binding.ProviderEvidence.ABSENT, paid_payload)
    if observation is None:
        observation = observe_provider(context, replica_info)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation
