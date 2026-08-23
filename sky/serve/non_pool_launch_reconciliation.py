"""Failure-isolated provider evidence for bound non-pool launches.

Provider observation is deliberately separate from association reduction.
Only reserved-fill profiles currently retain an immutable physical provider
identity.  Other profiles therefore remain ``UNKNOWN`` when their exact
request result cannot settle the action; a missing SkyPilot cluster record is
never promoted into provider absence.
"""

from __future__ import annotations

from collections.abc import Callable
import dataclasses
from typing import Any

from sky.adaptors import common as adaptors_common
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')


@dataclasses.dataclass(frozen=True)
class ProviderObservation:
    """One closed provider classification and its canonical evidence."""

    evidence: ordinary_launch_binding.ProviderEvidence
    payload: dict[str, Any]


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
    if (context.profile.kind
            != ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
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

    presence = reserved_capacity.probe_physical_replica_presence(
        fence, replica_info.cluster_name, use_cache=False)
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
    if (context.profile.kind
            != ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
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
    observation = observe_provider(context, replica_info)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation
