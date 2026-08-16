"""Failure-isolated provider evidence for bound non-pool launches.

Provider observation is deliberately separate from association reduction.
Only reserved-fill profiles currently retain an immutable physical provider
identity.  Other profiles therefore remain ``UNKNOWN`` when their exact
request result cannot settle the action; a missing SkyPilot cluster record is
never promoted into provider absence.
"""

from __future__ import annotations

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

    base.update({
        'kubernetes_context': fence.kubernetes_context,
        'physical_cluster_uid': fence.physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
    })
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


def reconcile(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
) -> ProviderObservation:
    """Observe outside locks, then owner-fence one durable evidence update."""
    if not request_postgres.bound_non_pool_provider_reconciliation_ready(
            context, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation is waiting for exact request '
            'quiescence.')
    observation = observe_provider(context, replica_info)
    request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, observation.evidence, observation.payload)
    return observation
