"""Per-row reconciliation for historical unbound SkyServe launches.

The reconciler never infers execution quiescence from a terminal request.
An operator first seals an exact scope and supplies durable evidence that the
old executor is terminated. Provider readback then classifies only that exact
Kubernetes context/physical-cluster UID and logical cluster name.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import time
from typing import Any
import uuid

import sqlalchemy

from sky.adaptors import common as adaptors_common
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity
from sky.serve import serve_state

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')


class ReconciliationDisposition(str, enum.Enum):
    """Outcome of one bounded legacy reconciliation attempt."""

    QUARANTINED = 'QUARANTINED'
    PROJECTED = 'PROJECTED'
    ALREADY_PROJECTED = 'ALREADY_PROJECTED'


@dataclasses.dataclass(frozen=True)
class ReconciliationResult:
    disposition: ReconciliationDisposition
    provider_evidence: ordinary_launch_binding.ProviderEvidence
    event_id: uuid.UUID


def _database_now() -> datetime.datetime:
    engine = serve_state.get_database_engine()
    with engine.connect() as connection:
        value = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Central PostgreSQL returned a malformed database timestamp.')
    return value


def _provider_observation(
    identity: ordinary_launch_binding.LegacyLaunchIdentity,
) -> tuple[ordinary_launch_binding.ProviderEvidence, datetime.datetime, dict[
        str, Any]]:
    fence = reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context=identity.provider_context,
        physical_cluster_uid=identity.provider_physical_resource_uid)
    # Cleanup authorization requires a provider observation made after the
    # executor-termination attestation. A cached snapshot has an older,
    # process-local observation time and therefore cannot satisfy that proof.
    provider_read_boundary = time.monotonic()
    presence = reserved_capacity.probe_physical_replica_presence(
        fence, identity.cluster_name, observed_after=provider_read_boundary)
    classification = {
        reserved_capacity.PhysicalReplicaPresence.ABSENT:
            ordinary_launch_binding.ProviderEvidence.ABSENT,
        reserved_capacity.PhysicalReplicaPresence.PRESENT:
            ordinary_launch_binding.ProviderEvidence.PRESENT,
        reserved_capacity.PhysicalReplicaPresence.UNPROVEN:
            ordinary_launch_binding.ProviderEvidence.UNKNOWN,
    }[presence]
    observed_at = _database_now()
    return classification, observed_at, {
        'cluster_name': identity.cluster_name,
        'kubernetes_context': identity.provider_context,
        'physical_cluster_uid': identity.provider_physical_resource_uid,
        'probe_contract': 'physical-replica-presence-v1',
        'result': presence.value,
    }


def reconcile_scoped_legacy_launch(
    scope_id: uuid.UUID | str,
    identity: ordinary_launch_binding.LegacyLaunchIdentity,
    *,
    actor: str,
    reason: str,
    executor_terminated_at: datetime.datetime,
    executor_termination_evidence: Mapping[str, Any],
) -> ReconciliationResult:
    """Reconcile one exact legacy row without blocking sibling control work.

    Provider I/O occurs with no manager, reconciliation, or database lock.
    An unreadable provider remains ``UNKNOWN`` and quarantined. Exact absence
    after the supplied executor-termination attestation authorizes and projects
    only the matching replica record in one launch-authority transaction.
    """
    latest = ordinary_launch_binding.get_latest_legacy_reconciliation(
        scope_id, identity)
    if (latest is not None and latest['resolution'] == ordinary_launch_binding.
            LegacyReconciliationResolution.PROJECTED.value):
        return ReconciliationResult(
            ReconciliationDisposition.ALREADY_PROJECTED,
            ordinary_launch_binding.ProviderEvidence(
                latest['provider_evidence']),
            uuid.UUID(str(latest['event_id'])))
    initial_evidence = request_postgres.read_legacy_launch_request_evidence(
        identity,
        executor_terminated_at=executor_terminated_at,
        executor_termination_evidence=executor_termination_evidence)
    ordinary_launch_binding.append_legacy_reconciliation(
        scope_id,
        identity,
        ordinary_launch_binding.LegacyReconciliationResolution.EFFECT_AMBIGUOUS,
        initial_evidence,
        actor=actor,
        reason=reason)

    provider_evidence, provider_observed_at, provider_payload = (
        _provider_observation(identity))
    # Re-read after provider I/O so cleanup never relies on a request snapshot
    # that was current only before a slow or failed provider operation.
    current_evidence = request_postgres.read_legacy_launch_request_evidence(
        identity,
        executor_terminated_at=executor_terminated_at,
        executor_termination_evidence=executor_termination_evidence)
    current_evidence = dataclasses.replace(
        current_evidence,
        provider_evidence=provider_evidence,
        provider_evidence_observed_at=provider_observed_at,
        provider_evidence_payload=provider_payload)
    if provider_evidence != ordinary_launch_binding.ProviderEvidence.ABSENT:
        event = ordinary_launch_binding.append_legacy_reconciliation(
            scope_id,
            identity,
            ordinary_launch_binding.LegacyReconciliationResolution.
            EFFECT_AMBIGUOUS,
            current_evidence,
            actor=actor,
            reason=(f'{reason} Exact provider readback is '
                    f'{provider_evidence.value}.'))
        return ReconciliationResult(ReconciliationDisposition.QUARANTINED,
                                    provider_evidence,
                                    uuid.UUID(str(event['event_id'])))

    changed = (
        ordinary_launch_binding.authorize_and_project_legacy_replica_cleanup(
            scope_id,
            identity,
            current_evidence,
            actor=actor,
            authorization_reason=(
                f'{reason} Exact provider absence follows executor '
                'termination.'),
            projection_reason=(
                'Project the exact legacy replica after provider absence.'),
            cleanup_completion_evidence={
                'deleted_replica_record_id': str(identity.replica_record_id),
                'operation': 'database-projection',
                'scope_id': str(scope_id),
            }))
    if not changed:
        latest = ordinary_launch_binding.get_latest_legacy_reconciliation(
            scope_id, identity)
        assert latest is not None
        return ReconciliationResult(ReconciliationDisposition.ALREADY_PROJECTED,
                                    provider_evidence,
                                    uuid.UUID(str(latest['event_id'])))
    latest = ordinary_launch_binding.get_latest_legacy_reconciliation(
        scope_id, identity)
    assert latest is not None
    return ReconciliationResult(ReconciliationDisposition.PROJECTED,
                                provider_evidence,
                                uuid.UUID(str(latest['event_id'])))
