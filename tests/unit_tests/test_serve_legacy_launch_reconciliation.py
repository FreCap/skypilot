"""Tests for bounded historical SkyServe launch reconciliation."""

import datetime
from unittest import mock
import uuid

from sky.serve import legacy_launch_reconciliation as reconciliation
from sky.serve import ordinary_launch_binding as binding
from sky.serve import reserved_capacity

_SCOPE_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_EVENT_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_RECORD_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')
_OBSERVED_AT = datetime.datetime(2026, 8, 16, 2, tzinfo=datetime.timezone.utc)


def _identity() -> binding.LegacyLaunchIdentity:
    return binding.LegacyLaunchIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=4,
        replica_id=3,
        replica_record_id=_RECORD_ID,
        replica_version=2,
        cluster_name='svc-3',
        request_id='legacy-request-3',
        provider_context='kubernetes-context-a',
        provider_physical_resource_uid='cluster-uid-a')


def _evidence() -> binding.LegacyReconciliationEvidence:
    return binding.LegacyReconciliationEvidence(
        observed_request_status='CANCELLED',
        observed_request_execution_generation=0,
        observed_request_queue_present=False,
        observed_request_claim_present=False,
        observed_request_result_digest=None,
        observed_request_at=_OBSERVED_AT,
        observed_request_evidence={
            'request_id': 'legacy-request-3',
        },
        executor_terminated_at=_OBSERVED_AT,
        executor_termination_evidence={
            'pod_uid': 'old-api-pod-uid',
        },
        provider_evidence=binding.ProviderEvidence.NOT_QUERIED,
        provider_evidence_observed_at=None,
        provider_evidence_payload=None)


def test_unknown_provider_evidence_quarantines_only_the_exact_row() -> None:
    identity = _identity()
    with mock.patch.object(
            binding, 'get_latest_legacy_reconciliation',
            return_value=None), mock.patch.object(
                reconciliation.request_postgres,
                'read_legacy_launch_request_evidence',
                side_effect=[_evidence(), _evidence()]), mock.patch.object(
                    binding,
                    'append_legacy_reconciliation',
                    side_effect=[{
                        'event_id': uuid.uuid4()
                    }, {
                        'event_id': _EVENT_ID
                    }]) as append, mock.patch.object(
                        reserved_capacity,
                        'probe_physical_replica_presence',
                        return_value=reserved_capacity.PhysicalReplicaPresence.
                        UNPROVEN) as probe, mock.patch.object(
                            reconciliation,
                            '_database_now',
                            return_value=_OBSERVED_AT), mock.patch.object(
                                binding,
                                'authorize_and_project_legacy_replica_cleanup'
                            ) as project:
        result = reconciliation.reconcile_scoped_legacy_launch(
            _SCOPE_ID,
            identity,
            actor='reconciler',
            reason='bounded review',
            executor_terminated_at=_OBSERVED_AT,
            executor_termination_evidence={
                'pod_uid': 'old-api-pod-uid',
            })

    assert result == reconciliation.ReconciliationResult(
        reconciliation.ReconciliationDisposition.QUARANTINED,
        binding.ProviderEvidence.UNKNOWN, _EVENT_ID)
    assert append.call_count == 2
    probe.assert_called_once_with(reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context=identity.provider_context,
        physical_cluster_uid=identity.provider_physical_resource_uid),
                                  identity.cluster_name,
                                  use_cache=False)
    project.assert_not_called()


def test_exact_provider_absence_authorizes_atomic_projection() -> None:
    identity = _identity()
    projected_event = uuid.uuid4()
    with mock.patch.object(
            binding,
            'get_latest_legacy_reconciliation',
            side_effect=[
                None, {
                    'event_id': projected_event,
                    'resolution': 'PROJECTED',
                    'provider_evidence': 'ABSENT',
                }
            ]), mock.patch.object(
                reconciliation.request_postgres,
                'read_legacy_launch_request_evidence',
                side_effect=[_evidence(), _evidence()]), mock.patch.object(
                    binding,
                    'append_legacy_reconciliation',
                    return_value={'event_id': _EVENT_ID}), mock.patch.object(
                        reserved_capacity,
                        'probe_physical_replica_presence',
                        return_value=reserved_capacity.PhysicalReplicaPresence.
                        ABSENT), mock.patch.object(
                            reconciliation,
                            '_database_now',
                            return_value=_OBSERVED_AT), mock.patch.object(
                                binding,
                                'authorize_and_project_legacy_replica_cleanup',
                                return_value=True) as project:
        result = reconciliation.reconcile_scoped_legacy_launch(
            _SCOPE_ID,
            identity,
            actor='reconciler',
            reason='bounded review',
            executor_terminated_at=_OBSERVED_AT,
            executor_termination_evidence={
                'pod_uid': 'old-api-pod-uid',
            })

    assert result == reconciliation.ReconciliationResult(
        reconciliation.ReconciliationDisposition.PROJECTED,
        binding.ProviderEvidence.ABSENT, projected_event)
    project.assert_called_once()


def test_projected_identity_is_an_idempotent_no_io_result() -> None:
    identity = _identity()
    with mock.patch.object(binding,
                           'get_latest_legacy_reconciliation',
                           return_value={
                               'event_id': _EVENT_ID,
                               'resolution': 'PROJECTED',
                               'provider_evidence': 'ABSENT',
                           }):
        with mock.patch.object(reconciliation.request_postgres,
                               'read_legacy_launch_request_evidence'
                              ) as read, mock.patch.object(
                                  reserved_capacity,
                                  'probe_physical_replica_presence') as probe:
            result = reconciliation.reconcile_scoped_legacy_launch(
                _SCOPE_ID,
                identity,
                actor='reconciler',
                reason='bounded review',
                executor_terminated_at=_OBSERVED_AT,
                executor_termination_evidence={
                    'pod_uid': 'old-api-pod-uid',
                })

    assert result.disposition is (
        reconciliation.ReconciliationDisposition.ALREADY_PROJECTED)
    read.assert_not_called()
    probe.assert_not_called()
