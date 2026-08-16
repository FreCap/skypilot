"""Tests for failure-isolated non-pool provider reconciliation."""

import types
import uuid

import pytest

from sky.serve import non_pool_launch_reconciliation as reconciliation
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker


def _context(
    kind: ordinary_launch_binding.NonPoolLaunchProfileKind,
) -> ordinary_launch_binding.BoundNonPoolLaunchContext:
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        kind,
        authorization_reference=f'test:{kind.value}',
        authorization_generation=1,
        authorization_payload={'kind': kind.value})
    return ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=3,
        replica_record_id=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=1,
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=1)


def _reserved_replica() -> types.SimpleNamespace:
    context = 'on-prem-a'
    physical_uid = 'physical-cluster-a'
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'L4',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    return types.SimpleNamespace(
        cluster_name='svc-3',
        reserved_fill=True,
        reserved_fill_pool_key=pool_key,
        reserved_fill_service_generation=7,
        reserved_fill_physical_cluster_uid=physical_uid,
        reserved_fill_kubernetes_context=context,
        location={
            'cloud': 'Kubernetes',
            'region': context,
            'accelerators': {
                'L4': 1,
            },
        },
        resources_override={
            'cloud': 'Kubernetes',
            'region': context,
            'accelerators': {
                'L4': 1,
            },
        })


@pytest.mark.parametrize(('presence', 'expected'),
                         [(reserved_capacity.PhysicalReplicaPresence.PRESENT,
                           ordinary_launch_binding.ProviderEvidence.PRESENT),
                          (reserved_capacity.PhysicalReplicaPresence.ABSENT,
                           ordinary_launch_binding.ProviderEvidence.ABSENT),
                          (reserved_capacity.PhysicalReplicaPresence.UNPROVEN,
                           ordinary_launch_binding.ProviderEvidence.UNKNOWN)])
def test_reserved_fill_provider_observation_is_closed(
        monkeypatch: pytest.MonkeyPatch,
        presence: reserved_capacity.PhysicalReplicaPresence,
        expected: ordinary_launch_binding.ProviderEvidence) -> None:
    replica = _reserved_replica()
    monkeypatch.setattr(
        reserved_capacity, 'get_kubernetes_physical_cluster_uid', lambda
        _context, force_refresh: replica.reserved_fill_physical_cluster_uid)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        lambda *_args, **_kwargs: presence)

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL),
        replica)

    assert observed.evidence == expected
    assert observed.payload['result'] == presence.value
    assert observed.payload['physical_cluster_uid'] == (
        replica.reserved_fill_physical_cluster_uid)


def test_retargeted_reserved_fill_context_is_replaced(
        monkeypatch: pytest.MonkeyPatch) -> None:
    replica = _reserved_replica()
    monkeypatch.setattr(reserved_capacity,
                        'get_kubernetes_physical_cluster_uid',
                        lambda _context, force_refresh: 'replacement-uid')
    probe = monkeypatch.setattr(
        reserved_capacity, 'probe_physical_replica_presence',
        lambda *_args, **_kwargs: pytest.fail('replacement must not be probed'))
    del probe

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL),
        replica)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.REPLACED
    assert observed.payload[
        'observed_physical_cluster_uid'] == 'replacement-uid'


def test_profile_without_durable_provider_uid_remains_unknown(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        reserved_capacity, 'get_kubernetes_physical_cluster_uid',
        lambda *_args, **_kwargs: pytest.fail('no provider UID may be guessed'))
    replica = types.SimpleNamespace(cluster_name='svc-3')

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID),
        replica)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'profile-has-no-durable-provider-uid'
