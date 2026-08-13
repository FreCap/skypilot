"""Provider-free operational status for sequenced reserved fill."""

import types
import typing
from unittest import mock

from sky.serve import controller
from sky.serve import pool_capacity_observation
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.server import constants as server_constants

_RECONCILIATION_GATE_GENERATION = 29
_RECLAIM_FLEET_BUNDLE_SHA256 = 'c' * 64
_RECLAIM_POLICY_REVISION = 'kueue-reclaim-v1'
_RECLAIM_PROVIDER_INVENTORY_SHA256 = 'd' * 64
_WORKER_PROJECTION_SHA256 = 'e' * 64


def test_status_contract_has_distinct_api_capability_version() -> None:
    assert (server_constants.
            MIN_SERVE_RESERVED_FILL_RECONCILIATION_STATUS_API_VERSION == 76)
    assert server_constants.API_VERSION == 77


def _allocation(
    *,
    broker_slot_width: int = 1,
    free_slots: int = 3,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    pool_key = reserved_capacity_broker.make_pool_key(  # pylint: disable=unexpected-keyword-arg
        'east-context',
        'A100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-east')
    location = reserved_fill_planner.LocationSnapshot(
        cloud='Kubernetes',
        region='east-context',
        zone=None,
        accelerators=(('A100', broker_slot_width),),
        use_spot=False)
    snapshot = reserved_fill_planner.PoolFillSnapshot(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=pool_key,
        physical_cluster_uid='uid-east',
        service_generation=7,
        edge_cap=5,
        broker_slot_width=broker_slot_width,
        free_slots=free_slots,
        free_slots_by_accelerator=(('a100', free_slots),),
        worker_projection_sha256_by_accelerator=(('a100',
                                                  _WORKER_PROJECTION_SHA256),),
        grant=4,
        grant_epoch=23,
        observation_generation=13,
        observation_sequence=17,
        ordinary_zero_cost_admission_sequence=17,
        valid_until=10_000.0,
        locations=(location,))
    return reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=3,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))


def _controller(
    allocation: reserved_fill_planner.AuthenticatedAllocationMap,
) -> controller.SkyServeController:
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._service_name = 'svc'  # pylint: disable=protected-access
    ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
        reserved_capacity_fill=True)
    ctrl._read_sequenced_reserved_fill_allocation = mock.Mock(  # type: ignore[method-assign]  # pylint: disable=protected-access
        return_value=(True, allocation))
    return ctrl


def _replica(
        allocation: reserved_fill_planner.AuthenticatedAllocationMap,
        *,
        ready: bool,
        terminal: bool = False,
        allocation_generation: int | None = None,
        reclaim_policy_revision: str | None = None,
        service_version: int | None = None,
        worker_projection_sha256: str | None = None) -> types.SimpleNamespace:
    snapshot = allocation.pool_snapshots[0]
    return types.SimpleNamespace(
        is_terminal=terminal,
        is_ready=ready,
        reserved_fill=True,
        is_zero_cost=True,
        version=(allocation.service_version
                 if service_version is None else service_version),
        get_spot_location=lambda: types.SimpleNamespace(
            accelerators={'A100': snapshot.broker_slot_width}),
        reserved_fill_pool_key=snapshot.pool_key,
        reserved_fill_service_generation=snapshot.service_generation,
        reserved_fill_physical_cluster_uid=snapshot.physical_cluster_uid,
        reserved_fill_kubernetes_context=snapshot.locations[0].region,
        reserved_fill_allocation_generation=(allocation.allocation_generation
                                             if allocation_generation is None
                                             else allocation_generation),
        reserved_fill_allocation_input_sha256=(
            allocation.allocation_input_sha256),
        reserved_fill_allocation_claim_generation=(
            allocation.allocation_claim_generation),
        reserved_fill_reconciliation_gate_generation=(
            allocation.reconciliation_gate_generation),
        reserved_fill_reclaim_fleet_bundle_sha256=(
            allocation.reclaim_fleet_bundle_sha256),
        reserved_fill_reclaim_policy_revision=(
            allocation.reclaim_policy_revision
            if reclaim_policy_revision is None else reclaim_policy_revision),
        reserved_fill_reclaim_provider_inventory_sha256=(
            allocation.reclaim_provider_inventory_sha256),
        reserved_fill_worker_projection_sha256=(
            _WORKER_PROJECTION_SHA256 if worker_projection_sha256 is None else
            worker_projection_sha256),
        reserved_fill_observation_generation=(snapshot.observation_generation),
        reserved_fill_observation_sequence=snapshot.observation_sequence,
        reserved_fill_intent_idempotency_key='intent-key',
        zero_cost_admission_sequence=19,
    )


def test_sequenced_status_joins_exact_durable_provenance_and_progress() -> None:
    allocation = _allocation()
    ctrl = _controller(allocation)
    snapshot = allocation.pool_snapshots[0]
    payload = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        6, {'A100': 6})
    observation_repository = mock.Mock()
    observation_repository.read_exact_completed.return_value = (
        types.SimpleNamespace(observation_sequence=17, payload=payload))
    ctrl._reserved_fill_observation_repository = (  # pylint: disable=protected-access
        observation_repository)
    replicas = [
        _replica(allocation, ready=True),
        _replica(allocation, ready=False),
        _replica(allocation, ready=True, terminal=True),
        _replica(allocation, ready=True, allocation_generation=4),
        _replica(allocation,
                 ready=True,
                 reclaim_policy_revision='tampered-reclaim-policy'),
        _replica(allocation, ready=True, service_version=2),
        _replica(allocation,
                 ready=True,
                 worker_projection_sha256='f' * 64),
    ]

    with mock.patch.object(controller.serve_state,
                           'get_replica_infos',
                           return_value=replicas):
        status = ctrl._reserved_fill_reconciliation_info()  # pylint: disable=protected-access

    assert status['authority_mode'] == 'sequenced'
    assert status['allocation_current'] is True
    assert status['allocation_generation'] == 5
    assert status['allocation_input_sha256'] == (
        allocation.allocation_input_sha256)
    assert status['allocation_claim_generation'] == 11
    assert status['reconciliation_gate_generation'] == (
        _RECONCILIATION_GATE_GENERATION)
    assert status['reclaim_policy_identity'] == {
        'fleet_bundle_sha256': _RECLAIM_FLEET_BUNDLE_SHA256,
        'policy_revision': _RECLAIM_POLICY_REVISION,
        'provider_inventory_sha256': _RECLAIM_PROVIDER_INVENTORY_SHA256,
    }
    pool = status['pools'][snapshot.pool_key]
    assert pool == {
        'physical_cluster_uid': 'uid-east',
        'kubernetes_context': 'east-context',
        'service_generation': 7,
        'observation_generation': 13,
        'observation_sequence': 17,
        'observation_valid_until': 10_000.0,
        'observation_available': True,
        'observed_free_gpus': 6,
        'observed_free_gpus_by_accelerator': {
            'a100': 6
        },
        'broker_slot_width': 1,
        'observed_free_slots': 6,
        'observed_free_slots_by_accelerator': {
            'a100': 6
        },
        'spendable_slots': 3,
        'spendable_slots_by_accelerator': {
            'a100': 3
        },
        'grant': 4,
        'edge_cap': 5,
        'current_allocation_admitted_replicas': 2,
        'current_allocation_ready_replicas': 1,
    }
    observation_repository.read_exact_completed.assert_called_once_with(
        snapshot.pool_key, 13)


def test_status_reports_raw_gpus_and_width_converted_slots_separately() -> None:
    allocation = _allocation(broker_slot_width=8, free_slots=1)
    ctrl = _controller(allocation)
    snapshot = allocation.pool_snapshots[0]
    payload = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        10, {'A100': 10})
    observation_repository = mock.Mock()
    observation_repository.read_exact_completed.return_value = (
        types.SimpleNamespace(observation_sequence=17, payload=payload))
    ctrl._reserved_fill_observation_repository = (  # pylint: disable=protected-access
        observation_repository)

    with mock.patch.object(controller.serve_state,
                           'get_replica_infos',
                           return_value=[]):
        status = ctrl._reserved_fill_reconciliation_info()  # pylint: disable=protected-access

    pool = status['pools'][snapshot.pool_key]
    assert pool['broker_slot_width'] == 8
    assert pool['observed_free_gpus'] == 10
    assert pool['observed_free_gpus_by_accelerator'] == {'a100': 10}
    assert pool['observed_free_slots'] == 1
    assert pool['observed_free_slots_by_accelerator'] == {'a100': 1}


def test_legacy_status_does_not_read_observations_or_replicas() -> None:
    allocation = _allocation()
    ctrl = _controller(allocation)
    typing.cast(mock.Mock,
                ctrl._read_sequenced_reserved_fill_allocation).return_value = (  # pylint: disable=protected-access
                    False, None)
    observation_repository = mock.Mock()
    ctrl._reserved_fill_observation_repository = (  # pylint: disable=protected-access
        observation_repository)

    with mock.patch.object(controller.serve_state,
                           'get_replica_infos',
                           side_effect=AssertionError('unexpected read')):
        status = ctrl._reserved_fill_reconciliation_info()  # pylint: disable=protected-access

    assert status == {
        'enabled': True,
        'authority_mode': 'legacy',
        'allocation_current': False,
        'allocation_generation': None,
        'allocation_input_sha256': None,
        'allocation_claim_generation': None,
        'reconciliation_gate_generation': None,
        'reclaim_policy_identity': None,
        'pools': {},
    }
    observation_repository.read_exact_completed.assert_not_called()


def test_optional_diagnostic_failures_do_not_hide_sequenced_authority() -> None:
    allocation = _allocation()
    ctrl = _controller(allocation)
    observation_repository = mock.Mock()
    observation_repository.read_exact_completed.side_effect = RuntimeError(
        'database unavailable')
    ctrl._reserved_fill_observation_repository = (  # pylint: disable=protected-access
        observation_repository)

    with mock.patch.object(controller.serve_state,
                           'get_replica_infos',
                           side_effect=RuntimeError('database unavailable')):
        status = ctrl._reserved_fill_reconciliation_info()  # pylint: disable=protected-access

    pool = status['pools'][allocation.pool_snapshots[0].pool_key]
    assert status['authority_mode'] == 'sequenced'
    assert status['allocation_current'] is True
    assert pool['observation_available'] is False
    assert pool['observed_free_slots'] is None
    assert pool['current_allocation_admitted_replicas'] is None
    assert pool['current_allocation_ready_replicas'] is None


def test_authority_inspection_failure_is_explicitly_unavailable() -> None:
    allocation = _allocation()
    ctrl = _controller(allocation)
    typing.cast(mock.Mock,
                ctrl._read_sequenced_reserved_fill_allocation).side_effect = (  # pylint: disable=protected-access
                    RuntimeError('database unavailable'))

    status = ctrl._reserved_fill_reconciliation_info()  # pylint: disable=protected-access

    assert status['authority_mode'] == 'unavailable'
    assert status['allocation_current'] is False
    assert not status['pools']
