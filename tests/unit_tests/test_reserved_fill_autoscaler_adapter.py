"""Tests for the sequenced reserved-fill planning boundary."""
# pylint: disable=protected-access,unexpected-keyword-arg
import dataclasses
import time
import types
from unittest import mock

import pytest

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import spot_placer

_RECONCILIATION_GATE_GENERATION = 29
_RECLAIM_FLEET_BUNDLE_SHA256 = 'c' * 64
_RECLAIM_POLICY_REVISION = 'kueue-reclaim-v1'
_RECLAIM_PROVIDER_INVENTORY_SHA256 = 'd' * 64
_WORKER_PROJECTION_SHA256 = 'e' * 64


def _spec(*, replica_unit: str = 'physical_backend') -> types.SimpleNamespace:
    return types.SimpleNamespace(
        min_replicas=0,
        min_replicas_by_accelerator={},
        max_replicas=20,
        num_overprovision=None,
        replica_unit=replica_unit,
        reserved_capacity_fill=True,
        reserved_fill_floor_replicas=0,
        reserved_fill_weight=1.0,
        reserved_fill_utilization_gate=False,
        cost_rebalance=False,
        cost_rebalance_min_savings_fraction=0.3,
        cost_rebalance_max_parallel_replacements=1,
        cost_rebalance_stabilization_seconds=300.0,
    )


class _LogicalAccountingAutoscaler(autoscalers.Autoscaler):
    """Minimal autoscaler whose logical unit matches physical GPU width."""

    def _fill_capacity_units(self, info) -> int:
        if self.replica_unit == 'logical':
            return int(info.planned_capacity)
        return 1


def _location(context: str, card: str,
              width: int) -> reserved_fill_planner.LocationSnapshot:
    return reserved_fill_planner.LocationSnapshot.from_pickleable({
        'cloud': 'Kubernetes',
        'region': context,
        'zone': None,
        'accelerators': {
            card: width
        },
        'use_spot': False,
        'image_id': None,
        'container_image': None,
        'disk_tier': None,
        'ephemeral_storage': None,
        'instance_type': None,
    })


def _snapshot(
        context: str,
        uid: str,
        card: str,
        free_slots: int,
        *,
        width: int = 1,
        observation_sequence: int = 10,
        ordinary_admission_sequence: int | None = None,
        service_generation: int = 1) -> reserved_fill_planner.PoolFillSnapshot:
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        card,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=uid)
    if ordinary_admission_sequence is None:
        ordinary_admission_sequence = observation_sequence
    return reserved_fill_planner.PoolFillSnapshot(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=pool_key,
        physical_cluster_uid=uid,
        service_generation=service_generation,
        worker_projection_sha256_by_accelerator=((card.casefold(),
                                                  _WORKER_PROJECTION_SHA256),),
        edge_cap=free_slots,
        broker_slot_width=width,
        free_slots=free_slots,
        free_slots_by_accelerator=((card.casefold(), free_slots),),
        grant=free_slots,
        grant_epoch=7 if free_slots > 0 else None,
        observation_generation=3,
        observation_sequence=observation_sequence,
        ordinary_zero_cost_admission_sequence=ordinary_admission_sequence,
        valid_until=float(time.time() + 600),
        locations=(_location(context, card, width),),
    )


def _allocation(
    *snapshots: reserved_fill_planner.PoolFillSnapshot,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    return reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=4,
        allocation_claim_generation=2,
        service_version=1,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshots[0].ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=tuple(snapshots),
    )


def _replica(
    replica_id: int,
    *,
    card: str = 'H200',
    width: int = 1,
    version: int = 1,
    planned_capacity: int | None = None,
    location: spot_placer.Location | None = None,
    reserved_fill: bool = False,
    is_zero_cost: bool = False,
    zero_cost_admission_sequence: int | None = None,
    status: serve_state.ReplicaStatus = serve_state.ReplicaStatus.READY
) -> types.SimpleNamespace:
    info = types.SimpleNamespace(
        replica_id=replica_id,
        version=version,
        planned_capacity=(width
                          if planned_capacity is None else planned_capacity),
        status=status,
        is_terminal=status in serve_state.ReplicaStatus.terminal_statuses(),
        is_ready=status is serve_state.ReplicaStatus.READY,
        status_property=types.SimpleNamespace(
            sky_launch_status=autoscalers.common_utils.ProcessStatus.SUCCEEDED,
            sky_down_status=None,
            is_scale_down=False,
            preempted=False,
            purged=False,
            wait_for_idle_before_termination=False,
            logical_retirement_committed=False,
            logical_retirement_version=None,
            logical_retirement_controller_epoch=None,
            logical_retirement_generation=None,
            logical_retirement_target_capacity=None,
            logical_retirement_confirmed_generation=None,
            logical_retirement_bounded_deadline=False),
        resources_override={'accelerators': {
            card: width
        }},
        reserved_fill=reserved_fill,
        is_zero_cost=is_zero_cost,
        created_at=1.0,
        zero_cost_admission_sequence=zero_cost_admission_sequence,
    )
    info.get_spot_location = lambda: location
    return info


def _pool_payload(snapshot: reserved_fill_planner.PoolFillSnapshot,
                  timestamp: float) -> dict[str, object]:
    return {
        'protocol_version': snapshot.protocol_version,
        'pool_key': snapshot.pool_key,
        'physical_cluster_uid': snapshot.physical_cluster_uid,
        'service_generation': snapshot.service_generation,
        'edge_cap': snapshot.edge_cap,
        'zero_cost_location_keys': [
            location.to_pickleable() for location in snapshot.locations
        ],
        'free_slots': snapshot.free_slots,
        'free_slots_by_accelerator': dict(snapshot.free_slots_by_accelerator or
                                          ()),
        'grant': snapshot.grant,
        'shelter_grant': snapshot.grant,
        'grant_epoch': snapshot.grant_epoch,
        'timestamp': timestamp,
    }


def test_sequenced_scope_bypasses_legacy_fill_state():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    autoscaler.max_replicas = 3
    snapshot = _snapshot('east-context', 'uid-east', 'H200', 2)
    timestamp = 100.0
    payload = {snapshot.pool_key: _pool_payload(snapshot, timestamp)}
    with mock.patch.object(autoscalers.time, 'time', return_value=timestamp):
        # The established v2 damping contract needs two equal observations
        # before an increase becomes spendable.
        autoscaler.collect_reserved_capacity_pools(payload)
        autoscaler.collect_reserved_capacity_pools(payload)

        location = snapshot.locations[0].to_location()
        fill = _replica(1,
                        location=location,
                        reserved_fill=True,
                        is_zero_cost=True)
        fill.reserved_fill_pool_key = snapshot.pool_key
        fill.reserved_fill_service_generation = 1
        fill.reserved_fill_physical_cluster_uid = 'uid-east'
        ordinary_up = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP,
            {'accelerators': {
                'H200': 1
            }})
        ordinary_down = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 1)
        assert autoscaler.commit_reserved_fill_rotation_anchor(
            snapshot.pool_key)
        before_free = autoscaler._fill_pool_states[snapshot.pool_key].free_slots

        with autoscaler.sequenced_reserved_fill_planning():
            decisions = autoscaler._apply_reserved_capacity_fill(
                [fill], [ordinary_up, ordinary_down])

    # The typed path owns shelter and launch admission outside this deprecated
    # overlay.  Ordinary decisions pass through without spending or mutating
    # the process-local feed.
    assert decisions == [ordinary_up, ordinary_down]
    assert autoscaler.fill_target == 0
    assert autoscaler._fill_pool_states[
        snapshot.pool_key].free_slots == before_free
    assert autoscaler.reserved_fill_rotation_anchor() == snapshot.pool_key

    # Exiting the scope restores the legacy path unchanged.
    with mock.patch.object(autoscalers.time, 'time', return_value=timestamp):
        legacy = autoscaler._apply_reserved_capacity_fill([fill], [ordinary_up])
    assert any(
        isinstance(decision.target, dict) and
        decision.target.get(constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY)
        for decision in legacy)


def test_authenticated_allocation_derives_frozen_first_restart_shelter():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    autoscaler.max_replicas = 45
    snapshot = _snapshot('east-context', 'uid-east', 'H200', 45)
    allocation = _allocation(snapshot)
    location = snapshot.locations[0].to_location()
    replicas = []
    for replica_id in range(1, 46):
        info = _replica(replica_id,
                        location=location,
                        reserved_fill=True,
                        is_zero_cost=True)
        info.reserved_fill_pool_key = snapshot.pool_key
        info.reserved_fill_service_generation = 1
        info.reserved_fill_physical_cluster_uid = 'uid-east'
        replicas.append(info)
    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=allocation,
        holdings=autoscaler.sequenced_reserved_fill_holdings(replicas),
        service_version=1,
        max_capacity=45,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    assert shelter.target_capacity == 45
    assert shelter.target_capacity_by_accelerator == (('h200', 45),)
    assert shelter.allocation_identity == allocation.identity


def test_reversible_retirements_are_frozen_materialized_holdings():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    autoscaler.max_replicas = 45
    snapshot = _snapshot('east-context', 'uid-east', 'H200', 45)
    allocation = _allocation(snapshot)
    location = snapshot.locations[0].to_location()
    replicas = []
    for replica_id in range(1, 46):
        info = _replica(replica_id,
                        location=location,
                        reserved_fill=True,
                        is_zero_cost=True,
                        status=serve_state.ReplicaStatus.SHUTTING_DOWN)
        info.reserved_fill_pool_key = snapshot.pool_key
        info.reserved_fill_service_generation = 1
        info.reserved_fill_physical_cluster_uid = 'uid-east'
        status = info.status_property
        status.sky_down_status = autoscalers.common_utils.ProcessStatus.SCHEDULED
        status.is_scale_down = True
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'old-controller'
        status.logical_retirement_generation = 9
        status.logical_retirement_target_capacity = 0
        replicas.append(info)

    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=allocation,
        holdings=autoscaler.sequenced_reserved_fill_holdings(replicas),
        service_version=1,
        max_capacity=45,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    assert shelter.target_capacity == 45
    assert autoscaler.reserved_fill_planned_capacity(replicas) == 45


def test_missing_sequenced_authority_is_distinct_from_grant_zero():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    location = _location('east-context', 'H200', 1).to_location()
    fill = _replica(1, location=location, reserved_fill=True, is_zero_cost=True)
    zero_snapshot = _snapshot('east-context', 'uid-east', 'H200', 0)
    fill.reserved_fill_pool_key = zero_snapshot.pool_key
    fill.reserved_fill_service_generation = 1
    fill.reserved_fill_physical_cluster_uid = 'uid-east'
    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=None,
        holdings=autoscaler.sequenced_reserved_fill_holdings([fill]),
        service_version=1,
        max_capacity=20,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    grant_zero = _allocation(zero_snapshot)
    explicit_zero = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=grant_zero,
        holdings=autoscaler.sequenced_reserved_fill_holdings([fill]),
        service_version=1,
        max_capacity=20,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    assert not shelter.authority_current
    assert shelter.target_capacity == 1
    assert explicit_zero.authority_current
    assert explicit_zero.target_capacity == 0


def test_unused_grant_or_feed_is_not_a_retirement_floor():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    snapshot = dataclasses.replace(_snapshot('east-context', 'uid-east', 'H200',
                                             2),
                                   edge_cap=10,
                                   grant=10)
    allocation = _allocation(snapshot)
    location = snapshot.locations[0].to_location()
    replicas = []
    for replica_id in range(1, 4):
        info = _replica(replica_id,
                        location=location,
                        reserved_fill=True,
                        is_zero_cost=True)
        info.reserved_fill_pool_key = snapshot.pool_key
        info.reserved_fill_service_generation = 1
        info.reserved_fill_physical_cluster_uid = 'uid-east'
        replicas.append(info)

    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=allocation,
        holdings=autoscaler.sequenced_reserved_fill_holdings(replicas),
        service_version=1,
        max_capacity=20,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    assert shelter.authority_current
    assert shelter.target_capacity == 3
    assert shelter.target_capacity_by_accelerator == (('h200', 3),)


def test_paid_at_max_can_make_headroom_for_free_fill():
    allocation = _allocation(_snapshot('east-context', 'uid-east', 'H200', 1))
    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=allocation,
        holdings=(),
        service_version=1,
        max_capacity=1,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    floor = reserved_fill_planner.compose_retirement_capacity_floor(
        demand_capacity=0,
        demand_capacity_by_accelerator=(),
        accelerator_shapes=(('H200', 1),),
        shelter=shelter,
        max_capacity=1)

    assert shelter.authority_current
    assert shelter.target_capacity == 0
    assert floor is not None
    assert floor.capacity == 0
    assert reserved_fill_planner.compose_retirement_capacity_floor(
        demand_capacity=1,
        demand_capacity_by_accelerator=(),
        accelerator_shapes=(('H200', 1),),
        shelter=shelter,
        max_capacity=1) is None


def test_logical_retirement_floor_composes_incompatible_cards_and_clips():
    snapshot = _snapshot('east-context', 'uid-east', 'H200', 10)
    allocation = _allocation(snapshot)
    holdings = tuple(
        reserved_fill_planner.MaterializedFillHolding(
            replica_id=replica_id,
            service_version=1,
            capacity=1,
            pool_key=snapshot.pool_key,
            physical_cluster_uid=snapshot.physical_cluster_uid,
            service_generation=snapshot.service_generation,
            accelerator='H200',
            accelerator_count=1) for replica_id in range(1, 11))
    shelter = reserved_fill_planner.derive_sequenced_retirement_shelter(
        allocation=allocation,
        holdings=holdings,
        service_version=1,
        max_capacity=55,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)

    floor = reserved_fill_planner.compose_retirement_capacity_floor(
        demand_capacity=50,
        demand_capacity_by_accelerator=(('L4', 50),),
        accelerator_shapes=(('L4', 1), ('H200', 1)),
        shelter=shelter,
        max_capacity=55)

    assert floor is not None
    assert floor.capacity == 55
    assert floor.capacity_by_accelerator == (('l4', 50), ('h200', 5))

    overlap = reserved_fill_planner.compose_retirement_capacity_floor(
        demand_capacity=7,
        demand_capacity_by_accelerator=(('H200', 7),),
        accelerator_shapes=(('H200', 1),),
        shelter=shelter,
        max_capacity=55)
    assert overlap is not None
    assert overlap.capacity == 10
    assert overlap.capacity_by_accelerator == (('h200', 10),)


def test_physical_debits_duplicate_ambiguous_card_and_use_map_local_caps():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    east = _snapshot('east-context', 'uid-east', 'H200', 2, width=8)
    west = _snapshot('west-context', 'uid-west', 'H200', 1, width=8)
    a100 = _snapshot('a100-context', 'uid-a100', 'A100-80GB', 3, width=8)
    allocation = _allocation(east, west, a100)
    # This stale global value must not cap an authenticated-map decision.
    autoscaler.free_reserved_slots_by_accelerator = {'H200': 0}
    ambiguous = autoscalers.AutoscalerDecision(
        autoscalers.AutoscalerDecisionOperator.SCALE_UP,
        {'accelerators': {
            'h200': 8
        }})
    east_only = autoscalers.AutoscalerDecision(
        autoscalers.AutoscalerDecisionOperator.SCALE_UP, {
            'region': 'east-context',
            'accelerators': {
                'H200': 8
            }
        })

    debits = autoscaler.reserved_fill_ordinary_demand_debits(
        allocation, [], [ambiguous, east_only])

    assert debits == (
        reserved_fill_planner.OrdinaryDemandDebit(east.pool_key, 'h200', 2),
        reserved_fill_planner.OrdinaryDemandDebit(west.pool_key, 'h200', 1),
    )


def test_targetless_ordinary_demand_debits_every_possible_pool_card():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    east = _snapshot('east-context', 'uid-east', 'H200', 2, width=8)
    west = _snapshot('west-context', 'uid-west', 'A100-80GB', 3, width=8)
    allocation = _allocation(east, west)
    targetless = [
        autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP, None)
        for _ in range(2)
    ]

    debits = autoscaler.reserved_fill_ordinary_demand_debits(
        allocation, [], targetless)

    # Until the ordinary placer commits a location, either authenticated pool
    # could be its consumer.  Conservatively reserve both possibilities; each
    # debit remains bounded by that pool/card's own authenticated feed.
    assert debits == (
        reserved_fill_planner.OrdinaryDemandDebit(east.pool_key, 'h200', 2),
        reserved_fill_planner.OrdinaryDemandDebit(west.pool_key, 'a100-80gb',
                                                  2),
    )


def test_logical_target_debits_physical_slots_with_wave_and_replacement_fence():
    autoscaler = _LogicalAccountingAutoscaler('svc',
                                              _spec(replica_unit='logical'))
    h200 = _snapshot('h200-context', 'uid-h200', 'H200', 3, width=8)
    a100 = _snapshot('a100-context', 'uid-a100', 'A100', 2, width=4)
    allocation = _allocation(h200, a100)
    replicas = [
        _replica(1, card='H200', width=8, planned_capacity=8),
        _replica(2, card='A100', width=4, planned_capacity=4),
    ]
    logical_target = autoscalers.LogicalScaleTarget(
        version=1,
        reconcile_generation=9,
        target_capacity=32,
        target_capacity_by_accelerator=(('H200', 24), ('A100', 8)),
        accelerator_shapes=(('H200', 8), ('A100', 4)),
        replace_unknown_replica_ids=(2,),
        launch_budget=8,
    )
    decision = autoscalers.AutoscalerDecision(
        autoscalers.AutoscalerDecisionOperator.SCALE_UP, logical_target)

    debits = autoscaler.reserved_fill_ordinary_demand_debits(
        allocation, replicas, [decision])

    # The shared logical wave could land on either card.  Each card therefore
    # receives its own conservative maximum, expressed as physical replicas.
    assert debits == (
        reserved_fill_planner.OrdinaryDemandDebit(h200.pool_key, 'h200', 1),
        reserved_fill_planner.OrdinaryDemandDebit(a100.pool_key, 'a100', 2),
    )


def test_persisted_rows_are_not_redebit_after_exact_ordinary_fence():
    autoscaler = autoscalers.Autoscaler('svc', _spec())
    east = _snapshot('east-context',
                     'uid-east',
                     'H200',
                     2,
                     observation_sequence=11,
                     ordinary_admission_sequence=10)
    allocation = _allocation(east)
    location = east.locations[0].to_location()
    before = _replica(1,
                      location=location,
                      is_zero_cost=True,
                      zero_cost_admission_sequence=10)
    after = _replica(2,
                     location=location,
                     is_zero_cost=True,
                     zero_cost_admission_sequence=11)

    debits = autoscaler.reserved_fill_ordinary_demand_debits(
        allocation, [before, after], [])

    # Total sequence 11 can include a peer fill before this observation while
    # ordinary high-water remains 10. Persisted rows are already reflected in
    # the observed feed; only same-tick ordinary decisions are debited.
    assert not debits


def test_planned_capacity_and_receipt_driven_rotation_use_configured_unit():
    autoscaler = _LogicalAccountingAutoscaler('svc',
                                              _spec(replica_unit='logical'))
    autoscaler.target_num_replicas = 12
    replicas = [
        _replica(1, planned_capacity=8),
        _replica(2, version=0, planned_capacity=4),
        _replica(3,
                 planned_capacity=100,
                 status=serve_state.ReplicaStatus.FAILED),
    ]
    assert autoscaler.reserved_fill_planned_capacity(replicas) == 16

    snapshot = _snapshot('east-context', 'uid-east', 'H200', 1)
    autoscaler.collect_reserved_capacity_pools(
        {snapshot.pool_key: _pool_payload(snapshot, 100.0)})
    assert autoscaler.reserved_fill_rotation_anchor() is None
    assert autoscaler.commit_reserved_fill_rotation_anchor(snapshot.pool_key)
    assert autoscaler.reserved_fill_rotation_anchor() == snapshot.pool_key
    assert not autoscaler.commit_reserved_fill_rotation_anchor(
        'not-a-current-pool')
    assert autoscaler.reserved_fill_rotation_anchor() == snapshot.pool_key
    with pytest.raises(ValueError, match='nonempty'):
        autoscaler.commit_reserved_fill_rotation_anchor('')
