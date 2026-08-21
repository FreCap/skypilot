"""Unit tests for the pure protocol-v2 reserved-fill planner."""

import copy
import dataclasses
from typing import Any

import pytest

from sky import clouds
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import spot_placer

_RECONCILIATION_GATE_GENERATION = 29
_RECLAIM_FLEET_BUNDLE_SHA256 = 'c' * 64
_RECLAIM_POLICY_REVISION = 'kueue-reclaim-v1'
_RECLAIM_PROVIDER_INVENTORY_SHA256 = 'd' * 64
_SERVICE_VERSION = 19


def _projection_sha256(accelerator: str) -> str:
    return (accelerator.casefold().encode('utf-8').hex() + '0' * 64)[:64]


def _location(context: str,
              accelerator: str,
              count: int = 1) -> spot_placer.Location:
    return spot_placer.Location(cloud=clouds.Kubernetes(),
                                region=context,
                                zone=None,
                                accelerators={accelerator: count},
                                use_spot=False)


def _pool_payload(
    context: str,
    physical_uid: str,
    free_by_accelerator: dict[str, int],
    *,
    location_order: tuple[str, ...] | None = None,
    accelerator_count: int = 1,
    service_generation: int = 7,
    observation_generation: int = 13,
    observation_sequence: int = 17,
    ordinary_admission_sequence: int | None = None,
) -> dict[str, Any]:
    if location_order is None:
        location_order = tuple(free_by_accelerator)
    cards = tuple(card.casefold() for card in location_order)
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        cards,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    free_slots = sum(free_by_accelerator.values())
    if ordinary_admission_sequence is None:
        ordinary_admission_sequence = observation_sequence
    return {
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': physical_uid,
        'service_generation': service_generation,
        'worker_projection_sha256_by_accelerator': {
            card: _projection_sha256(card) for card in cards
        },
        'edge_cap': free_slots,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': free_by_accelerator,
        'grant': free_slots,
        'grant_epoch': 23 if free_slots else None,
        'observation_generation': observation_generation,
        'observation_sequence': observation_sequence,
        'ordinary_zero_cost_admission_sequence': ordinary_admission_sequence,
        'valid_until': 10_000.0,
        'zero_cost_location_keys': [
            _location(context, card, accelerator_count).to_pickleable()
            for card in location_order
        ],
    }


def _snapshot(context: str, physical_uid: str, free_by_accelerator: dict[str,
                                                                         int],
              **kwargs: Any) -> reserved_fill_planner.PoolFillSnapshot:
    return reserved_fill_planner.PoolFillSnapshot.from_mapping(
        _pool_payload(context, physical_uid, free_by_accelerator, **kwargs))


def _plan(
    snapshots: tuple[reserved_fill_planner.PoolFillSnapshot, ...],
    *,
    max_replicas: int = 100,
    planned_replicas: int = 0,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL),
    committed_fill_debits: tuple[reserved_fill_planner.CommittedFillDebit,
                                 ...] = (),
    rotation_anchor: str | None = None,
    reconciliation_gate_generation: int = _RECONCILIATION_GATE_GENERATION,
    reclaim_fleet_bundle_sha256: str = _RECLAIM_FLEET_BUNDLE_SHA256,
    reclaim_policy_revision: str = _RECLAIM_POLICY_REVISION,
    reclaim_provider_inventory_sha256: str = (
        _RECLAIM_PROVIDER_INVENTORY_SHA256),
) -> reserved_fill_planner.FillPlan:
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshots[0].ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=reconciliation_gate_generation,
        reclaim_fleet_bundle_sha256=reclaim_fleet_bundle_sha256,
        reclaim_policy_revision=reclaim_policy_revision,
        reclaim_provider_inventory_sha256=reclaim_provider_inventory_sha256,
        pool_snapshots=snapshots,
    )
    return reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=2,
        reconcile_generation=3,
        allocation_map=allocation_map,
        service_incarnation='service-incarnation',
        service_version=_SERVICE_VERSION,
        controller_owner='controller-owner',
        max_replicas=max_replicas,
        planned_replicas=planned_replicas,
        capacity_unit=capacity_unit,
        committed_fill_debits=committed_fill_debits,
        rotation_anchor=rotation_anchor,
    )


def test_plan_values_are_deeply_immutable() -> None:
    snapshot = _snapshot('east-context', 'uid-east', {'a100': 1})
    plan = _plan((snapshot,))
    intent = plan.intents[0]

    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.policy_revision = 3  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        intent.accelerator = 'H200'  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.free_slots = 0  # type: ignore[misc]
    with pytest.raises(TypeError):
        intent.allowed_locations[0].accelerators[0] = (  # type: ignore[index]
            'H200', 1)

    mutable_copy = intent.allowed_location_keys()[0]
    mutable_copy['region'] = 'tampered'
    assert intent.allowed_locations[0].region == 'east-context'


def test_allocation_map_cannot_double_count_physical_pool_aliases() -> None:
    primary = _snapshot('primary-context', 'shared-uid', {'a100': 1})
    alias = _snapshot('alias-context', 'shared-uid', {'a100': 1})
    assert primary.pool_key == alias.pool_key

    with pytest.raises(ValueError, match='repeat a pool key'):
        reserved_fill_planner.AuthenticatedAllocationMap.create(
            allocation_generation=1,
            allocation_claim_generation=7,
            service_version=_SERVICE_VERSION,
            ordinary_zero_cost_admission_sequence_high_water=(
                primary.ordinary_zero_cost_admission_sequence),
            reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
            reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
            reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
            reclaim_provider_inventory_sha256=(
                _RECLAIM_PROVIDER_INVENTORY_SHA256),
            pool_snapshots=(primary, alias))


def test_allocation_map_allows_disjoint_cards_in_one_context() -> None:
    a100 = _snapshot('mixed-context', 'shared-uid', {'a100': 1})
    h200 = _snapshot('mixed-context', 'shared-uid', {'h200': 2})

    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=1,
        allocation_claim_generation=7,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            a100.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(a100, h200))
    plan = _plan((a100, h200))

    assert allocation_map.pool_snapshots == (a100, h200)
    assert [intent.accelerator for intent in plan.intents
           ] == ['a100', 'h200', 'h200']


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        (lambda payload: payload.__setitem__('protocol_version', True),
         'must be an integer'),
        (lambda payload: payload.__setitem__('pool_key', '["broken"]'),
         'malformed pool key'),
        (lambda payload: payload.__setitem__('physical_cluster_uid', 'wrong-uid'
                                            ), 'physical cluster UID'),
        (lambda payload: payload.__setitem__('free_slots', 2),
         'cannot exceed its grant'),
        (lambda payload: payload['free_slots_by_accelerator'].__setitem__(
            'a100', 0), 'must sum'),
        (lambda payload: payload.pop('observation_sequence'),
         'fields must be exact'),
    ],
)
def test_snapshot_strictly_rejects_malformed_authority(mutation,
                                                       message: str) -> None:
    payload = _pool_payload('east-context', 'uid-east', {'a100': 1})
    mutation(payload)
    with pytest.raises(ValueError, match=message):
        reserved_fill_planner.PoolFillSnapshot.from_mapping(payload)


def test_snapshot_rejects_mutable_nested_contracts() -> None:
    location = reserved_fill_planner.LocationSnapshot.from_pickleable(
        _location('east-context', 'A100').to_pickleable())
    pool_key = reserved_capacity_broker.make_pool_key(
        'east-context',
        'A100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-east')
    with pytest.raises(ValueError, match='immutable tuple'):
        reserved_fill_planner.PoolFillSnapshot(
            protocol_version=2,
            pool_key=pool_key,
            physical_cluster_uid='uid-east',
            service_generation=1,
            worker_projection_sha256_by_accelerator=((
                'a100', _projection_sha256('a100')),),
            edge_cap=1,
            free_slots=1,
            free_slots_by_accelerator=[('a100', 1)],  # type: ignore[arg-type]
            grant=1,
            grant_epoch=1,
            observation_generation=1,
            observation_sequence=1,
            ordinary_zero_cost_admission_sequence=1,
            valid_until=100.0,
            locations=(location,),
        )


def test_round_robin_intent_order_is_deterministic_and_rotatable() -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 2})
    west = _snapshot('west-context', 'uid-west', {'h100': 2})
    north = _snapshot('north-context', 'uid-north', {'h200': 1})
    snapshots = (east, west, north)

    first = _plan(snapshots)
    second = _plan(snapshots)
    assert first == second
    assert [intent.pool_key for intent in first.intents] == [
        east.pool_key,
        west.pool_key,
        north.pool_key,
        east.pool_key,
        west.pool_key,
    ]

    rotated = _plan(snapshots, rotation_anchor=east.pool_key)
    assert [intent.pool_key for intent in rotated.intents] == [
        west.pool_key,
        north.pool_key,
        east.pool_key,
        west.pool_key,
        east.pool_key,
    ]
    assert [intent.idempotency_key for intent in first.intents
           ] == [intent.idempotency_key for intent in second.intents]
    assert len({intent.idempotency_key for intent in first.intents
               }) == len(first.intents)


def test_intent_idempotency_key_covers_authority_payload() -> None:
    intent = _plan((_snapshot('east-context', 'uid-east',
                              {'a100': 1}),)).intents[0]

    with pytest.raises(ValueError, match='immutable authority payload'):
        dataclasses.replace(intent, idempotency_key='f' * 64)

    identity_mutations = {
        'reconciliation_gate_generation': intent.reconciliation_gate_generation
                                          + 1,
        'reclaim_fleet_bundle_sha256': 'e' * 64,
        'reclaim_policy_revision': 'kueue-reclaim-v2',
        'reclaim_provider_inventory_sha256': 'f' * 64,
    }
    for field, value in identity_mutations.items():
        with pytest.raises(ValueError, match='immutable authority payload'):
            dataclasses.replace(intent, **{field: value})


def test_reclaim_identity_is_bound_to_allocation_hash_and_every_intent(
) -> None:
    snapshot = _snapshot('east-context', 'uid-east', {'a100': 1})
    baseline = _plan((snapshot,))
    identity_mutations = {
        'reconciliation_gate_generation': _RECONCILIATION_GATE_GENERATION + 1,
        'reclaim_fleet_bundle_sha256': 'e' * 64,
        'reclaim_policy_revision': 'kueue-reclaim-v2',
        'reclaim_provider_inventory_sha256': 'f' * 64,
    }

    assert baseline.reconciliation_gate_generation == (
        _RECONCILIATION_GATE_GENERATION)
    assert baseline.reclaim_fleet_bundle_sha256 == (
        _RECLAIM_FLEET_BUNDLE_SHA256)
    assert baseline.reclaim_policy_revision == _RECLAIM_POLICY_REVISION
    assert baseline.reclaim_provider_inventory_sha256 == (
        _RECLAIM_PROVIDER_INVENTORY_SHA256)
    for field, value in identity_mutations.items():
        changed = _plan((snapshot,), **{field: value})
        assert changed.allocation_input_sha256 != (
            baseline.allocation_input_sha256)
        assert changed.intents[0].idempotency_key != (
            baseline.intents[0].idempotency_key)
        assert getattr(changed, field) == value
        assert getattr(changed.intents[0], field) == value

        with pytest.raises(ValueError, match='plan authority'):
            dataclasses.replace(baseline, **{field: value})


def test_plan_never_exceeds_service_global_headroom() -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 4})
    west = _snapshot('west-context', 'uid-west', {'h100': 4})

    plan = _plan((east, west), max_replicas=5, planned_replicas=3)

    assert len(plan.intents) == 2
    assert [intent.pool_key for intent in plan.intents
           ] == [east.pool_key, west.pool_key]
    assert not _plan((east,), max_replicas=2, planned_replicas=3).intents


def test_logical_headroom_charges_exact_accelerator_width() -> None:
    wide = _snapshot('wide-context',
                     'uid-wide', {'a100': 2},
                     accelerator_count=2)

    no_logical_room = _plan(
        (wide,),
        max_replicas=1,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    one_logical_slot = _plan(
        (wide,),
        max_replicas=3,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    one_physical_slot = _plan(
        (wide,),
        max_replicas=1,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    assert not no_logical_room.intents
    assert len(one_logical_slot.intents) == 1
    assert len(one_physical_slot.intents) == 1
    assert one_logical_slot.capacity_unit is (
        reserved_fill_planner.FillCapacityUnit.LOGICAL)
    assert one_logical_slot.intents[0].capacity_unit is (
        reserved_fill_planner.FillCapacityUnit.LOGICAL)


def test_committed_fill_debit_prevents_same_allocation_replay() -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 3})
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            east.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(east,))
    committed = reserved_fill_planner.CommittedFillDebit(
        allocation_generation=allocation_map.allocation_generation,
        allocation_input_sha256=allocation_map.allocation_input_sha256,
        allocation_claim_generation=(
            allocation_map.allocation_claim_generation),
        pool_key=east.pool_key,
        accelerator='A100',
        replica_slots=2)

    plan = _plan((east,), committed_fill_debits=(committed,))

    assert len(plan.intents) == 1
    assert plan.intents[0].accelerator == 'a100'


def test_committed_fill_debits_require_exact_allocation_and_unique_card(
) -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 2})
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            east.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(east,))
    committed = reserved_fill_planner.CommittedFillDebit(
        allocation_generation=allocation_map.allocation_generation,
        allocation_input_sha256=allocation_map.allocation_input_sha256,
        allocation_claim_generation=(
            allocation_map.allocation_claim_generation),
        pool_key=east.pool_key,
        accelerator='a100',
        replica_slots=1)

    with pytest.raises(ValueError, match='different authenticated'):
        _plan((east,),
              committed_fill_debits=(dataclasses.replace(
                  committed, allocation_generation=6),))
    with pytest.raises(ValueError, match='one entry per pool/card'):
        _plan((east,), committed_fill_debits=(committed, committed))


def test_composite_pool_intents_are_exact_card_shaped() -> None:
    snapshot = _snapshot('mixed-context',
                         'uid-mixed', {
                             'a100': 1,
                             'h200': 2,
                         },
                         location_order=('A100', 'H200'),
                         accelerator_count=2)

    plan = _plan((snapshot,))

    assert [(intent.accelerator, intent.accelerator_count)
            for intent in plan.intents] == [('A100', 2), ('H200', 2),
                                            ('H200', 2)]
    for intent in plan.intents:
        assert intent.allowed_locations
        assert {(location.accelerator, location.accelerator_count)
                for location in intent.allowed_locations
               } == {(intent.accelerator, intent.accelerator_count)}


def test_empty_composite_pool_without_exact_feed_is_non_actionable() -> None:
    payload = _pool_payload('mixed-context',
                            'uid-mixed', {
                                'a100': 0,
                                'h200': 0,
                            },
                            location_order=('A100', 'H200'))
    payload['free_slots_by_accelerator'] = None
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping(payload)

    assert not _plan((snapshot,)).intents


def test_planning_does_not_mutate_feed_or_rotation_anchor() -> None:
    raw = _pool_payload('east-context', 'uid-east', {'a100': 3})
    raw_before = copy.deepcopy(raw)
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping(raw)
    snapshot_before = dataclasses.asdict(snapshot)
    anchor = snapshot.pool_key

    first = _plan((snapshot,), rotation_anchor=anchor)
    second = _plan((snapshot,), rotation_anchor=anchor)

    assert first == second
    assert raw == raw_before
    assert dataclasses.asdict(snapshot) == snapshot_before
    assert snapshot.free_slots == 3
    assert snapshot.free_slots_by_accelerator == (('a100', 3),)
    assert anchor == snapshot.pool_key


def test_allocation_map_hash_binds_complete_ordered_authority() -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 1})
    west = _snapshot('west-context', 'uid-west', {'h100': 1})
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            east.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(east, west))

    assert (reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(
        allocation_map.to_mapping()) == allocation_map)
    assert allocation_map.to_mapping()['schema_version'] == 5

    inflated_east = dataclasses.replace(east,
                                        edge_cap=2,
                                        free_slots=2,
                                        free_slots_by_accelerator=(('a100',
                                                                    2),),
                                        grant=2)
    with pytest.raises(ValueError, match='does not match'):
        dataclasses.replace(allocation_map,
                            pool_snapshots=(inflated_east, west))
    with pytest.raises(ValueError, match='does not match'):
        dataclasses.replace(allocation_map, allocation_claim_generation=12)
    for field, value in {
            'service_version': _SERVICE_VERSION + 1,
            'reconciliation_gate_generation': _RECONCILIATION_GATE_GENERATION +
                                              1,
            'reclaim_fleet_bundle_sha256': 'e' * 64,
            'reclaim_policy_revision': 'kueue-reclaim-v2',
            'reclaim_provider_inventory_sha256': 'f' * 64,
    }.items():
        with pytest.raises(ValueError, match='does not match'):
            dataclasses.replace(allocation_map, **{field: value})
    with pytest.raises(ValueError, match='does not match'):
        dataclasses.replace(allocation_map, pool_snapshots=(west, east))


def test_allocation_map_parser_rejects_unknown_and_tampered_fields() -> None:
    snapshot = _snapshot('east-context', 'uid-east', {'a100': 1})
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))

    unknown = allocation_map.to_mapping()
    unknown['untrusted_hint'] = True
    with pytest.raises(ValueError, match='fields must be exact'):
        reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(unknown)

    missing_schema = allocation_map.to_mapping()
    missing_schema.pop('schema_version')
    with pytest.raises(ValueError, match='fields must be exact'):
        reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(
            missing_schema)

    unsupported_schema = allocation_map.to_mapping()
    unsupported_schema['schema_version'] = 2
    with pytest.raises(ValueError, match='schema version is unsupported'):
        reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(
            unsupported_schema)

    tampered = allocation_map.to_mapping()
    tampered['pool_snapshots'][0]['observation_sequence'] += 1
    with pytest.raises(ValueError, match='does not match'):
        reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(tampered)


@pytest.mark.parametrize('field', [
    'reconciliation_gate_generation',
    'reclaim_fleet_bundle_sha256',
    'reclaim_policy_revision',
    'reclaim_provider_inventory_sha256',
])
def test_allocation_map_parser_rejects_partial_reclaim_identity(field) -> None:
    snapshot = _snapshot('east-context', 'uid-east', {'a100': 1})
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=_SERVICE_VERSION,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))
    payload = allocation_map.to_mapping()
    payload.pop(field)

    with pytest.raises(ValueError, match='fields must be exact'):
        reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(payload)


def test_commit_result_validates_complete_plan_accounting() -> None:
    plan = _plan((_snapshot('east-context', 'uid-east', {'a100': 3}),))
    deferred = reserved_fill_planner.DeferredFillIntent(
        plan.intents[1],
        reserved_fill_planner.DeferredFillReason.PROVIDER_QUEUE_BACKPRESSURE)
    # The database may return rows in a different order.  The typed keys, not
    # positional zip order, recover the correct plan/replica relationship.
    result = reserved_fill_planner.FillCommitResult(accepted=(
        reserved_fill_planner.AcceptedFillIntent(
            plan.intents[2].idempotency_key, 102),
        reserved_fill_planner.AcceptedFillIntent(
            plan.intents[0].idempotency_key, 101),
    ),
                                                    deferred=(deferred,),
                                                    authority_current=True)

    result.validate_for_plan(plan)
    assert result.accepted_intents_for_plan(plan) == (
        (plan.intents[0], 101),
        (plan.intents[2], 102),
    )
    assert result.accepted_rotation_anchor(plan) == plan.intents[0].pool_key

    incomplete = dataclasses.replace(result, accepted=result.accepted[:1])
    with pytest.raises(ValueError, match='account for every'):
        incomplete.validate_for_plan(plan)
    duplicated_deferred = dataclasses.replace(result,
                                              deferred=(deferred, deferred))
    with pytest.raises(ValueError, match='more than once'):
        duplicated_deferred.validate_for_plan(plan)

    other_plan = _plan((_snapshot('other-context', 'uid-other', {'h100': 3}),))
    foreign = dataclasses.replace(
        result,
        deferred=(reserved_fill_planner.DeferredFillIntent(
            other_plan.intents[1],
            reserved_fill_planner.DeferredFillReason.CHANGED_EPOCH),))
    with pytest.raises(ValueError, match='different plan'):
        foreign.validate_for_plan(plan)


def test_rotation_anchor_advances_to_first_durably_accepted_intent() -> None:
    east = _snapshot('east-context', 'uid-east', {'a100': 1})
    west = _snapshot('west-context', 'uid-west', {'h100': 1})
    plan = _plan((east, west))
    receipt = reserved_fill_planner.FillCommitResult(
        accepted=(reserved_fill_planner.AcceptedFillIntent(
            plan.intents[1].idempotency_key, 202),),
        deferred=(reserved_fill_planner.DeferredFillIntent(
            plan.intents[0],
            reserved_fill_planner.DeferredFillReason.STALE_OBSERVATION),),
        authority_current=True)

    assert receipt.accepted_rotation_anchor(plan) == west.pool_key


def test_commit_result_rejects_malformed_receipts() -> None:
    key = 'a' * 64
    with pytest.raises(ValueError, match='unique'):
        reserved_fill_planner.FillCommitResult(
            accepted=(reserved_fill_planner.AcceptedFillIntent(key, 1),
                      reserved_fill_planner.AcceptedFillIntent('b' * 64, 1)),
            deferred=(),
            authority_current=True)
    with pytest.raises(ValueError, match='boolean'):
        reserved_fill_planner.FillCommitResult(
            accepted=(), deferred=(),
            authority_current=1)  # type: ignore[arg-type]
    plan = _plan((_snapshot('east-context', 'uid-east', {'a100': 1}),))
    with pytest.raises(ValueError, match='typed reason'):
        reserved_fill_planner.DeferredFillIntent(
            plan.intents[0], 'busy')  # type: ignore[arg-type]

    foreign = reserved_fill_planner.FillCommitResult(
        accepted=(reserved_fill_planner.AcceptedFillIntent('f' * 64, 999999),),
        deferred=(),
        authority_current=True)
    with pytest.raises(ValueError, match='different plan'):
        foreign.validate_for_plan(plan)

    both = reserved_fill_planner.FillCommitResult(
        accepted=(reserved_fill_planner.AcceptedFillIntent(
            plan.intents[0].idempotency_key, 1),),
        deferred=(reserved_fill_planner.DeferredFillIntent(
            plan.intents[0],
            reserved_fill_planner.DeferredFillReason.CHANGED_EPOCH),),
        authority_current=True)
    with pytest.raises(ValueError, match='both accept and defer'):
        both.validate_for_plan(plan)

    with pytest.raises(ValueError, match='idempotency keys must be unique'):
        reserved_fill_planner.FillCommitResult(accepted=(
            reserved_fill_planner.AcceptedFillIntent(
                plan.intents[0].idempotency_key, 1),
            reserved_fill_planner.AcceptedFillIntent(
                plan.intents[0].idempotency_key, 2),
        ),
                                               deferred=(),
                                               authority_current=True)


@pytest.mark.parametrize(
    ('mutation', 'message'),
    [
        (lambda payload: payload.__setitem__('protocol_version', 2.0),
         'must be an integer'),
        (lambda payload: payload.__setitem__('valid_until', 10_000),
         'finite positive float'),
        (lambda payload: payload.__setitem__('unknown_authority', 1),
         'fields must be exact'),
        (lambda payload: payload['zero_cost_location_keys'][0].__setitem__(
            'use_spot', True), 'zero-cost'),
        (lambda payload: payload['zero_cost_location_keys'][0].__setitem__(
            'unknown_location_field', 1), 'fields must be exact'),
    ],
)
def test_authority_parser_rejects_noncanonical_or_non_zero_cost_input(
        mutation, message: str) -> None:
    payload = _pool_payload('east-context', 'uid-east', {'a100': 1})
    mutation(payload)
    with pytest.raises(ValueError, match=message):
        reserved_fill_planner.PoolFillSnapshot.from_mapping(payload)
