"""Reproduce #1301 against the tree, no cluster required.

    python repro_1301.py

Prints the production target byte-for-byte, then shows the gate matrix that
decides whether preempted reserved capacity is re-priced or re-bought.
"""
from sky.serve.autoscaler_compatibility import _allocate_compatibility_target
from sky.serve.autoscaler_compatibility import _revalidate_actuation_target
from sky.serve.autoscaler_compatibility import SupplyPreference

CARDS = ['L4', 'L40S', 'A100', 'A100-80GB', 'H100', 'H200', 'B200']
TOTAL = 66  # observed aggregate target

# Observed live supply: 46 H200 + 4 A100 + 2 A100-80GB reserved, 7 A100
# (4 reserved + 3 paid) and 4 L4 paid.
READY_ZERO_COST = {'H200': 46, 'A100': 4, 'A100-80GB': 2}
READY = {'H200': 46, 'A100': 7, 'A100-80GB': 2, 'L4': 4}


def allocate(ready_zero_cost, ready, use_existing_supply):
    return _allocate_compatibility_target(
        configured_cards=CARDS,
        capacities={c: 1.0 for c in CARDS},
        floors={},
        min_replicas=0,
        max_replicas=1000,
        # One card-agnostic profile: no request names a card, which is the
        # real shape of this service's demand.
        demand_profiles=[(0, tuple(CARDS), float(TOTAL))],
        fixed_work_by_accelerator={},
        ready_zero_cost=ready_zero_cost,
        committed_zero_cost=ready_zero_cost,
        free_reserved={},
        ready_paid={
            card: max(0,
                      ready.get(card, 0) -
                      ready_zero_cost.get(card, 0)) for card in CARDS
        },
        committed_paid={
            card: max(0,
                      ready.get(card, 0) -
                      ready_zero_cost.get(card, 0)) for card in CARDS
        },
        supply_preference=SupplyPreference.WARM_FIRST,
        cold_order=CARDS,  # cheapest first
        use_existing_supply=use_existing_supply)


print('1. The allocator is correct on both passes')
print('   supply-blind  :', allocate(READY_ZERO_COST, READY, False))
print('   supply-aware  :', allocate(READY_ZERO_COST, READY, True),
      '  <- matches production exactly')
print('   after the reserved A100s are preempted:')
print(
    '   supply-aware  :',
    allocate({
        'H200': 46,
        'A100-80GB': 2
    }, {
        'H200': 46,
        'A100': 3,
        'A100-80GB': 2,
        'L4': 4
    }, True), '  <- re-prices to L4 by itself')

print()
print('2. The reconciler re-prices capacity known gone from every generation')
adopted = {'L4': 11, 'A100': 7, 'A100-80GB': 2, 'H200': 46}
desired = {'L4': 15, 'A100': 3, 'A100-80GB': 2, 'H200': 46}
supply = {'L4': 4, 'A100': 3, 'A100-80GB': 2, 'H200': 46}
for reassign in (True, False):
    for unbacked in (True, False):
        target = _revalidate_actuation_target(
            adopted_target=adopted,
            desired_target=desired,
            nonretiring_supply=supply,
            configured_cards=CARDS,
            final_target=TOTAL,
            allow_adopted_reassignment=reassign,
            allow_unbacked_adopted_reassignment=unbacked,
            # Complete snapshot: no old-version row backs the lost capacity.
            old_version_supply={})
        a100 = target.get('A100', 0)
        print('   reassign=%-5s unbacked=%-5s -> A100 target=%d, buys %d paid '
              'A100' %
              (reassign, unbacked, a100, max(0, a100 - supply['A100'])))
print('   allow_adopted_reassignment is `not any(old-version replica)`, so it')
print('   is False during every rolling update. The incident sat on row 3;')
print('   complete generation provenance now re-prices that row.')

print()
print('3. Fresh compatibility ownership completes the rolling replacement')
latest_supply = {'L4': 4, 'A100': 0, 'A100-80GB': 2, 'H200': 46}
old_supply = {'L4': 0, 'A100': 3, 'A100-80GB': 0, 'H200': 0}
fresh_desired = {'L4': 18, 'A100': 0, 'A100-80GB': 2, 'H200': 46}
target = _revalidate_actuation_target(
    adopted_target=adopted,
    desired_target=fresh_desired,
    nonretiring_supply=latest_supply,
    configured_cards=CARDS,
    final_target=TOTAL,
    allow_adopted_reassignment=False,
    allow_unbacked_adopted_reassignment=True,
    allow_mixed_version_backed_reassignment=True,
    old_version_supply=old_supply)
paid_authority = {
    card: max(0, count - latest_supply.get(card, 0))
    for card, count in target.items()
    if count > latest_supply.get(card, 0)
}
print('   latest target :', target)
print('   paid authority:', paid_authority)
print('   old A100 remains only as nonpreemptive retirement protection;')
print('   after the L4 wave commits, it can drain without a paid A100 launch.')
assert target == {'L4': 18, 'A100-80GB': 2, 'H200': 46}
assert paid_authority == {'L4': 14}
