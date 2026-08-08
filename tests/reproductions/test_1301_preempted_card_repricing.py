"""Regression scenarios for #1301, promoted from strict xfails by the fix.

These were authored before the fix as `xfail(strict=True)` so they would
hard-fail the moment the reconciler's answer changed; the provenance-aware
release (old_version_supply) is that change, and this file now runs green as
ordinary regression coverage. `TestCrossCardMigrationIsStillGated` and
`TestTheDownscaleHoldIsUnchanged` protect the other side of the trade: a fix
must not drop serving capacity mid-rollout.

These scenarios pass an explicit empty `old_version_supply`, meaning the
snapshot is complete and no old-generation rows back the adopted cards.
Capacity absent from latest-version supply is therefore known gone and is
re-priced even during a rollout. The unknown-provenance fail-closed behaviour
and old-version backing are pinned in
tests/unit_tests/test_serve_autoscaler_compatibility_contract.py.

    PYTHONPATH=. pytest tests/reproductions/test_1301_preempted_card_repricing.py
"""
# pylint: disable=protected-access
from sky.serve.autoscaler_compatibility import _allocate_compatibility_target
from sky.serve.autoscaler_compatibility import _revalidate_actuation_target

# Service order, cheapest paid card first.
_CARDS = ['L4', 'L40S', 'A100', 'A100-80GB', 'H100', 'H200', 'B200']

# The production snapshot: 46 reserved H200, 4 reserved + 3 paid A100,
# 2 reserved A100-80GB, 4 paid L4.
_ADOPTED = {'L4': 11, 'A100': 7, 'A100-80GB': 2, 'H200': 46}
_TOTAL = 66


def _revalidate(*, adopted, desired, supply, final=_TOTAL, rollout, holding):
    return _revalidate_actuation_target(
        adopted_target=adopted,
        desired_target=desired,
        nonretiring_supply=supply,
        configured_cards=_CARDS,
        final_target=final,
        # False whenever an old-version replica is still draining, i.e.
        # during every rolling update.
        allow_adopted_reassignment=not rollout,
        # False during an aggregate downscale hold.
        allow_unbacked_adopted_reassignment=not holding,
        # This harness models a complete snapshot with no old-version rows.
        old_version_supply={})


class TestPreemptedReservedCapacityIsRepricing:
    """Reserved A100s reclaimed mid-rollout, gone from every generation."""

    _SUPPLY = {'L4': 4, 'A100': 3, 'A100-80GB': 2, 'H200': 46}
    _DESIRED = {'L4': 15, 'A100': 3, 'A100-80GB': 2, 'H200': 46}

    def _target(self):
        return _revalidate(adopted=_ADOPTED,
                           desired=self._DESIRED,
                           supply=self._SUPPLY,
                           rollout=True,
                           holding=False)

    def test_a_rollout_no_longer_buys_the_preempted_card(self):
        # Today 7, i.e. 4 paid A100 purchases at roughly 6.8x the L4 price
        # that the same card-agnostic requests accept.
        assert self._target()['A100'] == 3

    def test_the_freed_work_lands_on_the_cheapest_card(self):
        assert self._target()['L4'] == 15

    def test_the_total_is_conserved(self):
        assert sum(self._target().values()) == _TOTAL

    def test_partially_backed_capacity_releases_only_the_missing_part(self):
        # 5 of the 7 adopted A100 units still exist, so only 2 may be released.
        target = _revalidate(adopted=_ADOPTED,
                             desired={
                                 'L4': 13,
                                 'A100': 5,
                                 'A100-80GB': 2,
                                 'H200': 46
                             },
                             supply={
                                 'L4': 4,
                                 'A100': 5,
                                 'A100-80GB': 2,
                                 'H200': 46
                             },
                             rollout=True,
                             holding=False)
        assert target['A100'] == 5
        assert sum(target.values()) == _TOTAL

    def test_it_already_works_outside_a_rollout(self):
        # PASSES TODAY. Pins that the rollout gate is the whole difference.
        assert _revalidate(adopted=_ADOPTED,
                           desired=self._DESIRED,
                           supply=self._SUPPLY,
                           rollout=False,
                           holding=False)['A100'] == 3


class TestCrossCardMigrationIsStillGated:
    """MUST KEEP PASSING.

    Releasing capacity that no longer exists claims nothing. Moving a unit off
    a card that still has supply is a compatibility claim, and an old version
    cannot prove one, so it stays blocked during a rollout.
    """

    # Every adopted A100 unit is still backed, and L4 has spare materialized
    # supply to receive it. Backed capacity only migrates onto supply that
    # already exists, never by cold launch, so that headroom is what makes the
    # migration possible at all.
    _SUPPLY = {'L4': 20, 'A100': 7, 'A100-80GB': 2, 'H200': 46}
    _DESIRED = {'L4': 18, 'A100': 0, 'A100-80GB': 2, 'H200': 46}

    def test_a_rollout_does_not_migrate_backed_capacity(self):
        target = _revalidate(adopted=_ADOPTED,
                             desired=self._DESIRED,
                             supply=self._SUPPLY,
                             rollout=True,
                             holding=False)
        assert target['A100'] == 7
        assert target['L4'] == 11

    def test_outside_a_rollout_the_migration_is_allowed(self):
        target = _revalidate(adopted=_ADOPTED,
                             desired=self._DESIRED,
                             supply=self._SUPPLY,
                             rollout=False,
                             holding=False)
        # Zero entries are dropped from the map entirely.
        assert target.get('A100', 0) == 0
        assert target['L4'] == 18


class TestTheDownscaleHoldIsUnchanged:
    """MUST KEEP PASSING."""

    _SUPPLY = {'L4': 4, 'A100': 3, 'A100-80GB': 2, 'H200': 46}
    _DESIRED = {'L4': 15, 'A100': 3, 'A100-80GB': 2, 'H200': 46}

    def test_a_hold_still_freezes_unbacked_units(self):
        assert _revalidate(adopted=_ADOPTED,
                           desired=self._DESIRED,
                           supply=self._SUPPLY,
                           rollout=True,
                           holding=True)['A100'] == 7

    def test_a_real_downscale_disables_card_targets_entirely(self):
        # An aggregate target below the adopted sum returns an empty map, which
        # switches the actuator off the card path, so the frozen entry above
        # cannot drive a purchase.
        assert _revalidate(adopted=_ADOPTED,
                           desired={
                               'L4': 9,
                               'A100': 3,
                               'A100-80GB': 2,
                               'H200': 46
                           },
                           supply=self._SUPPLY,
                           final=60,
                           rollout=True,
                           holding=True) == {}


class TestTheAllocatorItselfIsCorrect:
    """PASSES TODAY. Localizes the defect to the reconciler.

    Included so a future reader does not "fix" the allocator instead.
    """

    _READY_ZERO_COST = {'H200': 46, 'A100': 4, 'A100-80GB': 2}
    _READY = {'H200': 46, 'A100': 7, 'A100-80GB': 2, 'L4': 4}

    @staticmethod
    def _allocate(ready_zero_cost, ready, use_existing_supply):
        return _allocate_compatibility_target(
            configured_cards=_CARDS,
            capacities={card: 1.0 for card in _CARDS},
            floors={},
            min_replicas=0,
            max_replicas=1000,
            # One card-agnostic profile: no request names a card, which is the
            # real shape of this service's demand.
            demand_profiles=[(0, tuple(_CARDS), float(_TOTAL))],
            fixed_work_by_accelerator={},
            ready_zero_cost=ready_zero_cost,
            ready=ready,
            provisioning={},
            free_reserved={},
            cold_order=_CARDS,
            use_existing_supply=use_existing_supply)

    def test_the_supply_blind_pass_buys_only_the_cheapest_card(self):
        assert self._allocate(self._READY_ZERO_COST, self._READY, False) == {
            'L4': _TOTAL
        }

    def test_the_supply_aware_pass_reproduces_production(self):
        assert self._allocate(self._READY_ZERO_COST, self._READY,
                              True) == _ADOPTED

    def test_the_supply_aware_pass_reprices_after_preemption(self):
        target = self._allocate({
            'H200': 46,
            'A100-80GB': 2
        }, {
            'H200': 46,
            'A100': 3,
            'A100-80GB': 2,
            'L4': 4
        }, True)
        assert target['A100'] == 3
        assert target['L4'] == 15


# Why the obvious fix was rejected
# --------------------------------
# Moving the unbacked-release block above the mixed-version guard without
# generation provenance turns the incident cases above green. It also breaks
# `test_logical_exact_card_rollout_keeps_uncovered_old_card`
# in tests/unit_tests/test_concurrency_autoscaler.py: 40 old-version L4
# replicas serving demand whose profile is `compatible_accelerators: ['L4']`,
# with one new-version A100 replica ready. `nonretiring_supply` counts
# latest-version replicas only, so L4 reads as unbacked and all 40 replicas
# serving L4-only demand are retired in favour of a card that demand cannot
# use. That is an outage, not a saving.
#
# "Unbacked" therefore means two different things:
#   1. the replacement has not materialized yet during a rollout (preserve)
#   2. the capacity is genuinely gone (release and re-price)
#
# The reconciler now accepts complete per-card old-version supply provenance.
# An explicit empty map proves case 2 for this harness. Live old-version rows
# prove case 1 for conservative or stale reconciliation; a fresh, complete
# compatibility allocation may still choose a different compatible card for
# their nonpreemptive replacement. If provenance is unavailable (`None`), the
# mixed-version guard fails closed and preserves the adopted card map.
