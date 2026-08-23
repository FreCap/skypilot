"""A full reserved pool must stay reclaimable by weight and by floor.

The per-pool capacity hint becomes each claimant's ``effective_cap``, and
``compute_entitlements`` clamps both the weighted share and the retained floor
by it. Reporting ``own holdings + observed free`` therefore made a full pool
self-locking: with free at zero every claimant's cap collapsed to exactly what
it already held, so the whole-pool allocation the module documents could not
move a single slot.

Measured in production on one A100 pool, across seven consecutive broker
rounds with no movement::

    grants   prod=2   test=63     free=0  sum_holdings=65
    weights  prod=100 test=0.1    prod floor_replicas=10

A 1000:1 weight ratio changed nothing and production's floor of ten was
clamped to its actual two. Deleting the incumbent's Pods by hand moved
production's grant to 57 on the very next round, which is what proved the
arbitration itself was healthy and the cap was the binding constraint.
"""
# pylint: disable=protected-access
import json
import time
from unittest import mock
import uuid

import pytest

from sky.serve import pool_capacity_observation
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_allocation as allocation
from sky.serve import reserved_capacity_broker

_POOL = 'pool'


def _spec():
    return reserved_capacity.FillPoolSpec(position=0,
                                          context='prod_research_cluster_eks',
                                          shapes=(('A100', 1),),
                                          locations=(),
                                          physical_cluster_uid='uid',
                                          pool_key=_POOL,
                                          legacy_pool_key='legacy')


def _hint(holdings, free, sum_holdings=None, now=None, launchable=True):
    now = time.time() if now is None else now
    row = {'last_observed_free': free, 'last_observed_free_ts': now}
    if sum_holdings is not None:
        row['sum_holdings'] = sum_holdings
    return reserved_capacity._pool_capacity_hint(_spec(),
                                                 holdings=holdings,
                                                 launchable=launchable,
                                                 previous_cap=0,
                                                 now=now,
                                                 round_row=row)


class TestAFullPoolStaysReclaimable:
    """The production shape: a full pool, an incumbent, and a starved peer."""

    def test_the_starved_peer_sees_the_whole_pool(self):
        # 2 held of a 65-slot pool with nothing free. The old hint was 2,
        # which is what pinned its entitlement to its own occupancy.
        assert _hint(holdings=2, free=0, sum_holdings=65) == 65

    def test_the_incumbent_sees_the_same_pool(self):
        assert _hint(holdings=63, free=0, sum_holdings=65) == 65

    def test_free_capacity_still_adds_on_top(self):
        assert _hint(holdings=2, free=8, sum_holdings=65) == 73


class TestTheHintNeverNarrows:
    """sum_holdings is one round old; it must not cost anyone ground."""

    def test_a_stale_total_below_local_holdings_is_ignored(self):
        # This claimant grew to 40 since the round was published at 10.
        assert _hint(holdings=40, free=0, sum_holdings=10) == 40

    def test_a_legacy_row_without_the_total_is_unchanged(self):
        # Pre-existing behaviour, and what the older suite pins: 2 + 219.
        assert _hint(holdings=2, free=219) == 221

    def test_a_malformed_total_falls_back_instead_of_raising(self):
        for bad in ('65', -1, True, None, 1.5):
            assert _hint(holdings=2, free=219, sum_holdings=bad) == 221

    def test_widening_is_monotonic_across_the_grid(self):
        now = time.time()
        for holdings in (0, 1, 2, 40, 63):
            for free in (0, 1, 8, 219):
                for total in (0, 1, 65, 500):
                    new = _hint(holdings, free, total, now=now)
                    old = holdings + free
                    assert new >= old, (holdings, free, total, new, old)


class TestTheOtherBranchesAreUntouched:
    """Only the fresh-observation branch changes."""

    def test_an_unlaunchable_pool_still_reports_only_its_holdings(self):
        assert _hint(holdings=2, free=219, sum_holdings=65,
                     launchable=False) == 2

    def test_a_pool_with_no_observation_still_probes_by_one(self):
        hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                     holdings=2,
                                                     launchable=True,
                                                     previous_cap=0,
                                                     now=time.time(),
                                                     round_row={})
        assert hint == 3

    def test_a_stale_observation_still_carries_the_previous_cap(self):
        now = time.time()
        stale = now - reserved_capacity.poll_interval_seconds() * 1000
        row = {
            'last_observed_free': 219,
            'last_observed_free_ts': stale,
            'sum_holdings': 65,
        }
        hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                     holdings=2,
                                                     launchable=True,
                                                     previous_cap=9,
                                                     now=now,
                                                     round_row=row)
        assert hint == 9

    def test_a_missing_round_row_is_read_from_the_store(self):
        now = time.time()
        row = {
            'last_observed_free': 4,
            'last_observed_free_ts': now,
            'sum_holdings': 65,
        }
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=row):
            hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                         holdings=2,
                                                         launchable=True,
                                                         previous_cap=0,
                                                         now=now)
        assert hint == 69


def _claims(prod_cap, test_cap):
    """The measured production standoff, parameterized by the cap under test."""
    return {
        'boltz-l4-fleet': allocation.ClaimInput(floor=10,
                                                weight=100.0,
                                                holdings_fill=2,
                                                launchable=True,
                                                effective_cap=prod_cap),
        'boltz-l4-fleet-test': allocation.ClaimInput(floor=0,
                                                     weight=0.1,
                                                     holdings_fill=63,
                                                     launchable=True,
                                                     effective_cap=test_cap),
    }


class TestTheReclaimSignalActuallyAppears:
    """The point of the change: the allocator must be able to move a slot.

    Static caps are not the deliverable. What matters is that the incumbent's
    entitlement drops BELOW its holdings, because that is the signal it scales
    down on, and that the starved peer rises ABOVE its own.
    """

    _TOTAL = 65  # observed free 0 + 65 held, the whole pool

    def test_the_old_cap_reproduces_the_deadlock(self):
        # Exactly what production published: each cap is its own occupancy.
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=2, test_cap=63))
        assert entitlements['boltz-l4-fleet'] == 2
        assert entitlements['boltz-l4-fleet-test'] == 63

    def test_the_old_cap_defeats_the_floor(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=2, test_cap=63))
        # floor_replicas=10 is not honoured: a floor that cannot be reclaimed
        # reserves nothing.
        assert entitlements['boltz-l4-fleet'] < 10

    def test_the_new_cap_restores_the_floor(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=self._TOTAL, test_cap=self._TOTAL))
        assert entitlements['boltz-l4-fleet'] >= 10

    def test_the_new_cap_tells_the_incumbent_to_release(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=self._TOTAL, test_cap=self._TOTAL))
        # Below its 63 holdings, which is the scale-down signal.
        assert entitlements['boltz-l4-fleet-test'] < 63
        # And weight finally applies: 100 against 0.1.
        assert (entitlements['boltz-l4-fleet']
                > entitlements['boltz-l4-fleet-test'])

    def test_the_pool_is_never_oversubscribed(self):
        for caps in ((2, 63), (self._TOTAL, self._TOTAL)):
            entitlements = allocation.compute_entitlements(
                self._TOTAL, _claims(*caps))
            assert sum(entitlements.values()) <= self._TOTAL

    def test_an_equal_weight_peer_does_not_strip_the_incumbent(self):
        # Reclaim must follow weight, not merely punish whoever holds slots.
        claims = {
            'a': allocation.ClaimInput(floor=0,
                                       weight=1.0,
                                       holdings_fill=63,
                                       launchable=True,
                                       effective_cap=self._TOTAL),
            'b': allocation.ClaimInput(floor=0,
                                       weight=1.0,
                                       holdings_fill=2,
                                       launchable=True,
                                       effective_cap=self._TOTAL),
        }
        entitlements = allocation.compute_entitlements(self._TOTAL, claims)
        # 65 is odd, so an exact tie is impossible; largest-remainder gives
        # 33/32. What matters is that holding 63 buys no advantage.
        assert abs(entitlements['a'] - entitlements['b']) <= 1


_MISSING = object()


def _sequenced_spec(shapes=(('h200', 1),)):
    return reserved_capacity.FillPoolSpec(position=0,
                                          context='prod_research_cluster_eks',
                                          shapes=shapes,
                                          locations=(),
                                          physical_cluster_uid='physical-uid',
                                          pool_key='sequenced-pool',
                                          legacy_pool_key='legacy-pool')


def _committed_observation(spec,
                           raw_gpus_by_accelerator,
                           *,
                           observed_at=1000.0,
                           valid_until=1100.0):
    payload = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        sum(raw_gpus_by_accelerator.values()), raw_gpus_by_accelerator)
    return pool_capacity_observation.PoolCapacityObservation(
        pool_key=spec.pool_key,
        physical_cluster_uid=spec.physical_cluster_uid,
        accelerator_names=tuple(sorted(raw_gpus_by_accelerator)),
        access_context=spec.context,
        observation_generation=7,
        lease_token=uuid.UUID('00000000-0000-0000-0000-000000000007'),
        lease_expires_at=valid_until,
        observation_sequence=19,
        ordinary_admission_sequence=13,
        materialization_sequence=17,
        payload=payload,
        payload_sha256='a' * 64,
        observed_at=observed_at,
        completed_at=observed_at,
        valid_until=valid_until,
        published_at=observed_at)


def _sequenced_round(spec,
                     observation,
                     observed_slots,
                     spendable_slots=_MISSING,
                     *,
                     sum_holdings=0):
    envelope = {
        'service': {},
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: observed_slots,
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: spec.gpus_per_replica,
    }
    if spendable_slots is not _MISSING:
        envelope[reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY] = (
            spendable_slots)
    return {
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'observation_generation': observation.observation_generation,
        'observation_sequence': observation.observation_sequence,
        'observation_materialization_sequence':
            (observation.materialization_sequence),
        'observation_payload_sha256': observation.payload_sha256,
        'feed_by_accelerator': json.dumps(envelope, sort_keys=True),
        'last_observed_free': sum(observed_slots.values()),
        'last_observed_free_ts': observation.observed_at,
        'snapshot_time': observation.observed_at,
        'sum_holdings': sum_holdings,
    }


def _repository(observation):
    repository = mock.Mock(
        spec=pool_capacity_observation.PoolCapacityObservationRepository)
    repository.read_exact_completed.return_value = observation
    return repository


def _sequenced_hint(spec,
                    row,
                    observation,
                    *,
                    holdings=0,
                    previous_cap=0,
                    now=1001.0):
    # The source change and its tests coexist in this uninstalled worktree;
    # pylint resolves the installed predecessor signature instead.
    return reserved_capacity._pool_capacity_hint(
        spec,
        holdings=holdings,
        launchable=True,
        previous_cap=previous_cap,
        now=now,
        round_row=row,
        observation_repository=_repository(  # pylint: disable=unexpected-keyword-arg
            observation))


class TestSequencedCapacityHint:
    """Sequenced discovery consumes only authenticated spendable slots."""

    def test_committed_single_card_conserves_holdings_and_spendable(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 31})
        row = _sequenced_round(spec,
                               observation, {'h200': 31}, {'h200': 2},
                               sum_holdings=48)

        assert _sequenced_hint(spec, row, observation, holdings=48) == 50

    def test_newer_local_holdings_are_not_added_to_stale_round_free(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 5})
        row = _sequenced_round(spec,
                               observation, {'h200': 5}, {'h200': 5},
                               sum_holdings=10)

        hint = _sequenced_hint(spec, row, observation, holdings=40)
        budgets = reserved_capacity.allocate_fill_pool_budgets(
            100, 0,
            (reserved_capacity.FillPoolBudgetInput(holdings=40,
                                                   capacity_hint=hint),))

        assert hint == 15
        assert budgets[0].edge_cap == 40

    def test_closed_all_zero_mapping_is_authoritative(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 5})
        row = _sequenced_round(spec,
                               observation, {'h200': 5}, {'h200': 0},
                               sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=2,
                               previous_cap=20) == 7

    def test_literal_empty_mapping_is_malformed_and_withholds(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 5})
        row = _sequenced_round(spec,
                               observation, {'h200': 5}, {},
                               sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=3,
                               previous_cap=20) == 3

    def test_missing_n_minus_one_key_carries_existing_cap(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 5})
        row = _sequenced_round(spec, observation, {'h200': 5}, sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=3,
                               previous_cap=9) == 9

    @pytest.mark.parametrize('observed_slots,spendable_slots', [
        ({
            'h200': 2
        }, {
            'a100': 1
        }),
        ({
            'h200': 2
        }, {
            'H200': 1,
            'h200': 1
        }),
        ({
            'h200': 2
        }, {
            'h200': True
        }),
        ({
            'h200': 2
        }, {
            'h200': 3
        }),
    ])
    def test_malformed_spendable_mapping_withholds(self, observed_slots,
                                                   spendable_slots):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 2})
        row = _sequenced_round(spec,
                               observation,
                               observed_slots,
                               spendable_slots,
                               sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=3,
                               previous_cap=20) == 3

    def test_pointwise_card_fabrication_with_same_total_withholds(self):
        spec = _sequenced_spec((('a100', 1), ('h200', 1)))
        observation = _committed_observation(spec, {'a100': 0, 'h200': 4})
        row = _sequenced_round(spec,
                               observation, {
                                   'a100': 0,
                                   'h200': 4
                               }, {
                                   'a100': 1,
                                   'h200': 3
                               },
                               sum_holdings=2)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=2,
                               previous_cap=20) == 2

    def test_forged_observed_sentinel_withholds(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 4})
        row = _sequenced_round(spec,
                               observation, {'h200': 5}, {'h200': 2},
                               sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=3,
                               previous_cap=20) == 3

    def test_composite_zero_free_card_is_preserved(self):
        spec = _sequenced_spec((('a100', 1), ('h200', 1)))
        observation = _committed_observation(spec, {'a100': 0, 'h200': 4})
        row = _sequenced_round(spec,
                               observation, {
                                   'a100': 0,
                                   'h200': 4
                               }, {
                                   'a100': 0,
                                   'h200': 3
                               },
                               sum_holdings=1)

        assert _sequenced_hint(spec, row, observation, holdings=1) == 4

    def test_ambiguous_composite_debit_remains_conservative(self):
        spec = _sequenced_spec((('a100', 1), ('h200', 1)))
        observation = _committed_observation(spec, {'a100': 5, 'h200': 5})
        row = _sequenced_round(spec,
                               observation, {
                                   'a100': 5,
                                   'h200': 5
                               }, {
                                   'a100': 4,
                                   'h200': 4
                               },
                               sum_holdings=1)

        hint = _sequenced_hint(spec, row, observation, holdings=1)

        assert hint == 9
        assert hint < 10  # Aggregate one-slot debit would conserve ten.

    def test_multi_gpu_width_uses_replica_slots(self):
        spec = _sequenced_spec((('h200', 2),))
        observation = _committed_observation(spec, {'h200': 9})
        row = _sequenced_round(spec,
                               observation, {'h200': 4}, {'h200': 3},
                               sum_holdings=1)

        assert _sequenced_hint(spec, row, observation, holdings=1) == 4

    def test_stale_committed_observation_carries_previous_cap(self):
        spec = _sequenced_spec()
        observation = _committed_observation(spec, {'h200': 5},
                                             observed_at=999.0,
                                             valid_until=999.5)
        row = _sequenced_round(spec,
                               observation, {'h200': 5}, {'h200': 3},
                               sum_holdings=7)

        assert _sequenced_hint(spec,
                               row,
                               observation,
                               holdings=3,
                               previous_cap=9,
                               now=1000.0) == 9

    def test_protocol_v1_keeps_the_historical_formula(self):
        spec = _sequenced_spec()
        now = time.time()
        row = {
            'protocol_version': reserved_capacity_broker.PROTOCOL_V1,
            'last_observed_free': 8,
            'last_observed_free_ts': now,
            'sum_holdings': 65,
            'feed_by_accelerator': 'legacy bytes are not parsed',
        }

        assert reserved_capacity._pool_capacity_hint(spec,
                                                     holdings=2,
                                                     launchable=True,
                                                     previous_cap=0,
                                                     now=now,
                                                     round_row=row) == 73


def test_spendable_metadata_is_not_part_of_exact_feed_epoch_identity():
    base = {
        'service': {
            'h200': 2
        },
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 5
        },
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: 1,
    }
    first = dict(base)
    first[reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY] = {
        'h200': 3
    }
    second = dict(base)
    second[reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY] = {
        'h200': 1
    }

    assert reserved_capacity_broker._service_feed_payload_for_epoch(
        json.dumps(first, sort_keys=True)) == (
            reserved_capacity_broker._service_feed_payload_for_epoch(
                json.dumps(second, sort_keys=True)))
