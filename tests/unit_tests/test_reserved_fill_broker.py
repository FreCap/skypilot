"""Unit tests for the reserved-fill broker (multi-service arbitration).

Pure allocation math (entitlements / feeds / damping) plus round mechanics
against a throwaway sqlite serve DB. The single-claimant fast path must be
a behavioral no-op versus #108 (grant None, feed = raw measured free); the
#108 overlay suite itself (test_reserved_capacity_fill.py) pins the
downstream identity.
"""
# pylint: disable=protected-access,invalid-name
import contextlib
import json
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.sql import dml

from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_state

_POOL = broker.make_pool_key('research-ctx', 'A100')


def _claim(floor=0,
           weight=1.0,
           holdings_fill=0,
           launchable=True,
           effective_cap=None):
    return broker.ClaimInput(floor=floor,
                             weight=weight,
                             holdings_fill=holdings_fill,
                             launchable=launchable,
                             effective_cap=effective_cap)


# =========================== Pure allocation math ===========================


class TestWaterFill:

    def test_weighted_split(self):
        assert broker.water_fill(40, {'a': 1, 'b': 3}, {}) == {'a': 10, 'b': 30}

    def test_cap_redistributes_to_uncapped(self):
        # Lending: a's headroom caps its share; the rest flows to b instead
        # of evaporating (work conservation).
        assert broker.water_fill(20, {
            'a': 1,
            'b': 1
        }, {
            'a': 5,
            'b': None
        }) == {
            'a': 5,
            'b': 15
        }

    def test_integer_conservation_and_deterministic_ties(self):
        result = broker.water_fill(5, {'a': 1, 'b': 1}, {})
        assert sum(result.values()) == 5
        # Equal fractional remainders break by name: 'a' gets the odd unit.
        assert result == {'a': 3, 'b': 2}

    def test_all_capped_leaves_remainder_unassigned(self):
        result = broker.water_fill(10, {'a': 1, 'b': 1}, {'a': 2, 'b': 3})
        assert result == {'a': 2, 'b': 3}


class TestEntitlements:

    def test_floors_first_then_weighted_remainder(self):
        claims = {'a': _claim(floor=10), 'b': _claim()}
        assert broker.compute_entitlements(20, claims) == {'a': 15, 'b': 5}

    def test_floors_over_capacity_scaled_proportionally(self):
        claims = {'a': _claim(floor=10), 'b': _claim(floor=30)}
        assert broker.compute_entitlements(20, claims) == {'a': 5, 'b': 15}

    def test_sum_never_exceeds_total(self):
        claims = {
            'a': _claim(floor=3, weight=2.5),
            'b': _claim(floor=0, weight=1.0),
            'c': _claim(floor=7, weight=0.5),
        }
        for total in range(0, 30):
            grants = broker.compute_entitlements(total, claims)
            assert sum(grants.values()) <= total

    def test_unattainable_floor_clamped_excess_flows_to_peer(self):
        # a's floor of 5 exceeds what it can materialize (effective_cap
        # 2): the floor is clamped and the freed 3 slots flow to the
        # peer instead of sitting as a permanent phantom entitlement.
        claims = {
            'a': _claim(floor=5, effective_cap=2),
            'b': _claim(),
        }
        assert broker.compute_entitlements(5, claims) == {'a': 2, 'b': 3}

    def test_entitlement_never_exceeds_effective_cap(self):
        claims = {
            'a': _claim(weight=100, effective_cap=3),
            'b': _claim(),
        }
        grants = broker.compute_entitlements(10, claims)
        assert grants['a'] == 3
        assert grants['b'] == 7


class TestFeeds:

    def test_bounded_by_free_and_need(self):
        grants = {'a': 5, 'b': 5}
        claims = {
            'a': _claim(holdings_fill=5),
            'b': _claim(holdings_fill=2),
        }
        feeds, _ = broker.compute_feeds(10, grants, claims, {}, 0.0, 100.0)
        assert feeds == {'a': 0, 'b': 3}

    def test_benched_claimant_share_redistributed(self):
        grants = {'a': 3, 'b': 3}
        claims = {'a': _claim(launchable=False), 'b': _claim()}
        feeds, _ = broker.compute_feeds(3, grants, claims, {}, 0.0, 100.0)
        assert feeds == {'a': 0, 'b': 3}

    def test_sum_feeds_never_exceeds_free(self):
        grants = {'a': 10, 'b': 10}
        claims = {'a': _claim(), 'b': _claim()}
        feeds, _ = broker.compute_feeds(7, grants, claims, {}, 0.0, 100.0)
        assert sum(feeds.values()) <= 7

    def test_feed_need_clamped_by_effective_cap(self):
        # A grant kept above the claimant's real capacity (e.g. damping
        # holding a stale level) must not translate into feed the
        # claimant can never launch; the excess goes to the peer.
        grants = {'a': 5, 'b': 5}
        claims = {'a': _claim(effective_cap=2), 'b': _claim()}
        feeds, _ = broker.compute_feeds(10, grants, claims, {}, 0.0, 100.0)
        assert feeds == {'a': 2, 'b': 5}

    def test_feed_need_clamped_by_raw_grant_during_down_damping(self):
        # a is inside a down-move's damping window: its published (damped)
        # grant is still 5 but this round's raw entitlement is 1. Feeding
        # the gap would launch a replica the grant is about to catch down
        # to and cull -- the need must clamp to min(damped, raw).
        grants = {'a': 5, 'b': 2}
        raw = {'a': 1, 'b': 2}
        claims = {'a': _claim(), 'b': _claim()}
        feeds, _ = broker.compute_feeds(5,
                                        grants,
                                        claims, {},
                                        0.0,
                                        100.0,
                                        raw_grants=raw)
        assert feeds == {'a': 1, 'b': 2}

    def test_sticky_feed_survives_weight_shift_within_window(self):
        # A single free GPU must stay with its assignee long enough for the
        # local two-poll damping to act, even when fairness would now point
        # at the peer.
        claims_round1 = {'a': _claim(weight=1), 'b': _claim(weight=1)}
        feeds1, sticky = broker.compute_feeds(1, {
            'a': 5,
            'b': 5
        }, claims_round1, {}, 0.0, 120.0)
        assert feeds1 == {'a': 1, 'b': 0}
        claims_round2 = {'a': _claim(weight=1), 'b': _claim(weight=100)}
        feeds2, sticky2 = broker.compute_feeds(1, {
            'a': 5,
            'b': 5
        }, claims_round2, sticky, 60.0, 120.0)
        assert feeds2 == {'a': 1, 'b': 0}
        # Streak start preserved: the window measures the original grant.
        assert sticky2['a']['since'] == 0.0
        # Past the window the assignment is up for grabs again.
        feeds3, _ = broker.compute_feeds(1, {
            'a': 5,
            'b': 5
        }, claims_round2, sticky2, 121.0, 120.0)
        assert feeds3 == {'a': 0, 'b': 1}


class TestGrantDamping:

    def test_up_move_needs_two_rounds_acting_on_min(self):
        # First proposal above the published level: hold.
        assert broker.damp_grants({'a': 8}, {'a': 5}, {'a': 5},
                                  holdings_shrank=False) == {
                                      'a': 5
                                  }
        # Second consecutive up proposal: act on the persisted level.
        assert broker.damp_grants({'a': 9}, {'a': 5}, {'a': 8},
                                  holdings_shrank=False) == {
                                      'a': 8
                                  }

    def test_observed_free_down_needs_two_rounds_acting_on_max(self):
        assert broker.damp_grants({'a': 2}, {'a': 5}, {'a': 5},
                                  holdings_shrank=False) == {
                                      'a': 5
                                  }
        assert broker.damp_grants({'a': 1}, {'a': 5}, {'a': 2},
                                  holdings_shrank=False) == {
                                      'a': 2
                                  }

    def test_holdings_driven_down_is_immediate(self):
        assert broker.damp_grants({'a': 2}, {'a': 5}, {'a': 5},
                                  holdings_shrank=True) == {
                                      'a': 2
                                  }

    def test_no_baseline_applies_raw_immediately(self):
        assert broker.damp_grants({'a': 7}, None, None,
                                  holdings_shrank=False) == {
                                      'a': 7
                                  }
        # A service whose previous grant was the fast-path None has no
        # integer baseline either.
        assert broker.damp_grants({'a': 7}, {}, {}, holdings_shrank=False) == {
            'a': 7
        }


# ============================= Round mechanics ==============================


class _Clock:

    def __init__(self, start: float = 1000.0):
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = _Clock()
    monkeypatch.setattr(broker.time, 'time', fake.time)
    return fake


@pytest.fixture
def _broker_db(tmp_path, monkeypatch, clock):  # pylint: disable=unused-argument
    """Fresh sqlite serve DB + no-op round lock + empty in-process caches."""
    engine = create_engine(f'sqlite:///{tmp_path}/serve_state.db')
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(broker.locks, 'get_lock',
                        lambda *args, **kwargs: contextlib.nullcontext())
    # Round debits read replica rows; default to none (tests that exercise
    # the debit override this).
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        mock.Mock(return_value=[]))
    broker.clear_caches()
    yield engine
    broker.clear_caches()


def _upsert(name,
            pool_key=_POOL,
            weight=1.0,
            floor=0,
            gpus_per_replica=1,
            holdings_fill=0,
            launchable=True,
            effective_cap=None):
    broker.upsert_claim(name,
                        pool_key=pool_key,
                        weight=weight,
                        floor_replicas=floor,
                        gpus_per_replica=gpus_per_replica,
                        holdings_fill=holdings_fill,
                        launchable=launchable,
                        effective_cap=effective_cap)


def _obs(free, gpu_names=('A100',)):
    return broker.PoolObservation(free_slots=free, gpu_names=tuple(gpu_names))


def _run(name, free=0, interval=60.0, observation=None, pool=_POOL):
    obs = _obs(free) if observation is None else observation
    return broker.run_round_if_stale(name, pool, lambda: obs, interval)


def _replica_stub(is_ready=True,
                  is_terminal=False,
                  created_at=None,
                  region='research-ctx',
                  gpu='A100',
                  reserved_fill=False):
    info = mock.Mock()
    info.is_ready = is_ready
    info.is_terminal = is_terminal
    info.created_at = created_at
    info.reserved_fill = reserved_fill
    info.location = {
        'cloud': 'Kubernetes',
        'region': region,
        'zone': None,
        'accelerators': {
            gpu: 1
        },
    }
    return info


@pytest.mark.usefixtures('_broker_db')
class TestSingleClaimantFastPath:
    """Exactly one live claim: #108 identity, no ceiling, raw feed."""

    def test_grant_none_feed_raw_free(self, monkeypatch):
        # Even with an occupying row present the fast path must NOT debit:
        # the local overlay already subtracts its own occupying rows, and a
        # broker-side debit would double-subtract (a behavioral delta from
        # #108).
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(return_value=[_replica_stub(is_ready=False)]))
        _upsert('svc-a', holdings_fill=2)
        alloc = _run('svc-a', free=7)
        assert alloc is not None
        assert alloc.grant is None
        assert alloc.feed == 7

    def test_failed_query_reads_zero_free(self):
        _upsert('svc-a')
        alloc = _run('svc-a', observation=_obs(None, gpu_names=()))
        assert alloc is not None
        assert alloc.grant is None
        assert alloc.feed == 0


@pytest.mark.usefixtures('_broker_db')
class TestMultiClaimantRounds:

    def test_weighted_split(self):
        _upsert('svc-a', weight=1)
        _upsert('svc-b', weight=3)
        alloc_a = _run('svc-a', free=40)
        assert alloc_a is not None and alloc_a.grant == 10
        assert alloc_a.feed == 10
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 30

    def test_headroom_lending(self):
        # svc-a's headroom (derived from its effective_cap) caps its share
        # at 5; the rest lends to the peer instead of evaporating.
        _upsert('svc-a', effective_cap=5)
        _upsert('svc-b')
        alloc_a = _run('svc-a', free=20)
        assert alloc_a is not None and alloc_a.grant == 5
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 15

    def test_new_claimant_reclaims_from_borrower(self, clock):
        # svc-a borrowed the whole pool while alone (grant None); svc-b
        # arrives with a floor. The next round grants svc-a LESS than it
        # holds -- the ceiling then strips its surplus's shelter and the
        # normal graceful scale-down returns the machines (autoscaler-side
        # behavior pinned in test_reserved_capacity_fill.py).
        _upsert('svc-a', holdings_fill=10)
        alone = _run('svc-a', free=0)
        assert alone is not None and alone.grant is None
        clock.advance(61)
        _upsert('svc-a', holdings_fill=10)
        _upsert('svc-b', floor=5)
        alloc_a = _run('svc-a', free=0)
        assert alloc_a is not None
        assert alloc_a.grant is not None and alloc_a.grant < 10
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant is not None and alloc_b.grant >= 5
        # No free capacity yet: nobody gets a feed out of thin air.
        assert alloc_a.feed == 0 and alloc_b.feed == 0

    def test_unattainable_floor_grant_and_feed_clamped(self):
        # svc-a claims floor 5 but can only materialize 2 (demand pressure
        # against max_replicas): both its grant and its feed clamp to 2,
        # and the freed 3 slots flow to the peer -- no permanent phantom
        # need absorbing feed it never launches.
        _upsert('svc-a', floor=5, effective_cap=2)
        _upsert('svc-b')
        alloc_a = _run('svc-a', free=5)
        assert alloc_a is not None
        assert alloc_a.grant == 2
        assert alloc_a.feed == 2
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 3
        assert alloc_b.feed == 3

    def test_fresh_round_is_read_not_redriven(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        first = _run('svc-a', free=10)
        assert first is not None
        query = mock.Mock(side_effect=AssertionError('must not re-query'))
        clock.advance(10)  # well inside the freshness window
        again = broker.run_round_if_stale('svc-b', _POOL, query, 60.0)
        assert again is not None
        assert again.round_id == first.round_id
        assert again.epoch == first.epoch

    def test_claimant_after_round_waits_for_next(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        assert _run('svc-a', free=10) is not None
        clock.advance(10)
        _upsert('svc-c')
        # Round is fresh and predates svc-c's claim: no allocation yet.
        assert _run('svc-c', free=10) is None
        clock.advance(60)
        _upsert('svc-c')
        assert _run('svc-c', free=10) is not None


@pytest.mark.usefixtures('_broker_db')
class TestBlackout:

    def test_blackout_releases_nothing_and_feeds_nothing(self, clock):
        _upsert('svc-a', holdings_fill=5)
        _upsert('svc-b', holdings_fill=5)
        good = _run('svc-a', free=10)
        assert good is not None and good.grant == 10
        clock.advance(61)
        _upsert('svc-a', holdings_fill=5)
        _upsert('svc-b', holdings_fill=5)
        blind = _run('svc-a', observation=_obs(None, gpu_names=()))
        assert blind is not None
        assert blind.grant == 10  # last-known free carried, no release
        assert blind.feed == 0  # never launch blind
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.feed == 0

    def test_persistent_blackout_never_grants_below_holdings(self, clock):
        _upsert('svc-a', holdings_fill=8)
        _upsert('svc-b', holdings_fill=0)
        assert _run('svc-a', free=10) is not None
        # Blackout long past the staleness window: decayed free is 0, but
        # the holdings floor still prevents a release while blind.
        for _ in range(6):
            clock.advance(61)
            _upsert('svc-a', holdings_fill=8)
            _upsert('svc-b', holdings_fill=0)
            blind = _run('svc-a', observation=_obs(None, gpu_names=()))
            assert blind is not None
            assert blind.grant is not None and blind.grant >= 8


@pytest.mark.usefixtures('_broker_db')
class TestClaimLifecycle:

    def test_expired_claim_pruned_fast_respawn_readopts(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        assert _run('svc-a', free=10) is not None
        # svc-b's controller dies: its heartbeat goes stale while svc-a
        # keeps re-claiming.
        ttl = broker.claim_ttl_seconds()
        clock.advance(ttl + 61)
        _upsert('svc-a')
        alone = _run('svc-a', free=10)
        assert alone is not None
        assert alone.grant is None  # back to the single-claimant fast path
        live = {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims(pool_key=_POOL)
        }
        assert live == {'svc-a'}
        # Fast respawn: re-upserting the claim re-adopts it (same PK row).
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        rejoined = _run('svc-b', free=10)
        assert rejoined is not None and rejoined.grant is not None

    def test_phantom_gate_needs_consecutive_observations(self, clock):
        # kubernetes_catalog returns empty dicts WITHOUT raising on
        # credential/cache failures, so a single phantom reading can be a
        # transient kube-apiserver blip: suspect rounds must feed 0 and
        # keep every claim; only the Nth consecutive one rejects.
        _upsert('svc-a')
        _upsert('svc-b')
        phantom = _obs(0, gpu_names=())
        for _ in range(2):
            suspect = _run('svc-a', observation=phantom)
            assert suspect is not None
            assert suspect.feed == 0
            assert len(
                serve_state.get_reserved_fill_claims(pool_key=_POOL)) == 2
            clock.advance(61)
            _upsert('svc-a')
            _upsert('svc-b')
        # Third consecutive phantom observation: claims rejected.
        assert _run('svc-a', observation=phantom) is None
        assert not serve_state.get_reserved_fill_claims(pool_key=_POOL)

    def test_healthy_observation_resets_phantom_streak(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        phantom = _obs(0, gpu_names=())
        for _ in range(2):
            assert _run('svc-a', observation=phantom) is not None
            clock.advance(61)
            _upsert('svc-a')
            _upsert('svc-b')
        # A healthy observation interleaves: the streak resets, so two
        # MORE phantom rounds still do not reject...
        assert _run('svc-a', free=4) is not None
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a')
            _upsert('svc-b')
            assert _run('svc-a', observation=phantom) is not None
            assert len(
                serve_state.get_reserved_fill_claims(pool_key=_POOL)) == 2
        # ...and the third consecutive one does.
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        assert _run('svc-a', observation=phantom) is None
        assert not serve_state.get_reserved_fill_claims(pool_key=_POOL)

    def test_service_teardown_deletes_claim_row(self):
        # Service-level teardown must not leave a live claim absorbing
        # entitlement until the TTL expires.
        _upsert('svc-a')
        _upsert('svc-b')
        serve_state.remove_service_completely('svc-a')
        live = {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims(pool_key=_POOL)
        }
        assert live == {'svc-b'}

    def test_prune_race_spares_freshly_refreshed_heartbeat(self, monkeypatch):
        # A heartbeat upsert landing between the prune's candidate SELECT
        # and its DELETE must survive: the DELETE carries the staleness
        # predicate itself (the old select-then-delete-BY-NAME pair
        # deleted the freshly refreshed claim), and the report only names
        # rows actually deleted.
        _upsert('svc-a')
        _upsert('svc-b')
        real_execute = orm.Session.execute
        raced = {'done': False}
        claims_table = serve_state.reserved_fill_claims_table

        def racing_execute(session, statement, *args, **kwargs):
            if isinstance(statement, dml.Delete) and not raced['done']:
                raced['done'] = True
                # svc-b's poller refreshes its claim between the candidate
                # select and the delete.
                real_execute(
                    session,
                    sqlalchemy.update(claims_table).where(
                        claims_table.c.service_name == 'svc-b').values(
                            heartbeat_ts=broker.time.time() + 10_000.0))
            return real_execute(session, statement, *args, **kwargs)

        monkeypatch.setattr(orm.Session, 'execute', racing_execute)
        pruned = serve_state.prune_reserved_fill_claims(
            expired_before=broker.time.time() + 1.0)
        assert pruned == ['svc-a']
        live = {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims(pool_key=_POOL)
        }
        assert live == {'svc-b'}

    def test_mixed_gpus_per_replica_rejected(self):
        _upsert('svc-a', gpus_per_replica=1)
        _upsert('svc-b', gpus_per_replica=1)
        _upsert('svc-c', gpus_per_replica=8)
        assert _run('svc-c', free=10) is None  # the odd one out is rejected
        names = {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims(pool_key=_POOL)
        }
        assert names == {'svc-a', 'svc-b'}


@pytest.mark.usefixtures('_broker_db')
class TestEpochFencing:

    def test_epoch_bumps_only_on_allocation_change(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        first = _run('svc-a', free=10)
        assert first is not None
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        same = _run('svc-a', free=10)
        assert same is not None
        # Identical inputs -> identical grants -> stable epoch: steady-state
        # fill launches are never fenced out.
        assert same.epoch == first.epoch
        # Reallocation: grant damping needs the new split proposed by two
        # consecutive rounds before it is published (and the epoch bumps).
        proposal = None
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', weight=9)
            _upsert('svc-b')
            proposal = _run('svc-a', free=10)
            assert proposal is not None
        changed = proposal
        assert changed is not None
        assert changed.grant == 9
        assert changed.epoch == first.epoch + 1
        # Stale-epoch actuation fencing: an actuator carrying the old
        # allocation's epoch sees a newer current epoch and must skip.
        assert broker.current_epoch(_POOL) == changed.epoch
        assert first.epoch != broker.current_epoch(_POOL)

    def test_cross_pool_epoch_isolation(self, clock):
        # Rounds (and their fencing epochs) are per-pool: pool A's grant
        # churn must never fence pool B's fill launches, whose allocation
        # did not change.
        pool_b = broker.make_pool_key('other-ctx', 'H100')
        obs_b = _obs(10, gpu_names=('H100',))
        _upsert('svc-a')
        _upsert('svc-b')
        _upsert('svc-c', pool_key=pool_b)
        _upsert('svc-d', pool_key=pool_b)
        a_first = _run('svc-a', free=10)
        b_first = _run('svc-c', observation=obs_b, pool=pool_b)
        assert a_first is not None and b_first is not None
        # Reallocate pool A (weight shift; grant damping needs two rounds).
        a_changed = None
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', weight=9)
            _upsert('svc-b')
            _upsert('svc-c', pool_key=pool_b)
            _upsert('svc-d', pool_key=pool_b)
            a_changed = _run('svc-a', free=10)
        assert a_changed is not None
        assert a_changed.epoch == a_first.epoch + 1
        # Pool B's fencing epoch is untouched by pool A's bump: a pool-B
        # launch carrying b_first.epoch still passes the fence.
        assert broker.current_epoch(pool_b) == b_first.epoch
        # ... including after pool B republishes an UNCHANGED allocation.
        b_again = _run('svc-c', observation=obs_b, pool=pool_b)
        assert b_again is not None
        assert b_again.epoch == b_first.epoch
        assert broker.current_epoch(pool_b) == b_first.epoch
        # Pool B's OWN reallocation still bumps its epoch: a stale pool-B
        # epoch remains fenced.
        b_changed = None
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-c', pool_key=pool_b, weight=9)
            _upsert('svc-d', pool_key=pool_b)
            b_changed = _run('svc-c', observation=obs_b, pool=pool_b)
        assert b_changed is not None
        assert b_changed.epoch == b_first.epoch + 1
        assert broker.current_epoch(pool_b) == b_changed.epoch
        assert b_first.epoch != broker.current_epoch(pool_b)


@pytest.mark.usefixtures('_broker_db')
class TestMidQueryDemandBindDebit:

    def test_rows_binding_mid_query_debit_observed_free(self, monkeypatch):
        snapshot_holder = {}

        def query():
            # Called after the round captured its snapshot time; a READY
            # row created NOW models a demand launch binding mid-query.
            snapshot_holder['row'] = _replica_stub(
                is_ready=True, created_at=broker.time.time() + 0.001)
            return _obs(5)

        rows = [
            _replica_stub(is_ready=False, created_at=1.0),  # unbound pod
        ]
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows + list(
                snapshot_holder.values()) if name == 'svc-a' else []))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = broker.run_round_if_stale('svc-a', _POOL, query, 60.0)
        assert alloc is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        # 5 observed - 2 occupying rows = 3 spendable across the pool.
        assert sum(json.loads(round_row['feeds']).values()) == 3

    def test_terminal_and_off_pool_rows_do_not_debit(self, monkeypatch):
        rows = [
            _replica_stub(is_terminal=True, is_ready=False),
            _replica_stub(is_ready=False, region='other-ctx'),
            _replica_stub(is_ready=False, gpu='H100'),
        ]
        monkeypatch.setattr(serve_state, 'get_replica_infos',
                            mock.Mock(return_value=rows))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert sum(json.loads(round_row['feeds']).values()) == 4


@pytest.mark.usefixtures('_broker_db')
class TestMidQueryFillBindAttribution:
    """A FILL bind persisted mid-query stays attributed to its owner.

    Regression: the entitlement debit subtracted EVERY post-snapshot row
    from the whole-pool total while holdings came from the (stale) claim
    rows, so a fill replica persisted while the query ran was debited
    from free but absent from Sum(holdings) -- the total undercounted,
    the owner's grant dropped below its real holdings, and the ceiling
    culled the replica the previous round's feed had just launched.
    """

    def test_owner_grant_covers_mid_query_fill_bind(self, monkeypatch):
        rows = []

        def query():
            # A fill replica row (fed by the previous round) persists
            # while the query runs: svc-a's claim still reports holdings
            # 0, but the row IS arbitrated capacity.
            rows.append(
                _replica_stub(is_ready=False,
                              reserved_fill=True,
                              created_at=broker.time.time() + 0.001))
            return _obs(1)

        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        _upsert('svc-a', weight=2)
        _upsert('svc-b')
        alloc = broker.run_round_if_stale('svc-a', _POOL, query, 60.0)
        assert alloc is not None
        # The bind folds back into svc-a's holdings for the entitlement
        # total: its grant keeps covering the just-fed replica (no cull).
        assert alloc.grant is not None and alloc.grant >= 1
        # The FEED-side debit is untouched: the occupying row still spends
        # the observed free -- never over-launch.
        assert alloc.feed == 0
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.feed == 0


@pytest.mark.usefixtures('_broker_db')
class TestFedLaunchBootSurvival:
    """The broker must never cull replicas its own feeds just launched.

    Regression: bound-but-not-READY pods are excluded from the measured
    free AND counted in their owner's fill holdings, so also debiting them
    from the ENTITLEMENT total double-subtracted them for the whole
    bind->READY window; grants dropped below holdings, the ceiling
    stripped the booting replicas' shelter, and initializing-first victim
    ordering killed exactly the pods the previous round's feed launched.
    """

    def test_grant_never_drops_below_holdings_while_booting(
            self, clock, monkeypatch):
        rows = []
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        # Round 1: 4 free slots, no holdings -> each service fed 2.
        _upsert('svc-a')
        _upsert('svc-b')
        first = _run('svc-a', free=4)
        assert first is not None
        assert first.grant == 2 and first.feed == 2
        # svc-a launches its feed: 2 pods bind (created BEFORE the next
        # snapshot) but stay not-READY; the pool's measured free drops to
        # 2 because the bound pods already consume node capacity.
        created = clock.now
        rows = [
            _replica_stub(is_ready=False, created_at=created) for _ in range(2)
        ]
        for _ in range(3):
            clock.advance(61)
            _upsert('svc-a', holdings_fill=2)
            _upsert('svc-b')
            alloc = _run('svc-a', free=2)
            assert alloc is not None
            # The booting pods keep their shelter: the grant never drops
            # below holdings, so the ceiling never strips them.
            assert alloc.grant is not None and alloc.grant >= 2
            # The remaining free is fully debited by the in-flight rows:
            # conservative, no over-launch while the pods boot.
            assert alloc.feed == 0
        # Pods turn READY: the state converges with no cull, and the
        # still-free capacity flows to the peer.
        rows = [
            _replica_stub(is_ready=True, created_at=created) for _ in range(2)
        ]
        clock.advance(61)
        _upsert('svc-a', holdings_fill=2)
        _upsert('svc-b')
        settled = _run('svc-a', free=2)
        assert settled is not None
        assert settled.grant == 2
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 2 and alloc_b.feed == 2
