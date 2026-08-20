"""Unit tests for the reserved-fill broker (multi-service arbitration).

Pure allocation math (entitlements / feeds / damping) plus round mechanics
against a throwaway sqlite serve DB. The single-claimant fast path must be
a behavioral no-op versus #108 (grant None, feed = raw measured free); the
#108 overlay suite itself (test_reserved_capacity_fill.py) pins the
downstream identity.
"""
# pylint: disable=protected-access,invalid-name,missing-class-docstring
# pylint: disable=redefined-outer-name,not-callable
import contextlib
import dataclasses
import json
import pickle
import threading
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.sql import dml

from sky.serve import constants as serve_constants
from sky.serve import pool_capacity_observation
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils import locks

_POOL = broker.make_pool_key('research-ctx', 'A100')


def test_protocol_v2_pool_key_uses_physical_identity_not_context_alias():
    first = broker.make_pool_key('research-alias', ('H200', 'A100'),
                                 protocol_version=broker.PROTOCOL_V2,
                                 physical_cluster_uid='cluster-uid')
    second = broker.make_pool_key('phx-alias', ('a100', 'h200'),
                                  protocol_version=broker.PROTOCOL_V2,
                                  physical_cluster_uid='cluster-uid')

    assert first == second
    identity = broker.parse_pool_identity(first)
    assert identity.protocol_version == broker.PROTOCOL_V2
    assert identity.access_context is None
    assert identity.physical_cluster_uid == 'cluster-uid'
    assert identity.gpu_names == ('a100', 'h200')
    with pytest.raises(ValueError, match='do not contain'):
        broker.parse_pool_key(first)


def test_protocol_v1_pool_key_encoding_is_unchanged():
    assert broker.make_pool_key('research-ctx',
                                'A100') == json.dumps(['research-ctx', 'a100'])


def test_phase_owner_can_make_broker_lock_admission_zero_wait():
    timeout_lock = mock.MagicMock()
    timeout_lock.__enter__.side_effect = locks.LockTimeout('busy')
    query = mock.Mock()

    with mock.patch.object(broker.locks, 'get_lock',
                           return_value=timeout_lock) as get_lock:
        allocation = broker.run_round_if_stale('svc',
                                               _POOL,
                                               query,
                                               60.0,
                                               lock_timeout_seconds=0)

    assert allocation is None
    get_lock.assert_called_once_with(
        serve_constants.RESERVED_FILL_BROKER_LOCK_ID, timeout=0)
    query.assert_not_called()


def _replica(replica_id: int = 1) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)


def _fill_replica(replica_id: int = 1,
                  pool_key: str = _POOL) -> replica_managers.ReplicaInfo:
    info = _replica(replica_id)
    info.reserved_fill = True
    info.reserved_fill_pool_key = pool_key
    return info


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


def test_allocation_math_facade_identity_and_pickle_round_trip():
    assert broker.ClaimInput.__module__ == broker.__name__
    claim = _claim(floor=2, effective_cap=1)
    assert pickle.loads(pickle.dumps(claim)) == claim
    for function in (broker.scale_floors, broker.water_fill,
                     broker.compute_entitlements, broker.damp_grants,
                     broker.compute_feeds):
        assert function.__module__ == broker.__name__
        assert pickle.loads(pickle.dumps(function)) is function


def test_scale_floors_uses_deterministic_largest_remainder_rounding():
    assert broker.scale_floors(2, {
        'c': 1,
        'b': 1,
        'a': 1
    }) == {
        'a': 1,
        'b': 1,
        'c': 0,
    }
    assert broker.scale_floors(-1, {'a': 1}) == {'a': 0}


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

    def test_huge_finite_weights_do_not_overflow(self):
        # 1e308 passes isfinite, but without max-normalization
        # remaining*weight (and sum(weights) with two such claimants)
        # overflows to inf, shares go NaN, and the integer rounding
        # raises -- crashing every multi-claimant round.
        assert broker.water_fill(10, {
            'a': 1e308,
            'b': 1.0
        }, {}) == {
            'a': 10,
            'b': 0
        }
        assert broker.water_fill(10, {
            'a': 1e308,
            'b': 1e308
        }, {}) == {
            'a': 5,
            'b': 5
        }

    def test_wide_weight_ratio_produces_finite_correct_shares(self):
        # {1e6, 1} at an amount where both shares are exact integers.
        assert broker.water_fill(2_000_002, {
            'a': 1e6,
            'b': 1.0
        }, {}) == {
            'a': 2_000_000,
            'b': 2
        }


class TestEntitlements:

    def test_floors_first_then_weighted_remainder(self):
        claims = {'a': _claim(floor=10), 'b': _claim()}
        assert broker.compute_entitlements(20, claims) == {'a': 15, 'b': 5}

    def test_floor_holder_lends_remainder_to_preferred_borrowers(self):
        # Production policy: Boltz keeps ten warm reserved slots while the
        # equal-priority preferred borrowers split the realistic-size
        # remainder. Boltz keeps a positive fallback weight so the pool stays
        # work-conserving when the borrowers cannot materialize their shares.
        claims = {
            'boltz': _claim(floor=10, weight=100),
            'opendde': _claim(weight=1_000_000),
            'protenix': _claim(weight=1_000_000),
        }
        assert broker.compute_entitlements(100, claims) == {
            'boltz': 10,
            'opendde': 45,
            'protenix': 45,
        }

        claims['opendde'] = _claim(weight=1_000_000, effective_cap=30)
        assert broker.compute_entitlements(100, claims) == {
            'boltz': 10,
            'opendde': 30,
            'protenix': 60,
        }

        claims['protenix'] = _claim(weight=1_000_000, effective_cap=20)
        assert broker.compute_entitlements(100, claims) == {
            'boltz': 50,
            'opendde': 30,
            'protenix': 20,
        }

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

    def test_malformed_sticky_amount_ignored_but_valid_since_preserved(self):
        claims = {'a': _claim(), 'b': _claim()}
        feeds, sticky = broker.compute_feeds(2, {
            'a': 2,
            'b': 2
        }, claims, {
            'a': {
                'amount': 'invalid',
                'since': 0.0
            },
            'b': {
                'amount': 1,
            },
        }, 10.0, 100.0)
        assert feeds == {'a': 1, 'b': 1}
        assert sticky == {
            'a': {
                'amount': 1,
                'since': 0.0
            },
            'b': {
                'amount': 1,
                'since': 10.0
            },
        }

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
def broker_engine(tmp_path):
    """Engine the broker DB tests run against.

    sqlite here; test_reserved_fill_broker_pg.py overrides this fixture to
    re-run the same test bodies against a real Postgres server.
    """
    return create_engine(f'sqlite:///{tmp_path}/serve_state.db')


class _InertLock:
    """No-op stand-in for locks.get_lock supporting both call shapes:
    `with lock:` (the round driver) and `with lock.acquire(blocking=...):`
    (the fill persist)."""

    def acquire(self, blocking: bool = True):  # pylint: disable=unused-argument
        return contextlib.nullcontext()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


@pytest.fixture
def _broker_db(broker_engine, monkeypatch, clock):  # pylint: disable=unused-argument
    """Fresh serve DB + no-op round lock + empty in-process caches."""
    engine = broker_engine
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(broker.locks, 'get_lock',
                        lambda *args, **kwargs: _InertLock())
    # Round debits read replica rows; default to none (tests that exercise
    # the debit override this).
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state, 'get_replica_service_names',
                        mock.Mock(return_value=[]))
    # Existing allocation tests exercise the isolated-read fallback with
    # precise per-service mocks. Snapshot-specific tests below override this.
    monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                        mock.Mock(side_effect=RuntimeError('fallback')))
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
            effective_cap=None,
            activity=None):
    broker.upsert_claim(name,
                        pool_key=pool_key,
                        weight=weight,
                        floor_replicas=floor,
                        gpus_per_replica=gpus_per_replica,
                        holdings_fill=holdings_fill,
                        launchable=launchable,
                        effective_cap=effective_cap,
                        activity=activity)


def _obs(free, gpu_names=('A100',)):
    return broker.PoolObservation(free_slots=free, gpu_names=tuple(gpu_names))


def _run(name, free=0, interval=60.0, observation=None, pool=_POOL):
    obs = _obs(free) if observation is None else observation
    return broker.run_round_if_stale(name, pool, lambda: obs, interval)


def _add_replica_if_round_epoch(*args, **kwargs):
    """Call the state fence with the production-required PG persist token.

    The same state-contract cases are inherited by the real-PostgreSQL test
    module.  Those direct state calls do not own a broker lock session, so the
    test mints a token on a short raw session immediately before exercising
    the ordinary ORM transaction.  SQLite intentionally retains its
    historical tokenless path.
    """
    engine = serve_state._db_manager.get_engine()
    if engine.dialect.name != 'sqlite':
        raw_connection = engine.raw_connection()
        try:
            token = serve_state.advance_reserved_fill_persist_token(
                raw_connection)
        finally:
            raw_connection.close()
        assert token is not None
        kwargs['expected_lease_token'] = token
    return serve_state.add_replica_if_round_epoch(*args, **kwargs)


def test_exact_card_feed_partition_is_deterministic_and_conserved():
    assert broker._allocate_feed_by_accelerator({
        'svc-b': 1,
        'svc-a': 2,
    }, {
        'h200': 1,
        'l4': 2,
    }, 3) == {
        'svc-a': {
            'h200': 1,
            'l4': 1,
        },
        'svc-b': {
            'l4': 1,
        },
    }
    # An aggregate debit with no card identity clips the exact-card budget;
    # it never leaves more shaped authority than the already-conserved feed.
    clipped = broker._allocate_feed_by_accelerator({'svc-a': 3}, {
        'h200': 2,
        'l4': 2,
    }, 2)
    assert clipped == {'svc-a': {'h200': 2}}
    assert sum(clipped['svc-a'].values()) == 2


def test_exact_card_occupancy_debit_never_moves_to_another_card():
    aggregate, spendable = broker._apply_occupancy_to_exact_card_observation(
        {
            'a100': 1,
            'h200': 1,
        }, 1, {'a100': 1})

    assert aggregate == 1
    assert spendable == {'a100': 0, 'h200': 1}
    assert broker._allocate_feed_by_accelerator({'svc': 1}, spendable,
                                                aggregate) == {
                                                    'svc': {
                                                        'h200': 1
                                                    }
                                                }

    # An already-exhausted A100 card cannot transfer its debit to H200.
    aggregate, spendable = broker._apply_occupancy_to_exact_card_observation(
        {
            'a100': 0,
            'h200': 2,
        }, 1, {'a100': 1})
    assert aggregate == 2
    assert spendable == {'a100': 0, 'h200': 2}


def _replica_stub(is_ready=True,
                  is_terminal=False,
                  created_at=None,
                  region='research-ctx',
                  gpu='A100',
                  reserved_fill=False,
                  status=None,
                  launched=True):
    info = _replica()
    info.created_at = created_at
    info.reserved_fill = reserved_fill
    info.is_zero_cost = reserved_fill
    # launched=False models a launch-cancelled row (sky.launch interrupted
    # before a pod was provisioned).
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED
        if launched else common_utils.ProcessStatus.INTERRUPTED)
    if is_ready:
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
    if status == serve_state.ReplicaStatus.SHUTTING_DOWN:
        info.status_property.sky_down_status = common_utils.ProcessStatus.RUNNING
    elif (status == serve_state.ReplicaStatus.FAILED_CLEANUP or
          (is_terminal and status is None)):
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
    info.location = {
        'cloud': 'Kubernetes',
        'region': region,
        'zone': None,
        'accelerators': {
            gpu: 1
        },
    }
    return info


class TestProtocolV2ReplicaPoolProvenance:
    """Broker occupancy uses the same complete origin fence as shelter."""

    _POOL = broker.make_pool_key('phx-context',
                                 'H200',
                                 protocol_version=broker.PROTOCOL_V2,
                                 physical_cluster_uid='phx-cluster')

    @classmethod
    def _explicit_row(cls,
                      *,
                      pool_key=None,
                      generation=4,
                      physical_cluster_uid='phx-cluster',
                      region='phx-context',
                      gpu='H200'):
        info = _replica_stub(region=region, gpu=gpu, reserved_fill=True)
        info.is_zero_cost = True
        info.reserved_fill_pool_key = pool_key or cls._POOL
        info.reserved_fill_service_generation = generation
        info.reserved_fill_physical_cluster_uid = physical_cluster_uid
        return info

    @classmethod
    def _matches(cls, info):
        return broker._replica_row_on_pool(info, ('phx-context',), ('h200',),
                                           pool_key=cls._POOL,
                                           physical_cluster_uid='phx-cluster')

    def test_complete_matching_tuple_accepts_current_and_older_generation(self):
        assert self._matches(self._explicit_row(generation=5))
        assert self._matches(self._explicit_row(generation=4))
        assert self._matches(self._explicit_row(region='retired-alias'))
        prior_width = self._explicit_row()
        prior_width.location['accelerators'] = {'H200': 8}
        assert self._matches(prior_width)

    def test_complete_former_claimant_tuple_remains_physical_occupancy(self):
        assert self._matches(self._explicit_row())

    def test_only_self_consistent_other_pool_provenance_excludes_occupancy(
            self):
        other_pool = broker.make_pool_key(
            'phx-context',
            'H200',
            protocol_version=broker.PROTOCOL_V2,
            physical_cluster_uid='replacement-cluster')
        partial = _replica_stub(region='phx-context',
                                gpu='H200',
                                reserved_fill=True)
        partial.reserved_fill_pool_key = self._POOL
        # Partial provenance cannot prove that the row is absent from this
        # physical pool.  Conservatively debit every plausible same-card pool.
        assert self._matches(partial)
        assert self._matches(self._explicit_row(generation=6))
        assert self._matches(self._explicit_row(pool_key=other_pool))
        assert not self._matches(
            self._explicit_row(pool_key=other_pool,
                               physical_cluster_uid='replacement-cluster'))
        # A contradictory tuple is not proof of exact absence.  It falls back
        # to conservative hints instead of authorizing a potentially occupied
        # slot.
        assert self._matches(
            self._explicit_row(physical_cluster_uid='replacement-cluster'))
        assert self._matches(self._explicit_row(region='other-context'))
        assert self._matches(self._explicit_row(gpu='A100'))
        shapeless = self._explicit_row()
        shapeless.location['accelerators'] = {}
        assert self._matches(shapeless)
        wrong_width = self._explicit_row()
        wrong_width.location['accelerators'] = {'H200': 8}
        assert self._matches(wrong_width)
        mixed_cards = self._explicit_row()
        mixed_cards.location['accelerators'] = {'H200': 1, 'A100': 1}
        assert self._matches(mixed_cards)

    def test_v2_fill_without_explicit_provenance_is_conservatively_debited(
            self):
        legacy = _replica_stub(region='phx-context',
                               gpu='H200',
                               reserved_fill=True)
        assert self._matches(legacy)
        legacy.location['accelerators'] = {}
        assert self._matches(legacy)

    def test_occupancy_scan_applies_generation_only_to_live_claimant(
            self, monkeypatch):
        future_claimant = self._explicit_row(generation=6)
        former_claimant = self._explicit_row(generation=6)
        monkeypatch.setattr(
            serve_state, 'get_replica_infos_grouped',
            mock.Mock(return_value={
                'svc-a': [future_claimant],
                'svc-gone': [former_claimant],
            }))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1)

        # Generation authority can reject a new launch but cannot move a
        # persisted pod off its immutable physical pool. Count both physical
        # occupants; only the former claimant is unclaimed.
        assert result == (0, 0, {}, {'svc-a': 1}, 1)

    def test_pending_intents_are_physical_holdings_and_exact_card_debits(
            self, monkeypatch):
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={}))
        monkeypatch.setattr(
            broker.zero_cost_actuation, 'pending_pool_debits',
            mock.Mock(return_value=(
                broker.zero_cost_actuation.PendingPoolDebit(
                    service_name='svc-a',
                    pool_key=self._POOL,
                    accelerator='h200',
                    replica_slots=2),
                broker.zero_cost_actuation.PendingPoolDebit(
                    service_name='svc-gone',
                    pool_key=self._POOL,
                    accelerator='h200',
                    replica_slots=1),
            )))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=7,
                                         observation_materialization_sequence=7)

        assert result == (3, 3, {'h200': 3}, {'svc-a': 2}, 1)

    def test_materialized_during_observation_drainer_is_still_debited(
            self, monkeypatch):
        drainer = self._explicit_row()
        drainer.status_property.sky_down_status = (
            common_utils.ProcessStatus.RUNNING)
        drainer.zero_cost_admission_sequence = 1
        drainer.zero_cost_materialization_sequence = 2
        # Teardown may replace the current launch process status; the immutable
        # materialization marker remains the proof that cleanup is physical.
        drainer.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': [drainer]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=1,
                                         observation_materialization_sequence=1)

        assert result == (1, 1, {'h200': 1}, {'svc-a': 0}, 1)

    @pytest.mark.parametrize('reserved_fill', [False, True])
    def test_interrupted_cleanup_with_missing_materialization_is_debited(
            self, monkeypatch, reserved_fill):
        drainer = self._explicit_row()
        drainer.reserved_fill = reserved_fill
        drainer.status_property.sky_down_status = (
            common_utils.ProcessStatus.RUNNING)
        drainer.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        drainer.zero_cost_admission_sequence = 1
        drainer.zero_cost_materialization_sequence = None
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': [drainer]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=1,
                                         observation_materialization_sequence=1)

        assert result == (1, 1, {'h200': 1}, {'svc-a': 0}, 0)

    @pytest.mark.parametrize('reserved_fill', [False, True])
    def test_cleanup_proven_interrupted_row_does_not_debit(
            self, monkeypatch, reserved_fill):
        cleaned = self._explicit_row()
        cleaned.reserved_fill = reserved_fill
        cleaned.status_property.sky_down_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        cleaned.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        cleaned.zero_cost_admission_sequence = 1
        cleaned.zero_cost_materialization_sequence = None
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': [cleaned]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=1,
                                         observation_materialization_sequence=1)

        assert result == (0, 0, {}, {'svc-a': 0}, 0)

    def test_old_alias_prior_width_row_debits_current_slot_equivalent(
            self, monkeypatch):
        prior = self._explicit_row(region='retired-alias')
        prior.location['accelerators'] = {'H200': 8}
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-a': [prior]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=0,
                                         observation_materialization_sequence=0)

        assert result == (8, 8, {'h200': 8}, {'svc-a': 8}, 0)

    @pytest.mark.parametrize('terminal', [False, True])
    def test_false_cost_row_on_physical_pool_is_conservatively_debited(
            self, monkeypatch, terminal):
        physical = self._explicit_row()
        physical.reserved_fill = False
        physical.is_zero_cost = False
        physical.zero_cost_admission_sequence = None
        physical.zero_cost_materialization_sequence = None
        if terminal:
            physical.status_property.sky_down_status = (
                common_utils.ProcessStatus.RUNNING)
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-a': [physical]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=0,
                                         observation_materialization_sequence=0)

        assert result == (1, 1, {'h200': 1}, {'svc-a': 0}, 0)

    def test_unattributed_false_cost_row_on_retired_alias_is_conservative(
            self, monkeypatch):
        other = _replica_stub(region='other-context',
                              gpu='H200',
                              reserved_fill=False)
        other._version = 15
        other.is_zero_cost = False
        other.zero_cost_admission_sequence = None
        other.zero_cost_materialization_sequence = None
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': [other]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=1,
                                         observation_materialization_sequence=1)

        # With no immutable UID, another same-card Kubernetes context could be
        # a retired alias of this physical cluster. A rewrite preserves the
        # false compatibility default, so cost classification cannot prove
        # physical absence. Withhold conservatively until attribution/cleanup.
        assert result == (1, 1, {'h200': 1}, {'svc-a': 0}, 0)

    @pytest.mark.parametrize('reserved_fill', [False, True])
    @pytest.mark.parametrize('terminal', [False, True])
    @pytest.mark.parametrize('record_version', [10, 15])
    def test_legacy_row_rewritten_without_cost_truth_still_debits(
            self, monkeypatch, reserved_fill, terminal, record_version):
        legacy = _replica_stub(region='phx-context',
                               gpu='H200',
                               reserved_fill=reserved_fill)
        # Replica serialization upgrades an old row to the latest version but
        # cannot reconstruct historical is_zero_cost truth. Physical placement
        # must therefore remain authoritative after that rewrite.
        legacy._version = record_version
        legacy.is_zero_cost = False
        legacy.zero_cost_admission_sequence = None
        legacy.zero_cost_materialization_sequence = None
        if terminal:
            legacy.status_property.sky_down_status = (
                common_utils.ProcessStatus.RUNNING)
            legacy.status_property.sky_launch_status = (
                common_utils.ProcessStatus.INTERRUPTED)
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': [legacy]}))

        result = broker._occupying_debit(['svc-a'],
                                         self._POOL,
                                         10.0,
                                         access_contexts=('phx-context',),
                                         physical_cluster_uid='phx-cluster',
                                         claim_generations={'svc-a': 5},
                                         pool_gpus_per_replica=1,
                                         observation_admission_sequence=1,
                                         observation_materialization_sequence=1)

        expected_unclaimed = int(reserved_fill and not terminal)
        assert result == (1, 1, {'h200': 1}, {'svc-a': 0}, expected_unclaimed)


class TestSequencedOrdinaryZeroCostOccupancy:
    """Unattributed ordinary rows debit every plausible same-card pool."""

    @staticmethod
    def _ordinary_row(*, gpu='H200', count=1):
        info = _replica_stub(region='retired-alias',
                             gpu=gpu,
                             reserved_fill=False)
        info.location['accelerators'] = {gpu: count}
        info.is_zero_cost = True
        info.zero_cost_admission_sequence = 2
        info.zero_cost_materialization_sequence = 2
        return info

    @staticmethod
    def _debit(monkeypatch, row, *, uid='phx-cluster', gpu='H200', width=1):
        pool = broker.make_pool_key('ignored-alias',
                                    gpu,
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid=uid)
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'ordinary-svc': [row]}))
        return broker._occupying_debit(['fill-svc'],
                                       pool,
                                       10.0,
                                       access_contexts=('current-alias',),
                                       physical_cluster_uid=uid,
                                       claim_generations={'fill-svc': 7},
                                       pool_gpus_per_replica=width,
                                       observation_admission_sequence=1,
                                       observation_materialization_sequence=1)

    def test_same_card_and_width_debits_each_plausible_v2_pool(
            self, monkeypatch):
        row = self._ordinary_row()
        assert self._debit(monkeypatch, row, uid='physical-a') == (1, 1, {
            'h200': 1
        }, {
            'fill-svc': 0
        }, 0)
        assert self._debit(monkeypatch, row, uid='physical-b') == (1, 1, {
            'h200': 1
        }, {
            'fill-svc': 0
        }, 0)

    def test_different_card_is_excluded_but_width_is_conservative(
            self, monkeypatch):
        row = self._ordinary_row()
        assert self._debit(monkeypatch, row, gpu='H100') == (0, 0, {}, {
            'fill-svc': 0
        }, 0)
        assert self._debit(monkeypatch, row, width=2) == (1, 1, {
            'h200': 1
        }, {
            'fill-svc': 0
        }, 0)
        prior_width = self._ordinary_row(count=8)
        assert self._debit(monkeypatch, prior_width, width=2) == (4, 4, {
            'h200': 4
        }, {
            'fill-svc': 0
        }, 0)

    def test_complete_snapshot_failure_rejects_sequenced_authority(
            self, monkeypatch):
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(side_effect=ValueError('decode failed')))
        pool = broker.make_pool_key('phx-context',
                                    'H200',
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='phx-cluster')
        with pytest.raises(broker.IncompleteReplicaOccupancySnapshotError,
                           match='incomplete'):
            broker._occupying_debit(['fill-svc'],
                                    pool,
                                    10.0,
                                    access_contexts=('phx-context',),
                                    physical_cluster_uid='phx-cluster',
                                    claim_generations={'fill-svc': 7},
                                    pool_gpus_per_replica=1,
                                    observation_admission_sequence=1,
                                    observation_materialization_sequence=1)


def _live_fill_rows(count):
    """Nonterminal fill rows backing a claimant's holdings on the pool.

    The round replaces each claim's self-reported holdings_fill with the
    scan-derived live count whenever the replica rows are readable, so a
    test whose claims carry holdings must back them with rows.
    """
    return [
        _replica_stub(reserved_fill=True, created_at=1.0) for _ in range(count)
    ]


def _v2_edge(pool_key,
             *,
             access_context='phx-context',
             physical_cluster_uid='phx-cluster',
             generation=7,
             effective_cap=3):
    return {
        'service_name': 'svc-a',
        'pool_key': pool_key,
        'legacy_pool_key': broker.make_pool_key(access_context, 'H200'),
        'pool_position': 0,
        'access_context': access_context,
        'physical_cluster_uid': physical_cluster_uid,
        'accelerator_names': ['H200'],
        'service_generation': generation,
        'weight': 1.0,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 0,
        'effective_cap': effective_cap,
        'launchable': 1,
        'heartbeat_ts': 1000.0,
    }


def _install_legacy_reconciliation_gate(monkeypatch):
    """Keep non-PostgreSQL claim-facade tests on the legacy branch."""
    gate = pool_capacity_observation.ReconciliationGate(
        state=(pool_capacity_observation.ReconciliationGateState.LEGACY_ACTIVE),
        generation=0)
    repository = mock.Mock()
    repository.read_reconciliation_gate.return_value = gate
    monkeypatch.setattr(pool_capacity_observation,
                        'PoolCapacityObservationRepository',
                        mock.Mock(return_value=repository))


def _committed_observation(
    pool_key: str,
    *,
    free_gpus: int = 10,
    observed_at: float = 990.0,
    valid_until: float = 1060.0,
) -> pool_capacity_observation.PoolCapacityObservation:
    identity = broker.parse_pool_identity(pool_key)
    assert identity.physical_cluster_uid is not None
    payload = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        free_gpus, {identity.gpu_names[0]: free_gpus})
    return pool_capacity_observation.PoolCapacityObservation(
        pool_key=pool_key,
        physical_cluster_uid=identity.physical_cluster_uid,
        accelerator_names=identity.gpu_names,
        access_context='phx-context',
        observation_generation=11,
        lease_token=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'),
        lease_expires_at=1020.0,
        observation_sequence=29,
        ordinary_admission_sequence=29,
        materialization_sequence=23,
        payload=payload,
        payload_sha256='a' * 64,
        observed_at=observed_at,
        completed_at=995.0,
        valid_until=valid_until,
        published_at=999.0)


def test_replace_claim_set_overlap_withdraws_previous_generation(monkeypatch):
    _install_legacy_reconciliation_gate(monkeypatch)
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    overlapping = broker.make_pool_key('phx-alias', ('H200', 'H100'),
                                       protocol_version=broker.PROTOCOL_V2,
                                       physical_cluster_uid='phx-cluster')
    peer_edge = _v2_edge(overlapping)
    peer_edge['service_name'] = 'peer'
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[peer_edge]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    remove = mock.Mock(return_value=True)
    monkeypatch.setattr(serve_state, 'remove_reserved_fill_claim_set', remove)
    replace = mock.Mock()
    monkeypatch.setattr(serve_state, 'replace_reserved_fill_claim_set', replace)
    monkeypatch.setattr(broker.locks, 'get_lock',
                        lambda *args, **kwargs: _InertLock())

    assert broker.replace_claim_set('svc-a',
                                    semantic_hash='changed-set',
                                    global_headroom=1,
                                    utilization_ceiling=1,
                                    utilization_state=None,
                                    edges=[_v2_edge(pool)],
                                    expected_service_hash='owner-hash') is None
    replace.assert_not_called()
    remove.assert_called_once_with('svc-a',
                                   expected_service_hash='owner-hash',
                                   expected_controller_owner=None)
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    removed_pool = broker.make_pool_key('east-context',
                                        'H100',
                                        protocol_version=broker.PROTOCOL_V2,
                                        physical_cluster_uid='east-cluster')
    replace = mock.Mock(return_value=7)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'replace_reserved_fill_claim_set', replace)
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(broker.locks, 'get_lock',
                        lambda *args, **kwargs: _InertLock())
    monkeypatch.setattr(broker.time, 'time', mock.Mock(return_value=1234.0))
    broker._GRANT_CACHE[('svc-a', pool)] = broker._GrantCacheEntry(
        3, 1000.0, 'phx-context', ('h200',), 'phx-cluster', 7)
    broker._GRANT_CACHE[('svc-a', removed_pool)] = broker._GrantCacheEntry(
        2, 1000.0, 'east-context', ('h100',), 'east-cluster', 7)
    broker._GRANT_CACHE[('peer', removed_pool)] = broker._GrantCacheEntry(
        1, 1000.0, 'east-context', ('h100',), 'east-cluster', 1)

    assert broker.replace_claim_set('svc-a',
                                    semantic_hash='semantic-v1',
                                    global_headroom=3,
                                    utilization_ceiling=3,
                                    utilization_state={},
                                    edges=[_v2_edge(pool)],
                                    expected_service_hash='owner-hash') == 7
    assert ('svc-a', pool) in broker._GRANT_CACHE
    assert ('svc-a', removed_pool) not in broker._GRANT_CACHE
    assert ('peer', removed_pool) in broker._GRANT_CACHE
    replace.assert_called_once()
    assert replace.call_args.kwargs['heartbeat_ts'] == 1234.0
    assert (broker.replace_claim_set('svc-a',
                                     semantic_hash='semantic-v1',
                                     global_headroom=3,
                                     utilization_ceiling=3,
                                     utilization_state={},
                                     edges=[_v2_edge(pool)],
                                     expected_service_hash=None) is None)
    assert replace.call_count == 1
    assert all(service != 'svc-a' for service, _pool in broker._GRANT_CACHE)
    assert ('peer', removed_pool) in broker._GRANT_CACHE

    broker._GRANT_CACHE[('svc-a', pool)] = broker._GrantCacheEntry(
        3, 1000.0, 'phx-context', ('h200',), 'phx-cluster', 7)
    replace.return_value = None
    assert broker.replace_claim_set('svc-a',
                                    semantic_hash='semantic-v1',
                                    global_headroom=3,
                                    utilization_ceiling=3,
                                    utilization_state={},
                                    edges=[_v2_edge(pool)],
                                    expected_service_hash='owner-hash') is None
    assert all(service != 'svc-a' for service, _pool in broker._GRANT_CACHE)
    assert ('peer', removed_pool) in broker._GRANT_CACHE
    broker.clear_caches()


def test_replace_claim_set_allows_disjoint_cards_in_one_access_context(
        monkeypatch):
    _install_legacy_reconciliation_gate(monkeypatch)
    h200_pool = broker.make_pool_key('phx-context',
                                     'H200',
                                     protocol_version=broker.PROTOCOL_V2,
                                     physical_cluster_uid='phx-cluster')
    a100_pool = broker.make_pool_key('phx-context',
                                     'A100',
                                     protocol_version=broker.PROTOCOL_V2,
                                     physical_cluster_uid='phx-cluster')
    h200_edge = _v2_edge(h200_pool)
    a100_edge = _v2_edge(a100_pool)
    a100_edge.update({
        'legacy_pool_key': broker.make_pool_key('phx-context', 'A100'),
        'pool_position': 1,
        'accelerator_names': ['A100'],
    })
    replace = mock.Mock(return_value=7)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state, 'replace_reserved_fill_claim_set', replace)
    monkeypatch.setattr(broker.locks, 'get_lock',
                        lambda *args, **kwargs: _InertLock())

    generation = broker.replace_claim_set('svc-a',
                                          semantic_hash='two-exact-card-edges',
                                          global_headroom=3,
                                          utilization_ceiling=3,
                                          utilization_state={},
                                          edges=[h200_edge, a100_edge],
                                          expected_service_hash='owner-hash')

    assert generation == 7
    assert replace.call_args.kwargs['edges'] == [h200_edge, a100_edge]


def test_v2_round_single_claimant_is_integer_generation_fenced(
        _broker_db, monkeypatch):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    edge = _v2_edge(pool)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[edge]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    publish = mock.Mock(return_value=True)
    monkeypatch.setattr(serve_state, 'publish_reserved_fill_round', publish)

    allocation = broker.run_round_if_stale(
        'svc-a',
        pool,
        lambda: broker.PoolObservation(10, ('H200',), (('h200', 10),)),
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)

    assert allocation is not None
    assert allocation.grant == 3
    assert isinstance(allocation.grant, int)
    assert allocation.feed == 3
    assert allocation.protocol_version == broker.PROTOCOL_V2
    assert allocation.service_generation == 7
    assert allocation.physical_cluster_uid == 'phx-cluster'
    assert allocation.edge_cap == 3
    assert allocation.pool_key == pool
    assert allocation.feed_by_accelerator == {'h200': 3}
    assert allocation.observed_free_slots == 10
    assert allocation.observed_free_slots_by_accelerator == {'h200': 10}
    assert allocation.observed_at == allocation.snapshot_time
    assert publish.call_args.kwargs['protocol_version'] == broker.PROTOCOL_V2
    assert json.loads(publish.call_args.kwargs['feed_by_accelerator']) == {
        broker._OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 10
        },
        'svc-a': {
            'h200': 3
        }
    }
    assert json.loads(publish.call_args.kwargs['claim_generations']) == {
        'svc-a': 7
    }
    cached = broker.get_cached_pool_grants('svc-a', 60.0)
    assert cached[pool] == broker.CachedPoolGrant(
        grant=3,
        access_context='phx-context',
        accelerator_names=('h200',),
        physical_cluster_uid='phx-cluster',
        service_generation=7)


def test_committed_observation_drives_provider_free_round_with_provenance(
        _broker_db, monkeypatch):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    edge = _v2_edge(pool)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[edge]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    legacy_publish = mock.Mock(side_effect=AssertionError(
        'committed rounds must not use the legacy publication boundary'))
    monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                        legacy_publish)
    monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                        mock.Mock(return_value={}))
    publish_round = mock.Mock(return_value=True)
    observation = _committed_observation(pool)

    allocation = broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        observation,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round)

    assert allocation is not None
    assert allocation.feed == 3
    assert allocation.feed_by_accelerator == {'h200': 3}
    assert allocation.observed_free_slots == 10
    assert allocation.observed_free_slots_by_accelerator == {'h200': 10}
    assert allocation.snapshot_time == observation.observed_at
    assert allocation.observed_at == observation.observed_at
    publication = publish_round.call_args.args[0]
    assert isinstance(publication, broker.ReservedFillRoundPublication)
    assert publication.snapshot_time == observation.observed_at
    assert publication.observation_provenance == (
        broker.RoundObservationProvenance(pool_key=pool,
                                          physical_cluster_uid='phx-cluster',
                                          accelerator_names=('h200',),
                                          access_context='phx-context',
                                          observation_generation=11,
                                          observation_sequence=29,
                                          materialization_sequence=23,
                                          payload_sha256='a' * 64,
                                          observed_at=observation.observed_at,
                                          valid_until=observation.valid_until))
    legacy_publish.assert_not_called()


def test_committed_observation_converts_raw_gpus_once_with_broker_width(
        _broker_db, monkeypatch):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    edge = _v2_edge(pool)
    edge['gpus_per_replica'] = 8
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[edge]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                        mock.Mock(return_value={}))
    publish_round = mock.Mock(return_value=True)
    observation = _committed_observation(pool, free_gpus=10)

    allocation = broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        observation,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round)

    assert allocation is not None
    assert allocation.feed == 1
    assert allocation.feed_by_accelerator == {'h200': 1}
    assert allocation.observed_free_slots == 1
    assert allocation.observed_free_slots_by_accelerator == {'h200': 1}
    assert allocation.broker_slot_width == 8
    publication = publish_round.call_args.args[0]
    assert publication.last_observed_free == 1
    assert json.loads(publication.feed_by_accelerator) == {
        broker._OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 1
        },
        broker._BROKER_SLOT_WIDTH_KEY: 8,
        'svc-a': {
            'h200': 1
        },
    }


def test_committed_observation_fails_closed_before_lock_when_invalid(
        monkeypatch):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    lock = mock.Mock()
    monkeypatch.setattr(broker.locks, 'get_lock', lock)
    monkeypatch.setattr(broker.time, 'time', mock.Mock(return_value=1000.0))
    publish_round = mock.Mock(return_value=True)
    observation = _committed_observation(pool)

    blackout = dataclasses.replace(
        observation,
        payload=pool_capacity_observation.PoolCapacityBlackout(
            pool_capacity_observation.PoolCapacityBlackoutReason.TIMEOUT))
    assert broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        blackout,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round) is None

    mismatched_identity = dataclasses.replace(observation,
                                              physical_cluster_uid='other')
    assert broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        mismatched_identity,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round) is None

    mismatched_split = dataclasses.replace(
        observation,
        payload=pool_capacity_observation.PoolCapacitySuccess.from_counts(
            10, {'h100': 10}))
    assert broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        mismatched_split,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round) is None

    expired = dataclasses.replace(observation, valid_until=999.0)
    assert broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        expired,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round) is None

    lock.assert_not_called()
    publish_round.assert_not_called()


def test_committed_observation_expiring_while_waiting_for_lock_fails_closed(
        _broker_db, monkeypatch, clock):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    observation = _committed_observation(pool, valid_until=1001.0)

    class _ExpiringLock:

        def __enter__(self):
            clock.advance(2.0)
            return self

        def __exit__(self, *exc):
            return None

    monkeypatch.setattr(broker.locks, 'get_lock',
                        mock.Mock(return_value=_ExpiringLock()))
    acquire_lease = mock.Mock()
    monkeypatch.setattr(serve_state, 'acquire_reserved_fill_lease_token',
                        acquire_lease)
    publish_round = mock.Mock(return_value=True)

    assert broker.run_round_from_committed_observation(
        'svc-a',
        pool,
        observation,
        60.0,
        expected_service_generation=7,
        publish_round=publish_round) is None
    acquire_lease.assert_not_called()
    publish_round.assert_not_called()


def test_cas_lost_writer_returns_no_measured_capacity(_broker_db, monkeypatch):
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    edge = _v2_edge(pool)
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[edge]))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                        mock.Mock(return_value=False))
    query = mock.Mock(
        return_value=broker.PoolObservation(10, ('H200',), (('h200', 10),)))

    allocation = broker.run_round_if_stale(
        'svc-a',
        pool,
        query,
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)

    assert allocation is None
    query.assert_called_once_with()


def test_v2_confirmed_phantom_preserves_generation_and_healthy_sibling(
        _broker_db, monkeypatch, clock):
    phantom_pool = broker.make_pool_key('phx-context',
                                        'H200',
                                        protocol_version=broker.PROTOCOL_V2,
                                        physical_cluster_uid='phx-cluster')
    healthy_pool = broker.make_pool_key('east-context',
                                        'H100',
                                        protocol_version=broker.PROTOCOL_V2,
                                        physical_cluster_uid='east-cluster')
    phantom_edge = _v2_edge(phantom_pool)
    healthy_edge = _v2_edge(healthy_pool,
                            access_context='east-context',
                            physical_cluster_uid='east-cluster')
    healthy_edge.update({
        'legacy_pool_key': broker.make_pool_key('east-context', 'H100'),
        'pool_position': 1,
        'accelerator_names': ['H100'],
    })
    edges = {
        phantom_pool: phantom_edge,
        healthy_pool: healthy_edge,
    }
    previous_phantom_round = {
        'protocol_version': broker.PROTOCOL_V2,
        'claim_generations': json.dumps({'svc-a': 7}),
        'grants': json.dumps({'svc-a': 3}),
        'feeds': json.dumps({'svc-a': 3}),
        'feed_by_accelerator': json.dumps({'svc-a': {
            'h200': 3
        }}),
        'raw_grants': json.dumps({'svc-a': 3}),
        'feed_state': json.dumps({}),
        'utilization_state': None,
        'round_id': 2,
        'epoch': 4,
        'snapshot_time': clock.now - 100,
        'sum_holdings': 0,
        'last_observed_free': 3,
        'last_observed_free_ts': clock.now - 100,
        'phantom_streak':
            (serve_constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS - 1),
        'shrink_baseline': None,
        'fence_pending': False,
    }

    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(
        serve_state, 'get_authoritative_reserved_fill_claims',
        mock.Mock(side_effect=lambda pool_key=None: ([edges[
            pool_key]] if pool_key is not None else list(edges.values()))))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(
        serve_state, 'get_reserved_fill_round',
        mock.Mock(side_effect=lambda pool_key: previous_phantom_round
                  if pool_key == phantom_pool else None))
    publish = mock.Mock(return_value=True)
    monkeypatch.setattr(serve_state, 'publish_reserved_fill_round', publish)
    remove_pool = mock.Mock()
    monkeypatch.setattr(broker, '_remove_legacy_claims_for_pool', remove_pool)

    phantom = broker.run_round_if_stale(
        'svc-a',
        phantom_pool,
        lambda: broker.PoolObservation(0, (), ()),
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)
    assert phantom is not None
    assert phantom.grant == 0
    assert phantom.feed == 0
    assert phantom.observed_free_slots is None
    assert phantom.observed_free_slots_by_accelerator is None
    assert phantom.service_generation == 7
    remove_pool.assert_not_called()
    phantom_publish = publish.call_args
    assert json.loads(phantom_publish.kwargs['claim_generations']) == {
        'svc-a': 7
    }
    assert json.loads(phantom_publish.kwargs['grants']) == {'svc-a': 0}
    assert json.loads(phantom_publish.kwargs['feeds']) == {'svc-a': 0}

    healthy = broker.run_round_if_stale(
        'svc-a',
        healthy_pool,
        lambda: broker.PoolObservation(4, ('H100',), (('h100', 4),)),
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)
    assert healthy is not None
    assert healthy.grant == 3
    assert healthy.feed == 3
    assert healthy.service_generation == 7


def test_v2_mixed_width_blackout_preserves_generation_and_healthy_sibling(
        _broker_db, monkeypatch):
    mixed_pool = broker.make_pool_key('phx-context',
                                      'H200',
                                      protocol_version=broker.PROTOCOL_V2,
                                      physical_cluster_uid='phx-cluster')
    healthy_pool = broker.make_pool_key('east-context',
                                        'H100',
                                        protocol_version=broker.PROTOCOL_V2,
                                        physical_cluster_uid='east-cluster')
    loser = _v2_edge(mixed_pool, generation=7)
    loser['gpus_per_replica'] = 8
    first_winner = _v2_edge(mixed_pool, generation=8, effective_cap=4)
    first_winner['service_name'] = 'svc-b'
    second_winner = _v2_edge(mixed_pool, generation=9, effective_cap=4)
    second_winner['service_name'] = 'svc-c'
    healthy_sibling = _v2_edge(healthy_pool,
                               access_context='east-context',
                               physical_cluster_uid='east-cluster',
                               generation=7)
    healthy_sibling.update({
        'legacy_pool_key': broker.make_pool_key('east-context', 'H100'),
        'pool_position': 1,
        'accelerator_names': ['H100'],
        'gpus_per_replica': 8,
    })
    claims = {
        mixed_pool: [loser, first_winner, second_winner],
        healthy_pool: [healthy_sibling],
    }

    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(
        serve_state, 'get_authoritative_reserved_fill_claims',
        mock.Mock(side_effect=lambda pool_key=None: list(claims[pool_key])))
    monkeypatch.setattr(serve_state,
                        'prune_authoritative_reserved_fill_claim_sets',
                        mock.Mock(return_value=[]))
    remove_edge = mock.Mock()
    monkeypatch.setattr(serve_state, 'remove_authoritative_reserved_fill_claim',
                        remove_edge)
    publish = mock.Mock(return_value=True)
    monkeypatch.setattr(serve_state, 'publish_reserved_fill_round', publish)

    loser_query = mock.Mock(
        return_value=broker.PoolObservation(1, ('H200',), (('h200', 1),)))
    rejected = broker.run_round_if_stale(
        'svc-a',
        mixed_pool,
        loser_query,
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)
    assert rejected is not None
    assert rejected.grant == 0
    assert rejected.feed == 0
    assert rejected.service_generation == 7
    loser_query.assert_not_called()
    publish.assert_not_called()
    remove_edge.assert_not_called()

    winner = broker.run_round_if_stale(
        'svc-b',
        mixed_pool,
        lambda: broker.PoolObservation(10, ('H200',), (('h200', 10),)),
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=8)
    assert winner is not None
    assert winner.grant > 0
    mixed_publish = publish.call_args
    assert json.loads(mixed_publish.kwargs['claim_generations']) == {
        'svc-a': 7,
        'svc-b': 8,
        'svc-c': 9,
    }
    assert json.loads(mixed_publish.kwargs['grants'])['svc-a'] == 0
    assert json.loads(mixed_publish.kwargs['feeds'])['svc-a'] == 0
    remove_edge.assert_not_called()

    healthy = broker.run_round_if_stale(
        'svc-a',
        healthy_pool,
        lambda: broker.PoolObservation(4, ('H100',), (('h100', 4),)),
        60.0,
        expected_protocol_version=broker.PROTOCOL_V2,
        expected_service_generation=7)
    assert healthy is not None
    assert healthy.grant == 3
    assert healthy.feed == 3
    assert healthy.service_generation == 7
    assert loser['service_generation'] == 7
    assert healthy_sibling['service_generation'] == 7
    remove_edge.assert_not_called()


def test_cached_pool_grants_snapshot_blocks_concurrent_replacement(monkeypatch):
    """A placement read must not combine entries from two cache epochs."""
    first_pool = broker.make_pool_key('first-context',
                                      'H200',
                                      protocol_version=broker.PROTOCOL_V2,
                                      physical_cluster_uid='first-cluster')
    second_pool = broker.make_pool_key('second-context',
                                       'H100',
                                       protocol_version=broker.PROTOCOL_V2,
                                       physical_cluster_uid='second-cluster')
    now = broker.time.time()
    reader_paused = threading.Event()
    resume_reader = threading.Event()
    writer_started = threading.Event()
    writer_done = threading.Event()

    class _BlockingItemsDict(dict):

        def items(self):
            iterator = iter(dict.items(self))
            try:
                first_item = next(iterator)
            except StopIteration:
                return
            yield first_item
            reader_paused.set()
            if not resume_reader.wait(timeout=5):
                raise TimeoutError('cache snapshot reader was not resumed')
            yield from iterator

    cache = _BlockingItemsDict({
        ('svc-a', first_pool): broker._GrantCacheEntry(1, now, 'first-context',
                                                       ('h200',),
                                                       'first-cluster', 1),
        ('svc-a', second_pool): broker._GrantCacheEntry(2, now,
                                                        'second-context',
                                                        ('h100',),
                                                        'second-cluster', 1),
    })
    monkeypatch.setattr(broker, '_GRANT_CACHE', cache)
    reader_results = []
    thread_errors = []

    def _read_snapshot():
        try:
            reader_results.append(
                broker.get_cached_pool_grants('svc-a', max_age_seconds=60))
        except BaseException as exc:  # pylint: disable=broad-except
            thread_errors.append(exc)

    replacement = broker.Allocation(grant=9,
                                    feed=9,
                                    round_id=2,
                                    epoch=2,
                                    snapshot_time=now,
                                    demand_gate_grant=9,
                                    protocol_version=broker.PROTOCOL_V2,
                                    service_generation=2,
                                    physical_cluster_uid='second-cluster',
                                    edge_cap=9,
                                    pool_key=second_pool)

    def _replace_entry():
        writer_started.set()
        try:
            broker._cache_allocation('svc-a', replacement,
                                     {'access_context': 'new-context'})
        except BaseException as exc:  # pylint: disable=broad-except
            thread_errors.append(exc)
        finally:
            writer_done.set()

    reader = threading.Thread(target=_read_snapshot)
    writer = threading.Thread(target=_replace_entry)
    reader.start()
    try:
        assert reader_paused.wait(timeout=5)
        writer.start()
        assert writer_started.wait(timeout=5)
        # The reader is paused midway through dict iteration while holding the
        # cache lock. Replacement must wait rather than expose a mixed epoch.
        assert not writer_done.wait(timeout=0.25)
    finally:
        resume_reader.set()
        reader.join(timeout=5)
        if writer.ident is not None:
            writer.join(timeout=5)

    assert not reader.is_alive()
    assert not writer.is_alive()
    assert not thread_errors
    assert reader_results == [{
        first_pool: broker.CachedPoolGrant(1, 'first-context', ('h200',),
                                           'first-cluster', 1),
        second_pool: broker.CachedPoolGrant(2, 'second-context', ('h100',),
                                            'second-cluster', 1),
    }]
    assert broker.get_cached_pool_grants(
        'svc-a',
        60)[second_pool] == broker.CachedPoolGrant(9, 'new-context', ('h100',),
                                                   'second-cluster', 2)


def test_v2_round_reader_rejects_generation_mismatch_and_clamps_cap():
    broker.clear_caches()
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    round_row = {
        'protocol_version': broker.PROTOCOL_V2,
        'claim_generations': json.dumps({'svc-a': 6}),
        'grants': json.dumps({'svc-a': 20}),
        'feeds': json.dumps({'svc-a': 20}),
        'raw_grants': json.dumps({'svc-a': 20}),
        'round_id': 3,
        'epoch': 9,
        'snapshot_time': 1000.0,
    }
    edge = _v2_edge(pool, generation=7, effective_cap=2)
    assert broker._allocation_from_round('svc-a',
                                         pool,
                                         round_row,
                                         protocol_version=broker.PROTOCOL_V2,
                                         service_generation=7,
                                         claim_row=edge) is None

    round_row['claim_generations'] = json.dumps({'svc-a': 7})
    allocation = broker._allocation_from_round(
        'svc-a',
        pool,
        round_row,
        protocol_version=broker.PROTOCOL_V2,
        service_generation=7,
        claim_row=edge)
    assert allocation is not None
    assert allocation.grant == 2
    assert allocation.feed == 2
    assert allocation.feed_by_accelerator is None
    assert allocation.demand_gate_grant == 2

    round_row['last_observed_free'] = 3
    round_row['last_observed_free_ts'] = round_row['snapshot_time']
    round_row['feed_by_accelerator'] = json.dumps({
        'svc-a': {
            'h200': 1
        },
        broker._OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 3
        },
        broker._BROKER_SLOT_WIDTH_KEY: 1,
    })
    allocation = broker._allocation_from_round(
        'svc-a',
        pool,
        round_row,
        protocol_version=broker.PROTOCOL_V2,
        service_generation=7,
        claim_row=edge)
    assert allocation is not None
    assert allocation.feed == 1
    assert allocation.feed_by_accelerator == {'h200': 1}
    assert allocation.observed_free_slots == 3
    assert allocation.observed_free_slots_by_accelerator == {'h200': 3}
    assert allocation.observed_at == round_row['snapshot_time']

    # Observation corruption suppresses only bench-release evidence; valid
    # service launch authority remains intact.
    round_row['feed_by_accelerator'] = json.dumps({
        'svc-a': {
            'h200': 1
        },
        broker._OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 2
        },
        broker._BROKER_SLOT_WIDTH_KEY: 1,
    })
    allocation = broker._allocation_from_round(
        'svc-a',
        pool,
        round_row,
        protocol_version=broker.PROTOCOL_V2,
        service_generation=7,
        claim_row=edge)
    assert allocation is not None
    assert allocation.feed == 1
    assert allocation.feed_by_accelerator == {'h200': 1}
    assert allocation.observed_free_slots is None

    # Service corruption still fails launch authority closed, independently
    # of a valid committed observation.
    round_row['feed_by_accelerator'] = json.dumps({
        'svc-a': {
            'h200': 'bad'
        },
        broker._OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 3
        },
        broker._BROKER_SLOT_WIDTH_KEY: 1,
    })
    allocation = broker._allocation_from_round(
        'svc-a',
        pool,
        round_row,
        protocol_version=broker.PROTOCOL_V2,
        service_generation=7,
        claim_row=edge)
    assert allocation is not None
    assert allocation.feed == 0
    assert allocation.feed_by_accelerator == {}
    assert allocation.observed_free_slots == 3
    assert allocation.observed_free_slots_by_accelerator == {'h200': 3}

    round_row['feed_by_accelerator'] = '{malformed'
    allocation = broker._allocation_from_round(
        'svc-a',
        pool,
        round_row,
        protocol_version=broker.PROTOCOL_V2,
        service_generation=7,
        claim_row=edge)
    assert allocation is not None
    assert allocation.feed == 0
    assert allocation.feed_by_accelerator == {}
    assert allocation.observed_free_slots is None
    assert allocation.observed_free_slots_by_accelerator is None
    broker.clear_caches()


def test_v2_unambiguous_lookup_compares_only_its_pool_claim_set(monkeypatch):
    broker.clear_caches()
    pool = broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='phx-cluster')
    other_pool = broker.make_pool_key('east-context',
                                      'H100',
                                      protocol_version=broker.PROTOCOL_V2,
                                      physical_cluster_uid='east-cluster')
    edge = _v2_edge(pool)
    peer = _v2_edge(other_pool,
                    access_context='east-context',
                    physical_cluster_uid='east-cluster',
                    generation=9)
    peer['service_name'] = 'peer'
    peer['accelerator_names'] = ['H100']
    monkeypatch.setattr(broker, 'get_protocol_version',
                        mock.Mock(return_value=broker.PROTOCOL_V2))
    monkeypatch.setattr(serve_state, 'get_authoritative_reserved_fill_claims',
                        mock.Mock(return_value=[edge, peer]))
    monkeypatch.setattr(
        serve_state, 'get_reserved_fill_round',
        mock.Mock(
            return_value={
                'protocol_version': broker.PROTOCOL_V2,
                'claim_generations': json.dumps({'svc-a': 7}),
                'grants': json.dumps({'svc-a': 2}),
                'feeds': json.dumps({'svc-a': 1}),
                'raw_grants': json.dumps({'svc-a': 2}),
                'round_id': 1,
                'epoch': 1,
                'snapshot_time': 1000.0,
            }))
    monkeypatch.setattr(broker.time, 'time', mock.Mock(return_value=1000.0))

    allocation = broker.get_my_allocation('svc-a')
    assert allocation is not None
    assert allocation.pool_key == pool
    broker.clear_caches()


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
        assert alloc.observed_free_slots is None
        assert alloc.observed_free_slots_by_accelerator is None

    def test_v1_publishes_committed_exact_card_observation(self):
        _upsert('svc-a')
        alloc = _run('svc-a',
                     observation=broker.PoolObservation(7, ('A100',),
                                                        (('a100', 7),)))
        assert alloc is not None
        assert alloc.grant is None
        assert alloc.feed == 7
        assert alloc.feed_by_accelerator == {'a100': 7}
        assert alloc.observed_free_slots == 7
        assert alloc.observed_free_slots_by_accelerator == {'a100': 7}
        assert alloc.observed_at == alloc.snapshot_time

    def test_invalid_exact_card_split_is_a_blackout(self):
        _upsert('svc-a')
        alloc = _run('svc-a',
                     observation=broker.PoolObservation(7, ('A100',),
                                                        (('a100', 6),)))
        assert alloc is not None
        assert alloc.feed == 0
        assert alloc.observed_free_slots is None
        assert alloc.observed_free_slots_by_accelerator is None


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

    def test_new_claimant_reclaims_from_borrower(self, clock, monkeypatch):
        # svc-a borrowed the whole pool while alone (grant None); svc-b
        # arrives with a floor. The next round grants svc-a LESS than it
        # holds -- the ceiling then strips its surplus's shelter and the
        # normal graceful scale-down returns the machines (autoscaler-side
        # behavior pinned in test_reserved_capacity_fill.py).
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: _live_fill_rows(10)
                      if name == 'svc-a' else []))
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

    @pytest.mark.parametrize('poisoned', [float('inf'), float('nan')])
    def test_poisoned_non_finite_weight_clamps_to_default(self, poisoned):
        # SkyServiceSpec rejects non-finite weights at construction, but
        # a poisoned DB claim row (older writer, manual surgery) must not
        # crash weighted water-filling (inf/inf -> NaN in rounding) on
        # every round for the pool while the claim stays live: the broker
        # clamps it to the default weight and the round completes.
        _upsert('svc-a', weight=poisoned)
        _upsert('svc-b', weight=1.0)
        alloc = _run('svc-a', free=10)
        assert alloc is not None
        assert alloc.grant == 5  # clamped to 1.0: equal-weight split
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 5

    def test_poisoned_out_of_bound_weight_clamps_to_bound(self):
        # Finite but above the documented bound (the spec rejects it at
        # construction): a poisoned DB row is clamped to the bound, so the
        # round completes with extreme-but-finite sane grants instead of
        # crashing the water-fill.
        _upsert('svc-a', weight=1e308)
        _upsert('svc-b', weight=1.0)
        alloc = _run('svc-a', free=10)
        assert alloc is not None
        assert alloc.grant == 10 and alloc.feed == 10
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 0 and alloc_b.feed == 0

    def test_fresh_round_is_read_not_redriven(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        observation = broker.PoolObservation(10, ('A100',), (('a100', 10),))
        first = _run('svc-a', observation=observation)
        assert first is not None
        assert first.observed_free_slots == 10
        assert first.observed_free_slots_by_accelerator == {'a100': 10}
        query = mock.Mock(side_effect=AssertionError('must not re-query'))
        clock.advance(10)  # well inside the freshness window
        again = broker.run_round_if_stale('svc-b', _POOL, query, 60.0)
        assert again is not None
        assert again.round_id == first.round_id
        assert again.epoch == first.epoch
        assert again.observed_free_slots == first.observed_free_slots
        assert (again.observed_free_slots_by_accelerator ==
                first.observed_free_slots_by_accelerator)
        assert again.observed_at == first.observed_at
        query.assert_not_called()

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

    def test_explicit_opt_out_clears_gate_state_during_blackout(self, request):
        test_clock = request.getfixturevalue('clock')
        _upsert('svc-a',
                floor=70,
                holdings_fill=20,
                activity={
                    'demonstrated_need': 0,
                    'boot_hold': False,
                })
        _upsert('svc-b')
        assert _run('svc-a', free=0) is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert 'svc-a' in json.loads(round_row['utilization_state'] or '{}')

        test_clock.advance(61)
        # All-NULL activity is the explicit opt-out. A blackout must restore
        # static behavior immediately rather than carry the old gated cap.
        _upsert('svc-a', floor=70, holdings_fill=20, activity=None)
        _upsert('svc-b')
        assert _run('svc-a', observation=_obs(None, gpu_names=())) is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert json.loads(round_row['utilization_state'] or '{}') == {}

    def test_blackout_releases_nothing_and_feeds_nothing(
            self, clock, monkeypatch):
        # Holdings are row-backed: blind rounds still run the row scan
        # (a DB read), so the live counts feed the round math.
        monkeypatch.setattr(serve_state, 'get_replica_infos',
                            mock.Mock(return_value=_live_fill_rows(5)))
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

    def test_blackout_after_full_consumption_carries_grants(
            self, clock, monkeypatch):
        # Regression: the blind path used to recompute entitlements from
        # decayed last-known free + current holdings -- but holdings built
        # FROM those very slots since the last good observation double-
        # count them: 10 free observed -> 10 launched -> blackout read
        # 10 + 10 = 20 and granted 10+10 on a ten-slot pool, reopening the
        # demand-placement gate. A blackout must carry the previous
        # round's grants (floored at current holdings), never recompute.
        rows: dict = {}
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows.get(name, [])))
        _upsert('svc-a')
        _upsert('svc-b')
        good = _run('svc-a', free=10)
        assert good is not None
        assert good.grant == 5 and good.feed == 5
        # Both services materialize their feeds: all ten slots consumed.
        rows = {'svc-a': _live_fill_rows(5), 'svc-b': _live_fill_rows(5)}
        # Two consecutive blackout rounds: the second one used to inflate
        # even through grant damping (the first blind round's published
        # raw_grants became the damping baseline).
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', holdings_fill=5)
            _upsert('svc-b', holdings_fill=5)
            blind = _run('svc-a', observation=_obs(None, gpu_names=()))
            assert blind is not None
            # Carried 5+5, NOT 10+10; holdings >= grant stays true, so
            # the demand-placement gate never reopens during the blackout.
            assert blind.grant == 5
            assert blind.feed == 0
            alloc_b = broker.get_my_allocation('svc-b')
            assert alloc_b is not None
            assert alloc_b.grant == 5 and alloc_b.feed == 0
        # Recovery round: normal recompute at the fixpoint, no churn.
        clock.advance(61)
        _upsert('svc-a', holdings_fill=5)
        _upsert('svc-b', holdings_fill=5)
        recovered = _run('svc-a', free=0)
        assert recovered is not None
        assert recovered.grant == 5 and recovered.feed == 0
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 5 and alloc_b.feed == 0

    def test_persistent_blackout_never_grants_below_holdings(
            self, clock, monkeypatch):
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: _live_fill_rows(8)
                      if name == 'svc-a' else []))
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

    def test_overlapping_accelerator_groups_are_rejected(self):
        _upsert('svc-a', pool_key=broker.make_pool_key('research-ctx', 'A100'))
        accepted = broker.upsert_claim('svc-b',
                                       pool_key=broker.make_pool_key(
                                           'research-ctx',
                                           ('A100', 'A100-80GB')),
                                       weight=1,
                                       floor_replicas=0,
                                       gpus_per_replica=1,
                                       holdings_fill=0,
                                       launchable=True)
        assert accepted is False
        assert {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims()
        } == {'svc-a'}

    def test_identical_accelerator_groups_share_broker_round(self):
        pool = broker.make_pool_key('research-ctx', ('A100', 'A100-80GB'))
        _upsert('svc-a', pool_key=pool)
        _upsert('svc-b', pool_key=pool)
        assert {
            row['service_name']
            for row in serve_state.get_reserved_fill_claims(pool_key=pool)
        } == {'svc-a', 'svc-b'}

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

    def test_confirmed_phantom_publishes_blackout_round(self, clock):
        # A confirmed rejection must still publish a round: without one
        # the freshness gate never engages and every claimant re-drives
        # the full cluster query each interval forever (with the pinned
        # streak re-confirming each time).
        _upsert('svc-a')
        _upsert('svc-b')
        phantom = _obs(0, gpu_names=())
        for _ in range(2):
            assert _run('svc-a', observation=phantom) is not None
            clock.advance(61)
            _upsert('svc-a')
            _upsert('svc-b')
        before = serve_state.get_reserved_fill_round(_POOL)
        assert before is not None
        confirm_time = clock.now
        assert _run('svc-a', observation=phantom) is None
        assert not serve_state.get_reserved_fill_claims(pool_key=_POOL)
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert json.loads(round_row['grants']) == {}
        assert json.loads(round_row['feeds']) == {}
        assert float(round_row['snapshot_time']) == confirm_time
        # Grants went from something to nothing: an allocation change, so
        # the fencing epoch bumps once at the transition.
        assert int(round_row['epoch']) == int(before['epoch']) + 1
        # Within the interval, pollers (which re-upsert their claims each
        # cycle) read the fresh blackout round: no allocation, and
        # crucially NO new cluster query.
        clock.advance(10)
        _upsert('svc-a')
        _upsert('svc-b')
        query = mock.Mock(side_effect=AssertionError('must not re-query'))
        assert broker.run_round_if_stale('svc-a', _POOL, query, 60.0) is None
        query.assert_not_called()
        # A later healthy observation resets the streak and resumes
        # normal rounds for the re-claimed services.
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        healthy = _run('svc-a', free=6)
        assert healthy is not None
        assert healthy.grant == 3 and healthy.feed == 3
        resumed = serve_state.get_reserved_fill_round(_POOL)
        assert resumed is not None
        assert int(resumed['phantom_streak']) == 0

    def test_service_teardown_deletes_claim_row(self):
        # Service-level teardown must not leave a live claim absorbing
        # entitlement until the TTL expires.
        _upsert('svc-a')
        _upsert('svc-b')
        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            session.execute(serve_state.services_table.insert().values(
                name='svc-a', hash='incarnation-a'))
            session.commit()
        assert serve_state.remove_service_completely('svc-a', 'incarnation-a')
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

    def test_raw_observation_only_change_does_not_bump_v2_epoch(
            self, clock, monkeypatch):
        pool = broker.make_pool_key('phx-context', ('H200', 'H100'),
                                    protocol_version=broker.PROTOCOL_V2,
                                    physical_cluster_uid='phx-cluster')
        edge = _v2_edge(pool, effective_cap=3)
        edge['accelerator_names'] = ['H200', 'H100']
        rounds = {}

        def _publish(pool_key, **kwargs):
            rounds[pool_key] = dict(kwargs)
            rounds[pool_key]['fence_pending'] = False
            return True

        monkeypatch.setattr(broker, 'get_protocol_version',
                            mock.Mock(return_value=broker.PROTOCOL_V2))
        monkeypatch.setattr(serve_state,
                            'get_authoritative_reserved_fill_claims',
                            mock.Mock(return_value=[edge]))
        monkeypatch.setattr(serve_state,
                            'prune_authoritative_reserved_fill_claim_sets',
                            mock.Mock(return_value=[]))
        monkeypatch.setattr(serve_state, 'get_reserved_fill_round',
                            mock.Mock(side_effect=rounds.get))
        monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                            _publish)

        def _run_v2(by_card):
            observation = broker.PoolObservation(sum(by_card.values()),
                                                 tuple(by_card),
                                                 tuple(by_card.items()))
            return broker.run_round_if_stale(
                'svc-a',
                pool,
                mock.Mock(return_value=observation),
                60.0,
                expected_protocol_version=broker.PROTOCOL_V2,
                expected_service_generation=7)

        first = _run_v2({'h200': 10, 'h100': 0})
        assert first is not None
        assert first.feed_by_accelerator == {'h200': 3}

        clock.advance(61)
        raw_only = _run_v2({'h200': 11, 'h100': 0})
        assert raw_only is not None
        assert raw_only.observed_free_slots == 11
        assert raw_only.feed_by_accelerator == {'h200': 3}
        assert raw_only.epoch == first.epoch

        clock.advance(61)
        card_changed = _run_v2({'h200': 0, 'h100': 11})
        assert card_changed is not None
        assert card_changed.feed_by_accelerator == {'h100': 3}
        assert card_changed.epoch == first.epoch + 1

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
        # Identical inputs -> identical grants AND feeds -> stable epoch:
        # steady-state fill launches are never fenced out.
        assert same.epoch == first.epoch
        # Reallocation: grant damping needs the new split proposed by two
        # consecutive rounds before it is published. The epoch bumps twice
        # across the window: once when the raw-clamped FEEDS move (round
        # after the weight shift -- feed redistribution already supersedes
        # queued launches), once when the damped grants land.
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
        assert changed.epoch == first.epoch + 2
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
        # Reallocate pool A (weight shift; grant damping needs two rounds;
        # the epoch bumps twice -- feed re-clamp, then the damped grants).
        a_changed = None
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', weight=9)
            _upsert('svc-b')
            _upsert('svc-c', pool_key=pool_b)
            _upsert('svc-d', pool_key=pool_b)
            a_changed = _run('svc-a', free=10)
        assert a_changed is not None
        assert a_changed.epoch == a_first.epoch + 2
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
        assert b_changed.epoch == b_first.epoch + 2
        assert broker.current_epoch(pool_b) == b_changed.epoch
        assert b_first.epoch != broker.current_epoch(pool_b)

    def test_feed_only_redistribution_bumps_epoch(self, clock, monkeypatch):
        rows: list[mock.Mock] = []
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        _upsert('svc-a')
        _upsert('svc-b')
        first = _run('svc-a', free=4)
        assert first is not None
        assert first.grant == 2 and first.feed == 2
        # svc-a materializes its feed: the measured free drops by the
        # same amount, the total is conserved and the damped grants stay
        # {2, 2} -- but the launchable-now split moves entirely to svc-b.
        # A svc-a launch batch queued under the previous round would now
        # spend slots fed to the peer: the epoch must bump on the
        # feed-only change even though grants are identical.
        rows = _live_fill_rows(2)
        clock.advance(61)
        _upsert('svc-a', holdings_fill=2)
        _upsert('svc-b')
        second = _run('svc-a', free=2)
        assert second is not None
        assert second.grant == 2 and second.feed == 0
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.feed == 2
        assert second.epoch == first.epoch + 1

    def test_positive_feed_to_blackout_bumps_epoch(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        first = _run('svc-a', free=4)
        assert first is not None
        assert first.grant == 2 and first.feed == 2
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        blind = _run('svc-a', observation=_obs(None, gpu_names=()))
        assert blind is not None
        # The blackout releases nothing (grants carried) and feeds
        # nothing -- and the positive-feed -> blackout transition is an
        # allocation change: launches queued under the positive round
        # must not execute into an unmeasurable pool.
        assert blind.grant == first.grant
        assert blind.feed == 0
        assert blind.epoch == first.epoch + 1


@pytest.mark.usefixtures('_broker_db')
class TestStaleWriterFence:
    """A writer that lost the round handoff mid-query cannot publish.

    The lease epoch is the writer's ownership token, CAS-advanced and
    committed BEFORE the slow cluster query: a replacement writer's own
    acquisition invalidates it, so the stale writer's publish fails
    closed and its (older) observation is discarded -- even when the
    grants it computed are byte-identical to the published ones (the
    old post-query lease read allowed same-epoch overwrites).
    """

    def test_replacement_writer_supersedes_slow_query(self):
        _upsert('svc-a')
        _upsert('svc-b')
        rounds_table = serve_state.reserved_fill_rounds_table

        def racing_query():
            # Writer B drives a FULL round from inside A's query window
            # (the test lock is inert, so re-entry stands in for "A's
            # advisory-lock session died and B took over"). Two
            # sequential calls on one DB: no real threads needed for the
            # token logic.
            inner = broker.run_round_if_stale('svc-b', _POOL, lambda: _obs(6),
                                              60.0)
            assert inner is not None
            assert inner.feed == 3
            # A dead-gap marker stamped AFTER B's publish (e.g. by a
            # post-expiry writer that then crashed): A's failed publish
            # below must not clear it.
            engine = serve_state._db_manager.get_engine()
            with orm.Session(engine) as session:
                session.execute(
                    sqlalchemy.update(rounds_table).where(
                        rounds_table.c.pool_key == _POOL).values(
                            fence_pending=1))
                session.commit()
            return _obs(10)

        stale = broker.run_round_if_stale('svc-a', _POOL, racing_query, 60.0)
        # A's publish CAS failed: no allocation, observation discarded.
        assert stale is None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        # B's round survives untouched: A's older 10-free observation
        # never overwrote the newer 6-free one, the pool epoch never
        # regressed, and the peer's fence marker survived A's rollback.
        assert int(round_row['round_id']) == 1
        assert int(round_row['last_observed_free']) == 6
        assert sum(json.loads(round_row['feeds']).values()) == 6
        assert bool(round_row['fence_pending'])

    def test_resumed_writer_rereads_state_after_token(self, clock):
        # Token-first ordering: every input the publish persists is read
        # AFTER the ownership token commit. A writer suspended BEFORE its
        # token acquisition (the old code read claims + round first)
        # resumes here: writer B published a full round and a dead-gap
        # marker was stamped in the meantime. A must build on B's round
        # (no per-pool epoch regress, marker honored via a bump), not
        # abort or republish pre-B state.
        _upsert('svc-a')
        _upsert('svc-b')
        rounds_table = serve_state.reserved_fill_rounds_table
        real_acquire = serve_state.acquire_reserved_fill_lease_token
        raced = {'done': False}

        def racing_acquire(**kwargs):
            if not raced['done']:
                raced['done'] = True
                inner = broker.run_round_if_stale('svc-b', _POOL,
                                                  lambda: _obs(6), 60.0)
                assert inner is not None
                engine = serve_state._db_manager.get_engine()
                with orm.Session(engine) as session:
                    session.execute(
                        sqlalchemy.update(rounds_table).where(
                            rounds_table.c.pool_key == _POOL).values(
                                fence_pending=1))
                    session.commit()
                # B's round is fresh by wall clock; A only reached the
                # drive path because the round was absent pre-token.
                clock.advance(61)
                _upsert('svc-a')
                _upsert('svc-b')
            return real_acquire(**kwargs)

        with mock.patch.object(serve_state,
                               'acquire_reserved_fill_lease_token',
                               side_effect=racing_acquire):
            alloc = broker.run_round_if_stale('svc-a', _POOL, lambda: _obs(6),
                                              60.0)
        assert raced['done']
        # A's post-token reads saw B's round: it published on top of it.
        assert alloc is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert int(round_row['round_id']) == 2
        # The pending marker forced the bump (allocation itself was
        # unchanged) and was legitimately cleared by A's publish.
        assert alloc.epoch == 2
        assert not bool(round_row['fence_pending'])


@pytest.mark.usefixtures('_broker_db')
class TestAtomicPersistFence:
    """The launch-path epoch recheck is atomic with the replica persist."""

    _STUB_INFO = _fill_replica()

    def _replica_row_count(self):
        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            return session.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    serve_state.replicas_table)).scalar()

    def test_stale_epoch_not_persisted(self):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        assert not _add_replica_if_round_epoch('svc-a',
                                               1,
                                               self._STUB_INFO,
                                               pool_key=_POOL,
                                               expected_epoch=alloc.epoch - 1)
        assert self._replica_row_count() == 0

    def test_current_epoch_persists_and_missing_round_fails_open(self):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        assert _add_replica_if_round_epoch('svc-a',
                                           1,
                                           self._STUB_INFO,
                                           pool_key=_POOL,
                                           expected_epoch=alloc.epoch)
        # No round row for the pool: fail open, like the pre-check (the
        # claimant must still hold a live claim on that pool).
        pool_b = broker.make_pool_key('other-ctx', 'H100')
        _upsert('svc-c', pool_key=pool_b)
        assert _add_replica_if_round_epoch('svc-c',
                                           2,
                                           _fill_replica(2, pool_b),
                                           pool_key=pool_b,
                                           expected_epoch=99)
        assert self._replica_row_count() == 2

    def test_persist_requires_exact_fill_and_protocol_attribution(self):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None

        assert not _add_replica_if_round_epoch(
            'svc-a', 1, _replica(1), pool_key=_POOL, expected_epoch=alloc.epoch)
        wrong_pool = _fill_replica(
            2, broker.make_pool_key('other-context', 'A100'))
        assert not _add_replica_if_round_epoch(
            'svc-a', 2, wrong_pool, pool_key=_POOL, expected_epoch=alloc.epoch)

        # Even an exact live legacy claim cannot attribute a v1 persist to a
        # key whose embedded identity is protocol v2.
        v2_pool = broker.make_pool_key('alias',
                                       'H200',
                                       protocol_version=broker.PROTOCOL_V2,
                                       physical_cluster_uid='physical-cluster')
        _upsert('svc-v2-key', pool_key=v2_pool)
        assert not _add_replica_if_round_epoch(
            'svc-v2-key',
            3,
            _fill_replica(3, v2_pool),
            pool_key=v2_pool,
            expected_epoch=99,
            expected_protocol_version=broker.PROTOCOL_V1)
        assert self._replica_row_count() == 0

    def test_persist_writes_readable_authoritative_state(self):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        info = _fill_replica(7)

        assert _add_replica_if_round_epoch('svc-a',
                                           7,
                                           info,
                                           pool_key=_POOL,
                                           expected_epoch=alloc.epoch)

        stored = serve_state.get_replica_info_from_id('svc-a', 7)
        assert stored is not None
        assert stored.replica_id == 7
        assert stored.cluster_name == 'svc-7'

        # A second fill admission cannot adopt or overwrite the same numeric
        # key. Recovery instead refreshes the durable object and uses the
        # identity-fenced expected-existing update path.
        with pytest.raises(sqlalchemy_exc.IntegrityError):
            _add_replica_if_round_epoch('svc-a',
                                        7,
                                        _fill_replica(7),
                                        pool_key=_POOL,
                                        expected_epoch=alloc.epoch)
        unchanged = serve_state.get_replica_info_from_id('svc-a', 7)
        assert unchanged is not None
        assert unchanged.replica_record_id == info.replica_record_id
        assert unchanged.version == 1

        unchanged.cluster_name = 'svc-7-retry'
        unchanged.version = 2
        unchanged.created_at = 123.5
        unchanged.is_spot = True
        unchanged.planned_capacity = 4
        unchanged.reserved_fill = True
        unchanged.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        unchanged.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        assert serve_state.add_or_update_replica('svc-a',
                                                 7,
                                                 unchanged,
                                                 expected_replica_exists=True)

        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            row = session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc-a',
                    serve_state.replicas_table.c.replica_id == 7)).one()
        raw = row._mapping  # pylint: disable=protected-access
        assert raw['replica_state_version'] == 1
        assert raw['status'] == unchanged.status.value
        assert (raw['sky_down_status'] ==
                common_utils.ProcessStatus.SCHEDULED.value)
        assert raw['version'] == 2
        assert raw['cluster_name'] == 'svc-7-retry'
        assert raw['created_at'] == 123.5
        assert raw['is_spot'] is True
        assert raw['replica_state'] == unchanged.to_storage_dict()
        assert raw['replica_info'] is None
        stored = serve_state.get_replica_info_from_id('svc-a', 7)
        assert stored is not None
        assert stored.to_storage_dict() == unchanged.to_storage_dict()

    def test_persist_requires_live_same_pool_claim(self):
        # A disabled/pruned claimant's queued fill launch must fence out
        # at persist time instead of starting against a slot the broker
        # no longer attributes to it; a claim moved to another pool
        # fences the same way.
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        # No claim at all (former claimant, rows-only service).
        assert not _add_replica_if_round_epoch('svc-gone',
                                               1,
                                               self._STUB_INFO,
                                               pool_key=_POOL,
                                               expected_epoch=alloc.epoch)
        # Claim moved to a different pool.
        _upsert('svc-moved', pool_key=broker.make_pool_key('other', 'H100'))
        assert not _add_replica_if_round_epoch('svc-moved',
                                               1,
                                               self._STUB_INFO,
                                               pool_key=_POOL,
                                               expected_epoch=alloc.epoch)
        assert self._replica_row_count() == 0
        # The live claimant itself still persists.
        assert _add_replica_if_round_epoch('svc-a',
                                           1,
                                           self._STUB_INFO,
                                           pool_key=_POOL,
                                           expected_epoch=alloc.epoch)
        assert self._replica_row_count() == 1

    def test_round_published_between_precheck_and_persist_fences(
            self, monkeypatch):
        # The cheap pre-check read passes, then a new round publishes
        # BEFORE the row persist (injected via the session hook -- the
        # same pattern as the prune-race test): the persist must see the
        # new epoch and write nothing. The injection point is the last
        # pre-fence statement of each dialect's shape: PostgreSQL's
        # two-step fence hooks the FOR SHARE round SELECT (no lock held
        # yet when the hook fires); sqlite's single conditional INSERT
        # hooks the INSERT itself (the epoch predicate is evaluated
        # inside that very statement, so a publish landing just before
        # it must fence).
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        carried = alloc.epoch
        assert broker.current_epoch(_POOL) == carried  # pre-check passes
        real_execute = orm.Session.execute
        raced = {'done': False}
        rounds_table = serve_state.reserved_fill_rounds_table

        def racing_execute(session, statement, *args, **kwargs):
            fence_statement = ((isinstance(statement, sqlalchemy.Select) and
                                'reserved_fill_rounds' in str(statement)) or
                               (isinstance(statement, dml.Insert) and
                                statement.table.name == 'replicas'))
            if not raced['done'] and fence_statement:
                raced['done'] = True
                engine = serve_state._db_manager.get_engine()
                with orm.Session(engine) as other:
                    real_execute(
                        other,
                        sqlalchemy.update(rounds_table).where(
                            rounds_table.c.pool_key == _POOL).values(
                                epoch=carried + 1))
                    other.commit()
            return real_execute(session, statement, *args, **kwargs)

        monkeypatch.setattr(orm.Session, 'execute', racing_execute)
        assert not _add_replica_if_round_epoch(
            'svc-a', 1, self._STUB_INFO, pool_key=_POOL, expected_epoch=carried)
        assert raced['done']
        assert self._replica_row_count() == 0


@pytest.mark.usefixtures('_broker_db')
class TestRoundPersistExclusion:
    """Fill persists and broker rounds mutually exclude via the round lock.

    A persist landing inside a round's scan->publish window is counted by
    neither the completed debit scan nor the not-yet-bumped epoch fence:
    the broker would re-feed the just-taken slot to a peer. The persist
    therefore takes the same cross-process lock the round holds for its
    whole body, non-blocking: contention degrades into a fence-skip
    (False, retried next tick), so a persist lands either before the scan
    (counted -- pinned by the live_fill/debit tests) or after the publish
    (fenced by the bumped epoch).
    """

    _STUB_INFO = _fill_replica()

    def _replica_row_count(self):
        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            return session.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    serve_state.replicas_table)).scalar()

    def test_persist_skips_while_round_holds_the_lock(self, monkeypatch):
        # Use the real lock for the active database backend (the fixture's
        # inert lock cannot contend).  PostgreSQL persists must mint their
        # fencing token on the exact advisory-lock session, so substituting a
        # FileLock there would correctly fail closed even after its release.
        lock_id = f'test-broker-round-{id(self)}'
        engine = serve_state._db_manager.get_engine()
        if engine.dialect.name == 'postgresql':
            monkeypatch.setattr(locks.global_user_state,
                                'initialize_and_get_db', lambda: engine)

            def lock_factory(*args, **kwargs):
                del args
                return locks.PostgresLock(lock_id,
                                          timeout=kwargs.get('timeout'))

            round_lock = locks.PostgresLock(lock_id)
        else:

            def lock_factory(*args, **kwargs):
                del args
                return locks.FileLock(lock_id, timeout=kwargs.get('timeout'))

            round_lock = locks.FileLock(lock_id)
        monkeypatch.setattr(broker.locks, 'get_lock', lock_factory)
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        with round_lock.acquire():  # a round is in flight
            assert not broker.persist_fill_replica('svc-a',
                                                   1,
                                                   self._STUB_INFO,
                                                   pool_key=_POOL,
                                                   expected_epoch=alloc.epoch)
            assert self._replica_row_count() == 0
        # Lock released (round published nothing new): the same persist
        # goes through.
        assert broker.persist_fill_replica('svc-a',
                                           1,
                                           self._STUB_INFO,
                                           pool_key=_POOL,
                                           expected_epoch=alloc.epoch)
        assert self._replica_row_count() == 1
        # After a publish that bumped the epoch, a stale-epoch persist is
        # fenced even with the lock free.
        engine = serve_state._db_manager.get_engine()
        rounds_table = serve_state.reserved_fill_rounds_table
        with orm.Session(engine) as session:
            session.execute(
                sqlalchemy.update(rounds_table).where(
                    rounds_table.c.pool_key == _POOL).values(epoch=alloc.epoch +
                                                             1))
            session.commit()
        assert not broker.persist_fill_replica('svc-a',
                                               2,
                                               _fill_replica(2),
                                               pool_key=_POOL,
                                               expected_epoch=alloc.epoch)
        assert self._replica_row_count() == 1


@pytest.mark.usefixtures('_broker_db')
class TestFencePendingFailsClosed:
    """A pending dead-gap marker fences actuation until a publish clears it.

    The epoch alone cannot fence a pool whose marker can never be cleared
    (claims gone -> no round is ever published again): a stalled
    controller's pre-gap decision would pass the epoch check forever.
    Both the cheap pre-check read (current_epoch) and the atomic persist
    must fail closed while the marker is set.
    """

    _STUB_INFO = _fill_replica()

    def _replica_row_count(self):
        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            return session.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    serve_state.replicas_table)).scalar()

    def test_marker_fences_precheck_and_persist_until_publish(self, clock):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        engine = serve_state._db_manager.get_engine()
        rounds_table = serve_state.reserved_fill_rounds_table
        with orm.Session(engine) as session:
            session.execute(
                sqlalchemy.update(rounds_table).where(
                    rounds_table.c.pool_key == _POOL).values(fence_pending=1))
            session.commit()
        # Pre-check read: the sentinel mismatches any carried epoch.
        epoch_read = broker.current_epoch(_POOL)
        assert epoch_read is not None and epoch_read != alloc.epoch
        # Atomic persist: refused, nothing written -- even with the
        # matching epoch.
        assert not _add_replica_if_round_epoch('svc-a',
                                               1,
                                               self._STUB_INFO,
                                               pool_key=_POOL,
                                               expected_epoch=alloc.epoch)
        assert self._replica_row_count() == 0
        # An epoch-bumping publish clears the marker; the NEW epoch
        # actuates again (the old one stays fenced by the bump itself).
        clock.advance(61)
        _upsert('svc-a')
        _upsert('svc-b')
        fresh = _run('svc-a', free=4)
        assert fresh is not None
        assert fresh.epoch == alloc.epoch + 1
        assert broker.current_epoch(_POOL) == fresh.epoch
        assert _add_replica_if_round_epoch('svc-a',
                                           1,
                                           self._STUB_INFO,
                                           pool_key=_POOL,
                                           expected_epoch=fresh.epoch)
        assert self._replica_row_count() == 1


@pytest.mark.usefixtures('_broker_db')
class TestOrphanFillRowDebit:
    """Former claimants' fill rows stay visible to the round debit.

    A disabled/pruned/moved claimant's nonterminal pool-matched fill row
    (e.g. a queued launch not yet bound) no longer belongs to any
    claimant's holdings, but its slot must not be fed to a peer: the scan
    covers ALL services with replica rows and attributes such rows to the
    unclaimed occupancy/draining terms.
    """

    def test_orphan_pending_row_debits_feed_and_conserves_total(
            self, monkeypatch):
        # svc-gone claimed the pool once, launched a fill replica that is
        # still unbound (not READY -> its slot still reads free in the
        # measurement), then lost its claim.
        orphan_rows = [
            _replica_stub(is_ready=False, reserved_fill=True, created_at=1.0)
        ]
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': orphan_rows}))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        # The orphan's slot is NOT fed to the peers: 4 observed - 1
        # occupying orphan row = 3 spendable.
        assert sum(json.loads(round_row['feeds']).values()) == 3
        # ... but it is conserved in the entitlement total (4 free + 1
        # unclaimed = 5 granted), like a drainer: granted capacity the
        # orphan's teardown will eventually free.
        grants = json.loads(round_row['grants'])
        assert sum(grants.values()) == 5
        assert set(grants) == {'svc-a', 'svc-b'}

    def test_former_claimant_drainer_counts_into_total(self, monkeypatch):
        # A former claimant's graceful drainer (SHUTTING_DOWN, launched)
        # keeps occupying the pool: previously invisible (the scan only
        # covered current claimants), undercounting the total.
        orphan_rows = [
            _replica_stub(is_ready=False,
                          is_terminal=True,
                          status=serve_state.ReplicaStatus.SHUTTING_DOWN,
                          reserved_fill=True,
                          created_at=1.0)
        ]
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped',
                            mock.Mock(return_value={'svc-gone': orphan_rows}))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=0)
        assert alloc is not None
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        # total = 0 free + 0 holdings + 1 unclaimed drainer.
        assert sum(json.loads(round_row['grants']).values()) == 1
        # Nothing is feedable: the drainer's pod still holds its slot.
        assert sum(json.loads(round_row['feeds']).values()) == 0


@pytest.mark.usefixtures('_broker_db')
class TestReplicaSnapshotDebit:
    """The normal broker path consumes one row-consistent replica snapshot."""

    def test_snapshot_covers_claimants_and_former_claimants(self, monkeypatch):
        claimant_row = _replica_stub(reserved_fill=True, created_at=1.0)
        orphan_row = _replica_stub(is_ready=False,
                                   reserved_fill=True,
                                   created_at=1.0)
        snapshot = mock.Mock(return_value={
            'svc-a': [claimant_row],
            'svc-gone': [orphan_row],
        })
        monkeypatch.setattr(serve_state, 'get_replica_infos_grouped', snapshot)
        legacy_names = mock.Mock(side_effect=AssertionError('legacy query'))
        legacy_infos = mock.Mock(side_effect=AssertionError('legacy query'))
        monkeypatch.setattr(serve_state, 'get_replica_service_names',
                            legacy_names)
        monkeypatch.setattr(serve_state, 'get_replica_infos', legacy_infos)

        result = broker._occupying_debit(['svc-a', 'svc-b'], _POOL, 10.0)

        assert result == (1, 0, {'a100': 1}, {'svc-a': 1, 'svc-b': 0}, 1)
        snapshot.assert_called_once_with()
        legacy_names.assert_not_called()
        legacy_infos.assert_not_called()

    def test_snapshot_failure_falls_back_per_service(self, monkeypatch):
        monkeypatch.setattr(
            serve_state, 'get_replica_infos_grouped',
            mock.Mock(side_effect=ValueError('corrupt grouped row')))
        monkeypatch.setattr(serve_state, 'get_replica_service_names',
                            mock.Mock(return_value=['svc-gone', 'svc-bad']))
        claimant_row = _replica_stub(reserved_fill=True, created_at=1.0)
        orphan_row = _replica_stub(is_ready=False,
                                   reserved_fill=True,
                                   created_at=1.0)

        def _isolated_read(name):
            if name == 'svc-bad':
                raise ValueError('corrupt isolated row')
            if name == 'svc-a':
                return [claimant_row]
            if name == 'svc-gone':
                return [orphan_row]
            return []

        isolated_read = mock.Mock(side_effect=_isolated_read)
        monkeypatch.setattr(serve_state, 'get_replica_infos', isolated_read)

        result = broker._occupying_debit(['svc-a', 'svc-bad'], _POOL, 10.0)

        assert result == (1, 0, {'a100': 1}, {'svc-a': 1}, 1)
        assert {call.args[0] for call in isolated_read.call_args_list
               } == {'svc-a', 'svc-bad', 'svc-gone'}


@pytest.mark.usefixtures('_broker_db')
class TestSqliteFenceBusySkip:
    """SQLITE_BUSY-family failures degrade into a fence-skip, not a raise.

    sqlite-only semantics, deliberately NOT re-collected in the PG module:
    the PostgreSQL fence never returns False on lock contention (it blocks
    on the FOR SHARE row lock instead). A busy database at persist time
    must read as "fence held" (launch re-emitted next tick), never as an
    exception aborting the whole scale-up batch.
    """

    def test_busy_error_returns_false_without_raising(self, monkeypatch):
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        assert alloc is not None
        monkeypatch.setattr(serve_state, '_SQLITE_FENCE_BUSY_BACKOFF_SECONDS',
                            0.0)
        real_execute = orm.Session.execute
        attempts = {'count': 0}

        def busy_execute(session, statement, *args, **kwargs):
            if (isinstance(statement, dml.Insert) and
                    statement.table.name == 'replicas'):
                attempts['count'] += 1
                raise sqlalchemy_exc.OperationalError(
                    'INSERT INTO replicas', {}, Exception('database is locked'))
            return real_execute(session, statement, *args, **kwargs)

        monkeypatch.setattr(orm.Session, 'execute', busy_execute)
        assert not serve_state.add_replica_if_round_epoch(
            'svc-a',
            1,
            _fill_replica(),
            pool_key=_POOL,
            expected_epoch=alloc.epoch)
        assert attempts['count'] == serve_state._SQLITE_FENCE_BUSY_RETRIES
        monkeypatch.setattr(orm.Session, 'execute', real_execute)
        engine = serve_state._db_manager.get_engine()
        with orm.Session(engine) as session:
            count = session.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    serve_state.replicas_table)).scalar()
        assert count == 0


@pytest.mark.usefixtures('_broker_db')
class TestExpiredLeaseFenceMarker:
    """A dead-gap epoch bump survives an aborted post-expiry writer.

    Acquiring the lease token commits a fresh expires_at, consuming the
    expiry evidence: a post-expiry writer that dies before publishing
    would otherwise leave the next writer seeing an unexpired lease and
    (with unchanged grants/feeds) republishing the old pool epoch --
    letting launches queued before the dead gap keep passing the fence.
    The persisted per-pool fence_pending marker, set atomically with the
    post-expiry acquisition and cleared only by a successful publish,
    forces the bump regardless of which writer finally publishes.
    """

    _LEASE_TTL = serve_constants.RESERVED_FILL_LEASE_TTL_INTERVALS * 60.0

    def _upsert_all(self, pool_b=None):
        _upsert('svc-a')
        _upsert('svc-b')
        if pool_b is not None:
            _upsert('svc-c', pool_key=pool_b)
            _upsert('svc-d', pool_key=pool_b)

    def test_aborted_post_expiry_writer_still_forces_bump(
            self, clock, monkeypatch):
        pool_b = broker.make_pool_key('other-ctx', 'H100')
        obs_b = _obs(10, gpu_names=('H100',))
        self._upsert_all(pool_b)
        first = _run('svc-a', free=10)
        b_first = _run('svc-c', observation=obs_b, pool=pool_b)
        assert first is not None and b_first is not None
        # The lease dies: no rounds at all for longer than its TTL.
        clock.advance(self._LEASE_TTL + 61)
        self._upsert_all()
        # Writer A acquires the post-expiry token (committing a fresh
        # expires_at) and crashes before publishing.
        real_publish = serve_state.publish_reserved_fill_round
        monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                            mock.Mock(side_effect=RuntimeError('crashed')))
        with pytest.raises(RuntimeError):
            _run('svc-a', free=10)
        monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                            real_publish)
        # The marker survived the aborted writer, on EVERY pool's row.
        for pool in (_POOL, pool_b):
            row = serve_state.get_reserved_fill_round(pool)
            assert row is not None and bool(row['fence_pending'])
        # Writer B sees an UNEXPIRED lease (A refreshed it) and computes
        # unchanged grants/feeds -- the marker alone must force the bump.
        self._upsert_all()
        second = _run('svc-b', free=10)
        assert second is not None
        assert second.epoch == first.epoch + 1
        row = serve_state.get_reserved_fill_round(_POOL)
        assert row is not None and not bool(row['fence_pending'])
        # Multi-pool: the other pool's next publish (also on an unexpired
        # lease, also unchanged allocation) must bump its OWN epoch too,
        # then clear its marker.
        _upsert('svc-c', pool_key=pool_b)
        _upsert('svc-d', pool_key=pool_b)
        b_second = _run('svc-c', observation=obs_b, pool=pool_b)
        assert b_second is not None
        assert b_second.epoch == b_first.epoch + 1
        row_b = serve_state.get_reserved_fill_round(pool_b)
        assert row_b is not None and not bool(row_b['fence_pending'])
        # Once cleared, unchanged rounds keep a stable epoch again.
        clock.advance(61)
        self._upsert_all()
        third = _run('svc-a', free=10)
        assert third is not None
        assert third.epoch == second.epoch

    def test_non_expiry_abort_does_not_force_bump(self, clock, monkeypatch):
        self._upsert_all()
        first = _run('svc-a', free=10)
        assert first is not None
        # A writer crashes pre-publish WITHOUT a preceding dead gap (the
        # lease is still live): no marker, and the next unchanged publish
        # must not spuriously bump the epoch.
        clock.advance(61)
        self._upsert_all()
        real_publish = serve_state.publish_reserved_fill_round
        monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                            mock.Mock(side_effect=RuntimeError('crashed')))
        with pytest.raises(RuntimeError):
            _run('svc-a', free=10)
        monkeypatch.setattr(serve_state, 'publish_reserved_fill_round',
                            real_publish)
        row = serve_state.get_reserved_fill_round(_POOL)
        assert row is not None and not bool(row['fence_pending'])
        self._upsert_all()
        second = _run('svc-b', free=10)
        assert second is not None
        assert second.epoch == first.epoch


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
        rows: list[mock.Mock] = []
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
            _replica_stub(is_ready=False,
                          reserved_fill=True,
                          created_at=created) for _ in range(2)
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
            _replica_stub(is_ready=True, reserved_fill=True, created_at=created)
            for _ in range(2)
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


@pytest.mark.usefixtures('_broker_db')
class TestDrainWindowConservation:
    """Draining fill replicas must not vanish from the pool total.

    Regression: a culled zero-cost fill replica turns SHUTTING_DOWN
    (terminal -> excluded from its owner's claimed holdings) while its pod
    stays bound for the whole graceful drain (excluded from the measured
    free too), so the round total undercounted by every drainer AND the
    holdings drop read as "pods physically gone", firing the immediate
    down-move bypass -- a rebalance then culled warm replicas below the
    allocation fixpoint (killed 10 where 6 was correct, refilled after).
    """

    def _draining_stub(self, launched=True):
        return _replica_stub(is_ready=False,
                             is_terminal=True,
                             status=serve_state.ReplicaStatus.SHUTTING_DOWN,
                             reserved_fill=True,
                             created_at=1.0,
                             launched=launched)

    def test_grants_hold_the_fixpoint_while_drains_are_in_flight(
            self, clock, monkeypatch):
        rows = {
            'svc-a': _live_fill_rows(10),
            'svc-b': _live_fill_rows(10),
        }
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows.get(name, [])))
        # Steady state: pool of 20, svc-a and svc-b hold 10 fill each.
        _upsert('svc-a', holdings_fill=10)
        _upsert('svc-b', holdings_fill=10)
        steady = _run('svc-a', free=0)
        assert steady is not None and steady.grant == 10
        # svc-c arrives with floor 8: fixpoint is a=4, b=4, c=12. The
        # down-move persists across two rounds (damping), then a and b
        # each cull exactly 6 replicas.
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', holdings_fill=10)
            _upsert('svc-b', holdings_fill=10)
            _upsert('svc-c', floor=8)
            alloc = _run('svc-a', free=0)
            assert alloc is not None
        assert alloc.grant == 4
        # The 12 culled replicas enter their graceful drain: terminal (out
        # of claimed holdings) but their pods stay bound, so the measured
        # free stays 0 for multiple broker rounds.
        rows = {
            'svc-a': _live_fill_rows(4) +
                     [self._draining_stub() for _ in range(6)],
            'svc-b': _live_fill_rows(4) +
                     [self._draining_stub() for _ in range(6)],
        }
        for _ in range(2):
            clock.advance(61)
            _upsert('svc-a', holdings_fill=4)
            _upsert('svc-b', holdings_fill=4)
            _upsert('svc-c', floor=8)
            draining = _run('svc-a', free=0)
            # The drainers still count into the pool total: grants hold
            # the fixpoint (no spurious immediate down below it), and
            # nothing is fed out of thin air.
            assert draining is not None
            assert draining.grant == 4 and draining.feed == 0
            alloc_c = broker.get_my_allocation('svc-c')
            assert alloc_c is not None
            assert alloc_c.grant == 12 and alloc_c.feed == 0
        # Drains complete: the pods are actually gone, the freed slots
        # show up as measured free, and svc-c gets fed to its grant.
        rows = {'svc-a': _live_fill_rows(4), 'svc-b': _live_fill_rows(4)}
        clock.advance(61)
        _upsert('svc-a', holdings_fill=4)
        _upsert('svc-b', holdings_fill=4)
        _upsert('svc-c', floor=8)
        settled = _run('svc-a', free=12)
        assert settled is not None
        assert settled.grant == 4 and settled.feed == 0
        alloc_c = broker.get_my_allocation('svc-c')
        assert alloc_c is not None
        assert alloc_c.grant == 12 and alloc_c.feed == 12

    def test_draining_demand_and_failed_cleanup_rows_do_not_count(
            self, monkeypatch):
        # Scope of the conservation fix: only SHUTTING_DOWN rows with
        # reserved_fill=True join the total. A draining DEMAND row's pod
        # also occupies the pool, but demand rows were never in holdings
        # and their bound pod was already excluded from the measured free
        # while LIVE -- the pre-drain steady state undercounted them the
        # same way by design (demand capacity is not fill-arbitrable).
        # FAILED_CLEANUP rows persist indefinitely and would over-count
        # the pool once the pod eventually dies.
        rows = [
            _replica_stub(is_ready=False,
                          is_terminal=True,
                          status=serve_state.ReplicaStatus.SHUTTING_DOWN,
                          reserved_fill=False,
                          created_at=1.0),
            _replica_stub(is_ready=False,
                          is_terminal=True,
                          status=serve_state.ReplicaStatus.FAILED_CLEANUP,
                          reserved_fill=True,
                          created_at=1.0),
        ]
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        # total = 4 free + 0 holdings + 0 draining: neither terminal row
        # inflates the split.
        assert alloc is not None
        assert alloc.grant == 2 and alloc.feed == 2
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 2 and alloc_b.feed == 2

    def test_stale_claim_holdings_not_double_counted_with_drainers(
            self, monkeypatch):
        # svc-a's claim was heartbeated BEFORE 6 of its 10 fill replicas
        # entered their graceful drain: the claim still reports holdings
        # 10 while the live row scan sees 4 nonterminal + 6 SHUTTING_DOWN
        # (bound). The round must use the row-consistent 4 + 6 = 10
        # conserved total -- summing the stale claim with the scanned
        # drainers (10 + 6 = 16) would over-grant until svc-a's next
        # heartbeat.
        rows = _live_fill_rows(4) + [self._draining_stub() for _ in range(6)]
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        _upsert('svc-a', holdings_fill=10)  # stale: drains started after
        _upsert('svc-b')
        alloc = _run('svc-a', free=0)
        # total = 0 free + 4 live + 6 draining = 10 -> equal-weight
        # fixpoint 5/5 (the inflated total 16 would grant 8/8), and no
        # feed materializes out of thin air.
        assert alloc is not None
        assert alloc.grant == 5 and alloc.feed == 0
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 5 and alloc_b.feed == 0

    def test_interrupted_unbound_fill_row_is_not_a_drainer(self, monkeypatch):
        # A launch-cancelled fill row (sky.launch INTERRUPTED -> maps to
        # SHUTTING_DOWN) may never have bound a pod, so the measured free
        # still counts its slot; also treating the row as a drainer would
        # add phantom capacity to the pool total. It counts NOWHERE.
        rows = [self._draining_stub(launched=False)]
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows if name == 'svc-a' else []))
        _upsert('svc-a')
        _upsert('svc-b')
        alloc = _run('svc-a', free=4)
        # total = 4 free + 0 holdings + 0 draining (the unbound row's
        # slot is already in the measured free): 2/2, fully feedable.
        assert alloc is not None
        assert alloc.grant == 2 and alloc.feed == 2
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None
        assert alloc_b.grant == 2 and alloc_b.feed == 2

    def test_one_round_phantom_shrink_does_not_bypass_damping(
            self, clock, monkeypatch):
        rows = {'svc-a': _live_fill_rows(5), 'svc-b': _live_fill_rows(5)}
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows.get(name, [])))
        _upsert('svc-a', holdings_fill=5)
        _upsert('svc-b', holdings_fill=5)
        steady = _run('svc-a', free=0)
        assert steady is not None and steady.grant == 5
        # One svc-a drain completes AFTER the cluster query counted its
        # slot occupied but BEFORE the row scan deleted the row: the
        # conserved sum reads 9 while the freed slot is missing from
        # this round's measured free too -- a one-round observation
        # artifact, not capacity that physically vanished. The
        # immediate-down bypass must NOT fire on this unconfirmed
        # shrink; the round only records the pre-shrink baseline.
        rows = {'svc-a': _live_fill_rows(4), 'svc-b': _live_fill_rows(5)}
        clock.advance(61)
        _upsert('svc-a', holdings_fill=4)
        _upsert('svc-b', holdings_fill=5)
        phantom = _run('svc-a', free=0)
        assert phantom is not None
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 5  # damping held
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert int(round_row['shrink_baseline']) == 10
        # Next round the freed slot shows up in the measured free: the
        # total recovers to 10, so even though the holdings-level shrink
        # is now confirmed (the replica really is gone), the raw
        # entitlements are back at the fixpoint and nobody is culled.
        clock.advance(61)
        _upsert('svc-a', holdings_fill=4)
        _upsert('svc-b', holdings_fill=5)
        recovered = _run('svc-a', free=1)
        assert recovered is not None
        assert recovered.grant == 5
        assert recovered.feed == 1  # the visible slot refills svc-a
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 5
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert round_row['shrink_baseline'] is None  # resolved

    def test_two_round_confirmed_shrink_bypasses_damping(
            self, clock, monkeypatch):
        rows = {'svc-a': _live_fill_rows(5), 'svc-b': _live_fill_rows(5)}
        monkeypatch.setattr(
            serve_state, 'get_replica_infos',
            mock.Mock(side_effect=lambda name: rows.get(name, [])))
        _upsert('svc-a', holdings_fill=5)
        _upsert('svc-b', holdings_fill=5)
        steady = _run('svc-a', free=0)
        assert steady is not None and steady.grant == 5
        # svc-a's pods start physically vanishing (external preemption,
        # nothing shows up as free). First shrunken scan (conserved 8):
        # unconfirmed, damping holds the published grants.
        rows = {'svc-a': _live_fill_rows(3), 'svc-b': _live_fill_rows(5)}
        clock.advance(61)
        _upsert('svc-a', holdings_fill=3)
        _upsert('svc-b', holdings_fill=5)
        pending = _run('svc-a', free=0)
        assert pending is not None
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 5
        # Second consecutive shrunken scan (conserved 6 < baseline 10):
        # CONFIRMED -- the bypass fires and the down applies immediately
        # at the raw entitlement (3/3 of total 6). The ordinary two-round
        # damped path would only act on max(proposed, last proposed) =
        # 4: the bypass is observably faster on a confirmed shrink.
        rows = {'svc-a': _live_fill_rows(1), 'svc-b': _live_fill_rows(5)}
        clock.advance(61)
        _upsert('svc-a', holdings_fill=1)
        _upsert('svc-b', holdings_fill=5)
        confirmed = _run('svc-a', free=0)
        assert confirmed is not None
        assert confirmed.grant == 3
        alloc_b = broker.get_my_allocation('svc-b')
        assert alloc_b is not None and alloc_b.grant == 3
        round_row = serve_state.get_reserved_fill_round(_POOL)
        assert round_row is not None
        assert round_row['shrink_baseline'] is None  # consumed


class TestAdvanceReleaseTarget:
    """The release governor: rise fast, release slowly and reversibly."""

    _KW = dict(dwell=300.0,
               step_seconds=300.0,
               step_fraction=0.25,
               min_step=2,
               headroom=0.25,
               blind_grace=900.0)

    def _advance(self, prev, **kwargs):
        params = dict(floor=0,
                      holdings=0,
                      need=0,
                      boot_hold=False,
                      blind=False,
                      now=0.0)
        params.update(kwargs)
        return broker.advance_release_target(prev, **params, **self._KW)

    def test_first_round_anchors_on_holdings_and_starts_the_dwell(self):
        entry = self._advance(None, floor=16, holdings=77, now=1000.0)
        assert entry['cap'] == 77
        assert entry['hot_until'] == 1300.0

    def test_rise_is_immediate_and_ignores_the_step_schedule(self):
        # A burst must reclaim entitlement in ONE round: waiting out a dwell
        # or a step window would make reacquisition slower than the cold
        # start the floor exists to avoid.
        prev = {
            'cap': 16,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None
        }
        entry = self._advance(prev, floor=16, holdings=16, need=40, now=5000.0)
        # 40 * 1.25 headroom.
        assert entry['cap'] == 50
        assert entry['hot_until'] == 5300.0

    def test_nonzero_need_restores_utilization_proportional_cap(self):
        prev = {
            'cap': 0,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None
        }
        entry = self._advance(prev, floor=0, holdings=0, need=1, now=5000.0)
        assert entry['cap'] == 2
        assert entry['hot_until'] == 5300.0

    def test_activity_backed_floor_releases_fully_while_idle(self):
        prev = {
            'cap': 70,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None
        }
        now = 1000.0
        for _ in range(50):
            now += 400.0
            prev = self._advance(prev,
                                 floor=0,
                                 holdings=int(prev['cap']),
                                 need=0,
                                 now=now)
            if prev['cap'] == 0:
                break
        assert prev['cap'] == 0

    def test_dwell_blocks_the_first_step_in_wall_clock(self):
        prev = {
            'cap': 77,
            'hot_until': 1300.0,
            'stepped_at': 1000.0,
            'blind_since': None
        }
        # A skipped or delayed round must not shorten the dwell.
        assert self._advance(prev, floor=16, holdings=77,
                             now=1299.0)['cap'] == 77
        assert self._advance(prev, floor=16, holdings=77,
                             now=1301.0)['cap'] < 77

    def test_boot_hold_blocks_a_step_and_extends_the_dwell(self):
        # Pre-ready fill rows are the FIRST scale-down victims, so stepping
        # while they boot would cull exactly the capacity just ordered.
        prev = {
            'cap': 77,
            'hot_until': 1300.0,
            'stepped_at': 1000.0,
            'blind_since': None
        }
        entry = self._advance(prev,
                              floor=16,
                              holdings=77,
                              boot_hold=True,
                              now=2000.0)
        assert entry['cap'] == 77
        assert entry['hot_until'] == 2300.0

    def test_unactuated_step_blocks_the_next_one(self):
        # holdings above the cap means the previous step has not drained.
        prev = {
            'cap': 40,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None
        }
        entry = self._advance(prev, floor=16, holdings=60, now=5000.0)
        assert entry['cap'] == 40

    def test_step_rate_and_convergence_to_the_floor(self):
        floor = 16
        cap = 77
        now = 1000.0
        prev = {
            'cap': cap,
            'hot_until': now,
            'stepped_at': now - 400.0,
            'blind_since': None
        }
        seen = [cap]
        for _ in range(50):
            now += 400.0
            prev = self._advance(prev,
                                 floor=floor,
                                 holdings=prev['cap'],
                                 now=now)
            seen.append(prev['cap'])
            if prev['cap'] == floor:
                break
        assert seen[1] == 61  # 77 - ceil(0.25 * 61)
        assert seen[-1] == floor
        assert all(b <= a for a, b in zip(seen, seen[1:]))  # monotone
        # min_step guarantees termination rather than a 1-replica tail.
        assert len(seen) < 20

    def test_never_steps_below_the_floor(self):
        prev = {
            'cap': 17,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None
        }
        entry = self._advance(prev, floor=16, holdings=17, now=5000.0)
        assert entry['cap'] == 16

    def test_blind_freezes_without_raising_or_lowering(self):
        # Every serve controller lives in the api-server pod, so one deploy
        # blinds the whole pool at once. Raising here would reset every
        # in-progress decay on every deploy; lowering would release capacity
        # from a fleet that may be fully busy.
        prev = {
            'cap': 40,
            'hot_until': 1000.0,
            'stepped_at': 1000.0,
            'blind_since': None
        }
        entry = self._advance(prev,
                              floor=16,
                              holdings=77,
                              blind=True,
                              now=5000.0)
        assert entry['cap'] == 40
        assert entry['blind_since'] == 5000.0
        assert entry['hot_until'] == 5300.0

    def test_blind_past_grace_resumes_the_decay(self):
        # A permanently wedged telemetry path must not pin the pool.
        prev = {
            'cap': 40,
            'hot_until': 1000.0,
            'stepped_at': 1000.0,
            'blind_since': 1000.0
        }
        entry = self._advance(prev,
                              floor=16,
                              holdings=40,
                              blind=True,
                              now=1000.0 + 901.0)
        assert entry['cap'] < 40

    def test_continuous_blindness_resumes_and_completes_the_decay(self):
        # SEQUENTIAL regression for the wedged-telemetry escape hatch. The
        # single-call test above hand-crafts a prev with a stale hot_until and
        # a pre-set blind_since, a state the real round-over-round dynamics
        # never reach: the freeze branch pushes hot_until to now + dwell every
        # blind round, and resetting blind_since to None on the past-grace
        # return re-arms the grace window, so the one round that crosses grace
        # lands back in the dwell branch and re-freezes -- forever. Feeding
        # each output as the next input (holdings tracking the cap, so the
        # actuation gate never fires) is the only way to exercise it. Before
        # the fix the cap stays pinned at its start value indefinitely; after
        # it, the decay resumes past blind_grace and walks to the floor.
        prev = {
            'cap': 40,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None,
        }
        now = 1000.0
        # 80 rounds * 60s = 4800s, ~5x blind_grace (900s) and well past the
        # full 40 -> 16 step schedule.
        for _ in range(80):
            prev = self._advance(prev,
                                 floor=16,
                                 holdings=int(prev['cap']),
                                 need=0,
                                 blind=True,
                                 now=now)
            now += 60.0
        assert prev['cap'] == 16, (
            'a permanently blind claimant must resume the decay past '
            f'blind_grace and reach the floor, got cap={prev["cap"]}')

    def _drive_intermittent_blind(self,
                                  cadence_rounds,
                                  *,
                                  rounds=400,
                                  poll=60.0,
                                  floor=16,
                                  start_cap=40):
        """Idle-when-seen claimant with one blind round every cadence_rounds.

        Each round reads need==0 whenever it is seen; only the periodic blind
        round hides the (still zero) signal. Holdings track the cap so the
        actuation gate never masks the schedule -- the only brake exercised is
        the blind freeze itself. Returns the final cap.
        """
        prev = {
            'cap': start_cap,
            'hot_until': 0.0,
            'stepped_at': 0.0,
            'blind_since': None,
        }
        now = 1000.0
        for i in range(rounds):
            prev = self._advance(prev,
                                 floor=floor,
                                 holdings=int(prev['cap']),
                                 need=0,
                                 blind=(i % cadence_rounds == 0),
                                 now=now)
            now += poll
        return int(prev['cap'])

    def test_intermittent_blindness_within_the_dwell_stalls_the_release(self):
        # KNOWN CONSERVATIVE LIMITATION, pinned so a future change is a visible
        # diff. The blind freeze both restarts the dwell (hot_until = now +
        # dwell) and pauses the step clock (stepped_at = now). A blind round
        # recurring within the dwell window therefore rewinds the schedule
        # before it can complete: an idle-whenever-seen claimant NEVER
        # releases, even though the grace escape (test above) never triggers
        # because blind_since is cleared on every seen round. At poll 60s the
        # dwell (300s) is five rounds, so any blind cadence <= 5 rounds pins
        # the cap. This errs on the safe side (a possibly-busy service keeps
        # its capacity, never over-released) but silently defeats reclamation
        # under a flapping-telemetry / crash-looping-LB service -- see the
        # limitation note in docs/designs/serve-reserved-fill-utilization-gate
        # .md. The rollout gate that watches blind DURATION (<= one poll
        # interval) does not catch it, because the cadence, not the length, of
        # the blind rounds is what stalls the release.
        assert self._drive_intermittent_blind(4) == 40, (
            'a blind round recurring within the dwell must stall the release '
            'at the start cap (documented conservative behavior)')

    def test_intermittent_blindness_spaced_past_the_dwell_still_releases(self):
        # The complement of the boundary: once seen-idle runs longer than the
        # dwell AND the step schedule (both 300s = 5 rounds at poll 60s), a
        # periodic blind blip only delays the decay, it does not defeat it. A
        # cadence of 8 rounds (480s) leaves clean windows to complete the
        # dwell and each step, so the cap still walks to the floor.
        assert self._drive_intermittent_blind(8) == 16, (
            'blind rounds spaced beyond the dwell must not pin an idle '
            'claimant -- the decay resumes and reaches the floor')


class TestUtilizationCapEntitlements:
    """The gate can release floors while preserving total conservation."""

    def _claim(self, **kwargs):
        base = dict(floor=0, weight=1.0, holdings_fill=0, launchable=True)
        base.update(kwargs)
        return broker.ClaimInput(**base)

    def test_idle_opendde_releases_floor_and_share_to_ungated_boltz(self):
        opendde = self._claim(floor=70,
                              weight=1e6,
                              holdings_fill=74,
                              utilization_cap=0)
        boltz = self._claim(floor=10, weight=100, holdings_fill=10)
        out = broker.compute_entitlements(84, {
            'opendde': opendde,
            'boltz': boltz,
        })
        # The gate clamps both OpenDDE's 70-replica floor and its weighted
        # headroom. Explicitly ungated Boltz receives the complete remainder.
        assert out['opendde'] == 0
        assert out['boltz'] == 84

    def test_idle_gate_releases_the_declared_floor(self):
        claim = self._claim(floor=10,
                            weight=1.0,
                            holdings_fill=40,
                            utilization_cap=0)
        out = broker.compute_entitlements(40, {'a': claim})
        assert out['a'] == 0

    def test_positive_cap_clamps_the_declared_floor(self):
        claim = self._claim(floor=70,
                            weight=1.0,
                            holdings_fill=0,
                            utilization_cap=2)
        out = broker.compute_entitlements(40, {'a': claim})
        assert out['a'] == 2

    def test_conservation_holds_with_caps_on_both_sides(self):
        for cap in (None, 0, 5, 50):
            for eff in (None, 0, 7, 90):
                claims = {
                    'a': self._claim(floor=10,
                                     weight=3.0,
                                     holdings_fill=20,
                                     utilization_cap=cap,
                                     effective_cap=eff),
                    'b': self._claim(floor=5, weight=1.0, holdings_fill=5),
                }
                out = broker.compute_entitlements(60, claims)
                assert sum(out.values()) <= 60, (cap, eff)
                for name, claim in claims.items():
                    assert out[name] >= claim.allocation_floor(), (cap, eff)

    def test_all_gated_collapses_to_the_sum_of_floors(self):
        claims = {
            'a': self._claim(floor=4, weight=2.0, utilization_cap=4),
            'b': self._claim(floor=6, weight=9.0, utilization_cap=6),
        }
        out = broker.compute_entitlements(80, claims)
        assert out == {'a': 4, 'b': 6}

    def test_ungated_claim_is_byte_identical_to_before_the_gate(self):
        claims = {
            'a': self._claim(floor=10, weight=100.0, holdings_fill=10),
            'b': self._claim(floor=0, weight=1e6, holdings_fill=74),
        }
        assert broker.compute_entitlements(84, claims) == {'a': 10, 'b': 74}


class TestActivityInputSkew:
    """activity_ts is the version-skew discriminator, not decoration."""

    def _row(self, **kwargs):
        base = {
            'service_name': 'svc',
            'demonstrated_need': 3,
            'boot_hold': 0,
            'activity_ts': 1000.0,
            'heartbeat_ts': 1000.0,
        }
        base.update(kwargs)
        return base

    def test_paired_signal_is_trusted(self):
        got = broker._activity_input(self._row())
        assert got.armed is True
        assert got.blind is False
        assert got.demonstrated_need == 3

    def test_absent_signal_reads_blind(self):
        # A pre-migration row, or a claimant whose gate is off.
        got = broker._activity_input(
            self._row(demonstrated_need=None, activity_ts=None))
        assert got.armed is False
        assert got.blind

    def test_fresh_null_need_is_armed_but_blind(self):
        got = broker._activity_input(self._row(demonstrated_need=None))
        assert got.armed is True
        assert got.blind

    def test_frozen_signal_from_an_old_writer_reads_blind(self):
        # The upsert builds its values dict from the columns ITS binary
        # knows, so a pre-gate writer advances heartbeat_ts while leaving
        # demonstrated_need frozen. Trusting a frozen 0 would walk a fully
        # busy service down to zero.
        stale = self._row(demonstrated_need=0, heartbeat_ts=1000.0 + 61.0)
        got = broker._activity_input(stale)
        assert got.armed is True
        assert got.blind

    def test_negative_lag_reads_blind(self):
        assert broker._activity_input(self._row(heartbeat_ts=999.0)).blind

    def test_env_kill_switch_blinds_every_claim(self):
        with mock.patch.dict(
                'os.environ',
            {serve_constants.RESERVED_FILL_UTILIZATION_GATE_ENV_VAR: 'false'}):
            got = broker._activity_input(self._row())
        assert got.armed is False
        assert got.blind


def test_armed_blind_claim_persists_fresh_null_need(_broker_db):
    broker.upsert_claim('svc',
                        pool_key=_POOL,
                        weight=1.0,
                        floor_replicas=70,
                        gpus_per_replica=1,
                        holdings_fill=70,
                        launchable=True,
                        activity={
                            'demonstrated_need': None,
                            'boot_hold': False,
                        })
    rows = serve_state.get_reserved_fill_claims(pool_key=_POOL)
    assert len(rows) == 1
    row = rows[0]
    assert row['demonstrated_need'] is None
    assert row['activity_ts'] is not None
    signal = broker._activity_input(row)
    assert signal.armed is True
    assert signal.blind is True


class TestApplyUtilizationGate:
    """Current writers gate by default; explicit opt-outs stay static."""

    def _claim(self, **kwargs):
        base = dict(floor=0, weight=1.0, holdings_fill=0, launchable=True)
        base.update(kwargs)
        return broker.ClaimInput(**base)

    def test_never_signalled_claimant_stays_ungated(self):
        # Services that do not opt in must be byte-identical to before.
        claims = {'a': self._claim(floor=2, holdings_fill=9)}
        activity = {
            'a': broker.ActivityInput(armed=False,
                                      demonstrated_need=0,
                                      boot_hold=False,
                                      blind=True)
        }
        gated, state = broker._apply_utilization_gate(claims, activity, {},
                                                      1000.0)
        assert gated['a'].utilization_cap is None
        assert not state

    def test_blind_round_keeps_an_already_earned_cap_applied(self):
        # Dropping the cap while blind would be a RISE, restoring full
        # entitlement on every deploy.
        claims = {'a': self._claim(floor=2, holdings_fill=40)}
        activity = {
            'a': broker.ActivityInput(armed=True,
                                      demonstrated_need=0,
                                      boot_hold=False,
                                      blind=True)
        }
        prev = {
            'a': {
                'cap': 20,
                'hot_until': 0.0,
                'stepped_at': 0.0,
                'blind_since': None
            }
        }
        gated, state = broker._apply_utilization_gate(claims, activity, prev,
                                                      1000.0)
        assert gated['a'].utilization_cap == 20
        assert state['a']['cap'] == 20

    def test_nonzero_need_caps_the_declared_floor_to_measured_need(self):
        claims = {'a': self._claim(floor=70, holdings_fill=0)}
        activity = {
            'a': broker.ActivityInput(armed=True,
                                      demonstrated_need=1,
                                      boot_hold=False,
                                      blind=False)
        }
        prev = {
            'a': {
                'cap': 0,
                'hot_until': 0.0,
                'stepped_at': 0.0,
                'blind_since': None
            }
        }
        gated, state = broker._apply_utilization_gate(claims, activity, prev,
                                                      1000.0)
        assert gated['a'].utilization_cap == 2
        assert state['a']['cap'] == 2

    def test_armed_blind_claimant_starts_blind_grace(self):
        claims = {'a': self._claim(floor=70, holdings_fill=70)}
        activity = {
            'a': broker.ActivityInput(armed=True,
                                      demonstrated_need=0,
                                      boot_hold=False,
                                      blind=True)
        }
        gated, state = broker._apply_utilization_gate(claims, activity, {},
                                                      1000.0)
        assert gated['a'].utilization_cap == 70
        assert state['a']['blind_since'] == 1000.0

    def test_armed_blind_claimant_decays_past_grace_through_floor(self):
        claims = {'a': self._claim(floor=70, holdings_fill=70)}
        activity = {
            'a': broker.ActivityInput(armed=True,
                                      demonstrated_need=0,
                                      boot_hold=False,
                                      blind=True)
        }
        prev = {
            'a': {
                'cap': 70,
                'hot_until': 0.0,
                'stepped_at': 0.0,
                'blind_since': 0.0,
            }
        }
        gated, state = broker._apply_utilization_gate(claims, activity, prev,
                                                      901.0)
        assert gated['a'].utilization_cap == 52
        assert state['a']['cap'] == 52

    def test_explicit_opt_out_clears_prior_release_state(self):
        claims = {'a': self._claim(floor=70, holdings_fill=20)}
        activity = {
            'a': broker.ActivityInput(armed=False,
                                      demonstrated_need=0,
                                      boot_hold=False,
                                      blind=True)
        }
        prev = {
            'a': {
                'cap': 2,
                'hot_until': 0.0,
                'stepped_at': 0.0,
                'blind_since': None,
            }
        }
        gated, state = broker._apply_utilization_gate(claims, activity, prev,
                                                      1000.0)
        assert gated['a'].utilization_cap is None
        assert not state

    def test_env_kill_switch_ungates_an_already_gated_service(self):
        # Requirement 10: the process-wide kill switch disables the gate for
        # every service and restores static entitlement. A claimant that
        # already accrued release state must be fully ungated (cap dropped,
        # state cleared), not frozen at its decayed cap the way a transient
        # telemetry blind is. Freezing under the kill switch would eventually
        # walk a busy service down through the very lever meant to stop the
        # gate.
        now = 1_000_000.0
        claims = {'a': self._claim(floor=16, weight=4.0, holdings_fill=40)}
        # A genuinely BUSY claim: fresh paired signal, high demonstrated need.
        row = {
            'demonstrated_need': 50,
            'boot_hold': False,
            'activity_ts': now,
            'heartbeat_ts': now,
        }
        prev = {
            'a': {
                'cap': 40,
                'hot_until': now - 1.0,
                'stepped_at': now - 10_000.0,
                'blind_since': None,
            }
        }
        with mock.patch.dict(
                'os.environ',
            {serve_constants.RESERVED_FILL_UTILIZATION_GATE_ENV_VAR: '0'}):
            activity = {'a': broker._activity_input(row)}
            gated, state = broker._apply_utilization_gate(
                claims, activity, prev, now)
        # Ungated: today's entitlement (no cap), and the release state is
        # cleared so the round writer publishes NULL utilization_state
        # (`if utilization_state else None`) rather than a stale target.
        assert gated['a'].utilization_cap is None
        assert not state


class TestDemandGateGrant:
    """The demand gate reads the permissive grant, the ceiling the damped."""

    def test_rise_reopens_the_demand_gate_before_damping_catches_up(self):
        assert broker._demand_gate_grant(16, 50) == 50

    def test_no_ceiling_stays_inert(self):
        assert broker._demand_gate_grant(None, 50) is None

    def test_missing_raw_falls_back_to_the_damped_grant(self):
        assert broker._demand_gate_grant(16, None) == 16
