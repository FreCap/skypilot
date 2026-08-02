"""Tests for centralized SkyServe paid-capacity policy."""
# pylint: disable=protected-access
import json
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import constants
from sky.serve import paid_capacity
from sky.serve import replica_managers
from sky.utils import common_utils


@pytest.fixture(autouse=True)
def _clear_paid_capacity_config_cache():
    paid_capacity._parse_positive_int.cache_clear()
    paid_capacity._admission_summary_log_signature = None
    paid_capacity._admission_summary_logged_at = 0
    yield
    paid_capacity._parse_positive_int.cache_clear()
    paid_capacity._admission_summary_log_signature = None
    paid_capacity._admission_summary_logged_at = 0


def _pending_info(replica_id, location):
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=location.use_spot,
                                        location=location,
                                        version=1,
                                        resources_override=location.to_dict())


def _exploration_budget(locations,
                        *,
                        owned_locations,
                        remaining,
                        claimed_at=900,
                        max_frontier=3,
                        delay=30,
                        service_remaining=16):
    pool_keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    owned_keys = {pool_keys[location] for location in owned_locations}
    if isinstance(claimed_at, dict):
        newest = {
            pool_keys[location]: timestamp
            for location, timestamp in claimed_at.items()
        }
    else:
        newest = {key: claimed_at for key in owned_keys}
    return paid_capacity.LaunchBudget(
        remaining_by_location=dict(zip(locations, remaining)),
        pool_key_by_location=pool_keys,
        states_by_pool_key={},
        globally_managed=True,
        service_remaining=service_remaining,
        frontier_limit=2,
        max_frontier_limit=max_frontier,
        frontier_feedback_delay_seconds=delay,
        frontier_key_by_location={location: ('l4',) for location in locations},
        failure_domain_by_location={
            location: paid_capacity.failure_domain(location)
            for location in locations
        },
        owned_pool_keys_by_frontier={('l4',): owned_keys},
        newest_claimed_at_by_pool_key=newest)


def test_pool_key_distinguishes_every_provider_capacity_dimension():
    a100 = make_location('us-east-1', {'A100': 1}, cloud_name='AWS')
    a100_80 = make_location('us-east-1', {'A100-80GB': 1}, cloud_name='AWS')
    a100.instance_type = 'p4d.24xlarge'
    a100_80.instance_type = 'p4de.24xlarge'

    base = paid_capacity.pool_key(a100, workspace='w1', num_nodes=1)
    assert base != paid_capacity.pool_key(a100_80, workspace='w1', num_nodes=1)
    changed_instance = make_location('us-east-1', {'A100': 1}, cloud_name='AWS')
    changed_instance.instance_type = 'p4de.24xlarge'
    assert base != paid_capacity.pool_key(changed_instance,
                                          workspace='w1',
                                          num_nodes=1)
    assert base != paid_capacity.pool_key(a100, workspace='w2', num_nodes=1)
    assert base != paid_capacity.pool_key(a100, workspace='w1', num_nodes=2)


def test_pool_key_normalizes_equivalent_accelerator_counts():
    integral = make_location('us-east-1', {'A100': 1}, cloud_name='AWS')
    floating = make_location('us-east-1', {'a100': 1.0}, cloud_name='AWS')
    integral.instance_type = floating.instance_type = 'p4d.24xlarge'

    assert paid_capacity.pool_key(integral, workspace='w1',
                                  num_nodes=1) == paid_capacity.pool_key(
                                      floating, workspace='w1', num_nodes=1)


def test_frontier_key_groups_card_model_across_counts_and_instance_types():
    narrow = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    wide = make_location('us-west-2', {'l4': 8}, cloud_name='GCP')
    narrow.instance_type = 'g6.xlarge'
    wide.instance_type = 'g2-standard-96'

    narrow_pool = paid_capacity.pool_key(narrow, workspace='w', num_nodes=1)
    wide_pool = paid_capacity.pool_key(wide, workspace='w', num_nodes=1)

    assert narrow_pool != wide_pool
    assert paid_capacity.frontier_key(narrow) == ('l4',)
    assert paid_capacity.frontier_key(wide) == ('l4',)
    assert paid_capacity.frontier_key_from_pool_key(narrow_pool) == ('l4',)
    assert paid_capacity.frontier_key_from_pool_key(wide_pool) == ('l4',)


def test_failure_domain_uses_provider_and_region_only():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    location.zone = 'us-east-1a'
    location.instance_type = 'g6.xlarge'
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)

    assert paid_capacity.failure_domain(location) == ('aws', 'us-east-1')
    assert paid_capacity.failure_domain_from_pool_key(key) == ('aws',
                                                               'us-east-1')
    assert paid_capacity.failure_domain_from_pool_key('opaque') is None
    payload = json.loads(key)
    payload['region'] = None
    assert paid_capacity.failure_domain_from_pool_key(
        json.dumps(payload)) is None
    payload = json.loads(key)
    payload['accelerators'] = 'malformed'
    assert paid_capacity.failure_domain_from_pool_key(
        json.dumps(payload)) is None


def test_default_limits_and_invalid_failure_cooldown(monkeypatch):
    monkeypatch.delenv(paid_capacity._BASE_LIMIT_ENV_VAR, raising=False)
    monkeypatch.delenv(paid_capacity._MAX_LIMIT_ENV_VAR, raising=False)
    monkeypatch.delenv(paid_capacity._SERVICE_LIMIT_ENV_VAR, raising=False)
    monkeypatch.delenv(paid_capacity._FAILURE_COOLDOWN_SECONDS_ENV_VAR,
                       raising=False)
    assert paid_capacity.base_limit() == 4
    assert paid_capacity.max_limit() == 480
    assert paid_capacity.service_limit() == 16
    assert paid_capacity.failure_cooldown_seconds() == 600

    monkeypatch.setenv(paid_capacity._SERVICE_LIMIT_ENV_VAR, '0')
    monkeypatch.setenv(paid_capacity._FAILURE_COOLDOWN_SECONDS_ENV_VAR, '0')
    paid_capacity._parse_positive_int.cache_clear()
    assert paid_capacity.service_limit() == 16
    assert paid_capacity.failure_cooldown_seconds() == 600


def test_exploration_frontier_default_override_and_invalid_fallback(
        monkeypatch):
    monkeypatch.delenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR,
                       raising=False)
    assert paid_capacity.exploration_frontier() == 2

    monkeypatch.setenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR, '3')
    assert paid_capacity.exploration_frontier() == 3

    monkeypatch.setenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR, '0')
    assert paid_capacity.exploration_frontier() == 2

    monkeypatch.setenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR, 'invalid')
    assert paid_capacity.exploration_frontier() == 2


def test_delayed_exploration_defaults_overrides_and_clamps(monkeypatch):
    monkeypatch.delenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR,
                       raising=False)
    monkeypatch.delenv(paid_capacity._MAX_EXPLORATION_FRONTIER_ENV_VAR,
                       raising=False)
    monkeypatch.delenv(
        paid_capacity._EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR,
        raising=False)
    assert paid_capacity.max_exploration_frontier() == 3
    assert paid_capacity.exploration_feedback_delay_seconds() == 30

    monkeypatch.setenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR, '4')
    monkeypatch.setenv(paid_capacity._MAX_EXPLORATION_FRONTIER_ENV_VAR, '2')
    monkeypatch.setenv(
        paid_capacity._EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR, '45')
    paid_capacity._parse_positive_int.cache_clear()
    assert paid_capacity.max_exploration_frontier() == 4
    assert paid_capacity.exploration_feedback_delay_seconds() == 45

    monkeypatch.setenv(paid_capacity._EXPLORATION_FRONTIER_ENV_VAR, '2')
    monkeypatch.setenv(paid_capacity._MAX_EXPLORATION_FRONTIER_ENV_VAR, '0')
    monkeypatch.setenv(
        paid_capacity._EXPLORATION_FEEDBACK_DELAY_SECONDS_ENV_VAR, 'invalid')
    paid_capacity._parse_positive_int.cache_clear()
    assert paid_capacity.max_exploration_frontier() == 3
    assert paid_capacity.exploration_feedback_delay_seconds() == 30


def test_default_adaptive_limit_ramps_four_to_480():
    state = paid_capacity.RampUpdate(current_limit=4,
                                     successes_since_resize=0,
                                     expired=False,
                                     failed=False)
    for expected in (8, 16, 32, 64, 128, 256, 480):
        state = paid_capacity.record_outcomes(
            state.current_limit,
            state.successes_since_resize,
            last_success_at=100,
            outcomes=[paid_capacity.LaunchOutcome.SUCCESS] *
            state.current_limit,
            bootstrap_limit=4,
            ceiling_limit=480,
            now=101,
            ttl_seconds=600)
        assert state.current_limit == expected
        assert state.successes_since_resize == 0
        assert not state.failed


def test_explicit_sixty_limit_ramps_to_480_and_resets_on_failure():
    state = paid_capacity.RampUpdate(current_limit=60,
                                     successes_since_resize=0,
                                     expired=False,
                                     failed=False)
    for expected in (120, 240, 480):
        state = paid_capacity.record_outcomes(
            state.current_limit,
            state.successes_since_resize,
            last_success_at=100,
            outcomes=[paid_capacity.LaunchOutcome.SUCCESS] *
            state.current_limit,
            bootstrap_limit=60,
            ceiling_limit=480,
            now=101,
            ttl_seconds=600)
        assert state.current_limit == expected
        assert state.successes_since_resize == 0
        assert not state.failed

    failed = paid_capacity.record_outcomes(
        state.current_limit,
        state.successes_since_resize,
        last_success_at=101,
        outcomes=[
            paid_capacity.LaunchOutcome.SUCCESS,
            paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
        ],
        bootstrap_limit=60,
        ceiling_limit=480,
        now=102,
        ttl_seconds=600)
    assert failed.current_limit == 60
    assert failed.successes_since_resize == 0
    assert failed.failed


@pytest.mark.parametrize('legacy_limit', [60, 120, 240])
def test_legacy_limit_normalizes_to_default_bootstrap(legacy_limit):
    assert paid_capacity.effective_limit(legacy_limit,
                                         last_success_at=100,
                                         bootstrap_limit=4,
                                         ceiling_limit=480,
                                         now=101,
                                         ttl_seconds=600) == (4, True)


def test_fresh_valid_ceiling_survives_ladder_normalization():
    assert paid_capacity.effective_limit(480,
                                         last_success_at=100,
                                         bootstrap_limit=4,
                                         ceiling_limit=480,
                                         now=101,
                                         ttl_seconds=600) == (480, False)
    assert paid_capacity.limit_ladder(60, 480) == (60, 120, 240, 480)


def test_admission_summary_is_bounded_and_redacts_pool_keys():
    states = {
        '{"workspace":"secret-a"}': {
            'admission_state': 'cooldown',
            'active_claims': 3,
            'admission_limit': 0,
            'remaining': 0,
            'legacy_overage': 3,
        },
        '{"workspace":"secret-b"}': {
            'admission_state': 'active',
            'active_claims': 2,
            'admission_limit': 4,
            'remaining': 2,
            'legacy_overage': 0,
        },
    }
    with mock.patch.object(paid_capacity.time,
                           'monotonic',
                           side_effect=[100, 101, 500]), \
         mock.patch.object(paid_capacity.logger, 'info') as info:
        paid_capacity._log_admission_summary(states,
                                             service_claims=17,
                                             service_claim_limit=16)
        paid_capacity._log_admission_summary(states,
                                             service_claims=17,
                                             service_claim_limit=16)
        paid_capacity._log_admission_summary(states,
                                             service_claims=17,
                                             service_claim_limit=16)

    assert info.call_count == 2
    message = info.call_args.args[0]
    assert 'pools=2' in message
    assert "'active': 1" in message
    assert "'cooldown': 1" in message
    assert 'active_claims=5' in message
    assert 'legacy_overage_claims=3' in message
    assert 'service_claims=17' in message
    assert 'service_limit=16' in message
    assert 'service_remaining=0' in message
    assert 'secret-a' not in message
    assert 'secret-b' not in message


def test_failure_epoch_closes_then_allows_one_probe():
    closed = paid_capacity.effective_admission_limit(current_limit=4,
                                                     last_success_at=None,
                                                     last_failure_at=100,
                                                     bootstrap_limit=4,
                                                     ceiling_limit=480,
                                                     now=699,
                                                     success_ttl=600,
                                                     failure_cooldown=600)
    assert closed == paid_capacity.AdmissionLimit(limit=0,
                                                  state='cooldown',
                                                  cooldown_until=700)

    probe = paid_capacity.effective_admission_limit(current_limit=1,
                                                    last_success_at=None,
                                                    last_failure_at=100,
                                                    bootstrap_limit=4,
                                                    ceiling_limit=480,
                                                    now=700,
                                                    success_ttl=600,
                                                    failure_cooldown=600)
    assert probe == paid_capacity.AdmissionLimit(limit=1,
                                                 state='probe',
                                                 cooldown_until=700)


def test_adaptive_limit_expires_before_counting_new_successes():
    update = paid_capacity.record_outcomes(
        current_limit=240,
        successes_since_resize=239,
        last_success_at=100,
        outcomes=[paid_capacity.LaunchOutcome.SUCCESS],
        bootstrap_limit=60,
        ceiling_limit=480,
        now=701,
        ttl_seconds=600)

    assert update.current_limit == 60
    assert update.successes_since_resize == 1
    assert update.expired


def test_partial_bootstrap_evidence_expires_before_promotion():
    update = paid_capacity.record_outcomes(
        current_limit=60,
        successes_since_resize=59,
        last_success_at=100,
        outcomes=[paid_capacity.LaunchOutcome.SUCCESS],
        bootstrap_limit=60,
        ceiling_limit=480,
        now=701,
        ttl_seconds=600)

    assert update.current_limit == 60
    assert update.successes_since_resize == 1
    assert update.expired


def test_non_capacity_failure_preserves_provider_evidence():
    update = paid_capacity.record_outcomes(
        current_limit=240,
        successes_since_resize=17,
        last_success_at=100,
        outcomes=[paid_capacity.LaunchOutcome.OTHER_FAILURE],
        bootstrap_limit=60,
        ceiling_limit=480,
        now=701,
        ttl_seconds=600)

    assert update.current_limit == 240
    assert update.successes_since_resize == 17
    assert not update.expired
    assert not update.failed


def test_quota_failure_closes_paid_capacity_ramp():
    update = paid_capacity.record_outcomes(
        current_limit=240,
        successes_since_resize=17,
        last_success_at=100,
        outcomes=[paid_capacity.LaunchOutcome.QUOTA_FAILURE],
        bootstrap_limit=4,
        ceiling_limit=480,
        now=101,
        ttl_seconds=600)

    assert update.current_limit == 4
    assert update.successes_since_resize == 0
    assert update.failed


def test_admission_snapshot_distinguishes_open_and_cooldown():
    open_location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    cooldown_location = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    budget = paid_capacity.LaunchBudget(remaining_by_location={
        open_location: 2,
        cooldown_location: 0,
    },
                                        pool_key_by_location={
                                            open_location: 'open',
                                            cooldown_location: 'cooldown',
                                        },
                                        states_by_pool_key={
                                            'open': {
                                                'admission_state': 'active',
                                            },
                                            'cooldown': {
                                                'admission_state': 'cooldown',
                                                'cooldown_until': 1234.0,
                                            },
                                        },
                                        globally_managed=True,
                                        service_remaining=12)

    snapshot = paid_capacity.admission_snapshot_by_location(budget)

    assert snapshot[open_location] == {
        'state': 'open',
        'pool_remaining': 2,
        'service_remaining': 12,
        'cooldown_until': None,
        'frontier_limit': None,
        'frontier_max_limit': None,
        'frontier_owned': False,
        'frontier_owned_pool_count': 0,
        'youngest_unresolved_claim_age_seconds': None,
    }
    assert snapshot[cooldown_location]['state'] == 'cooldown'
    assert snapshot[cooldown_location]['cooldown_until'] == 1234.0


def test_global_snapshot_uses_shared_headroom_by_exact_pool():
    cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    zero = make_location('research', {'L4': 1},
                         use_spot=False,
                         cloud_name='Kubernetes')
    placer = make_placer({cheap: 1.0, expensive: 2.0, zero: 0.0})

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value={
                                   paid_capacity.pool_key(cheap,
                                                          workspace='w',
                                                          num_nodes=1): {
                                       'remaining': 7
                                   },
                                   paid_capacity.pool_key(expensive,
                                                          workspace='w',
                                                          num_nodes=1): {
                                       'remaining': 3
                                   },
                               }) as get_states:
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   existing_replica_infos=[],
                                                   globally_managed=True)

    assert budget.remaining_by_location == {cheap: 7, expensive: 3}
    assert budget.service_remaining == 16
    assert budget.frontier_limit == 2
    assert budget.max_frontier_limit == 3
    assert budget.frontier_feedback_delay_seconds == 30
    assert budget.failure_domain_by_location == {
        cheap: ('aws', 'us-east-1'),
        expensive: ('aws', 'us-west-2'),
    }
    assert zero not in budget.pool_key_by_location
    get_states.assert_called_once()


def test_global_budget_caps_paid_selection_across_exact_pools(monkeypatch):
    monkeypatch.setenv(paid_capacity._SERVICE_LIMIT_ENV_VAR, '2')
    cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({cheap: 1.0, expensive: 2.0})
    infos = [_pending_info(1, cheap), _pending_info(2, expensive)]
    infos[0].paid_capacity_pool_key = 'cheap'
    infos[1].paid_capacity_pool_key = 'expensive'
    states = {
        paid_capacity.pool_key(cheap, workspace='w', num_nodes=1): {
            'remaining': 4
        },
        paid_capacity.pool_key(expensive, workspace='w', num_nodes=1): {
            'remaining': 4
        },
    }

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   existing_replica_infos=infos,
                                                   globally_managed=True)

    assert budget.service_remaining == 0
    assert paid_capacity.select_location(placer, budget) is None


@pytest.mark.parametrize(
    'unknown_age',
    [None, float('nan'),
     float('inf'), float('-inf'), -1, True])
def test_global_snapshot_counts_catalog_hidden_and_unknown_owned_pools(
        unknown_age):
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    hidden = make_location('ap-south-1', {'L4': 8}, cloud_name='AWS')
    placer = make_placer({first: 1.0, second: 2.0, third: 3.0})
    active_keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in (first, second, third)
    }
    hidden_info = _pending_info(1, hidden)
    hidden_info.paid_capacity_pool_key = paid_capacity.pool_key(hidden,
                                                                workspace='w',
                                                                num_nodes=1)
    unknown_age_sibling = _pending_info(3, hidden)
    unknown_age_sibling.paid_capacity_pool_key = (
        hidden_info.paid_capacity_pool_key)
    unknown_age_sibling.created_at = unknown_age
    unknown_info = _pending_info(2, hidden)
    unknown_info.paid_capacity_pool_key = 'opaque-pre-versioned-pool'

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value={
                                   key: {
                                       'remaining': 4
                                   } for key in active_keys.values()
                               }):
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   existing_replica_infos=[
                                                       hidden_info,
                                                       unknown_age_sibling,
                                                       unknown_info
                                                   ],
                                                   globally_managed=True)

    assert budget.owned_pool_keys_by_frontier == {
        ('l4',): {hidden_info.paid_capacity_pool_key}
    }
    assert budget.unknown_owned_pool_keys == {'opaque-pre-versioned-pool'}
    assert budget.unknown_claim_age_pool_keys == {
        hidden_info.paid_capacity_pool_key
    }
    assert paid_capacity.select_location(placer, budget) is None
    assert budget.feedback_deferred_frontiers == {('l4',)}


def test_debit_and_authoritative_saturation_exhaust_service_budget():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    budget = paid_capacity.LaunchBudget(remaining_by_location={location: 4},
                                        pool_key_by_location={location: 'pool'},
                                        states_by_pool_key={},
                                        globally_managed=True,
                                        service_remaining=2)

    assert not paid_capacity.service_exhausted(None)
    assert not paid_capacity.service_exhausted(budget)
    paid_capacity.debit(budget, location)
    assert budget.service_remaining == 1
    assert not paid_capacity.service_exhausted(budget)
    paid_capacity.exhaust_service(budget)
    assert budget.service_remaining == 0
    assert paid_capacity.service_exhausted(budget)
    budget.service_remaining = None
    assert not paid_capacity.service_exhausted(budget)


def test_legacy_local_snapshot_only_debits_unresolved_rows(monkeypatch):
    monkeypatch.setenv(paid_capacity._BASE_LIMIT_ENV_VAR, '2')
    paid_capacity._parse_positive_int.cache_clear()
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({location: 1.0})
    pending = _pending_info(1, location)
    starting = _pending_info(2, location)
    starting.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED

    budget = paid_capacity.build_launch_budget(
        placer,
        workspace='w',
        existing_replica_infos=[pending, starting],
        globally_managed=False)

    assert budget.remaining_by_location == {location: 1}
    paid_capacity._parse_positive_int.cache_clear()


def test_non_postgresql_backend_uses_legacy_local_window(monkeypatch):
    monkeypatch.delenv(paid_capacity._BASE_LIMIT_ENV_VAR, raising=False)
    paid_capacity._parse_positive_int.cache_clear()
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({location: 1.0})
    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=False), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states') as get_states:
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            existing_replica_infos=[_pending_info(1, location)],
            globally_managed=True)

    assert not budget.globally_managed
    assert budget.remaining_by_location[location] == 3
    get_states.assert_not_called()
    paid_capacity._parse_positive_int.cache_clear()


def test_local_window_debits_ambiguous_legacy_row_from_cheapest_type(
        monkeypatch):
    monkeypatch.delenv(paid_capacity._BASE_LIMIT_ENV_VAR, raising=False)
    paid_capacity._parse_positive_int.cache_clear()
    cheapest = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    other = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    cheapest.instance_type = 'g6.xlarge'
    other.instance_type = 'g6.2xlarge'
    placer = make_placer({cheapest: 1.0, other: 2.0})
    legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')

    budget = paid_capacity.build_launch_budget(
        placer,
        workspace='w',
        existing_replica_infos=[_pending_info(1, legacy)],
        globally_managed=False)

    assert budget.remaining_by_location == {
        cheapest: 3,
        other: 4,
    }


def test_claim_clamps_priority_and_returns_typed_result():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    location.instance_type = 'g6.xlarge'
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={location: 1},
        pool_key_by_location={
            location: paid_capacity.pool_key(location,
                                             workspace='w',
                                             num_nodes=1)
        },
        states_by_pool_key={},
        globally_managed=True,
        frontier_limit=2,
        max_frontier_limit=3,
        frontier_key_by_location={location: ('l4',)},
        frontier_limit_overrides={('l4',): 3})
    info = _pending_info(1, location)
    with mock.patch.object(paid_capacity.serve_state,
                           'try_add_replica_with_paid_capacity_claim',
                           return_value='acquired') as claim:
        result = paid_capacity.try_persist_claim(service_name='svc',
                                                 service_hash='hash',
                                                 controller_owner=(1,
                                                                   '10.0.0.1'),
                                                 replica_id=1,
                                                 replica_info=info,
                                                 location=location,
                                                 budget=budget,
                                                 priority=1000)

    assert result is paid_capacity.ClaimResult.ACQUIRED
    assert claim.call_args.kwargs['priority'] == (
        constants.LB_REQUEST_PRIORITY_MAX)
    assert claim.call_args.kwargs['service_limit'] == 16
    assert claim.call_args.kwargs['frontier_key'] == ('l4',)
    assert claim.call_args.kwargs['frontier_limit'] == 3
    assert claim.call_args.kwargs['frontier_default_limit'] == 2
    assert claim.call_args.kwargs['frontier_limits_by_key'] == {('l4',): 3}


def test_saturated_pool_exhaustion_spills_to_next_pool():
    cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({cheap: 1.0, expensive: 2.0})
    budget = paid_capacity.LaunchBudget(remaining_by_location={
        cheap: 1,
        expensive: 1
    },
                                        pool_key_by_location={},
                                        states_by_pool_key={},
                                        globally_managed=False)

    assert paid_capacity.select_location(placer, budget) == cheap
    paid_capacity.exhaust(budget, cheap)
    assert paid_capacity.select_location(placer, budget) == expensive


def test_cold_large_wave_opens_only_two_l4_pools_before_feedback():
    locations = [
        make_location(region, {'L4': 1}, cloud_name='AWS')
        for region in ('us-east-1', 'us-west-2', 'eu-west-1')
    ]
    placer = make_placer({
        location: float(index)
        for index, location in enumerate(locations, start=1)
    })
    pool_keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={location: 4 for location in locations},
        pool_key_by_location=pool_keys,
        states_by_pool_key={},
        globally_managed=True,
        frontier_limit=2,
        frontier_key_by_location={location: ('l4',) for location in locations})

    selected = []
    for _ in range(400):
        location = paid_capacity.select_location(placer, budget)
        if location is None:
            break
        selected.append(location)
        paid_capacity.debit(budget, location)

    assert selected == [locations[0]] * 4 + [locations[1]] * 4
    assert budget.remaining_by_location[locations[2]] == 4
    assert budget.owned_pool_keys_by_frontier == {
        ('l4',): {pool_keys[locations[0]], pool_keys[locations[1]]}
    }
    assert budget.feedback_deferred_frontiers == {('l4',)}


def test_normal_second_pool_prefers_a_new_provider_region():
    primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    same_domain = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    different_domain = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    primary.instance_type = 'g6.xlarge'
    same_domain.instance_type = 'g6.2xlarge'
    different_domain.instance_type = 'g6.xlarge'
    locations = [primary, same_domain, different_domain]
    placer = make_placer({
        primary: 0.5,
        same_domain: 1.0,
        different_domain: 2.0
    })
    budget = _exploration_budget(locations,
                                 owned_locations=[primary],
                                 remaining=[0, 4, 4])

    assert paid_capacity.select_location(placer, budget) == different_domain


def test_normal_second_pool_allows_same_domain_fallback():
    primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    fallback = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    primary.instance_type = 'g6.xlarge'
    fallback.instance_type = 'g6.2xlarge'
    locations = [primary, fallback]
    placer = make_placer({primary: 0.5, fallback: 1.0})
    budget = _exploration_budget(locations,
                                 owned_locations=[primary],
                                 remaining=[0, 4])

    assert paid_capacity.select_location(placer, budget) == fallback


def test_delayed_third_pool_waits_for_the_youngest_unresolved_claim():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    locations = [first, second, third]
    placer = make_placer({first: 1.0, second: 2.0, third: 3.0})
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4],
                                 claimed_at={
                                     first: 800,
                                     second: 980
                                 },
                                 delay=30)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None

    assert not budget.frontier_limit_overrides
    assert budget.feedback_deferred_frontiers == {('l4',)}


def test_delayed_third_pool_uses_only_a_new_provider_region():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    same_domain = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    third_domain = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    first.instance_type = 'g6.xlarge'
    same_domain.instance_type = 'g6.2xlarge'
    locations = [first, second, same_domain, third_domain]
    placer = make_placer({
        first: 1.0,
        second: 2.0,
        same_domain: 0.5,
        third_domain: 3.0,
    })
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4, 4],
                                 claimed_at=900,
                                 delay=30)

    with mock.patch.object(paid_capacity.time, 'time',
                           return_value=1000), mock.patch.object(
                               paid_capacity.logger, 'info') as info:
        selected = paid_capacity.select_location(placer, budget)
        snapshot = paid_capacity.admission_snapshot_by_location(budget)

    assert selected == third_domain
    assert budget.frontier_limit_overrides == {('l4',): 3}
    assert budget.feedback_deferred_frontiers == set()
    message = info.call_args.args[0]
    assert 'from_limit=2' in message
    assert 'to_limit=3' in message
    assert 'youngest_unresolved_claim_age_seconds=100' in message
    assert 'candidate_cloud=aws' in message
    assert 'candidate_region=eu-west-1' in message
    assert budget.pool_key_by_location[first] not in message
    assert snapshot[third_domain]['frontier_limit'] == 3
    assert snapshot[third_domain]['frontier_max_limit'] == 3
    assert snapshot[first]['frontier_owned']
    assert not snapshot[third_domain]['frontier_owned']
    assert snapshot[third_domain]['frontier_owned_pool_count'] == 2
    assert snapshot[third_domain][
        'youngest_unresolved_claim_age_seconds'] == 100


@pytest.mark.parametrize('missing_age,max_frontier', [(True, 3), (False, 2)])
def test_delayed_third_pool_fails_closed_without_age_or_when_disabled(
        missing_age, max_frontier):
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    locations = [first, second, third]
    placer = make_placer({first: 1.0, second: 2.0, third: 3.0})
    claimed_at = ({first: 900} if missing_age else 900)
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4],
                                 claimed_at=claimed_at,
                                 max_frontier=max_frontier,
                                 delay=30)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None

    assert not budget.frontier_limit_overrides


def test_delayed_third_pool_requires_a_distinct_failure_domain():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    east_alternate = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    west_alternate = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    first.instance_type = 'g6.xlarge'
    east_alternate.instance_type = 'g6.2xlarge'
    second.instance_type = 'g6.xlarge'
    west_alternate.instance_type = 'g6.2xlarge'
    locations = [first, second, east_alternate, west_alternate]
    placer = make_placer({
        first: 1.0,
        second: 2.0,
        east_alternate: 3.0,
        west_alternate: 4.0,
    })
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4, 4],
                                 claimed_at=900)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None


def test_delayed_third_pool_fails_closed_on_malformed_owned_domain():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    locations = [first, second, third]
    placer = make_placer({first: 1.0, second: 2.0, third: 3.0})
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4],
                                 claimed_at=900)
    first_key = budget.pool_key_by_location[first]
    malformed_payload = json.loads(first_key)
    malformed_payload['accelerators'] = 'malformed'
    malformed_key = json.dumps(malformed_payload,
                               sort_keys=True,
                               separators=(',', ':'))
    budget.owned_pool_keys_by_frontier[('l4',)].remove(first_key)
    budget.owned_pool_keys_by_frontier[('l4',)].add(malformed_key)
    budget.newest_claimed_at_by_pool_key[malformed_key] = 900

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None
    assert not budget.frontier_limit_overrides


def test_delayed_third_pool_requires_age_for_every_unresolved_sibling():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    locations = [first, second, third]
    placer = make_placer({first: 1.0, second: 2.0, third: 3.0})
    budget = _exploration_budget(locations,
                                 owned_locations=[first, second],
                                 remaining=[0, 0, 4],
                                 claimed_at=900)
    budget.unknown_claim_age_pool_keys.add(budget.pool_key_by_location[first])

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None
    assert not budget.frontier_limit_overrides


def test_restart_reuses_owned_third_pool_without_opening_a_fourth():
    locations = [
        make_location(region, {'L4': 1}, cloud_name='AWS')
        for region in ('us-east-1', 'us-west-2', 'eu-west-1', 'ap-south-1')
    ]
    placer = make_placer({
        location: float(index)
        for index, location in enumerate(locations, start=1)
    })
    budget = _exploration_budget(locations,
                                 owned_locations=locations[:3],
                                 remaining=[0, 0, 2, 4],
                                 claimed_at=900,
                                 max_frontier=3)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) == locations[2]
        paid_capacity.exhaust(budget, locations[2])
        assert paid_capacity.select_location(placer, budget) is None

    assert paid_capacity._effective_frontier_limit(budget, ('l4',)) == 3
    assert not budget.frontier_limit_overrides


def test_only_one_delayed_frontier_expansion_occurs_per_budget():
    locations = [
        make_location(region, {'L4': 1}, cloud_name='AWS')
        for region in ('us-east-1', 'us-west-2', 'eu-west-1', 'ap-south-1')
    ]
    placer = make_placer({
        location: float(index)
        for index, location in enumerate(locations, start=1)
    })
    budget = _exploration_budget(locations,
                                 owned_locations=locations[:2],
                                 remaining=[0, 0, 1, 4],
                                 claimed_at=900,
                                 max_frontier=4,
                                 delay=30)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        selected = paid_capacity.select_location(placer, budget)
        assert selected == locations[2]
        paid_capacity.debit(budget, selected)
        assert paid_capacity.select_location(placer, budget) is None

    assert budget.frontier_limit_overrides == {('l4',): 3}


def test_delayed_frontier_never_bypasses_service_envelope():
    locations = [
        make_location(region, {'L4': 1}, cloud_name='AWS')
        for region in ('us-east-1', 'us-west-2', 'eu-west-1')
    ]
    placer = make_placer({
        location: float(index)
        for index, location in enumerate(locations, start=1)
    })
    budget = _exploration_budget(locations,
                                 owned_locations=locations[:2],
                                 remaining=[0, 0, 4],
                                 claimed_at=900,
                                 service_remaining=0)

    with mock.patch.object(paid_capacity.time, 'time', return_value=1000):
        assert paid_capacity.select_location(placer, budget) is None
    assert not budget.frontier_limit_overrides


def test_full_l4_frontier_does_not_block_independent_a100():
    l4_primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    l4_hedge = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    l4_third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    a100 = make_location('ap-south-1', {'A100': 1}, cloud_name='AWS')
    locations = (l4_primary, l4_hedge, l4_third, a100)
    placer = make_placer({
        location: float(index)
        for index, location in enumerate(locations, start=1)
    })
    pool_keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={
            l4_primary: 0,
            l4_hedge: 0,
            l4_third: 4,
            a100: 4,
        },
        pool_key_by_location=pool_keys,
        states_by_pool_key={},
        globally_managed=True,
        frontier_limit=2,
        frontier_key_by_location={
            l4_primary: ('l4',),
            l4_hedge: ('l4',),
            l4_third: ('l4',),
            a100: ('a100',),
        },
        owned_pool_keys_by_frontier={
            ('l4',): {pool_keys[l4_primary], pool_keys[l4_hedge]}
        })

    assert paid_capacity.select_location(
        placer, budget, allowed_locations={l4_primary, l4_hedge,
                                           l4_third}) is None
    assert budget.feedback_deferred_frontiers == {('l4',)}
    assert paid_capacity.select_location(placer,
                                         budget,
                                         allowed_locations={a100}) == a100


def test_feedback_deferral_logs_once_and_records_frontier_state():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={location: 4},
        pool_key_by_location={location: 'candidate'},
        states_by_pool_key={},
        globally_managed=True,
        frontier_limit=2,
        frontier_key_by_location={location: ('l4',)},
        owned_pool_keys_by_frontier={('l4',): {'primary', 'hedge'}},
        oldest_claimed_at_by_frontier={('l4',): 900})

    with mock.patch.object(paid_capacity.time, 'time',
                           return_value=1000), mock.patch.object(
                               paid_capacity.logger, 'info') as info:
        paid_capacity.defer_for_feedback(budget, location)
        paid_capacity.defer_for_feedback(budget, location)

    assert budget.feedback_deferred_frontiers == {('l4',)}
    info.assert_called_once()
    message = info.call_args.args[0]
    assert 'card=l4' in message
    assert 'owned_pools=2' in message
    assert 'limit=2' in message
    assert 'oldest_unresolved_claim_age_seconds=100' in message
    assert 'primary' not in message
    assert 'hedge' not in message


def test_priority_deferral_stops_same_pool_without_paid_spill():
    cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({cheap: 1.0, expensive: 2.0})
    budget = paid_capacity.LaunchBudget(remaining_by_location={
        cheap: 10,
        expensive: 10
    },
                                        pool_key_by_location={
                                            cheap: 'cheap',
                                            expensive: 'expensive'
                                        },
                                        states_by_pool_key={},
                                        globally_managed=True)

    assert paid_capacity.select_location(placer, budget) == cheap
    paid_capacity.defer_for_priority(budget, cheap)

    assert budget.remaining_by_location == {cheap: 10, expensive: 10}
    assert budget.priority_deferred_pool_keys == {'cheap'}
    assert paid_capacity.select_location(placer, budget) is None


def test_debit_and_exhaust_share_headroom_across_location_aliases():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    alias = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    alias.image_id = {None: 'different-image'}
    other = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    shared_key = paid_capacity.pool_key(first, workspace='w', num_nodes=1)
    budget = paid_capacity.LaunchBudget(remaining_by_location={
        first: 2,
        alias: 2,
        other: 2
    },
                                        pool_key_by_location={
                                            first: shared_key,
                                            alias: shared_key,
                                            other: paid_capacity.pool_key(
                                                other,
                                                workspace='w',
                                                num_nodes=1),
                                        },
                                        states_by_pool_key={},
                                        globally_managed=True)

    paid_capacity.debit(budget, first)
    assert budget.remaining_by_location == {first: 1, alias: 1, other: 2}
    paid_capacity.exhaust(budget, alias)
    assert budget.remaining_by_location == {first: 0, alias: 0, other: 2}


def test_restart_restores_missing_claim_from_persisted_pool_key():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    info = _pending_info(1, location)
    info.paid_capacity_pool_key = 'persisted-pool'

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'adopt_paid_capacity_claims',
                               return_value=True) as adopt:
        assert paid_capacity.adopt_existing_claims(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            workspace='w',
            placer=None,
            replica_infos=[info],
            priority=20)

    claims = adopt.call_args.args[2]
    assert claims == [(1, 'persisted-pool', 20, info)]


def test_restart_skips_ambiguous_legacy_instance_type_claim():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    first.instance_type = 'g6.xlarge'
    second.instance_type = 'g6.2xlarge'
    placer = make_placer({first: 1.0, second: 2.0})
    legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    info = _pending_info(1, legacy)

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'adopt_paid_capacity_claims',
                               return_value=True) as adopt:
        assert paid_capacity.adopt_existing_claims(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            workspace='w',
            placer=placer,
            replica_infos=[info],
            priority=20)

    assert adopt.call_args.args[2] == []


def test_restart_claim_adoption_reads_central_catalog_only():
    zero = make_location('research', {'A100': 1}, cloud_name='Kubernetes')
    paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({zero: 0.0, paid: 1.0})
    placer.zero_cost_locations = mock.Mock(wraps=placer.zero_cost_locations)
    info = _pending_info(1, paid)

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'adopt_paid_capacity_claims',
                               return_value=True) as adopt:
        assert paid_capacity.adopt_existing_claims(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            workspace='w',
            placer=placer,
            replica_infos=[info],
            priority=20)

    assert len(adopt.call_args.args[2]) == 1
    placer.zero_cost_locations.assert_called_once_with()


def test_restart_excludes_non_demand_rows_from_claim_adoption():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    infos = [_pending_info(replica_id, location) for replica_id in range(1, 4)]
    for info in infos:
        info.paid_capacity_pool_key = f'pool-{info.replica_id}'
    infos[0].reserved_fill = True
    infos[1].is_zero_cost = True
    infos[2].cost_rebalance_for_replica_id = 99

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'adopt_paid_capacity_claims',
                               return_value=True) as adopt:
        assert paid_capacity.adopt_existing_claims(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            workspace='w',
            placer=None,
            replica_infos=infos,
            priority=20)

    assert adopt.call_args.args[2] == []
