"""Tests for centralized SkyServe paid-capacity policy."""
# pylint: disable=protected-access
from unittest import mock

from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import constants
from sky.serve import paid_capacity
from sky.serve import replica_managers
from sky.utils import common_utils


def _pending_info(replica_id, location):
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=location.use_spot,
                                        location=location,
                                        version=1,
                                        resources_override=location.to_dict())


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


def test_adaptive_limit_ramps_60_to_480_and_resets_on_failure():
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
    assert zero not in budget.pool_key_by_location
    get_states.assert_called_once()


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
    budget = paid_capacity.LaunchBudget(remaining_by_location={location: 1},
                                        pool_key_by_location={
                                            location: paid_capacity.pool_key(
                                                location,
                                                workspace='w',
                                                num_nodes=1)
                                        },
                                        states_by_pool_key={},
                                        globally_managed=True)
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
