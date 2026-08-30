"""Tests for centralized SkyServe paid-capacity policy."""
# pylint: disable=protected-access
import dataclasses
import json
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import capacity_admission
from sky.serve import capacity_planning
from sky.serve import constants
from sky.serve import paid_capacity
from sky.serve import replica_managers
from sky.utils import common_utils


@pytest.fixture(autouse=True)
def _clear_paid_capacity_config_cache(monkeypatch):
    original_pool_key = paid_capacity.pool_key

    def _account_scoped_pool_key(location, **kwargs):
        is_mock_aws = (str(location.cloud).casefold() == 'aws' and
                       not isinstance(location.cloud, paid_capacity.clouds.AWS))
        if (str(location.cloud).casefold() == 'aws' and
                not isinstance(kwargs.get('aws_account_id'), str)):
            kwargs['aws_account_id'] = '123456789012'
        key = original_pool_key(location, **kwargs)
        if is_mock_aws:
            payload = json.loads(key)
            payload['version'] = 2
            payload['provider_identity'] = {
                'aws_account_id': kwargs['aws_account_id'],
            }
            key = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        return key

    monkeypatch.setattr(paid_capacity, 'pool_key', _account_scoped_pool_key)
    monkeypatch.setattr(paid_capacity, '_active_aws_account_id_for_workspace',
                        lambda *_args, **_kwargs: '123456789012')
    paid_capacity._parse_positive_int.cache_clear()
    paid_capacity._parse_service_limit_profiles.cache_clear()
    paid_capacity._warn_service_max_below_floor.cache_clear()
    paid_capacity._admission_summary_log_signature = None
    paid_capacity._admission_summary_logged_at = 0
    yield
    paid_capacity._parse_positive_int.cache_clear()
    paid_capacity._parse_service_limit_profiles.cache_clear()
    paid_capacity._warn_service_max_below_floor.cache_clear()
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


def _provider_free_launch_spec() -> paid_capacity.PaidLaunchSpec:
    location = make_location('us-central1', {'L4': 1}, cloud_name='GCP')
    location.instance_type = 'g2-standard-4'
    pool_key = paid_capacity.pool_key(location,
                                      workspace='default',
                                      num_nodes=1)
    info = _pending_info(7, location)
    info.replica_record_id = '11111111-1111-4111-8111-111111111111'
    info.created_at = None
    info.paid_capacity_pool_key = pool_key
    initial_state = paid_capacity.freeze_paid_launch_payload(
        info.to_storage_dict())
    override = info.to_storage_dict()['resources_override']
    frozen_override = paid_capacity.freeze_paid_launch_payload(override)
    worker = paid_capacity.freeze_paid_launch_payload({
        'schema_version': 1,
        'launch_yaml_content': 'resources: {}',
    })
    return paid_capacity.PaidLaunchSpec(
        ordinal=0,
        service_name='svc',
        service_hash='hash',
        service_lifecycle_epoch=2,
        service_version=1,
        replica_id=7,
        replica_record_id=info.replica_record_id,
        cluster_name_seed=info.cluster_name,
        initial_replica_state=initial_state,
        worker_construction=worker,
        provider_account=None,
        cloud='gcp',
        workspace='default',
        region='us-central1',
        zone=None,
        instance_type='g2-standard-4',
        pool_key=pool_key,
        frontier_key=('l4',),
        accelerator='l4',
        gpu_units_per_node=1,
        num_nodes=1,
        resources_override=frozen_override)


def test_paid_launch_spec_is_deeply_immutable_and_provider_free():
    source = {'nested': {'items': [1, 2]}}
    frozen = paid_capacity.freeze_paid_launch_payload(source)
    source['nested']['items'].append(3)
    assert paid_capacity.thaw_paid_launch_payload(frozen) == {
        'nested': {
            'items': [1, 2]
        }
    }

    spec = _provider_free_launch_spec()
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.replica_id = 8
    forbidden = {
        'replica_info', 'location', 'callback', 'worker', 'claim',
        'capacity_plan_generation', 'demand_feed_generation', 'priority',
        'created_at'
    }
    assert forbidden.isdisjoint(
        field.name for field in dataclasses.fields(spec))
    assert all(not isinstance(value, (dict, list, set))
               for value in vars(spec).values())


def test_paid_launch_spec_decodes_only_inside_persistence_adapter():
    spec = _provider_free_launch_spec()
    persistence = spec.persistence_spec(priority=17, frontier_limit=3)

    assert persistence.candidate.replica_id == spec.replica_id
    assert persistence.candidate.replica_info.replica_record_id == (
        spec.replica_record_id)
    assert persistence.candidate.location.to_pickleable() == (
        persistence.candidate.replica_info.location)
    assert persistence.candidate.capacity_plan_claim is None
    assert persistence.pool_key == spec.pool_key
    assert persistence.frontier_key == ('l4',)
    assert persistence.frontier_limit == 3


def test_paid_launch_receipt_is_sparse_accepted_identity_only():
    spec = _provider_free_launch_spec()
    member = paid_capacity.PaidLaunchReceiptMember(
        replica_id=spec.replica_id,
        replica_record_id=spec.replica_record_id,
        pool_key=spec.pool_key,
        priority=50,
        accelerator='l4',
        plan_units=1,
        physical_gpu_units=1)
    receipt = paid_capacity.PaidLaunchReceipt(service_name='svc',
                                              service_hash='hash',
                                              service_lifecycle_epoch=2,
                                              service_version=1,
                                              capacity_plan_generation=4,
                                              capacity_plan_sha256='a' * 64,
                                              capacity_unit='logical-gpu',
                                              members=(member,))

    assert receipt.members == (member,)
    assert 'outcome' not in {field.name for field in dataclasses.fields(member)}
    with pytest.raises(dataclasses.FrozenInstanceError):
        receipt.members = ()
    with pytest.raises(ValueError, match='canonical'):
        dataclasses.replace(spec,
                            replica_record_id=f'{{{spec.replica_record_id}}}')


def _paid_launch_authority(
    targets: dict[str, int],
    *,
    widths: dict[str, int] | None = None,
    capacity_unit: capacity_planning.CapacityUnit = (
        capacity_planning.CapacityUnit.LOGICAL_GPU),
    backend_num_nodes: int = 1,
) -> capacity_admission.PaidLaunchAuthority:
    canonical = tuple(
        sorted((card.casefold(), units) for card, units in targets.items()))
    if widths is None:
        widths = {card: 1 for card in targets}
    return capacity_admission.PaidLaunchAuthority(
        service_name='svc',
        service_hash='hash',
        generation=8,
        content_sha256='a' * 64,
        demand_feed_generation=9,
        demand_source_epoch=3,
        paid_residual_by_accelerator=canonical,
        paid_launch_target_by_accelerator=canonical,
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()),
        capacity_unit=capacity_unit,
        backend_num_nodes=backend_num_nodes,
        physical_gpu_width_by_accelerator=tuple(
            sorted((card.casefold(), width) for card, width in widths.items())))


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


def test_non_aws_pool_key_retains_existing_v1_identity():
    location = make_location('us-central1', {'L4': 1}, cloud_name='GCP')
    location.instance_type = 'g2-standard-4'

    payload = json.loads(
        paid_capacity.pool_key(location, workspace='w1', num_nodes=1))

    assert payload['version'] == 1
    assert 'provider_identity' not in payload
    stale_v2 = dict(payload, version=2, provider_identity=None)
    assert paid_capacity.pool_key_payload(
        json.dumps(stale_v2, sort_keys=True, separators=(',', ':'))) is None


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


@pytest.mark.parametrize('mutation', [
    lambda payload: payload.pop('workspace'),
    lambda payload: payload.update(accelerators=[['l4', 'bogus']]),
    lambda payload: payload.update(accelerators=[['l4', 1], ['l4', 2]]),
    lambda payload: payload.update(accelerators=[['', 1]]),
    lambda payload: payload.update(use_spot=1),
])
def test_malformed_pool_identity_fails_closed_for_frontier(mutation):
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    payload = json.loads(
        paid_capacity.pool_key(location, workspace='w', num_nodes=1))
    mutation(payload)
    malformed = json.dumps(payload, sort_keys=True, separators=(',', ':'))

    assert paid_capacity.frontier_key_from_pool_key(malformed) is None


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


def test_adaptive_service_limit_profiles_are_exact_and_fail_closed(monkeypatch):
    monkeypatch.delenv(paid_capacity._SERVICE_LIMIT_ENV_VAR, raising=False)
    monkeypatch.delenv(paid_capacity._SERVICE_MAX_LIMIT_ENV_VAR, raising=False)
    monkeypatch.delenv(paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
                       raising=False)
    monkeypatch.setenv(paid_capacity._MAX_EXPLORATION_FRONTIER_ENV_VAR, '2')
    assert paid_capacity.max_service_limit(workspace='w',
                                           service_name='svc',
                                           service_hash='hash') == 16

    monkeypatch.setenv(paid_capacity._SERVICE_MAX_LIMIT_ENV_VAR, '20')
    profile_document = {
        'version': 1,
        'profiles': [{
            'workspace': 'w',
            'service_name': 'svc',
            'service_hash': 'hash',
            'max_launch_window': 24,
            'max_exploration_frontier': 3,
        }],
    }
    monkeypatch.setenv(paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
                       json.dumps(profile_document))
    assert paid_capacity.max_service_limit(workspace='w',
                                           service_name='svc',
                                           service_hash='hash') == 24
    assert paid_capacity.max_service_limit(workspace='w',
                                           service_name='svc',
                                           service_hash='replacement') == 20
    assert paid_capacity.max_service_exploration_frontier(
        workspace='w', service_name='svc', service_hash='hash') == 3
    assert paid_capacity.max_service_exploration_frontier(
        workspace='w', service_name='svc', service_hash='replacement') == 2

    monkeypatch.setenv(paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
                       '{not-json')
    assert paid_capacity.max_service_limit(workspace='w',
                                           service_name='svc',
                                           service_hash='hash') == 20

    monkeypatch.setenv(paid_capacity._SERVICE_LIMIT_ENV_VAR, '32')
    monkeypatch.setenv(paid_capacity._SERVICE_MAX_LIMIT_ENV_VAR, '24')
    with mock.patch.object(paid_capacity.logger, 'warning') as warning:
        assert paid_capacity.max_service_limit(workspace='w',
                                               service_name='svc',
                                               service_hash='replacement') == 32
        assert paid_capacity.max_service_limit(workspace='w',
                                               service_name='svc',
                                               service_hash='replacement') == 32
    warning.assert_called_once()


def test_opaque_and_missing_owned_pools_consume_productive_frontier():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    locations = [first, second]
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    states = {
        keys[first]: {
            'admission_state': 'active',
            'admission_limit': 16,
            'last_success_at': 100,
        },
        keys[second]: {
            'admission_state': 'active',
            'admission_limit': 16,
            'last_success_at': 100,
        },
        'opaque-active-pool': {
            'admission_state': 'active',
            'admission_limit': 64,
            'last_success_at': 100,
        },
    }
    frontiers = {
        location: paid_capacity.frontier_key(location) for location in locations
    }

    # One opaque owned claim and one known-but-no-longer-catalogued owned pool
    # consume the two-pool frontier. Neither can provide current eligible-pool
    # evidence, and the opaque pool must not contribute merely because a stale
    # state row happens to exist for its key.
    assert paid_capacity._evidence_aware_service_limit(
        paid_locations=locations,
        states_by_pool_key=states,
        pool_key_by_location=keys,
        frontier_key_by_location=frontiers,
        owned_pool_keys_by_frontier={('l4',): {'missing-owned-pool'}},
        unknown_owned_pool_keys={'opaque-active-pool'},
        requested_frontier_keys={('l4',)},
        floor=16,
        ceiling=24) == 16


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
    assert budget.service_claim_limit == 16
    assert budget.frontier_limit == 2
    assert budget.max_frontier_limit == 3
    assert budget.frontier_feedback_delay_seconds == 30
    assert zero not in budget.pool_key_by_location
    get_states.assert_called_once()


def _plan_bound_budget(
        placer,
        states,
        *,
        target,
        widths,
        capacity_unit=(capacity_planning.CapacityUnit.LOGICAL_GPU),
        backend_num_nodes=1,
        claimed=None,
        infos=None):
    authority = _paid_launch_authority(target,
                                       widths=widths,
                                       capacity_unit=capacity_unit,
                                       backend_num_nodes=backend_num_nodes)
    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states), mock.patch.object(
                                   paid_capacity.serve_state,
                                   'get_paid_capacity_plan_claimed_units',
                                   return_value=claimed or {}):
        return paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            service_name='svc',
            service_hash='hash',
            existing_replica_infos=[] if infos is None else infos,
            globally_managed=True,
            requested_frontier_keys={(card.casefold(),) for card in target},
            paid_launch_authority=authority)


def test_plan_target_opens_minimum_cold_frontier_in_first_wave():
    locations = [
        make_location(f'us-central-{index}', {'L4': 1}, cloud_name='GCP')
        for index in range(30)
    ]
    placer = make_placer({
        location: float(index + 1) for index, location in enumerate(locations)
    })
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    states = {
        key: {
            'remaining': 4,
            'admission_state': 'active',
            'admission_limit': 4,
            'last_success_at': None,
        } for key in keys.values()
    }

    budget = _plan_bound_budget(placer,
                                states,
                                target={'l4': 100},
                                widths={'l4': 1})

    assert budget.service_remaining == 100
    assert budget.service_claim_limit == 100
    assert budget.plan_bound_cohort is not None
    assert budget.plan_bound_cohort.backend_claim_count == 100
    assert budget.frontier_limit_overrides == {('l4',): 25}
    assert paid_capacity._frontier_limits_by_key(budget)[('l4',)] == 25

    selected = []
    for _ in range(100):
        location = paid_capacity.select_location(
            placer,
            budget,
            skip_zero_cost_preference=True,
            allowed_locations=set(locations))
        assert location is not None
        selected.append(location)
        paid_capacity.debit(budget, location)
    assert paid_capacity.select_location(
        placer,
        budget,
        skip_zero_cost_preference=True,
        allowed_locations=set(locations)) is None
    assert set(selected) == set(locations[:25])
    assert all(selected.count(location) == 4 for location in locations[:25])
    assert not set(selected).intersection(locations[25:])

    # The generated service and frontier cohort is passed unchanged into the
    # real Phase-A adapter; it is not recomputed from the legacy cold window.
    authority = _paid_launch_authority({'l4': 100}, widths={'l4': 1})
    infos = [
        _pending_info(replica_id, location)
        for replica_id, location in enumerate(selected, start=1)
    ]
    candidates = tuple(
        paid_capacity.PaidClaimCandidate(
            replica_id=info.replica_id,
            replica_info=info,
            location=location,
            priority=20,
            capacity_plan_claim=authority.claim_values('l4'))
        for info, location in zip(infos, selected, strict=True))
    with mock.patch.object(paid_capacity.serve_state,
                           'try_add_replicas_with_paid_capacity_claims',
                           return_value=['acquired'] * 100) as persist:
        persisted = paid_capacity.try_persist_claim_batch(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            candidates=candidates,
            budget=budget)

    assert len(persisted.committed_members) == 100
    assert persist.call_args.kwargs['service_limit'] == 100
    assert persist.call_args.kwargs['frontier_limits_by_key'] == {('l4',): 25}
    assert len(persist.call_args.args[2]) == 100


def test_plan_cohort_converts_gpu_units_to_exact_backend_claims():
    locations = [
        make_location(f'us-central-{index}', {'L4': 4}, cloud_name='GCP')
        for index in range(8)
    ]
    # A cheaper shape for the same card is deliberately present. The manager
    # and Phase A require the planner's exact four-GPU backend width.
    wrong_width = make_location('us-cheap', {'L4': 1}, cloud_name='GCP')
    placer = make_placer({
        wrong_width: 0.5,
        **{
            location: float(index + 1) for index, location in enumerate(locations)
        },
    })
    all_locations = [wrong_width, *locations]
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in all_locations
    }
    states = {
        key: {
            'remaining': 4,
            'admission_state': 'active',
            'admission_limit': 4,
            'last_success_at': None,
        } for key in keys.values()
    }

    budget = _plan_bound_budget(placer,
                                states,
                                target={'l4': 100},
                                widths={'l4': 4})

    assert budget.plan_bound_cohort is not None
    assert budget.plan_bound_cohort.targets == (
        paid_capacity.PlanBoundAdmissionTarget(frontier_key=('l4',),
                                               remaining_plan_units=100,
                                               physical_backend_width=4,
                                               claim_units_per_backend=4,
                                               backend_claim_count=25,
                                               frontier_limit=7),)
    assert budget.service_remaining == 25
    assert budget.frontier_limit_overrides == {('l4',): 7}
    assert budget.remaining_by_location[wrong_width] == 0


def test_physical_backend_plan_debits_one_unit_on_eight_gpu_locations():
    locations = [
        make_location(f'a100-pool-{index}', {'A100': 8}, cloud_name='GCP')
        for index in range(30)
    ]
    placer = make_placer({
        location: float(index + 1) for index, location in enumerate(locations)
    })
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    states = {
        key: {
            'remaining': 4,
            'admission_state': 'active',
            'admission_limit': 4,
            'last_success_at': None,
        } for key in keys.values()
    }

    budget = _plan_bound_budget(
        placer,
        states,
        target={'a100': 100},
        widths={'a100': 8},
        capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND)

    assert budget.plan_bound_cohort is not None
    assert budget.plan_bound_cohort.targets == (
        paid_capacity.PlanBoundAdmissionTarget(frontier_key=('a100',),
                                               remaining_plan_units=100,
                                               physical_backend_width=8,
                                               claim_units_per_backend=1,
                                               backend_claim_count=100,
                                               frontier_limit=25),)
    assert budget.service_remaining == 100
    assert budget.frontier_limit_overrides == {('a100',): 25}


def test_plan_bound_multinode_shape_requires_per_node_width_and_node_count():
    matching = make_location('matching', {'L4': 8}, cloud_name='GCP')
    wrong_nodes = make_location('wrong-nodes', {'L4': 8}, cloud_name='GCP')
    wrong_width = make_location('wrong-width', {'L4': 4}, cloud_name='GCP')
    authority = _paid_launch_authority(
        {'l4': 1},
        widths={'l4': 8},
        capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
        backend_num_nodes=2)
    locations = (matching, wrong_nodes, wrong_width)
    pool_keys = {
        matching: paid_capacity.pool_key(matching, workspace='w', num_nodes=2),
        wrong_nodes: paid_capacity.pool_key(wrong_nodes,
                                            workspace='w',
                                            num_nodes=1),
        wrong_width: paid_capacity.pool_key(wrong_width,
                                            workspace='w',
                                            num_nodes=2),
    }

    def _cohort(candidates):
        return paid_capacity._plan_bound_admission_cohort(
            authority=authority,
            service_name='svc',
            service_hash='hash',
            paid_locations=candidates,
            remaining_by_location={location: 1 for location in candidates},
            pool_key_by_location={
                location: pool_keys[location] for location in candidates
            },
            frontier_key_by_location={
                location: ('l4',) for location in candidates
            },
            owned_pool_keys_by_frontier={},
            unknown_owned_pool_keys=set(),
            requested_frontier_keys={('l4',)},
            claimed_plan_units_by_accelerator={})

    cohort = _cohort(locations)
    assert cohort.targets == (paid_capacity.PlanBoundAdmissionTarget(
        frontier_key=('l4',),
        remaining_plan_units=1,
        physical_backend_width=16,
        claim_units_per_backend=1,
        backend_claim_count=1,
        frontier_limit=2),)
    assert _cohort((wrong_nodes, wrong_width)).targets == ()


def test_plan_cohort_subtracts_same_generation_debits_before_preparation():
    locations = [
        make_location(f'us-central-{index}', {'L4': 4}, cloud_name='GCP')
        for index in range(8)
    ]
    placer = make_placer({
        location: float(index + 1) for index, location in enumerate(locations)
    })
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    states = {
        key: {
            'remaining': 4,
            'admission_state': 'active',
            'admission_limit': 4,
            'last_success_at': None,
        } for key in keys.values()
    }
    infos = [_pending_info(replica_id, locations[0]) for replica_id in range(5)]
    for info in infos:
        info.paid_capacity_pool_key = keys[locations[0]]

    budget = _plan_bound_budget(placer,
                                states,
                                target={'l4': 100},
                                widths={'l4': 4},
                                claimed={'l4': 20},
                                infos=infos)

    assert budget.plan_bound_cohort is not None
    assert budget.plan_bound_cohort.targets[0].remaining_plan_units == 80
    assert budget.plan_bound_cohort.backend_claim_count == 20
    assert budget.service_remaining == 20
    assert budget.service_claim_limit == 25


def test_plan_cohort_skips_closed_cheapest_and_on_demand_pool():
    on_demand = make_location('on-demand', {'L4': 1},
                              use_spot=False,
                              cloud_name='GCP')
    spots = [
        make_location(f'spot-{index}', {'L4': 1}, cloud_name='GCP')
        for index in range(4)
    ]
    placer = make_placer({
        on_demand: 0.1,
        **{
            location: float(index + 1) for index, location in enumerate(spots)
        }
    })
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in spots
    }
    remaining = [0, 4, 4, 4]
    states = {
        keys[location]: {
            'remaining': pool_remaining,
            'admission_state': 'cooldown' if index == 0 else 'active',
            'admission_limit': pool_remaining,
            'last_success_at': None,
        } for index, (location,
                     pool_remaining) in enumerate(zip(spots, remaining))
    }

    budget = _plan_bound_budget(placer,
                                states,
                                target={'l4': 9},
                                widths={'l4': 1})

    assert on_demand not in budget.remaining_by_location
    assert budget.frontier_limit_overrides == {('l4',): 3}
    selected = []
    for _ in range(9):
        location = paid_capacity.select_location(placer,
                                                 budget,
                                                 skip_zero_cost_preference=True,
                                                 allowed_locations=set(spots))
        assert location is not None
        selected.append(location)
        paid_capacity.debit(budget, location)
    assert [selected.count(location) for location in spots] == [0, 4, 4, 1]


def test_unavailable_plan_card_does_not_block_independent_valid_card():
    l4_locations = [
        make_location(f'l4-{index}', {'L4': 1}, cloud_name='GCP')
        for index in range(2)
    ]
    wrong_a100_width = make_location('a100-wrong-width', {'A100': 1},
                                     cloud_name='GCP')
    on_demand_a100 = make_location('a100-on-demand', {'A100': 8},
                                   use_spot=False,
                                   cloud_name='GCP')
    paid_locations = [*l4_locations, wrong_a100_width]
    placer = make_placer({
        on_demand_a100: 0.1,
        wrong_a100_width: 0.2,
        l4_locations[0]: 1.0,
        l4_locations[1]: 2.0,
    })
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in paid_locations
    }
    states = {
        key: {
            'remaining': 4,
            'admission_state': 'active',
            'admission_limit': 4,
            'last_success_at': None,
        } for key in keys.values()
    }

    budget = _plan_bound_budget(placer,
                                states,
                                target={
                                    'a100': 8,
                                    'l4': 8,
                                },
                                widths={
                                    'a100': 8,
                                    'l4': 1,
                                })

    assert budget.plan_bound_cohort is not None
    assert [target.frontier_key for target in budget.plan_bound_cohort.targets
           ] == [('l4',)]
    assert budget.service_remaining == 8
    assert budget.frontier_limit_overrides == {('l4',): 2}
    assert budget.remaining_by_location[wrong_a100_width] == 0
    assert on_demand_a100 not in budget.remaining_by_location
    selected = []
    for _ in range(8):
        location = paid_capacity.select_location(
            placer,
            budget,
            skip_zero_cost_preference=True,
            allowed_locations=set(paid_locations))
        assert location in l4_locations
        selected.append(location)
        paid_capacity.debit(budget, location)
    assert {
        location: selected.count(location) for location in l4_locations
    } == {
        l4_locations[0]: 4,
        l4_locations[1]: 4,
    }


@pytest.mark.parametrize(('globally_managed', 'central_available'),
                         [(False, True), (True, False)])
def test_plan_authority_never_falls_back_to_legacy_local_admission(
        globally_managed, central_available):
    location = make_location('us-central1', {'L4': 1}, cloud_name='GCP')
    placer = make_placer({location: 1.0})
    authority = _paid_launch_authority({'l4': 4}, widths={'l4': 1})

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=central_available):
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            service_name='svc',
            service_hash='hash',
            existing_replica_infos=[],
            globally_managed=globally_managed,
            paid_launch_authority=authority)

    assert budget.remaining_by_location == {location: 0}
    assert budget.service_remaining == 0
    assert paid_capacity.select_location(placer,
                                         budget,
                                         skip_zero_cost_preference=True,
                                         allowed_locations={location}) is None
    info = _pending_info(1, location)
    persisted = paid_capacity.try_persist_claim_batch(
        service_name='svc',
        service_hash='hash',
        controller_owner=(1, '10.0.0.1'),
        candidates=(paid_capacity.PaidClaimCandidate(
            replica_id=1,
            replica_info=info,
            location=location,
            priority=20,
            capacity_plan_claim=authority.claim_values('l4')),),
        budget=budget)
    assert persisted.members[0].claim_result is (
        paid_capacity.ClaimResult.SERVICE_SATURATED)


def test_generic_paid_budget_preserves_ordinary_on_demand():
    on_demand = make_location('us-central1', {'L4': 1},
                              use_spot=False,
                              cloud_name='GCP')
    spot = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    # Planner-authorized reserved-fill launches narrow their candidates at the
    # ReplicaManager handoff.  The generic shared budget must continue to
    # support ordinary SkyServe services whose configured paid location is
    # on-demand.
    placer = make_placer({on_demand: 0.25, spot: 1.0})

    budget = paid_capacity.build_launch_budget(placer,
                                               workspace='w',
                                               existing_replica_infos=[],
                                               globally_managed=False)

    assert on_demand in budget.remaining_by_location
    assert on_demand in budget.pool_key_by_location
    assert spot in budget.remaining_by_location
    assert paid_capacity.select_location(placer, budget) == on_demand


def test_global_budget_uses_exact_profile_and_durable_pool_evidence(
        monkeypatch):
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({location: 1.0})
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)
    monkeypatch.setenv(paid_capacity._MAX_EXPLORATION_FRONTIER_ENV_VAR, '2')
    monkeypatch.setenv(
        paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
        json.dumps({
            'version': 1,
            'profiles': [{
                'workspace': 'w',
                'service_name': 'svc',
                'service_hash': 'hash',
                'max_launch_window': 24,
                'max_exploration_frontier': 3,
            }],
        }))
    states = {
        key: {
            'remaining': 24,
            'admission_state': 'active',
            'admission_limit': 32,
            'last_success_at': 100,
        }
    }

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   service_name='svc',
                                                   service_hash='hash',
                                                   existing_replica_infos=[],
                                                   globally_managed=True,
                                                   requested_frontier_keys={
                                                       ('l4',)
                                                   })
        replacement_budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            service_name='svc',
            service_hash='replacement',
            existing_replica_infos=[],
            globally_managed=True,
            requested_frontier_keys={('l4',)})

    assert budget.service_claim_limit == 24
    assert budget.service_remaining == 24
    assert budget.max_frontier_limit == 3
    assert replacement_budget.service_claim_limit == 16
    assert replacement_budget.service_remaining == 16
    assert replacement_budget.max_frontier_limit == 2


def test_productive_frontier_uses_placer_cost_order_not_catalog_order(
        monkeypatch):
    expensive = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
    middle = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    # Insertion order is deliberately the reverse of placement cost.
    placer = make_placer({expensive: 3.0, middle: 2.0, cheap: 1.0})
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in (expensive, middle, cheap)
    }
    monkeypatch.setenv(
        paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
        json.dumps({
            'version': 1,
            'profiles': [{
                'workspace': 'w',
                'service_name': 'svc',
                'service_hash': 'hash',
                'max_launch_window': 24,
            }],
        }))
    states = {
        key: {
            'remaining': 32,
            'admission_state': 'active',
            'admission_limit': 32,
            # Only the expensive pool has positive evidence. It is outside the
            # cheapest two-pool frontier and therefore cannot widen service
            # admission.
            'last_success_at': 100 if location is expensive else None,
        } for location, key in keys.items()
    }

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   service_name='svc',
                                                   service_hash='hash',
                                                   existing_replica_infos=[],
                                                   globally_managed=True,
                                                   requested_frontier_keys={
                                                       ('l4',)
                                                   })

    assert placer.ranked_active_locations() == [cheap, middle, expensive]
    assert budget.service_claim_limit == 16


@pytest.mark.parametrize('admission_state', ['cooldown', 'probe'])
def test_cooldown_and_probe_pools_do_not_widen_service_limit(admission_state):
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)

    assert paid_capacity._evidence_aware_service_limit(
        paid_locations=[location],
        states_by_pool_key={
            key: {
                'admission_state': admission_state,
                'admission_limit': 16,
                'last_success_at': 100,
            }
        },
        pool_key_by_location={location: key},
        frontier_key_by_location={location: ('l4',)},
        owned_pool_keys_by_frontier={},
        unknown_owned_pool_keys=set(),
        requested_frontier_keys={('l4',)},
        floor=4,
        ceiling=24) == 4


def test_duplicate_pool_alias_counts_once_and_productive_sum_is_capped():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    alias = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    alias.image_id = {'us-east-1': 'ami-alias'}
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    first_key = paid_capacity.pool_key(first, workspace='w', num_nodes=1)
    assert paid_capacity.pool_key(alias, workspace='w',
                                  num_nodes=1) == first_key
    second_key = paid_capacity.pool_key(second, workspace='w', num_nodes=1)
    locations = [first, alias, second]

    limit = paid_capacity._evidence_aware_service_limit(
        paid_locations=locations,
        states_by_pool_key={
            first_key: {
                'admission_state': 'active',
                'admission_limit': 16,
                'last_success_at': 100,
            },
            second_key: {
                'admission_state': 'active',
                'admission_limit': 16,
                'last_success_at': 100,
            },
        },
        pool_key_by_location={
            first: first_key,
            alias: first_key,
            second: second_key,
        },
        frontier_key_by_location={location: ('l4',) for location in locations},
        owned_pool_keys_by_frontier={},
        unknown_owned_pool_keys=set(),
        requested_frontier_keys={('l4',)},
        floor=4,
        ceiling=24)

    assert limit == 24


def test_dynamic_service_overage_is_preserved_and_blocks_new_claims(
        monkeypatch):
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({location: 1.0})
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)
    infos = [_pending_info(replica_id, location) for replica_id in range(25)]
    for info in infos:
        info.paid_capacity_pool_key = key
    monkeypatch.setenv(
        paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
        json.dumps({
            'version': 1,
            'profiles': [{
                'workspace': 'w',
                'service_name': 'svc',
                'service_hash': 'hash',
                'max_launch_window': 24,
            }],
        }))

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value={
                                   key: {
                                       'remaining': 7,
                                       'admission_state': 'active',
                                       'admission_limit': 32,
                                       'last_success_at': 100,
                                   }
                               }):
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   service_name='svc',
                                                   service_hash='hash',
                                                   existing_replica_infos=infos,
                                                   globally_managed=True,
                                                   requested_frontier_keys={
                                                       ('l4',)
                                                   })

    assert len(infos) == 25
    assert budget.service_claim_limit == 24
    assert budget.service_remaining == 0


def test_productive_frontier_widens_only_requested_bounded_card():
    l4_first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    l4_second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    l4_outside_frontier = make_location('eu-west-1', {'L4': 1},
                                        cloud_name='AWS')
    a100 = make_location('us-central1', {'A100': 1}, cloud_name='GCP')
    locations = [l4_first, l4_second, l4_outside_frontier, a100]
    keys = {
        location: paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        for location in locations
    }
    frontiers = {
        location: paid_capacity.frontier_key(location) for location in locations
    }
    states = {
        keys[l4_first]: {
            'admission_state': 'active',
            'admission_limit': 16,
            'last_success_at': 100,
        },
        keys[l4_second]: {
            'admission_state': 'active',
            'admission_limit': 8,
            'last_success_at': 100,
        },
        keys[l4_outside_frontier]: {
            'admission_state': 'active',
            'admission_limit': 64,
            'last_success_at': 100,
        },
        keys[a100]: {
            'admission_state': 'active',
            'admission_limit': 64,
            'last_success_at': 100,
        },
    }

    assert paid_capacity._evidence_aware_service_limit(
        paid_locations=locations,
        states_by_pool_key=states,
        pool_key_by_location=keys,
        frontier_key_by_location=frontiers,
        owned_pool_keys_by_frontier={},
        unknown_owned_pool_keys=set(),
        requested_frontier_keys={('l4',)},
        floor=16,
        ceiling=24) == 24

    states[keys[l4_first]]['last_success_at'] = None
    assert paid_capacity._evidence_aware_service_limit(
        paid_locations=locations,
        states_by_pool_key=states,
        pool_key_by_location=keys,
        frontier_key_by_location=frontiers,
        owned_pool_keys_by_frontier={},
        unknown_owned_pool_keys=set(),
        requested_frontier_keys={('l4',)},
        floor=16,
        ceiling=24) == 16

    assert paid_capacity._evidence_aware_service_limit(
        paid_locations=locations,
        states_by_pool_key=states,
        pool_key_by_location=keys,
        frontier_key_by_location=frontiers,
        owned_pool_keys_by_frontier={},
        unknown_owned_pool_keys=set(),
        requested_frontier_keys={('a100',)},
        floor=16,
        ceiling=24) == 24


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


def test_paid_gpu_cap_charges_logical_and_physical_backend_widths():
    logical_four = make_location('us-east-1', {'L4': 4}, cloud_name='AWS')
    physical_eight = make_location('us-west-2', {'L4': 8}, cloud_name='GCP')
    zero_cost_location = make_location('research', {'L4': 8},
                                       cloud_name='Kubernetes')
    placer = make_placer({
        logical_four: 1.0,
        physical_eight: 2.0,
        zero_cost_location: 0,
    })
    logical = _pending_info(1, logical_four)
    logical.planned_capacity = 4
    logical.paid_capacity_pool_key = paid_capacity.pool_key(logical_four,
                                                            workspace='w',
                                                            num_nodes=1)
    physical = _pending_info(2, physical_eight)
    # Physical-backend target accounting deliberately persists one unit.
    physical.planned_capacity = 1
    physical.paid_capacity_pool_key = paid_capacity.pool_key(physical_eight,
                                                             workspace='w',
                                                             num_nodes=1)
    zero_cost = _pending_info(3, physical_eight)
    zero_cost.is_zero_cost = True
    zero_cost.planned_capacity = 100
    cleaned_paid = _pending_info(4, physical_eight)
    cleaned_paid.planned_capacity = 100
    cleaned_paid.status_property.sky_down_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    states = {
        paid_capacity.pool_key(location, workspace='w', num_nodes=1): {
            'remaining': 16
        } for location in (logical_four, physical_eight)
    }

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            existing_replica_infos=[logical, physical, zero_cost, cleaned_paid],
            globally_managed=True,
            max_live_paid_gpu_units=16)

    # Logical width four and physical-backend width eight both charge the
    # physical cards they can bill. Zero-cost and cleanup-proven rows do not.
    assert budget.live_paid_gpu_units == 12
    assert budget.paid_gpu_units_remaining == 4
    assert budget.service_remaining == 14
    assert paid_capacity.select_location(placer, budget) == zero_cost_location
    assert paid_capacity.select_location(
        placer, budget, skip_zero_cost_preference=True) == logical_four
    assert paid_capacity.select_location(placer,
                                         budget,
                                         skip_zero_cost_preference=True,
                                         allowed_locations={physical_eight
                                                           }) is None


def test_paid_gpu_cap_uses_pool_node_count_and_malformed_rows_fail_closed():
    paid_location = make_location('us-west-2', {'L4': 8}, cloud_name='GCP')
    zero_cost_location = make_location('research', {'L4': 8},
                                       cloud_name='Kubernetes')
    placer = make_placer({paid_location: 1.0, zero_cost_location: 0})
    malformed = _pending_info(1, paid_location)
    malformed.planned_capacity = 1
    malformed.paid_capacity_pool_key = paid_capacity.pool_key(paid_location,
                                                              workspace='w',
                                                              num_nodes=2)
    assert paid_capacity.paid_replica_gpu_units(malformed) == 16
    # The exact pool says two width-eight nodes, while a corrupted duplicate
    # claims a different per-node shape. Never guess between them.
    malformed.resources_override['accelerators'] = {'L4': 4}
    states = {
        paid_capacity.pool_key(paid_location, workspace='w', num_nodes=1): {
            'remaining': 16
        }
    }

    with pytest.raises(paid_capacity.PaidGPUAttributionError):
        paid_capacity._live_paid_gpu_units([malformed])
    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            existing_replica_infos=[malformed],
            globally_managed=True,
            max_live_paid_gpu_units=32)

    assert budget.live_paid_gpu_units is None
    assert budget.paid_gpu_units_remaining == 0
    assert budget.service_remaining == 0
    # Paid attribution failure does not suppress an independent zero-cost path.
    assert paid_capacity.select_location(placer, budget) == zero_cost_location
    assert paid_capacity.select_location(placer,
                                         budget,
                                         skip_zero_cost_preference=True) is None


def test_multinode_paid_gpu_headroom_and_debit_use_total_backend_width():
    location = make_location('us-west-2', {'L4': 8}, cloud_name='GCP')
    zero_cost = make_location('research', {'L4': 8}, cloud_name='Kubernetes')
    placer = make_placer({location: 1.0, zero_cost: 0})
    placer.num_nodes = 2
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=2)
    states = {key: {'remaining': 2}}

    def _budget(cap):
        with mock.patch.object(paid_capacity,
                               'central_authority_available',
                               return_value=True), mock.patch.object(
                                   paid_capacity.serve_state,
                                   'get_paid_capacity_pool_states',
                                   return_value=states):
            return paid_capacity.build_launch_budget(
                placer,
                workspace='w',
                existing_replica_infos=[],
                globally_managed=True,
                max_live_paid_gpu_units=cap)

    insufficient = _budget(15)
    assert paid_capacity.select_location(placer,
                                         insufficient,
                                         skip_zero_cost_preference=True,
                                         allowed_locations={location}) is None
    assert paid_capacity.select_location(placer, insufficient) == zero_cost

    exact = _budget(16)
    assert paid_capacity.select_location(placer,
                                         exact,
                                         skip_zero_cost_preference=True,
                                         allowed_locations={location
                                                           }) == location
    paid_capacity.debit(exact, location)
    assert exact.paid_gpu_units_remaining == 0


def test_cpu_paid_pool_has_typed_zero_gpu_debit_without_cap():
    location = make_location('cpu-spot', None, cloud_name='GCP')
    placer = make_placer({location: 1.0})
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=3)
    placer.num_nodes = 3
    states = {key: {'remaining': 1}}
    info = _pending_info(1, location)
    info.paid_capacity_pool_key = key

    assert paid_capacity.paid_replica_gpu_units(info) == 0
    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               return_value=states):
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            existing_replica_infos=[info],
            globally_managed=True,
            max_live_paid_gpu_units=None)

    assert paid_capacity.select_location(
        placer, budget, skip_zero_cost_preference=True) == location


def test_paid_pool_shape_is_one_typed_per_node_authority():
    location = make_location('multi-node', {'L4': 8}, cloud_name='GCP')
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=2)

    shape = paid_capacity.paid_pool_gpu_shape(key)

    assert shape == paid_capacity.PhysicalBackendShape(accelerator='l4',
                                                       gpu_units_per_node=8,
                                                       num_nodes=2)
    assert shape.total_gpu_units == 16


def test_huge_json_gpu_count_fails_with_typed_attribution_error():
    location = make_location('huge', {'L4': 1}, cloud_name='GCP')
    payload = json.loads(
        paid_capacity.pool_key(location, workspace='w', num_nodes=1))
    payload['accelerators'][0][1] = 10**1000
    malformed = json.dumps(payload, sort_keys=True, separators=(',', ':'))

    with pytest.raises(paid_capacity.PaidGPUAttributionError):
        paid_capacity.paid_pool_gpu_units(malformed)


def test_physical_shape_product_overflow_fails_closed():
    max_exact = (1 << 63) - 1
    location = make_location('overflow', {'L4': max_exact}, cloud_name='GCP')
    key = paid_capacity.pool_key(location, workspace='w', num_nodes=2)

    with pytest.raises(paid_capacity.PaidGPUAttributionError,
                       match='exact accounting range'):
        paid_capacity.paid_pool_gpu_shape(key)
    authority = _paid_launch_authority(
        {'l4': 1},
        widths={'l4': max_exact},
        capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
        backend_num_nodes=2)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no exact backend claim shape'):
        authority.backend_shape('l4')


def test_paid_gpu_cap_zero_and_postgres_unavailable_fail_closed():
    location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({location: 1.0})
    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=False), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states') as get_states:
        budget = paid_capacity.build_launch_budget(placer,
                                                   workspace='w',
                                                   existing_replica_infos=[],
                                                   globally_managed=True,
                                                   max_live_paid_gpu_units=0)

    assert not budget.globally_managed
    assert budget.remaining_by_location == {location: 0}
    assert budget.service_remaining == 0
    assert budget.paid_gpu_units_remaining == 0
    assert paid_capacity.select_location(placer, budget) is None
    get_states.assert_not_called()
    with mock.patch.object(paid_capacity.serve_state,
                           'try_add_replica_with_paid_capacity_claim') as claim:
        result = paid_capacity.try_persist_claim(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            replica_id=1,
            replica_info=_pending_info(1, location),
            location=location,
            budget=budget,
            priority=20)
    assert result is paid_capacity.ClaimResult.SERVICE_SATURATED
    claim.assert_not_called()


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


def test_unmanaged_budget_never_resolves_aws_provider_identity(monkeypatch):
    location = paid_capacity.spot_placer.Location(
        cloud=paid_capacity.clouds.AWS(),
        region='us-east-1',
        zone='us-east-1a',
        accelerators={'L4': 1},
        use_spot=True,
        instance_type='g6.xlarge')
    placer = make_placer({location: 1.0})
    identity = mock.Mock(side_effect=AssertionError(
        'unmanaged admission must not resolve provider identity'))
    monkeypatch.setattr(paid_capacity, '_active_aws_account_id_for_locations',
                        identity)

    budget = paid_capacity.build_launch_budget(placer,
                                               workspace='w',
                                               existing_replica_infos=[],
                                               globally_managed=False)

    identity.assert_not_called()
    payload = paid_capacity.pool_key_payload(
        budget.pool_key_by_location[location])
    assert payload is not None
    assert payload['version'] == 1
    assert 'provider_identity' not in payload


def test_provider_free_budget_reuses_only_committed_aws_account(monkeypatch):
    location = paid_capacity.spot_placer.Location(
        cloud=paid_capacity.clouds.AWS(),
        region='us-east-1',
        zone='us-east-1a',
        accelerators={'L4': 1},
        use_spot=True,
        instance_type='g6.xlarge')
    placer = make_placer({location: 1.0})
    info = _pending_info(1, location)
    info.paid_capacity_pool_key = paid_capacity.pool_key(
        location, workspace='w', num_nodes=1, aws_account_id='123456789012')
    identity = mock.Mock(side_effect=AssertionError(
        'read-only admission must not resolve provider identity'))
    monkeypatch.setattr(paid_capacity, '_active_aws_account_id_for_locations',
                        identity)

    def _states(keys, **_):
        return {key: {'remaining': 1} for key in keys}

    with mock.patch.object(paid_capacity,
                           'central_authority_available',
                           return_value=True), mock.patch.object(
                               paid_capacity.serve_state,
                               'get_paid_capacity_pool_states',
                               side_effect=_states):
        budget = paid_capacity.build_launch_budget(
            placer,
            workspace='w',
            existing_replica_infos=[info],
            globally_managed=True,
            allow_provider_identity_lookup=False)

    identity.assert_not_called()
    payload = paid_capacity.pool_key_payload(
        budget.pool_key_by_location[location])
    assert payload is not None
    assert payload['version'] == 2
    assert payload['provider_identity'] == {'aws_account_id': '123456789012'}


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


def test_claim_rejects_out_of_range_priority():
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
        service_claim_limit=24,
        max_live_paid_gpu_units=8,
        live_paid_gpu_units=2,
        paid_gpu_units_remaining=6,
        frontier_limit=2,
        max_frontier_limit=3,
        frontier_key_by_location={location: ('l4',)},
        frontier_limit_overrides={('l4',): 3})
    info = _pending_info(1, location)
    with pytest.raises(ValueError, match='priority must be exact'):
        paid_capacity.try_persist_claim(service_name='svc',
                                        service_hash='hash',
                                        controller_owner=(1, '10.0.0.1'),
                                        replica_id=1,
                                        replica_info=info,
                                        location=location,
                                        budget=budget,
                                        priority=1000)


def test_claim_batch_returns_exact_typed_members_and_publishes_only_committed():
    first = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    second = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    first.instance_type = 'g6.xlarge'
    second.instance_type = 'g6.2xlarge'
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={
            first: 1,
            second: 1
        },
        pool_key_by_location={
            first: paid_capacity.pool_key(first, workspace='w', num_nodes=1),
            second: paid_capacity.pool_key(second, workspace='w', num_nodes=1),
        },
        states_by_pool_key={},
        globally_managed=True,
        frontier_key_by_location={
            first: ('l4',),
            second: ('l4',)
        },
        frontier_limit=2)
    first_info = _pending_info(1, first)
    second_info = _pending_info(2, second)
    candidates = (
        paid_capacity.PaidClaimCandidate(1, first_info, first, 20),
        paid_capacity.PaidClaimCandidate(2, second_info, second, 20),
    )

    with mock.patch.object(paid_capacity.serve_state,
                           'try_add_replicas_with_paid_capacity_claims',
                           return_value=['acquired', 'saturated']):
        result = paid_capacity.try_persist_claim_batch(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            candidates=candidates,
            budget=budget)

    assert result == paid_capacity.PaidClaimBatchResult((
        paid_capacity.PaidClaimBatchMemberResult(
            1, first_info.replica_record_id,
            paid_capacity.ClaimResult.ACQUIRED),
        paid_capacity.PaidClaimBatchMemberResult(
            2, second_info.replica_record_id,
            paid_capacity.ClaimResult.SATURATED),
    ))
    assert result.committed_members == result.members[:1]
    assert first_info.paid_capacity_pool_key == budget.pool_key_by_location[
        first]
    assert second_info.paid_capacity_pool_key is None


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


def test_normal_second_pool_preserves_cost_order_across_regions():
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

    assert paid_capacity.select_location(placer, budget) == same_domain


def test_owned_cheapest_pool_keeps_headroom_before_opening_another_pool():
    primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
    different_domain = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
    placer = make_placer({primary: 0.5, different_domain: 2.0})
    budget = _exploration_budget([primary, different_domain],
                                 owned_locations=[primary],
                                 remaining=[1, 4])

    assert paid_capacity.select_location(placer, budget) == primary
    paid_capacity.debit(budget, primary)
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


def test_delayed_third_pool_preserves_cost_order_across_regions():
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

    assert selected == same_domain
    assert budget.frontier_limit_overrides == {('l4',): 3}
    assert budget.feedback_deferred_frontiers == set()
    message = info.call_args.args[0]
    assert 'from_limit=2' in message
    assert 'to_limit=3' in message
    assert 'youngest_unresolved_claim_age_seconds=100' in message
    assert 'candidate_cloud=aws' in message
    assert 'candidate_region=us-east-1' in message
    assert budget.pool_key_by_location[first] not in message
    assert snapshot[same_domain]['frontier_limit'] == 3
    assert snapshot[same_domain]['frontier_max_limit'] == 3
    assert snapshot[first]['frontier_owned']
    assert not snapshot[same_domain]['frontier_owned']
    assert snapshot[same_domain]['frontier_owned_pool_count'] == 2
    assert snapshot[same_domain]['youngest_unresolved_claim_age_seconds'] == 100


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


def test_delayed_third_pool_can_use_a_cheaper_existing_region():
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
        assert paid_capacity.select_location(placer, budget) == east_alternate

    assert budget.frontier_limit_overrides == {('l4',): 3}


def test_delayed_third_pool_fails_closed_for_opaque_pool_identity():
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
    budget.unknown_owned_pool_keys.add(malformed_key)
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

    with mock.patch.object(
            paid_capacity, 'central_authority_available',
            return_value=True), mock.patch.object(
                paid_capacity.serve_state,
                'adopt_paid_capacity_claims',
                return_value=True) as adopt, mock.patch.object(
                    paid_capacity,
                    '_active_aws_account_id_for_locations',
                    side_effect=RuntimeError(
                        'identity must not be resolved')) as identity:
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
    identity.assert_not_called()


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

    with mock.patch.object(
            paid_capacity, 'central_authority_available',
            return_value=True), mock.patch.object(
                paid_capacity.serve_state,
                'adopt_paid_capacity_claims',
                return_value=True) as adopt, mock.patch.object(
                    paid_capacity,
                    '_active_aws_account_id_for_locations',
                    side_effect=RuntimeError(
                        'identity must not be resolved')) as identity:
        assert paid_capacity.adopt_existing_claims(
            service_name='svc',
            service_hash='hash',
            controller_owner=(1, '10.0.0.1'),
            workspace='w',
            placer=placer,
            replica_infos=[info],
            priority=20)

    claims = adopt.call_args.args[2]
    assert len(claims) == 1
    adopted_pool = paid_capacity.pool_key_payload(claims[0][1])
    assert adopted_pool is not None
    assert adopted_pool['version'] == 1
    assert 'provider_identity' not in adopted_pool
    identity.assert_not_called()
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
