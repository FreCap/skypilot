"""Tests for AWS capacity classification and cache scoping."""
# pylint: disable=protected-access
import inspect
import pickle
import unittest.mock as mock

import botocore.exceptions
import pytest

from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import capacity_cache
from sky.provision import capacity_policy
from sky.provision import common as provision_common
from sky.provision.aws import instance as aws_instance
from sky.provision.gcp import instance as gcp_instance
from sky.provision.gcp import instance_utils as gcp_instance_utils

_CAPACITY_POLICY_SIGNATURES = {
    '_iter_error_chain': '(error)',
    '_provider_error_codes': '(error)',
    '_classify_capacity_error': '(cloud, error)',
    '_terminal_failover_leaves': '(error)',
    '_terminal_leaf_cause_nodes': '(failure, *, history_depth, remaining_nodes)',
    'classify_resources_unavailable_error': '(cloud, error)',
    '_is_quota_error': '(error)',
    '_canonical_accelerators': '(to_provision)',
    '_capacity_cache_cloud_name': '(to_provision)',
    '_capacity_cache_account': '(cloud, cloud_user_identity)',
    '_capacity_cache_key': '(to_provision, region, zones, num_nodes, account)',
    '_quota_cooldown_key': '(to_provision, region, num_nodes, account)',
    '_fully_created_fresh_demand': '(provision_record, num_nodes, cluster_exists)',
    '_failure_requested_full_demand': '(error, num_nodes)',
    '_placement_error_code': '(error)',
    '_placement_outcome': '(error, capacity_reason=None)',
}


def _call_signature(symbol) -> str:
    """Returns the version-stable callable portion of a signature."""
    signature = inspect.signature(symbol)
    parameters = [
        parameter.replace(annotation=inspect.Parameter.empty)
        for parameter in signature.parameters.values()
    ]
    return str(
        signature.replace(parameters=parameters,
                          return_annotation=inspect.Signature.empty))


def test_capacity_policy_historical_contract():
    for name, signature in _CAPACITY_POLICY_SIGNATURES.items():
        symbol = getattr(backend, name)
        assert getattr(capacity_policy, name) is symbol
        assert _call_signature(symbol) == signature
        assert symbol.__module__ == 'sky.backends.cloud_vm_ray_backend'
        assert pickle.loads(pickle.dumps(symbol)) is symbol


class _FakeClientError(Exception):

    def __init__(self, code: str):
        self.response = {'Error': {'Code': code, 'Message': code}}
        super().__init__(f'An error occurred ({code})')


def _aggregate_error(*codes: str) -> provision_common.ProvisionerError:
    error = provision_common.ProvisionerError('Failed to launch instances.')
    error.errors = [{
        'code': code,
        'message': code,
    } for code in codes]
    if codes:
        error.__cause__ = _FakeClientError(codes[-1])
    return error


@pytest.mark.parametrize('code', [
    'VcpuLimitExceeded',
    'MaxSpotInstanceCountExceeded',
    'InstanceLimitExceeded',
])
def test_classify_quota(code):
    assert backend._classify_capacity_error(clouds.AWS(),
                                            _FakeClientError(code)) == 'quota'


def test_classify_capacity_through_explicit_cause():
    cause = _FakeClientError('InsufficientInstanceCapacity')
    outer = RuntimeError('Failed to launch instances.')
    outer.__cause__ = cause
    assert backend._classify_capacity_error(clouds.AWS(), outer) == 'capacity'


def test_classify_aggregate_requires_only_known_codes():
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'InsufficientInstanceCapacity')) == 'capacity'
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('VcpuLimitExceeded',
                         'MaxSpotInstanceCountExceeded')) == 'quota'
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InvalidParameterValue',
                         'InsufficientInstanceCapacity')) is None


def test_classify_known_mixed_capacity_and_quota_as_quota():
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'VcpuLimitExceeded')) == 'quota'


def test_terminal_resources_unavailable_requires_all_structured_evidence():
    capacity = exceptions.ResourcesUnavailableError(
        'capacity',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('InsufficientInstanceCapacity'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        capacity) == 'capacity'

    quota = exceptions.ResourcesUnavailableError(
        'quota',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('MaxSpotInstanceCountExceeded'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        quota) == 'quota'

    mixed = exceptions.ResourcesUnavailableError(
        'not availability',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('RequestLimitExceeded'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        mixed) is None


def _nested_terminal_error(
        *failures: Exception) -> exceptions.ResourcesUnavailableError:
    per_location = exceptions.ResourcesUnavailableError(
        'location unavailable', failover_history=list(failures))
    return exceptions.ResourcesUnavailableError('optimizer exhausted',
                                                failover_history=[per_location])


@pytest.mark.parametrize(
    ('cloud', 'capacity_code', 'quota_code'),
    [
        (clouds.AWS(), 'InsufficientInstanceCapacity',
         'MaxSpotInstanceCountExceeded'),
        (clouds.GCP(), 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
         'QUOTA_EXCEEDED'),
    ],
)
def test_terminal_resources_unavailable_recurses_nested_histories(
        cloud, capacity_code, quota_code):
    capacity = _aggregate_error(capacity_code)
    assert backend.classify_resources_unavailable_error(
        cloud, _nested_terminal_error(capacity)) == 'capacity'

    quota = _aggregate_error(quota_code)
    assert backend.classify_resources_unavailable_error(
        cloud, _nested_terminal_error(capacity, quota)) == 'quota'


def test_terminal_resources_unavailable_nested_history_is_conservative(
        monkeypatch):
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    unknown = _aggregate_error('RequestLimitExceeded')
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(capacity, unknown)) is None

    malformed = _nested_terminal_error(capacity)
    malformed.failover_history.append(
        'not an exception')  # type: ignore[arg-type]
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        malformed) is None

    malformed_container = _nested_terminal_error(capacity)
    malformed_container.failover_history = ()  # type: ignore[assignment]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), malformed_container) is None

    class _HiddenHistory(list):

        def __len__(self):
            return 0

    hidden_history = exceptions.ResourcesUnavailableError('hidden history')
    hidden_history.failover_history = _HiddenHistory(
        [unknown])  # type: ignore[assignment]
    hidden_history.__cause__ = capacity
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        hidden_history) is None

    cause_leaf = RuntimeError('leaf')
    malformed_cause = exceptions.ResourcesUnavailableError('malformed cause')
    malformed_cause.failover_history = ()  # type: ignore[assignment]
    cause_leaf.__cause__ = malformed_cause
    malformed_cause.__cause__ = capacity
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_leaf)) is None

    mixed_cause_leaf = RuntimeError('leaf')
    mixed_cause_leaf.__cause__ = exceptions.ResourcesUnavailableError(
        'history-bearing cause', failover_history=[capacity])
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(mixed_cause_leaf)) is None

    history_cycle = exceptions.ResourcesUnavailableError('cycle')
    history_cycle.failover_history.append(history_cycle)
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        history_cycle) is None

    cause_cycle = _FakeClientError('InsufficientInstanceCapacity')
    cause_wrapper = RuntimeError('cause wrapper')
    cause_cycle.__cause__ = cause_wrapper
    cause_wrapper.__cause__ = cause_cycle
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_cycle)) is None

    too_deep: Exception = capacity
    for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_DEPTH + 1):
        too_deep = exceptions.ResourcesUnavailableError(
            'nested', failover_history=[too_deep])
    assert isinstance(too_deep, exceptions.ResourcesUnavailableError)
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        too_deep) is None

    cause_too_deep: Exception = capacity
    for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_DEPTH + 1):
        cause_wrapper = RuntimeError('nested cause')
        cause_wrapper.__cause__ = cause_too_deep
        cause_too_deep = cause_wrapper
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_too_deep)) is None

    too_wide = exceptions.ResourcesUnavailableError(
        'wide',
        failover_history=[capacity] *
        (backend._MAX_TERMINAL_FAILOVER_HISTORY_NODES + 1))
    with monkeypatch.context() as patch:
        patch.setitem(
            backend._terminal_failover_leaves.__globals__,
            'reversed',
            mock.Mock(side_effect=AssertionError(
                'oversized history must not be scanned')),
        )
        assert backend.classify_resources_unavailable_error(
            clouds.AWS(), too_wide) is None

    cause_budget_overflow = exceptions.ResourcesUnavailableError(
        'wide',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity')
            for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_NODES // 2)
        ],
    )
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), cause_budget_overflow) is None


def test_terminal_resources_unavailable_wrapper_history_is_authoritative():
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    leaf_wrapper = exceptions.ResourcesUnavailableError('leaf')
    leaf_wrapper.__cause__ = capacity
    inner = exceptions.ResourcesUnavailableError(
        'inner', failover_history=[leaf_wrapper])
    outer = exceptions.ResourcesUnavailableError('outer',
                                                 failover_history=[inner])
    outer.__cause__ = _aggregate_error('RequestLimitExceeded')
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        outer) == 'capacity'


def test_terminal_resources_unavailable_allows_shared_leaf():
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    left = exceptions.ResourcesUnavailableError('left',
                                                failover_history=[capacity])
    right = exceptions.ResourcesUnavailableError('right',
                                                 failover_history=[capacity])
    outer = exceptions.ResourcesUnavailableError('outer',
                                                 failover_history=[left, right])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        outer) == 'capacity'


def test_terminal_resources_unavailable_does_not_parse_error_text():
    error = exceptions.ResourcesUnavailableError(
        'InsufficientInstanceCapacity',
        failover_history=[
            RuntimeError('InsufficientInstanceCapacity'),
            AssertionError('security group already exists'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        error) is None


def test_shared_hint_requires_full_demand_request_metadata():
    error = _aggregate_error('InsufficientInstanceCapacity')
    assert not backend._failure_requested_full_demand(error, 4)

    error.requested_count = 4
    outer = RuntimeError('wrapped')
    outer.__cause__ = error
    assert backend._failure_requested_full_demand(outer, 4)
    assert not backend._failure_requested_full_demand(outer, 1)


def test_aws_create_instances_preserves_every_failure(monkeypatch):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 2)

    class _FakeEC2:

        def create_instances(self, **kwargs):
            subnet_id = kwargs['NetworkInterfaces'][0]['SubnetId']
            code = ('InsufficientInstanceCapacity'
                    if subnet_id == 'subnet-a' else 'InvalidParameterValue')
            raise botocore.exceptions.ClientError(
                {'Error': {
                    'Code': code,
                    'Message': code,
                }}, 'RunInstances')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        aws_instance._create_instances(
            _FakeEC2(), 'cluster', {
                'SubnetIds': ['subnet-a', 'subnet-b'],
                'SecurityGroupIds': ['sg-1'],
                'InstanceType': 'g6.4xlarge',
            }, {}, 1, True, 0)

    assert [error['code'] for error in exc_info.value.errors
           ] == ['InsufficientInstanceCapacity', 'InvalidParameterValue']
    assert [error['subnet_id'] for error in exc_info.value.errors
           ] == ['subnet-a', 'subnet-b']
    assert exc_info.value.requested_count == 1


def test_classify_is_structured_and_aws_only():
    text_only = RuntimeError('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(clouds.AWS(), text_only) is None
    assert backend._classify_capacity_error(
        clouds.GCP(), _FakeClientError('VcpuLimitExceeded')) is None


def test_placement_classification_tolerates_malformed_provider_response():

    class _MalformedResponseError(Exception):

        def __init__(self):
            self.response = {'Error': 'not-an-object'}
            super().__init__('provider request failed')

    error = _MalformedResponseError()
    assert backend._placement_error_code(error) is None
    assert backend._placement_outcome(error) == 'failed'


def test_placement_outcome_reads_gcp_capacity_code_behind_summary_code():
    # GCP bulk insert reports the uninformative summary code first, so a
    # first-code-only check mislabels real exhaustion as a generic failure.
    error = _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                             'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')
    assert backend._placement_outcome(error) == 'capacity_failed'


def test_placement_outcome_prefers_quota_over_capacity_in_mixed_batch():
    error = _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                             'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                             'QUOTA_EXCEEDED')
    assert backend._placement_outcome(error) == 'quota_failed'


def test_placement_outcome_ignores_non_capacity_codes():
    # UNSUPPORTED_OPERATION blocks the zone on failover but means preemption
    # during creation, not an exhausted pool.
    assert backend._placement_outcome(
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                         'UNSUPPORTED_OPERATION')) == 'failed'


def test_placement_outcome_keeps_heterogeneous_aggregate_conservative():
    # AWS retries every subnet and appends one entry per distinct failure, so
    # a batch mixing capacity with an unrelated error is not exhaustion.
    assert backend._placement_outcome(
        _aggregate_error('InvalidParameterValue',
                         'InsufficientInstanceCapacity')) == 'failed'
    assert backend._placement_outcome(
        _aggregate_error('InsufficientInstanceCapacity',
                         'InsufficientInstanceCapacity')) == 'capacity_failed'


def test_placement_outcome_ignores_summary_only_batch():
    assert backend._placement_outcome(
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED')) == 'failed'


def test_placement_outcome_separates_tpu_quota_from_tpu_capacity():
    # sky/provision/gcp/tpu_node.py raises RESOURCE_EXHAUSTED for quota
    # exhaustion and CapacityExceeded for an exhausted zone.
    assert backend._placement_outcome(
        _aggregate_error('RESOURCE_EXHAUSTED')) == 'quota_failed'
    assert backend._placement_outcome(
        _aggregate_error('CapacityExceeded')) == 'capacity_failed'


def test_capacity_codes_do_not_cross_providers():
    """One cloud's capacity code must never classify another cloud's failure."""
    gcp_code = _aggregate_error('ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')
    aws_code = _aggregate_error('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(clouds.AWS(), gcp_code) is None
    assert backend._classify_capacity_error(clouds.GCP(), aws_code) is None
    # Each still classifies its own.
    assert backend._classify_capacity_error(clouds.GCP(),
                                            gcp_code) == 'capacity'
    assert backend._classify_capacity_error(clouds.AWS(),
                                            aws_code) == 'capacity'
    # GCP's uninformative summary code is not neutral for AWS.
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'VM_MIN_COUNT_NOT_REACHED')) is None


def test_gcp_classification_reads_past_the_summary_code():
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error(
            'VM_MIN_COUNT_NOT_REACHED',
            'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')) == 'capacity'
    # Quota dominates a mixed known batch because it is regional.
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                         'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                         'QUOTA_EXCEEDED')) == 'quota'
    # An unknown code keeps the conservative failover path.
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED', 'SOMETHING_ELSE')) is None
    # A summary code alone says nothing.
    assert backend._classify_capacity_error(
        clouds.GCP(), _aggregate_error('VM_MIN_COUNT_NOT_REACHED')) is None


@pytest.mark.parametrize('code', [
    'VcpuLimitExceeded',
    'quotaExceeded',
    'QUOTA_EXCEEDED',
    'type.googleapis.com/google.rpc.QuotaFailure',
])
def test_operator_quota_detection_uses_structured_codes(code):
    assert backend._is_quota_error(_aggregate_error(code))


def test_operator_quota_detection_ignores_capacity_and_text_only_errors():
    assert not backend._is_quota_error(
        _aggregate_error('InsufficientInstanceCapacity'))
    assert not backend._is_quota_error(RuntimeError('quotaExceeded'))


def test_classify_ignores_implicit_context():
    capacity = _FakeClientError('InsufficientInstanceCapacity')
    try:
        try:
            raise capacity
        except Exception:  # pylint: disable=broad-except
            raise ValueError(  # pylint: disable=raise-missing-from
                'unrelated failure')
    except ValueError as unrelated:
        assert unrelated.__context__ is capacity
        assert unrelated.__cause__ is None
        assert backend._classify_capacity_error(clouds.AWS(), unrelated) is None


def _to_provision(*, use_spot: bool = True, cloud=None):
    return resources_lib.Resources(cloud=cloud or clouds.AWS(),
                                   region='us-east-1',
                                   zone='us-east-1a',
                                   instance_type='g6.4xlarge',
                                   use_spot=use_spot)


def test_cache_key_is_exact_aws_spot_zone_only():
    region = clouds.Region('us-east-1')
    zone_a = clouds.Zone('us-east-1a')
    zone_b = clouds.Zone('us-east-1b')
    key = backend._capacity_cache_key(_to_provision(), region, [zone_a], 4,
                                      'acct')
    assert key == capacity_cache.ResourceKey(cloud='aws',
                                             account='acct',
                                             region='us-east-1',
                                             zone='us-east-1a',
                                             instance_type='g6.4xlarge',
                                             accelerators='L4:1',
                                             num_nodes=4)

    assert backend._capacity_cache_key(_to_provision(use_spot=False), region,
                                       [zone_a], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region,
                                       [zone_a, zone_b], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region, [zone_a], 4,
                                       None) is None
    # A cloud with no structured capacity codes never participates.
    assert backend._capacity_cache_key(_to_provision(cloud=clouds.Azure()),
                                       region, [zone_a], 4, 'acct') is None


def test_quota_cooldown_key_is_exact_spot_regional_demand():
    region = clouds.Region('us-east-1')
    key = backend._quota_cooldown_key(_to_provision(), region, 4, 'acct')
    assert key == capacity_cache.QuotaCooldownKey(cloud='aws',
                                                  account='acct',
                                                  region='us-east-1',
                                                  instance_type='g6.4xlarge',
                                                  accelerators='L4:1',
                                                  num_nodes=4)
    assert backend._quota_cooldown_key(_to_provision(), region, 4, None) is None
    assert backend._quota_cooldown_key(_to_provision(use_spot=False), region, 4,
                                       'acct') is None
    assert backend._quota_cooldown_key(_to_provision(cloud=clouds.Azure()),
                                       region, 4, 'acct') is None


def test_consult_returns_only_active_zone(monkeypatch):
    to_provision = _to_provision()
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a')]
    expected = backend._capacity_cache_key(to_provision, region, zones, 1,
                                           'acct')
    assert expected is not None
    monkeypatch.setattr(capacity_cache, 'active_exhausted_keys',
                        lambda keys: {expected})
    assert backend._capacity_cache_exhausted_zone_names(
        to_provision, region, zones, 1, 'acct') == {'us-east-1a'}


def test_consult_failure_falls_back_to_real_probe(monkeypatch):
    monkeypatch.setattr(
        capacity_cache, 'active_exhausted_keys', lambda keys:
        (_ for _ in ()).throw(RuntimeError('db unavailable')))
    assert backend._capacity_cache_exhausted_zone_names(
        _to_provision(), clouds.Region('us-east-1'),
        [clouds.Zone('us-east-1a')], 1, 'acct') == set()


def test_quota_consult_hit_and_failure(monkeypatch):
    key = capacity_cache.QuotaCooldownKey(cloud='aws',
                                          account='acct',
                                          region='us-east-1',
                                          instance_type='g6.4xlarge',
                                          accelerators='L4:1',
                                          num_nodes=1)
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        lambda _: True)
    assert backend._quota_cooldown_is_active(key)

    monkeypatch.setattr(
        capacity_cache, 'is_quota_cooldown_active', lambda _:
        (_ for _ in ()).throw(RuntimeError('db unavailable')))
    assert not backend._quota_cooldown_is_active(key)


def _call_retry_zones(provisioner, to_provision):
    return provisioner._retry_zones(
        to_provision=to_provision,
        num_nodes=1,
        requested_resources={to_provision},
        dryrun=False,
        stream_logs=False,
        cluster_name='test-cluster',
        cloud_user_identity=['arn:aws:iam::123456789012:role/test', 'acct'],
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        skip_if_config_hash_matches=None,
        volume_mounts=None,
        task=None,
    )


def _early_retry_provisioner(tmp_path, monkeypatch):
    provisioner = object.__new__(backend.RetryingVmProvisioner)
    provisioner.log_dir = str(tmp_path)
    provisioner._blocked_resources = set()
    monkeypatch.setattr(backend.os, 'system', lambda _: 0)
    monkeypatch.setattr(backend.rich_utils, 'force_update_status',
                        lambda _: None)
    return provisioner


def test_retry_zones_spot_quota_cooldown_precedes_quota_check_and_zone_yield(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    to_provision = _to_provision()
    expected_key = capacity_cache.QuotaCooldownKey(cloud='aws',
                                                   account='acct',
                                                   region='us-east-1',
                                                   instance_type='g6.4xlarge',
                                                   accelerators='L4:1',
                                                   num_nodes=1)
    cooldown_active = mock.Mock(return_value=True)
    check_quota = mock.Mock(return_value=False)
    yield_zones = mock.Mock(
        side_effect=AssertionError('zone iteration must be skipped'))
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        cooldown_active)
    monkeypatch.setattr(backend, '_record_capacity_metric', lambda *_: None)
    monkeypatch.setattr(clouds.AWS, 'check_quota_available', check_quota)
    monkeypatch.setattr(provisioner, '_yield_zones', yield_zones)

    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='quota-failure cooldown'):
        _call_retry_zones(provisioner, to_provision)

    cooldown_active.assert_called_once_with(expected_key)
    check_quota.assert_not_called()
    yield_zones.assert_not_called()
    assert len(provisioner._blocked_resources) == 1
    blocked = next(iter(provisioner._blocked_resources))
    assert blocked.region == 'us-east-1'
    assert blocked.zone is None


def test_retry_zones_quota_cooldown_does_not_apply_to_on_demand(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    to_provision = _to_provision(use_spot=False)
    cooldown_active = mock.Mock(return_value=True)
    check_quota = mock.Mock(return_value=False)
    yield_zones = mock.Mock(
        side_effect=AssertionError('zero quota must skip zone iteration'))
    notify = mock.Mock(return_value=False)
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        cooldown_active)
    monkeypatch.setattr(clouds.AWS, 'check_quota_available', check_quota)
    monkeypatch.setattr(provisioner, '_yield_zones', yield_zones)
    monkeypatch.setattr(backend, '_record_insufficient_quota_notification',
                        notify)

    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='Found no quota'):
        _call_retry_zones(provisioner, to_provision)

    cooldown_active.assert_not_called()
    check_quota.assert_called_once()
    yield_zones.assert_not_called()
    notify.assert_called_once_with(to_provision)
    assert not provisioner._blocked_resources


def test_retry_zones_preserves_structured_provider_failure(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    provisioner._local_wheel_path = None
    provisioner._wheel_hash = None
    provisioner._active_cluster_hash = None
    provisioner._is_managed = False
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = None
    to_provision = _to_provision()
    provider_error = _aggregate_error('InsufficientInstanceCapacity')
    provider_error.requested_count = 1

    monkeypatch.setattr(clouds.AWS, 'check_quota_available', lambda *_: True)
    monkeypatch.setattr(provisioner, '_yield_zones',
                        lambda *_: iter([[clouds.Zone('us-east-1a')]]))
    monkeypatch.setattr(backend, '_capacity_cache_exhausted_zone_names',
                        lambda *_: set())
    monkeypatch.setattr(backend, '_get_image_demand_attribution',
                        lambda *_: mock.MagicMock())
    monkeypatch.setattr(backend, '_resolve_container_image_for_placement',
                        lambda resources, **_: resources)
    monkeypatch.setattr(backend.provision_lib, 'get_registered_provisioner',
                        lambda *_: None)
    monkeypatch.setattr(backend, '_get_cluster_config_template',
                        lambda *_: '/tmp/template')
    monkeypatch.setattr(
        backend.backend_utils, 'write_cluster_config', lambda *_, **__: {
            'ray': '/tmp/cluster.yaml',
            'cluster_name_on_cloud': 'test-cluster',
        })
    monkeypatch.setattr(backend, '_get_workload_attribution', lambda *_:
                        (None, None))
    monkeypatch.setattr(backend.global_user_state, 'add_or_update_cluster',
                        lambda *_, **__: 'cluster-hash')
    monkeypatch.setattr(backend.global_user_state, 'add_cluster_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.global_user_state,
                        'set_owner_identity_for_cluster', lambda *_, **__: None)
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_final_cluster_status', lambda *_: None)
    monkeypatch.setattr(backend.controller_utils.Controllers, 'from_name',
                        lambda *_: None)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        mock.Mock(side_effect=provider_error))
    monkeypatch.setattr(backend.CloudVmRayBackend, 'post_teardown_cleanup',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.FailoverCloudErrorHandlerV2,
                        'update_blocklist_on_error', lambda *_, **__: None)
    monkeypatch.setattr(backend, '_record_service_placement_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.capacity_cache, 'mark_exhausted',
                        lambda *_, **__: None)

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert exc_info.value.failover_history == [provider_error]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), exc_info.value) == 'capacity'


def test_provision_with_retries_preserves_nested_terminal_failure(
        tmp_path, monkeypatch):
    to_provision = _to_provision()
    provider_error = _aggregate_error('InsufficientInstanceCapacity')
    per_location_error = exceptions.ResourcesUnavailableError(
        'location unavailable', failover_history=[provider_error])
    provisioner = backend.RetryingVmProvisioner(
        log_dir=str(tmp_path),
        dag=mock.Mock(),
        optimize_target=mock.Mock(),
        requested_features=set(),
        local_wheel_path=tmp_path / 'wheel',
        wheel_hash='',
        extra_launch_context={},
    )
    task = mock.Mock()
    task.is_controller_task.return_value = False
    task.num_nodes = 1
    task.resources = {to_provision}
    task.best_resources = to_provision
    task.volume_mounts = None
    config = backend.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='test-cluster',
        resources=to_provision,
        num_nodes=1,
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        prev_config_hash=None,
    )

    monkeypatch.setattr(clouds.AWS, 'get_active_user_identity',
                        lambda *_: ['acct'])
    monkeypatch.setattr(provisioner, '_retry_zones',
                        mock.Mock(side_effect=per_location_error))
    monkeypatch.setattr(
        backend.optimizer.Optimizer,
        'optimize',
        mock.Mock(side_effect=exceptions.ResourcesUnavailableError(
            'optimizer exhausted')),
    )
    monkeypatch.setattr(backend, '_format_provision_failure_blocks',
                        lambda *_: '')
    monkeypatch.setattr(backend.rich_utils, 'force_update_status',
                        lambda *_: None)

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        provisioner.provision_with_retries(
            task,
            config,
            dryrun=False,
            stream_logs=False,
            skip_unnecessary_provisioning=False,
        )

    assert exc_info.value.failover_history == [per_location_error]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), exc_info.value) == 'capacity'


def test_quota_notification_has_generic_actionable_context(monkeypatch):
    record = mock.Mock(return_value=True)
    monkeypatch.setattr(backend.operator_notifications, 'record_notification',
                        record)

    assert backend._record_insufficient_quota_notification(_to_provision())
    category, message = record.call_args.args
    assert category == (backend.operator_notifications.
                        OperatorNotificationCategory.INSUFFICIENT_QUOTA)
    assert 'AWS' in message
    assert 'us-east-1' in message
    assert 'g6.4xlarge' in message
    assert 'service' not in message.lower()
    assert record.call_args.kwargs['dedupe_window_seconds'] == 3600


def test_quota_notification_is_fail_open(monkeypatch):
    monkeypatch.setattr(
        backend.operator_notifications,
        'record_notification',
        mock.Mock(side_effect=RuntimeError('notification unavailable')),
    )

    assert not backend._record_insufficient_quota_notification(_to_provision())


def test_capacity_metric_failure_is_fail_open(monkeypatch):
    counter = mock.Mock()
    counter.labels.return_value.inc.side_effect = RuntimeError(
        'metrics unavailable')
    metrics = mock.Mock(METRICS_ENABLED=True,
                        SKY_PROVISION_CAPACITY_EVENTS_TOTAL=counter)
    monkeypatch.setattr(backend, 'metrics_utils', metrics)

    backend._record_capacity_metric('quota', 'hit')

    counter.labels.assert_called_once_with(reason='quota', action='hit')
    counter.labels.return_value.inc.assert_called_once_with()


def _provision_record(*, created, resumed=(), zone='us-east-1a'):
    return provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone=zone,
        cluster_name='test-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=list(resumed),
        created_instance_ids=list(created),
    )


@pytest.mark.parametrize(
    ('record', 'num_nodes', 'cluster_exists', 'expected'), [
        (_provision_record(created=['i-1', 'i-2']), 2, False, True),
        (_provision_record(created=[], resumed=[]), 1, False, False),
        (_provision_record(created=['i-1'], resumed=['i-2']), 2, False, False),
        (_provision_record(created=['i-1', 'i-2']), 2, True, False),
    ])
def test_fully_created_fresh_demand_clear_eligibility(record, num_nodes,
                                                      cluster_exists, expected):
    assert backend._fully_created_fresh_demand(record, num_nodes,
                                               cluster_exists) is expected


def _enable_gcp_cache(monkeypatch, enabled: bool = True):
    real_get_nested = backend.skypilot_config.get_nested

    def fake_get_nested(keys, *args, **kwargs):
        if tuple(keys) == ('provision', 'gcp_capacity_cache'):
            return enabled
        return real_get_nested(keys, *args, **kwargs)

    monkeypatch.setattr(backend.skypilot_config, 'get_nested', fake_get_nested)


def _gcp_provision(*, accelerators=None, instance_type='g2-standard-4'):
    return resources_lib.Resources(cloud=clouds.GCP(),
                                   region='asia-northeast3',
                                   zone='asia-northeast3-b',
                                   instance_type=instance_type,
                                   accelerators=accelerators,
                                   use_spot=True)


def test_gcp_cache_key_is_on_by_default_and_can_be_disabled(monkeypatch):
    region = clouds.Region('asia-northeast3')
    zone = clouds.Zone('asia-northeast3-b')

    # Disabling is the escape hatch: no key means nothing is written or read.
    _enable_gcp_cache(monkeypatch, enabled=False)
    assert backend._capacity_cache_key(_gcp_provision(), region, [zone], 1,
                                       'proj') is None
    assert backend._quota_cooldown_key(_gcp_provision(), region, 1,
                                       'proj') is None

    # With no configuration at all, GCP participates.
    monkeypatch.undo()
    key = backend._capacity_cache_key(_gcp_provision(accelerators={'L4': 1}),
                                      region, [zone], 1, 'proj')
    assert key == capacity_cache.ResourceKey(cloud='gcp',
                                             account='proj',
                                             region='asia-northeast3',
                                             zone='asia-northeast3-b',
                                             instance_type='g2-standard-4',
                                             accelerators='L4:1',
                                             num_nodes=1)


def test_gcp_key_separates_accelerators_on_the_same_machine_type(monkeypatch):
    """N1 attaches accelerators separately, so the machine type is not enough."""
    _enable_gcp_cache(monkeypatch)
    region = clouds.Region('asia-northeast3')
    zone = clouds.Zone('asia-northeast3-b')
    t4 = backend._capacity_cache_key(
        _gcp_provision(instance_type='n1-standard-8', accelerators={'T4': 1}),
        region, [zone], 1, 'proj')
    v100 = backend._capacity_cache_key(
        _gcp_provision(instance_type='n1-standard-8', accelerators={'V100': 1}),
        region, [zone], 1, 'proj')
    assert t4 is not None and v100 is not None
    assert t4 != v100


def test_cache_keys_separate_clouds(monkeypatch):
    _enable_gcp_cache(monkeypatch)
    region = clouds.Region('us-east-1')
    zone = clouds.Zone('us-east-1a')
    aws_key = backend._capacity_cache_key(_to_provision(), region, [zone], 1,
                                          'same')
    gcp_key = backend._capacity_cache_key(
        _gcp_provision(instance_type='g6.4xlarge'), region, [zone], 1, 'same')
    assert aws_key is not None and gcp_key is not None
    assert aws_key != gcp_key


def test_capacity_cache_account_scopes_by_project_without_the_email():
    aws = backend._capacity_cache_account(clouds.AWS(), ['user-id', '1234567'])
    assert aws == '1234567'

    gcp = backend._capacity_cache_account(
        clouds.GCP(), ['someone@example.com [project_id=my-project]'])
    assert gcp == 'my-project'

    # An identity that does not carry a project must not be cached under the
    # raw string, which would also leak the account email into the key.
    assert backend._capacity_cache_account(clouds.GCP(),
                                           ['someone@example.com']) is None
    assert backend._capacity_cache_account(clouds.GCP(), None) is None
    assert backend._capacity_cache_account(clouds.Azure(), ['x']) is None


def test_gcp_touches_no_cache_entry_point_when_flag_is_off(monkeypatch):
    """With the flag off, GCP must not read or write the cache at all."""
    _enable_gcp_cache(monkeypatch, enabled=False)

    def _boom(*args, **kwargs):
        raise AssertionError('cache must not be touched when the flag is off')

    for name in ('mark_exhausted', 'active_exhausted_keys', 'clear',
                 'mark_quota_failure', 'is_quota_cooldown_active',
                 'clear_quota_cooldown'):
        monkeypatch.setattr(capacity_cache, name, _boom)

    region = clouds.Region('asia-northeast3')
    zones = [clouds.Zone('asia-northeast3-b')]
    to_provision = _gcp_provision(accelerators={'L4': 1})

    assert backend._capacity_cache_key(to_provision, region, zones, 1,
                                       'proj') is None
    quota_key = backend._quota_cooldown_key(to_provision, region, 1, 'proj')
    assert quota_key is None
    # The consult helpers must short-circuit on the absent key rather than
    # reaching the cache.
    assert backend._capacity_cache_exhausted_zone_names(to_provision, region,
                                                        zones, 1,
                                                        'proj') == set()
    assert not backend._quota_cooldown_is_active(quota_key)


def test_gcp_provisioner_sets_requested_count_so_failures_can_cache(
        monkeypatch):
    """A GCP bulk-insert failure must prove it covered the whole demand.

    Without `requested_count`, `_failure_requested_full_demand` is False and
    the failure is silently never cached, making the GCP cache a no-op.
    """
    codes = [
        'VM_MIN_COUNT_NOT_REACHED', 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS'
    ]

    class _FakeCompute:
        """Minimal stand-in for the GCP compute resource."""

        STOPPING_STATES: list = []
        NON_TERMINATED_STATES: list = []
        PENDING_STATES: list = []

        @classmethod
        def filter(cls, *args, **kwargs):
            del args, kwargs
            return {}

        @classmethod
        def create_instances(cls, *args, **kwargs):
            del args, kwargs
            return ([{'code': code, 'message': code} for code in codes], [])

    monkeypatch.setattr(gcp_instance_utils, 'GCPComputeInstance', _FakeCompute)
    monkeypatch.setattr(gcp_instance.instance_utils, 'GCPComputeInstance',
                        _FakeCompute)
    monkeypatch.setattr(gcp_instance.instance_utils, 'get_node_type',
                        lambda _: gcp_instance_utils.GCPNodeType.COMPUTE)

    config = provision_common.ProvisionConfig(
        provider_config={
            'project_id': 'proj',
            'availability_zone': 'asia-northeast3-b',
        },
        authentication_config={},
        docker_config={},
        node_config={},
        count=2,
        tags={},
        resume_stopped_nodes=True,
        ports_to_open_on_launch=None,
    )

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        gcp_instance._run_instances('asia-northeast3', 'cluster', config)

    error = exc_info.value
    assert [entry['code'] for entry in error.errors] == codes
    # The wiring under test: without this the failure can never be cached.
    assert error.requested_count == 2
    assert backend._failure_requested_full_demand(error, 2)
    assert backend._classify_capacity_error(clouds.GCP(), error) == 'capacity'
