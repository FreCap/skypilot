"""Tests for AWS capacity classification and cache scoping."""
# pylint: disable=protected-access
import unittest.mock as mock

import botocore.exceptions
import pytest

from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import capacity_cache
from sky.provision import common as provision_common
from sky.provision.aws import instance as aws_instance


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


def test_gcp_capacity_codes_do_not_reach_the_aws_capacity_cache():
    # The wider placement set must not leak into the AWS-only cache gate.
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error('ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')) is None
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')) is None


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
    assert key == capacity_cache.ResourceKey(account='acct',
                                             region='us-east-1',
                                             zone='us-east-1a',
                                             instance_type='g6.4xlarge',
                                             num_nodes=4)

    assert backend._capacity_cache_key(_to_provision(use_spot=False), region,
                                       [zone_a], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region,
                                       [zone_a, zone_b], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region, [zone_a], 4,
                                       None) is None
    assert backend._capacity_cache_key(_to_provision(cloud=clouds.GCP()),
                                       region, [zone_a], 4, 'acct') is None


def test_quota_cooldown_key_is_exact_spot_regional_demand():
    region = clouds.Region('us-east-1')
    key = backend._quota_cooldown_key(_to_provision(), region, 4, 'acct')
    assert key == capacity_cache.QuotaCooldownKey(account='acct',
                                                  region='us-east-1',
                                                  instance_type='g6.4xlarge',
                                                  num_nodes=4)
    assert backend._quota_cooldown_key(_to_provision(), region, 4, None) is None
    assert backend._quota_cooldown_key(_to_provision(use_spot=False), region, 4,
                                       'acct') is None
    assert backend._quota_cooldown_key(_to_provision(cloud=clouds.GCP()),
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
    key = capacity_cache.QuotaCooldownKey(account='acct',
                                          region='us-east-1',
                                          instance_type='g6.4xlarge',
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
    expected_key = capacity_cache.QuotaCooldownKey(account='acct',
                                                   region='us-east-1',
                                                   instance_type='g6.4xlarge',
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
