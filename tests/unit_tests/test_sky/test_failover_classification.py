"""Tests for AWS capacity classification and cache scoping."""
# pylint: disable=protected-access
import botocore.exceptions
import pytest

from sky import clouds
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


def test_classify_aggregate_requires_all_codes_to_agree():
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


def test_classify_is_structured_and_aws_only():
    text_only = RuntimeError('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(clouds.AWS(), text_only) is None
    assert backend._classify_capacity_error(
        clouds.GCP(), _FakeClientError('VcpuLimitExceeded')) is None


def test_classify_ignores_implicit_context():
    capacity = _FakeClientError('InsufficientInstanceCapacity')
    try:
        try:
            raise capacity
        except Exception:  # pylint: disable=broad-except
            raise ValueError('unrelated failure')
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
