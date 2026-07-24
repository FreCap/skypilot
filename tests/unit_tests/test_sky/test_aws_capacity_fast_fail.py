"""Tests for terminal AWS RunInstances fast-fail behavior."""
# pylint: disable=protected-access
import pickle
from types import SimpleNamespace
from typing import Dict, List

import botocore.exceptions
import pytest

from sky.provision import common as provision_common
from sky.provision import constants as provision_constants
from sky.provision.aws import instance as aws_instance
from sky.provision.aws import instance_requests


def _client_error(code: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {'Error': {
            'Code': code,
            'Message': code,
        }}, 'RunInstances')


def _node_config(subnets: List[str], *, spot: bool) -> Dict:
    config = {
        'SubnetIds': subnets,
        'SecurityGroupIds': ['sg-1'],
        'InstanceType': 'g6.4xlarge',
    }
    if spot:
        config['InstanceMarketOptions'] = {'MarketType': 'spot'}
    return config


def _create(fake_ec2, node_config, *, single_zone: bool):
    return aws_instance._create_instances(fake_ec2, 'cluster', node_config, {},
                                          1, True, 0, single_zone)


def test_create_instances_projects_request_without_mutating_node_config():
    launched = [object()]
    observed = []

    class _FakeEC2:

        def create_instances(self, **kwargs):
            observed.append(kwargs)
            return launched

    node_config = {
        'SubnetIds': ['subnet-a'],
        'SecurityGroupIds': ['sg-1'],
        'InstanceType': 'g6.4xlarge',
        'TagSpecifications': [{
            'ResourceType': 'instance',
            'Tags': [{
                'Key': 'Name',
                'Value': 'custom-name',
            }, {
                'Key': 'purpose',
                'Value': 'test',
            }],
        }, {
            'ResourceType': 'volume',
            'Tags': [{
                'Key': 'storage',
                'Value': 'scratch',
            }],
        }],
    }

    result = aws_instance._create_instances(  # pylint: disable=protected-access
        _FakeEC2(), 'cluster', node_config, {'owner': 'sky'}, 1, True, 0)

    assert result is launched
    assert node_config['SubnetIds'] == ['subnet-a']
    assert node_config['SecurityGroupIds'] == ['sg-1']
    assert observed == [{
        'InstanceType': 'g6.4xlarge',
        'TagSpecifications': [{
            'ResourceType': 'instance',
            'Tags': [{
                'Key': 'Name',
                'Value': 'custom-name',
            }, {
                'Key': provision_constants.TAG_RAY_CLUSTER_NAME,
                'Value': 'cluster',
            }, {
                'Key': provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
                'Value': 'cluster',
            }, {
                'Key': 'owner',
                'Value': 'sky',
            }, {
                'Key': provision_constants.TAG_SKYPILOT_MANAGED,
                'Value': provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
            }, {
                'Key': 'purpose',
                'Value': 'test',
            }],
        }, {
            'ResourceType': 'volume',
            'Tags': [{
                'Key': provision_constants.TAG_SKYPILOT_MANAGED,
                'Value': provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
            }, {
                'Key': 'storage',
                'Value': 'scratch',
            }],
        }, {
            'ResourceType': 'network-interface',
            'Tags': [{
                'Key': provision_constants.TAG_SKYPILOT_MANAGED,
                'Value': provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
            }],
        }],
        'MinCount': 1,
        'MaxCount': 1,
        'NetworkInterfaces': [{
            'SubnetId': 'subnet-a',
            'DeviceIndex': 0,
            'AssociatePublicIpAddress': True,
            'Groups': ['sg-1'],
            'InterfaceType': 'interface',
        }],
    }]


def test_create_instances_tags_all_resources_and_reserves_managed_marker():
    observed = []

    class _FakeEC2:

        def create_instances(self, **kwargs):
            observed.append(kwargs)
            return [object()]

    node_config = {
        'SubnetIds': ['subnet-a'],
        'SecurityGroupIds': ['sg-1'],
        'InstanceType': 'g6.4xlarge',
        'InstanceMarketOptions': {
            'MarketType': 'SPOT',
        },
        'TagSpecifications': [{
            'ResourceType': resource_type,
            'Tags': [{
                'Key': provision_constants.TAG_SKYPILOT_MANAGED,
                'Value': 'false',
            }, {
                'Key': 'owner',
                'Value': resource_type,
            }],
        } for resource_type in ('instance', 'volume', 'network-interface',
                                'spot-instances-request')],
    }
    original_node_config = pickle.loads(pickle.dumps(node_config))

    aws_instance._create_instances(  # pylint: disable=protected-access
        _FakeEC2(), 'cluster', node_config, {
            provision_constants.TAG_SKYPILOT_MANAGED: 'false',
        }, 1, True, 0)

    assert node_config == original_node_config
    tag_specs = observed[0]['TagSpecifications']
    assert [spec['ResourceType'] for spec in tag_specs] == [
        'instance',
        'volume',
        'network-interface',
        'spot-instances-request',
    ]
    for tag_spec in tag_specs:
        tags = {tag['Key']: tag['Value'] for tag in tag_spec['Tags']}
        assert tags[provision_constants.TAG_SKYPILOT_MANAGED] == (
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE)
        assert tags['owner'] == tag_spec['ResourceType']


def test_instance_request_helpers_keep_instance_facade():
    assert (aws_instance._create_instances
            is not instance_requests.create_instances)
    assert (aws_instance._is_single_zone_request
            is instance_requests._is_single_zone_request)


@pytest.mark.parametrize('name',
                         ['_create_instances', '_is_single_zone_request'])
def test_instance_request_helpers_load_from_legacy_pickle_paths(name):
    payload = f'csky.provision.aws.instance\n{name}\n.'.encode()
    assert pickle.loads(payload) is getattr(aws_instance, name)


@pytest.mark.parametrize(('availability_zone', 'expected'), [
    ('us-east-1a', True),
    (' us-east-1a ', True),
    ('us-east-1a,us-east-1b', False),
    ('', False),
    (None, False),
])
def test_single_zone_signal_comes_from_provider_config(availability_zone,
                                                       expected):
    assert aws_instance._is_single_zone_request(
        {'availability_zone': availability_zone}) is expected


def test_run_instances_passes_single_zone_signal(monkeypatch):

    class _Instances:

        def filter(self, **kwargs):
            del kwargs
            return []

    class _Client:

        def __init__(self):
            self.meta = SimpleNamespace(region_name='us-east-1')

        def create_tags(self, **kwargs):
            del kwargs

    ec2 = SimpleNamespace(meta=SimpleNamespace(client=_Client()),
                          instances=_Instances())
    created = SimpleNamespace(id='i-created',
                              tags=[],
                              placement={'AvailabilityZone': 'us-east-1a'})
    observed = []

    def _fake_create_instances(ec2_fail_fast,
                               cluster_name,
                               node_config,
                               tags,
                               count,
                               associate_public_ip_address,
                               max_efa_interfaces,
                               is_single_zone_request=False):
        del (ec2_fail_fast, cluster_name, node_config, tags, count,
             associate_public_ip_address, max_efa_interfaces)
        observed.append(is_single_zone_request)
        return [created]

    monkeypatch.setattr(aws_instance, '_default_ec2_resource', lambda _: ec2)
    monkeypatch.setattr(aws_instance.aws, 'resource', lambda *_, **__: ec2)
    monkeypatch.setattr(aws_instance, '_create_instances',
                        _fake_create_instances)
    config = provision_common.ProvisionConfig(provider_config={
        'availability_zone': 'us-east-1a',
        'use_internal_ips': False,
    },
                                              authentication_config={},
                                              docker_config={},
                                              node_config={
                                                  'InstanceType': 'g6.4xlarge',
                                                  'InstanceMarketOptions': {
                                                      'MarketType': 'spot'
                                                  },
                                              },
                                              count=1,
                                              tags={},
                                              resume_stopped_nodes=False,
                                              ports_to_open_on_launch=None)

    record = aws_instance.run_instances('us-east-1', 'unused', 'cluster',
                                        config)

    assert observed == [True]
    assert record.created_instance_ids == ['i-created']


def test_run_instances_tags_resumed_instance_with_reserved_marker(monkeypatch):

    class _Instances:
        """Minimal stopped-instance collection for the resume path."""

        def __init__(self, instances):
            self._instances = instances

        def filter(self, **kwargs):
            del kwargs
            return self._instances

    class _Client:
        """Records EC2 resume and tag requests."""

        def __init__(self):
            self.meta = SimpleNamespace(region_name='us-east-1')
            self.create_tag_calls = []
            self.started_instance_ids = []

        def start_instances(self, InstanceIds):
            self.started_instance_ids.extend(InstanceIds)
            return {}

        def create_tags(self, **kwargs):
            self.create_tag_calls.append(kwargs)

    stopped = SimpleNamespace(
        id='i-stopped',
        state={'Name': 'stopped'},
        tags=[{
            'Key': provision_constants.TAG_RAY_CLUSTER_NAME,
            'Value': 'cluster',
        }],
        placement={'AvailabilityZone': 'us-east-1a'},
    )
    client = _Client()
    ec2 = SimpleNamespace(meta=SimpleNamespace(client=client),
                          instances=_Instances([stopped]))
    monkeypatch.setattr(aws_instance, '_default_ec2_resource', lambda _: ec2)
    monkeypatch.setattr(aws_instance.aws, 'resource', lambda *_, **__: ec2)
    config = provision_common.ProvisionConfig(
        provider_config={
            'use_internal_ips': False,
        },
        authentication_config={},
        docker_config={},
        node_config={
            'InstanceType': 'g6.4xlarge',
        },
        count=1,
        tags={
            'owner': 'test',
            provision_constants.TAG_SKYPILOT_MANAGED: 'false',
        },
        resume_stopped_nodes=True,
        ports_to_open_on_launch=None)

    record = aws_instance.run_instances('us-east-1', 'unused', 'cluster',
                                        config)

    assert record.resumed_instance_ids == ['i-stopped']
    assert client.started_instance_ids == ['i-stopped']
    resume_tags = {
        tag['Key']: tag['Value'] for tag in client.create_tag_calls[0]['Tags']
    }
    assert resume_tags == {
        'owner': 'test',
        provision_constants.TAG_SKYPILOT_MANAGED:
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
    }


def test_single_zone_spot_iic_stops_after_first_attempt(monkeypatch):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 5)
    calls = []

    class _FakeEC2:

        def create_instances(self, **kwargs):
            calls.append(kwargs['NetworkInterfaces'][0]['SubnetId'])
            raise _client_error('InsufficientInstanceCapacity')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        _create(_FakeEC2(),
                _node_config(['subnet-a'], spot=True),
                single_zone=True)

    assert calls == ['subnet-a']
    assert exc_info.value.errors == [{
        'code': 'InsufficientInstanceCapacity',
        'message': 'InsufficientInstanceCapacity',
        'subnet_id': 'subnet-a',
    }]
    assert exc_info.value.requested_count == 1
    assert isinstance(exc_info.value.__cause__, botocore.exceptions.ClientError)


@pytest.mark.parametrize('code', [
    'VcpuLimitExceeded',
    'MaxSpotInstanceCountExceeded',
    'InstanceLimitExceeded',
])
def test_regional_quota_stops_after_first_attempt(monkeypatch, code):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 5)
    calls = []

    class _FakeEC2:

        def create_instances(self, **kwargs):
            calls.append(kwargs['NetworkInterfaces'][0]['SubnetId'])
            raise _client_error(code)

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        _create(_FakeEC2(),
                _node_config(['subnet-a', 'subnet-b'], spot=False),
                single_zone=False)

    assert calls == ['subnet-a']
    assert [error['code'] for error in exc_info.value.errors] == [code]
    assert exc_info.value.requested_count == 1


@pytest.mark.parametrize(('spot', 'subnets', 'single_zone', 'expected_calls'), [
    (False, ['subnet-a'], True, ['subnet-a', 'subnet-a']),
    (True, ['subnet-a', 'subnet-b'], False, ['subnet-a', 'subnet-b']),
])
def test_iic_preserves_failover_without_single_zone_spot(
        monkeypatch, spot, subnets, single_zone, expected_calls):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 2)
    calls = []
    launched = [object()]

    class _FakeEC2:

        def create_instances(self, **kwargs):
            subnet_id = kwargs['NetworkInterfaces'][0]['SubnetId']
            calls.append(subnet_id)
            if len(calls) == 1:
                raise _client_error('InsufficientInstanceCapacity')
            return launched

    result = _create(_FakeEC2(),
                     _node_config(subnets, spot=spot),
                     single_zone=single_zone)

    assert result is launched
    assert calls == expected_calls


def test_single_zone_spot_retries_unrelated_errors(monkeypatch):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 2)
    calls = []
    launched = [object()]

    class _FakeEC2:

        def create_instances(self, **kwargs):
            calls.append(kwargs['NetworkInterfaces'][0]['SubnetId'])
            if len(calls) == 1:
                raise _client_error('InvalidParameterValue')
            return launched

    result = _create(_FakeEC2(),
                     _node_config(['subnet-a'], spot=True),
                     single_zone=True)

    assert result is launched
    assert calls == ['subnet-a', 'subnet-a']


def test_multi_interface_efa_failure_preserves_subnet_retries(monkeypatch):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 2)
    calls = []
    launched = [object()]

    class _FakeEC2:

        def create_instances(self, **kwargs):
            calls.append(kwargs['NetworkInterfaces'][0]['SubnetId'])
            if len(calls) == 1:
                raise _client_error('InvalidParameterValue')
            return launched

    result = aws_instance._create_instances(  # pylint: disable=protected-access
        _FakeEC2(),
        'cluster',
        _node_config(['subnet-a', 'subnet-b'], spot=False),
        {},
        1,
        False,
        32,
    )

    assert result is launched
    assert calls == ['subnet-a', 'subnet-b']
