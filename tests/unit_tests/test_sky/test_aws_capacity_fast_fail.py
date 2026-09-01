"""Tests for terminal AWS RunInstances fast-fail behavior."""
# pylint: disable=protected-access
import pickle
from types import SimpleNamespace
from typing import Dict, List

import botocore.exceptions
import pytest

from sky import exceptions
from sky.provision import common as provision_common
from sky.provision import constants as provision_constants
from sky.provision.aws import instance as aws_instance
from sky.provision.aws import instance_requests

_AWS_ACCOUNT_ID = '123456789012'
_OTHER_AWS_ACCOUNT_ID = '210987654321'
_AWS_CLIENT_TOKEN = 'a' * 64
_OTHER_AWS_CLIENT_TOKEN = 'b' * 64


def _client_error(
    code: str,
    *,
    request_id: str | None = 'provider-request-1',
    http_status_code: int | None = None,
    operation_name: str = 'RunInstances',
) -> botocore.exceptions.ClientError:
    if http_status_code is None:
        http_status_code = (500
                            if code == 'InsufficientInstanceCapacity' else 400)
    response = {
        'Error': {
            'Code': code,
            'Message': code,
        }
    }
    if request_id is not None:
        response['ResponseMetadata'] = {
            'RequestId': request_id,
            'HTTPStatusCode': http_status_code,
        }
    return botocore.exceptions.ClientError(response, operation_name)


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


class _InitialInstances:

    def __init__(self, instances=None):
        self.instances = [] if instances is None else instances
        self.filters = []

    def filter(self, **kwargs):
        self.filters.append(kwargs)
        return self.instances


class _RunInstancesEC2:

    def __init__(self, create, initial_instances=None):
        self.instances = _InitialInstances(initial_instances)
        self.meta = SimpleNamespace(
            client=SimpleNamespace(meta=SimpleNamespace(
                region_name='us-east-1'),
                                   create_tags=lambda **_: None))
        self._create = create
        self.create_calls = []

    def create_instances(self, **kwargs):
        self.create_calls.append(kwargs)
        return self._create(**kwargs)


class _RequestSession:
    """Minimal AWS session for lazy identity and subnet lookups."""

    def __init__(
            self,
            account_id=_AWS_ACCOUNT_ID,
            principal_arn=('arn:aws:sts::123456789012:assumed-role/test/run'),
            subnet_zones=None,
            identity_error: BaseException | None = None,
            subnet_error: BaseException | None = None,
            wait_error: BaseException | None = None):
        self.account_id = account_id
        self.principal_arn = principal_arn
        self.subnet_zones = ({
            'subnet-a': 'us-east-1a'
        } if subnet_zones is None else subnet_zones)
        self.identity_error = identity_error
        self.subnet_error = subnet_error
        self.wait_error = wait_error
        self.client_calls = []
        self.wait_calls = []

    def client(self, service_name, region_name=None):
        assert region_name == 'us-east-1'
        self.client_calls.append(service_name)
        if service_name == 'sts':

            def _get_caller_identity():
                if self.identity_error is not None:
                    raise self.identity_error
                return {
                    'Account': self.account_id,
                    'Arn': self.principal_arn,
                }

            return SimpleNamespace(get_caller_identity=_get_caller_identity)
        assert service_name == 'ec2'

        def _describe_subnets(SubnetIds):
            if self.subnet_error is not None:
                raise self.subnet_error
            return {
                'Subnets': [{
                    'SubnetId': subnet_id,
                    'AvailabilityZone': self.subnet_zones[subnet_id],
                } for subnet_id in SubnetIds if subnet_id in self.subnet_zones]
            }

        def _describe_instances(InstanceIds):
            return {
                'Reservations': [{
                    'Instances': [{
                        'InstanceId': instance_id,
                        'InstanceType': 'g6.4xlarge',
                        'State': {
                            'Name': 'running',
                        },
                        'Placement': {
                            'AvailabilityZone': 'us-east-1a'
                        },
                        'InstanceLifecycle': 'spot',
                    } for instance_id in InstanceIds]
                }]
            }

        def _get_waiter(name):
            assert name == 'instance_running'

            def _wait(**kwargs):
                self.wait_calls.append(kwargs)
                if self.wait_error is not None:
                    raise self.wait_error

            return SimpleNamespace(wait=_wait)

        return SimpleNamespace(describe_subnets=_describe_subnets,
                               describe_instances=_describe_instances,
                               get_waiter=_get_waiter)


def _run_config(*,
                count: int = 1,
                availability_zone: str = 'us-east-1a',
                spot: bool = True,
                capacity_reservation_ids=None,
                resume_stopped_nodes: bool = True,
                provider_create_idempotency_token: str |
                None = _AWS_CLIENT_TOKEN,
                provider_create_account_id: str | None = _AWS_ACCOUNT_ID):
    node_config = _node_config(['subnet-a'], spot=spot)
    if capacity_reservation_ids is not None:
        node_config['CapacityReservationSpecification'] = {
            'CapacityReservationTarget': {
                'CapacityReservationId': capacity_reservation_ids,
            }
        }
    return provision_common.ProvisionConfig(
        provider_config={
            'availability_zone': availability_zone,
            'use_internal_ips': False,
        },
        authentication_config={},
        docker_config={},
        node_config=node_config,
        count=count,
        tags={},
        resume_stopped_nodes=resume_stopped_nodes,
        ports_to_open_on_launch=None,
        provider_create_idempotency_token=(provider_create_idempotency_token),
        provider_create_account_id=provider_create_account_id)


def _install_run_instances_fakes(monkeypatch, ec2, *, session=None):
    monkeypatch.setattr(aws_instance, '_default_ec2_resource', lambda _: ec2)
    monkeypatch.setattr(aws_instance.aws, 'resource', lambda *_, **__: ec2)
    monkeypatch.setattr(aws_instance.aws, 'session', lambda *_, **__: session)


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
                'Key': 'owner',
                'Value': 'sky',
            }, {
                'Key': provision_constants.TAG_RAY_CLUSTER_NAME,
                'Value': 'cluster',
            }, {
                'Key': provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
                'Value': 'cluster',
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
                'Key': provision_constants.TAG_RAY_CLUSTER_NAME,
                'Value': 'cluster',
            }, {
                'Key': provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
                'Value': 'cluster',
            }, {
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
                'Key': provision_constants.TAG_RAY_CLUSTER_NAME,
                'Value': 'wrong-cluster',
            }, {
                'Key': provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
                'Value': 'wrong-cluster',
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
        if tag_spec['ResourceType'] in ('instance', 'volume'):
            assert tags[provision_constants.TAG_RAY_CLUSTER_NAME] == 'cluster'
            assert tags[
                provision_constants.TAG_SKYPILOT_CLUSTER_NAME] == 'cluster'
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
                               is_single_zone_request=False,
                               provider_create_idempotency_token=None):
        del (ec2_fail_fast, cluster_name, node_config, tags, count,
             associate_public_ip_address, max_efa_interfaces,
             provider_create_idempotency_token)
        observed.append(is_single_zone_request)
        return [created]

    monkeypatch.setattr(aws_instance, '_default_ec2_resource', lambda _: ec2)
    monkeypatch.setattr(aws_instance.aws, 'resource', lambda *_, **__: ec2)
    request_session = _RequestSession()
    monkeypatch.setattr(aws_instance.aws, 'session',
                        lambda *_, **__: request_session)
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
    # Successful launches retain only the existing post-create EC2/STS
    # identity capture; negative-ack identity/subnet reads are lazy.
    assert request_session.client_calls == ['ec2', 'sts']


def _fresh_instance_ec2(create_tags):
    created = SimpleNamespace(id='i-fresh',
                              tags=[],
                              placement={'AvailabilityZone': 'us-east-1a'})
    ec2 = _RunInstancesEC2(lambda **_: [created])
    ec2.meta.client.create_tags = create_tags
    return ec2


def test_fresh_instance_node_tag_retries_eventual_not_found(monkeypatch):
    calls = []
    sleeps = []

    def _create_tags(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise _client_error('InvalidInstanceID.NotFound',
                                operation_name='CreateTags')

    ec2 = _fresh_instance_ec2(_create_tags)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)
    monkeypatch.setattr(instance_requests.time, 'sleep', sleeps.append)

    record = aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                        _run_config())

    assert record.created_instance_ids == ['i-fresh']
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert len(sleeps) == 1
    assert sleeps[0] > 0


def test_fresh_identity_wait_failure_is_optional_after_create(monkeypatch):
    ec2 = _fresh_instance_ec2(lambda **_: None)
    session = _RequestSession(
        wait_error=RuntimeError('instance waiter unavailable'))
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    record = aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                        _run_config())

    assert record.created_instance_ids == ['i-fresh']
    assert record.fresh_aws_instance_identity is None
    assert session.wait_calls == [{
        'InstanceIds': ['i-fresh'],
        'WaiterConfig': {
            'Delay': 5,
            'MaxAttempts': 120,
        },
    }]


def test_fresh_instance_node_tag_does_not_retry_other_client_error(monkeypatch):
    calls = 0

    def _create_tags(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        raise _client_error('UnauthorizedOperation',
                            operation_name='CreateTags')

    ec2 = _fresh_instance_ec2(_create_tags)
    _install_run_instances_fakes(monkeypatch, ec2, session=_RequestSession())
    monkeypatch.setattr(
        instance_requests.time, 'sleep',
        lambda _: pytest.fail('Non-eventual-consistency errors must not retry'))

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert exc_info.value.response['Error']['Code'] == 'UnauthorizedOperation'
    assert calls == 1


def test_existing_instance_node_tag_does_not_use_fresh_visibility_retry(
        monkeypatch):
    calls = 0

    def _create_tags(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        raise _client_error('InvalidInstanceID.NotFound',
                            operation_name='CreateTags')

    existing = SimpleNamespace(id='i-existing',
                               state={'Name': 'pending'},
                               tags=[],
                               placement={'AvailabilityZone': 'us-east-1a'})
    ec2 = _RunInstancesEC2(
        lambda **_: pytest.fail('No fresh instance should be requested.'),
        initial_instances=[existing])
    ec2.meta.client.create_tags = _create_tags
    _install_run_instances_fakes(monkeypatch, ec2, session=_RequestSession())
    monkeypatch.setattr(
        instance_requests.time, 'sleep',
        lambda _: pytest.fail('Existing instances must not use this retry'))

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert exc_info.value.response['Error']['Code'] == (
        'InvalidInstanceID.NotFound')
    assert calls == 1


def test_fresh_instance_node_tag_eventual_not_found_retry_is_bounded(
        monkeypatch):
    calls = 0
    sleeps = []

    def _create_tags(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        raise _client_error('InvalidInstanceID.NotFound',
                            operation_name='CreateTags')

    ec2 = _fresh_instance_ec2(_create_tags)
    _install_run_instances_fakes(monkeypatch, ec2, session=_RequestSession())
    monkeypatch.setattr(instance_requests.time, 'sleep', sleeps.append)

    with pytest.raises(RuntimeError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert calls == aws_instance._FRESH_INSTANCE_TAG_MAX_ATTEMPTS
    assert len(sleeps) == calls - 1
    assert isinstance(exc_info.value.__cause__, botocore.exceptions.ClientError)
    assert exc_info.value.__cause__.response['Error']['Code'] == (
        'InvalidInstanceID.NotFound')


def test_fresh_spot_rejection_attaches_complete_provider_negative_ack(
        monkeypatch):

    def _reject(**kwargs):
        assert kwargs['MinCount'] == kwargs['MaxCount'] == 2
        assert kwargs['ClientToken'] == _AWS_CLIENT_TOKEN
        raise _client_error('InsufficientInstanceCapacity',
                            request_id='provider-request-capacity')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config(count=2))

    receipt = exc_info.value.provider_negative_ack
    assert not hasattr(
        exc_info.value,
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR)
    assert receipt == {
        'schema_version': 1,
        'provider': 'aws',
        'operation': 'RunInstances',
        'reason': 'capacity',
        'aws_account_id': '123456789012',
        'aws_principal_arn': 'arn:aws:sts::123456789012:assumed-role/test/run',
        'cluster_name_on_cloud': 'sky-cluster',
        'requested_count': 2,
        'market': 'spot',
        'instance_type': 'g6.4xlarge',
        'region': 'us-east-1',
        'availability_zone': 'us-east-1a',
        'client_token': _AWS_CLIENT_TOKEN,
        'invocations': [{
            'region': 'us-east-1',
            'availability_zone': 'us-east-1a',
            'initial_nonterminated_instance_ids': [],
            'resumed_instance_ids': [],
            'created_instance_ids': [],
            'successful_create_calls': 0,
            'ambiguous_create_calls': 0,
            'create_call_count': 1,
            'attempts': [{
                'provider_request_id': 'provider-request-capacity',
                'error_code': 'InsufficientInstanceCapacity',
                'reason': 'capacity',
                'http_status_code': 500,
                'aws_account_id': '123456789012',
                'aws_principal_arn': 'arn:aws:sts::123456789012:assumed-role/test/run',
                'region': 'us-east-1',
                'availability_zone': 'us-east-1a',
                'subnet_id': 'subnet-a',
                'market': 'spot',
                'instance_type': 'g6.4xlarge',
                'cluster_name_on_cloud': 'sky-cluster',
                'min_count': 2,
                'max_count': 2,
                'capacity_reservation_id': None,
                'client_token': _AWS_CLIENT_TOKEN,
            }],
        }],
    }
    # The initial inventory query explicitly includes shutting-down objects;
    # their absence is therefore part of the provider-native proof.
    assert ec2.instances.filters[0]['Filters'][0]['Values'] == [
        'pending', 'running', 'stopping', 'stopped', 'shutting-down'
    ]
    assert session.client_calls == ['sts', 'ec2']


def test_fresh_spot_quota_rejection_attaches_typed_negative_ack(monkeypatch):

    def _reject(**kwargs):
        del kwargs
        raise _client_error('MaxSpotInstanceCountExceeded',
                            request_id='provider-request-quota')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        aws_instance.run_instances('us-east-1', 'display-name', 'cloud-name',
                                   _run_config())

    receipt = exc_info.value.provider_negative_ack
    assert receipt['reason'] == 'quota'
    assert receipt['cluster_name_on_cloud'] == 'cloud-name'
    assert receipt['invocations'][0]['attempts'][0]['error_code'] == (
        'MaxSpotInstanceCountExceeded')


@pytest.mark.parametrize(
    ('error_kwargs', 'availability_zone', 'spot'),
    [
        ({
            'request_id': None
        }, 'us-east-1a', True),
        ({
            'http_status_code': 400
        }, 'us-east-1a', True),
        ({
            'http_status_code': 503
        }, 'us-east-1a', True),
        ({
            'operation_name': 'DescribeInstances'
        }, 'us-east-1a', True),
        ({}, 'us-east-1a,us-east-1b', True),
        ({}, 'us-east-1a', False),
    ],
)
def test_provider_negative_ack_missing_scope_fails_closed(
        monkeypatch, error_kwargs, availability_zone, spot):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 1)

    def _reject(**kwargs):
        del kwargs
        raise _client_error('InsufficientInstanceCapacity', **error_kwargs)

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    expected_error = (exceptions.ServeReplicaLaunchFenceError
                      if ',' in availability_zone else
                      exceptions.ProviderCreateAmbiguousError)
    with pytest.raises(expected_error) as exc_info:
        aws_instance.run_instances(
            'us-east-1', 'unused', 'sky-cluster',
            _run_config(availability_zone=availability_zone, spot=spot))

    assert not hasattr(exc_info.value, 'provider_negative_ack')


def test_bound_account_mismatch_precedes_inventory_and_run_instances(
        monkeypatch):

    def _unexpected_run_instances(**kwargs):
        pytest.fail(f'RunInstances must not be called: {kwargs!r}')

    ec2 = _RunInstancesEC2(_unexpected_run_instances)
    session = _RequestSession(
        account_id=_OTHER_AWS_ACCOUNT_ID,
        principal_arn=(
            'arn:aws:sts::210987654321:assumed-role/test/rotated-session'))
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert 'provider scope' in str(exc_info.value)
    assert ec2.instances.filters == []
    assert ec2.create_calls == []
    assert session.client_calls == ['sts']
    assert not hasattr(exc_info.value, 'provider_negative_ack')


@pytest.mark.parametrize('proof_layer', [
    'resource',
    'session',
    'identity',
    'subnet',
    'ec2-client',
    'inventory-query',
    'inventory-attribute',
])
def test_bound_provider_proof_unavailability_requeues_same_association(
        monkeypatch, proof_layer):
    """Read-only proof outages pause instead of terminalizing PROVIDER_IO."""

    def _unexpected_run_instances(**kwargs):
        pytest.fail(f'RunInstances must not be called: {kwargs!r}')

    class _UnreadableInstance:

        @property
        def id(self):
            raise TimeoutError('instance attributes unavailable')

    initial_instances = ([_UnreadableInstance()]
                         if proof_layer == 'inventory-attribute' else None)
    ec2 = _RunInstancesEC2(_unexpected_run_instances, initial_instances)
    if proof_layer == 'inventory-query':

        def _unavailable_inventory(**_):
            raise TimeoutError('instance inventory unavailable')

        ec2.instances.filter = _unavailable_inventory
    session = _RequestSession(
        identity_error=(TimeoutError('sts unavailable')
                        if proof_layer == 'identity' else None),
        subnet_error=(TimeoutError('subnet unavailable')
                      if proof_layer == 'subnet' else None))

    def _default_resource(_):
        if proof_layer == 'resource':
            raise TimeoutError('credentials unavailable')
        return ec2

    def _session(*_, **__):
        if proof_layer == 'session':
            raise TimeoutError('session unavailable')
        return session

    resource_calls = 0

    def _resource(*_, **__):
        nonlocal resource_calls
        resource_calls += 1
        if proof_layer == 'ec2-client':
            raise TimeoutError('EC2 client unavailable')
        return ec2

    monkeypatch.setattr(aws_instance, '_default_ec2_resource',
                        _default_resource)
    monkeypatch.setattr(aws_instance.aws, 'session', _session)
    monkeypatch.setattr(aws_instance.aws, 'resource', _resource)

    with pytest.raises(exceptions.ExecutionPausedError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert exc_info.value.retry_wait_seconds > 0
    assert 'same immutable ordinary-paid AWS association' in (
        exc_info.value.hint)
    assert len(ec2.instances.filters) == (1 if proof_layer
                                          == 'inventory-attribute' else 0)
    assert ec2.create_calls == []
    if proof_layer in ('resource', 'session'):
        assert session.client_calls == []
    elif proof_layer == 'identity':
        assert session.client_calls == ['sts']
    else:
        assert session.client_calls == ['sts', 'ec2']
    assert resource_calls == (0 if proof_layer in ('resource', 'session',
                                                   'identity', 'subnet') else 1)


@pytest.mark.parametrize(
    ('client_token', 'account_id'),
    [
        (None, _AWS_ACCOUNT_ID),
        (_AWS_CLIENT_TOKEN, None),
        ('not-a-valid-client-token', _AWS_ACCOUNT_ID),
    ],
)
def test_partial_or_invalid_bound_provider_scope_precedes_provider_io(
        monkeypatch, client_token, account_id):

    def _unexpected_run_instances(**kwargs):
        pytest.fail(f'RunInstances must not be called: {kwargs!r}')

    ec2 = _RunInstancesEC2(_unexpected_run_instances)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError) as exc_info:
        aws_instance.run_instances(
            'us-east-1', 'unused', 'sky-cluster',
            _run_config(provider_create_idempotency_token=client_token,
                        provider_create_account_id=account_id))

    assert ec2.instances.filters == []
    assert ec2.create_calls == []
    assert not hasattr(exc_info.value, 'provider_negative_ack')


def test_tokenized_paid_launch_rejects_multiple_subnets_before_ec2_io(
        monkeypatch):

    def _unexpected_run_instances(**kwargs):
        pytest.fail(f'RunInstances must not be called: {kwargs!r}')

    ec2 = _RunInstancesEC2(_unexpected_run_instances)
    session = _RequestSession(subnet_zones={
        'subnet-a': 'us-east-1a',
        'subnet-b': 'us-east-1a',
    })
    _install_run_instances_fakes(monkeypatch, ec2, session=session)
    config = _run_config()
    config.node_config['SubnetIds'] = ['subnet-a', 'subnet-b']

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError):
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster', config)

    assert ec2.instances.filters == []
    assert ec2.create_calls == []
    assert session.client_calls == []


def test_missing_client_token_cannot_produce_provider_negative_ack(monkeypatch):

    def _reject(**kwargs):
        assert 'ClientToken' not in kwargs
        raise _client_error('InsufficientInstanceCapacity')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        aws_instance.run_instances(
            'us-east-1', 'unused', 'sky-cluster',
            _run_config(provider_create_idempotency_token=None,
                        provider_create_account_id=None))

    assert len(ec2.create_calls) == 1
    assert not hasattr(exc_info.value, 'provider_negative_ack')


def test_provider_request_token_mismatch_cannot_produce_negative_ack(
        monkeypatch):
    original_create_instances = aws_instance._create_instances

    def _create_with_mismatched_token(*args, **kwargs):
        assert kwargs['provider_create_idempotency_token'] == _AWS_CLIENT_TOKEN
        kwargs['provider_create_idempotency_token'] = _OTHER_AWS_CLIENT_TOKEN
        return original_create_instances(*args, **kwargs)

    def _reject(**kwargs):
        assert kwargs['ClientToken'] == _OTHER_AWS_CLIENT_TOKEN
        raise _client_error('InsufficientInstanceCapacity')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)
    monkeypatch.setattr(aws_instance, '_create_instances',
                        _create_with_mismatched_token)

    with pytest.raises(exceptions.ProviderCreateAmbiguousError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert len(ec2.create_calls) == 1
    assert not hasattr(exc_info.value, 'provider_negative_ack')
    assert not hasattr(
        exc_info.value,
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR)


def test_lost_success_retry_uses_same_client_token_and_adopts_result(
        monkeypatch):
    monkeypatch.setattr(instance_requests.time, 'sleep', lambda _: None)
    created = SimpleNamespace(id='i-idempotently-created',
                              tags=[],
                              placement={'AvailabilityZone': 'us-east-1a'})
    instances_by_token = {}
    observed_tokens = []

    def _create_or_replay(**kwargs):
        token = kwargs['ClientToken']
        observed_tokens.append(token)
        if token not in instances_by_token:
            # Model EC2 accepting the request but the successful response being
            # lost. The next invocation with the same token returns that one
            # already-created instance rather than creating a duplicate.
            instances_by_token[token] = [created]
            raise _client_error('ServiceUnavailable',
                                request_id='provider-response-lost',
                                http_status_code=503)
        return instances_by_token[token]

    ec2 = _RunInstancesEC2(_create_or_replay)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    record = aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                        _run_config())

    assert observed_tokens == [_AWS_CLIENT_TOKEN, _AWS_CLIENT_TOKEN]
    assert len(instances_by_token) == 1
    assert record.created_instance_ids == ['i-idempotently-created']
    assert record.head_instance_id == 'i-idempotently-created'


def test_shutting_down_initial_instance_prevents_negative_ack(monkeypatch):
    shutting_down = SimpleNamespace(id='i-shutting-down',
                                    state={'Name': 'shutting-down'},
                                    tags=[])

    def _reject(**kwargs):
        del kwargs
        raise _client_error('InsufficientInstanceCapacity')

    ec2 = _RunInstancesEC2(_reject, [shutting_down])
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ProviderCreateAmbiguousError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert not hasattr(exc_info.value, 'provider_negative_ack')
    assert session.client_calls == ['sts', 'ec2']


def test_ambiguous_transport_outcome_replays_same_token_and_adopts_result(
        monkeypatch):
    monkeypatch.setattr(instance_requests.utils, 'BOTO_MAX_RETRIES', 1)
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 1)
    created = SimpleNamespace(id='i-created-after-replay',
                              tags=[],
                              placement={'AvailabilityZone': 'us-east-1a'})
    observed_tokens = []

    def _create_or_replay(**kwargs):
        observed_tokens.append(kwargs['ClientToken'])
        if len(observed_tokens) == 1:
            raise botocore.exceptions.ReadTimeoutError(
                endpoint_url='https://ec2.us-east-1.amazonaws.com')
        return [created]

    ec2 = _RunInstancesEC2(_create_or_replay)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)
    config = _run_config()

    with pytest.raises(exceptions.ProviderCreateAmbiguousError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster', config)

    assert 'same immutable ordinary-paid association' in exc_info.value.hint
    round_tripped = pickle.loads(pickle.dumps(exc_info.value))
    assert isinstance(round_tripped, exceptions.ProviderCreateAmbiguousError)
    assert round_tripped.retry_wait_seconds == exc_info.value.retry_wait_seconds
    record = aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                        config)

    assert observed_tokens == [_AWS_CLIENT_TOKEN, _AWS_CLIENT_TOKEN]
    assert record.created_instance_ids == ['i-created-after-replay']


@pytest.mark.parametrize(('error_type', 'error_kwargs'), [
    (botocore.exceptions.EndpointConnectionError, {
        'endpoint_url': 'https://ec2.example'
    }),
    (botocore.exceptions.ConnectTimeoutError, {
        'endpoint_url': 'https://ec2.example'
    }),
    (botocore.exceptions.ConnectionClosedError, {
        'endpoint_url': 'https://ec2.example',
        'request': None,
        'response': None,
    }),
    (botocore.exceptions.ProxyConnectionError, {
        'proxy_url': 'https://proxy.example'
    }),
    (botocore.exceptions.SSLError, {
        'endpoint_url': 'https://ec2.example',
        'error': 'TLS failure',
    }),
])
def test_tokenized_botocore_transport_errors_pause_fail_closed(
        monkeypatch, error_type, error_kwargs):
    monkeypatch.setattr(instance_requests.utils, 'BOTO_MAX_RETRIES', 1)
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 1)

    def _unavailable(**kwargs):
        assert kwargs['ClientToken'] == _AWS_CLIENT_TOKEN
        raise error_type(**error_kwargs)

    ec2 = _RunInstancesEC2(_unavailable)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ProviderCreateAmbiguousError):
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())


def test_exhausted_retryable_server_errors_pause_same_token(monkeypatch):
    monkeypatch.setattr(instance_requests.time, 'sleep', lambda _: None)
    monkeypatch.setattr(instance_requests.utils, 'BOTO_MAX_RETRIES', 2)
    observed_tokens = []

    def _unavailable(**kwargs):
        observed_tokens.append(kwargs['ClientToken'])
        raise _client_error('ServiceUnavailable',
                            request_id=f'request-{len(observed_tokens)}',
                            http_status_code=503)

    ec2 = _RunInstancesEC2(_unavailable)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ProviderCreateAmbiguousError):
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert observed_tokens == [_AWS_CLIENT_TOKEN, _AWS_CLIENT_TOKEN]


def test_retryable_server_error_before_capacity_rejection_is_ambiguous(
        monkeypatch):
    monkeypatch.setattr(instance_requests.time, 'sleep', lambda _: None)
    calls = 0

    def _reject(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        if calls == 1:
            raise _client_error('ServiceUnavailable',
                                request_id='provider-request-ambiguous',
                                http_status_code=503)
        raise _client_error('InsufficientInstanceCapacity',
                            request_id='provider-request-capacity')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ProviderCreateAmbiguousError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert calls == 2
    assert not hasattr(exc_info.value, 'provider_negative_ack')
    assert not hasattr(
        exc_info.value,
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR)
    assert session.client_calls == ['sts', 'ec2']


def test_subnet_provider_az_mismatch_prevents_negative_ack(monkeypatch):

    def _reject(**kwargs):
        del kwargs
        raise _client_error('InsufficientInstanceCapacity')

    ec2 = _RunInstancesEC2(_reject)
    session = _RequestSession(subnet_zones={'subnet-a': 'us-east-1b'})
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError) as exc_info:
        aws_instance.run_instances('us-east-1', 'unused', 'sky-cluster',
                                   _run_config())

    assert not hasattr(exc_info.value, 'provider_negative_ack')
    assert not hasattr(
        exc_info.value,
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR)
    assert session.client_calls == ['sts', 'ec2']
    assert ec2.create_calls == []


def test_tokenized_paid_launch_rejects_targeted_reservation_before_ec2_io(
        monkeypatch):
    calls = 0

    def _unexpected_create(**kwargs):
        nonlocal calls
        calls += 1
        pytest.fail(f'RunInstances must not be called: {kwargs!r}')

    ec2 = _RunInstancesEC2(_unexpected_create)
    session = _RequestSession()
    _install_run_instances_fakes(monkeypatch, ec2, session=session)

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError) as exc_info:
        aws_instance.run_instances(
            'us-east-1', 'display-name', 'cloud-name',
            _run_config(count=2, capacity_reservation_ids=['cr-1']))

    assert calls == 0
    assert not hasattr(exc_info.value, 'provider_negative_ack')
    assert not hasattr(
        exc_info.value,
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR)
    assert session.client_calls == []


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
