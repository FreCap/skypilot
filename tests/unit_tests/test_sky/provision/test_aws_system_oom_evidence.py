"""Tests for request-scoped AWS system-OOM identity evidence."""

from unittest import mock

import pytest

from sky.provision.aws import instance as aws_instance


def _describe(*, lifecycle=mock.sentinel.absent):
    instance = {
        'InstanceId': 'i-0123',
        'InstanceType': 'g6.xlarge',
        'Placement': {
            'AvailabilityZone': 'us-east-1a'
        },
    }
    if lifecycle is not mock.sentinel.absent:
        instance['InstanceLifecycle'] = lifecycle
    return {'Reservations': [{'Instances': [instance]}]}


@pytest.mark.parametrize(('lifecycle', 'expected_market'), [
    (mock.sentinel.absent, 'on_demand'),
    ('spot', 'spot'),
])
def test_capture_uses_one_session_for_exact_describe_and_sts(
        lifecycle, expected_market):
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = _describe(lifecycle=lifecycle)
    sts = mock.Mock()
    sts.get_caller_identity.return_value = {'Account': '123456789012'}
    request_session = mock.Mock()
    request_session.client.side_effect = lambda service, **_: (ec2 if service ==
                                                               'ec2' else sts)

    identity = aws_instance._capture_fresh_instance_identity(  # pylint: disable=protected-access
        request_session,
        region='us-east-1',
        created_instance_ids=['i-0123'])

    assert identity is not None
    assert identity.ec2_instance_id == 'i-0123'
    assert identity.instance_type == 'g6.xlarge'
    assert identity.availability_zone == 'us-east-1a'
    assert identity.aws_account_id == '123456789012'
    assert identity.market_type == expected_market
    ec2.describe_instances.assert_called_once_with(InstanceIds=['i-0123'])
    assert request_session.client.call_args_list == [
        mock.call('ec2', region_name='us-east-1'),
        mock.call('sts', region_name='us-east-1'),
    ]


def test_capture_rejects_unknown_non_null_lifecycle():
    ec2 = mock.Mock()
    ec2.describe_instances.return_value = _describe(lifecycle='scheduled')
    session = mock.Mock()
    session.client.return_value = ec2

    with pytest.raises(ValueError, match='InstanceLifecycle'):
        aws_instance._capture_fresh_instance_identity(  # pylint: disable=protected-access
            session,
            region='us-east-1',
            created_instance_ids=['i-0123'])


def test_capture_requires_one_fresh_created_id():
    ec2 = mock.Mock()
    session = mock.Mock()
    session.client.return_value = ec2

    assert aws_instance._capture_fresh_instance_identity(  # pylint: disable=protected-access
        session,
        region='us-east-1',
        created_instance_ids=[]) is None
    assert aws_instance._capture_fresh_instance_identity(  # pylint: disable=protected-access
        session,
        region='us-east-1',
        created_instance_ids=['i-1', 'i-2']) is None
    ec2.describe_instances.assert_not_called()
