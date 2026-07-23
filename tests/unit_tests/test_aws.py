import json
import pathlib
import shlex
import typing
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky import logs
from sky import resources
from sky import skypilot_config
from sky.backends import backend_utils
from sky.clouds import Region
from sky.clouds import Zone
from sky.clouds.aws import AWS
from sky.provision import constants as provision_constants
from sky.provision.aws import config
from sky.provision.aws import iam_profile
from sky.utils import common_utils
from sky.utils import config_utils


def test_aws_label():
    aws = AWS()
    # Invalid - AWS prefix
    assert not aws.is_label_valid('aws:whatever', 'value')[0]
    # Valid - valid prefix
    assert aws.is_label_valid('any:whatever', 'value')[0]
    # Valid - valid prefix
    assert aws.is_label_valid('Owner', 'username-1')[0]
    # Invalid - Too long
    assert not (aws.is_label_valid(
        'sprinto:thisiexample_string_with_123_characters_length_thing_thing_thing_thing_thing_thing_thing_thin_thing_thing_thing_thing_thing_thing',
        'value',
    )[0])
    # Invalid - Too long
    assert not (aws.is_label_valid(
        'sprinto:short',
        'thisiexample_string_with_123_characters_length_thing_thing_thing_thing_thing_thing_thing_thin_thing_thing_thing_thing_thing_thingthisiexample_string_with_123_characters_length_thing_thing_thing_thing_thing_thing_thing_thin_thing_thing_thing_thing_thing_thing',
    )[0])


def test_usable_subnets(monkeypatch):
    """Test the output of the usable_subnets function."""

    vpc_name = "test_vpc"
    vpc_id = "test-vpc-id"
    region = "test-region"

    subnets = MagicMock()
    monkeypatch.setattr(subnets, 'all', lambda: [])

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets
    # monkeypatch.setattr(mock_ec2, 'subnets', subnets
    # Case 1: default VPC has no subnets.
    monkeypatch.setattr(config, 'get_vpc_id_by_name',
                        lambda *args, **kwargs: vpc_id)
    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=False,
                                      vpc_name=None,
                                      subnet_names=None)

    error_message = str(e.value)
    assert f"{provision_constants.ERROR_NO_NODES_LAUNCHED}: The default VPC in {region} either does not exist or has no subnets." == error_message

    # Case 2: Specified VPC has no subnets.
    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=False,
                                      vpc_name=vpc_name,
                                      subnet_names=None)

    error_message = str(e.value)
    assert f"{provision_constants.ERROR_NO_NODES_LAUNCHED}: No candidate subnets found in specified VPC {vpc_id}." == error_message

    # Case 3: All the subnets are public and use_internal_ips is True.
    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)
    subnet = MagicMock()
    subnet.vpc = MagicMock()
    subnet.vpc.is_default = True
    subnet.vpc_id = vpc_id
    subnet.state = 'available'
    monkeypatch.setattr(subnets, 'all', lambda: [subnet])
    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=True,
                                      vpc_name=vpc_name,
                                      subnet_names=None)

    error_message = str(e.value)
    assert f"{provision_constants.ERROR_NO_NODES_LAUNCHED}: The use_internal_ips option is set to True, but all candidate subnets are public." == error_message

    # Case 4: All the subnets are private and use_internal_ips is False
    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: False)
    subnet = MagicMock()
    subnet.vpc = MagicMock()
    subnet.vpc.is_default = True
    subnet.vpc_id = vpc_id
    subnet.state = 'available'
    monkeypatch.setattr(subnets, 'all', lambda: [subnet])
    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=False,
                                      vpc_name=vpc_name,
                                      subnet_names=None)

    error_message = str(e.value)
    assert f"{provision_constants.ERROR_NO_NODES_LAUNCHED}: All candidate subnets are private, did you mean to set use_internal_ips to True?" == error_message


def test_subnet_names_resolves_by_tag(monkeypatch):
    """Test that subnet_names resolves subnets by tag:Name filter."""
    vpc_id = 'test-vpc-id'
    region = 'us-east-1'

    # Create mock subnets returned by the filter call
    mock_subnet_1 = MagicMock()
    mock_subnet_1.vpc_id = vpc_id
    mock_subnet_1.subnet_id = 'subnet-aaa'
    mock_subnet_1.state = 'available'
    mock_subnet_1.availability_zone = 'us-east-1a'
    mock_subnet_1.map_public_ip_on_launch = False

    mock_subnet_2 = MagicMock()
    mock_subnet_2.vpc_id = vpc_id
    mock_subnet_2.subnet_id = 'subnet-bbb'
    mock_subnet_2.state = 'available'
    mock_subnet_2.availability_zone = 'us-east-1b'
    mock_subnet_2.map_public_ip_on_launch = False

    filtered_subnets = [mock_subnet_1, mock_subnet_2]

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = filtered_subnets
    subnets_mock.filter.return_value = filtered_subnets

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    # Subnets are public
    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)

    result_subnets, result_vpc_id = config._get_subnet_and_vpc_id(
        ec2=mock_ec2,
        security_group_ids=None,
        region=region,
        availability_zone=None,
        use_internal_ips=False,
        vpc_name=None,
        subnet_names=['my-subnet-1', 'my-subnet-2'])

    # Verify filter was called with correct tag:Name filter
    subnets_mock.filter.assert_called_once_with(Filters=[{
        'Name': 'tag:Name',
        'Values': ['my-subnet-1', 'my-subnet-2'],
    }])
    assert result_vpc_id == vpc_id
    assert len(result_subnets) == 2


def test_subnet_names_single_string(monkeypatch):
    """Test that a single string subnet_name is converted to a list."""
    vpc_id = 'test-vpc-id'
    region = 'us-east-1'

    mock_subnet = MagicMock()
    mock_subnet.vpc_id = vpc_id
    mock_subnet.subnet_id = 'subnet-aaa'
    mock_subnet.state = 'available'
    mock_subnet.availability_zone = 'us-east-1a'
    mock_subnet.map_public_ip_on_launch = False

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = [mock_subnet]
    subnets_mock.filter.return_value = [mock_subnet]

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)

    result_subnets, result_vpc_id = config._get_subnet_and_vpc_id(
        ec2=mock_ec2,
        security_group_ids=None,
        region=region,
        availability_zone=None,
        use_internal_ips=False,
        vpc_name=None,
        subnet_names='my-single-subnet')

    # Should convert string to list and pass to filter
    subnets_mock.filter.assert_called_once_with(Filters=[{
        'Name': 'tag:Name',
        'Values': ['my-single-subnet'],
    }])
    assert result_vpc_id == vpc_id
    assert len(result_subnets) == 1


def test_subnet_names_not_found(monkeypatch):
    """Test error when specified subnet names don't match any subnets."""
    region = 'us-east-1'

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = []
    # No subnets match the filter
    subnets_mock.filter.return_value = []

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=False,
                                      vpc_name=None,
                                      subnet_names=['nonexistent-subnet'])

    error_message = str(e.value)
    assert 'No subnets with name(s)' in error_message
    assert 'nonexistent-subnet' in error_message


def test_subnet_names_infers_vpc(monkeypatch):
    """Test that VPC ID is inferred from specified subnets when no vpc_name."""
    vpc_id = 'vpc-inferred'
    region = 'us-east-1'

    mock_subnet = MagicMock()
    mock_subnet.vpc_id = vpc_id
    mock_subnet.subnet_id = 'subnet-aaa'
    mock_subnet.state = 'available'
    mock_subnet.availability_zone = 'us-east-1a'
    mock_subnet.map_public_ip_on_launch = False

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = [mock_subnet]
    subnets_mock.filter.return_value = [mock_subnet]

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)

    _, result_vpc_id = config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                                     security_group_ids=None,
                                                     region=region,
                                                     availability_zone=None,
                                                     use_internal_ips=False,
                                                     vpc_name=None,
                                                     subnet_names=['my-subnet'])

    # VPC should be inferred from the first matching subnet
    assert result_vpc_id == vpc_id


def test_subnet_names_with_vpc_name(monkeypatch):
    """Test that subnet_names works together with vpc_name."""
    vpc_id = 'vpc-explicit'
    region = 'us-east-1'

    mock_subnet = MagicMock()
    mock_subnet.vpc_id = vpc_id
    mock_subnet.subnet_id = 'subnet-aaa'
    mock_subnet.state = 'available'
    mock_subnet.availability_zone = 'us-east-1a'
    mock_subnet.map_public_ip_on_launch = False

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = [mock_subnet]
    subnets_mock.filter.return_value = [mock_subnet]

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    monkeypatch.setattr(config, 'get_vpc_id_by_name',
                        lambda *args, **kwargs: vpc_id)
    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)

    result_subnets, result_vpc_id = config._get_subnet_and_vpc_id(
        ec2=mock_ec2,
        security_group_ids=None,
        region=region,
        availability_zone=None,
        use_internal_ips=False,
        vpc_name='my-vpc',
        subnet_names=['my-subnet'])

    assert result_vpc_id == vpc_id
    assert len(result_subnets) == 1


def test_subnet_names_wrong_vpc(monkeypatch):
    """Test error when subnets don't belong to the specified VPC."""
    region = 'us-east-1'

    # Subnet belongs to a different VPC than the one specified
    mock_subnet = MagicMock()
    mock_subnet.vpc_id = 'vpc-other'
    mock_subnet.subnet_id = 'subnet-aaa'
    mock_subnet.state = 'available'
    mock_subnet.availability_zone = 'us-east-1a'

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = [mock_subnet]
    subnets_mock.filter.return_value = [mock_subnet]

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    monkeypatch.setattr(config, 'get_vpc_id_by_name',
                        lambda *args, **kwargs: 'vpc-specified')

    with pytest.raises(RuntimeError) as e:
        config._get_subnet_and_vpc_id(ec2=mock_ec2,
                                      security_group_ids=None,
                                      region=region,
                                      availability_zone=None,
                                      use_internal_ips=False,
                                      vpc_name='my-vpc',
                                      subnet_names=['my-subnet'])

    error_message = str(e.value)
    assert 'No candidate subnets found in specified VPC' in error_message


def test_security_group_tagged_on_create():
    """Test that create_security_group is called with skypilot tag."""
    mock_ec2 = MagicMock()

    # No existing security group found
    mock_ec2.SecurityGroup.return_value = None
    mock_ec2.security_groups = MagicMock()
    mock_ec2.security_groups.filter.return_value = []

    # After creation, return a mock security group
    created_sg = MagicMock(id='sg-new', group_name='test-sg')
    with patch.object(config,
                      'get_security_group_from_vpc_id',
                      side_effect=[None, created_sg]):
        config._get_or_create_vpc_security_group(ec2=mock_ec2,
                                                 vpc_id='vpc-123',
                                                 expected_sg_name='test-sg')

    mock_ec2.meta.client.create_security_group.assert_called_once()
    call_kwargs = mock_ec2.meta.client.create_security_group.call_args[1]
    assert 'TagSpecifications' in call_kwargs
    tag_specs = call_kwargs['TagSpecifications']
    assert tag_specs == [{
        'ResourceType': 'security-group',
        'Tags': [{
            'Key': 'skypilot',
            'Value': 'true',
        }],
    }]


def _client_error(code: str):
    return config.aws.botocore_exceptions().ClientError(
        {'Error': {
            'Code': code,
            'Message': code,
        }}, 'AuthorizeSecurityGroupIngress')


def test_configure_iam_role_reuses_existing_profile():
    assert config._configure_iam_role is iam_profile.configure_iam_role
    assert (config.DEFAULT_SKYPILOT_INSTANCE_PROFILE ==
            iam_profile.DEFAULT_SKYPILOT_INSTANCE_PROFILE)
    assert config.DEFAULT_SKYPILOT_IAM_ROLE == iam_profile.DEFAULT_SKYPILOT_IAM_ROLE

    iam = MagicMock()
    profile = iam.InstanceProfile.return_value
    profile.arn = 'arn:aws:iam::123:instance-profile/skypilot-v1'
    profile.roles = [MagicMock()]

    with patch.object(config.time, 'sleep') as mock_sleep:
        result = config._configure_iam_role(iam)

    assert result == {'Arn': profile.arn}
    iam.InstanceProfile.assert_called_once_with(
        config.DEFAULT_SKYPILOT_INSTANCE_PROFILE)
    profile.load.assert_called_once_with()
    iam.meta.client.create_instance_profile.assert_not_called()
    iam.Role.assert_not_called()
    profile.add_role.assert_not_called()
    mock_sleep.assert_not_called()


def test_configure_iam_role_creates_profile_role_and_policies():
    iam = MagicMock()
    missing_profile = MagicMock()
    missing_profile.load.side_effect = _client_error('NoSuchEntity')
    profile = MagicMock()
    profile.arn = 'arn:aws:iam::123:instance-profile/skypilot-v1'
    profile.roles = []
    iam.InstanceProfile.side_effect = [missing_profile, profile]

    missing_role = MagicMock()
    missing_role.load.side_effect = _client_error('NoSuchEntity')
    role = MagicMock()
    role.name = config.DEFAULT_SKYPILOT_IAM_ROLE
    role.arn = 'arn:aws:iam::123:role/skypilot-v1'
    iam.Role.side_effect = [missing_role, role]

    with patch.object(config.time, 'sleep') as mock_sleep:
        result = config._configure_iam_role(iam)

    assert result == {'Arn': profile.arn}
    iam.meta.client.create_instance_profile.assert_called_once_with(
        InstanceProfileName=config.DEFAULT_SKYPILOT_INSTANCE_PROFILE)
    iam.create_role.assert_called_once()
    role_kwargs = iam.create_role.call_args.kwargs
    assert role_kwargs['RoleName'] == config.DEFAULT_SKYPILOT_IAM_ROLE
    assert json.loads(role_kwargs['AssumeRolePolicyDocument']) == {
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {
                'Service': 'ec2.amazonaws.com'
            },
            'Action': 'sts:AssumeRole',
        }]
    }
    assert [
        call.kwargs['PolicyArn'] for call in role.attach_policy.call_args_list
    ] == [
        'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
        'arn:aws:iam::aws:policy/AmazonS3FullAccess',
    ]
    role.Policy.assert_called_once_with('SkyPilotPassRolePolicy')
    inline_policy = json.loads(
        role.Policy.return_value.put.call_args.kwargs['PolicyDocument'])
    assert inline_policy == {
        'Statement': [{
            'Effect': 'Allow',
            'Action': ['iam:GetRole', 'iam:PassRole'],
            'Resource': role.arn,
        }, {
            'Effect': 'Allow',
            'Action': 'iam:GetInstanceProfile',
            'Resource': profile.arn,
        }]
    }
    profile.add_role.assert_called_once_with(RoleName=role.name)
    assert mock_sleep.call_args_list == [call(15), call(15)]


def test_configure_iam_role_attaches_existing_role():
    iam = MagicMock()
    profile = iam.InstanceProfile.return_value
    profile.arn = 'arn:aws:iam::123:instance-profile/skypilot-v1'
    profile.roles = []
    role = iam.Role.return_value
    role.name = config.DEFAULT_SKYPILOT_IAM_ROLE

    with patch.object(config.time, 'sleep') as mock_sleep:
        result = config._configure_iam_role(iam)

    assert result == {'Arn': profile.arn}
    iam.create_role.assert_not_called()
    role.attach_policy.assert_not_called()
    role.Policy.assert_not_called()
    profile.add_role.assert_called_once_with(RoleName=role.name)
    mock_sleep.assert_called_once_with(15)


def test_configure_security_group_ignores_concurrent_duplicate_ingress():
    security_group = MagicMock(id='sg-shared')
    security_group.ip_permissions = []
    security_group.ip_permissions_egress = [{}]
    security_group.authorize_ingress.side_effect = _client_error(
        'InvalidPermission.Duplicate')

    with patch.object(config,
                      '_get_or_create_vpc_security_group',
                      return_value=security_group):
        result = config._configure_security_group(MagicMock(), 'vpc-123',
                                                  'shared-sg', [], False)

    assert result == ['sg-shared']
    security_group.authorize_ingress.assert_called_once()


def test_configure_security_group_raises_nonduplicate_ingress_error():
    security_group = MagicMock(id='sg-shared')
    security_group.ip_permissions = []
    security_group.ip_permissions_egress = [{}]
    security_group.authorize_ingress.side_effect = _client_error(
        'UnauthorizedOperation')

    with patch.object(config,
                      '_get_or_create_vpc_security_group',
                      return_value=security_group), pytest.raises(
                          config.aws.botocore_exceptions().ClientError):
        config._configure_security_group(MagicMock(), 'vpc-123', 'shared-sg',
                                         [], False)


def test_configure_security_group_ignores_concurrent_duplicate_egress():
    security_group = MagicMock(id='sg-shared')
    security_group.ip_permissions = [{}]
    security_group.ip_permissions_egress = []
    security_group.authorize_egress.side_effect = _client_error(
        'InvalidPermission.Duplicate')

    with patch.object(config,
                      '_get_or_create_vpc_security_group',
                      return_value=security_group):
        result = config._configure_security_group(MagicMock(), 'vpc-123',
                                                  'shared-sg', [], True)

    assert result == ['sg-shared']
    security_group.authorize_egress.assert_called_once()


def test_ssm_default(monkeypatch):
    """Test that SSM is explicitly set to true if use_internal_ips is true
    and ssh_proxy_command is not set.
    """
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    tmp_yaml_path = '/tmp/fake-yaml-path'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: tmp_yaml_path)
    # Patch make_deploy_variables.
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-east-1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    config_dict = {
        'aws': {
            'use_internal_ips': True
        },
    }
    config_dict = config_utils.Config.from_dict(config_dict)

    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda *args, **kwargs: config_dict)

    use_internal_ips = skypilot_config.get_effective_region_config(
        cloud=str(AWS()).lower(),
        region='us-east-1',
        keys=('use_internal_ips',),
        default_value=False)
    loaded_config = skypilot_config._get_loaded_config()
    print(f'_get_loaded_config: {loaded_config}')
    assert use_internal_ips is True

    def fill_template_side_effect(*args, **kwargs):
        config_dict = args[1]
        print(config_dict)
        assert 'ssh_proxy_command' in config_dict
        assert "ssm" in config_dict['ssh_proxy_command']
        assert 'use_internal_ips' in config_dict
        assert config_dict['use_internal_ips'] is True
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)
    with pytest.raises(RuntimeError) as e:
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(cloud=AWS(),
                                             instance_type='c2.xlarge'),
            num_nodes=1,
            cluster_config_template='aws-ray.yml.j2',
            cluster_name='fake-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='fake-region'),
            zones=[Zone(name='fake-zone')])


def test_subnet_names_in_cluster_config(monkeypatch):
    """Test that subnet_names from config is passed through to the template."""
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    tmp_yaml_path = '/tmp/fake-yaml-path'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: tmp_yaml_path)
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-east-1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    config_dict = {
        'aws': {
            'subnet_names': ['my-subnet-1', 'my-subnet-2'],
        },
    }
    config_dict = config_utils.Config.from_dict(config_dict)

    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda *args, **kwargs: config_dict)

    subnet_names = skypilot_config.get_effective_region_config(
        cloud=str(AWS()).lower(),
        region='us-east-1',
        keys=('subnet_names',),
        default_value=None)
    assert subnet_names == ['my-subnet-1', 'my-subnet-2']

    def fill_template_side_effect(*args, **kwargs):
        template_vars = args[1]
        assert 'subnet_names' in template_vars
        assert template_vars['subnet_names'] == ['my-subnet-1', 'my-subnet-2']
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)
    with pytest.raises(RuntimeError):
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(cloud=AWS(),
                                             instance_type='c2.xlarge'),
            num_nodes=1,
            cluster_config_template='aws-ray.yml.j2',
            cluster_name='fake-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='fake-region'),
            zones=[Zone(name='fake-zone')])


def test_subnet_names_default_none_in_cluster_config(monkeypatch):
    """Test that subnet_names defaults to None when not configured."""
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    tmp_yaml_path = '/tmp/fake-yaml-path'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: tmp_yaml_path)
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-east-1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    config_dict = {
        'aws': {},
    }
    config_dict = config_utils.Config.from_dict(config_dict)

    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda *args, **kwargs: config_dict)

    def fill_template_side_effect(*args, **kwargs):
        template_vars = args[1]
        assert 'subnet_names' in template_vars
        assert template_vars['subnet_names'] is None
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)
    with pytest.raises(RuntimeError):
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(cloud=AWS(),
                                             instance_type='c2.xlarge'),
            num_nodes=1,
            cluster_config_template='aws-ray.yml.j2',
            cluster_name='fake-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='fake-region'),
            zones=[Zone(name='fake-zone')])


def test_ssm_explicit_default(monkeypatch):
    """Test that SSM is false if explicitly set to false even if
    use_internal_ips is true and ssh_proxy_command is not set.
    """
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    tmp_yaml_path = '/tmp/fake-yaml-path'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: tmp_yaml_path)
    # Patch make_deploy_variables.
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-east-1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    config_dict = {
        'aws': {
            'use_ssm': False,
            'use_internal_ips': True
        },
    }
    config_dict = config_utils.Config.from_dict(config_dict)

    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda *args, **kwargs: config_dict)

    use_internal_ips = skypilot_config.get_effective_region_config(
        cloud=str(AWS()).lower(),
        region='us-east-1',
        keys=('use_internal_ips',),
        default_value=False)
    loaded_config = skypilot_config._get_loaded_config()
    print(f'_get_loaded_config: {loaded_config}')
    assert use_internal_ips is True

    def fill_template_side_effect(*args, **kwargs):
        config_dict = args[1]
        print(config_dict)
        assert 'ssh_proxy_command' in config_dict
        assert config_dict['ssh_proxy_command'] is None
        assert 'use_internal_ips' in config_dict
        assert config_dict['use_internal_ips'] is True
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)
    with pytest.raises(RuntimeError) as e:
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(cloud=AWS(),
                                             instance_type='c2.xlarge'),
            num_nodes=1,
            cluster_config_template='aws-ray.yml.j2',
            cluster_name='fake-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='fake-region'),
            zones=[Zone(name='fake-zone')])


def test_subnet_names_multi_az_no_error(monkeypatch):
    """Test that subnet_names spanning multiple AZs does not raise MISMATCH.

    When user specifies subnets in us-east-1a and us-east-1b, and SkyPilot
    picks AZ us-east-1a, the us-east-1b subnet is filtered out. This is
    expected behavior, not a mismatch error.
    """
    vpc_id = 'test-vpc-id'
    region = 'us-east-1'

    mock_subnet_1a = MagicMock()
    mock_subnet_1a.vpc_id = vpc_id
    mock_subnet_1a.subnet_id = 'subnet-1a'
    mock_subnet_1a.state = 'available'
    mock_subnet_1a.availability_zone = 'us-east-1a'
    mock_subnet_1a.map_public_ip_on_launch = False

    mock_subnet_1b = MagicMock()
    mock_subnet_1b.vpc_id = vpc_id
    mock_subnet_1b.subnet_id = 'subnet-1b'
    mock_subnet_1b.state = 'available'
    mock_subnet_1b.availability_zone = 'us-east-1b'
    mock_subnet_1b.map_public_ip_on_launch = False

    filtered_subnets = [mock_subnet_1a, mock_subnet_1b]

    subnets_mock = MagicMock()
    subnets_mock.all.return_value = filtered_subnets
    subnets_mock.filter.return_value = filtered_subnets

    mock_ec2 = MagicMock()
    mock_ec2.subnets = subnets_mock

    monkeypatch.setattr('sky.provision.aws.config._is_subnet_public',
                        lambda *args, **kwargs: True)

    # Launch with AZ us-east-1a — should succeed with only subnet-1a
    result_subnets, result_vpc_id = config._get_subnet_and_vpc_id(
        ec2=mock_ec2,
        security_group_ids=None,
        region=region,
        availability_zone='us-east-1a',
        use_internal_ips=False,
        vpc_name=None,
        subnet_names=['subnet-1a-name', 'subnet-1b-name'])

    assert result_vpc_id == vpc_id
    # Only the subnet in the chosen AZ should remain
    assert len(result_subnets) == 1
    assert result_subnets[0].availability_zone == 'us-east-1a'


def _write_cluster_config_with_ssm(monkeypatch, aws_config):
    """Run write_cluster_config with the given aws config and capture the
    template vars passed to fill_template (aborting before file I/O)."""
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: '/tmp/fake-yaml-path')
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-east-1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        skypilot_config, '_get_loaded_config', lambda *args, **kwargs:
        config_utils.Config.from_dict({'aws': aws_config}))

    captured = {}

    def fill_template_side_effect(*args, **kwargs):
        captured.update(args[1])
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)
    with pytest.raises(RuntimeError):
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(cloud=AWS(),
                                             instance_type='c2.xlarge'),
            num_nodes=1,
            cluster_config_template='aws-ray.yml.j2',
            cluster_name='fake-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='fake-region'),
            zones=[Zone(name='fake-zone')])
    return captured


def test_ssm_profile_from_config(monkeypatch):
    """aws.ssm_profile takes precedence over the AWS_PROFILE env var."""
    monkeypatch.setenv('AWS_PROFILE', 'env-profile')
    template_vars = _write_cluster_config_with_ssm(monkeypatch, {
        'use_ssm': True,
        'ssm_profile': 'config-profile',
    })
    proxy_command = template_vars['ssh_proxy_command']
    assert '--profile config-profile' in proxy_command
    assert 'env-profile' not in proxy_command


def test_ssm_profile_falls_back_to_env(monkeypatch):
    """Without aws.ssm_profile, the AWS_PROFILE env var is used."""
    monkeypatch.setenv('AWS_PROFILE', 'env-profile')
    template_vars = _write_cluster_config_with_ssm(monkeypatch,
                                                   {'use_ssm': True})
    assert '--profile env-profile' in template_vars['ssh_proxy_command']


def test_ssm_no_profile(monkeypatch):
    """Without aws.ssm_profile or AWS_PROFILE, no --profile is passed."""
    monkeypatch.delenv('AWS_PROFILE', raising=False)
    template_vars = _write_cluster_config_with_ssm(monkeypatch,
                                                   {'use_ssm': True})
    proxy_command = template_vars['ssh_proxy_command']
    assert 'aws ssm start-session' in proxy_command
    assert '--profile' not in proxy_command


def test_ssm_adaptive_retry_exec_safe_wrapper(monkeypatch):
    """The SSM proxy command self-throttles via the CLI's adaptive retry
    mode using an env wrapper that stays a single exec-able command:
    OpenSSH runs ProxyCommand as `$SHELL -c "exec <cmd>"`, so an
    `export ...;` prefix kills every proxied SSH."""
    monkeypatch.delenv('AWS_PROFILE', raising=False)
    template_vars = _write_cluster_config_with_ssm(monkeypatch,
                                                   {'use_ssm': True})
    proxy_command = template_vars['ssh_proxy_command']
    assert proxy_command.startswith(
        'env AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=')
    assert 'aws ssm start-session' in proxy_command


def test_ssm_profile_with_shell_metacharacters_is_quoted(monkeypatch):
    """The profile is spliced into a shell-executed ProxyCommand; a value
    with metacharacters must stay data, not become shell syntax."""
    profile = 'pro file;touch /tmp/pwned'
    template_vars = _write_cluster_config_with_ssm(monkeypatch, {
        'use_ssm': True,
        'ssm_profile': profile,
    })
    proxy_command = template_vars['ssh_proxy_command']
    # The command is now wrapped as `env ... /bin/sh -c '<inner>'`; the
    # profile quoting must hold in the INNER command the shell actually
    # runs (one unquoting level down).
    inner = shlex.split(proxy_command)[-1]
    assert f'--profile {shlex.quote(profile)}' in inner
    assert f'--profile {profile}' not in inner


def _ssh_credentials_for_proxy_command(monkeypatch, proxy_command):
    monkeypatch.setattr(
        backend_utils.global_user_state, 'get_cluster_yaml_dict', lambda _: {
            'cluster_name': 'fake-cluster',
            'auth': {
                'ssh_user': 'ubuntu',
                'ssh_proxy_command': proxy_command,
            },
            'provider': {
                'module': 'sky.provision.aws'
            },
        })
    return backend_utils.ssh_credential_from_yaml('/fake/cluster.yaml')


def test_legacy_ssm_proxy_command_upgraded_on_read(monkeypatch):
    """Persisted SSM lookups gain retry wrapping and an empty-target guard."""
    legacy = ('aws ssm start-session --target "$(aws ec2 describe-instances '
              '--output text)" --region us-east-1 '
              '--document-name AWS-StartSSHSession --parameters portNumber=%p')
    credentials = _ssh_credentials_for_proxy_command(monkeypatch, legacy)
    upgraded = credentials['ssh_proxy_command']
    assert upgraded.startswith('env AWS_RETRY_MODE=adaptive')
    inner = shlex.split(upgraded)[-1]
    assert 'skypilot_ssm_target="$(aws ec2 describe-instances' in inner
    assert 'if [ -z "$skypilot_ssm_target" ]; then' in inner
    assert ('exec aws ssm start-session '
            '--target "$skypilot_ssm_target"') in inner


def test_current_ssm_proxy_command_not_double_prefixed(monkeypatch):
    inner = 'aws ssm start-session --target "i-123"'
    current = ('env AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12 '
               f'/bin/sh -c {shlex.quote(inner)}')
    credentials = _ssh_credentials_for_proxy_command(monkeypatch, current)
    assert credentials['ssh_proxy_command'] == current


def test_broken_export_prefixed_form_repaired(monkeypatch):
    # The prefix form shipped briefly and may be persisted in cluster
    # YAMLs; it dies under OpenSSH's exec wrapping and must be rewritten,
    # not passed through.
    inner = 'aws ssm start-session --target "i-123"'
    broken = f'export AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12; {inner}'
    credentials = _ssh_credentials_for_proxy_command(monkeypatch, broken)
    repaired = credentials['ssh_proxy_command']
    assert repaired.startswith('env AWS_RETRY_MODE=adaptive')
    assert shlex.quote(inner) in repaired


def test_custom_proxy_command_left_untouched(monkeypatch):
    custom = 'ssh -W %h:%p bastion.example.com'
    credentials = _ssh_credentials_for_proxy_command(monkeypatch, custom)
    assert credentials['ssh_proxy_command'] == custom


def test_legacy_ssm_upgraded_in_credentials_from_handles(monkeypatch):
    """ssh_credentials_from_handles reads the auth section on its own path
    (not via ssh_credential_from_yaml) and must apply the same upgrade."""
    legacy = ('aws ssm start-session --target "i-123" --region us-east-1 '
              '--document-name AWS-StartSSHSession --parameters portNumber=%p')
    config = {
        'cluster_name': 'fake-cluster',
        'auth': {
            'ssh_user': 'ubuntu',
            'ssh_proxy_command': legacy,
        },
        'provider': {
            'module': 'sky.provision.aws'
        },
    }
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_yaml_dict_multiple',
                        lambda paths: [config for _ in paths])
    handle = MagicMock()
    handle.cluster_yaml = '/fake/cluster.yaml'
    handle.ssh_user = None
    handle.docker_user = None
    credentials, = backend_utils.ssh_credentials_from_handles([handle])
    upgraded = credentials['ssh_proxy_command']
    assert upgraded.startswith('env AWS_RETRY_MODE=adaptive')
    assert shlex.quote(legacy) in upgraded
