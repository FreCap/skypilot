"""Test the AWS class."""

import json
import shlex
import unittest.mock as mock

import pytest

from sky.clouds import aws as aws_mod
from sky.provision import constants as provision_constants
from sky.utils import resources_utils


def _assert_managed_image_tag_specifications(command: str) -> None:
    args = shlex.split(command)
    tag_specifications = json.loads(args[args.index('--tag-specifications') +
                                         1])
    assert tag_specifications == [{
        'ResourceType': resource_type,
        'Tags': [{
            'Key': provision_constants.TAG_SKYPILOT_MANAGED,
            'Value': provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
        }],
    } for resource_type in ('image', 'snapshot')]


class TestManagedImageTags:
    """Tests managed tagging of AMIs and their backing snapshots."""

    def test_create_image_tags_ami_and_snapshots(self):
        commands = []

        def run_with_retries(command, **kwargs):
            del kwargs
            commands.append(command)
            if ' create-image ' in command:
                return 0, 'ami-new\n', ''
            return 0, '', ''

        cluster_name = resources_utils.ClusterName('display', 'on-cloud')
        with mock.patch.object(aws_mod.provision_lib,
                               'query_instances',
                               return_value={'i-source': object()}), \
             mock.patch.object(aws_mod.subprocess_utils,
                               'run_with_retries',
                               side_effect=run_with_retries), \
             mock.patch.object(aws_mod.subprocess_utils,
                               'handle_returncode'), \
             mock.patch.object(aws_mod.rich_utils, 'force_update_status'), \
             mock.patch.object(aws_mod.sky_logging, 'print'):
            image_id = aws_mod.AWS.create_image_from_cluster(
                cluster_name, 'us-west-2', None)

        assert image_id == 'ami-new'
        _assert_managed_image_tag_specifications(commands[0])

    def test_copy_image_tags_ami_and_snapshots(self):
        commands = []

        def run_with_retries(command, **kwargs):
            del kwargs
            commands.append(command)
            if ' copy-image ' in command:
                return 0, 'ami-copy\n', ''
            return 0, '', ''

        with mock.patch.object(aws_mod.subprocess_utils,
                               'run_with_retries',
                               side_effect=run_with_retries), \
             mock.patch.object(aws_mod.subprocess_utils,
                               'handle_returncode'), \
             mock.patch.object(aws_mod.rich_utils, 'force_update_status'), \
             mock.patch.object(aws_mod.sky_logging, 'print'), \
             mock.patch.object(aws_mod.AWS, 'delete_image'):
            image_id = aws_mod.AWS.maybe_move_image('ami-source', 'us-east-1',
                                                    'us-west-2', None, None)

        assert image_id == 'ami-copy'
        _assert_managed_image_tag_specifications(commands[0])


class TestGetImageRootDeviceName:

    @pytest.fixture(autouse=True)
    def reset_logger(self):
        # Ensure logger is available
        yield

    def test_skypilot_image_returns_default(self):
        result = aws_mod.AWS.get_image_root_device_name('skypilot:ubuntu-22.04',
                                                        'us-east-1')
        assert result == aws_mod.DEFAULT_ROOT_DEVICE_NAME

    def test_missing_region_assertion(self):
        with pytest.raises(AssertionError):
            aws_mod.AWS.get_image_root_device_name('ami-0123456789abcdef0',
                                                   None)

    @mock.patch.object(aws_mod, 'aws')
    def test_returns_root_device_name_when_present(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [{
                'ImageId': 'ami-0123456789abcdef0',
                'RootDeviceName': '/dev/xvda'
            }],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        result = aws_mod.AWS.get_image_root_device_name('ami-0123456789abcdef0',
                                                        'us-west-2')
        assert result == '/dev/xvda'
        mock_aws.client.assert_called_with('ec2', region_name='us-west-2')

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_missing_root_device_name_debugs_and_returns_default(
            self, mock_aws, mock_logger):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [{
                'ImageId': 'ami-0123456789abcdef1',
                # No 'RootDeviceName'
            }],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        result = aws_mod.AWS.get_image_root_device_name('ami-0123456789abcdef1',
                                                        'us-west-2')
        assert result == aws_mod.DEFAULT_ROOT_DEVICE_NAME
        assert mock_logger.debug.called

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_no_credentials_fallback_default(self, mock_aws, mock_logger):
        # Simulate NoCredentialsError
        class DummyExc(Exception):
            pass

        # Build a dummy exceptions namespace matching access pattern
        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.NoCredentialsError(
        )

        result = aws_mod.AWS.get_image_root_device_name('ami-0123456789abcdef2',
                                                        'eu-central-1')
        assert result == aws_mod.DEFAULT_ROOT_DEVICE_NAME
        assert mock_logger.debug.called
        # Verify the debug log message contains expected content
        debug_call_args = mock_logger.debug.call_args[0][0]
        assert 'Failed to get image root device name' in debug_call_args
        assert 'ami-0123456789abcdef2' in debug_call_args
        assert 'eu-central-1' in debug_call_args

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_client_error_raises_value_error(self, mock_aws, mock_logger):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):

                def __init__(self, message):
                    self.message = message
                    super().__init__(message)

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.ClientError(
            'Image not found')

        with pytest.raises(ValueError) as ei:
            aws_mod.AWS.get_image_root_device_name('ami-0deadbeef',
                                                   'ap-south-1')
        assert 'not found' in str(ei.value)
        # Verify the debug log was called
        assert mock_logger.debug.called
        debug_call_args = mock_logger.debug.call_args_list[0][0][0]
        assert 'Failed to get image root device name' in debug_call_args
        assert 'ami-0deadbeef' in debug_call_args
        assert 'ap-south-1' in debug_call_args

    @mock.patch.object(aws_mod, 'aws')
    def test_image_not_found_raises_value_error(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }

        # Ensure botocore_exceptions returns real exception classes (not mocks)
        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        with pytest.raises(ValueError) as ei:
            aws_mod.AWS.get_image_root_device_name('ami-00000000', 'us-east-2')
        assert 'Image' in str(ei.value)


class TestGetImageSize:

    def test_skypilot_image_returns_default(self):
        result = aws_mod.AWS.get_image_size('skypilot:ubuntu-22.04',
                                            'us-east-1')
        assert result == aws_mod.DEFAULT_AMI_GB

    def test_missing_region_assertion(self):
        with pytest.raises(AssertionError):
            aws_mod.AWS.get_image_size('ami-0123456789abcdef0', None)

    @mock.patch.object(aws_mod, 'aws')
    def test_returns_image_size_when_found(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [{
                'ImageId': 'ami-0123456789abcdef0',
                'BlockDeviceMappings': [{
                    'Ebs': {
                        'VolumeSize': 100
                    }
                }]
            }],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        result = aws_mod.AWS.get_image_size('ami-0123456789abcdef0',
                                            'us-west-2')
        assert result == 100
        mock_aws.client.assert_called_with('ec2', region_name='us-west-2')

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_no_credentials_fallback_default(self, mock_aws, mock_logger):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.NoCredentialsError(
            'No credentials')

        result = aws_mod.AWS.get_image_size('ami-0123456789abcdef1',
                                            'eu-central-1')
        assert result == aws_mod.DEFAULT_AMI_GB
        # Verify the debug log was called
        assert mock_logger.debug.called
        debug_call_args = mock_logger.debug.call_args[0][0]
        assert 'Failed to get image size' in debug_call_args
        assert 'ami-0123456789abcdef1' in debug_call_args
        assert 'eu-central-1' in debug_call_args

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_profile_not_found_fallback_default(self, mock_aws, mock_logger):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.ProfileNotFound(
            'Profile not found')

        result = aws_mod.AWS.get_image_size('ami-0123456789abcdef2',
                                            'ap-south-1')
        assert result == aws_mod.DEFAULT_AMI_GB
        # Verify the debug log was called
        assert mock_logger.debug.called
        debug_call_args = mock_logger.debug.call_args[0][0]
        assert 'Failed to get image size' in debug_call_args
        assert 'ami-0123456789abcdef2' in debug_call_args
        assert 'ap-south-1' in debug_call_args

    @mock.patch.object(aws_mod, 'logger')
    @mock.patch.object(aws_mod, 'aws')
    def test_client_error_raises_value_error(self, mock_aws, mock_logger):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):

                def __init__(self, message):
                    self.message = message
                    super().__init__(message)

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.ClientError(
            'Image not found')

        with pytest.raises(ValueError) as ei:
            aws_mod.AWS.get_image_size('ami-0deadbeef', 'us-east-1')
        assert 'not found' in str(ei.value)
        # Verify the debug log was called
        assert mock_logger.debug.called
        debug_call_args = mock_logger.debug.call_args_list[0][0][0]
        assert 'Failed to get image size' in debug_call_args
        assert 'ami-0deadbeef' in debug_call_args
        assert 'us-east-1' in debug_call_args

    @mock.patch.object(aws_mod, 'aws')
    def test_image_not_found_raises_value_error(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }

        # Ensure botocore_exceptions returns real exception classes (not mocks)
        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions

        with pytest.raises(ValueError) as ei:
            aws_mod.AWS.get_image_size('ami-00000000', 'us-east-2')
        assert 'Image' in str(ei.value)


class TestEfaHelpers:

    def test_is_efa_instance_type(self):
        # True for EFA families
        assert aws_mod._is_efa_instance_type('g6.12xlarge') is True
        assert aws_mod._is_efa_instance_type('p5.48xlarge') is True
        assert aws_mod._is_efa_instance_type('p6-b200.24xlarge') is True
        # False for non-EFA families
        assert aws_mod._is_efa_instance_type('c5.2xlarge') is False
        assert aws_mod._is_efa_instance_type('t3.micro') is False

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_returns_latest_available(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [
                {
                    'ImageId': 'ami-old',
                    'State': 'available',
                    'CreationDate': '2024-01-01T00:00:00.000Z',
                },
                {
                    'ImageId': 'ami-new',
                    'State': 'available',
                    'CreationDate': '2024-06-01T00:00:00.000Z',
                },
                {
                    'ImageId': 'ami-pending',
                    'State': 'pending',
                    'CreationDate': '2024-07-01T00:00:00.000Z',
                },
            ]
        }
        result = aws_mod._get_efa_image_id('us-west-2')
        assert result == 'ami-new'
        mock_aws.client.assert_called_with('ec2', region_name='us-west-2')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_no_credentials_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.NoCredentialsError(
        )

        with pytest.raises(ValueError):
            aws_mod._get_efa_image_id('us-east-1')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_client_error_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.ClientError()

        with pytest.raises(ValueError):
            aws_mod._get_efa_image_id('eu-central-1')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_non_efa_type_short_circuit(self, mock_aws):
        # Should return 0 and not call aws.client at all
        result = aws_mod._get_max_efa_interfaces('c5.2xlarge', 'us-east-1')
        assert result == 0
        mock_aws.client.assert_not_called()

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_success(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.return_value = {
            'InstanceTypes': [{
                'NetworkInfo': {
                    'EfaInfo': {
                        'MaximumEfaInterfaces': 8,
                    }
                }
            }]
        }
        result = aws_mod._get_max_efa_interfaces('g6.8xlarge', 'us-west-2')
        assert result == 8
        mock_aws.client.assert_called_with('ec2', region_name='us-west-2')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_no_creds_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.side_effect = DummyExceptions.NoCredentialsError(
        )

        with pytest.raises(ValueError):
            aws_mod._get_max_efa_interfaces('g6.12xlarge', 'ap-south-1')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_client_error_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.side_effect = DummyExceptions.ClientError(
        )

        with pytest.raises(ValueError):
            aws_mod._get_max_efa_interfaces('g6.12xlarge', 'eu-west-1')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_missing_images_key_returns_none(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        assert aws_mod._get_efa_image_id('us-east-1') is None

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_empty_images_returns_none(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        assert aws_mod._get_efa_image_id('us-east-2') is None

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_no_available_returns_none(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.return_value = {
            'Images': [
                {
                    'ImageId': 'ami-1',
                    'State': 'pending',
                    'CreationDate': '2024-01-01T00:00:00.000Z',
                },
                {
                    'ImageId': 'ami-2',
                    'State': 'deregistered',
                    'CreationDate': '2024-02-01T00:00:00.000Z',
                },
            ],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
                'NextToken': None,
            },
        }
        assert aws_mod._get_efa_image_id('us-west-1') is None

    @mock.patch.object(aws_mod, 'aws')
    def test_get_efa_image_id_profile_not_found_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_images.side_effect = DummyExceptions.ProfileNotFound()
        with pytest.raises(ValueError):
            aws_mod._get_efa_image_id('ap-northeast-1')

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_missing_instance_types_returns_zero(
            self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.return_value = {}
        assert aws_mod._get_max_efa_interfaces('g6.12xlarge', 'us-east-1') == 0

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_empty_instance_types_returns_zero(
            self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.return_value = {'InstanceTypes': []}
        assert aws_mod._get_max_efa_interfaces('g6.24xlarge', 'us-west-1') == 0

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_no_efa_info_returns_zero(self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.return_value = {
            'InstanceTypes': [{
                'NetworkInfo': {}
            }]
        }
        assert aws_mod._get_max_efa_interfaces('g6.48xlarge', 'us-west-2') == 0

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_missing_max_field_returns_zero(
            self, mock_aws):
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.return_value = {
            'InstanceTypes': [{
                'NetworkInfo': {
                    'EfaInfo': {}
                }
            }]
        }
        assert aws_mod._get_max_efa_interfaces('g6.48xlarge',
                                               'eu-central-1') == 0

    @mock.patch.object(aws_mod, 'aws')
    def test_get_max_efa_interfaces_profile_not_found_raises(self, mock_aws):

        class DummyExceptions:

            class NoCredentialsError(Exception):
                pass

            class ProfileNotFound(Exception):
                pass

            class ClientError(Exception):
                pass

        mock_aws.botocore_exceptions.return_value = DummyExceptions
        client = mock.Mock()
        mock_aws.client.return_value = client
        client.describe_instance_types.side_effect = DummyExceptions.ProfileNotFound(
        )
        with pytest.raises(ValueError):
            aws_mod._get_max_efa_interfaces('g6.12xlarge', 'ap-northeast-1')


class TestAwsLoginIdentityDetection:
    """Detection of the `aws login` credential provider (2026 AWS CLI command)."""

    @mock.patch('sky.adaptors.aws.get_workspace_profile')
    @mock.patch('subprocess.run')
    def test_aws_login_detected_as_LOGIN_identity_type(self, mock_run,
                                                       mock_get_profile):
        """`aws configure list` Type column 'login' → AWSIdentityType.LOGIN."""
        configure_list_output = (
            b'      Name                    Value             Type    Location\n'
            b'      ----                    -----             ----    --------\n'
            b'   profile                <not set>             None    None\n'
            b'access_key     ****************abcd            login\n'
            b'secret_key     ****************abcd            login\n'
            b'    region                ap-northeast-1      config-file    ~/.aws/config\n'
        )
        mock_run.return_value = mock.Mock(returncode=0,
                                          stdout=configure_list_output)
        mock_get_profile.return_value = None
        aws_mod.AWS._aws_configure_list.cache_clear()

        identity_type = aws_mod.AWS._current_identity_type()
        assert identity_type == aws_mod.AWSIdentityType.LOGIN

    def test_login_credentials_do_not_auto_expire(self):
        """LOGIN auto-rotates via refresh token, so should not be in expirable set."""
        assert not aws_mod.AWSIdentityType.LOGIN.can_credential_expire()


class TestAwsConfigureList:

    @mock.patch('sky.adaptors.aws.get_workspace_profile')
    @mock.patch('subprocess.run')
    def test_missing_cli_returns_none(self, mock_run, mock_get_profile):
        """Missing AWS CLI preserves the identity fallback contract."""
        mock_get_profile.return_value = None
        mock_run.side_effect = FileNotFoundError
        aws_mod.AWS._aws_configure_list.cache_clear()

        assert aws_mod.AWS._aws_configure_list() is None

    @mock.patch('sky.adaptors.aws.get_workspace_profile')
    @mock.patch('subprocess.run')
    def test_profile_name_passed_as_single_argument(self, mock_run,
                                                    mock_get_profile):
        """Profile names with spaces and quotes remain one CLI argument."""
        mock_run.return_value = mock.Mock(returncode=0, stdout=b'output')
        profile = 'research team\'s "gpu" account'
        mock_get_profile.return_value = profile
        aws_mod.AWS._aws_configure_list.cache_clear()

        aws_mod.AWS._aws_configure_list()

        assert mock_run.call_args.args[0] == [
            'aws', 'configure', 'list', '--profile', profile
        ]
        assert not mock_run.call_args.kwargs.get('shell', False)

    @mock.patch('sky.adaptors.aws.get_workspace_profile')
    @mock.patch('subprocess.run')
    def test_command_generation_with_profiles(self, mock_run, mock_get_profile):
        """Test command generation with no profile, with profile, and different profiles."""
        mock_result = mock.Mock()
        mock_result.returncode = 0
        mock_result.stdout = b'output'
        mock_run.return_value = mock_result

        aws_mod.AWS._aws_configure_list.cache_clear()

        # Test with no profile
        mock_get_profile.return_value = None
        aws_mod.AWS._aws_configure_list()
        assert mock_run.call_args[0][0] == ['aws', 'configure', 'list']

        # Test with profile
        mock_get_profile.return_value = 'dev'
        aws_mod.AWS._aws_configure_list()
        assert mock_run.call_args[0][0] == [
            'aws', 'configure', 'list', '--profile', 'dev'
        ]

        # Test with different profiles
        mock_get_profile.return_value = 'profile1'
        aws_mod.AWS._aws_configure_list()
        assert mock_run.call_args[0][0] == [
            'aws', 'configure', 'list', '--profile', 'profile1'
        ]

    @mock.patch('sky.adaptors.aws.get_workspace_profile')
    @mock.patch('subprocess.run')
    def test_caching_behavior(self, mock_run, mock_get_profile):
        """Test caching: same profile cached, different profiles not cached."""
        mock_result = mock.Mock()
        mock_result.returncode = 0
        mock_result.stdout = b'output'
        mock_run.return_value = mock_result

        aws_mod.AWS._aws_configure_list.cache_clear()

        # Same profile should be cached
        mock_get_profile.return_value = 'test'
        aws_mod.AWS._aws_configure_list()
        aws_mod.AWS._aws_configure_list()
        assert mock_run.call_count == 1

        # Different profiles should NOT be cached together
        mock_get_profile.return_value = 'other'
        aws_mod.AWS._aws_configure_list()
        assert mock_run.call_count == 2


class TestAwsProfileAwareLruCache:
    """Tests for aws_profile_aware_lru_cache decorator."""

    def test_cache_distinguishes_by_aws_profile(self):
        """Test that cache differentiates between different AWS profiles."""
        import os

        from sky import skypilot_config
        from sky.clouds.aws import aws_profile_aware_lru_cache

        call_count = 0

        @aws_profile_aware_lru_cache(scope='request', maxsize=5)
        def expensive_func(cls):
            nonlocal call_count
            call_count += 1
            return f"result_{call_count}"

        expensive_func.cache_clear()

        # Set config file with multiple workspaces and profiles
        old_config_path = os.environ.get(
            skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
        try:
            os.environ[skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = \
                './tests/test_yamls/test_aws_profile_workspace_config.yaml'
            skypilot_config.reload_config()

            # Call with workspace-a
            with skypilot_config.local_active_workspace_ctx('workspace-a'):
                result = expensive_func(None)
                assert result == 'result_1'
                assert call_count == 1

                # Same workspace should use cache
                result = expensive_func(None)
                assert result == 'result_1'
                assert call_count == 1

            # Call with workspace-b
            with skypilot_config.local_active_workspace_ctx('workspace-b'):
                result = expensive_func(None)
                assert result == 'result_2'
                assert call_count == 2  # Called again for different workspace

                # Same workspace (workspace-b) should use cache
                result = expensive_func(None)
                assert result == 'result_2'
                assert call_count == 2

            with skypilot_config.local_active_workspace_ctx('workspace-a'):
                # Back to workspace-a should use its cached result
                result = expensive_func(None)
                assert result == 'result_1'
                assert call_count == 2

                # Clear cache
                expensive_func.cache_clear()
                result = expensive_func(None)
                assert result == 'result_3'
                assert call_count == 3

                result = expensive_func(None)
                assert result == 'result_3'
                assert call_count == 3
        finally:
            # Restore original config
            if old_config_path:
                os.environ[
                    skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = old_config_path
            else:
                os.environ.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
            skypilot_config.reload_config()


class TestAwsConfigFileEnvVar:
    """Tests for AWS_CONFIG_FILE credential override."""

    def test_get_credential_file_mounts_respects_env_override(
            self, tmp_path, monkeypatch):
        credential_file = tmp_path / 'aws_credentials'
        credential_file.write_text('dummy')
        monkeypatch.setenv('AWS_CONFIG_FILE', str(credential_file))

        aws = aws_mod.AWS()
        with mock.patch.object(
                aws_mod.AWS,
                '_current_identity_type',
                return_value=aws_mod.AWSIdentityType.SHARED_CREDENTIALS_FILE):
            mounts = aws.get_credential_file_mounts()

        assert mounts == {
            aws_mod._DEFAULT_AWS_CONFIG_PATH: str(credential_file)
        }


class TestGetDefaultAmi:
    """Tests for AWS default-image selection and regional eligibility."""

    def _patch(self, monkeypatch, acc, arch):
        # Echo the requested tag back so we can assert which image was chosen.
        monkeypatch.setattr(aws_mod.catalog, 'get_image_id_from_tag',
                            lambda tag, region, clouds: tag)
        monkeypatch.setattr(aws_mod.AWS, 'get_accelerators_from_instance_type',
                            classmethod(lambda cls, instance_type: acc))
        monkeypatch.setattr(aws_mod.AWS, 'get_arch_from_instance_type',
                            classmethod(lambda cls, instance_type: arch))

    def test_turing_gpu_uses_cuda13_default(self, monkeypatch):
        self._patch(monkeypatch, {'T4': 1}, 'x86_64')
        result = aws_mod.AWS._get_default_ami('us-east-1', 'g4dn.xlarge')
        assert result == aws_mod._DEFAULT_GPU_IMAGE_ID

    @pytest.mark.parametrize('acc_name', ['V100', 'M60'])
    def test_pre_turing_gpu_uses_legacy_cuda12(self, monkeypatch, acc_name):
        self._patch(monkeypatch, {acc_name: 1}, 'x86_64')
        result = aws_mod.AWS._get_default_ami('us-east-1', 'p3.2xlarge')
        assert result == aws_mod._DEFAULT_GPU_CUDA12_IMAGE_ID

    def test_k80_uses_k80_image(self, monkeypatch):
        self._patch(monkeypatch, {'K80': 1}, 'x86_64')
        result = aws_mod.AWS._get_default_ami('us-east-1', 'p2.xlarge')
        assert result == aws_mod._DEFAULT_GPU_K80_IMAGE_ID

    def test_arm64_gpu_uses_cuda13_arm64(self, monkeypatch):
        # All arm64 GPUs (e.g. GH200) are Turing+, so they use the default
        # cuda13 arm64 image.
        self._patch(monkeypatch, {'GH200': 1}, 'arm64')
        result = aws_mod.AWS._get_default_ami('us-east-1', 'g5g.xlarge')
        assert result == aws_mod._DEFAULT_GPU_ARM64_IMAGE_ID

    @pytest.mark.parametrize(('accelerators', 'arch', 'expected_tag'), [
        (None, 'x86_64', aws_mod._DEFAULT_CPU_IMAGE_ID),
        (None, 'arm64', aws_mod._DEFAULT_CPU_ARM64_IMAGE_ID),
        ({
            'Trainium2': 1
        }, 'x86_64', aws_mod._DEFAULT_NEURON_IMAGE_ID),
        ({
            'Inferentia2': 1
        }, 'x86_64', aws_mod._DEFAULT_NEURON_IMAGE_ID),
    ])
    def test_cpu_and_neuron_default_tags(self, monkeypatch, accelerators, arch,
                                         expected_tag):
        self._patch(monkeypatch, accelerators, arch)

        result = aws_mod.AWS._get_default_image_tag('instance-type')

        assert result == expected_tag

    def test_regions_without_required_default_image_are_excluded(
            self, monkeypatch):
        self._patch(monkeypatch, {'L4': 1}, 'x86_64')
        regions = [
            aws_mod.clouds.Region('qualified'),
            aws_mod.clouds.Region('missing'),
        ]
        monkeypatch.setattr(aws_mod.catalog,
                            'get_region_zones_for_instance_type',
                            lambda *args: regions)
        checked = []

        def _get_image_id_from_tag(tag, region, clouds):
            checked.append((tag, region, clouds))
            if region == 'qualified':
                return 'ami-0123456789abcdef0'
            return None

        monkeypatch.setattr(aws_mod.catalog, 'get_image_id_from_tag',
                            _get_image_id_from_tag)
        resources = mock.Mock()
        resources.get_cloud_image_id.return_value = None
        resources.network_tier = None

        result = aws_mod.AWS.regions_with_offering('g6.xlarge', {'L4': 1}, True,
                                                   None, None, resources)

        assert [region.name for region in result] == ['qualified']
        assert checked == [
            (aws_mod._DEFAULT_GPU_IMAGE_ID, 'qualified', 'aws'),
            (aws_mod._DEFAULT_GPU_IMAGE_ID, 'missing', 'aws'),
        ]

    def test_explicit_cloud_image_bypasses_default_image_eligibility(
            self, monkeypatch):
        regions = [aws_mod.clouds.Region('custom-image-region')]
        monkeypatch.setattr(aws_mod.catalog,
                            'get_region_zones_for_instance_type',
                            lambda *args: regions)
        image_check = mock.Mock(
            side_effect=AssertionError('default image must not be checked'))
        monkeypatch.setattr(aws_mod.catalog, 'get_image_id_from_tag',
                            image_check)
        resources = mock.Mock()
        resources.get_cloud_image_id.return_value = {
            None: 'ami-customer-supplied'
        }
        resources.network_tier = None

        result = aws_mod.AWS.regions_with_offering('g6.xlarge', {'L4': 1}, True,
                                                   None, None, resources)

        assert [region.name for region in result] == ['custom-image-region']
        image_check.assert_not_called()

    @pytest.mark.parametrize('resources_kind', ['efa', 'provisioning'])
    def test_non_default_image_paths_preserve_vm_offerings(
            self, monkeypatch, resources_kind):
        regions = [aws_mod.clouds.Region('offered')]
        monkeypatch.setattr(aws_mod.catalog,
                            'get_region_zones_for_instance_type',
                            lambda *args: regions)
        image_check = mock.Mock(
            side_effect=AssertionError('default image must not be checked'))
        monkeypatch.setattr(aws_mod.catalog, 'get_image_id_from_tag',
                            image_check)
        if resources_kind == 'efa':
            resources = mock.Mock()
            resources.get_cloud_image_id.return_value = None
            resources.network_tier = resources_utils.NetworkTier.BEST
        else:
            # zones_provision_loop() deliberately calls without Resources: the
            # selected region was already qualified during planning.
            resources = None

        result = aws_mod.AWS.regions_with_offering('g6.xlarge', {'L4': 1}, True,
                                                   None, None, resources)

        assert [region.name for region in result] == ['offered']
        image_check.assert_not_called()
