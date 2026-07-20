"""AWS default IAM instance-profile lifecycle."""

import json
import time
from typing import Any

import colorama

from sky import sky_logging
from sky.adaptors import aws
from sky.provision.aws import utils

# Keep the historical logger name so extracting this helper does not alter
# user-visible log prefixes.
logger = sky_logging.init_logger('sky.provision.aws.config')

SKYPILOT = 'skypilot'
DEFAULT_SKYPILOT_INSTANCE_PROFILE = SKYPILOT + '-v1'
DEFAULT_SKYPILOT_IAM_ROLE = SKYPILOT + '-v1'


def configure_iam_role(iam) -> dict[str, Any]:

    def _get_instance_profile(profile_name: str):
        profile = iam.InstanceProfile(profile_name)
        try:
            profile.load()
            return profile
        except aws.botocore_exceptions().ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == 'NoSuchEntity':
                return None
            else:
                utils.handle_boto_error(
                    exc, 'Failed to fetch IAM instance profile data for '
                    f'{colorama.Style.BRIGHT}{profile_name}'
                    f'{colorama.Style.RESET_ALL} from AWS.')
                raise exc

    def _get_role(role_name: str):
        role = iam.Role(role_name)
        try:
            role.load()
            return role
        except aws.botocore_exceptions().ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == 'NoSuchEntity':
                return None
            else:
                utils.handle_boto_error(
                    exc,
                    f'Failed to fetch IAM role data for {colorama.Style.BRIGHT}'
                    f'{role_name}{colorama.Style.RESET_ALL} from AWS.')
                raise exc

    instance_profile_name = DEFAULT_SKYPILOT_INSTANCE_PROFILE
    profile = _get_instance_profile(instance_profile_name)

    if profile is None:
        logger.info(
            f'Creating new IAM instance profile {colorama.Style.BRIGHT}'
            f'{instance_profile_name}{colorama.Style.RESET_ALL} for use as the '
            'default.')
        iam.meta.client.create_instance_profile(
            InstanceProfileName=instance_profile_name)
        profile = _get_instance_profile(instance_profile_name)
        time.sleep(15)  # wait for propagation
    assert profile is not None, 'Failed to create instance profile'

    if not profile.roles:
        role_name = DEFAULT_SKYPILOT_IAM_ROLE
        role = _get_role(role_name)
        if role is None:
            logger.info(
                f'Creating new IAM role {colorama.Style.BRIGHT}{role_name}'
                f'{colorama.Style.RESET_ALL} for use as the default instance '
                'role.')
            policy_doc = {
                'Statement': [{
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'ec2.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole',
                }]
            }
            attach_policy_arns = [
                'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess',
            ]

            iam.create_role(RoleName=role_name,
                            AssumeRolePolicyDocument=json.dumps(policy_doc))
            role = _get_role(role_name)
            assert role is not None, 'Failed to create role'

            for policy_arn in attach_policy_arns:
                role.attach_policy(PolicyArn=policy_arn)

            # SkyPilot: 'PassRole' is required by the controllers (jobs and
            # services) created with `aws.remote_identity: SERVICE_ACCOUNT` to
            # create instances with the IAM role.
            skypilot_pass_role_policy_doc = {
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Action': [
                            'iam:GetRole',
                            'iam:PassRole',
                        ],
                        'Resource': role.arn,
                    },
                    {
                        'Effect': 'Allow',
                        'Action': 'iam:GetInstanceProfile',
                        'Resource': profile.arn,
                    },
                ]
            }
            role.Policy('SkyPilotPassRolePolicy').put(
                PolicyDocument=json.dumps(skypilot_pass_role_policy_doc))

        profile.add_role(RoleName=role.name)
        time.sleep(15)  # wait for propagation
    return {'Arn': profile.arn}
