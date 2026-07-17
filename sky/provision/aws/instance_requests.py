"""AWS instance request construction and capacity retry policy."""

from collections.abc import Callable
import logging
import time
from typing import Any, TypeVar

from sky import sky_logging
from sky.adaptors import aws
from sky.provision import common
from sky.provision import constants
from sky.provision.aws import utils
from sky.utils import common_utils

logger = sky_logging.init_logger('sky.provision.aws.instance')

_T = TypeVar('_T')

# Max retries for creating an instance.
BOTO_CREATE_MAX_RETRIES = 5

# These RunInstances failures cannot be repaired by immediately retrying the
# same request. Quota errors are regional. InsufficientInstanceCapacity is safe
# to fast-fail only when the request is known to target one availability zone.
_INSUFFICIENT_CAPACITY_ERROR_CODE = 'InsufficientInstanceCapacity'
_REGIONAL_QUOTA_ERROR_CODES = frozenset({
    'VcpuLimitExceeded',
    'MaxSpotInstanceCountExceeded',
    'InstanceLimitExceeded',
})


def _ec2_call_with_retry_on_server_error(ec2_fail_fast_fn: Callable[..., _T],
                                         log_level=logging.DEBUG,
                                         **kwargs) -> _T:
    # Here we have to handle 'RequestLimitExceeded' error, so the provision
    # would not fail due to request limit issues.
    # Here the backoff config (5, 12) is picked at random and does not
    # have any special meaning.
    backoff = common_utils.Backoff(initial_backoff=5, max_backoff_factor=12)
    ret = None
    for _ in range(utils.BOTO_MAX_RETRIES):
        try:
            ret = ec2_fail_fast_fn(**kwargs)
            break
        except aws.botocore_exceptions().ClientError as e:
            # Retry server side errors, as they are likely to be transient.
            # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html#api-error-codes-table-server # pylint: disable=line-too-long
            error_code = e.response['Error']['Code']
            if error_code in [
                    'RequestLimitExceeded', 'ServerInternal',
                    'ServiceUnavailable', 'InternalError', 'Unavailable'
            ]:
                time.sleep(backoff.current_backoff())
                logger.debug(f'create_instances: {error_code}, retrying.')
                continue
            logger.log(log_level, f'create_instances: Attempt failed with {e}')
            raise
    if ret is None:
        raise RuntimeError(
            f'Failed to call ec2 function {ec2_fail_fast_fn} due to '
            'RequestLimitExceeded. Max attempts exceeded.')
    return ret


def _format_tags(tags: dict[str, str]) -> list:
    return [{'Key': k, 'Value': v} for k, v in tags.items()]


def _merge_tag_specs(tag_specs: list[dict[str, Any]],
                     user_tag_specs: list[dict[str, Any]]) -> None:
    """Merges user-provided node config tag specifications into a base
    list of node provider tag specifications. The base list of
    node provider tag specs is modified in-place.

    This allows users to add tags and override values of existing
    tags with their own, and only applies to the resource type
    'instance'. All other resource types are appended to the list of
    tag specs.

    Args:
        tag_specs (List[Dict[str, Any]]): base node provider tag specs
        user_tag_specs (List[Dict[str, Any]]): user's node config tag specs
    """

    for user_tag_spec in user_tag_specs:
        if user_tag_spec['ResourceType'] == 'instance':
            for user_tag in user_tag_spec['Tags']:
                exists = False
                for tag in tag_specs[0]['Tags']:
                    if user_tag['Key'] == tag['Key']:
                        exists = True
                        tag['Value'] = user_tag['Value']
                        break
                if not exists:
                    tag_specs[0]['Tags'] += [user_tag]
        else:
            tag_specs += [user_tag_spec]


def _is_single_zone_request(provider_config: dict[str, Any]) -> bool:
    """Whether the provisioner explicitly targets exactly one AWS zone."""
    availability_zone = provider_config.get('availability_zone')
    if not isinstance(availability_zone, str):
        return False
    configured_zones = [
        zone.strip() for zone in availability_zone.split(',') if zone.strip()
    ]
    return len(configured_zones) == 1


def create_instances(
    ec2_fail_fast,
    cluster_name: str,
    node_config: dict[str, Any],
    tags: dict[str, str],
    count: int,
    associate_public_ip_address: bool,
    max_efa_interfaces: int,
    is_single_zone_request: bool = False,
    create_max_retries: int | None = None,
) -> list:
    if create_max_retries is None:
        create_max_retries = BOTO_CREATE_MAX_RETRIES
    tags = {
        'Name': cluster_name,
        constants.TAG_RAY_CLUSTER_NAME: cluster_name,
        constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name,
        **tags
    }
    conf = node_config.copy()

    tag_specs = [{
        'ResourceType': 'instance',
        'Tags': _format_tags(tags),
    }]
    user_tag_specs = conf.get('TagSpecifications', [])
    _merge_tag_specs(tag_specs, user_tag_specs)

    # SubnetIds is not a real config key: we must resolve to a
    # single SubnetId before invoking the AWS API.
    subnet_ids = conf.pop('SubnetIds')
    market_options = conf.get('InstanceMarketOptions', {})
    market_type = (market_options.get('MarketType') if isinstance(
        market_options, dict) else None)
    is_spot = (isinstance(market_type, str) and market_type.lower() == 'spot')
    is_known_single_zone_spot = is_spot and is_single_zone_request

    # update config with min/max node counts and tag specs
    conf.update({
        'MinCount': count,
        'MaxCount': count,
        'TagSpecifications': tag_specs
    })

    # We are adding 'NetworkInterfaces' in the inner loop and having both keys
    # is considered invalid by the create_instances API.
    security_group_ids = conf.pop('SecurityGroupIds', None)
    # Guaranteed by config.py (the bootstrapping phase):
    assert 'NetworkInterfaces' not in conf, conf
    assert security_group_ids is not None, conf

    logger.debug(f'Creating {count} instances with config: \n{conf}')

    # NOTE: This ensures that we try ALL availability zones before
    # throwing an error.
    num_subnets = len(subnet_ids)
    max_tries = max(num_subnets * (create_max_retries // num_subnets),
                    len(subnet_ids))
    per_subnet_tries = max_tries // num_subnets
    errors: list[dict[str, str]] = []
    for i in range(max_tries):
        # Try each subnet for per_subnet_tries times.
        subnet_id = subnet_ids[i // per_subnet_tries]
        try:
            network_interfaces = [{
                'SubnetId': subnet_id,
                'DeviceIndex': 0,
                # Whether the VM(s) should have a public IP.
                'AssociatePublicIpAddress': associate_public_ip_address,
                'Groups': security_group_ids,
                'InterfaceType': 'efa'
                                 if max_efa_interfaces > 0 else 'interface',
            }]
            # Due to AWS limitation, if an instance type supports multiple
            # network cards, we cannot assign public IP addresses to the
            # instance during creation, which will raise the following error:
            #   (InvalidParameterCombination) when calling the RunInstances
            #   operation: The associatePublicIPAddress parameter cannot be
            #   specified when launching with multiple network interfaces.
            # So we only attach multiple network interfaces if public IP is
            # not required.
            # TODO(hailong): support attaching/detaching elastic IP to expose
            # public IP in this case.
            if max_efa_interfaces > 1 and not associate_public_ip_address:
                instance_type = conf['InstanceType']
                for i in range(1, max_efa_interfaces):
                    interface_type = 'efa-only'
                    # Special handling for P5 instances
                    # Refer to https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html#efa-for-p5 for more details. # pylint: disable=line-too-long
                    if (instance_type == 'p5.48xlarge' or
                            instance_type == 'p5e.48xlarge'):
                        interface_type = 'efa' if i % 4 == 0 else 'efa-only'
                    network_interfaces.append({
                        'SubnetId': subnet_id,
                        'DeviceIndex': 1,
                        'NetworkCardIndex': i,
                        'AssociatePublicIpAddress': False,
                        'Groups': security_group_ids,
                        'InterfaceType': interface_type,
                    })
            conf['NetworkInterfaces'] = network_interfaces

            instances = _ec2_call_with_retry_on_server_error(
                ec2_fail_fast.create_instances, **conf)
            return instances
        except aws.botocore_exceptions().ClientError as exc:
            error_data = exc.response.get('Error', {})
            error_code = str(error_data.get('Code', ''))
            errors.append({
                'code': error_code,
                'message': str(error_data.get('Message', exc)),
                'subnet_id': subnet_id,
            })
            is_terminal_error = (error_code in _REGIONAL_QUOTA_ERROR_CODES or
                                 (is_known_single_zone_spot and error_code
                                  == _INSUFFICIENT_CAPACITY_ERROR_CODE))
            echo = logger.debug
            if is_terminal_error or (i + 1) % per_subnet_tries == 0:
                # Print the warning only once per subnet
                echo = logger.warning
            echo(f'create_instances: Attempt failed with {exc}')
            if is_terminal_error:
                error = common.ProvisionerError(
                    'Failed to launch instances due to a terminal AWS error.')
                error.errors = errors
                error.requested_count = count
                raise error from exc
            if (i + 1) >= max_tries:
                error = common.ProvisionerError(
                    'Failed to launch instances. Max attempts exceeded.')
                error.errors = errors
                error.requested_count = count
                raise error from exc
    assert False, 'This code should not be reachable'
