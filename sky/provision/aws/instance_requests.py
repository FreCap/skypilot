"""AWS instance request construction and capacity retry policy."""

from collections.abc import Callable
import copy
import functools
import logging
import time
from typing import Any, TypeVar

from sky import exceptions
from sky import sky_logging
from sky.adaptors import aws
from sky.provision import capacity_policy
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

# Private handoff from the exact EC2 request boundary to ``instance.py``.
# ``run_instances()`` promotes this candidate to the public
# ``provider_negative_ack`` attribute only after it has also proved that the
# provider-native cluster inventory was empty and that this invocation neither
# resumed nor created anything.  Keeping the incomplete candidate private
# prevents a lower layer from accidentally claiming whole-invocation absence.
_AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR = (
    '_aws_run_instances_negative_ack_candidate')
_AMBIGUOUS_CREATE_RETRY_SECONDS = 5


def _provider_create_ambiguous_error(
) -> exceptions.ProviderCreateAmbiguousError:
    return exceptions.ProviderCreateAmbiguousError(
        'AWS RunInstances returned an indeterminate result for an '
        'idempotent ordinary-paid create.',
        hint=('Retrying the same immutable ordinary-paid association and EC2 '
              'ClientToken; provider cleanup and placement failover are '
              'disabled until the result is adopted or exactly rejected.'),
        retry_wait_seconds=_AMBIGUOUS_CREATE_RETRY_SECONDS)


def _run_instances_negative_ack_reason(error_code: str) -> str | None:
    if error_code == _INSUFFICIENT_CAPACITY_ERROR_CODE:
        return 'capacity'
    if error_code in _REGIONAL_QUOTA_ERROR_CODES:
        return 'quota'
    return None


def _exact_run_instances_negative_ack(
        error: BaseException, request: dict[str, Any], *, subnet_id: str,
        cluster_name: str,
        provider_create_idempotency_token: str | None) -> dict[str, Any] | None:
    """Return one exact EC2 rejection, or None for ambiguous evidence."""
    # A ClientError produced for another API operation is not evidence about
    # this RunInstances request, even if its response happens to reuse a known
    # error code.
    if getattr(error, 'operation_name', None) != 'RunInstances':
        return None
    response = getattr(error, 'response', None)
    if type(response) is not dict:
        return None
    error_payload = response.get('Error')
    response_metadata = response.get('ResponseMetadata')
    if type(error_payload) is not dict or type(response_metadata) is not dict:
        return None
    error_code = error_payload.get('Code')
    provider_request_id = response_metadata.get('RequestId')
    http_status_code = response_metadata.get('HTTPStatusCode')
    if (not isinstance(error_code, str) or not error_code or
            not isinstance(provider_request_id, str) or
            not provider_request_id or not capacity_policy.
            valid_aws_run_instances_negative_ack_http_status(
                error_code, http_status_code)):
        return None
    reason = _run_instances_negative_ack_reason(error_code)
    if reason is None:
        return None

    min_count = request.get('MinCount')
    max_count = request.get('MaxCount')
    instance_type = request.get('InstanceType')
    client_token = request.get('ClientToken')
    if (type(min_count) is not int or min_count < 1 or
            type(max_count) is not int or min_count != max_count or
            not isinstance(instance_type, str) or not instance_type or
            not isinstance(subnet_id, str) or not subnet_id or
            not isinstance(cluster_name, str) or not cluster_name or
            client_token != provider_create_idempotency_token or
            not capacity_policy.valid_aws_run_instances_client_token(
                client_token)):
        return None

    market_options = request.get('InstanceMarketOptions')
    market_type = (market_options.get('MarketType')
                   if type(market_options) is dict else None)
    if not isinstance(market_type, str) or market_type.lower() != 'spot':
        return None

    network_interfaces = request.get('NetworkInterfaces')
    if (type(network_interfaces) is not list or not network_interfaces or any(
            type(interface) is not dict or
            interface.get('SubnetId') != subnet_id
            for interface in network_interfaces)):
        return None

    tag_specifications = request.get('TagSpecifications')
    if type(tag_specifications) is not list:
        return None
    instance_tag_specs = [
        tag_spec for tag_spec in tag_specifications
        if type(tag_spec) is dict and tag_spec.get('ResourceType') == 'instance'
    ]
    if len(instance_tag_specs) != 1:
        return None
    instance_tags = instance_tag_specs[0].get('Tags')
    if type(instance_tags) is not list:
        return None
    tags_by_key: dict[str, str] = {}
    for tag in instance_tags:
        if type(tag) is not dict:
            return None
        key = tag.get('Key')
        value = tag.get('Value')
        if (not isinstance(key, str) or not key or not isinstance(value, str)):
            return None
        # Duplicate provider tags make the effective request binding
        # ambiguous, even if both happen to carry the expected value.
        if key in tags_by_key:
            return None
        tags_by_key[key] = value
    if (tags_by_key.get(constants.TAG_RAY_CLUSTER_NAME) != cluster_name or
            tags_by_key.get(
                constants.TAG_SKYPILOT_CLUSTER_NAME) != cluster_name):
        return None

    reservation_specification = request.get('CapacityReservationSpecification')
    capacity_reservation_id = None
    if reservation_specification is not None:
        if type(reservation_specification) is not dict:
            return None
        reservation_target = reservation_specification.get(
            'CapacityReservationTarget')
        if reservation_target is not None:
            if type(reservation_target) is not dict:
                return None
            capacity_reservation_id = reservation_target.get(
                'CapacityReservationId')
            if capacity_reservation_id is not None and (
                    not isinstance(capacity_reservation_id, str) or
                    not capacity_reservation_id):
                return None

    return {
        'provider_request_id': provider_request_id,
        'error_code': error_code,
        'reason': reason,
        'http_status_code': http_status_code,
        'subnet_id': subnet_id,
        'market': 'spot',
        'instance_type': instance_type,
        'cluster_name_on_cloud': cluster_name,
        'min_count': min_count,
        'max_count': max_count,
        'capacity_reservation_id': capacity_reservation_id,
        'client_token': client_token,
    }


def _attach_run_instances_negative_ack_candidate(
        error: common.ProvisionerError, *,
        provider_attempts: list[dict[str, Any] | None],
        is_single_zone_request: bool, cluster_name: str, requested_count: int,
        instance_type: object,
        provider_create_idempotency_token: str | None) -> None:
    candidate_attempts = [
        attempt for attempt in provider_attempts if attempt is not None
    ]
    candidate_reasons = {attempt['reason'] for attempt in candidate_attempts}
    if (not is_single_zone_request or
            not capacity_policy.valid_aws_run_instances_client_token(
                provider_create_idempotency_token) or
            len(candidate_attempts) != len(provider_attempts) or
            not candidate_attempts or len(candidate_reasons) != 1 or
            any(attempt['client_token'] != provider_create_idempotency_token
                for attempt in candidate_attempts)):
        return
    setattr(
        error, _AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR, {
            'schema_version': 1,
            'provider': 'aws',
            'operation': 'RunInstances',
            'reason': candidate_attempts[0]['reason'],
            'cluster_name_on_cloud': cluster_name,
            'requested_count': requested_count,
            'market': 'spot',
            'instance_type': instance_type,
            'client_token': provider_create_idempotency_token,
            'attempts': candidate_attempts,
        })


def _create_instances_with_negative_ack_observation(
        create_instances_fn: Callable[..., list],
        provider_attempts: list[dict[str, Any] | None], *, subnet_id: str,
        cluster_name: str, provider_create_idempotency_token: str | None,
        **request) -> list:
    try:
        return create_instances_fn(**request)
    except Exception as error:  # pylint: disable=broad-except
        negative_ack = None
        if isinstance(error, aws.botocore_exceptions().ClientError):
            negative_ack = _exact_run_instances_negative_ack(
                error,
                request,
                subnet_id=subnet_id,
                cluster_name=cluster_name,
                provider_create_idempotency_token=(
                    provider_create_idempotency_token))
        # Once the SDK call begins, a transport error or unrecognized response
        # is an unknown provider outcome.  Record it as None so a later typed
        # rejection cannot erase the earlier ambiguity.
        provider_attempts.append(negative_ack)
        raise


def _raise_ambiguous_create_pause(
    error: BaseException,
    *,
    provider_attempts: list[dict[str, Any] | None],
    provider_create_idempotency_token: str | None,
    negative_ack_owner: BaseException | None = None,
) -> None:
    """Pause a tokenized create unless every attempt proves zero effects."""
    if (not capacity_policy.valid_aws_run_instances_client_token(
            provider_create_idempotency_token) or not provider_attempts):
        return
    candidate_owner = (error
                       if negative_ack_owner is None else negative_ack_owner)
    candidate = getattr(candidate_owner,
                        _AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR, None)
    if candidate is not None:
        return
    raise _provider_create_ambiguous_error() from error


def _ec2_call_with_retry_on_server_error(
        ec2_fail_fast_fn: Callable[..., _T],
        log_level=logging.DEBUG,
        *,
        additional_retryable_error_codes: tuple[str, ...] = (),
        max_attempts: int | None = None,
        **kwargs) -> _T:
    """Call EC2 with bounded retries for server and opted-in error codes."""
    # Here we have to handle 'RequestLimitExceeded' error, so the provision
    # would not fail due to request limit issues.
    # Here the backoff config (5, 12) is picked at random and does not
    # have any special meaning.
    backoff = common_utils.Backoff(initial_backoff=5, max_backoff_factor=12)
    if max_attempts is None:
        max_attempts = utils.BOTO_MAX_RETRIES
    if max_attempts < 1:
        raise ValueError('max_attempts must be positive.')
    retryable_error_codes = frozenset({
        'RequestLimitExceeded',
        'ServerInternal',
        'ServiceUnavailable',
        'InternalError',
        'Unavailable',
    }).union(additional_retryable_error_codes)
    last_retryable_error: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return ec2_fail_fast_fn(**kwargs)
        except aws.botocore_exceptions().ClientError as e:
            # Retry server side errors, as they are likely to be transient.
            # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html#api-error-codes-table-server # pylint: disable=line-too-long
            error_code = e.response['Error']['Code']
            if error_code in retryable_error_codes:
                last_retryable_error = e
                if attempt + 1 < max_attempts:
                    time.sleep(backoff.current_backoff())
                    logger.debug(
                        f'EC2 call failed with {error_code}; retrying.')
                    continue
                break
            logger.log(log_level, f'create_instances: Attempt failed with {e}')
            raise
    error = RuntimeError(
        f'Failed to call EC2 function {ec2_fail_fast_fn}; retryable '
        'errors exhausted all local attempts.')
    if last_retryable_error is not None:
        raise error from last_retryable_error
    raise error


def _format_tags(tags: dict[str, str]) -> list:
    return [{'Key': k, 'Value': v} for k, v in tags.items()]


def _merge_tag_specs(
        tag_specs: list[dict[str, Any]],
        user_tag_specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merges user-provided tag specifications without mutating either input.

    User tags override SkyPilot defaults except for provider-identity and
    managed markers. Resource types not already emitted by SkyPilot are
    retained.

    Args:
        tag_specs (List[Dict[str, Any]]): base node provider tag specs
        user_tag_specs (List[Dict[str, Any]]): user's node config tag specs

    Returns:
        A new merged list of tag specifications.
    """
    merged_tag_specs = copy.deepcopy(tag_specs)
    reserved_tag_keys = {
        constants.TAG_RAY_CLUSTER_NAME,
        constants.TAG_SKYPILOT_CLUSTER_NAME,
        constants.TAG_SKYPILOT_MANAGED,
    }
    specs_by_resource_type = {
        tag_spec['ResourceType']: tag_spec for tag_spec in merged_tag_specs
    }
    for user_tag_spec in copy.deepcopy(user_tag_specs):
        resource_type = user_tag_spec['ResourceType']
        if resource_type not in specs_by_resource_type:
            merged_tag_specs.append(user_tag_spec)
            specs_by_resource_type[resource_type] = user_tag_spec
            continue

        tags_by_key = {
            tag['Key']: tag
            for tag in specs_by_resource_type[resource_type]['Tags']
        }
        for user_tag in user_tag_spec['Tags']:
            key = user_tag['Key']
            if key in reserved_tag_keys:
                continue
            if key in tags_by_key:
                tags_by_key[key]['Value'] = user_tag['Value']
            else:
                specs_by_resource_type[resource_type]['Tags'].append(user_tag)
                tags_by_key[key] = user_tag

    for tag_spec in merged_tag_specs:
        tags_by_key = {tag['Key']: tag for tag in tag_spec['Tags']}
        if constants.TAG_SKYPILOT_MANAGED in tags_by_key:
            tags_by_key[constants.TAG_SKYPILOT_MANAGED][
                'Value'] = constants.SKYPILOT_MANAGED_TAG_VALUE
        else:
            tag_spec['Tags'].append({
                'Key': constants.TAG_SKYPILOT_MANAGED,
                'Value': constants.SKYPILOT_MANAGED_TAG_VALUE,
            })
    return merged_tag_specs


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
    provider_create_idempotency_token: str | None = None,
) -> list:
    if create_max_retries is None:
        create_max_retries = BOTO_CREATE_MAX_RETRIES
    tags = {
        'Name': cluster_name,
        **tags,
        constants.TAG_RAY_CLUSTER_NAME: cluster_name,
        constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name,
        constants.TAG_SKYPILOT_MANAGED: constants.SKYPILOT_MANAGED_TAG_VALUE,
    }
    conf = copy.deepcopy(node_config)
    if provider_create_idempotency_token is not None:
        if not capacity_policy.valid_aws_run_instances_client_token(
                provider_create_idempotency_token):
            raise ValueError(
                'AWS provider create idempotency token is invalid.')
        # Runtime authority owns ClientToken.  Do not let persisted task YAML
        # or node-config overrides select the identity of this provider call.
        conf['ClientToken'] = provider_create_idempotency_token

    market_options = conf.get('InstanceMarketOptions', {})
    market_type = (market_options.get('MarketType') if isinstance(
        market_options, dict) else None)
    is_spot = (isinstance(market_type, str) and market_type.lower() == 'spot')

    tag_specs = [{
        'ResourceType': 'instance',
        'Tags': _format_tags(tags),
    }, {
        'ResourceType': 'volume',
        'Tags': [{
            'Key': constants.TAG_RAY_CLUSTER_NAME,
            'Value': cluster_name,
        }, {
            'Key': constants.TAG_SKYPILOT_CLUSTER_NAME,
            'Value': cluster_name,
        }, {
            'Key': constants.TAG_SKYPILOT_MANAGED,
            'Value': constants.SKYPILOT_MANAGED_TAG_VALUE,
        }],
    }, {
        'ResourceType': 'network-interface',
        'Tags': [{
            'Key': constants.TAG_SKYPILOT_MANAGED,
            'Value': constants.SKYPILOT_MANAGED_TAG_VALUE,
        }],
    }]
    if is_spot:
        tag_specs.append({
            'ResourceType': 'spot-instances-request',
            'Tags': [{
                'Key': constants.TAG_SKYPILOT_MANAGED,
                'Value': constants.SKYPILOT_MANAGED_TAG_VALUE,
            }],
        })
    user_tag_specs = conf.get('TagSpecifications', [])
    tag_specs = _merge_tag_specs(tag_specs, user_tag_specs)

    # SubnetIds is not a real config key: we must resolve to a
    # single SubnetId before invoking the AWS API.
    subnet_ids = conf.pop('SubnetIds')
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
    # This records every EC2 ClientError observed inside both retry layers.
    # A retryable 5xx followed by a typed capacity rejection therefore remains
    # ambiguous instead of being hidden by the final exception.
    provider_attempts: list[dict[str, Any] | None] = []
    for attempt_index in range(max_tries):
        # Try each subnet for per_subnet_tries times.
        subnet_id = subnet_ids[attempt_index // per_subnet_tries]
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
                for network_card_index in range(1, max_efa_interfaces):
                    interface_type = 'efa-only'
                    # Special handling for P5 instances
                    # Refer to https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html#efa-for-p5 for more details. # pylint: disable=line-too-long
                    if (instance_type == 'p5.48xlarge' or
                            instance_type == 'p5e.48xlarge'):
                        interface_type = ('efa' if network_card_index %
                                          4 == 0 else 'efa-only')
                    network_interfaces.append({
                        'SubnetId': subnet_id,
                        'DeviceIndex': 1,
                        'NetworkCardIndex': network_card_index,
                        'AssociatePublicIpAddress': False,
                        'Groups': security_group_ids,
                        'InterfaceType': interface_type,
                    })
            conf['NetworkInterfaces'] = network_interfaces

            observed_create_instances = functools.partial(
                _create_instances_with_negative_ack_observation,
                ec2_fail_fast.create_instances,
                provider_attempts,
                subnet_id=subnet_id,
                cluster_name=cluster_name,
                provider_create_idempotency_token=(
                    provider_create_idempotency_token))
            instances = _ec2_call_with_retry_on_server_error(
                observed_create_instances, **conf)
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
            if (is_terminal_error or
                (attempt_index + 1) % per_subnet_tries == 0):
                # Print the warning only once per subnet
                echo = logger.warning
            echo(f'create_instances: Attempt failed with {exc}')
            if is_terminal_error:
                error = common.ProvisionerError(
                    'Failed to launch instances due to a terminal AWS error.')
                error.errors = errors
                error.requested_count = count
                _attach_run_instances_negative_ack_candidate(
                    error,
                    provider_attempts=provider_attempts,
                    is_single_zone_request=is_single_zone_request,
                    cluster_name=cluster_name,
                    requested_count=count,
                    instance_type=conf.get('InstanceType'),
                    provider_create_idempotency_token=(
                        provider_create_idempotency_token))
                _raise_ambiguous_create_pause(
                    exc,
                    provider_attempts=provider_attempts,
                    provider_create_idempotency_token=(
                        provider_create_idempotency_token),
                    negative_ack_owner=error)
                raise error from exc
            _raise_ambiguous_create_pause(
                exc,
                provider_attempts=provider_attempts,
                provider_create_idempotency_token=(
                    provider_create_idempotency_token))
            if (attempt_index + 1) >= max_tries:
                error = common.ProvisionerError(
                    'Failed to launch instances. Max attempts exceeded.')
                error.errors = errors
                error.requested_count = count
                _attach_run_instances_negative_ack_candidate(
                    error,
                    provider_attempts=provider_attempts,
                    is_single_zone_request=is_single_zone_request,
                    cluster_name=cluster_name,
                    requested_count=count,
                    instance_type=conf.get('InstanceType'),
                    provider_create_idempotency_token=(
                        provider_create_idempotency_token))
                _raise_ambiguous_create_pause(
                    exc,
                    provider_attempts=provider_attempts,
                    provider_create_idempotency_token=(
                        provider_create_idempotency_token),
                    negative_ack_owner=error)
                raise error from exc
        except Exception as exc:  # pylint: disable=broad-except
            _raise_ambiguous_create_pause(
                exc,
                provider_attempts=provider_attempts,
                provider_create_idempotency_token=(
                    provider_create_idempotency_token))
            raise
    assert False, 'This code should not be reachable'
