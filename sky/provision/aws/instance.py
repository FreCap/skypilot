"""AWS instance provisioning.

Note (dev): If API changes are made to adaptors/aws.py and the new API is used
in this or config module, please make sure to reload it as in
_default_ec2_resource() to avoid version mismatch issues.
"""
import copy
import logging
from multiprocessing import pool
import re
import time
import typing
from typing import Any, Optional

from sky import exceptions
from sky import sky_logging
from sky.adaptors import aws
from sky.clouds import aws as aws_cloud
from sky.clouds.utils import aws_utils
from sky.provision import capacity_policy
from sky.provision import common
from sky.provision import constants
from sky.provision import provider_facets
from sky.provision.aws import config as aws_config
from sky.provision.aws import instance_requests
from sky.provision.aws.instance_requests import (
    _ec2_call_with_retry_on_server_error)
from sky.provision.aws.instance_requests import _format_tags
from sky.provision.aws.instance_requests import _is_single_zone_request
from sky.provision.aws.instance_requests import (
    BOTO_CREATE_MAX_RETRIES as _DEFAULT_BOTO_CREATE_MAX_RETRIES)
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from botocore import waiter as botowaiter
    import mypy_boto3_ec2
    from mypy_boto3_ec2 import type_defs as ec2_type_defs

logger = sky_logging.init_logger(__name__)

_DEFAULT_INGRESS_SOURCE_RANGE = '0.0.0.0/0'

# Max retries for general AWS API calls.
BOTO_MAX_RETRIES = 12
# Max retries for creating an instance. Kept on this facade for compatibility
# with tests and callers that tune the retry budget at runtime.
BOTO_CREATE_MAX_RETRIES = _DEFAULT_BOTO_CREATE_MAX_RETRIES
# Max retries for deleting security groups etc.
BOTO_DELETE_MAX_ATTEMPTS = 6
_DEPENDENCY_VIOLATION_PATTERN = re.compile(
    r'An error occurred \(DependencyViolation\) when calling the '
    r'DeleteSecurityGroup operation(.*): (.*)')

_RESUME_INSTANCE_TIMEOUT = 480  # 8 minutes
_RESUME_PER_INSTANCE_TIMEOUT = 120  # 2 minutes
_ORDINARY_PAID_PROVIDER_PROOF_RETRY_SECONDS = 5
_FRESH_INSTANCE_TAG_MAX_ATTEMPTS = 3
_FRESH_INSTANCE_TAG_RETRYABLE_ERROR_CODES = ('InvalidInstanceID.NotFound',)
_SERVE_INVENTORY_CONNECT_TIMEOUT_SECONDS = 5
_SERVE_INVENTORY_READ_TIMEOUT_SECONDS = 10
_SERVE_INVENTORY_TOTAL_MAX_ATTEMPTS = 1

# ======================== About AWS subnet/VPC ========================
# https://stackoverflow.com/questions/37407492/are-there-differences-in-networking-performance-if-ec2-instances-are-in-differen
# https://docs.aws.amazon.com/vpc/latest/userguide/how-it-works.html
# https://docs.aws.amazon.com/vpc/latest/userguide/configure-subnets.html

# ======================== Instance state and lifecycle ========================
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html

# ======================== About AWS availability zone ========================
# Data transfer within the same region but different availability zone
#  costs $0.01/GB:
# https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer_within_the_same_AWS_Region


def _default_ec2_resource(
        region: str,
        check_credentials: bool = True) -> 'mypy_boto3_ec2.ServiceResource':
    if not hasattr(aws, 'version'):
        # For backward compatibility, reload the module if the aws module was
        # imported before and stale. Used for, e.g., a live jobs controller
        # running an older version and a new version gets installed by
        # `sky jobs launch`.
        #
        # Detailed explanation follows. Assume we're in this situation: an old
        # jobs controller running a managed job and then the code gets updated
        # on the controller due to a new `sky jobs launch or `sky start`.
        #
        # First, controller consists of an outer process (sky.jobs.controller's
        # main) and an inner process running the controller logic (started as a
        # multiprocessing.Process in sky.jobs.controller). `sky.provision.aws`
        # is only imported in the inner process due to its load-on-use
        # semantics.
        #
        # At this point in the normal execution, inner process has loaded
        # {old sky.provision.aws, old sky.adaptors.aws}, and outer process has
        # loaded {old sky.adaptors.aws}.
        #
        # In controller.py's start(), the inner process may exit due to managed
        # job exits or `sky jobs cancel`, entering outer process'
        # `finally: ... _cleanup()` path. Inside _cleanup(), we eventually call
        # into `sky.provision.aws` which loads this module for the first time
        # for the outer process. At this point, outer process has loaded
        # {old sky.adaptors.aws, new sky.provision.aws}.
        #
        # This version mismatch becomes a "backward compatibility" problem if
        # `new sky.provision.aws` depends on `new sky.adaptors.aws` (assuming
        # API changes in sky.adaptors.aws). Therefore, here we use a hack to
        # reload sky.adaptors.aws to go from old to new.
        #
        # For version < 1 (variable does not exist), we do not have
        # `max_attempts` in the `aws.resource` call, so we need to reload the
        # module to get the latest `aws.resource` function.
        import importlib  # pylint: disable=import-outside-toplevel
        importlib.reload(aws)
    return aws.resource('ec2',
                        region_name=region,
                        max_attempts=BOTO_MAX_RETRIES,
                        check_credentials=check_credentials)


def _cluster_name_filter(
        cluster_name_on_cloud: str) -> list['ec2_type_defs.FilterTypeDef']:
    return [{
        'Name': f'tag:{constants.TAG_RAY_CLUSTER_NAME}',
        'Values': [cluster_name_on_cloud],
    }]


def _create_instances(
    ec2_fail_fast,
    cluster_name: str,
    node_config: dict[str, Any],
    tags: dict[str, str],
    count: int,
    associate_public_ip_address: bool,
    max_efa_interfaces: int,
    is_single_zone_request: bool = False,
    provider_create_idempotency_token: str | None = None,
) -> list:
    """Compatibility facade for the extracted request implementation."""
    return instance_requests.create_instances(
        ec2_fail_fast,
        cluster_name,
        node_config,
        tags,
        count,
        associate_public_ip_address,
        max_efa_interfaces,
        is_single_zone_request,
        create_max_retries=BOTO_CREATE_MAX_RETRIES,
        provider_create_idempotency_token=provider_create_idempotency_token)


def _get_head_instance_id(instances: list) -> str | None:
    head_instance_id = None
    head_node_markers = tuple(constants.HEAD_NODE_TAGS.items())

    for inst in instances:
        for t in inst.tags:
            if (t['Key'], t['Value']) in head_node_markers:
                if head_instance_id is not None:
                    logger.warning(
                        'There are multiple head nodes in the cluster '
                        f'(current head instance id: {head_instance_id}, '
                        f'newly discovered id: {inst.id}). It is likely '
                        f'that something goes wrong.')
                head_instance_id = inst.id
                break
    return head_instance_id


def _capture_fresh_instance_identity(
    request_session: Any,
    *,
    region: str,
    created_instance_ids: list[str],
    expected_aws_account_id: str | None = None,
) -> common.AWSInstanceIdentity | None:
    """Wait for and read one exact created instance with the same session."""
    if request_session is None or len(created_instance_ids) != 1:
        return None
    instance_id = created_instance_ids[0]
    ec2_client = request_session.client('ec2', region_name=region)
    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id],
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 120,
                })
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    reservations = response.get('Reservations')
    if not isinstance(reservations, list):
        raise ValueError('DescribeInstances returned no reservation list.')
    instances = [
        instance for reservation in reservations
        if isinstance(reservation, dict)
        for instance in reservation.get('Instances', [])
        if isinstance(instance, dict)
    ]
    if len(instances) != 1 or instances[0].get('InstanceId') != instance_id:
        raise ValueError('DescribeInstances did not return the exact create.')
    instance = instances[0]
    state = instance.get('State')
    if not isinstance(state, dict) or state.get('Name') != 'running':
        raise ValueError('DescribeInstances did not return a running instance.')
    instance_type = instance.get('InstanceType')
    if not isinstance(instance_type, str) or not instance_type:
        raise ValueError('DescribeInstances returned no instance type.')
    placement = instance.get('Placement')
    availability_zone = (placement.get('AvailabilityZone') if isinstance(
        placement, dict) else None)
    if not isinstance(availability_zone, str) or not availability_zone:
        raise ValueError('DescribeInstances returned no availability zone.')
    lifecycle = instance.get('InstanceLifecycle')
    if lifecycle is None:
        market_type = 'on_demand'
    elif lifecycle == 'spot':
        market_type = 'spot'
    else:
        raise ValueError(f'Unsupported InstanceLifecycle: {lifecycle!r}.')
    sts_client = request_session.client('sts', region_name=region)
    caller_identity = sts_client.get_caller_identity()
    account_id = caller_identity.get('Account')
    if not isinstance(account_id, str) or not account_id:
        raise ValueError('STS returned no AWS account ID.')
    if (expected_aws_account_id is not None and
            account_id != expected_aws_account_id):
        raise ValueError('STS account does not match the expected AWS account.')
    return common.AWSInstanceIdentity(aws_account_id=account_id,
                                      region=region,
                                      availability_zone=availability_zone,
                                      ec2_instance_id=instance_id,
                                      instance_type=instance_type,
                                      market_type=market_type)


def _capture_request_aws_identity(request_session: Any,
                                  region: str) -> tuple[str, str] | None:
    """Return the account and principal used by this provisioner session."""
    if request_session is None:
        return None
    try:
        sts_client = request_session.client('sts', region_name=region)
        caller_identity = sts_client.get_caller_identity()
        if type(caller_identity) is not dict:
            return None
        account_id = caller_identity.get('Account')
        principal_arn = caller_identity.get('Arn')
        if (not isinstance(account_id, str) or
                re.fullmatch(r'[0-9]{12}', account_id) is None or
                not isinstance(principal_arn, str) or not principal_arn):
            return None
        return account_id, principal_arn
    except Exception as error:  # pylint: disable=broad-except
        # The caller decides whether identity is optional audit evidence or a
        # required account-bound preflight. Keep this helper closed and let the
        # tokenized paid path pause instead of confusing unavailability with a
        # durable identity mismatch.
        logger.debug('AWS account/principal evidence is unavailable: '
                     f'{common_utils.format_exception(error)}')
        return None


def _capture_subnet_availability_zones(
        request_session: Any, region: str,
        subnet_ids: list[str]) -> dict[str, str] | None:
    """Resolve every attempted subnet to its provider-native exact AZ."""
    if request_session is None or not subnet_ids or any(
            not isinstance(subnet_id, str) or not subnet_id
            for subnet_id in subnet_ids):
        return None
    unique_subnet_ids = sorted(set(subnet_ids))
    try:
        ec2_client = request_session.client('ec2', region_name=region)
        response = ec2_client.describe_subnets(SubnetIds=unique_subnet_ids)
        if (type(response) is not dict or
                type(response.get('Subnets')) is not list):
            return None
        subnet_availability_zones: dict[str, str] = {}
        for subnet in response['Subnets']:
            if type(subnet) is not dict:
                return None
            subnet_id = subnet.get('SubnetId')
            availability_zone = subnet.get('AvailabilityZone')
            if (not isinstance(subnet_id, str) or not subnet_id or
                    not isinstance(availability_zone, str) or
                    not availability_zone or
                    subnet_id in subnet_availability_zones):
                return None
            subnet_availability_zones[subnet_id] = availability_zone
        if set(subnet_availability_zones) != set(unique_subnet_ids):
            return None
        return subnet_availability_zones
    except Exception as error:  # pylint: disable=broad-except
        logger.debug('AWS subnet/AZ evidence is unavailable: '
                     f'{common_utils.format_exception(error)}')
        return None


def _pause_ordinary_paid_provider_proof(
        detail: str) -> exceptions.ExecutionPausedError:
    """Pause one account-bound create until its read-only proof recovers.

    The outer ordinary-launch association is already in ``PROVIDER_IO`` when
    this AWS boundary runs. A prior execution generation may therefore have
    reached ``RunInstances`` even when the current generation stops in its
    STS/subnet preflight. Retrying the same immutable association is safe
    because it reuses the same EC2 ClientToken; terminalizing or failing over
    would instead strand the paid claim or risk a duplicate provider object.
    """
    return exceptions.ExecutionPausedError(
        detail,
        hint=('Retrying the same immutable ordinary-paid AWS association; '
              'no failover or provider teardown is permitted while its '
              'account/subnet proof is unavailable.'),
        retry_wait_seconds=_ORDINARY_PAID_PROVIDER_PROOF_RETRY_SECONDS)


def _single_availability_zone(provider_config: dict[str, Any]) -> str | None:
    value = provider_config.get('availability_zone')
    if not isinstance(value, str):
        return None
    zones = [zone.strip() for zone in value.split(',') if zone.strip()]
    return zones[0] if len(zones) == 1 else None


def _promote_provider_negative_ack(
    error: BaseException,
    *,
    request_aws_identity: tuple[str, str] | None,
    request_subnet_availability_zones: dict[str, str] | None,
    region: str,
    availability_zone: str | None,
    cluster_name_on_cloud: str,
    requested_count: int,
    instance_type: object,
    initial_nonterminated_instance_ids: list[str],
    resumed_instance_ids: list[str],
    created_instance_ids: list[str],
    successful_create_calls: int,
    provider_create_idempotency_token: str | None,
    provider_create_account_id: str | None,
) -> dict[str, Any] | None:
    """Attach whole-invocation zero-effect evidence when fully proven."""
    error_dict = getattr(error, '__dict__', None)
    if type(error_dict) is not dict:
        return None
    # The private request-layer candidate is intentionally never serialized.
    # Once the whole-invocation layer consumes it, only the independently
    # validated public receipt may remain on the exception.
    candidate = error_dict.pop(
        instance_requests._AWS_RUN_INSTANCES_NEGATIVE_ACK_CANDIDATE_ATTR,  # pylint: disable=protected-access
        None)
    if (availability_zone is None or initial_nonterminated_instance_ids or
            resumed_instance_ids or created_instance_ids or
            successful_create_calls != 0 or type(requested_count) is not int or
            requested_count < 1 or not isinstance(instance_type, str) or
            not instance_type):
        return None
    candidate_keys = {
        'schema_version', 'provider', 'operation', 'reason',
        'cluster_name_on_cloud', 'requested_count', 'market', 'instance_type',
        'client_token', 'attempts'
    }
    if (type(candidate) is not dict or set(candidate) != candidate_keys or
            candidate['schema_version'] != 1 or
            candidate['provider'] != 'aws' or
            candidate['operation'] != 'RunInstances' or
            candidate['reason'] not in ('capacity', 'quota') or
            candidate['cluster_name_on_cloud'] != cluster_name_on_cloud or
            candidate['requested_count'] != requested_count or
            candidate['market'] != 'spot' or
            candidate['instance_type'] != instance_type or
            candidate['client_token'] != provider_create_idempotency_token or
            not capacity_policy.valid_aws_run_instances_client_token(
                provider_create_idempotency_token) or
            type(candidate['attempts']) is not list or
            not candidate['attempts']):
        return None
    raw_attempt_keys = {
        'provider_request_id', 'error_code', 'reason', 'http_status_code',
        'subnet_id', 'market', 'instance_type', 'cluster_name_on_cloud',
        'min_count', 'max_count', 'capacity_reservation_id', 'client_token'
    }
    raw_subnet_ids: list[str] = []
    for raw_attempt in candidate['attempts']:
        if type(raw_attempt) is not dict or set(
                raw_attempt) != raw_attempt_keys:
            return None
        raw_subnet_ids.append(raw_attempt['subnet_id'])

    # Tokenized paid launches attest the provider account and their one exact
    # subnet before any EC2 inventory or create effect.  Reuse that immutable
    # preflight result here: a transient lookup after a typed rejection must
    # not strand an otherwise exact zero-effect launch indefinitely.
    if (request_aws_identity is None or
            request_aws_identity[0] != provider_create_account_id):
        return None
    if request_subnet_availability_zones is None:
        return None
    aws_account_id, aws_principal_arn = request_aws_identity
    if any(
            request_subnet_availability_zones.get(subnet_id) !=
            availability_zone for subnet_id in raw_subnet_ids):
        return None

    attempts: list[dict[str, Any]] = []
    for raw_attempt in candidate['attempts']:
        attempts.append({
            'provider_request_id': raw_attempt['provider_request_id'],
            'error_code': raw_attempt['error_code'],
            'reason': raw_attempt['reason'],
            'http_status_code': raw_attempt['http_status_code'],
            'aws_account_id': aws_account_id,
            'aws_principal_arn': aws_principal_arn,
            'region': region,
            'availability_zone': availability_zone,
            'subnet_id': raw_attempt['subnet_id'],
            'market': raw_attempt['market'],
            'instance_type': raw_attempt['instance_type'],
            'cluster_name_on_cloud': raw_attempt['cluster_name_on_cloud'],
            'min_count': raw_attempt['min_count'],
            'max_count': raw_attempt['max_count'],
            'capacity_reservation_id': raw_attempt['capacity_reservation_id'],
            'client_token': raw_attempt['client_token'],
        })
    receipt = {
        'schema_version': 1,
        'provider': 'aws',
        'operation': 'RunInstances',
        'reason': candidate['reason'],
        'aws_account_id': aws_account_id,
        'aws_principal_arn': aws_principal_arn,
        'cluster_name_on_cloud': cluster_name_on_cloud,
        'requested_count': requested_count,
        'market': 'spot',
        'instance_type': instance_type,
        'region': region,
        'availability_zone': availability_zone,
        'client_token': provider_create_idempotency_token,
        'invocations': [{
            'region': region,
            'availability_zone': availability_zone,
            'initial_nonterminated_instance_ids': [],
            'resumed_instance_ids': [],
            'created_instance_ids': [],
            'successful_create_calls': 0,
            'ambiguous_create_calls': 0,
            'create_call_count': len(attempts),
            'attempts': attempts,
        }],
    }
    canonical = capacity_policy.validate_provider_negative_ack(
        receipt,
        cluster_name=cluster_name_on_cloud,
        requested_count=requested_count,
        client_token=provider_create_idempotency_token,
        expected_aws_account_id=provider_create_account_id)
    return canonical


def run_instances(region: str, cluster_name: str, cluster_name_on_cloud: str,
                  config: common.ProvisionConfig) -> common.ProvisionRecord:
    """See sky/provision/__init__.py"""
    del cluster_name  # unused
    is_single_zone_request = _is_single_zone_request(config.provider_config)

    expected_account_id = config.provider_create_account_id
    expected_client_token = config.provider_create_idempotency_token
    if (expected_account_id is None) != (expected_client_token is None):
        raise exceptions.ServeReplicaLaunchFenceError(
            'Ordinary-paid AWS provider scope does not match the active '
            'workspace credentials.')
    exact_availability_zone = _single_availability_zone(config.provider_config)
    raw_subnet_ids = config.node_config.get('SubnetIds')
    if expected_account_id is not None:
        capacity_reservation_target = (config.node_config.get(
            'CapacityReservationSpecification',
            {}).get('CapacityReservationTarget'))
        # One association owns one EC2 idempotency token and therefore one
        # immutable RunInstances parameter set. Refuse configurations that
        # could split the launch across reservations/subnets or AZs before any
        # credential or EC2 call.
        if (re.fullmatch(r'[0-9]{12}', expected_account_id) is None or
                not capacity_policy.valid_aws_run_instances_client_token(
                    expected_client_token) or exact_availability_zone is None or
                capacity_reservation_target or
                type(raw_subnet_ids) is not list or len(raw_subnet_ids) != 1 or
                not isinstance(raw_subnet_ids[0], str) or
                not raw_subnet_ids[0]):
            raise exceptions.ServeReplicaLaunchFenceError(
                'Ordinary-paid AWS provider scope must bind one valid account '
                'and client token, one exact availability zone, and one exact '
                'subnet without a targeted capacity reservation.')
    try:
        ec2 = _default_ec2_resource(region)
    except Exception as error:  # pylint: disable=broad-except
        if expected_account_id is not None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS credentials are temporarily unavailable '
                'before provider inventory.') from error
        raise
    # aws.resource() above and aws.session() share the same thread-local
    # workspace-profile session. Fetch it after creating the resource so the
    # compatibility reload in _default_ec2_resource(), if any, has completed.
    # This retains the exact provisioning credential scope for the optional
    # post-create DescribeInstances/STS evidence rather than resolving a new
    # ambient/default session later in the backend.
    request_session_error = None
    try:
        request_session = aws.session(check_credentials=True,
                                      profile=aws.get_workspace_profile())
    except Exception as error:  # pylint: disable=broad-except
        request_session = None
        request_session_error = error
    request_aws_identity = None
    request_subnet_availability_zones = None
    if expected_account_id is not None:
        if request_session is None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS request credentials are temporarily '
                'unavailable before provider inventory.') from (
                    request_session_error)
        request_aws_identity = _capture_request_aws_identity(
            request_session, region)
        if request_aws_identity is None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS account proof is temporarily unavailable '
                'before provider inventory.')
        if request_aws_identity[0] != expected_account_id:
            raise exceptions.ServeReplicaLaunchFenceError(
                'Ordinary-paid AWS provider scope does not match the active '
                'workspace credentials.')
        assert isinstance(raw_subnet_ids, list)
        request_subnet_availability_zones = (_capture_subnet_availability_zones(
            request_session, region, raw_subnet_ids))
        if request_subnet_availability_zones is None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS subnet proof is temporarily unavailable '
                'before provider inventory.')
        if (request_subnet_availability_zones.get(raw_subnet_ids[0])
                != exact_availability_zone):
            raise exceptions.ServeReplicaLaunchFenceError(
                'Ordinary-paid AWS provider subnet does not match its '
                'immutable availability-zone scope.')
    # NOTE: We set max_attempts=0 for fast failing when the resource is not
    # available (although the doc says it will only retry for network
    # issues, practically, it retries for capacity errors, etc as well).
    try:
        ec2_fail_fast = aws.resource('ec2', region_name=region, max_attempts=0)
    except Exception as error:  # pylint: disable=broad-except
        if expected_account_id is not None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS EC2 request client is temporarily '
                'unavailable before provider inventory.') from error
        raise

    region = ec2.meta.client.meta.region_name
    zone = None
    resumed_instance_ids: list[str] = []
    created_instance_ids: list[str] = []
    max_efa_interfaces = config.provider_config.get('max_efa_interfaces', 0)

    # sort tags by key to support deterministic unit test stubbing
    tags = copy.deepcopy(config.tags)
    tags[constants.TAG_SKYPILOT_MANAGED] = (
        constants.SKYPILOT_MANAGED_TAG_VALUE)
    tags = dict(sorted(tags.items()))
    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': 'instance-state-name',
            # A shutting-down instance is still a provider object and therefore
            # prevents zero-effect certification, even though normal launch logic
            # does not try to reuse it.
            'Values': [
                'pending', 'running', 'stopping', 'stopped', 'shutting-down'
            ],
        },
        {
            'Name': f'tag:{constants.TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name_on_cloud],
        }
    ]
    try:
        initial_inventory_instances = list(
            ec2.instances.filter(Filters=filters))
        # Boto resources may lazily load these attributes. Materialize every
        # read needed to classify the pre-create inventory inside the same
        # retryable proof boundary; no provider mutation has happened yet.
        raw_inventory = [(instance, instance.id, instance.state, instance.tags)
                         for instance in initial_inventory_instances]
    except Exception as error:  # pylint: disable=broad-except
        if expected_account_id is not None:
            raise _pause_ordinary_paid_provider_proof(
                'Ordinary-paid AWS instance inventory is temporarily '
                'unavailable before provider create.') from error
        raise

    inventory: list[tuple[Any, str, str, list[Any]]] = []
    seen_instance_ids: set[str] = set()
    allowed_states = frozenset(
        {'pending', 'running', 'stopping', 'stopped', 'shutting-down'})
    for instance, instance_id, state, instance_tags in raw_inventory:
        state_name = state.get('Name') if isinstance(state, dict) else None
        valid_tags = bool(
            isinstance(instance_tags, list) and all(
                isinstance(tag, dict) and isinstance(tag.get('Key'), str) and
                isinstance(tag.get('Value'), str) for tag in instance_tags))
        if (not isinstance(instance_id, str) or not instance_id or
                instance_id in seen_instance_ids or
                not isinstance(state_name, str) or
                state_name not in allowed_states or not valid_tags):
            detail = ('AWS returned malformed or duplicate instance inventory '
                      f'for cluster {cluster_name_on_cloud!r}.')
            if expected_account_id is not None:
                raise exceptions.ServeReplicaLaunchFenceError(detail)
            raise RuntimeError(detail)
        seen_instance_ids.add(instance_id)
        assert isinstance(state_name, str)
        assert isinstance(instance_tags, list)
        inventory.append((instance, instance_id, state_name, instance_tags))

    initial_nonterminated_instance_ids = sorted(seen_instance_ids)
    existing_inventory = [
        item for item in inventory if item[2] != 'shutting-down'
    ]
    existing_inventory.sort(key=lambda item: item[1])
    exist_instances = [item[0] for item in existing_inventory]
    head_instance_id = None
    head_node_markers = tuple(constants.HEAD_NODE_TAGS.items())
    for _, instance_id, _, instance_tags in existing_inventory:
        if any((tag['Key'], tag['Value']) in head_node_markers
               for tag in instance_tags):
            if head_instance_id is not None:
                logger.warning(
                    'There are multiple head nodes in the cluster '
                    f'(current head instance id: {head_instance_id}, '
                    f'newly discovered id: {instance_id}). It is likely '
                    'that something goes wrong.')
            head_instance_id = instance_id

    pending_instances = []
    running_instances = []
    stopping_instances = []
    stopped_instances = []

    for inst, _, state, _ in existing_inventory:
        if state == 'pending':
            pending_instances.append(inst)
        elif state == 'running':
            running_instances.append(inst)
        elif state == 'stopping':
            stopping_instances.append(inst)
        elif state == 'stopped':
            stopped_instances.append(inst)
        else:
            assert False, state

    def _create_node_tag(target_instance,
                         is_head: bool = True,
                         retry_eventual_consistency: bool = False) -> str:
        node_type_tags = (constants.HEAD_NODE_TAGS
                          if is_head else constants.WORKER_NODE_TAGS)
        node_tag = [{'Key': k, 'Value': v} for k, v in node_type_tags.items()]
        if is_head:
            node_tag.append({
                'Key': 'Name',
                'Value': f'sky-{cluster_name_on_cloud}-head'
            })
        else:
            node_tag.append({
                'Key': 'Name',
                'Value': f'sky-{cluster_name_on_cloud}-worker'
            })
        # Remove AWS internal tags, as they are not allowed to be set by users.
        target_instance_tags = [
            tag for tag in target_instance.tags
            if not tag['Key'].startswith('aws:')
        ]
        resources = [target_instance.id]
        tags_to_create = target_instance_tags + node_tag
        if retry_eventual_consistency:
            # RunInstances can return before the new instance is visible to a
            # subsequent CreateTags request. Retry only that documented
            # eventual-consistency response; all other client errors retain
            # their existing fail-fast behavior.
            _ec2_call_with_retry_on_server_error(
                ec2.meta.client.create_tags,
                additional_retryable_error_codes=(
                    _FRESH_INSTANCE_TAG_RETRYABLE_ERROR_CODES),
                max_attempts=_FRESH_INSTANCE_TAG_MAX_ATTEMPTS,
                Resources=resources,
                Tags=tags_to_create)
        else:
            ec2.meta.client.create_tags(Resources=resources,
                                        Tags=tags_to_create)
        return target_instance.id

    if head_instance_id is None:
        if running_instances:
            head_instance_id = _create_node_tag(running_instances[0])
        elif pending_instances:
            head_instance_id = _create_node_tag(pending_instances[0])

    # TODO(suquark): Maybe in the future, users could adjust the number
    #  of instances dynamically. Then this case would not be an error.
    if config.resume_stopped_nodes and len(exist_instances) > config.count:
        raise RuntimeError(
            'The number of running/stopped/stopping '
            f'instances combined ({len(exist_instances)}) in '
            f'cluster "{cluster_name_on_cloud}" is greater than the '
            f'number requested by the user ({config.count}). '
            'This is likely a resource leak. '
            'Use "sky down" to terminate the cluster.')

    to_start_count = (config.count - len(running_instances) -
                      len(pending_instances))

    if running_instances:
        zone = running_instances[0].placement['AvailabilityZone']

    if to_start_count < 0:
        raise RuntimeError(
            'The number of running+pending instances '
            f'({config.count - to_start_count}) in cluster '
            f'"{cluster_name_on_cloud}" is greater than the number '
            f'requested by the user ({config.count}). '
            'This is likely a resource leak. '
            'Use "sky down" to terminate the cluster.')

    # Try to reuse previously stopped nodes with compatible configs
    if config.resume_stopped_nodes and to_start_count > 0 and (
            stopping_instances or stopped_instances):
        time_start = time.time()
        if stopping_instances:
            plural = 's' if len(stopping_instances) > 1 else ''
            verb = 'are' if len(stopping_instances) > 1 else 'is'
            logger.warning(
                f'Instance{plural} {stopping_instances} {verb} still in '
                'STOPPING state on AWS. It can only be resumed after it is '
                'fully STOPPED. Waiting ...')
        while (stopping_instances and
               to_start_count > len(stopped_instances) and
               time.time() - time_start < _RESUME_INSTANCE_TIMEOUT):
            inst = stopping_instances.pop(0)
            with pool.ThreadPool(processes=1) as pool_:
                # wait_until_stopped() is a blocking call, and sometimes it can
                # take significant time to return due to AWS keeping the
                # instance in STOPPING state. We add a timeout for it to make
                # SkyPilot more responsive.
                fut = pool_.apply_async(inst.wait_until_stopped)
                per_instance_time_start = time.time()
                while (time.time() - per_instance_time_start
                       < _RESUME_PER_INSTANCE_TIMEOUT):
                    if fut.ready():
                        fut.get()
                        break
                    time.sleep(1)
                else:
                    logger.warning(
                        f'Instance {inst.id} is still in stopping state '
                        f'(Timeout: {_RESUME_PER_INSTANCE_TIMEOUT}). '
                        'Retrying ...')
                    stopping_instances.append(inst)
                    time.sleep(5)
                    continue
            stopped_instances.append(inst)
        if stopping_instances and to_start_count > len(stopped_instances):
            msg = ('Timeout for waiting for existing instances '
                   f'{stopping_instances} in STOPPING state to '
                   'be STOPPED before restarting them. Please try again later.')
            logger.error(msg)
            raise RuntimeError(msg)

        resumed_instances = stopped_instances[:to_start_count]
        resumed_instances.sort(key=lambda x: x.id)
        resumed_instance_ids = [t.id for t in resumed_instances]
        logger.debug(f'Resuming stopped instances {resumed_instance_ids}.')
        _ec2_call_with_retry_on_server_error(
            ec2_fail_fast.meta.client.start_instances,
            InstanceIds=resumed_instance_ids,
            log_level=logging.WARNING)
        if tags:
            # empty tags will result in error in the API call
            ec2.meta.client.create_tags(Resources=resumed_instance_ids,
                                        Tags=_format_tags(tags))
            for inst in resumed_instances:
                inst.tags = _format_tags(tags)  # sync the tags info
        placement_zone = resumed_instances[0].placement['AvailabilityZone']
        if zone is None:
            zone = placement_zone
        elif zone != placement_zone:
            logger.warning(f'Resumed instances are in zone {placement_zone}, '
                           f'while previous instances are in zone {zone}.')
        to_start_count -= len(resumed_instances)

        if head_instance_id is None:
            head_instance_id = _create_node_tag(resumed_instances[0])

    if to_start_count > 0:
        target_reservation_names = (config.node_config.get(
            'CapacityReservationSpecification',
            {}).get('CapacityReservationTarget',
                    {}).get('CapacityReservationId', []))
        created_instances: list[Any] = []
        successful_create_calls = 0

        def _create_fresh_instances(node_config: dict[str, Any],
                                    count: int) -> list:
            nonlocal successful_create_calls
            try:
                instances = _create_instances(
                    ec2_fail_fast,
                    cluster_name_on_cloud,
                    node_config,
                    tags,
                    count,
                    associate_public_ip_address=(
                        not config.provider_config['use_internal_ips']),
                    max_efa_interfaces=max_efa_interfaces,
                    is_single_zone_request=is_single_zone_request,
                    provider_create_idempotency_token=(
                        config.provider_create_idempotency_token))
            except Exception as error:
                provider_negative_ack = _promote_provider_negative_ack(
                    error,
                    request_aws_identity=request_aws_identity,
                    request_subnet_availability_zones=(
                        request_subnet_availability_zones),
                    region=region,
                    availability_zone=exact_availability_zone,
                    cluster_name_on_cloud=cluster_name_on_cloud,
                    requested_count=config.count,
                    instance_type=config.node_config.get('InstanceType'),
                    initial_nonterminated_instance_ids=(
                        initial_nonterminated_instance_ids),
                    resumed_instance_ids=resumed_instance_ids,
                    created_instance_ids=[
                        instance.id for instance in created_instances
                    ],
                    successful_create_calls=successful_create_calls,
                    provider_create_idempotency_token=(
                        config.provider_create_idempotency_token),
                    provider_create_account_id=(
                        config.provider_create_account_id))
                if provider_negative_ack is not None:
                    rejected = common.ProviderCreateRejectedError(str(error))
                    rejected.provider_negative_ack = provider_negative_ack
                    rejected.requested_count = config.count
                    provider_errors = getattr(error, 'errors', None)
                    if isinstance(provider_errors, list):
                        rejected.errors = copy.deepcopy(provider_errors)
                    raise rejected from error
                if (config.provider_create_idempotency_token is not None and
                        not isinstance(
                            error, exceptions.ProviderCreateAmbiguousError)):
                    # The SDK call ran, but the closed whole-invocation
                    # rejection could not be promoted. Keep replaying this
                    # association/token; cleanup or failover could otherwise
                    # delete or duplicate an unobserved successful create.
                    raise instance_requests._provider_create_ambiguous_error(  # pylint: disable=protected-access
                    ) from error
                raise
            successful_create_calls += 1
            return instances

        if target_reservation_names:
            node_config = copy.deepcopy(config.node_config)
            # Clear the capacity reservation specification settings in the
            # original node config, as we will create instances with
            # reservations with specific settings for each reservation.
            node_config['CapacityReservationSpecification'] = {
                'CapacityReservationTarget': {}
            }

            reservations = aws_utils.list_reservations_for_instance_type(
                node_config['InstanceType'], region=region)
            # Filter the reservations by the user-specified ones, because
            # reservations contain 'open' reservations as well, which do not
            # need to explicitly specify in the config for creating instances.
            target_reservations = []
            for r in reservations:
                if (r.targeted and r.name in target_reservation_names):
                    target_reservations.append(r)
            logger.debug(f'Reservations: {reservations}')
            logger.debug(f'Target reservations: {target_reservations}')

            target_reservations_list = sorted(
                target_reservations,
                key=lambda x: x.available_resources,
                reverse=True)
            for r in target_reservations_list:
                if r.available_resources <= 0:
                    # We have sorted the reservations by the available
                    # resources, so if the reservation is not available, the
                    # following reservations are not available either.
                    break
                reservation_count = min(r.available_resources, to_start_count)
                logger.debug(f'Creating {reservation_count} instances '
                             f'with reservation {r.name}')
                node_config['CapacityReservationSpecification'][
                    'CapacityReservationTarget'] = {
                        'CapacityReservationId': r.name
                    }
                if r.type == aws_utils.ReservationType.BLOCK:
                    # Capacity block reservations needs to specify the market
                    # type during instance creation.
                    node_config['InstanceMarketOptions'] = {
                        'MarketType': aws_utils.ReservationType.BLOCK.value
                    }
                created_reserved_instances = _create_fresh_instances(
                    node_config, reservation_count)
                created_instances.extend(created_reserved_instances)
                to_start_count -= reservation_count
                if to_start_count <= 0:
                    break

        # TODO(suquark): If there are existing instances (already running or
        #  resumed), then we cannot guarantee that they will be in the same
        #  availability zone (when there are multiple zones specified).
        #  This is a known issue before.

        if to_start_count > 0:
            # Remove the capacity reservation specification from the node config
            # as we have already created the instances with the reservations.
            config.node_config.get('CapacityReservationSpecification',
                                   {}).pop('CapacityReservationTarget', None)
            created_remaining_instances = _create_fresh_instances(
                config.node_config, to_start_count)

            created_instances.extend(created_remaining_instances)
        created_instances.sort(key=lambda x: x.id)

        created_instance_ids = [n.id for n in created_instances]
        placement_zone = created_instances[0].placement['AvailabilityZone']
        if zone is None:
            zone = placement_zone
        elif zone != placement_zone:
            logger.warning('Newly created instances are in zone '
                           f'{placement_zone}, '
                           f'while previous instances are in zone {zone}.')

        # NOTE: we only create worker tags for newly started nodes, because
        # the worker tag is a legacy feature, so we would not care about
        # more corner cases.
        if head_instance_id is None:
            head_instance_id = _create_node_tag(created_instances[0],
                                                retry_eventual_consistency=True)
            for inst in created_instances[1:]:
                _create_node_tag(inst,
                                 is_head=False,
                                 retry_eventual_consistency=True)
        else:
            for inst in created_instances:
                _create_node_tag(inst,
                                 is_head=False,
                                 retry_eventual_consistency=True)

    assert head_instance_id is not None
    fresh_identity = None
    try:
        fresh_identity = _capture_fresh_instance_identity(
            request_session,
            region=region,
            created_instance_ids=created_instance_ids,
            expected_aws_account_id=config.provider_create_account_id)
    except Exception as error:  # pylint: disable=broad-except
        # Provisioning itself succeeded. Optional exact fresh-instance evidence
        # is fail-closed and must not make the ordinary launch fail.
        logger.debug('Fresh AWS identity evidence is unavailable: '
                     f'{common_utils.format_exception(error)}')
    return common.ProvisionRecord(provider_name='aws',
                                  region=region,
                                  zone=zone,
                                  cluster_name=cluster_name_on_cloud,
                                  head_instance_id=head_instance_id,
                                  resumed_instance_ids=resumed_instance_ids,
                                  created_instance_ids=created_instance_ids,
                                  fresh_aws_instance_identity=fresh_identity)


def _filter_instances(ec2: 'mypy_boto3_ec2.ServiceResource',
                      filters: list['ec2_type_defs.FilterTypeDef'],
                      included_instances: list[str] | None,
                      excluded_instances: list[str] | None):
    instances = ec2.instances.filter(Filters=filters)
    if included_instances is not None and excluded_instances is not None:
        raise ValueError('"included_instances" and "exclude_instances"'
                         'cannot be specified at the same time.')
    if included_instances is not None:
        instances = instances.filter(InstanceIds=included_instances)
    elif excluded_instances is not None:
        included_instances = []
        for inst in list(instances):
            if inst.id not in excluded_instances:
                included_instances.append(inst.id)
        instances = instances.filter(InstanceIds=included_instances)
    return instances


# TODO(suquark): Does it make sense to not expose this and always assume
# non_terminated_only=True?
# Will there be callers who would want this to be False?
# stop() and terminate() for example already implicitly assume non-terminated.
@common_utils.retry
def query_instances(
    cluster_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    non_terminated_only: bool = True,
    retry_if_missing: bool = False,
) -> dict[str, tuple[Optional['status_lib.ClusterStatus'], str | None]]:
    """See sky/provision/__init__.py"""
    del cluster_name, retry_if_missing  # unused
    assert provider_config is not None, (cluster_name_on_cloud, provider_config)
    region = provider_config['region']
    ec2 = _default_ec2_resource(region)
    filters = _cluster_name_filter(cluster_name_on_cloud)
    instances = _filter_instances(ec2,
                                  filters,
                                  included_instances=None,
                                  excluded_instances=None)
    status_map = {
        'pending': status_lib.ClusterStatus.INIT,
        'running': status_lib.ClusterStatus.UP,
        # TODO(zhwu): stopping and shutting-down could occasionally fail
        # due to internal errors of AWS. We should cover that case.
        'stopping': status_lib.ClusterStatus.STOPPED,
        'stopped': status_lib.ClusterStatus.STOPPED,
        'shutting-down': None,
        'terminated': None,
    }
    statuses: dict[str, tuple[status_lib.ClusterStatus | None, str | None]] = {}
    for inst in instances:
        status = status_map[inst.state['Name']]
        if non_terminated_only and status is None:
            continue
        statuses[inst.id] = (status, None)
    return statuses


def query_instances_batch(
    queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
    *,
    deadline_monotonic: float,
) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
    """Read one bounded EC2 inventory per region for a Serve generation."""
    queries_by_region: dict[
        str, list[provider_facets.InstanceStatusInventoryQueryV1]] = {}
    invalid: dict[str, str] = {}
    for query in queries:
        region = query.provider_config.get('region')
        if not isinstance(region, str) or not region:
            invalid[query.query_id] = 'AWS provider config has no region'
            continue
        queries_by_region.setdefault(region, []).append(query)

    observations: dict[
        str, provider_facets.InstanceStatusInventoryObservationV1] = {}
    for query_id, error in invalid.items():
        observations[query_id] = (
            provider_facets.InstanceStatusInventoryObservationV1(
                query_id=query_id,
                disposition=(provider_facets.
                             InstanceStatusInventoryDispositionV1.UNKNOWN),
                error=error))

    status_map = {
        'pending': status_lib.ClusterStatus.INIT,
        'running': status_lib.ClusterStatus.UP,
        'stopping': status_lib.ClusterStatus.STOPPED,
        'stopped': status_lib.ClusterStatus.STOPPED,
        'shutting-down': None,
        'terminated': None,
    }
    for region, partition in queries_by_region.items():
        query_ids_by_cluster: dict[str, list[str]] = {}
        for query in partition:
            query_ids_by_cluster.setdefault(query.cluster_name_on_cloud,
                                            []).append(query.query_id)
        entries_by_query_id: dict[
            str, list[provider_facets.InstanceStatusInventoryEntryV1]] = {
                query.query_id: [] for query in partition
            }
        try:
            remaining = deadline_monotonic - time.monotonic()
            if remaining < (_SERVE_INVENTORY_CONNECT_TIMEOUT_SECONDS +
                            _SERVE_INVENTORY_READ_TIMEOUT_SECONDS):
                raise TimeoutError(
                    'AWS batch inventory exhausted its aggregate deadline')
            session = aws.session_with_client_defaults(
                connect_timeout=_SERVE_INVENTORY_CONNECT_TIMEOUT_SECONDS,
                read_timeout=_SERVE_INVENTORY_READ_TIMEOUT_SECONDS,
                total_max_attempts=_SERVE_INVENTORY_TOTAL_MAX_ATTEMPTS,
                profile=aws.get_workspace_profile())
            ec2_client = session.client('ec2', region_name=region)
            request: dict[str, Any] = {
                # Inventory every SkyPilot cluster in the region once and
                # project only requested cluster names locally.  Sending up
                # to 800 exact tag values exceeds EC2 request-size limits and
                # couples correctness to a provider-specific filter ceiling.
                'Filters': [{
                    'Name': 'tag-key',
                    'Values': [constants.TAG_RAY_CLUSTER_NAME],
                }, {
                    'Name': 'instance-state-name',
                    'Values': ['pending', 'running', 'stopping', 'stopped'],
                }],
                'MaxResults': 1000,
            }
            seen_instance_ids: set[str] = set()
            while True:
                response = ec2_client.describe_instances(**request)
                for reservation in response.get('Reservations', []):
                    for instance in reservation.get('Instances', []):
                        instance_id = instance.get('InstanceId')
                        state = instance.get('State', {}).get('Name')
                        tags = instance.get('Tags')
                        if (not isinstance(instance_id, str) or
                                state not in status_map or
                                not isinstance(tags, list)):
                            raise ValueError(
                                'EC2 returned malformed instance inventory')
                        cluster_name = next(
                            (tag.get('Value')
                             for tag in tags
                             if tag.get('Key') == constants.TAG_RAY_CLUSTER_NAME
                            ), None)
                        if not isinstance(cluster_name, str):
                            continue
                        matching_query_ids = query_ids_by_cluster.get(
                            cluster_name, [])
                        if not matching_query_ids:
                            continue
                        if instance_id in seen_instance_ids:
                            raise ValueError(
                                'EC2 returned duplicate instance inventory')
                        seen_instance_ids.add(instance_id)
                        status = status_map[state]
                        if status is None:
                            continue
                        entry = (provider_facets.InstanceStatusInventoryEntryV1(
                            instance_id=instance_id, status=status))
                        for query_id in matching_query_ids:
                            entries_by_query_id[query_id].append(entry)
                next_token = response.get('NextToken')
                if not next_token:
                    break
                if (deadline_monotonic - time.monotonic()
                        < (_SERVE_INVENTORY_CONNECT_TIMEOUT_SECONDS +
                           _SERVE_INVENTORY_READ_TIMEOUT_SECONDS)):
                    raise TimeoutError(
                        'AWS batch inventory exhausted its aggregate deadline')
                request['NextToken'] = next_token
        except Exception as error:  # pylint: disable=broad-except
            message = f'{type(error).__name__}: {error}'
            for query in partition:
                observations[query.query_id] = (
                    provider_facets.InstanceStatusInventoryObservationV1(
                        query_id=query.query_id,
                        disposition=(
                            provider_facets.
                            InstanceStatusInventoryDispositionV1.UNKNOWN),
                        error=message))
            continue
        for query in partition:
            entries = tuple(
                sorted(entries_by_query_id[query.query_id],
                       key=lambda entry: entry.instance_id))
            observations[query.query_id] = (
                provider_facets.InstanceStatusInventoryObservationV1(
                    query_id=query.query_id,
                    disposition=(provider_facets.
                                 InstanceStatusInventoryDispositionV1.OBSERVED),
                    entries=entries))

    return tuple(observations[query.query_id] for query in queries)


def stop_instances(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    worker_only: bool = False,
) -> None:
    """See sky/provision/__init__.py"""
    assert provider_config is not None, (cluster_name_on_cloud, provider_config)
    region = provider_config['region']
    ec2 = _default_ec2_resource(region)
    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': 'instance-state-name',
            'Values': ['pending', 'running'],
        },
        *_cluster_name_filter(cluster_name_on_cloud),
    ]
    if worker_only:
        filters.append({
            'Name': f'tag:{constants.TAG_RAY_NODE_KIND}',
            'Values': ['worker'],
        })
    instances = _filter_instances(ec2,
                                  filters,
                                  included_instances=None,
                                  excluded_instances=None)
    instances.stop()
    # TODO(suquark): Currently, the implementation of GCP and Azure will
    #  wait util the cluster is fully terminated, while other clouds just
    #  trigger the termination process (via http call) and then return.
    #  It's not clear that which behavior should be expected. We will not
    #  wait for the termination for now, since this is the default behavior
    #  of most cloud implementations (including AWS).


def terminate_instances(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    worker_only: bool = False,
) -> None:
    """See sky/provision/__init__.py"""
    assert provider_config is not None, (cluster_name_on_cloud, provider_config)
    region = provider_config['region']
    sg_name = provider_config['security_group']['GroupName']
    managed_by_skypilot = provider_config['security_group'].get(
        'ManagedBySkyPilot', True)
    ec2 = _default_ec2_resource(region)
    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': 'instance-state-name',
            # exclude 'shutting-down' or 'terminated' states
            'Values': ['pending', 'running', 'stopping', 'stopped'],
        },
        *_cluster_name_filter(cluster_name_on_cloud),
    ]
    if worker_only:
        filters.append({
            'Name': f'tag:{constants.TAG_RAY_NODE_KIND}',
            'Values': ['worker'],
        })
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Instance
    instances = _filter_instances(ec2,
                                  filters,
                                  included_instances=None,
                                  excluded_instances=None)
    instance_list = list(instances)
    default_sg = aws_config.get_security_group_from_vpc_id(
        ec2, _get_vpc_id(provider_config),
        aws_cloud.DEFAULT_SECURITY_GROUP_NAME)
    if aws_cloud.is_shared_default_security_group(sg_name):
        # Case 1: The default SG is used, we don't need to ensure instance are
        # terminated.
        instances.terminate()
    elif not managed_by_skypilot:
        # Case 2: We are not managing the non-default sg. We don't need to
        # ensure instances are terminated.
        instances.terminate()
    elif (managed_by_skypilot and default_sg is not None):
        # Case 3: We are managing the non-default sg. The default SG exists
        # so we can move the instances to the default SG and terminate them
        # without blocking.

        # Make this multithreaded: modify all instances' SGs in parallel.
        def modify_instance_sg(instance):
            assert default_sg is not None  # Type narrowing for mypy
            instance.modify_attribute(Groups=[default_sg.id])
            logger.debug(f'Instance {instance.id} modified to use default SG:'
                         f'{default_sg.id} for quick deletion.')

        with pool.ThreadPool() as thread_pool:
            thread_pool.map(modify_instance_sg, instances)
            thread_pool.close()
            thread_pool.join()

        instances.terminate()
    else:
        # Case 4: We are managing the non-default sg. The default SG does not
        # exist. We must block on instance termination so that we can
        # delete the security group.
        instances.terminate()
        for instance in instance_list:
            instance.wait_until_terminated()

    # TODO(suquark): Currently, the implementation of GCP and Azure will
    #  wait util the cluster is fully terminated, while other clouds just
    #  trigger the termination process (via http call) and then return.
    #  It's not clear that which behavior should be expected. We will not
    #  wait for the termination for now, since this is the default behavior
    #  of most cloud implementations (including AWS).


def _maybe_move_to_new_sg(
    instance: Any,
    expected_sg: Any,
) -> None:
    """Move the instance to the new security group if needed.

    If the instance is already in the expected security group, do nothing.
    Otherwise, move it to the expected security group.
    Our config.py will automatically create a new security group for every
    GroupName specified in the provider config. But it won't change the
    security group of an existing cluster, so we need to move it to the
    expected security group.
    """
    sg_names = [sg['GroupName'] for sg in instance.security_groups]
    if len(sg_names) != 1:
        logger.warning(
            f'Expected 1 security group for instance {instance.id}, '
            f'but found {len(sg_names)}. Skip creating security group.')
        return
    sg_name = sg_names[0]
    if sg_name == expected_sg.group_name:
        return
    instance.modify_attribute(Groups=[expected_sg.id])


def _authorize_ingress_with_duplicate_retry(
    security_group: Any,
    permissions: list[dict[str, Any]],
) -> None:
    """Authorize ingress while preserving missing rules across duplicate races."""
    try:
        security_group.authorize_ingress(IpPermissions=permissions)
    except aws.botocore_exceptions().ClientError as exc:
        error_code = exc.response.get('Error', {}).get('Code')
        if error_code != 'InvalidPermission.Duplicate':
            raise
        # The security group snapshot can be stale when an interrupted or
        # concurrent reconciliation has already committed one of these rules.
        # Retry each permission independently so a duplicate does not hide
        # other permissions that are still missing.
        logger.debug('Security group rules were added concurrently; '
                     'retrying permissions independently.')
        for permission in permissions:
            try:
                security_group.authorize_ingress(IpPermissions=[permission])
            except aws.botocore_exceptions().ClientError as retry_exc:
                retry_error_code = retry_exc.response.get('Error',
                                                          {}).get('Code')
                if retry_error_code != 'InvalidPermission.Duplicate':
                    raise


def _ingress_source_ranges(provider_config: dict[str, Any] | None) -> list[str]:
    """Source CIDRs for cluster ingress, defaulting to the whole internet.

    The default preserves SkyPilot's historical behaviour. It is a poor default
    for a workload with no authentication of its own, which is why it is now
    configurable via ``aws.ingress_source_ranges``.
    """
    if not provider_config:
        return [_DEFAULT_INGRESS_SOURCE_RANGE]
    configured = provider_config.get('ingress_source_ranges')
    if not configured:
        return [_DEFAULT_INGRESS_SOURCE_RANGE]
    return list(configured)


def open_ports(
    cluster_name_on_cloud: str,
    ports: list[str],
    provider_config: dict[str, Any] | None = None,
) -> None:
    """See sky/provision/__init__.py"""
    assert provider_config is not None, cluster_name_on_cloud
    region = provider_config['region']
    ec2 = _default_ec2_resource(region)
    sg_name = provider_config['security_group']['GroupName']
    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': 'instance-state-name',
            # exclude 'shutting-down' or 'terminated' states
            'Values': ['pending', 'running', 'stopping', 'stopped'],
        },
        *_cluster_name_filter(cluster_name_on_cloud),
    ]
    instances = _filter_instances(ec2,
                                  filters,
                                  included_instances=None,
                                  excluded_instances=None)
    instance_list = list(instances)
    if not instance_list:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Instance with cluster name '
                             f'{cluster_name_on_cloud} not found.')
    sg = aws_config.get_security_group_from_vpc_id(ec2,
                                                   _get_vpc_id(provider_config),
                                                   sg_name)
    if sg is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Cannot find new security group '
                             f'{sg_name}. Please check the log '
                             'above and try again.')
    # For multinode cases, we need to change the SG for all instances.
    for instance in instance_list:
        _maybe_move_to_new_sg(instance, sg)

    existing_ports: set[int] = set()
    for existing_rule in sg.ip_permissions:
        # Skip any non-tcp rules or if all traffic (-1) is specified.
        if existing_rule['IpProtocol'] not in ['tcp', '-1']:
            continue
        # Skip any rules that don't have a FromPort or ToPort.
        if 'FromPort' in existing_rule and 'ToPort' in existing_rule:
            existing_ports.update(
                range(existing_rule['FromPort'], existing_rule['ToPort'] + 1))
        elif existing_rule['IpProtocol'] == '-1':
            # For AWS, IpProtocol = -1 means all traffic
            all_traffic_allowed: bool = False
            for group_pairs in existing_rule['UserIdGroupPairs']:
                if group_pairs['GroupId'] != sg.id:
                    # We skip the port opening when the rule allows access from
                    # other security groups, as that is likely added by a user
                    # manually and satisfy their requirement.
                    # The security group created by SkyPilot allows all traffic
                    # from the same security group, which should not be skipped.
                    existing_ports.add(-1)
                    all_traffic_allowed = True
                    break
            if all_traffic_allowed:
                break

    ports_to_open = []
    # Do not need to open any ports when all traffic is already allowed.
    if -1 not in existing_ports:
        ports_to_open = resources_utils.port_set_to_ranges(
            resources_utils.port_ranges_to_set(ports) - existing_ports)

    # Defaults to the whole internet, matching the historical behaviour. A
    # deployment whose workload behind these ports has no authentication of its
    # own should narrow `aws.ingress_source_ranges` to the control plane's
    # egress address; otherwise a requested port is reachable by anyone.
    source_ranges = _ingress_source_ranges(provider_config)
    ip_permissions = []
    for port in ports_to_open:
        if port.isdigit():
            from_port = to_port = port
        else:
            from_port, to_port = port.split('-')
        ip_permissions.append({
            'FromPort': int(from_port),
            'ToPort': int(to_port),
            'IpProtocol': 'tcp',
            'IpRanges': [{
                'CidrIp': cidr
            } for cidr in source_ranges],
        })

    # For the case when every new ports is already opened.
    if ip_permissions:
        # Filter out any permissions that already exist in the security group
        existing_permissions = set()
        wanted_ranges = set(source_ranges)
        for rule in sg.ip_permissions:
            if rule['IpProtocol'] == 'tcp':
                for ip_range in rule.get('IpRanges', []):
                    # Compare against the configured ranges: a rule that opens
                    # a port to a DIFFERENT CIDR must not count as already
                    # satisfying this one.
                    if ip_range.get('CidrIp') in wanted_ranges:
                        existing_permissions.add(
                            (rule['FromPort'], rule['ToPort']))

        # Remove any permissions that already exist
        filtered_permissions = []
        for perm in ip_permissions:
            if (perm['FromPort'], perm['ToPort']) not in existing_permissions:
                filtered_permissions.append(perm)

        if filtered_permissions:
            _authorize_ingress_with_duplicate_retry(sg, filtered_permissions)


def cleanup_ports(
    cluster_name_on_cloud: str,
    ports: list[str],
    provider_config: dict[str, Any] | None = None,
) -> None:
    """See sky/provision/__init__.py"""
    del ports  # Unused.
    assert provider_config is not None, cluster_name_on_cloud
    region = provider_config['region']
    ec2 = _default_ec2_resource(region)
    sg_name = provider_config['security_group']['GroupName']
    managed_by_skypilot = provider_config['security_group'].get(
        'ManagedBySkyPilot', True)
    if (aws_cloud.is_shared_default_security_group(sg_name) or
            not managed_by_skypilot):
        # 1) Using default AWS SG or 2) the SG is specified by the user.
        # We only want to delete the SG that is dedicated to this cluster (i.e.,
        # this cluster have opened some ports).
        return
    sg = aws_config.get_security_group_from_vpc_id(ec2,
                                                   _get_vpc_id(provider_config),
                                                   sg_name)
    if sg is None:
        logger.warning(
            'Find security group failed. Skip cleanup security group.')
        return
    backoff = common_utils.Backoff()
    for _ in range(BOTO_DELETE_MAX_ATTEMPTS):
        try:
            sg.delete()
        except aws.botocore_exceptions().ClientError as e:
            if _DEPENDENCY_VIOLATION_PATTERN.findall(str(e)):
                logger.debug(
                    f'Security group {sg_name} is still in use. Retry.')
                time.sleep(backoff.current_backoff())
                continue
            raise
        return
    logger.warning(
        f'Cannot delete security group {sg_name} after '
        f'{BOTO_DELETE_MAX_ATTEMPTS} attempts. Please delete it manually.')


def wait_instances(region: str, cluster_name_on_cloud: str,
                   state: status_lib.ClusterStatus | None) -> None:
    """See sky/provision/__init__.py"""
    # TODO(suquark): unify state for different clouds
    # possible exceptions: https://github.com/boto/boto3/issues/176
    ec2 = _default_ec2_resource(region)
    client = ec2.meta.client

    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': f'tag:{constants.TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name_on_cloud],
        },
    ]

    if state == status_lib.ClusterStatus.UP:
        # NOTE: there could be a terminated/terminating AWS cluster with
        # the same cluster name.
        # Wait the cluster result in errors (cannot wait for 'terminated').
        # So here we exclude terminated/terminating instances.
        filters.append({
            'Name': 'instance-state-name',
            'Values': ['pending', 'running'],
        })
    elif state == status_lib.ClusterStatus.STOPPED:
        filters.append({
            'Name': 'instance-state-name',
            'Values': ['stopping', 'stopped'],
        })

    # boto3 waiter would wait for an empty list forever
    instances = list(ec2.instances.filter(Filters=filters))
    logger.debug(instances)
    if not instances:
        raise RuntimeError(
            f'No instances found for cluster {cluster_name_on_cloud}.')

    waiter: botowaiter.Waiter
    if state == status_lib.ClusterStatus.UP:
        waiter = client.get_waiter('instance_running')
    elif state == status_lib.ClusterStatus.STOPPED:
        waiter = client.get_waiter('instance_stopped')
    elif state is None:
        waiter = client.get_waiter('instance_terminated')
    else:
        raise ValueError(f'Unsupported state to wait: {state}')
    # See https://github.com/boto/botocore/blob/develop/botocore/waiter.py
    waiter.wait(WaiterConfig={'Delay': 5, 'MaxAttempts': 120}, Filters=filters)


def get_cluster_info(
        region: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None) -> common.ClusterInfo:
    """See sky/provision/__init__.py"""
    ec2 = _default_ec2_resource(region)
    filters: list[ec2_type_defs.FilterTypeDef] = [
        {
            'Name': 'instance-state-name',
            'Values': ['running'],
        },
        {
            'Name': f'tag:{constants.TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name_on_cloud],
        },
    ]
    running_instances = list(ec2.instances.filter(Filters=filters))
    head_instance_id = _get_head_instance_id(running_instances)

    instances = {}
    for inst in running_instances:
        tags = [(t['Key'], t['Value']) for t in inst.tags] if inst.tags else []
        # sort tags by key to support deterministic unit test stubbing
        tags.sort(key=lambda x: x[0])
        tags_dict = dict(tags)
        # Get instance name from Name tag for dashboard display
        instance_name = tags_dict.get('Name')
        instances[inst.id] = [
            common.InstanceInfo(
                instance_id=inst.id,
                internal_ip=inst.private_ip_address,
                external_ip=inst.public_ip_address,
                tags=tags_dict,
                node_name=instance_name,
            )
        ]
    instances = dict(sorted(instances.items(), key=lambda x: x[0]))
    return common.ClusterInfo(
        instances=instances,
        head_instance_id=head_instance_id,
        provider_name='aws',
        provider_config=provider_config,
    )


def _get_vpc_id(provider_config: dict[str, Any]) -> str:
    region = provider_config['region']
    ec2 = _default_ec2_resource(provider_config['region'])
    if 'vpc_name' in provider_config:
        return aws_config.get_vpc_id_by_name(ec2, provider_config['vpc_name'],
                                             region)
    else:
        # Retrieve the default VPC name from the region.
        response = ec2.meta.client.describe_vpcs(Filters=[{
            'Name': 'isDefault',
            'Values': ['true']
        }])
        if len(response['Vpcs']) == 0:
            raise ValueError(f'No default VPC found in region {region}')
        elif len(response['Vpcs']) > 1:
            raise ValueError(f'Multiple default VPCs found in region {region}')
        else:
            return response['Vpcs'][0]['VpcId']
