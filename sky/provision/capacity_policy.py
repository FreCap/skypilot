"""Provider capacity classification and exact cache-key policy."""
from collections.abc import Iterable
import re
from typing import Any, Optional

from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky import skypilot_config
from sky.provision import capacity_cache
from sky.provision import common as provision_common

# AWS error codes used to distinguish physical capacity from regional quota.
_CAPACITY_ERROR_CODES = frozenset({'InsufficientInstanceCapacity'})
_QUOTA_ERROR_CODES = frozenset({
    'VcpuLimitExceeded',
    'MaxSpotInstanceCountExceeded',
    'InstanceLimitExceeded',
})
_AWS_RUN_INSTANCES_NEGATIVE_ACK_HTTP_STATUSES = {
    # EC2 documents InsufficientInstanceCapacity as a server error.  Its Query
    # API response is the named atomic rejection, not a generic retryable 5xx.
    'InsufficientInstanceCapacity': frozenset({500}),
    'VcpuLimitExceeded': frozenset({400}),
    'MaxSpotInstanceCountExceeded': frozenset({400}),
    'InstanceLimitExceeded': frozenset({400}),
}
_AWS_RUN_INSTANCES_CLIENT_TOKEN_RE = re.compile(r'[0-9a-f]{64}')
_PROVIDER_QUOTA_ERROR_CODES = _QUOTA_ERROR_CODES | frozenset({
    'QUOTA_EXCEEDED',
    'quotaExceeded',
    'type.googleapis.com/google.rpc.QuotaFailure',
    # The only producer, `sky/provision/gcp/tpu_node.py`, raises this for TPU
    # quota exhaustion, not for an exhausted pool.
    'RESOURCE_EXHAUSTED',
})
# Codes that identify physical capacity exhaustion across providers, used to
# label recorded placement outcomes. This is deliberately wider than
# `_CAPACITY_ERROR_CODES`, which stays AWS-only because it also gates the
# AWS capacity cache. UNSUPPORTED_OPERATION is excluded: the failover zone
# blocker treats it as capacity-like, but it is observed on preemption during
# creation rather than on an exhausted pool.
# NOTE(fcapponi): GCP also reports zonal TPU exhaustion as the bare numeric
# operation code 8, which `_provider_error_codes` stringifies to '8'. That
# token is too collision-prone to put in a cross-provider set; recognizing it
# needs provider-scoped normalization first.
_PLACEMENT_CAPACITY_ERROR_CODES = _CAPACITY_ERROR_CODES | frozenset({
    'ZONE_RESOURCE_POOL_EXHAUSTED',
    'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
    'insufficientCapacity',
    'CapacityExceeded',
})
# Codes that report that a request failed without saying why. They are dropped
# before classification so that a provider which pairs a summary code with the
# causal one still classifies, while a genuinely unknown code keeps the
# conservative outcome.
_NEUTRAL_PLACEMENT_ERROR_CODES = frozenset({'VM_MIN_COUNT_NOT_REACHED'})
# Cache-gating code sets, scoped per provider. These decide whether a failure
# suppresses a later launch, so each provider only ever matches its own codes.
_GCP_CAPACITY_ERROR_CODES = frozenset({
    'ZONE_RESOURCE_POOL_EXHAUSTED',
    'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
    'insufficientCapacity',
    'CapacityExceeded',
})
_GCP_QUOTA_ERROR_CODES = frozenset({
    'QUOTA_EXCEEDED',
    'quotaExceeded',
    'RESOURCE_EXHAUSTED',
    'type.googleapis.com/google.rpc.QuotaFailure',
})
# Terminal optimizer exhaustion can nest per-location failover histories.
# Bound defensive traversal so malformed or cyclic exception graphs remain
# conservatively unclassified instead of consuming unbounded controller work.
_MAX_TERMINAL_FAILOVER_HISTORY_DEPTH = 32
_MAX_TERMINAL_FAILOVER_HISTORY_NODES = 1024

_PROVIDER_NEGATIVE_ACK_ATTR = 'provider_negative_ack'
_PROVIDER_NEGATIVE_ACK_TOP_LEVEL_KEYS = frozenset({
    'schema_version',
    'provider',
    'operation',
    'reason',
    'aws_account_id',
    'aws_principal_arn',
    'cluster_name_on_cloud',
    'requested_count',
    'market',
    'instance_type',
    'region',
    'availability_zone',
    'client_token',
    'invocations',
})
_PROVIDER_NEGATIVE_ACK_INVOCATION_KEYS = frozenset({
    'region',
    'availability_zone',
    'initial_nonterminated_instance_ids',
    'resumed_instance_ids',
    'created_instance_ids',
    'successful_create_calls',
    'ambiguous_create_calls',
    'create_call_count',
    'attempts',
})
_PROVIDER_NEGATIVE_ACK_ATTEMPT_KEYS = frozenset({
    'provider_request_id',
    'error_code',
    'reason',
    'http_status_code',
    'aws_account_id',
    'aws_principal_arn',
    'region',
    'availability_zone',
    'subnet_id',
    'market',
    'instance_type',
    'cluster_name_on_cloud',
    'min_count',
    'max_count',
    'capacity_reservation_id',
    'client_token',
})


def valid_aws_run_instances_negative_ack_http_status(
        error_code: object, http_status_code: object) -> bool:
    """Whether one exact EC2 code/status pair is a typed rejection."""
    if not isinstance(error_code, str) or type(http_status_code) is not int:
        return False
    return http_status_code in _AWS_RUN_INSTANCES_NEGATIVE_ACK_HTTP_STATUSES.get(
        error_code, ())


def valid_aws_run_instances_client_token(value: object) -> bool:
    """Whether a receipt carries SkyPilot's closed EC2 token encoding."""
    return (isinstance(value, str) and
            _AWS_RUN_INSTANCES_CLIENT_TOKEN_RE.fullmatch(value) is not None)


def _iter_error_chain(error: BaseException) -> Iterable[BaseException]:
    """Yields explicit exception causes, excluding implicit context."""
    seen: set[int] = set()
    exc: BaseException | None = error
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__


def _provider_error_codes(error: BaseException) -> list[str]:
    """Return structured provider codes from the explicit exception chain."""
    codes: list[str] = []
    for exc in _iter_error_chain(error):
        errors = getattr(exc, 'errors', None)
        if isinstance(errors, list):
            codes.extend(
                str(item['code'])
                for item in errors
                if isinstance(item, dict) and item.get('code') is not None)
        response = getattr(exc, 'response', None)
        if not isinstance(response, dict):
            continue
        error_payload = response.get('Error')
        if isinstance(error_payload, dict):
            code = error_payload.get('Code')
            if code is not None:
                codes.append(str(code))
    return codes


def _classify_capacity_error(cloud: 'clouds.Cloud',
                             error: BaseException) -> str | None:
    """Classifies a provider failure using structured codes only.

    A provisioner records every failed create attempt on a
    ``ProvisionerError``. A batch is classified only when every code is a known
    capacity/quota code for that provider. Quota dominates a mixed known batch
    because it is regional; any unknown code takes the conservative normal
    failover path.

    Codes are matched per provider so one cloud's capacity code can never
    classify another cloud's failure.
    """
    if isinstance(cloud, clouds.AWS):
        capacity_codes = _CAPACITY_ERROR_CODES
        quota_codes = _QUOTA_ERROR_CODES
    elif isinstance(cloud, clouds.GCP):
        capacity_codes = _GCP_CAPACITY_ERROR_CODES
        quota_codes = _GCP_QUOTA_ERROR_CODES
    else:
        return None
    # GCP pairs the causal code with a `VM_MIN_COUNT_NOT_REACHED` summary that
    # says only that the request failed. Dropping it keeps the all-known check
    # meaningful without weakening it for a genuinely unknown code.
    neutral_codes = (_NEUTRAL_PLACEMENT_ERROR_CODES if isinstance(
        cloud, clouds.GCP) else frozenset())
    codes = [
        code for code in _provider_error_codes(error)
        if code not in neutral_codes
    ]
    known_codes = capacity_codes | quota_codes
    if codes and all(code in known_codes for code in codes):
        # A quota denial is regional and makes sibling-zone attempts for this
        # demand futile, so it dominates an otherwise-known capacity/quota
        # aggregate. Unknown codes remain unclassified and take the normal,
        # conservative failover path.
        if any(code in quota_codes for code in codes):
            return 'quota'
        return 'capacity'
    return None


def _terminal_failover_leaves(
    error: BaseException,
) -> tuple[list[tuple[BaseException, int]], int] | None:
    """Flatten nested terminal failover histories conservatively.

    ``_retry_zones()`` records provider failures in one
    ``ResourcesUnavailableError``. Cross-location optimizer exhaustion wraps
    that error in another terminal history. Preserve path-local ancestry so a
    shared leaf may appear in independent branches while a real history cycle,
    malformed entry, or excessive graph remains unclassified.
    """
    pending: list[tuple[BaseException, frozenset[int],
                        int]] = [(error, frozenset(), 0)]
    leaves: list[tuple[BaseException, int]] = []
    visited = 0
    while pending:
        failure, ancestors, depth = pending.pop()
        visited += 1
        if visited > _MAX_TERMINAL_FAILOVER_HISTORY_NODES:
            return None
        history = None
        if isinstance(failure, exceptions.ResourcesUnavailableError):
            history = failure.failover_history
            # Require the built-in type: a list subclass can override
            # iteration or length and hide an unknown child.
            if type(history) is not list:
                return None
        if history:
            identity = id(failure)
            if (identity in ancestors or
                    depth >= _MAX_TERMINAL_FAILOVER_HISTORY_DEPTH):
                return None
            # Account for already-queued nodes before scanning this fanout.
            # ``len(list)`` is constant-time, so an adversarially wide history
            # is rejected without allocating one pending tuple per child.
            remaining_nodes = (_MAX_TERMINAL_FAILOVER_HISTORY_NODES - visited -
                               len(pending))
            if len(history) > remaining_nodes:
                return None
            next_ancestors = ancestors | {identity}
            for nested in reversed(history):
                if not isinstance(nested, BaseException):
                    return None
                pending.append((nested, next_ancestors, depth + 1))
            continue
        leaves.append((failure, depth))
    return leaves, visited


def _terminal_leaf_cause_nodes(failure: BaseException, *, history_depth: int,
                               remaining_nodes: int) -> int | None:
    """Validate one leaf's explicit cause chain within terminal bounds."""
    seen = {id(failure)}
    cause = failure.__cause__
    cause_nodes = 0
    depth = history_depth
    while cause is not None:
        identity = id(cause)
        cause_nodes += 1
        depth += 1
        if (identity in seen or cause_nodes > remaining_nodes or
                depth > _MAX_TERMINAL_FAILOVER_HISTORY_DEPTH):
            return None
        seen.add(identity)
        # A history-bearing terminal wrapper is an internal attempt node, not
        # a valid member of one leaf's explicit cause chain. Treat this
        # malformed mixed graph conservatively instead of choosing one edge.
        if isinstance(cause, exceptions.ResourcesUnavailableError):
            if type(cause.failover_history) is not list:
                return None
            if cause.failover_history:
                return None
        cause = cause.__cause__
    return cause_nodes


def classify_resources_unavailable_error(
        cloud: 'clouds.Cloud',
        error: exceptions.ResourcesUnavailableError) -> str | None:
    """Classify a terminal failover history using typed provider evidence.

    Every recorded attempt must be recognizable as capacity or quota.  A
    mixed or unstructured history is intentionally left unclassified so
    caller-local placement policy does not bench a healthy location for an
    authentication, networking, throttling, or controller error.
    """
    traversal = _terminal_failover_leaves(error)
    if traversal is None:
        return None
    failures, visited = traversal
    reasons: list[str] = []
    for failure, history_depth in failures:
        cause_nodes = _terminal_leaf_cause_nodes(
            failure,
            history_depth=history_depth,
            remaining_nodes=_MAX_TERMINAL_FAILOVER_HISTORY_NODES - visited)
        if cause_nodes is None:
            return None
        visited += cause_nodes
        reason = _classify_capacity_error(cloud, failure)
        if reason is None:
            return None
        reasons.append(reason)
    if not reasons:
        return None
    return 'quota' if 'quota' in reasons else 'capacity'


def _canonical_provider_negative_ack_attempt(
    value: object,
    *,
    reason: str,
    aws_account_id: str,
    aws_principal_arn: str,
    cluster_name: str,
    requested_count: int,
    market: str,
    instance_type: str,
    region: str,
    availability_zone: str,
    client_token: str,
) -> dict[str, Any] | None:
    if (type(value) is not dict or
            set(value) != _PROVIDER_NEGATIVE_ACK_ATTEMPT_KEYS):
        return None
    attempt = value
    provider_request_id = attempt['provider_request_id']
    error_code = attempt['error_code']
    http_status_code = attempt['http_status_code']
    subnet_id = attempt['subnet_id']
    capacity_reservation_id = attempt['capacity_reservation_id']
    if (not isinstance(provider_request_id, str) or not provider_request_id or
            not isinstance(error_code, str) or not error_code or
            not valid_aws_run_instances_negative_ack_http_status(
                error_code, http_status_code) or
            not isinstance(subnet_id, str) or not subnet_id or
        (capacity_reservation_id is not None and
         (not isinstance(capacity_reservation_id, str) or
          not capacity_reservation_id))):
        return None
    if (attempt['reason'] != reason or
            attempt['aws_account_id'] != aws_account_id or
            attempt['aws_principal_arn'] != aws_principal_arn or
            attempt['region'] != region or
            attempt['availability_zone'] != availability_zone or
            attempt['client_token'] != client_token or
            attempt['market'] != market or
            attempt['instance_type'] != instance_type or
            attempt['cluster_name_on_cloud'] != cluster_name or
            type(attempt['min_count']) is not int or
            attempt['min_count'] != requested_count or
            type(attempt['max_count']) is not int or
            attempt['max_count'] != requested_count):
        return None
    expected_codes = (_CAPACITY_ERROR_CODES
                      if reason == 'capacity' else _QUOTA_ERROR_CODES)
    if error_code not in expected_codes:
        return None
    return {
        'provider_request_id': provider_request_id,
        'error_code': error_code,
        'reason': reason,
        'http_status_code': http_status_code,
        'aws_account_id': aws_account_id,
        'aws_principal_arn': aws_principal_arn,
        'region': region,
        'availability_zone': availability_zone,
        'subnet_id': subnet_id,
        'market': market,
        'instance_type': instance_type,
        'cluster_name_on_cloud': cluster_name,
        'min_count': requested_count,
        'max_count': requested_count,
        'capacity_reservation_id': capacity_reservation_id,
        'client_token': client_token,
    }


def validate_provider_negative_ack(
    receipt: object,
    *,
    cluster_name: str,
    requested_count: int | None = None,
    client_token: str | None = None,
    expected_aws_account_id: str | None = None,
) -> dict[str, Any] | None:
    """Validate and copy a complete provider zero-effect receipt.

    The schema is intentionally closed and composed only of JSON-compatible
    built-in mappings/lists/scalars.  Callers may therefore persist the return
    value and validate it again after restart without retaining exception
    classes or provider SDK objects. ``cluster_name`` is the provider-native
    cloud name used in instance tags, not a user-facing display name.
    """
    if (not isinstance(cluster_name, str) or not cluster_name or
        (requested_count is not None and
         (type(requested_count) is not int or requested_count < 1))):
        return None
    if (type(receipt) is not dict or
            set(receipt) != _PROVIDER_NEGATIVE_ACK_TOP_LEVEL_KEYS):
        return None
    if (type(receipt['schema_version']) is not int or
            receipt['schema_version'] != 1 or receipt['provider'] != 'aws' or
            receipt['operation'] != 'RunInstances' or
            receipt['reason'] not in ('capacity', 'quota') or
            receipt['cluster_name_on_cloud'] != cluster_name or
            receipt['market'] != 'spot'):
        return None
    receipt_count = receipt['requested_count']
    if (type(receipt_count) is not int or receipt_count < 1 or
        (requested_count is not None and receipt_count != requested_count)):
        return None
    reason = receipt['reason']
    aws_account_id = receipt['aws_account_id']
    aws_principal_arn = receipt['aws_principal_arn']
    market = receipt['market']
    instance_type = receipt['instance_type']
    receipt_region = receipt['region']
    receipt_availability_zone = receipt['availability_zone']
    receipt_client_token = receipt['client_token']
    principal_match = (re.fullmatch(
        r'arn:(aws(?:-[a-z0-9]+)*):(iam|sts)::([0-9]{12}):(\S+)',
        aws_principal_arn) if isinstance(aws_principal_arn, str) else None)
    if (not isinstance(aws_account_id, str) or
            re.fullmatch(r'[0-9]{12}', aws_account_id) is None or
            principal_match is None or
            principal_match.group(3) != aws_account_id or
            not isinstance(instance_type, str) or not instance_type):
        return None
    if (expected_aws_account_id is not None and
            receipt['aws_account_id'] != expected_aws_account_id):
        return None
    if (not valid_aws_run_instances_client_token(receipt_client_token) or
        (client_token is not None and receipt_client_token != client_token)):
        return None
    if (not isinstance(receipt_region, str) or not receipt_region or
            not isinstance(receipt_availability_zone, str) or
            not receipt_availability_zone):
        return None
    invocations = receipt['invocations']
    if type(invocations) is not list or not invocations:
        return None

    canonical_invocations: list[dict[str, Any]] = []
    provider_request_ids: set[str] = set()
    for invocation in invocations:
        if (type(invocation) is not dict or
                set(invocation) != _PROVIDER_NEGATIVE_ACK_INVOCATION_KEYS):
            return None
        region = invocation['region']
        availability_zone = invocation['availability_zone']
        if (not isinstance(region, str) or not region or
                not isinstance(availability_zone, str) or
                not availability_zone or region != receipt_region or
                availability_zone != receipt_availability_zone):
            return None
        for empty_list_key in ('initial_nonterminated_instance_ids',
                               'resumed_instance_ids', 'created_instance_ids'):
            value = invocation[empty_list_key]
            if type(value) is not list or value:
                return None
        if (type(invocation['successful_create_calls']) is not int or
                invocation['successful_create_calls'] != 0 or
                type(invocation['ambiguous_create_calls']) is not int or
                invocation['ambiguous_create_calls'] != 0 or
                type(invocation['create_call_count']) is not int or
                invocation['create_call_count'] < 1):
            return None
        attempts = invocation['attempts']
        if (type(attempts) is not list or not attempts or
                invocation['create_call_count'] != len(attempts)):
            return None
        canonical_attempts: list[dict[str, Any]] = []
        for attempt in attempts:
            canonical_attempt = _canonical_provider_negative_ack_attempt(
                attempt,
                reason=reason,
                aws_account_id=aws_account_id,
                aws_principal_arn=aws_principal_arn,
                cluster_name=cluster_name,
                requested_count=receipt_count,
                market=market,
                instance_type=instance_type,
                region=region,
                availability_zone=availability_zone,
                client_token=receipt_client_token)
            if canonical_attempt is None:
                return None
            provider_request_id = canonical_attempt['provider_request_id']
            if provider_request_id in provider_request_ids:
                return None
            provider_request_ids.add(provider_request_id)
            canonical_attempts.append(canonical_attempt)
        canonical_invocations.append({
            'region': region,
            'availability_zone': availability_zone,
            'initial_nonterminated_instance_ids': [],
            'resumed_instance_ids': [],
            'created_instance_ids': [],
            'successful_create_calls': 0,
            'ambiguous_create_calls': 0,
            'create_call_count': len(canonical_attempts),
            'attempts': canonical_attempts,
        })
    return {
        'schema_version': 1,
        'provider': 'aws',
        'operation': 'RunInstances',
        'reason': reason,
        'aws_account_id': aws_account_id,
        'aws_principal_arn': aws_principal_arn,
        'cluster_name_on_cloud': cluster_name,
        'requested_count': receipt_count,
        'market': market,
        'instance_type': instance_type,
        'region': receipt_region,
        'availability_zone': receipt_availability_zone,
        'client_token': receipt_client_token,
        'invocations': canonical_invocations,
    }


def extract_provider_negative_ack(
        error: BaseException) -> dict[str, Any] | None:
    """Extract complete provider absence evidence from a failover graph.

    Every terminal history leaf must contain exactly one independently valid
    receipt in its explicit cause chain.  Any missing, mixed, malformed, too
    large, or cyclic graph is deliberately UNKNOWN.
    """
    if not isinstance(error, BaseException):
        return None
    traversal = _terminal_failover_leaves(error)
    if traversal is None:
        return None
    failures, visited = traversal
    if not failures:
        return None

    canonical_receipts: list[dict[str, Any]] = []
    expected_cluster_name: str | None = None
    expected_requested_count: int | None = None
    for failure, history_depth in failures:
        cause_nodes = _terminal_leaf_cause_nodes(
            failure,
            history_depth=history_depth,
            remaining_nodes=_MAX_TERMINAL_FAILOVER_HISTORY_NODES - visited)
        if cause_nodes is None:
            return None
        visited += cause_nodes
        receipt_values: list[object] = []
        leaf: BaseException | None = failure
        while leaf is not None:
            leaf_dict = getattr(leaf, '__dict__', None)
            if (type(leaf_dict) is dict and
                    _PROVIDER_NEGATIVE_ACK_ATTR in leaf_dict):
                receipt_values.append(leaf_dict[_PROVIDER_NEGATIVE_ACK_ATTR])
            leaf = leaf.__cause__
        if len(receipt_values) != 1:
            return None
        receipt_value = receipt_values[0]
        if type(receipt_value) is not dict:
            return None
        cluster_name_value = receipt_value.get('cluster_name_on_cloud')
        requested_count_value = receipt_value.get('requested_count')
        if (not isinstance(cluster_name_value, str) or not cluster_name_value or
                type(requested_count_value) is not int or
                requested_count_value < 1):
            return None
        if expected_cluster_name is None:
            expected_cluster_name = cluster_name_value
            expected_requested_count = requested_count_value
        elif (cluster_name_value != expected_cluster_name or
              requested_count_value != expected_requested_count):
            return None
        canonical = validate_provider_negative_ack(
            receipt_value,
            cluster_name=cluster_name_value,
            requested_count=requested_count_value)
        if canonical is None:
            return None
        canonical_receipts.append(canonical)

    assert expected_cluster_name is not None
    assert expected_requested_count is not None
    first = canonical_receipts[0]
    common_keys = ('provider', 'operation', 'reason', 'aws_account_id',
                   'aws_principal_arn', 'market', 'instance_type', 'region',
                   'availability_zone', 'client_token')
    if any(
            any(receipt[key] != first[key]
                for key in common_keys)
            for receipt in canonical_receipts[1:]):
        return None
    aggregate = {
        'schema_version': 1,
        'provider': first['provider'],
        'operation': first['operation'],
        'reason': first['reason'],
        'aws_account_id': first['aws_account_id'],
        'aws_principal_arn': first['aws_principal_arn'],
        'cluster_name_on_cloud': first['cluster_name_on_cloud'],
        'requested_count': first['requested_count'],
        'market': first['market'],
        'instance_type': first['instance_type'],
        'region': first['region'],
        'availability_zone': first['availability_zone'],
        'client_token': first['client_token'],
        'invocations': [
            invocation for receipt in canonical_receipts
            for invocation in receipt['invocations']
        ],
    }
    return validate_provider_negative_ack(
        aggregate,
        cluster_name=expected_cluster_name,
        requested_count=expected_requested_count)


def _is_quota_error(error: BaseException) -> bool:
    """Whether an exception chain contains a recognized provider quota code."""
    return any(code in _PROVIDER_QUOTA_ERROR_CODES
               for code in _provider_error_codes(error))


def _canonical_accelerators(to_provision: 'resources_lib.Resources') -> str:
    """Returns a stable string for the requested accelerators.

    A machine type does not always determine the accelerator (GCP's N1 family
    attaches them separately), so the accelerator has to be part of any key
    that suppresses a later launch.
    """
    accelerators = to_provision.accelerators or {}
    return ','.join(
        f'{name}:{count}' for name, count in sorted(accelerators.items()))


def _capacity_cache_cloud_name(
        to_provision: 'resources_lib.Resources') -> str | None:
    """Returns the cache-eligible cloud name, or None when not eligible."""
    if isinstance(to_provision.cloud, clouds.AWS):
        return 'aws'
    if isinstance(to_provision.cloud, clouds.GCP):
        # Enabled by default, with `provision.gcp_capacity_cache: false` as the
        # escape hatch. Setting it false means no key is built, so nothing is
        # ever written or read and behavior returns to pre-cache provisioning.
        if skypilot_config.get_nested(('provision', 'gcp_capacity_cache'),
                                      True):
            return 'gcp'
    return None


_GCP_IDENTITY_PROJECT_RE = re.compile(r'\[project_id=([^\]]+)\]')


def _capacity_cache_account(
        cloud: Optional['clouds.Cloud'],
        cloud_user_identity: list[str] | None) -> str | None:
    """Returns the account that scopes cache keys, or None to skip caching.

    Hints must never be shared across accounts, so a cloud whose identity
    cannot be resolved simply does not participate. No extra provider call is
    made: the identity has already been fetched for this provisioning attempt.
    """
    if not cloud_user_identity:
        return None
    identity = str(cloud_user_identity[-1])
    if isinstance(cloud, clouds.AWS):
        return identity
    if isinstance(cloud, clouds.GCP):
        # GCP formats its identity as `<account> [project_id=<project>]`. Only
        # the project scopes capacity, and taking it alone keeps the user's
        # email address out of the cache key.
        match = _GCP_IDENTITY_PROJECT_RE.search(identity)
        return match.group(1) if match is not None else None
    return None


def _capacity_cache_key(
        to_provision: 'resources_lib.Resources', region: 'clouds.Region',
        zones: list['clouds.Zone'] | None, num_nodes: int,
        account: str | None) -> Optional['capacity_cache.ResourceKey']:
    """Returns a key only for the exact, safe-to-cache incident path."""
    cloud_name = _capacity_cache_cloud_name(to_provision)
    if (cloud_name is None or not to_provision.use_spot or zones is None or
            len(zones) != 1 or not account or not to_provision.instance_type):
        return None
    return capacity_cache.ResourceKey(
        cloud=cloud_name,
        account=account,
        region=region.name,
        zone=zones[0].name,
        instance_type=to_provision.instance_type,
        accelerators=_canonical_accelerators(to_provision),
        num_nodes=num_nodes)


def _quota_cooldown_key(
        to_provision: 'resources_lib.Resources', region: 'clouds.Region',
        num_nodes: int,
        account: str | None) -> Optional['capacity_cache.QuotaCooldownKey']:
    """Returns a demand-specific key for a brief Spot quota cooldown."""
    cloud_name = _capacity_cache_cloud_name(to_provision)
    if (cloud_name is None or not to_provision.use_spot or not account or
            not to_provision.instance_type):
        return None
    return capacity_cache.QuotaCooldownKey(
        cloud=cloud_name,
        account=account,
        region=region.name,
        instance_type=to_provision.instance_type,
        accelerators=_canonical_accelerators(to_provision),
        num_nodes=num_nodes)


def _fully_created_fresh_demand(
        provision_record: 'provision_common.ProvisionRecord', num_nodes: int,
        cluster_exists: bool) -> bool:
    """Whether success proves capacity/quota for the full requested demand."""
    return (not cluster_exists and
            len(provision_record.created_instance_ids) == num_nodes)


def _failure_requested_full_demand(error: BaseException,
                                   num_nodes: int) -> bool:
    """Whether provider metadata proves the failed request covered all nodes."""
    requested_counts = []
    for exc in _iter_error_chain(error):
        requested_count = getattr(exc, 'requested_count', None)
        if isinstance(requested_count, int):
            requested_counts.append(requested_count)
    return bool(requested_counts) and all(
        count == num_nodes for count in requested_counts)


def _placement_error_code(error: BaseException) -> str | None:
    """Return the first structured provider error code in an exception."""
    codes = _provider_error_codes(error)
    return codes[0] if codes else None


def _placement_outcome(error: Exception,
                       capacity_reason: str | None = None) -> str:
    if capacity_reason is not None:
        return f'{capacity_reason}_failed'
    if _is_quota_error(error):
        return 'quota_failed'
    # Every code is examined, not just the first: GCP's bulk insert reports
    # the generic VM_MIN_COUNT_NOT_REACHED summary ahead of the code that
    # says why the minimum was not reached. Quota is checked above, so a
    # mixed batch still reports the regional quota denial.
    #
    # Requiring every remaining code to be a capacity code keeps the
    # conservative reading of a heterogeneous batch: AWS retries each subnet
    # and appends one entry per distinct failure, so an aggregate that mixes
    # capacity with an unrelated error is not capacity exhaustion.
    codes = [
        code for code in _provider_error_codes(error)
        if code not in _NEUTRAL_PLACEMENT_ERROR_CODES
    ]
    if codes and all(code in _PLACEMENT_CAPACITY_ERROR_CODES for code in codes):
        return 'capacity_failed'
    return 'failed'
