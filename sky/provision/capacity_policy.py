"""Provider capacity classification and exact cache-key policy."""
from collections.abc import Iterable
import re
from typing import Optional

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
    error: exceptions.ResourcesUnavailableError,
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
