"""Global admission control for fresh paid SkyServe capacity.

Autoscalers decide how much capacity a service needs. Spot placers decide
which provider location is cheapest and currently usable. This module owns the
cross-service limit on unresolved, genuine demand launches into one exact paid
provider pool.
"""
import collections
from collections.abc import Iterable
from collections.abc import Mapping
import dataclasses
import enum
import functools
import json
import os
import threading
import time
import typing
from typing import Any

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.serve import constants
from sky.serve import spot_placer

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers

logger = sky_logging.init_logger(__name__)
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')

_BASE_LIMIT_DEFAULT = 4
_LEGACY_LOCAL_LIMIT_DEFAULT = 4
_MAX_LIMIT_DEFAULT = 480
_EXPLORATION_FRONTIER_DEFAULT = 2
_BASE_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_LOCATION_LAUNCH_WINDOW'
_MAX_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_LOCATION_MAX_LAUNCH_WINDOW'
_SERVICE_LIMIT_DEFAULT = 16
_SERVICE_LIMIT_ENV_VAR = 'SKYPILOT_SERVE_PAID_SERVICE_LAUNCH_WINDOW'
_EXPLORATION_FRONTIER_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_EXPLORATION_FRONTIER')
_SUCCESS_TTL_SECONDS_DEFAULT = 10 * 60
_SUCCESS_TTL_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_SUCCESS_TTL_SECONDS')
_WAITER_TTL_SECONDS_DEFAULT = 45
_WAITER_TTL_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_WAITER_TTL_SECONDS')
_FAILURE_COOLDOWN_SECONDS_DEFAULT = 10 * 60
_FAILURE_COOLDOWN_SECONDS_ENV_VAR = (
    'SKYPILOT_SERVE_PAID_LOCATION_FAILURE_COOLDOWN_SECONDS')
_ADMISSION_SUMMARY_LOG_MIN_INTERVAL_SECONDS = 30
_ADMISSION_SUMMARY_LOG_INTERVAL_SECONDS = 5 * 60
_POOL_KEY_VERSION = 1
_UNRESOLVED_STATUS_VALUES = frozenset({'PENDING', 'PROVISIONING'})
_admission_summary_log_lock = threading.Lock()
_admission_summary_log_signature: tuple[Any, ...] | None = None
_admission_summary_logged_at = 0.0
FrontierKey = tuple[str, ...]


class ClaimResult(enum.Enum):
    """Result of atomically persisting one paid-capacity claim."""

    ACQUIRED = 'acquired'
    SATURATED = 'saturated'
    SERVICE_SATURATED = 'service_saturated'
    FEEDBACK_PENDING = 'feedback_pending'
    HIGHER_PRIORITY_WAITING = 'higher_priority_waiting'
    OWNERSHIP_LOST = 'ownership_lost'
    LEGACY_LOCAL = 'legacy_local'


class LaunchOutcome(enum.Enum):
    """Capacity evidence from one completed paid launch."""

    SUCCESS = 'success'
    CAPACITY_FAILURE = 'capacity_failure'
    OTHER_FAILURE = 'other_failure'


@dataclasses.dataclass
class LaunchBudget:
    """One wave's advisory headroom and exact pool identity."""

    remaining_by_location: dict[spot_placer.Location, int]
    pool_key_by_location: dict[spot_placer.Location, str]
    states_by_pool_key: dict[str, dict[str, Any]]
    globally_managed: bool
    priority_deferred_pool_keys: set[str] = dataclasses.field(
        default_factory=set)
    service_remaining: int | None = None
    frontier_limit: int | None = None
    frontier_key_by_location: dict[spot_placer.Location,
                                   FrontierKey] = (dataclasses.field(
                                       default_factory=dict))
    owned_pool_keys_by_frontier: dict[FrontierKey,
                                      set[str]] = (dataclasses.field(
                                          default_factory=dict))
    unknown_owned_pool_keys: set[str] = dataclasses.field(default_factory=set)
    oldest_claimed_at_by_frontier: dict[FrontierKey,
                                        float] = (dataclasses.field(
                                            default_factory=dict))
    oldest_unknown_claimed_at: float | None = None
    feedback_deferred_frontiers: set[FrontierKey] = dataclasses.field(
        default_factory=set)
    stop_sequence: int = 0


@dataclasses.dataclass(frozen=True)
class RampUpdate:
    """Pure adaptive-limit transition produced from provider feedback."""

    current_limit: int
    successes_since_resize: int
    expired: bool
    failed: bool


@dataclasses.dataclass(frozen=True)
class AdmissionLimit:
    """Effective admission bound for one exact paid-capacity pool."""

    limit: int
    state: str
    cooldown_until: float | None


@functools.cache
def _parse_positive_int(raw_value: str | None, default: int,
                        variable: str) -> int:
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        value = 0
    if value <= 0:
        logger.warning(
            f'Invalid {variable} value {raw_value!r}; using {default}.')
        return default
    return value


def base_limit() -> int:
    """Return the first unresolved paid-capacity cohort size."""
    return _parse_positive_int(os.environ.get(_BASE_LIMIT_ENV_VAR),
                               _BASE_LIMIT_DEFAULT, _BASE_LIMIT_ENV_VAR)


def legacy_local_limit() -> int:
    """Return the pre-global per-service window for local SQLite."""
    return _parse_positive_int(os.environ.get(_BASE_LIMIT_ENV_VAR),
                               _LEGACY_LOCAL_LIMIT_DEFAULT, _BASE_LIMIT_ENV_VAR)


def max_limit() -> int:
    """Return the largest adaptive unresolved paid-capacity cohort."""
    configured = _parse_positive_int(os.environ.get(_MAX_LIMIT_ENV_VAR),
                                     _MAX_LIMIT_DEFAULT, _MAX_LIMIT_ENV_VAR)
    return max(base_limit(), configured)


def service_limit() -> int:
    """Return one service's cross-pool unresolved paid-claim envelope."""
    return _parse_positive_int(os.environ.get(_SERVICE_LIMIT_ENV_VAR),
                               _SERVICE_LIMIT_DEFAULT, _SERVICE_LIMIT_ENV_VAR)


def exploration_frontier() -> int:
    """Return the number of paid pools one service/card may explore."""
    return _parse_positive_int(os.environ.get(_EXPLORATION_FRONTIER_ENV_VAR),
                               _EXPLORATION_FRONTIER_DEFAULT,
                               _EXPLORATION_FRONTIER_ENV_VAR)


def success_ttl_seconds() -> int:
    """Return how long successful feedback keeps an expanded cohort."""
    return _parse_positive_int(os.environ.get(_SUCCESS_TTL_SECONDS_ENV_VAR),
                               _SUCCESS_TTL_SECONDS_DEFAULT,
                               _SUCCESS_TTL_SECONDS_ENV_VAR)


def waiter_ttl_seconds() -> int:
    """Return how long a service's priority heartbeat remains eligible."""
    return _parse_positive_int(os.environ.get(_WAITER_TTL_SECONDS_ENV_VAR),
                               _WAITER_TTL_SECONDS_DEFAULT,
                               _WAITER_TTL_SECONDS_ENV_VAR)


def failure_cooldown_seconds() -> int:
    """Return how long typed capacity failure closes an exact paid pool."""
    return _parse_positive_int(
        os.environ.get(_FAILURE_COOLDOWN_SECONDS_ENV_VAR),
        _FAILURE_COOLDOWN_SECONDS_DEFAULT, _FAILURE_COOLDOWN_SECONDS_ENV_VAR)


def limit_ladder(bootstrap_limit: int, ceiling_limit: int) -> tuple[int, ...]:
    """Return every valid persisted adaptive-limit rung."""
    bootstrap_limit = max(1, int(bootstrap_limit))
    ceiling_limit = max(bootstrap_limit, int(ceiling_limit))
    values = [bootstrap_limit]
    while values[-1] < ceiling_limit:
        next_value = min(ceiling_limit, values[-1] * 2)
        if next_value == values[-1]:
            break
        values.append(next_value)
    return tuple(values)


def effective_limit(
    current_limit: int,
    last_success_at: float | None,
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    ttl_seconds: float,
) -> tuple[int, bool]:
    """Clamp and expire one persisted adaptive limit."""
    ladder = limit_ladder(bootstrap_limit, ceiling_limit)
    if int(current_limit) not in ladder:
        # Revision 027 used a 60/120/240/480 ladder. Conservatively reset old
        # rungs when the configured bootstrap changes instead of preserving an
        # unearned cohort. The failure/probe path bypasses this helper while
        # current_limit=1 is its intentional marker.
        return bootstrap_limit, True
    effective = int(current_limit)
    has_positive_evidence = (effective > bootstrap_limit or
                             last_success_at is not None)
    expired = (has_positive_evidence and (last_success_at is None or
                                          now - last_success_at >= ttl_seconds))
    return (bootstrap_limit if expired else effective), expired


def effective_admission_limit(
    current_limit: int,
    last_success_at: float | None,
    last_failure_at: float | None,
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    success_ttl: float,
    failure_cooldown: float,
) -> AdmissionLimit:
    """Return normal, cooldown-closed, or one-probe pool admission."""
    if last_failure_at is not None:
        cooldown_until = last_failure_at + failure_cooldown
        if now < cooldown_until:
            return AdmissionLimit(limit=0,
                                  state='cooldown',
                                  cooldown_until=cooldown_until)
        return AdmissionLimit(limit=1,
                              state='probe',
                              cooldown_until=cooldown_until)
    effective, _ = effective_limit(current_limit,
                                   last_success_at,
                                   bootstrap_limit=bootstrap_limit,
                                   ceiling_limit=ceiling_limit,
                                   now=now,
                                   ttl_seconds=success_ttl)
    return AdmissionLimit(limit=effective, state='active', cooldown_until=None)


def record_outcomes(
    current_limit: int,
    successes_since_resize: int,
    last_success_at: float | None,
    outcomes: Iterable[LaunchOutcome],
    *,
    bootstrap_limit: int,
    ceiling_limit: int,
    now: float,
    ttl_seconds: float,
) -> RampUpdate:
    """Apply genuine launch feedback to one exact pool's adaptive limit."""
    completed = list(outcomes)
    if not completed:
        raise ValueError('At least one paid-capacity outcome is required.')
    if LaunchOutcome.CAPACITY_FAILURE in completed:
        return RampUpdate(current_limit=bootstrap_limit,
                          successes_since_resize=0,
                          expired=False,
                          failed=True)
    successful = sum(outcome == LaunchOutcome.SUCCESS for outcome in completed)
    if successful == 0:
        return RampUpdate(current_limit=int(current_limit),
                          successes_since_resize=max(
                              0, int(successes_since_resize)),
                          expired=False,
                          failed=False)

    current_limit, expired = effective_limit(current_limit,
                                             last_success_at,
                                             bootstrap_limit=bootstrap_limit,
                                             ceiling_limit=ceiling_limit,
                                             now=now,
                                             ttl_seconds=ttl_seconds)
    success_count = (0 if expired else max(0, int(successes_since_resize)))
    success_count += successful
    while success_count >= current_limit and current_limit < ceiling_limit:
        success_count -= current_limit
        current_limit = min(ceiling_limit, current_limit * 2)
    if current_limit >= ceiling_limit:
        success_count = min(success_count, ceiling_limit - 1)
    return RampUpdate(current_limit=current_limit,
                      successes_since_resize=success_count,
                      expired=expired,
                      failed=False)


def _normalized_accelerators(
    accelerators: Mapping[str, int | float] | None
) -> list[list[str | int | float]]:
    if not accelerators:
        return []
    normalized = []
    for name, count in sorted(accelerators.items(),
                              key=lambda item: item[0].casefold()):
        normalized_count: int | float = count
        if isinstance(count, float) and count.is_integer():
            normalized_count = int(count)
        normalized.append([str(name).casefold(), normalized_count])
    return normalized


def pool_key(location: spot_placer.Location, *, workspace: str,
             num_nodes: int) -> str:
    """Build a stable identity for one exact provider capacity pool."""
    payload = {
        'version': _POOL_KEY_VERSION,
        'workspace': workspace,
        'cloud': str(location.cloud).casefold(),
        'region': location.region,
        'zone': location.zone,
        'instance_type': location.instance_type,
        'accelerators': _normalized_accelerators(location.accelerators),
        'use_spot': location.use_spot,
        'num_nodes': num_nodes,
    }
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def frontier_key(location: spot_placer.Location) -> FrontierKey:
    """Return one model-only exploration identity for a paid location."""
    accelerators = location.accelerators
    if not accelerators:
        return ()
    return tuple(
        sorted((str(name).casefold() for name in accelerators),
               key=str.casefold))


def frontier_key_from_pool_key(key: str) -> FrontierKey | None:
    """Recover a card frontier identity from one versioned exact pool key."""
    try:
        payload = json.loads(key)
    except (TypeError, ValueError):
        return None
    if (not isinstance(payload, dict) or
            payload.get('version') != _POOL_KEY_VERSION):
        return None
    accelerators = payload.get('accelerators')
    if not isinstance(accelerators, list):
        return None
    names = []
    for accelerator in accelerators:
        if (not isinstance(accelerator, list) or len(accelerator) != 2 or
                not isinstance(accelerator[0], str)):
            return None
        names.append(accelerator[0].casefold())
    return tuple(sorted(names, key=str.casefold))


def _legacy_local_remaining(
    placer: spot_placer.SpotPlacer,
    paid_locations: Iterable[spot_placer.Location],
    existing_replica_infos: list['replica_managers.ReplicaInfo'],
) -> dict[spot_placer.Location, int]:
    remaining = {location: legacy_local_limit() for location in paid_locations}
    for info in existing_replica_infos:
        if (getattr(info.status, 'value', info.status)
                not in _UNRESOLVED_STATUS_VALUES):
            continue
        replica_location = info.get_spot_location()
        if replica_location is None:
            continue
        # Local SQLite has no cross-service claim identity to protect. Mirror
        # the operational rollout resolver so an ambiguous pre-instance-type
        # row still debits the cheapest compatible pool it would launch on.
        resolved = placer.resolve_location(replica_location,
                                           allow_ambiguous_legacy_shape=True)
        if resolved in remaining:
            remaining[resolved] = max(0, remaining[resolved] - 1)
    return remaining


def central_authority_available() -> bool:
    """Whether this process has the PostgreSQL shared-authority backend."""
    return (serve_state.get_database_engine().dialect.name == 'postgresql')


def _log_admission_summary(states: dict[str, dict[str,
                                                  Any]], *, service_claims: int,
                           service_claim_limit: int) -> None:
    """Log one bounded shared-admission summary on transition or interval."""
    if not states:
        return
    state_counts = collections.Counter(
        str(state.get('admission_state', 'active'))
        for state in states.values())
    overage_pools = sum(
        int(state.get('legacy_overage', 0)) > 0 for state in states.values())
    saturated_pools = sum(
        int(state.get('remaining', 0)) == 0 for state in states.values())
    service_remaining = max(0, service_claim_limit - service_claims)
    signature = (len(states), tuple(sorted(state_counts.items())), overage_pools
                 > 0, service_remaining == 0)
    observed_at = time.monotonic()
    global _admission_summary_log_signature
    global _admission_summary_logged_at
    with _admission_summary_log_lock:
        elapsed = observed_at - _admission_summary_logged_at
        if (_admission_summary_log_signature is not None and
                elapsed < _ADMISSION_SUMMARY_LOG_MIN_INTERVAL_SECONDS):
            return
        if (signature == _admission_summary_log_signature and
                elapsed < _ADMISSION_SUMMARY_LOG_INTERVAL_SECONDS):
            return
        _admission_summary_log_signature = signature
        _admission_summary_logged_at = observed_at
    logger.info(
        'Global paid-capacity admission: '
        f'pools={len(states)}, states={dict(sorted(state_counts.items()))}, '
        f'active_claims={sum(int(state.get("active_claims", 0)) for state in states.values())}, '
        f'admission_limit={sum(int(state.get("admission_limit", 0)) for state in states.values())}, '
        f'remaining={sum(int(state.get("remaining", 0)) for state in states.values())}, '
        f'saturated_pools={saturated_pools}, '
        f'legacy_overage_claims={sum(int(state.get("legacy_overage", 0)) for state in states.values())}, '
        f'service_claims={service_claims}, '
        f'service_limit={service_claim_limit}, '
        f'service_remaining={service_remaining}.')


def _service_claim_count(
        existing_replica_infos: Iterable['replica_managers.ReplicaInfo']
) -> int:
    """Count this service's unresolved rows with an exact paid claim."""
    return sum(
        getattr(info.status, 'value', info.status) in _UNRESOLVED_STATUS_VALUES
        and isinstance(getattr(info, 'paid_capacity_pool_key', None), str)
        for info in existing_replica_infos)


def build_launch_budget(
    placer: spot_placer.SpotPlacer,
    *,
    workspace: str,
    existing_replica_infos: list['replica_managers.ReplicaInfo'],
    globally_managed: bool,
) -> LaunchBudget:
    """Read one advisory shared-capacity snapshot for all active paid pools."""
    zero_cost = set(placer.zero_cost_locations())
    paid_locations = [
        location for location in placer.active_locations()
        if location not in zero_cost
    ]
    keys = {
        location: pool_key(location,
                           workspace=workspace,
                           num_nodes=getattr(placer, 'num_nodes',
                                             1)) for location in paid_locations
    }
    if not globally_managed or not central_authority_available():
        return LaunchBudget(remaining_by_location=_legacy_local_remaining(
            placer, paid_locations, existing_replica_infos),
                            pool_key_by_location=keys,
                            states_by_pool_key={},
                            globally_managed=False)

    states = serve_state.get_paid_capacity_pool_states(
        list(keys.values()),
        base_limit=base_limit(),
        max_limit=max_limit(),
        now=None,
        success_ttl_seconds=success_ttl_seconds(),
        failure_cooldown_seconds=failure_cooldown_seconds())
    remaining = {
        location: int(states[key]['remaining'])
        for location, key in keys.items()
    }
    service_claims = _service_claim_count(existing_replica_infos)
    service_claim_limit = service_limit()
    frontier_keys = {
        location: frontier_key(location) for location in paid_locations
    }
    owned_by_frontier: dict[FrontierKey,
                            set[str]] = collections.defaultdict(set)
    oldest_by_frontier: dict[FrontierKey, float] = {}
    unknown_owned_pool_keys = set()
    oldest_unknown_claimed_at = None
    for info in existing_replica_infos:
        if (getattr(info.status, 'value', info.status)
                not in _UNRESOLVED_STATUS_VALUES):
            continue
        key = getattr(info, 'paid_capacity_pool_key', None)
        if not isinstance(key, str):
            continue
        claimed_at = getattr(info, 'created_at', None)
        parsed_frontier = frontier_key_from_pool_key(key)
        if parsed_frontier is None:
            unknown_owned_pool_keys.add(key)
            if isinstance(claimed_at, (int, float)):
                oldest_unknown_claimed_at = min(
                    float(claimed_at),
                    oldest_unknown_claimed_at if oldest_unknown_claimed_at
                    is not None else float(claimed_at))
            continue
        owned_by_frontier[parsed_frontier].add(key)
        if isinstance(claimed_at, (int, float)):
            oldest_by_frontier[parsed_frontier] = min(
                float(claimed_at),
                oldest_by_frontier.get(parsed_frontier, float(claimed_at)))
    _log_admission_summary(states,
                           service_claims=service_claims,
                           service_claim_limit=service_claim_limit)
    return LaunchBudget(remaining_by_location=remaining,
                        pool_key_by_location=keys,
                        states_by_pool_key=states,
                        globally_managed=True,
                        service_remaining=max(
                            0, service_claim_limit - service_claims),
                        frontier_limit=exploration_frontier(),
                        frontier_key_by_location=frontier_keys,
                        owned_pool_keys_by_frontier=dict(owned_by_frontier),
                        unknown_owned_pool_keys=unknown_owned_pool_keys,
                        oldest_claimed_at_by_frontier=oldest_by_frontier,
                        oldest_unknown_claimed_at=oldest_unknown_claimed_at)


def _owned_pool_keys(budget: LaunchBudget, key: FrontierKey) -> set[str]:
    return (budget.owned_pool_keys_by_frontier.get(key, set()) |
            budget.unknown_owned_pool_keys)


def _defer_frontier(budget: LaunchBudget, key: FrontierKey) -> None:
    """Mark and log one card that cannot open another paid pool this wave."""
    if key in budget.feedback_deferred_frontiers:
        return
    budget.feedback_deferred_frontiers.add(key)
    oldest_candidates = [
        value for value in (budget.oldest_claimed_at_by_frontier.get(key),
                            budget.oldest_unknown_claimed_at)
        if value is not None
    ]
    age_text = 'unknown'
    if oldest_candidates:
        age_text = str(max(0, int(time.time() - min(oldest_candidates))))
    card = ','.join(key) if key else 'cpu'
    logger.info(
        'Paid-capacity exploration frontier awaiting feedback: '
        f'card={card}, owned_pools={len(_owned_pool_keys(budget, key))}, '
        f'limit={budget.frontier_limit}, '
        f'oldest_unresolved_claim_age_seconds={age_text}.')


def _record_selection_stop(budget: LaunchBudget) -> None:
    """Record one paid path that made no progress in this wave."""
    budget.stop_sequence += 1


def select_location(
    placer: spot_placer.SpotPlacer,
    budget: LaunchBudget,
    *,
    skip_zero_cost_preference: bool = False,
    allowed_locations: set[spot_placer.Location] | None = None,
) -> spot_placer.Location | None:
    """Select the cheapest location that still has advisory paid headroom."""
    active = [
        location for location in placer.active_locations()
        if allowed_locations is None or location in allowed_locations
    ]
    if not active:
        selection_kwargs: dict[str, Any] = {}
        if skip_zero_cost_preference:
            selection_kwargs['skip_zero_cost_preference'] = True
        if allowed_locations is not None:
            selection_kwargs['allowed_locations'] = set()
        selected = placer.select_next_location(**selection_kwargs)
        if selected is None:
            _record_selection_stop(budget)
        return selected
    zero_cost = set(placer.zero_cost_locations())
    active_paid = [location for location in active if location not in zero_cost]
    available_paid = {
        location for location in active_paid
        if budget.remaining_by_location.get(location, 0) > 0 and
        (budget.service_remaining is None or budget.service_remaining > 0)
    }
    eligible_paid = available_paid
    if budget.frontier_limit is not None:
        eligible_paid = set()
        blocked_frontiers = set()
        for location in available_paid:
            key = budget.frontier_key_by_location.get(location,
                                                      frontier_key(location))
            if key in budget.feedback_deferred_frontiers:
                continue
            pool = budget.pool_key_by_location.get(location)
            owned = _owned_pool_keys(budget, key)
            if pool in owned or len(owned) < budget.frontier_limit:
                eligible_paid.add(location)
            else:
                blocked_frontiers.add(key)
        if not eligible_paid:
            for location in active_paid:
                key = budget.frontier_key_by_location.get(
                    location, frontier_key(location))
                if (len(_owned_pool_keys(budget, key))
                        >= budget.frontier_limit):
                    blocked_frontiers.add(key)
            for key in blocked_frontiers:
                _defer_frontier(budget, key)
    if skip_zero_cost_preference and active_paid and not eligible_paid:
        _record_selection_stop(budget)
        return None
    candidates = eligible_paid | {
        location for location in active if location in zero_cost
    }
    if not candidates:
        _record_selection_stop(budget)
        return None
    selected = placer.select_next_location(
        skip_zero_cost_preference=skip_zero_cost_preference,
        allowed_locations=candidates)
    if selected is None:
        _record_selection_stop(budget)
        return None
    if selected in zero_cost:
        return selected
    selected_key = budget.pool_key_by_location.get(selected)
    if selected_key in budget.priority_deferred_pool_keys:
        _record_selection_stop(budget)
        return None
    return selected


def defer_for_priority(budget: LaunchBudget | None,
                       location: spot_placer.Location | None) -> None:
    """Stop this wave at a priority-deferred pool without enabling spill."""
    if budget is None or location is None:
        return
    key = budget.pool_key_by_location.get(location)
    if key is not None:
        budget.priority_deferred_pool_keys.add(key)
        _record_selection_stop(budget)


def defer_for_feedback(budget: LaunchBudget | None,
                       location: spot_placer.Location | None) -> None:
    """Stop this wave from opening another pool for one accelerator card."""
    if budget is None or location is None:
        return
    key = budget.frontier_key_by_location.get(location, frontier_key(location))
    _defer_frontier(budget, key)
    _record_selection_stop(budget)


def debit(budget: LaunchBudget | None,
          location: spot_placer.Location | None) -> None:
    """Debit a claim accepted after the advisory snapshot was read."""
    if budget is None or location not in budget.remaining_by_location:
        return
    key = budget.pool_key_by_location.get(location)
    aliases = [
        candidate for candidate in budget.remaining_by_location
        if candidate == location or
        (key is not None and budget.pool_key_by_location.get(candidate) == key)
    ]
    for candidate in aliases:
        remaining = budget.remaining_by_location[candidate]
        if remaining > 0:
            budget.remaining_by_location[candidate] = remaining - 1
    if budget.service_remaining is not None and budget.service_remaining > 0:
        budget.service_remaining -= 1
    if budget.frontier_limit is not None and key is not None:
        frontier = budget.frontier_key_by_location.get(location,
                                                       frontier_key(location))
        budget.owned_pool_keys_by_frontier.setdefault(frontier, set()).add(key)


def exhaust(budget: LaunchBudget | None,
            location: spot_placer.Location | None) -> None:
    """Stop this wave from repeatedly racing a saturated exact pool."""
    if budget is None or location not in budget.remaining_by_location:
        return
    key = budget.pool_key_by_location.get(location)
    for candidate in budget.remaining_by_location:
        if (candidate == location or
            (key is not None and
             budget.pool_key_by_location.get(candidate) == key)):
            budget.remaining_by_location[candidate] = 0


def exhaust_service(budget: LaunchBudget | None) -> None:
    """Stop a wave after the authoritative per-service envelope is full."""
    if budget is not None and budget.service_remaining is not None:
        budget.service_remaining = 0
        _record_selection_stop(budget)


def service_exhausted(budget: LaunchBudget | None) -> bool:
    """Whether fresh paid placement has no service-envelope headroom."""
    return (budget is not None and budget.service_remaining is not None and
            budget.service_remaining <= 0)


def try_persist_claim(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    location: spot_placer.Location,
    budget: LaunchBudget,
    priority: int,
) -> ClaimResult:
    """Atomically persist a replica row and exact-pool capacity claim."""
    if not budget.globally_managed or service_hash is None:
        return ClaimResult.LEGACY_LOCAL
    key = budget.pool_key_by_location[location]
    result = serve_state.try_add_replica_with_paid_capacity_claim(
        service_name,
        service_hash,
        replica_id,
        replica_info,
        pool_key=key,
        priority=max(constants.LB_REQUEST_PRIORITY_MIN,
                     min(constants.LB_REQUEST_PRIORITY_MAX, priority)),
        base_limit=base_limit(),
        max_limit=max_limit(),
        service_limit=service_limit(),
        now=None,
        success_ttl_seconds=success_ttl_seconds(),
        failure_cooldown_seconds=failure_cooldown_seconds(),
        waiter_ttl_seconds=waiter_ttl_seconds(),
        frontier_key=budget.frontier_key_by_location.get(
            location, frontier_key(location)),
        frontier_limit=(budget.frontier_limit if budget.frontier_limit
                        is not None else exploration_frontier()),
        expected_controller_owner=controller_owner)
    return ClaimResult(result)


def adopt_existing_claims(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    workspace: str,
    placer: spot_placer.SpotPlacer | None,
    replica_infos: list['replica_managers.ReplicaInfo'],
    priority: int,
) -> bool:
    """Adopt unresolved legacy rows before recovery re-drives their launches."""
    if service_hash is None or not central_authority_available():
        return True
    # Recovery starts before the controller HTTP port binds. The centralized
    # version catalog is already complete, so this cannot resolve providers.
    zero_cost = set(
        placer.zero_cost_locations()) if placer is not None else set()
    claims = []
    for info in replica_infos:
        if (getattr(info.status, 'value',
                    info.status) not in _UNRESOLVED_STATUS_VALUES or
                getattr(info, 'reserved_fill', False) or
                getattr(info, 'is_zero_cost', False) or getattr(
                    info, 'cost_rebalance_for_replica_id', None) is not None):
            continue
        existing_key = getattr(info, 'paid_capacity_pool_key', None)
        if isinstance(existing_key, str):
            claims.append((info.replica_id, existing_key, priority, info))
            continue
        if placer is None:
            continue
        replica_location = info.get_spot_location()
        if replica_location is None:
            continue
        location = placer.resolve_location(replica_location)
        if location is None or location in zero_cost:
            continue
        key = pool_key(location,
                       workspace=workspace,
                       num_nodes=getattr(placer, 'num_nodes', 1))
        claims.append((info.replica_id, key, priority, info))
    return serve_state.adopt_paid_capacity_claims(
        service_name,
        service_hash,
        claims,
        base_limit=base_limit(),
        now=None,
        expected_controller_owner=controller_owner)


def persist_completed_launches(
    *,
    service_name: str,
    service_hash: str | None,
    controller_owner: tuple[int | None, str | None] | None,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    outcomes: dict[int, LaunchOutcome],
) -> bool | None:
    """Persist completed rows and feed claimed outcomes into the ramp."""
    if service_hash is None or not central_authority_available():
        return None
    return serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
        service_name,
        service_hash,
        replica_infos,
        outcomes,
        base_limit=base_limit(),
        max_limit=max_limit(),
        now=None,
        success_ttl_seconds=success_ttl_seconds(),
        failure_cooldown_seconds=failure_cooldown_seconds(),
        expected_controller_owner=controller_owner)
