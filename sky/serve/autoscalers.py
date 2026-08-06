"""Autoscalers: perform autoscaling by monitoring metrics."""
import bisect
from collections.abc import Iterable
from collections.abc import Sequence
import copy
import dataclasses
import math
import threading
import time
import typing
from typing import Any

from sky import global_user_state
from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.serve import autoscaler_compatibility
from sky.serve import autoscaler_decisions
from sky.serve import constants
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils import operator_notifications

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers
    from sky.serve import service_spec

logger = sky_logging.init_logger(__name__)

AutoscalerDecisionOperator = (autoscaler_decisions.AutoscalerDecisionOperator)
AutoscalerDecisionReason = autoscaler_decisions.AutoscalerDecisionReason
LogicalScaleTarget = autoscaler_decisions.LogicalScaleTarget
LogicalScaleDownTarget = autoscaler_decisions.LogicalScaleDownTarget
UnrecoverableRolloutFailure = (autoscaler_decisions.UnrecoverableRolloutFailure)
FillDemandSample = autoscaler_decisions.FillDemandSample
AutoscalerDecision = autoscaler_decisions.AutoscalerDecision

# Preserve historical private import and pickle identities while the pure
# compatibility policy lives behind this module's facade. Internal call sites
# intentionally continue resolving these globals so facade monkeypatches keep
# controlling every strategy.
_allocate_compatibility_target = (
    autoscaler_compatibility._allocate_compatibility_target)  # pylint: disable=protected-access
_replica_is_retiring_card_supply = (
    autoscaler_compatibility._replica_is_retiring_card_supply)  # pylint: disable=protected-access
_merge_fresh_target_into_downscale_hold = (
    autoscaler_compatibility._merge_fresh_target_into_downscale_hold)  # pylint: disable=protected-access
_revalidate_actuation_target = (
    autoscaler_compatibility._revalidate_actuation_target)  # pylint: disable=protected-access
for _compatibility_helper in (
        _allocate_compatibility_target,
        _replica_is_retiring_card_supply,
        _merge_fresh_target_into_downscale_hold,
        _revalidate_actuation_target,
):
    _compatibility_helper.__module__ = __name__
del _compatibility_helper

_LOGICAL_ROLLING_UPDATE_MAX_RETIREMENTS_PER_TICK = 20
# Maximum consecutive downscale pressure vetoes per downscale episode.
# Genuine rising pressure raises the raw target and takes the upscale
# branch, which ends the episode on its own; the veto only needs to
# protect against downscaling at the exact moment pressure begins.
# Bounding it at 2 consecutive decision ticks preserves that protection
# while restoring downscale liveness under trickle traffic. The veto does
# not restart the already elapsed downscale delay.
_MAX_CONSECUTIVE_DOWNSCALE_VETOES = 2
_COST_REBALANCE_STATE_VERSION = 1
_COST_REBALANCE_STATE_MAX_ENTRIES = 256
# Converting a modeled work floor back into whole slots divides one float by
# another, and both sides carry binary-float tails. A retention floor built
# from n identical utilization-adjusted capacities is the exact case: three
# 0.7-work floors sum to 2.1, and 2.1 / 0.7 evaluates to 3.0000000000000004,
# so a bare ceil manufactures a fourth slot out of arithmetic noise. Real
# demand moves in whole-capacity quanta, never by 1e-9, so tolerate a
# sub-epsilon remainder here exactly as the compatibility allocator's
# demand_epsilon already does.
_SLOT_CONVERSION_EPSILON = 1e-9


@dataclasses.dataclass
class _PoolFillState:
    """One protocol-v2 reserved-fill pool's independently mutable gauges."""

    protocol_version: int
    pool_key: str
    physical_cluster_uid: str
    service_generation: int
    edge_cap: int
    free_slots: int = 0
    last_raw_free_slots: int | None = None
    # None means the broker round had no exact-card measurement. A present map
    # is this service's already-arbitrated portion of the aggregate pool feed.
    free_slots_by_accelerator: dict[str, int] | None = None
    zero_cost_locations: list[spot_placer.Location] = dataclasses.field(
        default_factory=list)
    snapshot_time: float | None = None
    # Scale-down protection is deliberately separate from live launch
    # authority.  A transient broker-round failure may carry the last real
    # same-generation grant here while clearing grant/feed/epoch, so existing
    # pool-local fill is not culled and no new launch can replay stale
    # authority.
    shelter_grant: int = 0
    grant: int = 0
    grant_epoch: int | None = None
    fill_target: int = 0

    def detached_copy(self) -> '_PoolFillState':
        return dataclasses.replace(
            self,
            zero_cost_locations=list(self.zero_cost_locations),
            free_slots_by_accelerator=(None if self.free_slots_by_accelerator
                                       is None else dict(
                                           self.free_slots_by_accelerator)))


@dataclasses.dataclass(frozen=True)
class _CompatibilityTargetResult:
    """Explicit provenance for one exact-card compatibility allocation.

    ``card_attribution_complete`` means every fixed replica row could be
    mapped to a configured physical card. ``explicit_target_by_accelerator``
    is the subset backed by explicit compatibility evidence, an exact-card
    floor, or fixed exact-card work; it bounds cross-card rollout movement.
    ``paid_target_by_accelerator`` is the independently allocated subset that
    may acquire paid capacity. It also includes ordinary aggregate minimums
    and headerless queued/rejected demand, but excludes inferred in-flight
    overflow and generic overprovision padding.
    """

    target_by_accelerator: dict[str, int]
    explicit_target_by_accelerator: dict[str, int]
    paid_target_by_accelerator: dict[str, int]
    card_attribution_complete: bool


def _work_to_slots(work: float, capacity: float) -> int:
    """Whole slots needed for `work`, ignoring sub-epsilon float remainders."""
    if capacity <= 0:
        return 0
    return math.ceil(work / capacity - _SLOT_CONVERSION_EPSILON)


def _scale_down_replica_id(target: int | LogicalScaleDownTarget) -> int:
    return target if isinstance(target, int) else target.replica_id


def _prediction_bucket_representative(index: int,
                                      bounds: Sequence[float]) -> float:
    """One duration standing in for every request in a histogram bucket.

    The buckets are log-scale and wide (the 10s-30s bucket spans 3x), so the
    choice of representative moves the estimate far more than it looks. The
    geometric midpoint is the unbiased summary of a log-scale bucket; taking
    the upper bound instead inflates the estimate by the square root of the
    bucket's width, measured at 1.70x against a real production
    distribution where 97% of requests landed in that one bucket.

    That inflation matters because it is invisible. Conservatism in fleet
    sizing belongs in the knobs an operator can see and tune
    (target_utilization_percentage, the provisioning lead, SLA weighting),
    not hidden inside a histogram summary where it silently compounds with
    them.
    """
    upper = bounds[min(index, len(bounds) - 1)]
    if index >= len(bounds):
        # The final bucket is unbounded above; its lower bound is the only
        # honest floor available.
        return upper
    lower = bounds[index - 1] if index > 0 else 0.0
    if lower <= 0:
        # The first bucket starts at zero, whose geometric mean is
        # degenerate; use the arithmetic midpoint.
        return upper / 2.0
    return math.sqrt(lower * upper)


def _generate_scale_up_decisions(
        num: int, target: dict[str, Any] | None) -> list[AutoscalerDecision]:
    return [
        AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                           copy.copy(target)) for _ in range(num)
    ]


def _order_cold_paid_cards(
    configured_cards: list[str],
    placer: spot_placer.SpotPlacer | None,
    configured_gpu_count: typing.Callable[[str], int],
    location_gpu_shape: typing.Callable[[spot_placer.Location], tuple[str,
                                                                      int]],
) -> list[str]:
    """Order paid-capable cold cards from the centralized catalog."""
    if placer is None:
        return list(configured_cards)
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    paid_costs: dict[str, float] = {}
    zero_cost_cards: set[str] = set()
    unpriced_cards: set[str] = set()
    try:
        known_locations = placer.known_locations()
    except Exception:  # pylint: disable=broad-except
        return list(configured_cards)
    for location in known_locations:
        raw_card, gpu_count = location_gpu_shape(location)
        card = canonical_by_name.get(raw_card.casefold())
        if card is None or gpu_count != configured_gpu_count(card):
            continue
        try:
            hourly_cost = float(placer.cost_per_hour(location))
        except Exception:  # pylint: disable=broad-except
            unpriced_cards.add(card)
            continue
        if not math.isfinite(hourly_cost) or hourly_cost < 0:
            unpriced_cards.add(card)
        elif hourly_cost == 0:
            zero_cost_cards.add(card)
        else:
            paid_costs[card] = min(hourly_cost,
                                   paid_costs.get(card, float('inf')))

    # A card is reserved-only only when every inspected location is free and
    # no lookup was inconclusive. Exact-card demand and reserved fill still
    # retain the card; this order governs flexible cold-paid attribution only.
    reserved_only_cards = {
        card for card in configured_cards if card in zero_cost_cards and
        card not in paid_costs and card not in unpriced_cards
    }
    paid_or_unpriced_cards = [
        card for card in configured_cards if card not in reserved_only_cards
    ]
    # An unavailable nominal price keeps service order deterministic instead
    # of letting incomplete provider pricing promote a different card.
    if (not unpriced_cards and
            all(card in paid_costs for card in paid_or_unpriced_cards)):
        service_order = {
            card: index for index, card in enumerate(configured_cards)
        }
        paid_or_unpriced_cards.sort(key=lambda card: (paid_costs.get(
            card, float('inf')), service_order[card]))
    return paid_or_unpriced_cards + [
        card for card in configured_cards if card in reserved_only_cards
    ]


def _generate_scale_down_decisions(
    replica_ids: list[int],
    reason: AutoscalerDecisionReason | None = None,
) -> list[AutoscalerDecision]:
    return [
        AutoscalerDecision(AutoscalerDecisionOperator.SCALE_DOWN,
                           replica_id,
                           reason=reason) for replica_id in replica_ids
    ]


def _select_nonterminal_replicas_to_scale_down(
    num_replica_to_scale_down: int,
    replica_infos: Iterable['replica_managers.ReplicaInfo'],
    service_name: str | None = None,
    cluster_job_counts: dict[str, int] | None = None,
) -> list[int]:
    """Select nonterminal replicas to scale down.

    We sort the replicas based on the following order:
        1. Based on the `scale_down_decision_order` of the status. We terminate
            the replicas that is in earlier stage first, as the replicas in
            later stage may become ready soon.
        2. Based on the version in ascending order, so we scale down the older
            versions first.
        3. For pools, based on the number of running jobs in ascending order,
            so we scale down idle workers first. For SkyServe services, job
            counts will be zero so this criterion has no effect.
        4. Based on the replica_id in descending order, which is also the order
            of the replicas being launched. We scale down the replicas that are
            launched earlier first, as the replicas that are launched later may
            become ready soon.

    Args:
        num_replica_to_scale_down: The number of replicas to scale down.
        replica_infos: The list of replica informations to select from.
        service_name: The name of the pool to query job counts for. When
            provided, replicas with fewer running jobs are scaled down first.
        cluster_job_counts: Optional pre-fetched pool job counts keyed by
            cluster name. When provided, avoids re-querying the same pool
            counts inside a caller that already fetched them.

    Returns:
        The list of replica ids to scale down.
    """
    replicas = list(replica_infos)
    status_order = serve_state.ReplicaStatus.scale_down_decision_order()
    assert all(info.status in status_order for info in replicas), (
        'All replicas to scale down should be in provisioning or launched '
        'status.', replicas)

    # Get the number of running jobs for each replica. For pools this
    # prioritizes scaling down idle workers; when service_name is not
    # provided all counts default to 0 and the sort falls through.
    if service_name is not None:
        if cluster_job_counts is None:
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    service_name))
    if cluster_job_counts is None:
        cluster_job_counts = {}
    replica_job_counts: dict[int, int] = {}
    for info in replicas:
        replica_job_counts[info.replica_id] = (cluster_job_counts.get(
            info.cluster_name, 0))

    replicas = sorted(
        replicas,
        key=lambda info: (
            status_order.index(info.status),
            # version in ascending order
            info.version,
            # number of running jobs in ascending order
            replica_job_counts[info.replica_id],
            # replica_id in descending order, i.e. launched order
            -info.replica_id))
    assert len(replicas) >= num_replica_to_scale_down, (
        'Not enough replicas to scale down. Available replicas: ',
        f'{replicas}, num_replica_to_scale_down: {num_replica_to_scale_down}.')
    return [info.replica_id for info in replicas][:num_replica_to_scale_down]


class Autoscaler:
    """Abstract class for autoscalers."""

    # --------------- APIs to implement for custom autoscaler ---------------

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the autoscaler.

        Variables:
            min_replicas: Minimum number of replicas.
            max_replicas: Maximum number of replicas. Default to fixed
                number of replicas, i.e. min_replicas == max_replicas.
            target_num_replicas: Target number of replicas output by autoscaler.
            latest_version: latest version of the service.
            latest_version_ever_ready: The latest version that is ever ready.
            update_mode: Update mode for the service.
        """
        self._service_name: str = service_name
        self.min_replicas: int = spec.min_replicas
        self.min_replicas_by_accelerator: dict[str, int] = dict(
            getattr(spec, 'min_replicas_by_accelerator', {}))
        self.max_replicas: int = (spec.max_replicas if spec.max_replicas
                                  is not None else spec.min_replicas)
        self.num_overprovision: int | None = spec.num_overprovision
        # Target number of replicas is initialized to min replicas
        self.target_num_replicas: int = max(
            spec.min_replicas, sum(self.min_replicas_by_accelerator.values()))
        self.target_num_replicas_by_accelerator: dict[str, int] = dict(
            self.min_replicas_by_accelerator)
        # Independent explanatory floor for running or occupancy-unknown work
        # on its already-materialized exact card. It is not additive with the
        # cheapest-compatible demand attribution above and need not be its
        # subset.
        self.warm_retention_target_by_accelerator: dict[str, int] = {}
        # Positive incremental exact-card shortage that can authorize a cold
        # launch in the most recent reconciliation tick. Unlike the serving
        # target, this never treats satisfied warm retention as scale-up
        # demand.
        self.cold_launch_authority_by_accelerator: dict[str, int] = {}
        # Seed from the constructed service version (not always
        # INITIAL_VERSION). On a controller restart/respawn the autoscaler is
        # rebuilt; if it reset to version 1 while live replicas are at version
        # >= 2 (any service updated at least once), the version filters below
        # would treat every running replica as outdated and drive permanent
        # replica churn. The caller (`from_spec`) passes the recovered latest
        # version so the autoscaler agrees with the replica manager.
        self.latest_version: int = version
        # The latest_version_ever_ready should be smaller than the
        # latest_version, so we can fail early if the initial version got
        # unrecoverable failure.
        self.latest_version_ever_ready: int = self.latest_version - 1
        # Set only for a never-ready candidate whose persisted replica state
        # satisfies ReplicaStatusProperty.unrecoverable_failure(). The
        # controller durably quarantines this exact version before respawning
        # onto the proven active runtime. Generic provisioning/capacity
        # failures deliberately never populate this signal.
        self._unrecoverable_rollout_failure: (UnrecoverableRolloutFailure |
                                              None) = None
        self.update_mode = serve_utils.DEFAULT_UPDATE_MODE
        # [boltz fork] Reserved-capacity fill (opt-in): snapshot state fed
        # by the controller's poller thread via collect_reserved_capacity.
        # Lives in the base class so fill composes with every autoscaler
        # type without touching their demand math. getattr: robust against
        # spec objects predating the flag (e.g. unpickled from old DB
        # rows).
        self.reserved_capacity_fill: bool = bool(
            getattr(spec, 'reserved_capacity_fill', False))
        # Broker claim parameters, snapshotted from the spec so the poller
        # can read them off the live autoscaler (update_version refreshes
        # them). getattr: spec objects predating the knobs.
        self.reserved_fill_floor_replicas: int = int(
            getattr(spec, 'reserved_fill_floor_replicas', 0) or 0)
        self.reserved_fill_weight: float = float(
            getattr(spec, 'reserved_fill_weight', 1.0) or 1.0)
        # Whether this service releases its whole fill entitlement while it
        # demonstrates no work (see reserved_capacity_broker).
        self.reserved_fill_utilization_gate: bool = bool(
            getattr(spec, 'reserved_fill_utilization_gate', False))
        # Damped free-slot value the fill target acts on (see
        # collect_reserved_capacity for the two-poll increase damping).
        self._fill_free_slots: int = 0
        self._fill_last_raw_free_slots: int | None = None
        self._fill_zero_cost_locations: list[spot_placer.Location] = []
        self._fill_snapshot_time: float | None = None
        # Last computed fill target, surfaced via info() only.
        self._fill_target: int = 0
        # Broker grant ceiling + the epoch it was issued under + the pool
        # key the epoch belongs to (epochs are per-pool round counters, so
        # the launch fence needs both). None grant = no ceiling
        # (single-service #108 identity; also the pre-broker default so
        # every existing call path is unchanged). DELIBERATELY not
        # persisted in dump_dynamic_states: grants are DB-authoritative
        # and the poller re-feeds them within one interval -- a swapped-in
        # autoscaler briefly without a ceiling is safe (ceilings only gate
        # NEW launches).
        self._fill_grant: int | None = None
        self._fill_grant_epoch: int | None = None
        self._fill_grant_pool_key: str | None = None
        self._fill_protocol_version: int = 1
        self._fill_service_generation: int = 0
        self._fill_physical_cluster_uid: str | None = None
        # Protocol-v2 state is a complete map published atomically by one
        # service poll cycle. The legacy scalar fields above remain the exact
        # protocol-v1 implementation and compatibility/status projection.
        self._fill_pool_state_lock = threading.RLock()
        self._fill_pool_states: dict[str, _PoolFillState] = {}
        # Opt-in economic replacement.  The placer reference is injected by
        # the controller each tick because ReplicaManager owns placement state.
        self.cost_rebalance: bool = bool(getattr(spec, 'cost_rebalance', False))
        self.cost_rebalance_min_savings_fraction: float = float(
            getattr(spec, 'cost_rebalance_min_savings_fraction', 0.3))
        self.cost_rebalance_max_parallel_replacements: int = int(
            getattr(spec, 'cost_rebalance_max_parallel_replacements', 1))
        self.cost_rebalance_stabilization_seconds: float = float(
            getattr(spec, 'cost_rebalance_stabilization_seconds', 300.0))
        self._cost_rebalance_spot_placer: spot_placer.SpotPlacer | None = None
        self._cost_rebalance_candidate_since: dict[tuple[int,
                                                         spot_placer.Location],
                                                   float] = {}
        self._cost_rebalance_state_dirty = False
        self._cost_rebalance_replica_cost_cache: dict[int, float] = {}
        # Freshness fence for priority-only gauges. A stale LB report may keep
        # a conservative scale-up target, but it must not keep refreshing a
        # high-priority paid-capacity waiter indefinitely.
        self._launch_priority_report_received_at: float | None = None

    def get_final_target_num_replicas(self) -> int:
        """Get the final target number of replicas."""
        if self.num_overprovision is None:
            return self.target_num_replicas
        return self.target_num_replicas + self.num_overprovision

    def current_launch_priority(self) -> int:
        """Highest recent demand priority that may require fresh capacity."""
        if not self._launch_priority_evidence_is_fresh():
            return constants.LB_REQUEST_PRIORITY_MIN
        priorities = [constants.LB_REQUEST_PRIORITY_MIN]
        by_priority = getattr(self, '_queue_depth_by_priority', None)
        if isinstance(by_priority, dict):
            priorities.extend(
                int(priority)
                for priority, count in by_priority.items()
                if isinstance(priority, int) and
                not isinstance(priority, bool) and isinstance(count, int) and
                not isinstance(count, bool) and count > 0)
        for field in ('queued_compatibility_profiles',
                      'rejected_compatibility_profiles',
                      'compatibility_profiles'):
            profiles = getattr(self, field, ())
            if not isinstance(profiles, list):
                continue
            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                priority = profile.get('priority')
                count = profile.get('recent_count', profile.get('count', 0))
                if (isinstance(priority, int) and
                        not isinstance(priority, bool) and
                        isinstance(count, (int, float)) and
                        not isinstance(count, bool) and count > 0):
                    priorities.append(priority)
        return max(constants.LB_REQUEST_PRIORITY_MIN,
                   min(constants.LB_REQUEST_PRIORITY_MAX, max(priorities)))

    def _launch_priority_evidence_is_fresh(self) -> bool:
        received_at = self._launch_priority_report_received_at
        if received_at is None:
            return False
        threshold = 3.0 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS
        return time.time() - received_at <= threshold

    def current_launch_priorities_by_accelerator(
            self, accelerators: Iterable[str]) -> dict[str, int]:
        """Highest active priority whose compatibility includes each card."""
        canonical = {
            str(accelerator).casefold(): str(accelerator)
            for accelerator in accelerators
        }
        priorities = {
            accelerator: constants.LB_REQUEST_PRIORITY_MIN
            for accelerator in canonical.values()
        }
        if not self._launch_priority_evidence_is_fresh():
            return priorities
        saw_valid_profile = False
        for field in ('queued_compatibility_profiles',
                      'rejected_compatibility_profiles',
                      'compatibility_profiles'):
            profiles = getattr(self, field, ())
            if not isinstance(profiles, list):
                continue
            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                priority = profile.get('priority')
                count = profile.get('recent_count', profile.get('count', 0))
                if (not isinstance(priority, int) or
                        isinstance(priority, bool) or
                        not isinstance(count, (int, float)) or
                        isinstance(count, bool) or count <= 0):
                    continue
                saw_valid_profile = True
                compatible = profile.get('compatible_accelerators')
                if not isinstance(compatible, (list, tuple)) or not compatible:
                    matching = list(priorities)
                else:
                    matching = [
                        canonical[str(card).casefold()]
                        for card in compatible
                        if str(card).casefold() in canonical
                    ]
                if not matching:
                    continue
                clamped = max(constants.LB_REQUEST_PRIORITY_MIN,
                              min(constants.LB_REQUEST_PRIORITY_MAX, priority))
                for card in matching:
                    priorities[card] = max(priorities[card], clamped)
        if not saw_valid_profile:
            fallback = self.current_launch_priority()
            return {card: fallback for card in priorities}
        return priorities

    @property
    def unrecoverable_rollout_failure(
            self) -> UnrecoverableRolloutFailure | None:
        """Return this tick's typed never-ready rollout failure, if any."""
        return self._unrecoverable_rollout_failure

    def _calculate_target_num_replicas(self) -> int:
        """Calculate target number of replicas."""
        raise NotImplementedError

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            logger.error(f'Invalid version: {version}, '
                         f'latest version: {self.latest_version}')
            return
        self.latest_version = version
        self.min_replicas = spec.min_replicas
        self.min_replicas_by_accelerator = dict(
            getattr(spec, 'min_replicas_by_accelerator', {}))
        self.max_replicas = (spec.max_replicas if spec.max_replicas is not None
                             else spec.min_replicas)
        # Re-clip self.target_num_replicas with new min and max replicas.
        self.target_num_replicas = self._clip_target_num_replicas(
            self.target_num_replicas)
        self.update_mode = update_mode
        # An update can toggle the fill flag; consumption follows the new
        # spec immediately. (The controller's update_service handler
        # seeds the zero-cost location set and starts the poller when an
        # update enables the flag -- no respawn needed, provided the spot
        # placer already exists.)
        self.reserved_capacity_fill = bool(
            getattr(spec, 'reserved_capacity_fill', False))
        # Broker claim knobs follow the update too: the poller reads them
        # off the live autoscaler on its next heartbeat.
        self.reserved_fill_floor_replicas = int(
            getattr(spec, 'reserved_fill_floor_replicas', 0) or 0)
        self.reserved_fill_weight = float(
            getattr(spec, 'reserved_fill_weight', 1.0) or 1.0)
        self.reserved_fill_utilization_gate = bool(
            getattr(spec, 'reserved_fill_utilization_gate', False))
        with self._fill_pool_state_lock:
            # A service update may add/remove/reorder pool edges and therefore
            # advance the authoritative service generation. Preserve location
            # identity for scale-down shelter, but invalidate all old feed
            # until the poller publishes the new complete generation.
            for pool_state in self._fill_pool_states.values():
                pool_state.free_slots = 0
                pool_state.last_raw_free_slots = None
                # Shelter-only until the next exact-generation heartbeat:
                # preserve only the last real broker entitlement. Zero feed
                # cannot authorize a launch under it, while widening the grant
                # to edge_cap would let an update shelter holdings that a peer
                # had already been granted.
                pool_state.shelter_grant = min(pool_state.shelter_grant,
                                               pool_state.edge_cap)
                pool_state.grant = 0
                pool_state.grant_epoch = None
            self._refresh_legacy_fill_projection_locked()
        self.cost_rebalance = bool(getattr(spec, 'cost_rebalance', False))
        self.cost_rebalance_min_savings_fraction = float(
            getattr(spec, 'cost_rebalance_min_savings_fraction', 0.3))
        self.cost_rebalance_max_parallel_replacements = int(
            getattr(spec, 'cost_rebalance_max_parallel_replacements', 1))
        self.cost_rebalance_stabilization_seconds = float(
            getattr(spec, 'cost_rebalance_stabilization_seconds', 300.0))
        self._clear_cost_rebalance_candidates()
        self.warm_retention_target_by_accelerator = {}
        self.cold_launch_authority_by_accelerator = {}

    def set_spot_placer(self, placer: spot_placer.SpotPlacer | None) -> None:
        """Publish ReplicaManager's live placement/bench state for this tick."""
        self._cost_rebalance_spot_placer = placer

    def _clear_cost_rebalance_candidates(self) -> None:
        if self._cost_rebalance_candidate_since:
            self._cost_rebalance_candidate_since.clear()
            self._cost_rebalance_state_dirty = True

    def dump_cost_rebalance_state(self) -> dict[str, Any]:
        """Return bounded JSON-safe continuous-eligibility evidence."""
        limit = min(_COST_REBALANCE_STATE_MAX_ENTRIES,
                    max(16, 4 * self.cost_rebalance_max_parallel_replacements))
        candidates = []
        for (replica_id, location), first_seen_at in list(
                self._cost_rebalance_candidate_since.items())[:limit]:
            if not math.isfinite(first_seen_at):
                continue
            candidates.append({
                'replica_id': replica_id,
                'location': location.to_pickleable(),
                'first_seen_at': first_seen_at,
            })
        return {
            'version': _COST_REBALANCE_STATE_VERSION,
            'service_version': self.latest_version,
            'candidates': candidates,
        }

    def load_cost_rebalance_state(self, state: dict[str, Any] | None) -> None:
        """Restore candidate timers without extending them across a restart."""
        if (not isinstance(state, dict) or
                state.get('version') != _COST_REBALANCE_STATE_VERSION or
                state.get('service_version') != self.latest_version):
            return
        candidates = state.get('candidates')
        if not isinstance(candidates, list):
            return
        limit = min(_COST_REBALANCE_STATE_MAX_ENTRIES,
                    max(16, 4 * self.cost_rebalance_max_parallel_replacements))
        now = time.time()
        restored = {}
        for raw in candidates[:limit]:
            if not isinstance(raw, dict):
                continue
            replica_id = raw.get('replica_id')
            first_seen_at = raw.get('first_seen_at')
            if (not isinstance(replica_id, int) or
                    isinstance(replica_id, bool) or replica_id < 0 or
                    not isinstance(first_seen_at, (int, float)) or
                    isinstance(first_seen_at, bool) or
                    not math.isfinite(first_seen_at)):
                continue
            raw_location = raw.get('location')
            if not isinstance(raw_location, dict):
                continue
            try:
                location = spot_placer.Location.from_pickleable(raw_location)
            except (AssertionError, KeyError, TypeError, ValueError):
                continue
            if location is None:
                continue
            restored[(replica_id, location)] = min(float(first_seen_at), now)
        self._cost_rebalance_candidate_since = restored
        self._cost_rebalance_state_dirty = False

    @property
    def cost_rebalance_state_dirty(self) -> bool:
        return self._cost_rebalance_state_dirty

    def mark_cost_rebalance_state_persisted(self) -> None:
        self._cost_rebalance_state_dirty = False

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling."""
        raise NotImplementedError

    def collect_reserved_capacity(
            self,
            free_slots: int,
            zero_cost_location_keys: list[dict[str, Any]],
            timestamp: float,
            grant: int | None = None,
            grant_epoch: int | None = None,
            grant_pool_key: str | None = None,
            protocol_version: int = 1,
            service_generation: int = 0,
            physical_cluster_uid: str | None = None) -> None:
        """Ingest a free-capacity snapshot from the reserved-capacity poller.

        `zero_cost_location_keys` are Location.to_pickleable() dicts of
        the placer's zero-cost location set (benched ones included: they
        still identify which existing replicas are fill).

        Damping: an INCREASE in free slots is acted on only when two
        consecutive snapshots both exceed the previously-acted-on value
        (acting on the min of the two -- the level that persisted across
        both polls), so an eviction storm's transient free spike cannot
        cause launch/evict churn. A DECREASE applies immediately:
        capacity that vanished must stop being filled now.

        grant/grant_epoch/grant_pool_key come from the reserved-fill
        broker: grant is the entitlement ceiling on the FILL fleet (None =
        no ceiling, the single-service identity), grant_epoch the fencing
        token stamped onto fill scale-up decisions so a launch outliving
        its allocation round is skipped at actuation time, and
        grant_pool_key the pool the epoch belongs to (epochs are per-pool
        round counters; the fence compares against that pool's round).
        """
        if int(protocol_version) == 1:
            # Explicit protocol demotion: scalar state becomes authoritative.
            # A retained v2 map would otherwise make the overlay ignore every
            # subsequent v1 heartbeat forever.
            with self._fill_pool_state_lock:
                self._fill_pool_states = {}
        free_slots = max(0, int(free_slots))
        prev_raw = self._fill_last_raw_free_slots
        self._fill_last_raw_free_slots = free_slots
        if free_slots <= self._fill_free_slots:
            self._fill_free_slots = free_slots
        elif prev_raw is not None and prev_raw > self._fill_free_slots:
            self._fill_free_slots = min(prev_raw, free_slots)
        self._fill_zero_cost_locations = [
            location for location in (spot_placer.Location.from_pickleable(key)
                                      for key in zero_cost_location_keys)
            if location is not None
        ]
        self._fill_snapshot_time = timestamp
        self._fill_grant = grant
        self._fill_grant_epoch = grant_epoch
        self._fill_grant_pool_key = grant_pool_key
        self._fill_protocol_version = int(protocol_version)
        self._fill_service_generation = int(service_generation)
        self._fill_physical_cluster_uid = physical_cluster_uid

    def collect_reserved_capacity_pools(
        self,
        pool_snapshots: dict[str, dict[str, Any]],
    ) -> None:
        """Atomically ingest one complete protocol-v2 pool snapshot map.

        Every entry must describe the same authoritative service generation.
        A pool without an exact-generation round is still present, but carries
        ``free_slots=0`` and ``grant=0``. A generation change starts damping
        from zero, so feed from the old cross-pool budget cannot survive the
        atomic map swap.
        """
        parsed: dict[str, _PoolFillState] = {}
        generations: set[int] = set()
        for map_key, snapshot in pool_snapshots.items():
            pool_key = str(snapshot.get('pool_key', map_key))
            if pool_key != map_key:
                raise ValueError('Reserved-fill pool snapshot key mismatch: '
                                 f'{map_key!r} != {pool_key!r}.')
            protocol_version = int(snapshot.get('protocol_version', 0))
            if protocol_version != 2:
                raise ValueError('Multi-pool snapshots require reserved-fill '
                                 f'protocol 2, got {protocol_version!r}.')
            generation = int(snapshot['service_generation'])
            if generation < 1:
                raise ValueError('Reserved-fill service generation must be '
                                 'positive under protocol 2.')
            generations.add(generation)
            edge_cap = max(0, int(snapshot['edge_cap']))
            raw_free = max(0, int(snapshot.get('free_slots', 0)))
            raw_free_by_accelerator = snapshot.get('free_slots_by_accelerator')
            free_by_accelerator: dict[str, int] | None = None
            if raw_free_by_accelerator is not None:
                if not isinstance(raw_free_by_accelerator, dict):
                    raise ValueError('Protocol-v2 exact-card feed must be a '
                                     'mapping when present.')
                free_by_accelerator = {}
                for raw_card, raw_count in raw_free_by_accelerator.items():
                    if (not isinstance(raw_card, str) or not raw_card or
                            isinstance(raw_count, bool) or
                            not isinstance(raw_count, int) or raw_count < 0):
                        raise ValueError('Protocol-v2 exact-card feed contains '
                                         'an invalid card/count entry.')
                    card = raw_card.casefold()
                    if card in free_by_accelerator:
                        raise ValueError('Protocol-v2 exact-card feed contains '
                                         'duplicate card identities.')
                    if raw_count > 0:
                        free_by_accelerator[card] = raw_count
                if sum(free_by_accelerator.values()) != raw_free:
                    raise ValueError('Protocol-v2 exact-card feed must sum to '
                                     'its aggregate free-slot feed.')
            grant = max(0, min(edge_cap, int(snapshot.get('grant', 0))))
            shelter_grant = max(
                0, min(edge_cap, int(snapshot.get('shelter_grant', grant))))
            locations = [
                location for location in (
                    spot_placer.Location.from_pickleable(key)
                    for key in snapshot.get('zero_cost_location_keys', []))
                if location is not None
            ]
            physical_uid = snapshot.get('physical_cluster_uid')
            if not isinstance(physical_uid, str) or not physical_uid:
                raise ValueError('Protocol-v2 pool snapshot requires a '
                                 'physical Kubernetes cluster UID.')
            parsed[pool_key] = _PoolFillState(
                protocol_version=protocol_version,
                pool_key=pool_key,
                physical_cluster_uid=physical_uid,
                service_generation=generation,
                edge_cap=edge_cap,
                free_slots_by_accelerator=free_by_accelerator,
                zero_cost_locations=locations,
                snapshot_time=float(snapshot['timestamp']),
                shelter_grant=shelter_grant,
                grant=grant,
                grant_epoch=(None if snapshot.get('grant_epoch') is None else
                             int(snapshot['grant_epoch'])),
            )
            # Damping is filled under the lock from the prior exact-generation
            # state; raw_free remains local so no half-updated map is visible.
            parsed[pool_key].last_raw_free_slots = raw_free

        if len(generations) > 1:
            raise ValueError('A complete reserved-fill pool map must carry '
                             f'one service generation, got {generations}.')

        with self._fill_pool_state_lock:
            previous = self._fill_pool_states
            for pool_key, state in parsed.items():
                raw_free = state.last_raw_free_slots or 0
                prior = previous.get(pool_key)
                if (prior is None or
                        prior.service_generation != state.service_generation or
                        prior.physical_cluster_uid
                        != state.physical_cluster_uid):
                    # A newly authorized generation gets no feed on its first
                    # sample. The next exact-generation sample confirms the
                    # increase, mirroring protocol-v1 two-poll damping.
                    state.free_slots = 0
                    state.last_raw_free_slots = raw_free
                else:
                    state.free_slots = prior.free_slots
                    previous_raw = prior.last_raw_free_slots
                    state.last_raw_free_slots = raw_free
                    if raw_free <= state.free_slots:
                        state.free_slots = raw_free
                    elif (previous_raw is not None and
                          previous_raw > state.free_slots):
                        state.free_slots = min(previous_raw, raw_free)
                state.free_slots = min(state.free_slots, state.edge_cap)
            self._fill_pool_states = parsed
            self._refresh_legacy_fill_projection_locked()

    def seed_zero_cost_pools(
        self,
        pool_location_keys: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Seed protocol-v2 location identity without authorizing feed."""
        with self._fill_pool_state_lock:
            if self._fill_pool_states:
                return
            # A seed intentionally lacks protocol/generation/UID authority.
            # Keep it only in the aggregate legacy location projection for
            # restart-time scale-down protection; it cannot launch.
            self._fill_zero_cost_locations = [
                location for keys in pool_location_keys.values()
                for location in (
                    spot_placer.Location.from_pickleable(key) for key in keys)
                if location is not None
            ]

    def _refresh_legacy_fill_projection_locked(self) -> None:
        """Refresh scalar compatibility/status fields from the v2 map."""
        states = list(self._fill_pool_states.values())
        self._fill_free_slots = sum(state.free_slots for state in states)
        raw_values = [
            state.last_raw_free_slots
            for state in states
            if state.last_raw_free_slots is not None
        ]
        self._fill_last_raw_free_slots = (sum(raw_values)
                                          if raw_values else None)
        self._fill_zero_cost_locations = [
            location for state in states
            for location in state.zero_cost_locations
        ]
        timestamps = [
            state.snapshot_time
            for state in states
            if state.snapshot_time is not None
        ]
        # The oldest component controls aggregate freshness.
        self._fill_snapshot_time = min(timestamps) if timestamps else None
        self._fill_grant = sum(state.grant for state in states)
        self._fill_grant_epoch = None
        self._fill_grant_pool_key = None

    def _pool_fill_states_snapshot(self) -> dict[str, _PoolFillState]:
        with self._fill_pool_state_lock:
            return {
                key: state.detached_copy()
                for key, state in self._fill_pool_states.items()
            }

    def get_reserved_capacity_pool_shelter_grant(self, pool_key: str, *,
                                                 service_generation: int,
                                                 physical_cluster_uid: str,
                                                 edge_cap: int) -> int:
        """Return clipped, non-launching shelter from an exact prior edge.

        The broker poller uses this only after a protocol-v2 round failed to
        return an allocation.  Pool identity and service generation are both
        fenced so neither a removed/re-added edge nor a same-name physical
        cluster replacement can inherit stale shelter.
        """
        with self._fill_pool_state_lock:
            prior = self._fill_pool_states.get(pool_key)
            if (prior is None or prior.protocol_version != 2 or
                    prior.service_generation != service_generation or
                    prior.physical_cluster_uid != physical_cluster_uid):
                return 0
            return max(0, min(int(edge_cap), prior.shelter_grant))

    @staticmethod
    def _location_in_pool(location: spot_placer.Location,
                          state: _PoolFillState) -> bool:
        return any(
            spot_placer.locations_match_placement(location, candidate)
            for candidate in state.zero_cost_locations)

    def _fill_pool_key_for_replica(
        self,
        info: 'replica_managers.ReplicaInfo',
        states: dict[str, _PoolFillState],
    ) -> str | None:
        # Read persisted fields without triggering unittest.mock.Mock's dynamic
        # attribute synthesis: only actual row state is provenance authority.
        try:
            persisted = vars(info)
        except TypeError:
            persisted = {}
        persisted_key = persisted.get('reserved_fill_pool_key')
        persisted_generation = persisted.get('reserved_fill_service_generation')
        persisted_uid = persisted.get('reserved_fill_physical_cluster_uid')
        provenance = (persisted_key, persisted_generation, persisted_uid)
        if any(value is not None for value in provenance):
            # Once any v2 origin field exists, the trio is authoritative.  A
            # partial, malformed, retargeted, or future-generation row must not
            # be re-attributed by a coincidentally matching context/location.
            if (not isinstance(persisted_key, str) or not persisted_key or
                    isinstance(persisted_generation, bool) or
                    not isinstance(persisted_generation, int) or
                    persisted_generation < 1 or
                    not isinstance(persisted_uid, str) or not persisted_uid):
                return None
            state = states.get(persisted_key)
            if (state is None or persisted_uid != state.physical_cluster_uid or
                    persisted_generation > state.service_generation):
                return None
            location = info.get_spot_location()
            if (location is None or
                    not self._location_in_pool(location, state)):
                # Explicit origin and persisted placement are one authority
                # tuple.  A retargeted/corrupt row must not consume shelter
                # from either its claimed pool or a coincidentally matching
                # replacement pool.
                return None
            # Older positive generations remain valid for existing holdings:
            # the generation is the immutable launch fence and is expected to
            # trail the service set after later cap/policy heartbeats.
            return persisted_key

        # Only genuinely legacy rows (and ordinary demand rows), for which all
        # three origin fields are absent, may use exact location attribution.
        location = info.get_spot_location()
        if location is None:
            return None
        matches = [
            pool_key for pool_key, state in states.items()
            if self._location_in_pool(location, state)
        ]
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _exact_launch_shapes_for_pool(
        state: _PoolFillState,) -> dict[str, tuple[str, int]] | None:
        """Return normalized card -> exact launch shape in location order."""
        try:
            identity = reserved_capacity_broker.parse_pool_identity(
                state.pool_key)
        except (TypeError, ValueError):
            return None
        if identity.protocol_version != 2:
            return None
        shapes: dict[str, tuple[str, int]] = {}
        for location in state.zero_cost_locations:
            accelerators = location.accelerators
            if not isinstance(accelerators, dict) or len(accelerators) != 1:
                return None
            raw_card, raw_count = next(iter(accelerators.items()))
            if (not isinstance(raw_card, str) or not raw_card or
                    isinstance(raw_count, bool) or
                    not isinstance(raw_count, (int, float)) or
                    not float(raw_count).is_integer() or raw_count <= 0):
                return None
            card = raw_card.casefold()
            if card not in identity.gpu_names:
                return None
            shape = (raw_card, int(raw_count))
            prior = shapes.get(card)
            if prior is not None and prior[1] != shape[1]:
                return None
            shapes.setdefault(card, shape)
        return shapes or None

    def fill_demand_sample(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> 'FillDemandSample | None':
        """Work this service can demonstrate on its zero-cost tier.

        Read-only projection for the reserved-fill poller thread, which
        calls it once per poll from _broker_cycle. None means "no usable
        telemetry". For an armed utilization gate, the poller publishes that
        as fresh NULL need: the broker freezes for its 900s blind grace and
        then resumes bounded decay if blindness persists.

        The base class has no per-replica occupancy signal, so it returns
        None. A service that needs static reservation behavior must explicitly
        set utilization_gate: false.
        """
        del replica_infos  # Unused: no occupancy telemetry on the base.
        return None

    def count_zero_cost_holdings(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> tuple[int, int]:
        """(fill, demand) split of nonterminal zero-cost replicas.

        The broker claim heartbeat reports this split: fill holdings are
        broker property (arbitrated by grants), demand-placed rows are
        demand-protected and exempt from the ceiling. Rows pickled before
        the reserved_fill flag existed read as demand (getattr default) --
        the conservative direction: they keep their shelter and inflate
        nobody's fill count.
        """
        fill = 0
        demand = 0
        for info in replica_infos:
            if info.is_terminal:
                continue
            if not self._replica_on_zero_cost_location(info):
                continue
            if getattr(info, 'reserved_fill', False):
                fill += 1
            else:
                demand += 1
        return fill, demand

    def count_zero_cost_holdings_by_pool(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        pool_location_keys: dict[str, list[dict[str, Any]]] | None = None,
        pool_authority: dict[str, tuple[str, int]] | None = None,
    ) -> dict[str, tuple[int, int]]:
        """Return the fill/demand holdings split for every v2 pool.

        ``pool_authority`` supplies the durable UID/current generation during
        controller restart, before an in-memory pool snapshot exists. It is
        required to validate explicit v2 provenance; location-only fallback is
        reserved for rows whose entire provenance trio predates protocol v2.
        """
        states = self._pool_fill_states_snapshot()
        if pool_location_keys is not None:
            for pool_key, keys in pool_location_keys.items():
                if pool_key in states:
                    continue
                locations = [
                    location
                    for location in (spot_placer.Location.from_pickleable(key)
                                     for key in keys)
                    if location is not None
                ]
                authority = (pool_authority or {}).get(pool_key)
                physical_uid, generation = (authority if authority is not None
                                            else ('', 0))
                states[pool_key] = _PoolFillState(
                    protocol_version=2,
                    pool_key=pool_key,
                    physical_cluster_uid=(physical_uid),
                    service_generation=generation,
                    edge_cap=0,
                    zero_cost_locations=locations)
        counts = {pool_key: [0, 0] for pool_key in states}
        for info in replica_infos:
            if info.is_terminal:
                continue
            replica_pool_key = self._fill_pool_key_for_replica(info, states)
            if replica_pool_key is None:
                continue
            index = 0 if getattr(info, 'reserved_fill', False) else 1
            counts[replica_pool_key][index] += self._fill_capacity_units(info)
        return {
            pool_key: (values[0], values[1])
            for pool_key, values in counts.items()
        }

    def seed_zero_cost_locations(
            self, zero_cost_location_keys: list[dict[str, Any]]) -> None:
        """Seed the zero-cost location set WITHOUT granting free slots.

        Called synchronously by the controller (at boot, and on the
        autoscaler swap in update_service) with the placer's spec-derived
        location set, BEFORE the seeded instance takes decision ticks.
        After a controller respawn the fill state is empty (boot builds
        the autoscaler via from_spec; there is no dump/load across
        processes) and the first poll can lag the first decision tick by
        a lot (per-location cost warm-up + the cluster-wide realtime
        query). A QPS-family autoscaler's first tick then computes
        target=min_replicas from its empty window and, with
        zero_cost_count=0, suppression cannot shelter the live fill
        fleet from the resulting mass scale-down. Seeding only the
        location set makes zero_cost_count-based suppression work from
        tick zero, while _fill_snapshot_time stays None and free slots
        stay 0 so no new fill is launched until the first real poll.

        A loaded dump wins: never overwrite an existing location set
        (it may carry a fresher view than the spec-derived one).
        """
        if self._fill_zero_cost_locations:
            return
        self._fill_zero_cost_locations = [
            location for location in (spot_placer.Location.from_pickleable(key)
                                      for key in zero_cost_location_keys)
            if location is not None
        ]

    def _fresh_fill_free_slots(self) -> int:
        """Damped free slots, decayed to 0 when the snapshot is stale."""
        if self._fill_snapshot_time is None:
            return 0
        max_age = (reserved_capacity.poll_interval_seconds() *
                   constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
        if time.time() - self._fill_snapshot_time > max_age:
            return 0
        return self._fill_free_slots

    # Kept as a staticmethod alias: the matcher moved to spot_placer so the
    # launch path's demand-placement gate can share it without importing
    # autoscalers (see spot_placer.locations_match_placement for the full
    # relaxed-identity rationale).
    _fill_location_matches = staticmethod(spot_placer.locations_match_placement)

    def _fill_row_occupies_free_slot(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether a zero-cost row occupies a slot the snapshot counted free.

        Subtract rows that are (not READY) OR (created after the
        snapshot). Each row is evaluated once against this single
        predicate, so the two clauses can never double-subtract the same
        row:
        - not READY: launched-but-unbound pods are invisible to the
          poller, so their slots still read free. A not-READY row OLDER
          than the snapshot (long provisioning) may in fact have a bound
          pod the poll already excluded; still subtracting it is the
          conservative direction -- never over-launch, at worst
          under-fill until it turns READY (layer 3 re-syncs).
        - created after the snapshot: a DEMAND launch placed on the
          zero-cost tier that binds AND turns READY within one
          inter-poll gap escapes the not-READY clause, yet the slot it
          sits on was counted free when the snapshot was taken. Any
          zero-cost row newer than the snapshot occupies such a slot
          regardless of readiness.
        Rows without a creation timestamp (pickles from builds predating
        ReplicaInfo.created_at) are treated as older than the snapshot:
        they predate this build entirely, their bound pods are already
        excluded by every fresh poll, and always-subtracting them would
        under-fill for their whole lifetime.

        Known sampling window (accepted): a row created BEFORE the
        snapshot whose pod binds after it escapes both clauses once
        READY -- up to one poll interval of over-launch; the extra fill
        fails fast on the full tier and at worst benches it for one
        retry TTL. Inherent to sampling free capacity at an instant.
        """
        if not info.is_ready:
            return True
        if self._fill_snapshot_time is None:
            # No snapshot: spendable free slots are 0 regardless.
            return False
        created_at = getattr(info, 'created_at', None)
        return created_at is not None and created_at > self._fill_snapshot_time

    def _replica_on_zero_cost_location(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        if not self._fill_zero_cost_locations:
            return False
        location = info.get_spot_location()
        if location is None:
            return False
        return any(
            self._fill_location_matches(location, zero_cost)
            for zero_cost in self._fill_zero_cost_locations)

    def is_replica_on_zero_cost_location(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether a replica occupies a configured zero-cost location.

        The controller uses this same classifier for exact-card history.  In
        particular, legacy ReplicaInfo rows predate persisted is_zero_cost
        provenance but still retain enough placement identity to match the
        autoscaler's active reserved locations.
        """
        return self._replica_on_zero_cost_location(info)

    def _fill_capacity_units(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Autoscaling units represented by one row for fill accounting."""
        del info
        return 1

    def _exact_card_fill_shelter(
        self,
        zero_cost_infos: list['replica_managers.ReplicaInfo'],
        fill_target: int,
    ) -> tuple[dict[str, int], dict[int, str]] | None:
        """Return per-card scale-down shelter and replica attribution.

        The aggregate fill target overlaps only demand assigned to the same
        exact reserved card. Existing zero-cost holdings receive the target
        first, in configured card order, so a demand-only downscale cannot
        drain one reserved card and immediately refill another. Autoscalers
        without a complete exact-card view retain the legacy aggregate path.
        """
        demand_target = getattr(self, 'target_num_replicas_by_accelerator', {})
        configured_shapes = getattr(self, 'configured_accelerator_shapes', {})
        if (not isinstance(demand_target, dict) or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or
                not getattr(self, '_compatibility_demand_complete', False)):
            return None

        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        demand_by_card: dict[str, int] = {}
        for raw_card, raw_target in demand_target.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                return None
            demand_by_card[card] = max(0, int(raw_target))
        if sum(demand_by_card.values()) != self.get_final_target_num_replicas():
            # Generic overprovision and stale/partial maps do not have a safe
            # exact-card attribution. The aggregate path still enforces the
            # fill ceiling without guessing where that demand belongs.
            return None

        current_by_card: dict[str, int] = {}
        replica_cards: dict[int, str] = {}
        for info in zero_cost_infos:
            location = info.get_spot_location()
            accelerators = (location.accelerators
                            if location is not None else None)
            if not accelerators or len(accelerators) != 1:
                return None
            raw_card = next(iter(accelerators))
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                # Old-version or partially launched rows may not have a
                # trustworthy exact shape. Aggregate shelter is conservative
                # and avoids inventing an accelerator identity for them.
                return None
            replica_cards[info.replica_id] = card
            current_by_card[card] = (current_by_card.get(card, 0) +
                                     self._fill_capacity_units(info))

        remaining = max(0, fill_target)
        fill_by_card: dict[str, int] = {}
        for card in configured_shapes:
            canonical = canonical_by_name[str(card).casefold()]
            allocated = min(remaining, current_by_card.get(canonical, 0))
            if allocated > 0:
                fill_by_card[canonical] = allocated
                remaining -= allocated
            if remaining <= 0:
                break
        shelter = {
            card: max(0, fill - demand_by_card.get(card, 0))
            for card, fill in fill_by_card.items()
        }
        return shelter, replica_cards

    def _reserved_slots_claimed_by_demand(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> tuple[int, dict[str, int] | None, dict[str, int] | None]:
        """Count free exact-card slots already claimed by demand decisions.

        Reserved fill is overlaid after ordinary demand scaling. A shaped
        demand launch can consume one of the same freshly reported reserved
        slots, so emitting the full fill delta as well would create two rows
        for one physical slot. Count only claims that match a currently free
        exact card. Unknown or aggregate decisions retain the legacy fill
        behavior because they cannot be reconciled safely by card here.  The
        third return value preserves the exact-card split so protocol v2 can
        debit only compatible physical pools instead of assigning an H200
        demand claim to (for example) an unrelated L4 pool.
        """
        raw_free = getattr(self, 'free_reserved_slots_by_accelerator', None)
        configured_shapes = getattr(self, 'configured_accelerator_shapes', {})
        shape_resolver = getattr(self, '_get_gpu_shape_from_replica_info', None)
        if (not isinstance(raw_free, dict) or not raw_free or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or not callable(shape_resolver)):
            return 0, None, None
        shape_resolver_fn = typing.cast(
            typing.Callable[['replica_managers.ReplicaInfo'], tuple[str, int]],
            shape_resolver)
        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        remaining_free: dict[str, int] = {}
        for raw_card, raw_count in raw_free.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                continue
            remaining_free[card] = max(0, int(raw_count))

        current_capacity_by_card = {
            card: 0 for card in canonical_by_name.values()
        }
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = shape_resolver_fn(  # pylint: disable=not-callable
                info)
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is not None:
                current_capacity_by_card[card] += self._fill_capacity_units(
                    info)

        claimed = 0
        claimed_by_card: dict[str, int] = {}

        def claim(card: str, count: int) -> None:
            nonlocal claimed
            available = remaining_free.get(card, 0)
            consumed = min(available, max(0, count))
            remaining_free[card] = available - consumed
            claimed += consumed
            if consumed > 0:
                claimed_by_card[card] = (claimed_by_card.get(card, 0) +
                                         consumed)

        for decision in decisions:
            if decision.operator != AutoscalerDecisionOperator.SCALE_UP:
                continue
            target = decision.target
            if isinstance(target, dict):
                if target.get(constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY):
                    continue
                accelerators = target.get('accelerators')
                if not isinstance(accelerators, dict) or len(accelerators) != 1:
                    continue
                raw_card = next(iter(accelerators))
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is not None:
                    claim(card, 1)
                continue
            if not isinstance(target, LogicalScaleTarget):
                continue
            target_by_card = dict(target.target_capacity_by_accelerator)
            shapes = dict(target.accelerator_shapes)
            for raw_card, raw_target in target_by_card.items():
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is None:
                    continue
                raw_gpu_count = shapes.get(raw_card)
                if raw_gpu_count is None:
                    raw_gpu_count = configured_shapes.get(card)
                if (not isinstance(raw_gpu_count, int) or
                        isinstance(raw_gpu_count, bool) or raw_gpu_count <= 0):
                    continue
                gpu_count = raw_gpu_count
                shortfall = max(
                    0,
                    int(raw_target) - current_capacity_by_card.get(card, 0))
                claim(card, math.ceil(shortfall / gpu_count))
        return claimed, remaining_free, claimed_by_card

    def _fresh_pool_fill_free_slots(self, state: _PoolFillState) -> int:
        if state.snapshot_time is None:
            return 0
        max_age = (reserved_capacity.poll_interval_seconds() *
                   constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
        if time.time() - state.snapshot_time > max_age:
            return 0
        return min(state.edge_cap, state.free_slots)

    def _exact_card_pool_shelter(
        self,
        data: dict[str, dict[str, Any]],
        targets: dict[str, int],
        ordered_keys: list[str],
    ) -> tuple[dict[str, dict[str, int]], dict[int, str]] | None:
        """Partition exact-card demand coverage independently by pool."""
        demand_target = getattr(self, 'target_num_replicas_by_accelerator', {})
        configured_shapes = getattr(self, 'configured_accelerator_shapes', {})
        if (not isinstance(demand_target, dict) or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or
                not getattr(self, '_compatibility_demand_complete', False)):
            return None
        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        demand_by_card: dict[str, int] = {}
        for raw_card, raw_target in demand_target.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                return None
            demand_by_card[card] = max(0, int(raw_target))
        if sum(demand_by_card.values()) != self.get_final_target_num_replicas():
            return None

        replica_cards: dict[int, str] = {}
        targets_by_pool_card: dict[str, dict[str, int]] = {}
        for pool_key in ordered_keys:
            current_by_card: dict[str, int] = {}
            for info in data[pool_key]['infos']:
                location = info.get_spot_location()
                accelerators = (location.accelerators
                                if location is not None else None)
                if not accelerators or len(accelerators) != 1:
                    return None
                raw_card = next(iter(accelerators))
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is None:
                    return None
                replica_cards[info.replica_id] = card
                current_by_card[card] = (current_by_card.get(card, 0) +
                                         self._fill_capacity_units(info))
            remaining = targets[pool_key]
            pool_targets: dict[str, int] = {}
            for configured_card in configured_shapes:
                card = canonical_by_name[str(configured_card).casefold()]
                assigned = min(remaining, current_by_card.get(card, 0))
                if assigned > 0:
                    pool_targets[card] = assigned
                    remaining -= assigned
                if remaining <= 0:
                    break
            targets_by_pool_card[pool_key] = pool_targets

        shelter: dict[str, dict[str, int]] = {
            pool_key: {} for pool_key in ordered_keys
        }
        for configured_card in configured_shapes:
            card = canonical_by_name[str(configured_card).casefold()]
            remaining_demand = demand_by_card.get(card, 0)
            for pool_key in ordered_keys:
                target = targets_by_pool_card[pool_key].get(card, 0)
                covered = min(target, remaining_demand)
                remaining_demand -= covered
                quota = target - covered
                if quota > 0:
                    shelter[pool_key][card] = quota
        return shelter, replica_cards

    def _apply_reserved_capacity_fill_v2(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
        states: dict[str, _PoolFillState],
    ) -> list[AutoscalerDecision]:
        """Apply independently fenced pool feeds under one service ceiling."""
        if not states:
            return decisions
        ordered_keys = list(states)
        generations = {state.service_generation for state in states.values()}
        if len(generations) != 1:
            # A complete poll publication may never mix budgets. Fail closed
            # if corrupted in-memory state reaches a decision tick.
            logger.error('Reserved-fill protocol-v2 state mixes service '
                         f'generations {generations}; feeding zero.')
            return decisions

        data: dict[str, dict[str, Any]] = {
            key: {
                'count': 0,
                'latest': 0,
                'occupying': 0,
                'demand': 0,
                'demand_latest': 0,
                'infos': [],
            } for key in ordered_keys
        }
        num_nonterminal = 0
        num_latest_nonterminal = 0
        for info in replica_infos:
            if info.is_terminal:
                continue
            units = self._fill_capacity_units(info)
            num_nonterminal += units
            is_latest = info.version == self.latest_version
            if is_latest:
                num_latest_nonterminal += units
            pool_key = self._fill_pool_key_for_replica(info, states)
            if pool_key is None:
                continue
            entry = data[pool_key]
            entry['infos'].append(info)
            entry['count'] += units
            if is_latest:
                entry['latest'] += units
            state = states[pool_key]
            created_at = getattr(info, 'created_at', None)
            if (not info.is_ready or
                (state.snapshot_time is not None and created_at is not None and
                 created_at > state.snapshot_time)):
                entry['occupying'] += units
            if not getattr(info, 'reserved_fill', False):
                entry['demand'] += units
                if is_latest:
                    entry['demand_latest'] += units

        spendable: dict[str, int] = {
            key: max(
                0,
                self._fresh_pool_fill_free_slots(states[key]) -
                int(data[key]['occupying'])) for key in ordered_keys
        }
        # Ordinary decisions are emitted before this overlay and may consume
        # a just-observed reserved slot. Debit each exact-card claim only from
        # pools that can serve that card. If several contexts expose the same
        # card, the ordinary decision does not yet carry its eventual context;
        # debit the claim from every compatible pool. This intentionally
        # withholds some fill while placement is ambiguous, but guarantees that
        # whichever context demand selects cannot receive both the demand launch
        # and fill for the same physical slot.
        (_, remaining_global_free_by_card, demand_reserved_claims_by_card) = (
            self._reserved_slots_claimed_by_demand(replica_infos, decisions))
        pool_cards: dict[str, frozenset[str]] = {}
        pool_shapes: dict[str, dict[str, tuple[str, int]] | None] = {}
        pool_exact_slots: dict[str, dict[str, int] | None] = {}
        for key in ordered_keys:
            try:
                identity = reserved_capacity_broker.parse_pool_identity(key)
            except (TypeError, ValueError):
                logger.error('Reserved-fill protocol-v2 state has a malformed '
                             f'pool key {key!r}; feeding it zero.')
                spendable[key] = 0
                pool_shapes[key] = None
                pool_exact_slots[key] = {}
                continue
            if identity.protocol_version != 2:
                logger.error('Reserved-fill protocol-v2 state has a non-v2 '
                             f'pool key {key!r}; feeding it zero.')
                spendable[key] = 0
                pool_shapes[key] = None
                pool_exact_slots[key] = {}
                continue
            pool_cards[key] = frozenset(identity.gpu_names)
            shapes = self._exact_launch_shapes_for_pool(states[key])
            pool_shapes[key] = shapes
            exact_slots = states[key].free_slots_by_accelerator
            if exact_slots is None:
                pool_exact_slots[key] = None
            elif (shapes is None or
                  any(card not in shapes for card in exact_slots)):
                # A present per-card feed is authoritative. If it cannot be
                # translated back to an exact task shape, never degrade it to
                # an aggregate launch.
                logger.error('Reserved-fill protocol-v2 exact-card feed does '
                             f'not match pool locations for {key!r}; '
                             'withholding its launches.')
                pool_exact_slots[key] = {}
                spendable[key] = 0
            else:
                pool_exact_slots[key] = dict(exact_slots)
        if demand_reserved_claims_by_card:
            for card, claimed_slots in demand_reserved_claims_by_card.items():
                canonical_card = str(card).casefold()
                for key in ordered_keys:
                    if canonical_card not in pool_cards.get(key, frozenset()):
                        continue
                    spendable[key] = max(0, spendable[key] - claimed_slots)
                    exact_slots = pool_exact_slots[key]
                    if exact_slots is not None:
                        exact_slots[canonical_card] = max(
                            0,
                            exact_slots.get(canonical_card, 0) - claimed_slots)

        targets: dict[str, int] = {}
        launch_targets: dict[str, int] = {}
        for key in ordered_keys:
            state = states[key]
            entry = data[key]
            ceiling = state.shelter_grant + int(entry['demand'])
            launch_ceiling = state.grant + int(entry['demand_latest'])
            targets[key] = min(
                int(entry['count']) + spendable[key], ceiling,
                self.max_replicas)
            launch_targets[key] = min(
                int(entry['latest']) + spendable[key], launch_ceiling,
                self.max_replicas)
            state.fill_target = targets[key]

        remaining_target_budget = self.max_replicas
        for key in ordered_keys:
            targets[key] = min(targets[key], remaining_target_budget)
            remaining_target_budget -= targets[key]

        total_target = sum(targets.values())
        self._fill_target = total_target
        with self._fill_pool_state_lock:
            for key, target in targets.items():
                live = self._fill_pool_states.get(key)
                if (live is not None and live.service_generation
                        == states[key].service_generation):
                    live.fill_target = target
        result = list(decisions)

        # Partition the exact v1 shelter equation over pools. When complete
        # exact-card demand telemetry exists, run the same coverage equation
        # independently per card before the stable pool pass.
        exact_shelter = self._exact_card_pool_shelter(data, targets,
                                                      ordered_keys)
        shelter_quota: dict[str, int] = {}
        if exact_shelter is None:
            remaining_demand = min(self.get_final_target_num_replicas(),
                                   sum(targets.values()))
            for key in ordered_keys:
                covered = min(targets[key], remaining_demand)
                remaining_demand -= covered
                shelter_quota[key] = max(0, targets[key] - covered)

        id_to_info = {info.replica_id: info for info in replica_infos}
        victims_by_pool: dict[str, list[int]] = {key: [] for key in ordered_keys}
        for index, decision in enumerate(decisions):
            if decision.operator != AutoscalerDecisionOperator.SCALE_DOWN:
                continue
            assert isinstance(decision.target, (int, LogicalScaleDownTarget))
            victim = id_to_info.get(_scale_down_replica_id(decision.target))
            if victim is None:
                continue
            pool_key = self._fill_pool_key_for_replica(victim, states)
            if pool_key is not None:
                victims_by_pool[pool_key].append(index)
        suppressed: set[int] = set()
        for key in ordered_keys:
            if exact_shelter is not None:
                quotas_by_card, replica_cards = exact_shelter
                remaining_by_card = dict(quotas_by_card[key])
                for index in reversed(victims_by_pool[key]):
                    victim_target = decisions[index].target
                    assert isinstance(victim_target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info[_scale_down_replica_id(victim_target)]
                    card = replica_cards[victim.replica_id]
                    if remaining_by_card.get(card, 0) <= 0:
                        continue
                    suppressed.add(index)
                    remaining_by_card[card] = max(
                        0, remaining_by_card[card] -
                        self._fill_capacity_units(victim))
            else:
                remaining = shelter_quota[key]
                for index in reversed(victims_by_pool[key]):
                    if remaining <= 0:
                        break
                    victim_target = decisions[index].target
                    assert isinstance(victim_target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info[_scale_down_replica_id(victim_target)]
                    suppressed.add(index)
                    remaining -= self._fill_capacity_units(victim)
        if suppressed:
            result = [
                decision for index, decision in enumerate(decisions)
                if index not in suppressed
            ]

        num_old_nonterminal = num_nonterminal - num_latest_nonterminal
        demand_target = self.get_final_target_num_replicas()
        planned_total = (num_old_nonterminal +
                         max(num_latest_nonterminal, demand_target))
        hard_headroom = max(0, self.max_replicas - planned_total)
        emitted_by_pool: dict[str, int] = {key: 0 for key in ordered_keys}
        emitted_by_pool_card: dict[str, dict[str, int]] = {
            key: {} for key in ordered_keys
        }
        # Additive round compatibility: a round written before the broker
        # persisted its exact-card split still carries valid aggregate
        # authority.  If this autoscaler independently has exact-card
        # telemetry, use one shared, debit-aware budget across every legacy
        # pool instead of multiplying it once per pool.  With no exact
        # telemetry at all, retain the old unshaped launch behavior.
        global_exact_slots: dict[str, int] | None = None
        if remaining_global_free_by_card is not None:
            global_exact_slots = {}
            for raw_card, raw_count in remaining_global_free_by_card.items():
                if (isinstance(raw_card, str) and raw_card and
                        not isinstance(raw_count, bool) and
                        isinstance(raw_count, int) and raw_count >= 0):
                    card = raw_card.casefold()
                    global_exact_slots[card] = (
                        global_exact_slots.get(card, 0) + raw_count)
        for key in ordered_keys:
            if hard_headroom <= 0:
                break
            entry = data[key]
            desired = max(0, launch_targets[key] - int(entry['latest']))
            count = min(desired, hard_headroom)
            if count <= 0:
                continue
            state = states[key]
            override: dict[str, Any] = {
                constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True,
                constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY:
                    state.protocol_version,
                constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY: key,
                constants.RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY:
                    state.service_generation,
                constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY:
                    state.physical_cluster_uid,
                constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY: [
                    location.to_pickleable()
                    for location in state.zero_cost_locations
                ],
            }
            if state.grant_epoch is not None:
                override[constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY] = (
                    state.grant_epoch)
            exact_slots = pool_exact_slots[key]
            if exact_slots is None and global_exact_slots is not None:
                exact_slots = global_exact_slots
            if exact_slots is None:
                # No exact-card measurement exists in either authority path.
                # This is the compatibility behavior for an old v2 round.
                result.extend(_generate_scale_up_decisions(count, override))
                emitted_by_pool[key] = count
                hard_headroom -= count
                continue

            shapes = pool_shapes[key]
            if shapes is None:
                # A present exact-card budget is authoritative.  If it cannot
                # be expressed as one of this pool's exact location shapes,
                # never silently fall back to an aggregate launch.
                continue
            remaining = count
            for card, (display_card, gpu_count) in shapes.items():
                if remaining <= 0 or hard_headroom <= 0:
                    break
                available = max(0, int(exact_slots.get(card, 0)))
                shaped_count = min(remaining, hard_headroom, available)
                if shaped_count <= 0:
                    continue
                shaped_override = dict(override)
                shaped_override['accelerators'] = {display_card: gpu_count}
                result.extend(
                    _generate_scale_up_decisions(shaped_count, shaped_override))
                exact_slots[card] = available - shaped_count
                emitted_by_pool_card[key][card] = (
                    emitted_by_pool_card[key].get(card, 0) + shaped_count)
                emitted_by_pool[key] += shaped_count
                remaining -= shaped_count
                hard_headroom -= shaped_count

        if any(emitted_by_pool.values()):
            with self._fill_pool_state_lock:
                for key, emitted in emitted_by_pool.items():
                    if emitted <= 0:
                        continue
                    live = self._fill_pool_states.get(key)
                    source = states[key]
                    if (live is None or live.service_generation
                            != source.service_generation):
                        continue
                    live.free_slots = max(0, live.free_slots - emitted)
                    if live.last_raw_free_slots is not None:
                        live.last_raw_free_slots = max(
                            0, live.last_raw_free_slots - emitted)
                    if live.free_slots_by_accelerator is not None:
                        for card, card_emitted in emitted_by_pool_card[
                                key].items():
                            live.free_slots_by_accelerator[card] = max(
                                0,
                                live.free_slots_by_accelerator.get(card, 0) -
                                card_emitted)
                self._refresh_legacy_fill_projection_locked()
        return result

    def _apply_reserved_capacity_fill(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Overlay zero-cost capacity fill onto the demand decisions.

        fill_target = (nonterminal replicas already on a zero-cost
        location) + (spendable free slots: fresh damped free slots minus
        launched-but-not-READY latest zero-cost replicas), clamped to
        max_replicas but deliberately NOT floored by min_replicas -- an
        empty free tier must not assert a floor. target_num_replicas and
        thus the controller's capacity hint stay DEMAND-ONLY: fill
        replicas are opportunistic supply, and the platform's spill
        logic must not read them as demand.

        - Every spendable free slot that fits below the hard aggregate
          max_replicas ceiling carries the sentinel override so the launch
          path pins it to zero-cost ACTIVE locations only (and skips entirely
          when none is). Demand and rolling-update launches reserve their
          planned ceiling headroom first, but do not otherwise suppress fill.
        - Scale-downs covered by the fill surplus are suppressed, taking
          the shelter quota from the TAIL of the zero-cost victims: the
          subclass ordered its victims most-preferred-first (initializing
          replicas before READY ones), so when the surplus only covers
          part of them the ones sheltered must be the LEAST preferred --
          a prefix keep would shelter a warming PROVISIONING replica
          while killing the READY one serving traffic. Output order (and
          the cost-aware / drain-aware selection itself) is untouched.
        - With a stale snapshot, fill_target degrades to exactly the
          zero-cost replica count: existing fill replicas are protected
          from victimization by staleness, but no new fill is launched.
        """
        if not self.reserved_capacity_fill:
            return decisions
        pool_states = self._pool_fill_states_snapshot()
        if pool_states:
            return self._apply_reserved_capacity_fill_v2(
                replica_infos, decisions, pool_states)
        # Zero-cost accounting is version-asymmetric by design; the
        # four roles use different version scopes:
        # - LAUNCH TARGET: latest-version zero-cost rows only. Old-version
        #   zero-cost replicas (a rolling update draining its previous fleet)
        #   would otherwise inflate the target and compound fill launches.
        #   The HARD CEILING below is deliberately all-version: old rows still
        #   occupy physical capacity and must reduce aggregate headroom.
        # - OCCUPANCY DEBIT: all versions. ANY nonterminal zero-cost row
        #   whose pod may be unbound (not READY, or created after the
        #   snapshot) holds a claim on a slot the snapshot counted free
        #   regardless of version -- an old-version PROVISIONING row
        #   left out of the debit would let a fill launch collide with
        #   its claim, fail on capacity, and bench the zero-cost tier.
        # - SUPPRESSION: all versions. Every existing zero-cost replica
        #   occupies free-tier capacity regardless of version and
        #   deserves shelter from DEMAND scale-downs; sheltering is
        #   bounded by the victims actually present (demand victims are
        #   latest-version, and the outdated-version drain bypasses this
        #   overlay entirely).
        # - CEILING: split by side, mirroring the asymmetry above. The
        #   LAUNCH-side ceiling (grant + demand-placed rows riding on top
        #   of it) counts latest-version demand-placed rows only: it caps
        #   fill_target_launch, which is latest-only, and an old-version
        #   demand row draining through a rolling update would otherwise
        #   inflate the ceiling and let fill overshoot the grant by its
        #   count (bench churn against peers). The TARGET/SHELTER-side
        #   ceiling keeps the all-version count, consistent with
        #   all-version suppression: every existing demand-placed row
        #   deserves its exemption regardless of version.
        zero_cost_count = 0
        zero_cost_latest = 0
        zero_cost_occupying = 0
        zero_cost_demand_placed = 0
        zero_cost_demand_placed_latest = 0
        num_nonterminal = 0
        num_latest_nonterminal = 0
        zero_cost_infos: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if info.is_terminal:
                continue
            capacity_units = self._fill_capacity_units(info)
            num_nonterminal += capacity_units
            is_latest = info.version == self.latest_version
            if is_latest:
                num_latest_nonterminal += capacity_units
            if self._replica_on_zero_cost_location(info):
                zero_cost_infos.append(info)
                zero_cost_count += capacity_units
                if is_latest:
                    zero_cost_latest += capacity_units
                if self._fill_row_occupies_free_slot(info):
                    zero_cost_occupying += capacity_units
                # reserved_fill is the persisted launch-origin flag: only
                # sentinel (fill) launches carry it. Demand-placed
                # zero-cost rows are demand-protected, not broker
                # property, so the grant ceiling below exempts them. Rows
                # pickled before the flag existed read as demand
                # (__setstate__ default False, same as the claim-heartbeat
                # split in count_zero_cost_holdings): they keep their
                # shelter but stay ceiling-exempt until natural churn
                # replaces them with flagged rows.
                if not getattr(info, 'reserved_fill', False):
                    zero_cost_demand_placed += capacity_units
                    if is_latest:
                        zero_cost_demand_placed_latest += capacity_units
        # Three defense layers keep fill launches within physical free
        # capacity:
        # 1. Emission-time spend (below): free-slot memory is deducted
        #    the moment launch decisions are emitted, covering the
        #    intra-poll window.
        # 2. Occupied-slot subtraction (here): zero-cost replicas of ANY
        #    version that are not READY (pods invisible to the poller --
        #    launch threads can queue for multiple poll intervals) or
        #    that were created after the snapshot (e.g. a demand launch
        #    landing on the zero-cost tier and turning READY within one
        #    inter-poll gap) occupy slots the snapshot counted free, so
        #    they are subtracted from the spendable free level (see
        #    _fill_row_occupies_free_slot). This may overlap with slots
        #    the poller already excluded once pods bind; subtracting is
        #    the conservative direction -- never over-launch, worst case
        #    under-fill for one poll.
        # 3. Poll re-sync: subsequent snapshots restore the true level
        #    (immediately on decrease, two-poll damped on increase).
        spendable_free_slots = max(
            0,
            self._fresh_fill_free_slots() - zero_cost_occupying)
        # Broker grant ceiling: the one new actuator arbitration needs.
        # The #108 fill target is structurally >= current holdings, so
        # lowering the FEED alone can never shrink a fleet; capping the
        # target at grant + demand-placed rows makes holdings above the
        # ceiling lose their scale-down shelter, and the normal graceful,
        # drain-aware scale-down returns the machines. None = no ceiling
        # (single-service identity). Demand-placed zero-cost rows ride on
        # top of the grant: they are demand-protected, and the broker
        # already excludes them from the fill capacity it arbitrates.
        # Version scope per side per the CEILING note above: launch-side
        # counts latest-version demand-placed rows only.
        fill_ceiling: int | None = None
        fill_ceiling_launch: int | None = None
        if self._fill_grant is not None:
            fill_ceiling = self._fill_grant + zero_cost_demand_placed
            fill_ceiling_launch = (self._fill_grant +
                                   zero_cost_demand_placed_latest)
        fill_target = min(zero_cost_count + spendable_free_slots,
                          self.max_replicas)
        if fill_ceiling is not None:
            fill_target = min(fill_target, fill_ceiling)
        self._fill_target = fill_target
        demand_target = self.get_final_target_num_replicas()
        surplus_covered = fill_target - demand_target
        # Keep this overlay side-effect free for callers that retain the
        # ordinary decision list for later policy checks.
        result = list(decisions)
        exact_shelter = self._exact_card_fill_shelter(zero_cost_infos,
                                                      fill_target)
        if surplus_covered > 0 or exact_shelter is not None:
            # Victim-aware suppression: shelter ONLY scale-downs whose victim
            # replica sits on a zero-cost location, up to the fill surplus.
            # Downs targeting paid replicas always pass through -- fill
            # surplus must never keep a PAID replica alive (the subclass
            # orders victims newest-first, so a victim-blind prefix keep
            # could shelter a paid replica indefinitely while repeatedly
            # killing and relaunching zero-cost ones).
            id_to_info = {info.replica_id: info for info in replica_infos}
            # Take the shelter quota from the TAIL of the zero-cost victims:
            # the subclass emits victims most-preferred-first, so a partial
            # surplus must shelter the LEAST-preferred ones (e.g. keep the
            # READY replica serving traffic, not the PROVISIONING one ahead
            # of it in the list). Two passes so output order is preserved.
            zero_cost_decisions = []
            for idx, decision in enumerate(decisions):
                if decision.operator == AutoscalerDecisionOperator.SCALE_DOWN:
                    assert isinstance(decision.target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info.get(
                        _scale_down_replica_id(decision.target))
                    if (victim is not None and
                            self._replica_on_zero_cost_location(victim)):
                        zero_cost_decisions.append((idx, victim))
            suppressed_ids: set[int] = set()
            if exact_shelter is not None:
                shelter_by_card, replica_cards = exact_shelter
                remaining_by_card = dict(shelter_by_card)
                for idx, victim in reversed(zero_cost_decisions):
                    card = replica_cards[victim.replica_id]
                    if remaining_by_card.get(card, 0) <= 0:
                        continue
                    suppressed_ids.add(idx)
                    remaining_by_card[card] = max(
                        0, remaining_by_card[card] -
                        self._fill_capacity_units(victim))
            else:
                suppressed_ids = {
                    idx for idx, _ in zero_cost_decisions[-surplus_covered:]
                }
            result = [
                decision for idx, decision in enumerate(decisions)
                if idx not in suppressed_ids
            ]
        # Launch target: latest-version zero-cost replicas only (see the
        # version-asymmetry note above). Fill intent is independent of demand;
        # the hard aggregate headroom calculation below separately reserves
        # latest demand and counts every old-version nonterminal row.
        fill_target_launch = min(zero_cost_latest + spendable_free_slots,
                                 self.max_replicas)
        if fill_ceiling_launch is not None:
            # Launch-side ceiling: a feed above the remaining grant
            # headroom (e.g. a stale feed raced by a peer's launch) must
            # not push the fleet past its entitlement. Latest-only
            # demand-placed exemption here (see the CEILING note above):
            # old-version demand rows must not inflate launches during a
            # rolling update.
            fill_target_launch = min(fill_target_launch, fill_ceiling_launch)
        desired_fill_up = max(0, fill_target_launch - zero_cost_latest)
        (demand_reserved_claims, remaining_free_by_card,
         _) = self._reserved_slots_claimed_by_demand(replica_infos, decisions)
        desired_fill_up = max(0, desired_fill_up - demand_reserved_claims)
        if remaining_free_by_card is not None:
            desired_fill_up = min(desired_fill_up,
                                  sum(remaining_free_by_card.values()))
        num_old_nonterminal = num_nonterminal - num_latest_nonterminal
        planned_total = (num_old_nonterminal +
                         max(num_latest_nonterminal, demand_target))
        hard_ceiling_headroom = max(0, self.max_replicas - planned_total)
        num_fill_up = min(desired_fill_up, hard_ceiling_headroom)
        if num_fill_up <= 0 and self._fill_grant:
            # A pool that is granted capacity but launches nothing is
            # indistinguishable from a pool with nothing to launch, because
            # the success line below is the only one emitted. That ambiguity
            # cost a full debugging session against a fleet holding one
            # replica while the broker fed it thirty. Name the term that is
            # actually zero.
            snapshot_age = (None if self._fill_snapshot_time is None else round(
                time.time() - self._fill_snapshot_time, 1))
            logger.info(
                f'Reserved-capacity fill: no launch. spendable free slots '
                f'{spendable_free_slots} (raw feed {self._fill_free_slots}, '
                f'fresh {self._fresh_fill_free_slots()}, snapshot age '
                f'{snapshot_age}s, zero-cost occupying '
                f'{zero_cost_occupying}), desired {desired_fill_up}, '
                f'launch target {fill_target_launch}, latest zero-cost '
                f'{zero_cost_latest}, grant {self._fill_grant}, ceiling '
                f'{fill_ceiling_launch}, demand target {demand_target}, '
                f'hard-ceiling headroom {hard_ceiling_headroom}.')
        if num_fill_up > 0:
            logger.info(f'Reserved-capacity fill: launch target '
                        f'{fill_target_launch} (latest zero-cost replicas '
                        f'{zero_cost_latest} + spendable free slots '
                        f'{spendable_free_slots}), demand target '
                        f'{demand_target}, planned total {planned_total}, '
                        f'demand-reserved claims {demand_reserved_claims}, '
                        f'hard-ceiling headroom {hard_ceiling_headroom}; '
                        f'scaling up {num_fill_up} '
                        'zero-cost-only replica(s).')
            fill_override: dict[str, Any] = {
                constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
            }
            if self._fill_grant_epoch is not None:
                # Epoch fencing: the launch path re-checks this against
                # the POOL's current round epoch right before committing
                # (epochs are per-pool, so the pool key rides along).
                # Attached only when a broker round supplied one, so the
                # pre-broker decision shape (and every existing test) is
                # unchanged.
                fill_override[
                    constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY] = (
                        self._fill_grant_epoch)
                if self._fill_grant_pool_key is not None:
                    fill_override[
                        constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY] = (
                            self._fill_grant_pool_key)
                    fill_override[
                        constants.
                        RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY] = (
                            self._fill_protocol_version)
                    fill_override[
                        constants.
                        RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY] = (
                            self._fill_service_generation)
                    if self._fill_physical_cluster_uid is not None:
                        fill_override[
                            constants.
                            RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY] = (
                                self._fill_physical_cluster_uid)
            if remaining_free_by_card is None:
                result.extend(
                    _generate_scale_up_decisions(num_fill_up, fill_override))
            else:
                configured_shapes = getattr(self,
                                            'configured_accelerator_shapes', {})
                remaining = num_fill_up
                for card, raw_gpu_count in configured_shapes.items():
                    if remaining <= 0:
                        break
                    if (not isinstance(raw_gpu_count, int) or
                            isinstance(raw_gpu_count, bool) or
                            raw_gpu_count <= 0):
                        continue
                    launches = min(remaining,
                                   remaining_free_by_card.get(card, 0))
                    exact_fill_override = {
                        **fill_override,
                        'accelerators': {
                            card: raw_gpu_count
                        },
                    }
                    result.extend(
                        _generate_scale_up_decisions(launches,
                                                     exact_fill_override))
                    remaining -= launches
                if remaining > 0:
                    # Exact free-slot telemetry was present, so never guess a
                    # card for the unaccounted remainder. A later poll can
                    # restore the conservatively withheld fill.
                    num_fill_up -= remaining
            # Invariant: a free slot is SPENT the moment a launch decision
            # is emitted, not when the poller next observes the pod. Fill
            # launches persist replica rows immediately, so
            # zero_cost_count already grows on the next tick while the
            # snapshot only refreshes on the poll interval -- without this
            # deduction the same static snapshot would be re-consumed
            # every tick, compounding the fill fleet. Deduct from BOTH the
            # damped value and the last raw poll value:
            # collect_reserved_capacity re-raises the damped value from
            # min(prev_raw, new) on the next poll, so an undeducted stale
            # prev_raw would re-grant the spent slots after a single poll,
            # defeating the two-poll damping. The next polls re-sync the
            # true level (immediate on decrease, damped on increase).
            # This read-modify-write races the poller thread's
            # collect_reserved_capacity (no lock, same as the other
            # cross-thread gauges here): worst case one poll's decrease
            # is overwritten for a single interval, and the resulting
            # over-launch fails fast on the benched location and is
            # re-synced by the next poll.
            self._fill_free_slots = max(0, self._fill_free_slots - num_fill_up)
            if self._fill_last_raw_free_slots is not None:
                self._fill_last_raw_free_slots = max(
                    0, self._fill_last_raw_free_slots - num_fill_up)
        return result

    def has_recomputed_with_fresh_data(self) -> bool:
        """Whether target_num_replicas reflects a fresh-data recompute.

        QPS/queue autoscalers recompute from always-available signals on
        every tick, so their target is never the rebuilt-blind minimum.
        The concurrency autoscaler overrides this: after a controller
        restart its target stays at min_replicas until the first
        decision tick that consumed a fresh demand report, and the
        capacity hint must keep flooring until then.
        """
        return True

    def info(self) -> dict[str, Any]:
        """Get information about the autoscaler."""
        info: dict[str, Any] = {
            'target_num_replicas': self.target_num_replicas,
            'min_replicas': self.min_replicas,
            'max_replicas': self.max_replicas,
            'min_replicas_by_accelerator': dict(
                getattr(self, 'min_replicas_by_accelerator', {})),
            'target_num_replicas_by_accelerator': dict(
                getattr(self, 'target_num_replicas_by_accelerator', {})),
            'demand_target_by_accelerator': dict(
                getattr(self, 'target_num_replicas_by_accelerator', {})),
            'warm_retention_target_by_accelerator': dict(
                getattr(self, 'warm_retention_target_by_accelerator', {})),
            'cold_launch_authority_by_accelerator': dict(
                getattr(self, 'cold_launch_authority_by_accelerator', {})),
        }
        request_timestamps = getattr(self, 'request_timestamps', None)
        request_window_seconds = getattr(self, 'qps_window_size', None)
        if (isinstance(request_timestamps, list) and
                isinstance(request_window_seconds, int) and
                request_window_seconds > 0):
            cutoff = time.time() - request_window_seconds
            recent_request_count = sum(
                timestamp >= cutoff for timestamp in request_timestamps)
            info.update({
                'recent_request_count': recent_request_count,
                'request_window_seconds': request_window_seconds,
                'requests_per_second': recent_request_count /
                                       request_window_seconds,
            })
        if self.reserved_capacity_fill:
            # target_num_replicas above stays demand-only; the fill
            # overlay is observable through these keys instead.
            snapshot_age = (time.time() - self._fill_snapshot_time
                            if self._fill_snapshot_time is not None else None)
            info.update({
                'fill_free_slots': self._fill_free_slots,
                'fill_snapshot_age': snapshot_age,
                'fill_target': self._fill_target,
            })
            pool_states = self._pool_fill_states_snapshot()
            if pool_states:
                now = time.time()
                info['fill_by_pool'] = {
                    pool_key: {
                        'free_slots': state.free_slots,
                        'snapshot_age':
                            (None if state.snapshot_time is None else now -
                             state.snapshot_time),
                        'fill_target': state.fill_target,
                        'edge_cap': state.edge_cap,
                        'grant': state.grant,
                        'shelter_grant': state.shelter_grant,
                        'service_generation': state.service_generation,
                        'physical_cluster_uid': state.physical_cluster_uid,
                    } for pool_key, state in pool_states.items()
                }
        return info

    def get_ready_replica_capacity(self,
                                   info: 'replica_managers.ReplicaInfo') -> int:
        """Return the public replica units currently ready on one backend."""
        return 1 if info.is_ready else 0

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on replica information."""
        raise NotImplementedError

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler."""
        raise NotImplementedError

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler."""
        raise NotImplementedError

    # --------------- Utility Functions ---------------

    def _clip_target_num_replicas(self, target_num_replicas: int) -> int:
        """Clip target number of replicas with current minimal and maximum
        number of replicas.
        """
        return max(self.min_replicas, min(self.max_replicas,
                                          target_num_replicas))

    @classmethod
    def from_spec(cls,
                  service_name: str,
                  spec: 'service_spec.SkyServiceSpec',
                  version: int = constants.INITIAL_VERSION) -> 'Autoscaler':
        # TODO(MaoZiming): use NAME to get the class.
        if spec.pool:
            return QueueLengthAutoscaler(service_name, spec, version)
        # getattr: keep from_spec robust against spec objects predating the
        # concurrency knob (e.g. specs unpickled from old DB rows).
        elif getattr(spec, 'target_concurrency_per_replica', None) is not None:
            # Checked before the qps branches: the knob is mutually
            # exclusive with target_qps_per_replica (validated at spec
            # load), so a set knob unambiguously selects concurrency-based
            # autoscaling.
            return ConcurrencyAutoscaler(service_name, spec, version)
        elif spec.use_ondemand_fallback:
            return FallbackRequestRateAutoscaler(service_name, spec, version)
        elif isinstance(spec.target_qps_per_replica, dict):
            # Use instance-aware autoscaler
            # when target_qps_per_replica is a dict
            return InstanceAwareRequestRateAutoscaler(service_name, spec,
                                                      version)
        else:
            return RequestRateAutoscaler(service_name, spec, version)

    def get_decision_interval(self) -> int:
        """Get the decision interval for the autoscaler.

        We reduce the decision interval when the desired number of replicas is
        0, to make the service scale faster when the service is not running.
        This will happen when min_replicas = 0 and no traffic.
        """
        if self.get_final_target_num_replicas() == 0:
            return constants.AUTOSCALER_NO_REPLICA_DECISION_INTERVAL_SECONDS
        else:
            return constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Select outdated replicas to scale down."""

        if self.update_mode == serve_utils.UpdateMode.ROLLING:
            latest_ready_replicas: list[replica_managers.ReplicaInfo] = []
            old_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []
            for info in replica_infos:
                if info.version == self.latest_version:
                    if info.is_ready:
                        latest_ready_replicas.append(info)
                elif not info.is_terminal:
                    old_nonterminal_replicas.append(info)

            num_latest_ready_replicas = len(latest_ready_replicas)

            # We compare to target_num_replicas instead of min_replicas, to
            # guarantee better service quality. Since mixing traffic across
            # old and latest versions are allowed in rolling update, this will
            # not affect the time it takes for the service to updated to the
            # latest version.
            if (num_latest_ready_replicas
                    >= self.get_final_target_num_replicas()):
                # Once the number of ready new replicas is greater than or equal
                # to the target, we can scale down all old replicas.
                return [info.replica_id for info in old_nonterminal_replicas]
            # If rolling update is in progress, we scale down old replicas
            # based on the number of ready new replicas.
            num_old_replicas_to_keep = (self.get_final_target_num_replicas() -
                                        num_latest_ready_replicas)
            # Remove old replicas (especially old launching replicas) and only
            # keep the required number of replicas, as we want to let the new
            # replicas to take over the provisioning old replicas faster.
            # `_select_replicas_to_scale_down` will make sure we scale the
            # replicas in initializing statuses first before scaling down the
            # READY old replicas.
            return _select_nonterminal_replicas_to_scale_down(
                max(0,
                    len(old_nonterminal_replicas) - num_old_replicas_to_keep),
                old_nonterminal_replicas,
            )

        if not active_versions:
            # active_versions can be empty when none of the replicas are ready
            # when the load balancer sync with the controller.
            return []
        # The active_versions should supposedly only having one version, but
        # we use min() here to make sure this works when rolling update and
        # blue-green update are mixed. min is used as we will scale down all old
        # replicas with version smaller than `latest_version_with_min_replicas`.
        latest_version_with_min_replicas = min(active_versions)
        # When it is blue green update, we scale down old replicas when the
        # number of ready new replicas is greater than or equal to the min
        # replicas instead of the target, to ensure the service being updated
        # to the latest version faster.
        return [
            info.replica_id
            for info in replica_infos
            if info.version < latest_version_with_min_replicas
        ]

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        """Serving-capacity units represented by an existing replica."""
        del info
        return 1.0

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        """Serving-capacity units represented by a candidate location."""
        del location
        return 1.0

    def _cost_rebalance_location_is_compatible(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        location: spot_placer.Location,
    ) -> bool:
        """Whether an economic replacement preserves autoscaler policy."""
        del incumbent, location
        return True

    def _get_hourly_cost_from_replica_info(
            self, replica_info: 'replica_managers.ReplicaInfo') -> float:
        """Resolve whole-replica hourly cost conservatively."""
        cached = self._cost_rebalance_replica_cost_cache.get(
            replica_info.replica_id)
        if cached is not None:
            return cached
        cost = 0.0
        resolved = False
        try:
            handle = replica_info.handle()
            if handle is not None:
                cost = float(handle.launched_resources.get_cost(seconds=3600))
                resolved = True
        except Exception:  # pylint: disable=broad-except
            cost = 0.0
        if (resolved and replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._cost_rebalance_replica_cost_cache[
                replica_info.replica_id] = cost
        return cost

    def _cost_rebalance_pairs(
        self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, 'replica_managers.ReplicaInfo']:
        """Return one live replacement row per live incumbent."""
        by_id = {info.replica_id: info for info in replica_infos}
        pairs: dict[int, replica_managers.ReplicaInfo] = {}
        for replacement in replica_infos:
            victim_id = getattr(replacement, 'cost_rebalance_for_replica_id',
                                None)
            if victim_id is None or replacement.is_terminal:
                continue
            victim = by_id.get(victim_id)
            if victim is None or victim.is_terminal:
                continue
            prior = pairs.get(victim_id)
            if prior is None or replacement.replica_id < prior.replica_id:
                pairs[victim_id] = replacement
        return pairs

    def _protect_cost_rebalance_overlap(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Keep ordinary autoscaling from consuming replacement overlap."""
        pairs = self._cost_rebalance_pairs(replica_infos)
        if not pairs:
            return decisions
        protected_ids = set(pairs)
        protected_ids.update(
            replacement.replica_id for replacement in pairs.values())
        overlap_to_ignore = len(pairs)
        kept: list[AutoscalerDecision] = []
        for decision in decisions:
            if decision.operator != AutoscalerDecisionOperator.SCALE_DOWN:
                kept.append(decision)
                continue
            assert isinstance(decision.target, (int, LogicalScaleDownTarget))
            replica_id = _scale_down_replica_id(decision.target)
            if replica_id in protected_ids:
                logger.info('Suppressing ordinary scale-down of cost-rebalance '
                            f'pair member {replica_id}.')
                if overlap_to_ignore > 0:
                    overlap_to_ignore -= 1
                continue
            if overlap_to_ignore > 0:
                logger.info('Suppressing one ordinary scale-down for temporary '
                            'cost-rebalance replacement headroom.')
                overlap_to_ignore -= 1
                continue
            kept.append(decision)
        return kept

    @staticmethod
    def _location_gpu_shape(location: spot_placer.Location) -> tuple[str, int]:
        accelerators = location.accelerators or {}
        if not accelerators:
            return 'unknown', 1
        gpu_type, gpu_count = next(iter(accelerators.items()))
        return gpu_type, max(1, int(gpu_count))

    def _best_cost_rebalance_candidate(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        active_locations: list[spot_placer.Location],
        location_load: dict[spot_placer.Location, int],
    ) -> spot_placer.Location | None:
        placer = self._cost_rebalance_spot_placer
        if placer is None:
            return None
        if (self.reserved_capacity_fill and
            (getattr(incumbent, 'reserved_fill', False) or
             incumbent.is_zero_cost is True)):
            # The reserved-fill controller exclusively owns convergence to
            # free capacity. Generic rebalance handles paid-to-paid movement.
            return None
        incumbent_location = incumbent.get_spot_location()
        if incumbent_location is None:
            return None
        incumbent_capacity = self._cost_rebalance_replica_capacity(incumbent)
        incumbent_cost = self._get_hourly_cost_from_replica_info(incumbent)
        if incumbent_capacity <= 0 or incumbent_cost <= 0:
            # Unknown cost is deliberately conservative in the existing cost
            # resolver.  Never replace an unknown/zero-cost incumbent.
            return None
        incumbent_unit_cost = incumbent_cost / incumbent_capacity
        maximum_unit_cost = incumbent_unit_cost * (
            1.0 - self.cost_rebalance_min_savings_fraction)

        eligible: list[tuple[float, int, str, spot_placer.Location]] = []
        for location in active_locations:
            if spot_placer.locations_match_placement(incumbent_location,
                                                     location):
                continue
            if not self._cost_rebalance_location_is_compatible(
                    incumbent, location):
                continue
            candidate_capacity = self._cost_rebalance_location_capacity(
                location)
            if candidate_capacity + 1e-9 < incumbent_capacity:
                continue
            # This method can run while the concurrency autoscaler holds its
            # logical-state lock. cost_per_hour() is a pure lookup over the
            # complete centralized catalog and cannot resolve providers.
            candidate_cost = placer.cost_per_hour(location)
            if not math.isfinite(candidate_cost) or candidate_cost < 0:
                continue
            if self.reserved_capacity_fill and candidate_cost == 0:
                continue
            candidate_unit_cost = candidate_cost / candidate_capacity
            if candidate_unit_cost > maximum_unit_cost + 1e-12:
                continue
            eligible.append((candidate_unit_cost, location_load[location],
                             repr(location.to_pickleable()), location))
        if not eligible:
            return None
        return min(eligible, key=lambda item: item[:3])[-1]

    def _generate_cost_rebalance_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        ordinary_decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Progress durable pairs and, when stable, start cheaper replacements."""
        live_replica_ids = {
            info.replica_id for info in replica_infos if not info.is_terminal
        }
        for replica_id in list(self._cost_rebalance_replica_cost_cache):
            if replica_id not in live_replica_ids:
                del self._cost_rebalance_replica_cost_cache[replica_id]
        pairs = self._cost_rebalance_pairs(replica_infos)
        by_id = {info.replica_id: info for info in replica_infos}
        decisions: list[AutoscalerDecision] = []

        # Existing pairs are completed even when a later update disables the
        # policy or changes the placement contract. In either case keep the
        # incumbent and drain the replacement; otherwise wait for replacement
        # readiness, then strictly drain the incumbent. COST_REBALANCE means
        # off-route now, terminate only after the LB proves zero occupancy.
        for victim_id, replacement in sorted(pairs.items()):
            victim = by_id[victim_id]
            replacement_location = replacement.get_spot_location()
            replacement_preserves_policy = (
                replacement_location is not None and
                self._cost_rebalance_location_is_compatible(
                    victim, replacement_location))
            if self.cost_rebalance and replacement_preserves_policy:
                if (replacement.is_ready and
                        not _replica_is_retiring_card_supply(replacement) and
                        getattr(victim.status_property, 'sky_down_status',
                                None) is None):
                    decisions.extend(
                        _generate_scale_down_decisions(
                            [victim.replica_id],
                            reason=AutoscalerDecisionReason.COST_REBALANCE))
            elif (replacement.is_ready and
                  getattr(replacement.status_property, 'sky_down_status',
                          None) is None):
                decisions.extend(
                    _generate_scale_down_decisions(
                        [replacement.replica_id],
                        reason=AutoscalerDecisionReason.COST_REBALANCE))
            elif getattr(replacement.status_property, 'sky_down_status',
                         None) is None:
                decisions.extend(
                    _generate_scale_down_decisions([replacement.replica_id]))

        if (not self.cost_rebalance or
                self._cost_rebalance_spot_placer is None):
            self._clear_cost_rebalance_candidates()
            return decisions
        if ordinary_decisions:
            self._clear_cost_rebalance_candidates()
            return decisions
        if any(not info.is_terminal and info.version != self.latest_version
               for info in replica_infos):
            self._clear_cost_rebalance_candidates()
            return decisions

        slots = self.cost_rebalance_max_parallel_replacements - len(pairs)
        paired_ids = set(pairs)
        candidates = [
            info for info in replica_infos
            if (info.version == self.latest_version and info.is_ready and
                not _replica_is_retiring_card_supply(info) and
                info.replica_id not in paired_ids)
        ]
        candidates.sort(
            key=lambda info: -self._get_hourly_cost_from_replica_info(info))
        planned_locations = [
            location for location in (info.get_spot_location()
                                      for info in replica_infos
                                      if not info.is_terminal)
            if location is not None
        ]
        active_locations = self._cost_rebalance_spot_placer.active_locations()
        # This load is shared by every incumbent evaluated in the tick.  On a
        # large fleet, rebuilding it inside `_best_cost_rebalance_candidate`
        # turns one placement scan into a redundant scan per replica.
        location_load = {
            location: sum(
                spot_placer.locations_match_placement(current, location)
                for current in planned_locations
            ) for location in active_locations
        }
        now = time.time()
        current_candidate_keys: set[tuple[int, spot_placer.Location]] = set()
        for incumbent in candidates:
            location = self._best_cost_rebalance_candidate(
                incumbent, active_locations, location_load)
            if location is None:
                continue
            key = (incumbent.replica_id, location)
            current_candidate_keys.add(key)
            first_seen = self._cost_rebalance_candidate_since.get(key)
            if first_seen is None:
                self._cost_rebalance_candidate_since[key] = now
                self._cost_rebalance_state_dirty = True
                first_seen = now
            if (now - first_seen < self.cost_rebalance_stabilization_seconds):
                continue
            if slots <= 0:
                # Keep validating continuous eligibility while another pair
                # occupies the replacement slot, but do not launch overlap.
                continue
            override = location.to_dict()
            override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = (
                incumbent.replica_id)
            decisions.append(
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_UP,
                    override,
                    reason=AutoscalerDecisionReason.COST_REBALANCE))
            planned_locations.append(location)
            for active_location in active_locations:
                if spot_placer.locations_match_placement(
                        location, active_location):
                    location_load[active_location] += 1
            slots -= 1

        for key in list(self._cost_rebalance_candidate_since):
            if key not in current_candidate_keys:
                del self._cost_rebalance_candidate_since[key]
                self._cost_rebalance_state_dirty = True
        return decisions

    def _notify_rollout_blocked(self, previous_version: int) -> None:
        operator_notifications.record_notification(
            operator_notifications.OperatorNotificationCategory.
            SERVE_ROLLOUT_BLOCKED,
            f'SkyServe rollout blocked for service {self._service_name!r}: '
            f'version {self.latest_version} failed before any replica became '
            f'ready. Version {previous_version} remains active. Inspect the '
            'new replica provisioning and setup logs.',
            dedupe_window_seconds=operator_notifications.
            SERVE_ROLLOUT_BLOCKED_DEDUPE_WINDOW_SECONDS)

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on replica information.
        If the number of launched replicas is less than the target, trigger a
        scale up. Else, trigger a scale down. This function also handles the
        version control of the replicas.

        For future compatibility, we return a list of AutoscalerDecision.
        Scale-up could include both spot and on-demand, each with a resource
        override dict. Active migration could require returning both SCALE_UP
        and SCALE_DOWN.
        """

        # Handle latest version unrecoverable failure first.
        self._unrecoverable_rollout_failure = None
        latest_replicas: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if info.version == self.latest_version:
                latest_replicas.append(info)
                if info.is_ready:
                    self.latest_version_ever_ready = self.latest_version
        previous_versions = [
            version for version in active_versions
            if version < self.latest_version
        ]
        if self.latest_version_ever_ready < self.latest_version:
            unrecoverable = [
                info for info in latest_replicas
                if info.status_property.unrecoverable_failure()
            ]
            if unrecoverable:
                if previous_versions:
                    self._notify_rollout_blocked(max(previous_versions))
                    evidence = ', '.join(
                        f'{info.replica_id}:{info.status.value}' for info in
                        sorted(unrecoverable,
                               key=lambda replica: replica.replica_id)[:20])
                    if len(unrecoverable) > 20:
                        evidence += f', and {len(unrecoverable) - 20} more'
                    self._unrecoverable_rollout_failure = (
                        UnrecoverableRolloutFailure(
                            version=self.latest_version,
                            reason=(
                                f'Version {self.latest_version} never became '
                                'ready and has unrecoverable replica evidence: '
                                f'{evidence}.')))
                # Stop scaling if one replica of the latest version has a
                # typed never-ready failure. With a previous active version,
                # the controller consumes the signal above by quarantining
                # this exact candidate and respawning onto the proven runtime.
                # Without a fallback, preserve the historical fail-closed
                # behavior rather than retrying a broken initial version.
                return []
            if (previous_versions and latest_replicas and
                    all(info.is_terminal for info in latest_replicas) and
                    any(info.status in
                        serve_state.ReplicaStatus.failed_statuses() and
                        not info.status_property.is_scale_down
                        for info in latest_replicas)):
                self._notify_rollout_blocked(max(previous_versions))

        scaling_decisions = []

        # If rolling update is in progress, we scale down old replicas based on
        # the number of ready new replicas and the traffic is directed to both
        # old and new replicas. Or, for blue_green update, once there is
        # min_replicas number of ready new replicas, we will direct all traffic
        # to them, we can scale down all old replicas.
        # TODO(MaoZiming,zhwu): corner case: We should make sure the fallback
        # replicas are ready before scaling down the old replicas to avoid the
        # situation that all the ready new replicas are preempted together.
        scaling_decisions.extend(
            _generate_scale_down_decisions(
                self._select_outdated_replicas_to_scale_down(
                    replica_infos, active_versions)))

        # If the latest version is ever ready, we can proceed to generate
        # decisions from the implementations in subclasses. The
        # reserved-capacity fill overlay wraps only the subclass's demand
        # decisions -- the outdated-replica drain above is version
        # control, not demand, and must never be suppressed by fill.
        ordinary_decisions = self._apply_reserved_capacity_fill(
            replica_infos, self._generate_scaling_decisions(replica_infos))
        ordinary_decisions = self._protect_cost_rebalance_overlap(
            replica_infos, ordinary_decisions)
        scaling_decisions.extend(ordinary_decisions)
        scaling_decisions.extend(
            self._generate_cost_rebalance_decisions(replica_infos,
                                                    ordinary_decisions))

        if not scaling_decisions:
            logger.info('No scaling needed.')

        return scaling_decisions

    def dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler."""
        states: dict[str, Any] = {
            'latest_version_ever_ready': self.latest_version_ever_ready
        }
        # Reserved-capacity fill snapshot: carried across the in-process
        # autoscaler swap in update_service. Without it a fresh autoscaler
        # instance has no zero-cost location set until the next poll, so
        # one decision tick with suppression off could terminate the whole
        # fill fleet. Nested under a single key (in pickleable form) so
        # subclass _load_dynamic_states leftover-logging never sees it.
        with self._fill_pool_state_lock:
            fill_state_version = 2 if self._fill_pool_states else 1
            # Capture the version discriminator and complete v2 pool map from
            # one critical section. A poller map swap between two independent
            # reads must not produce a v1 discriminator with v2 contents (or
            # vice versa).
            dumped_pools = {
                key: {
                    'protocol_version': pool.protocol_version,
                    'physical_cluster_uid': pool.physical_cluster_uid,
                    'service_generation': pool.service_generation,
                    'edge_cap': pool.edge_cap,
                    # This is non-launching restart shelter only. Feed is never
                    # restored and the epoch remains DB-authoritative.
                    'shelter_grant': max(0,
                                         min(pool.edge_cap,
                                             pool.shelter_grant)),
                    'zero_cost_location_keys': [
                        location.to_pickleable()
                        for location in pool.zero_cost_locations
                    ],
                    'snapshot_time': pool.snapshot_time,
                } for key, pool in self._fill_pool_states.items()
            }
        states['reserved_capacity_fill_state'] = {
            'version': fill_state_version,
            # A brokered feed is round authority, not durable autoscaler
            # state. Preserve standalone pre-broker behavior, but make every
            # brokered v1 swap fail closed until its poller republishes.
            'broker_authority': (self._fill_grant_pool_key is not None or
                                 self._fill_grant_epoch is not None),
            'fill_free_slots': self._fill_free_slots,
            'fill_last_raw_free_slots': self._fill_last_raw_free_slots,
            'fill_zero_cost_location_keys': [
                location.to_pickleable()
                for location in self._fill_zero_cost_locations
            ],
            'fill_snapshot_time': self._fill_snapshot_time,
            # Grants/epochs remain DB-authoritative and are deliberately not
            # restored. Locations and identity are enough to protect existing
            # replicas during an in-process autoscaler swap; feed resumes only
            # after the poller publishes an exact-generation round.
            'pools': dumped_pools,
        }
        states.update(self._dump_dynamic_states())
        return states

    def load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler."""
        self.latest_version_ever_ready = dynamic_states.pop(
            'latest_version_ever_ready', constants.INITIAL_VERSION)
        # Absent in dumps from builds predating the fill feature: keep
        # the constructor defaults (empty snapshot).
        fill_state = dynamic_states.pop('reserved_capacity_fill_state', None)
        if fill_state is not None:
            broker_authority = bool(fill_state.get('broker_authority', True))
            self._fill_free_slots = (0 if broker_authority else max(
                0, int(fill_state.get('fill_free_slots', 0))))
            self._fill_last_raw_free_slots = (
                None if broker_authority else
                fill_state.get('fill_last_raw_free_slots'))
            self._fill_zero_cost_locations = [
                location for location in
                (spot_placer.Location.from_pickleable(key)
                 for key in fill_state.get('fill_zero_cost_location_keys', []))
                if location is not None
            ]
            self._fill_snapshot_time = fill_state.get('fill_snapshot_time')
            if fill_state.get('version') == 2:
                restored: dict[str, _PoolFillState] = {}
                for pool_key, raw_pool in fill_state.get('pools', {}).items():
                    try:
                        locations = [
                            location for location in (
                                spot_placer.Location.from_pickleable(key)
                                for key in raw_pool.get(
                                    'zero_cost_location_keys', []))
                            if location is not None
                        ]
                        restored_edge_cap = max(0, int(raw_pool['edge_cap']))
                        raw_shelter_grant = raw_pool.get('shelter_grant')
                        if (isinstance(raw_shelter_grant, bool) or
                                not isinstance(raw_shelter_grant, int) or
                                raw_shelter_grant < 0):
                            # Protocol v2 did not exist before this dump field.
                            # Missing/malformed authority is corruption, not a
                            # compatibility shape: retain location identity but
                            # fail closed to zero shelter.
                            restored_shelter_grant = 0
                        else:
                            restored_shelter_grant = min(
                                restored_edge_cap, raw_shelter_grant)
                        restored[str(pool_key)] = _PoolFillState(
                            protocol_version=int(raw_pool['protocol_version']),
                            pool_key=str(pool_key),
                            physical_cluster_uid=str(
                                raw_pool['physical_cluster_uid']),
                            service_generation=int(
                                raw_pool['service_generation']),
                            edge_cap=restored_edge_cap,
                            # Feed and epoch fail closed across the swap. The
                            # prior real grant remains a shelter-only ceiling;
                            # with zero feed it authorizes no launch.
                            free_slots=0,
                            last_raw_free_slots=None,
                            zero_cost_locations=locations,
                            snapshot_time=raw_pool.get('snapshot_time'),
                            shelter_grant=restored_shelter_grant,
                            grant=0,
                            grant_epoch=None)
                    except (KeyError, TypeError, ValueError):
                        continue
                with self._fill_pool_state_lock:
                    self._fill_pool_states = restored
                    self._refresh_legacy_fill_projection_locked()
        self._load_dynamic_states(dynamic_states)


class _AutoscalerWithHysteresis(Autoscaler):
    """_AutoscalerWithHysteresis: Autoscale with hysteresis.

    This is an internal class for developing autoscalers with hysteresis. It
    only scales when the number of replicas is above or below the target number
    of replicas for a certain number of consecutive periods.
    """

    def _setup_thresholds(self, spec: 'service_spec.SkyServiceSpec') -> None:
        upscale_delay_seconds = (
            spec.upscale_delay_seconds if spec.upscale_delay_seconds is not None
            else constants.AUTOSCALER_DEFAULT_UPSCALE_DELAY_SECONDS)
        self.scale_up_threshold: int = int(
            upscale_delay_seconds /
            constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS)
        downscale_delay_seconds = (
            spec.downscale_delay_seconds
            if spec.downscale_delay_seconds is not None else
            constants.AUTOSCALER_DEFAULT_DOWNSCALE_DELAY_SECONDS)
        self.downscale_delay_seconds: float = float(downscale_delay_seconds)
        self.scale_down_threshold: int = int(
            self.downscale_delay_seconds /
            constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS)

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the hysteresis autoscaler.

        Variables:
            upscale_counter: Counter for upscale decisions of replicas.
            downscale_counter: Counter for downscale decisions of replicas.
            scale_up_threshold: The threshold to trigger a scale up.
            scale_down_threshold: The threshold to trigger a scale down.
        """
        super().__init__(service_name, spec, version)
        self.upscale_counter: int = 0
        self.downscale_counter: int = 0
        self._setup_thresholds(spec)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions but returns normally;
            # without this guard we would still reset the hysteresis
            # counters and thresholds from the stale spec below.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        # We directly set the target_num_replicas here instead of calling
        # `_set_target_num_replicas_with_hysteresis` to have the replicas
        # quickly scale after each update.
        self.target_num_replicas = self._calculate_target_num_replicas()
        logger.debug(f'Target number of replicas: {self.target_num_replicas}'
                     'after update_version.')
        # Cleanup hysteresis counters.
        self.upscale_counter = 0
        self.downscale_counter = 0
        self._setup_thresholds(spec)

    def _set_target_num_replicas_with_hysteresis(self) -> None:
        """Set target_num_replicas based on request rate with hysteresis."""
        target_num_replicas = self._calculate_target_num_replicas()
        old_target_num_replicas = self.target_num_replicas

        # Faster scale up when there is no replica.
        if self.target_num_replicas == 0:
            self.target_num_replicas = target_num_replicas
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self.target_num_replicas = target_num_replicas
        elif target_num_replicas < self.target_num_replicas:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                self.target_num_replicas = target_num_replicas
        else:
            self.upscale_counter = self.downscale_counter = 0

        logger.info(
            f'Old target number of replicas: {old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Num overprovision: {self.num_overprovision}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'Downscale counter: {self.downscale_counter}/'
            f'{self.scale_down_threshold}. ')


class RequestRateAutoscaler(_AutoscalerWithHysteresis):
    """RequestRateAutoscaler: Autoscale according to request rate.

    Scales when the number of requests per replica in the given interval
    is above or below the target qps per replica. The instance can be
    either spot or on-demand, but not both.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the request rate autoscaler.

        Variables:
            target_qps_per_replica: Target qps per replica for autoscaling.
            qps_window_size: Window size for qps calculating.
            request_timestamps: All request timestamps within the window.
        """
        super().__init__(service_name, spec, version)
        self.target_qps_per_replica: float | dict[
            str, float] | None = spec.target_qps_per_replica
        self.qps_window_size: int = constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS
        self.request_timestamps: list[float] = []

    def _calculate_target_num_replicas(self) -> int:
        if self.target_qps_per_replica is None:
            return self.min_replicas

        # RequestRateAutoscaler should only handle float values
        if isinstance(self.target_qps_per_replica, dict):
            raise ValueError('RequestRateAutoscaler does not support dict '
                             'target_qps_per_replica. Should use '
                             'InstanceAwareRequestRateAutoscaler instead.')

        num_requests_per_second = len(
            self.request_timestamps) / self.qps_window_size
        target_num_replicas = \
            math.ceil(num_requests_per_second / self.target_qps_per_replica)
        logger.info(f'Requests per second: {num_requests_per_second}. '
                    f'Target number of replicas: {target_num_replicas}.')

        return self._clip_target_num_replicas(target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't overwrite the
            # live qps target from a stale spec either.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        self.target_qps_per_replica = spec.target_qps_per_replica

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling.

        request_aggregator_info should be a dict with the following format:

        {
            'timestamps': [timestamp1 (float), timestamp2 (float), ...]
        }
        """
        self.request_timestamps.extend(
            request_aggregator_info.get('timestamps', []))
        current_time = time.time()
        index = bisect.bisect_left(self.request_timestamps,
                                   current_time - self.qps_window_size)
        self.request_timestamps = self.request_timestamps[index:]
        logger.info(f'Num of requests in the last {self.qps_window_size} '
                    f'seconds: {len(self.request_timestamps)}')

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on request rate."""

        # Use standard hysteresis-based logic (non-instance-aware)
        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if info.version == self.latest_version:
                if not info.is_terminal:
                    latest_nonterminal_replicas.append(info)

        scaling_decisions: list[AutoscalerDecision] = []

        # Case 1. when latest_nonterminal_replicas is less
        # than num_to_provision, we always scale up new replicas.
        target_num_replicas = self.get_final_target_num_replicas()
        if len(latest_nonterminal_replicas) < target_num_replicas:
            num_replicas_to_scale_up = (target_num_replicas -
                                        len(latest_nonterminal_replicas))
            logger.info('Number of replicas to scale up: '
                        f'{num_replicas_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_replicas_to_scale_up, None))

        # Case 2: when latest_nonterminal_replicas is more
        # than target_num_replicas, we scale down new replicas.
        replicas_to_scale_down = []
        if len(latest_nonterminal_replicas) > target_num_replicas:
            num_replicas_to_scale_down = (len(latest_nonterminal_replicas) -
                                          target_num_replicas)
            # Use standard downscaling logic
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_replicas_to_scale_down, latest_nonterminal_replicas))
            logger.info(
                'Number of replicas to scale down: '
                f'{num_replicas_to_scale_down} {replicas_to_scale_down}')

        scaling_decisions.extend(
            _generate_scale_down_decisions(replicas_to_scale_down))

        return scaling_decisions

    def _dump_dynamic_states(self) -> dict[str, Any]:
        return {
            'request_timestamps': self.request_timestamps,
        }

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        if 'request_timestamps' in dynamic_states:
            self.request_timestamps = dynamic_states.pop('request_timestamps')
        if dynamic_states:
            logger.info(f'Remaining dynamic states: {dynamic_states}')


# Distinguishes "caller did not resolve a handle" from a resolved None
# (cluster row or handle genuinely absent) in the mixin helpers below.
_UNRESOLVED_HANDLE = object()


class _GpuShapeResolverMixin:
    """Shared GPU-shape resolution with a post-launch-only memo.

    Used by the shape-aware autoscalers (instance-aware QPS and
    concurrency): both need a replica's (gpu_type, gpu_count) to size its
    capacity, and both must avoid repeating the blocking handle() DB read
    + unpickle for the same replica across the 2-3 passes per decision
    tick. Subclasses must initialize `_gpu_shape_cache` in __init__ and
    prune it to the live replica set each tick via
    `_prune_gpu_shape_cache` so the memo stays bounded.
    """
    # replica_id -> (gpu_type, gpu_count). A shape is cached only once the
    # replica's launch has finished: while it is still provisioning, the
    # cluster record is rewritten for every failover attempt and its
    # accelerators can change, so a mid-launch resolution must be
    # re-resolved on later ticks. After launch the shape is fixed for the
    # replica's lifetime.
    _gpu_shape_cache: dict[int, tuple[str, int]]
    # replica_id -> hourly cost of launched resources (same lifecycle
    # rules as the shape cache). Backs cost-aware victim ordering in both
    # shape-aware autoscalers.
    _replica_cost_cache: dict[int, float]
    # Immutable per-decision legacy handle snapshot, populated before the
    # autoscaler state lock is acquired.
    _gpu_shape_handles_for_tick: dict[int, Any] | None

    @staticmethod
    def _gpu_shape_from_resources_override(
            replica_info: 'replica_managers.ReplicaInfo'
    ) -> tuple[str, int] | None:
        """Return the exact shape carried by a replica launch override."""
        resources_override = getattr(replica_info, 'resources_override', None)
        if not isinstance(resources_override, dict):
            return None
        accelerators = resources_override.get('accelerators')
        if not isinstance(accelerators, dict) or not accelerators:
            return None
        gpu_type = next(iter(accelerators))
        if not isinstance(gpu_type, str) or not gpu_type:
            return None
        try:
            gpu_count = max(1, int(accelerators[gpu_type]))
        except (TypeError, ValueError):
            gpu_count = 1
        return gpu_type, gpu_count

    def _prune_gpu_shape_cache(self, live_replica_ids: set[int]) -> None:
        """Drop cached shapes/costs for replicas that no longer exist."""
        for replica_id in list(self._gpu_shape_cache):
            if replica_id not in live_replica_ids:
                del self._gpu_shape_cache[replica_id]
        for replica_id in list(self._replica_cost_cache):
            if replica_id not in live_replica_ids:
                del self._replica_cost_cache[replica_id]

    def _resolve_replica_handles(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, Any]:
        """Batch-resolve cluster handles for replicas missing a cached memo.

        `ReplicaInfo.handle()` with no record hits the cluster table once per
        call, and while a replica is provisioning neither the shape nor the
        cost memo may cache (the record is rewritten per failover attempt), so
        a selection pass that scores each replica twice would pay 2 reads per
        provisioning replica. One batched read replaces all of them, and also
        scores shape and cost from the same record snapshot instead of two
        reads at different times mid-sort.
        """
        uncached = [
            info for info in replica_infos
            if info.replica_id not in self._gpu_shape_cache or
            info.replica_id not in self._replica_cost_cache
        ]
        if not uncached:
            return {}
        tick_handles = getattr(self, '_gpu_shape_handles_for_tick', None)
        if tick_handles is not None:
            return {
                info.replica_id: tick_handles.get(info.replica_id)
                for info in uncached
            }
        records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in uncached])
        handles: dict[int, Any] = {}
        for info in uncached:
            record = records.get(info.cluster_name)
            # A missing record means the cluster row is gone; a bare
            # info.handle() would resolve to None too, just via another read.
            handles[info.replica_id] = (info.handle(record)
                                        if record is not None else None)
        return handles

    def _resolve_gpu_shape_handles(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, Any]:
        """Batch-resolve legacy shapes before entering an autoscaler lock.

        Exact-card launch overrides are hard resource constraints and can be
        read directly. They are deliberately not memoized until launch
        succeeds, so an override rewritten by failover is observed next tick.
        Cost-aware victim selection still needs launched-resource handles, so
        every missing shape or cost memo is included in this one outside-lock
        batch instead of falling back to per-replica reads under the lock.
        """
        unresolved = [
            info for info in replica_infos
            if ((info.replica_id not in self._gpu_shape_cache and
                 self._gpu_shape_from_resources_override(info) is None) or
                info.replica_id not in self._replica_cost_cache)
        ]
        if not unresolved:
            return {}
        records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in unresolved])
        return {
            info.replica_id: (info.handle(records[info.cluster_name])
                              if info.cluster_name in records else None
                             ) for info in unresolved
        }

    def _get_hourly_cost_from_replica_info(
            self,
            replica_info: 'replica_managers.ReplicaInfo',
            handle: Any = _UNRESOLVED_HANDLE) -> float:
        """Hourly cost of a replica's launched resources (0.0 = reserved).

        Used to prefer scaling down PAID replicas before zero-cost ones
        (e.g. cloud spot before a reserved Kubernetes pool) -- without
        this, shedding the expensive replica first is luck, not policy.
        Unknown costs resolve to 0.0 (treated like reserved capacity, so
        they are shed last -- the conservative direction for cost).
        """
        cached = self._replica_cost_cache.get(replica_info.replica_id)
        if cached is not None:
            return cached
        cost = 0.0
        resolved = False
        try:
            if handle is _UNRESOLVED_HANDLE:
                tick_handles = getattr(self, '_gpu_shape_handles_for_tick',
                                       None)
                if tick_handles is not None:
                    handle = tick_handles.get(replica_info.replica_id)
                else:
                    handle = replica_info.handle()
            if handle is not None:
                # Coerce: anything non-numeric degrades to 0.0 (shed last).
                cost = float(handle.launched_resources.get_cost(seconds=3600))
                resolved = True
        except Exception:  # pylint: disable=broad-except
            cost = 0.0
        # Same post-launch-only cache rule as the shape memo: while the
        # replica is provisioning the record may be rewritten by failover.
        if (resolved and replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._replica_cost_cache[replica_info.replica_id] = cost
        return cost

    def _get_gpu_shape_from_replica_info(
            self,
            replica_info: 'replica_managers.ReplicaInfo',
            handle: Any = _UNRESOLVED_HANDLE) -> tuple[str, int]:
        """Extract (GPU type, GPU count) from ReplicaInfo object."""
        cached = self._gpu_shape_cache.get(replica_info.replica_id)
        if cached is not None:
            return cached
        override_shape = self._gpu_shape_from_resources_override(replica_info)
        if override_shape is not None:
            gpu_type, gpu_count = override_shape
        else:
            gpu_type = 'unknown'
            gpu_count = 1
            if handle is _UNRESOLVED_HANDLE:
                tick_handles = getattr(self, '_gpu_shape_handles_for_tick',
                                       None)
                if tick_handles is not None:
                    handle = tick_handles.get(replica_info.replica_id,
                                              _UNRESOLVED_HANDLE)
            if handle is _UNRESOLVED_HANDLE:
                handle = replica_info.handle()
            if handle is not None:
                accelerators = handle.launched_resources.accelerators
                if accelerators and len(accelerators) > 0:
                    # Get the first accelerator entry.
                    gpu_type = list(accelerators.keys())[0]
                    try:
                        gpu_count = max(1, int(accelerators[gpu_type]))
                    except (TypeError, ValueError):
                        gpu_count = 1
        # Cache only a resolved shape of a replica whose launch has finished.
        # While the replica is still provisioning, the cluster record (and
        # thus launched_resources) is rewritten for every failover attempt, so
        # the accelerator resolved mid-launch may not be the one the launch
        # finally lands on and must be re-resolved on later ticks.
        if (gpu_type != 'unknown' and
                replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._gpu_shape_cache[replica_info.replica_id] = (gpu_type,
                                                              gpu_count)
        return gpu_type, gpu_count

    def _cost_rebalance_location_is_compatible(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        location: spot_placer.Location,
    ) -> bool:
        """Keep authoritative exact-card targets stable during rebalancing."""
        configured_shapes = getattr(self, 'configured_accelerator_shapes', {})
        if not configured_shapes:
            return True
        canonical_by_name = {
            card.casefold(): (card, count)
            for card, count in configured_shapes.items()
        }
        incumbent_card, incumbent_count = (
            self._get_gpu_shape_from_replica_info(incumbent))
        candidate_card, candidate_count = Autoscaler._location_gpu_shape(  # pylint: disable=protected-access
            location)
        configured = canonical_by_name.get(candidate_card.casefold())
        if configured is None:
            return False
        canonical_card, configured_count = configured
        return (incumbent_card.casefold() == canonical_card.casefold() and
                incumbent_count == configured_count and
                candidate_count == configured_count)


class InstanceAwareRequestRateAutoscaler(_GpuShapeResolverMixin,
                                         RequestRateAutoscaler):
    """Instance-aware RequestRateAutoscaler:
    Autoscale based on each replica's GPU-specific QPS.

    This autoscaler considers different QPS targets for different GPU types
    when target_qps_per_replica is provided as a dictionary mapping GPU types
    to their respective QPS targets.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        super().__init__(service_name, spec, version)
        # Serializes version/catalog publication, demand ingestion, reserved
        # supply, and decision generation. The controller publishes a retained
        # QPS autoscaler through update_version_and_accelerator_shapes(), so a
        # decision can never combine the new QPS dict with the old card catalog.
        self._instance_state_lock = threading.RLock()
        # Ensure target_qps_per_replica is a dict for instance-aware logic
        assert isinstance(spec.target_qps_per_replica, dict), \
            'InstanceAware Autoscaler requires dict type target_qps_per_replica'
        # Re-assign with correct type using setattr to avoid typing issues
        self.target_qps_per_replica = spec.target_qps_per_replica
        # Memoizes a replica's resolved GPU shape (replica_id ->
        # (gpu_type, gpu_count)) so the blocking handle() DB read + unpickle
        # is not repeated for the same replica across the 2-3 passes per
        # decision tick. A shape is cached only once the replica's launch has
        # finished: while it is still provisioning, the cluster record is
        # rewritten for every failover attempt and its accelerators can
        # change, so a mid-launch resolution must be re-resolved on later
        # ticks. After launch the shape is fixed for the replica's lifetime.
        # Pruned to the live replica set each tick.
        self._gpu_shape_cache: dict[int, tuple[str, int]] = {}
        # replica_id -> hourly cost of launched resources (same lifecycle
        # rules as the shape cache).
        self._replica_cost_cache: dict[int, float] = {}
        # Shapes already warned about bare-key per-GPU scaling.
        self._bare_key_warned: set[tuple[str, int]] = set()
        # One-shot hysteresis bypass, armed by update_version AND at
        # construction: the base class snaps target_num_replicas directly
        # after an update so the service scales quickly; the instance-
        # aware equivalent must wait for the next tick's shape-aware
        # recompute, which must then apply its result immediately instead
        # of being gated behind the upscale/downscale delay counters.
        # Armed at construction because a rebuilt autoscaler (controller
        # restart) starts at target=min_replicas with no hysteresis
        # history worth protecting: mid-rolling-update, letting that
        # stale minimum stand for the upscale delay would satisfy the
        # drain's 'ready latest >= target' cutoff and retire all old
        # capacity while the real target is still counters away.
        self._snap_target_on_next_recompute: bool = True
        # version -> that version's qps dict. A live replica's capacity is
        # a property of the spec it was launched under: after a
        # shape-changing update (e.g. {'L4': 0.1} -> {'A100': 10.0}) the
        # old shape is missing from the new dict and would resolve via the
        # min-value fallback — overestimating 100 old L4s by 100x, which
        # collapses the computed target and lets the rolling drain kill
        # them before the new capacity exists. Pruned each tick to the
        # live replica versions (+ latest).
        self._qps_dict_by_version: dict[int, dict[str, float]] = {
            version: spec.target_qps_per_replica
        }
        # Missing or failed historical-spec reads fall back for one decision
        # tick. Keep that fallback out of the durable live-version cache so a
        # later tick retries and can heal.
        self._qps_dict_unavailable_versions_for_tick: set[int] | None = None
        self.compatibility_profiles: list[dict[str, Any]] = []
        # Outstanding queue demand is a last-writer-wins gauge. Unlike arrival
        # profiles, it must be replaced on every authoritative LB report rather
        # than accumulated across the QPS window.
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
        # Recent rejections are a replaceable gauge used for launch priority.
        # They do not change the QPS magnitude, which remains derived from the
        # accepted-arrival window.
        self.rejected_compatibility_profiles: list[dict[str, Any]] = []
        # False after a catalog transition until a version-matched LB report
        # replaces every exact-card gauge. Incomplete/old reports may still
        # refresh aggregate QPS timestamps but cannot re-arm cleared profiles.
        self._compatibility_demand_complete: bool = False
        # Controller-owned exact task shapes. target_qps_per_replica keys are
        # performance profiles, not an authoritative resource shape: a bare
        # A100 profile can still describe an A100:8 task resource.
        self.configured_accelerator_shapes: dict[str, int] = {}
        # Fresh cached physical reserved supply, fed once per controller tick.
        # This is marginal supply only; ready/provisioning replicas are counted
        # independently below and must not be double-counted.
        self.free_reserved_slots_by_accelerator: dict[str, int] = {}
        configured_cards = self._configured_cards_from_profiles()
        while (sum(self.target_num_replicas_by_accelerator.values())
               < self.target_num_replicas and configured_cards):
            card = configured_cards[0]
            self.target_num_replicas_by_accelerator[card] = (
                self.target_num_replicas_by_accelerator.get(card, 0) + 1)

    def set_configured_accelerator_shapes(self, shapes: dict[str, int]) -> None:
        """Set canonical exact-card GPU counts from active task resources."""
        with self._instance_state_lock:
            self._set_configured_accelerator_shapes_locked(shapes)

    def _set_configured_accelerator_shapes_locked(
            self, shapes: dict[str, int]) -> None:
        """Set exact-card shapes while holding the instance-state lock."""
        previous_shapes = self.configured_accelerator_shapes
        configured_shapes = {
            str(card): int(count)
            for card, count in shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        }
        catalog_changed = (bool(previous_shapes) and
                           configured_shapes != previous_shapes)
        self.configured_accelerator_shapes = configured_shapes
        if catalog_changed or not configured_shapes:
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.rejected_compatibility_profiles = []
            self.target_num_replicas_by_accelerator = {}
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self.free_reserved_slots_by_accelerator = {}
            self._compatibility_demand_complete = False

    def set_free_reserved_slots_by_accelerator(self, slots: dict[str,
                                                                 int]) -> None:
        """Set fresh unmaterialized reserved supply by exact card."""
        with self._instance_state_lock:
            self._set_free_reserved_slots_by_accelerator_locked(slots)

    def _set_free_reserved_slots_by_accelerator_locked(
            self, slots: dict[str, int]) -> None:
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        normalized: dict[str, int] = {}
        for raw_card, raw_count in slots.items():
            card = configured_by_name.get(str(raw_card).casefold())
            if card is None or isinstance(raw_count, bool):
                continue
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                continue
            if count > 0:
                normalized[card] = normalized.get(card, 0) + count
        self.free_reserved_slots_by_accelerator = normalized

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        with self._instance_state_lock:
            self._collect_request_information_locked(request_aggregator_info)

    def _collect_request_information_locked(
            self, request_aggregator_info: dict[str, Any]) -> None:
        super().collect_request_information(request_aggregator_info)
        compatibility_complete = request_aggregator_info.get(
            'compatibility_demand_complete')
        if compatibility_complete is not True:
            # Direct/legacy construction without an authoritative catalog
            # retains the pre-fence test and compatibility behavior. Once the
            # controller supplies a catalog, only a version-matched complete
            # report may replace exact-card state.
            compatibility_complete = ('compatibility_demand_complete'
                                      not in request_aggregator_info and
                                      not self.configured_accelerator_shapes)
        if not compatibility_complete:
            self._compatibility_demand_complete = False
            return
        for profile in request_aggregator_info.get('compatibility_profiles',
                                                   []):
            if not isinstance(profile, dict):
                continue
            timestamp = profile.get('timestamp')
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            count = profile.get('count', 1)
            if (not isinstance(timestamp,
                               (int, float)) or isinstance(timestamp, bool) or
                    not isinstance(priority, int) or
                    isinstance(priority, bool) or accelerators is None or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1 or not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            self.compatibility_profiles.append({
                'timestamp': float(timestamp),
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        queued_profiles: list[dict[str, Any]] = []
        for profile in request_aggregator_info.get(
                'queued_requests_by_compatibility', []):
            if not isinstance(profile, dict):
                continue
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            count = profile.get('count', 1)
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1 or not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            queued_profiles.append({
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        self.queued_compatibility_profiles = queued_profiles
        rejected_profiles: list[dict[str, Any]] = []
        for profile in request_aggregator_info.get(
                'rejected_requests_by_compatibility', []):
            if not isinstance(profile, dict):
                continue
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            recent_count = profile.get('recent_count', profile.get('count', 1))
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(recent_count, int) or
                    isinstance(recent_count, bool) or recent_count < 1 or
                    not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            rejected_profiles.append({
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'recent_count': recent_count,
            })
        self.rejected_compatibility_profiles = rejected_profiles
        self._compatibility_demand_complete = True
        self._launch_priority_report_received_at = time.time()
        cutoff = time.time() - self.qps_window_size
        self.compatibility_profiles = [
            profile for profile in self.compatibility_profiles
            if profile['timestamp'] >= cutoff
        ]

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Preserve exact-card demand across an autoscaler replacement."""
        with self._instance_state_lock:
            return self._dump_dynamic_states_locked()

    def _dump_dynamic_states_locked(self) -> dict[str, Any]:
        states = super()._dump_dynamic_states()
        states['compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.compatibility_profiles]
        states['queued_compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.queued_compatibility_profiles]
        states['rejected_compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.rejected_compatibility_profiles]
        states['compatibility_demand_complete'] = (
            self._compatibility_demand_complete)
        states['configured_accelerator_shapes'] = dict(
            self.configured_accelerator_shapes)
        states['launch_priority_report_received_at'] = (
            self._launch_priority_report_received_at)
        return states

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Restore exact-card arrivals and the replaceable queue gauge."""
        compatibility_arrivals_present = ('compatibility_profiles'
                                          in dynamic_states)
        profiles = dynamic_states.pop('compatibility_profiles', [])
        queued_profiles = dynamic_states.pop('queued_compatibility_profiles',
                                             [])
        rejected_profiles = dynamic_states.pop(
            'rejected_compatibility_profiles', [])
        compatibility_complete = bool(
            dynamic_states.pop('compatibility_demand_complete', False))
        source_shapes = dynamic_states.pop('configured_accelerator_shapes', {})
        priority_report_received_at = dynamic_states.pop(
            'launch_priority_report_received_at', None)
        super()._load_dynamic_states(dynamic_states)
        self.compatibility_profiles = []
        self.queued_compatibility_profiles = []
        self.rejected_compatibility_profiles = []
        self.configured_accelerator_shapes = {
            str(card): int(count)
            for card, count in source_shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        } if isinstance(source_shapes, dict) else {}
        # Cross-type dumps from older binaries do not identify the catalog
        # that admitted their profiles. Preserve aggregate timestamps but fail
        # closed on exact-card transfer until a fresh report arrives.
        compatibility_complete = (compatibility_complete and
                                  compatibility_arrivals_present and
                                  bool(self.configured_accelerator_shapes))
        self.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': profiles,
            'queued_requests_by_compatibility': queued_profiles,
            'rejected_requests_by_compatibility': rejected_profiles,
            'compatibility_demand_complete': compatibility_complete,
        })
        self._launch_priority_report_received_at = (
            float(priority_report_received_at)
            if isinstance(priority_report_received_at, (int, float)) and
            not isinstance(priority_report_received_at, bool) else None)

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        shape_handles = self._resolve_gpu_shape_handles(replica_infos)
        with self._instance_state_lock:
            self._gpu_shape_handles_for_tick = shape_handles
            self._qps_dict_unavailable_versions_for_tick = set()
            try:
                return self._generate_scaling_decisions_locked(
                    replica_infos, active_versions)
            finally:
                self._qps_dict_unavailable_versions_for_tick = None
                self._gpu_shape_handles_for_tick = None

    def _generate_scaling_decisions_locked(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        # Recompute the shape-aware target BEFORE the base class runs the
        # outdated-replica drain: the drain compares ready new-version
        # replicas against target_num_replicas, and a stale target (e.g.
        # right after an update that lowered per-replica capacity) would
        # scale down every old replica while only a fraction of the
        # required new capacity exists. This is the single recompute for
        # the tick; _generate_scaling_decisions must not recompute again
        # or the hysteresis counters would double-increment.
        # Drop cached GPU types for replicas that no longer exist so the
        # cache stays bounded to the live replica set.
        live_replica_ids = {info.replica_id for info in replica_infos}
        self._prune_gpu_shape_cache(live_replica_ids)
        keep_versions = {info.version for info in replica_infos}
        keep_versions.add(self.latest_version)
        for version in list(self._qps_dict_by_version):
            if version not in keep_versions:
                del self._qps_dict_by_version[version]
        self._set_target_num_replicas_with_instance_aware_logic(replica_infos)
        return super().generate_scaling_decisions(replica_infos,
                                                  active_versions)

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate autoscaling decisions with instance-aware logic.

        The shape-aware target was already recomputed for this tick in
        generate_scaling_decisions (before the outdated-replica drain).
        """
        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if not info.is_terminal and info.version == self.latest_version:
                latest_nonterminal_replicas.append(info)

        target_num_replicas = self.get_final_target_num_replicas()
        current_num_replicas = len(latest_nonterminal_replicas)

        scaling_decisions: list[AutoscalerDecision] = []

        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        if use_card_targets:
            replicas_by_card: dict[str, list[replica_managers.ReplicaInfo]] = {}
            ready_by_card: dict[str, int] = {}
            for info in latest_nonterminal_replicas:
                if _replica_is_retiring_card_supply(info):
                    continue
                card, _ = self._get_gpu_shape_from_replica_info(info)
                replicas_by_card.setdefault(card, []).append(info)
                if info.is_ready:
                    ready_by_card[card] = ready_by_card.get(card, 0) + 1
            shortages = {
                card: max(0, target - len(replicas_by_card.get(card, [])))
                for card, target in target_by_card.items()
            }
            if any(shortages.values()):
                for card, shortage in shortages.items():
                    for _ in range(shortage):
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_UP,
                                target={
                                    'accelerators': {
                                        card: self._configured_gpu_count(card)
                                    }
                                }))
                # Graceful non-preemptive transition: provisioning rows count
                # against duplicate launches, but excess old-card capacity is
                # retained until every target card is actually READY.
                return scaling_decisions
            all_targets_ready = all(
                ready_by_card.get(card, 0) >= target
                for card, target in target_by_card.items())
            if all_targets_ready:
                for card, replicas in replicas_by_card.items():
                    excess = max(0, len(replicas) - target_by_card.get(card, 0))
                    if excess <= 0:
                        continue
                    for replica_id in self._select_replicas_to_scale_down_by_qps(
                            excess, replicas):
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_DOWN,
                                target=replica_id))
            return scaling_decisions

        # Decide if to scale up or down.
        if target_num_replicas > current_num_replicas:
            for _ in range(target_num_replicas - current_num_replicas):
                # No resources_override to use when scaling up
                scaling_decisions.append(
                    AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                                       target=None))
        elif target_num_replicas < current_num_replicas:
            num_replicas_to_scale_down = \
                current_num_replicas - target_num_replicas

            # Use instance-aware scale down logic
            replicas_to_scale_down = self._select_replicas_to_scale_down_by_qps(
                num_replicas_to_scale_down, latest_nonterminal_replicas)
            for replica_id in replicas_to_scale_down:
                scaling_decisions.append(
                    AutoscalerDecision(AutoscalerDecisionOperator.SCALE_DOWN,
                                       target=replica_id))

        # Outdated replicas are handled by base class generate_scaling_decisions
        # No need to handle them here

        upscale_decisions = [
            d for d in scaling_decisions
            if d.operator == AutoscalerDecisionOperator.SCALE_UP
        ]
        downscale_decisions = [
            d for d in scaling_decisions
            if d.operator == AutoscalerDecisionOperator.SCALE_DOWN
        ]
        logger.info(f'Scaling decisions: '
                    f'{len(upscale_decisions)} scale up, '
                    f'{len(downscale_decisions)} scale down '
                    f'(latest nonterminal: {current_num_replicas}, '
                    f'target: {target_num_replicas})')

        return scaling_decisions

    def _configured_cards_from_profiles(self) -> list[str]:
        # A controller-provided task catalog is authoritative. In particular,
        # recent arrivals from the previous service version must not revive a
        # card that the active version removed. Direct/unit-test construction
        # has no task catalog and retains the additive fallbacks below.
        if self.configured_accelerator_shapes:
            return list(self.configured_accelerator_shapes)
        cards: list[str] = []
        seen: set[str] = set()
        if isinstance(self.target_qps_per_replica, dict):
            for key in self.target_qps_per_replica:
                card = key.partition(':')[0]
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for profile in (self.compatibility_profiles +
                        self.queued_compatibility_profiles):
            for card in profile['compatible_accelerators']:
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for card in self.min_replicas_by_accelerator:
            if card.casefold() not in seen:
                cards.append(card)
                seen.add(card.casefold())
        return cards

    def _actuation_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, int], bool]:
        """Revalidate exact QPS cold launches at the adopted total target."""
        demand_target = self.target_num_replicas_by_accelerator
        compatibility_complete = (self._compatibility_demand_complete or
                                  not self.configured_accelerator_shapes)
        if (not compatibility_complete or
                sum(demand_target.values()) != self.target_num_replicas):
            return {}, False
        has_exact_profiles = bool(self.compatibility_profiles or
                                  self.queued_compatibility_profiles)
        exact_profiles_available = (has_exact_profiles and
                                    (self._compatibility_demand_complete or
                                     not self.configured_accelerator_shapes))
        exact_arrival_qps = 0.0
        if exact_profiles_available:
            exact_arrival_qps = (sum(
                float(profile.get('count', 1))
                for profile in self.compatibility_profiles) /
                                 self.qps_window_size)
        aggregate_qps = len(self.request_timestamps) / self.qps_window_size
        aggregate_fallback_qps = max(0.0, aggregate_qps - exact_arrival_qps)
        final_target = self.get_final_target_num_replicas()
        desired_target = self._calculate_target_by_accelerator(
            replica_infos,
            include_exact_profiles=exact_profiles_available,
            fallback_aggregate_qps=aggregate_fallback_qps,
            min_replicas_override=final_target,
            max_replicas_override=final_target,
            use_existing_supply=True)
        cards = self._configured_cards_from_profiles()
        canonical_by_name = {card.casefold(): card for card in cards}
        nonretiring_supply = {card: 0 for card in cards}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is not None:
                nonretiring_supply[card] += 1
        for raw_card, count in self.free_reserved_slots_by_accelerator.items():
            card = canonical_by_name.get(raw_card.casefold())
            if card is not None:
                nonretiring_supply[card] += max(0, int(count))
        target = _revalidate_actuation_target(
            adopted_target=demand_target,
            desired_target=desired_target,
            nonretiring_supply=nonretiring_supply,
            configured_cards=cards,
            final_target=final_target)
        return target, sum(target.values()) == final_target

    def _cold_paid_card_order(self, configured_cards: list[str]) -> list[str]:
        """Order cold cards by nominal paid cost, independent of availability."""
        return _order_cold_paid_cards(configured_cards,
                                      self._cost_rebalance_spot_placer,
                                      self._configured_gpu_count,
                                      self._location_gpu_shape)

    def _configured_gpu_count(self, card: str) -> int:
        """Return the service's unique configured GPU count for a card."""
        for configured, count in self.configured_accelerator_shapes.items():
            if configured.casefold() == card.casefold():
                return count
        if isinstance(self.target_qps_per_replica, dict):
            prefix = f'{card.casefold()}:'
            for key in self.target_qps_per_replica:
                normalized = key.casefold()
                if normalized == card.casefold():
                    return 1
                if normalized.startswith(prefix):
                    try:
                        count = int(normalized[len(prefix):])
                    except ValueError:
                        continue
                    if count > 0:
                        return count
        return 1

    def _calculate_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        *,
        include_exact_profiles: bool = True,
        fallback_aggregate_qps: float | None = None,
        min_replicas_override: int | None = None,
        max_replicas_override: int | None = None,
        use_existing_supply: bool = False,
    ) -> dict[str, int]:
        """Allocate recent demand to exact cards, priority first."""
        configured_cards = self._configured_cards_from_profiles()
        floors_by_name = {
            card.casefold(): floor
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        capacities = {
            card: self._get_target_qps_for_gpu_shape(
                card,
                self._configured_gpu_count(card),
                version=self.latest_version) for card in configured_cards
        }
        ready_zero_cost: dict[str, int] = {card: 0 for card in configured_cards}
        ready: dict[str, int] = {card: 0 for card in configured_cards}
        provisioning: dict[str, int] = {card: 0 for card in configured_cards}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            card, _ = self._get_gpu_shape_from_replica_info(info)
            if card not in ready:
                continue
            if info.is_ready:
                ready[card] += 1
                if info.is_zero_cost is True:
                    ready_zero_cost[card] += 1
            else:
                # Every nonterminal non-ready row is committed future
                # capacity. It prevents duplicate launches, but the decision
                # path below does not let it authorize scale-down.
                provisioning[card] += 1

        cold_order = self._cold_paid_card_order(configured_cards)
        profiles = ([(int(profile['priority']),
                      tuple(profile['compatible_accelerators']),
                      float(profile.get('count', 1)) / self.qps_window_size)
                     for profile in (self.compatibility_profiles +
                                     self.queued_compatibility_profiles)]
                    if include_exact_profiles else [])
        if fallback_aggregate_qps is not None and fallback_aggregate_qps > 0:
            # Missing/incomplete exact telemetry means every configured card
            # is compatible. Preserve aggregate demand and let the shared
            # supply-aware allocator compose it with hard per-card floors.
            profiles.append(
                (0, tuple(configured_cards), fallback_aggregate_qps))
        return _allocate_compatibility_target(
            configured_cards=configured_cards,
            capacities=capacities,
            floors=floors_by_name,
            min_replicas=(self.min_replicas if min_replicas_override is None
                          else min_replicas_override),
            max_replicas=(self.max_replicas if max_replicas_override is None
                          else max_replicas_override),
            demand_profiles=profiles,
            fixed_work_by_accelerator={},
            ready_zero_cost=ready_zero_cost,
            ready=ready,
            provisioning=provisioning,
            free_reserved=self.free_reserved_slots_by_accelerator,
            cold_order=cold_order,
            use_existing_supply=use_existing_supply)

    def _set_target_num_replicas_with_instance_aware_logic(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Set target_num_replicas using instance-aware logic."""
        assert isinstance(self.target_qps_per_replica,
                          dict), 'Expected dict for instance-aware logic'
        num_requests_per_second = len(
            self.request_timestamps) / self.qps_window_size
        candidate_target_by_accelerator: dict[str, int] | None = None
        latest_capacities: list[float] = []
        configured_accelerator_shapes = getattr(
            self, 'configured_accelerator_shapes', {})
        has_exact_profiles = bool(
            getattr(self, 'compatibility_profiles', []) or
            getattr(self, 'queued_compatibility_profiles', []))
        exact_profiles_available = (
            has_exact_profiles and
            (getattr(self, '_compatibility_demand_complete', False) or
             not configured_accelerator_shapes))
        exact_arrival_qps = 0.0
        if exact_profiles_available:
            exact_arrival_qps = (sum(
                float(profile.get('count', 1))
                for profile in getattr(self, 'compatibility_profiles', [])) /
                                 self.qps_window_size)
        # Completeness describes the current report and its replaceable
        # gauges. It cannot retroactively attribute aggregate arrivals from an
        # earlier incomplete report that are still inside the QPS window.
        # Preserve that unmatched remainder as all-configured-card demand.
        aggregate_fallback_qps = max(
            0.0, num_requests_per_second - exact_arrival_qps)
        if (configured_accelerator_shapes or exact_profiles_available or
                getattr(self, 'min_replicas_by_accelerator', {})):
            candidate_target_by_accelerator = (
                self._calculate_target_by_accelerator(
                    replica_infos,
                    include_exact_profiles=exact_profiles_available,
                    fallback_aggregate_qps=aggregate_fallback_qps))
            target_num_replicas = self._clip_target_num_replicas(
                sum(candidate_target_by_accelerator.values()))
        else:
            # Compatibility telemetry is additive and versioned. Preserve the
            # pre-feature aggregate algorithm for an old LB rather than
            # inventing card assignments from missing data.
            target_qps_dict = self.target_qps_per_replica
            for info in replica_infos:
                if info.is_terminal or info.version != self.latest_version:
                    continue
                capacity = self._get_target_qps_for_gpu_shape(
                    *self._get_gpu_shape_from_replica_info(info),
                    version=info.version)
                if capacity > 0:
                    latest_capacities.append(capacity)
            latest_capacities.sort(reverse=True)
            raw_target_num = 0
            covered_qps = 0.0
            for capacity in latest_capacities:
                raw_target_num += 1
                covered_qps += capacity
                if covered_qps > num_requests_per_second:
                    break
            if covered_qps <= num_requests_per_second:
                remaining_qps = num_requests_per_second - covered_qps
                estimated_qps = (latest_capacities[0]
                                 if latest_capacities else 0.0)
                if estimated_qps <= 0:
                    estimated_qps = max(target_qps_dict.values())
                if estimated_qps > 0 and remaining_qps > 0:
                    raw_target_num += math.ceil(remaining_qps / estimated_qps)
            raw_target_num = max(
                raw_target_num,
                sum(getattr(self, 'min_replicas_by_accelerator', {}).values()))
            target_num_replicas = self._clip_target_num_replicas(raw_target_num)
        logger.info(f'Instance-aware autoscaling: '
                    f'requests/s: {num_requests_per_second}, '
                    f'latest-version capacities: {latest_capacities}, '
                    'target by accelerator: '
                    f'{candidate_target_by_accelerator}, '
                    f'target replicas (latest version): '
                    f'{target_num_replicas}')

        # Apply hysteresis logic
        old_target_num_replicas = self.target_num_replicas

        target_map_changed = (candidate_target_by_accelerator is not None and
                              candidate_target_by_accelerator != getattr(
                                  self, 'target_num_replicas_by_accelerator',
                                  {}))
        candidate_target_map = candidate_target_by_accelerator or {}
        apply_target = False
        if self._snap_target_on_next_recompute:
            # First recompute after an update: apply directly (the base
            # class's post-update snap semantics, but shape-aware).
            self._snap_target_on_next_recompute = False
            self.upscale_counter = 0
            self.downscale_counter = 0
            apply_target = True
        # Faster scale up when there is no replica.
        elif self.target_num_replicas == 0:
            apply_target = True
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                apply_target = True
        elif target_num_replicas < self.target_num_replicas:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                apply_target = True
        elif (target_map_changed and any(
                candidate_target_map.get(card, 0) > getattr(
                    self, 'target_num_replicas_by_accelerator', {}).get(
                        card, 0) for card in candidate_target_map)):
            # A same-size exact-card migration is an upscale for hysteresis
            # purposes. A LOWER aggregate target is handled by the branch
            # above even when its card mix contains a positive delta: card
            # churn must not restart aggregate downscale proof indefinitely.
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                apply_target = True
        elif target_map_changed:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                apply_target = True
        else:
            self.upscale_counter = self.downscale_counter = 0
        if apply_target:
            self.target_num_replicas = target_num_replicas
            # Aggregate fallback deliberately has no exact-card assignment.
            # Clear an older compatibility map so status and the decision path
            # cannot reuse a stale shape target after the gauge becomes empty
            # or an old LB stops publishing compatibility telemetry.
            self.target_num_replicas_by_accelerator = dict(
                candidate_target_by_accelerator or {})

        logger.info(
            f'Instance-aware: Old target number of replicas: '
            f'{old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Num overprovision: {self.num_overprovision}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'Downscale counter: {self.downscale_counter}/'
            f'{self.scale_down_threshold}. ')

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Capacity-aware rolling drain of old-version replicas.

        The base class keeps (target - ready_new) OLD replicas — a count
        that treats every replica as interchangeable. With per-shape
        capacities that can retire 99% of the serving capacity while one
        big new replica is still alone. Keep old replicas by CAPACITY:
        enough READY old ones to cover the demand the ready latest
        replicas cannot yet serve, never fewer than the base class would
        have kept.
        """
        if self.update_mode != serve_utils.UpdateMode.ROLLING:
            return super()._select_outdated_replicas_to_scale_down(
                replica_infos, active_versions)
        old_nonterminal = [
            info for info in replica_infos
            if info.version < self.latest_version and not info.is_terminal
        ]
        if not old_nonterminal:
            return []
        actuation_target, exact_target_complete = (
            self._actuation_target_by_accelerator(replica_infos))
        if exact_target_complete:
            canonical_by_name = {
                card.casefold(): card for card in actuation_target
            }
            ready_latest_by_card = {card: 0 for card in actuation_target}
            for info in replica_infos:
                if (info.version != self.latest_version or not info.is_ready or
                        _replica_is_retiring_card_supply(info)):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is not None:
                    ready_latest_by_card[card] += 1
            if any(
                    ready_latest_by_card.get(card, 0) < target
                    for card, target in actuation_target.items()):
                # The latest fleet may satisfy the aggregate count entirely
                # with the wrong card. Launch the exact replacement first;
                # retaining all old replicas for one more tick is the only
                # non-preemptive rollout choice.
                return []
        num_ready_latest = 0
        ready_latest_capacity = 0.0
        for info in replica_infos:
            if (info.version == self.latest_version and info.is_ready and
                    not _replica_is_retiring_card_supply(info)):
                num_ready_latest += 1
                ready_latest_capacity += self._get_target_qps_for_gpu_shape(
                    *self._get_gpu_shape_from_replica_info(info),
                    version=info.version)
        if num_ready_latest >= self.get_final_target_num_replicas():
            # Enough latest-version replicas: retire all old ones (same
            # terminal condition as the base class).
            return [info.replica_id for info in old_nonterminal]

        demand = len(self.request_timestamps) / self.qps_window_size
        shortfall = demand - ready_latest_capacity
        # Never keep fewer old replicas than the base class's count rule
        # (target - ready_new): capacity packing with a few big old
        # replicas could otherwise drain the standby pool a low-traffic
        # service relies on for its next request.
        keep_count_floor = min(
            len(old_nonterminal),
            max(0,
                self.get_final_target_num_replicas() - num_ready_latest))

        ready_old = []
        nonready_old = []
        for info in old_nonterminal:
            capacity = self._get_target_qps_for_gpu_shape(
                *self._get_gpu_shape_from_replica_info(info),
                version=info.version)
            if info.is_ready:
                ready_old.append((capacity, info))
            else:
                nonready_old.append((capacity, info))
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        if unavailable_versions:
            logger.info(
                'Instance-aware rolling drain waiting for historical '
                'capacity for versions: %s.', sorted(unavailable_versions))
            return []
        # Largest capacity first: fewest old replicas kept, fastest
        # rollout. Replica id tie-break keeps the selection stable
        # across ticks.
        ready_old.sort(key=lambda pair: (-pair[0], pair[1].replica_id))

        keep_ids: set[int] = set()
        covered_qps = 0.0
        for capacity, info in ready_old:
            if covered_qps >= shortfall and len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
            if capacity > 0:
                covered_qps += capacity
        # Not-yet-ready old replicas add no serving capacity; they only
        # count toward the base-class floor (the base helper likewise
        # prefers draining initializing replicas first).
        for _, info in nonready_old:
            if len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)

        return [
            info.replica_id
            for info in old_nonterminal
            if info.replica_id not in keep_ids
        ]

    def _get_qps_dict_for_version(self, version: int) -> dict[str, float]:
        """The qps dict a given service version was launched under.

        Unknown versions (the autoscaler was rebuilt after the update
        that created them, e.g. a controller restart mid-rolling-update)
        rehydrate from the durable per-version spec so old-version
        replicas keep their real capacity. Falls back to the latest dict
        when the version's spec is unavailable; misses are not memoized
        across ticks so a transient DB error can heal on the next tick.
        """
        cached = self._qps_dict_by_version.get(version)
        if cached is not None:
            return cached
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        if (unavailable_versions is not None and
                version in unavailable_versions):
            assert isinstance(self.target_qps_per_replica, dict), \
                'Expected dict for instance-aware logic'
            return self.target_qps_per_replica
        qps_dict = None
        load_failed = False
        try:
            spec = serve_state.get_spec(self._service_name, version)
            if spec is not None:
                qps_dict = spec.target_qps_per_replica
        except Exception as e:  # pylint: disable=broad-except
            load_failed = True
            logger.warning('Failed to load spec for version '
                           f'{version}: {common_utils.format_exception(e)}')
        if not isinstance(qps_dict, dict):
            if not load_failed:
                logger.warning(
                    'No usable target QPS spec for historical version %s; '
                    'using the latest-version fallback.', version)
            if unavailable_versions is not None:
                unavailable_versions.add(version)
            assert isinstance(self.target_qps_per_replica, dict), \
                'Expected dict for instance-aware logic'
            return self.target_qps_per_replica
        self._qps_dict_by_version[version] = qps_dict
        return qps_dict

    def _get_target_qps_for_gpu_shape(self,
                                      gpu_type: str,
                                      gpu_count: int,
                                      version: int | None = None) -> float:
        """Per-replica target QPS for a `gpu_count` x `gpu_type` replica.

        Resolution (see serve_utils.resolve_target_qps_for_gpu_shape):
        exact shape key is a per-replica value; a bare type key is
        per-GPU and is multiplied by the replica's GPU count.

        `version` selects the qps dict the replica was launched under, so
        old-version replicas keep their real capacity across a
        shape-changing update (falls back to the latest dict when the
        version's dict is unknown, e.g. after a controller restart).
        """
        assert isinstance(self.target_qps_per_replica,
                          dict), 'Expected dict for instance-aware logic'
        target_qps_dict = self.target_qps_per_replica
        if version is not None and version != self.latest_version:
            target_qps_dict = self._get_qps_dict_for_version(version)

        resolved = serve_utils.resolve_target_qps_for_gpu_shape(
            gpu_type, gpu_count, target_qps_dict)
        if resolved is not None:
            if (gpu_count > 1 and
                    f'{gpu_type}:{gpu_count}' not in target_qps_dict and
                (gpu_type, gpu_count) not in self._bare_key_warned):
                # Per-GPU scaling of a bare type key assumes ONE model
                # instance per GPU. A replica serving K-GPU model
                # instances is over-counted by K unless an exact shape
                # key pins its per-replica capacity. Warn once per shape.
                self._bare_key_warned.add((gpu_type, gpu_count))
                logger.warning(
                    f'Multi-GPU replica shape {gpu_type}:{gpu_count} is '
                    'scaled from a bare per-GPU QPS key. This is correct '
                    'ONLY if each GPU hosts one model instance; for '
                    'k-GPU-per-instance models declare an exact shape '
                    f'key (e.g. "{gpu_type}:{gpu_count}": '
                    '<instances_per_replica * qps_per_instance>).')
            return resolved

        # Fallback to minimum QPS
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        using_historical_fallback = (version is not None and
                                     version != self.latest_version and
                                     unavailable_versions is not None and
                                     version in unavailable_versions)
        if not using_historical_fallback:
            logger.warning(f'No matching QPS found for GPU shape: '
                           f'{gpu_type}:{gpu_count}. '
                           f'Available types: {list(target_qps_dict.keys())}. '
                           f'Using minimum QPS as fallback.')
        return min(target_qps_dict.values())

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        return self._get_target_qps_for_gpu_shape(
            *self._get_gpu_shape_from_replica_info(info), version=info.version)

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        return self._get_target_qps_for_gpu_shape(
            *self._location_gpu_shape(location), version=self.latest_version)

    def _select_replicas_to_scale_down_by_qps(
            self, num_replicas_to_scale_down: int,
            replica_infos: list['replica_managers.ReplicaInfo']) -> list[int]:
        """Select replicas to scale down (lowest QPS first)."""
        # Create a list of (replica_info, target_qps) tuples
        replica_qps_pairs: list[tuple[replica_managers.ReplicaInfo, float]] = []

        # One batched cluster-table read for every replica the memos cannot
        # serve; the sort below scores each replica twice (shape + cost).
        handles = self._resolve_replica_handles(replica_infos)

        for info in replica_infos:
            # Include old-version replicas as well so they also get a target_qps
            # assigned. Skip terminal replicas only.
            if info.is_terminal:
                continue

            # Get GPU shape directly from replica info
            gpu_type, gpu_count = self._get_gpu_shape_from_replica_info(
                info, handles.get(info.replica_id, _UNRESOLVED_HANDLE))

            # Use flexible matching logic, weighted by GPU count so
            # smaller-capacity replicas are preferred for scale-down.
            target_qps = self._get_target_qps_for_gpu_shape(
                gpu_type, gpu_count, version=info.version)

            replica_qps_pairs.append((info, float(target_qps)))
            logger.info(f'Replica {info.replica_id} '
                        f'with GPU {gpu_type}:{gpu_count}: {target_qps} QPS')

        # Create a mapping from replica_id to target_qps for sorting
        replica_qps_map = {
            info.replica_id: target_qps
            for info, target_qps in replica_qps_pairs
        }

        # Sort replicas by: 1. status order, 2. target_qps (asc),
        # 3. version (asc), 4. replica_id (desc).
        # scale_down_decision_order() is a classmethod returning the
        # static ordering list; the sort key needs this replica's INDEX
        # in it (the list itself is identical for every replica and
        # would let weighted QPS outrank status).
        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        # Cost breaks ties AFTER capacity (qps): among replicas of equal
        # serving capacity, shed the most expensive first (cloud spot
        # before a zero-cost reserved pool). Cost must NOT outrank qps —
        # the downscale target is computed assuming the highest-capacity
        # replicas are kept, so shedding a high-capacity paid replica
        # ahead of low-capacity free ones could leave less capacity than
        # the target assumed. Uniform-capacity fleets (all per-type qps
        # equal) get full cost-priority within each status tier.
        #
        # PER-MACHINE vs PER-GPU: the cost used here is the replica's
        # whole-machine hourly cost, and that is deliberate. Because cost
        # only compares replicas of equal RESOLVED qps (the configured
        # per-type targets, count-weighted — for unresolved shapes the
        # min-qps fallback applies, so this is not a guarantee about TRUE
        # capacity), machine cost ranks identically to cost-per-unit-of-
        # serving-capacity (same denominator) — the economically correct
        # metric. It is strictly better than per-GPU price, which would
        # misrank GPU types with different throughput (an A100:1 at
        # \$2/hr serving 0.4 qps beats an L4:4 at \$2.40/hr serving the
        # same 0.4 qps, despite the L4s' lower per-GPU price). The qps
        # key is quantized so float noise (3 * 0.1 != 0.3) cannot split
        # mathematically equal capacities away from the cost tie-break.
        sorted_replicas = sorted(
            replica_infos,
            key=lambda info: (
                _status_rank(info),
                round(replica_qps_map.get(info.replica_id, float('inf')), 9),
                -self._get_hourly_cost_from_replica_info(
                    info, handles.get(info.replica_id, _UNRESOLVED_HANDLE)),
                info.version,
                -info.replica_id,
            ))

        selected_replica_ids = []
        for info in sorted_replicas:
            if info.is_terminal:
                continue
            selected_replica_ids.append(info.replica_id)
            if len(selected_replica_ids) >= num_replicas_to_scale_down:
                break

        logger.info(
            f'Selected {len(selected_replica_ids)} replicas to scale down: '
            f'{selected_replica_ids}')
        return selected_replica_ids

    def _calculate_target_num_replicas(self) -> int:
        # Shape-aware sizing needs replica_infos, which this hook (invoked
        # by the base update_version to snap the target after an update)
        # does not receive. Keep the current target instead of snapping to
        # a shape-blind estimate: the outdated-replica drain in
        # generate_scaling_decisions consumes the target BEFORE the
        # instance-aware recompute runs, so an underestimate here could
        # scale down all old replicas mid-rolling-update with only a
        # fraction of the new capacity ready. The next decision tick
        # recomputes from live replica shapes via
        # _set_target_num_replicas_with_instance_aware_logic.
        return self._clip_target_num_replicas(self.target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        with self._instance_state_lock:
            self._update_version_locked(version, spec, update_mode)

    def update_version_and_accelerator_shapes(
            self, version: int, spec: 'service_spec.SkyServiceSpec',
            update_mode: serve_utils.UpdateMode,
            accelerator_shapes: dict[str, int]) -> None:
        """Atomically publish a QPS version and its exact-card catalog."""
        with self._instance_state_lock:
            self._update_version_locked(version, spec, update_mode)
            self._set_configured_accelerator_shapes_locked(accelerator_shapes)

    def _update_version_locked(self, version: int,
                               spec: 'service_spec.SkyServiceSpec',
                               update_mode: serve_utils.UpdateMode) -> None:
        # Ensure it's a dict and re-assign using setattr to avoid typing.
        # Must happen BEFORE super().update_version: the base class
        # recomputes target_num_replicas via _calculate_target_num_replicas,
        # which must see the new version's dict.
        if version <= self.latest_version:
            # The base class rejects stale versions; don't mutate the qps
            # dict or arm the post-update snap for a rejected call either.
            super(RequestRateAutoscaler,
                  self).update_version(version, spec, update_mode)
            return
        assert isinstance(spec.target_qps_per_replica, dict), \
            'InstanceAware Autoscaler requires dict type target_qps_per_replica'
        # Assign BEFORE the base update runs so any recompute it triggers
        # sees the new version's dict.
        self.target_qps_per_replica = spec.target_qps_per_replica
        self._qps_dict_by_version[version] = spec.target_qps_per_replica
        super(RequestRateAutoscaler,
              self).update_version(version, spec, update_mode)
        self._snap_target_on_next_recompute = True


class ConcurrencyAutoscaler(_GpuShapeResolverMixin, _AutoscalerWithHysteresis):
    """ConcurrencyAutoscaler: size the fleet by outstanding work.

    For long synchronous jobs (~1 h, one per GPU) request RATE measures
    arrival compression, not load: 100 hour-long jobs arriving over 2 min
    vs over 10 min are the same 100 concurrent jobs but produce 3x
    different QPS targets. This autoscaler instead targets
    `ceil(outstanding / per_replica_concurrency)` where outstanding =
    in-flight + queued + recently-rejected jobs, all reported by the LB as
    GAUGES over the sync channel (no clear-on-ack batches to lose or
    double-count on controller hiccups).

    The knob `target_concurrency_per_replica` is PER GPU. Physical-backend
    services pack outstanding work onto knob x gpu_count capacities. Logical
    services publish GPU-slot targets and divide outstanding work by the knob;
    backend packing happens later from those whole-slot targets.

    SIGNAL-GAP RULE: the demand gauges only exist in LB reports. A report
    is fresh iff it carried a non-None in-flight map and is younger than
    3x the LB sync interval. While no fresh report exists -- including a
    freshly (re)built autoscaler, which starts stale -- this autoscaler
    emits NO scale-down decisions and NO rolling-drain retirements at all:
    a rebuilt controller starts at target=min_replicas with no data, and
    acting on that would mass-retire a live fleet before the first sync.
    Scale-UP stays available while stale via the arrival floor
    ceil(arrivals_in_window / best_capacity) from request timestamps
    (which ride every sync), so a blind controller can still grow, never
    shrink.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        super().__init__(service_name, spec, version)
        target_concurrency = getattr(spec, 'target_concurrency_per_replica',
                                     None)
        assert target_concurrency is not None, (
            'ConcurrencyAutoscaler requires target_concurrency_per_replica')
        # Per-GPU target concurrency; a replica's capacity in concurrency
        # units is this knob x its gpu_count.
        self.target_concurrency_per_replica: float = float(target_concurrency)
        self.replica_unit: str = getattr(spec, 'replica_unit',
                                         'physical_backend')
        self.target_utilization_percentage: int = int(
            getattr(spec, 'target_utilization_percentage', 100))
        self.expected_request_duration_seconds: float | None = getattr(
            spec, 'expected_request_duration_seconds', None)
        self.initial_provision_lead_time_seconds: float | str | None = getattr(
            spec, 'initial_provision_lead_time_seconds', None)
        self.adaptive_demand_estimation: bool = (getattr(
            spec, 'adaptive_demand_estimation', True) is not False)
        # Live demand-estimation state. Both estimators supersede their
        # configured counterpart only while they hold enough fresh evidence;
        # configuration remains the fallback and the cold-start value.
        self._measured_duration_seconds: float | None = None
        self._measured_duration_samples: int = 0
        self._measured_duration_at: float | None = None
        # Cumulative per-bucket counts already folded into the estimate, so
        # a repeated (unacknowledged) histogram report is not double counted.
        self._prediction_counts_seen: dict[int, list[int]] = {}
        self._provision_lead_samples: list[float] = []
        self._provision_lead_at: float | None = None
        # Replica rows whose launch-to-ready has already been sampled.
        self._provision_lead_seen_replica_ids: set[int] = set()
        self.max_scale_up_rate_percentage: int | None = getattr(
            spec, 'max_scale_up_rate_percentage', None)
        self.scale_up_rate_min_replicas: int | None = getattr(
            spec, 'scale_up_rate_min_replicas', None)
        self.scale_up_rate_period_seconds: int | None = getattr(
            spec, 'scale_up_rate_period_seconds', None)
        adaptive_scale_up = getattr(spec, 'adaptive_scale_up', None)
        self.adaptive_scale_up: dict[str, Any] | None = (
            dict(adaptive_scale_up)
            if isinstance(adaptive_scale_up, dict) else None)
        queue_config = getattr(spec, 'lb_request_queue', None) or {}
        self._queue_timeout_seconds: float | None = queue_config.get(
            'timeout_seconds')
        self._queue_timeout_thresholds: tuple[tuple[int, float], ...] = tuple(
            (int(entry['min_priority']), float(entry['timeout_seconds']))
            for entry in queue_config.get('timeout_seconds_by_priority', ()))
        # SkyServiceSpec exposes 50 for new specs and restores 100 for old
        # pickles. Attribute-less test/legacy objects preserve old behavior.
        self.max_scale_down_rate_percentage: int = int(
            getattr(spec, 'max_scale_down_rate_percentage', 100))
        self._last_scale_up_wave_at: float | None = None
        # The timestamp opens a rollout window; this ceiling retains the
        # unspent part of that window when placement cannot make progress on
        # its first reconciliation tick. It is latest-version committed
        # logical capacity plus the authorized wave width.
        self._logical_scale_up_wave_ceiling: int | None = None
        # Logical downscale hysteresis is elapsed-time based. A nominal
        # decision tick can stretch substantially while probing a large fleet,
        # so a tick counter cannot implement a duration contract. This state is
        # deliberately controller-local and resets conservatively on rebuilds
        # and service updates.
        self._downscale_started_at: float | None = None
        self._raw_target_num_replicas: int = self.target_num_replicas
        self._latest_committed_capacity: int = 0
        self._latest_provisioning_capacity: int = 0
        self._rejected_concurrency: float = 0.0
        self._weighted_queue_work: float = 0.0
        self._arrival_floor_target: int = 0
        # Request timestamps back the arrival floor (the only up-signal
        # available while the demand report is stale), windowed exactly
        # like RequestRateAutoscaler's QPS window.
        self.qps_window_size: int = constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS
        self.request_timestamps: list[float] = []
        # Latest demand report from the LB. `None` in-flight means no
        # usable report has ever been received (or the loaded one carried
        # none): the signal-gap rule keys on this plus the report's age.
        # The gauges are stored verbatim; freshness is derived, never
        # stored, so a report ages out automatically (also after a
        # _load_dynamic_states round-trip, since the received-at time is
        # absolute).
        self._in_flight_by_replica_id: dict[int, int] | None = None
        self._queue_depth: int = 0
        self._queue_depth_by_priority: dict[int, int] | None = None
        self._rejected_in_window: int = 0
        self._rejected_in_recent_window: int | None = None
        self._rejected_in_window_by_priority: dict[int, int] | None = None
        self._rejected_in_recent_window_by_priority: dict[int,
                                                          int] | None = None
        self._unique_job_arrivals_60s: int | None = None
        self._unique_job_arrivals_300s: int | None = None
        self._headerless_arrivals_60s: int | None = None
        self._headerless_arrivals_300s: int | None = None
        self._offered_arrival_tracking_saturated: bool = False
        self._pressure_baseline: tuple[int, int, int] | None = None
        self._pressure_latched: bool = False
        self._pressure_reasons: tuple[str, ...] = ()
        self._pressure_streak: int = 0
        self._adaptive_until: float | None = None
        self._downscale_veto_reason: str | None = None
        # Consecutive pressure vetoes within the current downscale episode
        # (a run of recomputes whose raw target stays below the adopted
        # target). Bounded by _MAX_CONSECUTIVE_DOWNSCALE_VETOES: under
        # trickle traffic a tiny positive delta re-latches pressure nearly
        # every decision tick, and an unbounded veto would defer downscale
        # forever even after the hysteresis timer elapsed.
        self._downscale_veto_streak: int = 0
        self._pending_retention_floor: int | None = None
        self._pending_capacity_at_adoption: int = 0
        self._pending_budget_spent: int = 0
        self._last_scale_down_allowance: int = 0
        self._last_pending_allowance: int = 0
        # Replica ids whose declared async occupancy could not be sampled.
        # They contribute a retention floor to outstanding work: raw capacity
        # in physical mode and utilization-adjusted capacity in logical mode.
        # Unknown is a potentially-full replica, never an idle zero, but it is
        # not measured saturation that authorizes extra utilization headroom.
        self._unknown_in_flight_replica_ids: set[int] = set()
        self._report_received_at: float | None = None
        self._reconcile_generation: int = 0
        self._observed_slots_by_replica_id: dict[int, int] = {}
        self._unknown_capacity_replica_ids: set[int] = set()
        # Unknown capacity and an authoritative zero-slot report both mean a
        # ready backend cannot currently serve work. They share one bounded,
        # one-wave replacement incident state.
        self._degraded_capacity_since_by_replica_id: dict[int, float] = {}
        self._logical_state_lock = threading.RLock()
        self._last_logical_target_state: (
            tuple[int, int, int] |
            tuple[int, int, int, tuple[tuple[str, int], ...],
                  tuple[tuple[str, int], ...]] | None) = None
        self._gpu_shape_cache: dict[int, tuple[str, int]] = {}
        # Backs the cost-descending victim tiebreak (shed paid spot
        # before zero-cost reserved capacity); pruned with the shape
        # cache each tick.
        self._replica_cost_cache: dict[int, float] = {}
        # Replaceable exact-card gauges shipped with the authoritative
        # concurrency report. Running work is attributed separately from the
        # per-replica in-flight map at decision time, so it remains pinned to
        # the card already serving it.
        # Windowed accepted-arrival profiles shape the deduplicated offered-
        # arrival floor without controlling its magnitude. They also survive
        # a later switch to QPS autoscaling.
        self.compatibility_profiles: list[dict[str, Any]] = []
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
        self.rejected_compatibility_profiles: list[dict[str, Any]] = []
        self.configured_accelerator_shapes: dict[str, int] = {}
        self.free_reserved_slots_by_accelerator: dict[str, int] = {}
        self._compatibility_demand_complete: bool = False
        # version -> that version's per-GPU knob. A live replica's
        # capacity is a property of the spec it was launched under: after
        # an update that raises the knob (1 -> 2), sizing old-version
        # replicas with the NEW knob overstates their coverage 2x, so
        # the rolling drain would retire old replicas the kept set cannot
        # actually replace (same hazard the instance-aware autoscaler
        # guards with _qps_dict_by_version). Pruned each tick to the live
        # replica versions (+ latest).
        self._knob_by_version: dict[int, float] = {
            version: float(target_concurrency)
        }
        # See the request-rate autoscaler's matching tick-local memo. A failed
        # historical knob read is shared only within one decision tick.
        self._knob_unavailable_versions_for_tick: set[int] | None = None
        # One-shot hysteresis bypass, armed by update_version AND at
        # construction, same as the instance-aware autoscaler: the target
        # can only be recomputed on a tick (it needs replica shapes), and
        # that first recompute must apply immediately instead of being
        # gated behind the delay counters -- a rebuilt autoscaler
        # (controller restart) starts at target=min_replicas with no
        # hysteresis history worth protecting. Unlike the instance-aware
        # class the snap is consumed only once a FRESH demand report
        # exists: snapping on stale data would just re-assert the blind
        # minimum.
        self._snap_target_on_next_recompute: bool = True
        # Construction means controller restart: the first fresh report may
        # recover the demand-owned target from every surviving version. An
        # in-process version update also arms the snap above, but explicitly
        # clears this flag so its cold replacement still enters through the
        # configured rollout wave.
        self._adopt_total_capacity_on_next_recompute: bool = True
        # Per-tick freshness snapshot (see _fresh_for_tick). None outside
        # a tick.
        self._tick_fresh: bool | None = None
        # True only while an increase in the demand-derived target is waiting
        # for upscale hysteresis.  The live fleet must not be shrunk toward
        # the old target during that wait: doing so makes the autoscaler issue
        # scale-down and scale-up intents for opposite demand snapshots.
        self._upscale_pending: bool = False
        # Snapshotted before each decision tick mutates the aggregate wave
        # timestamp. Exact-card actuation uses this budget to limit cold card
        # migrations without retaining the physical supply mix in the public
        # demand target.
        self._logical_actuation_wave_budget: int | None = None
        self._logical_actuation_wave_started: bool = False
        self._logical_actuation_wave_is_new: bool = False
        self._logical_card_transition_pending: bool = False
        self._logical_actuation_target_by_accelerator: dict[str, int] = {}
        self._logical_actuation_desired_by_accelerator: dict[str, int] = {}
        # Explicit compatibility/floor ownership carried with the adopted
        # demand map. A later empty history can retry that exact owned card,
        # while synthesized aggregate padding remains distinguishable.
        self._logical_adopted_explicit_target_by_accelerator: dict[str,
                                                                   int] = {}
        # Paid-launch ownership is distinct from compatibility proof. An
        # aggregate minimum or headerless queued request may buy the cheapest
        # compatible card without proving that old-version work can move to
        # it during a rollout.
        self._logical_adopted_paid_target_by_accelerator: dict[str, int] = {}
        # Absolute paid capacity ceiling for the current actuation map. It is
        # derived from the separately allocated/adopted ownership map; during
        # rollout it also includes live same-card old-version backing. The
        # decision generator subtracts latest committed supply to obtain the
        # incremental launch authority.
        self._logical_paid_launch_target_by_accelerator: dict[str, int] = {}
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            # A cold logical service must enter through the configured slot
            # wave even when its aggregate or per-card floor is larger than
            # one wave. A rebuilt controller remains fail-closed at zero until
            # the first complete fresh report, then reconstructs live
            # committed capacity before applying this same limiter.
            self.target_num_replicas = 0
            self.target_num_replicas_by_accelerator = {}

    def set_configured_accelerator_shapes(self, shapes: dict[str, int]) -> None:
        """Set the active version's authoritative exact-card shapes."""
        with self._logical_state_lock:
            self._set_configured_accelerator_shapes_locked(shapes)

    def _set_configured_accelerator_shapes_locked(
            self, shapes: dict[str, int]) -> None:
        """Set exact-card shapes while holding the decision-state lock."""
        previous_shapes = self.configured_accelerator_shapes
        configured_shapes = {
            str(card): int(count)
            for card, count in shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        }
        catalog_changed = (bool(previous_shapes) and
                           configured_shapes != previous_shapes)
        self.configured_accelerator_shapes = configured_shapes
        if catalog_changed:
            # Compatibility gauges describe the catalog under which the LB
            # admitted them. Never reinterpret an A100-only waiter as H100
            # demand, or an A100:1 target as A100:8 capacity, across an atomic
            # task-catalog update. The next complete report re-establishes all
            # replaceable gauges under the new routing version.
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.rejected_compatibility_profiles = []
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self._logical_card_transition_pending = False
            self._logical_actuation_target_by_accelerator = {}
            self._logical_actuation_desired_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self._compatibility_demand_complete = False
        if not self.configured_accelerator_shapes:
            self.target_num_replicas_by_accelerator = {}
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.rejected_compatibility_profiles = []
            self.free_reserved_slots_by_accelerator = {}
            self._logical_card_transition_pending = False
            self._logical_actuation_target_by_accelerator = {}
            self._logical_actuation_desired_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self._compatibility_demand_complete = False
            return
        floors = {
            card.casefold(): floor
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        canonical_target: dict[str, int] = {}
        remaining = self.target_num_replicas
        for card in self.configured_accelerator_shapes:
            floor = min(remaining, int(floors.get(card.casefold(), 0)))
            if floor > 0:
                canonical_target[card] = floor
                remaining -= floor
        first = next(iter(self.configured_accelerator_shapes))
        while sum(canonical_target.values()) < self.target_num_replicas:
            canonical_target[first] = canonical_target.get(first, 0) + 1
        self.target_num_replicas_by_accelerator = canonical_target
        self._logical_adopted_explicit_target_by_accelerator = {
            card: min(canonical_target.get(card, 0),
                      max(0, int(floors.get(card.casefold(), 0))))
            for card in canonical_target
            if floors.get(card.casefold(), 0) > 0
        }
        self._logical_adopted_paid_target_by_accelerator = dict(
            canonical_target)

    def set_free_reserved_slots_by_accelerator(self, slots: dict[str,
                                                                 int]) -> None:
        """Set fresh unmaterialized reserved supply by exact card."""
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        normalized: dict[str, int] = {}
        for raw_card, raw_count in slots.items():
            card = configured_by_name.get(str(raw_card).casefold())
            if card is None or isinstance(raw_count, bool):
                continue
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                continue
            if count > 0:
                normalized[card] = normalized.get(card, 0) + count
        self.free_reserved_slots_by_accelerator = normalized

    def _configured_cards_from_profiles(self) -> list[str]:
        if self.configured_accelerator_shapes:
            return list(self.configured_accelerator_shapes)
        cards: list[str] = []
        seen: set[str] = set()
        for profile in (self.queued_compatibility_profiles +
                        self.rejected_compatibility_profiles):
            for card in profile['compatible_accelerators']:
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for card in self.min_replicas_by_accelerator:
            if card.casefold() not in seen:
                cards.append(card)
                seen.add(card.casefold())
        return cards

    def _configured_gpu_count(self, card: str) -> int:
        for configured, count in self.configured_accelerator_shapes.items():
            if configured.casefold() == card.casefold():
                return count
        return 1

    def _cold_paid_card_order(self, configured_cards: list[str]) -> list[str]:
        """Order cold cards by nominal paid cost, independent of availability."""
        return _order_cold_paid_cards(configured_cards,
                                      self._cost_rebalance_spot_placer,
                                      self._configured_gpu_count,
                                      self._location_gpu_shape)

    def _staleness_threshold_seconds(self) -> float:
        """Age beyond which a demand report no longer counts as fresh.

        Three sync intervals: one for the in-flight sync, one for jitter,
        one for a single dropped sync -- beyond that the LB is gone or
        wedged and the gauges describe a fleet state that may no longer
        exist.
        """
        return 3.0 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS

    def has_fresh_demand_report(self) -> bool:
        if (self._in_flight_by_replica_id is None or
                self._report_received_at is None):
            return False
        return (
            time.time() -
            self._report_received_at) <= self._staleness_threshold_seconds()

    def has_recomputed_with_fresh_data(self) -> bool:
        """Whether the target reflects at least one fresh-data recompute.

        The first LB report flips has_fresh_demand_report() on the SYNC
        thread, but target_num_replicas stays at the rebuilt-blind
        min_replicas until the autoscaler thread's next decision tick
        consumes the one-shot snap. Consumers that would act on a blind
        target (the controller's capacity hint) must keep their
        stale-mode floor until this is True, or a routine controller
        restart reports target=min_replicas to the platform's spill
        logic for a tick.
        """
        return not self._snap_target_on_next_recompute

    @property
    def reconcile_generation(self) -> int:
        return self._reconcile_generation

    @property
    def logical_target_state(
        self,
    ) -> (tuple[int, int, int] | tuple[int, int, int, tuple[tuple[
            str, int], ...], tuple[tuple[str, int], ...]] | None):
        """Version, report generation, and target from the last full tick."""
        with self._logical_state_lock:
            return self._last_logical_target_state

    def _fresh_for_tick(self) -> bool:
        """Freshness as snapshotted once at the top of the current tick.

        collect_request_information runs concurrently on the sync
        thread; if the first fresh report landed mid-tick,
        re-evaluating freshness at each consumer would let the
        recompute take the stale path (target still the rebuilt-blind
        minimum) while the later drain/scale-down guards saw fresh and
        proceeded -- marrying a blind target to fresh-mode kills. Falls
        back to a live evaluation when no tick snapshot is active (a
        direct call outside generate_scaling_decisions).
        """
        if self._tick_fresh is not None:
            return self._tick_fresh
        return self.has_fresh_demand_report()

    def fill_demand_sample(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> 'FillDemandSample | None':
        """Demonstrated work for the reserved-fill utilization gate.

        Called from the poller thread, never from the decision tick, so it
        must not mutate decision-owned state: it uses the pure
        _outstanding_work_parts rather than _outstanding_work.

        Returns None whenever the demand report is not fresh. The poller
        publishes this as armed-but-blind (fresh activity_ts, NULL need), so
        the broker freezes for the blind grace before it resumes bounded
        decay; it does not mistake telemetry loss for confirmed idle.
        """
        with self._logical_state_lock:
            if not self.has_fresh_demand_report():
                return None
            if self._in_flight_by_replica_id is None:
                return None
            queue_work, rejected, unknown_floor = (
                self._outstanding_work_parts(replica_infos))
            outstanding = float(
                sum(self._in_flight_by_replica_id.values()) + queue_work +
                rejected + unknown_floor)
            busy = 0
            pre_ready = 0
            pre_ready_statuses = (
                serve_state.ReplicaStatus.PENDING,
                serve_state.ReplicaStatus.PROVISIONING,
                serve_state.ReplicaStatus.STARTING,
            )
            for info in replica_infos:
                if info.is_terminal:
                    continue
                if not self._replica_on_zero_cost_location(info):
                    continue
                if not getattr(info, 'reserved_fill', False):
                    # Demand-placed zero-cost rows are demand-protected and
                    # already exempt from the grant ceiling, so counting
                    # them here would inflate the need by capacity the gate
                    # can never reclaim anyway.
                    continue
                if getattr(info, 'status', None) in pre_ready_statuses:
                    pre_ready += 1
                elif self._replica_is_busy(info):
                    busy += 1
            work_per_replica = float(self.target_concurrency_per_replica)
            if self.replica_unit == 'logical':
                work_per_replica = self._effective_logical_capacity_per_gpu()
            return FillDemandSample(
                outstanding_work=outstanding,
                busy_fill_holdings=busy,
                pre_ready_fill_holdings=pre_ready,
                upscale_pending=self.upscale_counter > 0,
                work_per_replica=work_per_replica,
            )

    def _replica_is_busy(self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether the latest report shows in-flight work on a replica.

        READY and NOT_READY replicas missing from the report count as
        BUSY: for READY the LB may simply not have picked them up yet;
        for NOT_READY the replica WAS serving and blipped a probe -- for
        async fast-ack work the LB's occupancy probe only covers the
        routable set, so a blipped replica's running jobs may be
        unreported, and guessing idle kills them. Both also count busy
        with reported work > 0, which the controller keeps attributable
        (sticky url translation) while the replica is nonterminal.
        Never-served statuses (PENDING/PROVISIONING/STARTING) missing
        from the report count as idle: they cannot carry jobs, and
        treating them busy would starve scale-down of its preferred
        kill-first victims.
        """
        if info.replica_id in self._unknown_in_flight_replica_ids:
            return True
        in_flight = self._in_flight_by_replica_id or {}
        if info.status in (serve_state.ReplicaStatus.READY,
                           serve_state.ReplicaStatus.NOT_READY):
            return in_flight.get(info.replica_id) != 0
        return in_flight.get(info.replica_id, 0) > 0

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        with self._logical_state_lock:
            self._collect_request_information_locked(request_aggregator_info)

    def _collect_request_information_locked(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect timestamps and the latest LB demand report.

        Expected dict (extra keys ignored; all demand keys optional so an
        old LB that only ships timestamps degrades to the signal-gap
        rules):

        {
            'timestamps': [...],
            'in_flight_by_replica_id': {replica_id: int} | None,
            'queue_depth': int | None,
            'rejected_in_window': int | None,
            'rejected_in_recent_window': int | None,
            'unknown_in_flight_replica_ids': [replica_id, ...],
            'observed_slots_by_replica_id': {replica_id: int},
            'unknown_capacity_replica_ids': [replica_id, ...],
            'reconcile_generation': int,
        }
        """
        self.request_timestamps.extend(
            request_aggregator_info.get('timestamps', []))
        current_time = time.time()
        index = bisect.bisect_left(self.request_timestamps,
                                   current_time - self.qps_window_size)
        self.request_timestamps = self.request_timestamps[index:]
        self.compatibility_profiles = [
            profile for profile in self.compatibility_profiles
            if profile['timestamp'] >= current_time -
            constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS
        ]

        in_flight = request_aggregator_info.get('in_flight_by_replica_id')
        if in_flight is None:
            # No usable demand report in this sync (old LB, or a policy
            # that cannot track in-flight). Keep the previous report: it
            # ages out on its own; overwriting it with nothing would
            # discard a still-fresh signal.
            return
        compatibility_complete = (
            bool(self.configured_accelerator_shapes) and
            request_aggregator_info.get('compatibility_demand_complete')
            is True)
        if compatibility_complete:
            self.compatibility_profiles.extend(
                self._parse_compatibility_arrivals(
                    request_aggregator_info.get('compatibility_profiles', [])))
            self.queued_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    request_aggregator_info.get(
                        'queued_requests_by_compatibility', [])))
            self.rejected_compatibility_profiles = (
                self._parse_compatibility_gauge(request_aggregator_info.get(
                    'rejected_requests_by_compatibility', []),
                                                include_recent_count=True))
        self._compatibility_demand_complete = compatibility_complete
        # Normalize keys/values: the controller builds this dict
        # in-process today, but a defensive int() keeps us safe if it is
        # ever rebuilt from a JSON round-trip (string keys).
        self._in_flight_by_replica_id = {
            int(replica_id): int(count)
            for replica_id, count in in_flight.items()
        }
        queue_depth = request_aggregator_info.get('queue_depth')
        self._queue_depth = int(queue_depth) if queue_depth is not None else 0

        def _priority_counts(value: Any) -> dict[int, int] | None:
            if not isinstance(value, dict):
                return None
            return {
                int(priority): int(count)
                for priority, count in value.items()
                if (str(priority).isdigit() and 0 <= int(priority) <= 100 and
                    isinstance(count, int) and not isinstance(count, bool) and
                    count >= 0)
            }

        self._queue_depth_by_priority = _priority_counts(
            request_aggregator_info.get('queue_depth_by_priority'))
        rejected = request_aggregator_info.get('rejected_in_window')
        self._rejected_in_window = int(rejected) if rejected is not None else 0
        recent_rejected = request_aggregator_info.get(
            'rejected_in_recent_window')
        self._rejected_in_recent_window = (
            int(recent_rejected) if recent_rejected is not None else None)
        self._rejected_in_window_by_priority = _priority_counts(
            request_aggregator_info.get('rejected_in_window_by_priority'))
        self._rejected_in_recent_window_by_priority = _priority_counts(
            request_aggregator_info.get(
                'rejected_in_recent_window_by_priority'))

        def _optional_count(field: str) -> int | None:
            value = request_aggregator_info.get(field)
            if (not isinstance(value, int) or isinstance(value, bool) or
                    value < 0):
                return None
            return value

        self._unique_job_arrivals_60s = _optional_count(
            'unique_job_arrivals_60s')
        self._unique_job_arrivals_300s = _optional_count(
            'unique_job_arrivals_300s')
        self._headerless_arrivals_60s = _optional_count(
            'headerless_arrivals_60s')
        self._headerless_arrivals_300s = _optional_count(
            'headerless_arrivals_300s')
        self._offered_arrival_tracking_saturated = (
            request_aggregator_info.get('offered_arrival_tracking_saturated')
            is True)
        self._ingest_prediction_time_history(
            request_aggregator_info.get('prediction_time_history'))
        report_is_floored = request_aggregator_info.get(
            'pressure_report_is_floored') is True
        arrival_60 = self._offered_arrival_count(60)
        pressure_sample = (self._queue_depth, self._rejected_in_recent_window or
                           0, arrival_60)
        if not report_is_floored:
            if self._pressure_baseline is None:
                self._pressure_streak = 0
            else:
                labels = ('queue_depth', 'recent_rejections',
                          'offered_arrivals_60s')
                reasons = tuple(label for label, current, previous in zip(
                    labels, pressure_sample, self._pressure_baseline)
                                if current > previous)
                if not reasons:
                    # A queue pinned flat at its cap is saturation, not
                    # relief; requiring strictly increasing samples disarms
                    # adaptive scale-up exactly when overload plateaus.
                    # Only a draining queue resets the streak. The plateau
                    # floor keeps a benign flat trickle queue from latching
                    # pressure indefinitely, and stable rejection
                    # populations deliberately stay non-latching (bounded
                    # downscale vetoes) -- cap and timeout rejections always
                    # ride on a deep queue, which this clause covers.
                    plateau_floor = max(1, self.scale_up_rate_min_replicas or 1)
                    if (pressure_sample[0] >= plateau_floor and
                            pressure_sample[0] >= self._pressure_baseline[0]):
                        reasons = ('queue_plateau',)
                if reasons:
                    self._pressure_latched = True
                    self._pressure_reasons = reasons
                    self._pressure_streak += 1
                    if (self.adaptive_scale_up is not None and
                            self._pressure_streak
                            >= self.adaptive_scale_up['pressure_observations']):
                        self._adaptive_until = (
                            time.monotonic() +
                            self.adaptive_scale_up['hold_seconds'])
                else:
                    self._pressure_streak = 0
            self._pressure_baseline = pressure_sample
        else:
            # A maximum-merged handoff gauge is not an authoritative
            # observation of new offered demand. It also breaks a run of
            # consecutive pressure observations, while leaving an already
            # active adaptive hold untouched until its normal expiry.
            self._pressure_streak = 0
        self._unknown_in_flight_replica_ids = {
            int(replica_id) for replica_id in (request_aggregator_info.get(
                'unknown_in_flight_replica_ids', []) or [])
        }
        self._observed_slots_by_replica_id = {
            int(replica_id): max(0, int(slots))
            for replica_id, slots in request_aggregator_info.get(
                'observed_slots_by_replica_id', {}).items()
        }
        self._unknown_capacity_replica_ids = {
            int(replica_id) for replica_id in request_aggregator_info.get(
                'unknown_capacity_replica_ids', [])
        }
        degraded_capacity_ids = self._unknown_capacity_replica_ids | {
            replica_id
            for replica_id, slots in self._observed_slots_by_replica_id.items()
            if slots == 0
        }
        for replica_id in degraded_capacity_ids:
            self._degraded_capacity_since_by_replica_id.setdefault(
                replica_id, current_time)
        self._degraded_capacity_since_by_replica_id = {
            replica_id: since
            for replica_id, since in
            self._degraded_capacity_since_by_replica_id.items()
            if replica_id in degraded_capacity_ids
        }
        self._reconcile_generation = int(
            request_aggregator_info.get('reconcile_generation',
                                        self._reconcile_generation + 1))
        self._report_received_at = current_time
        self._launch_priority_report_received_at = current_time
        logger.info(f'Concurrency report: in_flight_total='
                    f'{sum(self._in_flight_by_replica_id.values())}, '
                    f'queue_depth={self._queue_depth}, '
                    f'rejected_in_window={self._rejected_in_window}, '
                    f'rejected_in_recent_window='
                    f'{self._rejected_in_recent_window}, '
                    f'unknown_replicas='
                    f'{len(self._unknown_in_flight_replica_ids)}, '
                    f'requests in the last {self.qps_window_size}s: '
                    f'{len(self.request_timestamps)}')

    @staticmethod
    def _parse_compatibility_arrivals(
            raw_profiles: Any) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        if not isinstance(raw_profiles, list):
            return profiles
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                continue
            timestamp = raw.get('timestamp')
            priority = raw.get('priority')
            accelerators = raw.get('compatible_accelerators')
            count = raw.get('count', 1)
            if (not isinstance(timestamp,
                               (int, float)) or isinstance(timestamp, bool) or
                    not math.isfinite(timestamp) or timestamp < 0 or
                    not isinstance(priority, int) or
                    isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not all(
                        isinstance(card, str) and card for card in accelerators)
                    or not isinstance(count, int) or isinstance(count, bool) or
                    count < 1):
                continue
            profiles.append({
                'timestamp': float(timestamp),
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        return profiles

    @staticmethod
    def _parse_compatibility_gauge(
        raw_profiles: Any,
        *,
        include_recent_count: bool = False,
    ) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        if not isinstance(raw_profiles, list):
            return profiles
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                continue
            priority = raw.get('priority')
            accelerators = raw.get('compatible_accelerators')
            count = raw.get('count', 1)
            recent_count = raw.get('recent_count')
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not all(
                        isinstance(card, str) and card for card in accelerators)
                    or not isinstance(count, int) or isinstance(count, bool) or
                    count < 1):
                continue
            profile: dict[str, Any] = {
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            }
            if include_recent_count:
                if (not isinstance(recent_count, int) or
                        isinstance(recent_count, bool) or recent_count < 0 or
                        recent_count > count):
                    continue
                profile['recent_count'] = recent_count
            profiles.append(profile)
        return profiles

    def _get_knob_for_version(self, version: int) -> float:
        """The per-GPU knob a given service version was launched under.

        Unknown versions (the autoscaler was rebuilt after the update
        that created them, e.g. a controller restart mid-rolling-update)
        rehydrate from the durable per-version spec so old-version
        replicas keep their real capacity. Falls back to the latest knob
        when the version's spec is unavailable; misses are not memoized
        across ticks so a transient DB error can heal on the next tick.
        """
        cached = self._knob_by_version.get(version)
        if cached is not None:
            return cached
        unavailable_versions = self._knob_unavailable_versions_for_tick
        if (unavailable_versions is not None and
                version in unavailable_versions):
            return self.target_concurrency_per_replica
        knob = None
        try:
            spec = serve_state.get_spec(self._service_name, version)
            if spec is not None:
                knob = getattr(spec, 'target_concurrency_per_replica', None)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to load spec for version '
                           f'{version}: {common_utils.format_exception(e)}')
        if knob is None:
            if unavailable_versions is not None:
                unavailable_versions.add(version)
            return self.target_concurrency_per_replica
        self._knob_by_version[version] = float(knob)
        return float(knob)

    def _replica_capacity(self, info: 'replica_managers.ReplicaInfo') -> float:
        """A replica's capacity in the autoscaler's target units.

        Logical targets are GPU slots, so a physical backend contributes its
        immutable planned slot width. Physical-backend targets are replica
        counts, so each replica contributes knob x gpu_count concurrency.
        The knob is resolved for the replica's OWN version after updates.
        """
        if self.replica_unit == 'logical':
            return float(getattr(info, 'planned_capacity', 1))
        _, gpu_count = self._get_gpu_shape_from_replica_info(info)
        return self._get_knob_for_version(info.version) * gpu_count

    def _fill_capacity_units(self, info: 'replica_managers.ReplicaInfo') -> int:
        if self.replica_unit == 'logical':
            return max(1, int(self._replica_capacity(info)))
        return super()._fill_capacity_units(info)

    def _ready_capacity(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Observed ready logical slots, or zero when not proven fresh."""
        if not info.is_ready:
            return 0
        observed = self._observed_slots_by_replica_id.get(info.replica_id)
        if (observed is None or
                info.replica_id in self._unknown_capacity_replica_ids):
            return 0
        return min(int(self._replica_capacity(info)), observed)

    def _committed_capacity(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Pinned capacity used to suppress duplicate logical launches."""
        if _replica_is_retiring_card_supply(info):
            return 0
        planned = int(self._replica_capacity(info))
        observed = self._observed_slots_by_replica_id.get(info.replica_id)
        degraded = (info.replica_id in self._unknown_capacity_replica_ids or
                    (info.is_ready and observed == 0))
        if degraded:
            degraded_since = self._degraded_capacity_since_by_replica_id.get(
                info.replica_id)
            replacement_age = (time.time() - degraded_since
                               if degraded_since is not None else 0)
            replacement_timeout = (
                constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS)
            if (self.replica_unit == 'logical' and
                    degraded_since is not None and
                    replacement_age >= replacement_timeout and
                    getattr(info, 'unknown_capacity_replacement',
                            False) is not True):
                return 0
            return planned
        if info.is_ready and observed is not None:
            return min(planned, observed)
        return planned

    def get_ready_replica_capacity(self,
                                   info: 'replica_managers.ReplicaInfo') -> int:
        if self.replica_unit == 'logical':
            # Public status reports materialized GPU inventory. Occupancy
            # freshness remains a separate safety signal: internal scale-down,
            # replacement, and retirement paths call `_ready_capacity()`
            # directly and continue to fail closed on unknown observations.
            return (max(1, int(self._replica_capacity(info)))
                    if info.is_ready else 0)
        return super().get_ready_replica_capacity(info)

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        return self._replica_capacity(info)

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        _, gpu_count = self._location_gpu_shape(location)
        if self.replica_unit == 'logical':
            return float(gpu_count)
        return self.target_concurrency_per_replica * gpu_count

    def _latest_capacities(
            self,
            replica_infos: list['replica_managers.ReplicaInfo']) -> list[float]:
        """Capacities of live latest-version replicas, largest first."""
        capacities = []
        for info in replica_infos:
            if info.is_terminal or info.version != self.latest_version:
                continue
            capacity = self._replica_capacity(info)
            if capacity > 0:
                capacities.append(capacity)
        capacities.sort(reverse=True)
        return capacities

    def _effective_logical_capacity_per_gpu(self) -> float:
        return (self.target_concurrency_per_replica *
                self.target_utilization_percentage / 100.0)

    def _clip_concurrency_demand_target(self, target: int) -> int:
        """Clip demand, allowing a logical wave to approach floors."""
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            return max(0, min(self.max_replicas, target))
        return self._clip_target_num_replicas(target)

    def _priority_timeout(self, priority: int) -> float | None:
        timeout = self._queue_timeout_seconds
        for min_priority, threshold_timeout in self._queue_timeout_thresholds:
            if priority < min_priority:
                break
            timeout = threshold_timeout
        return timeout

    def _queue_work(self) -> float:
        if (self.replica_unit != 'logical' or
                self.effective_request_duration_seconds is None or
                not self._queue_timeout_thresholds or
                self._queue_depth_by_priority is None or
                sum(self._queue_depth_by_priority.values())
                < self._queue_depth):
            # A mixed-version HA floor can carry aggregate demand from an old
            # active beside an empty or partial priority map from the new
            # active. Never let the optional map erase that proven queue.
            return float(self._queue_depth)
        # A queued request must be dispatched before its priority timeout,
        # and newly authorized capacity only starts serving after the
        # provisioning lead time. Sizing against the full timeout budget
        # plans delivery exactly at the deadline assuming instant capacity;
        # subtracting the lead sizes against the budget that actually
        # remains once capacity can exist.
        lead = self.effective_provision_lead_seconds
        duration = self.effective_request_duration_seconds
        work = 0.0
        for priority, count in self._queue_depth_by_priority.items():
            timeout = self._priority_timeout(priority)
            weight = 1.0
            if timeout is not None:
                weight = min(1.0, duration / max(duration, timeout - lead))
            work += count * weight
        return work

    def _offered_arrival_count(self, window_seconds: int) -> int:
        if self._offered_arrival_tracking_saturated:
            return constants.LB_OFFERED_ARRIVAL_CAP
        if window_seconds == constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS:
            values = (self._unique_job_arrivals_60s,
                      self._headerless_arrivals_60s)
        else:
            values = (self._unique_job_arrivals_300s,
                      self._headerless_arrivals_300s)
        if any(value is None for value in values):
            return 0
        return sum(typing.cast(int, value) for value in values)

    def _arrival_work(self) -> float:
        duration = self.effective_request_duration_seconds
        if duration is None:
            return 0.0
        if (self._unique_job_arrivals_60s is None or
                self._unique_job_arrivals_300s is None or
                self._headerless_arrivals_60s is None or
                self._headerless_arrivals_300s is None):
            return (len(self.request_timestamps) * duration /
                    self.qps_window_size)
        recent = (self._offered_arrival_count(60) * duration /
                  constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        retained = (1.15 * self._offered_arrival_count(300) * duration /
                    constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
        return max(recent, retained)

    def _arrival_compatibility_work(
        self,
        arrival_work: float,
        allocator_attributed_work: float,
    ) -> list[tuple[int, tuple[str, ...], float]]:
        """Shape only the offered-arrival work not already attributed.

        Offered-arrival counters are the deduplicated magnitude authority.
        Accepted-arrival profiles and the current queued gauge are used only
        as compatibility/priority distribution evidence, so retries cannot
        inflate total work here. The queued gauge covers requests that cannot
        be admitted until a compatible card exists. Both sources stay in
        request-count units because they shape the same offered-arrival counter:
        every request is recorded there before admission.
        """
        arrival_gap = max(0.0, arrival_work - allocator_attributed_work)
        if arrival_gap <= 0:
            return []

        duration = self.effective_request_duration_seconds
        offered_counts_complete = (duration is not None and
                                   self._unique_job_arrivals_60s is not None and
                                   self._unique_job_arrivals_300s is not None
                                   and
                                   self._headerless_arrivals_60s is not None and
                                   self._headerless_arrivals_300s is not None)
        window_seconds = self.qps_window_size
        if offered_counts_complete:
            assert duration is not None
            recent_work = (self._offered_arrival_count(60) * duration /
                           constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
            retained_work = (1.15 * self._offered_arrival_count(300) *
                             duration /
                             constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
            if retained_work > recent_work:
                window_seconds = constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS

        cutoff = time.time() - window_seconds
        evidence = [
            (int(profile['priority']),
             tuple(profile['compatible_accelerators']), float(profile['count']))
            for profile in self.compatibility_profiles
            if profile['timestamp'] >= cutoff and float(profile['count']) > 0
        ]
        evidence.extend(
            (int(profile['priority']),
             tuple(profile['compatible_accelerators']), float(profile['count']))
            for profile in self.queued_compatibility_profiles
            if float(profile['count']) > 0)
        evidence_total = sum(work for _, _, work in evidence)
        if evidence_total <= 0:
            # Compatibility-unknown work may hold the aggregate target, but
            # it must never authorize a guessed exact-card launch.
            return []
        scale = arrival_gap / evidence_total
        return [(priority, compatible, work * scale)
                for priority, compatible, work in evidence]

    def _adaptive_sample_is_fresh(self, observed_at: float | None) -> bool:
        if observed_at is None:
            return False
        age = time.time() - observed_at
        # Tolerate a small negative age from clock adjustment rather than
        # discarding an otherwise usable estimate.
        return -60.0 <= age <= constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS

    @property
    def effective_request_duration_seconds(self) -> float | None:
        """Measured request duration, falling back to configuration.

        Configuration is a hand-set estimate that silently mis-sizes every
        target it feeds once the workload drifts. A measured duration backed
        by enough fresh completions is strictly better evidence, so it wins
        while it holds; otherwise the configured value stands.
        """
        if (self.adaptive_demand_estimation and
                self._measured_duration_seconds is not None and
                self._measured_duration_samples
                >= constants.AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES and
                self._adaptive_sample_is_fresh(self._measured_duration_at)):
            return self._measured_duration_seconds
        return self.expected_request_duration_seconds

    @property
    def configured_provision_lead_seconds(self) -> float:
        """Resolve the configured seed, including the 'auto' sentinel.

        'auto' (the default) means the service has not declared a lead and
        wants one measured. Until it has, assume the order of magnitude
        every supported cloud actually takes to provision a GPU replica:
        assuming zero would size a young service's first bursts as if
        capacity were instant.
        """
        configured = self.initial_provision_lead_time_seconds
        if isinstance(configured,
                      (int, float)) and not isinstance(configured, bool):
            return float(configured)
        return constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS

    @property
    def effective_provision_lead_seconds(self) -> float:
        """Observed launch-to-ready quantile, falling back to the seed."""
        if (self.adaptive_demand_estimation and
                len(self._provision_lead_samples)
                >= constants.AUTOSCALER_ADAPTIVE_LEAD_MIN_SAMPLES and
                self._adaptive_sample_is_fresh(self._provision_lead_at)):
            ordered = sorted(self._provision_lead_samples)
            index = min(
                len(ordered) - 1,
                int(constants.AUTOSCALER_ADAPTIVE_LEAD_QUANTILE * len(ordered)))
            return ordered[index]
        return self.configured_provision_lead_seconds

    def _ingest_prediction_time_history(self,
                                        prediction_time_history: Any) -> None:
        """Fold newly completed request durations into the EMA.

        The load balancer reports per-minute cumulative histograms and keeps
        re-reporting a bucket until the controller durably accepts it, so
        only the positive delta against what this estimator already folded
        in may contribute.
        """
        if not isinstance(prediction_time_history, dict):
            return
        if (prediction_time_history.get('histogram_version')
                != constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION):
            # Bucket arrays are interpreted by index; a different version
            # is not comparable and is dropped rather than guessed.
            return
        buckets = prediction_time_history.get('buckets')
        if not isinstance(buckets, list):
            return
        bounds = constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS
        total_new = 0
        weighted_new = 0.0
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            bucket_start = bucket.get('bucket_start')
            outcome_counts = bucket.get('outcome_counts')
            if (not isinstance(bucket_start, int) or
                    isinstance(bucket_start, bool) or
                    not isinstance(outcome_counts, dict)):
                continue
            # Only successful requests describe how long serving a request
            # occupies a slot. A fast failure would drag the estimate down
            # and undersize the fleet.
            counts = outcome_counts.get('succeeded')
            if not isinstance(counts, list):
                continue
            seen = self._prediction_counts_seen.setdefault(
                bucket_start, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
            for index, count in enumerate(counts):
                if index >= len(seen):
                    break
                if (not isinstance(count, int) or isinstance(count, bool) or
                        count <= seen[index]):
                    continue
                delta = count - seen[index]
                seen[index] = count
                representative = _prediction_bucket_representative(
                    index, bounds)
                total_new += delta
                weighted_new += delta * representative
        if total_new <= 0:
            return
        self._prune_prediction_counts_seen()
        sample = weighted_new / total_new
        alpha = constants.AUTOSCALER_ADAPTIVE_DURATION_EMA_ALPHA
        if self._measured_duration_seconds is None:
            self._measured_duration_seconds = sample
        else:
            self._measured_duration_seconds = (
                (1.0 - alpha) * self._measured_duration_seconds +
                alpha * sample)
        self._measured_duration_samples += total_new
        self._measured_duration_at = time.time()

    def _prune_prediction_counts_seen(self) -> None:
        """Bound the per-bucket dedup ledger to the freshness window."""
        cutoff = (time.time() -
                  constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS)
        for bucket_start in list(self._prediction_counts_seen):
            if bucket_start < cutoff:
                del self._prediction_counts_seen[bucket_start]

    def _observe_provision_leads(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Sample launch-to-ready for replicas that just became ready."""
        live_ids = set()
        for info in replica_infos:
            replica_id = info.replica_id
            live_ids.add(replica_id)
            if replica_id in self._provision_lead_seen_replica_ids:
                continue
            created_at = getattr(info, 'created_at', None)
            ready_at = getattr(getattr(info, 'status_property', None),
                               'first_ready_time', None)
            if (not isinstance(created_at,
                               (int, float)) or isinstance(created_at, bool) or
                    not isinstance(ready_at,
                                   (int, float)) or isinstance(ready_at, bool)):
                continue
            lead = ready_at - created_at
            if lead <= 0:
                # -1 is the never-ready sentinel; a non-positive span is
                # not a launch measurement.
                continue
            self._provision_lead_seen_replica_ids.add(replica_id)
            self._provision_lead_samples.append(float(lead))
            del self._provision_lead_samples[:-constants.
                                             AUTOSCALER_ADAPTIVE_LEAD_SAMPLE_CAP]
            self._provision_lead_at = time.time()
        # Terminated rows can never be sampled again, so the ledger tracks
        # the live fleet rather than growing for the service's lifetime.
        self._provision_lead_seen_replica_ids &= live_ids

    def _adaptive_scale_up_active(self) -> bool:
        return (self.adaptive_scale_up is not None and
                self._adaptive_until is not None and
                time.monotonic() < self._adaptive_until)

    def _rejected_work(self) -> float:
        """Convert the retained rejection population to concurrent work."""
        duration = self.effective_request_duration_seconds
        if self.replica_unit != 'logical' or duration is None:
            return float(self._rejected_in_window)
        retained_work = (self._rejected_in_window * duration /
                         constants.LB_REJECT_WINDOW_SECONDS)
        if self._rejected_in_recent_window is None:
            return retained_work
        recent_work = (self._rejected_in_recent_window * duration /
                       self.qps_window_size)
        return max(retained_work, recent_work)

    def _latest_committed_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Latest-version planned slots, from every launch origin."""
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if
            (not info.is_terminal and info.version == self.latest_version and
             getattr(info.status_property, 'is_scale_down', False) is not True))

    def _latest_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Latest-version planned slots whose launch origin was demand.

        ``reserved_fill`` is launch-origin attribution, not placement-cost
        provenance. A demand launch remains demand-owned when it lands on a
        zero-cost location. Legacy rows missing the additive flag default to
        demand-owned, which is the conservative compatibility direction.
        """
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                getattr(info.status_property, 'is_scale_down', False)
                is not True and not getattr(info, 'reserved_fill', False)))

    def _total_ready_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Ready demand-owned slots across every active rollout version."""
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if (info.is_ready and not info.is_terminal and getattr(
                info.status_property, 'is_scale_down', False) is not True and
                not getattr(info, 'reserved_fill', False)))

    def _nonterminal_committed_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Planned slots across every non-retiring version and launch origin.

        This is the base for the aggregate target CEILING, not for the wave
        rate. During a rolling update the serving fleet can be entirely
        old-version, and a latest-only ceiling pins the adopted target
        below the fleet that is already saturated: growing to meet demand
        becomes gated behind version replacement progress (observed live at
        raw target 1000, adopted 50, fleet 156). Replacement pacing itself
        stays on the latest-version rate base.
        """
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if (not info.is_terminal and getattr(
                info.status_property, 'is_scale_down', False) is not True))

    def _limit_logical_scale_up(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Bound one demand-driven target increase to a configured wave."""
        budget = self._logical_scale_up_budget(replica_infos)
        if budget is None:
            return raw_target
        if budget == 0:
            return self.target_num_replicas
        committed = self._nonterminal_committed_logical_capacity(replica_infos)
        return max(self.target_num_replicas, min(raw_target,
                                                 committed + budget))

    def _logical_scale_up_budget(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int | None:
        """Return new or retained slot authority for this reconciliation."""
        self._logical_actuation_wave_is_new = False
        if (self.replica_unit != 'logical' or
                self.max_scale_up_rate_percentage is None):
            return None
        assert self.scale_up_rate_min_replicas is not None
        assert self.scale_up_rate_period_seconds is not None
        now = time.time()
        if (self._last_scale_up_wave_at is not None and
                now - self._last_scale_up_wave_at
                < self.scale_up_rate_period_seconds):
            if self._logical_scale_up_wave_ceiling is None:
                # Dynamic handoff deliberately carries the timer but not its
                # version-specific ceiling. Preserve a fail-closed cooldown
                # for the remainder of that window.
                return 0
            committed = self._nonterminal_committed_logical_capacity(
                replica_infos)
            return max(0, self._logical_scale_up_wave_ceiling - committed)
        # The wave RATE stays on latest-version capacity: it also paces
        # rollout replacement launches, and ramping a new version from its
        # own committed capacity is a deliberate contract. Only the target
        # ceiling below counts the whole fleet.
        committed = self._latest_committed_logical_capacity(replica_infos)
        rate_percentage = self.max_scale_up_rate_percentage
        min_replicas = self.scale_up_rate_min_replicas
        if self._adaptive_scale_up_active():
            assert self.adaptive_scale_up is not None
            rate_percentage = self.adaptive_scale_up[
                'max_scale_up_rate_percentage']
            min_replicas = self.adaptive_scale_up['scale_up_rate_min_replicas']
        assert rate_percentage is not None
        assert min_replicas is not None
        self._logical_actuation_wave_is_new = True
        return max(min_replicas, math.ceil(committed * rate_percentage / 100.0))

    def _record_logical_scale_up_wave(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        launch_budget: int | None,
    ) -> None:
        """Open a new wave without burning retained cooldown authority.

        The ceiling base is derived here rather than accepted from callers:
        the retained-cooldown branch of _logical_scale_up_budget spends this
        ceiling against the same all-version base, and a caller passing a
        latest-version base would leave the ceiling below that subtrahend,
        silently zeroing retained authority for the rest of the cooldown.
        """
        if (launch_budget is None or launch_budget <= 0 or
                self._logical_actuation_wave_started):
            return
        if self._logical_actuation_wave_is_new:
            committed = self._nonterminal_committed_logical_capacity(
                replica_infos)
            self._last_scale_up_wave_at = time.time()
            self._logical_scale_up_wave_ceiling = committed + launch_budget
        self._logical_actuation_wave_started = True

    def _adopt_scale_up_target(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> None:
        old_target = self.target_num_replicas
        committed = (self._nonterminal_committed_logical_capacity(replica_infos)
                     if self.replica_unit == 'logical' else 0)
        self.target_num_replicas = self._limit_logical_scale_up(
            raw_target, replica_infos)
        # Only an increase that requires capacity beyond what is already
        # committed consumes the wave timer. Raising a recovered target inside
        # an already-live fleet does not delay the next real launch wave.
        if (self.max_scale_up_rate_percentage is not None and
                self.target_num_replicas > old_target and
                self.target_num_replicas > committed):
            if self.replica_unit == 'logical':
                launch_budget = self._logical_actuation_wave_budget
                if launch_budget is None:
                    launch_budget = self.target_num_replicas - committed
                self._record_logical_scale_up_wave(replica_infos, launch_budget)
            else:
                self._last_scale_up_wave_at = time.time()
        if self.target_num_replicas > old_target:
            self._pending_retention_floor = None
            self._pending_capacity_at_adoption = 0
            self._pending_budget_spent = 0

    def _limit_logical_scale_down(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        if self.replica_unit != 'logical':
            return raw_target
        committed = self._latest_demand_owned_logical_capacity(replica_infos)
        allowance = max(
            1,
            math.ceil(committed * self.max_scale_down_rate_percentage / 100.0))
        self._last_scale_down_allowance = allowance
        return max(raw_target, committed - allowance)

    def _provisioning_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        return sum(
            self._committed_capacity(info)
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status in provisioning_statuses and getattr(
                    info.status_property, 'is_scale_down', False) is not True))

    def _provisioning_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Demand-owned subset of provisioning logical capacity."""
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        return sum(
            self._committed_capacity(info)
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status in provisioning_statuses and
                getattr(info.status_property, 'is_scale_down', False)
                is not True and not getattr(info, 'reserved_fill', False)))

    def _adopt_scale_down_target(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> None:
        if self.replica_unit != 'logical':
            self.target_num_replicas = raw_target
            return
        self.target_num_replicas = self._limit_logical_scale_down(
            raw_target, replica_infos)
        provisioning = self._provisioning_demand_owned_logical_capacity(
            replica_infos)
        allowance = (max(
            1,
            math.ceil(provisioning * self.max_scale_down_rate_percentage /
                      100.0)) if provisioning > 0 else 0)
        self._last_pending_allowance = allowance
        self._pending_capacity_at_adoption = provisioning
        self._pending_retention_floor = max(0, provisioning - allowance)
        self._pending_budget_spent = 0

    def _reset_downscale_hysteresis(self) -> None:
        self.downscale_counter = 0
        self._downscale_started_at = None

    def _downscale_hysteresis_elapsed(self) -> bool:
        """Whether this lower-target observation completes its delay.

        Logical concurrency policies use elapsed monotonic time. Other
        concurrency modes retain the legacy decision-count behavior.
        """
        self.downscale_counter += 1
        if self.replica_unit != 'logical':
            return self.downscale_counter >= self.scale_down_threshold
        now = time.monotonic()
        if self._downscale_started_at is None:
            # Preserve the established one-tick default: the first lower
            # observation represents the nominal decision interval that just
            # elapsed. Further progress is real monotonic time, never loop
            # counts, so slow large-fleet ticks cannot stretch the duration.
            initial_credit = min(
                self.downscale_delay_seconds,
                float(constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS))
            self._downscale_started_at = now - initial_credit
            # The current raw target already incorporates every report seen
            # before this quiet interval. Only later positive deltas may veto
            # its acceptance.
            self._pressure_latched = False
            self._pressure_reasons = ()
        return (now - self._downscale_started_at
                >= self.downscale_delay_seconds)

    def _downscale_elapsed_seconds(self) -> float:
        if self._downscale_started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._downscale_started_at)

    def _consume_downscale_pressure_veto(self) -> bool:
        if not self._pressure_latched:
            self._downscale_veto_reason = None
            self._downscale_veto_streak = 0
            return False
        if self._downscale_veto_streak >= _MAX_CONSECUTIVE_DOWNSCALE_VETOES:
            # The latch is magnitude-blind: under trickle traffic a tiny
            # positive delta re-arms it nearly every decision tick, and an
            # unbounded veto would defer downscale forever.
            # After the cap, let the downscale proceed; a genuine burst
            # raises the raw target and exits the downscale episode via
            # the upscale branch anyway.
            self._downscale_veto_reason = None
            self._pressure_latched = False
            self._pressure_reasons = ()
            self._downscale_veto_streak = 0
            return False
        self._downscale_veto_streak += 1
        self._downscale_veto_reason = ','.join(self._pressure_reasons)[:128]
        self._pressure_latched = False
        self._pressure_reasons = ()
        return True

    def _outstanding_work(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'] | None = None,
    ) -> float:
        """Outstanding jobs per the latest report (gauges, one snapshot).

        A job can transiently appear in both queue_depth (one sync) and
        rejected_in_window (a later sync) -- at most a 2x count per job,
        absorbed by hysteresis (accepted in the plan).
        """
        queue_work, rejected, unknown_floor = self._outstanding_work_parts(
            replica_infos)
        # These two are observability fields owned by the decision tick (see
        # info()). The pure variant assigns nothing, so the reserved-fill
        # poller thread can sample outstanding work without clobbering them.
        self._weighted_queue_work = queue_work
        self._rejected_concurrency = rejected
        assert self._in_flight_by_replica_id is not None
        return float(
            sum(self._in_flight_by_replica_id.values()) + queue_work +
            rejected + unknown_floor)

    def _outstanding_work_parts(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'] | None = None,
    ) -> tuple[float, float, float]:
        """(queue work, rejected work, unknown-occupancy floor). Pure."""
        assert self._in_flight_by_replica_id is not None
        unknown_floor = 0.0
        if self._unknown_in_flight_replica_ids:
            infos_by_id = {
                info.replica_id: info
                for info in (replica_infos or [])
                if not info.is_terminal
            }
            default_capacity = self.target_concurrency_per_replica
            if self.replica_unit == 'logical':
                default_capacity = self._effective_logical_capacity_per_gpu()
            fallback_capacity = max((self._unknown_occupancy_work(info)
                                     for info in infos_by_id.values()),
                                    default=default_capacity)
            original_unknown_floor = 0.0
            replacement_unknown_floor = 0.0
            for replica_id in self._unknown_in_flight_replica_ids:
                info = infos_by_id.get(replica_id)
                if info is None:
                    # Defensive fallback for transient list/cache skew: use
                    # the best live capacity rather than silently shrinking a
                    # potentially multi-GPU unknown replica to one GPU.
                    original_unknown_floor += fallback_capacity
                else:
                    capacity = self._unknown_occupancy_work(info)
                    if getattr(info, 'unknown_capacity_replacement',
                               False) is True:
                        replacement_unknown_floor += capacity
                    else:
                        original_unknown_floor += capacity
            # A degraded replacement wave overlaps uncertain originals. If
            # both sides are unobservable, counting their floors additively
            # creates recursive phantom demand. The larger side is the safe
            # possible-work floor; when either side recovers, the other still
            # protects its own capacity.
            # Repeated fractional utilization capacities (for example ten
            # 0.9-slot floors) can accumulate a positive binary-float tail.
            # Normalize only this modeled floor so ceil(work / capacity) does
            # not manufacture a slot from arithmetic noise.
            unknown_floor = round(
                max(original_unknown_floor, replacement_unknown_floor), 12)
        return (self._queue_work(), self._rejected_work(), unknown_floor)

    def _unknown_occupancy_work(self,
                                info: 'replica_managers.ReplicaInfo') -> float:
        """Work floor that preserves an occupancy-unknown replica.

        Unknown occupancy is a retention signal, not observed demand. In
        logical mode, express it at the configured utilization-adjusted work
        capacity so dividing by that same capacity preserves exactly the
        materialized slots. Charging the raw saturation capacity here would
        apply utilization headroom a second time and turn a controller/LB
        handoff that marks the whole fleet unknown into a phantom scale-up.
        Physical-backend mode retains its existing raw-capacity semantics.
        """
        capacity = self._replica_capacity(info)
        if self.replica_unit == 'logical':
            capacity *= self._effective_logical_capacity_per_gpu()
        return capacity

    def _fixed_concurrency_work_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, float], float, bool]:
        """Split fixed work into card-retention work and flexible overflow.

        Running and occupancy-unknown work cannot be moved off the replica
        that already owns it, so it protects materialized capacity on that
        replica's exact card. Work above the card's materialized serving
        capacity is already being served through temporary oversubscription;
        treating that excess as an exact-card capacity deficit would cold
        start the same card even when a cheaper compatible card can absorb new
        work. Return that excess separately so the allocator can preserve the
        aggregate work as a flexible compatibility profile.
        """
        assert self._in_flight_by_replica_id is not None
        infos_by_id = {
            info.replica_id: info
            for info in replica_infos
            if not info.is_terminal
        }
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        fixed: dict[str, float] = {}
        complete = True

        def add(replica_id: int, work: float, destination: dict[str,
                                                                float]) -> None:
            nonlocal complete
            info = infos_by_id.get(replica_id)
            if info is None:
                complete = False
                return
            if _replica_is_retiring_card_supply(info):
                # The row remains in aggregate outstanding work until its
                # bounded drain completes, but replacing that work on the
                # retiring row's exact card would turn a graceful retirement
                # into a cold same-card relaunch.
                return
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = configured_by_name.get(raw_card.casefold())
            if card is None:
                complete = False
                return
            destination[card] = destination.get(card, 0.0) + max(0.0, work)

        for replica_id, count in self._in_flight_by_replica_id.items():
            add(replica_id, float(count), fixed)

        original_unknown: dict[str, float] = {}
        replacement_unknown: dict[str, float] = {}
        for replica_id in self._unknown_in_flight_replica_ids:
            info = infos_by_id.get(replica_id)
            if info is None:
                complete = False
                continue
            destination = (replacement_unknown if getattr(
                info, 'unknown_capacity_replacement', False) is True else
                           original_unknown)
            add(replica_id, self._unknown_occupancy_work(info), destination)
        # Mirror _outstanding_work(): an uncertain bounded replacement wave
        # overlaps its original, so only the larger side contributes.
        unknown = (replacement_unknown if sum(replacement_unknown.values())
                   > sum(original_unknown.values()) else original_unknown)
        for card, work in unknown.items():
            fixed[card] = fixed.get(card, 0.0) + work

        materialized_work_capacity = {
            card: 0.0 for card in configured_by_name.values()
        }
        materialized_statuses = {
            serve_state.ReplicaStatus.READY,
            serve_state.ReplicaStatus.NOT_READY,
        }
        for info in replica_infos:
            if (info.is_terminal or info.status not in materialized_statuses or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            materialized_card = configured_by_name.get(raw_card.casefold())
            if materialized_card is None:
                continue
            capacity = self._replica_capacity(info)
            if self.replica_unit == 'logical':
                capacity *= self._effective_logical_capacity_per_gpu()
            materialized_work_capacity[materialized_card] += max(
                0.0, float(capacity))

        flexible_overflow = 0.0
        capped_fixed: dict[str, float] = {}
        for card, work in fixed.items():
            retained = min(max(0.0, work),
                           materialized_work_capacity.get(card, 0.0))
            if retained > 0:
                capped_fixed[card] = retained
            flexible_overflow += max(0.0, work - retained)
        return capped_fixed, flexible_overflow, complete

    def _rejected_compatibility_work(
            self) -> list[tuple[int, tuple[str, ...], float]]:
        """Distribute aggregate rejection work without changing its total."""
        raw: list[tuple[int, tuple[str, ...], float]] = []
        for profile in self.rejected_compatibility_profiles:
            count = int(profile['count'])
            duration = self.effective_request_duration_seconds
            if self.replica_unit != 'logical' or duration is None:
                work = float(count)
            else:
                retained = (count * duration /
                            constants.LB_REJECT_WINDOW_SECONDS)
                recent = (int(profile.get('recent_count', 0)) * duration /
                          self.qps_window_size)
                work = max(retained, recent)
            raw.append((int(profile['priority']),
                        tuple(profile['compatible_accelerators']), work))
        aggregate = self._rejected_work()
        raw_total = sum(work for _, _, work in raw)
        if raw_total <= 0:
            return []
        scale = aggregate / raw_total
        return [(priority, compatible, work * scale)
                for priority, compatible, work in raw]

    def _calculate_concurrency_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        *,
        target_ceiling: int | None = None,
        min_replicas_override: int | None = None,
        use_existing_supply: bool = False,
        pin_running_work: bool = False,
        use_free_reserved: bool = True,
    ) -> _CompatibilityTargetResult:
        """Allocate the concurrency target in physical or logical units."""
        configured_cards = self._configured_cards_from_profiles()
        if not configured_cards:
            self.warm_retention_target_by_accelerator = {}
            return _CompatibilityTargetResult({}, {}, {}, False)
        if self.replica_unit == 'logical':
            capacity_per_card = {
                card: self._effective_logical_capacity_per_gpu()
                for card in configured_cards
            }
        else:
            capacity_per_card = {
                card: (self.target_concurrency_per_replica *
                       self._configured_gpu_count(card)
                      ) for card in configured_cards
            }
        ready_zero_cost = {card: 0 for card in configured_cards}
        ready = {card: 0 for card in configured_cards}
        provisioning = {card: 0 for card in configured_cards}
        canonical_by_name = {card.casefold(): card for card in configured_cards}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is None:
                continue
            width = (max(0, self._committed_capacity(info))
                     if self.replica_unit == 'logical' else 1)
            if width == 0:
                continue
            if info.is_ready:
                ready[card] += width
                if info.is_zero_cost is True:
                    ready_zero_cost[card] += width
            else:
                provisioning[card] += width

        profiles = [(int(profile['priority']),
                     tuple(profile['compatible_accelerators']),
                     float(profile['count']))
                    for profile in self.queued_compatibility_profiles]
        explicit_profiles = list(profiles)
        paid_profiles = list(profiles)
        queue_profile_total = sum(work for _, _, work in profiles)
        default_compatible = tuple(configured_cards)
        if self._queue_depth > queue_profile_total:
            default_queue_profile = (constants.LB_REQUEST_PRIORITY_MIN,
                                     default_compatible,
                                     self._queue_depth - queue_profile_total)
            profiles.append(default_queue_profile)
            paid_profiles.append(default_queue_profile)
        rejected_profiles = self._rejected_compatibility_work()
        profiles.extend(rejected_profiles)
        explicit_profiles.extend(rejected_profiles)
        paid_profiles.extend(rejected_profiles)
        rejected_profile_total = sum(work for _, _, work in rejected_profiles)
        rejected_total = self._rejected_work()
        if rejected_total > rejected_profile_total:
            default_rejected_profile = (constants.LB_REQUEST_PRIORITY_MIN,
                                        default_compatible,
                                        rejected_total - rejected_profile_total)
            profiles.append(default_rejected_profile)
            paid_profiles.append(default_rejected_profile)
        retention_fixed, flexible_fixed_overflow, attribution_complete = (
            self._fixed_concurrency_work_by_accelerator(replica_infos))
        allocation_fixed = retention_fixed
        explicit_fixed = retention_fixed
        if (self.replica_unit == 'logical' and
                self._compatibility_demand_complete and not pin_running_work):
            # Running work is physically non-preemptive but does not make its
            # serving card the owner of flexible demand. Reuse the bounded
            # accepted-arrival histogram as compatibility evidence for the
            # current in-flight population. When that history has aged out,
            # the protocol default remains all configured cards; warm
            # retention and the supply-aware actuation pass still keep the
            # actual serving cards until their work drains. The allocation
            # result marks that fallback as insufficient proof for a
            # mixed-version cross-card replacement.
            fixed_work = (sum(retention_fixed.values()) +
                          flexible_fixed_overflow)
            allocation_fixed = {}
            explicit_fixed = {}
            evidence = [(int(profile['priority']),
                         tuple(profile['compatible_accelerators']),
                         float(profile['count']))
                        for profile in self.compatibility_profiles
                        if float(profile['count']) > 0]
            evidence_total = sum(work for _, _, work in evidence)
            if fixed_work > 0 and evidence_total > 0:
                scale = fixed_work / evidence_total
                scaled_evidence = [(priority, compatible, work * scale)
                                   for priority, compatible, work in evidence]
                profiles.extend(scaled_evidence)
                explicit_profiles.extend(scaled_evidence)
                paid_profiles.extend(scaled_evidence)
            elif fixed_work > 0:
                profiles.append((constants.LB_REQUEST_PRIORITY_MIN,
                                 default_compatible, fixed_work))
        elif flexible_fixed_overflow > 0:
            profiles.append((constants.LB_REQUEST_PRIORITY_MIN,
                             default_compatible, flexible_fixed_overflow))
        if self.replica_unit == 'logical' and self._fresh_for_tick():
            allocator_attributed_work = (sum(allocation_fixed.values()) +
                                         sum(work for _, _, work in profiles))
            arrival_work = self._arrival_work()
            arrival_profiles = self._arrival_compatibility_work(
                arrival_work, allocator_attributed_work)
            profiles.extend(arrival_profiles)
            explicit_profiles.extend(arrival_profiles)
            paid_profiles.extend(arrival_profiles)
        floors = {
            card.casefold(): int(floor)
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        free_reserved = (dict(self.free_reserved_slots_by_accelerator)
                         if use_free_reserved else {})
        if self.replica_unit == 'logical':
            free_reserved = {
                card: count * self._configured_gpu_count(card)
                for card, count in free_reserved.items()
            }
        ceiling = (self.max_replicas if target_ceiling is None else min(
            self.max_replicas, target_ceiling))
        cold_order = self._cold_paid_card_order(configured_cards)

        def allocate(
            minimum: int,
            demand_profiles: list[tuple[int, tuple[str, ...], float]],
            fixed_work_by_accelerator: dict[str, float],
        ) -> dict[str, int]:
            return _allocate_compatibility_target(
                configured_cards=configured_cards,
                capacities=capacity_per_card,
                floors=floors,
                min_replicas=minimum,
                max_replicas=ceiling,
                demand_profiles=demand_profiles,
                fixed_work_by_accelerator=fixed_work_by_accelerator,
                ready_zero_cost=ready_zero_cost,
                ready=ready,
                provisioning=provisioning,
                free_reserved=free_reserved,
                cold_order=cold_order,
                use_existing_supply=use_existing_supply)

        target = allocate(
            min(
                self.min_replicas if min_replicas_override is None else
                min_replicas_override, ceiling), profiles, allocation_fixed)
        raw_explicit_target = allocate(0, explicit_profiles, explicit_fixed)
        raw_paid_target = allocate(min(self.min_replicas, ceiling),
                                   paid_profiles, {})
        # Unproven aggregate work can change a flexible profile's marginal
        # placement under a hard ceiling. Intersect exact cards rather than
        # transferring ownership to a different card by inference.
        explicit_target = {
            card: min(count, target.get(card, 0))
            for card, count in raw_explicit_target.items()
            if count > 0 and target.get(card, 0) > 0
        }
        paid_target = {
            card: min(count, target.get(card, 0))
            for card, count in raw_paid_target.items()
            if count > 0 and target.get(card, 0) > 0
        }
        if attribution_complete:
            self.warm_retention_target_by_accelerator = {
                card: _work_to_slots(work, capacity_per_card[card])
                for card, work in retention_fixed.items()
                if work > 0 and capacity_per_card[card] > 0
            }
        else:
            self.warm_retention_target_by_accelerator = {}
        return _CompatibilityTargetResult(target, explicit_target, paid_target,
                                          attribution_complete)

    def _logical_committed_capacity_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> dict[str, int]:
        """Return latest-version committed logical slots by exact card."""
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        committed: dict[str, int] = {}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = configured_by_name.get(raw_card.casefold())
            if card is None:
                continue
            committed[card] = (committed.get(card, 0) +
                               self._committed_capacity(info))
        return committed

    def _limit_logical_actuation_transition(
        self,
        desired: dict[str, int],
        target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
        wave_budget: int | None,
    ) -> tuple[dict[str, int], int]:
        """Limit cold card migration without changing demand attribution.

        Reconstruct the transition baseline from committed supply, preferring
        cards already wanted by the fresh supply-aware actuator. Positive
        deficits then consume the exact-card wave budget. Old-card capacity is
        retained only in this private actuation map until each replacement
        wave commits; it never appears in the public cheapest-compatible
        demand map.
        """
        if sum(desired.values()) != target:
            return {}, 0
        cards = self._configured_cards_from_profiles()
        committed = self._logical_committed_capacity_by_accelerator(
            replica_infos)
        previous = self._logical_actuation_target_by_accelerator
        same_desired = (
            desired == self._logical_actuation_desired_by_accelerator)
        if same_desired and sum(previous.values()) == target:
            # Preserve a previously authorized cold wave until its pending
            # rows become committed, so a transiently dropped manager decision
            # is retried during the cooldown instead of being forgotten.
            current = {
                card: max(0, int(previous.get(card, 0))) for card in cards
            }
        else:
            current = {card: 0 for card in cards}
            remaining = max(0, target)
            # Existing capacity on a desired card is a supply reuse, not a
            # cold migration, so it does not consume a launch wave.
            for card in cards:
                kept = min(remaining, committed.get(card, 0),
                           max(0, int(desired.get(card, 0))))
                current[card] = kept
                remaining -= kept
            # Retain other committed cards as transition placeholders. They
            # are removed only as authorized replacement capacity enters the
            # map.
            for card in cards:
                available = max(0, committed.get(card, 0) - current[card])
                kept = min(remaining, available)
                current[card] += kept
                remaining -= kept

        # Supply that appeared after the prior authorization can immediately
        # replace a transition placeholder. This is reuse, not a new cold
        # wave, even when the desired profile itself is unchanged.
        reusable = 0
        for card in cards:
            moved = min(max(0,
                            desired.get(card, 0) - current.get(card, 0)),
                        max(0,
                            committed.get(card, 0) - current.get(card, 0)))
            current[card] += moved
            reusable += moved
        for card in reversed(cards):
            if reusable <= 0:
                break
            removable = max(0, current.get(card, 0) - desired.get(card, 0))
            removed = min(reusable, removable)
            current[card] -= removed
            reusable -= removed

        desired_additions = sum(
            max(0,
                desired.get(card, 0) - current.get(card, 0)) for card in cards)
        # The aggregate target may have been recovered from healthy old
        # versions or adopted by an earlier demand wave. Its exact-card map
        # must be complete even when latest-version committed supply plus this
        # tick's card budget is smaller. Completing that held target does not
        # create demand; target and max_replicas remain the hard ceilings.
        required_to_complete = max(0, target - sum(current.values()))
        additions_left = (desired_additions if wave_budget is None else max(
            0, wave_budget, required_to_complete))
        added = 0
        for card in cards:
            increase = max(0, desired.get(card, 0) - current.get(card, 0))
            accepted = min(increase, additions_left)
            current[card] = current.get(card, 0) + accepted
            additions_left -= accepted
            added += accepted

        # A target-map reduction is only an intent. The decision generator
        # still proves replacement readiness and per-card coverage before it
        # emits any idle victim, so balancing the map here is non-preemptive.
        excess = max(0, sum(current.values()) - target)
        for card in reversed(cards):
            removable = max(0, current.get(card, 0) - desired.get(card, 0))
            removed = min(excess, removable)
            current[card] -= removed
            excess -= removed
            if excess == 0:
                break

        limited = {card: count for card, count in current.items() if count > 0}
        if sum(limited.values()) != target:
            # A valid aggregate wave always leaves enough budget to cover any
            # target units not backed by committed supply. Fail closed rather
            # than publish an incomplete exact-card actuator.
            return {}, 0
        return limited, added

    def _actuation_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, int], bool]:
        """Revalidate logical cold launches at the adopted total target."""
        demand_target = self.target_num_replicas_by_accelerator
        if (not self._compatibility_demand_complete or
                sum(demand_target.values()) != self.target_num_replicas):
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        final_target = self.get_final_target_num_replicas()
        cards = self._configured_cards_from_profiles()
        allocation = self._calculate_concurrency_target_by_accelerator(
            replica_infos,
            target_ceiling=final_target,
            min_replicas_override=final_target,
            use_existing_supply=True,
            pin_running_work=False,
            use_free_reserved=False)
        desired_target = allocation.target_by_accelerator
        explicit_target = allocation.explicit_target_by_accelerator
        paid_target = allocation.paid_target_by_accelerator
        attribution_complete = allocation.card_attribution_complete
        if (not attribution_complete or
                sum(desired_target.values()) != final_target):
            self._logical_paid_launch_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        fresh_complete_attribution = (self._fresh_for_tick() and
                                      self._compatibility_demand_complete and
                                      attribution_complete and
                                      sum(desired_target.values())
                                      == final_target)
        canonical_by_name = {card.casefold(): card for card in cards}
        nonretiring_supply = {card: 0 for card in cards}
        # Old-version rows are provenance for the reconciler: they cannot
        # authorize a launch, but a card they still serve is mid-replacement
        # rather than gone, and must not be released as vanished capacity
        # while the rollout drains. Preempted and scale-down rows are excluded
        # on both versions for the same reason they are excluded from latest
        # supply: they must not preserve, let alone replace, their card.
        old_version_supply = {card: 0 for card in cards}
        has_active_old_version = any(
            not info.is_terminal and info.version != self.latest_version
            for info in replica_infos)
        for info in replica_infos:
            if info.is_terminal or _replica_is_retiring_card_supply(info):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is None:
                continue
            width = (max(0, self._committed_capacity(info))
                     if self.replica_unit == 'logical' else 1)
            if width == 0:
                continue
            if info.version == self.latest_version:
                nonretiring_supply[card] += width
            else:
                old_version_supply[card] += width
        # Broker-reported free slots are opportunities, not materialized
        # supply. Treating them as backing here can move flexible L4 demand
        # onto A100 during a rollout. If the research slot then disappears,
        # the exact-card shortage retries on a paid A100 location. Reserved
        # fill owns those opportunities independently and carries the
        # zero-cost-only launch fence; demand actuation may reuse the card only
        # after a latest-version replica row materializes it.
        downscale_hold = (self._raw_target_num_replicas
                          < self.target_num_replicas)
        if self.replica_unit != 'logical':
            # Physical-backend scaling retains the legacy actuation contract:
            # its exact-card target is itself the launch decision, so there is
            # no separate paid-ownership channel to reconcile here.
            target = _revalidate_actuation_target(
                adopted_target=demand_target,
                desired_target=desired_target,
                nonretiring_supply=nonretiring_supply,
                configured_cards=cards,
                final_target=final_target,
                allow_adopted_reassignment=not has_active_old_version,
                allow_unbacked_adopted_reassignment=not downscale_hold,
                old_version_supply=old_version_supply)
            return target, (attribution_complete and
                            sum(target.values()) == final_target)
        # Rollout movement requires explicit compatibility evidence. In the
        # latest-only case, paid-owned headerless/minimum demand can also move
        # to its freshly allocated card. Inferred in-flight overflow and
        # generic overprovision padding remain reconciliation-only.
        allow_mixed_version_backed_reassignment = (has_active_old_version and
                                                   not downscale_hold and
                                                   fresh_complete_attribution
                                                   and bool(explicit_target))
        if fresh_complete_attribution and not downscale_hold:
            reassignment_target = (explicit_target
                                   if has_active_old_version else paid_target)
        else:
            reassignment_target = {}
        target = _revalidate_actuation_target(
            adopted_target=demand_target,
            desired_target=desired_target,
            nonretiring_supply=nonretiring_supply,
            configured_cards=cards,
            final_target=final_target,
            allow_adopted_reassignment=(not has_active_old_version and
                                        fresh_complete_attribution and
                                        not downscale_hold),
            allow_unbacked_adopted_reassignment=(fresh_complete_attribution and
                                                 not downscale_hold),
            allow_mixed_version_backed_reassignment=(
                allow_mixed_version_backed_reassignment),
            old_version_supply=old_version_supply,
            reassignment_target_by_accelerator=reassignment_target)
        if not target and final_target > 0:
            self._logical_paid_launch_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        # Stale reports may preserve a prior exact-card reconciliation fence,
        # but never authorize paid acquisition.  A downscale hold keeps its
        # adopted exact-card retry contract; otherwise the fresh supply-aware
        # placement is the sole economic authority for a new backend.
        if not fresh_complete_attribution:
            paid_launch_target: dict[str, int] = {}
        else:
            current_ownership = (explicit_target
                                 if has_active_old_version else paid_target)
            adopted_ownership = (
                self._logical_adopted_explicit_target_by_accelerator
                if has_active_old_version else
                self._logical_adopted_paid_target_by_accelerator)
            # Ownership is adopted with the aggregate target and survives a
            # transiently empty histogram until a later target adoption
            # replaces it. Current evidence may add ownership immediately;
            # neither source can exceed the retained exact-card demand map.
            paid_ownership = {
                card: min(
                    int(target.get(card, 0)),
                    max(
                        int(current_ownership.get(card, 0)),
                        min(int(adopted_ownership.get(card, 0)),
                            int(demand_target.get(card, 0)))))
                for card in cards
                if target.get(card, 0) > 0 and (current_ownership.get(
                    card, 0) > 0 or adopted_ownership.get(card, 0) > 0)
            }
            # Paid ownership is separate from compatibility ownership. A
            # latest-only minimum or headerless queue can buy its allocator-
            # selected card; a mixed-version rollout requires explicit proof.
            # Vanished adopted units and inferred in-flight/overprovision
            # padding own no paid placement.
            paid_launch_target = {
                card: count
                for card, count in paid_ownership.items()
                if count > 0
            }
            if has_active_old_version:
                for card in cards:
                    # This is an absolute latest-version ceiling. The decision
                    # generator subtracts latest committed supply later,
                    # leaving exactly the live old-version backing as
                    # same-card retry authority. Using old supply as an
                    # incremental ceiling would stall a partially completed
                    # rollout (latest=1, old=1, target=2) at zero authority.
                    same_card_ceiling = min(
                        int(target.get(card,
                                       0)), int(demand_target.get(card, 0)),
                        int(nonretiring_supply.get(card, 0)) +
                        int(old_version_supply.get(card, 0)))
                    if same_card_ceiling > paid_launch_target.get(card, 0):
                        paid_launch_target[card] = same_card_ceiling
        self._logical_paid_launch_target_by_accelerator = {
            card: max(0, int(paid_launch_target.get(card, 0)))
            for card in cards
            if paid_launch_target.get(card, 0) > 0
        }
        wave_budget = self._logical_actuation_wave_budget
        if wave_budget is not None:
            if self._logical_actuation_wave_started:
                # Several consumers ask for the actuation map in one
                # controller tick. The shared snapshot, including the
                # overprovision allowance, is one budget rather than one
                # budget per caller.
                wave_budget = 0
            else:
                # num_overprovision is deliberately outside the traffic target
                # and historically was not charged to its demand scale-up
                # wave.
                wave_budget += max(0, final_target - self.target_num_replicas)
        limited_target, added_card_slots = (
            self._limit_logical_actuation_transition(target, final_target,
                                                     replica_infos,
                                                     wave_budget))
        if not limited_target and final_target > 0:
            self._logical_paid_launch_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        self._logical_actuation_target_by_accelerator = dict(limited_target)
        self._logical_actuation_desired_by_accelerator = dict(target)
        self._logical_card_transition_pending = limited_target != target
        if (added_card_slots > 0 and
                self.max_scale_up_rate_percentage is not None and
                not self._logical_actuation_wave_started):
            self._record_logical_scale_up_wave(
                replica_infos, self._logical_actuation_wave_budget)
        return (limited_target, attribution_complete and
                sum(limited_target.values()) == final_target)

    def _set_target_num_replicas_with_concurrency_logic(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Recompute target_num_replicas for this tick.

        Mirrors _set_target_num_replicas_with_instance_aware_logic's
        structure: pack demand onto the existing latest replicas (largest
        first), size the remainder with the best live capacity (falling
        back to knob x 1 for an empty fleet so scale-from-zero is not
        stuck), then apply the snap/zero/hysteresis ladder.
        """
        latest_capacities = self._latest_capacities(replica_infos)
        if self.replica_unit == 'logical':
            # Public targets count GPU slots. Each slot absorbs the configured
            # amount of outstanding work; physical backend packing happens
            # later, after the manager selects exact 1/4/8-GPU placements.
            best_capacity = self._effective_logical_capacity_per_gpu()
            self._latest_committed_capacity = (
                self._latest_committed_logical_capacity(replica_infos))
            self._latest_provisioning_capacity = (
                self._provisioning_logical_capacity(replica_infos))
        else:
            best_capacity = (latest_capacities[0] if latest_capacities else
                             self.target_concurrency_per_replica)
        self._upscale_pending = False

        if not self._fresh_for_tick():
            if self.replica_unit == 'logical':
                # A signal gap cannot prove continuous low demand. Require a
                # complete fresh elapsed window after reports recover.
                self._reset_downscale_hysteresis()
                self._pressure_baseline = None
                self._pressure_latched = False
                self._pressure_reasons = ()
                self._pressure_streak = 0
                self._downscale_veto_streak = 0
            # SIGNAL GAP: the only trustworthy signal is arrivals (they
            # ride every sync). Raise-only floor, applied without
            # hysteresis -- while blind we must not delay growth, and we
            # never shrink. The one-shot snap is deliberately NOT
            # consumed here: it waits for the first recompute with fresh
            # data.
            # Prune the window here, not just in
            # collect_request_information: once syncs stop entirely,
            # collect is never called again, and unpruned timestamps
            # would keep asserting an arrival floor for arrivals long
            # outside the window.
            index = bisect.bisect_left(self.request_timestamps,
                                       time.time() - self.qps_window_size)
            self.request_timestamps = self.request_timestamps[index:]
            arrivals = len(self.request_timestamps)
            if arrivals > 0 and best_capacity > 0:
                arrival_work = float(arrivals)
                duration = self.effective_request_duration_seconds
                if duration is not None:
                    arrival_work *= (duration / self.qps_window_size)
                arrival_floor = self._clip_target_num_replicas(
                    math.ceil(arrival_work / best_capacity))
                if arrival_floor > self.target_num_replicas:
                    logger.info(
                        'Concurrency autoscaler signal-stale: raising '
                        f'target to arrival floor {arrival_floor} '
                        f'({arrivals} arrivals / capacity {best_capacity}).')
                    self._raw_target_num_replicas = arrival_floor
                    self._adopt_scale_up_target(arrival_floor, replica_infos)
            else:
                logger.info('Concurrency autoscaler signal-stale: holding '
                            f'target at {self.target_num_replicas}.')
            return

        if (self.configured_accelerator_shapes and
                not self._compatibility_demand_complete):
            # Mixed controller/LB rollout: aggregate gauges are fresh, but a
            # card assignment is not. Keep the prior target and leave the
            # one-shot restart fence armed. This prevents both an unshaped
            # launch and a card-blind downscale until the new active LB has
            # reported every replaceable compatibility gauge.
            logger.info(
                'Concurrency compatibility gauges incomplete: '
                'holding exact-card target at %s.',
                self.target_num_replicas_by_accelerator)
            return

        outstanding = self._outstanding_work(replica_infos)
        if self.replica_unit == 'logical':
            raw_target_num = _work_to_slots(outstanding, best_capacity)
            arrival_work = self._arrival_work()
            self._arrival_floor_target = self._clip_target_num_replicas(
                math.ceil(arrival_work / best_capacity))
            raw_target_num = max(raw_target_num, self._arrival_floor_target)
        else:
            self._arrival_floor_target = 0
            raw_target_num = 0
            covered = 0.0
            for capacity in latest_capacities:
                if covered >= outstanding:
                    break
                raw_target_num += 1
                covered += capacity
            if covered < outstanding:
                remaining = outstanding - covered
                if best_capacity > 0:
                    raw_target_num += math.ceil(remaining / best_capacity)

        candidate_allocation: _CompatibilityTargetResult | None = None
        candidate_target_by_accelerator: dict[str, int] | None = None
        if self._compatibility_demand_complete:
            candidate_allocation = (
                self._calculate_concurrency_target_by_accelerator(replica_infos)
            )
            if candidate_allocation.card_attribution_complete:
                candidate_target_by_accelerator = (
                    candidate_allocation.target_by_accelerator)
                # Compatibility constraints can require a different physical
                # packing than the aggregate best-capacity estimate. The
                # aggregate offered-arrival floor remains independently
                # authoritative when compatibility evidence is unavailable.
                raw_target_num = max(
                    raw_target_num,
                    sum(candidate_allocation.target_by_accelerator.values()))

        target_num_replicas = self._clip_concurrency_demand_target(
            raw_target_num)
        self._raw_target_num_replicas = target_num_replicas
        candidate_covers_raw_target = (
            candidate_target_by_accelerator is not None and
            sum(candidate_target_by_accelerator.values())
            >= target_num_replicas)
        if (self.replica_unit == 'logical' and
                self._snap_target_on_next_recompute and
                self._adopt_total_capacity_on_next_recompute):
            # The adopted target is controller-local and rebuilds at
            # min_replicas, while the latest-version demand-owned fleet may
            # already be much larger. Re-establish that traffic fleet as the
            # actuation baseline once, before applying hysteresis and the
            # downscale limit. Fill-origin rows remain independently protected
            # by the reserved-capacity overlay; including them here would turn
            # opportunistic supply into paid replacement demand.
            # Otherwise the first fresh report after a restart can publish a
            # tiny target and retire the whole live fleet in one tick. Do not
            # repeat this after the one-shot snap: an adopted downscale target
            # must remain below committed capacity while retirement catches up.
            committed = self._total_ready_demand_owned_logical_capacity(
                replica_infos)
            self.target_num_replicas = max(
                self.target_num_replicas,
                self._clip_concurrency_demand_target(committed))
        old_target_num_replicas = self.target_num_replicas
        old_target_by_accelerator = dict(
            self.target_num_replicas_by_accelerator)
        old_explicit_target_by_accelerator = dict(
            self._logical_adopted_explicit_target_by_accelerator)
        old_paid_target_by_accelerator = dict(
            self._logical_adopted_paid_target_by_accelerator)
        if (self.replica_unit == 'logical' and candidate_covers_raw_target and
                sum(old_target_by_accelerator.values())
                != old_target_num_replicas):
            # A rebuilt controller reconstructs the aggregate safety target
            # from committed demand-owned capacity, while its process-local
            # exact-card demand map starts empty. Attribute the entire held
            # aggregate through the fresh compatibility allocator. Committed
            # A100 supply belongs to the separate actuation map and must not
            # become A100 demand merely because it survived the restart.
            recovered_allocation = (
                self._calculate_concurrency_target_by_accelerator(
                    replica_infos,
                    target_ceiling=old_target_num_replicas,
                    min_replicas_override=old_target_num_replicas))
            recovered_map = recovered_allocation.target_by_accelerator
            if (recovered_allocation.card_attribution_complete and
                    sum(recovered_map.values()) == old_target_num_replicas):
                self.target_num_replicas_by_accelerator = recovered_map
                self._logical_adopted_explicit_target_by_accelerator = {
                    card: min(count, recovered_map.get(card, 0))
                    for card, count in
                    recovered_allocation.explicit_target_by_accelerator.items()
                    if count > 0 and recovered_map.get(card, 0) > 0
                }
                self._logical_adopted_paid_target_by_accelerator = {
                    card: min(count, recovered_map.get(card, 0))
                    for card, count in
                    recovered_allocation.paid_target_by_accelerator.items()
                    if count > 0 and recovered_map.get(card, 0) > 0
                }
                old_target_by_accelerator = dict(recovered_map)
                old_explicit_target_by_accelerator = dict(
                    self._logical_adopted_explicit_target_by_accelerator)
                old_paid_target_by_accelerator = dict(
                    self._logical_adopted_paid_target_by_accelerator)
        target_map_changed = (candidate_target_by_accelerator is not None and
                              candidate_target_by_accelerator
                              != old_target_by_accelerator)
        target_map_increases = False
        if target_map_changed and candidate_target_by_accelerator is not None:
            target_map_increases = any(
                candidate_target_by_accelerator.get(card, 0) >
                old_target_by_accelerator.get(card, 0)
                for card in candidate_target_by_accelerator)
        apply_target = False
        apply_card_transition = False

        if self._snap_target_on_next_recompute:
            # First recompute with fresh data after construction or an update:
            # snap upward immediately, but never bypass downscale hysteresis.
            # A policy-only update can land during a brief idle interval; an
            # immediate downward snap would tear down the live fleet before
            # the configured downscale delay has proved sustained idleness.
            self._snap_target_on_next_recompute = False
            self._adopt_total_capacity_on_next_recompute = False
            self.upscale_counter = 0
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            if target_num_replicas >= self.target_num_replicas:
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
            else:
                if self._downscale_hysteresis_elapsed():
                    if not self._consume_downscale_pressure_veto():
                        self._reset_downscale_hysteresis()
                        self._adopt_scale_down_target(target_num_replicas,
                                                      replica_infos)
                        apply_target = True
        # Faster scale up when there is no replica.
        elif self.target_num_replicas == 0:
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            self._adopt_scale_up_target(target_num_replicas, replica_infos)
            apply_target = True
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self._reset_downscale_hysteresis()
            # A rising raw target ends the downscale episode: the next
            # episode gets a fresh veto budget.
            self._downscale_veto_streak = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
        elif target_num_replicas < self.target_num_replicas:
            # Aggregate and exact-card directions are independent. A lower
            # aggregate target must continue its elapsed proof even if the
            # compatibility mix asks for more of one card. The card migration
            # may still advance under the normal upscale observation and wave
            # bounds while the aggregate target remains held.
            if target_map_increases:
                self.upscale_counter += 1
                if self.upscale_counter >= self.scale_up_threshold:
                    self.upscale_counter = 0
                    apply_card_transition = True
            else:
                self.upscale_counter = 0
            if self._downscale_hysteresis_elapsed():
                if not self._consume_downscale_pressure_veto():
                    self._reset_downscale_hysteresis()
                    self._adopt_scale_down_target(target_num_replicas,
                                                  replica_infos)
                    apply_target = True
        elif target_map_increases:
            # A same-size migration is still an upscale. It ends any prior
            # lower-demand episode, but never changes the aggregate target.
            self.upscale_counter += 1
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
        elif target_map_changed:
            self.upscale_counter = 0
            if self._downscale_hysteresis_elapsed():
                if not self._consume_downscale_pressure_veto():
                    self._reset_downscale_hysteresis()
                    self._adopt_scale_down_target(target_num_replicas,
                                                  replica_infos)
                    apply_target = True
        else:
            self.upscale_counter = 0
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0

        if ((apply_target or apply_card_transition) and
                candidate_covers_raw_target):
            if (self._raw_target_num_replicas < self.target_num_replicas):
                fresh_allocation = (
                    self._calculate_concurrency_target_by_accelerator(
                        replica_infos,
                        target_ceiling=self._raw_target_num_replicas,
                        min_replicas_override=self._raw_target_num_replicas))
                fresh_map = fresh_allocation.target_by_accelerator
                adopted_map = _merge_fresh_target_into_downscale_hold(
                    adopted_target=old_target_by_accelerator,
                    fresh_target=fresh_map,
                    configured_cards=self._configured_cards_from_profiles(),
                    replacement_order=self._cold_paid_card_order(
                        self._configured_cards_from_profiles()),
                    target_total=self.target_num_replicas)
            else:
                adopted_allocation = (
                    self._calculate_concurrency_target_by_accelerator(
                        replica_infos,
                        target_ceiling=self.target_num_replicas,
                        min_replicas_override=self.target_num_replicas))
                adopted_map = adopted_allocation.target_by_accelerator
                fresh_allocation = adopted_allocation
            if (fresh_allocation.card_attribution_complete and
                    sum(adopted_map.values()) == self.target_num_replicas):
                self.target_num_replicas_by_accelerator = adopted_map
                fresh_explicit = {
                    card: min(count, adopted_map.get(card, 0))
                    for card, count in
                    fresh_allocation.explicit_target_by_accelerator.items()
                    if count > 0 and adopted_map.get(card, 0) > 0
                }
                fresh_paid = {
                    card: min(count, adopted_map.get(card, 0))
                    for card, count in
                    fresh_allocation.paid_target_by_accelerator.items()
                    if count > 0 and adopted_map.get(card, 0) > 0
                }
                if self._raw_target_num_replicas < self.target_num_replicas:
                    self._logical_adopted_explicit_target_by_accelerator = {
                        card: max(
                            fresh_explicit.get(card, 0),
                            min(old_explicit_target_by_accelerator.get(card, 0),
                                adopted_map.get(card, 0)))
                        for card in adopted_map
                        if (fresh_explicit.get(card, 0) > 0 or
                            old_explicit_target_by_accelerator.get(card, 0) > 0)
                    }
                    self._logical_adopted_paid_target_by_accelerator = {
                        card: max(
                            fresh_paid.get(card, 0),
                            min(old_paid_target_by_accelerator.get(card, 0),
                                adopted_map.get(card, 0)))
                        for card in adopted_map
                        if (fresh_paid.get(card, 0) > 0 or
                            old_paid_target_by_accelerator.get(card, 0) > 0)
                    }
                else:
                    self._logical_adopted_explicit_target_by_accelerator = (
                        fresh_explicit)
                    self._logical_adopted_paid_target_by_accelerator = (
                        fresh_paid)

        self._upscale_pending = (
            target_num_replicas > self.target_num_replicas or
            (candidate_target_by_accelerator is not None and any(
                candidate_target_by_accelerator.get(card, 0) >
                self.target_num_replicas_by_accelerator.get(card, 0)
                for card in candidate_target_by_accelerator)))

        if self.replica_unit == 'logical':
            downscale_status = (
                f'Downscale observations: {self.downscale_counter}. '
                f'Downscale elapsed: {self._downscale_elapsed_seconds():.1f}/'
                f'{self.downscale_delay_seconds:.1f}s. ')
        else:
            downscale_status = (f'Downscale counter: {self.downscale_counter}/'
                                f'{self.scale_down_threshold}. ')
        logger.info(
            f'Concurrency: outstanding work: {outstanding}. '
            f'Latest-version capacities: {latest_capacities}. '
            f'Old target number of replicas: {old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Target by accelerator: '
            f'{self.target_num_replicas_by_accelerator}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'{downscale_status}')

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        shape_handles = self._resolve_gpu_shape_handles(replica_infos)
        with self._logical_state_lock:
            self._gpu_shape_handles_for_tick = shape_handles
            self._knob_unavailable_versions_for_tick = set()
            try:
                return self._generate_scaling_decisions_locked(
                    replica_infos, active_versions)
            finally:
                self._knob_unavailable_versions_for_tick = None
                self._gpu_shape_handles_for_tick = None

    def _generate_scaling_decisions_locked(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        # Recompute the target BEFORE the base class runs the
        # outdated-replica drain, for the same reason as the
        # instance-aware autoscaler: the drain compares ready new-version
        # replicas against target_num_replicas, and a stale target would
        # let it retire old capacity that is still needed. Single
        # recompute per tick.
        # Freshness is snapshotted ONCE per tick: collect_request_
        # information runs concurrently on the sync thread, and if the
        # first fresh report landed mid-tick the recompute would take
        # the stale path (target still the rebuilt-blind minimum) while
        # the later drain/scale-down guards saw "fresh" and proceeded --
        # marrying a blind target to fresh-mode kills. All three
        # consumers read this snapshot instead of re-evaluating.
        self._tick_fresh = self.has_fresh_demand_report()
        # Sample launch-to-ready before sizing, so a wave that just landed
        # informs this tick's lead estimate.
        self._observe_provision_leads(replica_infos)
        self._logical_actuation_wave_is_new = False
        self._logical_actuation_wave_budget = self._logical_scale_up_budget(
            replica_infos)
        self._logical_actuation_wave_started = False
        self._logical_card_transition_pending = False
        try:
            self._prune_gpu_shape_cache(
                {info.replica_id for info in replica_infos})
            keep_versions = {info.version for info in replica_infos}
            keep_versions.add(self.latest_version)
            for version in list(self._knob_by_version):
                if version not in keep_versions:
                    del self._knob_by_version[version]
            self._set_target_num_replicas_with_concurrency_logic(replica_infos)
            decisions = super().generate_scaling_decisions(
                replica_infos, active_versions)
            if self.replica_unit != 'logical':
                return decisions
            fenced: list[AutoscalerDecision] = []
            target = self.get_final_target_num_replicas()
            target_by_card, use_card_targets = (
                self._actuation_target_by_accelerator(replica_infos))
            target_by_card_state = (tuple(target_by_card.items())
                                    if use_card_targets else ())
            shape_state = (tuple(self.configured_accelerator_shapes.items())
                           if use_card_targets else ())
            if self.configured_accelerator_shapes and not use_card_targets:
                # An authoritative exact-card catalog without a complete
                # compatibility report cannot safely authorize aggregate-only
                # retirement. None tells the controller to revoke any target
                # published by an earlier complete generation.
                self._last_logical_target_state = None
            elif use_card_targets:
                self._last_logical_target_state = (self.latest_version,
                                                   self._reconcile_generation,
                                                   target, target_by_card_state,
                                                   shape_state)
            else:
                self._last_logical_target_state = (self.latest_version,
                                                   self._reconcile_generation,
                                                   target)
            for decision in decisions:
                if (decision.operator == AutoscalerDecisionOperator.SCALE_DOWN
                        and isinstance(decision.target, int)):
                    if not self._fresh_for_tick():
                        continue
                    decision = AutoscalerDecision(
                        AutoscalerDecisionOperator.SCALE_DOWN,
                        LogicalScaleDownTarget(
                            version=self.latest_version,
                            reconcile_generation=self._reconcile_generation,
                            target_capacity=target,
                            replica_id=decision.target,
                            target_capacity_by_accelerator=(
                                target_by_card_state),
                            accelerator_shapes=shape_state),
                        reason=decision.reason)
                fenced.append(decision)
            return fenced
        finally:
            self._tick_fresh = None

    def _calculate_target_num_replicas(self) -> int:
        # Demand-aware sizing needs replica_infos, which this hook
        # (invoked by the base update_version to snap the target after an
        # update) does not receive. Keep the current target (re-clipped to
        # the new bounds); the next decision tick recomputes from live
        # replica shapes and the fresh demand report.
        return self._clip_target_num_replicas(self.target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        with self._logical_state_lock:
            self._update_version_locked(version, spec, update_mode)

    def update_version_and_accelerator_shapes(
            self, version: int, spec: 'service_spec.SkyServiceSpec',
            update_mode: serve_utils.UpdateMode,
            accelerator_shapes: dict[str, int]) -> None:
        """Atomically publish a version and its exact-card policy state."""
        with self._logical_state_lock:
            self._update_version_locked(version, spec, update_mode)
            self._set_configured_accelerator_shapes_locked(accelerator_shapes)

    def _update_version_locked(self, version: int,
                               spec: 'service_spec.SkyServiceSpec',
                               update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't overwrite the
            # live concurrency knob or arm the post-update snap for a
            # rejected call either.
            super().update_version(version, spec, update_mode)
            return
        target_concurrency = getattr(spec, 'target_concurrency_per_replica',
                                     None)
        if target_concurrency is not None:
            # Assign BEFORE the base update runs so any recompute it
            # triggers sees the new knob.
            self.target_concurrency_per_replica = float(target_concurrency)
            self._knob_by_version[version] = float(target_concurrency)
        self.replica_unit = getattr(spec, 'replica_unit', 'physical_backend')
        self.target_utilization_percentage = int(
            getattr(spec, 'target_utilization_percentage', 100))
        self.expected_request_duration_seconds = getattr(
            spec, 'expected_request_duration_seconds', None)
        self.initial_provision_lead_time_seconds = getattr(
            spec, 'initial_provision_lead_time_seconds', None)
        # Measurements describe the workload, not the spec revision, so an
        # update keeps them. Disabling the feature must take effect at once.
        self.adaptive_demand_estimation = (getattr(
            spec, 'adaptive_demand_estimation', True) is not False)
        self.max_scale_up_rate_percentage = getattr(
            spec, 'max_scale_up_rate_percentage', None)
        self.scale_up_rate_min_replicas = getattr(spec,
                                                  'scale_up_rate_min_replicas',
                                                  None)
        self.scale_up_rate_period_seconds = getattr(
            spec, 'scale_up_rate_period_seconds', None)
        adaptive_scale_up = getattr(spec, 'adaptive_scale_up', None)
        self.adaptive_scale_up = (dict(adaptive_scale_up) if isinstance(
            adaptive_scale_up, dict) else None)
        queue_config = getattr(spec, 'lb_request_queue', None) or {}
        self._queue_timeout_seconds = queue_config.get('timeout_seconds')
        self._queue_timeout_thresholds = tuple(
            (int(entry['min_priority']), float(entry['timeout_seconds']))
            for entry in queue_config.get('timeout_seconds_by_priority', ()))
        self.max_scale_down_rate_percentage = int(
            getattr(spec, 'max_scale_down_rate_percentage', 100))
        super().update_version(version, spec, update_mode)
        self._reset_downscale_hysteresis()
        self._downscale_veto_streak = 0
        self._pending_retention_floor = None
        self._pending_capacity_at_adoption = 0
        self._pending_budget_spent = 0
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            # target_num_replicas described the previous version's launch
            # intent.  The new version has no committed capacity yet, so
            # carrying that target across the update would let its first
            # reconciliation bypass the scale-up wave and launch the whole
            # inherited target from zero. Start from a cold zero baseline; the
            # next fresh or stale recompute authorizes at most one configured
            # wave, including any aggregate or per-card floor.
            self.target_num_replicas = 0
            self.target_num_replicas_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            # A retained ceiling belongs to the previous version's committed
            # capacity. Keep the shared timer, but fail closed for the rest of
            # that cooldown instead of granting the new version the old
            # version's unspent authority.
            self._logical_scale_up_wave_ceiling = None
        self._snap_target_on_next_recompute = True
        self._adopt_total_capacity_on_next_recompute = False
        self._last_logical_target_state = None

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Capacity-aware rolling drain in concurrency units.

        Mirrors the instance-aware implementation with demand =
        outstanding work: keep enough READY old replicas to cover the
        demand the ready latest replicas cannot yet serve (never fewer
        than the base class's count rule), and retire the rest. Two
        concurrency-specific twists:
        - SIGNAL GAP: no retirements at all while the demand report is
          stale (a rebuilt controller at target=min_replicas would
          otherwise mass-retire a live fleet before the first sync).
        - Idle-only victims: among READY old replicas, busy ones (fresh
          in-flight > 0 or unknown) are kept as coverage so in-progress jobs
          are never aborted. They become eligible on a later idle tick.
        """
        if not self._fresh_for_tick():
            return []
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        canonical_by_name: dict[str, str] = {}
        ready_latest_by_card: dict[str, int] = {}
        if use_card_targets:
            canonical_by_name = {
                card.casefold(): card
                for card in self._configured_cards_from_profiles()
            }
            ready_latest_by_card = {card: 0 for card in target_by_card}
            for info in replica_infos:
                if (info.version != self.latest_version or not info.is_ready or
                        _replica_is_retiring_card_supply(info)):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is None:
                    continue
                width = (self._ready_capacity(info)
                         if self.replica_unit == 'logical' else 1)
                ready_latest_by_card[card] = (
                    ready_latest_by_card.get(card, 0) + width)
            exact_card_shortfall = any(
                ready_latest_by_card.get(card, 0) < target
                for card, target in target_by_card.items())
            incremental_logical_rollout = (self.replica_unit == 'logical' and
                                           self.update_mode
                                           == serve_utils.UpdateMode.ROLLING)
            if exact_card_shortfall and not incremental_logical_rollout:
                logger.info(
                    'Concurrency rolling drain waiting for '
                    'latest-version exact-card coverage: ready=%s, '
                    'target=%s.', ready_latest_by_card, target_by_card)
                return []
        if (self.replica_unit == 'logical' and
                self.update_mode == serve_utils.UpdateMode.ROLLING):
            old_nonterminal = [
                info for info in replica_infos
                if (info.version < self.latest_version and not info.is_terminal
                    and getattr(info.status_property, 'is_scale_down',
                                False) is not True)
            ]
            if not old_nonterminal:
                return []
            latest_ready_capacity = sum(
                self._ready_capacity(info)
                for info in replica_infos
                if (info.version == self.latest_version and
                    not _replica_is_retiring_card_supply(info)))

            # Old physical rows predate authoritative logical-width reports.
            # Every READY backend nevertheless represents at least one serving
            # slot, so counting one slot per old backend is a conservative
            # coverage floor. Keep enough old READY backends to cover both raw
            # demand and the adopted target while latest-version observed
            # logical capacity comes online. This permits incremental rollout
            # progress even when the complete latest target cannot be placed.
            coverage_target = max(self.get_final_target_num_replicas(),
                                  self._raw_target_num_replicas)
            old_ready = [info for info in old_nonterminal if info.is_ready]
            required_ready_old = max(0, coverage_target - latest_ready_capacity)
            excess_ready_old = max(0, len(old_ready) - required_ready_old)

            # Never-served old replicas add no live coverage and can be
            # retired first. Probe-blipped or occupancy-unknown backends still
            # count as busy through _replica_is_busy and remain protected.
            idle_nonready_old = [
                info for info in old_nonterminal
                if not info.is_ready and not self._replica_is_busy(info)
            ]
            idle_ready_old = [
                info for info in old_ready if not self._replica_is_busy(info)
            ]
            batch_limit = _LOGICAL_ROLLING_UPDATE_MAX_RETIREMENTS_PER_TICK
            selected_nonready = _select_nonterminal_replicas_to_scale_down(
                min(batch_limit, len(idle_nonready_old)), idle_nonready_old)
            remaining_limit = batch_limit - len(selected_nonready)
            ready_limit = min(remaining_limit, excess_ready_old,
                              len(idle_ready_old))
            if use_card_targets and ready_limit > 0:
                old_ready_by_card: dict[str, int] = {}
                for info in old_ready:
                    raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                    card = canonical_by_name.get(raw_card.casefold())
                    if card is not None:
                        old_ready_by_card[card] = (
                            old_ready_by_card.get(card, 0) + 1)
                old_or_target_cards = (set(old_ready_by_card) |
                                       set(target_by_card))
                excess_old_by_card = {
                    card: max(
                        0,
                        old_ready_by_card.get(card, 0) - max(
                            0,
                            target_by_card.get(card, 0) -
                            ready_latest_by_card.get(card, 0),
                        ),
                    ) for card in old_or_target_cards
                }
                ordered_idle_ids = (_select_nonterminal_replicas_to_scale_down(
                    len(idle_ready_old), idle_ready_old))
                idle_by_id = {info.replica_id: info for info in idle_ready_old}
                selected_ready = []
                for replica_id in ordered_idle_ids:
                    info = idle_by_id[replica_id]
                    raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                    card = canonical_by_name.get(raw_card.casefold())
                    if card is None or excess_old_by_card.get(card, 0) <= 0:
                        continue
                    selected_ready.append(replica_id)
                    excess_old_by_card[card] -= 1
                    if len(selected_ready) >= ready_limit:
                        break
            else:
                selected_ready = _select_nonterminal_replicas_to_scale_down(
                    ready_limit, idle_ready_old)
            selected = selected_nonready + selected_ready
            logger.info(
                'Logical rolling drain: coverage_target=%s, '
                'latest_ready_capacity=%s, ready_old=%s, '
                'required_ready_old=%s, idle_nonready_old=%s, '
                'selected=%s.', coverage_target, latest_ready_capacity,
                len(old_ready), required_ready_old, len(idle_nonready_old),
                len(selected))
            return selected
        if self._upscale_pending:
            logger.info('Concurrency autoscaler suppressing outdated-replica '
                        'drain while an upscale is pending hysteresis.')
            return []
        if self.update_mode != serve_utils.UpdateMode.ROLLING:
            return super()._select_outdated_replicas_to_scale_down(
                replica_infos, active_versions)
        old_nonterminal = [
            info for info in replica_infos
            if info.version < self.latest_version and not info.is_terminal
        ]
        if not old_nonterminal:
            return []
        num_ready_latest = 0
        ready_latest_capacity = 0.0
        for info in replica_infos:
            if (info.version == self.latest_version and info.is_ready and
                    not _replica_is_retiring_card_supply(info)):
                num_ready_latest += 1
                ready_latest_capacity += self._replica_capacity(info)
        if num_ready_latest >= self.get_final_target_num_replicas():
            # Enough latest-version replicas: retire the old ones -- but
            # only those not visibly mid-job. The base class retires all
            # of them unconditionally, which for hour-long jobs would
            # abort every in-progress prediction the moment the new
            # fleet is ready; a busy old replica is instead retired on a
            # later tick, once its job finishes and it reports idle.
            return [
                info.replica_id
                for info in old_nonterminal
                if not self._replica_is_busy(info)
            ]

        shortfall = (self._outstanding_work(replica_infos) -
                     ready_latest_capacity)
        # Never keep fewer old replicas than the base class's count rule
        # (target - ready_new): capacity packing with a few big old
        # replicas could otherwise drain the standby pool a low-traffic
        # service relies on for its next request.
        keep_count_floor = min(
            len(old_nonterminal),
            max(0,
                self.get_final_target_num_replicas() - num_ready_latest))

        ready_old = []
        nonready_old = []
        for info in old_nonterminal:
            capacity = self._replica_capacity(info)
            if info.is_ready:
                ready_old.append((capacity, info))
            else:
                nonready_old.append((capacity, info))
        unavailable_versions = self._knob_unavailable_versions_for_tick
        if unavailable_versions:
            logger.info(
                'Concurrency rolling drain waiting for historical capacity '
                'for versions: %s.', sorted(unavailable_versions))
            return []
        # Keep-preference order: busy replicas first (retiring them kills
        # jobs; keeping them retains capacity that is provably serving),
        # then largest capacity (fewest old replicas kept, fastest
        # rollout), replica id as a stable tie-break across ticks. A
        # READY replica missing from the fresh in-flight map counts as
        # busy: the LB may simply not have reported it yet.
        ready_old.sort(key=lambda pair: (not self._replica_is_busy(pair[1]),
                                         -pair[0], pair[1].replica_id))

        keep_ids: set[int] = set()
        covered = 0.0
        for capacity, info in ready_old:
            if covered >= shortfall and len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
            if capacity > 0:
                covered += capacity
        # Not-yet-ready old replicas add no serving capacity; they only
        # count toward the base-class floor (the base helper likewise
        # prefers draining initializing replicas first).
        for _, info in nonready_old:
            if len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
        # Never retire a visibly-busy old replica, regardless of the
        # coverage math: killing it aborts an hour-long job that will
        # re-run from scratch. The busy-first keep-preference above
        # usually keeps them anyway; this makes it a guarantee (they
        # are retired on a later tick, once idle).
        for info in old_nonterminal:
            if self._replica_is_busy(info):
                keep_ids.add(info.replica_id)

        return [
            info.replica_id
            for info in old_nonterminal
            if info.replica_id not in keep_ids
        ]

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate scale-up/down decisions with drain-aware victims.

        The target was already recomputed for this tick in
        generate_scaling_decisions (before the outdated-replica drain).
        """
        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if not info.is_terminal and info.version == self.latest_version:
                latest_nonterminal_replicas.append(info)

        if self.replica_unit == 'logical':
            return self._generate_logical_scaling_decisions(
                replica_infos, latest_nonterminal_replicas)

        scaling_decisions: list[AutoscalerDecision] = []
        self.cold_launch_authority_by_accelerator = {}
        target_num_replicas = self.get_final_target_num_replicas()
        current_num_replicas = len(latest_nonterminal_replicas)
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        if use_card_targets:
            replicas_by_card: dict[str, list[replica_managers.ReplicaInfo]] = {}
            ready_by_card: dict[str, int] = {}
            canonical_by_name = {
                card.casefold(): card
                for card in self._configured_cards_from_profiles()
            }
            for info in latest_nonterminal_replicas:
                if _replica_is_retiring_card_supply(info):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is None:
                    continue
                replicas_by_card.setdefault(card, []).append(info)
                if info.is_ready:
                    ready_by_card[card] = ready_by_card.get(card, 0) + 1
            shortages = {
                card: max(0, target - len(replicas_by_card.get(card, [])))
                for card, target in target_by_card.items()
            }
            self.cold_launch_authority_by_accelerator = {
                card: shortage
                for card, shortage in shortages.items()
                if shortage > 0
            }
            if any(shortages.values()):
                for card, shortage in shortages.items():
                    for _ in range(shortage):
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_UP,
                                target={
                                    'accelerators': {
                                        card: self._configured_gpu_count(card)
                                    }
                                }))
                # Do not retire excess old-card capacity until every target
                # card is actually ready. This is a non-preemptive migration.
                return scaling_decisions
            if not self._compatibility_demand_complete:
                return scaling_decisions
            if not self._fresh_for_tick():
                logger.info('Concurrency autoscaler signal-stale: '
                            'suppressing exact-card scale-down decisions.')
                return scaling_decisions
            if self._upscale_pending:
                logger.info(
                    'Concurrency autoscaler suppressing exact-card '
                    'scale-down while an upscale is pending hysteresis.')
                return scaling_decisions
            all_targets_ready = all(
                ready_by_card.get(card, 0) >= target
                for card, target in target_by_card.items())
            if all_targets_ready:
                for card, replicas in replicas_by_card.items():
                    excess = max(0, len(replicas) - target_by_card.get(card, 0))
                    if excess <= 0:
                        continue
                    eligible = [
                        info for info in replicas
                        if not self._replica_is_busy(info)
                    ]
                    scaling_decisions.extend(
                        _generate_scale_down_decisions(
                            self._select_victims_capacity_and_cost_aware(
                                min(excess, len(eligible)), eligible)))
            return scaling_decisions

        if self.configured_accelerator_shapes:
            # A compatibility-capable service must never fall back to an
            # unshaped launch or card-blind retirement while a mixed-version
            # report leaves the per-card target incomplete.
            logger.info('Concurrency exact-card target is incomplete; '
                        'suppressing card-blind scaling decisions.')
            return scaling_decisions

        if current_num_replicas < target_num_replicas:
            scaling_decisions.extend(
                _generate_scale_up_decisions(
                    target_num_replicas - current_num_replicas, None))
        elif current_num_replicas > target_num_replicas:
            if not self._fresh_for_tick():
                # SIGNAL GAP: never shrink while blind. (The stale-path
                # recompute also never lowers the target, but the target
                # can sit below the live fleet right after a controller
                # rebuild -- this is the guard that actually prevents the
                # kills.)
                logger.info('Concurrency autoscaler signal-stale: suppressing '
                            f'{current_num_replicas - target_num_replicas} '
                            'scale-down decision(s).')
                return scaling_decisions
            if self._upscale_pending:
                logger.info(
                    'Concurrency autoscaler suppressing scale-down while an '
                    'upscale is pending hysteresis.')
                return scaling_decisions
            num_to_scale_down = current_num_replicas - target_num_replicas
            # Drain-aware victim eligibility (see _replica_is_busy): a
            # READY replica may be killed ONLY if the fresh report shows
            # zero in-flight work on it (missing entry counts as busy);
            # non-READY replicas are eligible unless the report shows
            # work on them (probe-blipped mid-job).
            eligible_victims = [
                info for info in latest_nonterminal_replicas
                if not self._replica_is_busy(info)
            ]
            # Clip to the eligible victims and wait otherwise (same
            # pattern as QueueLengthAutoscaler's idle clip): a busy
            # replica finishing its ~1 h job frees up on a later tick.
            actual_num_to_scale_down = min(num_to_scale_down,
                                           len(eligible_victims))
            if actual_num_to_scale_down < num_to_scale_down:
                logger.info(
                    'Concurrency autoscaler clipping scale-down: requested '
                    f'{num_to_scale_down}, but only '
                    f'{len(eligible_victims)} idle/non-ready replicas are '
                    'eligible.')
            if actual_num_to_scale_down > 0:
                scaling_decisions.extend(
                    _generate_scale_down_decisions(
                        self._select_victims_capacity_and_cost_aware(
                            actual_num_to_scale_down, eligible_victims)))

        return scaling_decisions

    def _generate_logical_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        latest_nonterminal_replicas: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate one shaped scale target or capacity-safe retirements.

        Exact-card revalidation needs the complete active fleet so running
        work on an old version remains attributable during a rolling update.
        Committed and ready capacity below stays latest-version-only: old
        replicas prove the transition shape but never satisfy its launch
        target.
        """
        target = self.get_final_target_num_replicas()
        self.cold_launch_authority_by_accelerator = {}
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        if self.configured_accelerator_shapes and not use_card_targets:
            logger.info('Logical concurrency exact-card target is incomplete; '
                        'suppressing card-blind scaling decisions.')
            return []
        canonical_by_name = {
            card.casefold(): card
            for card in (self.configured_accelerator_shapes or target_by_card)
        }

        def _card(info: 'replica_managers.ReplicaInfo') -> str | None:
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            return canonical_by_name.get(raw_card.casefold())

        committed = sum(
            self._committed_capacity(info)
            for info in latest_nonterminal_replicas)
        committed_by_card: dict[str, int] = {}
        for info in latest_nonterminal_replicas:
            card = _card(info)
            if card is not None:
                committed_by_card[card] = (committed_by_card.get(card, 0) +
                                           self._committed_capacity(info))
        launch_budget = self._logical_actuation_wave_budget
        launch_authority_left = launch_budget
        if use_card_targets:
            paid_launch_target = (
                self._logical_paid_launch_target_by_accelerator)
            for card, card_target in target_by_card.items():
                committed_on_card = committed_by_card.get(card, 0)
                target_shortage = max(0, card_target - committed_on_card)
                ownership_shortage = max(
                    0,
                    paid_launch_target.get(card, 0) - committed_on_card)
                # The retained actuation map remains the reconciliation and
                # retirement fence. Paid acquisition is the intersection of
                # that shortage with the fresh supply-aware replacement
                # placement: a warm/old-version card may remain in the former
                # without becoming purchase authority in the latter.
                shortage = min(target_shortage, ownership_shortage)
                if launch_authority_left is not None:
                    shortage = min(shortage, launch_authority_left)
                    launch_authority_left -= shortage
                if shortage > 0:
                    self.cold_launch_authority_by_accelerator[card] = shortage
        paid_card_shortage = bool(use_card_targets and
                                  self.cold_launch_authority_by_accelerator)
        # A proof-free card mismatch with sufficient aggregate committed
        # capacity is only a zero-cost placement preference. Do not emit a
        # no-op reconciliation request for it; an aggregate shortage still
        # emits the exact fence so eligible zero-cost supply can fill it.
        if committed < target or paid_card_shortage:
            if launch_budget is not None and launch_budget > 0:
                # Completing the full exact-card fence can consume the map's
                # transition delta in the first restart tick. Later launch
                # waves still need to advance the cooldown when they are
                # authorized, even though that complete map no longer changes.
                self._record_logical_scale_up_wave(replica_infos, launch_budget)
            replace_unknown_replica_ids = tuple(
                sorted(info.replica_id
                       for info in latest_nonterminal_replicas
                       if getattr(info.status_property, 'is_scale_down',
                                  False) is not True and info.replica_id in
                       self._degraded_capacity_since_by_replica_id and
                       self._committed_capacity(info) == 0))
            launch_priorities_by_accelerator: tuple[tuple[str, int], ...] = ()
            if use_card_targets:
                current_priorities = (
                    self.current_launch_priorities_by_accelerator(
                        target_by_card))
                launch_priorities_by_accelerator = tuple(
                    current_priorities.items())
            return [
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_UP,
                    LogicalScaleTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        launch_budget=launch_budget,
                        target_capacity_by_accelerator=tuple(
                            target_by_card.items()) if use_card_targets else (),
                        accelerator_shapes=tuple(
                            self.configured_accelerator_shapes.items())
                        if use_card_targets else (),
                        replace_unknown_replica_ids=replace_unknown_replica_ids,
                        launch_priority=self.current_launch_priority(),
                        launch_priority_by_accelerator=(
                            launch_priorities_by_accelerator),
                        cold_launch_authority_by_accelerator=(tuple(
                            self.cold_launch_authority_by_accelerator.items())
                                                              if
                                                              use_card_targets
                                                              else None)))
            ]
        if (not self._fresh_for_tick() or self._upscale_pending or
                self._logical_card_transition_pending or
            (use_card_targets and not self._compatibility_demand_complete)):
            return []

        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        candidates = [
            info for info in latest_nonterminal_replicas
            if (getattr(info.status_property, 'is_scale_down', False)
                is not True and not self._replica_is_busy(info))
        ]
        candidates.sort(key=lambda info: (
            _status_rank(info),
            self._ready_capacity(info)
            if info.is_ready else self._committed_capacity(info),
            -self._get_hourly_cost_from_replica_info(info),
            -info.replica_id,
        ))
        remaining_committed = committed
        remaining_ready = sum(
            self._ready_capacity(info) for info in latest_nonterminal_replicas)
        remaining_ready_by_card: dict[str, int] = {}
        for info in latest_nonterminal_replicas:
            card = _card(info)
            if card is not None:
                remaining_ready_by_card[card] = (
                    remaining_ready_by_card.get(card, 0) +
                    self._ready_capacity(info))
        remaining_committed_by_card = dict(committed_by_card)
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        remaining_demand_pending = sum(
            self._committed_capacity(info)
            for info in latest_nonterminal_replicas
            if (info.status in provisioning_statuses and
                not getattr(info, 'reserved_fill', False)))
        decisions: list[AutoscalerDecision] = []
        for info in candidates:
            card = _card(info)
            if use_card_targets and card is None:
                continue
            card_target = target_by_card.get(card, 0) if card is not None else 0
            remaining_card_committed = (remaining_committed_by_card.get(
                card, 0) if card is not None else 0)
            remaining_card_ready = (remaining_ready_by_card.get(card, 0)
                                    if card is not None else 0)
            committed_width = self._committed_capacity(info)
            demand_owned = not getattr(info, 'reserved_fill', False)
            if (info.status in provisioning_statuses and demand_owned and
                    self._pending_retention_floor is not None and
                    remaining_demand_pending - committed_width
                    < self._pending_retention_floor):
                # The frozen episode budget is measured in logical slots. A
                # multi-slot victim that would overspend is conservatively
                # skipped rather than rounded through the percentage cap.
                continue
            if info.is_ready:
                ready_width = self._ready_capacity(info)
                if ready_width <= 0:
                    # A fresh, idle backend that serves no logical slots can be
                    # retired once the OTHER positive ready capacity and the
                    # remaining committed capacity cover the target. This
                    # cleans up both a recovered original's redundant zero-slot
                    # replacement and a timed-out zero-slot original after its
                    # replacement becomes healthy.
                    if (remaining_ready < target or
                            remaining_committed - committed_width < target):
                        continue
                    if (use_card_targets and
                            remaining_card_committed - committed_width
                            < card_target):
                        continue
                else:
                    if (remaining_ready - ready_width < target or
                        (use_card_targets and
                         remaining_card_ready - ready_width < card_target)):
                        continue
                    remaining_ready -= ready_width
                    if card is not None:
                        remaining_ready_by_card[card] = (
                            remaining_ready_by_card.get(card, 0) - ready_width)
            elif remaining_committed - committed_width < target:
                continue
            elif (use_card_targets and
                  remaining_card_committed - committed_width < card_target):
                continue
            remaining_committed -= committed_width
            if card is not None:
                remaining_committed_by_card[card] = (
                    remaining_committed_by_card.get(card, 0) - committed_width)
            if info.status in provisioning_statuses and demand_owned:
                remaining_demand_pending -= committed_width
            decisions.append(
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_DOWN,
                    LogicalScaleDownTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        replica_id=info.replica_id,
                        target_capacity_by_accelerator=(tuple(
                            target_by_card.items()) if use_card_targets else
                                                        ()),
                        accelerator_shapes=(tuple(
                            self.configured_accelerator_shapes.items())
                                            if use_card_targets else ()))))
        self._pending_budget_spent = max(
            0, self._pending_capacity_at_adoption - remaining_demand_pending)
        return decisions

    def _select_victims_capacity_and_cost_aware(
            self, num_to_scale_down: int,
            eligible_victims: list['replica_managers.ReplicaInfo']
    ) -> list[int]:
        """Order victims: status, capacity (asc), then COST (desc).

        Mirrors the instance-aware autoscaler's rationale: among equal
        status, shed the lowest-capacity replicas first (the packing
        target assumes the largest capacities are kept), and among equal
        capacity shed the most EXPENSIVE first -- cloud spot before a
        zero-cost reserved pool. Without the cost key, the routine
        reclaim cycle (research jobs evict the zero-cost fill fleet,
        demand relaunches land on paid spot, jobs finish, fill relaunches
        zero-cost with the newest ids, demand drops) picks the newest --
        zero-cost -- replicas as victims and settles into a stable state
        that pays for spot while free reserved slots sit unfilled.
        Cost must not outrank capacity, same as the instance-aware
        ordering (the target math assumed the biggest replicas survive);
        the capacity key is quantized so float noise cannot split
        mathematically equal capacities away from the cost tiebreak.
        """
        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        ordered = sorted(eligible_victims,
                         key=lambda info: (
                             _status_rank(info),
                             round(self._replica_capacity(info), 9),
                             -self._get_hourly_cost_from_replica_info(info),
                             info.version,
                             -info.replica_id,
                         ))
        return [info.replica_id for info in ordered[:num_to_scale_down]]

    def info(self) -> dict[str, Any]:
        info = super().info()
        if not self.has_recomputed_with_fresh_data():
            info['target_num_replicas_by_accelerator'] = {}
            info['demand_target_by_accelerator'] = {}
            info['warm_retention_target_by_accelerator'] = {}
            info['cold_launch_authority_by_accelerator'] = {}
        in_flight_total = (sum(self._in_flight_by_replica_id.values()) if
                           self._in_flight_by_replica_id is not None else None)
        report_age = (time.time() - self._report_received_at
                      if self._report_received_at is not None else None)
        adaptive_remaining = 0.0
        if self._adaptive_until is not None:
            adaptive_remaining = max(0.0,
                                     self._adaptive_until - time.monotonic())
        info.update({
            'replica_unit': self.replica_unit,
            'adaptive_demand_estimation': self.adaptive_demand_estimation,
            'effective_request_duration_seconds':
                self.effective_request_duration_seconds,
            'effective_provision_lead_seconds':
                self.effective_provision_lead_seconds,
            'measured_duration_seconds': self._measured_duration_seconds,
            'measured_duration_samples': self._measured_duration_samples,
            'provision_lead_samples': len(self._provision_lead_samples),
            'in_flight_total': in_flight_total,
            'queue_depth': self._queue_depth,
            'queue_depth_by_priority': self._queue_depth_by_priority,
            'weighted_queue_work': self._weighted_queue_work,
            'rejected_in_window': self._rejected_in_window,
            'rejected_in_recent_window': self._rejected_in_recent_window,
            'rejected_in_window_by_priority':
                self._rejected_in_window_by_priority,
            'rejected_in_recent_window_by_priority':
                self._rejected_in_recent_window_by_priority,
            'rejected_concurrency': self._rejected_concurrency,
            'unique_job_arrivals_60s': self._unique_job_arrivals_60s,
            'unique_job_arrivals_300s': self._unique_job_arrivals_300s,
            'headerless_arrivals_60s': self._headerless_arrivals_60s,
            'headerless_arrivals_300s': self._headerless_arrivals_300s,
            'offered_arrival_tracking_saturated':
                self._offered_arrival_tracking_saturated,
            'arrival_floor_target': self._arrival_floor_target,
            'raw_target_num_replicas': self._raw_target_num_replicas,
            'committed_capacity': self._latest_committed_capacity,
            'provisioning_capacity': self._latest_provisioning_capacity,
            'target_utilization_percentage': self.target_utilization_percentage,
            'latest_scale_up_wave_at': self._last_scale_up_wave_at,
            'pressure_streak': self._pressure_streak,
            'pressure_latched': self._pressure_latched,
            'pressure_reasons': list(self._pressure_reasons),
            'adaptive_scale_up_active': self._adaptive_scale_up_active(),
            'adaptive_hold_remaining_seconds': adaptive_remaining,
            'downscale_elapsed_seconds': self._downscale_elapsed_seconds(),
            'downscale_delay_seconds': self.downscale_delay_seconds,
            'downscale_veto_reason': self._downscale_veto_reason,
            'downscale_veto_streak': self._downscale_veto_streak,
            'downscale_veto_budget': _MAX_CONSECUTIVE_DOWNSCALE_VETOES,
            'scale_down_allowance': self._last_scale_down_allowance,
            'pending_scale_down_allowance': self._last_pending_allowance,
            'pending_retention_floor': self._pending_retention_floor,
            'pending_budget_spent': self._pending_budget_spent,
            'unknown_in_flight_replicas': len(
                self._unknown_in_flight_replica_ids),
            'report_age_seconds': report_age,
            'compatibility_demand_complete':
                self._compatibility_demand_complete,
        })
        return info

    def _dump_dynamic_states(self) -> dict[str, Any]:
        # Only consumed by the in-process autoscaler swap during
        # update_service (NOT on controller restart). The received-at
        # time is absolute, so a report that crosses the swap simply
        # reads as stale once it exceeds the staleness threshold.
        with self._logical_state_lock:
            return self._dump_dynamic_states_locked()

    def _dump_dynamic_states_locked(self) -> dict[str, Any]:
        return {
            'request_timestamps': self.request_timestamps,
            'in_flight_by_replica_id': self._in_flight_by_replica_id,
            'queue_depth': self._queue_depth,
            'queue_depth_by_priority': self._queue_depth_by_priority,
            'rejected_in_window': self._rejected_in_window,
            'rejected_in_recent_window': self._rejected_in_recent_window,
            'rejected_in_window_by_priority':
                self._rejected_in_window_by_priority,
            'rejected_in_recent_window_by_priority':
                self._rejected_in_recent_window_by_priority,
            'unique_job_arrivals_60s': self._unique_job_arrivals_60s,
            'unique_job_arrivals_300s': self._unique_job_arrivals_300s,
            'headerless_arrivals_60s': self._headerless_arrivals_60s,
            'headerless_arrivals_300s': self._headerless_arrivals_300s,
            'offered_arrival_tracking_saturated':
                self._offered_arrival_tracking_saturated,
            'measured_duration_seconds': self._measured_duration_seconds,
            'measured_duration_samples': self._measured_duration_samples,
            'measured_duration_at': self._measured_duration_at,
            'provision_lead_samples': list(self._provision_lead_samples),
            'provision_lead_at': self._provision_lead_at,
            'pressure_baseline': self._pressure_baseline,
            'pressure_latched': self._pressure_latched,
            'pressure_reasons': self._pressure_reasons,
            'pressure_streak': self._pressure_streak,
            'downscale_veto_streak': self._downscale_veto_streak,
            'adaptive_until': self._adaptive_until,
            'unknown_in_flight_replica_ids': sorted(
                self._unknown_in_flight_replica_ids),
            'report_received_at': self._report_received_at,
            'launch_priority_report_received_at':
                self._launch_priority_report_received_at,
            'reconcile_generation': self._reconcile_generation,
            'observed_slots_by_replica_id': self._observed_slots_by_replica_id,
            'unknown_capacity_replica_ids': sorted(
                self._unknown_capacity_replica_ids),
            'degraded_capacity_since_by_replica_id':
                self._degraded_capacity_since_by_replica_id,
            'last_scale_up_wave_at': self._last_scale_up_wave_at,
            # Always present, including when empty. A QPS replacement uses
            # presence as proof that the source binary could preserve exact
            # arrival constraints; older dumps fail closed on this field.
            'compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.compatibility_profiles],
            'queued_compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.queued_compatibility_profiles],
            'rejected_compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.rejected_compatibility_profiles],
            'compatibility_demand_complete':
                self._compatibility_demand_complete,
            'configured_accelerator_shapes': dict(
                self.configured_accelerator_shapes),
        }

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        # Tolerate dumps from other autoscaler types (an update can
        # change the autoscaler class; e.g. RequestRateAutoscaler only
        # dumps request_timestamps): missing keys keep the stale-start
        # defaults.
        compatibility_arrivals_present = ('compatibility_profiles'
                                          in dynamic_states)
        if compatibility_arrivals_present:
            self.compatibility_profiles = self._parse_compatibility_arrivals(
                dynamic_states.pop('compatibility_profiles'))
        if 'request_timestamps' in dynamic_states:
            self.request_timestamps = dynamic_states.pop('request_timestamps')
        # Estimator state survives a controller restart: re-learning a
        # duration from zero would silently fall back to the configured
        # value for the whole warm-up, which is exactly when a restart
        # under load can least afford an undersized target.
        measured_duration = dynamic_states.pop('measured_duration_seconds',
                                               None)
        if (isinstance(measured_duration, (int, float)) and
                not isinstance(measured_duration, bool) and
                math.isfinite(measured_duration) and measured_duration > 0):
            self._measured_duration_seconds = float(measured_duration)
            samples = dynamic_states.pop('measured_duration_samples', 0)
            self._measured_duration_samples = (
                int(samples) if isinstance(samples, int) and
                not isinstance(samples, bool) and samples >= 0 else 0)
            observed_at = dynamic_states.pop('measured_duration_at', None)
            self._measured_duration_at = (float(observed_at) if isinstance(
                observed_at,
                (int, float)) and not isinstance(observed_at, bool) else None)
        else:
            dynamic_states.pop('measured_duration_samples', None)
            dynamic_states.pop('measured_duration_at', None)
        lead_samples = dynamic_states.pop('provision_lead_samples', None)
        if isinstance(lead_samples, list):
            self._provision_lead_samples = [
                float(sample)
                for sample in lead_samples
                if (isinstance(sample, (int, float)) and not isinstance(
                    sample, bool) and math.isfinite(sample) and sample > 0)
            ][-constants.AUTOSCALER_ADAPTIVE_LEAD_SAMPLE_CAP:]
        lead_at = dynamic_states.pop('provision_lead_at', None)
        if (isinstance(lead_at, (int, float)) and
                not isinstance(lead_at, bool)):
            self._provision_lead_at = float(lead_at)
        if 'in_flight_by_replica_id' in dynamic_states:
            self._in_flight_by_replica_id = dynamic_states.pop(
                'in_flight_by_replica_id')
        if 'queue_depth' in dynamic_states:
            self._queue_depth = dynamic_states.pop('queue_depth')
        if 'queue_depth_by_priority' in dynamic_states:
            self._queue_depth_by_priority = dynamic_states.pop(
                'queue_depth_by_priority')
        if 'rejected_in_window' in dynamic_states:
            self._rejected_in_window = dynamic_states.pop('rejected_in_window')
        if 'rejected_in_recent_window' in dynamic_states:
            self._rejected_in_recent_window = dynamic_states.pop(
                'rejected_in_recent_window')
        if 'queued_compatibility_profiles' in dynamic_states:
            self.queued_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    dynamic_states.pop('queued_compatibility_profiles')))
        if 'rejected_compatibility_profiles' in dynamic_states:
            self.rejected_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    dynamic_states.pop('rejected_compatibility_profiles'),
                    include_recent_count=True))
        if 'compatibility_demand_complete' in dynamic_states:
            self._compatibility_demand_complete = bool(
                dynamic_states.pop('compatibility_demand_complete'))
        if 'configured_accelerator_shapes' in dynamic_states:
            source_shapes = dynamic_states.pop('configured_accelerator_shapes')
            if isinstance(source_shapes, dict):
                self.configured_accelerator_shapes = {
                    str(card): int(count)
                    for card, count in source_shapes.items()
                    if isinstance(card, str) and card and
                    isinstance(count, int) and not isinstance(count, bool) and
                    count > 0
                }
        if (not self.configured_accelerator_shapes or
                not compatibility_arrivals_present):
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.rejected_compatibility_profiles = []
            self._compatibility_demand_complete = False
        for field in ('rejected_in_window_by_priority',
                      'rejected_in_recent_window_by_priority',
                      'unique_job_arrivals_60s', 'unique_job_arrivals_300s',
                      'headerless_arrivals_60s', 'headerless_arrivals_300s',
                      'offered_arrival_tracking_saturated', 'pressure_baseline',
                      'pressure_latched', 'pressure_reasons', 'pressure_streak',
                      'downscale_veto_streak', 'adaptive_until'):
            key = field
            if key in dynamic_states:
                setattr(self, f'_{field}', dynamic_states.pop(key))
        if 'unknown_in_flight_replica_ids' in dynamic_states:
            self._unknown_in_flight_replica_ids = {
                int(replica_id) for replica_id in dynamic_states.pop(
                    'unknown_in_flight_replica_ids')
            }
        if 'report_received_at' in dynamic_states:
            self._report_received_at = dynamic_states.pop('report_received_at')
        if 'launch_priority_report_received_at' in dynamic_states:
            priority_report_received_at = dynamic_states.pop(
                'launch_priority_report_received_at')
            self._launch_priority_report_received_at = (
                float(priority_report_received_at)
                if isinstance(priority_report_received_at, (int, float)) and
                not isinstance(priority_report_received_at, bool) else None)
        if 'last_scale_up_wave_at' in dynamic_states:
            self._last_scale_up_wave_at = dynamic_states.pop(
                'last_scale_up_wave_at')
        if 'reconcile_generation' in dynamic_states:
            self._reconcile_generation = int(
                dynamic_states.pop('reconcile_generation'))
        if 'observed_slots_by_replica_id' in dynamic_states:
            self._observed_slots_by_replica_id = {
                int(replica_id): int(slots) for replica_id, slots in
                dynamic_states.pop('observed_slots_by_replica_id').items()
            }
        if 'unknown_capacity_replica_ids' in dynamic_states:
            self._unknown_capacity_replica_ids = {
                int(replica_id) for replica_id in dynamic_states.pop(
                    'unknown_capacity_replica_ids')
            }
        degraded_state = dynamic_states.pop(
            'degraded_capacity_since_by_replica_id',
            dynamic_states.pop('unknown_capacity_since_by_replica_id', None))
        if degraded_state is not None:
            self._degraded_capacity_since_by_replica_id = {
                int(replica_id): float(since)
                for replica_id, since in degraded_state.items()
            }
        if dynamic_states:
            logger.info(f'Remaining dynamic states: {dynamic_states}')


class FallbackRequestRateAutoscaler(RequestRateAutoscaler):
    """FallbackRequestRateAutoscaler

    Autoscale based on request rate. It adds additional ability to
    RequestRateAutoscaler for having spot with on-demand fallback.

    When spec.base_ondemand_fallback_replicas is set, we make sure
    there are at least spec.base_ondemand_fallback_replicas on-demands
    to be always there to provide basic guarantee for the availability.

    When spec.dynamic_ondemand_fallback is set, on-demand instances
    will be scheduled to provision for any preempted spot instance, i.e.,
    on-demand instance are used as dynamic fallback of spot.
    """

    # job_recovery field is checked earlier in core
    SPOT_OVERRIDE = {'use_spot': True}
    ONDEMAND_OVERRIDE = {'use_spot': False}

    def _setup_fallback_options(self,
                                spec: 'service_spec.SkyServiceSpec') -> None:
        self.base_ondemand_fallback_replicas: int = (
            spec.base_ondemand_fallback_replicas
            if spec.base_ondemand_fallback_replicas is not None else 0)
        # Assert: Either dynamic_ondemand_fallback is set
        # or base_ondemand_fallback_replicas is greater than 0.
        assert spec.use_ondemand_fallback
        self.dynamic_ondemand_fallback: bool = (
            spec.dynamic_ondemand_fallback
            if spec.dynamic_ondemand_fallback is not None else False)

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the fallback request rate autoscaler.

        Variables:
            base_ondemand_fallback_replicas: Minimum number of on-demand
                replicas to be always there.
            dynamic_ondemand_fallback: Whether to dynamically provision
                on-demand instances for preempted spot instances.
        """
        super().__init__(service_name, spec, version)
        self._setup_fallback_options(spec)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't reset fallback
            # options from a stale spec either.
            super().update_version(version, spec, update_mode=update_mode)
            return
        super().update_version(version, spec, update_mode=update_mode)
        self._setup_fallback_options(spec)

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on request rate, with on-demand
        fallback.

        The autoscaler will make sure there are at least
        `base_ondemand_fallback_replicas` on-demand replicas to be always there,
        so the service can provide basic guarantee for the availability.
        """

        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas = list(
            filter(
                lambda info: not info.is_terminal and info.version == self.
                latest_version, replica_infos))
        num_nonterminal_spot, num_ready_spot = 0, 0
        num_nonterminal_ondemand, num_ready_ondemand = 0, 0

        for info in latest_nonterminal_replicas:
            if info.is_spot:
                if info.status == serve_state.ReplicaStatus.READY:
                    num_ready_spot += 1
                num_nonterminal_spot += 1
            else:
                if info.status == serve_state.ReplicaStatus.READY:
                    num_ready_ondemand += 1
                num_nonterminal_ondemand += 1

        logger.info(
            f'Number of alive spot instances: {num_nonterminal_spot}, '
            f'Number of ready spot instances: {num_ready_spot}, '
            f'Number of alive on-demand instances: {num_nonterminal_ondemand}, '
            f'Number of ready on-demand instances: {num_ready_ondemand}')

        scaling_decisions: list[AutoscalerDecision] = []
        all_replica_ids_to_scale_down: list[int] = []

        # Decide how many spot instances to launch.
        num_spot_to_provision = (self.get_final_target_num_replicas() -
                                 self.base_ondemand_fallback_replicas)
        if num_nonterminal_spot < num_spot_to_provision:
            # Not enough spot instances, scale up.
            num_spot_to_scale_up = (num_spot_to_provision -
                                    num_nonterminal_spot)
            logger.info('Number of spot instances to scale up: '
                        f'{num_spot_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_spot_to_scale_up,
                                             self.SPOT_OVERRIDE))
        elif num_nonterminal_spot > num_spot_to_provision:
            # Too many spot instances, scale down.
            # Get the replica to scale down with _select_replicas_to_scale_down
            num_spot_to_scale_down = (num_nonterminal_spot -
                                      num_spot_to_provision)
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_spot_to_scale_down,
                    filter(lambda info: info.is_spot,
                           latest_nonterminal_replicas)))
            logger.info('Number of spot instances to scale down: '
                        f'{num_spot_to_scale_down} {replicas_to_scale_down}')
            all_replica_ids_to_scale_down.extend(replicas_to_scale_down)

        # Decide how many on-demand instances to launch.
        num_ondemand_to_provision = self.base_ondemand_fallback_replicas
        if self.dynamic_ondemand_fallback:
            # `num_ready_spot` instead of `num_nonterminal_spot`
            # because the provisioning spot can fail to UP due to the capacity
            # issue, and on-demand should fill the gap between the required
            # number of spot and ready spot.
            # When scaling down spot instances, it is possible that the number
            # of ready spot is more than the number of spot to provision, thus
            # generate a negative number. In this case, we don't need to
            # provision on-demand instances.
            num_ondemand_to_provision += max(
                0, num_spot_to_provision - num_ready_spot)

        # Make sure we don't launch on-demand fallback for
        # overprovisioned replicas.
        num_ondemand_to_provision = min(num_ondemand_to_provision,
                                        self.target_num_replicas)
        if num_ondemand_to_provision > num_nonterminal_ondemand:
            num_ondemand_to_scale_up = (num_ondemand_to_provision -
                                        num_nonterminal_ondemand)
            logger.info('Number of on-demand instances to scale up: '
                        f'{num_ondemand_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_ondemand_to_scale_up,
                                             self.ONDEMAND_OVERRIDE))
        else:
            num_ondemand_to_scale_down = (num_nonterminal_ondemand -
                                          num_ondemand_to_provision)
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_ondemand_to_scale_down,
                    filter(lambda info: not info.is_spot,
                           latest_nonterminal_replicas)))
            logger.info(
                'Number of on-demand instances to scale down: '
                f'{num_ondemand_to_scale_down} {replicas_to_scale_down}')

            all_replica_ids_to_scale_down.extend(replicas_to_scale_down)

        scaling_decisions.extend(
            _generate_scale_down_decisions(all_replica_ids_to_scale_down))

        return scaling_decisions


class QueueLengthAutoscaler(_AutoscalerWithHysteresis):
    """QueueLengthAutoscaler: Autoscale pools based on queue length.

    Scales pool workers based on the number of pending jobs in the queue.
    When queue length exceeds the threshold, scales up by 1 worker.
    When queue length is below the threshold, scales down by 1 worker.
    Uses hysteresis to prevent rapid scaling decisions.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the queue length autoscaler.

        Variables:
            queue_length_threshold: Threshold for queue length to trigger
            scaling up or down.
            service_name: The pool name (used to query pending jobs).
        """
        super().__init__(service_name, spec, version)
        # Use default threshold if not specified
        self.queue_length_threshold = (
            spec.queue_length_threshold
            if spec.queue_length_threshold is not None else
            constants.AUTOSCALER_DEFAULT_QUEUE_LENGTH_THRESHOLD)
        self._service_name: str = service_name
        logger.info(f'QueueLengthAutoscaler for pool "{service_name}": '
                    f'min_replicas={self.min_replicas}, '
                    f'max_replicas={self.max_replicas}, '
                    f'queue_length_threshold={self.queue_length_threshold}')

    def _calculate_target_num_replicas(self) -> int:
        """Calculate target number of replicas based on queue length."""
        queue_length = managed_job_state.get_pending_jobs_count_by_pool(
            self._service_name)
        current_num_replicas = self.target_num_replicas

        logger.info(f'[QueueLengthAutoscaler] Pool "{self._service_name}": '
                    f'queue_length={queue_length}, '
                    f'threshold={self.queue_length_threshold}, '
                    f'current_target_replicas={current_num_replicas}, '
                    f'min_replicas={self.min_replicas}, '
                    f'max_replicas={self.max_replicas}')

        # Determine target based on queue length vs threshold
        if queue_length == 0:
            # There are no pending jobs, we should quickly scale down to 0.
            target_num_replicas = 0
            decision = 'SCALE_DOWN_TO_ZERO'
        elif queue_length > self.queue_length_threshold:
            # Scale up by 1
            # TODO(lloyd): we probably want support for scaling up by more than
            # 1 in the future. We are punting on this currently because without
            # an understanding of the workload the right number of replicas to
            # scale up by is not clear and the user can just tweak the upscale
            # delay to control the rate of scaling up.
            target_num_replicas = current_num_replicas + 1
            decision = 'SCALE_UP'
        elif queue_length < self.queue_length_threshold:
            # Scale down by 1
            target_num_replicas = current_num_replicas - 1
            decision = 'SCALE_DOWN'
        else:
            # Queue length equals threshold, keep current
            target_num_replicas = current_num_replicas
            decision = 'NO_CHANGE'
        logger.info(f'[QueueLengthAutoscaler] Decision: {decision} '
                    f'{current_num_replicas} -> {target_num_replicas}')

        # Special case: if target_num_replicas is 0 and queue_length is greater
        # than 0, we should not scale down to 0. This is to prevent the service
        # from scaling to zero when there are jobs in the queue.
        if target_num_replicas == 0 and queue_length > 0:
            target_num_replicas = 1
            logger.info('Preventing scale to zero since there are jobs in the'
                        f'queue: {queue_length}')

        clipped_target = self._clip_target_num_replicas(target_num_replicas)
        if clipped_target != target_num_replicas:
            logger.info(f'[QueueLengthAutoscaler] Clipped target: '
                        f'{target_num_replicas} -> {clipped_target} '
                        f'(bounds: [{self.min_replicas}, {self.max_replicas}])')

        return clipped_target

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't update the
            # queue threshold from a stale spec either.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        # Update threshold.
        if isinstance(spec.queue_length_threshold, int):
            self.queue_length_threshold = spec.queue_length_threshold

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling.

        Not needed for queue-based autoscaling, we query the job queue directly.
        """
        pass

    def _get_idle_replicas(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        cluster_job_counts: dict[str, int] | None = None,
    ) -> list['replica_managers.ReplicaInfo']:
        """Get replicas that have no active jobs (idle replicas).

        Args:
            replica_infos: List of replica information to check.

        Returns:
            List of replicas that have no active jobs running on them.
        """
        if cluster_job_counts is None:
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    self._service_name))
        idle_replicas = []
        for info in replica_infos:
            active_job_count = cluster_job_counts.get(info.cluster_name, 0)
            if active_job_count == 0:
                idle_replicas.append(info)
                logger.debug(
                    f'[QueueLengthAutoscaler] Replica {info.replica_id} '
                    f'({info.cluster_name}) is idle (no active jobs)')
            else:
                logger.debug(
                    f'[QueueLengthAutoscaler] Replica {info.replica_id} '
                    f'({info.cluster_name}) has {active_job_count} active '
                    'jobs,'
                    ' skipping for scale-down')
        return idle_replicas

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on queue length.

        Overrides parent to ensure we only scale down replicas that are idle
        (not running any jobs).
        """
        # Use standard hysteresis-based logic
        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if info.version == self.latest_version:
                if not info.is_terminal:
                    latest_nonterminal_replicas.append(info)

        scaling_decisions: list[AutoscalerDecision] = []

        # Case 1. when latest_nonterminal_replicas is less
        # than num_to_provision, we always scale up new replicas.
        target_num_replicas = self.get_final_target_num_replicas()
        if len(latest_nonterminal_replicas) < target_num_replicas:
            num_replicas_to_scale_up = (target_num_replicas -
                                        len(latest_nonterminal_replicas))
            logger.info('[QueueLengthAutoscaler] Number of replicas to scale up'
                        f': {num_replicas_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_replicas_to_scale_up, None))

        # Case 2: when latest_nonterminal_replicas is more
        # than target_num_replicas, we scale down new replicas.
        # IMPORTANT: Only scale down replicas that are idle (no active jobs).
        replicas_to_scale_down = []
        if len(latest_nonterminal_replicas) > target_num_replicas:
            num_replicas_to_scale_down = (len(latest_nonterminal_replicas) -
                                          target_num_replicas)
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    self._service_name))

            # Get idle replicas (replicas with no active jobs)
            idle_replicas = self._get_idle_replicas(latest_nonterminal_replicas,
                                                    cluster_job_counts)
            num_idle_replicas = len(idle_replicas)

            # Clip the number of replicas to scale down to the number of idle
            # replicas.
            actual_num_to_scale_down = min(num_replicas_to_scale_down,
                                           num_idle_replicas)

            if actual_num_to_scale_down < num_replicas_to_scale_down:
                logger.info(
                    f'[QueueLengthAutoscaler] Clipping scale-down: requested '
                    f'{num_replicas_to_scale_down} replicas, but only '
                    f'{num_idle_replicas} idle replicas available. Scaling down'
                    f' {actual_num_to_scale_down} replicas.')

            if actual_num_to_scale_down > 0:
                # Select replicas to scale down from idle replicas only
                replicas_to_scale_down = (
                    _select_nonterminal_replicas_to_scale_down(
                        actual_num_to_scale_down, idle_replicas,
                        self._service_name, cluster_job_counts))
                logger.info(
                    f'[QueueLengthAutoscaler] Number of replicas to scale down:'
                    f' {actual_num_to_scale_down} {replicas_to_scale_down}')
            elif num_replicas_to_scale_down > 0:
                logger.info(
                    f'[QueueLengthAutoscaler] Cannot scale down: requested '
                    f'{num_replicas_to_scale_down} replicas, but all replicas '
                    'have active jobs. Skipping scale-down.')

        scaling_decisions.extend(
            _generate_scale_down_decisions(replicas_to_scale_down))

        return scaling_decisions

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler.

        Hysteresis state is handled by base class, no additional state needed.
        """
        return {}

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler.

        Hysteresis state is handled by base class, no additional state needed.
        """
        pass
