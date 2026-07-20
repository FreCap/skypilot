"""Autoscalers: perform autoscaling by monitoring metrics."""
import bisect
from collections.abc import Iterable
import copy
import dataclasses
import enum
import math
import threading
import time
import typing
from typing import Any

from sky import global_user_state
from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.serve import constants
from sky.serve import reserved_capacity
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils import operator_notifications

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers
    from sky.serve import service_spec

logger = sky_logging.init_logger(__name__)


class AutoscalerDecisionOperator(enum.Enum):
    SCALE_UP = 'scale_up'
    SCALE_DOWN = 'scale_down'


class AutoscalerDecisionReason(enum.Enum):
    COST_REBALANCE = 'cost_rebalance'


@dataclasses.dataclass(frozen=True)
class LogicalScaleTarget:
    """One capacity target derived from an immutable LB generation."""

    version: int
    reconcile_generation: int
    target_capacity: int
    replace_unknown_replica_ids: tuple[int, ...] = ()


@dataclasses.dataclass(frozen=True)
class LogicalScaleDownTarget:
    """One backend retirement selected against a logical target."""

    version: int
    reconcile_generation: int
    target_capacity: int
    replica_id: int


@dataclasses.dataclass
class AutoscalerDecision:
    """Autoscaling decisions.

    |------------------------------------------------------------------------|
    | Operator   | TargetType                | Meaning                       |
    |------------|---------------------------|-------------------------------|
    | SCALE_UP   | Optional[Dict[str, Any]   | Resource override to add      |
    |------------|---------------------------|-------------------------------|
    | SCALE_DOWN | int                       | Replica id to remove          |
    |------------------------------------------------------------------------|
    """
    operator: AutoscalerDecisionOperator
    target: (dict[str, Any] | None | int | LogicalScaleTarget |
             LogicalScaleDownTarget)
    reason: AutoscalerDecisionReason | None

    # TODO(MaoZiming): Add a doc to elaborate on autoscaling policies.
    def __init__(self,
                 operator: AutoscalerDecisionOperator,
                 target: (dict[str, Any] | None | int | LogicalScaleTarget |
                          LogicalScaleDownTarget),
                 reason: AutoscalerDecisionReason | None = None):
        if operator == AutoscalerDecisionOperator.SCALE_UP:
            assert (target is None or isinstance(target,
                                                 (dict, LogicalScaleTarget)))
        else:
            assert isinstance(target, (int, LogicalScaleDownTarget))
        self.operator = operator
        self.target = target
        self.reason = reason

    def __repr__(self) -> str:
        return (f'AutoscalerDecision({self.operator}, {self.target}, '
                f'reason={self.reason})')


def _scale_down_replica_id(target: int | LogicalScaleDownTarget) -> int:
    return target if isinstance(target, int) else target.replica_id


def _generate_scale_up_decisions(
        num: int, target: dict[str, Any] | None) -> list[AutoscalerDecision]:
    return [
        AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                           copy.copy(target)) for _ in range(num)
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
        self._cost_rebalance_replica_cost_cache: dict[int, float] = {}

    def get_final_target_num_replicas(self) -> int:
        """Get the final target number of replicas."""
        if self.num_overprovision is None:
            return self.target_num_replicas
        return self.target_num_replicas + self.num_overprovision

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
        self.cost_rebalance = bool(getattr(spec, 'cost_rebalance', False))
        self.cost_rebalance_min_savings_fraction = float(
            getattr(spec, 'cost_rebalance_min_savings_fraction', 0.3))
        self.cost_rebalance_max_parallel_replacements = int(
            getattr(spec, 'cost_rebalance_max_parallel_replacements', 1))
        self.cost_rebalance_stabilization_seconds = float(
            getattr(spec, 'cost_rebalance_stabilization_seconds', 300.0))
        self._cost_rebalance_candidate_since.clear()

    def set_spot_placer(self, placer: spot_placer.SpotPlacer | None) -> None:
        """Publish ReplicaManager's live placement/bench state for this tick."""
        self._cost_rebalance_spot_placer = placer

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling."""
        raise NotImplementedError

    def collect_reserved_capacity(self,
                                  free_slots: int,
                                  zero_cost_location_keys: list[dict[str, Any]],
                                  timestamp: float,
                                  grant: int | None = None,
                                  grant_epoch: int | None = None,
                                  grant_pool_key: str | None = None) -> None:
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

    def _fill_capacity_units(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Autoscaling units represented by one row for fill accounting."""
        del info
        return 1

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

        - Surplus scale-ups beyond max(current, demand target) carry the
          sentinel override so the launch path pins them to zero-cost
          ACTIVE locations only (and skips entirely when none is).
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
        # Zero-cost accounting is version-asymmetric by design; the
        # three roles use different version scopes:
        # - LAUNCH BASELINE: latest-version only. The baseline below is
        #   max(num_latest_nonterminal, demand_target), which is
        #   latest-only, so old-version zero-cost replicas (a rolling
        #   update draining its previous fleet) would inflate the launch
        #   target by replicas the baseline never sees -- compounding
        #   fill launches every tick.
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
        num_latest_nonterminal = 0
        for info in replica_infos:
            if info.is_terminal:
                continue
            capacity_units = self._fill_capacity_units(info)
            is_latest = info.version == self.latest_version
            if is_latest:
                num_latest_nonterminal += capacity_units
            if self._replica_on_zero_cost_location(info):
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
        if surplus_covered <= 0:
            return decisions
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
        zero_cost_decision_ids = []
        for idx, decision in enumerate(decisions):
            if decision.operator == AutoscalerDecisionOperator.SCALE_DOWN:
                assert isinstance(decision.target,
                                  (int, LogicalScaleDownTarget))
                victim = id_to_info.get(_scale_down_replica_id(decision.target))
                if (victim is not None and
                        self._replica_on_zero_cost_location(victim)):
                    zero_cost_decision_ids.append(idx)
        suppressed_ids = set(zero_cost_decision_ids[-surplus_covered:])
        result: list[AutoscalerDecision] = [
            decision for idx, decision in enumerate(decisions)
            if idx not in suppressed_ids
        ]
        # Launch target: latest-version zero-cost replicas only (see the
        # version-asymmetry note above), against the latest-only
        # baseline.
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
        num_fill_up = (fill_target_launch -
                       max(num_latest_nonterminal, demand_target))
        if num_fill_up > 0:
            logger.info(f'Reserved-capacity fill: launch target '
                        f'{fill_target_launch} (latest zero-cost replicas '
                        f'{zero_cost_latest} + spendable free slots '
                        f'{spendable_free_slots}), demand target '
                        f'{demand_target}; scaling up {num_fill_up} '
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
            result.extend(
                _generate_scale_up_decisions(num_fill_up, fill_override))
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
            candidate_capacity = self._cost_rebalance_location_capacity(
                location)
            if candidate_capacity + 1e-9 < incumbent_capacity:
                continue
            candidate_cost = placer.cost_per_hour(location)
            if not math.isfinite(candidate_cost) or candidate_cost < 0:
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
        # policy.  In that case keep the incumbent and drain the replacement;
        # otherwise wait for replacement readiness, then strictly drain the
        # incumbent.  COST_REBALANCE means off-route now, terminate only after
        # the LB proves zero occupancy.
        for victim_id, replacement in sorted(pairs.items()):
            victim = by_id[victim_id]
            if self.cost_rebalance:
                if (replacement.is_ready and
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
            self._cost_rebalance_candidate_since.clear()
            return decisions
        if ordinary_decisions:
            self._cost_rebalance_candidate_since.clear()
            return decisions
        if any(not info.is_terminal and info.version != self.latest_version
               for info in replica_infos):
            self._cost_rebalance_candidate_since.clear()
            return decisions

        slots = self.cost_rebalance_max_parallel_replacements - len(pairs)
        paired_ids = set(pairs)
        candidates = [
            info for info in replica_infos
            if (info.version == self.latest_version and info.is_ready and
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
        now = time.monotonic()
        current_candidate_keys: set[tuple[int, spot_placer.Location]] = set()
        for incumbent in candidates:
            location = self._best_cost_rebalance_candidate(
                incumbent, active_locations, location_load)
            if location is None:
                continue
            key = (incumbent.replica_id, location)
            current_candidate_keys.add(key)
            first_seen = self._cost_rebalance_candidate_since.setdefault(
                key, now)
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
            for info in latest_replicas:
                if info.status_property.unrecoverable_failure():
                    if previous_versions:
                        self._notify_rollout_blocked(max(previous_versions))
                    # Stop scaling if one of replica of the latest version
                    # failed, it is likely that a fatal error happens to the
                    # user application and may lead to a infinte termination
                    # and restart.
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
        states['reserved_capacity_fill_state'] = {
            'fill_free_slots': self._fill_free_slots,
            'fill_last_raw_free_slots': self._fill_last_raw_free_slots,
            'fill_zero_cost_location_keys': [
                location.to_pickleable()
                for location in self._fill_zero_cost_locations
            ],
            'fill_snapshot_time': self._fill_snapshot_time,
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
            self._fill_free_slots = fill_state.get('fill_free_slots', 0)
            self._fill_last_raw_free_slots = fill_state.get(
                'fill_last_raw_free_slots')
            self._fill_zero_cost_locations = [
                location for location in
                (spot_placer.Location.from_pickleable(key)
                 for key in fill_state.get('fill_zero_cost_location_keys', []))
                if location is not None
            ]
            self._fill_snapshot_time = fill_state.get('fill_snapshot_time')
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
        self.scale_down_threshold: int = int(
            downscale_delay_seconds /
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
        gpu_type = 'unknown'
        gpu_count = 1
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
        if gpu_type == 'unknown':
            accelerators = (replica_info.resources_override or
                            {}).get('accelerators')
            if isinstance(accelerators, dict) and accelerators:
                gpu_type = next(iter(accelerators))
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
        self.compatibility_profiles: list[dict[str, Any]] = []
        # Outstanding queue demand is a last-writer-wins gauge. Unlike arrival
        # profiles, it must be replaced on every authoritative LB report rather
        # than accumulated across the QPS window.
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
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
        self.configured_accelerator_shapes = {
            str(card): int(count)
            for card, count in shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        }

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

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        super().collect_request_information(request_aggregator_info)
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
        cutoff = time.time() - self.qps_window_size
        self.compatibility_profiles = [
            profile for profile in self.compatibility_profiles
            if profile['timestamp'] >= cutoff
        ]

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Preserve exact-card demand across an autoscaler replacement."""
        states = super()._dump_dynamic_states()
        states['compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.compatibility_profiles]
        states['queued_compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.queued_compatibility_profiles]
        return states

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Restore exact-card arrivals and the replaceable queue gauge."""
        profiles = dynamic_states.pop('compatibility_profiles', [])
        queued_profiles = dynamic_states.pop('queued_compatibility_profiles',
                                             [])
        super()._load_dynamic_states(dynamic_states)
        self.compatibility_profiles = []
        self.queued_compatibility_profiles = []
        self.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': profiles,
            'queued_requests_by_compatibility': queued_profiles,
        })

    def generate_scaling_decisions(
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

        target_by_card = self.target_num_replicas_by_accelerator
        use_card_targets = (bool(target_by_card) and
                            self.num_overprovision is None and
                            sum(target_by_card.values()) == target_num_replicas)
        if use_card_targets:
            replicas_by_card: dict[str, list[replica_managers.ReplicaInfo]] = {}
            ready_by_card: dict[str, int] = {}
            for info in latest_nonterminal_replicas:
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

    def _cold_paid_card_order(self, configured_cards: list[str]) -> list[str]:
        """Order cold cards by live paid placement cost when available."""
        placer = self._cost_rebalance_spot_placer
        if placer is None:
            return list(configured_cards)
        canonical_by_name = {card.casefold(): card for card in configured_cards}
        paid_costs: dict[str, float] = {}
        try:
            active_locations = placer.active_locations()
        except Exception:  # pylint: disable=broad-except
            return list(configured_cards)
        for location in active_locations:
            raw_card, gpu_count = self._location_gpu_shape(location)
            card = canonical_by_name.get(raw_card.casefold())
            if (card is None or gpu_count != self._configured_gpu_count(card)):
                continue
            try:
                hourly_cost = float(placer.cost_per_hour(location))
            except Exception:  # pylint: disable=broad-except
                continue
            # Zero-cost supply has its own fresh-capacity tier. It must not
            # make a saturated reserved-only card look like a cold option.
            if not math.isfinite(hourly_cost) or hourly_cost <= 0:
                continue
            paid_costs[card] = min(hourly_cost,
                                   paid_costs.get(card, float('inf')))
        service_order = {
            card: index for index, card in enumerate(configured_cards)
        }
        return sorted(configured_cards,
                      key=lambda card:
                      (paid_costs.get(card, float('inf')), service_order[card]))

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
    ) -> dict[str, int]:
        """Allocate recent demand to exact cards, priority first."""
        configured_cards = self._configured_cards_from_profiles()
        floors_by_name = {
            card.casefold(): floor
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        target = {
            card: int(floors_by_name.get(card.casefold(), 0))
            for card in configured_cards
        }
        capacities = {
            card: self._get_target_qps_for_gpu_shape(
                card,
                self._configured_gpu_count(card),
                version=self.latest_version) for card in configured_cards
        }
        # Hard per-card floors are serving capacity, not idle ballast. Consume
        # their capacity against the highest-priority compatible demand first.
        unused_capacity = {
            card: target[card] * max(0.0, capacities[card])
            for card in configured_cards
        }

        ready_zero_cost: dict[str, int] = {card: 0 for card in configured_cards}
        ready: dict[str, int] = {card: 0 for card in configured_cards}
        provisioning: dict[str, int] = {card: 0 for card in configured_cards}
        for info in replica_infos:
            if info.is_terminal or info.version != self.latest_version:
                continue
            card, _ = self._get_gpu_shape_from_replica_info(info)
            if card not in ready:
                continue
            if info.is_ready:
                ready[card] += 1
                if bool(getattr(info, 'is_zero_cost', False)):
                    ready_zero_cost[card] += 1
            else:
                # Every nonterminal non-ready row is committed future
                # capacity. It prevents duplicate launches, but the decision
                # path below does not let it authorize scale-down.
                provisioning[card] += 1

        cold_order = self._cold_paid_card_order(configured_cards)
        cold_rank = {card: index for index, card in enumerate(cold_order)}
        canonical_by_name = {card.casefold(): card for card in configured_cards}
        grouped: dict[tuple[int, tuple[str, ...]], int] = {}
        for profile in (self.compatibility_profiles +
                        self.queued_compatibility_profiles):
            requested = {
                canonical_by_name[card.casefold()]
                for card in profile['compatible_accelerators']
                if card.casefold() in canonical_by_name
            }
            # Compatibility is a set. Canonicalize by service cost order so
            # [L4, A100] and [A100, L4] share one bounded demand profile and
            # caller order cannot force the cold paid-card choice.
            cards = tuple(card for card in cold_order
                          if card in requested and card in capacities)
            if not cards:
                continue
            key = (profile['priority'], cards)
            grouped[key] = grouped.get(key, 0) + int(profile.get('count', 1))

        # Cumulative marginal-supply tiers. Comparing each cumulative count to
        # the target already assigned to the card consumes each unit exactly
        # once while preserving the preference order:
        # ready reserved -> any ready -> provisioning -> free reserved -> cold.
        planned_by_tier = [dict(ready_zero_cost), dict(ready)]
        planned_by_tier.append({
            card: ready.get(card, 0) + provisioning.get(card, 0)
            for card in configured_cards
        })
        planned_by_tier.append({
            card: (ready.get(card, 0) + provisioning.get(card, 0) +
                   self.free_reserved_slots_by_accelerator.get(card, 0)
                  ) for card in configured_cards
        })

        def fallback_after_next_assignment(
                compatible: tuple[str, ...]) -> tuple[int, int]:
            """Return the second-best marginal supply tier for one profile."""
            options: list[tuple[int, int]] = []
            for card in compatible:
                if unused_capacity.get(card, 0.0) > 0:
                    options.append((0, cold_rank[card]))
                previous_count = target.get(card, 0)
                for tier_index, tier in enumerate(planned_by_tier, start=1):
                    tier_count = max(previous_count, tier.get(card, 0))
                    # Two copies are sufficient: only the best option is
                    # consumed before comparing the fallback.
                    options.extend([(tier_index, cold_rank[card])] *
                                   min(2, max(0, tier_count - previous_count)))
                    previous_count = tier_count
                # Another paid replica remains the final fallback after all
                # already-materialized or reserved marginal supply is used.
                options.append((len(planned_by_tier) + 1, cold_rank[card]))
            options.sort()
            if len(options) > 1:
                return options[1]
            if options:
                return options[0]
            return len(planned_by_tier) + 2, len(cold_order)

        groups_by_priority: dict[int, list[tuple[tuple[str, ...], int]]] = {}
        for (priority, compatible), request_count in grouped.items():
            groups_by_priority.setdefault(priority, []).append(
                (compatible, request_count))
        for priority in sorted(groups_by_priority, reverse=True):
            pending = groups_by_priority[priority]
            while pending:
                # Protect the profile whose best non-selected fallback is
                # worse. The list is stable, so FIFO/report order breaks a
                # true fallback tie without relying on raw set cardinality.
                fallback_keys = [
                    tuple(
                        -value
                        for value in fallback_after_next_assignment(compatible))
                    for compatible, _ in pending
                ]
                selected_index = min(range(len(pending)),
                                     key=fallback_keys.__getitem__)
                compatible, request_count = pending.pop(selected_index)
                if sum(target.values()) >= self.max_replicas:
                    continue
                remaining_qps = request_count / self.qps_window_size
                for card in compatible:
                    consumed = min(remaining_qps,
                                   unused_capacity.get(card, 0.0))
                    remaining_qps -= consumed
                    unused_capacity[card] = max(
                        0.0,
                        unused_capacity.get(card, 0.0) - consumed)
                    if remaining_qps <= 0:
                        break
                while (remaining_qps > 0 and
                       sum(target.values()) < self.max_replicas):
                    selected: str | None = None
                    for tier in planned_by_tier:
                        selected = next(
                            (card for card in compatible
                             if tier.get(card, 0) > target.get(card, 0)), None)
                        if selected is not None:
                            break
                    if selected is None:
                        # Compatibility order is not a hardware preference.
                        # Use live paid placement cost/availability when
                        # available, with service order only as the
                        # deterministic fallback.
                        selected = next(
                            card for card in cold_order if card in compatible)
                    capacity = capacities.get(selected, 0.0)
                    if capacity <= 0:
                        break
                    target[selected] = target.get(selected, 0) + 1
                    remaining_qps -= capacity

        # Aggregate min_replicas remains independent from per-card floors.
        # Materialize any residual aggregate floor on already-ready reserved,
        # then ready, then provisioning capacity before using service order.
        while sum(target.values()) < self.min_replicas and configured_cards:
            selected = None
            for tier in planned_by_tier:
                selected = next((card for card in configured_cards
                                 if tier.get(card, 0) > target.get(card, 0)),
                                None)
                if selected is not None:
                    break
            if selected is None:
                selected = cold_order[0]
            target[selected] = target.get(selected, 0) + 1
        return {card: count for card, count in target.items() if count > 0}

    def _set_target_num_replicas_with_instance_aware_logic(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Set target_num_replicas using instance-aware logic."""
        assert isinstance(self.target_qps_per_replica,
                          dict), 'Expected dict for instance-aware logic'
        num_requests_per_second = len(
            self.request_timestamps) / self.qps_window_size
        candidate_target_by_accelerator: dict[str, int] | None = None
        latest_capacities: list[float] = []
        if (getattr(self, 'compatibility_profiles', []) or
                getattr(self, 'queued_compatibility_profiles', []) or
                getattr(self, 'min_replicas_by_accelerator', {})):
            candidate_target_by_accelerator = (
                self._calculate_target_by_accelerator(replica_infos))
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
        elif (target_num_replicas > self.target_num_replicas or
              (target_map_changed and any(
                  candidate_target_map.get(card, 0) > getattr(
                      self, 'target_num_replicas_by_accelerator', {}).get(
                          card, 0) for card in candidate_target_map))):
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                apply_target = True
        elif (target_num_replicas < self.target_num_replicas or
              target_map_changed):
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
        num_ready_latest = 0
        ready_latest_capacity = 0.0
        for info in replica_infos:
            if info.version == self.latest_version and info.is_ready:
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
        so a transient DB error can heal on the next tick.
        """
        cached = self._qps_dict_by_version.get(version)
        if cached is not None:
            return cached
        qps_dict = None
        try:
            spec = serve_state.get_spec(self._service_name, version)
            if spec is not None:
                qps_dict = spec.target_qps_per_replica
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to load spec for version '
                           f'{version}: {common_utils.format_exception(e)}')
        if not isinstance(qps_dict, dict):
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
        self._rejected_in_window: int = 0
        # Replica ids whose declared async occupancy could not be sampled.
        # They contribute their full per-version capacity to outstanding work:
        # unknown is a potentially-full replica, never an idle zero.
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
        self._last_logical_target_state: tuple[int, int, int] | None = None
        self._gpu_shape_cache: dict[int, tuple[str, int]] = {}
        # Backs the cost-descending victim tiebreak (shed paid spot
        # before zero-cost reserved capacity); pruned with the shape
        # cache each tick.
        self._replica_cost_cache: dict[int, float] = {}
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
        # Per-tick freshness snapshot (see _fresh_for_tick). None outside
        # a tick.
        self._tick_fresh: bool | None = None
        # True only while an increase in the demand-derived target is waiting
        # for upscale hysteresis.  The live fleet must not be shrunk toward
        # the old target during that wait: doing so makes the autoscaler issue
        # scale-down and scale-up intents for opposite demand snapshots.
        self._upscale_pending: bool = False

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
    def logical_target_state(self) -> tuple[int, int, int] | None:
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

        in_flight = request_aggregator_info.get('in_flight_by_replica_id')
        if in_flight is None:
            # No usable demand report in this sync (old LB, or a policy
            # that cannot track in-flight). Keep the previous report: it
            # ages out on its own; overwriting it with nothing would
            # discard a still-fresh signal.
            return
        # Normalize keys/values: the controller builds this dict
        # in-process today, but a defensive int() keeps us safe if it is
        # ever rebuilt from a JSON round-trip (string keys).
        self._in_flight_by_replica_id = {
            int(replica_id): int(count)
            for replica_id, count in in_flight.items()
        }
        queue_depth = request_aggregator_info.get('queue_depth')
        self._queue_depth = int(queue_depth) if queue_depth is not None else 0
        rejected = request_aggregator_info.get('rejected_in_window')
        self._rejected_in_window = int(rejected) if rejected is not None else 0
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
        logger.info(f'Concurrency report: in_flight_total='
                    f'{sum(self._in_flight_by_replica_id.values())}, '
                    f'queue_depth={self._queue_depth}, '
                    f'rejected_in_window={self._rejected_in_window}, '
                    f'unknown_replicas='
                    f'{len(self._unknown_in_flight_replica_ids)}, '
                    f'requests in the last {self.qps_window_size}s: '
                    f'{len(self.request_timestamps)}')

    def _get_knob_for_version(self, version: int) -> float:
        """The per-GPU knob a given service version was launched under.

        Unknown versions (the autoscaler was rebuilt after the update
        that created them, e.g. a controller restart mid-rolling-update)
        rehydrate from the durable per-version spec so old-version
        replicas keep their real capacity. Falls back to the latest knob
        when the version's spec is unavailable; misses are not memoized
        so a transient DB error can heal on the next tick.
        """
        cached = self._knob_by_version.get(version)
        if cached is not None:
            return cached
        knob = None
        try:
            spec = serve_state.get_spec(self._service_name, version)
            if spec is not None:
                knob = getattr(spec, 'target_concurrency_per_replica', None)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to load spec for version '
                           f'{version}: {common_utils.format_exception(e)}')
        if knob is None:
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
        if getattr(info.status_property, 'is_scale_down', False) is True:
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

    def _outstanding_work(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'] | None = None,
    ) -> float:
        """Outstanding jobs per the latest report (gauges, one snapshot).

        A job can transiently appear in both queue_depth (one sync) and
        rejected_in_window (a later sync) -- at most a 2x count per job,
        absorbed by hysteresis (accepted in the plan).
        """
        assert self._in_flight_by_replica_id is not None
        unknown_floor = 0.0
        if self._unknown_in_flight_replica_ids:
            infos_by_id = {
                info.replica_id: info
                for info in (replica_infos or [])
                if not info.is_terminal
            }
            fallback_capacity = max(
                (self._replica_capacity(info) for info in infos_by_id.values()),
                default=self.target_concurrency_per_replica)
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
                    capacity = self._replica_capacity(info)
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
            unknown_floor = max(original_unknown_floor,
                                replacement_unknown_floor)
        return float(
            sum(self._in_flight_by_replica_id.values()) + self._queue_depth +
            self._rejected_in_window + unknown_floor)

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
            best_capacity = self.target_concurrency_per_replica
        else:
            best_capacity = (latest_capacities[0] if latest_capacities else
                             self.target_concurrency_per_replica)
        self._upscale_pending = False

        if not self._fresh_for_tick():
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
                arrival_floor = self._clip_target_num_replicas(
                    math.ceil(arrivals / best_capacity))
                if arrival_floor > self.target_num_replicas:
                    logger.info(
                        'Concurrency autoscaler signal-stale: raising '
                        f'target to arrival floor {arrival_floor} '
                        f'({arrivals} arrivals / capacity {best_capacity}).')
                    self.target_num_replicas = arrival_floor
            else:
                logger.info('Concurrency autoscaler signal-stale: holding '
                            f'target at {self.target_num_replicas}.')
            return

        outstanding = self._outstanding_work(replica_infos)
        if self.replica_unit == 'logical':
            raw_target_num = math.ceil(outstanding / best_capacity)
        else:
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

        target_num_replicas = self._clip_target_num_replicas(raw_target_num)
        old_target_num_replicas = self.target_num_replicas

        if self._snap_target_on_next_recompute:
            # First recompute with fresh data after construction or an update:
            # snap upward immediately, but never bypass downscale hysteresis.
            # A policy-only update can land during a brief idle interval; an
            # immediate downward snap would tear down the live fleet before
            # the configured downscale delay has proved sustained idleness.
            self._snap_target_on_next_recompute = False
            self.upscale_counter = 0
            self.downscale_counter = 0
            if target_num_replicas >= self.target_num_replicas:
                self.target_num_replicas = target_num_replicas
            else:
                self.downscale_counter = 1
                if self.downscale_counter >= self.scale_down_threshold:
                    self.downscale_counter = 0
                    self.target_num_replicas = target_num_replicas
        # Faster scale up when there is no replica.
        elif self.target_num_replicas == 0:
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

        self._upscale_pending = (target_num_replicas > self.target_num_replicas)

        logger.info(
            f'Concurrency: outstanding work: {outstanding}. '
            f'Latest-version capacities: {latest_capacities}. '
            f'Old target number of replicas: {old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'Downscale counter: {self.downscale_counter}/'
            f'{self.scale_down_threshold}. ')

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        with self._logical_state_lock:
            return self._generate_scaling_decisions_locked(
                replica_infos, active_versions)

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
                            replica_id=decision.target),
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
        super().update_version(version, spec, update_mode)
        self._snap_target_on_next_recompute = True
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
        - Idle-preferred victims: among READY old replicas, busy ones
          (fresh in-flight > 0 or unknown) are preferentially KEPT as
          coverage, so the retired remainder skews to idle replicas and
          mid-job kills are avoided when coverage allows. This is a
          preference, not a hard gate -- a rolling update must still
          complete when every old replica is busy.
        """
        if not self._fresh_for_tick():
            return []
        if self._upscale_pending:
            logger.info('Concurrency autoscaler suppressing outdated-replica '
                        'drain while an upscale is pending hysteresis.')
            return []
        if self.replica_unit == 'logical':
            old_nonterminal = [
                info for info in replica_infos
                if info.version < self.latest_version and not info.is_terminal
            ]
            if not old_nonterminal:
                return []
            latest_ready_capacity = sum(
                self._ready_capacity(info)
                for info in replica_infos
                if info.version == self.latest_version)
            if latest_ready_capacity < self.get_final_target_num_replicas():
                # The activation bridge deliberately gives legacy backends no
                # inferred logical width. Launch and observe the complete new
                # logical target before retiring any old backend.
                return []
            return [
                info.replica_id
                for info in old_nonterminal
                if not self._replica_is_busy(info)
            ]
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
            if info.version == self.latest_version and info.is_ready:
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
                latest_nonterminal_replicas)

        scaling_decisions: list[AutoscalerDecision] = []
        target_num_replicas = self.get_final_target_num_replicas()
        current_num_replicas = len(latest_nonterminal_replicas)

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
        latest_nonterminal_replicas: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate one shaped scale target or capacity-safe retirements."""
        target = self.get_final_target_num_replicas()
        committed = sum(
            self._committed_capacity(info)
            for info in latest_nonterminal_replicas)
        if committed < target:
            replace_unknown_replica_ids = tuple(
                sorted(info.replica_id
                       for info in latest_nonterminal_replicas
                       if getattr(info.status_property, 'is_scale_down',
                                  False) is not True and info.replica_id in
                       self._degraded_capacity_since_by_replica_id and
                       self._committed_capacity(info) == 0))
            return [
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_UP,
                    LogicalScaleTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        replace_unknown_replica_ids=replace_unknown_replica_ids)
                )
            ]
        if not self._fresh_for_tick() or self._upscale_pending:
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
        decisions: list[AutoscalerDecision] = []
        for info in candidates:
            committed_width = self._committed_capacity(info)
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
                else:
                    if remaining_ready - ready_width < target:
                        continue
                    remaining_ready -= ready_width
            elif remaining_committed - committed_width < target:
                continue
            remaining_committed -= committed_width
            decisions.append(
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_DOWN,
                    LogicalScaleDownTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        replica_id=info.replica_id)))
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
        in_flight_total = (sum(self._in_flight_by_replica_id.values()) if
                           self._in_flight_by_replica_id is not None else None)
        report_age = (time.time() - self._report_received_at
                      if self._report_received_at is not None else None)
        info.update({
            'replica_unit': self.replica_unit,
            'in_flight_total': in_flight_total,
            'queue_depth': self._queue_depth,
            'rejected_in_window': self._rejected_in_window,
            'unknown_in_flight_replicas': len(
                self._unknown_in_flight_replica_ids),
            'report_age_seconds': report_age,
        })
        return info

    def _dump_dynamic_states(self) -> dict[str, Any]:
        # Only consumed by the in-process autoscaler swap during
        # update_service (NOT on controller restart). The received-at
        # time is absolute, so a report that crosses the swap simply
        # reads as stale once it exceeds the staleness threshold.
        return {
            'request_timestamps': self.request_timestamps,
            'in_flight_by_replica_id': self._in_flight_by_replica_id,
            'queue_depth': self._queue_depth,
            'rejected_in_window': self._rejected_in_window,
            'unknown_in_flight_replica_ids': sorted(
                self._unknown_in_flight_replica_ids),
            'report_received_at': self._report_received_at,
            'reconcile_generation': self._reconcile_generation,
            'observed_slots_by_replica_id': self._observed_slots_by_replica_id,
            'unknown_capacity_replica_ids': sorted(
                self._unknown_capacity_replica_ids),
            'degraded_capacity_since_by_replica_id':
                self._degraded_capacity_since_by_replica_id,
        }

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        # Tolerate dumps from other autoscaler types (an update can
        # change the autoscaler class; e.g. RequestRateAutoscaler only
        # dumps request_timestamps): missing keys keep the stale-start
        # defaults.
        if 'request_timestamps' in dynamic_states:
            self.request_timestamps = dynamic_states.pop('request_timestamps')
        if 'in_flight_by_replica_id' in dynamic_states:
            self._in_flight_by_replica_id = dynamic_states.pop(
                'in_flight_by_replica_id')
        if 'queue_depth' in dynamic_states:
            self._queue_depth = dynamic_states.pop('queue_depth')
        if 'rejected_in_window' in dynamic_states:
            self._rejected_in_window = dynamic_states.pop('rejected_in_window')
        if 'unknown_in_flight_replica_ids' in dynamic_states:
            self._unknown_in_flight_replica_ids = {
                int(replica_id) for replica_id in dynamic_states.pop(
                    'unknown_in_flight_replica_ids')
            }
        if 'report_received_at' in dynamic_states:
            self._report_received_at = dynamic_states.pop('report_received_at')
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
