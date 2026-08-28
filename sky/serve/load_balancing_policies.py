"""LoadBalancingPolicy: Policy to select endpoint."""
import collections
import random
import threading
import typing
from typing import Any

from sky import sky_logging

if typing.TYPE_CHECKING:
    import fastapi

logger = sky_logging.init_logger(__name__)

# Define a registry for load balancing policies
LB_POLICIES: dict[str, typing.Type['LoadBalancingPolicy']] = {}
DEFAULT_LB_POLICY: str | None = None


class LoadBalancingPolicy:
    """Abstract class for load balancing policies."""

    def __init__(self) -> None:
        self.ready_replicas: list[str] = []

    def __init_subclass__(cls, name: str, default: bool = False):
        LB_POLICIES[name] = cls
        if default:
            global DEFAULT_LB_POLICY
            assert DEFAULT_LB_POLICY is None, (
                'Only one policy can be default.')
            DEFAULT_LB_POLICY = name

    @classmethod
    def make_policy_name(cls, policy_name: str | None) -> str:
        """Return the policy name."""
        assert DEFAULT_LB_POLICY is not None, 'No default policy set.'
        if policy_name is None:
            return DEFAULT_LB_POLICY
        return policy_name

    @classmethod
    def make(cls, policy_name: str | None = None) -> 'LoadBalancingPolicy':
        """Create a load balancing policy from a name."""
        policy_name = cls.make_policy_name(policy_name)
        if policy_name not in LB_POLICIES:
            raise ValueError(f'Unknown load balancing policy: {policy_name}')
        return LB_POLICIES[policy_name]()

    def set_ready_replicas(self, ready_replicas: list[str]) -> None:
        raise NotImplementedError

    def select_replica(self,
                       request: 'fastapi.Request',
                       exclude: set[str] | None = None,
                       eligible: set[str] | None = None) -> str | None:
        """Select a replica from an optional strict eligibility set.

        `exclude` carries the URLs that already failed THIS request's
        earlier retry attempts. Without it, least-load retries are a
        failure magnet on a busy fleet: a dead-but-not-yet-pruned
        replica sits at load 0 (its failed attempts release their
        slots) while every healthy replica carries traffic, so it is
        the strict minimum and every retry deterministically re-selects
        the corpse. Falls back to the full set when every candidate has
        failed — a lone replica with a transient blip deserves the
        remaining attempts more than a guaranteed error.

        `eligible` is a strict capacity filter. Unlike `exclude`, an empty
        eligible set never falls back to all ready replicas: callers use it
        when an occupancy sample proves that only a subset has a free slot.
        """
        candidates = self.ready_replicas
        if eligible is not None:
            candidates = [url for url in candidates if url in eligible]
        if exclude:
            filtered = [url for url in candidates if url not in exclude]
            if filtered:
                candidates = filtered
        replica = self._select_replica(request, candidates)
        # Keep the lock-held request hot path free of per-attempt logs. Access
        # logs and bounded aggregate telemetry cover request outcomes; logging
        # every selection scales with traffic (and retries) and can expose URL
        # query parameters.
        return replica

    # TODO(tian): We should have an abstract class for Request to
    # compatible with all frameworks.
    def _select_replica(self, request: 'fastapi.Request',
                        candidates: list[str]) -> str | None:
        raise NotImplementedError

    def snapshot_in_flight(self) -> dict[str, int] | None:
        """Per-replica in-flight snapshot (url -> count), when tracked.

        None for policies without load accounting (round robin): the
        controller sync then reports demand as unknown -- the autoscaler's
        signal-gap rules apply -- rather than a false all-idle fleet.
        """
        return None

    def set_occupancy(self, occupancy: dict[str, int]) -> None:
        """Set the replica-reported async occupancy (url -> running jobs).

        [boltz fork] Fed by the LB's occupancy probe: async fast-ack
        workloads finish the HTTP envelope in milliseconds while the
        replica crunches for hours, so the in-flight accounting alone
        reads every replica as idle. Policies that track load fold this
        into selection; policies without load accounting ignore it.
        """
        del occupancy

    def set_occupancy_for_replica(self, replica_url: str,
                                  occupancy: int | None) -> None:
        """Update one replica's occupancy on the routing hot path.

        Full probe rounds use `set_occupancy`. Optimistic async reservations
        change one URL at a time and use this method to avoid rebuilding an
        O(fleet-size) dictionary for every dispatch.
        """
        del replica_url, occupancy

    def pre_execute_hook(self, replica_url: str,
                         request: 'fastapi.Request') -> Any | None:
        """Account an in-flight request. Returns an opaque token that the
        caller must hand back to post_execute_hook, so a release is tied
        to the exact accounting generation it incremented (a replica URL
        pruned and re-added between the two must not absorb a stale
        release — the ABA problem)."""
        del replica_url, request
        return None

    def post_execute_hook(self,
                          replica_url: str,
                          request: 'fastapi.Request',
                          token: Any | None = None) -> None:
        del replica_url, request, token


class RoundRobinPolicy(LoadBalancingPolicy, name='round_robin'):
    """Round-robin load balancing policy."""

    def __init__(self) -> None:
        super().__init__()
        self.index = 0

    def set_ready_replicas(self, ready_replicas: list[str]) -> None:
        if set(self.ready_replicas) == set(ready_replicas):
            return
        # If the autoscaler keeps scaling up and down the replicas,
        # we need this shuffle to not let the first replica have the
        # most of the load.
        random.shuffle(ready_replicas)
        self.ready_replicas = ready_replicas
        self.index = 0

    def _select_replica(self, request: 'fastapi.Request',
                        candidates: list[str]) -> str | None:
        del request  # Unused.
        if not candidates:
            return None
        ready_replica_url = candidates[self.index % len(candidates)]
        self.index = (self.index + 1) % len(candidates)
        return ready_replica_url


# [boltz fork] Weight of one replica-reported running async job in the
# load-selection score, relative to one in-flight HTTP request. An async job
# occupies a whole predict slot for up to hours while envelope requests live
# for milliseconds, so occupancy must dominate: any busy replica sorts after
# any idle one regardless of transient envelope counts. Deliberately a
# weight (not a hard filter) — when EVERY replica is busy the min is still
# defined, the request proxies, and the replica's own shedding (429 ->
# retry -> LB 503) stays the authoritative backstop, so a stale probe can
# deprioritize but never black-hole routing.
OCCUPANCY_LOAD_WEIGHT = 1000.0


class LeastLoadPolicy(LoadBalancingPolicy, name='least_load', default=True):
    """Least load load balancing policy."""

    def __init__(self) -> None:
        super().__init__()
        self.load_map: dict[str, int] = collections.defaultdict(int)
        # url -> replica-reported running async jobs (see set_occupancy).
        # Absent url == occupancy unknown == treated as 0: an unreachable
        # probe must fall back to today's envelope-only behavior, not
        # exile the replica.
        self.occupancy_map: dict[str, int] = {}
        # url -> accounting generation; bumped whenever a key is (re)added
        # so stale releases from before a prune are ignored (ABA).
        self._generation: dict[str, int] = collections.defaultdict(int)
        self.lock = threading.Lock()

    def set_occupancy(self, occupancy: dict[str, int]) -> None:
        # Replace wholesale: the probe rebuilds the map every round from
        # the current ready set, so replacement is also the prune.
        with self.lock:
            self.occupancy_map = dict(occupancy)

    def set_occupancy_for_replica(self, replica_url: str,
                                  occupancy: int | None) -> None:
        with self.lock:
            if occupancy is None:
                self.occupancy_map.pop(replica_url, None)
            else:
                self.occupancy_map[replica_url] = occupancy

    def _effective_load(self, replica_url: str) -> float:
        """Selection score: envelope in-flight + weighted async occupancy.

        Must be called while holding `self.lock`.
        """
        return (self.load_map.get(replica_url, 0) +
                OCCUPANCY_LOAD_WEIGHT * self.occupancy_map.get(replica_url, 0))

    def set_ready_replicas(self, ready_replicas: list[str]) -> None:
        if set(self.ready_replicas) == set(ready_replicas):
            return
        with self.lock:
            self.ready_replicas = ready_replicas
            for r in list(self.load_map.keys()):
                if r not in ready_replicas:
                    del self.load_map[r]
            for replica in ready_replicas:
                if replica not in self.load_map:
                    # New (or re-added) key: bump its generation so
                    # releases from streams counted BEFORE a prune cannot
                    # decrement the fresh counter (ABA).
                    self._generation[replica] += 1
                    self.load_map[replica] = 0

    def snapshot_in_flight(self) -> dict[str, int] | None:
        with self.lock:
            # Scoped to the READY set: an entry for a pruned replica must
            # not report phantom demand. A COPY, not the live defaultdict
            # -- the caller serializes it outside the lock while the
            # routing hot path keeps mutating the original.
            return {
                replica: self.load_map.get(replica, 0)
                for replica in self.ready_replicas
            }

    def _select_replica(self, request: 'fastapi.Request',
                        candidates: list[str]) -> str | None:
        del request  # Unused.
        if not candidates:
            return None
        with self.lock:
            # Score each candidate exactly once: this runs per proxied
            # request, and _effective_load reads two maps per call.
            replica_loads = [(replica, self._effective_load(replica))
                             for replica in candidates]
            min_load = min(load for _, load in replica_loads)
            # Random tie-break: deterministic min() over URL order biases
            # cold starts (all-zero loads) onto the same replica wave
            # after wave.
            tie_break = [
                replica for replica, load in replica_loads if load == min_load
            ]
            return random.choice(tie_break)

    def pre_execute_hook(self, replica_url: str,
                         request: 'fastapi.Request') -> Any | None:
        del request  # Unused.
        with self.lock:
            # Live keys only: a replica pruned between selection and this
            # hook must not be recreated (its paired post is skipped the
            # same way, so accounting stays consistent).
            if replica_url in self.load_map:
                self.load_map[replica_url] += 1
                return self._generation[replica_url]
            logger.debug(
                'pre_execute_hook: %s not in load map (pruned '
                'between selection and dispatch); not counted.', replica_url)
            return None

    def post_execute_hook(self,
                          replica_url: str,
                          request: 'fastapi.Request',
                          token: Any | None = None) -> None:
        del request  # Unused.
        with self.lock:
            # Only decrement live keys, clamped at zero: a replica pruned
            # from the ready set mid-stream must not be recreated at -1
            # (phantom capacity that would attract traffic on re-add).
            if replica_url not in self.load_map:
                return
            if token is None:
                # pre_execute_hook never accounted this request (the URL
                # was pruned at dispatch time and it returned None), so
                # there is no slot to release. Decrementing here would
                # steal a live request's slot if the URL was re-added in
                # between (ABA via the None token bypassing the
                # generation guard below).
                return
            if token != self._generation[replica_url]:
                # The increment belonged to a PREVIOUS generation of this
                # URL (pruned and re-added since): releasing here would
                # steal a slot from the new generation's streams (ABA).
                logger.debug(
                    'post_execute_hook: stale-generation release for %s '
                    'ignored.', replica_url)
                return
            if self.load_map[replica_url] <= 0:
                # A live key at zero receiving a release means a double
                # release or a missed increment upstream — clamp, but do
                # not hide it.
                logger.warning(
                    'post_execute_hook: load underflow for %s; clamping '
                    'at 0 (possible double release).', replica_url)
                self.load_map[replica_url] = 0
                return
            self.load_map[replica_url] -= 1


class InstanceAwareLeastLoadPolicy(LeastLoadPolicy,
                                   name='instance_aware_least_load'):
    """Instance-aware least load load balancing policy.

    This policy considers the accelerator type and its QPS capabilities
    when distributing load. It normalizes the load by dividing the current
    load by the target QPS for that accelerator type.
    """

    def __init__(self) -> None:
        super().__init__()
        self.replica_info: dict[str, dict[str, Any]] = {}  # replica_url -> info
        self.target_qps_per_accelerator: dict[str, float] = {
        }  # accelerator_type -> target_qps
        # Resolved per-replica target QPS by (gpu_type, gpu_count). This keeps
        # flexible matching off the per-request routing hot path after the
        # first lookup for a given shape and is invalidated on routing-spec
        # updates.
        self._target_qps_cache: dict[tuple[str, int], float] = {}
        # Uniform per-GPU weight for services without a QPS dict
        # (concurrency-sized): consulted after the dict keys, before the
        # flat fallback. See set_default_per_gpu_target.
        self._default_per_gpu_qps: float | None = None

    def set_ready_replicas(self, ready_replicas: list[str]) -> None:
        if set(self.ready_replicas) == set(ready_replicas):
            return
        with self.lock:
            self.ready_replicas = ready_replicas
            # Clean up load map for removed replicas
            for r in list(self.load_map.keys()):
                if r not in ready_replicas:
                    del self.load_map[r]
            # Initialize load for new replicas. Same generation bump as
            # the base class: releases from before a prune must not
            # decrement a re-added key's fresh counter (ABA).
            for replica in ready_replicas:
                if replica not in self.load_map:
                    self._generation[replica] += 1
                    self.load_map[replica] = 0

    def set_replica_info(self, replica_info: dict[str, dict[str, Any]]) -> None:
        """Set replica information including accelerator types.

        Args:
            replica_info: Dict mapping replica URL to replica information
                         e.g., {'http://url1': {'gpu_type': 'A100'}}
        """
        with self.lock:
            self.replica_info = replica_info
            logger.debug('Set replica info: %s', self.replica_info)

    def set_target_qps_per_accelerator(
            self, target_qps_per_accelerator: dict[str, float]) -> None:
        """Set target QPS for each accelerator type."""
        with self.lock:
            self.target_qps_per_accelerator = dict(target_qps_per_accelerator)
            self._target_qps_cache.clear()
            # A concrete QPS dict is authoritative: drop the uniform
            # per-GPU default so the two weighting modes never mix.
            if target_qps_per_accelerator:
                self._default_per_gpu_qps = None

    def set_default_per_gpu_target(self, per_gpu_target: float | None) -> None:
        """Uniform per-GPU weight for services without a QPS dict.

        Concurrency-sized services (target_concurrency_per_replica) have
        no per-accelerator QPS dict; their per-GPU capacity is uniform,
        so replicas should absorb load proportionally to gpu_count. The
        default is consulted after the dict keys and before the flat-1.0
        fallback, and replaces the dict wholesale when set: mixing a
        previous version's QPS weights with uniform weighting would bias
        routing toward whichever shapes the stale dict under-weighted.
        """
        with self.lock:
            self._default_per_gpu_qps = per_gpu_target
            self._target_qps_cache.clear()
            if per_gpu_target is not None:
                self.target_qps_per_accelerator = {}

    def _get_normalized_load(self, replica_url: str) -> float:
        """Get normalized load for a replica based on its GPU shape."""
        # Occupancy folds in BEFORE normalization, like extra in-flight
        # load: within one GPU shape (the common single-shape fleet) busy
        # replicas sort strictly after idle ones; across shapes the same
        # target-QPS normalization applies to both terms.
        current_load = self._effective_load(replica_url)

        # Get accelerator shape for this replica
        replica_data = self.replica_info.get(replica_url, {})
        accelerator_type = replica_data.get('gpu_type', 'unknown')
        try:
            accelerator_count = max(1, int(replica_data.get('gpu_count', '1')))
        except (TypeError, ValueError):
            accelerator_count = 1

        # Get per-replica target QPS with flexible matching, weighted by
        # GPU count: a 4-GPU replica absorbs 4x the load of a 1-GPU one
        # before looking equally loaded.
        target_qps = self._get_target_qps_for_accelerator(
            accelerator_type, accelerator_count)
        if target_qps <= 0:
            logger.warning(
                'Non-positive target QPS (%s) for accelerator shape %s:%s; '
                'using default value 1.0 to avoid division by zero.',
                target_qps, accelerator_type, accelerator_count)
            target_qps = 1.0

        # Load is normalized by target QPS
        normalized_load = current_load / target_qps

        logger.debug(
            'InstanceAwareLeastLoadPolicy: Replica %s - GPU shape: %s:%s, '
            'current load: %s, target QPS: %s, normalized load: %s',
            replica_url, accelerator_type, accelerator_count, current_load,
            target_qps, normalized_load)

        return normalized_load

    def _get_target_qps_for_accelerator(self,
                                        accelerator_type: str,
                                        accelerator_count: int = 1) -> float:
        """Per-replica target QPS with flexible matching.

        The result is memoized by GPU shape. Callers already hold
        ``self.lock`` on the load-balancer hot path, so cache reads and writes
        stay serialized with routing-spec updates.

        Same key semantics as the instance-aware autoscaler (kept inline —
        this module runs in the load balancer process and must not pull in
        serve_utils' import graph):
          1. exact shape key ('L4:4') -> per-replica value, as-is;
          2. bare type key ('L4') -> per-GPU value, x count;
          3. other count-suffixed key of the same type ('L4:1') ->
             normalized to per-GPU (value / key count), x count.
        Per-GPU semantics assume one model instance per GPU; models
        needing k GPUs per instance must use exact shape keys.
        """
        cache_key = (accelerator_type, accelerator_count)
        cached = self._target_qps_cache.get(cache_key)
        if cached is not None:
            return cached

        target_qps = self._resolve_target_qps_for_accelerator(
            accelerator_type, accelerator_count)
        self._target_qps_cache[cache_key] = target_qps
        return target_qps

    def _resolve_target_qps_for_accelerator(self, accelerator_type: str,
                                            accelerator_count: int) -> float:
        """Resolve target QPS for one GPU shape without memoization."""
        # Exact shape match first
        exact_key = f'{accelerator_type}:{accelerator_count}'
        if exact_key in self.target_qps_per_accelerator:
            return self.target_qps_per_accelerator[exact_key]

        # Bare type key is a per-GPU value
        if accelerator_type in self.target_qps_per_accelerator:
            return (self.target_qps_per_accelerator[accelerator_type] *
                    accelerator_count)

        # Count-suffixed key of the same type, normalized to per-GPU
        for config_key, value in self.target_qps_per_accelerator.items():
            base_name, _, count_str = config_key.partition(':')
            if (base_name == accelerator_type and count_str.isdigit() and
                    int(count_str) > 0):
                return value / int(count_str) * accelerator_count

        # Uniform per-GPU default (concurrency-sized services): every
        # GPU carries the same weight, so capacity scales with count.
        if self._default_per_gpu_qps is not None:
            return self._default_per_gpu_qps * accelerator_count

        # Fallback
        logger.warning(
            f'No matching QPS found for accelerator type: {accelerator_type}. '
            f'Available types: {list(self.target_qps_per_accelerator.keys())}. '
            f'Using default value 1.0 as fallback.')
        return 1.0

    def _select_replica(self, request: 'fastapi.Request',
                        candidates: list[str]) -> str | None:
        del request  # Unused.
        if not candidates:
            return None
        with self.lock:
            # Calculate normalized loads for all replicas
            replica_loads = []
            for replica in candidates:
                normalized_load = self._get_normalized_load(replica)
                replica_loads.append((replica, normalized_load))

            # Select among the (near-)minimum normalized loads at random:
            # deterministic min() over URL order biases cold starts.
            min_load = min(load for _, load in replica_loads)
            tie_break = [
                replica for replica, load in replica_loads
                if load - min_load <= 1e-9
            ]
            # Priority and compatibility are enforced before policy selection.
            # Within the resulting equally safe, equally loaded set, consume
            # ready reserved capacity first so an equivalent paid replica can
            # become idle and scale down. Never accept extra load merely for
            # cost: this filter applies only to the normalized-load tie.
            zero_cost = [
                replica for replica in tie_break if str(
                    self.replica_info.get(replica, {}).get(
                        'is_zero_cost', '')).lower() == 'true'
            ]
            if zero_cost:
                tie_break = zero_cost
            selected_replica = random.choice(tie_break)
            logger.debug('Available replicas and loads: %s', replica_loads)
            logger.debug('Selected replica: %s', selected_replica)
            return selected_replica

    # pre_execute_hook and post_execute_hook are inherited from LeastLoadPolicy
