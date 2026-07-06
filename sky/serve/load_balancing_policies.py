"""LoadBalancingPolicy: Policy to select endpoint."""
import collections
import random
import threading
import typing
from typing import Any, Dict, List, Optional

from sky import sky_logging

if typing.TYPE_CHECKING:
    import fastapi

logger = sky_logging.init_logger(__name__)

# Define a registry for load balancing policies
LB_POLICIES = {}
DEFAULT_LB_POLICY = None


class LoadBalancingPolicy:
    """Abstract class for load balancing policies."""

    def __init__(self) -> None:
        self.ready_replicas: List[str] = []

    def __init_subclass__(cls, name: str, default: bool = False):
        LB_POLICIES[name] = cls
        if default:
            global DEFAULT_LB_POLICY
            assert DEFAULT_LB_POLICY is None, (
                'Only one policy can be default.')
            DEFAULT_LB_POLICY = name

    @classmethod
    def make_policy_name(cls, policy_name: Optional[str]) -> str:
        """Return the policy name."""
        assert DEFAULT_LB_POLICY is not None, 'No default policy set.'
        if policy_name is None:
            return DEFAULT_LB_POLICY
        return policy_name

    @classmethod
    def make(cls, policy_name: Optional[str] = None) -> 'LoadBalancingPolicy':
        """Create a load balancing policy from a name."""
        policy_name = cls.make_policy_name(policy_name)
        if policy_name not in LB_POLICIES:
            raise ValueError(f'Unknown load balancing policy: {policy_name}')
        return LB_POLICIES[policy_name]()

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        raise NotImplementedError

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        replica = self._select_replica(request)
        # NOTE: this runs on the per-request routing hot path, inside the load
        # balancer's client-pool lock on the uvicorn event-loop thread, so log
        # only the cheap method + url. The previous code formatted a full
        # request dump (``dict(request.headers)`` + the query params) as an
        # f-string argument on *every* request, which (a) added per-
        # request CPU on the lock-held routing path and (b) leaked auth headers
        # into the LB log. ``request.url`` already includes the path + query.
        # A DEBUG gate would not help: SkyPilot sets the logger level to DEBUG
        # (the default *handler* filters at INFO), so ``isEnabledFor(DEBUG)`` is
        # True and the dump would still be built every request.
        if replica is not None:
            logger.info('Selected replica %s for request %s %s', replica,
                        request.method, request.url)
        else:
            logger.warning('No replica selected for request %s %s',
                           request.method, request.url)
        return replica

    # TODO(tian): We should have an abstract class for Request to
    # compatible with all frameworks.
    def _select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        raise NotImplementedError

    def pre_execute_hook(self, replica_url: str,
                         request: 'fastapi.Request') -> Optional[Any]:
        """Account an in-flight request. Returns an opaque token that the
        caller must hand back to post_execute_hook, so a release is tied
        to the exact accounting generation it incremented (a replica URL
        pruned and re-added between the two must not absorb a stale
        release — the ABA problem)."""
        return None

    def post_execute_hook(self,
                          replica_url: str,
                          request: 'fastapi.Request',
                          token: Optional[Any] = None) -> None:
        del replica_url, request, token


class RoundRobinPolicy(LoadBalancingPolicy, name='round_robin'):
    """Round-robin load balancing policy."""

    def __init__(self) -> None:
        super().__init__()
        self.index = 0

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        if set(self.ready_replicas) == set(ready_replicas):
            return
        # If the autoscaler keeps scaling up and down the replicas,
        # we need this shuffle to not let the first replica have the
        # most of the load.
        random.shuffle(ready_replicas)
        self.ready_replicas = ready_replicas
        self.index = 0

    def _select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        del request  # Unused.
        if not self.ready_replicas:
            return None
        ready_replica_url = self.ready_replicas[self.index]
        self.index = (self.index + 1) % len(self.ready_replicas)
        return ready_replica_url


class LeastLoadPolicy(LoadBalancingPolicy, name='least_load', default=True):
    """Least load load balancing policy."""

    def __init__(self) -> None:
        super().__init__()
        self.load_map: Dict[str, int] = collections.defaultdict(int)
        # url -> accounting generation; bumped whenever a key is (re)added
        # so stale releases from before a prune are ignored (ABA).
        self._generation: Dict[str, int] = collections.defaultdict(int)
        self.lock = threading.Lock()

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
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

    def _select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        del request  # Unused.
        if not self.ready_replicas:
            return None
        with self.lock:
            min_load = min(
                self.load_map.get(replica, 0)
                for replica in self.ready_replicas)
            # Random tie-break: deterministic min() over URL order biases
            # cold starts (all-zero loads) onto the same replica wave
            # after wave.
            candidates = [
                replica for replica in self.ready_replicas
                if self.load_map.get(replica, 0) == min_load
            ]
            return random.choice(candidates)

    def pre_execute_hook(self, replica_url: str,
                         request: 'fastapi.Request') -> Optional[Any]:
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
                          token: Optional[Any] = None) -> None:
        del request  # Unused.
        with self.lock:
            # Only decrement live keys, clamped at zero: a replica pruned
            # from the ready set mid-stream must not be recreated at -1
            # (phantom capacity that would attract traffic on re-add).
            if replica_url not in self.load_map:
                return
            if (token is not None and token != self._generation[replica_url]):
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
        self.replica_info: Dict[str, Dict[str, Any]] = {}  # replica_url -> info
        self.target_qps_per_accelerator: Dict[str, float] = {
        }  # accelerator_type -> target_qps

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
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

    def set_replica_info(self, replica_info: Dict[str, Dict[str, Any]]) -> None:
        """Set replica information including accelerator types.

        Args:
            replica_info: Dict mapping replica URL to replica information
                         e.g., {'http://url1': {'gpu_type': 'A100'}}
        """
        with self.lock:
            self.replica_info = replica_info
            logger.debug('Set replica info: %s', self.replica_info)

    def set_target_qps_per_accelerator(
            self, target_qps_per_accelerator: Dict[str, float]) -> None:
        """Set target QPS for each accelerator type."""
        with self.lock:
            self.target_qps_per_accelerator = target_qps_per_accelerator

    def _get_normalized_load(self, replica_url: str) -> float:
        """Get normalized load for a replica based on its GPU shape."""
        current_load = self.load_map.get(replica_url, 0)

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

        # Fallback
        logger.warning(
            f'No matching QPS found for accelerator type: {accelerator_type}. '
            f'Available types: {list(self.target_qps_per_accelerator.keys())}. '
            f'Using default value 1.0 as fallback.')
        return 1.0

    def _select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        del request  # Unused.
        if not self.ready_replicas:
            return None
        with self.lock:
            # Calculate normalized loads for all replicas
            replica_loads = []
            for replica in self.ready_replicas:
                normalized_load = self._get_normalized_load(replica)
                replica_loads.append((replica, normalized_load))

            # Select among the (near-)minimum normalized loads at random:
            # deterministic min() over URL order biases cold starts.
            min_load = min(load for _, load in replica_loads)
            candidates = [
                replica for replica, load in replica_loads
                if load - min_load <= 1e-9
            ]
            selected_replica = random.choice(candidates)
            logger.debug('Available replicas and loads: %s', replica_loads)
            logger.debug('Selected replica: %s', selected_replica)
            return selected_replica

    # pre_execute_hook and post_execute_hook are inherited from LeastLoadPolicy
