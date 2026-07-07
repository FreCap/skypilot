"""Tests for the external LB applying a controller-fetched routing spec.

`SkyServeLoadBalancer._apply_routing_spec` is how `sky serve update` reaches a
running load balancer without a re-roll: the load-balancing policy, per-replica
target QPS, and stream timeout arrive over the sync channel and are applied
live. These tests exercise that handler on parsed state (which policy object is
active, its target-qps map, the stored stream timeout) -- not on any logging.
"""
# pylint: disable=invalid-name,protected-access
from sky.serve import constants
from sky.serve import load_balancer
from sky.serve import load_balancing_policies as lb_policies


def _make_lb(policy_name=None) -> load_balancer.SkyServeLoadBalancer:
    return load_balancer.SkyServeLoadBalancer(
        controller_url='http://ctrl:8001',
        load_balancer_port=8890,
        load_balancing_policy_name=policy_name)


def test_defaults_until_synced():
    # A standalone LB seeds None -> the default policy and the built-in stream
    # timeout, until the first sync populates the real spec.
    lb = _make_lb()
    assert (lb._load_balancing_policy_name == lb_policies.DEFAULT_LB_POLICY)
    assert (lb._stream_timeout_seconds == constants.DEFAULT_LB_STREAM_TIMEOUT)


def test_policy_swap_rebuilds_and_updates_name():
    lb = _make_lb()  # default: least_load
    assert not isinstance(lb._load_balancing_policy,
                          lb_policies.InstanceAwareLeastLoadPolicy)
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'L4': 2.5
        },
        'stream_timeout_seconds': 90,
    })
    # Policy object swapped to the instance-aware policy...
    assert isinstance(lb._load_balancing_policy,
                      lb_policies.InstanceAwareLeastLoadPolicy)
    assert lb._load_balancing_policy_name == 'instance_aware_least_load'
    # ...target QPS applied to the (now instance-aware) policy...
    assert (lb._load_balancing_policy.target_qps_per_accelerator == {'L4': 2.5})
    # ...and the stream timeout is live.
    assert lb._stream_timeout_seconds == 90


def test_policy_not_rebuilt_when_name_unchanged():
    lb = _make_lb('instance_aware_least_load')
    policy_obj = lb._load_balancing_policy
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'A100': 4.0
        },
        'stream_timeout_seconds': 30,
    })
    # Same policy name -> the object is not rebuilt (identity preserved),
    # but target QPS / stream timeout still update in place.
    assert lb._load_balancing_policy is policy_obj
    assert (lb._load_balancing_policy.target_qps_per_accelerator == {
        'A100': 4.0
    })
    assert lb._stream_timeout_seconds == 30


def test_target_qps_ignored_for_non_instance_aware_policy():
    lb = _make_lb('round_robin')
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'round_robin',
        'target_qps_per_replica': {
            'L4': 2.5
        },
        'stream_timeout_seconds': 60,
    })
    # A non-instance-aware policy has no target-qps state to set; only the
    # stream timeout applies.
    assert lb._load_balancing_policy_name == 'round_robin'
    assert not hasattr(lb._load_balancing_policy, 'target_qps_per_accelerator')
    assert lb._stream_timeout_seconds == 60


def test_ready_replicas_repopulated_after_swap():
    # After a policy swap the new object is empty; a subsequent
    # set_ready_replicas (what the sync loop does) fully initializes it.
    lb = _make_lb()  # least_load
    lb._apply_routing_spec(
        {'load_balancing_policy_name': 'instance_aware_least_load'})
    urls = ['http://a:8080', 'http://b:8080']
    lb._load_balancing_policy.set_ready_replicas(urls)
    assert set(lb._load_balancing_policy.ready_replicas) == set(urls)
    # Load map initialized for each replica (not short-circuited).
    assert all(lb._load_balancing_policy.load_map[u] == 0 for u in urls)
