"""Tests for the multi-node recovery debouncer in sky/jobs/controller.py.

The cluster health probe is all-or-nothing (every node must appear in
`ray status`), so for large multi-node jobs a single transiently lagging
raylet flags the whole cluster non-UP while the job is still running.
Recovery tears down and relaunches the entire cluster, so a spurious
single-tick verdict must not trigger it: the controller requires
consecutive not-UP observations (while the job reports non-terminal)
before recovering. Single-node jobs keep the immediate behavior.
"""
from sky.jobs import controller


def test_multinode_requires_consecutive_observations():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=500)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    assert threshold > 1
    for _ in range(threshold - 1):
        assert debouncer.should_recover_now() is False
    assert debouncer.should_recover_now() is True
    # Once past the threshold, it stays triggered until reset.
    assert debouncer.should_recover_now() is True


def test_single_node_recovers_on_first_observation():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=1)
    assert debouncer.should_recover_now() is True


def test_reset_clears_accumulated_observations():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=2)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    for _ in range(threshold - 1):
        assert debouncer.should_recover_now() is False
    # An UP observation (or a completed recovery) resets the streak: a new
    # flap must re-accumulate the full threshold.
    debouncer.reset()
    for _ in range(threshold - 1):
        assert debouncer.should_recover_now() is False
    assert debouncer.should_recover_now() is True
