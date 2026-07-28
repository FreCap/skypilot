"""Tests for cluster not-UP confirmation in sky/jobs/controller.py.

The cluster health probe is all-or-nothing (every node must appear in
`ray status`), so for large multi-node jobs a single transiently lagging
raylet flags the whole cluster non-UP while the job is still running.
Recovery tears down and relaunches the entire cluster, so a spurious
single-tick verdict must not trigger it. The controller requires
consecutive INIT observations before recovering, including when the same
control-plane flap temporarily hides the remote job status.
"""
# pylint: disable=protected-access

from sky.jobs import controller
from sky.skylet import job_lib
from sky.utils import status_lib


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


def test_reset_restores_single_node_default_threshold():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=1)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    assert debouncer.should_recover_now(threshold) is False
    assert debouncer.required_confirmations == threshold

    debouncer.reset()

    assert debouncer.observations == 0
    assert debouncer.required_confirmations == 1
    assert debouncer.should_recover_now() is True


def test_init_with_running_job_waits_for_confirmation():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=4)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    for _ in range(threshold - 1):
        assert controller._should_wait_for_cluster_not_up_confirmation(
            status_lib.ClusterStatus.INIT, job_lib.JobStatus.RUNNING, None,
            None, debouncer) is True
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.INIT, job_lib.JobStatus.RUNNING, None, None,
        debouncer) is False


def test_init_with_transient_status_fetch_waits_for_confirmation():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=4)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    for _ in range(threshold - 1):
        assert controller._should_wait_for_cluster_not_up_confirmation(
            status_lib.ClusterStatus.INIT, None, 'transient', None,
            debouncer) is True
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.INIT, None, 'transient', None,
        debouncer) is False


def test_single_node_transient_init_waits_if_last_status_was_running():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=1)
    threshold = controller._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
    for _ in range(threshold - 1):
        assert controller._should_wait_for_cluster_not_up_confirmation(
            status_lib.ClusterStatus.INIT, None, 'transient',
            job_lib.JobStatus.RUNNING, debouncer) is True
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.INIT, None, 'transient',
        job_lib.JobStatus.RUNNING, debouncer) is False


def test_single_node_transient_init_without_last_status_recovers_immediately():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=1)
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.INIT, None, 'transient', None,
        debouncer) is False


def test_non_init_statuses_do_not_wait_for_confirmation():
    debouncer = controller._ClusterNotUpDebouncer(num_nodes=4)
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.STOPPED, None, 'transient', None,
        debouncer) is False
    assert controller._should_wait_for_cluster_not_up_confirmation(
        None, None, 'transient', None, debouncer) is False
    assert controller._should_wait_for_cluster_not_up_confirmation(
        status_lib.ClusterStatus.INIT, None, None, None, debouncer) is False
