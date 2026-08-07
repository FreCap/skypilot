"""Adversarial probes for PR #699: bounded precommit drains across restart."""
# pylint: disable=protected-access
import threading
from unittest import mock

from sky.serve import replica_managers
from sky.utils import common_utils


def _make_manager(service_name='svc', next_replica_id=1):
    """Bare SkyPilotReplicaManager skipping the heavy __init__ (mirrors the
    helper in test_serve_replica_managers.py, which CI cannot import across
    test modules)."""
    mgr = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.RLock()
    mgr._service_name = service_name
    mgr._next_replica_id = next_replica_id
    mgr.latest_version = 1
    mgr.yaml_content = 'resources: {}'
    mgr._launch_thread_pool = {}
    mgr._down_thread_pool = {}
    mgr._failed_cleanup_retry_attempts = {}
    mgr._failed_cleanup_retry_at = {}
    mgr._tick_version_spec_cache = {}
    mgr._spot_placer = None
    mgr._pending_version = None
    mgr._uses_logical_replicas = False
    mgr._logical_reconcile_snapshot = None
    mgr._logical_target = None
    mgr._logical_state_lock = threading.RLock()
    mgr._logical_controller_epoch = 'test-controller-epoch'
    mgr._wait_for_idle_trackers = {}
    mgr._recovering_logical_retirement_ids = set()
    mgr._logical_retirement_recovery_deadline = None
    mgr._logical_retirement_reactivation_generation = None
    return mgr


def _bounded_precommit_info(replica_id=1, epoch='old-epoch'):
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None,
                                        planned_capacity=1)
    s = info.status_property
    s.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    s.service_ready_now = True
    s.is_scale_down = True
    s.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    s.drain_cap_seconds = 3900
    s.drain_started_at = replica_managers.time.time() - 4000  # consumed
    s.wait_for_idle_before_termination = False
    s.logical_retirement_version = 2
    s.logical_retirement_controller_epoch = epoch
    s.logical_retirement_generation = 4
    s.logical_retirement_target_capacity = 1
    s.logical_retirement_confirmed_generation = 4
    s.logical_retirement_bounded_deadline = True
    s.logical_retirement_committed = False
    return info


def _restart_manager(info):
    """Manager as after boot recovery indexed the bounded precommit row."""
    mgr = _make_manager()
    mgr.latest_version = 2
    mgr._uses_logical_replicas = True
    mgr._is_pool = True  # skip URL tracker
    mgr._persist_replica = mock.Mock()
    mgr._terminate_replica = mock.Mock()
    # Mirror the recovery-scan branch (eb3f8bab, replica_managers.py ~2481).
    assert replica_managers.SkyPilotReplicaManager.\
        _is_recoverable_uncommitted_logical_retirement(info)
    mgr._register_wait_for_idle(info)
    mgr._recovering_logical_retirement_ids = {info.replica_id}
    return mgr


def test_probe1_refresh_pops_tracker_of_recovering_bounded_row():
    """First refresh tick after restart: does the tracker survive?"""
    info = _bounded_precommit_info()
    mgr = _restart_manager(info)
    assert 1 in mgr._wait_for_idle_trackers
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos_from_ids',
                           return_value={1: info}), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_status_fields',
                           return_value={'svc-1': object()}):
        mgr._refresh_wait_for_idle()
    assert 1 in mgr._wait_for_idle_trackers, (
        'BUG: tracker popped while row still owned by recovery '
        '(wait_for_idle False, no down worker after restart)')


def test_probe2_full_restart_lifecycle_strands_teardown():
    """Restart -> refresh tick -> adopt -> release: teardown never resumes."""
    info = _bounded_precommit_info()
    mgr = _restart_manager(info)
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos_from_ids',
                           return_value={1: info}), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_status_fields',
                           return_value={'svc-1': object()}):
        mgr._refresh_wait_for_idle()  # tick before evidence arrives

    # Fresh evidence arrives: adoption pass, then newer generation releases.
    for gen in (5, 6):
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=2,
                generation=gen,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (2, gen, 0)  # target 0: no shortfall
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]):
            mgr._reconcile_recovering_logical_retirements()

    assert not mgr._recovering_logical_retirement_ids
    assert info.status_property.is_scale_down  # still off route: good
    # Now: who resumes teardown? No down worker, run another refresh tick.
    mgr._finish_logical_retirement = mock.Mock()
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos_from_ids',
                           return_value={1: info}), \
         mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_cluster_status_fields',
                           return_value={'svc-1': object()}):
        mgr._refresh_wait_for_idle()
    assert (mgr._finish_logical_retirement.called or
            mgr._terminate_replica.called or 1 in mgr._down_thread_pool or
            1 in mgr._wait_for_idle_trackers), (
                'BUG: bounded precommit row stranded after adoption+release: '
                'no tracker, no down worker, teardown never re-driven')


def test_probe3_shortfall_reactivation_still_works():
    """Reactivation on shortfall must still readmit bounded rows (sanity)."""
    info = _bounded_precommit_info()
    mgr = _restart_manager(info)
    mgr._logical_reconcile_snapshot = (
        replica_managers.LogicalReconcileSnapshot(
            version=2,
            generation=5,
            observed_slots_by_replica_id={},
            in_flight_by_replica_id={1: 0},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic()))
    mgr._logical_target = (2, 5, 3)  # shortfall of 3
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]):
        mgr._reconcile_recovering_logical_retirements()
    assert not info.status_property.is_scale_down
    assert not mgr._recovering_logical_retirement_ids


def test_same_total_exact_card_shift_reactivates_required_card():
    """Aggregate coverage cannot retire the only backend of a target card."""
    retiring_l4 = _bounded_precommit_info()
    retiring_l4.resources_override = {'accelerators': {'L4': 1}}
    ready_a100 = replica_managers.ReplicaInfo(
        replica_id=2,
        cluster_name='svc-2',
        replica_port='8080',
        is_spot=True,
        location=None,
        version=2,
        resources_override={'accelerators': {
            'A100': 1
        }},
        planned_capacity=1)
    ready_status = ready_a100.status_property
    ready_status.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready_status.service_ready_now = True
    ready_status.first_ready_time = 1

    mgr = _restart_manager(retiring_l4)
    mgr._logical_reconcile_snapshot = (
        replica_managers.LogicalReconcileSnapshot(
            version=2,
            generation=5,
            observed_slots_by_replica_id={2: 1},
            in_flight_by_replica_id={
                1: 0,
                2: 0
            },
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic()))
    mgr._logical_target = (2, 5, 1, (('L4', 1),), (('L4', 1), ('A100', 1)))

    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[retiring_l4, ready_a100]):
        mgr._reconcile_recovering_logical_retirements()

    assert not retiring_l4.status_property.is_scale_down
    assert not mgr._recovering_logical_retirement_ids
