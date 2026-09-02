"""Consecutive-failure window bookkeeping must stay O(1) per replica.

The probe loop used to append every failed probe time to
ReplicaInfo.consecutive_failure_times but only ever read the first and
last entries (== the current probe time), so the list was pure growth:
a replica flapping for hours accumulated hundreds of timestamps, all
pickled into the replica row on every probe round. The window is now a
single first-failure timestamp with identical teardown semantics.

The reducer plans a desired row per probe result, commits the window
through ``SkyPilotReplicaManager._commit_probe_row_plans`` and publishes
the accepted rows; these tests accept every plan at that seam.
"""
# pylint: disable=protected-access
import copy
import pickle
import threading
import unittest
from unittest import mock

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import system_recovery_state
from sky.utils import common_utils


def _replica_info(replica_id):
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-replica-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=None,
        version=1,
        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.first_ready_time = 1.0
    return info


def _make_manager(failure_threshold):
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager.lock = threading.Lock()
    manager._service_name = 'svc'
    manager._is_pool = False
    manager._uptime = 1.0
    manager._tick_version_spec_cache = {}
    manager._get_readiness_path = mock.Mock(return_value='/')
    manager._get_post_data = mock.Mock(return_value=None)
    manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
    manager._get_readiness_headers = mock.Mock(return_value=None)
    manager._get_initial_delay_seconds = mock.Mock(return_value=1200)
    manager._consecutive_failure_threshold_timeout = mock.Mock(
        return_value=failure_threshold)
    manager._cloud_instance_looks_alive = mock.Mock(
        return_value=(replica_managers._PreemptionPrefilterResult(
            replica_managers._PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN)))
    manager._terminate_replica = mock.Mock()
    manager._db_fence_kwargs = mock.Mock(return_value={})
    manager._resolve_probe_urls = mock.Mock(
        side_effect=lambda infos, **_kwargs: {
            info.replica_id: f'http://10.0.0.{info.replica_id}:8080'
            for info in infos
        })
    manager._system_recovery_route_registry = mock.Mock()
    manager._teardown_wakeup = threading.Event()
    manager._launch_completion_state = mock.Mock(
        return_value=(mock.Mock(), manager._teardown_wakeup))
    # The committed rows are what the next probe round reads back from the
    # database, so the commit seam writes into the same in-memory table the
    # patched readers serve from.
    manager._rows = {}

    def _accept(plans):
        accepted = {
            plan.opening_info.replica_id: copy.deepcopy(plan.desired_info)
            for plan in plans
        }
        manager._rows.update(accepted)
        return accepted, set()

    manager._commit_probe_row_plans = mock.Mock(side_effect=_accept)
    return manager


def _probe_round(manager, info):
    """Run one probe round with ``info`` as the only durable row."""
    manager._rows[info.replica_id] = info
    with mock.patch.object(serve_state, 'get_replica_infos',
                           side_effect=lambda _name: list(
                               manager._rows.values())), \
         mock.patch.object(serve_state, 'get_specs',
                           return_value={1: mock.Mock()}), \
         mock.patch.object(serve_state, 'get_replica_infos_from_ids',
                           side_effect=lambda _name, ids: {
                               replica_id: manager._rows[replica_id]
                               for replica_id in ids
                           }), \
         mock.patch.object(serve_state, 'set_service_uptime'):
        (published,) = manager._probe_all_replicas()
    (durable,) = manager._rows.values()
    assert published is durable
    return durable


class TestConsecutiveFailureWindow(unittest.TestCase):
    """Window start / reset / teardown semantics across probe rounds.

    Since the plan/commit/publish probe reducer, a round never mutates the
    opening row: it plans a desired row, commits it through
    ``_commit_probe_row_plans`` and publishes the accepted row. Each round
    below therefore feeds the previously accepted row back in, exactly like
    the next probe round reads it from the database.
    """

    def _run_round(self, manager, info, probe_succeeded, probe_time):
        info.probe = mock.Mock(return_value=(info, probe_succeeded, probe_time))
        accepted = _probe_round(manager, info)
        self.assertIsNot(accepted, info)
        return accepted

    def test_window_opens_on_first_failure_and_keeps_start(self):
        manager = _make_manager(failure_threshold=60)
        info = self._run_round(manager,
                               _replica_info(1),
                               False,
                               probe_time=100.0)
        self.assertEqual(info.first_consecutive_failure_time, 100.0)
        info = self._run_round(manager, info, False, probe_time=130.0)
        # Window start is preserved, not moved to the latest failure.
        self.assertEqual(info.first_consecutive_failure_time, 100.0)
        manager._terminate_replica.assert_not_called()
        self.assertFalse(manager._teardown_wakeup.is_set())

    def test_teardown_fires_when_window_reaches_threshold(self):
        manager = _make_manager(failure_threshold=60)
        info = self._run_round(manager,
                               _replica_info(1),
                               False,
                               probe_time=100.0)
        info = self._run_round(manager, info, False, probe_time=159.9)
        self.assertIsNone(info.status_property.sky_down_status)
        self.assertFalse(manager._teardown_wakeup.is_set())
        info = self._run_round(manager, info, False, probe_time=160.0)
        # Teardown is a durable intent on the committed row plus a cleanup
        # wakeup; provider cleanup never starts under the fleet mutex.
        manager._terminate_replica.assert_not_called()
        self.assertEqual(info.status_property.sky_down_status,
                         common_utils.ProcessStatus.SCHEDULED)
        self.assertFalse(info.status_property.service_ready_now)
        self.assertTrue(manager._teardown_wakeup.is_set())

    def test_successful_probe_resets_window(self):
        manager = _make_manager(failure_threshold=60)
        info = self._run_round(manager,
                               _replica_info(1),
                               False,
                               probe_time=100.0)
        info = self._run_round(manager, info, True, probe_time=110.0)
        self.assertIsNone(info.first_consecutive_failure_time)
        # A new failure run starts a fresh window: old failures at 100.0
        # must not count toward the threshold.
        info = self._run_round(manager, info, False, probe_time=120.0)
        self.assertEqual(info.first_consecutive_failure_time, 120.0)
        info = self._run_round(manager, info, False, probe_time=170.0)
        self.assertIsNone(info.status_property.sky_down_status)
        manager._terminate_replica.assert_not_called()
        self.assertFalse(manager._teardown_wakeup.is_set())

    def test_window_not_opened_before_first_ready(self):
        manager = _make_manager(failure_threshold=60)
        info = _replica_info(1)
        info.status_property.first_ready_time = None
        info = self._run_round(manager, info, False, probe_time=100.0)
        self.assertIsNone(info.first_consecutive_failure_time)
        self.assertEqual(info.first_not_ready_time, 100.0)

    def test_rows_never_change_when_the_commit_seam_has_no_owner_fence(self):
        # A manager without its exact owner fence must not publish evidence.
        # This is the silent path the production commit seam takes; the
        # published snapshot is then the unchanged opening row.
        manager = _make_manager(failure_threshold=60)
        manager._commit_probe_row_plans = (
            replica_managers.SkyPilotReplicaManager._commit_probe_row_plans.
            __get__(manager))
        self.assertIsNone(manager._system_recovery_mutation_fence())
        info = _replica_info(1)
        info.probe = mock.Mock(return_value=(info, False, 100.0))
        with mock.patch.object(serve_state,
                               'commit_replica_observations_batch') as commit:
            published = _probe_round(manager, info)
        commit.assert_not_called()
        self.assertIs(published, info)
        self.assertIsNone(info.first_consecutive_failure_time)


class TestFailureBookkeepingIsConstantSize(unittest.TestCase):
    """Per-replica probe bookkeeping must not grow with failure count."""

    def test_pickled_row_size_constant_across_failed_rounds(self):
        manager = _make_manager(failure_threshold=10**9)
        info = replica_managers.ReplicaInfo(replica_id=1,
                                            cluster_name='svc-replica-1',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.first_ready_time = 1.0

        def size_after_failed_rounds(info, num_rounds):
            for i in range(num_rounds):
                probe = mock.Mock(return_value=(info, False, 100.0 + i))
                with mock.patch.object(replica_managers.ReplicaInfo, 'probe',
                                       probe):
                    info = _probe_round(manager, info)
            return info, len(pickle.dumps(info))

        info, size_after_2 = size_after_failed_rounds(info, 2)
        info, size_after_100 = size_after_failed_rounds(info, 98)
        self.assertEqual(size_after_2, size_after_100)
        self.assertEqual(info.first_consecutive_failure_time, 100.0)


class TestReplicaInfoUnpickleMigration(unittest.TestCase):
    """Pre-version-7 rows migrate the failure-times list to the scalar."""

    def _base_state(self, version):
        return {
            '_version': version,
            'replica_id': 1,
            'cluster_name': 'svc-replica-1',
            'version': 1,
            'replica_port': '8080',
            'created_at': 1.0,
            'first_not_ready_time': None,
            'status_property': replica_managers.ReplicaStatusProperty(),
            'is_spot': False,
            'location': None,
            'resources_override': None,
            'reserved_fill': False,
            'cost_rebalance_for_replica_id': None,
        }

    def _restore(self, state):
        info = object.__new__(replica_managers.ReplicaInfo)
        info.__setstate__(state)
        return info

    def test_old_row_with_failures_keeps_window_start(self):
        state = self._base_state(version=6)
        state['consecutive_failure_times'] = [5.0, 7.0, 9.0]
        info = self._restore(state)
        self.assertEqual(info.first_consecutive_failure_time, 5.0)
        self.assertFalse(hasattr(info, 'consecutive_failure_times'))

    def test_old_row_without_failures_has_no_window(self):
        state = self._base_state(version=6)
        state['consecutive_failure_times'] = []
        info = self._restore(state)
        self.assertIsNone(info.first_consecutive_failure_time)

    def test_current_version_round_trips(self):
        info = replica_managers.ReplicaInfo(replica_id=1,
                                            cluster_name='svc-replica-1',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.first_consecutive_failure_time = 42.0
        restored = pickle.loads(pickle.dumps(info))
        self.assertEqual(restored.first_consecutive_failure_time, 42.0)


if __name__ == '__main__':
    unittest.main()
