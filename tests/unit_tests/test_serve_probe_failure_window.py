"""Consecutive-failure window bookkeeping must stay O(1) per replica.

The probe loop used to append every failed probe time to
ReplicaInfo.consecutive_failure_times but only ever read the first and
last entries (== the current probe time), so the list was pure growth:
a replica flapping for hours accumulated hundreds of timestamps, all
pickled into the replica row on every probe round. The window is now a
single first-failure timestamp with identical teardown semantics.
"""
# pylint: disable=protected-access
import pickle
import threading
import unittest
from unittest import mock

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import system_recovery_state
from sky.utils import common_utils


def _replica_info(replica_id):
    info = mock.Mock()
    info.replica_id = replica_id
    info.cluster_name = f'svc-replica-{replica_id}'
    info.version = 1
    info.url = f'http://10.0.0.{replica_id}:8080'
    info.is_spot = False
    info.status_property.should_track_service_status.return_value = True
    info.status_property.first_ready_time = 1.0
    info.first_consecutive_failure_time = None
    info.first_not_ready_time = None
    # This fixture models an ordinary replica.  Leaving newly added recovery
    # fields as implicit Mock children makes ``quarantine is not None`` true and
    # correctly drives the production probe loop into fail-closed teardown.
    info.system_recovery_quarantine = None
    info.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.ORDINARY)
    info.system_recovery = None
    return info


def _make_manager(failure_threshold):
    manager = object.__new__(replica_managers.SkyPilotReplicaManager)
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
    manager._handle_preemption = mock.Mock(return_value=False)
    manager._cloud_instance_looks_alive = mock.Mock(return_value=True)
    manager._terminate_replica = mock.Mock()
    manager._db_fence_kwargs = mock.Mock(return_value={})
    manager._resolve_probe_urls = mock.Mock(
        side_effect=lambda infos: {info.replica_id: info.url for info in infos})
    return manager


class TestConsecutiveFailureWindow(unittest.TestCase):
    """Window start / reset / teardown semantics across probe rounds."""

    def _run_round(self, manager, info, probe_succeeded, probe_time):
        info.probe = mock.Mock(return_value=(info, probe_succeeded, probe_time))
        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'get_replica_infos_from_ids',
                               return_value={}), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

    def test_window_opens_on_first_failure_and_keeps_start(self):
        manager = _make_manager(failure_threshold=60)
        info = _replica_info(1)
        self._run_round(manager, info, False, probe_time=100.0)
        self.assertEqual(info.first_consecutive_failure_time, 100.0)
        self._run_round(manager, info, False, probe_time=130.0)
        # Window start is preserved, not moved to the latest failure.
        self.assertEqual(info.first_consecutive_failure_time, 100.0)
        manager._terminate_replica.assert_not_called()

    def test_teardown_fires_when_window_reaches_threshold(self):
        manager = _make_manager(failure_threshold=60)
        info = _replica_info(1)
        self._run_round(manager, info, False, probe_time=100.0)
        self._run_round(manager, info, False, probe_time=159.9)
        manager._terminate_replica.assert_not_called()
        self._run_round(manager, info, False, probe_time=160.0)
        manager._terminate_replica.assert_called_once_with(
            1, sync_down_logs=True, replica_drain_delay_seconds=0)

    def test_successful_probe_resets_window(self):
        manager = _make_manager(failure_threshold=60)
        info = _replica_info(1)
        self._run_round(manager, info, False, probe_time=100.0)
        self._run_round(manager, info, True, probe_time=110.0)
        self.assertIsNone(info.first_consecutive_failure_time)
        # A new failure run starts a fresh window: old failures at 100.0
        # must not count toward the threshold.
        self._run_round(manager, info, False, probe_time=120.0)
        self.assertEqual(info.first_consecutive_failure_time, 120.0)
        self._run_round(manager, info, False, probe_time=170.0)
        manager._terminate_replica.assert_not_called()

    def test_window_not_opened_before_first_ready(self):
        manager = _make_manager(failure_threshold=60)
        info = _replica_info(1)
        info.status_property.first_ready_time = None
        self._run_round(manager, info, False, probe_time=100.0)
        self.assertIsNone(info.first_consecutive_failure_time)
        self.assertEqual(info.first_not_ready_time, 100.0)


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

        def size_after_failed_rounds(num_rounds):
            for i in range(num_rounds):
                probe = mock.Mock(return_value=(info, False, 100.0 + i))
                with mock.patch.object(serve_state, 'get_replica_infos',
                                       return_value=[info]), \
                     mock.patch.object(serve_state, 'get_specs',
                                       return_value={1: mock.Mock()}), \
                     mock.patch.object(serve_state,
                                       'add_or_update_replicas'), \
                     mock.patch.object(replica_managers.ReplicaInfo, 'probe',
                                       probe):
                    manager._probe_all_replicas()
            return len(pickle.dumps(info))

        size_after_2 = size_after_failed_rounds(2)
        size_after_100 = size_after_failed_rounds(98)
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
