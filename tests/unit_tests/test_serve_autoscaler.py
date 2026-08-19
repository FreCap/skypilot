"""Unit tests for sky.serve.autoscalers."""
# pylint: disable=protected-access
import threading
import time
import types
import unittest
from unittest import mock

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import controller as serve_controller
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils import operator_notifications


def _autoscaler_spec(**overrides):
    """Build a complete mutable SkyServiceSpec test interface."""
    values = {
        'min_replicas': 0,
        'min_replicas_by_accelerator': {},
        'max_replicas': 10,
        'num_overprovision': None,
        'replica_unit': 'physical_backend',
        'target_qps_per_replica': None,
        'target_concurrency_per_replica': None,
        'pool': False,
        'use_ondemand_fallback': False,
        'queue_length_threshold': None,
        'upscale_delay_seconds': None,
        'downscale_delay_seconds': None,
        'reserved_capacity_fill': False,
        'reserved_fill_floor_replicas': 0,
        'reserved_fill_weight': 1.0,
        'reserved_fill_utilization_gate': False,
        'cost_rebalance': False,
        'cost_rebalance_min_savings_fraction': 0.3,
        'cost_rebalance_max_parallel_replacements': 1,
        'cost_rebalance_stabilization_seconds': 300.0,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class TestSelectNonterminalReplicasToScaleDown(unittest.TestCase):
    """Test cases for _select_nonterminal_replicas_to_scale_down."""

    def setUp(self):
        """Set up test fixtures."""
        self.service_name = 'test-service'

        # Create mock ReplicaInfo objects
        self.replica1 = mock.Mock(spec=replica_managers.ReplicaInfo)
        self.replica1.replica_id = 1
        self.replica1.cluster_name = 'test-cluster-1'
        self.replica1.version = 1
        self.replica1.status = serve_state.ReplicaStatus.READY

        self.replica2 = mock.Mock(spec=replica_managers.ReplicaInfo)
        self.replica2.replica_id = 2
        self.replica2.cluster_name = 'test-cluster-2'
        self.replica2.version = 1
        self.replica2.status = serve_state.ReplicaStatus.READY

        self.replica3 = mock.Mock(spec=replica_managers.ReplicaInfo)
        self.replica3.replica_id = 3
        self.replica3.cluster_name = 'test-cluster-3'
        self.replica3.version = 1
        self.replica3.status = serve_state.ReplicaStatus.READY

    @mock.patch('sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool')
    def test_select_replicas_with_job_counts(self, mock_get_counts):
        """Test that replicas with fewer jobs are selected first."""

        # Mock job counts: replica1 has 2 jobs, replica2 has 0 jobs,
        # replica3 has 1 job
        mock_get_counts.return_value = {
            'test-cluster-1': 2,
            'test-cluster-3': 1,
            # test-cluster-2 absent means 0 jobs
        }

        replica_infos = [self.replica1, self.replica2, self.replica3]

        # Select 2 replicas to scale down
        result = autoscalers._select_nonterminal_replicas_to_scale_down(
            2, replica_infos, self.service_name)

        # Should select replica2 (0 jobs) and replica3 (1 job) first
        # Order should be: replica2 (0 jobs), replica3 (1 job), replica1
        # (2 jobs). Since we're selecting 2, we should get [2, 3]
        self.assertEqual(len(result), 2)
        self.assertEqual(result, [2, 3])

        # Verify the function was called once with the service name
        mock_get_counts.assert_called_once_with(self.service_name)

    @mock.patch('sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool')
    def test_select_replicas_with_same_job_counts(self, mock_get_counts):
        """Test that when job counts are equal, other sorting criteria apply."""
        # All replicas have the same number of jobs
        mock_get_counts.return_value = {
            'test-cluster-1': 1,
            'test-cluster-2': 1,
            'test-cluster-3': 1,
        }

        replica_infos = [self.replica1, self.replica2, self.replica3]

        # Select 2 replicas to scale down
        result = autoscalers._select_nonterminal_replicas_to_scale_down(
            2, replica_infos, self.service_name)

        # When job counts are equal, should fall back to replica_id
        # descending order. So replica3 (id=3) and replica2 (id=2)
        # should be selected.
        self.assertEqual(len(result), 2)
        self.assertEqual(result, [3, 2])

    @mock.patch('sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool')
    def test_select_replicas_with_status_priority(self, mock_get_counts):
        """Test that status priority is still respected."""
        # Create replicas with different statuses
        replica_provisioning = mock.Mock(spec=replica_managers.ReplicaInfo)
        replica_provisioning.replica_id = 1
        replica_provisioning.cluster_name = 'test-cluster-1'
        replica_provisioning.version = 1
        replica_provisioning.status = serve_state.ReplicaStatus.PROVISIONING

        replica_ready = mock.Mock(spec=replica_managers.ReplicaInfo)
        replica_ready.replica_id = 2
        replica_ready.cluster_name = 'test-cluster-2'
        replica_ready.version = 1
        replica_ready.status = serve_state.ReplicaStatus.READY

        # PROVISIONING replica has more jobs, but should still be selected
        # first
        mock_get_counts.return_value = {
            'test-cluster-1': 3,
            'test-cluster-2': 1,
        }

        replica_infos = [replica_provisioning, replica_ready]

        # Select 1 replica to scale down
        result = autoscalers._select_nonterminal_replicas_to_scale_down(
            1, replica_infos, self.service_name)

        # Should select PROVISIONING replica first despite having more jobs
        self.assertEqual(len(result), 1)
        self.assertEqual(result, [1])

    @mock.patch('sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool')
    def test_select_replicas_with_version_priority(self, mock_get_counts):
        """Test that version priority is still respected."""
        # Create replicas with different versions
        replica_old = mock.Mock(spec=replica_managers.ReplicaInfo)
        replica_old.replica_id = 1
        replica_old.cluster_name = 'test-cluster-1'
        replica_old.version = 1
        replica_old.status = serve_state.ReplicaStatus.READY

        replica_new = mock.Mock(spec=replica_managers.ReplicaInfo)
        replica_new.replica_id = 2
        replica_new.cluster_name = 'test-cluster-2'
        replica_new.version = 2
        replica_new.status = serve_state.ReplicaStatus.READY

        # New version replica has fewer jobs, but old version should be
        # selected first
        mock_get_counts.return_value = {
            'test-cluster-1': 2,
            # test-cluster-2 absent means 0 jobs
        }

        replica_infos = [replica_old, replica_new]

        # Select 1 replica to scale down
        result = autoscalers._select_nonterminal_replicas_to_scale_down(
            1, replica_infos, self.service_name)

        # Should select old version replica first despite having more jobs
        self.assertEqual(len(result), 1)
        self.assertEqual(result, [1])


class TestAutoscalerVersionInitialization(unittest.TestCase):
    """The autoscaler must be constructed at the recovered service version.

    On an API-server restart/respawn the controller is rebuilt and passes the
    durable latest version to `Autoscaler.from_spec`. If the autoscaler instead
    reset to INITIAL_VERSION (1) while live replicas are at version >= 2 (any
    service updated at least once), its version filters would treat every
    running replica as outdated and drive permanent replica churn. These tests
    pin that `version` is honored at construction and forwarded by `from_spec`.
    """

    def test_base_init_sets_latest_version_from_arg(self):
        spec = _autoscaler_spec(min_replicas=1, max_replicas=3)
        autoscaler = object.__new__(autoscalers.Autoscaler)
        autoscalers.Autoscaler.__init__(autoscaler, 'svc', spec, version=5)
        self.assertEqual(autoscaler.latest_version, 5)
        # Must stay one below latest so the unrecoverable-failure early-return
        # only arms once a replica at the latest version becomes ready.
        self.assertEqual(autoscaler.latest_version_ever_ready, 4)

    def test_base_init_defaults_to_initial_version(self):
        spec = _autoscaler_spec(min_replicas=1, max_replicas=3)
        autoscaler = object.__new__(autoscalers.Autoscaler)
        autoscalers.Autoscaler.__init__(autoscaler, 'svc', spec)
        self.assertEqual(autoscaler.latest_version, constants.INITIAL_VERSION)
        self.assertEqual(autoscaler.latest_version_ever_ready,
                         constants.INITIAL_VERSION - 1)

    def _route_spec(self,
                    pool=False,
                    use_ondemand_fallback=False,
                    target_qps_per_replica=2.0):
        return _autoscaler_spec(pool=pool,
                                use_ondemand_fallback=use_ondemand_fallback,
                                target_qps_per_replica=target_qps_per_replica)

    def test_from_spec_forwards_version_to_every_variant(self):
        # Each `from_spec` dispatch branch is a separate call site that could
        # individually drop the version argument.
        cases = [
            ('RequestRateAutoscaler', self._route_spec()),
            ('QueueLengthAutoscaler', self._route_spec(pool=True)),
            ('FallbackRequestRateAutoscaler',
             self._route_spec(use_ondemand_fallback=True)),
            ('InstanceAwareRequestRateAutoscaler',
             self._route_spec(target_qps_per_replica={'A100': 2.0})),
        ]
        for cls_name, spec in cases:
            with self.subTest(cls_name):
                with mock.patch.object(autoscalers, cls_name) as mock_cls:
                    autoscalers.Autoscaler.from_spec('svc', spec, version=7)
                mock_cls.assert_called_once_with('svc', spec, 7)

    def test_from_spec_defaults_to_initial_version(self):
        spec = self._route_spec()
        with mock.patch.object(autoscalers,
                               'RequestRateAutoscaler') as mock_cls:
            autoscalers.Autoscaler.from_spec('svc', spec)
        mock_cls.assert_called_once_with('svc', spec, constants.INITIAL_VERSION)

    def test_controller_passes_recovered_version_to_autoscaler(self):
        # Regression for the actual bug boundary: SkyServeController must hand
        # the recovered service version to the autoscaler factory (not drop
        # it), so a controller rebuilt on restart at version >= 2 doesn't
        # churn.
        with mock.patch.object(serve_controller.replica_managers,
                               'SkyPilotReplicaManager'), \
             mock.patch.object(serve_controller.autoscalers.Autoscaler,
                               'from_spec') as mock_from_spec, \
             mock.patch.object(
                 serve_controller.SkyServeController,
                 '_acknowledge_pending_placement_normalization'):
            serve_controller.SkyServeController(
                'svc',
                mock.MagicMock(),
                version=7,
                host='localhost',
                port=8000,
                controller_owner_fingerprint=('owner-a'))
        mock_from_spec.assert_called_once()
        # version is the 3rd positional arg (service_name, service_spec,
        # version).
        self.assertEqual(mock_from_spec.call_args.args[2], 7)


class TestRolloutBlockedNotifications(unittest.TestCase):
    """Rollout failures alert operators without replacing the old fleet."""

    @staticmethod
    def _autoscaler():
        spec = _autoscaler_spec(min_replicas=1,
                                max_replicas=1,
                                target_qps_per_replica=1.0)
        return autoscalers.RequestRateAutoscaler('svc', spec, version=2)

    @staticmethod
    def _failed_replica(*,
                        unrecoverable: bool,
                        status=serve_state.ReplicaStatus.FAILED_PROVISION,
                        is_scale_down: bool = False):
        info = mock.Mock()
        info.replica_id = 2
        info.version = 2
        info.is_ready = False
        info.is_terminal = True
        info.status = status
        info.status_property.is_scale_down = is_scale_down
        info.status_property.unrecoverable_failure.return_value = unrecoverable
        return info

    def test_unrecoverable_update_notifies_and_keeps_previous_version(self):
        autoscaler = self._autoscaler()
        info = self._failed_replica(unrecoverable=True)

        with mock.patch.object(autoscalers.operator_notifications,
                               'record_notification') as record_notification:
            decisions = autoscaler.generate_scaling_decisions([info], [1])

        self.assertEqual(decisions, [])
        failure = autoscaler.unrecoverable_rollout_failure
        self.assertIsNotNone(failure)
        assert failure is not None
        self.assertEqual(failure.version, 2)
        self.assertIn('2:FAILED_PROVISION', failure.reason)
        record_notification.assert_called_once()
        category, message = record_notification.call_args.args
        self.assertEqual(
            category, operator_notifications.OperatorNotificationCategory.
            SERVE_ROLLOUT_BLOCKED)
        self.assertIn("service 'svc'", message)
        self.assertIn('Version 1 remains active', message)

    def test_all_terminal_provisioning_attempts_notify_while_retrying(self):
        autoscaler = self._autoscaler()
        info = self._failed_replica(unrecoverable=False)

        with mock.patch.object(autoscalers.operator_notifications,
                               'record_notification') as record_notification:
            decisions = autoscaler.generate_scaling_decisions([info], [1])

        record_notification.assert_called_once()
        self.assertIsNone(autoscaler.unrecoverable_rollout_failure)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator,
                         autoscalers.AutoscalerDecisionOperator.SCALE_UP)

    def test_transient_terminal_replicas_retry_without_blocked_notification(
            self):
        cases = (
            (serve_state.ReplicaStatus.PREEMPTED, False),
            (serve_state.ReplicaStatus.SHUTTING_DOWN, False),
            (serve_state.ReplicaStatus.UNKNOWN, True),
        )
        for status, is_scale_down in cases:
            with self.subTest(status=status, is_scale_down=is_scale_down):
                autoscaler = self._autoscaler()
                info = self._failed_replica(unrecoverable=False,
                                            status=status,
                                            is_scale_down=is_scale_down)

                with mock.patch.object(
                        autoscalers.operator_notifications,
                        'record_notification') as record_notification:
                    decisions = autoscaler.generate_scaling_decisions([info],
                                                                      [1])

                record_notification.assert_not_called()
                self.assertEqual(len(decisions), 1)
                self.assertEqual(
                    decisions[0].operator,
                    autoscalers.AutoscalerDecisionOperator.SCALE_UP)


class TestAutoscalerInfo(unittest.TestCase):
    """Autoscaler status should expose a current rolling request rate."""

    def test_info_reports_only_requests_inside_window(self):
        autoscaler = object.__new__(autoscalers.Autoscaler)
        autoscaler.target_num_replicas = 2
        autoscaler.min_replicas = 1
        autoscaler.max_replicas = 4
        autoscaler.min_replicas_by_accelerator = {}
        autoscaler.target_num_replicas_by_accelerator = {}
        autoscaler.warm_retention_target_by_accelerator = {}
        autoscaler.cold_launch_authority_by_accelerator = {}
        autoscaler.reserved_capacity_fill = False
        autoscaler.qps_window_size = 60
        autoscaler.request_timestamps = [900.0, 940.0, 950.0, 999.0]

        with mock.patch('sky.serve.autoscalers.time.time', return_value=1000.0):
            info = autoscaler.info()

        self.assertEqual(info['recent_request_count'], 3)
        self.assertEqual(info['request_window_seconds'], 60)
        self.assertEqual(info['requests_per_second'], 0.05)
        self.assertEqual(info['capacity_target_by_accelerator'], {})
        self.assertFalse(info['capacity_target_complete'])


class TestQueueLengthAutoscalerIdleReplicas(unittest.TestCase):
    """Idle detection should use one grouped pool lookup."""

    def _spec(self):
        return _autoscaler_spec(min_replicas=0,
                                max_replicas=10,
                                queue_length_threshold=1)

    def _replica(self, replica_id, cluster_name):
        info = mock.Mock(spec=replica_managers.ReplicaInfo)
        info.replica_id = replica_id
        info.cluster_name = cluster_name
        info.version = 1
        info.status = serve_state.ReplicaStatus.READY
        info.is_terminal = False
        return info

    def test_idle_replicas_use_grouped_counts_once(self):
        autoscaler = autoscalers.QueueLengthAutoscaler('pool-a',
                                                       self._spec(),
                                                       version=1)
        replicas = [
            self._replica(1, 'replica-1'),
            self._replica(2, 'replica-2'),
            self._replica(3, 'replica-3'),
        ]

        with mock.patch(
                'sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool',
                return_value={'replica-2': 2}) as grouped_counts, \
             mock.patch(
                 'sky.serve.autoscalers.managed_job_state.'
                 'get_nonterminal_job_ids_by_pool') as per_replica_lookup:
            idle = autoscaler._get_idle_replicas(replicas)

        self.assertEqual([info.replica_id for info in idle], [1, 3])
        grouped_counts.assert_called_once_with('pool-a')
        per_replica_lookup.assert_not_called()

    def test_idle_replicas_treat_zero_or_missing_counts_as_idle(self):
        autoscaler = autoscalers.QueueLengthAutoscaler('pool-a',
                                                       self._spec(),
                                                       version=1)
        replicas = [
            self._replica(1, 'replica-1'),
            self._replica(2, 'replica-2'),
            self._replica(3, 'replica-3'),
        ]

        with mock.patch(
                'sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool',
                return_value={
                    'replica-1': 0,
                    'replica-2': 1,
                }):
            idle = autoscaler._get_idle_replicas(replicas)

        self.assertEqual([info.replica_id for info in idle], [1, 3])

    def test_scale_down_reuses_grouped_counts_once(self):
        autoscaler = autoscalers.QueueLengthAutoscaler('pool-a',
                                                       self._spec(),
                                                       version=1)
        replicas = [
            self._replica(1, 'replica-1'),
            self._replica(2, 'replica-2'),
            self._replica(3, 'replica-3'),
        ]
        autoscaler.target_num_replicas = 1
        autoscaler._set_target_num_replicas_with_hysteresis = lambda: None

        with mock.patch(
                'sky.serve.autoscalers.managed_job_state.'
                'get_nonterminal_job_counts_by_pool',
                return_value={'replica-2': 2}) as grouped_counts, \
             mock.patch(
                 'sky.serve.autoscalers.managed_job_state.'
                 'get_nonterminal_job_ids_by_pool') as per_replica_lookup:
            decisions = autoscaler._generate_scaling_decisions(replicas)

        self.assertEqual([decision.target for decision in decisions], [3, 1])
        grouped_counts.assert_called_once_with('pool-a')
        per_replica_lookup.assert_not_called()


class TestInstanceAwareGpuShapeCache(unittest.TestCase):
    """The GPU-shape memo must only cache a post-launch resolution.

    While a replica is provisioning, its cluster record is rewritten for every
    failover attempt, so the accelerator resolved mid-launch can change until
    the launch finishes.
    """

    def setUp(self):
        # Scale-down selection batch-resolves cluster records once; feed it
        # non-None records so ReplicaInfo.handle(record) (mocked) is used.
        patcher = mock.patch(
            'sky.serve.autoscalers.global_user_state.get_clusters_from_names',
            side_effect=lambda names: {name: {
                'handle': None
            } for name in names})
        self.mock_get_clusters = patcher.start()
        self.addCleanup(patcher.stop)

    def _make_autoscaler(self):
        autoscaler = object.__new__(
            autoscalers.InstanceAwareRequestRateAutoscaler)
        autoscaler._gpu_shape_cache = {}
        autoscaler._replica_cost_cache = {}
        autoscaler._gpu_shape_handles_for_tick = None
        autoscaler._bare_key_warned = set()
        autoscaler._snap_target_on_next_recompute = False
        autoscaler._qps_dict_by_version = {}
        autoscaler._qps_dict_unavailable_versions_for_tick = None
        autoscaler.min_replicas_by_accelerator = {}
        autoscaler.target_num_replicas_by_accelerator = {}
        autoscaler.warm_retention_target_by_accelerator = {}
        autoscaler.cold_launch_authority_by_accelerator = {}
        autoscaler.compatibility_profiles = []
        autoscaler.queued_compatibility_profiles = []
        autoscaler.rejected_compatibility_profiles = []
        autoscaler._compatibility_demand_complete = False
        autoscaler.configured_accelerator_shapes = {}
        autoscaler.free_reserved_slots_by_accelerator = {}
        autoscaler.request_timestamps = []
        autoscaler.qps_window_size = (
            constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        autoscaler.latest_version = 1
        return autoscaler

    def _make_replica(self, gpu_type, launch_status, count=1):
        info = mock.Mock()
        info.replica_id = 1
        info.version = 1
        info.cluster_name = 'mock-cluster'
        info.status_property.sky_launch_status = launch_status
        info.handle.return_value.launched_resources.accelerators = {
            gpu_type: count
        }
        return info

    def test_provisioning_resolution_is_not_cached(self):
        autoscaler = self._make_autoscaler()
        info = self._make_replica('A100', common_utils.ProcessStatus.RUNNING)
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('A100', 1))
        # Failover rewrote the cluster record with a different accelerator
        # while the replica was still provisioning: it must be re-resolved.
        info.handle.return_value.launched_resources.accelerators = {'L4': 4}
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 4))

    def test_decision_preload_skips_terminal_replica_without_cluster(self):
        """A retained failed row must not reopen PostgreSQL every tick."""
        autoscaler = self._make_autoscaler()
        terminal = self._make_replica('A100', common_utils.ProcessStatus.FAILED)
        terminal.replica_id = 1
        terminal.cluster_name = 'deleted-cluster'
        terminal.is_terminal = True
        live = self._make_replica('L4', common_utils.ProcessStatus.RUNNING)
        live.replica_id = 2
        live.cluster_name = 'live-cluster'
        live.is_terminal = False

        decision_inputs = (
            autoscalers.prepare_controller_scaling_decision_inputs(
                autoscaler, [terminal, live]))

        self.mock_get_clusters.assert_called_once_with(['live-cluster'])
        assert decision_inputs is not None
        self.assertEqual(decision_inputs.replica_ids, (1, 2))
        self.assertNotIn(1, decision_inputs.gpu_shape_handles)
        self.assertIn(2, decision_inputs.gpu_shape_handles)
        terminal.handle.assert_not_called()

    def test_controller_adapter_preserves_old_public_override_signature(self):

        class OldSignatureAutoscaler(
                autoscalers.InstanceAwareRequestRateAutoscaler):

            def generate_scaling_decisions(self, replica_infos,
                                           active_versions):
                self.calls.append((replica_infos, active_versions))
                return []

        autoscaler = object.__new__(OldSignatureAutoscaler)
        autoscaler.calls = []

        decision_inputs = (
            autoscalers.prepare_controller_scaling_decision_inputs(
                autoscaler, []))
        decisions = autoscalers.generate_controller_scaling_decisions(
            autoscaler, [], [7], decision_inputs)

        self.assertIsNone(decision_inputs)
        self.assertEqual(decisions, [])
        self.assertEqual(autoscaler.calls, [([], [7])])

    def test_controller_adapter_preserves_duck_autoscaler_signature(self):

        class DuckAutoscaler:

            def __init__(self):
                self.calls = []

            def generate_scaling_decisions(self, replica_infos,
                                           active_versions):
                self.calls.append((replica_infos, active_versions))
                return []

        autoscaler = DuckAutoscaler()

        decision_inputs = (
            autoscalers.prepare_controller_scaling_decision_inputs(
                autoscaler, []))
        decisions = autoscalers.generate_controller_scaling_decisions(
            autoscaler, [], [9], decision_inputs)

        self.assertIsNone(decision_inputs)
        self.assertEqual(decisions, [])
        self.assertEqual(autoscaler.calls, [([], [9])])

    def test_resolution_cached_once_launch_succeeds(self):
        autoscaler = self._make_autoscaler()
        info = self._make_replica('L4',
                                  common_utils.ProcessStatus.SUCCEEDED,
                                  count=4)
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 4))
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 4))
        self.assertEqual(info.handle.call_count, 1)

    def test_upscale_uses_observed_replica_capacity(self):
        """Excess QPS equal to one 4-GPU replica's capacity must add ONE
        replica, not four (per-GPU key + count-weighted fleet)."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        autoscaler.qps_window_size = 10
        # Two READY 4-GPU replicas -> capacity 0.8 qps.
        infos = []
        for rid in (1, 2):
            info = self._make_replica('L4',
                                      common_utils.ProcessStatus.SUCCEEDED,
                                      count=4)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            infos.append(info)
        # 12 requests in a 10s window = 1.2 qps: 0.4 excess = exactly one
        # more 4-GPU replica.
        autoscaler.request_timestamps = [0.0] * 12
        autoscaler.target_num_replicas = 2
        autoscaler.upscale_counter = 0
        autoscaler.downscale_counter = 0
        autoscaler.scale_up_threshold = 1
        autoscaler.scale_down_threshold = 1
        autoscaler.min_replicas = 1
        autoscaler.max_replicas = 20
        autoscaler.num_overprovision = None
        autoscaler._set_target_num_replicas_with_instance_aware_logic(infos)
        self.assertEqual(autoscaler.target_num_replicas, 3)

    def test_scale_from_zero_with_pending_traffic(self):
        """min_replicas=0 + traffic + empty fleet must scale up, not
        stay pinned at zero."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        autoscaler.qps_window_size = 10
        # 2 requests in a 10s window = 0.2 qps -> 2 replicas at 0.1 each.
        autoscaler.request_timestamps = [0.0] * 2
        autoscaler.target_num_replicas = 0
        autoscaler.upscale_counter = 0
        autoscaler.downscale_counter = 0
        autoscaler.scale_up_threshold = 1
        autoscaler.scale_down_threshold = 1
        autoscaler.min_replicas = 0
        autoscaler.max_replicas = 20
        autoscaler.num_overprovision = None
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas, 2)

    def test_scale_down_sheds_expensive_replicas_first(self):
        """Among same-status replicas, paid cloud replicas are scaled
        down before zero-cost reserved ones (drill 2026-07-06 showed the
        old order shed the paid GCP replica only by luck)."""
        autoscaler = self._make_autoscaler()
        autoscaler._replica_cost_cache = {}
        autoscaler.target_qps_per_replica = {'L4': 0.1, 'A100': 0.1}

        def _replica(rid, cost):
            info = self._make_replica('L4',
                                      common_utils.ProcessStatus.SUCCEEDED)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            info.version = 1
            info.handle.return_value.launched_resources.get_cost.return_value \
                = cost
            return info

        free_k8s = _replica(1, 0.0)
        paid_spot = _replica(2, 0.21)
        cheap_spot = _replica(3, 0.11)
        selected = autoscaler._select_replicas_to_scale_down_by_qps(
            2, [free_k8s, paid_spot, cheap_spot])
        # Equal capacity (same type/qps): most expensive first, the
        # zero-cost replica survives. Cost breaks ties AFTER qps so a
        # high-capacity paid replica is never shed ahead of low-capacity
        # free ones (the downscale target assumes top-capacity retention).
        assert selected == [2, 3]

    def test_equal_capacity_tie_ranks_by_cost_per_capacity(self):
        """Among EQUAL-capacity replicas of different shapes, the
        machine-cost tie-break equals cost-per-unit-capacity: the
        pricier-for-the-same-throughput machine is shed first, even if
        its per-GPU price is lower."""
        autoscaler = self._make_autoscaler()
        autoscaler._replica_cost_cache = {}
        # A100 serves 0.4 qps on 1 GPU; L4 serves 0.1/GPU so L4:4 also 0.4.
        autoscaler.target_qps_per_replica = {'A100': 0.4, 'L4': 0.1}

        def _replica(rid, gpu, count, cost):
            info = self._make_replica(gpu,
                                      common_utils.ProcessStatus.SUCCEEDED,
                                      count=count)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            info.version = 1
            info.handle.return_value.launched_resources.get_cost.return_value \
                = cost
            return info

        a100 = _replica(1, 'A100', 1, 2.0)  # $5.0 per qps
        l4x4 = _replica(2, 'L4', 4, 2.4)  # $6.0 per qps (lower per-GPU!)
        selected = autoscaler._select_replicas_to_scale_down_by_qps(
            1, [a100, l4x4])
        assert selected == [2]

    def test_float_noise_does_not_split_equal_capacity_ties(self):
        """3 * 0.1 != 0.3 in floats: quantization must keep the
        mathematically equal capacities in one cost tie-break bucket."""
        autoscaler = self._make_autoscaler()
        autoscaler._replica_cost_cache = {}
        autoscaler.target_qps_per_replica = {'A100': 0.3, 'L4': 0.1}

        def _replica(rid, gpu, count, cost):
            info = self._make_replica(gpu,
                                      common_utils.ProcessStatus.SUCCEEDED,
                                      count=count)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            info.version = 1
            info.handle.return_value.launched_resources.get_cost.return_value \
                = cost
            return info

        a100 = _replica(1, 'A100', 1, 1.5)  # 0.3 qps
        l4x3 = _replica(2, 'L4', 3, 1.8)  # 3 * 0.1 qps (float-noisy)
        selected = autoscaler._select_replicas_to_scale_down_by_qps(
            1, [a100, l4x3])
        # Same capacity bucket after quantization -> pricier machine goes.
        assert selected == [2]

    def test_capacity_outranks_cost(self):
        """A high-capacity paid replica outlives low-capacity free ones:
        qps ranks before cost."""
        autoscaler = self._make_autoscaler()
        autoscaler._replica_cost_cache = {}
        autoscaler.target_qps_per_replica = {'L4': 0.1, 'A100': 0.4}

        def _replica(rid, gpu, cost):
            info = self._make_replica(gpu, common_utils.ProcessStatus.SUCCEEDED)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            info.version = 1
            info.handle.return_value.launched_resources.get_cost.return_value \
                = cost
            return info

        big_paid = _replica(1, 'A100', 2.0)  # qps 0.4, expensive
        small_free = _replica(2, 'L4', 0.0)  # qps 0.1, free
        small_free2 = _replica(3, 'L4', 0.0)
        selected = autoscaler._select_replicas_to_scale_down_by_qps(
            1, [big_paid, small_free, small_free2])
        # Lowest capacity goes first despite being free.
        assert selected == [3]

    def test_scale_down_uniform_cost_order_unchanged(self):
        """Uniform-cost fleets keep the pre-change ordering exactly."""
        autoscaler = self._make_autoscaler()
        autoscaler._replica_cost_cache = {}
        autoscaler.target_qps_per_replica = {'L4': 0.1}

        def _replica(rid):
            info = self._make_replica('L4',
                                      common_utils.ProcessStatus.SUCCEEDED)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            info.version = 1
            info.handle.return_value.launched_resources.get_cost.return_value \
                = 0.11
            return info

        infos = [_replica(1), _replica(2), _replica(3)]
        selected = autoscaler._select_replicas_to_scale_down_by_qps(1, infos)
        # Tie on status/cost/qps/version -> highest replica_id first.
        assert selected == [3]

    def test_scale_down_prefers_earlier_lifecycle_status(self):
        """A PROVISIONING replica must be selected before a READY one
        even when the READY replica has lower weighted capacity."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        ready_small = self._make_replica('L4',
                                         common_utils.ProcessStatus.SUCCEEDED,
                                         count=1)
        ready_small.replica_id = 1
        ready_small.status = serve_state.ReplicaStatus.READY
        ready_small.is_terminal = False
        ready_small.version = 1
        provisioning_big = self._make_replica(
            'L4', common_utils.ProcessStatus.RUNNING, count=4)
        provisioning_big.replica_id = 2
        provisioning_big.status = serve_state.ReplicaStatus.PROVISIONING
        provisioning_big.is_terminal = False
        provisioning_big.version = 1
        selected = autoscaler._select_replicas_to_scale_down_by_qps(
            1, [ready_small, provisioning_big])
        self.assertEqual(selected, [2])

    def test_scale_down_batches_handle_resolution(self):
        """One batched cluster read per selection; no bare handle() DB
        reads even though provisioning replicas are scored twice
        (shape + cost) and can never be served from the memos."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        infos = []
        for rid in (1, 2, 3):
            info = self._make_replica('L4', common_utils.ProcessStatus.RUNNING)
            info.replica_id = rid
            info.cluster_name = f'cluster-{rid}'
            info.status = serve_state.ReplicaStatus.PROVISIONING
            info.is_terminal = False
            info.handle.return_value.launched_resources.get_cost.return_value \
                = 0.5
            infos.append(info)
        selected = autoscaler._select_replicas_to_scale_down_by_qps(1, infos)
        self.assertEqual(selected, [3])
        self.mock_get_clusters.assert_called_once_with(
            ['cluster-1', 'cluster-2', 'cluster-3'])
        for info in infos:
            # Every handle() call must carry the pre-fetched record; a bare
            # call would be a per-replica cluster-table read.
            self.assertTrue(info.handle.call_args_list)
            for call in info.handle.call_args_list:
                self.assertTrue(call.args or call.kwargs)

    def test_scale_down_skips_batch_read_when_memos_cover_fleet(self):
        """A fully cached fleet must not touch the cluster table at all."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        infos = []
        for rid in (1, 2):
            info = self._make_replica('L4',
                                      common_utils.ProcessStatus.SUCCEEDED)
            info.replica_id = rid
            info.status = serve_state.ReplicaStatus.READY
            info.is_terminal = False
            infos.append(info)
            autoscaler._gpu_shape_cache[rid] = ('L4', 1)
            autoscaler._replica_cost_cache[rid] = 0.5
        selected = autoscaler._select_replicas_to_scale_down_by_qps(1, infos)
        self.assertEqual(selected, [2])
        self.mock_get_clusters.assert_not_called()
        for info in infos:
            info.handle.assert_not_called()

    def test_scale_down_missing_cluster_record_resolves_to_no_handle(self):
        """A replica whose cluster row is gone must not trigger a bare
        handle() fallback read; it degrades to unknown shape / zero cost."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        self.mock_get_clusters.side_effect = lambda names: {
            name: None for name in names
        }
        info = self._make_replica('L4', common_utils.ProcessStatus.RUNNING)
        info.replica_id = 1
        info.status = serve_state.ReplicaStatus.PROVISIONING
        info.is_terminal = False
        selected = autoscaler._select_replicas_to_scale_down_by_qps(1, [info])
        self.assertEqual(selected, [1])
        info.handle.assert_not_called()

    def test_gpu_count_weights_capacity(self):
        """A 4-GPU replica contributes 4x per-GPU capacity; an exact
        shape key overrides with a per-replica value."""
        autoscaler = self._make_autoscaler()
        autoscaler.target_qps_per_replica = {'L4': 0.1}
        self.assertAlmostEqual(
            autoscaler._get_target_qps_for_gpu_shape('L4', 1), 0.1)
        self.assertAlmostEqual(
            autoscaler._get_target_qps_for_gpu_shape('L4', 4), 0.4)
        # Exact shape key wins as a per-replica value (no multiplication).
        autoscaler.target_qps_per_replica = {'L4': 0.1, 'L4:4': 0.3}
        self.assertAlmostEqual(
            autoscaler._get_target_qps_for_gpu_shape('L4', 4), 0.3)
        # Count-suffixed key of a different count is normalized to per-GPU.
        autoscaler.target_qps_per_replica = {'L4:2': 0.2}
        self.assertAlmostEqual(
            autoscaler._get_target_qps_for_gpu_shape('L4', 4), 0.4)


if __name__ == '__main__':
    unittest.main()


class TestInstanceAwareUpdateVersion(unittest.TestCase):
    """`sky serve update` on a dict-QPS service must not crash.

    The base update_version snaps target_num_replicas via
    _calculate_target_num_replicas; without an instance-aware override that
    hook resolved to RequestRateAutoscaler's, which raises on dict
    target_qps_per_replica — so every update of an instance-aware service
    failed after the version was already persisted (partial update).
    """

    def _spec(self, qps_dict, min_replicas=1, max_replicas=20):
        return _autoscaler_spec(min_replicas=min_replicas,
                                max_replicas=max_replicas,
                                target_qps_per_replica=qps_dict)

    def _make_autoscaler(self, qps_dict):
        return autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec(qps_dict), version=1)

    def test_update_version_does_not_raise_on_dict_qps(self):
        autoscaler = self._make_autoscaler({'L4': 0.1, 'A100': 0.1})
        autoscaler.update_version(2, self._spec({
            'L4': 0.1,
            'A100': 0.2
        }), serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.latest_version, 2)
        # The new version's dict must be live (assigned before the base
        # recompute, not after).
        self.assertEqual(autoscaler.target_qps_per_replica, {
            'L4': 0.1,
            'A100': 0.2
        })

    def test_update_version_keeps_current_target(self):
        # The outdated-replica drain consumes the target before the
        # instance-aware recompute runs; a shape-blind snap here could
        # scale down all old replicas mid-rolling-update.
        autoscaler = self._make_autoscaler({'L4': 0.1})
        autoscaler.target_num_replicas = 12
        autoscaler.request_timestamps = [0.0] * autoscaler.qps_window_size
        autoscaler.update_version(2, self._spec({'L4': 0.8}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.target_num_replicas, 12)

    def test_update_version_reclips_target_to_new_max(self):
        autoscaler = self._make_autoscaler({'L4': 0.1})
        autoscaler.target_num_replicas = 12
        autoscaler.update_version(2, self._spec({'L4': 0.1}, max_replicas=5),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_version_and_catalog_change_are_atomic_for_decisions(self):
        autoscaler = self._make_autoscaler({'A100': 1.0})
        autoscaler.set_configured_accelerator_shapes({'A100': 1})
        now = time.time()
        autoscaler.collect_request_information({
            'timestamps': [now] * 60,
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 60,
            }],
            'compatibility_demand_complete': True,
        })
        new_spec = self._spec({'H100': 1.0}, min_replicas=0)
        entered_catalog_transition = threading.Event()
        resume_catalog_transition = threading.Event()
        decision_started = threading.Event()
        decisions = []
        errors = []
        original_setter = (autoscaler._set_configured_accelerator_shapes_locked)

        def _blocking_setter(shapes):
            entered_catalog_transition.set()
            assert resume_catalog_transition.wait(timeout=5)
            original_setter(shapes)

        def _update():
            try:
                autoscaler.update_version_and_accelerator_shapes(
                    2, new_spec, serve_utils.DEFAULT_UPDATE_MODE, {'H100': 1})
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        def _decide():
            decision_started.set()
            try:
                decisions.extend(autoscaler.generate_scaling_decisions([], [2]))
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        with mock.patch.object(autoscaler,
                               '_set_configured_accelerator_shapes_locked',
                               side_effect=_blocking_setter):
            updater = threading.Thread(target=_update)
            updater.start()
            self.assertTrue(entered_catalog_transition.wait(timeout=5))
            decision_thread = threading.Thread(target=_decide)
            decision_thread.start()
            self.assertTrue(decision_started.wait(timeout=5))
            decision_thread.join(timeout=0.05)
            self.assertTrue(decision_thread.is_alive())
            resume_catalog_transition.set()
            updater.join(timeout=5)
            decision_thread.join(timeout=5)

        self.assertFalse(updater.is_alive())
        self.assertFalse(decision_thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(autoscaler.latest_version, 2)
        self.assertEqual(autoscaler.configured_accelerator_shapes, {'H100': 1})
        self.assertFalse(
            any(decision.target == {'accelerators': {
                'A100': 1
            }} for decision in decisions))


class TestInstanceAwareUpdateRolloutSafety(unittest.TestCase):
    """After an update, the drain must never act on a stale target.

    The base generate_scaling_decisions drains outdated replicas by
    comparing ready new-version replicas against target_num_replicas
    BEFORE the subclass recomputes. If an update lowered per-replica
    capacity (raising the replica need), a stale target lets the drain
    scale down every old replica with only a fraction of the required
    new capacity ready — and hysteresis would delay the corrective
    upscale by the full upscale delay.
    """

    def _spec(self, qps_dict, min_replicas=1, max_replicas=20):
        return _autoscaler_spec(min_replicas=min_replicas,
                                max_replicas=max_replicas,
                                target_qps_per_replica=qps_dict)

    def _make_autoscaler(self, qps_dict):
        return autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec(qps_dict), version=1)

    def test_recompute_runs_before_outdated_drain(self):
        autoscaler = self._make_autoscaler({'L4': 0.1})
        calls = []
        with mock.patch.object(
                autoscaler,
                '_set_target_num_replicas_with_instance_aware_logic',
                side_effect=lambda infos: calls.append('recompute')), \
             mock.patch.object(
                autoscaler,
                '_select_outdated_replicas_to_scale_down',
                side_effect=lambda infos, versions:
                    (calls.append('drain'), [])[1]), \
             mock.patch.object(
                autoscaler,
                '_generate_scaling_decisions',
                return_value=[]):
            autoscaler.generate_scaling_decisions([], [1])
        self.assertEqual(calls, ['recompute', 'drain'])

    def test_single_recompute_per_tick(self):
        autoscaler = self._make_autoscaler({'L4': 0.1})
        with mock.patch.object(
                autoscaler,
                '_set_target_num_replicas_with_instance_aware_logic'
        ) as mock_set, \
             mock.patch.object(
                autoscaler,
                '_select_outdated_replicas_to_scale_down',
                return_value=[]):
            autoscaler.generate_scaling_decisions([], [1])
        self.assertEqual(mock_set.call_count, 1)

    def test_post_update_recompute_bypasses_hysteresis_once(self):
        # Update lowers per-replica capacity 10 -> 1 with 20 rps: the
        # first recompute after the update must snap the target to 20
        # immediately, not wait out the upscale delay counters.
        autoscaler = self._make_autoscaler({'A100': 10.0})
        autoscaler.target_num_replicas = 2
        autoscaler.update_version(2, self._spec({'A100': 1.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        autoscaler.request_timestamps = [0.0
                                        ] * (20 * autoscaler.qps_window_size)
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas, 20)
        # The bypass is one-shot: a subsequent noisy recompute is gated
        # by hysteresis again.
        autoscaler.request_timestamps = []
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas, 20)

    def test_stale_version_update_does_not_mutate_state(self):
        autoscaler = self._make_autoscaler({'A100': 10.0})
        # Consume the construction-armed snap so the assertion isolates
        # the stale update's (non-)effect.
        autoscaler._snap_target_on_next_recompute = False
        autoscaler.update_version(1, self._spec({'A100': 1.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.target_qps_per_replica, {'A100': 10.0})
        self.assertFalse(autoscaler._snap_target_on_next_recompute)

    def test_old_replicas_resolve_capacity_from_their_own_version_dict(self):
        # Shape-changing update {'L4': 0.1} -> {'A100': 10.0}: live L4
        # replicas launched under v1 must keep 0.1 capacity, not resolve
        # via the new dict's min-value fallback (10.0 — a 100x
        # overestimate that collapses the computed target and lets the
        # drain kill the old fleet before new capacity exists).
        autoscaler = self._make_autoscaler({'L4': 0.1})
        autoscaler.update_version(2, self._spec({'A100': 10.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(
            autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1), 0.1)
        self.assertEqual(
            autoscaler._get_target_qps_for_gpu_shape('A100', 1, version=2),
            10.0)
        # Unknown version (e.g. controller restarted and only knows the
        # latest dict) falls back to the latest dict.
        self.assertEqual(
            autoscaler._get_target_qps_for_gpu_shape('A100', 1, version=99),
            10.0)

    def test_version_dicts_pruned_to_live_versions(self):
        autoscaler = self._make_autoscaler({'L4': 0.1})
        autoscaler.update_version(2, self._spec({'A100': 10.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertIn(1, autoscaler._qps_dict_by_version)
        info = mock.Mock()
        info.replica_id = 1
        info.cluster_name = 'svc-1'
        info.version = 2
        info.is_terminal = False
        info.resources_override = {'accelerators': {'A100': 1}}
        with mock.patch.object(
                autoscaler,
                '_set_target_num_replicas_with_instance_aware_logic'), \
             mock.patch.object(
                autoscaler,
                '_select_outdated_replicas_to_scale_down',
                return_value=[]), \
             mock.patch.object(
                autoscaler, '_generate_scaling_decisions', return_value=[]):
            autoscaler.generate_scaling_decisions([info], [2])
        # No live replica on v1 anymore: its dict is dropped; v2 kept.
        self.assertNotIn(1, autoscaler._qps_dict_by_version)
        self.assertIn(2, autoscaler._qps_dict_by_version)

    def test_stale_version_update_does_not_reset_hysteresis(self):
        autoscaler = self._make_autoscaler({'A100': 10.0})
        autoscaler.upscale_counter = 7
        autoscaler.update_version(1, self._spec({'A100': 1.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.upscale_counter, 7)


class TestInstanceAwareMixedVersionArithmetic(unittest.TestCase):
    """Target and drain must agree on what a 'replica' is mid-update.

    Scenario from review: 100 old L4 (0.1 qps each, v1) + 1 ready new
    A100 (10 qps, v2) at 20 rps. A whole-fleet count target (102) made
    _generate_scaling_decisions enqueue 101 new A100s (it compares the
    target against latest-version replicas only); a count-based drain
    (target 2, 1 ready new) retired 99 old replicas while their capacity
    was still needed.
    """

    def _spec(self, qps_dict, min_replicas=1, max_replicas=200):
        return _autoscaler_spec(min_replicas=min_replicas,
                                max_replicas=max_replicas,
                                target_qps_per_replica=qps_dict)

    def _replica(self, replica_id, gpu_type, version, is_ready=True):
        info = mock.Mock()
        info.replica_id = replica_id
        info.version = version
        info.is_terminal = False
        info.is_ready = is_ready
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.handle.return_value.launched_resources.accelerators = {gpu_type: 1}
        return info

    def _mid_update_fleet(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'L4': 0.1}), version=1)
        autoscaler.update_version(2, self._spec({'A100': 10.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        replicas = [self._replica(i, 'L4', version=1) for i in range(1, 101)]
        replicas.append(self._replica(101, 'A100', version=2))
        # 20 rps over the window.
        autoscaler.request_timestamps = [0.0
                                        ] * (20 * autoscaler.qps_window_size)
        return autoscaler, replicas

    def test_target_counts_latest_version_replicas_only(self):
        autoscaler, replicas = self._mid_update_fleet()
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        # Demand 20, one ready A100 covers 10, one more A100 covers the
        # rest: the target is 2 latest-version replicas — not 102.
        self.assertEqual(autoscaler.target_num_replicas, 2)

    def test_drain_keeps_old_capacity_covering_shortfall(self):
        autoscaler, replicas = self._mid_update_fleet()
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        # Ready new capacity 10 of demand 20: all 100 old L4s (10 qps
        # total) are needed to cover the shortfall — none drained.
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(
                replicas, [1, 2]), [])

    def test_drain_retires_all_old_once_target_ready(self):
        autoscaler, replicas = self._mid_update_fleet()
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        replicas.append(self._replica(102, 'A100', version=2))
        drained = autoscaler._select_outdated_replicas_to_scale_down(
            replicas, [1, 2])
        self.assertEqual(sorted(drained), list(range(1, 101)))

    def test_drain_low_traffic_keeps_base_count_floor(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'L4': 0.1}), version=1)
        autoscaler.update_version(2, self._spec({'A100': 10.0}),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        autoscaler.target_num_replicas = 1
        autoscaler.request_timestamps = []
        replicas = [self._replica(1, 'L4', version=1)]
        # No traffic and no ready new replica: zero shortfall, but the
        # base-class count floor (target 1 - ready 0) keeps the standby.
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(replicas, [1]),
            [])

    def test_unknown_version_rehydrates_from_prepared_batch(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        old = self._replica(1, 'L4', version=1)
        autoscaler._gpu_shape_cache[1] = ('L4', 1)
        autoscaler._replica_cost_cache[1] = 0.0
        old_spec = mock.Mock()
        old_spec.target_qps_per_replica = {'L4': 0.1}

        def _resolve(*_args):
            return [
                autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1)
            ]

        with mock.patch.object(autoscalers.serve_state,
                               'get_specs',
                               return_value={1: old_spec}) as mock_get, \
             mock.patch.object(autoscaler,
                               '_generate_scaling_decisions_locked',
                               side_effect=_resolve):
            self.assertEqual(
                autoscaler.generate_scaling_decisions([old], [1, 3]), [0.1])
            # Memoized: the second decision must not hit the DB again.
            self.assertEqual(
                autoscaler.generate_scaling_decisions([old], [1, 3]), [0.1])
        mock_get.assert_called_once_with('svc', [1])

    def test_unprepared_unknown_version_falls_back_without_db(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        with mock.patch.object(autoscalers.serve_state,
                               'get_specs',
                               side_effect=AssertionError):
            # Falls back to the latest dict's min-value fallback.
            self.assertEqual(
                autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1),
                10.0)

    def test_version_fallback_read_once_per_tick_and_retries(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        recovered_spec = mock.Mock()
        recovered_spec.target_qps_per_replica = {'L4': 0.1}
        state = {'recovered': False}
        old = self._replica(1, 'L4', version=1)
        autoscaler._gpu_shape_cache[1] = ('L4', 1)
        autoscaler._replica_cost_cache[1] = 0.0

        def _get_specs(*_args):
            if not state['recovered']:
                raise RuntimeError('state store unavailable')
            return {1: recovered_spec}

        def _resolve_repeatedly(*_args):
            return [
                autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1)
                for _ in range(3)
            ]

        with mock.patch.object(autoscalers.serve_state,
                               'get_specs',
                               side_effect=_get_specs) as mock_get, \
             mock.patch.object(autoscaler,
                               '_generate_scaling_decisions_locked',
                               side_effect=_resolve_repeatedly):
            self.assertEqual(
                autoscaler.generate_scaling_decisions([old], [1, 3]),
                [10.0, 10.0, 10.0])
            mock_get.assert_called_once_with('svc', [1])

            state['recovered'] = True
            self.assertEqual(
                autoscaler.generate_scaling_decisions([old], [1, 3]),
                [0.1, 0.1, 0.1])
            self.assertEqual(mock_get.call_count, 2)

    def test_version_fallback_does_not_authorize_rolling_drain(self):
        # Controller restart mid-update: only the latest spec is cached.
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=2)
        replicas = [self._replica(i, 'L4', version=1) for i in range(1, 101)]
        replicas.append(self._replica(101, 'A100', version=2))
        autoscaler.request_timestamps = [0.0
                                        ] * (60 * autoscaler.qps_window_size)
        for info in replicas:
            gpu_type = 'A100' if info.version == 2 else 'L4'
            autoscaler._gpu_shape_cache[info.replica_id] = (gpu_type, 1)
            autoscaler._replica_cost_cache[info.replica_id] = 0.5
        recovered_spec = self._spec({'L4': 0.1})

        with mock.patch.object(
                autoscalers.serve_state,
                'get_specs',
                side_effect=[RuntimeError('state store unavailable'), {
                    1: recovered_spec
                }]) as mock_get, \
             mock.patch.object(autoscalers.logger,
                               'warning') as mock_warning:
            first = autoscaler.generate_scaling_decisions(replicas, [1, 2])
            self.assertEqual([
                decision for decision in first if decision.operator ==
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
            ], [])
            self.assertEqual(
                len([
                    decision for decision in first if decision.operator ==
                    autoscalers.AutoscalerDecisionOperator.SCALE_UP
                ]), 5)
            mock_get.assert_called_once_with('svc', [1])
            self.assertEqual(mock_warning.call_count, 1)

            second = autoscaler.generate_scaling_decisions(replicas, [1, 2])
            self.assertEqual([
                decision for decision in second if decision.operator ==
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
            ], [])
            self.assertEqual(mock_get.call_count, 2)

    def test_version_fallback_tick_cleanup_after_decision_failure(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        recovered_spec = mock.Mock()
        recovered_spec.target_qps_per_replica = {'L4': 0.1}
        old = self._replica(1, 'L4', version=1)
        autoscaler._gpu_shape_cache[1] = ('L4', 1)
        autoscaler._replica_cost_cache[1] = 0.0
        calls = 0

        def _decide(*_args):
            nonlocal calls
            calls += 1
            capacity = autoscaler._get_target_qps_for_gpu_shape('L4',
                                                                1,
                                                                version=1)
            if calls == 1:
                raise RuntimeError('decision failed')
            return [capacity]

        with mock.patch.object(
                autoscalers.serve_state,
                'get_specs',
                side_effect=[{}, {
                    1: recovered_spec
                }]) as mock_get, \
             mock.patch.object(autoscaler,
                               '_generate_scaling_decisions_locked',
                               side_effect=_decide):
            with self.assertRaisesRegex(RuntimeError, 'decision failed'):
                autoscaler.generate_scaling_decisions([old], [1, 3])
            self.assertEqual(
                autoscaler.generate_scaling_decisions([old], [1, 3]), [0.1])
        self.assertEqual(mock_get.call_count, 2)

    def test_rebuilt_autoscaler_first_tick_snaps_before_drain(self):
        # Controller restart mid-rolling-update: the fresh autoscaler
        # starts at target=min_replicas (1). Its first recompute must
        # apply the real target directly — with hysteresis gating it for
        # the upscale delay, the drain's 'ready latest >= target' cutoff
        # would retire all 100 old L4s against the stale minimum.
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=2)
        autoscaler._qps_dict_by_version[1] = {'L4': 0.1}
        replicas = [self._replica(i, 'L4', version=1) for i in range(1, 101)]
        replicas.append(self._replica(101, 'A100', version=2))
        autoscaler.request_timestamps = [0.0
                                        ] * (20 * autoscaler.qps_window_size)
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        self.assertEqual(autoscaler.target_num_replicas, 2)
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(
                replicas, [1, 2]), [])


class TestCompatibilityAwareAutoscaling(unittest.TestCase):
    """Exact-card demand allocation and graceful transition behavior."""

    def _spec(self,
              *,
              max_replicas=4,
              floors=None,
              num_overprovision=None,
              reserved_capacity_fill=False,
              upscale_delay_seconds=0,
              downscale_delay_seconds=0):
        return _autoscaler_spec(min_replicas=0,
                                min_replicas_by_accelerator=floors or {},
                                max_replicas=max_replicas,
                                num_overprovision=num_overprovision,
                                target_qps_per_replica={
                                    'L4': 1.0,
                                    'A100': 1.0,
                                    'H100': 1.0,
                                },
                                upscale_delay_seconds=upscale_delay_seconds,
                                downscale_delay_seconds=downscale_delay_seconds,
                                reserved_capacity_fill=reserved_capacity_fill)

    def _autoscaler(self, **kwargs):
        return autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec(**kwargs), version=1)

    def _profiles(self, priority, cards, count=60):
        now = time.time()
        return [{
            'timestamp': now,
            'priority': priority,
            'compatible_accelerators': tuple(cards),
        } for _ in range(count)]

    def _replica(self,
                 replica_id,
                 card,
                 *,
                 ready=True,
                 zero_cost=False,
                 version=1):
        info = mock.Mock()
        info.replica_id = replica_id
        info.cluster_name = f'svc-{replica_id}'
        info.version = version
        info.is_terminal = False
        info.is_ready = ready
        info.is_zero_cost = zero_cost
        info.resources_override = {'accelerators': {card: 1}}
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED if ready else None)
        info.handle.return_value = None
        return info

    def test_priority_allocates_scarce_max_capacity_first(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.compatibility_profiles = (self._profiles(20, ['L4']) +
                                             self._profiles(50, ['A100']))
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})

    def test_ready_reserved_card_serves_flexible_cheapest_card_demand(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        replicas = [
            self._replica(1, 'L4'),
            self._replica(2, 'A100', zero_cost=True),
        ]
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_qps_retiring_warm_card_cold_replacement_uses_cheapest_card(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = self._autoscaler(max_replicas=1,
                                      upscale_delay_seconds=4 * interval,
                                      downscale_delay_seconds=300)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        autoscaler.request_timestamps = [now] * 60
        autoscaler._compatibility_demand_complete = True
        a100 = self._replica(1, 'A100', zero_cost=True)

        self.assertEqual(autoscaler.generate_scaling_decisions([a100], [1]), [])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

        a100.status_property.is_scale_down = True
        decisions = autoscaler.generate_scaling_decisions([a100], [1])

        # Demand attribution already stays on L4. Retirement only changes the
        # supply-aware actuation target, which authorizes an L4 replacement.
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'L4': 1
            }
        }])

        # The cold-launch fence is a supply invariant, not a property of the
        # retiring row. It must survive the row disappearing before card-map
        # hysteresis adopts the new placement.
        decisions = autoscaler.generate_scaling_decisions([], [1])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'L4': 1
            }
        }])

    def test_qps_reclaimed_floor_card_uses_returned_reserved_slot(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = self._autoscaler(max_replicas=2,
                                      floors={'A100': 1},
                                      upscale_delay_seconds=4 * interval,
                                      downscale_delay_seconds=300)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        autoscaler.compatibility_profiles = self._profiles(20, ['L4', 'A100'],
                                                           count=120)
        autoscaler.request_timestamps = [now] * 120
        autoscaler._compatibility_demand_complete = True
        l4 = self._replica(1, 'L4')
        a100 = self._replica(2, 'A100', zero_cost=True)

        self.assertEqual(autoscaler.generate_scaling_decisions([l4, a100], [1]),
                         [])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })

        # The reclaimed A100 row disappears and its physical reserved slot is
        # now free. The hard floor must stay on A100 and consume that returned
        # zero-cost slot; flexible demand must not duplicate the unit on L4.
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 1})
        decisions = autoscaler.generate_scaling_decisions([l4], [1])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'A100': 1
            }
        }])

    def test_free_reserved_card_does_not_own_flexible_demand(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 1})
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_qps_shape_preload_does_not_hold_demand_state_lock(self):
        autoscaler = self._autoscaler(max_replicas=1)
        preload_started = threading.Event()
        release_preload = threading.Event()
        decision_errors = []

        def _blocked_preload(_):
            preload_started.set()
            if not release_preload.wait(timeout=5):
                raise TimeoutError('test did not release shape preload')
            return {}

        def _decide():
            try:
                autoscaler.generate_scaling_decisions([], [1])
            except Exception as error:  # pylint: disable=broad-except
                decision_errors.append(error)

        with mock.patch.object(autoscaler,
                               '_resolve_gpu_shape_handles',
                               side_effect=_blocked_preload):
            decision_thread = threading.Thread(target=_decide)
            decision_thread.start()
            self.assertTrue(preload_started.wait(timeout=5))
            acquired = autoscaler._instance_state_lock.acquire(timeout=1)
            self.assertTrue(acquired)
            if acquired:
                autoscaler._instance_state_lock.release()
            release_preload.set()
            decision_thread.join(timeout=5)

        self.assertFalse(decision_thread.is_alive())
        self.assertEqual(decision_errors, [])

    def test_flexible_demand_claims_reserved_slot_before_fill(self):
        autoscaler = self._autoscaler(max_replicas=1,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        autoscaler.compatibility_profiles = self._profiles(20, ['L4', 'A100'])
        autoscaler.request_timestamps = [now] * 60
        autoscaler._compatibility_demand_complete = True
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 1})
        reserved_key = {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                'A100': 1
            },
            'use_spot': False,
            'image_id': None,
            'disk_tier': None,
        }
        for _ in range(2):
            autoscaler.collect_reserved_capacity(1, [reserved_key], now)

        decisions = autoscaler.generate_scaling_decisions([], [1])

        scale_ups = [
            decision for decision in decisions if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        self.assertEqual([decision.target for decision in scale_ups], [{
            'accelerators': {
                'A100': 1
            }
        }])

    def test_reserved_fill_targets_card_left_after_demand_claim(self):
        autoscaler = self._autoscaler(max_replicas=2,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        autoscaler.compatibility_profiles = self._profiles(50, ['A100'])
        autoscaler.request_timestamps = [now] * 60
        autoscaler._compatibility_demand_complete = True
        autoscaler.set_free_reserved_slots_by_accelerator({
            'L4': 1,
            'A100': 1,
        })
        reserved_keys = [{
            'cloud': 'Kubernetes',
            'region': f'research-{card.lower()}',
            'zone': None,
            'accelerators': {
                card: 1
            },
            'use_spot': False,
            'image_id': None,
            'disk_tier': None,
        } for card in ('L4', 'A100')]
        for _ in range(2):
            autoscaler.collect_reserved_capacity(2, reserved_keys, now)

        decisions = autoscaler.generate_scaling_decisions([], [1])

        scale_ups = [
            decision.target for decision in decisions if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        self.assertEqual(scale_ups[0], {'accelerators': {'A100': 1}})
        self.assertEqual(
            scale_ups[1], {
                constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True,
                'accelerators': {
                    'L4': 1
                },
            })

    def test_reserved_fill_stays_independent_then_replaces_paid_capacity(self):
        autoscaler = self._autoscaler(max_replicas=10,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'],
                                                           count=300)
        autoscaler.request_timestamps = [now] * 300
        autoscaler._compatibility_demand_complete = True
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 2})
        reserved_key = {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                'A100': 1
            },
            'use_spot': False,
            'image_id': None,
            'disk_tier': None,
        }
        for _ in range(2):
            autoscaler.collect_reserved_capacity(2, [reserved_key], now)

        paid = [self._replica(replica_id, 'L4') for replica_id in range(1, 6)]
        for info in paid:
            info.status = serve_state.ReplicaStatus.READY
            info.reserved_fill = False
            info.created_at = now - 10
            info.get_spot_location.return_value = None

        first = autoscaler.generate_scaling_decisions(paid, [1])
        fill_ups = [
            decision for decision in first
            if decision.operator == autoscalers.AutoscalerDecisionOperator.
            SCALE_UP and isinstance(decision.target, dict) and
            decision.target.get(constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY)
        ]

        self.assertEqual(autoscaler.get_final_target_num_replicas(), 5)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 5})
        self.assertEqual(len(fill_ups), 2)

        reserved = [
            self._replica(replica_id, 'A100', zero_cost=True)
            for replica_id in (6, 7)
        ]
        for info in reserved:
            info.status = serve_state.ReplicaStatus.READY
            info.reserved_fill = True
            info.created_at = now - 10
            info.get_spot_location.return_value = (
                spot_placer.Location.from_pickleable(reserved_key))
        autoscaler.collect_reserved_capacity(0, [reserved_key], now + 1)
        autoscaler.set_free_reserved_slots_by_accelerator({})

        second = autoscaler.generate_scaling_decisions([*paid, *reserved], [1])
        scale_downs = [
            decision.target for decision in second if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
        ]

        self.assertEqual(autoscaler.get_final_target_num_replicas(), 5)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 5})
        self.assertEqual(len(scale_downs), 2)
        self.assertTrue(set(scale_downs).issubset({1, 2, 3, 4, 5}))

    def test_empty_fleet_cold_starts_service_order_without_reserved_supply(
            self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_queued_compatibility_demand_is_a_replaceable_gauge(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 60,
            }],
        })
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})

        autoscaler.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [],
        })
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})

    def test_complete_report_keeps_prior_unattributed_arrivals(self):
        autoscaler = self._autoscaler(max_replicas=20)
        autoscaler.set_configured_accelerator_shapes({
            'A100': 1,
            'H100': 1,
        })
        now = time.time()

        def collect(count, *, complete):
            autoscaler.collect_request_information({
                'timestamps': [now] * count,
                'compatibility_profiles': ([{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': count,
                }] if complete else []),
                'queued_requests_by_compatibility': [],
                'compatibility_demand_complete': complete,
            })

        collect(60, complete=True)
        collect(600, complete=False)
        collect(60, complete=True)
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertTrue(autoscaler._compatibility_demand_complete)
        self.assertEqual(autoscaler.target_num_replicas, 12)
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 12)
        self.assertGreaterEqual(
            autoscaler.target_num_replicas_by_accelerator['A100'], 2)

    def test_compatibility_demand_survives_dynamic_state_handoff(self):
        autoscaler = self._autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        now = time.time()
        autoscaler.collect_request_information({
            'timestamps': [now],
            'compatibility_profiles': [{
                'timestamp': now,
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 2,
            }],
            'queued_requests_by_compatibility': [{
                'priority': 20,
                'compatible_accelerators': ['L4', 'A100'],
                'count': 3,
            }],
            'compatibility_demand_complete': True,
        })

        restored = self._autoscaler(max_replicas=2)
        restored.load_dynamic_states(autoscaler.dump_dynamic_states())

        self.assertEqual(restored.request_timestamps, [now])
        self.assertEqual(restored.compatibility_profiles,
                         autoscaler.compatibility_profiles)
        self.assertEqual(restored.queued_compatibility_profiles,
                         autoscaler.queued_compatibility_profiles)
        self.assertEqual(restored.configured_accelerator_shapes,
                         autoscaler.configured_accelerator_shapes)

    def test_task_shape_controls_capacity_and_exact_scale_up_override(self):
        autoscaler = self._autoscaler(max_replicas=4)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 8,
            'H100': 1,
        })
        autoscaler.compatibility_profiles = self._profiles(50, ['A100'],
                                                           count=480)
        autoscaler._compatibility_demand_complete = True
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        decisions = autoscaler._generate_scaling_decisions([])
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].target, {'accelerators': {'A100': 8}})

    def test_num_overprovision_keeps_qps_scale_up_exactly_shaped(self):
        autoscaler = self._autoscaler(max_replicas=2, num_overprovision=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.compatibility_profiles = self._profiles(50, ['A100'])
        autoscaler._compatibility_demand_complete = True
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        decisions = autoscaler._generate_scaling_decisions([])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertCountEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'A100': 1
            }
        }, {
            'accelerators': {
                'L4': 1
            }
        }])

    def test_num_overprovision_qps_scale_down_preserves_card_floor(self):
        autoscaler = self._autoscaler(max_replicas=2,
                                      floors={'A100': 1},
                                      num_overprovision=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler._compatibility_demand_complete = True
        replicas = [
            self._replica(1, 'A100'),
            self._replica(2, 'L4'),
            self._replica(3, 'L4'),
        ]
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)

        decisions = autoscaler._generate_scaling_decisions(replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertEqual([decision.target for decision in decisions], [3])

    def test_rolling_drain_waits_for_ready_exact_card_replacement(self):
        spec = self._spec(max_replicas=2, num_overprovision=1)
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                    spec,
                                                                    version=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.compatibility_profiles = (self._profiles(50, ['L4']) +
                                             self._profiles(50, ['A100']))
        autoscaler._compatibility_demand_complete = True
        old_l4 = self._replica(1, 'L4', version=1)
        latest_a100s = [
            self._replica(replica_id, 'A100', version=2)
            for replica_id in (2, 3, 4)
        ]

        first = autoscaler.generate_scaling_decisions([old_l4, *latest_a100s],
                                                      [1, 2])

        self.assertNotIn(1, [decision.target for decision in first])
        self.assertIn({'accelerators': {
            'L4': 1
        }}, [decision.target for decision in first])

        latest_l4 = self._replica(5, 'L4', version=2)
        second = autoscaler.generate_scaling_decisions(
            [old_l4, *latest_a100s, latest_l4], [1, 2])

        self.assertIn(1, [decision.target for decision in second])

    def test_constrained_peer_gets_a100_and_flexible_peer_gets_l4(self):
        autoscaler = self._autoscaler(max_replicas=2)
        autoscaler.compatibility_profiles = (
            self._profiles(50, ['L4', 'A100']) + self._profiles(50, ['A100']))
        replicas = [
            self._replica(1, 'L4'),
            self._replica(2, 'A100', zero_cost=True),
        ]
        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })

    def test_crossed_sets_keep_demand_on_cheapest_compatible_cards(self):
        autoscaler = self._autoscaler(max_replicas=2)
        # Demand attribution is independent of ready supply. The first profile
        # needs A100; the second profile stays on its cheapest card, L4.
        autoscaler.compatibility_profiles = (
            self._profiles(50, ['A100', 'H100']) +
            self._profiles(50, ['L4', 'A100']))
        replicas = [self._replica(1, 'A100'), self._replica(2, 'H100')]

        autoscaler._set_target_num_replicas_with_instance_aware_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })

    def test_crossed_sets_protect_worse_cold_fallback_at_capacity(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.compatibility_profiles = (
            self._profiles(50, ['L4', 'A100']) +
            self._profiles(50, ['A100', 'H100']))

        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})

    def test_benched_cheapest_card_is_not_replaced_by_costlier_cold_card(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4_location = mock.Mock(accelerators={'L4': 1})
        a100_location = mock.Mock(accelerators={'A100': 1})
        placer = mock.Mock()
        # L4 is currently benched, but it remains the nominal cheapest cold
        # card. Warm A100 stays compatible; its availability is not permission
        # to cold-launch an A100 for flexible demand.
        placer.active_locations.return_value = [a100_location]
        placer.known_location_costs.return_value = {
            l4_location: 1.0,
            a100_location: 2.0,
        }
        autoscaler.set_spot_placer(placer)
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        autoscaler._compatibility_demand_complete = True

        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_zero_cost_only_card_does_not_precede_paid_fallback(self):
        a100_location = mock.Mock(accelerators={'A100': 1})
        l4_location = mock.Mock(accelerators={'L4': 1})
        placer = mock.Mock()
        placer.known_location_costs.return_value = {
            a100_location: 0.0,
            l4_location: 1.0,
        }

        flexible = self._autoscaler(max_replicas=1)
        flexible.set_configured_accelerator_shapes({'A100': 1, 'L4': 1})
        flexible.set_spot_placer(placer)
        flexible.compatibility_profiles = self._profiles(50, ['A100', 'L4'])
        flexible._compatibility_demand_complete = True

        flexible._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(flexible.target_num_replicas_by_accelerator, {'L4': 1})

        exact = self._autoscaler(max_replicas=1)
        exact.set_configured_accelerator_shapes({'A100': 1, 'L4': 1})
        exact.set_spot_placer(placer)
        exact.compatibility_profiles = self._profiles(50, ['A100'])
        exact._compatibility_demand_complete = True

        exact._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(exact.target_num_replicas_by_accelerator, {'A100': 1})

    def test_partial_nominal_prices_preserve_service_order(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4_location = mock.Mock(accelerators={'L4': 1})
        a100_location = mock.Mock(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.known_location_costs.return_value = {
            l4_location: float('inf'),
            a100_location: 2.0,
        }
        autoscaler.set_spot_placer(placer)
        autoscaler.compatibility_profiles = self._profiles(50, ['L4', 'A100'])
        autoscaler._compatibility_demand_complete = True

        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_preempted_latest_qps_replica_cannot_cover_rolling_drain(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec(max_replicas=1), version=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        autoscaler.compatibility_profiles = self._profiles(50, ['L4'])
        autoscaler._compatibility_demand_complete = True
        old = self._replica(1, 'L4', version=1)
        preempted = self._replica(2, 'L4', version=2)
        preempted.status_property.preempted = True
        autoscaler.target_num_replicas = 1
        autoscaler.target_num_replicas_by_accelerator = {'L4': 1}

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [old, preempted], [1, 2])

        self.assertEqual(retired, [])

    def test_active_task_catalog_drops_removed_card_demand(self):
        autoscaler = self._autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        autoscaler.compatibility_profiles = self._profiles(50, ['A100'])
        autoscaler._compatibility_demand_complete = True

        autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})

    def test_provisioning_counts_for_launch_but_not_for_graceful_retirement(
            self):
        autoscaler = self._autoscaler(max_replicas=2)
        autoscaler.compatibility_profiles = self._profiles(50, ['A100'])
        paid_l4 = self._replica(1, 'L4')
        provisioning_a100 = self._replica(2, 'A100', ready=False)
        autoscaler._set_target_num_replicas_with_instance_aware_logic(
            [paid_l4, provisioning_a100])
        self.assertEqual(
            autoscaler._generate_scaling_decisions([paid_l4,
                                                    provisioning_a100]), [])
        provisioning_a100.is_ready = True
        decisions = autoscaler._generate_scaling_decisions(
            [paid_l4, provisioning_a100])
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator,
                         autoscalers.AutoscalerDecisionOperator.SCALE_DOWN)
        self.assertEqual(decisions[0].target, 1)

    def test_per_card_floor_is_independent_from_aggregate_floor(self):
        autoscaler = self._autoscaler(max_replicas=3,
                                      floors={
                                          'L4': 1,
                                          'A100-80GB': 1,
                                      })
        autoscaler.target_qps_per_replica['A100-80GB'] = 1.0
        autoscaler.compatibility_profiles = self._profiles(50, ['H100'])
        autoscaler._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100-80GB': 1,
            'H100': 1,
        })
