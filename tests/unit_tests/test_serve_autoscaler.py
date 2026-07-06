"""Unit tests for sky.serve.autoscalers."""
# pylint: disable=protected-access
import types
import unittest
from unittest import mock

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import controller as serve_controller
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.utils import common_utils


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
        spec = types.SimpleNamespace(min_replicas=1,
                                     max_replicas=3,
                                     num_overprovision=None)
        autoscaler = object.__new__(autoscalers.Autoscaler)
        autoscalers.Autoscaler.__init__(autoscaler, 'svc', spec, version=5)
        self.assertEqual(autoscaler.latest_version, 5)
        # Must stay one below latest so the unrecoverable-failure early-return
        # only arms once a replica at the latest version becomes ready.
        self.assertEqual(autoscaler.latest_version_ever_ready, 4)

    def test_base_init_defaults_to_initial_version(self):
        spec = types.SimpleNamespace(min_replicas=1,
                                     max_replicas=3,
                                     num_overprovision=None)
        autoscaler = object.__new__(autoscalers.Autoscaler)
        autoscalers.Autoscaler.__init__(autoscaler, 'svc', spec)
        self.assertEqual(autoscaler.latest_version, constants.INITIAL_VERSION)
        self.assertEqual(autoscaler.latest_version_ever_ready,
                         constants.INITIAL_VERSION - 1)

    def _route_spec(self,
                    pool=False,
                    use_ondemand_fallback=False,
                    target_qps_per_replica=2.0):
        return types.SimpleNamespace(
            pool=pool,
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
                               'from_spec') as mock_from_spec:
            serve_controller.SkyServeController('svc',
                                                mock.MagicMock(),
                                                version=7,
                                                host='localhost',
                                                port=8000)
        mock_from_spec.assert_called_once()
        # version is the 3rd positional arg (service_name, service_spec,
        # version).
        self.assertEqual(mock_from_spec.call_args.args[2], 7)


class TestInstanceAwareGpuShapeCache(unittest.TestCase):
    """The GPU-shape memo must only cache a post-launch resolution.

    While a replica is provisioning, its cluster record is rewritten for every
    failover attempt, so the accelerator resolved mid-launch can change until
    the launch finishes.
    """

    def _make_autoscaler(self):
        autoscaler = object.__new__(
            autoscalers.InstanceAwareRequestRateAutoscaler)
        autoscaler._gpu_shape_cache = {}
        autoscaler._replica_cost_cache = {}
        return autoscaler

    def _make_replica(self, gpu_type, launch_status, count=1):
        info = mock.Mock()
        info.replica_id = 1
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
