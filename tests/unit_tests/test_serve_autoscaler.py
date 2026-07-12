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
from sky.serve import serve_utils
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


class TestQueueLengthAutoscalerIdleReplicas(unittest.TestCase):
    """Idle detection should use one grouped pool lookup."""

    def _spec(self):
        return types.SimpleNamespace(min_replicas=0,
                                     max_replicas=10,
                                     num_overprovision=None,
                                     queue_length_threshold=1,
                                     upscale_delay_seconds=None,
                                     downscale_delay_seconds=None)

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

    def _make_autoscaler(self):
        autoscaler = object.__new__(
            autoscalers.InstanceAwareRequestRateAutoscaler)
        autoscaler._gpu_shape_cache = {}
        autoscaler._replica_cost_cache = {}
        autoscaler._bare_key_warned = set()
        autoscaler._snap_target_on_next_recompute = False
        autoscaler._qps_dict_by_version = {}
        autoscaler.latest_version = 1
        return autoscaler

    def _make_replica(self, gpu_type, launch_status, count=1):
        info = mock.Mock()
        info.replica_id = 1
        info.version = 1
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


class TestInstanceAwareUpdateVersion(unittest.TestCase):
    """`sky serve update` on a dict-QPS service must not crash.

    The base update_version snaps target_num_replicas via
    _calculate_target_num_replicas; without an instance-aware override that
    hook resolved to RequestRateAutoscaler's, which raises on dict
    target_qps_per_replica — so every update of an instance-aware service
    failed after the version was already persisted (partial update).
    """

    def _spec(self, qps_dict, min_replicas=1, max_replicas=20):
        return types.SimpleNamespace(min_replicas=min_replicas,
                                     max_replicas=max_replicas,
                                     num_overprovision=None,
                                     target_qps_per_replica=qps_dict,
                                     upscale_delay_seconds=None,
                                     downscale_delay_seconds=None)

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
        return types.SimpleNamespace(min_replicas=min_replicas,
                                     max_replicas=max_replicas,
                                     num_overprovision=None,
                                     target_qps_per_replica=qps_dict,
                                     upscale_delay_seconds=None,
                                     downscale_delay_seconds=None)

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
        info.version = 2
        info.is_terminal = False
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
        return types.SimpleNamespace(min_replicas=min_replicas,
                                     max_replicas=max_replicas,
                                     num_overprovision=None,
                                     target_qps_per_replica=qps_dict,
                                     upscale_delay_seconds=None,
                                     downscale_delay_seconds=None)

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

    def test_unknown_version_rehydrates_from_serve_state(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        old_spec = mock.Mock()
        old_spec.target_qps_per_replica = {'L4': 0.1}
        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               return_value=old_spec) as mock_get:
            self.assertEqual(
                autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1),
                0.1)
            # Memoized: the second resolution must not hit the DB again.
            autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1)
        mock_get.assert_called_once_with('svc', 1)

    def test_unknown_version_db_miss_falls_back_to_latest(self):
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', self._spec({'A100': 10.0}), version=3)
        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               return_value=None):
            # Falls back to the latest dict's min-value fallback.
            self.assertEqual(
                autoscaler._get_target_qps_for_gpu_shape('L4', 1, version=1),
                10.0)

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
